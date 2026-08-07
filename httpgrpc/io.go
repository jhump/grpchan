package httpgrpc

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/fullstorydev/grpchan/internal/sse"
	"google.golang.org/grpc/mem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	maxMessageSize = 100 * 1024 * 1024 // 100mb
)

// Default message size limits. These are defaults, not ceilings: an explicit
// limit is honored even when it is larger.
//
// They live here, rather than in the package that gathers call options, because
// they are not uniform. Streaming responses have always been bounded by
// maxMessageSize, where a unary response was never bounded at all, so a single
// default for both would change the behavior of one of them. Callers that want
// them to agree can say so with a call option.
const (
	// defaultMaxSend applies to both unary and streaming sends, neither of which
	// was ever bounded. It is not unlimited only because the size preface cannot
	// describe a larger message.
	defaultMaxSend = math.MaxInt32
	// defaultMaxRecvUnary preserves a unary response having no limit.
	defaultMaxRecvUnary = math.MaxInt32
	// defaultMaxRecvStream preserves the bound that the size-prefixed framing has
	// always applied to a streaming response.
	defaultMaxRecvStream = maxMessageSize
)

// drainLimit bounds what a client will read from a response body it is otherwise
// finished with. Reaching the end of a body is what leaves its connection
// reusable, since net/http will not pool one whose response was not fully read.
//
// It is small because the only tails worth having are small: a stream that ended
// with a trailer is already at its end, and an RPC that failed before the body
// leaves only what the error renderer wrote. Anything past that is not a tail but
// a stream, and discarding it would cost more than the connection is worth --
// especially to a client that is paying for the bytes.
const drainLimit = 4 * 1024

// limitOrDefault returns the given limit, or def when no limit was set. Call
// options report an unset limit as zero, and a negative one is meaningless.
func limitOrDefault(limit, def int) int {
	if limit <= 0 {
		return def
	}
	return limit
}

// checkSendSize reports whether an encoded message of the given size may be sent
// under the given limit. The size is checked here, where the message is encoded,
// so that enforcing the limit does not require encoding it a second time.
//
// Trailers are exempt: the limit applies to the messages of an RPC, and a stream
// that could not report its own outcome would be worse than one that exceeded a
// limit.
//
// TODO: the exemption is not free, since the receiver applies its limit to the
// trailer like any other message. A status carrying large error details can
// therefore be refused, leaving the caller knowing only that something was too
// big and never what the RPC actually returned. Consider bounding the trailer as
// well, shedding what can be spared -- error details first, then trailer
// metadata -- and recording that something was dropped, so that the outcome
// always gets through even when the whole of it cannot.
func checkSendSize(size int, isTrailer bool, maxSend int) error {
	if isTrailer || size <= maxSend {
		return nil
	}
	return &sendLimitError{size: size, limit: maxSend}
}

// sendLimitError reports a message refused for exceeding the send limit.
//
// It is a distinct type because refusing a message is not the same as failing to
// write one: nothing has been written, so the stream is intact and can still
// carry a status explaining what happened. A write failure leaves the stream in
// no state to report anything.
type sendLimitError struct {
	size  int
	limit int
}

func (e *sendLimitError) Error() string {
	return fmt.Sprintf("payload too large: %v > %v", e.size, e.limit)
}

// GRPCStatus lets status.FromError recover the code, so this reaches a caller as
// ResourceExhausted rather than an opaque error.
func (e *sendLimitError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, e.Error())
}

// checkRecvSize reports whether a message the sender has announced as the given
// size may be received under the given limit. This is checked before the message
// is read, so that an over-large one is rejected instead of buffered.
//
// Unlike checkSendSize, this makes no exception for the trailer. The limit exists
// to bound what a peer can make this process allocate, and a trailer is read from
// the same connection as everything else, so exempting it would leave a hole
// exactly the size of the limit. The event-stream framing has always applied its
// limit to trailers for a more prosaic reason -- an event's type is not known
// until it has been read -- so this also makes the two framings agree.
func checkRecvSize(size int, maxRecv int) error {
	if size <= maxRecv {
		return nil
	}
	return recvTooLarge("payload too large to receive: %v > %v", size, maxRecv)
}

// recvLimitError reports a message refused for exceeding the receive limit.
//
// As with sendLimitError this is a distinct type, so that a stream can tell a
// refusal apart from a transport failure. gRPC-Go ends the RPC on either, but
// only a refusal leaves the connection in a state where the outcome can still be
// reported, which is what lets this package answer with the real status instead
// of an abrupt end of stream.
type recvLimitError struct {
	msg string
}

func recvTooLarge(format string, args ...any) error {
	return &recvLimitError{msg: fmt.Sprintf(format, args...)}
}

func (e *recvLimitError) Error() string { return e.msg }

// GRPCStatus lets status.FromError recover the code, so this reaches a caller as
// ResourceExhausted rather than an opaque error.
func (e *recvLimitError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, e.msg)
}

// isLimitError reports whether err is a message refused for exceeding a size
// limit, as opposed to a failure to transfer one. Nothing was read or written in
// that case, so the stream itself is still intact.
func isLimitError(err error) bool {
	var sendErr *sendLimitError
	var recvErr *recvLimitError
	return errors.As(err, &sendErr) || errors.As(err, &recvErr)
}

// writeSizePreface writes the given 32-bit size to the given writer.
func writeSizePreface(w io.Writer, sz int32) error {
	return binary.Write(w, binary.BigEndian, sz)
}

// writeProtoMessage writes a length-delimited proto message to the given
// writer. This writes the size preface, indicating the size of the encoded
// message, followed by the actual message contents. If end is true, the
// size is written as a negative value, indicating to the receiver that this
// is the last message in the stream. (The last message should be an instance
// of HttpTrailer.)
func writeProtoMessage(w io.Writer, codec encoding.CodecV2, m interface{}, end bool, maxSend int) error {
	buf, err := codec.Marshal(m)
	if err != nil {
		return err
	}
	b := buf.Materialize()

	sz := len(b)
	if sz > math.MaxInt32 {
		return fmt.Errorf("message too large to send: %d bytes", sz)
	}
	if err := checkSendSize(sz, end, maxSend); err != nil {
		return err
	}
	if end {
		// trailer message is indicated w/ negative size
		sz = -sz
	}
	err = writeSizePreface(w, int32(sz))
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	if err == nil {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	return err
}

// readSizePreface reads a 32-bit size from the given reader. If the value is
// negative, it indicates the last message in the stream. Messages can have zero
// size, but the last message in the stream should never have zero size (so its
// size will be negative).
func readSizePreface(in io.Reader) (int32, error) {
	var sz int32
	err := binary.Read(in, binary.BigEndian, &sz)
	if err != nil {
		return 0, err
	}
	// Reject math.MinInt32: negating it overflows in two's complement and would
	// mean that -size would remain negative and potentially cause a panic if the
	// callers tries to allocate a buffer for the message.
	if sz == math.MinInt32 {
		return 0, errors.New("bad size preface: size overflow")
	}

	return sz, err
}

// asMetadata converts the given HTTP headers into GRPC metadata.
func asMetadata(header http.Header) (metadata.MD, error) {
	// metadata has same shape as http.Header,
	md := metadata.MD{}
	for k, vs := range header {
		k = strings.ToLower(k)
		for _, v := range vs {
			if strings.HasSuffix(k, "-bin") {
				vv, err := base64.URLEncoding.DecodeString(v)
				if err != nil {
					return nil, err
				}
				v = string(vv)
			}
			md[k] = append(md[k], v)
		}
	}
	return md, nil
}

var reservedHeaders = map[string]struct{}{
	"accept-encoding":   {},
	"connection":        {},
	"content-type":      {},
	"content-length":    {},
	"keep-alive":        {},
	"te":                {},
	"trailer":           {},
	"transfer-encoding": {},
	"upgrade":           {},
}

func toHeaders(md metadata.MD, h http.Header, prefix string) {
	// binary headers must be base-64-encoded
	for k, vs := range md {
		lowerK := strings.ToLower(k)
		if _, ok := reservedHeaders[lowerK]; ok {
			// ignore reserved header keys
			continue
		}
		isBin := strings.HasSuffix(lowerK, "-bin")
		for _, v := range vs {
			if isBin {
				v = base64.URLEncoding.EncodeToString([]byte(v))
			}
			h.Add(prefix+k, v)
		}
	}
}

type strAddr string

func (a strAddr) Network() string {
	if a != "" {
		// Per the documentation on net/http.Request.RemoteAddr, if this is
		// set, it's set to the IP:port of the peer (hence, TCP):
		// https://golang.org/pkg/net/http/#Request
		//
		// If we want to support Unix sockets later, we can
		// add our own grpc-specific convention within the
		// grpc codebase to set RemoteAddr to a different
		// format, or probably better: we can attach it to the
		// context and use that from serverHandlerTransport.RemoteAddr.
		return "tcp"
	}
	return ""
}

func (a strAddr) String() string { return string(a) }

type streamReader func() (streamMsg, error)

type streamWriter func(m any, isTrailer bool) error

type flusher interface {
	Flush() error
}

type streamMsg struct {
	codec     encoding.CodecV2
	data      []byte
	isTrailer bool
}

func (s *streamMsg) Decode(m any) error {
	return s.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(s.data)}, m)
}

func newSizePrefixedReader(r io.Reader, codec encoding.CodecV2, maxRecv int) func() (streamMsg, error) {
	return func() (streamMsg, error) {
		size, err := readSizePreface(r)
		if err != nil {
			return streamMsg{}, err
		}

		isTrailer := size < 0
		if isTrailer {
			size = -size
		}

		// The preface announces the size, so an over-large message is refused here,
		// before it is read, rather than after it has been pulled into memory. This
		// is also what keeps a bogus preface from being turned into an allocation of
		// whatever size it names: maxRecv defaults to maxMessageSize, which is the
		// bound this framing has always had.
		if err := checkRecvSize(int(size), maxRecv); err != nil {
			return streamMsg{}, err
		}

		data := make([]byte, size)
		_, err = io.ReadAtLeast(r, data, int(size))
		if errors.Is(err, io.EOF) { // io.EOF is returned if no bytes were read
			return streamMsg{}, io.ErrUnexpectedEOF
		} else if err != nil {
			return streamMsg{}, err
		}

		return streamMsg{
			codec:     codec,
			data:      data,
			isTrailer: isTrailer,
		}, nil
	}
}

func newSizePrefixedWriter(w io.Writer, codec encoding.CodecV2, maxSend int) func(m any, isTrailer bool) error {
	return func(m any, isTrailer bool) error {
		return writeProtoMessage(w, codec, m, isTrailer, maxSend)
	}
}

// jsonValueTooLargeError reports a single JSON value that exceeded the limit.
//
// As with a server-sent event, the reader gives up as soon as the limit is
// passed rather than reading on to find out how big the value really is, so Read
// is a lower bound on its full size.
type jsonValueTooLargeError struct {
	// Read is the number of bytes read for the value before it was abandoned.
	Read int64
	// Limit is the maximum size of a single value the reader will accept.
	Limit int
}

func (e *jsonValueTooLargeError) Error() string {
	return fmt.Sprintf("json value too large: read %d bytes with a limit of %d", e.Read, e.Limit)
}

// jsonValueLimiter bounds a single value in a stream of concatenated JSON values.
//
// A JSON stream carries no size ahead of each value, so the extent of one is only
// known once it has been parsed. Rather than let the decoder buffer a value of any
// size and measure it afterwards, this sits underneath the decoder and never hands
// it more than the limit beyond the end of the last value it finished. That end is
// where consumed reports, which is the decoder's own input offset.
//
// Capping each read is what makes the test reliable, rather than simply failing
// once that much is outstanding. The decoder refills only when the buffered data
// does not hold a complete value, but a refill reads as much as it is given, which
// may run well past the current value and into later ones. Bounding the read means
// anything still outstanding belongs to the value being parsed now, so a decoder
// that wants yet more has a value that genuinely exceeds the limit.
//
// The bound covers any whitespace between values as well, which costs a few bytes
// of headroom against a limit measured in megabytes.
type jsonValueLimiter struct {
	r     io.Reader
	limit int
	// consumed reports how much of the stream has been decoded into values already.
	consumed func() int64
	read     int64
	err      error
}

func (j *jsonValueLimiter) Read(p []byte) (int, error) {
	if j.err != nil {
		return 0, j.err
	}
	// One byte past the limit, so that a value of exactly the limit still fits and
	// the decoder asking for more is unambiguous.
	outstanding := j.read - j.consumed()
	allowed := int64(j.limit) + 1 - outstanding
	if allowed <= 0 {
		// Sticky, so that a decoder which reads again after an error cannot get
		// past this by reading the rest of the oversized value.
		j.err = &jsonValueTooLargeError{Read: outstanding, Limit: j.limit}
		return 0, j.err
	}
	if int64(len(p)) > allowed {
		p = p[:allowed]
	}
	n, err := j.r.Read(p)
	j.read += int64(n)
	return n, err
}

// newJSONReader reads a stream of concatenated JSON values, refusing any single
// value larger than maxValue.
//
// It takes no per-call receive limit because it is only used to read requests on
// the server, which has no call options to take one from. The limit it does take
// is the same ceiling the size-prefixed framing applies, so that neither framing
// can be made to buffer without bound.
func newJSONReader(r io.Reader, codec encoding.CodecV2, maxValue int) func() (streamMsg, error) {
	limiter := &jsonValueLimiter{r: r, limit: maxValue}
	d := json.NewDecoder(limiter)
	limiter.consumed = d.InputOffset
	return func() (streamMsg, error) {
		var msg json.RawMessage
		if err := d.Decode(&msg); err != nil {
			var tooLarge *jsonValueTooLargeError
			if errors.As(err, &tooLarge) {
				return streamMsg{}, recvTooLarge("payload too large to receive: %v", tooLarge)
			}
			return streamMsg{}, err
		}

		return streamMsg{
			codec: codec,
			data:  msg,
		}, nil
	}
}

func newJSONWriter(w io.Writer, codec encoding.CodecV2, maxSend int) func(m any, isTrailer bool) error {
	return func(m any, isTrailer bool) error {
		if isTrailer {
			panic("trailers are not supported for JSON")
		}

		data, err := codec.Marshal(m)
		if err != nil {
			return err
		}

		b := data.Materialize()
		if err := checkSendSize(len(b), isTrailer, maxSend); err != nil {
			return err
		}

		_, err = w.Write(b)
		return err
	}
}

func newSSEWriter(w io.Writer, flusher flusher, codec encoding.CodecV2, maxSend int) func(m any, isTrailer bool) error {
	e := sse.NewEncoder(w)
	return func(m any, isTrailer bool) error {
		data, err := codec.Marshal(m)
		if err != nil {
			return err
		}
		if err := checkSendSize(data.Len(), isTrailer, maxSend); err != nil {
			return err
		}

		if isTrailer {
			if err := e.Encode(&sse.Event{
				Type: "trailer",
				Data: data.Materialize(),
			}); err != nil {
				return err
			}
		} else {
			if err := e.Encode(&sse.Event{
				Data: data.Materialize(),
			}); err != nil {
				return err
			}
		}

		return flusher.Flush()
	}
}

func newSSEReader(r io.Reader, codec encoding.CodecV2, maxRecv int) func() (streamMsg, error) {
	// Server-sent events carry no size ahead of the data, so the limit is given to
	// the decoder, which fails as soon as an event exceeds it rather than reading
	// all of it first. Trailer events are not exempt here, as they are elsewhere,
	// because an event's type is not known until it has been read.
	d := sse.NewDecoderWithMaxData(r, maxRecv)

	return func() (streamMsg, error) {
		event, err := d.Decode()
		if errors.Is(err, sse.ErrDataTooLarge) {
			// The decoder's message reports how much was read and the limit it hit,
			// which is the useful part; it must not be dropped for a bare status.
			return streamMsg{}, recvTooLarge("payload too large to receive: %v", err)
		}
		if err != nil {
			return streamMsg{}, err
		}

		return streamMsg{
			codec:     codec,
			data:      event.Data,
			isTrailer: event.Type == "trailer",
		}, nil
	}
}
