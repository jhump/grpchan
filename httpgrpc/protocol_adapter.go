package httpgrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fullstorydev/grpchan/internal/sse"
)

// The protocol adapters defined in this file separate what is particular to a
// wire protocol from what every protocol shares. Each one carries gRPC semantics
// over HTTP 1.1 without requiring HTTP trailers: this package's own
// GRPC-over-HTTP protocol, which makes unary RPCs look like basic POST
// operations (just POST application/json and get application/json back, for
// example), and others like it, such as gRPC-Web and ConnectRPC.

// protocolAdapter contains methods used by both client and server protocols.
type protocolAdapter interface {
	// streamMessage encodes the given message into a "frame" for the protocol. The
	// given parameters are the encoded message bytes and a flag indicating whether
	// the bytes are compressed. The response is the framed data, as a sequence of
	// slices to be written in order, or an error.
	//
	// This method is called if the RPC is not a unary operation. So, even if a
	// single direction is unary (for example, in a client streaming operation, the
	// server response is unary) this method is used for both requests and responses.
	streamMessage(data []byte, compressed bool) ([][]byte, error)
}

// clientProtocolAdapterFactory creates a clientProtocolAdapter for a single RPC,
// whose messages are encoded with the named codec.
//
// Adapters are created per-RPC, rather than shared by the channel, because
// deframing a response can be stateful: the httpgrpc protocol reads JSON streams
// as server-sent events, and the SSE decoder buffers across messages. So an
// adapter must not outlive the RPC whose response it is reading. (Server-side
// adapters are already per-RPC, since determineProtocolAdapter is called for
// each request.)
//
// The codec is given at construction because the adapter needs it throughout:
// it labels the request, may decide how messages are framed, and is what a
// response has to be encoded with. A protocol that cannot carry the codec
// reports an error.
//
// The receive limit is given at construction because deframing is where it has to
// be applied: a frame announces or delimits its own size, so the adapter is what
// can refuse an over-large message before reading it. It is per-RPC for the same
// reason the adapter is -- it comes from that call's options.
type clientProtocolAdapterFactory func(codecName string, maxRecv int) (clientProtocolAdapter, error)

// clientProtocolAdapter provides the methods needed to implement the client side of
// an HTTP-based protocol.
type clientProtocolAdapter interface {
	protocolAdapter
	// unaryMessage encodes the given message into the frames of a unary request
	// body. The given parameters are the encoded message bytes and a flag
	// indicating whether the bytes are compressed.
	//
	// This is separate from streamMessage since some protocols frame unary bodies
	// differently from streaming ones. It is only used for unary operations.
	//
	// There is no server-side counterpart: a server hands its response message to
	// writeUnary instead, because the adapter owns the response writer.
	unaryMessage(data []byte, compressed bool) ([][]byte, error)
	// requestHeaders creates the request headers for the operation. If the given
	// context has a deadline, it may be encoded in request headers to propagate that
	// deadline. This method need not encode any custom metadata, only headers needed
	// by the protocol to define the call.
	//
	// The compressorName is the encoding of the request messages, and is empty
	// when they are not compressed. The acceptCompressors are the encodings the
	// response may use, in order of preference. Both are empty for a protocol that
	// does not support compression; see channelOptions.protocolCompresses.
	requestHeaders(ctx context.Context, isStream bool, compressorName string, acceptCompressors []string) (http.Header, error)
	// processUnaryResponse reads the given response, body included, and returns all
	// aspects of the RPC result. The error should be non-nil if the call indicates a
	// non-OK RPC status code, or if the response is not one the adapter can use,
	// such as one encoded with a different codec than the request. The header and
	// trailer metadata may be populated either way, but the message only when the
	// error is nil. This is only called for unary operations.
	//
	// Reading the body may block for as long as the server takes to send it, so the
	// driver calls this where it can stop waiting when the RPC's context ends.
	processUnaryResponse(resp *http.Response) (unaryResponse, error)
	// processStreamHeaders processes the given stream response into header metadata,
	// the compressor that compressed response messages use (empty if none), and an
	// optional error indicating whether the RPC already failed. This is only called for
	// stream operations, and will be combined with calls to readStreamResponse.
	//
	// The adapter also takes ownership of the response body here, which is what
	// subsequent calls to readStreamResponse read from. It is captured once, at this
	// point, because deframing may be stateful and so cannot re-wrap the body on
	// every message.
	processStreamHeaders(resp *http.Response) (md metadata.MD, compressorName string, err error)
	// readStreamResponse reads the next response message from the body captured by
	// processStreamHeaders, which must have been called first. The returned data
	// should be the encoded (and optionally compressed) response message, and the
	// returned flag indicates whether it is compressed. If an error occurs or if the
	// end of the stream has been reached, the returned data should be nil and the
	// given metadata and error should be populated as trailer metadata and the cause
	// of failure. If the RPC operation completed successfully, the error returned
	// should be io.EOF.
	readStreamResponse() ([]byte, bool, metadata.MD, error)
}

// unaryResponse is the result of a unary RPC, as processUnaryResponse reads it
// from the response.
type unaryResponse struct {
	headers  metadata.MD
	trailers metadata.MD
	// message is the encoded response message, which is nil if the RPC failed.
	// compressorName is the encoding it is compressed with, and is empty if it is
	// not compressed.
	message        []byte
	compressorName string
}

// serverProtocolAdapter provides the methods needed to implement the server side of
// an HTTP-based protocol.
//
// The adapter owns the response writer, which it is given when it is created. That
// is what lets it decide *when* the response body is written, and how the
// outcome is reported: a protocol that encodes the RPC's result as headers, as
// httpgrpc does for unary calls, cannot write the body until the result is known,
// since the first write flushes the header block. So a unary response is handed
// over whole, once its outcome is known, and a stream's headers are sent by the
// adapter, which commits the HTTP status as it does.
type serverProtocolAdapter interface {
	protocolAdapter
	// processHeaders is called to process the given request headers. It reports
	// what they say about the call, which the driver uses to build the context the
	// RPC is served in.
	processHeaders(header http.Header) (requestInfo, error)
	// processUnaryRequest reads the request message from the request the adapter
	// was created with. It returns the encoded message and a flag indicating
	// whether it is compressed, or an error.
	//
	// The body is not passed in, because it is not always where the message is: the
	// connect protocol can carry a unary request in the query string of a GET, for
	// idempotent calls. An adapter that accepts such a request reads it from there
	// instead.
	processUnaryRequest() ([]byte, bool, error)
	// writeUnary writes the whole response of a unary RPC. It is called exactly
	// once, and is all that is written for a unary RPC, whether it succeeded or
	// failed.
	writeUnary(ctx context.Context, reply unaryReply)
	// sendStreamHeaders writes the response headers of a streaming RPC, which
	// carry the given compression encoding (empty if none) and header metadata,
	// and commits the HTTP status. It is called once, before anything is written to
	// the body. The stream's outcome is reported later, by finishStream.
	sendStreamHeaders(compressorName string, md metadata.MD)
	// readStreamRequest extracts the next message from the request body, which the
	// adapter took ownership of when it was created. It returns the encoded message
	// data and a flag indicating whether it is compressed. It returns io.EOF once
	// the client has closed the stream.
	//
	// The body is not passed in, for the same reason it is not passed to
	// readStreamResponse: deframing may be stateful, so it cannot re-wrap the body
	// on every message.
	readStreamRequest() ([]byte, bool, error)
	// finishStream records the given status, which is nil on successful completion
	// and otherwise never OK. The protocols supported here all encode the final
	// status and trailing metadata into the response body, as a final frame, so this
	// is where that frame is written.
	finishStream(st *status.Status, trailers metadata.MD)
}

// requestInfo is what the headers of a request say about the call.
type requestInfo struct {
	// metadata is the request's header metadata, which the handler receives as
	// incoming metadata.
	metadata metadata.MD
	// timeout is how long the client gave the call to complete, if hasTimeout is
	// set.
	timeout    time.Duration
	hasTimeout bool
	// compressorName is the encoding of the request messages, and is empty if
	// they are not compressed. acceptCompressors are the encodings the client
	// accepts for the response, in order of preference.
	compressorName    string
	acceptCompressors []string
}

// unaryReply is the whole response of a unary RPC, as given to writeUnary.
type unaryReply struct {
	headers metadata.MD
	// message is the encoded response message. It is only set when the RPC
	// succeeded. compressorName is the encoding it is compressed with, and is
	// empty if it is not compressed.
	message        []byte
	compressorName string
	// status is nil when the RPC succeeded, and otherwise is never OK; see
	// errorStatus.
	status   *status.Status
	trailers metadata.MD
}

// protocolMatch is the outcome of matching an incoming request to a protocol.
type protocolMatch struct {
	// adapter is nil when no protocol recognized the request. That is the only
	// case a handler cannot report in some protocol's own error format.
	adapter serverProtocolAdapter
	// subFormat names the message codec the request selected.
	subFormat string
	// allowUnary and allowStream report which kinds of RPC a request of this shape
	// can carry: a content-type that frames a stream cannot serve a unary method,
	// and vice versa.
	allowUnary  bool
	allowStream bool
	// allowsGet reports whether the protocol accepts GET as well as POST, for a
	// unary call to a method free of side effects, as Connect does; the others are
	// POST-only. Whether the method is free of side effects is the handler's to
	// know.
	allowsGet bool
	// httpMethodErrors reports whether the protocol refuses a method it does not
	// accept as HTTP, with a bare 405, rather than as an RPC error, as Connect does.
	httpMethodErrors bool
}

// isOtherProtocol reports whether the given content-type names a protocol other
// than Connect; see determineProtocolAdapter. JSON is Connect's as well as
// httpgrpc's, and belongs to Connect when the request says it is Connect.
func isOtherProtocol(mediaType string, isConnect bool) bool {
	if _, _, ok := parseGRPCWebContentType(mediaType); ok {
		return true
	}
	switch mediaType {
	case UnaryRpcContentType_V1, StreamRpcContentType_V1:
		return true
	case ApplicationJson:
		return !isConnect
	default:
		return false
	}
}

func determineProtocolAdapter(req *http.Request, w http.ResponseWriter, opts handlerOpts) protocolMatch {
	mediaType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	// A Connect client sends the version header, and a Connect GET also names its
	// codec and message in the query string; a client of another protocol sends
	// none of them.
	isConnect := req.Header.Get(connectProtocolVersionHeader) != ""
	if req.Method == http.MethodGet {
		query := req.URL.Query()
		isConnect = isConnect || query.Has(connectGetVersionParam) ||
			query.Has(connectGetEncodingParam) || query.Has(connectGetMessageParam)
		// Only Connect uses GET, so a GET is taken to be Connect unless its
		// content-type names another protocol: then its client is told, in that
		// protocol's error format, that the method is wrong. A Connect GET has no
		// body for a content-type to describe, so any it claims is otherwise ignored;
		// its codec is named in the query string.
		if !isOtherProtocol(mediaType, isConnect) {
			return connectUnaryMatch(req, w, query.Get(connectGetEncodingParam), opts)
		}
	}
	if codecName, text, ok := parseGRPCWebContentType(mediaType); ok {
		// Both the binary and the text form; the codec follows the plus sign, and
		// is proto when there is none.
		return protocolMatch{
			adapter:     newGRPCWebServerAdapter(req, w, codecName, text, opts),
			subFormat:   codecName,
			allowUnary:  true,
			allowStream: true,
		}
	}
	if codecName, ok := strings.CutPrefix(mediaType, connectStreamContentTypePrefix); ok {
		return protocolMatch{
			adapter:          newConnectServerAdapter(req, w, codecName, true, opts),
			subFormat:        codecName,
			allowStream:      true,
			httpMethodErrors: true,
		}
	}
	switch {
	case mediaType == ApplicationJson:
		// Both the Connect and httpgrpc protocols use this content type for unary
		// calls, so it takes another header to tell them apart. Connect clients send
		// it; a request without it is httpgrpc, as such requests always were.
		if isConnect {
			return connectUnaryMatch(req, w, jsonCodecName, opts)
		}
		// The httpgrpc protocol accepts JSON for streams as well as for unary calls:
		// the request body is a sequence of JSON values and the response is encoded
		// as server-sent events.
		return protocolMatch{
			adapter:     newHTTPGRPCServerAdapter(req, w, jsonCodecName, opts),
			subFormat:   jsonCodecName,
			allowUnary:  true,
			allowStream: true,
		}
	case mediaType == UnaryRpcContentType_V1:
		return protocolMatch{
			adapter:    newHTTPGRPCServerAdapter(req, w, grpcproto.Name, opts),
			subFormat:  grpcproto.Name,
			allowUnary: true,
		}
	case mediaType == StreamRpcContentType_V1:
		return protocolMatch{
			adapter:     newHTTPGRPCServerAdapter(req, w, grpcproto.Name, opts),
			subFormat:   grpcproto.Name,
			allowStream: true,
		}
	case mediaType == connectUnaryContentTypePrefix+grpcproto.Name:
		return connectUnaryMatch(req, w, grpcproto.Name, opts)
	case isConnect && strings.HasPrefix(mediaType, connectUnaryContentTypePrefix):
		// Any other codec, but only from a client that says it is speaking Connect,
		// since application/* is otherwise far too broad to claim.
		return connectUnaryMatch(req, w, strings.TrimPrefix(mediaType, connectUnaryContentTypePrefix), opts)
	default:
		return notImplementedProtocol()
	}
}

// responseCodecName returns the name of the codec a response is labelled with,
// given the one the request named. That is the same codec, unless none is
// registered under its name: the RPC then fails, and since nothing in that
// failure is encoded with the codec, the response names the default instead of
// one the server cannot use.
func responseCodecName(codecName string) string {
	if encoding.GetCodecV2(codecName) == nil {
		return grpcproto.Name
	}
	return codecName
}

// connectUnaryMatch claims a request for a unary RPC in the Connect protocol.
func connectUnaryMatch(req *http.Request, w http.ResponseWriter, codecName string, opts handlerOpts) protocolMatch {
	return protocolMatch{
		adapter:          newConnectServerAdapter(req, w, codecName, false, opts),
		subFormat:        codecName,
		allowUnary:       true,
		allowsGet:        true,
		httpMethodErrors: true,
	}
}

// notImplementedProtocol leaves a content-type unclaimed.
//
// A request in a protocol this server does not implement is not malformed, just
// one that cannot be served here. Since no adapter claims it, there is also no
// protocol in whose error format the failure could be reported, so the handler
// answers with a plain 415.
func notImplementedProtocol() protocolMatch {
	return protocolMatch{}
}

// checkResponseContentType checks the content-type of a response whose body holds
// messages. It must be one the protocol uses, which parse decides, reporting the
// codec it names; and that codec must be the one the request was sent with,
// whose content-type is want. A content-type the protocol does not recognize is
// reported as Unknown, since nothing can be made of the body, and one naming
// another codec as Internal.
func checkResponseContentType(resp *http.Response, codecName, want string, parse func(mediaType string) (string, bool)) error {
	contentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return status.Errorf(codes.Unknown, "response has unparseable content-type %q", contentType)
	}
	respCodecName, ok := parse(mediaType)
	if !ok {
		return status.Errorf(codes.Unknown, "response has unsupported content-type %q", contentType)
	}
	if respCodecName != codecName {
		return status.Errorf(codes.Internal, "response has content-type %q; expecting %q", contentType, want)
	}
	return nil
}

// sseTrailerEvent is the SSE event type that carries the final status and
// trailing metadata of a stream, as opposed to a response message.
const sseTrailerEvent = "trailer"

// sizePrefixedFrame frames the given message data with a 32-bit big-endian size
// prefix. A trailer is indicated by writing that size as a negative number.
func sizePrefixedFrame(data []byte, isTrailer bool) ([][]byte, error) {
	sz := len(data)
	if sz > math.MaxInt32 {
		return nil, fmt.Errorf("message too large to send: %d bytes", sz)
	}
	if isTrailer {
		sz = -sz
	}
	var buf [4]byte
	if _, err := binary.Encode(buf[:], binary.BigEndian, int32(sz)); err != nil {
		return nil, err
	}
	return [][]byte{buf[:], data}, nil
}

// readSizePrefixedFrame reads one size-prefixed frame from r, returning the
// message data and whether that data is a trailer. It returns io.EOF when the
// stream ends cleanly between frames.
func readSizePrefixedFrame(r io.Reader, maxRecv int) ([]byte, bool, error) {
	sz, err := readSizePreface(r)
	if err != nil {
		return nil, false, err
	}
	isTrailer := sz < 0
	if isTrailer {
		sz = -sz
	}
	// The preface announces the size, so an over-large message is refused here,
	// before it is read, rather than after it has been pulled into memory. This is
	// also what keeps a bogus preface from being turned into an allocation of
	// whatever size it names.
	if err := checkRecvSize(int(sz), maxRecv); err != nil {
		return nil, false, err
	}
	data := make([]byte, sz)
	if _, err := io.ReadAtLeast(r, data, int(sz)); err != nil {
		if err == io.EOF {
			// A truncated frame is not a clean end of stream.
			err = io.ErrUnexpectedEOF
		}
		return nil, false, err
	}
	return data, isTrailer, nil
}

// envelopeFlagCompressed marks a frame of the gRPC and gRPC-Web wire formats
// whose message is compressed.
const envelopeFlagCompressed = 0x01

// envelopeFrame frames the given data the way gRPC does: a byte of flags, then
// the length of the data as a 32-bit big-endian integer, then the data itself.
func envelopeFrame(flags byte, data []byte) ([][]byte, error) {
	if uint64(len(data)) > math.MaxUint32 {
		return nil, fmt.Errorf("message too large to send: %d bytes", len(data))
	}
	var prefix [5]byte
	prefix[0] = flags
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(data)))
	return [][]byte{prefix[:], data}, nil
}

// envelopeMessage frames a message the way gRPC does, flagging it if it is
// compressed.
func envelopeMessage(data []byte, compressed bool) ([][]byte, error) {
	var flags byte
	if compressed {
		flags |= envelopeFlagCompressed
	}
	return envelopeFrame(flags, data)
}

// readEnvelopeFrame reads one frame written by envelopeFrame, returning its flags
// and data. It returns io.EOF when the stream ends cleanly between frames.
func readEnvelopeFrame(r io.Reader, maxRecv int) (byte, []byte, error) {
	var prefix [5]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		// ReadFull reports io.EOF only when nothing was read, which is the clean end
		// of a stream; a partial prefix is io.ErrUnexpectedEOF.
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(prefix[1:])
	// As with readSizePrefixedFrame: the prefix announces the size, so an
	// over-large message is refused here, before it is read, and a bogus prefix
	// cannot be turned into an allocation of whatever size it names.
	if uint64(size) > uint64(maxRecv) {
		return 0, nil, recvTooLarge("payload too large to receive: %v > %v", size, maxRecv)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		if err == io.EOF {
			// A truncated frame is not a clean end of stream.
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return prefix[0], data, nil
}

// envelopeReader reads a body made of frames written by envelopeFrame, as
// gRPC-Web's and Connect's are. Each frame is a message unless its flags mark it
// as the body's final frame, whose payload means whatever the protocol says.
type envelopeReader struct {
	r io.Reader
	// maxRecv bounds the payload of a single frame.
	maxRecv int
	// endFlag is the flag that marks the final frame.
	endFlag byte
	// malformedCode is the code that reports a body which cannot be decoded at
	// all, as errMalformedText reports one in the text form of gRPC-Web. Which code
	// that is depends on whose fault it is, the client's or the server's.
	malformedCode codes.Code
}

func newEnvelopeReader(r io.Reader, maxRecv int, endFlag byte, malformedCode codes.Code) *envelopeReader {
	return &envelopeReader{r: r, maxRecv: maxRecv, endFlag: endFlag, malformedCode: malformedCode}
}

// next reads the next frame, reporting its payload, whether that is compressed,
// and whether the frame is the final one. It returns io.EOF when the body ends
// cleanly between frames.
func (e *envelopeReader) next() (data []byte, compressed bool, end bool, err error) {
	flags, data, err := readEnvelopeFrame(e.r, e.maxRecv)
	if errors.Is(err, errMalformedText) {
		return nil, false, false, status.Error(e.malformedCode, err.Error())
	}
	if err != nil {
		return nil, false, false, err
	}
	return data, flags&envelopeFlagCompressed != 0, flags&e.endFlag != 0, nil
}

// decompressEndFrame decompresses the payload of a body's final frame, which the
// protocols that use envelopeReader allow to be compressed, with the same
// encoding as the response's messages. what names the frame, for the error that
// reports an encoding with no compressor registered.
func decompressEndFrame(what string, data []byte, compressorName string, maxRecv int) ([]byte, error) {
	compressor := compressorFor(compressorName)
	if compressor == nil {
		return nil, status.Errorf(codes.Internal, "%s is compressed with unknown encoding %q", what, compressorName)
	}
	return decompressLimited(compressor, data, maxRecv)
}

// sseFrame encodes the given data as a server-sent event. An empty eventType
// produces a default "message" event, which is how response messages are sent;
// sseTrailerEvent marks the final frame.
func sseFrame(eventType string, data []byte) ([][]byte, error) {
	var buf bytes.Buffer
	if err := sse.NewEncoder(&buf).Encode(&sse.Event{Type: eventType, Data: data}); err != nil {
		return nil, err
	}
	return [][]byte{buf.Bytes()}, nil
}

// flushResponse pushes any buffered response data to the client, so that a
// streamed message is not held back waiting for later ones.
func flushResponse(w http.ResponseWriter) {
	_ = http.NewResponseController(w).Flush()
}

// writeFrames writes the given frames to w in order, stopping at the first
// failure.
func writeFrames(w io.Writer, frames [][]byte) error {
	for _, frame := range frames {
		if _, err := w.Write(frame); err != nil {
			return err
		}
	}
	return nil
}

// timeoutMillis returns the time remaining until the given deadline in whole
// milliseconds, which is how the protocols here propagate a deadline. It is
// never less than one, since a deadline that has already passed must still be
// sent as one rather than as none at all.
func timeoutMillis(deadline time.Time) int64 {
	millis := int64(time.Until(deadline) / time.Millisecond)
	if millis <= 0 {
		millis = 1
	}
	return millis
}

// readerFromByteSlices returns a reader of the given slices in order, without
// copying them into one.
func readerFromByteSlices(data [][]byte) io.Reader {
	readers := make([]io.Reader, len(data))
	for i := range data {
		readers[i] = bytes.NewReader(data[i])
	}
	return io.MultiReader(readers...)
}
