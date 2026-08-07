package httpgrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
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
//
// TODO: Implement the gRPC-Web and ConnectRPC adapters. Only the httpgrpc
// protocol is implemented so far, which is why parts of this interface have no
// implementation that exercises them yet: it accepts only POST. Those parts are
// noted where they appear. Both of those protocols also support compression,
// which httpgrpc does not, so this interface will grow to carry it when the
// first of them is added.

// protocolAdapter contains methods used by both client and server protocols.
type protocolAdapter interface {
	// streamMessage encodes the given message into a "frame" for the protocol. The
	// given parameter is the encoded message bytes. The response is the framed
	// data, as a sequence of slices to be written in order, or an error.
	//
	// This method is called if the RPC is not a unary operation. So, even if a
	// single direction is unary (for example, in a client streaming operation, the
	// server response is unary) this method is used for both requests and responses.
	streamMessage(data []byte) ([][]byte, error)
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
type clientProtocolAdapterFactory func(codecName string) (clientProtocolAdapter, error)

// clientProtocolAdapter provides the methods needed to implement the client side of
// an HTTP-based protocol.
type clientProtocolAdapter interface {
	protocolAdapter
	// unaryMessage encodes the given message into the frames of a unary request
	// body. The given parameter is the encoded message bytes.
	//
	// This is separate from streamMessage since some protocols frame unary bodies
	// differently from streaming ones. It is only used for unary operations.
	//
	// There is no server-side counterpart: a server hands its response message to
	// writeUnary instead, because the adapter owns the response writer.
	unaryMessage(data []byte) ([][]byte, error)
	// requestHeaders creates the request headers for the operation. If the given
	// context has a deadline, it may be encoded in request headers to propagate that
	// deadline. This method need not encode any custom metadata, only headers needed
	// by the protocol to define the call.
	requestHeaders(ctx context.Context, isStream bool) (http.Header, error)
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
	// processStreamHeaders processes the given stream response into header metadata and
	// an optional error indicating whether the RPC already failed. This is only called for
	// stream operations, and will be combined with calls to readStreamResponse.
	//
	// The adapter also takes ownership of the response body here, which is what
	// subsequent calls to readStreamResponse read from. It is captured once, at this
	// point, because deframing may be stateful and so cannot re-wrap the body on
	// every message.
	processStreamHeaders(resp *http.Response) (metadata.MD, error)
	// readStreamResponse reads the next response message from the body captured by
	// processStreamHeaders, which must have been called first. The returned data
	// should be the encoded response message. If an error occurs or if the end of
	// the stream has been reached, the returned data should be nil and the given
	// metadata and error should be populated as trailer metadata and the cause of
	// failure. If the RPC operation completed successfully, the error returned
	// should be io.EOF.
	readStreamResponse() ([]byte, metadata.MD, error)
}

// unaryResponse is the result of a unary RPC, as processUnaryResponse reads it
// from the response.
type unaryResponse struct {
	headers  metadata.MD
	trailers metadata.MD
	// message is the encoded response message, which is nil if the RPC failed.
	message []byte
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
	// was created with. It returns the encoded message, or an error.
	//
	// The body is not passed in, because it is not always where the message is: the
	// connect protocol can carry a unary request in the query string of a GET, for
	// idempotent calls. An adapter that accepts such a request reads it from there
	// instead.
	processUnaryRequest() ([]byte, error)
	// writeUnary writes the whole response of a unary RPC. It is called exactly
	// once, and is all that is written for a unary RPC, whether it succeeded or
	// failed.
	writeUnary(ctx context.Context, reply unaryReply)
	// sendStreamHeaders writes the response headers of a streaming RPC, which
	// carry the given header metadata, and commits the HTTP status. It is called
	// once, before anything is written to the body. The stream's outcome is
	// reported later, by finishStream.
	sendStreamHeaders(md metadata.MD)
	// readStreamRequest extracts the next message from the request body, which the
	// adapter took ownership of when it was created. It returns the encoded message
	// data. It returns io.EOF once the client has closed the stream.
	//
	// The body is not passed in, for the same reason it is not passed to
	// readStreamResponse: deframing may be stateful, so it cannot re-wrap the body
	// on every message.
	readStreamRequest() ([]byte, error)
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
}

// unaryReply is the whole response of a unary RPC, as given to writeUnary.
type unaryReply struct {
	headers metadata.MD
	// message is the encoded response message. It is only set when the RPC
	// succeeded.
	message []byte
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
	// methods lists the HTTP methods the protocol accepts for this request. Which
	// methods are acceptable is the protocol's business, not the handler's: connect
	// accepts GET for idempotent unary calls, where the others are POST-only. It is
	// also what populates the Allow header when a request is rejected.
	methods []string
}

func (m protocolMatch) allowsMethod(method string) bool {
	return slices.Contains(m.methods, method)
}

// postOnly is the method set of a protocol that only ever accepts POST.
var postOnly = []string{http.MethodPost}

func determineProtocolAdapter(req *http.Request, w http.ResponseWriter, opts handlerOpts) protocolMatch {
	mediaType, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	switch {
	case mediaType == ApplicationJson:
		// Both connectrpc and httpgrpc protocols can use this content type.
		// So we must look at another header to distinguish.
		if connectVersion := req.Header.Get("Connect-Protocol-Version"); connectVersion != "" {
			return notImplementedProtocol()
		}
		// The httpgrpc protocol accepts JSON for streams as well as for unary calls:
		// the request body is a sequence of JSON values and the response is encoded
		// as server-sent events.
		return protocolMatch{
			adapter:     newHTTPGRPCServerAdapter(req, w, jsonCodecName, opts),
			subFormat:   jsonCodecName,
			allowUnary:  true,
			allowStream: true,
			methods:     postOnly,
		}
	case mediaType == UnaryRpcContentType_V1:
		return protocolMatch{
			adapter:    newHTTPGRPCServerAdapter(req, w, grpcproto.Name, opts),
			subFormat:  grpcproto.Name,
			allowUnary: true,
			methods:    postOnly,
		}
	case mediaType == StreamRpcContentType_V1:
		return protocolMatch{
			adapter:     newHTTPGRPCServerAdapter(req, w, grpcproto.Name, opts),
			subFormat:   grpcproto.Name,
			allowStream: true,
			methods:     postOnly,
		}
	default:
		return notImplementedProtocol()
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
func readSizePrefixedFrame(r io.Reader) ([]byte, bool, error) {
	sz, err := readSizePreface(r)
	if err != nil {
		return nil, false, err
	}
	isTrailer := sz < 0
	if isTrailer {
		sz = -sz
	}
	if sz > maxMessageSize {
		return nil, false, fmt.Errorf("bad size preface: indicated size is too large: %d", sz)
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

// readerFromByteSlices returns a reader of the given slices in order, without
// copying them into one.
func readerFromByteSlices(data [][]byte) io.Reader {
	readers := make([]io.Reader, len(data))
	for i := range data {
		readers[i] = bytes.NewReader(data[i])
	}
	return io.MultiReader(readers...)
}
