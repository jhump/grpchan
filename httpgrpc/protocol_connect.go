package httpgrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The Connect protocol carries a unary RPC as a plain HTTP request and response:
// the body is the message, compressed with the standard Content-Encoding header,
// and a failure is reported with an HTTP status and a JSON body. Trailing
// metadata travels as headers with a "Trailer-" prefix.
//
// Streams are framed like gRPC's, with a flag for the final frame, which holds a
// JSON object with the RPC's outcome and trailing metadata. Their compression is
// negotiated with headers of their own, and their HTTP status is always 200.
//
// The protocol also allows a unary request to be made with GET, for idempotent
// methods. That is not implemented.

const (
	// connectUnaryContentTypePrefix is as broad as it looks: a unary request is
	// application/ followed by the codec's name. Which such requests are taken to
	// be Connect, rather than another protocol's, is decided in
	// determineProtocolAdapter.
	connectUnaryContentTypePrefix  = "application/"
	connectStreamContentTypePrefix = "application/connect+"

	connectProtocolVersionHeader           = "Connect-Protocol-Version"
	connectTimeoutHeader                   = "Connect-Timeout-Ms"
	connectUnaryEncodingHeader             = "Content-Encoding"
	connectUnaryAcceptEncodingHeader       = "Accept-Encoding"
	connectStreamEncodingHeader            = "Connect-Content-Encoding"
	connectStreamAcceptEncodingHeader      = "Connect-Accept-Encoding"
	connectUnaryTrailerPrefix              = "Trailer-"
	connectFlagEndStream              byte = 0x02
)

// connectEndStream is the JSON payload of a stream's final frame.
type connectEndStream struct {
	Error    *connectWireError   `json:"error,omitempty"`
	Metadata map[string][]string `json:"metadata,omitempty"`
}

// connectContentType returns the content-type of a unary or streaming request
// or response whose messages use the given codec.
func connectContentType(stream bool, codecName string) string {
	if stream {
		return connectStreamContentTypePrefix + codecName
	}
	return connectUnaryContentTypePrefix + codecName
}

// connectUnaryCodec reports the codec named by the media type of a unary
// response, and whether it is one this protocol uses for one at all.
func connectUnaryCodec(mediaType string) (string, bool) {
	codecName, ok := strings.CutPrefix(mediaType, connectUnaryContentTypePrefix)
	if !ok || strings.HasPrefix(codecName, "connect+") {
		return "", false
	}
	return codecName, true
}

// connectStreamCodec reports the codec named by the media type of a streaming
// response, and whether it is one this protocol uses for one at all.
func connectStreamCodec(mediaType string) (string, bool) {
	return strings.CutPrefix(mediaType, connectStreamContentTypePrefix)
}

// connectCompressionHeaders returns the names of the headers that carry the
// encoding of a call's messages and the encodings accepted in reply. A unary
// call uses HTTP's own, since its body is the message, compressed as a whole;
// a stream compresses message by message, and has headers of its own.
func connectCompressionHeaders(stream bool) (encodingHeader, acceptHeader string) {
	if stream {
		return connectStreamEncodingHeader, connectStreamAcceptEncodingHeader
	}
	return connectUnaryEncodingHeader, connectUnaryAcceptEncodingHeader
}

// isReservedConnectHeader reports whether the given lower-case header name
// belongs to HTTP or to a protocol, and so is not metadata.
func isReservedConnectHeader(lowerK string) bool {
	if _, reserved := reservedHeaders[lowerK]; reserved {
		return true
	}
	return lowerK == "content-encoding" ||
		strings.HasPrefix(lowerK, "connect-") || strings.HasPrefix(lowerK, "grpc-")
}

// connectMetadataToHeaders writes metadata as headers, each name with the given
// prefix, encoding binary values as gRPC does.
func connectMetadataToHeaders(md metadata.MD, h http.Header, prefix string) {
	metadataToHeaders(md, h, prefix, isReservedConnectHeader)
}

// connectHeadersToMetadata reads metadata from headers. If trailers is not nil,
// headers with the unary trailer prefix are read into it instead, without the
// prefix.
func connectHeadersToMetadata(h http.Header, trailers metadata.MD) (metadata.MD, error) {
	return headersToMetadata(h, isReservedConnectHeader, connectUnaryTrailerPrefix, trailers)
}

// connectTimeout returns the timeout named by the Connect-Timeout-Ms header, if
// there is a valid one.
func connectTimeout(h http.Header) (time.Duration, bool) {
	v := h.Get(connectTimeoutHeader)
	if v == "" || len(v) > 10 {
		// The protocol allows at most ten digits.
		return 0, false
	}
	millis, err := strconv.ParseInt(v, 10, 64)
	if err != nil || millis < 0 {
		return 0, false
	}
	return time.Duration(millis) * time.Millisecond, true
}

// parseAcceptEncoding returns the encodings an Accept-Encoding header lists, in
// order, leaving out any that a quality of zero rules out. Other qualities are
// not weighed; the order is taken as the preference.
func parseAcceptEncoding(values []string) []string {
	var names []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
			name = strings.TrimSpace(name)
			if name == "" || name == "*" {
				continue
			}
			if q, found := strings.CutPrefix(strings.TrimSpace(params), "q="); found {
				if quality, err := strconv.ParseFloat(q, 64); err == nil && quality == 0 {
					continue
				}
			}
			names = append(names, name)
		}
	}
	return names
}

type connectServerProtocolAdapter struct {
	// codecName is the message format the response is labelled with; see
	// responseCodecName.
	codecName string
	// stream says whether the request is for a streaming RPC, which the protocol
	// frames and labels differently from a unary one.
	stream  bool
	maxRecv int
	// reqBody is the request body, which is the message of a unary request, and
	// reqFrames reads the messages of a streaming one from it.
	reqBody   io.Reader
	reqFrames *envelopeReader
	// contentLength is the request's declared length, or -1 if it declared none.
	// reqCompressed says whether a unary request's body is compressed, which its
	// headers say, since the body is compressed as a whole.
	contentLength int64
	reqCompressed bool

	// w is the response writer this adapter owns.
	w http.ResponseWriter
}

var _ serverProtocolAdapter = (*connectServerProtocolAdapter)(nil)

func newConnectServerAdapter(req *http.Request, w http.ResponseWriter, codecName string, stream bool, opts handlerOpts) *connectServerProtocolAdapter {
	return &connectServerProtocolAdapter{
		codecName:     responseCodecName(codecName),
		stream:        stream,
		maxRecv:       opts.recvLimit(),
		reqBody:       req.Body,
		reqFrames:     newEnvelopeReader(req.Body, opts.recvLimit(), connectFlagEndStream, codes.InvalidArgument),
		contentLength: req.ContentLength,
		reqCompressed: !stream && !isIdentity(req.Header.Get(connectUnaryEncodingHeader)),
		w:             w,
	}
}

func (c *connectServerProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	return envelopeMessage(data, compressed)
}

func (c *connectServerProtocolAdapter) processHeaders(header http.Header) (requestInfo, error) {
	// The version header is optional, but one that names another version is
	// refused, since its requests may not mean what this version's do.
	if version := header.Get(connectProtocolVersionHeader); version != "" && version != "1" {
		return requestInfo{}, fmt.Errorf("unsupported connect protocol version %q", version)
	}
	md, err := connectHeadersToMetadata(header, nil)
	if err != nil {
		return requestInfo{}, err
	}
	encodingHeader, acceptHeader := connectCompressionHeaders(c.stream)
	info := requestInfo{
		metadata:          md,
		compressorName:    header.Get(encodingHeader),
		acceptCompressors: parseAcceptEncoding(header.Values(acceptHeader)),
	}
	info.timeout, info.hasTimeout = connectTimeout(header)
	return info, nil
}

// setResponseHeaders sets the headers of a response other than its
// content-type, which for a unary call depends on whether it failed.
func (c *connectServerProtocolAdapter) setResponseHeaders(stream bool, compressorName string, md metadata.MD) {
	h := c.w.Header()
	encodingHeader, acceptHeader := connectCompressionHeaders(stream)
	if !isIdentity(compressorName) {
		h.Set(encodingHeader, compressorName)
	}
	// Telling the client what this server can decompress lets one that sent an
	// encoding this server lacks choose another next time.
	if accept := advertisedCompressors(); len(accept) > 0 {
		h.Set(acceptHeader, strings.Join(accept, ","))
	}
	connectMetadataToHeaders(md, h, "")
}

func (c *connectServerProtocolAdapter) processUnaryRequest() ([]byte, bool, error) {
	// The body is the whole message, as in httpgrpc, so when the request declares
	// its length, an over-large one can be refused without reading any of it. It
	// is compressed as a whole, so whether it is compressed is said by the
	// request's headers; processHeaders has already reported which encoding.
	data, err := readBodyLimited(c.reqBody, c.contentLength, c.maxRecv)
	return data, c.reqCompressed, err
}

// writeUnary reports the RPC's outcome with the HTTP status, and a failure as a
// JSON body in place of the message.
func (c *connectServerProtocolAdapter) writeUnary(_ context.Context, reply unaryReply) {
	h := c.w.Header()
	c.setResponseHeaders(false, reply.compressorName, reply.headers)
	connectMetadataToHeaders(reply.trailers, h, connectUnaryTrailerPrefix)
	if reply.status == nil {
		h.Set("Content-Type", connectContentType(false, c.codecName))
		c.w.WriteHeader(http.StatusOK)
		_, _ = c.w.Write(reply.message)
		return
	}

	body, marshalErr := json.Marshal(newConnectWireError(reply.status))
	if marshalErr != nil {
		body = []byte(`{"code":"internal"}`)
	}
	// The error body is JSON whatever the codec, and since a failed call has no
	// message, nothing in it is compressed.
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	c.w.WriteHeader(connectHTTPStatus(reply.status.Code()))
	_, _ = c.w.Write(body)
}

func (c *connectServerProtocolAdapter) sendStreamHeaders(compressorName string, md metadata.MD) {
	c.w.Header().Set("Content-Type", connectContentType(true, c.codecName))
	c.setResponseHeaders(true, compressorName, md)
	c.w.WriteHeader(http.StatusOK)
}

func (c *connectServerProtocolAdapter) readStreamRequest() ([]byte, bool, error) {
	data, compressed, end, err := c.reqFrames.next()
	if err != nil {
		return nil, false, err
	}
	if end {
		// Clients do not end a stream with a frame in this protocol, only by ending
		// the body; treat one as the end of the stream.
		return nil, false, io.EOF
	}
	return data, compressed, nil
}

func (c *connectServerProtocolAdapter) finishStream(st *status.Status, trailers metadata.MD) {
	end := connectEndStream{}
	if st != nil {
		end.Error = newConnectWireError(st)
	}
	if len(trailers) > 0 {
		h := http.Header{}
		connectMetadataToHeaders(trailers, h, "")
		end.Metadata = h
	}
	payload, marshalErr := json.Marshal(end)
	if marshalErr != nil {
		payload = []byte(`{"error":{"code":"internal"}}`)
	}
	frames, frameErr := envelopeFrame(connectFlagEndStream, payload)
	if frameErr != nil {
		return
	}
	if writeFrames(c.w, frames) != nil {
		return
	}
	flushResponse(c.w)
}

type connectClientProtocolAdapter struct {
	// codecName is what the request is encoded with, and so what the response must
	// be encoded with too.
	codecName string
	// maxRecv bounds a single response message; see the field of the same name on
	// httpGRPCClientProtocolAdapter.
	maxRecv int
	// respBody is created over the response body by processStreamHeaders, and read
	// by readStreamResponse. respCompressName is the encoding the response named,
	// which the final frame may use as well as a message frame.
	respBody         *envelopeReader
	respCompressName string
}

var _ clientProtocolAdapter = (*connectClientProtocolAdapter)(nil)

func newConnectClientAdapter(codecName string, maxRecv int) (clientProtocolAdapter, error) {
	return &connectClientProtocolAdapter{codecName: codecName, maxRecv: maxRecv}, nil
}

// unaryMessage sends the message as the whole body: the protocol adds no framing
// to a unary request.
func (c *connectClientProtocolAdapter) unaryMessage(data []byte, _ bool) ([][]byte, error) {
	return [][]byte{data}, nil
}

func (c *connectClientProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	return envelopeMessage(data, compressed)
}

func (c *connectClientProtocolAdapter) requestHeaders(ctx context.Context, isStream bool, compressorName string, acceptCompressors []string) (http.Header, error) {
	h := http.Header{}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		connectMetadataToHeaders(md, h, "")
	}
	h.Set(connectProtocolVersionHeader, "1")
	if deadline, ok := ctx.Deadline(); ok {
		h.Set(connectTimeoutHeader, strconv.FormatInt(timeoutMillis(deadline), 10))
	}
	h.Set("Content-Type", connectContentType(isStream, c.codecName))
	encodingHeader, acceptHeader := connectCompressionHeaders(isStream)
	if !isIdentity(compressorName) {
		h.Set(encodingHeader, compressorName)
	}
	if len(acceptCompressors) > 0 {
		h.Set(acceptHeader, strings.Join(acceptCompressors, ","))
	} else if !isStream {
		// Without this, net/http would ask for gzip itself, and decompress the
		// response where this package cannot apply its limits.
		h.Set(connectUnaryAcceptEncodingHeader, "identity")
	}
	return h, nil
}

func (c *connectClientProtocolAdapter) processUnaryResponse(resp *http.Response) (unaryResponse, error) {
	trailers := metadata.MD{}
	headers, err := connectHeadersToMetadata(resp.Header, trailers)
	if err != nil {
		return unaryResponse{}, err
	}
	result := unaryResponse{headers: headers, trailers: trailers}
	if resp.StatusCode != http.StatusOK {
		return result, c.errorFromResponse(resp)
	}
	if err := checkResponseContentType(resp, c.codecName, connectContentType(false, c.codecName), connectUnaryCodec); err != nil {
		return result, err
	}
	// The body is the whole message, as in httpgrpc, so when the response
	// declares its length, an over-large one can be refused without reading it.
	result.message, err = readBodyLimited(resp.Body, resp.ContentLength, c.maxRecv)
	if err != nil {
		return result, err
	}
	result.compressorName = resp.Header.Get(connectUnaryEncodingHeader)
	return result, nil
}

// errorFromResponse reads the error a failed response reports. Its body is a
// Connect error in JSON, unless the response came from something other than a
// Connect server, such as a proxy, in which case the HTTP status is all there
// is to go on.
func (c *connectClientProtocolAdapter) errorFromResponse(resp *http.Response) error {
	fallback := status.Errorf(connectCodeFromHTTPStatus(resp.StatusCode), "server replied with HTTP %s", resp.Status)
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType != ApplicationJson {
		return fallback
	}
	data, err := readBodyLimited(resp.Body, resp.ContentLength, c.maxRecv)
	if err != nil {
		if isLimitError(err) {
			return err
		}
		return fallback
	}
	compressor, err := lookupCompressor(resp.Header.Get(connectUnaryEncodingHeader))
	if err != nil {
		return fallback
	}
	if compressor != nil {
		if data, err = decompressLimited(compressor, data, c.maxRecv); err != nil {
			return err
		}
	}
	var wire connectWireError
	if err := json.Unmarshal(data, &wire); err != nil || wire.Code == "" {
		return fallback
	}
	return wire.status().Err()
}

func (c *connectClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, string, error) {
	md, err := connectHeadersToMetadata(resp.Header, nil)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		// A stream's status is always 200, so this one failed before it started,
		// for a reason reported as for a unary call.
		return md, "", c.errorFromResponse(resp)
	}
	if err := checkResponseContentType(resp, c.codecName, connectContentType(true, c.codecName), connectStreamCodec); err != nil {
		return md, "", err
	}
	c.respBody = newEnvelopeReader(resp.Body, c.maxRecv, connectFlagEndStream, codes.Internal)
	c.respCompressName = resp.Header.Get(connectStreamEncodingHeader)
	return md, c.respCompressName, nil
}

func (c *connectClientProtocolAdapter) readStreamResponse() ([]byte, bool, metadata.MD, error) {
	if c.respBody == nil {
		return nil, false, nil, fmt.Errorf("readStreamResponse called before processStreamHeaders")
	}
	data, compressed, end, err := c.respBody.next()
	if err != nil {
		if err == io.EOF {
			// The body ended without the final frame, so the outcome is unknown.
			err = status.Error(codes.Internal, "response ended without an end-of-stream message")
		}
		return nil, false, nil, err
	}
	if !end {
		return data, compressed, nil, nil
	}

	if compressed {
		if data, err = decompressEndFrame("end-of-stream message", data, c.respCompressName, c.maxRecv); err != nil {
			return nil, false, nil, err
		}
	}
	var endStream connectEndStream
	if err := json.Unmarshal(data, &endStream); err != nil {
		return nil, false, nil, status.Errorf(codes.Internal, "malformed end-of-stream message: %v", err)
	}
	trailers, err := connectHeadersToMetadata(http.Header(endStream.Metadata), nil)
	if err != nil {
		return nil, false, nil, status.Errorf(codes.Internal, "malformed end-of-stream metadata: %v", err)
	}
	if endStream.Error != nil {
		return nil, false, trailers, endStream.Error.status().Err()
	}
	// The stream ended successfully, which callers distinguish from a failure by
	// the io.EOF sentinel rather than a nil error.
	return nil, false, trailers, io.EOF
}
