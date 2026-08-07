package httpgrpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fullstorydev/grpchan/internal/sse"
)

// httpGRPCClientProtocolAdapter is created per-RPC (see
// clientProtocolAdapterFactory) so that it can hold the per-stream state needed
// to deframe a response body.
type httpGRPCClientProtocolAdapter struct {
	// codecName and codec are what the request is encoded with. That codec is also
	// what X-GRPC-Details must be decoded with, since the server encodes those with
	// the request's codec. It cannot be inferred from the response: when an RPC
	// fails, the response's Content-Type is the error renderer's (text/plain, for
	// DefaultErrorRenderer) rather than the message format.
	//
	// The name is kept rather than taken from codec.Name(), for the reason given on
	// channelOptions: a codec is registered under the lower-cased form of its name
	// but stored as-is.
	codecName string
	codec     encoding.CodecV2
	// respBody is captured by processStreamHeaders and read by readStreamResponse.
	// sseDec is created over it when the server replies with server-sent events,
	// and must persist across messages because it buffers.
	respBody io.Reader
	sseDec   *sse.Decoder
}

var _ clientProtocolAdapter = (*httpGRPCClientProtocolAdapter)(nil)

func newHTTPGRPCClientAdapter(codecName string) (clientProtocolAdapter, error) {
	switch codecName {
	case grpcproto.Name, jsonCodecName:
	default:
		return nil, fmt.Errorf("httpgrpc protocol does not support the given codec name: %v", codecName)
	}
	codec, err := codecByName(codecName)
	if err != nil {
		return nil, err
	}
	return &httpGRPCClientProtocolAdapter{codecName: codecName, codec: codec}, nil
}

func (h *httpGRPCClientProtocolAdapter) isJSON() bool {
	return h.codecName == jsonCodecName
}

func (h *httpGRPCClientProtocolAdapter) unaryMessage(data []byte) ([][]byte, error) {
	return [][]byte{data}, nil
}

// streamMessage frames a request message. Proto streams use a size prefix; a JSON
// stream's request body is just a sequence of JSON values, which are
// self-delimiting, so the message data is sent as-is. (Server-sent events appear
// only in the other direction; see the server adapter.)
func (h *httpGRPCClientProtocolAdapter) streamMessage(data []byte) ([][]byte, error) {
	if h.isJSON() {
		return [][]byte{data}, nil
	}
	return sizePrefixedFrame(data, false)
}

func (h *httpGRPCClientProtocolAdapter) requestHeaders(ctx context.Context, isStream bool) (http.Header, error) {
	var contentType, acceptType string
	switch {
	case h.isJSON():
		contentType = ApplicationJson
		if isStream {
			// The request body is a sequence of JSON values, but the response comes
			// back as server-sent events, so the two differ for JSON streams.
			acceptType = EventStreamContentType
		}
	case isStream:
		contentType, acceptType = StreamRpcContentType_V1, StreamRpcContentType_V1
	default:
		contentType = UnaryRpcContentType_V1
	}
	hdrs := headersFromContext(ctx)
	hdrs.Set("Content-Type", contentType)
	if acceptType != "" {
		hdrs.Set("Accept", acceptType)
	}
	return hdrs, nil
}

func (h *httpGRPCClientProtocolAdapter) processUnaryResponse(resp *http.Response) (unaryResponse, error) {
	hdr, err := asMetadata(resp.Header)
	if err != nil {
		return unaryResponse{}, err
	}
	result := unaryResponse{headers: hdr, trailers: metadata.MD{}}

	const trailerPrefix = "x-grpc-trailer-"

	for k, v := range hdr {
		if strings.HasPrefix(strings.ToLower(k), trailerPrefix) {
			trailerName := k[len(trailerPrefix):]
			if trailerName != "" {
				result.trailers[trailerName] = v
				delete(hdr, k)
			}
		}
	}

	// The status travels in headers, so it is recovered before looking at the body
	// at all. Details are decoded with the codec we sent, not one inferred from the
	// response: see codecName.
	stat := statFromResponse(resp, h.codec)
	if err := stat.Err(); err != nil {
		// A failed RPC carries no message, just whatever the error renderer wrote,
		// so there is no response format to report or to validate.
		return result, err
	}

	// The RPC succeeded, so the body is a message and the response has to name the
	// format it was asked for.
	want := UnaryRpcContentType_V1
	if h.isJSON() {
		want = ApplicationJson
	}
	if err := checkResponseContentType(resp, h.codecName, want, httpGRPCUnaryCodec); err != nil {
		return result, err
	}

	result.message, err = io.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}
	return result, nil
}

// httpGRPCUnaryCodec reports the codec named by the media type of a unary
// response, and whether it is one this protocol uses at all.
func httpGRPCUnaryCodec(mediaType string) (string, bool) {
	switch mediaType {
	case UnaryRpcContentType_V1:
		return grpcproto.Name, true
	case ApplicationJson:
		return jsonCodecName, true
	default:
		return "", false
	}
}

func (h *httpGRPCClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, error) {
	md, err := asMetadata(resp.Header)
	if err != nil {
		return nil, err
	}
	// Take ownership of the body; readStreamResponse reads successive messages
	// from it. When the server replies with server-sent events, deframing needs a
	// decoder that buffers across messages, so it is created once here.
	h.respBody = resp.Body
	if mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mediaType == EventStreamContentType {
		h.sseDec = sse.NewDecoder(resp.Body)
	}
	stat := statFromResponse(resp, h.codec)
	return md, stat.Err()
}

func (h *httpGRPCClientProtocolAdapter) readStreamResponse() ([]byte, metadata.MD, error) {
	if h.respBody == nil {
		return nil, nil, fmt.Errorf("readStreamResponse called before processStreamHeaders")
	}
	data, isTrailer, err := h.readStreamFrame()
	if err != nil {
		if err == io.EOF {
			// A body that simply stopped did not end the stream cleanly: this
			// protocol ends one with a trailing frame, and io.EOF is what this method
			// reports when it has read that frame.
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	if !isTrailer {
		return data, nil, nil
	}

	// The final frame carries the RPC's status and any trailing metadata.
	var tr HttpTrailer
	if err := h.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, &tr); err != nil {
		return nil, nil, err
	}
	var trailers metadata.MD
	if len(tr.Metadata) > 0 {
		trailers = metadataFromProto(tr.Metadata)
	}
	if err := status.ErrorProto(&spb.Status{
		Code:    tr.Code,
		Message: tr.Message,
		Details: tr.Details,
	}); err != nil {
		return nil, trailers, err
	}
	// The stream ended successfully, which callers distinguish from a failure by
	// the io.EOF sentinel rather than a nil error.
	return nil, trailers, io.EOF
}

// readStreamFrame reads the next frame of the response body, in whichever framing
// the server used, and reports whether it is the stream's final frame.
func (h *httpGRPCClientProtocolAdapter) readStreamFrame() ([]byte, bool, error) {
	if h.sseDec != nil {
		event, err := h.sseDec.Decode()
		if err != nil {
			return nil, false, err
		}
		return event.Data, event.Type == sseTrailerEvent, nil
	}
	return readSizePrefixedFrame(h.respBody)
}

// httpGRPCServerProtocolAdapter is created per-RPC by newHTTPGRPCServerAdapter, so
// that it can hold the state needed to deframe a request body.
type httpGRPCServerProtocolAdapter struct {
	errFunc func(context.Context, *status.Status, http.ResponseWriter)
	// codecName is the message format negotiated from the request's Content-Type.
	// It selects the framing as well: proto streams are size-prefixed, whereas JSON
	// streams are read as a sequence of JSON values and written as server-sent
	// events.
	codecName string
	// reqBody is the request body, read by processUnaryRequest and
	// readStreamRequest. jsonDec is created over it on first use, and must persist
	// across messages because it buffers.
	reqBody io.ReadCloser
	jsonDec *json.Decoder

	// w is the response writer this adapter owns.
	w http.ResponseWriter
}

var _ serverProtocolAdapter = &httpGRPCServerProtocolAdapter{}

func newHTTPGRPCServerAdapter(req *http.Request, w http.ResponseWriter, codecName string, opts handlerOpts) *httpGRPCServerProtocolAdapter {
	return &httpGRPCServerProtocolAdapter{
		errFunc:   opts.errFunc,
		codecName: codecName,
		reqBody:   req.Body,
		w:         w,
	}
}

func (h *httpGRPCServerProtocolAdapter) isJSON() bool {
	return h.codecName == jsonCodecName
}

func (h *httpGRPCServerProtocolAdapter) codec() encoding.CodecV2 {
	return encoding.GetCodecV2(h.codecName)
}

// streamMessage frames a response message. Proto streams use a size prefix; JSON
// streams are sent to the client as server-sent events.
func (h *httpGRPCServerProtocolAdapter) streamMessage(data []byte) ([][]byte, error) {
	if h.isJSON() {
		return sseFrame("", data)
	}
	return sizePrefixedFrame(data, false)
}

func (h *httpGRPCServerProtocolAdapter) processHeaders(header http.Header) (requestInfo, error) {
	md, err := asMetadata(header)
	if err != nil {
		return requestInfo{}, err
	}
	info := requestInfo{metadata: md}
	info.timeout, info.hasTimeout = timeoutFromHeaders(header)
	return info, nil
}

func (h *httpGRPCServerProtocolAdapter) processUnaryRequest() ([]byte, error) {
	// This protocol adds no framing to a unary body: the whole body is the message.
	return io.ReadAll(h.reqBody)
}

// writeUnary reports the RPC's status and trailing metadata as headers, so they
// are all set before anything is written: the first write flushes the header
// block.
func (h *httpGRPCServerProtocolAdapter) writeUnary(ctx context.Context, reply unaryReply) {
	hdr := h.w.Header()
	// Label the body so the client can tell how it is encoded; a client is entitled
	// to reject a response that names no format it understands. For a failed call
	// this gets overwritten by the error renderer (http.Error sets text/plain),
	// which is why clients recover the status from headers.
	if h.isJSON() {
		hdr.Set("Content-Type", ApplicationJson)
	} else {
		hdr.Set("Content-Type", UnaryRpcContentType_V1)
	}
	toHeaders(reply.headers, hdr, "")
	toHeaders(reply.trailers, hdr, "X-GRPC-Trailer-")
	if reply.status != nil {
		errHandler := h.errFunc
		if errHandler == nil {
			errHandler = DefaultErrorRenderer
		}
		codec := h.codec()
		statProto := reply.status.Proto()
		hdr.Set("X-GRPC-Status", fmt.Sprintf("%d:%s", statProto.Code, statProto.Message))
		for _, d := range statProto.Details {
			buf, err := codec.Marshal(d)
			if err != nil {
				continue
			}
			str := base64.RawURLEncoding.EncodeToString(buf.Materialize())
			hdr.Add(grpcDetailsHeader, str)
		}
		// A failed unary RPC sends no message, only whatever the renderer writes.
		errHandler(ctx, reply.status, h.w)
		return
	}

	hdr.Set("Content-Length", strconv.Itoa(len(reply.message)))
	_, _ = h.w.Write(reply.message)
}

func (h *httpGRPCServerProtocolAdapter) sendStreamHeaders(md metadata.MD) {
	hdr := h.w.Header()
	// Label the body so the client can tell how it is framed and encoded. A JSON
	// stream is the one case where the response Content-Type differs from the
	// request's: the client sends a sequence of JSON values, and the server replies
	// with server-sent events.
	if h.isJSON() {
		hdr.Set("Content-Type", EventStreamContentType)
	} else {
		hdr.Set("Content-Type", StreamRpcContentType_V1)
	}
	toHeaders(md, hdr, "")
	h.w.WriteHeader(http.StatusOK)
}

func (h *httpGRPCServerProtocolAdapter) readStreamRequest() ([]byte, error) {
	if h.isJSON() {
		// A JSON request body is a sequence of JSON values. The decoder buffers, so
		// it is created once and reused for the life of the stream.
		if h.jsonDec == nil {
			h.jsonDec = json.NewDecoder(h.reqBody)
		}
		var msg json.RawMessage
		if err := h.jsonDec.Decode(&msg); err != nil {
			return nil, err
		}
		return msg, nil
	}

	data, isTrailer, err := readSizePrefixedFrame(h.reqBody)
	if err != nil {
		return nil, err
	}
	if isTrailer {
		// Clients do not send trailers in this protocol; treat it as end-of-stream.
		return nil, io.EOF
	}
	return data, nil
}

// finishStream writes the final frame of the response body, which is where this
// protocol encodes the RPC's status and any trailing metadata.
func (h *httpGRPCServerProtocolAdapter) finishStream(st *status.Status, trailers metadata.MD) {
	tr := HttpTrailer{
		Code:     int32(codes.OK),
		Message:  codes.OK.String(),
		Metadata: asTrailerProto(trailers),
	}
	if st != nil {
		statProto := st.Proto()
		tr.Code = statProto.Code
		tr.Message = statProto.Message
		tr.Details = statProto.Details
	}

	buf, marshalErr := h.codec().Marshal(&tr)
	if marshalErr != nil {
		return
	}
	data := buf.Materialize()

	var frames [][]byte
	if h.isJSON() {
		frames, marshalErr = sseFrame(sseTrailerEvent, data)
	} else {
		// The trailer is indicated by a negative size prefix.
		frames, marshalErr = sizePrefixedFrame(data, true)
	}
	if marshalErr != nil {
		return
	}
	if writeFrames(h.w, frames) != nil {
		return
	}
	flushResponse(h.w)
}
