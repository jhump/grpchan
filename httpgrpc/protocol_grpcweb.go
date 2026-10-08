package httpgrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The gRPC-Web protocol is the gRPC wire protocol with one change: HTTP/1.1
// cannot carry trailers reliably through proxies, so the final status and any
// trailing metadata are sent as the last frame of the response body instead of
// as HTTP trailers. Message framing is gRPC's own; see envelopeFrame.
//
// A response therefore has status 200, with the RPC's real outcome at the end of
// the body, and a unary response is framed exactly like a streaming one: a single
// message frame, then that final frame. The one exception is a "trailers-only"
// response, which has no body and carries the status in its headers instead; a
// server may send one for an RPC that fails before producing a message.
//
// Only the binary form is implemented. The text form, application/grpc-web-text,
// base64-encodes the same frames, and is not recognized.

const (
	grpcWebContentType = "application/grpc-web"

	// grpcWebFlagTrailers marks the final frame of a response body, whose payload
	// is trailing metadata in HTTP header form rather than a message.
	grpcWebFlagTrailers = 0x80
)

// parseGRPCWebContentType reports whether the given media type is one of
// gRPC-Web's, and if so which codec it names. A media type without a codec means
// proto.
func parseGRPCWebContentType(mediaType string) (codecName string, ok bool) {
	base, codecName, found := strings.Cut(mediaType, "+")
	if base != grpcWebContentType {
		return "", false
	}
	if !found {
		codecName = grpcproto.Name
	}
	return codecName, true
}

// grpcWebContentTypeFor returns the content-type naming the given message codec.
func grpcWebContentTypeFor(codecName string) string {
	return grpcWebContentType + "+" + codecName
}

// grpcWebTrailers renders the final frame's payload: the RPC's status, which is
// nil for success, and any trailing metadata, in the same form as a block of HTTP
// headers.
func grpcWebTrailers(st *status.Status, trailers metadata.MD) []byte {
	h := http.Header{}
	grpcMetadataToHeaders(trailers, h)

	if st == nil {
		st = status.New(codes.OK, "")
	}
	h.Set(grpcStatusHeader, strconv.Itoa(int(st.Code())))
	if msg := st.Message(); msg != "" {
		h.Set(grpcMessageHeader, encodeGRPCMessage(msg))
	}
	if len(st.Proto().GetDetails()) > 0 {
		// Details travel as the whole status proto, since one header cannot hold a
		// repeated field on its own.
		if raw, marshalErr := proto.Marshal(st.Proto()); marshalErr == nil {
			h.Set(grpcStatusDetailsHeader, base64.RawStdEncoding.EncodeToString(raw))
		}
	}

	var buf bytes.Buffer
	for k, vs := range h {
		for _, v := range vs {
			fmt.Fprintf(&buf, "%s: %s\r\n", strings.ToLower(k), v)
		}
	}
	return buf.Bytes()
}

// parseGRPCWebTrailers is the inverse of grpcWebTrailers, splitting the final
// frame into trailing metadata and the RPC's status. A successful RPC yields a
// nil error.
func parseGRPCWebTrailers(data []byte) (metadata.MD, error) {
	// The payload is a block of HTTP headers, so read it as one. ReadMIMEHeader
	// wants the terminating blank line, which the frame does not include.
	r := textproto.NewReader(bufio.NewReader(
		io.MultiReader(bytes.NewReader(data), strings.NewReader("\r\n"))))
	mimeHeader, err := r.ReadMIMEHeader()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "malformed trailers frame: %v", err)
	}
	return grpcStatusFromHeaders(http.Header(mimeHeader))
}

type grpcWebServerProtocolAdapter struct {
	// codecName is the message format the response is labelled with; see
	// responseCodecName.
	codecName string
	reqBody   *envelopeReader
	w         http.ResponseWriter
}

var _ serverProtocolAdapter = (*grpcWebServerProtocolAdapter)(nil)

func newGRPCWebServerAdapter(req *http.Request, w http.ResponseWriter, codecName string, opts handlerOpts) *grpcWebServerProtocolAdapter {
	return &grpcWebServerProtocolAdapter{
		codecName: responseCodecName(codecName),
		reqBody:   newEnvelopeReader(req.Body, opts.recvLimit(), grpcWebFlagTrailers),
		w:         w,
	}
}

func (g *grpcWebServerProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	return envelopeMessage(data, compressed)
}

func (g *grpcWebServerProtocolAdapter) processHeaders(header http.Header) (requestInfo, error) {
	md, err := grpcHeadersToMetadata(header)
	if err != nil {
		return requestInfo{}, err
	}
	info := requestInfo{
		metadata:       md,
		compressorName: header.Get(grpcEncodingHeader),
	}
	info.timeout, info.hasTimeout = timeoutFromHeaders(header)
	for _, name := range strings.Split(header.Get(grpcAcceptEncodingHeader), ",") {
		if name = strings.TrimSpace(name); name != "" {
			info.acceptCompressors = append(info.acceptCompressors, name)
		}
	}
	return info, nil
}

// setResponseHeaders sets the headers of a response, unary or streaming alike.
func (g *grpcWebServerProtocolAdapter) setResponseHeaders(compressorName string, md metadata.MD) {
	h := g.w.Header()
	// Unary and streaming responses are framed alike, so they are labelled alike.
	h.Set("Content-Type", grpcWebContentTypeFor(g.codecName))
	if !isIdentity(compressorName) {
		h.Set(grpcEncodingHeader, compressorName)
	}
	// Telling the client what this server can decompress lets one that sent an
	// encoding this server lacks choose another next time.
	if accept := advertisedCompressors(); len(accept) > 0 {
		h.Set(grpcAcceptEncodingHeader, strings.Join(accept, ","))
	}
	grpcMetadataToHeaders(md, h)
}

func (g *grpcWebServerProtocolAdapter) processUnaryRequest() ([]byte, bool, error) {
	data, compressed, end, err := g.reqBody.next()
	if err != nil {
		if err == io.EOF {
			return nil, false, status.Error(codes.InvalidArgument, "request body has no message")
		}
		return nil, false, err
	}
	if end {
		return nil, false, status.Error(codes.InvalidArgument, "request body has no message")
	}
	// A unary request carries exactly one message. The body has ended by now, so
	// reading on does not wait for anything.
	if _, _, _, err := g.reqBody.next(); err != io.EOF {
		return nil, false, status.Error(codes.InvalidArgument, "method accepts 1 request message but client sent >1")
	}
	return data, compressed, nil
}

// writeUnary writes a unary response exactly like a stream's: this protocol
// reports the RPC's result in the body, after the message, so nothing needs to be
// held back.
func (g *grpcWebServerProtocolAdapter) writeUnary(_ context.Context, reply unaryReply) {
	g.setResponseHeaders(reply.compressorName, reply.headers)
	st, trailers := reply.status, reply.trailers
	if st == nil {
		if err := g.writeMessage(reply.message, reply.compressorName != ""); err != nil {
			st, trailers = status.Newf(codes.Internal, "could not write response: %v", err), nil
		}
	}
	g.writeTrailers(st, trailers)
}

// writeMessage writes a frame holding the given message.
func (g *grpcWebServerProtocolAdapter) writeMessage(data []byte, compressed bool) error {
	frames, err := g.streamMessage(data, compressed)
	if err != nil {
		return err
	}
	return writeFrames(g.w, frames)
}

func (g *grpcWebServerProtocolAdapter) sendStreamHeaders(compressorName string, md metadata.MD) {
	g.setResponseHeaders(compressorName, md)
	g.w.WriteHeader(http.StatusOK)
}

func (g *grpcWebServerProtocolAdapter) readStreamRequest() ([]byte, bool, error) {
	data, compressed, end, err := g.reqBody.next()
	if err != nil {
		return nil, false, err
	}
	if end {
		// Clients do not send trailers in this protocol; treat it as end-of-stream.
		return nil, false, io.EOF
	}
	return data, compressed, nil
}

func (g *grpcWebServerProtocolAdapter) finishStream(st *status.Status, trailers metadata.MD) {
	g.writeTrailers(st, trailers)
}

// writeTrailers writes the final frame, which is where this protocol puts the
// RPC's status and trailing metadata, for unary and streaming calls alike.
func (g *grpcWebServerProtocolAdapter) writeTrailers(st *status.Status, trailers metadata.MD) {
	frames, frameErr := envelopeFrame(grpcWebFlagTrailers, grpcWebTrailers(st, trailers))
	if frameErr != nil {
		return
	}
	if writeFrames(g.w, frames) != nil {
		return
	}
	flushResponse(g.w)
}

type grpcWebClientProtocolAdapter struct {
	// codecName is what the request is encoded with, and so what the response must
	// be encoded with too.
	codecName string
	// maxRecv bounds a single response message; see the field of the same name on
	// httpGRPCClientProtocolAdapter.
	maxRecv int
	// respBody is created over the response body by processStreamHeaders, and read
	// by readStreamResponse. respCompressName is the encoding the response named,
	// which a trailers frame may use as well as a message frame.
	respBody         *envelopeReader
	respCompressName string
	// trailersOnly is set by processStreamHeaders for a response with no body, whose
	// outcome is in its headers; readStreamResponse then reports it, with
	// trailersOnlyMD, rather than reading frames.
	trailersOnly   bool
	trailersOnlyMD metadata.MD
	trailersErr    error
}

var _ clientProtocolAdapter = (*grpcWebClientProtocolAdapter)(nil)

func newGRPCWebClientAdapter(codecName string, maxRecv int) (clientProtocolAdapter, error) {
	return &grpcWebClientProtocolAdapter{codecName: codecName, maxRecv: maxRecv}, nil
}

// unaryMessage frames a unary body exactly like a streaming one: this protocol
// draws no distinction between them.
func (g *grpcWebClientProtocolAdapter) unaryMessage(data []byte, compressed bool) ([][]byte, error) {
	return g.streamMessage(data, compressed)
}

func (g *grpcWebClientProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	return envelopeMessage(data, compressed)
}

func (g *grpcWebClientProtocolAdapter) requestHeaders(ctx context.Context, _ bool, compressorName string, acceptCompressors []string) (http.Header, error) {
	h := http.Header{}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		grpcMetadataToHeaders(md, h)
	}
	if deadline, ok := ctx.Deadline(); ok {
		h.Set("GRPC-Timeout", grpcTimeout(deadline))
	}
	h.Set("Content-Type", grpcWebContentTypeFor(g.codecName))
	h.Set("X-Grpc-Web", "1")
	if !isIdentity(compressorName) {
		h.Set(grpcEncodingHeader, compressorName)
	}
	if len(acceptCompressors) > 0 {
		h.Set(grpcAcceptEncodingHeader, strings.Join(acceptCompressors, ","))
	}
	return h, nil
}

func (g *grpcWebClientProtocolAdapter) processUnaryResponse(resp *http.Response) (unaryResponse, error) {
	if err := g.checkResponse(resp); err != nil {
		return unaryResponse{}, err
	}
	if isTrailersOnly(resp) {
		trailers, err := grpcStatusFromHeaders(resp.Header)
		if err == nil {
			err = status.Error(codes.Internal, "server sent no response message")
		}
		return unaryResponse{trailers: trailers}, err
	}
	headers, err := grpcHeadersToMetadata(resp.Header)
	if err != nil {
		return unaryResponse{}, err
	}
	result := unaryResponse{headers: headers}

	// A unary response is a message frame followed by the trailers frame, so both
	// are read here: the RPC's outcome is only known after the message.
	body := newEnvelopeReader(resp.Body, g.maxRecv, grpcWebFlagTrailers)
	data, compressed, end, err := body.next()
	if err != nil {
		if isLimitError(err) {
			return result, err
		}
		return result, status.Errorf(codes.Internal, "could not read response: %v", err)
	}

	var msg []byte
	var msgCompressed bool
	if !end {
		msg, msgCompressed = data, compressed
		data, compressed, end, err = body.next()
		if err != nil {
			if isLimitError(err) {
				return result, err
			}
			return result, status.Errorf(codes.Internal, "response has no trailers frame: %v", err)
		}
		if !end {
			return result, status.Error(codes.Internal, "method should return 1 response message but server sent >1")
		}
	}

	result.trailers, err = g.parseTrailers(data, compressed, resp.Header.Get(grpcEncodingHeader))
	if err != nil {
		return result, err
	}
	if msg == nil {
		return result, status.Error(codes.Internal, "server sent no response message")
	}

	result.message = msg
	if msgCompressed {
		result.compressorName = resp.Header.Get(grpcEncodingHeader)
	}
	return result, nil
}

func (g *grpcWebClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, string, error) {
	if err := g.checkResponse(resp); err != nil {
		return nil, "", err
	}
	if isTrailersOnly(resp) {
		// The whole outcome is in the headers. It is reported as the end of the
		// stream, from readStreamResponse, rather than as a failure to start one:
		// a stream that succeeds without sending anything ends this way too.
		g.trailersOnly = true
		g.trailersOnlyMD, g.trailersErr = grpcStatusFromHeaders(resp.Header)
		return metadata.MD{}, "", nil
	}
	md, err := grpcHeadersToMetadata(resp.Header)
	if err != nil {
		return nil, "", err
	}
	g.respBody = newEnvelopeReader(resp.Body, g.maxRecv, grpcWebFlagTrailers)
	g.respCompressName = resp.Header.Get(grpcEncodingHeader)
	return md, g.respCompressName, nil
}

func (g *grpcWebClientProtocolAdapter) readStreamResponse() ([]byte, bool, metadata.MD, error) {
	if g.trailersOnly {
		if g.trailersErr != nil {
			return nil, false, g.trailersOnlyMD, g.trailersErr
		}
		return nil, false, g.trailersOnlyMD, io.EOF
	}
	if g.respBody == nil {
		return nil, false, nil, fmt.Errorf("readStreamResponse called before processStreamHeaders")
	}
	data, compressed, end, err := g.respBody.next()
	if err != nil {
		if err == io.EOF {
			// The body ended without the trailers frame, so the outcome is unknown.
			err = status.Error(codes.Internal, "response ended without a trailers frame")
		}
		return nil, false, nil, err
	}
	if !end {
		return data, compressed, nil, nil
	}

	trailers, statErr := g.parseTrailers(data, compressed, g.respCompressName)
	if statErr != nil {
		return nil, false, trailers, statErr
	}
	// The stream ended successfully, which callers distinguish from a failure by
	// the io.EOF sentinel rather than a nil error.
	return nil, false, trailers, io.EOF
}

// parseTrailers parses a trailers frame, decompressing it first if it is
// compressed. The protocol allows that, and some servers do it, using the same
// encoding as the response's messages.
func (g *grpcWebClientProtocolAdapter) parseTrailers(data []byte, compressed bool, compressName string) (metadata.MD, error) {
	if compressed {
		var err error
		if data, err = decompressEndFrame("trailers frame", data, compressName, g.maxRecv); err != nil {
			return nil, err
		}
	}
	return parseGRPCWebTrailers(data)
}

// isTrailersOnly reports whether a response carries the RPC's outcome in its
// headers, with no body to follow.
func isTrailersOnly(resp *http.Response) bool {
	return resp.Header.Get(grpcStatusHeader) != ""
}

// checkResponse rejects a reply that is not a gRPC-Web response at all. This
// protocol always answers 200 with the outcome in the body or, for a
// trailers-only response, in the headers, so any other status came from
// something in between, such as a proxy.
func (g *grpcWebClientProtocolAdapter) checkResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return status.Errorf(codeFromHttpStatus(resp.StatusCode),
			"server replied with HTTP %s", resp.Status)
	}
	if isTrailersOnly(resp) && resp.Header.Get("Content-Type") == "" {
		// A trailers-only response need not label a body it does not have.
		return nil
	}
	return checkResponseContentType(resp, g.codecName, grpcWebContentTypeFor(g.codecName), parseGRPCWebContentType)
}
