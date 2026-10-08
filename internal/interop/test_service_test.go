package interop

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/fullstorydev/grpchan/grpchantesting"
)

const testServicePrefix = "/grpchantesting.TestService/"

// newConnectTestService serves grpchantesting's TestService with connect-go. It
// mirrors grpchantesting.TestServer, so that this repo's client can run the
// shared channel test suite against a server that is not this repo's own.
func newConnectTestService(opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(testServicePrefix+"Unary",
		connect.NewUnaryHandler(testServicePrefix+"Unary", connectUnary, opts...))
	mux.Handle(testServicePrefix+"ClientStream",
		connect.NewClientStreamHandler(testServicePrefix+"ClientStream", connectClientStream, opts...))
	mux.Handle(testServicePrefix+"ServerStream",
		connect.NewServerStreamHandler(testServicePrefix+"ServerStream", connectServerStream, opts...))
	mux.Handle(testServicePrefix+"BidiStream",
		connect.NewBidiStreamHandler(testServicePrefix+"BidiStream", connectBidiStream, opts...))
	return mux
}

type message = grpchantesting.Message

func connectUnary(ctx context.Context, req *connect.Request[message]) (*connect.Response[message], error) {
	msg := req.Msg
	delay(msg)
	setResponseMetadata(ctx, msg)
	if msg.Code != 0 {
		return nil, errorFromRequest(msg)
	}
	return connect.NewResponse(&message{
		Headers: headersAsMap(req.Header()),
		Payload: msg.Payload,
	}), nil
}

func connectClientStream(ctx context.Context, stream *connect.ClientStream[message]) (*connect.Response[message], error) {
	var req *message
	count := int32(0)
	for stream.Receive() {
		req = stream.Msg()
		count++
		if req.Code != 0 {
			break
		}
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	if req == nil {
		req = &message{}
	}
	delay(req)
	setResponseMetadata(ctx, req)
	if req.Code != 0 {
		return nil, errorFromRequest(req)
	}
	return connect.NewResponse(&message{
		Headers: headersAsMap(stream.RequestHeader()),
		Payload: req.Payload,
		Count:   count,
	}), nil
}

func connectServerStream(_ context.Context, req *connect.Request[message], stream *connect.ServerStream[message]) error {
	msg := req.Msg
	delay(msg)
	setHeaders(stream.ResponseHeader(), msg.Headers)
	for i := 0; i < int(msg.Count); i++ {
		err := stream.Send(&message{
			Headers: headersAsMap(req.Header()),
			Payload: msg.Payload,
		})
		if err != nil {
			return err
		}
	}
	setHeaders(stream.ResponseTrailer(), msg.Trailers)
	if msg.Code != 0 {
		return errorFromRequest(msg)
	}
	return nil
}

func connectBidiStream(_ context.Context, stream *connect.BidiStream[message, message]) error {
	md := headersAsMap(stream.RequestHeader())
	var req *message
	count := int32(0)
	var responses []*message
	isHalfDuplex := false
	for {
		r, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		req = r
		delay(req)
		if count == 0 {
			setHeaders(stream.ResponseHeader(), req.Headers)
			isHalfDuplex = req.Count < 0
		}
		count++
		if req.Code != 0 {
			break
		}
		reply := &message{Headers: md, Payload: req.Payload, Count: count}
		if isHalfDuplex {
			// Half duplex means the client stream is consumed in full before any
			// response is sent, so these are held until then.
			responses = append(responses, reply)
		} else if err := stream.Send(reply); err != nil {
			return err
		}
	}
	for _, reply := range responses {
		if err := stream.Send(reply); err != nil {
			return err
		}
	}
	if req != nil {
		setHeaders(stream.ResponseTrailer(), req.Trailers)
		if req.Code != 0 {
			return errorFromRequest(req)
		}
	}
	return nil
}

// setResponseMetadata sets the response headers and trailers the request asks
// for. They are set through the call info rather than on a response, since a
// call that fails has no response, and the metadata must still be sent.
func setResponseMetadata(ctx context.Context, msg *message) {
	info, ok := connect.CallInfoForHandlerContext(ctx)
	if !ok {
		return
	}
	setHeaders(info.ResponseHeader(), msg.Headers)
	setHeaders(info.ResponseTrailer(), msg.Trailers)
}

func delay(msg *message) {
	if msg.DelayMillis > 0 {
		time.Sleep(time.Millisecond * time.Duration(msg.DelayMillis))
	}
}

// errorFromRequest builds the error the request asks for, as
// grpchantesting.TestServer does.
func errorFromRequest(req *message) *connect.Error {
	err := connect.NewError(connect.Code(req.Code), errors.New("error"))
	for _, detail := range req.ErrorDetails {
		if d, detailErr := connect.NewErrorDetail(detail); detailErr == nil {
			err.AddDetail(d)
		}
	}
	return err
}

// setHeaders writes metadata given as a map into HTTP headers, encoding binary
// values as gRPC does.
func setHeaders(h http.Header, md map[string][]byte) {
	for k, v := range md {
		if strings.HasSuffix(k, "-bin") {
			h.Set(k, connect.EncodeBinaryHeader(v))
		} else {
			h.Set(k, string(v))
		}
	}
}

// headersAsMap is the inverse of setHeaders, and mirrors how
// grpchantesting.TestServer echoes request metadata: the last value of each key.
func headersAsMap(h http.Header) map[string][]byte {
	m := map[string][]byte{}
	for k, vs := range h {
		if len(vs) == 0 {
			continue
		}
		k = strings.ToLower(k)
		v := []byte(vs[len(vs)-1])
		if strings.HasSuffix(k, "-bin") {
			decoded, err := connect.DecodeBinaryHeader(vs[len(vs)-1])
			if err != nil {
				continue
			}
			v = decoded
		}
		m[k] = v
	}
	return m
}
