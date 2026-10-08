package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/fullstorydev/grpchan/internal"
)

// connectChannel is a grpc.ClientConnInterface that makes its calls with
// connect-go, so that the shared channel test suite can drive a client that is
// not this repo's own. It translates between the two APIs: gRPC metadata and
// call options on one side, HTTP headers and connect's call info on the other.
//
// It only handles grpchantesting.Message, which is all the suite sends.
type connectChannel struct {
	httpClient connect.HTTPClient
	baseURL    string
	opts       []connect.ClientOption

	mu      sync.Mutex
	clients map[string]*connect.Client[message, message]
}

var _ grpc.ClientConnInterface = (*connectChannel)(nil)

func newConnectChannel(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) *connectChannel {
	return &connectChannel{
		httpClient: httpClient,
		baseURL:    baseURL,
		opts:       opts,
		clients:    map[string]*connect.Client[message, message]{},
	}
}

func (c *connectChannel) client(method string) *connect.Client[message, message] {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.clients[method]
	if !ok {
		client = connect.NewClient[message, message](c.httpClient, c.baseURL+method, c.opts...)
		c.clients[method] = client
	}
	return client
}

func (c *connectChannel) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	req, ok := args.(*message)
	if !ok {
		return fmt.Errorf("unsupported request type %T", args)
	}
	resp, ok := reply.(*message)
	if !ok {
		return fmt.Errorf("unsupported response type %T", reply)
	}
	copts := internal.GetCallOptions(opts)

	// The call info is what reports response metadata on a call that fails.
	ctx, info := connect.NewClientContext(ctx)
	connectReq := connect.NewRequest(req)
	setOutgoingHeaders(ctx, connectReq.Header())
	connectResp, err := c.client(method).CallUnary(ctx, connectReq)
	copts.SetHeaders(headersToMetadata(info.ResponseHeader()))
	copts.SetTrailers(headersToMetadata(info.ResponseTrailer()))
	if err != nil {
		return grpcError(err)
	}
	proto.Reset(resp)
	proto.Merge(resp, connectResp.Msg)
	return nil
}

func (c *connectChannel) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// Every kind of stream is made as a bidirectional one, which gRPC-Web frames
	// the same way as the others.
	stream := c.client(method).CallBidiStream(ctx)
	setOutgoingHeaders(ctx, stream.RequestHeader())
	return &connectChannelStream{
		ctx:           ctx,
		stream:        stream,
		copts:         internal.GetCallOptions(opts),
		serverStreams: desc.ServerStreams,
	}, nil
}

// connectChannelStream adapts a connect-go stream to grpc.ClientStream.
type connectChannelStream struct {
	ctx    context.Context
	stream *connect.BidiStreamForClient[message, message]
	copts  *internal.CallOptions
	// serverStreams is false when the method has a single response, which RecvMsg
	// then reads through to the end of the stream; see RecvMsg.
	serverStreams bool
}

func (s *connectChannelStream) Header() (metadata.MD, error) {
	// connect-go has the response headers once the first receive returns, which a
	// caller of Header has to wait for in gRPC too.
	return headersToMetadata(s.stream.ResponseHeader()), nil
}

func (s *connectChannelStream) Trailer() metadata.MD {
	return headersToMetadata(s.stream.ResponseTrailer())
}

func (s *connectChannelStream) CloseSend() error {
	return s.stream.CloseRequest()
}

func (s *connectChannelStream) Context() context.Context {
	return s.ctx
}

func (s *connectChannelStream) SendMsg(m any) error {
	msg, ok := m.(*message)
	if !ok {
		return fmt.Errorf("unsupported message type %T", m)
	}
	if err := s.stream.Send(msg); err != nil {
		if errors.Is(err, io.EOF) {
			// As in gRPC: the stream has ended, and RecvMsg reports why.
			return io.EOF
		}
		return grpcError(err)
	}
	return nil
}

func (s *connectChannelStream) RecvMsg(m any) error {
	msg, ok := m.(*message)
	if !ok {
		return fmt.Errorf("unsupported message type %T", m)
	}
	received, err := s.stream.Receive()
	if err != nil {
		s.copts.SetHeaders(headersToMetadata(s.stream.ResponseHeader()))
		s.copts.SetTrailers(headersToMetadata(s.stream.ResponseTrailer()))
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return grpcError(err)
	}
	proto.Reset(msg)
	proto.Merge(msg, received)
	if !s.serverStreams {
		// As gRPC-Go does for a method with a single response: read on to the end of
		// the stream, which is what makes the trailers available, and which also
		// catches a server that sends too many responses.
		if _, err := s.stream.Receive(); !errors.Is(err, io.EOF) {
			if err == nil {
				return status.Error(codes.Internal, "method should return 1 response message but server sent >1")
			}
			return grpcError(err)
		}
		s.copts.SetHeaders(headersToMetadata(s.stream.ResponseHeader()))
		s.copts.SetTrailers(headersToMetadata(s.stream.ResponseTrailer()))
	}
	return nil
}

// grpcError converts an error from connect-go into a gRPC status error, which is
// what the suite checks for. Connect's codes are gRPC's, number for number.
func grpcError(err error) error {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return err
	}
	stpb := status.New(codes.Code(connectErr.Code()), connectErr.Message()).Proto()
	for _, d := range connectErr.Details() {
		stpb.Details = append(stpb.Details, &anypb.Any{
			TypeUrl: "type.googleapis.com/" + d.Type(),
			Value:   d.Bytes(),
		})
	}
	return status.FromProto(stpb).Err()
}

// setOutgoingHeaders writes the outgoing metadata in the given context as request
// headers, encoding binary values as gRPC does.
func setOutgoingHeaders(ctx context.Context, h http.Header) {
	md, _ := metadata.FromOutgoingContext(ctx)
	for k, vs := range md {
		for _, v := range vs {
			if strings.HasSuffix(k, "-bin") {
				v = connect.EncodeBinaryHeader([]byte(v))
			}
			h.Add(k, v)
		}
	}
}

// headersToMetadata is the inverse of setOutgoingHeaders, for response headers
// and trailers.
func headersToMetadata(h http.Header) metadata.MD {
	md := metadata.MD{}
	for k, vs := range h {
		k = strings.ToLower(k)
		for _, v := range vs {
			if strings.HasSuffix(k, "-bin") {
				decoded, err := connect.DecodeBinaryHeader(v)
				if err != nil {
					continue
				}
				v = string(decoded)
			}
			md[k] = append(md[k], v)
		}
	}
	return md
}
