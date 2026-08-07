package httpgrpc_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan"
	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGrpcOverHttp(t *testing.T) {
	svr := &grpchantesting.TestServer{}
	reg := grpchan.HandlerMap{}
	grpchantesting.RegisterTestServiceServer(reg, svr)

	var mux http.ServeMux
	httpgrpc.HandleServices(mux.HandleFunc, "/", reg, nil, nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: &mux}
	go httpServer.Serve(l)
	defer httpServer.Close()

	// now setup client stub
	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	cc := httpgrpc.Channel{
		Transport: http.DefaultTransport,
		BaseURL:   u,
	}

	grpchantesting.RunChannelTestCases(t, &cc, false)

	t.Run("empty-trailer", func(t *testing.T) {
		// test RPC w/ streaming response where trailer message is empty
		// (e.g. no trailer metadata and code == 0 [OK])
		cli := grpchantesting.NewTestServiceClient(&cc)
		str, err := cli.ServerStream(context.Background(), &grpchantesting.Message{})
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		// if there is an issue with trailer message, it will appear to be
		// a regular message and err would be nil
		_, err = str.Recv()
		if err != io.EOF {
			t.Fatalf("server stream should not have returned any messages")
		}
	})
}

// This test is nearly identical to TestGrpcOverHttp, except that it uses
// *httpgrpc.Server instead of httpgrpc.HandleServices.
func TestServer(t *testing.T) {
	errFunc := func(reqCtx context.Context, st *status.Status, response http.ResponseWriter) {
	}

	svc := &grpchantesting.TestServer{}
	svr := httpgrpc.NewServer(httpgrpc.WithBasePath("/foo/"), httpgrpc.ErrorRenderer(errFunc))
	grpchantesting.RegisterTestServiceServer(svr, svc)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: svr}
	go httpServer.Serve(l)
	defer httpServer.Close()

	// now setup client stub
	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d/foo/", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	cc := httpgrpc.Channel{
		Transport: http.DefaultTransport,
		BaseURL:   u,
	}

	grpchantesting.RunChannelTestCases(t, &cc, false)

	t.Run("empty-trailer", func(t *testing.T) {
		// test RPC w/ streaming response where trailer message is empty
		// (e.g. no trailer metadata and code == 0 [OK])
		cli := grpchantesting.NewTestServiceClient(&cc)
		str, err := cli.ServerStream(context.Background(), &grpchantesting.Message{})
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		// if there is an issue with trailer message, it will appear to be
		// a regular message and err would be nil
		_, err = str.Recv()
		if err != io.EOF {
			t.Fatalf("server stream should not have returned any messages")
		}
	})
}

func TestJSONSSEServer(t *testing.T) {
	errFunc := func(reqCtx context.Context, st *status.Status, response http.ResponseWriter) {
	}

	svc := &grpchantesting.TestServer{}
	svr := httpgrpc.NewServer(httpgrpc.WithBasePath("/foo/"), httpgrpc.ErrorRenderer(errFunc))
	grpchantesting.RegisterTestServiceServer(svr, svc)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: svr}
	go httpServer.Serve(l)
	defer httpServer.Close()

	// now setup client stub
	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d/foo/", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	cc, err := httpgrpc.NewChannel(u, http.DefaultTransport, httpgrpc.WithJSONEncoding(true))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	grpchantesting.RunChannelTestCases(t, cc, false)

	t.Run("empty-trailer", func(t *testing.T) {
		// test RPC w/ streaming response where trailer message is empty
		// (e.g. no trailer metadata and code == 0 [OK])
		cli := grpchantesting.NewTestServiceClient(cc)
		str, err := cli.ServerStream(context.Background(), &grpchantesting.Message{})
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		// if there is an issue with trailer message, it will appear to be
		// a regular message and err would be nil
		_, err = str.Recv()
		if err != io.EOF {
			t.Fatalf("server stream should not have returned any messages")
		}
	})
}

// TestUnaryXGrpcDetailsWireCodec asserts that X-GRPC-Details header payloads use
// the same encoding as the unary request body (protobuf vs JSON), so the
// client recovers google.rpc.Status details correctly for both modes.
func TestUnaryXGrpcDetailsWireCodec(t *testing.T) {
	detailMsg := &structpb.ListValue{
		Values: []*structpb.Value{
			{Kind: &structpb.Value_StringValue{StringValue: "x-grpc-details-wire"}},
		},
	}
	wantAny := new(anypb.Any)
	if err := anypb.MarshalFrom(wantAny, detailMsg, proto.MarshalOptions{}); err != nil {
		t.Fatalf("marshal detail any: %v", err)
	}

	svc := &grpchantesting.TestServer{}
	reg := grpchan.HandlerMap{}
	grpchantesting.RegisterTestServiceServer(reg, svc)

	mux := http.NewServeMux()
	httpgrpc.HandleServices(mux.HandleFunc, "/", reg, nil, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()

	u, err := url.Parse(fmt.Sprintf("http://%s", ln.Addr().String()))
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	mkReq := func() *grpchantesting.Message {
		return &grpchantesting.Message{
			Code:         int32(codes.FailedPrecondition),
			ErrorDetails: []*anypb.Any{proto.Clone(wantAny).(*anypb.Any)},
		}
	}

	t.Run("protobuf", func(t *testing.T) {
		cc := &httpgrpc.Channel{Transport: http.DefaultTransport, BaseURL: u}
		cli := grpchantesting.NewTestServiceClient(cc)
		_, err := cli.Unary(context.Background(), mkReq())
		assertUnaryErrorHasDetail(t, err, codes.FailedPrecondition, detailMsg)
	})

	t.Run("json", func(t *testing.T) {
		cc, err := httpgrpc.NewChannel(u, http.DefaultTransport, httpgrpc.WithJSONEncoding(true))
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		cli := grpchantesting.NewTestServiceClient(cc)
		_, err = cli.Unary(context.Background(), mkReq())
		assertUnaryErrorHasDetail(t, err, codes.FailedPrecondition, detailMsg)
	})
}

func assertUnaryErrorHasDetail(t *testing.T, err error, wantCode codes.Code, wantDetail proto.Message) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != wantCode {
		t.Fatalf("status code: got %v want %v", st.Code(), wantCode)
	}
	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("status details: got %d want 1 (%v)", len(details), details)
	}
	if !proto.Equal(details[0].(proto.Message), wantDetail) {
		t.Fatalf("status detail mismatch:\ngot  %v\nwant %v", details[0], wantDetail)
	}
}

func TestNewChannelValidation(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	t.Run("base URL is required", func(t *testing.T) {
		if _, err := httpgrpc.NewChannel(nil, http.DefaultTransport); err == nil {
			t.Fatal("expected an error for a nil base URL")
		}
	})
	t.Run("transport is required", func(t *testing.T) {
		if _, err := httpgrpc.NewChannel(u, nil); err == nil {
			t.Fatal("expected an error for a nil transport")
		}
	})
	t.Run("both supplied", func(t *testing.T) {
		ch, err := httpgrpc.NewChannel(u, http.DefaultTransport, httpgrpc.WithJSONEncoding(true))
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		if ch.BaseURL != u || ch.Transport == nil {
			t.Fatal("channel was not configured with what it was given")
		}
	})
}

// TestChannelMissingFields covers the checks that cannot be made by NewChannel: a
// Channel may also be built as a struct literal, and its fields are exported, so
// they can be missing or cleared after construction. Either way the RPC should
// report the problem, where it used to panic dereferencing a nil base URL.
func TestChannelMissingFields(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	checkFails := func(t *testing.T, ch *httpgrpc.Channel) {
		t.Helper()
		cli := grpchantesting.NewTestServiceClient(ch)
		if _, err := cli.Unary(context.Background(), &grpchantesting.Message{}); err == nil {
			t.Error("expected a unary RPC to report the missing field")
		}
		if _, err := cli.ServerStream(context.Background(), &grpchantesting.Message{}); err == nil {
			t.Error("expected a streaming RPC to report the missing field")
		}
	}

	t.Run("struct literal without base URL", func(t *testing.T) {
		checkFails(t, &httpgrpc.Channel{Transport: http.DefaultTransport})
	})
	t.Run("struct literal without transport", func(t *testing.T) {
		checkFails(t, &httpgrpc.Channel{BaseURL: u})
	})
	t.Run("field cleared after NewChannel", func(t *testing.T) {
		ch, err := httpgrpc.NewChannel(u, http.DefaultTransport)
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		ch.BaseURL = nil
		checkFails(t, ch)
	})
}

// TestStreamSurvivesGC guards the finalizer that cancels an abandoned stream's
// context against cancelling one that is still being used.
//
// The finalizer is attached to the wrapper value handed back to the caller. If
// that wrapper's methods were promoted from an embedded interface, the wrapper
// would fall out of reach as soon as a call descended into the stream underneath
// it, since nothing on the stack refers to it any more. A garbage collection
// during a blocking Recv would then run the finalizer and cancel an RPC that was
// still in progress, surfacing as a spurious "context canceled" in place of
// whatever really ended the call. With collections forced, that reproduced on
// every attempt.
func TestStreamSurvivesGC(t *testing.T) {
	svr := &grpchantesting.TestServer{}
	reg := grpchan.HandlerMap{}
	grpchantesting.RegisterTestServiceServer(reg, svr)

	var mux http.ServeMux
	httpgrpc.HandleServices(mux.HandleFunc, "/", reg, nil, nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: &mux}
	go httpServer.Serve(l)
	defer httpServer.Close()

	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	cc := httpgrpc.Channel{Transport: http.DefaultTransport, BaseURL: u}
	cli := grpchantesting.NewTestServiceClient(&cc)

	for i := 0; i < 5; i++ {
		// The deadline is what should end this call: the server sleeps for far
		// longer, so the stream stays blocked in Recv throughout.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		ss, err := cli.ServerStream(ctx, &grpchantesting.Message{Count: 3, DelayMillis: 500})
		if err != nil {
			cancel()
			t.Fatalf("opening stream failed: %v", err)
		}

		// Collect aggressively while the call is blocked, and make no further use of
		// ss afterwards, so nothing but the wrapper's own methods can keep it
		// reachable for the duration of the call.
		done := make(chan struct{})
		go func() {
			defer close(done)
			for j := 0; j < 20; j++ {
				runtime.GC()
				time.Sleep(2 * time.Millisecond)
			}
		}()
		_, err = ss.Recv()
		<-done
		cancel()

		if got := status.Code(err); got != codes.DeadlineExceeded {
			t.Fatalf("got %v (%v), want DeadlineExceeded: a collection during the call "+
				"cancelled a stream that was still in use", got, err)
		}
	}
}

// roundTripWatcher reports when a round trip ends.
type roundTripWatcher struct {
	inner http.RoundTripper
	ended chan error
}

func (t *roundTripWatcher) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(r)
	select {
	case t.ended <- err:
	default:
	}
	return resp, err
}

// TestAbandonedStreamIsCleanedUp covers what the cleanup on clientStreamWrapper
// is for: a caller that stops using a stream without finishing or cancelling it
// should not leave the RPC running.
//
// The assertion is deliberately client-side only. Whether the *server* notices is
// a different question with a different answer: net/http does not watch a
// connection for a disconnect while a request body remains unread, so a handler
// finds out by way of a failed read rather than a cancelled context.
func TestAbandonedStreamIsCleanedUp(t *testing.T) {
	// A server that never answers, so the round trip stays pending until the
	// client itself gives up.
	block := make(chan struct{})
	defer close(block)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	})}
	go httpServer.Serve(l)
	defer httpServer.Close()

	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	transport := &roundTripWatcher{inner: http.DefaultTransport, ended: make(chan error, 1)}
	cc := httpgrpc.Channel{Transport: transport, BaseURL: u}

	// Open a stream and send on it, then abandon it: no CloseSend, no cancel. It
	// is created on a goroutine that then exits, so no stack frame keeps the
	// stream reachable and it can actually be collected.
	desc := &grpc.StreamDesc{StreamName: "Abandoned", ServerStreams: true, ClientStreams: true}
	started := make(chan error, 1)
	go func() {
		stream, err := cc.NewStream(context.Background(), desc, "/test.Abandoned/Abandoned")
		if err != nil {
			started <- err
			return
		}
		started <- stream.SendMsg(&grpchantesting.Message{})
	}()
	if err := <-started; err != nil {
		t.Fatalf("failed to start the stream: %v", err)
	}

	for i := 0; i < 100; i++ {
		runtime.GC()
		select {
		case err := <-transport.ended:
			if err == nil {
				t.Fatalf("round trip ended without an error, so the stream was not abandoned")
			}
			t.Logf("abandoned stream torn down after %d collections: %v", i+1, err)
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("the abandoned stream's round trip never ended, so its goroutine and " +
		"connection are still held")
}

// TestChannelMessageSizeLimits covers the MaxCallRecvMsgSize and MaxCallSendMsgSize
// call options, which the channel accepted but did not act on. The default limits
// must not reject ordinary payloads.
func TestChannelMessageSizeLimits(t *testing.T) {
	svr := httpgrpc.NewServer()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	httpServer := http.Server{Handler: svr}
	go httpServer.Serve(l)
	defer httpServer.Close()

	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	ch, err := httpgrpc.NewChannel(u, http.DefaultTransport)
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	cli := grpchantesting.NewTestServiceClient(ch)
	ctx := context.Background()
	msg := &grpchantesting.Message{Payload: make([]byte, 1024)}

	t.Run("defaults allow a real payload", func(t *testing.T) {
		if _, err := cli.Unary(ctx, msg); err != nil {
			t.Fatalf("unary with a 1kb payload failed: %v", err)
		}
	})
	t.Run("unary send limit enforced", func(t *testing.T) {
		_, err := cli.Unary(ctx, msg, grpc.MaxCallSendMsgSize(10))
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("unary recv limit enforced", func(t *testing.T) {
		_, err := cli.Unary(ctx, msg, grpc.MaxCallRecvMsgSize(10))
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("stream send limit enforced", func(t *testing.T) {
		str, err := cli.ClientStream(ctx, grpc.MaxCallSendMsgSize(10))
		if err != nil {
			t.Fatalf("failed to initiate client stream: %v", err)
		}
		if got := status.Code(str.Send(msg)); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v", got)
		}
	})
	t.Run("unary recv limit rejects before reading the body", func(t *testing.T) {
		// A limit is only worth having if an over-large response is refused rather
		// than pulled into memory and measured afterwards. When the response
		// declares its length, that can happen before a single byte is read.
		body := &recordingBody{}
		transport := &fixedResponse{resp: &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{httpgrpc.UnaryRpcContentType_V1}},
			ContentLength: 1 << 20,
			Body:          body,
		}}
		ch, err := httpgrpc.NewChannel(u, transport)
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		_, err = grpchantesting.NewTestServiceClient(ch).Unary(ctx, msg, grpc.MaxCallRecvMsgSize(10))
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
		if body.read {
			t.Error("the response body was read, so the declared length was not acted on")
		}
	})
	t.Run("stream recv limit rejects before reading the message", func(t *testing.T) {
		// The size preface announces how big the message is, so an over-large one
		// can be refused without reading it. The server below announces a large
		// message and never sends it: if the client waited for the body it would
		// fail with an unexpected EOF instead of a limit error.
		announced := make([]byte, 4)
		binary.BigEndian.PutUint32(announced, 1<<20)
		svrURL := serveHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", httpgrpc.StreamRpcContentType_V1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(announced)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}))
		ch, err := httpgrpc.NewChannel(svrURL, http.DefaultTransport)
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		str, err := grpchantesting.NewTestServiceClient(ch).ServerStream(
			ctx, &grpchantesting.Message{}, grpc.MaxCallRecvMsgSize(10))
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		_, err = str.Recv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("stream recv limit enforced", func(t *testing.T) {
		// Count makes the server send one message back, echoing the payload.
		req := &grpchantesting.Message{Payload: make([]byte, 1024), Count: 1}
		str, err := cli.ServerStream(ctx, req, grpc.MaxCallRecvMsgSize(10))
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		_, err = str.Recv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("recv limit applies to the trailer", func(t *testing.T) {
		// The sender exempts the trailer from its own limit, so that a stream can
		// always report how it ended. The receiver grants no such exemption: the
		// trailer arrives on the same connection as everything else, and a limit
		// that did not cover it would not bound much.
		//
		// The cost of that is the RPC's real outcome, which the caller never learns.
		// See the TODO on checkSendSize.
		req := &grpchantesting.Message{
			Trailers: map[string][]byte{"large-trailer": make([]byte, 1024)},
		}
		str, err := cli.ServerStream(ctx, req, grpc.MaxCallRecvMsgSize(100))
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		_, err = str.Recv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
}

// TestServerMessageSizeLimits covers the server-side limits. Unlike the client,
// the server takes them from options rather than per-call, so a server with no
// options must keep behaving as it did before the options existed.
func TestServerMessageSizeLimits(t *testing.T) {
	// HandlerOption satisfies ServerOption, so the limit options can be given
	// directly to NewServer.
	newClient := func(t *testing.T, opts ...httpgrpc.ServerOption) grpchantesting.TestServiceClient {
		t.Helper()
		svr := httpgrpc.NewServer(opts...)
		grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
		ch, err := httpgrpc.NewChannel(serveHandler(t, svr), http.DefaultTransport)
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		return grpchantesting.NewTestServiceClient(ch)
	}
	ctx := context.Background()
	msg := &grpchantesting.Message{Payload: make([]byte, 1024)}

	t.Run("defaults allow a real payload", func(t *testing.T) {
		if _, err := newClient(t).Unary(ctx, msg); err != nil {
			t.Fatalf("unary with a 1kb payload failed: %v", err)
		}
	})
	t.Run("unary recv limit enforced", func(t *testing.T) {
		_, err := newClient(t, httpgrpc.WithMaxRecvMsgSize(10)).Unary(ctx, msg)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("unary send limit enforced", func(t *testing.T) {
		// The service echoes the payload, so a small send limit fails the response
		// rather than the request.
		_, err := newClient(t, httpgrpc.WithMaxSendMsgSize(10)).Unary(ctx, msg)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("stream recv limit enforced", func(t *testing.T) {
		str, err := newClient(t, httpgrpc.WithMaxRecvMsgSize(10)).ClientStream(ctx)
		if err != nil {
			t.Fatalf("failed to initiate client stream: %v", err)
		}
		// The send itself may succeed, since the server only reports the refusal
		// once it has read the message; the result of the call is what matters.
		_ = str.Send(msg)
		_, err = str.CloseAndRecv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("stream send limit enforced", func(t *testing.T) {
		req := &grpchantesting.Message{Payload: make([]byte, 1024), Count: 1}
		str, err := newClient(t, httpgrpc.WithMaxSendMsgSize(10)).ServerStream(ctx, req)
		if err != nil {
			t.Fatalf("failed to initiate server stream: %v", err)
		}
		_, err = str.Recv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("recv limit rejects before reading the body", func(t *testing.T) {
		// As on the client, a declared length means an over-large request can be
		// refused without reading it. The request is written by hand because that is
		// the only way to declare a length and then send nothing: an HTTP client will
		// not knowingly send a request whose body is shorter than it claims. A server
		// that waits for the body it was promised gets no answer, so this hangs until
		// the deadline rather than passing by accident.
		svr := httpgrpc.NewServer(httpgrpc.WithMaxRecvMsgSize(10))
		grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
		base := serveHandler(t, svr)

		conn, err := net.Dial("tcp", base.Host)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}

		_, err = io.WriteString(conn, "POST /grpchantesting.TestService/Unary HTTP/1.1\r\n"+
			"Host: "+base.Host+"\r\n"+
			"Content-Type: "+httpgrpc.UnaryRpcContentType_V1+"\r\n"+
			"Content-Length: 1048576\r\n"+
			"\r\n")
		if err != nil {
			t.Fatalf("failed to write request: %v", err)
		}

		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		defer resp.Body.Close()

		// codes.ResourceExhausted is 8.
		if got := resp.Header.Get("X-GRPC-Status"); !strings.HasPrefix(got, "8:") {
			t.Fatalf("X-GRPC-Status = %q, want a ResourceExhausted status", got)
		}
	})
	t.Run("refusal outlives a handler that ignores it", func(t *testing.T) {
		// The handler here does what a careless one does: it discards the error and
		// returns as if all were well. Since net/http offers no way to abort a
		// response from outside the handler, the only defense is that the refusal
		// sticks -- every later operation fails the same way, and the recorded status
		// is what the caller is told, not the handler's rosier account.
		for _, name := range []string{"Recv", "Send"} {
			t.Run(name, func(t *testing.T) {
				svr := httpgrpc.NewServer(
					httpgrpc.WithMaxRecvMsgSize(10), httpgrpc.WithMaxSendMsgSize(10))
				svr.RegisterService(&swallowingServiceDesc, struct{}{})
				ch, err := httpgrpc.NewChannel(serveHandler(t, svr), http.DefaultTransport)
				if err != nil {
					t.Fatalf("failed to create channel: %v", err)
				}

				desc := &grpc.StreamDesc{StreamName: name, ClientStreams: true, ServerStreams: true}
				str, err := ch.NewStream(ctx, desc, "/"+swallowingServiceDesc.ServiceName+"/"+name)
				if err != nil {
					t.Fatalf("failed to initiate stream: %v", err)
				}
				_ = str.SendMsg(msg)
				_ = str.CloseSend()
				err = str.RecvMsg(&grpchantesting.Message{})
				if got := status.Code(err); got != codes.ResourceExhausted {
					t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
				}
			})
		}
	})
	t.Run("refusal reaches the caller before the handler returns", func(t *testing.T) {
		// The status is written where the stream ends, not where the handler returns.
		// This handler never returns until the test lets it, so a status that arrives
		// before then can only have come from the refusal itself.
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		blockingServiceDesc := grpc.ServiceDesc{
			ServiceName: "grpchantesting.BlockingService",
			HandlerType: (*interface{})(nil),
			Streams: []grpc.StreamDesc{
				{
					StreamName:    "Recv",
					ClientStreams: true,
					ServerStreams: true,
					Handler: func(_ interface{}, stream grpc.ServerStream) error {
						_ = stream.RecvMsg(&grpchantesting.Message{})
						<-release
						return nil
					},
				},
			},
		}

		svr := httpgrpc.NewServer(httpgrpc.WithMaxRecvMsgSize(10))
		svr.RegisterService(&blockingServiceDesc, struct{}{})
		ch, err := httpgrpc.NewChannel(serveHandler(t, svr), http.DefaultTransport)
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		desc := &grpc.StreamDesc{StreamName: "Recv", ClientStreams: true, ServerStreams: true}
		str, err := ch.NewStream(ctx, desc, "/"+blockingServiceDesc.ServiceName+"/Recv")
		if err != nil {
			t.Fatalf("failed to initiate stream: %v", err)
		}
		_ = str.SendMsg(msg)

		recvd := make(chan error, 1)
		go func() { recvd <- str.RecvMsg(&grpchantesting.Message{}) }()
		select {
		case err := <-recvd:
			if got := status.Code(err); got != codes.ResourceExhausted {
				t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("status did not arrive while the handler was still running")
		}
	})
}

// swallowingServiceDesc is a service whose handlers ignore the errors they are
// given, so that a test can tell what the transport reports on its own.
//
// Recv refuses the request message and Send refuses the response, which are the
// two ways a limit ends a stream; both then return nil, claiming success.
var swallowingServiceDesc = grpc.ServiceDesc{
	ServiceName: "grpchantesting.SwallowingService",
	HandlerType: (*interface{})(nil),
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Recv",
			ClientStreams: true,
			ServerStreams: true,
			Handler: func(_ interface{}, stream grpc.ServerStream) error {
				_ = stream.RecvMsg(&grpchantesting.Message{})
				return nil
			},
		},
		{
			StreamName:    "Send",
			ClientStreams: true,
			ServerStreams: true,
			Handler: func(_ interface{}, stream grpc.ServerStream) error {
				_ = stream.SendMsg(&grpchantesting.Message{Payload: make([]byte, 1024)})
				return nil
			},
		},
	},
}

// serveHandler starts an http.Handler on a loopback port and returns its base URL.
func serveHandler(t *testing.T, h http.Handler) *url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed it listen on socket: %v", err)
	}
	svr := http.Server{Handler: h}
	go svr.Serve(l)
	t.Cleanup(func() { svr.Close() })

	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	return u
}

// recordingBody reports whether anything tried to read it.
type recordingBody struct {
	read bool
}

func (b *recordingBody) Read([]byte) (int, error) {
	b.read = true
	return 0, io.EOF
}

func (b *recordingBody) Close() error { return nil }

// fixedResponse is a transport that answers every request with the same response,
// so that a test can hand the client a reply it could not get from a real server.
type fixedResponse struct {
	resp *http.Response
}

func (f *fixedResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	resp := *f.resp
	resp.Request = r
	return &resp, nil
}
