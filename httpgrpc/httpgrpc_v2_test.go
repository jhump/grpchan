package httpgrpc_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"runtime"
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

// The httpgrpc protocol cannot support full-duplex bidirectional streams over
// HTTP 1.1, so every case below runs the suite with supportsFullDuplex false.
const supportsFullDuplex = false

func TestGRPCOverHTTPV2(t *testing.T) {
	reg := grpchan.HandlerMap{}
	grpchantesting.RegisterTestServiceServer(reg, &grpchantesting.TestServer{})

	var mux http.ServeMux
	httpgrpc.HandleServicesV2(mux.HandleFunc, "/", reg, nil, nil)

	// now setup client stub
	cc := httpgrpc.ChannelV2{
		Transport: http.DefaultTransport,
		BaseURL:   serveV2(t, &mux),
	}

	grpchantesting.RunChannelTestCases(t, &cc, supportsFullDuplex)

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

	// The cases above pass against either implementation, since the two are wire
	// compatible (that is what TestV1V2Compatibility asserts). This one does not:
	// a rejected HTTP method is reported in the protocol's own error format, where
	// the older handlers answer with a bare 405. It is what pins these handlers to
	// the protocol adapter.
	t.Run("wrong method", func(t *testing.T) {
		u := *cc.BaseURL
		u.Path = path.Join(u.Path, "grpchantesting.TestService/Unary")
		req, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}
		defer resp.Body.Close()

		// The status is reported as "<code>:<message>"; codes.Unimplemented is 12.
		got := resp.Header.Get("X-GRPC-Status")
		if got == "" {
			t.Fatalf("no X-GRPC-Status header: the error did not go through the protocol adapter (HTTP %s)", resp.Status)
		}
		if want := fmt.Sprintf("%d:", codes.Unimplemented); len(got) < len(want) || got[:len(want)] != want {
			t.Errorf("X-GRPC-Status = %q, want code %v", got, codes.Unimplemented)
		}
	})
}

// This test is nearly identical to TestGrpcOverHttpV2, except that it uses
// *httpgrpc.ServerV2 instead of httpgrpc.HandleServicesV2. It also exercises
// both sub-formats; the JSON case covers the server-sent-events framing used
// for JSON streams.
func TestServerV2(t *testing.T) {
	run := func(t *testing.T, opts ...httpgrpc.ChannelV2Option) {
		t.Helper()
		cc := newV2(t, v2ServerURL(t), opts...)

		grpchantesting.RunChannelTestCases(t, cc, supportsFullDuplex)

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

	t.Run("proto", func(t *testing.T) {
		run(t)
	})
	t.Run("json", func(t *testing.T) {
		run(t, httpgrpc.WithJSONEncodingV2(true))
	})
}

// TestUnaryXGrpcDetailsWireCodecV2 asserts that X-GRPC-Details header payloads use
// the same encoding as the unary request body (protobuf vs JSON), so the
// client recovers google.rpc.Status details correctly for both modes.
func TestUnaryXGRPCDetailsWireCodecV2(t *testing.T) {
	detailMsg := &structpb.ListValue{
		Values: []*structpb.Value{
			{Kind: &structpb.Value_StringValue{StringValue: "x-grpc-details-wire"}},
		},
	}
	wantAny := new(anypb.Any)
	if err := anypb.MarshalFrom(wantAny, detailMsg, proto.MarshalOptions{}); err != nil {
		t.Fatalf("marshal detail any: %v", err)
	}

	u := v2ServerURL(t)

	mkReq := func() *grpchantesting.Message {
		return &grpchantesting.Message{
			Code:         int32(codes.FailedPrecondition),
			ErrorDetails: []*anypb.Any{proto.Clone(wantAny).(*anypb.Any)},
		}
	}

	t.Run("protobuf", func(t *testing.T) {
		cc := &httpgrpc.ChannelV2{Transport: http.DefaultTransport, BaseURL: u}
		cli := grpchantesting.NewTestServiceClient(cc)
		_, err := cli.Unary(context.Background(), mkReq())
		assertUnaryErrorHasDetailV2(t, err, codes.FailedPrecondition, detailMsg)
	})

	t.Run("json", func(t *testing.T) {
		cc := newV2(t, u, httpgrpc.WithJSONEncodingV2(true))
		cli := grpchantesting.NewTestServiceClient(cc)
		_, err := cli.Unary(context.Background(), mkReq())
		assertUnaryErrorHasDetailV2(t, err, codes.FailedPrecondition, detailMsg)
	})
}

func assertUnaryErrorHasDetailV2(t *testing.T, err error, wantCode codes.Code, wantDetail proto.Message) {
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

func TestNewChannelV2Validation(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	t.Run("base URL is required", func(t *testing.T) {
		if _, err := httpgrpc.NewChannelV2(nil, http.DefaultTransport); err == nil {
			t.Fatal("expected an error for a nil base URL")
		}
	})
	t.Run("transport is required", func(t *testing.T) {
		if _, err := httpgrpc.NewChannelV2(u, nil); err == nil {
			t.Fatal("expected an error for a nil transport")
		}
	})
	t.Run("both supplied", func(t *testing.T) {
		ch, err := httpgrpc.NewChannelV2(u, http.DefaultTransport, httpgrpc.WithJSONEncodingV2(true))
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		if ch.BaseURL != u || ch.Transport == nil {
			t.Fatal("channel was not configured with what it was given")
		}
	})
}

// TestChannelV2MissingFields covers the checks that cannot be made by
// NewChannelV2: a ChannelV2 may also be built as a struct literal, and its fields
// are exported, so they can be missing or cleared after construction. Either way
// the RPC should report the problem, where it used to panic dereferencing a nil
// base URL.
func TestChannelV2MissingFields(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}

	checkFails := func(t *testing.T, ch *httpgrpc.ChannelV2) {
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
		checkFails(t, &httpgrpc.ChannelV2{Transport: http.DefaultTransport})
	})
	t.Run("struct literal without transport", func(t *testing.T) {
		checkFails(t, &httpgrpc.ChannelV2{BaseURL: u})
	})
	t.Run("field cleared after NewChannelV2", func(t *testing.T) {
		ch := newV2(t, u)
		ch.BaseURL = nil
		checkFails(t, ch)
	})
}

// TestStreamSurvivesGCV2 guards the cleanup that cancels an abandoned stream's
// context against cancelling one that is still being used.
//
// The cleanup is attached to the wrapper value handed back to the caller. If
// that wrapper's methods were promoted from an embedded interface, the wrapper
// would fall out of reach as soon as a call descended into the stream underneath
// it, since nothing on the stack refers to it any more. A garbage collection
// during a blocking Recv would then run the cleanup and cancel an RPC that was
// still in progress, surfacing as a spurious "context canceled" in place of
// whatever really ended the call. With collections forced, that reproduced on
// every attempt.
func TestStreamSurvivesGCV2(t *testing.T) {
	cc := httpgrpc.ChannelV2{Transport: http.DefaultTransport, BaseURL: v2ServerURL(t)}
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

// roundTripWatcherV2 reports when a round trip ends.
type roundTripWatcherV2 struct {
	inner http.RoundTripper
	ended chan error
}

func (t *roundTripWatcherV2) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(r)
	select {
	case t.ended <- err:
	default:
	}
	return resp, err
}

// TestAbandonedStreamIsCleanedUpV2 covers what the cleanup on the stream wrapper
// is for: a caller that stops using a stream without finishing or cancelling it
// should not leave the RPC running.
//
// The assertion is deliberately client-side only. Whether the *server* notices is
// a different question with a different answer: net/http does not watch a
// connection for a disconnect while a request body remains unread, so a handler
// finds out by way of a failed read rather than a cancelled context.
func TestAbandonedStreamIsCleanedUpV2(t *testing.T) {
	// A server that never answers, so the round trip stays pending until the
	// client itself gives up.
	block := make(chan struct{})
	defer close(block)
	u := serveV2(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	transport := &roundTripWatcherV2{inner: http.DefaultTransport, ended: make(chan error, 1)}
	cc := httpgrpc.ChannelV2{Transport: transport, BaseURL: u}

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
		"connection would leak")
}

// TestV1V2Compatibility is the reason the V2 implementations were written
// alongside the originals instead of replacing them: it pins down that either
// client interoperates with either server, so the two can eventually be collapsed
// without breaking deployments that upgrade one side at a time.
func TestV1V2Compatibility(t *testing.T) {
	t.Run("v1client_v2server", func(t *testing.T) {
		t.Run("proto", func(t *testing.T) {
			ch := newV1(t, v2ServerURL(t))
			grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
		})
		t.Run("json", func(t *testing.T) {
			ch := newV1(t, v2ServerURL(t), httpgrpc.WithJSONEncoding(true))
			grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
		})
	})
	t.Run("v2client_v1server", func(t *testing.T) {
		t.Run("proto", func(t *testing.T) {
			ch := newV2(t, v1ServerURL(t))
			grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
		})
		t.Run("json", func(t *testing.T) {
			ch := newV2(t, v1ServerURL(t), httpgrpc.WithJSONEncodingV2(true))
			grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
		})
	})
}

// TestChannelV2ResponseValidation covers the two ways a unary response can fail to
// match what was asked for. Neither can be produced by a real grpchan server, so
// the responses are hand-rolled.
func TestChannelV2ResponseValidation(t *testing.T) {
	respondWith := func(t *testing.T, contentType string) error {
		u := serveV2(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
		}))
		cli := grpchantesting.NewTestServiceClient(newV2(t, u))
		_, err := cli.Unary(context.Background(), &grpchantesting.Message{})
		return err
	}

	t.Run("unknown content-type", func(t *testing.T) {
		// Nothing can be decoded from a format the protocol does not know, so this
		// is not an internal failure so much as an unintelligible reply.
		err := respondWith(t, "application/x-bogus")
		if got := status.Code(err); got != codes.Unknown {
			t.Fatalf("expected Unknown, got %v (err=%v)", got, err)
		}
	})
	t.Run("codec mismatch", func(t *testing.T) {
		// A recognized format, but not the one the request used.
		err := respondWith(t, "application/json")
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("expected Internal, got %v (err=%v)", got, err)
		}
	})
}

// TestServerV2ErrorsUseProtocolFormat checks that once a protocol has been
// recognized from the content-type, failures are reported in that protocol's
// error format rather than as bare HTTP errors. For httpgrpc that means an
// X-GRPC-Status header carrying the real code, which a bare http.Error would not
// have: the client would otherwise be left inferring a code from the HTTP status.
func TestServerV2ErrorsUseProtocolFormat(t *testing.T) {
	base := v2ServerURL(t)

	send := func(t *testing.T, method, rpc, contentType string, hdrs map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, base.String()+"/grpchantesting.TestService/"+rpc, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", contentType)
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	// The status is reported as "<code>:<message>", so the code is the part before
	// the first colon. codes.Unimplemented is 12, codes.InvalidArgument is 3.
	wantCode := func(t *testing.T, resp *http.Response, want codes.Code) {
		t.Helper()
		got := resp.Header.Get("X-GRPC-Status")
		if got == "" {
			t.Fatalf("no X-GRPC-Status header: the error did not go through the protocol adapter (HTTP %s)", resp.Status)
		}
		if prefix := fmt.Sprintf("%d:", want); len(got) < len(prefix) || got[:len(prefix)] != prefix {
			t.Fatalf("X-GRPC-Status = %q, want code %v (%d)", got, want, want)
		}
	}

	t.Run("unary wrong method", func(t *testing.T) {
		resp := send(t, "GET", "Unary", "application/x-protobuf", nil)
		wantCode(t, resp, codes.Unimplemented)
		if allow := resp.Header.Get("Allow"); allow != "POST" {
			t.Errorf("Allow = %q, want POST", allow)
		}
	})
	t.Run("unary undecodable metadata", func(t *testing.T) {
		// A "-bin" header must be base64; one that is not makes the adapter's
		// processHeaders fail, which is the reachable way to fail that step.
		// (A malformed GRPC-Timeout is not: timeoutFromHeaders ignores one it
		// cannot parse rather than rejecting the request.)
		resp := send(t, "POST", "Unary", "application/x-protobuf",
			map[string]string{"X-Test-Bin": "!!! not base64 !!!"})
		wantCode(t, resp, codes.InvalidArgument)
	})
	t.Run("stream wrong method", func(t *testing.T) {
		resp := send(t, "GET", "ServerStream", "application/x-httpgrpc-proto+v1", nil)
		if ct := resp.Header.Get("Content-Type"); ct != "application/x-httpgrpc-proto+v1" {
			t.Errorf("Content-Type = %q, want the stream content-type: the adapter should "+
				"have labelled the response before writing the trailer frame", ct)
		}
	})

	// A content-type no protocol claims has no error format to use, so it stays a
	// bare HTTP error.
	t.Run("unknown content-type falls back to HTTP", func(t *testing.T) {
		resp := send(t, "POST", "Unary", "application/x-nonsense", nil)
		if resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Errorf("status = %v, want 415", resp.Status)
		}
		if got := resp.Header.Get("X-GRPC-Status"); got != "" {
			t.Errorf("X-GRPC-Status = %q, want none for an unrecognized content-type", got)
		}
	})
}

// serveV2 starts an http.Handler on a loopback port and returns its base URL.
func serveV2(t *testing.T, h http.Handler) *url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on socket: %v", err)
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

// v1ServerURL exposes the test service using the Server implementation.
func v1ServerURL(t *testing.T) *url.URL {
	t.Helper()
	reg := grpchan.HandlerMap{}
	grpchantesting.RegisterTestServiceServer(reg, &grpchantesting.TestServer{})
	var mux http.ServeMux
	httpgrpc.HandleServices(mux.HandleFunc, "/", reg, nil, nil)
	return serveV2(t, &mux)
}

// v2ServerURL exposes the test service using the ServerV2 implementation.
func v2ServerURL(t *testing.T) *url.URL {
	t.Helper()
	svr := httpgrpc.NewServerV2()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	return serveV2(t, svr)
}

// newV1 and newV2 build a channel of each kind, failing the test rather than
// making every case handle a constructor error it does not care about.
func newV1(t *testing.T, u *url.URL, opts ...httpgrpc.ChannelOption) *httpgrpc.Channel {
	t.Helper()
	ch, err := httpgrpc.NewChannel(u, http.DefaultTransport, opts...)
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	return ch
}

func newV2(t *testing.T, u *url.URL, opts ...httpgrpc.ChannelV2Option) *httpgrpc.ChannelV2 {
	t.Helper()
	ch, err := httpgrpc.NewChannelV2(u, http.DefaultTransport, opts...)
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	return ch
}
