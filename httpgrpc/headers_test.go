package httpgrpc_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// TestTimeoutHeader covers GRPC-Timeout values that a server must not turn into
// a deadline that has already passed: negative ones, which the format doesn't
// allow, and ones too large to represent as a time.Duration.
func TestTimeoutHeader(t *testing.T) {
	type observed struct {
		hasDeadline bool
		remaining   time.Duration
		err         error
	}
	seen := make(chan observed, 1)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		deadline, ok := ctx.Deadline()
		seen <- observed{hasDeadline: ok, remaining: time.Until(deadline), err: ctx.Err()}
		return handler(ctx, req)
	}
	svr := httpgrpc.NewServer(httpgrpc.WithServerUnaryInterceptor(interceptor))
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	httpServer := httptest.NewServer(svr)
	defer httpServer.Close()

	body, err := proto.Marshal(&grpchantesting.Message{})
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}
	testCases := []struct {
		name         string
		timeout      string
		wantDeadline bool
	}{
		{name: "valid", timeout: "30S", wantDeadline: true},
		{name: "negative", timeout: "-5S"},
		{name: "overflow", timeout: "9223372036854775807H"},
		{name: "unknown unit", timeout: "5X"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/grpchantesting.TestService/Unary", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", httpgrpc.UnaryRpcContentType_V1)
			req.Header.Set("GRPC-Timeout", testCase.timeout)
			resp, err := httpServer.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()

			got := <-seen
			if got.err != nil {
				t.Fatalf("handler's context had already ended: %v", got.err)
			}
			if got.hasDeadline != testCase.wantDeadline {
				t.Fatalf("handler's context has deadline: got %v, want %v", got.hasDeadline, testCase.wantDeadline)
			}
			if got.hasDeadline && (got.remaining <= 0 || got.remaining > 30*time.Second) {
				t.Fatalf("handler's deadline is %v away, want within 30s", got.remaining)
			}
		})
	}
}

// TestMetadataCannotSetProtocolHeaders checks that metadata can't masquerade as
// the headers that HTTP or httpgrpc itself uses, in either direction. Sent as is,
// these would change how the peer reads the call: a timeout the caller never
// set, an error status on a call that succeeded, a body the transport tries to
// decompress, or a header that turns up as a trailer.
func TestMetadataCannotSetProtocolHeaders(t *testing.T) {
	svr := httpgrpc.NewServer()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	httpServer := httptest.NewServer(svr)
	defer httpServer.Close()

	u, err := url.Parse(httpServer.URL)
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	ch, err := httpgrpc.NewChannel(u, http.DefaultTransport)
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	cli := grpchantesting.NewTestServiceClient(ch)

	reserved := map[string][]byte{
		"grpc-timeout":       []byte("1n"),
		"x-grpc-status":      []byte("5:not found"),
		"x-grpc-trailer-foo": []byte("bar"),
		"content-encoding":   []byte("gzip"),
	}
	checkFiltered := func(t *testing.T, what string, got map[string]bool) {
		t.Helper()
		for k := range reserved {
			if got[k] {
				t.Errorf("%s includes %q", what, k)
			}
		}
		if !got["custom"] {
			t.Errorf("%s is missing ordinary metadata", what)
		}
	}

	t.Run("request", func(t *testing.T) {
		md := grpchantesting.MetadataNew(reserved)
		md.Set("custom", "value")
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		resp, err := cli.Unary(ctx, &grpchantesting.Message{})
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		// The server echoes the metadata it received.
		got := map[string]bool{}
		for k := range resp.Headers {
			got[k] = true
		}
		checkFiltered(t, "server's metadata", got)
	})
	t.Run("response", func(t *testing.T) {
		headers := map[string][]byte{"custom": []byte("value")}
		for k, v := range reserved {
			headers[k] = v
		}
		var hdr, tlr metadata.MD
		_, err := cli.Unary(context.Background(), &grpchantesting.Message{Headers: headers}, grpc.Header(&hdr), grpc.Trailer(&tlr))
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		got := map[string]bool{}
		for k := range hdr {
			got[k] = true
		}
		checkFiltered(t, "client's headers", got)
		if len(tlr) != 0 {
			t.Errorf("client's trailers: got %v, want none", tlr)
		}
	})
}
