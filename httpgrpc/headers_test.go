package httpgrpc_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
	"google.golang.org/grpc"
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
