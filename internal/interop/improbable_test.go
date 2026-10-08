package interop

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

// TestGRPCWebClientImprobable runs this repo's client against improbable-eng's
// gRPC-Web server, a wrapper around a grpc-go server, over HTTP/1.1. Unlike
// connect-go, it implements the text form of the protocol as well as the binary
// one, so this is what tests the client's text form against another
// implementation.
func TestGRPCWebClientImprobable(t *testing.T) {
	grpcSvr := grpc.NewServer()
	grpchantesting.RegisterTestServiceServer(grpcSvr, &grpchantesting.TestServer{})
	svr := httptest.NewServer(grpcweb.WrapServer(grpcSvr,
		grpcweb.WithOriginFunc(func(string) bool { return true })))
	t.Cleanup(svr.Close)
	u, err := url.Parse(svr.URL)
	if err != nil {
		t.Fatal(err)
	}

	// When a call fails before sending a response message, improbable-eng merges
	// the response headers into a trailers-only response, where no client can tell
	// them from trailers, so every case is run with TrailersOnlyFailures.
	//
	// The text form is tested only with unary and server-streaming RPCs. In it,
	// improbable-eng decodes a request body as a single base64 entity, so it
	// rejects one whose frames are padded separately, as this repo's client pads
	// them. That only matters when a request has more than one message, which
	// browsers, the clients the text form exists for, never send.
	textTests := []grpchantesting.ChannelTesterOption{
		grpchantesting.UnaryTests(), grpchantesting.ServerStreamTests(),
	}
	testCases := []struct {
		name  string
		opts  []httpgrpc.ChannelOption
		tests []grpchantesting.ChannelTesterOption
	}{
		{name: "binary", opts: []httpgrpc.ChannelOption{httpgrpc.WithGRPCWeb()}},
		{name: "text", opts: []httpgrpc.ChannelOption{httpgrpc.WithGRPCWebText()}, tests: textTests},
		{name: "text compressed", opts: []httpgrpc.ChannelOption{
			httpgrpc.WithGRPCWebText(),
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		}, tests: textTests},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ch, err := httpgrpc.NewChannel(u, svr.Client().Transport, testCase.opts...)
			if err != nil {
				t.Fatalf("failed to create channel: %v", err)
			}
			grpchantesting.NewChannelTester(ch).Run(t,
				append(testCase.tests, grpchantesting.TrailersOnlyFailures())...)
		})
	}
}
