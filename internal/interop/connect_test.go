package interop

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

// TestConnectClient runs this repo's client, speaking the Connect protocol,
// against a connect-go server, over HTTP/1.1.
func TestConnectClient(t *testing.T) {
	svr := httptest.NewServer(claimHTTP2(newConnectTestService()))
	t.Cleanup(svr.Close)
	u, err := url.Parse(svr.URL)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name string
		opts []httpgrpc.ChannelOption
	}{
		{name: "proto"},
		{name: "json", opts: []httpgrpc.ChannelOption{httpgrpc.WithJSONEncoding(true)}},
		{name: "compressed", opts: []httpgrpc.ChannelOption{
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			opts := append([]httpgrpc.ChannelOption{httpgrpc.WithConnectRPC()}, testCase.opts...)
			ch, err := httpgrpc.NewChannel(u, svr.Client().Transport, opts...)
			if err != nil {
				t.Fatalf("failed to create channel: %v", err)
			}
			grpchantesting.RunChannelTestCases(t, ch, false)
		})
	}
}

// TestConnectServer runs a connect-go client, speaking the Connect protocol,
// against this repo's server, over HTTP/1.1; see TestGRPCWebServer.
func TestConnectServer(t *testing.T) {
	svr := httpgrpc.NewServer()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	httpSvr := httptest.NewServer(svr)
	t.Cleanup(httpSvr.Close)

	testCases := []struct {
		name string
		opts []connect.ClientOption
	}{
		{name: "proto"},
		{name: "json", opts: []connect.ClientOption{connect.WithProtoJSON()}},
		{name: "compressed", opts: []connect.ClientOption{connect.WithSendGzip()}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			httpClient := &http.Client{Transport: claimHTTP2Responses(httpSvr.Client().Transport)}
			ch := newConnectChannel(httpClient, httpSvr.URL, testCase.opts...)
			grpchantesting.RunChannelTestCases(t, ch, false)
		})
	}
}
