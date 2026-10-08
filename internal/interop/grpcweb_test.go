package interop

import (
	"bytes"
	"context"
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

// TestGRPCWebClient runs this repo's client, speaking gRPC-Web, against a
// connect-go server, over HTTP/1.1.
func TestGRPCWebClient(t *testing.T) {
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
			opts := append([]httpgrpc.ChannelOption{httpgrpc.WithGRPCWeb()}, testCase.opts...)
			ch, err := httpgrpc.NewChannel(u, svr.Client().Transport, opts...)
			if err != nil {
				t.Fatalf("failed to create channel: %v", err)
			}
			grpchantesting.RunChannelTestCases(t, ch, false)
		})
	}
}

// claimHTTP2 makes every request look like HTTP/2 to the given handler.
// connect-go refuses a bidirectional stream over anything less, since it cannot
// be full duplex over HTTP/1.1; but this repo's client only ever streams half
// duplex, which HTTP/1.1 carries fine. The conformance suite uses the same trick
// so that connect-go can stand in for every kind of server.
func claimHTTP2(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ProtoMajor, r.ProtoMinor, r.Proto = 2, 0, "HTTP/2.0"
		h.ServeHTTP(w, r)
	})
}

// claimHTTP2Responses is claimHTTP2 for a client: connect-go's client refuses a
// bidirectional stream whose response came over HTTP/1.1. connectChannel makes
// every stream a bidirectional one, and the suite only streams half duplex.
func claimHTTP2Responses(rt http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := rt.RoundTrip(r)
		if resp != nil {
			resp.ProtoMajor, resp.ProtoMinor, resp.Proto = 2, 0, "HTTP/2.0"
		}
		return resp, err
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestGRPCWebServer runs a connect-go client, speaking gRPC-Web, against this
// repo's server, over HTTP/1.1. The client is adapted to grpc.ClientConnInterface
// so that it can run the shared channel test suite; see connectChannel.
func TestGRPCWebServer(t *testing.T) {
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
			opts := append([]connect.ClientOption{connect.WithGRPCWeb()}, testCase.opts...)
			httpClient := &http.Client{Transport: claimHTTP2Responses(httpSvr.Client().Transport)}
			ch := newConnectChannel(httpClient, httpSvr.URL, opts...)
			grpchantesting.RunChannelTestCases(t, ch, false)
		})
	}
}

// TestGRPCWebCompressionNegotiated checks that this repo's server compresses its
// response for a connect-go client that accepts gzip but did not compress its
// request: a browser-like client gets a compressed response without asking.
func TestGRPCWebCompressionNegotiated(t *testing.T) {
	svr := httpgrpc.NewServer()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	var respEncoding string
	httpSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svr.ServeHTTP(w, r)
		respEncoding = w.Header().Get("Grpc-Encoding")
	}))
	t.Cleanup(httpSvr.Close)

	client := connect.NewClient[message, message](
		httpSvr.Client(), httpSvr.URL+testServicePrefix+"Unary", connect.WithGRPCWeb())
	big := bytes.Repeat([]byte("compressible "), 1024)
	res, err := client.CallUnary(context.Background(), connect.NewRequest(&message{Payload: big}))
	if err != nil {
		t.Fatalf("unary call failed: %v", err)
	}
	if !bytes.Equal(res.Msg.Payload, big) {
		t.Error("payload did not survive the round trip")
	}
	if respEncoding != gzip.Name {
		t.Errorf("response grpc-encoding = %q, want %s", respEncoding, gzip.Name)
	}
}
