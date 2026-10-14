package httpgrpc_test

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// TestPeerTLSInfo checks that a call over TLS reports the connection's TLS
// state in its peer, for unary as well as streaming RPCs.
func TestPeerTLSInfo(t *testing.T) {
	svr := httpgrpc.NewServer()
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	httpServer := httptest.NewTLSServer(svr)
	defer httpServer.Close()

	u, err := url.Parse(httpServer.URL)
	if err != nil {
		t.Fatalf("failed to parse base URL: %v", err)
	}
	ch, err := httpgrpc.NewChannel(u, httpServer.Client().Transport)
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	cli := grpchantesting.NewTestServiceClient(ch)

	checkPeer := func(t *testing.T, p *peer.Peer) {
		t.Helper()
		info, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok {
			t.Fatalf("peer's auth info: got %T, want credentials.TLSInfo", p.AuthInfo)
		}
		if len(info.State.PeerCertificates) == 0 {
			t.Fatal("peer's TLS state has no certificates")
		}
	}

	t.Run("unary", func(t *testing.T) {
		var p peer.Peer
		if _, err := cli.Unary(context.Background(), &grpchantesting.Message{}, grpc.Peer(&p)); err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		checkPeer(t, &p)
	})
	t.Run("stream", func(t *testing.T) {
		var p peer.Peer
		stream, err := cli.ServerStream(context.Background(), &grpchantesting.Message{}, grpc.Peer(&p))
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		if _, err := stream.Header(); err != nil {
			t.Fatalf("failed to receive headers: %v", err)
		}
		checkPeer(t, &p)
	})
}
