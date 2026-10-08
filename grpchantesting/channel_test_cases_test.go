package grpchantesting

import (
	"context"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// We test all of our channel test cases by running them against a normal
// *grpc.Server and *grpc.ClientConn, to make sure they are asserting the
// same behavior exhibited by the standard HTTP/2 channel implementation.
func TestChannelTestCases(t *testing.T) {
	s := grpc.NewServer()
	RegisterTestServiceServer(s, &TestServer{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on socket: %v", err)
	}
	go s.Serve(l)
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr := l.Addr().String()
	cc, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		t.Fatalf("failed to dial address: %s", addr)
	}
	defer cc.Close()

	RunChannelTestCases(t, cc, true)
}

func TestNewTestOptions(t *testing.T) {
	testCases := []struct {
		desc string
		opts []ChannelTesterOption
		want testOptions
	}{
		{
			desc: "none means every kind of RPC, half duplex",
			want: testOptions{unary: true, clientStream: true, serverStream: true, bidiStream: true},
		},
		{
			desc: "only the kinds chosen",
			opts: []ChannelTesterOption{UnaryTests(), ServerStreamTests()},
			want: testOptions{unary: true, serverStream: true},
		},
		{
			desc: "full duplex",
			opts: []ChannelTesterOption{BidiStreamTests(true)},
			want: testOptions{bidiStream: true, fullDuplex: true},
		},
		{
			desc: "trailers-only failures alone still tests everything",
			opts: []ChannelTesterOption{TrailersOnlyFailures()},
			want: testOptions{unary: true, clientStream: true, serverStream: true, bidiStream: true, trailersOnlyFailures: true},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			if got := newTestOptions(testCase.opts); got != testCase.want {
				t.Errorf("newTestOptions() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}
