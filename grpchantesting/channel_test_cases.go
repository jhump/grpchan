package grpchantesting

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// RunChannelTestCases runs numerous test cases to exercise the behavior of the
// given channel. The server side of the channel needs to have a *TestServer (in
// this package) registered to provide the implementation of fsgrpc.TestService
// (proto in this package). If the channel does not support full-duplex
// communication, it must provide at least half-duplex support for bidirectional
// streams.
//
// The test cases will be defined as child tests by invoking t.Run on the given
// *testing.T.
//
// To run only some of the test cases, use a ChannelTester instead.
func RunChannelTestCases(t *testing.T, ch grpc.ClientConnInterface, supportsFullDuplex bool) {
	NewChannelTester(ch).Run(t, UnaryTests(), ClientStreamTests(), ServerStreamTests(),
		BidiStreamTests(supportsFullDuplex))
}

// ChannelTester runs the test cases of RunChannelTestCases, or a chosen subset of
// them, against a channel. It is for a channel or a server that cannot handle
// every kind of RPC.
type ChannelTester struct {
	cli TestServiceClient
}

// NewChannelTester returns a ChannelTester for the given channel, which has the
// same requirements as one given to RunChannelTestCases.
func NewChannelTester(ch grpc.ClientConnInterface) *ChannelTester {
	return &ChannelTester{cli: NewTestServiceClient(ch)}
}

// ChannelTesterOption chooses what a ChannelTester runs.
type ChannelTesterOption func(*testOptions)

type testOptions struct {
	unary, clientStream, serverStream, bidiStream bool
	fullDuplex                                    bool
	trailersOnlyFailures                          bool
}

// UnaryTests includes the test cases for unary RPCs.
func UnaryTests() ChannelTesterOption {
	return func(o *testOptions) { o.unary = true }
}

// ClientStreamTests includes the test cases for client-streaming RPCs.
func ClientStreamTests() ChannelTesterOption {
	return func(o *testOptions) { o.clientStream = true }
}

// ServerStreamTests includes the test cases for server-streaming RPCs.
func ServerStreamTests() ChannelTesterOption {
	return func(o *testOptions) { o.serverStream = true }
}

// BidiStreamTests includes the test cases for bidirectional-streaming RPCs. The
// half-duplex cases are always included; the full-duplex ones only if fullDuplex
// is true, since a channel needs to support it for them to pass.
func BidiStreamTests(fullDuplex bool) ChannelTesterOption {
	return func(o *testOptions) {
		o.bidiStream = true
		o.fullDuplex = fullDuplex
	}
}

// newTestOptions applies the given options, defaulting to every kind of RPC when
// none is chosen.
func newTestOptions(opts []ChannelTesterOption) testOptions {
	var o testOptions
	for _, opt := range opts {
		opt(&o)
	}
	if !o.unary && !o.clientStream && !o.serverStream && !o.bidiStream {
		o.unary, o.clientStream, o.serverStream, o.bidiStream = true, true, true, true
	}
	return o
}

// TrailersOnlyFailures relaxes the checks of calls that fail before the server
// sends a response message, for a server that reports such a failure as a
// "trailers-only" response. That merges the response's header metadata into its
// trailers, where no client can tell them apart, so the expected header metadata
// is accepted in either place.
func TrailersOnlyFailures() ChannelTesterOption {
	return func(o *testOptions) { o.trailersOnlyFailures = true }
}

// Run runs the chosen test cases as child tests of t. They run in the same order
// as in RunChannelTestCases, whatever order the options are given in. With no
// options, every kind of RPC is tested, with bidirectional streams tested only as
// half duplex.
func (c *ChannelTester) Run(t *testing.T, opts ...ChannelTesterOption) {
	o := newTestOptions(opts)
	if o.unary {
		t.Run("unary", func(t *testing.T) { testUnary(t, c.cli, o) })
	}
	if o.clientStream {
		t.Run("client-stream", func(t *testing.T) { testClientStream(t, c.cli, o) })
	}
	if o.serverStream {
		t.Run("server-stream", func(t *testing.T) { testServerStream(t, c.cli) })
	}
	if o.bidiStream {
		t.Run("half-duplex bidi-stream", func(t *testing.T) { testHalfDuplexBidiStream(t, c.cli) })
		if o.fullDuplex {
			t.Run("full-duplex bidi-stream", func(t *testing.T) { testFullDuplexBidiStream(t, c.cli) })
		}
	}
}

var (
	testPayload = []byte{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}

	testOutgoingMd = map[string][]byte{
		"foo":        []byte("bar"),
		"baz":        []byte("bedazzle"),
		"pickle-bin": testPayload,
	}

	testMdHeaders = map[string][]byte{
		"foo1":        []byte("bar4"),
		"baz2":        []byte("bedazzle5"),
		"pickle3-bin": testPayload,
	}

	testMdTrailers = map[string][]byte{
		"4foo4":        []byte("7bar7"),
		"5baz5":        []byte("8bedazzle8"),
		"6pickle6-bin": testPayload,
	}

	testErrorMessages = []proto.Message{
		&structpb.ListValue{
			Values: []*structpb.Value{
				{Kind: &structpb.Value_NumberValue{NumberValue: 123}},
				{Kind: &structpb.Value_StringValue{StringValue: "foo"}},
			},
		},
		&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"FOO": {Kind: &structpb.Value_NumberValue{NumberValue: 456}},
				"BAR": {Kind: &structpb.Value_StringValue{StringValue: "bar"}},
			},
		},
	}
	testErrorDetails []*anypb.Any
)

func init() {
	for _, msg := range testErrorMessages {
		var a anypb.Any
		if err := anypb.MarshalFrom(&a, msg, proto.MarshalOptions{}); err != nil {
			panic(err)
		} else {
			testErrorDetails = append(testErrorDetails, &a)
		}
	}
}

func MetadataNew(m map[string][]byte) metadata.MD {
	md := metadata.MD{}
	for k, val := range m {
		key := strings.ToLower(k)
		md[key] = append(md[key], string(val))
	}
	return md
}

func testUnary(t *testing.T, cli TestServiceClient, o testOptions) {
	ctx := metadata.NewOutgoingContext(context.Background(), MetadataNew(testOutgoingMd))
	reqPrototype := Message{
		Payload:  testPayload,
		Headers:  testMdHeaders,
		Trailers: testMdTrailers,
	}
	t.Run("success", func(t *testing.T) {
		var hdr, tlr metadata.MD
		req := proto.Clone(&reqPrototype).(*Message)
		rsp, err := cli.Unary(ctx, req, grpc.Header(&hdr), grpc.Trailer(&tlr))
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		if !bytes.Equal(testPayload, rsp.Payload) {
			t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, rsp.Payload)
		}
		checkRequestHeaders(t, testOutgoingMd, rsp.Headers)

		checkMetadata(t, testMdHeaders, hdr, "header")
		checkMetadata(t, testMdTrailers, tlr, "trailer")
	})

	t.Run("failure", func(t *testing.T) {
		var hdr, tlr metadata.MD
		req := proto.Clone(&reqPrototype).(*Message)
		req.Code = int32(codes.AlreadyExists)
		req.ErrorDetails = testErrorDetails
		_, err := cli.Unary(ctx, req, grpc.Header(&hdr), grpc.Trailer(&tlr))
		checkError(t, err, codes.AlreadyExists, testErrorMessages...)

		// A failed call still reports its metadata.
		checkFailureHeaders(t, o, hdr, tlr)
		checkMetadata(t, testMdTrailers, tlr, "trailer")
	})

	t.Run("timeout", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		req.DelayMillis = 500
		tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err := cli.Unary(tctx, req)
		checkError(t, err, codes.DeadlineExceeded)
	})

	t.Run("canceled", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		req.DelayMillis = 500
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, cancel)

		_, err := cli.Unary(cctx, req)
		checkError(t, err, codes.Canceled)
	})
}

func testClientStream(t *testing.T, cli TestServiceClient, o testOptions) {
	ctx := metadata.NewOutgoingContext(context.Background(), MetadataNew(testOutgoingMd))
	reqPrototype := Message{
		Payload:  testPayload,
		Headers:  testMdHeaders,
		Trailers: testMdTrailers,
	}

	t.Run("success", func(t *testing.T) {
		cs, err := cli.ClientStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}
		req := proto.Clone(&reqPrototype).(*Message)
		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message #1 failed: %v", err)
		}

		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message #2 failed: %v", err)
		}

		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message #3 failed: %v", err)
		}

		m, err := cs.CloseAndRecv()
		if err != nil {
			t.Fatalf("receiving message failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, m.Payload)
		}
		if m.Count != 3 {
			t.Fatalf("wrong count returned: expecting %d; got %d", 3, m.Count)
		}
		checkRequestHeaders(t, testOutgoingMd, m.Headers)

		checkResponseMetadata(t, cs, testMdHeaders, testMdTrailers)
	})

	t.Run("failure", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		cs, err := cli.ClientStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.Code = int32(codes.ResourceExhausted)
		req.ErrorDetails = testErrorDetails
		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		_, err = cs.CloseAndRecv()
		checkError(t, err, codes.ResourceExhausted, testErrorMessages...)

		hdr, err := cs.Header()
		if err != nil {
			t.Fatalf("failed to get header metadata: %v", err)
		}
		checkFailureHeaders(t, o, hdr, cs.Trailer())
		checkResponseTrailers(t, cs, testMdTrailers)
	})

	t.Run("timeout", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		cs, err := cli.ClientStream(tctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		_, err = cs.CloseAndRecv()
		checkError(t, err, codes.DeadlineExceeded)
	})

	t.Run("canceled", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, cancel)

		cs, err := cli.ClientStream(cctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		err = cs.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		_, err = cs.CloseAndRecv()
		checkError(t, err, codes.Canceled)
	})
}

func testServerStream(t *testing.T, cli TestServiceClient) {
	ctx := metadata.NewOutgoingContext(context.Background(), MetadataNew(testOutgoingMd))
	reqPrototype := Message{
		Payload:  testPayload,
		Count:    5,
		Headers:  testMdHeaders,
		Trailers: testMdTrailers,
	}

	t.Run("success", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		ss, err := cli.ServerStream(ctx, req)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		checkResponseHeaders(t, ss, testMdHeaders)

		for i := 0; i < 5; i++ {
			m, err := ss.Recv()
			if err != nil {
				t.Fatalf("receiving message #%d failed: %v", i+1, err)
			}
			if !bytes.Equal(testPayload, m.Payload) {
				t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, m.Payload)
			}
			checkRequestHeaders(t, testOutgoingMd, m.Headers)
		}
		_, err = ss.Recv()
		if err != io.EOF {
			t.Fatalf("expected EOF; got %v", err)
		}

		checkResponseTrailers(t, ss, testMdTrailers)
	})

	t.Run("failure", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		req.Count = 2
		req.Code = int32(codes.FailedPrecondition)
		req.ErrorDetails = testErrorDetails
		ss, err := cli.ServerStream(ctx, req)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		checkResponseHeaders(t, ss, testMdHeaders)

		for i := 0; i < 2; i++ {
			m, err := ss.Recv()
			if err != nil {
				t.Fatalf("receiving message #%d failed: %v", i+1, err)
			}
			if !bytes.Equal(testPayload, m.Payload) {
				t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, m.Payload)
			}
			checkRequestHeaders(t, testOutgoingMd, m.Headers)
		}
		_, err = ss.Recv()
		checkError(t, err, codes.FailedPrecondition, testErrorMessages...)

		checkResponseTrailers(t, ss, testMdTrailers)
	})

	t.Run("timeout", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		ss, err := cli.ServerStream(tctx, req)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		_, err = ss.Recv()
		checkError(t, err, codes.DeadlineExceeded)
	})

	t.Run("canceled", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, cancel)

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		ss, err := cli.ServerStream(cctx, req)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		_, err = ss.Recv()
		checkError(t, err, codes.Canceled)
	})
}

func testHalfDuplexBidiStream(t *testing.T, cli TestServiceClient) {
	ctx := metadata.NewOutgoingContext(context.Background(), MetadataNew(testOutgoingMd))
	reqPrototype := Message{
		Payload:  testPayload,
		Count:    -1, // enables half-duplex mode in server
		Headers:  testMdHeaders,
		Trailers: testMdTrailers,
	}

	t.Run("success", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		bidi, err := cli.BidiStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #1 failed: %v", err)
		}
		req.Headers = nil

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #2 failed: %v", err)
		}

		req.Trailers = testMdTrailers
		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #3 failed: %v", err)
		}
		req.Trailers = nil

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		checkResponseHeaders(t, bidi, testMdHeaders)

		m, err := bidi.Recv()
		if err != nil {
			t.Fatalf("receiving message #1 failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload in message #1: expecting %v; got %v", testPayload, m.Payload)
		}
		checkRequestHeaders(t, testOutgoingMd, m.Headers)

		m, err = bidi.Recv()
		if err != nil {
			t.Fatalf("receiving message #2 failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload in message #2: expecting %v; got %v", testPayload, m.Payload)
		}

		m, err = bidi.Recv()
		if err != nil {
			t.Fatalf("receiving message #3 failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload in message #3: expecting %v; got %v", testPayload, m.Payload)
		}

		_, err = bidi.Recv()
		if err != io.EOF {
			t.Fatalf("expected EOF; got %v", err)
		}

		checkResponseTrailers(t, bidi, testMdTrailers)
	})

	t.Run("failure", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		bidi, err := cli.BidiStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #1 failed: %v", err)
		}

		req.Code = int32(codes.DataLoss)
		req.ErrorDetails = testErrorDetails
		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #2 failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		checkResponseHeaders(t, bidi, testMdHeaders)

		m, err := bidi.Recv()
		if err != nil {
			t.Fatalf("receiving message failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, m.Payload)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.DataLoss, testErrorMessages...)

		checkResponseTrailers(t, bidi, testMdTrailers)
	})

	t.Run("timeout", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		bidi, err := cli.BidiStream(tctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.DeadlineExceeded)
	})

	t.Run("canceled", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, cancel)

		bidi, err := cli.BidiStream(cctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.Code = int32(codes.OK)
		req.DelayMillis = 500

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.Canceled)
	})
}

func testFullDuplexBidiStream(t *testing.T, cli TestServiceClient) {
	ctx := metadata.NewOutgoingContext(context.Background(), MetadataNew(testOutgoingMd))
	reqPrototype := Message{
		Payload:  testPayload,
		Headers:  testMdHeaders,
		Trailers: testMdTrailers,
	}

	t.Run("success", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		bidi, err := cli.BidiStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			err = bidi.Send(req)
			if err != nil {
				t.Fatalf("sending message #%d failed: %v", i+1, err)
			}

			if i == 0 {
				checkResponseHeaders(t, bidi, testMdHeaders)
			}

			m, err := bidi.Recv()
			if err != nil {
				t.Fatalf("receiving message #%d failed: %v", i+1, err)
			}
			if !bytes.Equal(testPayload, m.Payload) {
				t.Fatalf("wrong payload in message #%d: expecting %v; got %v", i+1, testPayload, m.Payload)
			}
			checkRequestHeaders(t, testOutgoingMd, m.Headers)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		if err != io.EOF {
			t.Fatalf("expected EOF; got %v", err)
		}

		checkResponseTrailers(t, bidi, testMdTrailers)
	})

	t.Run("failure", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		bidi, err := cli.BidiStream(ctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #1 failed: %v", err)
		}

		checkResponseHeaders(t, bidi, testMdHeaders)

		m, err := bidi.Recv()
		if err != nil {
			t.Fatalf("receiving message failed: %v", err)
		}
		if !bytes.Equal(testPayload, m.Payload) {
			t.Fatalf("wrong payload returned: expecting %v; got %v", testPayload, m.Payload)
		}

		req.Code = int32(codes.DataLoss)
		req.ErrorDetails = testErrorDetails
		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message #2 failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.DataLoss, testErrorMessages...)

		checkResponseTrailers(t, bidi, testMdTrailers)
	})

	t.Run("timeout", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		bidi, err := cli.BidiStream(tctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.DelayMillis = 500
		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.DeadlineExceeded)
	})

	t.Run("canceled", func(t *testing.T) {
		req := proto.Clone(&reqPrototype).(*Message)
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, cancel)

		bidi, err := cli.BidiStream(cctx)
		if err != nil {
			t.Fatalf("RPC failed: %v", err)
		}

		req.DelayMillis = 500
		err = bidi.Send(req)
		if err != nil {
			t.Fatalf("sending message failed: %v", err)
		}

		err = bidi.CloseSend()
		if err != nil {
			t.Fatalf("closing send-side of RPC failed: %v", err)
		}

		_, err = bidi.Recv()
		checkError(t, err, codes.Canceled)
	})
}

func checkRequestHeaders(t *testing.T, expected, actual map[string][]byte) {
	t.Helper()
	// we don't just do a strict equals check because the actual headers
	// echoed back could have extra headers that were added implicitly
	// by the GRPC-over-HTTP client (such as GRPC-Timeout, Content-Type, etc).
	for k, v := range expected {
		v2, ok := actual[k]
		if !ok || !bytes.Equal(v2, v) {
			t.Fatalf("wrong headers echoed back: expecting header %s to be %q, instead was %q", k, v, v2)
		}
	}
}

// checkFailureHeaders checks the header metadata of a call that failed before
// the server sent a response message; see TrailersOnlyFailures.
func checkFailureHeaders(t *testing.T, o testOptions, hdr, tlr metadata.MD) {
	t.Helper()
	if o.trailersOnlyFailures {
		hdr = metadata.Join(hdr, tlr)
	}
	checkMetadata(t, testMdHeaders, hdr, "header")
}

func checkResponseMetadata(t *testing.T, cs grpc.ClientStream, hdrs map[string][]byte, tlrs map[string][]byte) {
	t.Helper()
	checkResponseHeaders(t, cs, hdrs)
	checkResponseTrailers(t, cs, tlrs)
}

func checkResponseHeaders(t *testing.T, cs grpc.ClientStream, md map[string][]byte) {
	t.Helper()
	h, err := cs.Header()
	if err != nil {
		t.Fatalf("failed to get header metadata: %v", err)
	}
	checkMetadata(t, md, h, "header")
}

func checkResponseTrailers(t *testing.T, cs grpc.ClientStream, md map[string][]byte) {
	t.Helper()
	checkMetadata(t, md, cs.Trailer(), "trailer")
}

func checkMetadata(t *testing.T, expected map[string][]byte, actual metadata.MD, name string) {
	t.Helper()

	// we don't just do a strict equals check because the actual headers
	// echoed back could have extra headers that were added implicitly
	// by the GRPC-over-HTTP client (such as GRPC-Timeout, Content-Type, etc).
	for k, v := range expected {
		v2, ok := actual[k]
		if !ok || len(v2) != 1 || !bytes.Equal([]byte(v2[0]), v) {
			t.Fatalf("wrong %ss echoed back: expecting %s %s to be [%s], instead was %v", name, name, k, v, v2)
		}
	}
}

func checkError(t *testing.T, err error, expectedCode codes.Code, expectedDetails ...proto.Message) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("wrong type of error: %v", err)
	}
	if st.Code() != expectedCode {
		t.Fatalf("wrong response code: %v != %v", st.Code(), expectedCode)
	}
	actualDetails := st.Details()
	if len(actualDetails) != len(expectedDetails) {
		t.Fatalf("wrong number of error details: %v != %v", len(actualDetails), len(expectedDetails))
	}
	for i, msg := range actualDetails {
		if !proto.Equal(msg.(proto.Message), expectedDetails[i]) {
			t.Fatalf("wrong error detail message at index %d: %v != %v", i, msg, expectedDetails[i])
		}
	}
}
