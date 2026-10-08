package httpgrpc_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

func TestGRPCWeb(t *testing.T) {
	t.Run("proto", func(t *testing.T) {
		ch := newChannel(t, serverURL(t), httpgrpc.WithGRPCWeb())
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("json", func(t *testing.T) {
		ch := newChannel(t, serverURL(t), httpgrpc.WithGRPCWeb(), httpgrpc.WithJSONEncoding(true))
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("compressed", func(t *testing.T) {
		// The server compresses every response it can, so that the suite runs over
		// compressed messages and not only small ones that fall under the minimum.
		ch := newChannel(t, grpcWebServerURL(t, httpgrpc.WithCompressMinBytes(0)), httpgrpc.WithGRPCWeb(),
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("text", func(t *testing.T) {
		ch := newChannel(t, serverURL(t), httpgrpc.WithGRPCWebText())
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("text compressed", func(t *testing.T) {
		ch := newChannel(t, grpcWebServerURL(t, httpgrpc.WithCompressMinBytes(0)), httpgrpc.WithGRPCWebText(),
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("last protocol option wins", func(t *testing.T) {
		// WithJSONEncoding is not a protocol, so it leaves gRPC-Web in place.
		var seen string
		transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			seen = r.Header.Get("Content-Type")
			return http.DefaultTransport.RoundTrip(r)
		})
		ch, err := httpgrpc.NewChannel(serverURL(t), transport,
			httpgrpc.WithGRPCWeb(), httpgrpc.WithJSONEncoding(true), httpgrpc.WithGRPCWeb())
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		if _, err := grpchantesting.NewTestServiceClient(ch).Unary(context.Background(), &grpchantesting.Message{}); err != nil {
			t.Fatalf("unary call failed: %v", err)
		}
		if seen != "application/grpc-web+json" {
			t.Errorf("request content-type = %q, want application/grpc-web+json", seen)
		}
	})
}

// TestGRPCWebWireFormat checks the server's framing against the protocol itself
// rather than against this package's own client, which could share any
// misunderstanding of it.
func TestGRPCWebWireFormat(t *testing.T) {
	base := serverURL(t)
	req := &grpchantesting.Message{Payload: []byte("hello")}
	reqData, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	resp := postGRPCWeb(t, base, "/grpchantesting.TestService/Unary", nil, frame(0, reqData))
	if got := resp.Header.Get("Content-Type"); got != "application/grpc-web+proto" {
		t.Errorf("content-type = %q, want application/grpc-web+proto", got)
	}
	frames := readFrames(t, resp.Body)
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want a message then trailers", len(frames))
	}
	if frames[0].flags != 0 {
		t.Errorf("message frame flags = %#x, want 0", frames[0].flags)
	}
	var reply grpchantesting.Message
	if err := proto.Unmarshal(frames[0].data, &reply); err != nil {
		t.Fatalf("message frame does not hold a message: %v", err)
	}
	if !bytes.Equal(reply.Payload, req.Payload) {
		t.Errorf("payload = %q, want %q", reply.Payload, req.Payload)
	}
	if frames[1].flags != 0x80 {
		t.Errorf("final frame flags = %#x, want 0x80", frames[1].flags)
	}
	if got := trailerHeaders(t, frames[1].data).Get("Grpc-Status"); got != "0" {
		t.Errorf("grpc-status = %q, want 0", got)
	}
}

// TestGRPCWebTextWireFormat checks the server's handling of the text form against
// the protocol itself, including what this package's own client never sends: a
// request body in several separately padded chunks, with line breaks.
func TestGRPCWebTextWireFormat(t *testing.T) {
	base := serverURL(t)
	req := &grpchantesting.Message{Payload: []byte("hello")}
	reqData, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	reqFrame := frame(0, reqData)
	// The frame header and the message are encoded separately, each with its own
	// padding, and a line break between them.
	body := base64.StdEncoding.EncodeToString(reqFrame[:5]) + "\r\n" +
		base64.StdEncoding.EncodeToString(reqFrame[5:])

	checkReply := func(t *testing.T, frames []grpcWebFrame) {
		t.Helper()
		if len(frames) != 2 {
			t.Fatalf("got %d frames, want a message then trailers", len(frames))
		}
		var reply grpchantesting.Message
		if err := proto.Unmarshal(frames[0].data, &reply); err != nil {
			t.Fatalf("message frame does not hold a message: %v", err)
		}
		if !bytes.Equal(reply.Payload, req.Payload) {
			t.Errorf("payload = %q, want %q", reply.Payload, req.Payload)
		}
		if got := trailerHeaders(t, frames[1].data).Get("Grpc-Status"); got != "0" {
			t.Errorf("grpc-status = %q, want 0", got)
		}
	}

	t.Run("text request", func(t *testing.T) {
		resp := postGRPCWebAs(t, base, "/grpchantesting.TestService/Unary", "application/grpc-web-text", nil, []byte(body))
		if got := resp.Header.Get("Content-Type"); got != "application/grpc-web-text+proto" {
			t.Errorf("content-type = %q, want application/grpc-web-text+proto", got)
		}
		checkReply(t, readTextFrames(t, resp.Body))
	})
	t.Run("binary request asking for text", func(t *testing.T) {
		header := http.Header{"Accept": []string{"application/grpc-web-text"}}
		resp := postGRPCWebAs(t, base, "/grpchantesting.TestService/Unary", "application/grpc-web+proto", header, reqFrame)
		if got := resp.Header.Get("Content-Type"); got != "application/grpc-web-text+proto" {
			t.Errorf("content-type = %q, want application/grpc-web-text+proto", got)
		}
		checkReply(t, readTextFrames(t, resp.Body))
	})
	t.Run("malformed text", func(t *testing.T) {
		resp := postGRPCWebAs(t, base, "/grpchantesting.TestService/Unary", "application/grpc-web-text", nil, []byte("not*base64"))
		frames := readTextFrames(t, resp.Body)
		if len(frames) == 0 {
			t.Fatal("response has no frames")
		}
		if got := trailerHeaders(t, frames[len(frames)-1].data).Get("Grpc-Status"); got != "3" {
			t.Errorf("grpc-status = %q, want 3 (InvalidArgument)", got)
		}
	})
}

// readTextFrames decodes a text-form body, whose padding may fall between
// chunks, by decoding a group of four characters at a time.
func readTextFrames(t *testing.T, r io.Reader) []grpcWebFrame {
	t.Helper()
	text, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	var decoded []byte
	for i := 0; i+4 <= len(text); i += 4 {
		group, err := base64.StdEncoding.DecodeString(string(text[i : i+4]))
		if err != nil {
			t.Fatalf("response is not base64: %v", err)
		}
		decoded = append(decoded, group...)
	}
	return parseFrames(decoded)
}

// TestGRPCWebCompression checks that messages are really compressed on the wire,
// which the suite cannot: it would pass just the same if nothing were.
func TestGRPCWebCompression(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("compressible "), 1024)

	newRecordingClient := func(t *testing.T) (grpchantesting.TestServiceClient, *wireRecorder) {
		t.Helper()
		rec := &wireRecorder{}
		ch, err := httpgrpc.NewChannel(grpcWebServerURL(t), rec, httpgrpc.WithGRPCWeb(),
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}
		return grpchantesting.NewTestServiceClient(ch), rec
	}

	t.Run("unary", func(t *testing.T) {
		cli, rec := newRecordingClient(t)
		reply, err := cli.Unary(ctx, &grpchantesting.Message{Payload: payload})
		if err != nil {
			t.Fatalf("unary call failed: %v", err)
		}
		if !bytes.Equal(reply.Payload, payload) {
			t.Error("payload did not survive the round trip")
		}
		rec.checkCompressed(t)
	})
	t.Run("streams", func(t *testing.T) {
		cli, rec := newRecordingClient(t)
		str, err := cli.BidiStream(ctx)
		if err != nil {
			t.Fatalf("failed to start stream: %v", err)
		}
		if err := str.Send(&grpchantesting.Message{Payload: payload, Count: 1}); err != nil {
			t.Fatalf("send failed: %v", err)
		}
		if err := str.CloseSend(); err != nil {
			t.Fatalf("close send failed: %v", err)
		}
		reply, err := str.Recv()
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		if !bytes.Equal(reply.Payload, payload) {
			t.Error("payload did not survive the round trip")
		}
		for {
			if _, err := str.Recv(); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("stream failed: %v", err)
			}
		}
		rec.checkCompressed(t)
	})
	t.Run("unsupported request encoding", func(t *testing.T) {
		header := http.Header{"Grpc-Encoding": []string{"no-such-compressor"}}
		resp := postGRPCWeb(t, serverURL(t), "/grpchantesting.TestService/Unary", header, frame(1, []byte("x")))
		frames := readFrames(t, resp.Body)
		if len(frames) == 0 {
			t.Fatal("response has no frames")
		}
		if got := trailerHeaders(t, frames[len(frames)-1].data).Get("Grpc-Status"); got != "12" {
			t.Errorf("grpc-status = %q, want 12 (Unimplemented)", got)
		}
		// The client is told what it could have used instead.
		if got := resp.Header.Get("Grpc-Accept-Encoding"); !strings.Contains(got, gzip.Name) {
			t.Errorf("grpc-accept-encoding = %q, want it to list %s", got, gzip.Name)
		}
	})
}

// TestGRPCWebCompressedTrailers checks that the client accepts a trailers frame
// that is itself compressed, which the protocol allows and some servers send.
func TestGRPCWebCompressedTrailers(t *testing.T) {
	gzipped := func(t *testing.T, data []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		w, err := encoding.GetCompressor(gzip.Name).Compress(&buf)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(data)
		_ = w.Close()
		return buf.Bytes()
	}
	replyData, err := proto.Marshal(&grpchantesting.Message{Payload: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	trailers := gzipped(t, []byte("grpc-status: 0\r\nreply-trailer: t\r\n"))
	u := serveHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		w.Header().Set("Grpc-Encoding", gzip.Name)
		_, _ = w.Write(frame(0x01, gzipped(t, replyData)))
		_, _ = w.Write(frame(0x81, trailers))
	}))

	var tlr metadata.MD
	cli := grpchantesting.NewTestServiceClient(newChannel(t, u, httpgrpc.WithGRPCWeb()))
	reply, err := cli.Unary(context.Background(), &grpchantesting.Message{}, grpc.Trailer(&tlr))
	if err != nil {
		t.Fatalf("unary call failed: %v", err)
	}
	if string(reply.Payload) != "hi" {
		t.Errorf("payload = %q, want hi", reply.Payload)
	}
	if got := tlr.Get("reply-trailer"); len(got) != 1 || got[0] != "t" {
		t.Errorf("reply-trailer = %v, want [t]", got)
	}
}

func TestGRPCWebMessageSizeLimits(t *testing.T) {
	ctx := context.Background()
	msg := &grpchantesting.Message{Payload: make([]byte, 1024)}

	t.Run("client recv limit enforced", func(t *testing.T) {
		cli := grpchantesting.NewTestServiceClient(newChannel(t, serverURL(t), httpgrpc.WithGRPCWeb()))
		_, err := cli.Unary(ctx, msg, grpc.MaxCallRecvMsgSize(10))
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("server recv limit enforced", func(t *testing.T) {
		u := grpcWebServerURL(t, httpgrpc.WithMaxRecvMsgSize(10))
		cli := grpchantesting.NewTestServiceClient(newChannel(t, u, httpgrpc.WithGRPCWeb()))
		_, err := cli.Unary(ctx, msg)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("message exactly at the limit is accepted", func(t *testing.T) {
		// The frame header makes the body a little larger than the message, so a
		// limit applied to the length the body declares, rather than to the message,
		// would refuse this. The request is written by hand so that it does declare
		// one: this package's client streams the body without a Content-Length.
		reqData, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		u := grpcWebServerURL(t, httpgrpc.WithMaxRecvMsgSize(len(reqData)))
		resp := postGRPCWeb(t, u, "/grpchantesting.TestService/Unary", nil, frame(0, reqData))
		frames := readFrames(t, resp.Body)
		if len(frames) == 0 {
			t.Fatal("response has no frames")
		}
		if got := trailerHeaders(t, frames[len(frames)-1].data).Get("Grpc-Status"); got != "0" {
			t.Errorf("grpc-status = %q, want 0: a message of exactly the limit was refused", got)
		}
	})
	t.Run("compressed request expanding past the limit is refused", func(t *testing.T) {
		// A small compressed message that decompresses to far more than the limit.
		reqData, err := proto.Marshal(&grpchantesting.Message{Payload: make([]byte, 1<<20)})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		w, err := encoding.GetCompressor(gzip.Name).Compress(&buf)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(reqData)
		_ = w.Close()

		u := grpcWebServerURL(t, httpgrpc.WithMaxRecvMsgSize(1024))
		header := http.Header{"Grpc-Encoding": []string{gzip.Name}}
		resp := postGRPCWeb(t, u, "/grpchantesting.TestService/Unary", header, frame(1, buf.Bytes()))
		frames := readFrames(t, resp.Body)
		if len(frames) == 0 {
			t.Fatal("response has no frames")
		}
		if got := trailerHeaders(t, frames[len(frames)-1].data).Get("Grpc-Status"); got != "8" {
			t.Errorf("grpc-status = %q, want 8 (ResourceExhausted)", got)
		}
	})
}

// grpcWebServerURL serves the test service with the given options.
func grpcWebServerURL(t *testing.T, opts ...httpgrpc.ServerOption) *url.URL {
	t.Helper()
	svr := httpgrpc.NewServer(opts...)
	grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
	return serveHandler(t, svr)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// wireRecorder is a transport that keeps the frames of every request and
// response it carries, so a test can see what was actually sent.
type wireRecorder struct {
	mu        sync.Mutex
	reqFrames []grpcWebFrame
	resFrames []grpcWebFrame
	reqHeader http.Header
	resHeader http.Header
}

func (w *wireRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	respBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	w.mu.Lock()
	defer w.mu.Unlock()
	w.reqHeader, w.resHeader = r.Header, resp.Header
	w.reqFrames = append(w.reqFrames, parseFrames(body)...)
	w.resFrames = append(w.resFrames, parseFrames(respBody)...)
	return resp, nil
}

// checkCompressed checks that the recorded request and response each carried a
// compressed message frame, and named the compressor in their headers.
func (w *wireRecorder) checkCompressed(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, side := range []struct {
		name   string
		header http.Header
		frames []grpcWebFrame
	}{
		{"request", w.reqHeader, w.reqFrames},
		{"response", w.resHeader, w.resFrames},
	} {
		if got := side.header.Get("Grpc-Encoding"); got != gzip.Name {
			t.Errorf("%s grpc-encoding = %q, want %s", side.name, got, gzip.Name)
		}
		compressed := false
		for _, f := range side.frames {
			if f.flags&0x80 == 0 && f.flags&0x01 != 0 {
				compressed = true
			}
		}
		if !compressed {
			t.Errorf("%s has no compressed message frame", side.name)
		}
	}
}

type grpcWebFrame struct {
	flags byte
	data  []byte
}

func frame(flags byte, data []byte) []byte {
	out := make([]byte, 5, 5+len(data))
	out[0] = flags
	binary.BigEndian.PutUint32(out[1:], uint32(len(data)))
	return append(out, data...)
}

func parseFrames(b []byte) []grpcWebFrame {
	var frames []grpcWebFrame
	for len(b) >= 5 {
		size := int(binary.BigEndian.Uint32(b[1:5]))
		if len(b) < 5+size {
			break
		}
		frames = append(frames, grpcWebFrame{flags: b[0], data: b[5 : 5+size]})
		b = b[5+size:]
	}
	return frames
}

func readFrames(t *testing.T, r io.Reader) []grpcWebFrame {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return parseFrames(b)
}

func trailerHeaders(t *testing.T, data []byte) http.Header {
	t.Helper()
	r := textproto.NewReader(bufio.NewReader(io.MultiReader(bytes.NewReader(data), strings.NewReader("\r\n"))))
	h, err := r.ReadMIMEHeader()
	if err != nil {
		t.Fatalf("malformed trailers frame: %v", err)
	}
	return http.Header(h)
}

// postGRPCWeb sends a gRPC-Web request with the given body, written by hand.
func postGRPCWeb(t *testing.T, base *url.URL, path string, header http.Header, body []byte) *http.Response {
	t.Helper()
	return postGRPCWebAs(t, base, path, "application/grpc-web+proto", header, body)
}

// postGRPCWebAs is postGRPCWeb with the given content-type.
func postGRPCWebAs(t *testing.T, base *url.URL, path, contentType string, header http.Header, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base.JoinPath(path).String(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", resp.StatusCode)
	}
	return resp
}
