package httpgrpc_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

func TestConnect(t *testing.T) {
	t.Run("proto", func(t *testing.T) {
		ch := newChannel(t, serverURL(t), httpgrpc.WithConnectRPC())
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("json", func(t *testing.T) {
		ch := newChannel(t, serverURL(t), httpgrpc.WithConnectRPC(), httpgrpc.WithJSONEncoding(true))
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
	t.Run("compressed", func(t *testing.T) {
		ch := newChannel(t, grpcWebServerURL(t, httpgrpc.WithCompressMinBytes(0)), httpgrpc.WithConnectRPC(),
			httpgrpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
		grpchantesting.RunChannelTestCases(t, ch, supportsFullDuplex)
	})
}

// TestConnectWireFormat checks the server's handling of the Connect protocol
// against the protocol itself rather than against this package's own client.
func TestConnectWireFormat(t *testing.T) {
	base := serverURL(t)
	connectHeader := http.Header{"Connect-Protocol-Version": []string{"1"}}

	t.Run("unary", func(t *testing.T) {
		req := &grpchantesting.Message{
			Payload:  []byte("hello"),
			Trailers: map[string][]byte{"reply-trailer": []byte("t")},
		}
		resp, body := postRaw(t, base, "/grpchantesting.TestService/Unary", "application/proto", connectHeader, marshal(t, req))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HTTP status = %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/proto" {
			t.Errorf("content-type = %q, want application/proto", got)
		}
		// The body is the message, with no framing.
		var reply grpchantesting.Message
		if err := proto.Unmarshal(body, &reply); err != nil {
			t.Fatalf("body is not a message: %v", err)
		}
		if !bytes.Equal(reply.Payload, req.Payload) {
			t.Errorf("payload = %q, want %q", reply.Payload, req.Payload)
		}
		if got := resp.Header.Get("Trailer-Reply-Trailer"); got != "t" {
			t.Errorf("trailer-reply-trailer = %q, want t", got)
		}
	})
	t.Run("unary error", func(t *testing.T) {
		detail, err := anypb.New(structpb.NewStringValue("detail"))
		if err != nil {
			t.Fatal(err)
		}
		req := &grpchantesting.Message{Code: int32(codes.NotFound), ErrorDetails: []*anypb.Any{detail}}
		resp, body := postRaw(t, base, "/grpchantesting.TestService/Unary", "application/proto", connectHeader, marshal(t, req))
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("HTTP status = %d, want 404", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type = %q, want application/json", got)
		}
		var wire struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Details []struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"details"`
		}
		if err := json.Unmarshal(body, &wire); err != nil {
			t.Fatalf("error body is not JSON: %v (%q)", err, body)
		}
		if wire.Code != "not_found" || wire.Message != "error" {
			t.Errorf("error = %q %q, want not_found error", wire.Code, wire.Message)
		}
		if len(wire.Details) != 1 || wire.Details[0].Type != "google.protobuf.Value" {
			t.Fatalf("details = %+v, want one google.protobuf.Value", wire.Details)
		}
		value, err := base64.RawStdEncoding.DecodeString(wire.Details[0].Value)
		if err != nil || !bytes.Equal(value, detail.Value) {
			t.Errorf("detail value = %q (err=%v), want the detail sent, unpadded", wire.Details[0].Value, err)
		}
	})
	t.Run("unsupported protocol version", func(t *testing.T) {
		header := http.Header{"Connect-Protocol-Version": []string{"2"}}
		resp, body := postRaw(t, base, "/grpchantesting.TestService/Unary", "application/proto", header,
			marshal(t, &grpchantesting.Message{}))
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("HTTP status = %d, want 400", resp.StatusCode)
		}
		var wire struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(body, &wire); err != nil || wire.Code != "invalid_argument" {
			t.Errorf("error body = %q (err=%v), want an invalid_argument error", body, err)
		}
	})
	t.Run("json without the protocol header is httpgrpc", func(t *testing.T) {
		resp, _ := postRaw(t, base, "/grpchantesting.TestService/Unary", "application/json", nil, []byte(`{"code":5}`))
		// httpgrpc reports a unary error with its own header, not a Connect body.
		if got := resp.Header.Get("X-GRPC-Status"); got == "" {
			t.Errorf("response has no X-GRPC-Status header, so it was not served as httpgrpc")
		}
	})
	t.Run("server stream", func(t *testing.T) {
		req := &grpchantesting.Message{
			Payload:  []byte("hello"),
			Count:    2,
			Trailers: map[string][]byte{"reply-trailer": []byte("t")},
		}
		resp, body := postRaw(t, base, "/grpchantesting.TestService/ServerStream", "application/connect+proto",
			connectHeader, frame(0, marshal(t, req)))
		if got := resp.Header.Get("Content-Type"); got != "application/connect+proto" {
			t.Errorf("content-type = %q, want application/connect+proto", got)
		}
		frames := parseFrames(body)
		if len(frames) != 3 {
			t.Fatalf("got %d frames, want two messages and an end-of-stream message", len(frames))
		}
		if frames[2].flags != 0x02 {
			t.Fatalf("final frame flags = %#x, want 0x02", frames[2].flags)
		}
		var end struct {
			Error    json.RawMessage     `json:"error"`
			Metadata map[string][]string `json:"metadata"`
		}
		if err := json.Unmarshal(frames[2].data, &end); err != nil {
			t.Fatalf("end-of-stream message is not JSON: %v (%q)", err, frames[2].data)
		}
		if end.Error != nil {
			t.Errorf("end-of-stream message reports an error: %s", end.Error)
		}
		if got := http.Header(end.Metadata).Get("Reply-Trailer"); got != "t" {
			t.Errorf("end-of-stream metadata = %v, want reply-trailer: t", end.Metadata)
		}
	})
	t.Run("browser gets a compressed response", func(t *testing.T) {
		// A browser asks for compressed responses with Accept-Encoding, which it
		// sends on every request, and decompresses them itself.
		payload := bytes.Repeat([]byte("compressible "), 1024)
		header := http.Header{
			"Connect-Protocol-Version": []string{"1"},
			"Accept-Encoding":          []string{"gzip, deflate, br, zstd"},
		}
		resp, body := postRaw(t, base, "/grpchantesting.TestService/Unary", "application/proto", header,
			marshal(t, &grpchantesting.Message{Payload: payload}))
		if got := resp.Header.Get("Content-Encoding"); got != gzip.Name {
			t.Fatalf("content-encoding = %q, want %s", got, gzip.Name)
		}
		r, err := encoding.GetCompressor(gzip.Name).Decompress(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		decompressed, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		var reply grpchantesting.Message
		if err := proto.Unmarshal(decompressed, &reply); err != nil {
			t.Fatalf("decompressed body is not a message: %v", err)
		}
		if !bytes.Equal(reply.Payload, payload) {
			t.Error("payload did not survive the round trip")
		}
	})
}

func marshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
