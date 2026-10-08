package httpgrpc

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestBase64Reader(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString

	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "single chunk", input: enc([]byte("hello, world")), want: "hello, world"},
		{
			// Each chunk is padded, as a server that flushes each frame produces.
			name:  "padding mid-stream",
			input: enc([]byte("a")) + enc([]byte("bc")) + enc([]byte("def")),
			want:  "abcdef",
		},
		{name: "line breaks", input: "aGVs\r\nbG8s\nIHdv\rcmxk", want: "hello, world"},
		{name: "empty", input: "", want: ""},
		{
			name:  "longer than a batch",
			input: enc(bytes.Repeat([]byte("xyz"), 3*base64Batch)) + enc([]byte("!")),
			want:  strings.Repeat("xyz", 3*base64Batch) + "!",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Reading a byte at a time checks that a group split across reads of the
			// underlying body is put back together.
			r := newBase64Reader(iotest.OneByteReader(strings.NewReader(testCase.input)))
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("decoding failed: %v", err)
			}
			if string(got) != testCase.want {
				t.Errorf("decoded %q, want %q", got, testCase.want)
			}
		})
	}

	for name, input := range map[string]string{
		"truncated group":      "aGVsbG8",
		"invalid characters":   "aGV*bG8=",
		"data after padding":   "aG==bG8=x",
		"padding in mid-group": "a=Vs",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := io.ReadAll(newBase64Reader(strings.NewReader(input)))
			if !errors.Is(err, errMalformedText) {
				t.Errorf("expected a malformed text error, got %v", err)
			}
		})
	}
}

// TestBase64ReaderDoesNotWaitForMore checks that a frame is decoded as soon as
// it has arrived, even when its encoding ends without padding and the body has
// not ended. In a stream, what follows may not be sent until the peer replies to
// this frame, so waiting for more would deadlock.
func TestBase64ReaderDoesNotWaitForMore(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	frames, err := envelopeFrame(0, []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	// Five bytes of prefix and four of data: nine, so no padding.
	go func() {
		_, _ = pw.Write(encodeTextFrame(frames)[0])
	}()

	result := make(chan error, 1)
	go func() {
		_, data, err := readEnvelopeFrame(newBase64Reader(pr), 1024)
		if err == nil && string(data) != "abcd" {
			err = fmt.Errorf("read %q, want abcd", data)
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the frame was not decoded until more input arrived")
	}
}

func TestRespondInText(t *testing.T) {
	testCases := []struct {
		desc        string
		accept      []string
		requestText bool
		want        bool
	}{
		{desc: "no accept, binary request", want: false},
		{desc: "no accept, text request", requestText: true, want: true},
		{desc: "binary request asks for text", accept: []string{"application/grpc-web-text"}, want: true},
		{desc: "text request asks for binary", accept: []string{"application/grpc-web+proto"}, requestText: true, want: false},
		{desc: "both accepted mirrors binary", accept: []string{"application/grpc-web, application/grpc-web-text"}, want: false},
		{desc: "both accepted mirrors text", accept: []string{"application/grpc-web-text", "application/grpc-web"}, requestText: true, want: true},
		{desc: "unrelated accept mirrors", accept: []string{"*/*"}, requestText: true, want: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			h := http.Header{}
			for _, v := range testCase.accept {
				h.Add("Accept", v)
			}
			if got := respondInText(h, testCase.requestText); got != testCase.want {
				t.Errorf("respondInText(%v, %v) = %v, want %v", testCase.accept, testCase.requestText, got, testCase.want)
			}
		})
	}
}
