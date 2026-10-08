package httpgrpc_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/mem"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

// TestConnectGet checks the server's handling of the GET requests the Connect
// protocol allows for unary calls to methods free of side effects. The test
// service is served twice: as generated, where no method is declared free of side
// effects, and as sideEffectFreeService, where Unary is.
func TestConnectGet(t *testing.T) {
	newServer := func(t *testing.T, opts ...httpgrpc.ServerOption) *url.URL {
		t.Helper()
		svr := httpgrpc.NewServer(opts...)
		grpchantesting.RegisterTestServiceServer(svr, &grpchantesting.TestServer{})
		svr.RegisterService(sideEffectFreeService(t), &grpchantesting.TestServer{})
		return serveHandler(t, svr)
	}
	base := newServer(t)
	sideEffectFreeUnary := "/" + sideEffectFreeServiceName + "/Unary"
	req := &grpchantesting.Message{Payload: []byte("hello")}

	checkReply := func(t *testing.T, resp *http.Response, body []byte, codecName string) {
		t.Helper()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HTTP status = %d, want 200 (body %q)", resp.StatusCode, body)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/"+codecName {
			t.Errorf("content-type = %q, want application/%s", got, codecName)
		}
		var reply grpchantesting.Message
		if err := encoding.GetCodecV2(codecName).Unmarshal(mem.BufferSlice{mem.SliceBuffer(body)}, &reply); err != nil {
			t.Fatalf("body is not a message: %v", err)
		}
		if !bytes.Equal(reply.Payload, req.Payload) {
			t.Errorf("payload = %q, want %q", reply.Payload, req.Payload)
		}
	}

	t.Run("proto, base64", func(t *testing.T) {
		query := url.Values{
			"connect":  {"v1"},
			"encoding": {"proto"},
			"base64":   {"1"},
			"message":  {base64.RawURLEncoding.EncodeToString(marshal(t, req))},
		}
		resp, body := getRaw(t, base, sideEffectFreeUnary, query, nil)
		checkReply(t, resp, body, "proto")
	})
	t.Run("json, as is", func(t *testing.T) {
		// The protocol leaves out the version parameter here, which is optional.
		query := url.Values{
			"encoding": {"json"},
			"message":  {`{"payload":"` + base64.StdEncoding.EncodeToString(req.Payload) + `"}`},
		}
		resp, body := getRaw(t, base, sideEffectFreeUnary, query, nil)
		checkReply(t, resp, body, "json")
	})
	t.Run("compressed", func(t *testing.T) {
		var compressed bytes.Buffer
		w, err := encoding.GetCompressor(gzip.Name).Compress(&compressed)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(marshal(t, req))
		_ = w.Close()
		query := url.Values{
			"encoding":    {"proto"},
			"compression": {gzip.Name},
			"base64":      {"1"},
			"message":     {base64.URLEncoding.EncodeToString(compressed.Bytes())}, // padded
		}
		resp, body := getRaw(t, base, sideEffectFreeUnary, query, nil)
		checkReply(t, resp, body, "proto")
	})
	t.Run("method with side effects is refused", func(t *testing.T) {
		query := url.Values{"encoding": {"proto"}, "message": {""}}
		resp, _ := getRaw(t, base, "/grpchantesting.TestService/Unary", query, nil)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("HTTP status = %d, want 405", resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "POST" {
			t.Errorf("Allow = %q, want POST", got)
		}
	})
	t.Run("streaming method is refused", func(t *testing.T) {
		query := url.Values{"encoding": {"proto"}, "message": {""}}
		resp, _ := getRaw(t, base, "/"+sideEffectFreeServiceName+"/ServerStream", query, nil)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("HTTP status = %d, want 405", resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "POST" {
			t.Errorf("Allow = %q, want POST", got)
		}
	})
	t.Run("plain GET to a method with side effects is refused", func(t *testing.T) {
		// What a browser sends when its user opens the URL.
		resp, _ := getRaw(t, base, "/grpchantesting.TestService/Unary", nil, nil)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("HTTP status = %d, want 405", resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "POST" {
			t.Errorf("Allow = %q, want POST", got)
		}
	})
	t.Run("missing parameters are named", func(t *testing.T) {
		testCases := []struct {
			name    string
			query   url.Values
			missing string
		}{
			{name: "nothing", missing: `"encoding"`},
			{name: "version only", query: url.Values{"connect": {"v1"}}, missing: `"encoding"`},
			{name: "encoding only", query: url.Values{"encoding": {"proto"}}, missing: `"message"`},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				resp, body := getRaw(t, base, sideEffectFreeUnary, testCase.query, nil)
				wire := checkConnectError(t, resp, body, http.StatusBadRequest, "invalid_argument")
				if !strings.Contains(wire.Message, testCase.missing) {
					t.Errorf("error message = %q, want it to name %s", wire.Message, testCase.missing)
				}
			})
		}
	})
	t.Run("other protocols report the wrong method their own way", func(t *testing.T) {
		// JSON without anything that says it is Connect is httpgrpc's, which reports
		// errors in headers.
		header := http.Header{"Content-Type": {"application/json"}}
		resp, _ := getRaw(t, base, sideEffectFreeUnary, nil, header)
		if resp.Header.Get("X-GRPC-Status") == "" {
			t.Errorf("JSON: HTTP %d with no X-GRPC-Status header, so not reported as httpgrpc", resp.StatusCode)
		}
		header = http.Header{"Content-Type": {"application/grpc-web+proto"}}
		resp, _ = getRaw(t, base, sideEffectFreeUnary, nil, header)
		if got := resp.Header.Get("Content-Type"); got != "application/grpc-web+proto" {
			t.Errorf("gRPC-Web: HTTP %d with content-type %q, so not reported as gRPC-Web", resp.StatusCode, got)
		}
	})
	t.Run("unsupported protocol version", func(t *testing.T) {
		query := url.Values{"connect": {"v2"}, "encoding": {"proto"}, "message": {""}}
		resp, body := getRaw(t, base, sideEffectFreeUnary, query, nil)
		checkConnectError(t, resp, body, http.StatusBadRequest, "invalid_argument")
	})
	t.Run("message over the limit", func(t *testing.T) {
		limited := newServer(t, httpgrpc.WithMaxRecvMsgSize(4))
		query := url.Values{
			"encoding": {"proto"},
			"base64":   {"1"},
			"message":  {base64.RawURLEncoding.EncodeToString(marshal(t, req))},
		}
		resp, body := getRaw(t, limited, sideEffectFreeUnary, query, nil)
		checkConnectError(t, resp, body, http.StatusTooManyRequests, "resource_exhausted")
	})
}

// connectError is the JSON form of a Connect error, as far as these tests look.
type connectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// checkConnectError checks that a response reports a Connect error with the
// given HTTP status and code, and returns the error for further checks.
func checkConnectError(t *testing.T, resp *http.Response, body []byte, httpStatus int, code string) connectError {
	t.Helper()
	if resp.StatusCode != httpStatus {
		t.Errorf("HTTP status = %d, want %d", resp.StatusCode, httpStatus)
	}
	var wire connectError
	if err := json.Unmarshal(body, &wire); err != nil || wire.Code != code {
		t.Errorf("error body = %q (err=%v), want a %s error", body, err, code)
	}
	return wire
}

// getRaw sends a GET request written by hand, and returns the response and its
// body, read in full; see postRaw.
func getRaw(t *testing.T, base *url.URL, path string, query url.Values, header http.Header) (*http.Response, []byte) {
	t.Helper()
	u := base.JoinPath(path)
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return resp, body
}
