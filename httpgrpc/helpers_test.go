package httpgrpc_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"
)

// postRaw sends a request written by hand, and returns the response and its
// body, which has been read in full. The transport is told not to ask for or
// undo compression, so the body is exactly what the server sent.
func postRaw(t *testing.T, base *url.URL, path, contentType string, header http.Header, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base.JoinPath(path).String(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	req.Header.Set("Content-Type", contentType)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return resp, respBody
}
