package httpgrpc

import (
	"bytes"
	"crypto/rand"
	"io"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// countingCompressor reports how much a decompression actually produced, which
// is what shows that an oversized message was refused rather than expanded in
// full and measured afterwards.
type countingCompressor struct {
	encoding.Compressor
	decompressed *countingReader
}

func (c *countingCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dr, err := c.Compressor.Decompress(r)
	if err != nil {
		return nil, err
	}
	c.decompressed = &countingReader{r: dr}
	return c.decompressed, nil
}

func TestCompressMessage(t *testing.T) {
	compressor := encoding.GetCompressor(gzip.Name)

	t.Run("compressible message is compressed", func(t *testing.T) {
		data := bytes.Repeat([]byte("compressible "), 1024)
		out, compressed, err := compressMessage(compressor, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !compressed || len(out) >= len(data) {
			t.Fatalf("compressed=%v, %d bytes from %d; expected a smaller, compressed message",
				compressed, len(out), len(data))
		}
		roundTripped, err := decompressLimited(compressor, out, len(data))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(roundTripped, data) {
			t.Error("message did not survive a round trip")
		}
	})
	t.Run("incompressible message is sent as is", func(t *testing.T) {
		// Random bytes come out of gzip larger than they went in, so sending them
		// compressed would cost more than it saves.
		data := make([]byte, 4096)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		out, compressed, err := compressMessage(compressor, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if compressed || !bytes.Equal(out, data) {
			t.Errorf("compressed=%v; expected the message back unchanged", compressed)
		}
	})
	t.Run("message under the minimum is sent as is", func(t *testing.T) {
		data := bytes.Repeat([]byte("compressible "), 100)
		out, compressed, err := compressMessage(compressor, data, len(data)+1)
		if err != nil {
			t.Fatal(err)
		}
		if compressed || !bytes.Equal(out, data) {
			t.Errorf("compressed=%v; expected the message back unchanged", compressed)
		}
		// At the minimum, it is compressed.
		if _, compressed, _ := compressMessage(compressor, data, len(data)); !compressed {
			t.Error("a message of exactly the minimum was not compressed")
		}
	})
	t.Run("no compressor or no data is a no-op", func(t *testing.T) {
		data := []byte("data")
		if out, compressed, err := compressMessage(nil, data, 0); err != nil || compressed || !bytes.Equal(out, data) {
			t.Errorf("nil compressor: compressed=%v, err=%v; expected the message back unchanged", compressed, err)
		}
		if out, compressed, err := compressMessage(compressor, nil, 0); err != nil || compressed || len(out) != 0 {
			t.Errorf("empty message: compressed=%v, err=%v; expected nothing back", compressed, err)
		}
	})
}

// TestDecompressLimited covers the limit applied to what a message decompresses
// to. The compressed size is bounded before decompression, but a message that is
// small on the wire can expand to almost any size, so that bound alone protects
// nothing.
func TestDecompressLimited(t *testing.T) {
	const limit = 1 << 10
	compressor := encoding.GetCompressor(gzip.Name)
	compress := func(t *testing.T, data []byte) []byte {
		t.Helper()
		out, compressed, err := compressMessage(compressor, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !compressed {
			t.Fatal("test data did not compress")
		}
		return out
	}

	t.Run("message at the limit is accepted", func(t *testing.T) {
		data := bytes.Repeat([]byte("a"), limit)
		out, err := decompressLimited(compressor, compress(t, data), limit)
		if err != nil {
			t.Fatalf("a message of exactly the limit was refused: %v", err)
		}
		if !bytes.Equal(out, data) {
			t.Error("message did not survive a round trip")
		}
	})
	t.Run("message past the limit is refused", func(t *testing.T) {
		data := bytes.Repeat([]byte("a"), limit+1)
		_, err := decompressLimited(compressor, compress(t, data), limit)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
	})
	t.Run("expansion stops at the limit", func(t *testing.T) {
		// 64mb of zeros compresses to well under the limit, so only the bound on
		// the decompressed size stands between it and a 64mb allocation.
		bomb := compress(t, make([]byte, 64<<20))
		if len(bomb) > 1<<20 {
			t.Fatalf("test is not exercising the limit: %d compressed bytes", len(bomb))
		}
		counting := &countingCompressor{Compressor: compressor}
		_, err := decompressLimited(counting, bomb, limit)
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
		if n := counting.decompressed.n; n > limit+1 {
			t.Errorf("decompressed %d bytes before refusing; it should stop one past the limit", n)
		}
	})
	t.Run("corrupt message is Internal", func(t *testing.T) {
		_, err := decompressLimited(compressor, []byte("not gzip"), limit)
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("expected Internal, got %v (err=%v)", got, err)
		}
	})
}

func TestAdvertisedCompressors(t *testing.T) {
	// gzip is registered by importing it, above; the others are not, so only it is
	// advertised.
	if got := advertisedCompressors(); !slices.Equal(got, []string{gzip.Name}) {
		t.Errorf("advertisedCompressors() = %v, want [%s]", got, gzip.Name)
	}
}

func TestCheckDefaultCompressor(t *testing.T) {
	testCases := []struct {
		name      string
		supported bool
		wantErr   bool
	}{
		{name: "", supported: false},
		{name: encoding.Identity, supported: false},
		{name: gzip.Name, supported: true},
		{name: gzip.Name, supported: false, wantErr: true},
		{name: "no-such-compressor", supported: true, wantErr: true},
	}
	for _, testCase := range testCases {
		err := checkDefaultCompressor(testCase.name, testCase.supported)
		if (err != nil) != testCase.wantErr {
			t.Errorf("checkDefaultCompressor(%q, %v) = %v, want error: %v",
				testCase.name, testCase.supported, err, testCase.wantErr)
		}
	}
}

func TestRequestCompressor(t *testing.T) {
	testCases := []struct {
		desc        string
		callName    string
		defaultName string
		supported   bool
		want        string
	}{
		{desc: "nothing named", want: "", supported: true},
		{desc: "call names a compressor", callName: gzip.Name, supported: true, want: gzip.Name},
		{desc: "default only", callName: gzip.Name, defaultName: gzip.Name, supported: true, want: gzip.Name},
		{desc: "call opts out of the default", callName: encoding.Identity, defaultName: gzip.Name, supported: true, want: ""},
		{desc: "unknown call name falls back to default", callName: "no-such", defaultName: gzip.Name, supported: true, want: gzip.Name},
		{desc: "unknown call name is ignored", callName: "no-such", supported: true, want: ""},
		{desc: "protocol cannot compress", callName: gzip.Name, defaultName: gzip.Name, want: ""},
	}
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			got := requestCompressor(testCase.callName, testCase.defaultName, testCase.supported)
			if got != testCase.want {
				t.Errorf("requestCompressor(%q, %q, %v) = %q, want %q",
					testCase.callName, testCase.defaultName, testCase.supported, got, testCase.want)
			}
		})
	}
}

func TestResponseCompressor(t *testing.T) {
	testCases := []struct {
		desc        string
		requestName string
		accepted    []string
		want        string
	}{
		{desc: "nothing named", want: ""},
		{desc: "request's own is used", requestName: gzip.Name, want: gzip.Name},
		{desc: "uncompressed request, accepted encoding", accepted: []string{gzip.Name}, want: gzip.Name},
		{desc: "first registered accepted encoding", accepted: []string{"no-such", encoding.Identity, gzip.Name}, want: gzip.Name},
		{desc: "nothing usable accepted", accepted: []string{"no-such"}, want: ""},
		{desc: "unknown request encoding falls back to accepted", requestName: "no-such", accepted: []string{gzip.Name}, want: gzip.Name},
	}
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			if got := responseCompressor(testCase.requestName, testCase.accepted); got != testCase.want {
				t.Errorf("responseCompressor(%q, %v) = %q, want %q",
					testCase.requestName, testCase.accepted, got, testCase.want)
			}
		})
	}
}
