package httpgrpc

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

// countingReader reports how much of the underlying data was actually consumed.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// TestJSONReaderValueLimit covers the ceiling on a single value in a stream of
// concatenated JSON values. That framing announces no size, so without a limit
// underneath it the decoder would buffer a value of any size before anything
// could object to it.
func TestJSONReaderValueLimit(t *testing.T) {
	const limit = 1024
	codec := encoding.GetCodecV2(jsonCodecName)

	t.Run("oversized value is refused as it arrives", func(t *testing.T) {
		huge := fmt.Sprintf(`{"payload":%q}`, strings.Repeat("x", 1<<20))
		src := &countingReader{r: strings.NewReader(`{"payload":"ok"}` + huge)}
		read := newJSONReader(src, codec, limit)

		if _, err := read(); err != nil {
			t.Fatalf("the first value is well under the limit: %v", err)
		}

		_, err := read()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted, got %v (err=%v)", got, err)
		}
		// The error has to say how much was read and what the limit was, or a caller
		// cannot tell how far over the limit the value was.
		if !strings.Contains(err.Error(), fmt.Sprint(limit)) {
			t.Errorf("error %q does not report the limit", err)
		}
		// The decoder reads ahead in chunks, so some overshoot is expected; what must
		// not happen is buffering the whole value.
		if src.n > 1<<16 {
			t.Errorf("consumed %d bytes of a 1mb value; it should have stopped early", src.n)
		}
	})

	t.Run("limit is per value, not cumulative", func(t *testing.T) {
		// Each value is small, but together they far exceed the limit. A reader that
		// counted the whole stream instead of one value at a time would reject these.
		var sb strings.Builder
		const count = 200
		for i := 0; i < count; i++ {
			fmt.Fprintf(&sb, `{"payload":%q}`, strings.Repeat("y", 64))
		}
		if sb.Len() <= limit {
			t.Fatalf("test is not exercising the limit: %d bytes total", sb.Len())
		}

		read := newJSONReader(strings.NewReader(sb.String()), codec, limit)
		for i := 0; i < count; i++ {
			if _, err := read(); err != nil {
				t.Fatalf("value %d of %d failed: %v", i+1, count, err)
			}
		}
		if _, err := read(); err != io.EOF {
			t.Fatalf("expected EOF after the last value, got %v", err)
		}
	})
}
