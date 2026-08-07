package sse_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/fullstorydev/grpchan/internal/sse"
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

// TestDecoderMaxData covers the limit that lets an over-large event be rejected
// as it arrives. Reading it in full and measuring afterwards would defeat the
// point of having a limit at all, so the test also pins down that the bulk of the
// event is never consumed.
func TestDecoderMaxData(t *testing.T) {
	const limit = 64
	huge := strings.Repeat("x", 1<<20)
	src := &countingReader{r: strings.NewReader(fmt.Sprintf("data: %s\n\n", huge))}

	_, err := sse.NewDecoderWithMaxData(src, limit).Decode()
	if !errors.Is(err, sse.ErrDataTooLarge) {
		t.Fatalf("expected ErrDataTooLarge, got %v", err)
	}

	// The error must say how much was read and what the limit was; a bare sentinel
	// would leave a caller unable to tell how far over the limit the event was.
	var tooLarge *sse.DataTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected a *DataTooLargeError, got %T", err)
	}
	if tooLarge.Limit != limit {
		t.Errorf("Limit = %d, want %d", tooLarge.Limit, limit)
	}
	if tooLarge.Read <= limit {
		t.Errorf("Read = %d, want more than the limit of %d", tooLarge.Read, limit)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(limit)) {
		t.Errorf("error message %q does not report the limit", err.Error())
	}

	// bufio reads in buffer-sized chunks, so some overshoot is expected; what must
	// not happen is consuming the whole event.
	if src.n > 1<<16 {
		t.Errorf("consumed %d bytes of a %d byte event; it should have stopped early", src.n, len(huge))
	}
}

// TestDecoderMaxDataAllowsLimitSizedEvent checks the limit is not off by the
// length of the "data: " prefix, which is framing rather than data.
func TestDecoderMaxDataAllowsLimitSizedEvent(t *testing.T) {
	const limit = 64
	payload := strings.Repeat("y", limit)
	r := strings.NewReader(fmt.Sprintf("data: %s\n\n", payload))

	event, err := sse.NewDecoderWithMaxData(r, limit).Decode()
	if err != nil {
		t.Fatalf("an event exactly at the limit was rejected: %v", err)
	}
	if string(event.Data) != payload {
		t.Errorf("data = %q, want %q", event.Data, payload)
	}
}
