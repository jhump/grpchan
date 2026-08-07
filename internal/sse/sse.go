package sse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Event represents an SSE event. The decoder only supports the data and event fields.
type Event struct {
	Type string
	Data []byte
}

// ErrDataTooLarge is returned by a decoder created with NewDecoderWithMaxData
// when a single event's data exceeds the given limit. The error returned wraps
// this one and reports the sizes involved; see DataTooLargeError.
var ErrDataTooLarge = errors.New("event data too large")

// DataTooLargeError reports an event whose data exceeded the decoder's limit.
//
// The decoder gives up as soon as the limit is passed, rather than reading the
// rest of the event to find out how big it really is, so Read is how much had
// been read at that point. It is a lower bound on the event's full size.
type DataTooLargeError struct {
	// Read is the number of bytes read for the event before it was abandoned.
	Read int
	// Limit is the maximum amount of data the decoder was willing to accept.
	Limit int
}

func (e *DataTooLargeError) Error() string {
	return fmt.Sprintf("event data too large: read %d bytes with a limit of %d", e.Read, e.Limit)
}

func (e *DataTooLargeError) Unwrap() error { return ErrDataTooLarge }

// maxFieldPrefix is the length of the longest field prefix the decoder
// recognizes ("event: "). A line may exceed the data limit by this much before
// it is rejected, since the limit applies to an event's data and not to the
// framing around it.
const maxFieldPrefix = len("event: ")

type Decoder struct {
	r *bufio.Reader
	// maxData bounds the data of a single event. Zero means no limit.
	maxData int
}

// NewDecoder creates a new SSE decoder. The decoder does not implement the full SSE specification.
// It only supports what we need, which only includes the data and event fields.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReader(r)}
}

// NewDecoderWithMaxData is like NewDecoder, but fails with ErrDataTooLarge as soon
// as a single event's data exceeds maxData, rather than buffering all of it first.
// A maxData of zero means no limit.
func NewDecoderWithMaxData(r io.Reader, maxData int) *Decoder {
	return &Decoder{r: bufio.NewReader(r), maxData: maxData}
}

// readLine reads the next line, stripped of its line ending. It accumulates the
// line a buffer at a time so that an over-long one can be rejected as it arrives,
// instead of being read into memory in full and rejected afterwards.
func (d *Decoder) readLine() ([]byte, error) {
	var line []byte
	for {
		// ReadSlice returns a view of the decoder's buffer, which the next read
		// invalidates, so the result is always copied into line.
		frag, err := d.r.ReadSlice('\n')
		if d.maxData > 0 && len(line)+len(frag) > d.maxData+maxFieldPrefix {
			return nil, &DataTooLargeError{Read: len(line) + len(frag), Limit: d.maxData}
		}
		line = append(line, frag...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return bytes.TrimRight(line, "\r\n"), err
	}
}

func (d *Decoder) Decode() (*Event, error) {
	var current *Event
	for {
		line, err := d.readLine()

		if err == io.EOF {
			if len(line) != 0 {
				return nil, io.ErrUnexpectedEOF
			}
			if current != nil {
				if current.Type == "" {
					current.Type = "message"
				}
				return current, nil
			}
			return nil, io.EOF
		} else if err != nil {
			return nil, err
		}

		switch {
		case bytes.HasPrefix(line, []byte("data: ")):
			payload := line[6:]
			if current == nil {
				current = &Event{Data: payload}
			} else {
				current.Data = append(current.Data, payload...)
			}
			// An event's data may be split across several lines, so the limit is
			// enforced on the running total as well as on each line.
			if d.maxData > 0 && len(current.Data) > d.maxData {
				return nil, &DataTooLargeError{Read: len(current.Data), Limit: d.maxData}
			}
		case bytes.HasPrefix(line, []byte("event: ")):
			if current == nil {
				current = &Event{}
			}
			current.Type = string(line[7:])
		case len(line) == 0:
			if current != nil {
				if current.Type == "" {
					current.Type = "message"
				}
				return current, nil
			}
		default:
			return nil, errors.New("malformed event")
		}
	}
}

type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new SSE encoder. The encoder does not implement the full SSE specification.
// It only supports what we need, which only includes the data and event fields.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (e *Encoder) Encode(event *Event) error {
	if event.Type == "" || event.Type == "message" {
		_, err := fmt.Fprintf(e.w, "data: %s\n\n", event.Data)
		return err
	}

	_, err := fmt.Fprintf(e.w, "event: %s\ndata: %s\n\n", event.Type, event.Data)
	return err
}
