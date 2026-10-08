package httpgrpc

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	grpcproto "google.golang.org/grpc/encoding/proto"
)

// The text form of gRPC-Web, application/grpc-web-text, is the binary form with
// the body base64-encoded, for clients that cannot handle a binary response
// body. Everything else -- framing, trailers, compression -- is the same.
//
// A body is not one base64 entity but a series of them: each flush is encoded on
// its own, padding included, so that a streamed message can be sent as soon as it
// is ready. So padding may appear in the middle of a body, which a decoder must
// accept wherever it falls, not only at frame boundaries.

// grpcWebTextContentType is the content-type of the text form, without a codec.
const grpcWebTextContentType = "application/grpc-web-text"

// parseGRPCWebContentType reports whether the given media type is one of
// gRPC-Web's, and if so which codec it names and whether it is the text form. A
// media type without a codec means proto.
func parseGRPCWebContentType(mediaType string) (codecName string, text bool, ok bool) {
	base, codecName, found := strings.Cut(mediaType, "+")
	switch base {
	case grpcWebContentType:
	case grpcWebTextContentType:
		text = true
	default:
		return "", false, false
	}
	if !found {
		codecName = grpcproto.Name
	}
	return codecName, text, true
}

// grpcWebContentTypeFor returns the content-type naming the given message codec,
// in the binary or the text form.
func grpcWebContentTypeFor(codecName string, text bool) string {
	if text {
		return grpcWebTextContentType + "+" + codecName
	}
	return grpcWebContentType + "+" + codecName
}

// respondInText reports whether a response should use the text form. A client
// asks for one form with the Accept header; one that names only one of the two
// forms gets that one, and otherwise the response mirrors the request.
func respondInText(header http.Header, requestText bool) bool {
	acceptsText, acceptsBinary := false, false
	for _, value := range header.Values("Accept") {
		for _, part := range strings.Split(value, ",") {
			mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err != nil {
				continue
			}
			if _, text, ok := parseGRPCWebContentType(mediaType); ok {
				if text {
					acceptsText = true
				} else {
					acceptsBinary = true
				}
			}
		}
	}
	if acceptsText != acceptsBinary {
		return acceptsText
	}
	return requestText
}

// encodeTextFrame encodes the parts of one frame as a single base64 chunk, padded,
// so that it can be written and flushed on its own.
func encodeTextFrame(frames [][]byte) [][]byte {
	size := 0
	for _, part := range frames {
		size += len(part)
	}
	var encoded bytes.Buffer
	encoded.Grow(base64.StdEncoding.EncodedLen(size))
	encoder := base64.NewEncoder(base64.StdEncoding, &encoded)
	for _, part := range frames {
		_, _ = encoder.Write(part) // writes to a bytes.Buffer cannot fail
	}
	_ = encoder.Close()
	return [][]byte{encoded.Bytes()}
}

// encodeTextFrameIf encodes the given frame as encodeTextFrame does if text is
// set, and otherwise returns it as it is. err is any error from framing it, which
// is passed on.
func encodeTextFrameIf(text bool, frames [][]byte, err error) ([][]byte, error) {
	if err != nil || !text {
		return frames, err
	}
	return encodeTextFrame(frames), nil
}

// errMalformedText reports a text-form body that is not valid base64.
var errMalformedText = errors.New("malformed base64 in grpc-web-text body")

// base64Reader decodes a text-form body. It decodes a group of four characters at
// a time, which is what lets padding fall anywhere: a padded group simply decodes
// to fewer bytes. Line breaks are skipped; the protocol does not call for them,
// but they are harmless, and Go's own base64 decoder ignores them too.
type base64Reader struct {
	r   *bufio.Reader
	buf []byte // decoded and not yet read
	err error
	// encoded and decoded are scratch space for fill, reused from one batch to
	// the next; buf is a slice of decoded, so fill only reuses it once buf is
	// empty, which is the only time it is called.
	encoded, decoded []byte
}

func newBase64Reader(r io.Reader) *base64Reader {
	return &base64Reader{r: bufio.NewReader(r)}
}

// base64Batch is how many groups are decoded at once, rather than one by one.
const base64Batch = 1024

func (d *base64Reader) Read(p []byte) (int, error) {
	for len(d.buf) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		d.err = d.fill()
	}
	n := copy(p, d.buf)
	d.buf = d.buf[n:]
	return n, nil
}

// fill decodes up to base64Batch groups. It stops early after a padded group,
// since the decoder will not accept anything after padding in the same call, and
// as soon as no more input is buffered: a frame may end with a whole group, and
// whatever follows it may not be sent until the peer has had a reply.
func (d *base64Reader) fill() error {
	if d.encoded == nil {
		d.encoded = make([]byte, 0, 4*base64Batch)
		d.decoded = make([]byte, 3*base64Batch)
	}
	encoded := d.encoded[:0]
	for len(encoded) < cap(encoded) {
		var group [4]byte
		n := 0
		for n < len(group) {
			c, err := d.r.ReadByte()
			if err != nil {
				if err == io.EOF && n > 0 {
					return fmt.Errorf("%w: truncated", errMalformedText)
				}
				if len(encoded) > 0 {
					// Decode what was read; the error is reported on the next fill.
					return d.decode(encoded, err)
				}
				return err
			}
			if c == '\r' || c == '\n' {
				continue
			}
			group[n] = c
			n++
		}
		encoded = append(encoded, group[:]...)
		if group[3] == '=' || d.r.Buffered() == 0 {
			break
		}
	}
	return d.decode(encoded, nil)
}

func (d *base64Reader) decode(encoded []byte, next error) error {
	n, err := base64.StdEncoding.Decode(d.decoded, encoded)
	if err != nil {
		return fmt.Errorf("%w: %w", errMalformedText, err)
	}
	d.buf = d.decoded[:n]
	return next
}
