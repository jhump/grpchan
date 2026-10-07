package httpgrpc

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	maxMessageSize = 100 * 1024 * 1024 // 100mb
)

// marshalError is returned when sending a message that could not be encoded,
// to tell it apart from a failure to write the encoded message.
type marshalError struct {
	err error
}

func (e marshalError) Error() string {
	return e.err.Error()
}

func (e marshalError) Unwrap() error {
	return e.err
}

// readSizePreface reads a 32-bit size from the given reader. If the value is
// negative, it indicates the last message in the stream. Messages can have zero
// size, but the last message in the stream should never have zero size (so its
// size will be negative).
func readSizePreface(in io.Reader) (int32, error) {
	var sz int32
	err := binary.Read(in, binary.BigEndian, &sz)
	if err != nil {
		return 0, err
	}
	// Reject math.MinInt32: negating it overflows in two's complement and would
	// mean that -size would remain negative and potentially cause a panic if the
	// callers tries to allocate a buffer for the message.
	if sz == math.MinInt32 {
		return 0, errors.New("bad size preface: size overflow")
	}

	return sz, err
}

// asMetadata converts the given HTTP headers into GRPC metadata.
func asMetadata(header http.Header) (metadata.MD, error) {
	// metadata has same shape as http.Header,
	md := metadata.MD{}
	for k, vs := range header {
		k = strings.ToLower(k)
		for _, v := range vs {
			if strings.HasSuffix(k, "-bin") {
				vv, err := base64.URLEncoding.DecodeString(v)
				if err != nil {
					return nil, err
				}
				v = string(vv)
			}
			md[k] = append(md[k], v)
		}
	}
	return md, nil
}

var reservedHeaders = map[string]struct{}{
	"accept-encoding":   {},
	"connection":        {},
	"content-encoding":  {},
	"content-type":      {},
	"content-length":    {},
	"keep-alive":        {},
	"te":                {},
	"trailer":           {},
	"transfer-encoding": {},
	"upgrade":           {},
}

// isReservedHeader reports whether the given lower-case header name belongs to
// HTTP or to this protocol, and so cannot be sent as metadata: the peer would
// take it for something other than metadata. The protocol's own headers are
// GRPC-Timeout and those that start with "X-GRPC-", for status, details and
// unary trailers.
func isReservedHeader(lowerK string) bool {
	if _, ok := reservedHeaders[lowerK]; ok {
		return true
	}
	return lowerK == "grpc-timeout" || strings.HasPrefix(lowerK, "x-grpc-")
}

func toHeaders(md metadata.MD, h http.Header, prefix string) {
	// binary headers must be base-64-encoded
	for k, vs := range md {
		lowerK := strings.ToLower(k)
		if isReservedHeader(lowerK) {
			continue
		}
		isBin := strings.HasSuffix(lowerK, "-bin")
		for _, v := range vs {
			if isBin {
				v = base64.URLEncoding.EncodeToString([]byte(v))
			}
			h.Add(prefix+k, v)
		}
	}
}

type strAddr string

func (a strAddr) Network() string {
	if a != "" {
		// Per the documentation on net/http.Request.RemoteAddr, if this is
		// set, it's set to the IP:port of the peer (hence, TCP):
		// https://golang.org/pkg/net/http/#Request
		//
		// If we want to support Unix sockets later, we can
		// add our own grpc-specific convention within the
		// grpc codebase to set RemoteAddr to a different
		// format, or probably better: we can attach it to the
		// context and use that from serverHandlerTransport.RemoteAddr.
		return "tcp"
	}
	return ""
}

func (a strAddr) String() string { return string(a) }
