package httpgrpc

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The helpers in this file deal in gRPC's own conventions for headers: how
// metadata, a status, and a deadline are written as HTTP headers. gRPC-Web uses
// them unchanged, and the others borrow parts of them.

const (
	grpcStatusHeader         = "Grpc-Status"
	grpcMessageHeader        = "Grpc-Message"
	grpcStatusDetailsHeader  = "Grpc-Status-Details-Bin"
	grpcEncodingHeader       = "Grpc-Encoding"
	grpcAcceptEncodingHeader = "Grpc-Accept-Encoding"
)

// grpcMetadataToHeaders writes metadata using gRPC's conventions, which differ
// from httpgrpc's in that a binary value uses standard rather than URL base64.
func grpcMetadataToHeaders(md metadata.MD, h http.Header) {
	metadataToHeaders(md, h, "", isReservedGRPCHeader)
}

// grpcHeadersToMetadata is the inverse of grpcMetadataToHeaders.
func grpcHeadersToMetadata(h http.Header) (metadata.MD, error) {
	return headersToMetadata(h, isReservedGRPCHeader, "", nil)
}

// metadataToHeaders writes metadata as headers using gRPC's conventions, each
// name with the given prefix. Names that reserved reports as belonging to HTTP or
// to a protocol are left out, since they would be taken for something other than
// metadata.
func metadataToHeaders(md metadata.MD, h http.Header, prefix string, reserved func(lowerK string) bool) {
	for k, vs := range md {
		lowerK := strings.ToLower(k)
		if reserved(lowerK) {
			continue
		}
		isBin := strings.HasSuffix(lowerK, "-bin")
		for _, v := range vs {
			if isBin {
				v = base64.RawStdEncoding.EncodeToString([]byte(v))
			}
			h.Add(prefix+k, v)
		}
	}
}

// headersToMetadata is the inverse of metadataToHeaders. If trailers is not nil,
// headers whose names have trailerPrefix are read into it instead, without the
// prefix, which is how a protocol can carry trailers in the same block as
// headers.
func headersToMetadata(h http.Header, reserved func(lowerK string) bool, trailerPrefix string, trailers metadata.MD) (metadata.MD, error) {
	md := metadata.MD{}
	lowerTrailerPrefix := strings.ToLower(trailerPrefix)
	for k, vs := range h {
		lowerK := strings.ToLower(k)
		target := md
		if trailers != nil && strings.HasPrefix(lowerK, lowerTrailerPrefix) {
			lowerK = lowerK[len(lowerTrailerPrefix):]
			target = trailers
		}
		if reserved(lowerK) {
			continue
		}
		for _, v := range vs {
			if strings.HasSuffix(lowerK, "-bin") {
				decoded, err := decodeBinHeaderValue(v)
				if err != nil {
					return nil, fmt.Errorf("could not decode binary header %q: %w", k, err)
				}
				v = string(decoded)
			}
			target[lowerK] = append(target[lowerK], v)
		}
	}
	return md, nil
}

// isReservedGRPCHeader reports whether the given lower-case header name belongs
// to HTTP or to the protocol itself, and so is not metadata. The protocol's own
// headers, for status, encoding and timeouts, all start with "grpc-".
func isReservedGRPCHeader(lowerK string) bool {
	if _, reserved := reservedHeaders[lowerK]; reserved {
		return true
	}
	return strings.HasPrefix(lowerK, "grpc-") || lowerK == "x-grpc-web"
}

// decodeBinHeaderValue decodes a binary metadata value. Padding is optional in
// gRPC's encoding, so both forms are accepted.
func decodeBinHeaderValue(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
}

// encodeGRPCMessage percent-encodes a status message, which may hold bytes that
// are not valid in an HTTP header value.
func encodeGRPCMessage(msg string) string {
	var buf strings.Builder
	for i := 0; i < len(msg); i++ {
		if c := msg[i]; c >= 0x20 && c <= 0x7E && c != '%' {
			buf.WriteByte(c)
		} else {
			fmt.Fprintf(&buf, "%%%02X", c)
		}
	}
	return buf.String()
}

func decodeGRPCMessage(msg string) string {
	var buf strings.Builder
	for i := 0; i < len(msg); i++ {
		if msg[i] == '%' && i+2 < len(msg) {
			if b, err := strconv.ParseUint(msg[i+1:i+3], 16, 8); err == nil {
				buf.WriteByte(byte(b))
				i += 2
				continue
			}
		}
		buf.WriteByte(msg[i])
	}
	return buf.String()
}

// grpcStatusFromHeaders extracts the RPC's status and trailing metadata from a
// block of headers: either the final frame of a body, or the headers of a
// trailers-only response. A successful RPC yields a nil error.
func grpcStatusFromHeaders(h http.Header) (metadata.MD, error) {
	code := codes.Unknown
	if v := h.Get(grpcStatusHeader); v != "" {
		parsed, parseErr := strconv.ParseUint(v, 10, 32)
		if parseErr != nil {
			return nil, status.Errorf(codes.Internal, "malformed %s trailer: %q", grpcStatusHeader, v)
		}
		code = codes.Code(parsed)
	}

	// Details carry a whole status proto, but nothing specifies what to do when it
	// disagrees with the status spelled out separately, and implementations have
	// differed. Here the code always comes from Grpc-Status, so a blob claiming
	// success cannot override a failure; the blob supplies the details, and the
	// message only if there is no Grpc-Message.
	stpb := &spb.Status{}
	if v := h.Get(grpcStatusDetailsHeader); v != "" {
		if raw, decodeErr := decodeBinHeaderValue(v); decodeErr == nil {
			var fromDetails spb.Status
			if proto.Unmarshal(raw, &fromDetails) == nil {
				stpb = &fromDetails
			}
		}
	}
	stpb.Code = int32(code)
	if msg := h.Values(grpcMessageHeader); len(msg) > 0 {
		stpb.Message = decodeGRPCMessage(msg[0])
	}
	st := status.FromProto(stpb)

	md, err := grpcHeadersToMetadata(h)
	if err != nil {
		return nil, err
	}
	return md, st.Err()
}

// grpcTimeout encodes the time remaining until the given deadline in the form
// of the GRPC-Timeout header.
func grpcTimeout(deadline time.Time) string {
	return strconv.FormatInt(timeoutMillis(deadline), 10) + "m"
}
