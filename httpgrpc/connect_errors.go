package httpgrpc

import (
	"encoding/base64"
	"net/http"
	"strings"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// The Connect protocol reports a failed RPC as a JSON object: as the body of a
// unary response, whose HTTP status reflects the code, or inside the final frame
// of a stream. This file has that object and the mappings between the protocol's
// codes, gRPC's, and HTTP statuses.

// connectWireError is the JSON form of a failed RPC's status.
type connectWireError struct {
	Code    string               `json:"code"`
	Message string               `json:"message,omitempty"`
	Details []*connectWireDetail `json:"details,omitempty"`
}

// connectWireDetail is the JSON form of an error detail: the name of its message
// type, and its serialized form in base64. Padding is omitted when writing, and
// optional when reading.
type connectWireDetail struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// connectCodeNames are the protocol's names for status codes, which are the
// names of gRPC's codes in snake case.
var connectCodeNames = map[codes.Code]string{
	codes.Canceled:           "canceled",
	codes.Unknown:            "unknown",
	codes.InvalidArgument:    "invalid_argument",
	codes.DeadlineExceeded:   "deadline_exceeded",
	codes.NotFound:           "not_found",
	codes.AlreadyExists:      "already_exists",
	codes.PermissionDenied:   "permission_denied",
	codes.ResourceExhausted:  "resource_exhausted",
	codes.FailedPrecondition: "failed_precondition",
	codes.Aborted:            "aborted",
	codes.OutOfRange:         "out_of_range",
	codes.Unimplemented:      "unimplemented",
	codes.Internal:           "internal",
	codes.Unavailable:        "unavailable",
	codes.DataLoss:           "data_loss",
	codes.Unauthenticated:    "unauthenticated",
}

var connectCodesByName = func() map[string]codes.Code {
	m := make(map[string]codes.Code, len(connectCodeNames))
	for code, name := range connectCodeNames {
		m[name] = code
	}
	return m
}()

// connectHTTPStatus is the HTTP status of a failed unary RPC with the given code.
func connectHTTPStatus(code codes.Code) int {
	switch code {
	case codes.Canceled:
		return 499
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// connectCodeFromHTTPStatus is the code of a response that failed without a
// Connect error body, such as one from a proxy. As the protocol specifies, it is
// gRPC's mapping, which is not the inverse of connectHTTPStatus.
func connectCodeFromHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.Internal
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.Unimplemented
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return codes.Unavailable
	default:
		return codes.Unknown
	}
}

// newConnectWireError renders the status of a failed RPC, which is never OK.
func newConnectWireError(st *status.Status) *connectWireError {
	wire := &connectWireError{
		Code:    connectCodeNames[st.Code()],
		Message: st.Message(),
	}
	if wire.Code == "" {
		wire.Code = connectCodeNames[codes.Unknown]
	}
	for _, detail := range st.Proto().GetDetails() {
		typeName := detail.GetTypeUrl()
		if i := strings.LastIndexByte(typeName, '/'); i >= 0 {
			typeName = typeName[i+1:]
		}
		wire.Details = append(wire.Details, &connectWireDetail{
			Type:  typeName,
			Value: base64.RawStdEncoding.EncodeToString(detail.GetValue()),
		})
	}
	return wire
}

func (w *connectWireError) status() *status.Status {
	code, ok := connectCodesByName[w.Code]
	if !ok {
		code = codes.Unknown
	}
	stpb := &spb.Status{Code: int32(code), Message: w.Message}
	for _, detail := range w.Details {
		value, err := decodeBinHeaderValue(detail.Value)
		if err != nil {
			continue
		}
		stpb.Details = append(stpb.Details, &anypb.Any{
			TypeUrl: "type.googleapis.com/" + detail.Type,
			Value:   value,
		})
	}
	return status.FromProto(stpb)
}
