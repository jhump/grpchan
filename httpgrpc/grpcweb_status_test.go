package httpgrpc

import (
	"encoding/base64"
	"net/http"
	"testing"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestGRPCStatusFromHeaders checks which of the two places a status can be
// reported wins when they disagree: Grpc-Status for the code, always, and
// Grpc-Message for the message when there is one.
func TestGRPCStatusFromHeaders(t *testing.T) {
	detail, err := anypb.New(structpb.NewStringValue("detail"))
	if err != nil {
		t.Fatal(err)
	}
	// The blob claims success, with a message of its own.
	raw, err := proto.Marshal(&spb.Status{Code: int32(codes.OK), Message: "from details", Details: []*anypb.Any{detail}})
	if err != nil {
		t.Fatal(err)
	}
	details := base64.RawStdEncoding.EncodeToString(raw)

	testCases := []struct {
		name        string
		header      http.Header
		wantMessage string
	}{
		{
			name: "message header wins",
			header: http.Header{
				"Grpc-Status":             []string{"13"},
				"Grpc-Message":            []string{"from header"},
				"Grpc-Status-Details-Bin": []string{details},
			},
			wantMessage: "from header",
		},
		{
			name: "details message used without a message header",
			header: http.Header{
				"Grpc-Status":             []string{"13"},
				"Grpc-Status-Details-Bin": []string{details},
			},
			wantMessage: "from details",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := grpcStatusFromHeaders(testCase.header)
			st, _ := status.FromError(err)
			if st.Code() != codes.Internal {
				t.Errorf("code = %v, want Internal: the details must not override Grpc-Status", st.Code())
			}
			if st.Message() != testCase.wantMessage {
				t.Errorf("message = %q, want %q", st.Message(), testCase.wantMessage)
			}
			if len(st.Details()) != 1 {
				t.Errorf("got %d details, want the one from the blob", len(st.Details()))
			}
		})
	}
}
