package httpgrpc

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestServerStreamEnded checks that a stream refuses to send or receive once it
// has ended, however it ended. The final frame has been written by then, and
// nothing may follow it; a stream can end while its handler is still using it,
// when a message is refused for its size.
func TestServerStreamEnded(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantEOF  bool
	}{
		{name: "successfully", wantEOF: true},
		{name: "with an error", err: status.Error(codes.ResourceExhausted, "too big"), wantCode: codes.ResourceExhausted},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/grpchantesting.TestService/BidiStream", strings.NewReader(""))
			rec := httptest.NewRecorder()
			str := &serverStream{
				ctx:       context.Background(),
				protocol:  newHTTPGRPCServerAdapter(req, rec, grpcproto.Name, handlerOpts{}),
				codec:     encoding.GetCodecV2(grpcproto.Name),
				maxSend:   math.MaxInt32,
				w:         rec,
				reqStream: true,
			}
			str.finish(testCase.err)
			written := rec.Body.Len()

			check := func(op string, err error) {
				t.Helper()
				if testCase.wantEOF {
					if !errors.Is(err, io.EOF) {
						t.Errorf("%s on a stream that ended successfully = %v, want io.EOF", op, err)
					}
				} else if got := status.Code(err); got != testCase.wantCode {
					t.Errorf("%s on a stream that ended with an error = %v, want %v", op, err, testCase.wantCode)
				}
			}
			check("SendMsg", str.SendMsg(&emptypb.Empty{}))
			check("RecvMsg", str.RecvMsg(&emptypb.Empty{}))
			if rec.Body.Len() != written {
				t.Errorf("%d bytes were written after the stream's final frame", rec.Body.Len()-written)
			}
		})
	}
}
