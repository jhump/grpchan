package httpgrpc

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"

	"google.golang.org/grpc/metadata"
)

type connectServerProtocolAdapter struct{}

func (c connectServerProtocolAdapter) unaryMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) processHeaders(ctx context.Context, header http.Header) (_ context.Context, _ context.CancelFunc, compressorName string, supportedCompressors []string, _ error) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) responseHeaders(isStream bool, codecName string, compressorName string, md metadata.MD, targetHeaders http.Header) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) processUnaryRequest(closer io.ReadCloser) (io.ReadCloser, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) finishUnary(ctx context.Context, err error, trailers metadata.MD, w http.ResponseWriter) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) readStreamRequest(closer io.ReadCloser) (io.Reader, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectServerProtocolAdapter) finishStream(err error, trailers metadata.MD, w http.ResponseWriter) {
	//TODO implement me
	panic("implement me")
}

type connectClientProtocolAdapter struct{}

var _ clientProtocolAdapter = connectClientProtocolAdapter{}

func (c connectClientProtocolAdapter) unaryMessage(data []byte, _ bool) ([][]byte, error) {
	return [][]byte{data}, nil
}

func (c connectClientProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	var buf [5]byte
	if compressed {
		buf[0] = 1
	}
	if _, err := binary.Encode(buf[1:], binary.BigEndian, int32(len(data))); err != nil {
		return nil, err
	}
	return [][]byte{buf[:], data}, nil
}

func (c connectClientProtocolAdapter) supportsCompression() bool {
	return true
}

func (c connectClientProtocolAdapter) requestHeaders(ctx context.Context, isStream bool, codecName string, compressorName string, supportedCompressors []string) (http.Header, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectClientProtocolAdapter) processUnaryResponse(resp *http.Response) (metadata.MD, io.Reader, bool, metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectClientProtocolAdapter) readStreamResponse(r io.ReadCloser) (io.Reader, bool, metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}
