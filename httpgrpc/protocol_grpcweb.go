package httpgrpc

import (
	"context"
	"io"
	"net/http"

	"google.golang.org/grpc/metadata"
)

type grpcWebServerProtocolAdapter struct{}

func (g grpcWebServerProtocolAdapter) unaryMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) processHeaders(ctx context.Context, header http.Header) (_ context.Context, _ context.CancelFunc, compressorName string, supportedCompressors []string, _ error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) responseHeaders(isStream bool, codecName string, compressorName string, md metadata.MD, targetHeaders http.Header) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) processUnaryRequest(closer io.ReadCloser) (io.ReadCloser, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) finishUnary(ctx context.Context, err error, trailers metadata.MD, w http.ResponseWriter) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) readStreamRequest(closer io.ReadCloser) (io.Reader, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebServerProtocolAdapter) finishStream(err error, trailers metadata.MD, w http.ResponseWriter) {
	//TODO implement me
	panic("implement me")
}

type grpcWebClientProtocolAdapter struct{}

func (g grpcWebClientProtocolAdapter) unaryMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) streamMessage(data []byte, compressed bool) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) supportsCompression() bool {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) requestHeaders(ctx context.Context, isStream bool, codecName string, compressorName string, supportedCompressors []string) (http.Header, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) processUnaryResponse(resp *http.Response) (metadata.MD, io.Reader, bool, metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (g grpcWebClientProtocolAdapter) readStreamResponse(r io.ReadCloser) (io.Reader, bool, metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}
