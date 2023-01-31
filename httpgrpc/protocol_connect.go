package httpgrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"

	"google.golang.org/grpc/metadata"
)

// NB: This is nil, so if protocol detection sees connect, it will
// result in unsupported media type error.
// TODO: Make this real by implementing the type below
var connectAdapter serverProtocolAdapter

type connectServerProtocolAdapter struct{}

type connectClientProtocolAdapter struct{}

var _ clientProtocolAdapter = connectClientProtocolAdapter{}

func (c connectClientProtocolAdapter) unaryMessage(data []byte, _ bool) (io.Reader, error) {
	return bytes.NewReader(data), nil
}

func (c connectClientProtocolAdapter) streamMessage(data []byte, compressed bool) (io.Reader, error) {
	prefix := bytes.NewBuffer(make([]byte, 5))
	var encodingByte byte
	if compressed {
		encodingByte = 1
	}
	_ = prefix.WriteByte(encodingByte)
	_ = binary.Write(prefix, binary.BigEndian, int32(len(data)))
	return io.MultiReader(prefix, bytes.NewReader(data)), nil
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

var _ clientProtocolAdapter = connectClientProtocolAdapter{}
