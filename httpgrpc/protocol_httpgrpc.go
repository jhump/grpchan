package httpgrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"strings"
)

type httpgrpcClientProtocolAdapter struct{}

var _ clientProtocolAdapter = httpgrpcClientProtocolAdapter{}

func (h httpgrpcClientProtocolAdapter) unaryMessage(data []byte, _ bool) (io.Reader, error) {
	return bytes.NewReader(data), nil
}

func (h httpgrpcClientProtocolAdapter) streamMessage(data []byte, _ bool) (io.Reader, error) {
	prefix := bytes.NewBuffer(make([]byte, 4))
	_ = binary.Write(prefix, binary.BigEndian, int32(len(data)))
	return io.MultiReader(prefix, bytes.NewReader(data)), nil
}

func (h httpgrpcClientProtocolAdapter) supportsCompression() bool {
	return false
}

func (h httpgrpcClientProtocolAdapter) requestHeaders(ctx context.Context, isStream bool, codecName string, _ string, _ []string) (http.Header, error) {
	var contentType string
	switch codecName {
	case grpcproto.Name:
		if isStream {
			contentType = StreamRpcContentType_V1
		} else {
			contentType = UnaryRpcContentType_V1
		}
	case "json":
		if isStream {
			return nil, fmt.Errorf("httpgrpc protocol does not support a streaming RPC with JSON format")
		}
		contentType = ApplicationJson
	default:
		return nil, fmt.Errorf("httpgrpc protocol does not support the given codec name: %v", codecName)
	}
	hdrs := headersFromContext(ctx)
	hdrs.Set("Content-Type", contentType)
	return hdrs, nil
}

func (h httpgrpcClientProtocolAdapter) processUnaryResponse(resp *http.Response) (metadata.MD, io.Reader, bool, metadata.MD, error) {
	hdr, err := asMetadata(resp.Header)
	if err != nil {
		return nil, nil, false, nil, err
	}
	tlr := metadata.MD{}

	const trailerPrefix = "x-grpc-trailer-"

	for k, v := range hdr {
		if strings.HasPrefix(strings.ToLower(k), trailerPrefix) {
			trailerName := k[len(trailerPrefix):]
			if trailerName != "" {
				tlr[trailerName] = v
				delete(hdr, k)
			}
		}
	}

	stat := statFromResponse(resp)
	return hdr, resp.Body, false, tlr, stat.Err()
}

func (h httpgrpcClientProtocolAdapter) processStreamHeaders(resp *http.Response) (metadata.MD, error) {
	md, err := asMetadata(resp.Header)
	if err != nil {
		return nil, err
	}
	stat := statFromResponse(resp)
	return md, stat.Err()
}

func (h httpgrpcClientProtocolAdapter) readStreamResponse(r io.ReadCloser) (io.Reader, bool, metadata.MD, error) {
	sz, err := readSizePreface(r)
	if err != nil {
		return nil, false, nil, err
	}
	if sz < 0 {
		// final message is a trailer (need lock to write to cs.tr)
		var tr HttpTrailer
		err := readProtoMessage(r, encoding.GetCodec(grpcproto.Name), -sz, &tr)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, false, nil, err
		}
		var trailers metadata.MD
		if len(tr.Metadata) > 0 {
			trailers = metadataFromProto(tr.Metadata)
		}
		return nil, false, trailers, status.ErrorProto(&spb.Status{
			Code:    tr.Code,
			Message: tr.Message,
			Details: tr.Details,
		})
	}
	msg := make([]byte, sz)
	_, err = io.ReadAtLeast(r, msg, int(sz))
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, false, nil, err
	}
	return bytes.NewReader(msg), false, nil, nil
}

type httpgrpcServerProtocolAdapter struct {
	errFunc func(context.Context, *status.Status, http.ResponseWriter)
	jsonErr bool
}

var _ serverProtocolAdapter = &httpgrpcServerProtocolAdapter{}

func (h *httpgrpcServerProtocolAdapter) unaryMessage(data []byte, _ bool) (io.Reader, error) {
	return bytes.NewReader(data), nil
}

func (h *httpgrpcServerProtocolAdapter) streamMessage(data []byte, _ bool) (io.Reader, error) {
	var prefix bytes.Buffer
	_ = binary.Write(&prefix, binary.BigEndian, int32(len(data)))
	return io.MultiReader(&prefix, bytes.NewReader(data)), nil
}

func (h *httpgrpcServerProtocolAdapter) processHeaders(ctx context.Context, header http.Header) (_ context.Context, _ context.CancelFunc, compressorName string, supportedCompressors []string, _ error) {
	ctx, cancel, err := contextFromHeaders(ctx, header)
	return ctx, cancel, "", nil, err
}

func (h *httpgrpcServerProtocolAdapter) responseHeaders(_ bool, _ string, _ string, md metadata.MD, targetHeaders http.Header) {
	toHeaders(md, targetHeaders, "")
}

func (h *httpgrpcServerProtocolAdapter) processUnaryRequest(r io.ReadCloser) (io.ReadCloser, bool, error) {
	return r, false, nil
}

func (h *httpgrpcServerProtocolAdapter) finishUnary(ctx context.Context, err error, trailers metadata.MD, w http.ResponseWriter) {
	toHeaders(trailers, w.Header(), "X-GRPC-Trailer-")
	if err != nil {
		errHandler := h.errFunc
		if errHandler == nil {
			errHandler = DefaultErrorRenderer
		}
		var codec encoding.Codec
		if h.jsonErr {
			codec = encoding.GetCodec("json")
		} else {
			codec = encoding.GetCodec(grpcproto.Name)
		}
		st, _ := status.FromError(err)
		if st.Code() == codes.OK {
			// preserve all error details, but rewrite the code since we don't want
			// to send back a non-error status when we know an error occured
			stpb := st.Proto()
			stpb.Code = int32(codes.Internal)
			st = status.FromProto(stpb)
		}
		statProto := st.Proto()
		w.Header().Set("X-GRPC-Status", fmt.Sprintf("%d:%s", statProto.Code, statProto.Message))
		for _, d := range statProto.Details {
			b, err := codec.Marshal(d)
			if err != nil {
				continue
			}
			str := base64.RawURLEncoding.EncodeToString(b)
			w.Header().Add(grpcDetailsHeader, str)
		}
		errHandler(ctx, st, w)
	}
}

func (h *httpgrpcServerProtocolAdapter) readStreamRequest(r io.ReadCloser) (io.Reader, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (h *httpgrpcServerProtocolAdapter) finishStream(err error, trailers metadata.MD, w http.ResponseWriter) {
	//TODO implement me
	panic("implement me")
}
