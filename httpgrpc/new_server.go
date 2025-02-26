package httpgrpc

import (
	"bytes"
	"fmt"
	"google.golang.org/grpc/encoding"
	"io"
	"net/http"
	"path"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/fullstorydev/grpchan"
	"github.com/fullstorydev/grpchan/internal"
)

// ServerV2 is a gRPC-over-HTTP server. It acts as a grpc.ServiceRegistrar,
// for registering server implementations, and also implements http.Handler,
// for exposing the services via HTTP.
type ServerV2 Server

// NewServerV2 returns a new gRPC-over-HTTP server. The given options (which can
// include instances of HandlerOption) can be used to customize the server behavior.
func NewServerV2(opts ...ServerOption) *ServerV2 {
	var s ServerV2
	s.basePath = "/"
	s.handlers = grpchan.HandlerMap{}
	for _, o := range opts {
		o.apply((*Server)(&s))
	}
	return &s
}

// RegisterService registers the given service and implementation. Like a normal
// gRPC server, a gRPC-over-HTTP server only allows a single implementation for a
// particular service. Services are identified by their fully-qualified name
// (e.g. "<package>.<service>").
func (s *ServerV2) RegisterService(desc *grpc.ServiceDesc, svr interface{}) {
	s.handlers.RegisterService(desc, svr)
	for i := range desc.Methods {
		md := desc.Methods[i]
		h := handleMethodV2(svr, desc.ServiceName, &md, s.unaryInt, &s.opts)
		s.mux.HandleFunc(path.Join(s.basePath, fmt.Sprintf("%s/%s", desc.ServiceName, md.MethodName)), h)
	}
	for i := range desc.Streams {
		sd := desc.Streams[i]
		h := handleStreamV2(svr, desc.ServiceName, &sd, s.streamInt, &s.opts)
		s.mux.HandleFunc(path.Join(s.basePath, fmt.Sprintf("%s/%s", desc.ServiceName, sd.StreamName)), h)
	}
}

func handleMethodV2(svr interface{}, serviceName string, desc *grpc.MethodDesc, unaryInt grpc.UnaryServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	errHandler := opts.errFunc
	if errHandler == nil {
		errHandler = DefaultErrorRenderer
	}
	fullMethod := fmt.Sprintf("/%s/%s", serviceName, desc.MethodName)
	return func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		protocol, subFormatType, allowUnary, _, err := determineProtocolAdapter(contentType, r, *opts)
		if !allowUnary {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}

		ctx := r.Context()
		if p := peerFromRequest(r); p != nil {
			ctx = peer.NewContext(ctx, p)
		}
		defer drainAndClose(r.Body)
		if r.Method != "POST" {
			// TODO: Connect GET
			w.Header().Set("Allow", "POST")
			writeError(w, http.StatusMethodNotAllowed)
			return
		}

		codec := encoding.GetCodec(subFormatType)
		if codec == nil {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}

		ctx, cancel, compression, supportedCompression, err := protocol.processHeaders(ctx, r.Header)
		if err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
		defer cancel()

		var compressor encoding.Compressor
		if compression != "" && compression != encoding.Identity {
			compressor = encoding.GetCompressor(compression)
			if compressor == nil {
				// TODO: use adapter.finishUnary...
				writeError(w, http.StatusUnsupportedMediaType)
				return
			}
		}

		reader, compressed, err := protocol.processUnaryRequest(r.Body)
		if err != nil {
			// TODO: use adapter.finishUnary...
			writeError(w, 499)
			return
		}

		if compressed && compressor != nil {
			decomp, err := compressor.Decompress(reader)
			if err != nil {
				// TODO: use adapter.finishUnary...
				writeError(w, 499)
				return
			}
			reader = struct {
				io.Reader
				io.Closer
			}{
				decomp,
				reader,
			}
		}

		req, err := io.ReadAll(reader)
		if err != nil {
			// TODO: use adapter.finishUnary...
			writeError(w, 499)
			return
		}

		dec := func(msg interface{}) error {
			if err := codec.Unmarshal(req, msg); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			return nil
		}
		sts := internal.UnaryServerTransportStream{Name: fullMethod}
		resp, err := desc.Handler(svr, grpc.NewContextWithServerTransportStream(ctx, &sts), dec, unaryInt)
		if err != nil {
			protocol.responseHeaders(false, subFormatType, "", sts.GetHeaders(), w.Header())
			protocol.finishUnary(ctx, err, sts.GetTrailers(), w)
			return
		}
		b, err := codec.Marshal(resp)
		if err != nil {
			// TODO: use adapter.finishUnary
			writeError(w, http.StatusInternalServerError)
			return
		}

		if compressor == nil {
			for _, comp := range supportedCompression {
				compressor = encoding.GetCompressor(comp)
				if compressor != nil {
					compression = comp
					break
				}
			}
		}
		if compressor != nil {
			var buf bytes.Buffer
			wc, err := compressor.Compress(&buf)
			if err != nil {
				// TODO: use adapter.finishUnary
				writeError(w, http.StatusInternalServerError)
				return
			}
			_, err = wc.Write(b)
			closeErr := wc.Close()
			if err == nil && closeErr != nil {
				err = closeErr
			}
			if err != nil {
				// TODO: use adapter.finishUnary
				writeError(w, http.StatusInternalServerError)
				return
			}
			b = buf.Bytes()
		}

		parts, err := protocol.unaryMessage(b, compressor != nil)
		if err != nil {
			// TODO: use adapter.finishUnary
			writeError(w, http.StatusInternalServerError)
			return
		}
		var length int
		for _, part := range parts {
			length += len(part)
		}

		protocol.responseHeaders(false, subFormatType, compression, sts.GetHeaders(), w.Header())
		w.Header().Set("Content-Length", fmt.Sprintf("%d", length))
		for _, part := range parts {
			w.Write(part)
		}
		protocol.finishUnary(ctx, nil, sts.GetTrailers(), w)
	}
}

func handleStreamV2(svr interface{}, serviceName string, desc *grpc.StreamDesc, streamInt grpc.StreamServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	info := &grpc.StreamServerInfo{
		FullMethod:     fmt.Sprintf("/%s/%s", serviceName, desc.StreamName),
		IsClientStream: desc.ClientStreams,
		IsServerStream: desc.ServerStreams,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		protocol, subFormatType, _, allowStream, err := determineProtocolAdapter(contentType, r, *opts)
		if !allowStream {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}
		ctx := r.Context()
		if p := peerFromRequest(r); p != nil {
			ctx = peer.NewContext(ctx, p)
		}
		defer drainAndClose(r.Body)
		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			writeError(w, http.StatusMethodNotAllowed)
			return
		}

		codec := encoding.GetCodec(subFormatType)
		if codec == nil {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}

		ctx, cancel, compression, supportedCompression, err := protocol.processHeaders(ctx, r.Header)
		if err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
		defer cancel()

		// TODO: move to stream.sendHeader
		protocol.responseHeaders(true, subFormatType, compression, nil, w.Header())

		str := &serverStream{r: r, w: w, respStream: desc.ClientStreams, codec: codec}
		sts := internal.ServerTransportStream{Name: info.FullMethod, Stream: str}
		str.ctx = grpc.NewContextWithServerTransportStream(ctx, &sts)
		if streamInt != nil {
			err = streamInt(svr, str, info, desc.Handler)
		} else {
			err = desc.Handler(svr, str)
		}
		if str.writeFailed {
			// nothing else we can do
			return
		}

		tr := HttpTrailer{
			Code:     int32(codes.OK),
			Message:  codes.OK.String(),
			Metadata: asTrailerProto(metadata.Join(str.tr...)),
		}
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.OK {
				// preserve all error details, but rewrite the code since we don't want
				// to send back a non-error status when we know an error occured
				stpb := st.Proto()
				stpb.Code = int32(codes.Internal)
				st = status.FromProto(stpb)
			}
			statProto := st.Proto()
			tr.Code = statProto.Code
			tr.Message = statProto.Message
			tr.Details = statProto.Details
		}

		writeProtoMessage(w, codec, &tr, true)
	}
}
