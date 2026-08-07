package httpgrpc

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/mem"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/fullstorydev/grpchan"
	"github.com/fullstorydev/grpchan/internal"
)

// Server is a gRPC-over-HTTP server. It acts as a grpc.ServiceRegistrar,
// for registering server implementations, and also implements http.Handler,
// for exposing the services via HTTP.
type Server struct {
	mux       http.ServeMux
	handlers  grpchan.HandlerMap
	basePath  string
	unaryInt  grpc.UnaryServerInterceptor
	streamInt grpc.StreamServerInterceptor
	opts      handlerOpts
}

var (
	_ http.Handler          = (*Server)(nil)
	_ grpc.ServiceRegistrar = (*Server)(nil)
)

// ServerOption is an option used when constructing a NewServer.
type ServerOption interface {
	apply(*Server)
}

type serverOptFunc func(*Server)

func (fn serverOptFunc) apply(s *Server) {
	fn(s)
}

// WithBasePath configured the gRPC-over-HTTP server to use the given base path.
// The default base path is "/". If the caller mounts the *httpgrpc.Server at
// some sub-path, this can be used to inform the handler of that path. As an
// alternative, the caller could instead use http.StripPrefix so that the
// *httpgrpc.Server does not need to know the sub-path.
func WithBasePath(path string) ServerOption {
	return serverOptFunc(func(s *Server) {
		s.basePath = path
	})
}

// WithServerUnaryInterceptor configures the gRPC-over-HTTP server to use the given
// server interceptor for unary RPCs when dispatching.
func WithServerUnaryInterceptor(interceptor grpc.UnaryServerInterceptor) ServerOption {
	return serverOptFunc(func(s *Server) {
		s.unaryInt = interceptor
	})
}

// WithServerStreamInterceptor configures the gRP-over-HTTP server to use the
// given server interceptor for streaming RPCs when dispatching.
func WithServerStreamInterceptor(interceptor grpc.StreamServerInterceptor) ServerOption {
	return serverOptFunc(func(s *Server) {
		s.streamInt = interceptor
	})
}

// NewServer returns a new gRPC-over-HTTP server. The given options (which can
// include instances of HandlerOption) can be used to customize the server behavior.
func NewServer(opts ...ServerOption) *Server {
	var s Server
	s.basePath = "/"
	s.handlers = grpchan.HandlerMap{}
	for _, o := range opts {
		o.apply(&s)
	}
	return &s
}

// RegisterService registers the given service and implementation. Like a normal
// gRPC server, a gRPC-over-HTTP server only allows a single implementation for a
// particular service. Services are identified by their fully-qualified name
// (e.g. "<package>.<service>").
func (s *Server) RegisterService(desc *grpc.ServiceDesc, svr interface{}) {
	s.handlers.RegisterService(desc, svr)
	for i := range desc.Methods {
		md := desc.Methods[i]
		h := handleMethod(svr, desc.ServiceName, &md, s.unaryInt, &s.opts)
		s.mux.HandleFunc(path.Join(s.basePath, fmt.Sprintf("%s/%s", desc.ServiceName, md.MethodName)), h)
	}
	for i := range desc.Streams {
		sd := desc.Streams[i]
		h := handleStream(svr, desc.ServiceName, &sd, s.streamInt, &s.opts)
		s.mux.HandleFunc(path.Join(s.basePath, fmt.Sprintf("%s/%s", desc.ServiceName, sd.StreamName)), h)
	}
}

// GetServiceInfo returns information about the registered services. This allows
// the channel to implement the reflection.GRPCServer interface (so that a
// gRPC-over-HTTP channel be the source of descriptors for server reflection).
func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	return s.handlers.GetServiceInfo()
}

// ServeHTTP implements http.Handler, allowing the server to be attached to an
// *http.Server, to actually expose the registered servers to HTTP clients.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Mux is a function that can register a gRPC-over-HTTP handler. This is used to
// register handlers in bulk for an RPC service. Its signature matches that of
// the HandleFunc method of the http.ServeMux type, and it also matches that of
// the http.HandleFunc function (for registering handlers with the default mux).
//
// Callers can provide custom Mux functions that further decorate the handler
// (for example, adding authentication checks, logging, error handling, etc).
type Mux func(pattern string, handler func(http.ResponseWriter, *http.Request))

// HandlerOption is an option to customize some aspect of the HTTP handler
// behavior, such as rendering gRPC errors to HTTP responses.
//
// HandlerOptions also implement ServerOption.
type HandlerOption func(*handlerOpts)

func (ho HandlerOption) apply(s *Server) {
	ho(&s.opts)
}

type handlerOpts struct {
	errFunc func(context.Context, *status.Status, http.ResponseWriter)
	// maxRecv and maxSend are zero when no limit was configured, which is also the
	// case for a Server built as a struct literal. They are read through recvLimit
	// and sendLimit so that the defaults apply either way.
	maxRecv int
	maxSend int
}

// recvLimit returns the largest request message the handler will accept.
func (h *handlerOpts) recvLimit() int {
	return limitOrDefault(h.maxRecv, defaultMaxRecvStream)
}

// sendLimit returns the largest response message the handler will produce.
func (h *handlerOpts) sendLimit() int {
	return limitOrDefault(h.maxSend, defaultMaxSend)
}

// ErrorRenderer returns a HandlerOption that will cause the handler to use the
// given function to render an error.  It is only used for unary RPCs since
// streaming RPCs serialize a status message to the response trailer (in the
// HTTP body) instead.
//
// The function should call methods on response in order to write an error
// response, including any response headers, the HTTP status code, and any
// response body.
//
// If no such option is used, the handler will use DefaultErrorRenderer.
func ErrorRenderer(errFunc func(reqCtx context.Context, st *status.Status, response http.ResponseWriter)) HandlerOption {
	return func(h *handlerOpts) {
		h.errFunc = errFunc
	}
}

// WithMaxRecvMsgSize returns a HandlerOption that sets the largest request
// message the handler will accept. A request message larger than this is
// rejected with a ResourceExhausted status, without being read into memory.
//
// A value of zero or less selects the default, which is 100mb. That is the bound
// streaming requests have always had, so a server that uses no such option
// behaves as it did before this option existed.
func WithMaxRecvMsgSize(limit int) HandlerOption {
	return func(h *handlerOpts) {
		h.maxRecv = limit
	}
}

// WithMaxSendMsgSize returns a HandlerOption that sets the largest response
// message the handler will send. Attempting to send a larger one fails the RPC
// with a ResourceExhausted status rather than writing a partial response.
//
// A value of zero or less selects the default, which is the largest message the
// protocol can describe, so a server that uses no such option behaves as it did
// before this option existed.
func WithMaxSendMsgSize(limit int) HandlerOption {
	return func(h *handlerOpts) {
		h.maxSend = limit
	}
}

// DefaultErrorRenderer translates the gRPC code in the given status to an HTTP
// error response. The following table shows how status codes are translated:
//
//	Canceled:         * 502 Bad Gateway
//	Unknown:            500 Internal Server Error
//	InvalidArgument:    400 Bad Request
//	DeadlineExceeded: * 504 Gateway Timeout
//	NotFound:           404 Not Found
//	AlreadyExists:      409 Conflict
//	PermissionDenied:   403 Forbidden
//	Unauthenticated:    401 Unauthorized
//	ResourceExhausted:  429 Too Many Requests
//	FailedPrecondition: 412 Precondition Failed
//	Aborted:            409 Conflict
//	OutOfRange:         422 Unprocessable Entity
//	Unimplemented:      501 Not Implemented
//	Internal:           500 Internal Server Error
//	Unavailable:        503 Service Unavailable
//	DataLoss:           500 Internal Server Error
//
//	* If the gRPC status indicates Canceled or DeadlineExceeded
//	  and the given request context ALSO indicates a context error
//	  (meaning that the request was cancelled by the client), then
//	  a 499 Client Closed Request code is used instead.
//
// If any other gRPC status code is observed, it would get translated into a
// 500 Internal Server Error.
//
// Note that OK is absent from the mapping because the error renderer will never
// be called for a non-error status.
//
// This function uses http.Error to render the computed code (and corresponding
// status text) to the given ResponseWriter.
func DefaultErrorRenderer(ctx context.Context, st *status.Status, w http.ResponseWriter) {
	if (st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded) && ctx.Err() != nil {
		http.Error(w, "Client Closed Request", 499)
		return
	}
	code := httpStatusFromCode(st.Code())
	msg := http.StatusText(code)
	if msg == "" {
		msg = st.Code().String()
	}
	http.Error(w, msg, code)
}

// HandleServices uses the given mux to register handlers for all methods
// exposed by handlers registered in reg. They are registered using a path of
// "basePath/name.of.Service/Method". If non-nil interceptor(s) are provided
// then they will be used to intercept applicable RPCs before dispatch to the
// registered handler.
func HandleServices(mux Mux, basePath string, reg grpchan.HandlerMap, unaryInt grpc.UnaryServerInterceptor, streamInt grpc.StreamServerInterceptor, opts ...HandlerOption) {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}

	reg.ForEach(func(desc *grpc.ServiceDesc, svr interface{}) {
		for i := range desc.Methods {
			md := desc.Methods[i]
			h := handleMethod(svr, desc.ServiceName, &md, unaryInt, &hOpts)
			mux(path.Join(basePath, fmt.Sprintf("%s/%s", desc.ServiceName, md.MethodName)), h)
		}
		for i := range desc.Streams {
			sd := desc.Streams[i]
			h := handleStream(svr, desc.ServiceName, &sd, streamInt, &hOpts)
			mux(path.Join(basePath, fmt.Sprintf("%s/%s", desc.ServiceName, sd.StreamName)), h)
		}
	})
}

// HandleMethod returns an HTTP handler that will handle a unary RPC method
// by dispatching the given method on the given server.
func HandleMethod(svr interface{}, serviceName string, desc *grpc.MethodDesc, unaryInt grpc.UnaryServerInterceptor, opts ...HandlerOption) http.HandlerFunc {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}
	return handleMethod(svr, serviceName, desc, unaryInt, &hOpts)
}

func handleMethod(svr interface{}, serviceName string, desc *grpc.MethodDesc, unaryInt grpc.UnaryServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	errHandler := opts.errFunc
	if errHandler == nil {
		errHandler = DefaultErrorRenderer
	}
	fullMethod := fmt.Sprintf("/%s/%s", serviceName, desc.MethodName)
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if p := peerFromRequest(r); p != nil {
			ctx = peer.NewContext(ctx, p)
		}
		// Draining the body lets the connection be reused, but not when the reason
		// the request failed is that the body was too big to accept: reading the rest
		// of it would pull in the very bytes the limit exists to refuse. Give up the
		// connection instead, which is also what makes the refusal reach the caller
		// promptly, since the response is not written until this handler returns.
		refused := false
		defer func() {
			if refused {
				_ = r.Body.Close()
				return
			}
			_ = drainAndClose(r.Body)
		}()
		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			writeError(w, http.StatusMethodNotAllowed)
			return
		}

		contentType := r.Header.Get("Content-Type")
		codec := getCodecForServerUnaryResponse(contentType)
		if codec == nil {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}

		ctx, cancel, err := contextFromHeaders(ctx, r.Header)
		if err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
		defer cancel()

		// When the request declares its length, an over-large one can be refused
		// without reading any of the body. ContentLength is -1 when it is not known,
		// in which case the limited read below is what catches it.
		maxRecv := opts.recvLimit()
		if r.ContentLength > int64(maxRecv) {
			refused = true
			renderStatus(r.Context(), w, codec, errHandler, status.Errorf(codes.ResourceExhausted,
				"request payload too large to receive: %v > %v", r.ContentLength, maxRecv))
			return
		}
		var body io.Reader = r.Body
		if maxRecv < math.MaxInt32 {
			// One past the limit, so an over-large body is distinguishable from one
			// that is exactly at it.
			body = io.LimitReader(r.Body, int64(maxRecv)+1)
		}
		req, err := ioutil.ReadAll(body)
		if err != nil {
			writeError(w, 499)
			return
		}
		if len(req) > maxRecv {
			refused = true
			renderStatus(r.Context(), w, codec, errHandler, status.Errorf(codes.ResourceExhausted,
				"request payload too large to receive: %v > %v", len(req), maxRecv))
			return
		}

		dec := func(msg interface{}) error {
			if err := codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(req)}, msg); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			return nil
		}
		sts := internal.UnaryServerTransportStream{Name: fullMethod}
		resp, err := desc.Handler(svr, grpc.NewContextWithServerTransportStream(ctx, &sts), dec, unaryInt)
		toHeaders(sts.GetHeaders(), w.Header(), "")
		toHeaders(sts.GetTrailers(), w.Header(), "X-GRPC-Trailer-")
		if err != nil {
			renderStatus(r.Context(), w, codec, errHandler, err)
			return
		}

		buf, err := codec.Marshal(resp)
		if err != nil {
			writeError(w, http.StatusInternalServerError)
			return
		}
		b := buf.Materialize()
		// Checked where the response is encoded, so the limit costs no extra work.
		if maxSend := opts.sendLimit(); len(b) > maxSend {
			renderStatus(r.Context(), w, codec, errHandler, status.Errorf(codes.ResourceExhausted,
				"response payload too large to send: %v > %v", len(b), maxSend))
			return
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
		w.Write(b)
	}
}

// renderStatus reports the given error as the result of a unary RPC: the status
// and any details it carries go into response headers, and the error renderer
// produces the response itself.
func renderStatus(ctx context.Context, w http.ResponseWriter, codec encoding.CodecV2, errHandler func(context.Context, *status.Status, http.ResponseWriter), err error) {
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
		buf, err := codec.Marshal(d)
		if err != nil {
			continue
		}
		b := buf.Materialize()
		str := base64.RawURLEncoding.EncodeToString(b)
		w.Header().Add(grpcDetailsHeader, str)
	}
	errHandler(ctx, st, w)
}

// HandleStream returns an HTTP handler that will handle a streaming RPC method
// by dispatching the given method on the given server.
func HandleStream(svr interface{}, serviceName string, desc *grpc.StreamDesc, streamInt grpc.StreamServerInterceptor, opts ...HandlerOption) http.HandlerFunc {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}
	return handleStream(svr, serviceName, desc, streamInt, &hOpts)
}

func handleStream(svr interface{}, serviceName string, desc *grpc.StreamDesc, streamInt grpc.StreamServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	info := &grpc.StreamServerInfo{
		FullMethod:     fmt.Sprintf("/%s/%s", serviceName, desc.StreamName),
		IsClientStream: desc.ClientStreams,
		IsServerStream: desc.ServerStreams,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if p := peerFromRequest(r); p != nil {
			ctx = peer.NewContext(ctx, p)
		}
		// A stream that refused a message is not drained, for the same reason a unary
		// request is not: what remains is the message the limit rejected.
		var str *serverStream
		defer func() {
			if str != nil && isLimitError(str.finishError()) {
				_ = r.Body.Close()
				return
			}
			_ = drainAndClose(r.Body)
		}()
		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			writeError(w, http.StatusMethodNotAllowed)
			return
		}

		contentType := r.Header.Get("Content-Type")

		// A stream reads and writes at the same time, which is not what net/http's
		// HTTP/1 server expects of a handler: before it will write response headers
		// with the request body unread, it drains up to 256kb of that body so that the
		// connection stays reusable. Draining waits on a peer that is itself waiting
		// for the response, so a handler which answers before it has finished reading
		// deadlocks with its own caller. A stream that ends early always does, since
		// the message that ended it is still on the wire.
		//
		// Turning that off leaves the draining to this handler, which does it on the
		// way out, where nothing is waiting on the result. An error means the
		// ResponseWriter does not offer the choice, which is not fatal: the drain
		// stays, and so does the hazard it poses, exactly as before.
		responseController := http.NewResponseController(w)
		_ = responseController.EnableFullDuplex()

		streamReader, streamWriter, resContentType := getServerStreamReaderAndWriter(
			contentType, r.Body, w, responseController, opts.recvLimit(), opts.sendLimit())
		if streamReader == nil || streamWriter == nil {
			writeError(w, http.StatusUnsupportedMediaType)
			return
		}

		ctx, cancel, err := contextFromHeaders(ctx, r.Header)
		if err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
		defer cancel()

		w.Header().Set("Content-Type", resContentType)

		str = &serverStream{r: r, w: w, respStream: desc.ClientStreams, streamWriter: streamWriter, streamReader: streamReader}
		sts := internal.ServerTransportStream{Name: info.FullMethod, Stream: str}
		str.ctx = grpc.NewContextWithServerTransportStream(ctx, &sts)
		if streamInt != nil {
			err = streamInt(svr, str, info, desc.Handler)
		} else {
			err = desc.Handler(svr, str)
		}
		// The handler's error only ends the stream if nothing else already has. A
		// message that exceeded a size limit ended it where it was refused, and that
		// is the reason worth reporting: whatever the handler made of the error it
		// was given is a symptom of it.
		str.finish(err)
	}
}

func peerFromRequest(r *http.Request) *peer.Peer {
	pr := peer.Peer{Addr: strAddr(r.RemoteAddr)}
	if r.TLS != nil {
		pr.AuthInfo = credentials.TLSInfo{State: *r.TLS}
	}
	return &pr
}

func drainAndClose(r io.ReadCloser) error {
	_, copyErr := io.Copy(ioutil.Discard, r)
	closeErr := r.Close()
	// error from io.Copy likely more useful than the one from Close
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func writeError(w http.ResponseWriter, code int) {
	msg := http.StatusText(code)
	if msg == "" {
		if code == 499 {
			msg = "Client Closed Request"
		} else {
			msg = "Unknown"
		}
	}
	http.Error(w, msg, code)
}

// asTrailerProto converts the given metadata into a map that can be used with
// HttpTrailer to convey trailers back to the caller via a final message in the
// response body.
func asTrailerProto(md metadata.MD) map[string]*TrailerValues {
	result := map[string]*TrailerValues{}
	for k, vs := range md {
		tvs := TrailerValues{}
		tvs.Values = append(tvs.Values, vs...)
		result[k] = &tvs
	}
	return result
}

// serverStream implements a server stream over HTTP 1.1.
//
// It has three mutexes, which are always acquired in the order they are declared
// below: rmu, then wmu, then lmu. Ending the stream is what makes more than one of
// them a possibility, since a message refused while receiving must be reported by
// writing. A method whose name ends in Locked expects a lock to be held already,
// and names which one; every other method takes what it needs itself.
type serverStream struct {
	ctx context.Context
	// respStream is set to indicate whether client expects stream response; unary if false
	respStream bool

	streamWriter streamWriter
	streamReader streamReader

	// rmu serializes access to r and protects recvd
	rmu sync.Mutex
	r   *http.Request
	// recvd tracks the number of request messages received
	recvd int

	// wmu serializes access to w and protects headersSent, writeFailed, and tr
	wmu         sync.Mutex
	w           http.ResponseWriter
	headersSent bool
	writeFailed bool
	tr          []metadata.MD

	// lmu protects finished and finishErr, which are reachable from both
	// directions of the stream and so cannot live under either of the locks above.
	lmu       sync.Mutex
	finished  bool
	finishErr error
}

// finish ends the stream, recording err as its outcome and writing the
// terminating message that reports it. Only the first call has any effect, since
// a stream can only end once and the first reason for it is the one that explains
// the rest; later calls, including the one the handler's return produces, are
// dropped.
//
// A stream that refuses a message for exceeding a size limit ends here, rather
// than waiting for the handler to return. gRPC-Go ends such an RPC outright by
// resetting the HTTP/2 stream, which a handler built on net/http cannot do: the
// only way out is to return, which the handler may not do promptly, or at all.
// But this protocol carries the trailer as a message in the response body rather
// than as an HTTP trailer, so it can be written the moment the stream ends
// instead of whenever the handler gets around to returning. Every later operation
// on the stream then fails with the recorded error, which is enough to unwind a
// handler that checks what it is told, and a handler that does not check costs
// the caller nothing but the connection.
//
// Reporting the recorded outcome in preference to the handler's is also what
// gRPC-Java gets wrong in https://github.com/grpc/grpc-java/issues/11245, where a
// stream reset can race the trailers and leave the client with CANCELLED instead
// of RESOURCE_EXHAUSTED.
//
// One thing is lost: trailer metadata the handler adds after the stream has ended
// never reaches the caller, because the message it would have travelled in is
// already written. gRPC-Go loses it too, once a status is out.
//
// The caller must hold neither wmu nor lmu; see finishLocked for a caller that
// already holds wmu.
func (s *serverStream) finish(err error) {
	if !s.recordFinish(err) {
		return
	}

	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.writeTrailerLocked(err)
}

// finishLocked is finish for a caller that already holds wmu. The caller must not
// hold lmu.
func (s *serverStream) finishLocked(err error) {
	if !s.recordFinish(err) {
		return
	}

	s.writeTrailerLocked(err)
}

// recordFinish records the outcome of the stream, reporting whether this call is
// the one that ended it and so owes the caller a terminating message. It takes
// lmu, which is why it does not hold it while the message is written: doing so
// would put a socket write inside the lock that both directions of the stream
// contend for.
func (s *serverStream) recordFinish(err error) bool {
	s.lmu.Lock()
	defer s.lmu.Unlock()

	if s.finished {
		return false
	}
	s.finished = true
	s.finishErr = err
	return true
}

// finishError returns the error the stream ended with, or nil if it has not ended
// or ended without one.
func (s *serverStream) finishError() error {
	s.lmu.Lock()
	defer s.lmu.Unlock()

	return s.finishErr
}

// writeTrailerLocked writes the final message of the response, which carries the
// status of the RPC and any trailer metadata. The caller must hold wmu, and must
// have won the race in recordFinish, so that exactly one such message is written.
func (s *serverStream) writeTrailerLocked(err error) {
	if s.writeFailed {
		// A stream whose last write failed is in no state to report anything.
		return
	}

	tr := HttpTrailer{
		Code:     int32(codes.OK),
		Message:  codes.OK.String(),
		Metadata: asTrailerProto(metadata.Join(s.tr...)),
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

	s.headersSent = true // sent implicitly, if they were not already
	_ = s.streamWriter(&tr, true)
}

func (s *serverStream) SetHeader(md metadata.MD) error {
	return s.setHeader(md, false)
}

func (s *serverStream) SendHeader(md metadata.MD) error {
	return s.setHeader(md, true)
}

func (s *serverStream) setHeader(md metadata.MD, send bool) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if s.headersSent {
		return errors.New("headers already sent")
	}

	h := s.w.Header()
	toHeaders(md, h, "")

	if send {
		s.w.WriteHeader(http.StatusOK)
		s.headersSent = true
	}

	return nil
}

func (s *serverStream) SetTrailer(md metadata.MD) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.tr = append(s.tr, md)
}

func (s *serverStream) Context() context.Context {
	return s.ctx
}

func (s *serverStream) SendMsg(m interface{}) error {
	if err := s.finishError(); err != nil {
		return err
	}

	s.wmu.Lock()
	defer s.wmu.Unlock()

	if s.writeFailed {
		// strange, but simulates what happens in real GRPC: stream
		// is closed after a write failure, and trying to send message
		// on a closed stream returns EOF
		return io.EOF
	}

	s.headersSent = true // sent implicitly
	err := s.streamWriter(m, false)
	if isLimitError(err) {
		// A refused message was never written, so the stream is still able to
		// carry a status; it ends here rather than being marked as failed, which
		// would suppress the trailer and leave the caller with a truncated
		// response instead of an explanation.
		s.finishLocked(err)
	} else if err != nil {
		s.writeFailed = true
	}
	return err
}

func (s *serverStream) RecvMsg(m interface{}) error {
	if err := s.finishError(); err != nil {
		return err
	}

	s.rmu.Lock()
	defer s.rmu.Unlock()

	if !s.respStream && s.recvd > 0 {
		return io.EOF
	}

	s.recvd++

	msg, err := s.streamReader()
	if err != nil {
		if isLimitError(err) {
			// The rest of the request body belongs to a message that was refused, so
			// there is no way to find the start of the next one. Ending the stream is
			// the only correct answer, and is what gRPC-Go does here as well.
			s.finish(err)
		}
		return err
	}

	if err := msg.Decode(m); err != nil {
		return err
	}

	if !s.respStream {
		if _, err := s.streamReader(); err != io.EOF {
			// client tried to send >1 message!
			return status.Error(codes.InvalidArgument, "method accepts 1 request message but client sent >1")
		}
	}

	return nil
}

// contextFromHeaders returns a child of the given context that is populated
// using the given headers. The headers are converted to incoming metadata that
// can be retrieved via metadata.FromIncomingContext. If the headers contain a
// GRPC timeout, that is used to create a timeout for the returned context.
func contextFromHeaders(parent context.Context, h http.Header) (context.Context, context.CancelFunc, error) {
	cancel := func() {} // default to no-op
	md, err := asMetadata(h)
	if err != nil {
		return parent, cancel, err
	}
	ctx := metadata.NewIncomingContext(parent, md)

	// deadline propagation
	timeout := h.Get("GRPC-Timeout")
	if timeout != "" {
		// See GRPC wire format, "Timeout" component of request: https://grpc.io/docs/guides/wire.html#requests
		suffix := timeout[len(timeout)-1]
		if timeoutVal, err := strconv.ParseInt(timeout[:len(timeout)-1], 10, 64); err == nil {
			var unit time.Duration
			switch suffix {
			case 'H':
				unit = time.Hour
			case 'M':
				unit = time.Minute
			case 'S':
				unit = time.Second
			case 'm':
				unit = time.Millisecond
			case 'u':
				unit = time.Microsecond
			case 'n':
				unit = time.Nanosecond
			}
			if unit != 0 {
				ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutVal)*unit)
			}
		}
	}
	return ctx, cancel, nil
}
