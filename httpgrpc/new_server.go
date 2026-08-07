package httpgrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
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

// GetServiceInfo returns information about the registered services. This allows
// the channel to implement the reflection.GRPCServer interface (so that a
// gRPC-over-HTTP channel be the source of descriptors for server reflection).
func (s *ServerV2) GetServiceInfo() map[string]grpc.ServiceInfo {
	return s.handlers.GetServiceInfo()
}

// ServeHTTP implements http.Handler, allowing the server to be attached to an
// *http.Server, to actually expose the registered servers to HTTP clients.
func (s *ServerV2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// HandleServicesV2 uses the given mux to register handlers for all methods
// exposed by handlers registered in reg. They are registered using a path of
// "basePath/name.of.Service/Method". If non-nil interceptor(s) are provided
// then they will be used to intercept applicable RPCs before dispatch to the
// registered handler.
func HandleServicesV2(mux Mux, basePath string, reg grpchan.HandlerMap, unaryInt grpc.UnaryServerInterceptor, streamInt grpc.StreamServerInterceptor, opts ...HandlerOption) {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}

	reg.ForEach(func(desc *grpc.ServiceDesc, svr interface{}) {
		for i := range desc.Methods {
			md := desc.Methods[i]
			h := handleMethodV2(svr, desc.ServiceName, &md, unaryInt, &hOpts)
			mux(path.Join(basePath, fmt.Sprintf("%s/%s", desc.ServiceName, md.MethodName)), h)
		}
		for i := range desc.Streams {
			sd := desc.Streams[i]
			h := handleStreamV2(svr, desc.ServiceName, &sd, streamInt, &hOpts)
			mux(path.Join(basePath, fmt.Sprintf("%s/%s", desc.ServiceName, sd.StreamName)), h)
		}
	})
}

// HandleMethodV2 returns an HTTP handler that will handle a unary RPC method
// by dispatching the given method on the given server.
func HandleMethodV2(svr interface{}, serviceName string, desc *grpc.MethodDesc, unaryInt grpc.UnaryServerInterceptor, opts ...HandlerOption) http.HandlerFunc {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}
	return handleMethodV2(svr, serviceName, desc, unaryInt, &hOpts)
}

func handleMethodV2(svr interface{}, serviceName string, desc *grpc.MethodDesc, unaryInt grpc.UnaryServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	fullMethod := fmt.Sprintf("/%s/%s", serviceName, desc.MethodName)
	return func(w http.ResponseWriter, r *http.Request) {
		call, err := beginCall(w, r, opts, false)
		if call == nil {
			return
		}
		defer drainAndClose(r.Body)
		defer call.cancel()

		protocol, ctx := call.protocol, call.ctx
		fail := func(err error) {
			protocol.writeUnary(ctx, unaryReply{status: errorStatus(err)})
		}
		if err != nil {
			fail(err)
			return
		}

		req, err := protocol.processUnaryRequest()
		if err != nil {
			fail(status.Error(codes.Canceled, err.Error()))
			return
		}

		dec := func(msg interface{}) error {
			if err := call.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(req)}, msg); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			return nil
		}
		sts := internal.UnaryServerTransportStream{Name: fullMethod}
		resp, err := desc.Handler(svr, grpc.NewContextWithServerTransportStream(ctx, &sts), dec, unaryInt)
		if err != nil {
			protocol.writeUnary(ctx, unaryReply{
				headers:  sts.GetHeaders(),
				status:   errorStatus(err),
				trailers: sts.GetTrailers(),
			})
			return
		}
		respBuf, err := call.codec.Marshal(resp)
		if err != nil {
			fail(status.Errorf(codes.Internal, "could not encode response: %v", err))
			return
		}

		protocol.writeUnary(ctx, unaryReply{
			headers:  sts.GetHeaders(),
			message:  respBuf.Materialize(),
			trailers: sts.GetTrailers(),
		})
	}
}

// HandleStreamV2 returns an HTTP handler that will handle a streaming RPC method
// by dispatching the given method on the given server.
func HandleStreamV2(svr interface{}, serviceName string, desc *grpc.StreamDesc, streamInt grpc.StreamServerInterceptor, opts ...HandlerOption) http.HandlerFunc {
	var hOpts handlerOpts
	for _, opt := range opts {
		opt(&hOpts)
	}
	return handleStreamV2(svr, serviceName, desc, streamInt, &hOpts)
}

func handleStreamV2(svr interface{}, serviceName string, desc *grpc.StreamDesc, streamInt grpc.StreamServerInterceptor, opts *handlerOpts) http.HandlerFunc {
	info := &grpc.StreamServerInfo{
		FullMethod:     fmt.Sprintf("/%s/%s", serviceName, desc.StreamName),
		IsClientStream: desc.ClientStreams,
		IsServerStream: desc.ServerStreams,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		call, err := beginCall(w, r, opts, true)
		if call == nil {
			return
		}
		defer drainAndClose(r.Body)
		defer call.cancel()

		if err != nil {
			call.protocol.sendStreamHeaders(nil)
			call.protocol.finishStream(errorStatus(err), nil)
			return
		}

		str := &serverStreamV2{
			protocol:  call.protocol,
			codec:     call.codec,
			w:         w,
			reqStream: desc.ClientStreams,
		}
		sts := internal.ServerTransportStream{Name: info.FullMethod, Stream: str}
		str.ctx = grpc.NewContextWithServerTransportStream(call.ctx, &sts)
		if streamInt != nil {
			err = streamInt(svr, str, info, desc.Handler)
		} else {
			err = desc.Handler(svr, str)
		}
		str.finish(err)
	}
}

// serverCall is what serving a request involves before it matters whether the
// method is unary or streaming: the protocol the request is in, the codec its
// messages use, and the context it is served in.
type serverCall struct {
	protocol serverProtocolAdapter
	codec    encoding.CodecV2
	ctx      context.Context
	// cancel releases the deadline the client asked for, if any. It is never nil.
	cancel context.CancelFunc
}

// beginCall matches a request to a protocol and checks that the method can be
// served in it.
//
// It returns nil if no protocol claimed the request, having already answered it:
// there is no protocol in whose error format to report that, so it gets a bare
// HTTP error. Otherwise, a protocol was recognized, so every failure from here on
// is reported in that protocol's error format, which would otherwise leave the
// client without an RPC status. A non-nil error is such a failure, and the call
// is still valid for reporting it.
func beginCall(w http.ResponseWriter, r *http.Request, opts *handlerOpts, isStream bool) (*serverCall, error) {
	match := determineProtocolAdapter(r, w, *opts)
	if match.adapter == nil {
		writeError(w, http.StatusUnsupportedMediaType)
		return nil, nil
	}

	call := &serverCall{protocol: match.adapter, ctx: r.Context(), cancel: func() {}}
	if p := peerFromRequest(r); p != nil {
		call.ctx = peer.NewContext(call.ctx, p)
	}

	kind, allowed := "unary", match.allowUnary
	if isStream {
		kind, allowed = "streaming", match.allowStream
	}
	if !allowed {
		return call, status.Errorf(codes.Unimplemented,
			"content-type %q cannot be used for a %s RPC", r.Header.Get("Content-Type"), kind)
	}
	if !match.allowsMethod(r.Method) {
		w.Header().Set("Allow", strings.Join(match.methods, ", "))
		return call, status.Errorf(codes.Unimplemented,
			"%s is not allowed, use %s", r.Method, strings.Join(match.methods, " or "))
	}

	call.codec = encoding.GetCodecV2(match.subFormat)
	if call.codec == nil {
		return call, status.Errorf(codes.Unimplemented, "unsupported message format: %q", match.subFormat)
	}

	reqInfo, err := call.protocol.processHeaders(r.Header)
	if err != nil {
		return call, status.Error(codes.InvalidArgument, err.Error())
	}
	call.ctx = metadata.NewIncomingContext(call.ctx, reqInfo.metadata)
	if reqInfo.hasTimeout {
		call.ctx, call.cancel = context.WithTimeout(call.ctx, reqInfo.timeout)
	}
	return call, nil
}

// errorStatus returns the status that reports the given error, or nil if there
// is no error. An error that claims to be OK is reported as Internal, keeping its
// details, since a failed RPC must not reach the client as a success.
func errorStatus(err error) *status.Status {
	if err == nil {
		return nil
	}
	st, _ := status.FromError(err)
	if st.Code() == codes.OK {
		stpb := st.Proto()
		stpb.Code = int32(codes.Internal)
		st = status.FromProto(stpb)
	}
	return st
}

// serverStreamV2 implements grpc.ServerStream on top of a serverProtocolAdapter.
// Unlike serverStream, it holds no knowledge of how messages are framed or how a
// stream is terminated: it encodes and decodes messages with a codec, and defers
// framing to the adapter, so that each protocol can frame differently.
type serverStreamV2 struct {
	ctx      context.Context
	protocol serverProtocolAdapter
	codec    encoding.CodecV2
	// reqStream indicates whether the client may send more than one message.
	reqStream bool

	// rmu serializes reads and protects recvd
	rmu sync.Mutex
	// recvd tracks the number of request messages received
	recvd int

	// wmu serializes writes and protects hd, headersSent, writeFailed, and tr
	wmu sync.Mutex
	w   http.ResponseWriter
	// hd accumulates header metadata until the headers are sent
	hd          []metadata.MD
	headersSent bool
	writeFailed bool
	tr          []metadata.MD
}

var _ grpc.ServerStream = (*serverStreamV2)(nil)

// finish ends the stream once the handler has returned, writing the terminating
// message that reports err as its outcome.
func (s *serverStreamV2) finish(err error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.writeTrailerLocked(err)
}

// writeTrailerLocked writes the final frame of the response, which carries the
// status of the RPC and any trailer metadata. The caller must hold wmu.
func (s *serverStreamV2) writeTrailerLocked(err error) {
	if s.writeFailed {
		// A stream whose last write failed is in no state to report anything.
		return
	}

	// A handler that sent no message left the response headers unsent, and the
	// trailing frame must not be the thing that commits them: the adapter has to
	// label the body first.
	s.sendHeadersLocked()

	// The protocols handled here all encode the final status and trailing metadata
	// into the response body, so the adapter writes that last frame.
	s.protocol.finishStream(errorStatus(err), metadata.Join(s.tr...))
}

func (s *serverStreamV2) SetHeader(md metadata.MD) error {
	return s.setHeader(md, false)
}

func (s *serverStreamV2) SendHeader(md metadata.MD) error {
	return s.setHeader(md, true)
}

func (s *serverStreamV2) setHeader(md metadata.MD, send bool) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if s.headersSent {
		return errors.New("headers already sent")
	}

	// Hold the metadata rather than encoding it now: how header metadata is
	// represented is the protocol's business, so it is handed to the adapter in
	// one go when the headers are actually sent.
	s.hd = append(s.hd, md)

	if send {
		s.sendHeadersLocked()
	}

	return nil
}

func (s *serverStreamV2) SetTrailer(md metadata.MD) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.tr = append(s.tr, md)
}

func (s *serverStreamV2) Context() context.Context {
	return s.ctx
}

func (s *serverStreamV2) SendMsg(m interface{}) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if s.writeFailed {
		// strange, but simulates what happens in real GRPC: stream
		// is closed after a write failure, and trying to send message
		// on a closed stream returns EOF
		return io.EOF
	}

	s.sendHeadersLocked() // sending a message sends the headers implicitly
	err := s.sendMsgLocked(m)
	if err != nil {
		s.writeFailed = true
	}
	return err
}

func (s *serverStreamV2) RecvMsg(m interface{}) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	if !s.reqStream && s.recvd > 0 {
		return io.EOF
	}

	s.recvd++

	if err := s.recvMsgLocked(m); err != nil {
		return err
	}

	if !s.reqStream {
		if _, err := s.protocol.readStreamRequest(); err != io.EOF {
			// client tried to send >1 message!
			return status.Error(codes.InvalidArgument, "method accepts 1 request message but client sent >1")
		}
	}

	return nil
}

// sendHeadersLocked hands the accumulated header metadata to the adapter, which
// labels the response body and commits the status line. Once this has run, no
// further header may be set. It is a no-op if the headers are already sent.
func (s *serverStreamV2) sendHeadersLocked() {
	if s.headersSent {
		return
	}
	s.headersSent = true
	s.protocol.sendStreamHeaders(metadata.Join(s.hd...))
}

func (s *serverStreamV2) sendMsgLocked(m interface{}) error {
	buf, err := s.codec.Marshal(m)
	if err != nil {
		return err
	}
	b := buf.Materialize()
	frames, err := s.protocol.streamMessage(b)
	if err != nil {
		return err
	}
	if err := writeFrames(s.w, frames); err != nil {
		return err
	}
	// Push the message out now rather than letting it sit in a buffer waiting for
	// the next one, which would defeat streaming (and, for server-sent events,
	// stall the client).
	flushResponse(s.w)
	return nil
}

func (s *serverStreamV2) recvMsgLocked(m interface{}) error {
	data, err := s.protocol.readStreamRequest()
	if err != nil {
		return err
	}
	return s.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, m)
}

// timeoutFromHeaders returns the timeout named by the GRPC-Timeout header, if
// there is a valid one. The format is gRPC's: see the "Timeout" component of a
// request at https://grpc.io/docs/guides/wire.html#requests.
func timeoutFromHeaders(h http.Header) (time.Duration, bool) {
	timeout := h.Get("GRPC-Timeout")
	if timeout == "" {
		return 0, false
	}
	suffix := timeout[len(timeout)-1]
	timeoutVal, err := strconv.ParseInt(timeout[:len(timeout)-1], 10, 64)
	if err != nil {
		return 0, false
	}
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
	default:
		return 0, false
	}
	return time.Duration(timeoutVal) * unit, true
}
