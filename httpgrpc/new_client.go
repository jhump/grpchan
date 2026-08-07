package httpgrpc

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/fullstorydev/grpchan/internal"
)

// ChannelV2Option is a function that can be used to configure a ChannelV2.
type ChannelV2Option func(*channelV2Options) error

type channelV2Options struct {
	// codecName is the name the codec was looked up by. It selects the content type
	// of a request and, with the grpc-over-http protocol defined in this package,
	// also how streams are framed.
	//
	// We preserve this instead of using codec.Name() because this value is
	// already normalized and safe to compare, case-sensitive, with just ==.
	codecName string
	codec     encoding.CodecV2

	// newProtocol creates this RPC's protocol adapter. A nil value means the
	// httpgrpc protocol, which is the only one implemented so far; it is the seam
	// through which gRPC-Web and ConnectRPC will be selected. See newAdapter.
	newProtocol clientProtocolAdapterFactory
}

func (o *channelV2Options) setCodec(name string) error {
	codec, err := codecByName(name)
	if err != nil {
		return err
	}
	o.codecName, o.codec = name, codec
	return nil
}

func defaultChannelV2Options() (channelV2Options, error) {
	var opts channelV2Options
	// Default to protobuf binary encoding.
	if err := opts.setCodec(grpcproto.Name); err != nil {
		return channelV2Options{}, err
	}
	return opts, nil
}

// WithJSONEncodingV2 configures the channel to use JSON encoding between the client and server.
// For unary calls, the request and response are JSON values. For streaming calls, the request is
// a series of JSON values and the response is SSE events containing JSON values.
func WithJSONEncodingV2(useJSONEncoding bool) ChannelV2Option {
	return func(o *channelV2Options) error {
		name := grpcproto.Name
		if useJSONEncoding {
			name = jsonCodecName
		}
		// Resolved in both directions, so that a later WithJSONEncodingV2(false) undoes
		// an earlier WithJSONEncodingV2(true) rather than leaving JSON in place.
		return o.setCodec(name)
	}
}

// NewChannelV2 creates a new ChannelV2 with the given base URL and transport, both of
// which are required. The ChannelV2Option functions can be used to configure the
// ChannelV2. The error reports a channel that cannot be configured as asked.
func NewChannelV2(baseURL *url.URL, transport http.RoundTripper, opts ...ChannelV2Option) (*ChannelV2, error) {
	if err := checkChannelParams(baseURL, transport); err != nil {
		return nil, err
	}
	chOpts, err := defaultChannelV2Options()
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if err := opt(&chOpts); err != nil {
			return nil, err
		}
	}
	return &ChannelV2{
		BaseURL:   baseURL,
		Transport: transport,
		opts:      &chOpts,
	}, nil
}

// ChannelV2 is used as a connection for GRPC requests issued over HTTP 1.1.
// Values should be created using the NewChannelV2 constructor.
//
// For backwards compatibility, it is still allowed to construct the channel
// via a struct literal, as long as both Transport and BaseURL fields are set
// to non-nil values. Construction via struct literal produces a ChannelV2 with
// all default behavior; use of NewChannelV2 is required to provide channel
// options.
//
// It implements no one protocol: the wire format is supplied by a protocol
// adapter, which defaults to the GRPC-over-HTTP transport protocol defined in
// this package.
type ChannelV2 struct {
	Transport http.RoundTripper
	BaseURL   *url.URL
	// opts is nil when the channel was built as a struct literal, which is the
	// older and still supported form, and so carries all default behavior. A
	// non-nil value means NewChannelV2 already validated the configuration.
	opts *channelV2Options
}

var _ grpc.ClientConnInterface = (*ChannelV2)(nil)

// Invoke satisfies the grpchan.Channel interface and supports sending unary
// RPCs via the in-process channel.
func (ch *ChannelV2) Invoke(ctx context.Context, methodName string, req, resp interface{}, opts ...grpc.CallOption) error {
	chOpts, err := ch.channelOptions()
	if err != nil {
		return err
	}
	copts := internal.GetCallOptions(opts)

	reqUrl := *ch.BaseURL
	reqUrl.Path = path.Join(reqUrl.Path, methodName)
	reqUrlStr := reqUrl.String()
	ctx, err = internal.ApplyPerRPCCreds(ctx, copts, reqUrlStr, reqUrl.Scheme == "https")
	if err != nil {
		return err
	}
	protocol, err := chOpts.newAdapter()
	if err != nil {
		return err
	}
	h, err := protocol.requestHeaders(ctx, false)
	if err != nil {
		return err
	}

	msgBuf, err := chOpts.codec.Marshal(req)
	if err != nil {
		return err
	}
	b := msgBuf.Materialize()

	// TODO: enforce max send and receive size in call options

	body, err := protocol.unaryMessage(b)
	if err != nil {
		return err
	}
	r, err := http.NewRequest("POST", reqUrlStr, readerFromByteSlices(body))
	if err != nil {
		return err
	}
	r.Header = h
	reply, err := ch.Transport.RoundTrip(r.WithContext(ctx))
	if err != nil {
		return statusFromContextError(err)
	}

	// we fire up a goroutine to read the response so that we can properly
	// respect any context deadline (e.g. don't want to be blocked, reading
	// from socket, long past requested timeout).
	var result unaryResponse
	var respErr error
	respCh := make(chan struct{})
	go func() {
		defer close(respCh)
		result, respErr = protocol.processUnaryResponse(reply)
		_ = reply.Body.Close()
	}()

	if len(copts.Peer) > 0 {
		copts.SetPeer(getPeer(ch.BaseURL, r.TLS))
	}

	select {
	case <-ctx.Done():
		return statusFromContextError(ctx.Err())
	case <-respCh:
	}
	// The metadata is reported whether or not the call succeeded, since a failed
	// call's metadata may be what explains the failure.
	copts.SetHeaders(result.headers)
	copts.SetTrailers(result.trailers)
	if respErr != nil {
		return respErr
	}

	return chOpts.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(result.message)}, resp)
}

// NewStream satisfies the grpchan.Channel interface and supports sending
// streaming RPCs via the in-process channel.
func (ch *ChannelV2) NewStream(ctx context.Context, desc *grpc.StreamDesc, methodName string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	chOpts, err := ch.channelOptions()
	if err != nil {
		return nil, err
	}
	copts := internal.GetCallOptions(opts)

	reqUrl := *ch.BaseURL
	reqUrl.Path = path.Join(reqUrl.Path, methodName)
	reqUrlStr := reqUrl.String()
	ctx, err = internal.ApplyPerRPCCreds(ctx, copts, reqUrlStr, reqUrl.Scheme == "https")
	if err != nil {
		return nil, err
	}
	protocol, err := chOpts.newAdapter()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	h, err := protocol.requestHeaders(ctx, true)
	if err != nil {
		cancel()
		return nil, err
	}

	// Intercept r.Close() so we can control the error sent across to the writer thread.
	r, w := io.Pipe()
	req, err := http.NewRequest("POST", reqUrlStr, io.NopCloser(r))
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header = h

	cs := &clientStreamV2{
		ctx:        ctx,
		cancel:     cancel,
		copts:      copts,
		baseUrl:    ch.BaseURL,
		protocol:   protocol,
		codec:      chOpts.codec,
		w:          w,
		respStream: desc.ServerStreams,
		rCh:        make(chan []byte),
	}
	cs.ready.Add(1)
	go cs.doHttpCall(ch.Transport, req, r)

	// ensure that context is cancelled, even if caller
	// fails to fully consume or cancel the stream
	ret := &clientStreamWrapper{cs}
	runtime.AddCleanup(ret, func(struct{}) { cancel() }, struct{}{})

	return ret, nil
}

func (ch *ChannelV2) channelOptions() (channelV2Options, error) {
	// These fields are exported and mutable, so we check them on every call.
	err := checkChannelParams(ch.BaseURL, ch.Transport)
	if err != nil {
		return channelV2Options{}, err
	}
	if ch.opts != nil {
		// Configured and validated by NewChannelV2.
		return *ch.opts, nil
	}
	return defaultChannelV2Options()
}

// clientStreamV2 implements grpc.ClientStream on top of a clientProtocolAdapter.
// It knows neither how messages are framed nor how a stream conveys its final
// status: both are left to the adapter, which is what lets one implementation
// serve protocols that terminate streams differently.
type clientStreamV2 struct {
	ctx     context.Context
	cancel  context.CancelFunc
	copts   *internal.CallOptions
	baseUrl *url.URL

	protocol clientProtocolAdapter
	codec    encoding.CodecV2

	// respStream is set to indicate whether client expects stream response; unary if false
	respStream bool

	// hd and hdErr are populated when ready is done
	ready sync.WaitGroup
	hdErr error
	hd    metadata.MD

	// rCh delivers encoded response messages from doHttpCall to RecvMsg.
	// done must be set to true before it is closed.
	rCh chan []byte

	// rMu protects done, rErr, and trailers
	rMu      sync.RWMutex
	done     bool
	rErr     error
	trailers metadata.MD

	// wMu protects w and wErr
	wMu  sync.Mutex
	w    io.WriteCloser
	wErr error
}

var _ grpc.ClientStream = (*clientStreamV2)(nil)

func (cs *clientStreamV2) Header() (metadata.MD, error) {
	cs.ready.Wait()
	return cs.hd, cs.hdErr
}

func (cs *clientStreamV2) Trailer() metadata.MD {
	// only safe to read trailers after stream has completed
	cs.rMu.RLock()
	defer cs.rMu.RUnlock()
	if cs.done {
		return cs.trailers
	}
	return nil
}

func (cs *clientStreamV2) CloseSend() error {
	cs.wMu.Lock()
	defer cs.wMu.Unlock()
	return cs.w.Close()
}

func (cs *clientStreamV2) Context() context.Context {
	return cs.ctx
}

// readErrorIfDone reports whether the stream has finished and, if so, why: io.EOF
// when it completed successfully, otherwise the RPC's failure status.
func (cs *clientStreamV2) readErrorIfDone() (bool, error) {
	cs.rMu.RLock()
	defer cs.rMu.RUnlock()
	if !cs.done {
		return false, nil
	}
	return true, cs.rErr
}

func (cs *clientStreamV2) SendMsg(m interface{}) error {
	// GRPC streams return EOF error for attempts to send on closed stream
	if done, _ := cs.readErrorIfDone(); done {
		return io.EOF
	}

	cs.wMu.Lock()
	defer cs.wMu.Unlock()
	if cs.wErr != nil {
		// earlier write error means stream is effectively closed
		return io.EOF
	}

	cs.wErr = cs.sendMsgLocked(m)
	return cs.wErr
}

func (cs *clientStreamV2) RecvMsg(m interface{}) error {
	if done, err := cs.readErrorIfDone(); done {
		return err
	}

	select {
	case <-cs.ctx.Done():
		return statusFromContextError(cs.ctx.Err())
	case data, ok := <-cs.rCh:
		if !ok {
			return cs.errAfterClose()
		}
		if err := cs.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, m); err != nil {
			return status.Errorf(codes.Internal, "server sent invalid message: %v", err)
		}
		if !cs.respStream {
			// We need to query the channel for a second message. If there *is* a
			// second message, the server tried to send too many, and that's an
			// error. And if there isn't, we still need to observe the channel close
			// (e.g. end-of-stream) so that trailers are set and available to a
			// subsequent call to Trailer.
			select {
			case <-cs.ctx.Done():
				return statusFromContextError(cs.ctx.Err())
			case _, ok := <-cs.rCh:
				if ok {
					return cs.tooManyResponses()
				}
				// if the server reported a failure after the single message, that
				// failure takes precedence over the successful end of the stream
				if err := cs.errAfterClose(); err != io.EOF {
					return err
				}
			}
		}
		return nil
	}
}

// doHttpCall performs the HTTP round trip and then reads the response body,
// handing each message's encoded bytes to RecvMsg via rCh.
func (cs *clientStreamV2) doHttpCall(transport http.RoundTripper, req *http.Request, readPipe *io.PipeReader) {
	// On completion we must record why the stream ended and then close the
	// channel, which is what signals end-of-stream to client code.
	var rErr error
	var reply *http.Response
	rMuHeld := false
	readySignalled := false

	// onReady releases Header and the first RecvMsg. It must run exactly once,
	// because ready carries a single count and signalling it twice would panic.
	onReady := func(err error, headers metadata.MD) {
		if readySignalled {
			return
		}
		readySignalled = true
		cs.hdErr = err
		cs.hd = headers
		if len(headers) > 0 && len(cs.copts.Headers) > 0 {
			cs.copts.SetHeaders(headers)
		}
		if err != nil {
			rErr = err
		}
		cs.ready.Done()
	}

	defer func() {
		func() {
			// Nothing below may leave a caller blocked in Header, so release it here
			// if no earlier path did.
			onReady(rErr, nil)

			if !rMuHeld {
				cs.rMu.Lock()
			}
			defer cs.rMu.Unlock()

			if cs.rErr == nil {
				if rErr != nil {
					cs.rErr = rErr
				} else {
					cs.rErr = io.EOF
				}
			}
			cs.done = true
			readPipe.CloseWithError(rErr)
			close(cs.rCh)
		}()

		if reply == nil {
			return
		}
		// Read off whatever is left, so that the connection can be reused.
		_, _ = io.Copy(io.Discard, reply.Body)
		_ = reply.Body.Close()
	}()

	// Release the round trip if the context ends while the request body is still
	// open, which is the case whenever the caller stops using the stream without
	// calling CloseSend. The transport sends the body by reading from the pipe and
	// cannot interrupt that read when it is told to abort -- not even by closing
	// the body -- so RoundTrip would block indefinitely and the deferred close
	// above, which is what would otherwise release it, is unreachable from here.
	// Closing the read end is the only thing that ends it.
	//
	// Stopping this on the way out matters: the context is not always cancelled,
	// since a caller that holds on to a finished stream never cancels it.
	stopOnCancel := context.AfterFunc(cs.ctx, func() {
		_ = readPipe.CloseWithError(statusFromContextError(cs.ctx.Err()))
	})
	defer stopOnCancel()

	var err error
	reply, err = transport.RoundTrip(req.WithContext(cs.ctx))
	if err != nil {
		onReady(statusFromContextError(err), nil)
		return
	}

	if len(cs.copts.Peer) > 0 {
		cs.copts.SetPeer(getPeer(cs.baseUrl, reply.TLS))
	}

	md, err := cs.protocol.processStreamHeaders(reply)
	if err != nil {
		onReady(err, md)
		return
	}
	onReady(nil, md)

	for {
		// TODO: enforce max send and receive size in call options

		data, trailers, err := cs.protocol.readStreamResponse()
		if err != nil {
			// End of stream: err is io.EOF if it completed successfully, otherwise
			// the RPC's failure status. Either way the adapter has given us the
			// trailing metadata.
			cs.rMu.Lock()
			rMuHeld = true // defer above will unlock for us
			cs.trailers = trailers
			if len(trailers) > 0 && len(cs.copts.Trailers) > 0 {
				cs.copts.SetTrailers(trailers)
			}
			cs.rErr = err
			return
		}

		select {
		case <-cs.ctx.Done():
			// operation timed out or was cancelled before we could
			// successfully hand this message to client code
			rErr = statusFromContextError(cs.ctx.Err())
			return
		case cs.rCh <- data:
		}
	}
}

// newAdapter creates the protocol adapter for a single RPC. Adapters are per-RPC
// because deframing a response can be stateful; see clientProtocolAdapterFactory.
func (o channelV2Options) newAdapter() (clientProtocolAdapter, error) {
	if o.newProtocol != nil {
		return o.newProtocol(o.codecName)
	}
	return newHTTPGRPCClientAdapter(o.codecName)
}

func (cs *clientStreamV2) sendMsgLocked(m interface{}) error {
	buf, err := cs.codec.Marshal(m)
	if err != nil {
		return err
	}
	data := buf.Materialize()
	frames, err := cs.protocol.streamMessage(data)
	if err != nil {
		return err
	}
	return writeFrames(cs.w, frames)
}

// errAfterClose reports why the stream ended, once rCh has been closed.
func (cs *clientStreamV2) errAfterClose() error {
	done, err := cs.readErrorIfDone()
	if !done {
		// sanity check: this shouldn't be possible, since done is always set
		// before rCh is closed
		return status.Error(codes.Internal, "stream ended without recording a result")
	}
	return err
}

func (cs *clientStreamV2) tooManyResponses() error {
	cs.rMu.Lock()
	defer cs.rMu.Unlock()
	if cs.rErr == nil {
		cs.rErr = status.Error(codes.Internal, "method should return 1 response message but server sent >1")
		cs.done = true
		// we won't be reading from the channel anymore, so we must cancel the
		// context so that doHttpCall doesn't hang trying to write to it
		cs.cancel()
	}
	return cs.rErr
}
