package httpgrpc

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/grpc/status"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"

	"github.com/fullstorydev/grpchan/internal"
)

type ChannelOption func(*channelOptions)

func WithConnect() ChannelOption {
	return func(opts *channelOptions) {
		opts.protocol = connectClientProtocolAdapter{}
	}
}

func WithGRPCWeb() ChannelOption {
	return func(opts *channelOptions) {
		opts.protocol = grpcWebClientProtocolAdapter{}
	}
}

type channelOptions struct {
	protocol            clientProtocolAdapter
	compressor          encoding.Compressor
	compressType        string
	acceptCompressTypes []string
	codec               encoding.Codec
	subFormat           string
}

type ChannelV2 struct {
	Transport http.RoundTripper
	BaseURL   *url.URL

	channelOptions
}

func NewChannel(transport http.RoundTripper, baseURL *url.URL, opts ...ChannelOption) (*ChannelV2, error) {
	ch := &ChannelV2{Transport: transport, BaseURL: baseURL}
	for _, opt := range opts {
		opt(&ch.channelOptions)
	}
	if ch.compressType == "" {
		ch.compressType = encoding.Identity
	}
	if ch.compressor == nil && ch.compressType != encoding.Identity {
		ch.compressor = encoding.GetCompressor(ch.compressType)
		if ch.compressor == nil {
			return nil, fmt.Errorf("unknown compressor name: %v", ch.compressType)
		}
	}
	if ch.codec == nil {
		if ch.subFormat == "" {
			ch.subFormat = grpcproto.Name
		}
		ch.codec = encoding.GetCodec(ch.subFormat)
		if ch.codec == nil {
			return nil, fmt.Errorf("unknown codec name: %v", ch.subFormat)
		}
	}
	if ch.protocol == nil {
		ch.protocol = httpgrpcClientProtocolAdapter{}
	}
	for _, compressType := range ch.acceptCompressTypes {
		if compressor := encoding.GetCompressor(compressType); compressor == nil {
			return nil, fmt.Errorf("unknown compressor name: %v", compressType)
		}
	}

	return ch, nil
}

// Invoke satisfies the grpchan.Channel interface and supports sending unary
// RPCs via the in-process channel.
func (ch *ChannelV2) Invoke(ctx context.Context, methodName string, req, resp interface{}, opts ...grpc.CallOption) error {
	copts := internal.GetCallOptions(opts)

	reqUrl := *ch.BaseURL
	reqUrl.Path = path.Join(reqUrl.Path, methodName)
	reqUrlStr := reqUrl.String()
	ctx, err := internal.ApplyPerRPCCreds(ctx, copts, reqUrlStr, reqUrl.Scheme == "https")
	if err != nil {
		return err
	}
	h, err := ch.protocol.requestHeaders(ctx, false, ch.subFormat, ch.compressType, ch.acceptCompressTypes)
	if err != nil {
		return err
	}

	b, err := ch.codec.Marshal(req)
	if err != nil {
		return err
	}
	if len(b) > copts.MaxSend {
		return status.Errorf(codes.ResourceExhausted, "payload too large: %v > %v", len(b), copts.MaxSend)
	}
	if ch.compressor != nil {
		var buf bytes.Buffer
		w, err := ch.compressor.Compress(&buf)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		b = buf.Bytes()
	}
	if len(b) > copts.MaxSend {
		return status.Errorf(codes.ResourceExhausted, "payload too large: %v > %v", len(b), copts.MaxSend)
	}

	// NB: The current interface of clientProtocolAdapter doesn't allow for a future
	// where connect can support GET or QUERY requests for idempotent/cacheable RPCs.
	// If/when such a future becomes the present, we'll need to iterate this interface
	// a bit.
	body, err := ch.protocol.unaryMessage(b, ch.compressor != nil)
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
	respCh := make(chan struct{})
	go func() {
		defer close(respCh)
		reader := io.Reader(reply.Body)
		if copts.MaxRecv > 0 && copts.MaxRecv != math.MaxUint64 {
			// We limit to one higher than the limit so we can tell below
			// if the payload was actually > limit.
			reader = io.LimitReader(reply.Body, int64(copts.MaxRecv)+1)
		}
		b, err = io.ReadAll(reader)
		if err == nil && copts.MaxRecv > 0 && len(b) > copts.MaxRecv {
			err = status.Errorf(codes.ResourceExhausted, "response payload too large to receive: %v > %v", len(b), copts.MaxRecv)
		}
		_ = reply.Body.Close()
	}()

	if len(copts.Peer) > 0 {
		copts.SetPeer(getPeer(ch.BaseURL, r.TLS))
	}

	// gather headers and trailers
	if len(copts.Headers) > 0 || len(copts.Trailers) > 0 {
		if err := setMetadata(reply.Header, copts); err != nil {
			return err
		}
	}

	if stat := statFromResponse(reply); stat.Code() != codes.OK {
		return stat.Err()
	}

	select {
	case <-ctx.Done():
		return statusFromContextError(ctx.Err())
	case <-respCh:
	}
	if err != nil {
		return err
	}
	// TODO: maybe decompress response
	return ch.codec.Unmarshal(b, resp)
}

// NewStream satisfies the grpchan.Channel interface and supports sending
// streaming RPCs via the in-process channel.
func (ch *ChannelV2) NewStream(ctx context.Context, desc *grpc.StreamDesc, methodName string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	copts := internal.GetCallOptions(opts)

	reqUrl := *ch.BaseURL
	reqUrl.Path = path.Join(reqUrl.Path, methodName)
	reqUrlStr := reqUrl.String()
	ctx, err := internal.ApplyPerRPCCreds(ctx, copts, reqUrlStr, reqUrl.Scheme == "https")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	h := headersFromContext(ctx)
	h.Set("Content-Type", StreamRpcContentType_V1)

	// Intercept r.Close() so we can control the error sent across to the writer thread.
	r, w := io.Pipe()
	req, err := http.NewRequest("POST", reqUrlStr, io.NopCloser(r))
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header = h

	// TODO: Need a newClientStreamV2
	cs := newClientStream(ctx, cancel, w, desc.ServerStreams, copts, ch.BaseURL)
	go cs.doHttpCall(ch.Transport, req, r)

	// ensure that context is cancelled, even if caller
	// fails to fully consume or cancel the stream
	ret := &clientStreamWrapper{cs}
	runtime.SetFinalizer(ret, func(*clientStreamWrapper) { cancel() })

	return ret, nil
}
