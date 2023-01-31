package httpgrpc

import (
	"context"
	"io"
	"mime"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// This package supports three different protocols:
//
// 1. "httpgrpc": The original gRPC-over-HTTP-1.1 protocol that was implemented for this
//    package. It provides gRPC semantics, even without HTTP/2, except that full-duplex
//    bidirectional streams cannot be supported. It uses a simpler protocol for unary
//    RPCs than for streams.
// 2. "grpc-web": The variant of the gRPC protocol, adapted to work over HTTP 1.1. It
//    closely resembles the gRPC HTTP/2-based protocol except that the final RPC status
//    and any trailers are encoded at the end of the response body. Ironically, though
//    this protocol ostensibly targets web support, JSON codecs are not supported due
//    to limitations in the official Protobuf JavaScript runtime.
// 3. "connect": A new protocol that is intended to address issues in gRPC, largely
//    related to support for web and mobile clients. Connect clients and servers also
//    support the gRPC Web protocol. Some may additionally support the HTTP/2-based
//    gRPC protocol, too (if the language runtime environment provides adequate
//    network support for HTTP/2).

// These are the content-types used for the "httpgrpc" protocol.
const (
	UnaryRpcContentType_V1  = "application/x-protobuf"
	StreamRpcContentType_V1 = "application/x-httpgrpc-proto+v1"
)

const (
	// ApplicationJson is the content-type for JSON-encoded unary RPCs.
	// Uses the `jsonpb.Marshaler` by default. Use `encoding.RegisterCodec` to
	// override the default encoder with a custom encoder.
	//
	// This is coincidentally used by both "connect" and "httpgrpc" protocols.
	ApplicationJson = "application/json"
)

// Content types for other supported protocols
const (
	// ConnectContentType indicates a streaming call using the Connect protocol
	// (https://connect.build). Unary calls use a format like "application/<sub-format>",
	// such as "application/proto" for example. Streaming calls indicate sub-format
	// with a "+<sub-format>" suffix, such as "application/connect+proto".
	ConnectContentType = "application/connect"

	// GrpcWebContentType indicates an RPC that uses the gRPC Web protocol (a small
	// tweak on top of the normal gRPC protocol to support HTTP 1.1). A sub-format
	// (that indicates which codec to use) is indicated with a "+<sub-format>" suffix,
	// for example "application/grpc-web+proto".
	GrpcWebContentType = "application/grpc-web"
)

// protocolAdapter contains methods used by both client and server protocols.
type protocolAdapter interface {
	// unaryMessage encodes the given message into a "frame" for the protocol. The
	// given parameters are the encoded message bytes and a flag indicating whether
	// the bytes are compressed. The response is a reader, from which the framed
	// data can be read or an error.
	//
	// This method is separate from streamMessage since some protocols may encode
	// unary request and response bodies differently from streaming request and
	// response bodies.
	//
	// This method is called for encoding the request and the response only if the
	// RPC is a unary operation.
	unaryMessage(data []byte, compressed bool) (io.Reader, error)
	// streamMessage encodes the given message into a "frame" for the protocol. The
	// given parameters are the encoded message bytes and a flag indicating whether
	// the bytes are compressed. The response is a reader, from which the framed
	// data can be read or an error.
	//
	// This method is called if the RPC is not a unary operation. So, even if a
	// single direction is unary (for example, in a client streaming operation, the
	// server response is unary) this method is used for both requests and responses.
	streamMessage(data []byte, compressed bool) (io.Reader, error)
}

// clientProtocolAdapter provides the methods needed to implement the client side of
// an HTTP-based protocol.
type clientProtocolAdapter interface {
	protocolAdapter
	// supportsCompression returns true if the protocol supports compression. If the
	// protocol does not support compression, the compressorName used for all other
	// methods of the adapter will always be "identity".
	supportsCompression() bool
	// requestHeaders creates the request headers for the operation. If the given
	// context head a deadline, it may be encoded in request headers to propagate that
	// deadline. This method need not encode any custom metadata, only headers needed
	// by the protocol to define the call.
	requestHeaders(ctx context.Context, isStream bool, codecName string, compressorName string, supportedCompressors []string) (http.Header, error)
	// processUnaryResponse processes the given response and returns all aspects of the
	// RPC result: header metadata, response message, whether the response is compressed,
	// trailer metadata, and an optional error. The error should be non-nil if the call
	// indicates a non-OK RPC status code. If the given error is non-nil, the response
	// message should be nil but the header and trailer metadata may be populated. This
	// is only called for unary operations.
	processUnaryResponse(resp *http.Response) (metadata.MD, io.Reader, bool, metadata.MD, error)
	// processStreamHeeaders processes the given stream response into header metadata and
	// an optional error indicating whether the RPC already failed. This is only called for
	// stream operations, and will be combined with calls to readStreamResponse.
	processStreamHeaders(resp *http.Response) (metadata.MD, error)
	// readStreamResponse reads a response message from the given reader. The returned
	// reader should be the encoded (and optionally compressed) response message data.
	// The returned flag indicates whether the message data is compressed. If an error
	// occurs or if the end of the stream has been reached, the returned reader should
	// be nil and the given metadata and error should be populated as trailer metadata
	// and the cause of failure. If the RPC operation completed successfully, the error
	// returned should be io.EOF.
	readStreamResponse(r io.ReadCloser) (io.Reader, bool, metadata.MD, error)
}

// serverProtocolAdapter provides the methods needed to implement the server side of
// an HTTP-based protocol.
type serverProtocolAdapter interface {
	protocolAdapter
	// processHeaders is called to process the given request headers. This returns
	// a context which may be modified from the incoming context (such as to add a
	// deadline), the request encoding ("identity" if none), and the supported
	// encodings in priority order.
	processHeaders(ctx context.Context, header http.Header) (_ context.Context, _ context.CancelFunc, compressorName string, supportedCompressors []string, _ error)
	// responseHeaders encodes the given codec, compression encoding, and header metadata
	// into the given response headers.
	responseHeaders(isStream bool, codecName string, compressorName string, md metadata.MD, targetHeaders http.Header)
	// processUnaryRequest extracts the request data from the given request body. It
	// returns a reader from which the message data is read, a flag indicating whether
	// the message data is encoded/compressed, or an error.
	processUnaryRequest(io.ReadCloser) (io.ReadCloser, bool, error)
	// finishUnary records the given error result (which may be nil on successful
	// completion) to the given response writer. This may encode the result either in
	// headers or in the response body (in which case it is appended to the data
	// returned from unaryMessage if err is nil).
	finishUnary(ctx context.Context, err error, trailers metadata.MD, w http.ResponseWriter)
	// readStreamRequest extracts a single message from the given reader. It returns a
	// reader from which the message data may be read and a flag indicating whether the
	// message data is encoded/compressed.
	readStreamRequest(io.ReadCloser) (io.Reader, bool, error)
	// finishStream records the given error result (which may be nil on successful
	// completion) to the given response writer.
	finishStream(err error, trailers metadata.MD, w http.ResponseWriter)
}

func determineProtocolAdapter(contentType string, req *http.Request, opts handlerOpts) (_ serverProtocolAdapter, subFormatType string, allowUnary, allowStream bool, err error) {
	mediaType, _, _ := mime.ParseMediaType(contentType)
	switch {
	case mediaType == ApplicationJson:
		// Both connect and httpgrpc protocols can use this content type.
		// So we must look at another header to distinguish.
		if connectVersion := req.Header.Get("Connect-Protocol-Version"); connectVersion != "" {
			return connectAdapter, "json", true, false, nil
		}
		return &httpgrpcServerProtocolAdapter{errFunc: opts.errFunc, jsonErr: true}, "json", true, false, nil
	case mediaType == UnaryRpcContentType_V1:
		return &httpgrpcServerProtocolAdapter{errFunc: opts.errFunc}, "proto", true, false, nil
	case mediaType == StreamRpcContentType_V1:
		return &httpgrpcServerProtocolAdapter{errFunc: opts.errFunc}, "proto", false, true, nil
	}

	protocolType, subFormatType := mediaType, "proto"
	pos := strings.IndexRune(mediaType, '+')
	if pos != -1 {
		protocolType = mediaType[:pos]
		subFormatType = mediaType[pos+1:]
	}
	switch protocolType {
	case GrpcWebContentType:
		return grpcWebAdapter, subFormatType, true, true, nil
	case ConnectContentType:
		return connectAdapter, subFormatType, false, true, nil
	default:
		subFormatType = strings.TrimPrefix(mediaType, "application/")
		if subFormatType == mediaType {
			return nil, "", false, false, nil
		}
		return connectAdapter, subFormatType, true, false, nil
	}
}
