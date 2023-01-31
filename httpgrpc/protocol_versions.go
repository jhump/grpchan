package httpgrpc

import (
	"mime"

	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
)

func getUnaryCodec(contentType string) encoding.Codec {
	// Ignore any errors or charsets for now, just parse the main type.
	// TODO: should this be more picky / return an error?  Maybe charset utf8 only?
	mediaType, _, _ := mime.ParseMediaType(contentType)

	if mediaType == UnaryRpcContentType_V1 {
		return encoding.GetCodec(grpcproto.Name)
	}

	if mediaType == ApplicationJson {
		return encoding.GetCodec("json")
	}

	return nil
}

func getStreamingCodec(contentType string) encoding.Codec {
	// Ignore any errors or charsets for now, just parse the main type.
	// TODO: should this be more picky / return an error?  Maybe charset utf8 only?
	mediaType, _, _ := mime.ParseMediaType(contentType)

	if mediaType == StreamRpcContentType_V1 {
		return encoding.GetCodec(grpcproto.Name)
	}

	if mediaType == ApplicationJson {
		// TODO: support half-duplex JSON streaming?
		// https://en.wikipedia.org/wiki/JSON_streaming#Record_separator-delimited_JSON
		return nil
	}

	return nil
}
