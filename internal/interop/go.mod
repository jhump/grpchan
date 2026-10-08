module github.com/fullstorydev/grpchan/internal/interop

go 1.25.0

require (
	connectrpc.com/connect v1.20.0
	github.com/fullstorydev/grpchan v0.0.0
	github.com/improbable-eng/grpc-web v0.15.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/jhump/protoreflect v1.18.1 // indirect
	github.com/jhump/protoreflect/v2 v2.0.0-beta.1 // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/rs/cors v1.7.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260825221802-da73d73af1c5 // indirect
	nhooyr.io/websocket v1.8.6 // indirect
)

// This module exists only to test this repo against other implementations of the
// protocols it speaks, without adding them to the main module's dependencies. It
// always tests the code alongside it.
replace github.com/fullstorydev/grpchan => ../..
