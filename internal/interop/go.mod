module github.com/fullstorydev/grpchan/internal/interop

go 1.25.0

require (
	connectrpc.com/connect v1.20.0
	github.com/fullstorydev/grpchan v0.0.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/jhump/protoreflect v1.18.1 // indirect
	github.com/jhump/protoreflect/v2 v2.0.0-beta.1 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260825221802-da73d73af1c5 // indirect
)

// This module exists only to test this repo against other implementations of the
// protocols it speaks, without adding them to the main module's dependencies. It
// always tests the code alongside it.
replace github.com/fullstorydev/grpchan => ../..
