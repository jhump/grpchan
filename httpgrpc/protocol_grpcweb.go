package httpgrpc

// NB: This is nil, so if protocol detection sees grpc-web, it will
// result in unsupported media type error.
// TODO: Make this real by implementing the type below
var grpcWebAdapter serverProtocolAdapter

type grpcWebServerProtocolAdapter struct{}

type grpcWebClientProtocolAdapter struct{}
