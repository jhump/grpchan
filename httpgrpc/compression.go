package httpgrpc

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

// defaultCompressMinBytes is the smallest message that is compressed when no
// minimum is configured. A message that fits in a single TCP segment, about 1460
// bytes, rarely saves a packet by being compressed, so below about a kilobyte the
// CPU spent compressing buys next to nothing.
const defaultCompressMinBytes = 1024

// knownCompressors are the compressor names this package looks for when telling
// a server which encodings a response may use, in order of preference.
//
// The list is needed because there is no way to enumerate what has been
// registered: encoding exports only RegisterCompressor and GetCompressor. So the
// registry is probed for these names instead, which spares callers from having to
// list them: importing a compressor's package is enough for it to be offered.
var knownCompressors = []string{"br", "zstd", "snappy", "gzip", "deflate"}

// advertisedCompressors returns the registered subset of knownCompressors, which
// is what a server may choose from when compressing its response. It is computed
// per call rather than once, since a compressor can be registered by any
// package's init, including one that runs after this package's.
func advertisedCompressors() []string {
	var names []string
	for _, name := range knownCompressors {
		if encoding.GetCompressor(name) != nil {
			names = append(names, name)
		}
	}
	return names
}

// isIdentity reports whether the given compressor name means no compression.
func isIdentity(name string) bool {
	return name == "" || name == encoding.Identity
}

// checkDefaultCompressor validates a compressor named in a channel's default
// call options. Unlike one given to a single call, which is ignored when it
// cannot be used, a default is checked when the channel is created, which is the
// one place a bad one can be reported without breaking existing callers.
func checkDefaultCompressor(name string, supported bool) error {
	if isIdentity(name) {
		return nil
	}
	if !supported {
		return fmt.Errorf("compressor %q cannot be used: the protocol does not support compression", name)
	}
	if encoding.GetCompressor(name) == nil {
		return fmt.Errorf("compressor %q is not registered", name)
	}
	return nil
}

// requestCompressor picks the compressor for the messages a call sends, or ""
// for none. The callName comes from the call's options, which include the
// channel's defaults; defaultName is the channel's own, already validated by
// checkDefaultCompressor.
//
// A callName that cannot be used is ignored in favor of the default, where
// gRPC-Go would fail the call: grpc.UseCompressor once had no effect here at all,
// and code written then may pass a name that this would otherwise reject.
func requestCompressor(callName, defaultName string, supported bool) string {
	if !supported || callName == encoding.Identity {
		return ""
	}
	if callName != "" && encoding.GetCompressor(callName) != nil {
		return callName
	}
	if isIdentity(defaultName) {
		return ""
	}
	return defaultName
}

// responseCompressor picks the compressor for the messages a server sends, or ""
// for none. The request's own compressor is preferred, since a client that used
// it can certainly decode it; otherwise the first of the client's accepted
// encodings that is registered here.
//
// This compresses whenever the client accepts an encoding, unlike gRPC-Go,
// which compresses a response only when the request was compressed. A client
// served over HTTP/1.1 is often a browser, which rarely compresses what it sends
// but may gain the most from a compressed response.
func responseCompressor(requestName string, accepted []string) string {
	if !isIdentity(requestName) && encoding.GetCompressor(requestName) != nil {
		return requestName
	}
	for _, name := range accepted {
		if !isIdentity(name) && encoding.GetCompressor(name) != nil {
			return name
		}
	}
	return ""
}

// compressMessage encodes an already-marshaled message with the given
// compressor, reporting whether what it returns is in fact compressed.
//
// It declines to compress a message smaller than minBytes, since compressing it
// costs CPU and saves little or nothing on the wire. And it declines when
// compressing does not make the message smaller, which is not an exotic case: an
// incompressible or already-compressed payload comes back slightly larger than it
// went in. Every protocol here reports compression per message, so a message may
// go out uncompressed even though an encoding was negotiated -- but the caller
// must then not label it as encoded, which is what the reported flag is for.
//
// Declining is also what lets a send limit be checked against the marshaled
// message alone: what goes out is never larger than what was measured.
func compressMessage(compressor encoding.Compressor, data []byte, minBytes int) ([]byte, bool, error) {
	if compressor == nil || len(data) == 0 || len(data) < minBytes {
		return data, false, nil
	}
	var buf bytes.Buffer
	// Most messages worth compressing shrink by at least half.
	buf.Grow(len(data) / 2)
	w, err := compressor.Compress(&buf)
	if err != nil {
		return nil, false, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, false, err
	}
	if err := w.Close(); err != nil {
		return nil, false, err
	}
	if buf.Len() >= len(data) {
		return data, false, nil
	}
	return buf.Bytes(), true, nil
}

// decompressLimited decodes a compressed message with the given compressor,
// refusing one that expands past the given limit.
//
// The compressed bytes have already been bounded by the limit when they were
// read; the message they decode to is bounded here, since a message that fits
// compressed can expand far past the limit. So a message costs at most twice the
// limit.
//
// A message that cannot be decompressed is reported as Internal, as gRPC-Go
// reports it; one that exceeds the limit is a refusal, as anywhere else.
func decompressLimited(compressor encoding.Compressor, data []byte, maxRecv int) ([]byte, error) {
	r, err := compressor.Decompress(bytes.NewReader(data))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not decompress message: %v", err)
	}
	if maxRecv < math.MaxInt32 {
		// One past the limit, so a message that is exactly at it is distinguishable
		// from one that exceeds it.
		r = io.LimitReader(r, int64(maxRecv)+1)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not decompress message: %v", err)
	}
	if err := checkRecvSize(len(b), maxRecv); err != nil {
		return nil, err
	}
	return b, nil
}

// compressorFor returns the registered compressor with the given name, or nil
// for no compression. The name must already be known to be registered, as the
// ones requestCompressor and responseCompressor return are; lookupCompressor is
// for a name that came from the peer.
func compressorFor(name string) encoding.Compressor {
	if isIdentity(name) {
		return nil
	}
	return encoding.GetCompressor(name)
}

// lookupCompressor returns the registered compressor with the name a peer gave,
// or nil for no compression. A name that no compressor is registered under is
// reported as Internal, as gRPC-Go reports a response it cannot decompress.
func lookupCompressor(name string) (encoding.Compressor, error) {
	if isIdentity(name) {
		return nil, nil
	}
	compressor := encoding.GetCompressor(name)
	if compressor == nil {
		return nil, status.Errorf(codes.Internal, "unknown compressor name: %v", name)
	}
	return compressor, nil
}
