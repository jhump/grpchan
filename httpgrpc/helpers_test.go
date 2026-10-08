package httpgrpc_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// postRaw sends a request written by hand, and returns the response and its
// body, which has been read in full. The transport is told not to ask for or
// undo compression, so the body is exactly what the server sent.
func postRaw(t *testing.T, base *url.URL, path, contentType string, header http.Header, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base.JoinPath(path).String(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	req.Header.Set("Content-Type", contentType)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return resp, respBody
}

// sideEffectFreeServiceName is the name of the test service as registered by
// sideEffectFreeService.
const sideEffectFreeServiceName = "grpchantesting.sideeffectfree.TestService"

var registerSideEffectFree sync.Once

// sideEffectFreeService returns the test service under another name, whose
// descriptor declares its Unary method free of side effects. The descriptor is a
// copy of the generated one with that option added, registered once, so that one
// implementation can be served both with the option and without it.
func sideEffectFreeService(t *testing.T) *grpc.ServiceDesc {
	t.Helper()
	registerSideEffectFree.Do(func() {
		orig := protodesc.ToFileDescriptorProto(grpchantesting.File_test_proto)
		service := proto.Clone(orig.GetService()[0]).(*descriptorpb.ServiceDescriptorProto)
		for _, method := range service.GetMethod() {
			if method.GetName() == "Unary" {
				method.Options = &descriptorpb.MethodOptions{
					IdempotencyLevel: descriptorpb.MethodOptions_NO_SIDE_EFFECTS.Enum(),
				}
			}
		}
		// The copy depends on the original for its message types, so that only the
		// service is redefined.
		file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
			Name:       proto.String("grpchantesting/test_side_effect_free.proto"),
			Package:    proto.String("grpchantesting.sideeffectfree"),
			Dependency: append(slices.Clone(orig.GetDependency()), orig.GetName()),
			Service:    []*descriptorpb.ServiceDescriptorProto{service},
			Syntax:     orig.Syntax,
		}, protoregistry.GlobalFiles)
		if err != nil {
			t.Fatalf("failed to build descriptor: %v", err)
		}
		if err := protoregistry.GlobalFiles.RegisterFile(file); err != nil {
			t.Fatalf("failed to register descriptor: %v", err)
		}
	})
	desc := grpchantesting.TestService_ServiceDesc
	desc.ServiceName = sideEffectFreeServiceName
	return &desc
}
