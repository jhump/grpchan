package interop

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/fullstorydev/grpchan/httpgrpc"
)

// TestConnectGetServer runs a connect-go client, making unary calls with GET as
// the Connect protocol allows, against this repo's server, over HTTP/1.1. The
// server sees only GET requests, which only a method free of side effects
// accepts; see sideEffectFreeService.
func TestConnectGetServer(t *testing.T) {
	svr := httpgrpc.NewServer()
	svr.RegisterService(sideEffectFreeService(t), &grpchantesting.TestServer{})
	var mu sync.Mutex
	var methods []string
	httpSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		svr.ServeHTTP(w, r)
	}))
	t.Cleanup(httpSvr.Close)

	testCases := []struct {
		name string
		opts []connect.ClientOption
	}{
		{name: "proto"},
		{name: "json", opts: []connect.ClientOption{connect.WithProtoJSON()}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			opts := append([]connect.ClientOption{
				connect.WithHTTPGet(),
				connect.WithIdempotency(connect.IdempotencyNoSideEffects),
			}, testCase.opts...)
			ch := renamedService{
				ClientConnInterface: newConnectChannel(httpSvr.Client(), httpSvr.URL, opts...),
				from:                "/grpchantesting.TestService/",
				to:                  "/" + sideEffectFreeServiceName + "/",
			}
			grpchantesting.NewChannelTester(ch).Run(t, grpchantesting.UnaryTests())
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if len(methods) == 0 {
		t.Fatal("the server saw no requests")
	}
	if i := slices.IndexFunc(methods, func(m string) bool { return m != http.MethodGet }); i >= 0 {
		t.Errorf("request %d of %d was %s, not GET", i+1, len(methods), methods[i])
	}
}

// renamedService sends calls for one service to another of the same shape.
type renamedService struct {
	grpc.ClientConnInterface
	from, to string
}

func (r renamedService) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return r.ClientConnInterface.Invoke(ctx, strings.Replace(method, r.from, r.to, 1), args, reply, opts...)
}

func (r renamedService) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return r.ClientConnInterface.NewStream(ctx, desc, strings.Replace(method, r.from, r.to, 1), opts...)
}

// sideEffectFreeServiceName is the name of the test service as registered by
// sideEffectFreeService.
const sideEffectFreeServiceName = "grpchantesting.sideeffectfree.TestService"

var registerSideEffectFree sync.Once

// sideEffectFreeService returns the test service under another name, whose
// descriptor declares its Unary method free of side effects. The descriptor is a
// copy of the generated one with that option added, registered once; the main
// module's tests use the same fixture.
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
