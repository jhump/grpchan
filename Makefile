export PATH := $(shell pwd)/.tmp/protoc/bin:$(PATH)
export PROTOC_VERSION := 22.0

.PHONY: ci
ci: deps checkgofmt vet vet32 staticcheck ineffassign predeclared test

.PHONY: deps
deps:
	go get -d -v -t ./...
	go mod tidy

.PHONY: updatedeps
updatedeps:
	go get -d -v -t -u -f ./...
	go mod tidy

.PHONY: install
install:
	go install ./...

.PHONY: checkgofmt
checkgofmt:
	gofmt -s -l .
	@if [ -n "$$(gofmt -s -l .)" ]; then \
		exit 1; \
	fi

.PHONY: generate
generate: .tmp/protoc/bin/protoc
	@go install ./cmd/protoc-gen-grpchan
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0
	go generate ./...

.PHONY: vet
vet:
	go vet ./...

# A 32-bit build catches size arithmetic that only overflows where int is 32
# bits, which nothing else here would notice.
.PHONY: vet32
vet32:
	GOOS=linux GOARCH=386 go vet ./...

.PHONY: staticcheck
staticcheck:
	@go install honnef.co/go/tools/cmd/staticcheck@2025.1.1
	staticcheck -checks "inherit,-SA1019" ./...

.PHONY: ineffassign
ineffassign:
	@go install github.com/gordonklaus/ineffassign@7953dde2c7bf
	ineffassign .

.PHONY: predeclared
predeclared:
	@go install github.com/nishanths/predeclared@51e8c974458a0f93dc03fe356f91ae1a6d791e6f
	predeclared .

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: golint
golint:
	@go install golang.org/x/lint/golint@v0.0.0-20210508222113-6edffad5e616
	golint -min_confidence 0.9 -set_exit_status ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: errcheck
errcheck:
	@go install github.com/kisielk/errcheck@v1.2.0
	errcheck ./...

# The interop tests are a separate module, so that the implementations they test
# against are not dependencies of this one.
.PHONY: test
test:
	go test -race ./...
	cd internal/interop && go test -race ./...

.tmp/protoc/bin/protoc: ./Makefile ./download_protoc.sh
	./download_protoc.sh
