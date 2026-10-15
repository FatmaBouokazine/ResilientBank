//go:build tools

// Package tools pins the build-time toolchain in a dedicated module so that every
// developer and CI runner produces byte-identical generated code.
//
// Tool binaries are NOT compiled into the services. Keeping them in their own module
// means their transitive dependencies never leak into the service build, while still
// recording exact versions that `make tools` installs.
//
// Run `make tools` after cloning, and `make verify` to confirm your installed versions
// match the pins below.
package tools

import (
	// buf lints, generates, and checks for breaking changes to the protobuf contracts.
	_ "github.com/bufbuild/buf/cmd/buf"

	// protoc-gen-go generates Go types from .proto files.
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"

	// protoc-gen-go-grpc generates gRPC service stubs.
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
)