/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"errors"
	"io"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"google.golang.org/grpc/metadata"
)

var (
	// Compile-time proof that this needs no change to the ext_proc contract.
	_ extProcPb.ExternalProcessor_ProcessServer = (*inprocStream)(nil)

	errNotGRPC = errors.New("inproc stream is not gRPC")
)

// inprocStream presents one buffered request as an ext_proc stream, because
// Process accepts only streams.
type inprocStream struct {
	ctx context.Context
	in  chan *extProcPb.ProcessingRequest
	out chan *extProcPb.ProcessingResponse
}

func newStream(ctx context.Context) *inprocStream {
	return &inprocStream{
		ctx: ctx,
		in:  make(chan *extProcPb.ProcessingRequest, 4),
		out: make(chan *extProcPb.ProcessingResponse, 8),
	}
}

func (s *inprocStream) Recv() (*extProcPb.ProcessingRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case req, ok := <-s.in:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (s *inprocStream) Send(resp *extProcPb.ProcessingResponse) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.out <- resp:
		return nil
	}
}

func (s *inprocStream) Context() context.Context { return s.ctx }

// The rest of grpc.ServerStream. Headers and trailers are wire concepts with no
// in-process meaning; SendMsg and RecvMsg would bypass the typed pair above.
func (s *inprocStream) SetHeader(metadata.MD) error  { return nil }
func (s *inprocStream) SendHeader(metadata.MD) error { return nil }
func (s *inprocStream) SetTrailer(metadata.MD)       {}
func (s *inprocStream) SendMsg(any) error            { return errNotGRPC }
func (s *inprocStream) RecvMsg(any) error            { return errNotGRPC }

// DeclinesRequestBodyEcho reports that this caller discards the body echo:
// it already holds the bytes it sent, so chunking them back is pure waste.
func (*inprocStream) DeclinesRequestBodyEcho() bool { return true }
