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
	"fmt"
	"io"
	"net"
	"net/http"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/internal/runnable"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

var (
	_ http.Handler = (*httpHandler)(nil)

	errBodyTooLarge = errors.New("request body exceeds the configured limit")
	errNoEndpoint   = errors.New("no endpoint selected")
)

// httpHandler answers a routing call over HTTP. The request is the one
// to route, and the response carries the endpoint EPP picked, so the exchange
// has no schema of its own.
//
// A passthrough with no route of its own: EPP resolves parsers by path suffix,
// so a request presented as /v1/schedule matches none.
type httpHandler struct {
	ss      *handlers.StreamingServer
	maxBody int64
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	max := h.maxBody
	if max <= 0 {
		max = defaultMaxRoutableBodyBytes
	}
	body, err := readBody(r, max)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), code)
		return
	}

	if err := h.decide(w, r, body); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}

// writeRefusal is the one place an ext_proc refusal becomes an HTTP response.
// It passes the status, headers, and body through unchanged.
func writeRefusal(w http.ResponseWriter, r *http.Request, imm *extProcPb.ImmediateResponse) {
	for _, hv := range imm.GetHeaders().GetSetHeaders() {
		w.Header().Add(hv.GetHeader().GetKey(), headerValue(hv))
	}
	code := int(imm.GetStatus().GetCode())
	if code == 0 { // refused without saying how; WriteHeader would panic
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)
	body := imm.GetBody()
	if len(body) == 0 {
		return
	}
	// The status line is already written, so a failure here cannot change the
	// response; it is almost always the client hanging up mid-write.
	if _, err := w.Write(body); err != nil {
		log.FromContext(r.Context()).V(logutil.DEBUG).Info("failed to write refusal body", "error", err)
	}
}

// decide drives one request phase over a channel pair rather than a hop, then
// writes the outcome. It writes nothing until the stream drains, so a late
// refusal still replaces the response instead of appending to it.
//
// It writes and closes the input before reading anything back, so Process
// always reaches EOF. It also drains the output fully, because breaking out
// early leaves Process blocked on a full channel and leaks a goroutine.
func (h *httpHandler) decide(w http.ResponseWriter, r *http.Request, body []byte) error {
	s := newStream(r.Context())

	// Buffered wide enough for both sends, so these never block.
	s.in <- &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extProcPb.HttpHeaders{Headers: toHeaderMap(r)},
		},
	}
	s.in <- &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestBody{
			RequestBody: &extProcPb.HttpBody{Body: body, EndOfStream: true},
		},
	}
	close(s.in)

	done := make(chan error, 1)
	go func() {
		err := h.ss.Process(s)
		close(s.out)
		done <- err
	}()

	var endpoint string
	var refused *extProcPb.ImmediateResponse
	for resp := range s.out {
		if imm := resp.GetImmediateResponse(); imm != nil {
			if refused == nil {
				refused = imm
			}
			continue
		}
		if ep := destinationEndpoint(resp); ep != "" && endpoint == "" {
			endpoint = ep
		}
	}

	err := <-done
	switch {
	case refused != nil:
		writeRefusal(w, r, refused)
	case endpoint != "":
		w.Header().Set(metadata.DestinationEndpointKey, endpoint)
		w.WriteHeader(http.StatusOK)
	case err != nil:
		// Process reports EOF through the same return value as a real failure.
		return err
	default:
		return errNoEndpoint
	}
	return nil
}

// readBody sizes the buffer from Content-Length instead of growing it.
func readBody(r *http.Request, max int64) ([]byte, error) {
	n := r.ContentLength
	if n > max {
		return nil, fmt.Errorf("%w: %d > %d", errBodyTooLarge, n, max)
	}
	// One byte past the limit, to catch an over-long body rather than
	// truncating it at exactly max.
	lr := io.LimitReader(r.Body, max+1)

	if n <= 0 {
		b, err := io.ReadAll(lr)
		if err != nil {
			return nil, err
		}
		if int64(len(b)) > max {
			return nil, fmt.Errorf("%w: exceeds %d", errBodyTooLarge, max)
		}
		return b, nil
	}

	buf := make([]byte, n)
	nr, err := io.ReadFull(lr, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	// Content-Length is a client-supplied hint, so check for a longer body.
	var extra [1]byte
	if m, _ := lr.Read(extra[:]); m > 0 {
		return nil, fmt.Errorf("%w: body longer than Content-Length %d", errBodyTooLarge, n)
	}
	return buf[:nr], nil
}

func toHeaderMap(r *http.Request) *corev3.HeaderMap {
	hm := &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte(r.Method)},
		{Key: ":path", RawValue: []byte(r.URL.Path)},
	}}
	for k, vs := range r.Header {
		for _, v := range vs {
			hm.Headers = append(hm.Headers, &corev3.HeaderValue{Key: k, RawValue: []byte(v)})
		}
	}
	return hm
}

func headerValue(hv *corev3.HeaderValueOption) string {
	if v := hv.GetHeader().GetRawValue(); len(v) > 0 {
		return string(v)
	}
	return hv.GetHeader().GetValue()
}

// destinationEndpoint reads the selected endpoint out of a header mutation.
// The rest of that mutation is ext_proc plumbing, not the decision.
func destinationEndpoint(resp *extProcPb.ProcessingResponse) string {
	var common *extProcPb.CommonResponse
	switch {
	case resp.GetRequestHeaders() != nil:
		common = resp.GetRequestHeaders().GetResponse()
	case resp.GetRequestBody() != nil:
		common = resp.GetRequestBody().GetResponse()
	}
	for _, hv := range common.GetHeaderMutation().GetSetHeaders() {
		if hv.GetHeader().GetKey() == metadata.DestinationEndpointKey {
			return headerValue(hv)
		}
	}
	return ""
}

// httpFrontend serves the routing decision over net/http, translated into
// ext_proc messages by inprocStream.
type httpFrontend struct {
	port int
	// lis, when set, serves in place of port so a caller can reserve the port
	// before this frontend starts.
	lis     net.Listener
	maxBody int64
}

// Option configures the HTTP frontend.
type Option func(*httpFrontend)

// WithListener serves on a pre-bound listener. When set, port is ignored,
// mirroring the ext_proc GrpcListener path.
func WithListener(lis net.Listener) Option {
	return func(f *httpFrontend) { f.lis = lis }
}

// HTTPFrontend opens the HTTP frontend on port. maxBody at or below zero
// takes the shared default, keeping the two frontends' ceilings identical.
func HTTPFrontend(port int, maxBody int64, opts ...Option) Frontend {
	if maxBody <= 0 {
		maxBody = defaultMaxRoutableBodyBytes
	}
	f := httpFrontend{port: port, maxBody: maxBody}
	for _, o := range opts {
		o(&f)
	}
	return f
}

func (f httpFrontend) Serve(ctx context.Context, ss *handlers.StreamingServer) error {
	h := &httpHandler{ss: ss, maxBody: f.maxBody}
	if f.lis != nil {
		return runnable.HTTPServerOnListener("http", h, f.lis).Start(ctx)
	}
	return runnable.HTTPServer("http", h, f.port).Start(ctx)
}
