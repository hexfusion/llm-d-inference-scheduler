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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

const wantEndpoint = "10.244.0.11:8000"

type fakeDatastore struct{}

func (fakeDatastore) PoolGet() (*datalayer.EndpointPool, error) {
	return &datalayer.EndpointPool{}, nil
}

// fakeDirector stands in for scheduling. Everything upstream of it, the phase
// handling and the response construction, is EPP's real code.
type fakeDirector struct{}

func (d *fakeDirector) HandleRequest(_ context.Context, reqCtx *handlers.RequestContext, _ *fwkrh.InferenceRequestBody) (*handlers.RequestContext, error) {
	reqCtx.TargetEndpoint = wantEndpoint
	return reqCtx, nil
}
func (d *fakeDirector) HandleResponseHeader(_ context.Context, r *handlers.RequestContext) *handlers.RequestContext {
	return r
}
func (d *fakeDirector) HandleResponseBody(_ context.Context, r *handlers.RequestContext, _ bool) *handlers.RequestContext {
	return r
}
func (d *fakeDirector) GetRandomEndpoint() *fwkdl.EndpointMetadata { return nil }

// newStreaming builds a real StreamingServer around the given director, so
// tests substitute scheduling behaviour at the seam handlers already exposes.
func newStreaming(d handlers.Director) *handlers.StreamingServer {
	reg := handlers.NewParserRegistry([]fwkrh.Parser{openai.NewOpenAIParser()}, logr.Discard())
	return handlers.NewStreamingServer(fakeDatastore{}, d, reg, 1<<20)
}

func newDecisionServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(&httpHandler{ss: newStreaming(&fakeDirector{}), maxBody: defaultMaxRoutableBodyBytes})
	t.Cleanup(srv.Close)
	return srv
}

func chatBody(i int) []byte {
	b, _ := json.Marshal(map[string]any{
		"model": "llama-3.1-8b",
		"messages": []map[string]string{
			{"role": "user", "content": fmt.Sprintf("request %d", i)},
		},
	})
	return b
}

// postDecision returns the response headers, which are the decision.
func postDecision(t *testing.T, url string, i int) http.Header {
	t.Helper()
	req, err := http.NewRequest("POST", url+"/v1/chat/completions", bytes.NewReader(chatBody(i)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return resp.Header
}

func settle() {
	for i := 0; i < 12; i++ {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
}

// blockingDirector stands in for a scheduler stuck on something slow: it holds
// every request until the caller's context ends.
type blockingDirector struct {
	fakeDirector
	entered sync.Once
	blocked chan struct{}
}

func (d *blockingDirector) HandleRequest(ctx context.Context, reqCtx *handlers.RequestContext, _ *fwkrh.InferenceRequestBody) (*handlers.RequestContext, error) {
	d.entered.Do(func() { close(d.blocked) })
	<-ctx.Done()
	return reqCtx, ctx.Err()
}

type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(int)             {}

func sizedBody(kb int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"model":"llama-3.1-8b","messages":[{"role":"user","content":"`)
	b.WriteString(strings.Repeat("token ", kb*1024/6))
	b.WriteString(`"}]}`)
	return b.Bytes()
}

// TestDrivesRealProcess is the claim: an HTTP request reaches a routing
// decision through EPP's unmodified ext_proc entry point.
func TestDrivesRealProcess(t *testing.T) {
	srv := newDecisionServer(t)
	hdr := postDecision(t, srv.URL, 0)

	require.Equal(t, wantEndpoint, hdr.Get(metadata.DestinationEndpointKey),
		"the decision should arrive through EPP's own header mutation, not the frontend")
}

// TestResponseCarriesOnlyTheDecision pins a defect. An ext_proc header mutation
// re-emits the caller's own headers and a Content-Length for the request body.
// Copying that set onto the response declared a body length nothing would
// write, so the server closed the connection under the client.
func TestResponseCarriesOnlyTheDecision(t *testing.T) {
	srv := newDecisionServer(t)

	req, err := http.NewRequest("POST", srv.URL+"/v1/chat/completions", bytes.NewReader(chatBody(0)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Caller-Header", "do-not-echo")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "the response must be readable, not a declared length that never arrives")
	require.Empty(t, body)
	require.Equal(t, wantEndpoint, resp.Header.Get(metadata.DestinationEndpointKey))
	require.Empty(t, resp.Header.Get("X-Caller-Header"), "the caller's own headers must not come back")
}

// TestConcurrentRequests: each request runs its own Process on the shared
// StreamingServer, so no two requests may share stream state and no goroutine
// may outlive its request.
func TestConcurrentRequests(t *testing.T) {
	srv := newDecisionServer(t)

	postDecision(t, srv.URL, 0) // warm lazy state before counting
	settle()
	before := runtime.NumGoroutine()

	const n = 300
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if got := postDecision(t, srv.URL, i).Get(metadata.DestinationEndpointKey); got != wantEndpoint {
				errs <- fmt.Errorf("request %d got %q", i, got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	settle()
	after := runtime.NumGoroutine()
	// Idle HTTP server goroutines linger, so allow slack. A per-request leak
	// shows up as roughly n, not a handful.
	require.Less(t, after-before, 50,
		"goroutines grew by %d over %d requests: Process is not returning", after-before, n)
}

// TestPathIsPassedThrough proves the frontend owns no route of its own. EPP
// resolves parsers by path suffix, so EPP must reject an unknown path and name
// that exact path, rather than the frontend swallowing or rewriting it.
func TestPathIsPassedThrough(t *testing.T) {
	srv := newDecisionServer(t)

	req, err := http.NewRequest("POST", srv.URL+"/v1/not-a-real-api", bytes.NewReader(chatBody(0)))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(raw), "/v1/not-a-real-api",
		"EPP should see the caller's path verbatim")
}

// TestClientCancellationDoesNotLeak: a goroutine per in-flight request means a
// client that disappears must not strand one. Process observes cancellation
// through the stream context; decide drains to completion either way.
func TestClientCancellationDoesNotLeak(t *testing.T) {
	slow := &blockingDirector{blocked: make(chan struct{})}
	blocked := slow.blocked
	h := &httpHandler{ss: newStreaming(slow), maxBody: 1 << 20}

	settle()
	before := runtime.NumGoroutine()

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				bytes.NewReader(sizedBody(1))).WithContext(ctx)
			done := make(chan struct{})
			go func() { defer close(done); h.ServeHTTP(&discardWriter{h: make(http.Header)}, req) }()
			time.Sleep(2 * time.Millisecond) // hang up mid-decision
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("handler did not return after the client cancelled")
			}
		}()
	}
	wg.Wait()

	settle()
	after := runtime.NumGoroutine()
	if after-before > 25 {
		t.Errorf("goroutines grew by %d over %d cancelled requests", after-before, n)
	}
	<-blocked
}

// TestOverLimitBodyIsRejected pins a reviewed defect: readBody once wrapped the
// body at exactly maxBody and tolerated the resulting ErrUnexpectedEOF, so an
// over-limit request was silently truncated and routed on. The gRPC frontend
// errors at its ceiling; accepting a short body here made the two frontends
// disagree about the same request.
func TestOverLimitBodyIsRejected(t *testing.T) {
	h := &httpHandler{ss: newStreaming(&fakeDirector{}), maxBody: 8 << 10}
	big := bytes.Repeat([]byte("x"), 64<<10)

	for _, tc := range []struct {
		name   string
		length int64 // -1 keeps the real length
	}{
		{"honest over-limit", -1},
		{"understated length", 4 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(big))
			if tc.length >= 0 {
				req.ContentLength = tc.length
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("got %d, want 413: an over-limit body must be rejected, not truncated", w.Code)
			}
		})
	}
}

// TestUnderLimitBodyStillWorks guards the obvious over-correction.
func TestUnderLimitBodyStillWorks(t *testing.T) {
	h := &httpHandler{ss: newStreaming(&fakeDirector{}), maxBody: 8 << 10}
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		bytes.NewReader(sizedBody(4)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", w.Code)
	}
}
