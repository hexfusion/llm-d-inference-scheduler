package epp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	envoyCorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

// Both transports drive the same StreamingServer, so the measured gap is
// transport plus translation only. ext_proc echoes the body back over a fresh
// per-request stream, HTTP declines the echo and reads the decision off a header.

const chatCompletionsPath = "/v1/chat/completions"

// perfWinner is the pod the fixed metrics below make the unambiguous pick, so a
// silent misroute cannot benchmark as a fast success.
const perfWinner = "192.168.1.2:8000"

// buildChatBody is a chat completion of roughly kb kilobytes. Both transports
// send these exact bytes, so any decision difference is the transport, not the
// payload.
func buildChatBody(kb int) []byte {
	content := strings.Repeat("token ", kb*1024/6)
	b, err := json.Marshal(map[string]any{
		"model":    modelMyModel,
		"messages": []map[string]string{{"role": "user", "content": content}},
	})
	if err != nil {
		panic(err)
	}
	return b
}

// extProcHeader is the request-headers phase message. The path resolves the
// openai parser, matching the URL the HTTP client posts to.
func extProcHeader() *extProcPb.ProcessingRequest {
	return &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extProcPb.HttpHeaders{
				Headers: &envoyCorev3.HeaderMap{Headers: []*envoyCorev3.HeaderValue{
					{Key: ":path", RawValue: []byte(chatCompletionsPath)},
					{Key: "content-type", RawValue: []byte("application/json")},
					{Key: metadata.ObjectiveKey, RawValue: []byte(modelMyModel)},
					{Key: reqcommon.RequestIDHeaderKey, RawValue: []byte("test-request-id")},
				}},
			},
		},
	}
}

func extProcBody(body []byte) *extProcPb.ProcessingRequest {
	return &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestBody{
			RequestBody: &extProcPb.HttpBody{Body: body, EndOfStream: true},
		},
	}
}

// endpointFromResponse reads the picked endpoint out of a header mutation. The
// rest of the mutation is ext_proc plumbing, not the decision.
func endpointFromResponse(resp *extProcPb.ProcessingResponse) string {
	var common *extProcPb.CommonResponse
	switch {
	case resp.GetRequestHeaders() != nil:
		common = resp.GetRequestHeaders().GetResponse()
	case resp.GetRequestBody() != nil:
		common = resp.GetRequestBody().GetResponse()
	}
	for _, hv := range common.GetHeaderMutation().GetSetHeaders() {
		if hv.GetHeader().GetKey() == metadata.DestinationEndpointKey {
			if v := hv.GetHeader().GetRawValue(); len(v) > 0 {
				return string(v)
			}
			return hv.GetHeader().GetValue()
		}
	}
	return ""
}

// extProcDecide runs one request over a fresh ext_proc stream and returns the
// picked endpoint. It closes send before draining, so the server reaches EOF
// and the stream ends cleanly rather than leaking a goroutine. Draining to EOF
// pays the full body-echo cost, which is the point of the comparison.
func extProcDecide(ctx context.Context, stub extProcPb.ExternalProcessorClient, header, body *extProcPb.ProcessingRequest) (string, error) {
	stream, err := stub.Process(ctx)
	if err != nil {
		return "", err
	}
	if err := stream.Send(header); err != nil {
		return "", err
	}
	if err := stream.Send(body); err != nil {
		return "", err
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}
	var endpoint string
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return endpoint, nil
		}
		if err != nil {
			return "", err
		}
		if ep := endpointFromResponse(resp); ep != "" {
			endpoint = ep
		}
	}
}

// httpDecide posts the body and reads the decision off the response header. The
// body is drained so the keep-alive connection is reused across iterations.
func httpDecide(client *http.Client, url string, body []byte) (string, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(metadata.ObjectiveKey, modelMyModel)
	req.Header.Set(reqcommon.RequestIDHeaderKey, "test-request-id")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp.Header.Get(metadata.DestinationEndpointKey), nil
}

// newPerfHarness stands up one EPP serving both frontends, with pod metrics that
// make perfWinner the unambiguous pick.
func newPerfHarness(ctx context.Context, t testing.TB) *TestHarness {
	t.Helper()
	h := NewTestHarness(ctx, t, WithStandardMode(), WithHTTPFrontend())
	h = h.WithBaseResources()
	h.WithPods([]PodState{
		P(0, 10, 0.9),
		P(1, 0, 0.1), // lowest queue and kv: the winner
		P(2, 10, 0.9),
	}).WaitForSync(3, modelMyModel)
	h.WaitForReadyPodsMetric(3)
	return h
}

// warmBoth blocks until both frontends answer, warming the connection and the
// shared prefix cache so the first timed request is not an outlier, and returns
// the two endpoints for a parity check.
func warmBoth(t testing.TB, ctx context.Context, stub extProcPb.ExternalProcessorClient, header, body *extProcPb.ProcessingRequest, client *http.Client, url string, httpBody []byte) (string, string) {
	t.Helper()
	var grpcEP, httpEP string
	require.Eventually(t, func() bool {
		var err error
		grpcEP, err = extProcDecide(ctx, stub, header, body)
		if err != nil || grpcEP == "" {
			return false
		}
		httpEP, err = httpDecide(client, url, httpBody)
		return err == nil && httpEP != ""
	}, 15*time.Second, 100*time.Millisecond, "both frontends should return a decision")
	return grpcEP, httpEP
}

// TestHTTPvsExtProcParity is the correctness gate the benchmark leans on: the
// same content must route to the same endpoint over both transports.
func TestHTTPvsExtProcParity(t *testing.T) {
	ctx := t.Context()
	h := newPerfHarness(ctx, t)

	stub := extProcPb.NewExternalProcessorClient(h.grpcConn)
	client := &http.Client{Timeout: 10 * time.Second}
	url := h.HTTPBaseURL + chatCompletionsPath
	body := buildChatBody(8)

	grpcEP, httpEP := warmBoth(t, ctx, stub, extProcHeader(), extProcBody(body), client, url, body)
	require.Equal(t, grpcEP, httpEP, "same content routed to different endpoints across transports")
	require.Equal(t, perfWinner, httpEP)
}

// BenchmarkHTTPvsExtProc times both transports across body sizes so the standard
// ns/op and allocs/op output shows the HTTP frontend is not a performance
// regression against ext_proc. One envtest harness is reused across sub-benchmarks.
func BenchmarkHTTPvsExtProc(b *testing.B) {
	ctx := b.Context()
	h := newPerfHarness(ctx, b)

	stub := extProcPb.NewExternalProcessorClient(h.grpcConn)
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256},
	}
	url := h.HTTPBaseURL + chatCompletionsPath
	header := extProcHeader()

	// Parity must hold before timing, and the round trip warms the connection and
	// shared prefix cache so the first timed request is not an outlier.
	warm := buildChatBody(8)
	grpcEP, httpEP := warmBoth(b, ctx, stub, header, extProcBody(warm), client, url, warm)
	require.Equal(b, grpcEP, httpEP, "parity must hold before timing")

	for _, kb := range []int{1, 8, 64, 512} {
		body := buildChatBody(kb)
		bodyMsg := extProcBody(body)
		size := fmt.Sprintf("%dKB", kb)

		b.Run("extproc/"+size, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ep, err := extProcDecide(ctx, stub, header, bodyMsg)
				if err != nil || ep == "" {
					b.Fatalf("extproc decide failed: err=%v ep=%q", err, ep)
				}
			}
		})
		b.Run("http/"+size, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ep, err := httpDecide(client, url, body)
				if err != nil || ep == "" {
					b.Fatalf("http decide failed: err=%v ep=%q", err, ep)
				}
			}
		})
	}

	// Concurrent case: cost is per concurrent request. A single fixed size (64KB)
	// keeps the two rows comparable.
	body := buildChatBody(64)
	bodyMsg := extProcBody(body)
	b.Run("extproc/64KB-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ep, err := extProcDecide(ctx, stub, header, bodyMsg)
				if err != nil || ep == "" {
					b.Fatalf("extproc decide failed: err=%v ep=%q", err, ep)
				}
			}
		})
	})
	b.Run("http/64KB-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ep, err := httpDecide(client, url, body)
				if err != nil || ep == "" {
					b.Fatalf("http decide failed: err=%v ep=%q", err, ep)
				}
			}
		})
	})
}
