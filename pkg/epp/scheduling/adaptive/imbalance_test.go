package adaptive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
)

func TestCoefficientOfVariation(t *testing.T) {
	cases := []struct {
		name string
		in   []float64
		want float64
	}{
		{"empty", nil, 0},
		{"single element", []float64{0.5}, 0},
		{"all equal", []float64{0.4, 0.4, 0.4, 0.4}, 0},
		{"zero mean", []float64{0, 0, 0}, 0},
		{"two-element split", []float64{0.2, 0.8}, 0.6},
		{"hotspot one of four", []float64{0.95, 0.3, 0.3, 0.3}, 0.609},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.InDelta(t, tc.want, coefficientOfVariation(tc.in), 0.01)
		})
	}
}

type fakeLoadSampler struct {
	obs []LoadObservation
	idx int
}

func (f *fakeLoadSampler) Sample(_ context.Context) (LoadObservation, error) {
	if f.idx >= len(f.obs) {
		return f.obs[len(f.obs)-1], nil
	}
	out := f.obs[f.idx]
	f.idx++
	return out, nil
}

// kvOnly is a test helper to build observations from a single KV vector.
func kvOnly(loads ...float64) LoadObservation { return LoadObservation{KV: loads} }

func TestImbalanceDetector_EmitsSmoothedSignalUnderHotspotRamp(t *testing.T) {
	sampler := &fakeLoadSampler{
		obs: []LoadObservation{
			kvOnly(0.5, 0.5, 0.5, 0.5),
			kvOnly(0.5, 0.5, 0.5, 0.6),
			kvOnly(0.5, 0.5, 0.5, 0.7),
			kvOnly(0.5, 0.5, 0.5, 0.8),
			kvOnly(0.5, 0.5, 0.5, 0.9),
		},
	}
	det := NewImbalanceDetector(sampler, 5*time.Millisecond)
	out := make(chan Signal, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _ = det.Run(ctx, out) }()

	var lastValue float64
	collected := 0
	deadline := time.After(150 * time.Millisecond)
loop:
	for {
		select {
		case sig := <-out:
			require.Equal(t, "imbalance", sig.Name)
			require.GreaterOrEqual(t, sig.Value, 0.0)
			require.LessOrEqual(t, sig.Value, 1.0)
			lastValue = sig.Value
			collected++
			if collected >= 5 {
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	require.Greater(t, collected, 0, "no signals emitted")
	require.Greater(t, lastValue, 0.0, "smoothed signal should rise after hotspot ramp")
}

func TestImbalanceDetector_StopsOnContextCancel(t *testing.T) {
	det := NewImbalanceDetector(&fakeLoadSampler{obs: []LoadObservation{kvOnly(0.5, 0.5)}}, time.Millisecond)
	out := make(chan Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = det.Run(ctx, out)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("detector did not exit after context cancel")
	}
}

// fakePodLister returns a fixed slice of endpoints.
type fakePodLister struct {
	pods []fwkdl.Endpoint
}

func (f *fakePodLister) PodList(predicate func(fwkdl.Endpoint) bool) []fwkdl.Endpoint {
	out := make([]fwkdl.Endpoint, 0, len(f.pods))
	for _, p := range f.pods {
		if predicate == nil || predicate(p) {
			out = append(out, p)
		}
	}
	return out
}

func endpoint(name string, kvUsage float64) fwkdl.Endpoint {
	return endpointFull(name, kvUsage, 0, 0)
}

func endpointFull(name string, kvUsage float64, waiting, running int) fwkdl.Endpoint {
	meta := &fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}}
	metrics := fwkdl.NewMetrics()
	metrics.KVCacheUsagePercent = kvUsage
	metrics.WaitingQueueSize = waiting
	metrics.RunningRequestsSize = running
	return fwkdl.NewEndpoint(meta, metrics)
}

// endpointNoMetrics simulates a just-discovered pod with no scrape yet.
type endpointNoMetrics struct {
	fwkdl.Endpoint
}

func (e endpointNoMetrics) GetMetrics() *fwkdl.Metrics { return nil }

func TestDatastoreLoadSampler(t *testing.T) {
	cases := []struct {
		name        string
		pods        []fwkdl.Endpoint
		wantKV      []float64
		wantWaiting []float64
		wantRunning []float64
	}{
		{
			name: "collects all three metrics per pod",
			pods: []fwkdl.Endpoint{
				endpointFull("a", 0.62, 5, 12),
				endpointFull("b", 0.30, 0, 3),
				endpointFull("c", 0.95, 50, 200),
			},
			wantKV:      []float64{0.62, 0.30, 0.95},
			wantWaiting: []float64{5, 0, 50},
			wantRunning: []float64{12, 3, 200},
		},
		{
			name: "skips pods with nil metrics",
			pods: []fwkdl.Endpoint{
				endpointFull("a", 0.5, 1, 2),
				endpointNoMetrics{Endpoint: endpoint("just-discovered", 0)},
				endpointFull("c", 0.7, 3, 4),
			},
			wantKV:      []float64{0.5, 0.7},
			wantWaiting: []float64{1, 3},
			wantRunning: []float64{2, 4},
		},
		{
			name:        "empty pool returns empty observation",
			pods:        nil,
			wantKV:      []float64{},
			wantWaiting: []float64{},
			wantRunning: []float64{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewDatastoreLoadSampler(&fakePodLister{pods: tc.pods})
			obs, err := s.Sample(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.wantKV, obs.KV)
			require.Equal(t, tc.wantWaiting, obs.Waiting)
			require.Equal(t, tc.wantRunning, obs.Running)
		})
	}
}

// TestImbalanceDetector_MaxCVAcrossMetrics verifies the detector picks up
// imbalance from any one metric — proves KV-flat-but-queue-imbalanced
// (which is what we measured on T4 with max_num_seqs=256) still fires.
func TestImbalanceDetector_MaxCVAcrossMetrics(t *testing.T) {
	cases := []struct {
		name      string
		obs       LoadObservation
		wantSig   bool // true if signal should be > 0
	}{
		{
			name:    "all balanced",
			obs:     LoadObservation{KV: []float64{0.5, 0.5}, Waiting: []float64{10, 10}, Running: []float64{50, 50}},
			wantSig: false,
		},
		{
			name:    "kv flat but waiting hotspot",
			obs:     LoadObservation{KV: []float64{0.1, 0.1}, Waiting: []float64{0, 100}, Running: []float64{50, 50}},
			wantSig: true,
		},
		{
			name:    "kv flat but running hotspot",
			obs:     LoadObservation{KV: []float64{0.1, 0.1}, Waiting: []float64{0, 0}, Running: []float64{10, 250}},
			wantSig: true,
		},
		{
			name:    "kv hotspot",
			obs:     LoadObservation{KV: []float64{0.1, 0.9}, Waiting: []float64{0, 0}, Running: []float64{0, 0}},
			wantSig: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sampler := &fakeLoadSampler{obs: []LoadObservation{tc.obs}}
			det := NewImbalanceDetector(sampler, 2*time.Millisecond)
			out := make(chan Signal, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			go func() { _ = det.Run(ctx, out) }()
			sig := <-out
			if tc.wantSig {
				require.Greater(t, sig.Value, 0.0)
			} else {
				require.Equal(t, 0.0, sig.Value)
			}
		})
	}
}
