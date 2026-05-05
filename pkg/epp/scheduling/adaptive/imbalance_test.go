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
	loads [][]float64
	idx   int
}

func (f *fakeLoadSampler) Sample(_ context.Context) (LoadObservation, error) {
	if f.idx >= len(f.loads) {
		return LoadObservation{Loads: f.loads[len(f.loads)-1]}, nil
	}
	out := LoadObservation{Loads: f.loads[f.idx]}
	f.idx++
	return out, nil
}

func TestImbalanceDetector_EmitsSmoothedSignalUnderHotspotRamp(t *testing.T) {
	sampler := &fakeLoadSampler{
		loads: [][]float64{
			{0.5, 0.5, 0.5, 0.5},
			{0.5, 0.5, 0.5, 0.6},
			{0.5, 0.5, 0.5, 0.7},
			{0.5, 0.5, 0.5, 0.8},
			{0.5, 0.5, 0.5, 0.9},
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
	det := NewImbalanceDetector(&fakeLoadSampler{loads: [][]float64{{0.5, 0.5}}}, time.Millisecond)
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
	meta := &fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}}
	metrics := fwkdl.NewMetrics()
	metrics.KVCacheUsagePercent = kvUsage
	return fwkdl.NewEndpoint(meta, metrics)
}

// endpointNoMetrics simulates a just-discovered pod with no scrape yet.
type endpointNoMetrics struct {
	fwkdl.Endpoint
}

func (e endpointNoMetrics) GetMetrics() *fwkdl.Metrics { return nil }

func TestDatastoreLoadSampler(t *testing.T) {
	cases := []struct {
		name string
		pods []fwkdl.Endpoint
		want []float64
	}{
		{
			name: "collects from all pods",
			pods: []fwkdl.Endpoint{endpoint("a", 0.62), endpoint("b", 0.30), endpoint("c", 0.95)},
			want: []float64{0.62, 0.30, 0.95},
		},
		{
			name: "skips pods with nil metrics",
			pods: []fwkdl.Endpoint{
				endpoint("a", 0.5),
				endpointNoMetrics{Endpoint: endpoint("just-discovered", 0)},
				endpoint("c", 0.7),
			},
			want: []float64{0.5, 0.7},
		},
		{
			name: "empty pool returns empty observation",
			pods: nil,
			want: []float64{},
		},
		{
			name: "boundary values pass through",
			pods: []fwkdl.Endpoint{endpoint("idle", 0.0), endpoint("saturated", 1.0)},
			want: []float64{0.0, 1.0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewDatastoreLoadSampler(&fakePodLister{pods: tc.pods})
			obs, err := s.Sample(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.want, obs.Loads)
		})
	}
}
