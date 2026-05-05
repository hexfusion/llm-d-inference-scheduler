package adaptive

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

type fakeScorer struct {
	weight   float64
	category fwksched.ScorerCategory
}

func (f fakeScorer) Weight() float64                   { return f.weight }
func (f fakeScorer) Category() fwksched.ScorerCategory { return f.category }

func TestEffectiveWeight_PassThrough(t *testing.T) {
	cases := []struct {
		name     string
		category fwksched.ScorerCategory
		sigs     SignalState
	}{
		{"zero signal / affinity", fwksched.Affinity, SignalState{}},
		{"zero signal / distribution", fwksched.Distribution, SignalState{}},
		{"zero signal / balance", fwksched.Balance, SignalState{}},
		{"balance / mid imbalance", fwksched.Balance, SignalState{Imbalance: 0.5}},
		{"balance / max imbalance", fwksched.Balance, SignalState{Imbalance: 1.0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := fakeScorer{weight: 0.7, category: tc.category}
			require.Equal(t, 0.7, EffectiveWeight(s, tc.sigs))
		})
	}
}

func TestEffectiveWeight_AffinityMonotonicallyDecreases(t *testing.T) {
	scorer := fakeScorer{weight: 1.0, category: fwksched.Affinity}
	low := EffectiveWeight(scorer, SignalState{Imbalance: 0.1})
	mid := EffectiveWeight(scorer, SignalState{Imbalance: 0.5})
	high := EffectiveWeight(scorer, SignalState{Imbalance: 0.9})
	require.Greater(t, low, mid)
	require.Greater(t, mid, high)
	require.GreaterOrEqual(t, high, minWeight*1.0, "must stay at or above floor")
}

func TestEffectiveWeight_DistributionMonotonicallyIncreases(t *testing.T) {
	scorer := fakeScorer{weight: 1.0, category: fwksched.Distribution}
	low := EffectiveWeight(scorer, SignalState{Imbalance: 0.1})
	mid := EffectiveWeight(scorer, SignalState{Imbalance: 0.5})
	high := EffectiveWeight(scorer, SignalState{Imbalance: 0.9})
	require.Less(t, low, mid)
	require.Less(t, mid, high)
	require.LessOrEqual(t, high, maxWeight*1.0, "must stay at or below ceiling")
}

func TestSigmoid(t *testing.T) {
	cases := []struct {
		name string
		x    float64
		want float64
	}{
		{"midpoint", 0.5, 0.5},
		{"saturates near 0", 0.0, 0.0},
		{"saturates near 1", 1.0, 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sigmoid(tc.x, sigmoidMidpoint, sigmoidSteepness)
			require.InDelta(t, tc.want, got, 0.01)
		})
	}
}

func TestClamp(t *testing.T) {
	cases := []struct {
		name string
		v    float64
		want float64
	}{
		{"inside", 0.5, 0.5},
		{"below floor", -1.0, 0.0},
		{"above ceiling", 2.0, 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clamp(tc.v, 0.0, 1.0))
		})
	}
}

type fakePicker struct{ name string }

func (f fakePicker) TypedName() plugin.TypedName { return plugin.TypedName{Name: f.name} }
func (f fakePicker) Pick(_ context.Context, _ *fwksched.CycleState, _ []*fwksched.ScoredEndpoint) *fwksched.ProfileRunResult {
	return nil
}

func TestPickerSelector(t *testing.T) {
	def := fakePicker{name: "default"}
	burst := fakePicker{name: "burst"}
	cases := []struct {
		name string
		sel  PickerSelector
		sigs SignalState
		want string
	}{
		{"burst inactive → default", PickerSelector{Default: def, Burst: burst}, SignalState{}, "default"},
		{"burst active → burst", PickerSelector{Default: def, Burst: burst}, SignalState{BurstActive: true}, "burst"},
		{"burst active but Burst nil → default", PickerSelector{Default: def}, SignalState{BurstActive: true}, "default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.sel.SelectPicker(tc.sigs).TypedName().Name)
		})
	}
}
