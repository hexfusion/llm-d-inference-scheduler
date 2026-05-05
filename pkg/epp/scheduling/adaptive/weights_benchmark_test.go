package adaptive

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)


func BenchmarkEffectiveWeight_ZeroSignal(b *testing.B) {
	scorer := fakeScorer{weight: 0.7, category: fwksched.Affinity}
	sigs := SignalState{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EffectiveWeight(scorer, sigs)
	}
}

func BenchmarkEffectiveWeight_Modulating(b *testing.B) {
	scorer := fakeScorer{weight: 0.7, category: fwksched.Affinity}
	sigs := SignalState{Imbalance: 0.7, Burst: 0.2}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EffectiveWeight(scorer, sigs)
	}
}

func recordWeightSetup(b *testing.B) {
	b.Helper()
	RegisterMetrics(prometheus.NewRegistry())
}

func BenchmarkRecordWeight(b *testing.B) {
	recordWeightSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RecordWeight("queue-scorer", "Balance", 0.7)
	}
}

func BenchmarkRecordWeight_Parallel(b *testing.B) {
	recordWeightSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			RecordWeight("queue-scorer", "Balance", 0.7)
		}
	})
}

func BenchmarkAdaptiveHotPath_TwoScorers(b *testing.B) {
	// Mimics what runScorerPlugins does per-request when adaptive
	// is on: load signals once (atomic.Pointer dereference), then
	// for each scorer compute EffectiveWeight + RecordWeight. With
	// the production config (queue-scorer + prefix-cache-scorer)
	// that's two of each per request.
	recordWeightSetup(b)
	store := NewSignalStore()
	store.Store(SignalState{Imbalance: 0.7})
	scorerA := fakeScorer{weight: 2.0, category: fwksched.Balance}
	scorerB := fakeScorer{weight: 3.0, category: fwksched.Affinity}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sigs := store.Load()
		wA := EffectiveWeight(scorerA, sigs)
		RecordWeight("queue-scorer", string(scorerA.Category()), wA)
		wB := EffectiveWeight(scorerB, sigs)
		RecordWeight("prefix-cache-scorer", string(scorerB.Category()), wB)
	}
}

func BenchmarkAdaptiveHotPath_TwoScorers_NoRecord(b *testing.B) {
	store := NewSignalStore()
	store.Store(SignalState{Imbalance: 0.7})
	scorerA := fakeScorer{weight: 2.0, category: fwksched.Balance}
	scorerB := fakeScorer{weight: 3.0, category: fwksched.Affinity}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sigs := store.Load()
		_ = EffectiveWeight(scorerA, sigs)
		_ = EffectiveWeight(scorerB, sigs)
	}
}
