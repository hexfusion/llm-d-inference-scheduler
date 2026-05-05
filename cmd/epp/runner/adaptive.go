package runner

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/scheduling/adaptive"
)

const (
	imbalanceDetectorCycle = 15 * time.Second
	burstDetectorCycle     = 5 * time.Second
)

// bootstrapAdaptive wires the adaptive subsystem into the running
// EPP. Called only when the AdaptiveRouting feature gate is true.
func bootstrapAdaptive(
	ctx context.Context,
	ds datastore.Datastore,
	reg prometheus.Registerer,
) (*adaptive.SignalStore, *adaptive.ArrivalCounter) {
	logger := ctrl.Log.WithName("adaptive")
	logger.Info("bootstrapping adaptive routing subsystem",
		"imbalance_cycle", imbalanceDetectorCycle.String(),
		"burst_cycle", burstDetectorCycle.String())

	store := adaptive.NewSignalStore()
	counter := &adaptive.ArrivalCounter{}

	loadSampler := adaptive.NewDatastoreLoadSampler(ds)
	arrivalSampler := adaptive.NewCounterArrivalSampler(counter)

	imbalance := adaptive.NewImbalanceDetector(loadSampler, imbalanceDetectorCycle)
	burst := adaptive.NewBurstDetector(arrivalSampler, burstDetectorCycle)

	cfg := adaptive.NewConfigurator(store, imbalance, burst)
	go func() {
		runLogger := log.IntoContext(ctx, logger)
		if err := cfg.Run(runLogger); err != nil {
			logger.Error(err, "adaptive configurator exited with error")
		}
	}()

	if reg != nil {
		adaptive.RegisterMetrics(reg)
	}
	return store, counter
}
