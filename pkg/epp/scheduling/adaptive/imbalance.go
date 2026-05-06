package adaptive

import (
	"context"
	"math"
	"time"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
)

const (
	// imbalanceSmoothingAlpha: EMA weight on the new sample. Lower
	// = heavier smoothing = slower response, quieter noise floor.
	imbalanceSmoothingAlpha = 0.3

	// imbalanceCVCap: CV value at which the signal saturates to 1.0.
	// CV rarely exceeds 1 in practice; capping protects the sigmoid
	// from extreme inputs.
	imbalanceCVCap = 1.0
)

// LoadObservation is one tick of pool-level load metrics, one entry
// per endpoint per metric. CV is scale-invariant, so raw counts are fine.
type LoadObservation struct {
	KV      []float64 // KVCacheUsagePercent per pod
	Waiting []float64 // WaitingQueueSize per pod
	Running []float64 // RunningRequestsSize per pod
}

// LoadSampler is the seam between detectors and the data layer.
// Implementations live in the EPP wiring layer so the adaptive
// subsystem stays domain-loose and unit-testable.
type LoadSampler interface {
	Sample(ctx context.Context) (LoadObservation, error)
}

// ImbalanceDetector emits a smoothed [0, 1] signal proportional to
// the coefficient of variation of per-endpoint load.
type ImbalanceDetector struct {
	sampler  LoadSampler
	cycle    time.Duration
	smoothed float64
	primed   bool
}

// NewImbalanceDetector builds a detector with the given sampler and
// cycle interval. 
func NewImbalanceDetector(sampler LoadSampler, cycle time.Duration) *ImbalanceDetector {
	return &ImbalanceDetector{sampler: sampler, cycle: cycle}
}

func (d *ImbalanceDetector) Name() string { return "imbalance" }

func (d *ImbalanceDetector) Run(ctx context.Context, out chan<- Signal) error {
	ticker := time.NewTicker(d.cycle)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			obs, err := d.sampler.Sample(ctx)
			if err != nil {
				continue
			}
			// Max CV across metrics: any one dimension diverging is a real signal.
			cv := math.Max(coefficientOfVariation(obs.KV),
				math.Max(coefficientOfVariation(obs.Waiting),
					coefficientOfVariation(obs.Running)))
			if math.IsNaN(cv) {
				cv = 0
			}
			if cv > imbalanceCVCap {
				cv = imbalanceCVCap
			}
			if !d.primed {
				d.smoothed = cv
				d.primed = true
			} else {
				d.smoothed = imbalanceSmoothingAlpha*cv + (1.0-imbalanceSmoothingAlpha)*d.smoothed
			}
			select {
			case out <- Signal{Name: d.Name(), Value: d.smoothed, Timestamp: time.Now()}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// coefficientOfVariation returns stddev/mean, or 0 for empty, single-
// element, or zero-mean inputs (no spread observable).
func coefficientOfVariation(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	if mean == 0 {
		return 0
	}
	var sq float64
	for _, x := range xs {
		d := x - mean
		sq += d * d
	}
	return math.Sqrt(sq/float64(len(xs))) / mean
}

// PodLister is the subset of datastore.Datastore that
// DatastoreLoadSampler needs. 
type PodLister interface {
	PodList(predicate func(fwkdl.Endpoint) bool) []fwkdl.Endpoint
}

// DatastoreLoadSampler walks the EPP datastore and collects KV, waiting,
// and running per pod — the three signals the proposal calls out.
type DatastoreLoadSampler struct {
	ds PodLister
}

func NewDatastoreLoadSampler(ds PodLister) *DatastoreLoadSampler {
	return &DatastoreLoadSampler{ds: ds}
}

func (s *DatastoreLoadSampler) Sample(_ context.Context) (LoadObservation, error) {
	pods := s.ds.PodList(func(_ fwkdl.Endpoint) bool { return true })
	obs := LoadObservation{
		KV:      make([]float64, 0, len(pods)),
		Waiting: make([]float64, 0, len(pods)),
		Running: make([]float64, 0, len(pods)),
	}
	for _, pod := range pods {
		m := pod.GetMetrics()
		if m == nil {
			continue
		}
		obs.KV = append(obs.KV, m.KVCacheUsagePercent)
		obs.Waiting = append(obs.Waiting, float64(m.WaitingQueueSize))
		obs.Running = append(obs.Running, float64(m.RunningRequestsSize))
	}
	return obs, nil
}
