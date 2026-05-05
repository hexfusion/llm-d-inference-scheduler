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
// per endpoint. ImbalanceDetector computes CV across these values.
type LoadObservation struct {
	// Loads is one float per endpoint in [0, 1]. The detector
	// computes spread; it doesn't care which metric (KV usage,
	// queue depth, etc.) the deployment chose.
	Loads []float64
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
			cv := coefficientOfVariation(obs.Loads)
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

// DatastoreLoadSampler is the production LoadSampler: it walks the
// EPP datastore on each Sample call and collects per-pod KV cache
// utilization. KVCacheUsagePercent ships as a fraction in [0, 1] so
// no scaling is needed before CV computation.
type DatastoreLoadSampler struct {
	ds PodLister
}

// NewDatastoreLoadSampler binds a sampler to the supplied pod source.
func NewDatastoreLoadSampler(ds PodLister) *DatastoreLoadSampler {
	return &DatastoreLoadSampler{ds: ds}
}

// Sample collects per-endpoint KV cache utilization. 
func (s *DatastoreLoadSampler) Sample(_ context.Context) (LoadObservation, error) {
	pods := s.ds.PodList(func(_ fwkdl.Endpoint) bool { return true })
	loads := make([]float64, 0, len(pods))
	for _, pod := range pods {
		m := pod.GetMetrics()
		if m == nil {
			continue
		}
		loads = append(loads, m.KVCacheUsagePercent)
	}
	return LoadObservation{Loads: loads}, nil
}
