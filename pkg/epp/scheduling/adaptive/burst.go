package adaptive

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	// burstFastWindow: smallest window where Poisson noise (≈1/√(λW))
	// fits below the 0.3 hysteresis band at typical inference rates.
	// Aligned to burstDetectorCycle.
	burstFastWindow = 5 * time.Second

	// burstSlowWindow: 6× fast for clean drift-vs-burst separation.
	// Longer than typical request lifecycle, shorter than autoscaler
	// reaction.
	burstSlowWindow = 30 * time.Second

	// burstActivateThreshold = 1.6× rate at amplitude=1. Above the
	// noise floor, below the 2× peak of typical UI/agent surges.
	burstActivateThreshold = 0.6

	// burstDeactivateThreshold: hysteresis band of 0.3 ≈ 2× noise
	// floor. Asymmetric exit (1.6× enter / 1.3× leave) survives
	// transient gaps inside a real burst and prefers staying in burst
	// slightly too long over flapping.
	burstDeactivateThreshold = 0.3

	// burstAmplitude: signal saturates at fast/slow = 2×. Real bursts
	// span 2–5× and BurstActive is binary, so >2× should all map to
	// signal=1.
	burstAmplitude = 1.0
)

// ArrivalSampler reports request arrivals since the last call.
type ArrivalSampler interface {
	SampleSince(ctx context.Context, since time.Time) (count int, now time.Time, err error)
}

// BurstDetector emits a [0, 1] signal proportional to (fast/slow) - 1
// and toggles BurstActive with hysteresis.
type BurstDetector struct {
	sampler ArrivalSampler
	cycle   time.Duration

	fast []sample
	slow []sample
	last time.Time

	active bool
}

type sample struct {
	at    time.Time
	count int
}

// NewBurstDetector builds a detector with the given sampler and cycle.
func NewBurstDetector(sampler ArrivalSampler, cycle time.Duration) *BurstDetector {
	return &BurstDetector{sampler: sampler, cycle: cycle}
}

func (d *BurstDetector) Name() string { return "burst" }

func (d *BurstDetector) Run(ctx context.Context, out chan<- Signal) error {
	ticker := time.NewTicker(d.cycle)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			count, now, err := d.sampler.SampleSince(ctx, d.last)
			if err != nil {
				continue
			}
			d.last = now
			d.recordSample(now, count)
			signal := d.computeSignal(now)
			d.updateActive(signal)
			select {
			case out <- Signal{Name: d.Name(), Value: signal, Timestamp: now}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// IsActive reports whether BurstActive is set after the most recent cycle.
func (d *BurstDetector) IsActive() bool { return d.active }

func (d *BurstDetector) recordSample(at time.Time, count int) {
	s := sample{at: at, count: count}
	d.fast = pruneAndAppend(d.fast, s, at.Add(-burstFastWindow))
	d.slow = pruneAndAppend(d.slow, s, at.Add(-burstSlowWindow))
}

func (d *BurstDetector) computeSignal(now time.Time) float64 {
	fastRate := windowRate(d.fast, now, burstFastWindow)
	slowRate := windowRate(d.slow, now, burstSlowWindow)
	if slowRate == 0 {
		return 0
	}
	excess := fastRate/slowRate - 1.0
	if excess <= 0 {
		return 0
	}
	if excess >= burstAmplitude {
		return 1.0
	}
	return excess / burstAmplitude
}

func (d *BurstDetector) updateActive(signal float64) {
	if !d.active && signal >= burstActivateThreshold {
		d.active = true
	} else if d.active && signal <= burstDeactivateThreshold {
		d.active = false
	}
}

func pruneAndAppend(buf []sample, s sample, cutoff time.Time) []sample {
	keep := buf[:0]
	for _, b := range buf {
		if b.at.After(cutoff) {
			keep = append(keep, b)
		}
	}
	return append(keep, s)
}

func windowRate(buf []sample, now time.Time, window time.Duration) float64 {
	if len(buf) == 0 {
		return 0
	}
	var total int
	cutoff := now.Add(-window)
	for _, s := range buf {
		if s.at.After(cutoff) {
			total += s.count
		}
	}
	return float64(total) / window.Seconds()
}

// ArrivalCounter is the global request-arrival counter the BurstDetector
// observes. 
type ArrivalCounter struct {
	n atomic.Int64
}

func (c *ArrivalCounter) Inc() { c.n.Add(1) }

func (c *ArrivalCounter) Add(n int64) { c.n.Add(n) }

func (c *ArrivalCounter) Load() int64 { return c.n.Load() }

// CounterArrivalSampler implements ArrivalSampler over an ArrivalCounter.
type CounterArrivalSampler struct {
	counter *ArrivalCounter
	lastN   int64
}

// NewCounterArrivalSampler builds a sampler over the supplied counter.
func NewCounterArrivalSampler(counter *ArrivalCounter) *CounterArrivalSampler {
	return &CounterArrivalSampler{counter: counter}
}

// SampleSince returns the delta in arrivals since the previous call
// (or 0 for the first call) and the current wall-clock time. The
// `since` timestamp is unused — the counter is the source of truth.
func (s *CounterArrivalSampler) SampleSince(_ context.Context, _ time.Time) (int, time.Time, error) {
	cur := s.counter.Load()
	delta := cur - s.lastN
	s.lastN = cur
	return int(delta), time.Now(), nil
}
