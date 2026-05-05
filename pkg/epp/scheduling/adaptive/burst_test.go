package adaptive

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPruneAndAppend(t *testing.T) {
	now := time.Now()
	buf := []sample{
		{at: now.Add(-10 * time.Second), count: 1}, // outside window, will be pruned
		{at: now.Add(-2 * time.Second), count: 2},  // inside, kept
	}
	out := pruneAndAppend(buf, sample{at: now, count: 3}, now.Add(-5*time.Second))
	require.Len(t, out, 2, "one entry should be pruned, two kept (one old + the new)")
	require.Equal(t, 3, out[len(out)-1].count, "last entry should be the appended one")
}

func TestBurstDetector_Hysteresis(t *testing.T) {
	cases := []struct {
		name       string
		signals    []float64
		wantActive bool
	}{
		{"below activate stays inactive", []float64{0.5}, false},
		{"above activate flips on", []float64{0.5, 0.7}, true},
		{"in hysteresis band remains active", []float64{0.5, 0.7, 0.45}, true},
		{"drop below deactivate flips off", []float64{0.5, 0.7, 0.45, 0.2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &BurstDetector{}
			for _, sig := range tc.signals {
				d.updateActive(sig)
			}
			require.Equal(t, tc.wantActive, d.active)
		})
	}
}

func TestBurstDetector_ComputeSignal(t *testing.T) {
	cases := []struct {
		name string
		// build seeds the detector buffers via recordSample.
		build func(d *BurstDetector, now time.Time)
		want  float64
	}{
		{
			name: "massive surge saturates to 1.0",
			build: func(d *BurstDetector, now time.Time) {
				for i := 0; i < 30; i++ {
					d.recordSample(now.Add(-time.Duration(30-i)*time.Second), 1)
				}
				for i := 0; i < 5; i++ {
					d.recordSample(now.Add(-time.Duration(5-i)*time.Second), 20)
				}
			},
			want: 1.0,
		},
		{
			name: "steady traffic produces zero burst",
			build: func(d *BurstDetector, now time.Time) {
				for i := 0; i < 30; i++ {
					d.recordSample(now.Add(-time.Duration(30-i)*time.Second), 5)
				}
			},
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &BurstDetector{}
			now := time.Now()
			tc.build(d, now)
			require.Equal(t, tc.want, d.computeSignal(now))
		})
	}
}

type fakeArrivalSampler struct {
	counts []int
	idx    int
}

func (f *fakeArrivalSampler) SampleSince(_ context.Context, _ time.Time) (int, time.Time, error) {
	if f.idx >= len(f.counts) {
		return 0, time.Now(), nil
	}
	c := f.counts[f.idx]
	f.idx++
	return c, time.Now(), nil
}

func TestBurstDetector_StopsOnContextCancel(t *testing.T) {
	d := NewBurstDetector(&fakeArrivalSampler{counts: []int{1, 1, 1}}, time.Millisecond)
	out := make(chan Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = d.Run(ctx, out)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("burst detector did not exit after context cancel")
	}
}

func TestArrivalCounter_ConcurrentInc(t *testing.T) {
	const goroutines = 32
	const incsPer = 1000
	c := &ArrivalCounter{}

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range incsPer {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(goroutines*incsPer), c.Load())
}

func TestArrivalCounter_AddAccumulates(t *testing.T) {
	c := &ArrivalCounter{}
	c.Add(5)
	c.Add(7)
	c.Inc()
	require.Equal(t, int64(13), c.Load())
}

func TestCounterArrivalSampler(t *testing.T) {
	cases := []struct {
		name string
		// drive operates the counter+sampler through a sequence and
		// returns the observed deltas in order.
		drive  func(*ArrivalCounter, *CounterArrivalSampler) []int
		want   []int
	}{
		{
			name: "first call returns zero delta",
			drive: func(c *ArrivalCounter, s *CounterArrivalSampler) []int {
				d, _, _ := s.SampleSince(context.Background(), time.Time{})
				return []int{d}
			},
			want: []int{0},
		},
		{
			name: "subsequent call returns count since last",
			drive: func(c *ArrivalCounter, s *CounterArrivalSampler) []int {
				_, _, _ = s.SampleSince(context.Background(), time.Time{})
				for range 17 {
					c.Inc()
				}
				d, _, _ := s.SampleSince(context.Background(), time.Time{})
				return []int{d}
			},
			want: []int{17},
		},
		{
			name: "deltas are non-overlapping across calls",
			drive: func(c *ArrivalCounter, s *CounterArrivalSampler) []int {
				_, _, _ = s.SampleSince(context.Background(), time.Time{})
				c.Add(5)
				d1, _, _ := s.SampleSince(context.Background(), time.Time{})
				c.Add(3)
				d2, _, _ := s.SampleSince(context.Background(), time.Time{})
				return []int{d1, d2}
			},
			want: []int{5, 3},
		},
		{
			name: "no arrivals between calls returns zero",
			drive: func(c *ArrivalCounter, s *CounterArrivalSampler) []int {
				_, _, _ = s.SampleSince(context.Background(), time.Time{})
				d, _, _ := s.SampleSince(context.Background(), time.Time{})
				return []int{d}
			},
			want: []int{0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &ArrivalCounter{}
			s := NewCounterArrivalSampler(c)
			require.Equal(t, tc.want, tc.drive(c, s))
		})
	}
}
