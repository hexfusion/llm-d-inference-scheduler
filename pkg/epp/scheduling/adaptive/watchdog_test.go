package adaptive

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock is a minimal monotonic clock for watchdog tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newFakeClockWatchdog() (*Watchdog, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)}
	return newWatchdogWithClock(clk.Now), clk
}

func TestWatchdog_TransitionCounting(t *testing.T) {
	cases := []struct {
		name string
		// run drives a fresh watchdog/clock pair, returning whether
		// the watchdog tripped at any point.
		run             func(*testing.T, *Watchdog, *fakeClock) (tripped bool)
		wantTripped     bool
		wantFrozenAtEnd bool
	}{
		{
			name: "below threshold never trips",
			run: func(t *testing.T, w *Watchdog, clk *fakeClock) bool {
				for i := 0; i < watchdogMaxTransitions; i++ {
					require.False(t, w.RecordTransition(), "trip at i=%d", i)
					clk.Advance(time.Second)
				}
				return false
			},
			wantTripped:     false,
			wantFrozenAtEnd: false,
		},
		{
			name: "at threshold+1 trips exactly once",
			run: func(t *testing.T, w *Watchdog, clk *fakeClock) bool {
				for i := 0; i < watchdogMaxTransitions; i++ {
					w.RecordTransition()
					clk.Advance(time.Second)
				}
				tripped := w.RecordTransition()
				// Subsequent in-freeze transitions must NOT re-trip
				// (avoids duplicate metric emission).
				clk.Advance(time.Second)
				require.False(t, w.RecordTransition(), "re-tripped while already frozen")
				return tripped
			},
			wantTripped:     true,
			wantFrozenAtEnd: true,
		},
		{
			name: "stale transitions outside window are pruned",
			run: func(t *testing.T, w *Watchdog, clk *fakeClock) bool {
				w.RecordTransition()
				clk.Advance(watchdogWindow + time.Second)
				for i := 0; i < watchdogMaxTransitions; i++ {
					require.False(t, w.RecordTransition(), "stale entry not pruned, i=%d", i)
					clk.Advance(time.Second)
				}
				return false
			},
			wantTripped:     false,
			wantFrozenAtEnd: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, clk := newFakeClockWatchdog()
			tripped := tc.run(t, w, clk)
			require.Equal(t, tc.wantTripped, tripped, "tripped")
			require.Equal(t, tc.wantFrozenAtEnd, w.IsFrozen(), "IsFrozen at end")
		})
	}
}

func TestWatchdog_FreezeLifecycle(t *testing.T) {
	w, clk := newFakeClockWatchdog()

	// Trip the watchdog.
	for i := 0; i <= watchdogMaxTransitions; i++ {
		w.RecordTransition()
	}
	require.True(t, w.IsFrozen(), "expected initial freeze")

	// Advance past the freeze duration.
	clk.Advance(watchdogFreezeDuration + time.Second)
	require.False(t, w.IsFrozen(), "expected unfreeze after freeze duration elapses")

	// Advance further so old transitions also fall out of the
	// rolling window — otherwise the rolling-window count alone
	// would re-trip immediately, masking whether freeze tracking
	// reset properly.
	clk.Advance(watchdogWindow)

	// Build up under threshold again — should not trip.
	for i := 0; i < watchdogMaxTransitions; i++ {
		require.False(t, w.RecordTransition(), "trip at i=%d after unfreeze, expected fresh count", i)
		clk.Advance(time.Second)
	}
	// One more should re-trip.
	require.True(t, w.RecordTransition(), "expected re-trip after fresh accumulation")
}

func TestWatchdog_Concurrent(t *testing.T) {
	w := NewWatchdog()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				_ = w.RecordTransition()
				_ = w.IsFrozen()
			}
		})
	}
	wg.Wait()
}
