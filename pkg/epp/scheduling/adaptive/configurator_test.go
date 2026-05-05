package adaptive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigurator_HandleSignal(t *testing.T) {
	cases := []struct {
		name        string
		signals     []Signal
		wantState   SignalState
		strictEqual bool // when true, compare full struct minus UpdatedAt; otherwise only fields named below
	}{
		{
			name:    "imbalance signal sets Imbalance",
			signals: []Signal{{Name: "imbalance", Value: 0.42, Timestamp: time.Now()}},
			wantState: SignalState{Imbalance: 0.42},
		},
		{
			name: "burst below activate stays inactive",
			signals: []Signal{
				{Name: "burst", Value: 0.5, Timestamp: time.Now()},
			},
			wantState: SignalState{Burst: 0.5, BurstActive: false},
		},
		{
			name: "burst crosses activate then drops in hysteresis band stays active",
			signals: []Signal{
				{Name: "burst", Value: 0.5, Timestamp: time.Now()},
				{Name: "burst", Value: 0.7, Timestamp: time.Now()},
				{Name: "burst", Value: 0.4, Timestamp: time.Now()},
			},
			wantState: SignalState{Burst: 0.4, BurstActive: true},
		},
		{
			name: "burst drops below deactivate flips off",
			signals: []Signal{
				{Name: "burst", Value: 0.7, Timestamp: time.Now()},
				{Name: "burst", Value: 0.2, Timestamp: time.Now()},
			},
			wantState: SignalState{Burst: 0.2, BurstActive: false},
		},
		{
			name: "unknown detector signal is ignored",
			signals: []Signal{
				{Name: "future-detector", Value: 0.9, Timestamp: time.Now()},
			},
			wantState: SignalState{}, // no change vs zero
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewSignalStore()
			c := &Configurator{store: store, fanIn: make(chan Signal, 1)}
			for _, sig := range tc.signals {
				c.handleSignal(sig)
			}
			got := store.Load()
			require.Equal(t, tc.wantState.Imbalance, got.Imbalance)
			require.Equal(t, tc.wantState.Burst, got.Burst)
			require.Equal(t, tc.wantState.BurstActive, got.BurstActive)
		})
	}
}

// TestConfigurator_WatchdogFreezePublishesNeutralState is a lifecycle
// test (pre-freeze → freeze → unfreeze) — kept as a single function
// rather than table-driven.
func TestConfigurator_WatchdogFreezePublishesNeutralState(t *testing.T) {
	store := NewSignalStore()
	clk := &fakeClock{t: time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)}
	c := (&Configurator{store: store, fanIn: make(chan Signal, 1)}).WithWatchdog(newWatchdogWithClock(clk.Now))

	// Pump imbalance so c.state.Imbalance is non-zero.
	c.handleSignal(Signal{Name: "imbalance", Value: 0.5, Timestamp: clk.Now()})
	require.Equal(t, 0.5, store.Load().Imbalance)

	// Rapid burst on/off flips to trip the watchdog.
	for range watchdogMaxTransitions + 1 {
		clk.Advance(time.Second)
		c.handleSignal(Signal{Name: "burst", Value: 0.7, Timestamp: clk.Now()})
		clk.Advance(time.Second)
		c.handleSignal(Signal{Name: "burst", Value: 0.2, Timestamp: clk.Now()})
	}

	// After freeze: published state is neutral, internal state still tracks real values.
	clk.Advance(time.Second)
	c.handleSignal(Signal{Name: "imbalance", Value: 0.9, Timestamp: clk.Now()})
	out := store.Load()
	require.Zero(t, out.Imbalance, "neutral imbalance during freeze")
	require.Zero(t, out.Burst, "neutral burst during freeze")
	require.False(t, out.BurstActive, "neutral BurstActive during freeze")
	require.NotZero(t, c.state.Imbalance, "internal c.state must keep tracking real values during freeze")

	// After freeze expires, real values resume.
	clk.Advance(watchdogFreezeDuration + time.Second)
	c.handleSignal(Signal{Name: "imbalance", Value: 0.3, Timestamp: clk.Now()})
	require.NotZero(t, store.Load().Imbalance, "real values resume after unfreeze")
}

func TestConfigurator_InitialPublishOnRun(t *testing.T) {
	// Run() with no detectors must still publish an initial state so
	// a fresh EPP has a non-zero UpdatedAt that monitors can use to
	// confirm the configurator is alive.
	store := NewSignalStore()
	c := NewConfigurator(store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return !store.Load().UpdatedAt.IsZero()
	}, 200*time.Millisecond, 5*time.Millisecond, "expected initial publish with non-zero UpdatedAt")

	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("configurator did not exit after context cancel")
	}
}
