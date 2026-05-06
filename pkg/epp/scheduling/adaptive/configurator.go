package adaptive

import (
	"context"
	"time"
)

const (
	// Signals are state, not events; coalescing is fine.
	configuratorFanInBuffer = 4
	// Force BurstActive off after this idle gap to prevent stuck-active.
	configuratorBurstActiveTimeout = 60 * time.Second
)

// Publisher receives every state transition. Runs inline on the
// configurator goroutine — must not block.
type Publisher func(SignalState)

// Configurator is the single goroutine that owns the SignalStore.
type Configurator struct {
	store       *SignalStore
	detectors   []Detector
	fanIn       chan Signal
	state       SignalState
	lastBurstAt time.Time
	watchdog    *Watchdog
	publishers  []Publisher
}

// NewConfigurator wires the supplied detectors to the given store.
func NewConfigurator(store *SignalStore, detectors ...Detector) *Configurator {
	return &Configurator{
		store:     store,
		detectors: detectors,
		fanIn:     make(chan Signal, configuratorFanInBuffer),
		watchdog:  NewWatchdog(),
	}
}

// WithWatchdog overrides the default watchdog. Returns the
// configurator so callers can chain.
func (c *Configurator) WithWatchdog(w *Watchdog) *Configurator {
	c.watchdog = w
	return c
}

// AddPublisher registers p; called on every state transition (and once at start).
func (c *Configurator) AddPublisher(p Publisher) *Configurator {
	c.publishers = append(c.publishers, p)
	return c
}

func (c *Configurator) Run(ctx context.Context) error {
	for _, det := range c.detectors {
		d := det
		go func() {
			_ = d.Run(ctx, c.fanIn)
		}()
	}

	c.state.UpdatedAt = time.Now()
	c.store.Put(c.state)
	for _, p := range c.publishers {
		p(c.state)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case sig := <-c.fanIn:
			c.handleSignal(sig)
		}
	}
}

// handleSignal merges one signal into the configurator state and
// publishes the result.
func (c *Configurator) handleSignal(sig Signal) {
	prevBurstActive := c.state.BurstActive
	switch sig.Name {
	case "imbalance":
		c.state.Imbalance = sig.Value
	case "burst":
		c.state.Burst = sig.Value
		c.lastBurstAt = sig.Timestamp
		if sig.Value >= burstActivateThreshold {
			c.state.BurstActive = true
		} else if sig.Value <= burstDeactivateThreshold {
			c.state.BurstActive = false
		}
	default:
		return
	}

	RecordSignal(sig.Name, sig.Value)
	burstTransitioned := sig.Name == "burst" && prevBurstActive != c.state.BurstActive
	if burstTransitioned {
		from, to := "Idle", "Adapting"
		if !c.state.BurstActive {
			from, to = "Adapting", "Idle"
		}
		RecordTransition("burst", from, to, "hysteresis")
		// Emit flap metric only on the trip itself, not subsequent transitions.
		if c.watchdog != nil && c.watchdog.RecordTransition() {
			RecordFlapDetected()
		}
	}

	// Stale-burst safety: silent detector must not leave us stuck active.
	if c.state.BurstActive && !c.lastBurstAt.IsZero() &&
		time.Since(c.lastBurstAt) > configuratorBurstActiveTimeout {
		c.state.BurstActive = false
	}

	// Watchdog freeze forces neutral state — adaptive disabled until expiry.
	state := c.state
	if c.watchdog != nil && c.watchdog.IsFrozen() {
		state.Imbalance = 0
		state.Burst = 0
		state.BurstActive = false
	}
	state.UpdatedAt = sig.Timestamp
	c.store.Put(state)
	for _, p := range c.publishers {
		p(state)
	}
}
