package adaptive

import (
	"context"
	"time"
)

const (
	// configuratorFanInBuffer: depth of the fan-in channel detectors
	// push to. Signals are state not events; coalescing is fine.
	configuratorFanInBuffer = 4

	// configuratorBurstActiveTimeout: how long the configurator keeps
	// BurstActive set without a fresh burst signal. Prevents stuck-
	// active when the burst detector goes silent.
	configuratorBurstActiveTimeout = 60 * time.Second
)

// Configurator is the single goroutine that owns the SignalStore. 
type Configurator struct {
	store       *SignalStore
	detectors   []Detector
	fanIn       chan Signal
	state       SignalState
	lastBurstAt time.Time
	watchdog    *Watchdog
}

// NewConfigurator wires the supplied detectors to a fresh store. 
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

func (c *Configurator) Run(ctx context.Context) error {
	for _, det := range c.detectors {
		d := det
		go func() {
			_ = d.Run(ctx, c.fanIn)
		}()
	}

	c.state.UpdatedAt = time.Now()
	c.store.Store(c.state)

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
		// Feed the watchdog: a burst on/off flip is the transition
		// it counts. If THIS transition trips the freeze, emit the
		// flap-detected metric (only on the trip itself, not on
		// further transitions while frozen).
		if c.watchdog != nil && c.watchdog.RecordTransition() {
			RecordFlapDetected()
		}
	}

	// Stale-burst safety: if BurstActive was set but no burst signal
	// has arrived in configuratorBurstActiveTimeout, force it off so
	// a silent burst detector cannot leave the system stuck in burst
	// mode forever.
	if c.state.BurstActive && !c.lastBurstAt.IsZero() &&
		time.Since(c.lastBurstAt) > configuratorBurstActiveTimeout {
		c.state.BurstActive = false
	}

	// Publish a copy of state. If the watchdog is frozen, force a
	// neutral state so EffectiveWeight returns declared weights and
	// the picker selector returns the default picker — adaptive
	// routing is effectively disabled until the freeze expires. 
	publish := c.state
	if c.watchdog != nil && c.watchdog.IsFrozen() {
		publish.Imbalance = 0
		publish.Burst = 0
		publish.BurstActive = false
	}
	publish.UpdatedAt = sig.Timestamp
	c.store.Store(publish)
}
