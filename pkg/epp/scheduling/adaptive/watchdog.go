package adaptive

import (
	"sync"
	"time"
)

const (
	// watchdogWindow is the rolling window over which transitions are
	// counted. Sized to be larger than the burst-detector cycle (5s)
	// so a healthy burst on/off pattern with multi-cycle dwell does
	// not trip the watchdog.
	watchdogWindow = 60 * time.Second

	// watchdogMaxTransitions is the count above which the watchdog
	// trips into freeze. Six transitions in a 60s window = roughly
	// once every 10s, well above the dwell time hysteresis enforces.
	watchdogMaxTransitions = 5

	// watchdogFreezeDuration is how long the watchdog forces neutral
	// state after tripping. Long enough that the underlying cause
	// (load spike, sampler bug, misconfigured detector) has time to
	// either resolve or be observed by an operator via the
	// adaptive_routing_flap_detected_total metric.
	watchdogFreezeDuration = 60 * time.Second
)

// watchdogRingSize is one slot beyond the trip threshold. 
const watchdogRingSize = watchdogMaxTransitions + 1

// Watchdog tracks adaptive transitions and freezes the configurator
// when oscillation is detected.
type Watchdog struct {
	mu sync.Mutex
	ring        [watchdogRingSize]time.Time
	head        int  
	count       int 
	frozenUntil time.Time
	now         func() time.Time
}

// NewWatchdog returns a Watchdog using time.Now as its clock.
func NewWatchdog() *Watchdog {
	return &Watchdog{now: time.Now}
}

// newWatchdogWithClock is a test seam: lets tests inject a
// deterministic clock without exposing the field publicly.
func newWatchdogWithClock(now func() time.Time) *Watchdog {
	return &Watchdog{now: now}
}

// RecordTransition records a transition into the ring buffer and
// trips the watchdog if the oldest of the last watchdogRingSize
// transitions is still within watchdogWindow of now. Returns true
// only if THIS call caused a freeze.
func (w *Watchdog) RecordTransition() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()

	w.ring[w.head] = now
	w.head = (w.head + 1) % watchdogRingSize
	if w.count < watchdogRingSize {
		w.count++
	}

	if w.count < watchdogRingSize {
		return false
	}

	oldest := w.ring[w.head]
	cutoff := now.Add(-watchdogWindow)
	if oldest.After(cutoff) && !now.Before(w.frozenUntil) {
		w.frozenUntil = now.Add(watchdogFreezeDuration)
		return true
	}
	return false
}

// IsFrozen reports whether the watchdog is currently in freeze.
func (w *Watchdog) IsFrozen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now().Before(w.frozenUntil)
}
