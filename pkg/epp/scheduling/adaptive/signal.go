package adaptive

import (
	"context"
	"sync/atomic"
	"time"
)

const FeatureGate = "AdaptiveRouting"

// Signal is one detector reading bound for the configurator fan-in.
type Signal struct {
	Name      string
	Value     float64
	Timestamp time.Time
}

// SignalState is the merged view of all detector signals the read path
// observes. Published atomically by the configurator via SignalStore.
type SignalState struct {
	Imbalance   float64
	Burst       float64
	BurstActive bool
	UpdatedAt   time.Time
}

// SignalStore wraps an atomic.Pointer[SignalState]. Lock-free read path
// for any consumer that wants raw signal state (metrics, debug surfaces).
type SignalStore struct {
	ptr atomic.Pointer[SignalState]
}

// NewSignalStore returns a store seeded with the zero SignalState.
func NewSignalStore() *SignalStore {
	s := &SignalStore{}
	zero := SignalState{}
	s.ptr.Store(&zero)
	return s
}

// Get returns a snapshot of the current state.
func (s *SignalStore) Get() SignalState {
	if p := s.ptr.Load(); p != nil {
		return *p
	}
	return SignalState{}
}

// Put replaces the current SignalState. Configurator-only in normal use.
func (s *SignalStore) Put(state SignalState) {
	s.ptr.Store(&state)
}

// Detector observes pool state and emits a smoothed Signal in [0, 1] per cycle.
type Detector interface {
	// Name identifies the detector in metrics and logs.
	Name() string

	// Run blocks until ctx is done.
	Run(ctx context.Context, out chan<- Signal) error
}
