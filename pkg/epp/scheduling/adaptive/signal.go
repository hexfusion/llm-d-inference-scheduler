package adaptive

import (
	"context"
	"sync/atomic"
	"time"
)

const FeatureGate = "AdaptiveRouting"

// Signal is one reading from a detector destined for the configurator
// fan-in channel.
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

// SignalStore wraps an atomic.Pointer[SignalState] so the configurator
// can publish a fresh state and the request hot path can read a
// consistent snapshot without locks.
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

// Load returns a snapshot of the current state.
func (s *SignalStore) Load() SignalState {
	if p := s.ptr.Load(); p != nil {
		return *p
	}
	return SignalState{}
}

// Store replaces the current SignalState. The configurator is the only
// goroutine that calls this in normal operation.
func (s *SignalStore) Store(state SignalState) {
	s.ptr.Store(&state)
}

// Detector is a goroutine that observes pool state and emits a smoothed
// Signal in [0, 1] on each cycle. 
type Detector interface {
	// Name identifies the detector in metrics and logs.
	Name() string

	// Run blocks until ctx is done. 
	Run(ctx context.Context, out chan<- Signal) error
}
