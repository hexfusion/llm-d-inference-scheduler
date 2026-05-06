package adaptive

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignalStore_RoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		state SignalState
	}{
		{"fresh store returns zero state", SignalState{}},
		{
			name: "round-trip preserves all fields",
			state: SignalState{
				Imbalance:   0.42,
				Burst:       0.7,
				BurstActive: true,
				UpdatedAt:   time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSignalStore()
			if tc.state != (SignalState{}) {
				s.Put(tc.state)
			}
			require.Equal(t, tc.state, s.Get())
		})
	}
}

func TestSignalStore_ConcurrentReadersAndOneWriter(t *testing.T) {
	const readers = 16
	const iterations = 200

	s := NewSignalStore()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			s.Put(SignalState{Imbalance: float64(i) / float64(iterations)})
			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				got := s.Get()
				require.GreaterOrEqual(t, got.Imbalance, 0.0)
				require.LessOrEqual(t, got.Imbalance, 1.0)
			}
		}()
	}

	wg.Wait()
	close(stop)
}
