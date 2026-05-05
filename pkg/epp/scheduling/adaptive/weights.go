package adaptive

import (
	"math"

	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

const (
	// sigmoidMidpoint: imbalance signal value at which the sigmoid
	// output crosses 0.5. Geometric center of the [0,1] input range.
	sigmoidMidpoint = 0.5

	// sigmoidSteepness: controls how sharply the curve rises around
	// the midpoint. 10.0 produces a transition band of ~0.4 — sharp
	// enough that sub-0.3 noise doesn't perturb weights, gentle
	// enough that 0.5–0.7 signals don't snap to a step function.
	sigmoidSteepness = 10.0

	// minWeight clamps the lower bound so an affinity scorer at high
	// imbalance never falls all the way to zero. Retaining some
	// affinity prevents pure distribution-mode oscillation.
	minWeight = 0.1

	// maxWeight clamps the upper bound. Asymmetric vs minWeight:
	// scorers can attenuate harder than they amplify, because
	// reducing-toward-zero is more dangerous than amplifying.
	maxWeight = 1.5
)

// EffectiveWeight returns the runtime-modulated weight for a scorer
// given the current SignalState. 
func EffectiveWeight(scorer interface {
	Weight() float64
	Category() fwksched.ScorerCategory
}, sigs SignalState) float64 {
	declared := scorer.Weight()
	if sigs.Imbalance == 0 && !sigs.BurstActive {
		return declared
	}
	pressure := sigmoid(sigs.Imbalance, sigmoidMidpoint, sigmoidSteepness)
	switch scorer.Category() {
	case fwksched.Affinity:
		return clamp(declared*(1.0-pressure*0.5), minWeight*declared, maxWeight*declared)
	case fwksched.Distribution:
		return clamp(declared*(1.0+pressure*0.5), minWeight*declared, maxWeight*declared)
	default:
		return declared
	}
}

// sigmoid returns 1 / (1 + exp(-k*(x - x0))).
func sigmoid(x, x0, k float64) float64 {
	return 1.0 / (1.0 + math.Exp(-k*(x-x0)))
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// PickerSelector chooses between a default and a burst-time picker
// based on the current SignalState. 
type PickerSelector struct {
	Default fwksched.Picker
	Burst   fwksched.Picker
}

// SelectPicker returns the active picker for the current signals.
func (s PickerSelector) SelectPicker(sigs SignalState) fwksched.Picker {
	if sigs.BurstActive && s.Burst != nil {
		return s.Burst
	}
	return s.Default
}
