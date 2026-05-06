package adaptive

import (
	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/scheduling"
)

// ProfileBinding translates SignalState into a *scheduling.ProfileConfig
// for one profile. Register OnSignal as a Configurator publisher.
type ProfileBinding struct {
	UpdateConfig func(*scheduling.ProfileConfig)
	Scorers      []*scheduling.WeightedScorer
	Default      fwksched.Picker
	Burst        fwksched.Picker
}

// OnSignal computes resolved weights + picker and pushes to the profile.
func (b *ProfileBinding) OnSignal(sigs SignalState) {
	weights := make(map[string]float64, len(b.Scorers))
	for _, s := range b.Scorers {
		w := EffectiveWeight(s, sigs)
		name := s.TypedName().Name
		weights[name] = w
		RecordWeight(name, string(s.Category()), w)
	}

	var picker fwksched.Picker
	if b.Burst != nil && b.Default != nil {
		sel := PickerSelector{Default: b.Default, Burst: b.Burst}
		chosen := sel.SelectPicker(sigs)
		burstActive := chosen.TypedName().Name == b.Burst.TypedName().Name
		RecordPickerActive(b.Burst.TypedName().Name, burstActive)
		RecordPickerActive(b.Default.TypedName().Name, !burstActive)
		if burstActive {
			picker = b.Burst
		}
	}

	b.UpdateConfig(&scheduling.ProfileConfig{
		ScorerWeights: weights,
		Picker:        picker,
	})
}
