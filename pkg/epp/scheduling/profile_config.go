package scheduling

import (
	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// ProfileConfig is the resolved per-profile runtime configuration the
// scheduler reads at request time. Control-plane writers publish via
// SchedulerProfile.UpdateConfig; the data plane snapshots once per Run.
type ProfileConfig struct {
	// ScorerWeights overrides declared scorer weights by name. Missing
	// entries fall through to the scorer's declared Weight().
	ScorerWeights map[string]float64

	// Picker overrides the declared picker. nil means "use declared."
	Picker fwksched.Picker
}
