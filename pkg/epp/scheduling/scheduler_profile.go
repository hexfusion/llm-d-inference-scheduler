/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommmon "github.com/llm-d/llm-d-inference-scheduler/pkg/common/error"
	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/scheduling/adaptive"
)

// NewSchedulerProfile creates a new SchedulerProfile object and returns its pointer.
func NewSchedulerProfile() *SchedulerProfile {
	return &SchedulerProfile{
		filters: []fwksched.Filter{},
		scorers: []*WeightedScorer{},
		// picker remains nil since profile doesn't support multiple pickers
	}
}

// SchedulerProfile provides a profile configuration for the scheduler which influence routing decisions.
type SchedulerProfile struct {
	filters []fwksched.Filter
	scorers []*WeightedScorer
	picker  fwksched.Picker

	// adaptiveSignals is set when the AdaptiveRouting feature gate is
	// enabled. When nil, the scoring loop reads the declared scorer
	// weight unchanged, matching pre-adaptive behavior. When set, the
	// loop reads the current SignalState and modulates each weight
	// via adaptive.EffectiveWeight.
	adaptiveSignals *adaptive.SignalStore

	// burstPicker is an optional picker swapped in when the current
	// SignalState's BurstActive flag is true. When nil (the default),
	// the profile uses the declared picker for every request; the
	// adaptive subsystem then only modulates scorer weights, not
	// picker selection. Operators opt in by attaching a burst picker
	// via WithBurstPicker.
	burstPicker fwksched.Picker
}

// WithFilters sets the given filter plugins as the Filter plugins.
// if the SchedulerProfile has Filter plugins, this call replaces the existing plugins with the given ones.
func (p *SchedulerProfile) WithFilters(filters ...fwksched.Filter) *SchedulerProfile {
	p.filters = filters
	return p
}

// WithScorers sets the given scorer plugins as the Scorer plugins.
// if the SchedulerProfile has Scorer plugins, this call replaces the existing plugins with the given ones.
func (p *SchedulerProfile) WithScorers(scorers ...*WeightedScorer) *SchedulerProfile {
	p.scorers = scorers
	return p
}

// WithPicker sets the given picker plugins as the Picker plugin.
// if the SchedulerProfile has Picker plugin, this call replaces the existing plugin with the given one.
func (p *SchedulerProfile) WithPicker(picker fwksched.Picker) *SchedulerProfile {
	p.picker = picker
	return p
}

// WithAdaptiveSignals attaches an adaptive.SignalStore to this profile.
// When set, the scoring loop reads the current SignalState on every
// request and modulates each scorer's declared weight via
// adaptive.EffectiveWeight.
func (p *SchedulerProfile) WithAdaptiveSignals(store *adaptive.SignalStore) *SchedulerProfile {
	p.adaptiveSignals = store
	return p
}

// WithBurstPicker attaches an optional burst-time picker. When the
// adaptive SignalState's BurstActive is true, the profile picks an
// endpoint via the burst picker instead of the declared default;
// otherwise it uses the default. Has no effect unless
// WithAdaptiveSignals is also set.
func (p *SchedulerProfile) WithBurstPicker(picker fwksched.Picker) *SchedulerProfile {
	p.burstPicker = picker
	return p
}

// AddPlugins adds the given plugins to all scheduler plugins according to the interfaces each plugin implements.
// A plugin may implement more than one scheduler plugin interface.
// Special Case: In order to add a scorer, one must use the scorer.NewWeightedScorer function in order to provide a weight.
// if a scorer implements more than one interface, supplying a WeightedScorer is sufficient. The function will take the internal
// scorer object and register it to all interfaces it implements.
func (p *SchedulerProfile) AddPlugins(pluginObjects ...plugin.Plugin) error {
	for _, plugin := range pluginObjects {
		if weightedScorer, ok := plugin.(*WeightedScorer); ok {
			p.scorers = append(p.scorers, weightedScorer)
			plugin = weightedScorer.Scorer // if we got WeightedScorer, unwrap the plugin
		} else if scorer, ok := plugin.(fwksched.Scorer); ok { // if we got a Scorer instead of WeightedScorer that's an error.
			return fmt.Errorf("failed to register scorer '%s' without a weight. follow function documentation to register a scorer", scorer.TypedName())
		}
		if filter, ok := plugin.(fwksched.Filter); ok {
			p.filters = append(p.filters, filter)
		}
		if picker, ok := plugin.(fwksched.Picker); ok {
			if p.picker != nil {
				return fmt.Errorf("failed to set '%s' as picker, already have a registered picker plugin '%s'", picker.TypedName(), p.picker.TypedName())
			}
			p.picker = picker
		}
	}
	return nil
}

func (p *SchedulerProfile) String() string {
	filterNames := make([]string, len(p.filters))
	for i, filter := range p.filters {
		filterNames[i] = filter.TypedName().String()
	}
	scorerNames := make([]string, len(p.scorers))
	for i, scorer := range p.scorers {
		scorerNames[i] = fmt.Sprintf("%s: %f", scorer.TypedName(), scorer.Weight())
	}

	return fmt.Sprintf(
		"{Filters: [%s], Scorers: [%s], Picker: %s}",
		strings.Join(filterNames, ", "),
		strings.Join(scorerNames, ", "),
		p.picker.TypedName(),
	)
}

// Run runs a SchedulerProfile. It invokes all the SchedulerProfile plugins for the given request in this
// order - Filters, Scorers, Picker. After completing all, it returns the result.
func (p *SchedulerProfile) Run(ctx context.Context, request *fwksched.InferenceRequest, cycleState *fwksched.CycleState, candidateEndpoints []fwksched.Endpoint) (*fwksched.ProfileRunResult, error) {
	endpoints := p.runFilterPlugins(ctx, request, cycleState, candidateEndpoints)
	if len(endpoints) == 0 {
		return nil, errcommmon.Error{Code: errcommmon.Internal, Msg: "no endpoints available for the given request"}
	}

	// Snapshot the adaptive SignalState ONCE per request so scoring
	// and picker selection see consistent signals.
	var sigs adaptive.SignalState
	if p.adaptiveSignals != nil {
		sigs = p.adaptiveSignals.Load()
	}

	weightedScorePerEndpoint := p.runScorerPlugins(ctx, request, cycleState, endpoints, sigs)
	result := p.runPickerPlugin(ctx, cycleState, weightedScorePerEndpoint, sigs)

	return result, nil
}

func (p *SchedulerProfile) runFilterPlugins(ctx context.Context, request *fwksched.InferenceRequest, cycleState *fwksched.CycleState, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	logger := log.FromContext(ctx)
	filteredEndpoints := endpoints
	logger.V(logutil.DEBUG).Info("Before running filter plugins", "endpoints", filteredEndpoints)

	for _, filter := range p.filters {
		logger.V(logutil.VERBOSE).Info("Running filter plugin", "plugin", filter.TypedName())
		before := time.Now()
		filteredEndpoints = filter.Filter(ctx, cycleState, request, filteredEndpoints)
		metrics.RecordPluginProcessingLatency(filterExtensionPoint, filter.TypedName().Type, filter.TypedName().Name, time.Since(before))
		logger.V(logutil.DEBUG).Info("Completed running filter plugin successfully", "plugin", filter.TypedName(), "endpoints", filteredEndpoints)
		if len(filteredEndpoints) == 0 {
			logger.V(logutil.VERBOSE).Info("Filter eliminated all endpoints", "plugin", filter.TypedName(), "endpointsBefore", len(endpoints))
			break
		}
	}
	logger.V(logutil.VERBOSE).Info("Completed running filter plugins", "remainingEndpoints", len(filteredEndpoints))

	return filteredEndpoints
}

func (p *SchedulerProfile) runScorerPlugins(ctx context.Context, request *fwksched.InferenceRequest, cycleState *fwksched.CycleState, endpoints []fwksched.Endpoint, sigs adaptive.SignalState) map[fwksched.Endpoint]float64 {
	logger := log.FromContext(ctx)
	logger.V(logutil.DEBUG).Info("Before running scorer plugins", "endpoints", endpoints)

	weightedScorePerEndpoint := make(map[fwksched.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		weightedScorePerEndpoint[endpoint] = float64(0) // initialize weighted score per endpoint with 0 value
	}
	// When adaptive routing is disabled (p.adaptiveSignals == nil)
	// every scorer's weight is its declared weight; when enabled the
	// SignalState modulates per adaptive.EffectiveWeight. The
	// SignalState was loaded once in Run() and passed in.
	adaptiveOn := p.adaptiveSignals != nil

	// Iterate through each scorer in the chain and accumulate the weighted scores.
	for _, scorer := range p.scorers {
		logger.V(logutil.VERBOSE).Info("Running scorer plugin", "plugin", scorer.TypedName())
		before := time.Now()
		scores := scorer.Score(ctx, cycleState, request, endpoints)
		metrics.RecordPluginProcessingLatency(scorerExtensionPoint, scorer.TypedName().Type, scorer.TypedName().Name, time.Since(before))
		weight := scorer.Weight()
		if adaptiveOn {
			weight = adaptive.EffectiveWeight(scorer, sigs)
			adaptive.RecordWeight(scorer.TypedName().Name, string(scorer.Category()), weight)
		}
		for endpoint, score := range scores { // weight is relative to the sum of weights
			logger.V(logutil.DEBUG).Info("Calculated score", "plugin", scorer.TypedName(), "endpoint", endpoint.GetMetadata().NamespacedName, "score", score)
			weightedScorePerEndpoint[endpoint] += enforceScoreRange(score) * weight
		}
		logger.V(logutil.DEBUG).Info("Completed running scorer plugin successfully", "plugin", scorer.TypedName())
	}
	logger.V(logutil.VERBOSE).Info("Completed running scorer plugins successfully")

	return weightedScorePerEndpoint
}

func (p *SchedulerProfile) runPickerPlugin(ctx context.Context, cycleState *fwksched.CycleState, weightedScorePerEndpoint map[fwksched.Endpoint]float64, sigs adaptive.SignalState) *fwksched.ProfileRunResult {
	logger := log.FromContext(ctx)
	scoredEndpoints := make([]*fwksched.ScoredEndpoint, len(weightedScorePerEndpoint))
	i := 0
	for endpoint, score := range weightedScorePerEndpoint {
		scoredEndpoints[i] = &fwksched.ScoredEndpoint{Endpoint: endpoint, Score: score}
		i++
	}

	// Pick the active picker. When adaptive routing is disabled, or
	// no burst picker is configured, this is always the declared
	// picker. Otherwise PickerSelector returns the burst picker iff
	// the snapshot's BurstActive is true. RecordPickerActive is
	// emitted every request so the metric reflects current routing
	// state, not just transitions.
	picker := p.picker
	if p.adaptiveSignals != nil && p.burstPicker != nil {
		sel := adaptive.PickerSelector{Default: p.picker, Burst: p.burstPicker}
		picker = sel.SelectPicker(sigs)
		burstActive := picker.TypedName().Name == p.burstPicker.TypedName().Name
		adaptive.RecordPickerActive(p.burstPicker.TypedName().Name, burstActive)
		adaptive.RecordPickerActive(p.picker.TypedName().Name, !burstActive)
	}

	logger.V(logutil.VERBOSE).Info("Running picker plugin", "plugin", picker.TypedName())
	logger.V(logutil.DEBUG).Info("Candidate pods for picking", "endpoints-weighted-score", scoredEndpoints)
	before := time.Now()
	result := picker.Pick(ctx, cycleState, scoredEndpoints)
	metrics.RecordPluginProcessingLatency(pickerExtensionPoint, picker.TypedName().Type, picker.TypedName().Name, time.Since(before))
	logger.V(logutil.DEBUG).Info("Completed running picker plugin successfully", "plugin", picker.TypedName(), "result", result)

	return result
}

func enforceScoreRange(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}
