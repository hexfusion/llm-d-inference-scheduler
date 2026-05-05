package adaptive

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const Subsystem = "adaptive_routing"

var (
	registerOnce sync.Once

	signalGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: Subsystem,
			Name:      "signal",
			Help:      "Current smoothed value of each adaptive detector signal in [0, 1].",
		},
		[]string{"name"},
	)

	weightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: Subsystem,
			Name:      "weight",
			Help:      "Current effective weight per scorer after adaptive modulation.",
		},
		[]string{"scorer", "category"},
	)

	pickerActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: Subsystem,
			Name:      "picker_active",
			Help:      "1 if the named picker is currently active, 0 otherwise.",
		},
		[]string{"picker"},
	)

	transitionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: Subsystem,
			Name:      "transition_total",
			Help:      "Counter of state-machine transitions, attributed by detector and reason.",
		},
		[]string{"detector", "from", "to", "reason"},
	)

	flapDetectedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: Subsystem,
			Name:      "flap_detected_total",
			Help:      "Counter incremented each time the watchdog freezes the configurator.",
		},
	)
)

// RegisterMetrics registers the package metrics with the supplied
// Prometheus registerer. 
func RegisterMetrics(reg prometheus.Registerer) {
	registerOnce.Do(func() {
		reg.MustRegister(
			signalGauge,
			weightGauge,
			pickerActive,
			transitionTotal,
			flapDetectedTotal,
		)
	})
}

// RecordSignal sets the current value of a named detector signal.
func RecordSignal(name string, value float64) {
	signalGauge.WithLabelValues(name).Set(value)
}

// RecordWeight sets the current effective weight for a scorer.
func RecordWeight(scorer, category string, weight float64) {
	weightGauge.WithLabelValues(scorer, category).Set(weight)
}

// RecordPickerActive marks the named picker as active (1) or inactive (0).
func RecordPickerActive(picker string, active bool) {
	v := 0.0
	if active {
		v = 1.0
	}
	pickerActive.WithLabelValues(picker).Set(v)
}

// RecordTransition increments the state-transition counter.
func RecordTransition(detector, from, to, reason string) {
	transitionTotal.WithLabelValues(detector, from, to, reason).Inc()
}

// RecordFlapDetected increments the watchdog-tripped counter.
func RecordFlapDetected() {
	flapDetectedTotal.Inc()
}
