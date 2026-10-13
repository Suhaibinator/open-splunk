package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerAgentInputMetrics registers the agent-session input metrics: how much human input waits
// for a busy or suspended session, how long it waits before a run admits it, how often a run's
// ending drains it into a follow-up run, and how long a Stop or cancel takes to end the local
// drive. The only label is the bounded admission path.
func (h *CommonMetricsHandler) registerAgentInputMetrics(namespace string) {
	f := promauto.With(h.registry)
	h.chatAgentInputPendingDepth = f.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "chat_agent_input_pending_depth",
		Help:      "Messages waiting for an agent session, including the one enqueued, observed at each committed enqueue",
		Buckets:   []float64{1, 2, 3, 5, 10, 20, 50, 100},
	})
	h.chatAgentInputAdmissions = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_input_admissions_total",
		Help:      "Batches of waiting agent input admitted, by path: step (into an active run's next step) or drain (into one new follow-up run, so drain counts drained runs); a drain is counted when its transaction writes it",
	}, []string{"path"})
	h.chatAgentInputAdmissionWait = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "chat_agent_input_admission_wait_seconds",
		Help:      "Time each admitted message waited between being enqueued and being admitted into a run, by path",
		Buckets:   []float64{.1, .5, 1, 5, 15, 30, 60, 300, 900, 3600, 86400},
	}, []string{"path"})
	h.chatAgentRunCancelPropagation = f.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "chat_agent_run_cancel_propagation_seconds",
		Help:      "Time from a committed Stop or cancel to the run's local drive exiting",
		Buckets:   []float64{.005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})
}

// RecordChatAgentInputEnqueued records the waiting-input depth of a session a message was just
// enqueued for, counting that message.
func (h *CommonMetricsHandler) RecordChatAgentInputEnqueued(depth uint32) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentInputPendingDepth.Observe(float64(depth))
}

// RecordChatAgentInputStepAdmission records one batch of waiting input admitted into an active
// run's next step; waits holds how long each admitted message waited. An empty batch records
// nothing.
func (h *CommonMetricsHandler) RecordChatAgentInputStepAdmission(waits []time.Duration) {
	h.recordChatAgentInputAdmission("step", waits)
}

// RecordChatAgentInputDrain records one drain of waiting input into a new follow-up run; waits
// holds how long each drained message waited. An empty batch records nothing.
func (h *CommonMetricsHandler) RecordChatAgentInputDrain(waits []time.Duration) {
	h.recordChatAgentInputAdmission("drain", waits)
}

func (h *CommonMetricsHandler) recordChatAgentInputAdmission(path string, waits []time.Duration) {
	if h == nil || !h.IsEnabled() || len(waits) == 0 {
		return
	}
	h.chatAgentInputAdmissions.WithLabelValues(path).Inc()
	wait := h.chatAgentInputAdmissionWait.WithLabelValues(path)
	for _, d := range waits {
		wait.Observe(max(0, d.Seconds()))
	}
}

// RecordChatAgentRunCancelPropagation records how long a run's local drive took to exit after its
// cancellation was requested.
func (h *CommonMetricsHandler) RecordChatAgentRunCancelPropagation(latency time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentRunCancelPropagation.Observe(max(0, latency.Seconds()))
}
