// Package common_metrics: generic background-worker Prometheus metrics registration and methods.
package common_metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// workerStageDurationBuckets defines the histogram buckets shared by the worker
// stage-duration and queue-wait histograms. They span sub-second polling work up
// to multi-minute long-running stages.
var workerStageDurationBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

// registerWorkerMetrics builds and registers the generic background-worker collectors.
// These apply to every worker built on pkg/worker, keyed by a low-cardinality "worker"
// label so consuming apps get the same dashboards and alerts for free.
//
// Parameters:
//   - namespace (string): The metric namespace prefix applied to collector names.
func (h *CommonMetricsHandler) registerWorkerMetrics(namespace string) {
	factory := promauto.With(h.registry)

	h.workerStageDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "worker_stage_duration_seconds",
		Help:      "Duration of background worker stage executions in seconds.",
		Buckets:   workerStageDurationBuckets,
	}, []string{"worker", "stage", "outcome"})

	h.workerStageJobsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_stage_jobs_total",
		Help:      "Total number of background worker stage executions by outcome.",
	}, []string{"worker", "stage", "outcome"})

	h.workerStageQueueWait = factory.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "worker_stage_queue_wait_seconds",
		Help:      "Time background worker jobs spend queued before a stage claim.",
		Buckets:   workerStageDurationBuckets,
	}, []string{"worker", "stage"})

	h.workerStageInFlight = factory.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_stage_in_flight",
		Help:      "Current number of background worker jobs actively executing per stage.",
	}, []string{"worker", "stage"})

	h.workerStageQueueDepth = factory.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_stage_queue_depth",
		Help:      "Current queued background worker jobs per stage.",
	}, []string{"worker", "stage"})

	h.workerBacklog = factory.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_backlog",
		Help:      "Current backlog count for worker-specific reconciliation classes.",
	}, []string{"worker", "kind"})

	h.workerStaleRecovered = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_stale_recovered_total",
		Help:      "Total number of stale background worker jobs recovered and re-queued.",
	}, []string{"worker", "stage"})

	h.workerHeartbeatFailures = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_heartbeat_failures_total",
		Help:      "Total number of background worker heartbeat update failures.",
	}, []string{"worker"})

	h.workerPollTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_poll_total",
		Help:      "Total number of background worker queue polls by result.",
	}, []string{"worker", "result"})
}

// ObserveWorkerStageDuration records execution time and increments outcome totals for a worker stage.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - stage (string): The worker stage label.
//   - outcome (string): The outcome label for the completed stage execution.
//   - d (time.Duration): The elapsed execution duration to observe.
func (h *CommonMetricsHandler) ObserveWorkerStageDuration(worker, stage, outcome string, d time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerStageDuration.WithLabelValues(worker, stage, outcome).Observe(d.Seconds())
	h.workerStageJobsTotal.WithLabelValues(worker, stage, outcome).Inc()
}

// ObserveWorkerStageQueueWait records queue-wait time before a worker stage claim.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - stage (string): The worker stage label.
//   - d (time.Duration): The elapsed queue-wait duration to observe.
func (h *CommonMetricsHandler) ObserveWorkerStageQueueWait(worker, stage string, d time.Duration) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerStageQueueWait.WithLabelValues(worker, stage).Observe(d.Seconds())
}

// AddWorkerStageInFlight adjusts the in-flight gauge for a worker stage.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - stage (string): The worker stage label.
//   - delta (float64): The signed adjustment applied to the in-flight gauge.
func (h *CommonMetricsHandler) AddWorkerStageInFlight(worker, stage string, delta float64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerStageInFlight.WithLabelValues(worker, stage).Add(delta)
}

// SetWorkerStageQueueDepth sets the queued-job gauge for a worker stage.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - stage (string): The worker stage label.
//   - depth (int64): The absolute queue depth value to publish.
func (h *CommonMetricsHandler) SetWorkerStageQueueDepth(worker, stage string, depth int64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerStageQueueDepth.WithLabelValues(worker, stage).Set(float64(depth))
}

// SetWorkerBacklog sets a worker-specific reconciliation backlog gauge.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - kind (string): The low-cardinality backlog kind label.
//   - count (int64): The absolute backlog count to publish.
func (h *CommonMetricsHandler) SetWorkerBacklog(worker, kind string, count int64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerBacklog.WithLabelValues(worker, kind).Set(float64(count))
}

// IncWorkerStaleRecovered increments the stale-recovery counter for a worker stage by n.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - stage (string): The worker stage label.
//   - n (int): The number of recovered jobs to add; non-positive values are ignored.
func (h *CommonMetricsHandler) IncWorkerStaleRecovered(worker, stage string, n int) {
	if h == nil || !h.IsEnabled() || n <= 0 {
		return
	}
	h.workerStaleRecovered.WithLabelValues(worker, stage).Add(float64(n))
}

// IncWorkerHeartbeatFailure increments the heartbeat-failure counter for a worker.
//
// Parameters:
//   - worker (string): The logical worker label.
func (h *CommonMetricsHandler) IncWorkerHeartbeatFailure(worker string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerHeartbeatFailures.WithLabelValues(worker).Inc()
}

// IncWorkerPoll increments the poll counter for a worker by result label.
//
// Parameters:
//   - worker (string): The logical worker label.
//   - result (string): The poll-outcome label (e.g. claimed/idle/error).
func (h *CommonMetricsHandler) IncWorkerPoll(worker, result string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.workerPollTotal.WithLabelValues(worker, result).Inc()
}
