// Package common_metrics: AI control-plane Prometheus metrics registration and record methods.
// These satisfy the controlplane.Metrics interface structurally (no import of the control plane), so
// the application can install the CommonMetricsHandler as the control plane's bounded metrics sink.
// Every label is a bounded, low-cardinality value; trace IDs, user IDs, and error strings are never
// used as labels.
package common_metrics

import (
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// registerInferenceControlPlaneMetrics registers the control-plane metric vectors.
func (h *CommonMetricsHandler) registerInferenceControlPlaneMetrics(namespace string) {
	factory := promauto.With(h.registry)
	h.inferencePolicyBlocksTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_policy_blocks_total",
			Help:      "Total inference policy blocks by bounded reason",
		},
		[]string{"reason"},
	)
	h.inferenceRequestTPS = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "inference_request_tokens_per_second",
			Help:      "Output tokens per full attempt second",
			Buckets:   []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000},
		},
		[]string{"provider", "operation", "status"},
	)
	h.inferenceGenerationTPS = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "inference_generation_tokens_per_second",
			Help:      "Approximate output tokens per second from first content chunk to completion",
			Buckets:   []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000},
		},
		[]string{"provider", "operation", "status"},
	)

	h.inferenceAttemptsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_attempts_total",
			Help:      "Total inference provider attempts by provider, registered operation, and terminal status",
		},
		[]string{"provider", "operation", "status"},
	)

	h.inferenceAttemptLatencySeconds = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "inference_attempt_latency_seconds",
			Help:      "Latency of inference provider attempts in seconds",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0},
		},
		[]string{"provider", "operation", "status"},
	)

	h.inferenceAttemptTokensTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_attempt_tokens_total",
			Help:      "Total reported tokens across inference attempts by provider, operation, and status",
		},
		[]string{"provider", "operation", "status"},
	)

	h.inferenceRetriesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_retries_total",
			Help:      "Total inference retry decisions by provider, registered operation, and outcome (retried, exhausted, not_retryable)",
		},
		[]string{"provider", "operation", "outcome"},
	)

	h.inferenceThrottleWaitsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_throttle_waits_total",
			Help:      "Total inference bucket wait-cap exhaustions by provider and throttle kind (request, token)",
		},
		[]string{"provider", "kind"},
	)

	h.inferenceBackoffSeconds = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "inference_backoff_seconds",
			Help:      "Scheduled jittered retry backoff in seconds by provider and registered operation",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0},
		},
		[]string{"provider", "operation"},
	)

	h.inferenceRouteResolutionFailuresTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_route_resolution_failures_total",
			Help:      "Total inference route-resolution failures",
		},
	)

	h.inferenceRecorderFailuresTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "inference_recorder_failures_total",
			Help:      "Total inference attempt-recorder failures by stage (start, finalize)",
		},
		[]string{"stage"},
	)
}

// RecordInferenceAttempt records one finished attempt with its latency and token total.
func (h *CommonMetricsHandler) RecordInferenceAttempt(provider, operation, status string, latencySeconds float64, totalTokens int64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceAttemptsTotal.WithLabelValues(provider, operation, status).Inc()
	h.inferenceAttemptLatencySeconds.WithLabelValues(provider, operation, status).Observe(latencySeconds)
	if totalTokens > 0 {
		h.inferenceAttemptTokensTotal.WithLabelValues(provider, operation, status).Add(float64(totalTokens))
	}
}

// RecordInferenceRetry records one retry disposition.
func (h *CommonMetricsHandler) RecordInferenceRetry(provider, operation, outcome string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceRetriesTotal.WithLabelValues(provider, operation, outcome).Inc()
}

// RecordInferenceThrottleWait records one bucket wait-cap exhaustion.
func (h *CommonMetricsHandler) RecordInferenceThrottleWait(provider, kind string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceThrottleWaitsTotal.WithLabelValues(provider, kind).Inc()
}

// RecordInferenceBackoff records one scheduled backoff.
func (h *CommonMetricsHandler) RecordInferenceBackoff(provider, operation string, backoffSeconds float64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceBackoffSeconds.WithLabelValues(provider, operation).Observe(backoffSeconds)
}

// RecordInferenceRouteResolutionFailure records one failed route resolution.
func (h *CommonMetricsHandler) RecordInferenceRouteResolutionFailure() {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceRouteResolutionFailuresTotal.Inc()
}

// RecordInferenceRecorderFailure records one recorder start/finalize failure.
func (h *CommonMetricsHandler) RecordInferenceRecorderFailure(stage string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferenceRecorderFailuresTotal.WithLabelValues(stage).Inc()
}

func (h *CommonMetricsHandler) RecordInferencePolicyBlock(reason proto_models.InferenceBlockReason) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.inferencePolicyBlocksTotal.WithLabelValues(enumLabel(reason, proto_models.InferenceBlockReason_name)).Inc()
}

// RecordInferenceThroughput observes available rates without creating samples for missing data.
func (h *CommonMetricsHandler) RecordInferenceThroughput(provider, operation, status string, requestTPS, generationTPS *float64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	if requestTPS != nil {
		h.inferenceRequestTPS.WithLabelValues(provider, operation, status).Observe(*requestTPS)
	}
	if generationTPS != nil {
		h.inferenceGenerationTPS.WithLabelValues(provider, operation, status).Observe(*generationTPS)
	}
}
