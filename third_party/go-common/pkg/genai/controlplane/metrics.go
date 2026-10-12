package controlplane

import (
	"context"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Metrics is the bounded-cardinality metrics sink for control-plane events. Every label is a
// bounded value — provider, registered operation symbolic name, attempt status, retry outcome,
// throttle kind, or a fixed recorder stage. Implementations must NEVER use trace IDs,
// user IDs, error strings, or arbitrary context as labels. A nil Metrics is a no-op.
//
// The concrete implementation lives in pkg/common_metrics; this narrow interface keeps the control
// plane decoupled from the metrics registry.
type Metrics interface {
	// RecordInferenceThroughput observes only available output-token rates.
	RecordInferenceThroughput(provider, operation, status string, requestTPS, generationTPS *float64)
	// RecordInferenceAttempt records one finished attempt with its latency and token total.
	RecordInferenceAttempt(provider, operation, status string, latencySeconds float64, totalTokens int64)
	// RecordInferenceRetry records one retry disposition (retried, exhausted, not_retryable).
	RecordInferenceRetry(provider, operation, outcome string)
	// RecordInferenceThrottleWait records one bucket wait-cap exhaustion before an attempt.
	RecordInferenceThrottleWait(provider, kind string)
	// RecordInferenceBackoff records one scheduled jittered backoff before a retry.
	RecordInferenceBackoff(provider, operation string, backoffSeconds float64)
	// RecordInferenceRouteResolutionFailure records one failed route resolution.
	RecordInferenceRouteResolutionFailure()
	// RecordInferencePolicyBlock records one organization-policy block by its bounded reason.
	RecordInferencePolicyBlock(reason proto_models.InferenceBlockReason)
	// RecordInferenceRecorderFailure records one recorder start/finalize failure by stage.
	RecordInferenceRecorderFailure(stage string)
}

// MetricsObserver adapts control-plane attempt-lifecycle events into bounded Metrics calls. It is an
// AttemptObserver and can be composed with the attempt recorder via MultiObserver at the single
// application composition boundary.
type MetricsObserver struct {
	metrics  Metrics
	registry *Registry
}

// NewMetricsObserver builds a metrics observer. The registry supplies the bounded operation label
// (its symbolic name); an unregistered operation falls back to "unregistered".
func NewMetricsObserver(metrics Metrics, registry *Registry) *MetricsObserver {
	return &MetricsObserver{metrics: metrics, registry: registry}
}

// AttemptStarted is unused by metrics; the finish event carries the outcome and latency.
func (m *MetricsObserver) AttemptStarted(context.Context, AttemptStartEvent) {}

// AttemptFinished records the attempt with its bounded provider/operation/status labels.
func (m *MetricsObserver) AttemptFinished(_ context.Context, event AttemptFinishEvent) {
	if m == nil || m.metrics == nil {
		return
	}
	requestTPS, generationTPS := event.Throughput()
	m.metrics.RecordInferenceThroughput(providerLabel(event.ProviderID), m.operationLabel(event.Operation), attemptStatusLabel(event.Status), requestTPS, generationTPS)
	m.metrics.RecordInferenceAttempt(
		providerLabel(event.ProviderID),
		m.operationLabel(event.Operation),
		attemptStatusLabel(event.Status),
		event.Latency().Seconds(),
		event.Result.TokenCount,
	)
}

// RetryDecided records the retry outcome.
func (m *MetricsObserver) RetryDecided(_ context.Context, event RetryDecisionEvent) {
	if m == nil || m.metrics == nil {
		return
	}
	m.metrics.RecordInferenceRetry(
		providerLabel(event.ProviderID),
		m.operationLabel(event.Operation),
		retryOutcomeLabel(event.Outcome),
	)
}

// Throttled records the bucket wait-cap exhaustion.
func (m *MetricsObserver) Throttled(_ context.Context, event ThrottleEvent) {
	if m == nil || m.metrics == nil {
		return
	}
	m.metrics.RecordInferenceThrottleWait(providerLabel(event.ProviderID), throttleKindLabel(event.Kind))
}

// BackoffScheduled records the scheduled backoff.
func (m *MetricsObserver) BackoffScheduled(_ context.Context, event BackoffEvent) {
	if m == nil || m.metrics == nil {
		return
	}
	m.metrics.RecordInferenceBackoff(providerLabel(event.ProviderID), m.operationLabel(event.Operation), event.Backoff.Seconds())
}

// operationLabel returns the bounded symbolic-name label for an operation reference.
func (m *MetricsObserver) operationLabel(operation Operation) string {
	if m.registry != nil {
		if symbol, ok := m.registry.symbolicName(operation); ok {
			return symbol
		}
	}
	return "unregistered"
}

// trimEnumPrefix drops the shared SCREAMING_SNAKE prefix of a proto enum name and lower-cases the
// remainder, producing compact, stable, bounded metric labels.
func trimEnumPrefix(name, prefix string) string {
	trimmed := strings.TrimPrefix(name, prefix)
	if trimmed == "" {
		trimmed = name
	}
	return strings.ToLower(trimmed)
}

func providerLabel(p string) string {
	if p == "" {
		return "unspecified"
	}
	return p
}

func attemptStatusLabel(s proto_models.InferenceAttemptStatus) string {
	return trimEnumPrefix(s.String(), "INFERENCE_ATTEMPT_STATUS_")
}

func retryOutcomeLabel(o proto_models.InferenceRetryOutcome) string {
	return trimEnumPrefix(o.String(), "INFERENCE_RETRY_OUTCOME_")
}

func throttleKindLabel(k proto_models.InferenceThrottleKind) string {
	return trimEnumPrefix(k.String(), "INFERENCE_THROTTLE_KIND_")
}
