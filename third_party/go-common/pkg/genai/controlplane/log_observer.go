package controlplane

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// InferenceLogObserver emits one structured event per finished provider attempt.
// It does not record usage or log provider errors, prompts, or responses. Metadata
// uses the same registered allowlist and bounded scalar projection as recorder diagnostics.
type InferenceLogObserver struct {
	NopObserver
	registry  *Registry
	loggerFor func(context.Context) *zap.Logger
}

// NewInferenceLogObserver constructs an attempt logger. loggerFor resolves the
// current caller's logger on each event; applications can use logger.For
// here to preserve SRouter runtime identity and execution traces. A nil resolver
// (or a nil result) disables logging. A nil registry admits no caller metadata.
func NewInferenceLogObserver(registry *Registry, loggerFor func(context.Context) *zap.Logger) *InferenceLogObserver {
	return &InferenceLogObserver{registry: registry, loggerFor: loggerFor}
}

func (o *InferenceLogObserver) AttemptFinished(ctx context.Context, event AttemptFinishEvent) {
	if o == nil || o.loggerFor == nil {
		return
	}
	logger := o.loggerFor(ctx)
	if logger == nil {
		return
	}
	level := zap.InfoLevel
	message := "AI provider attempt completed"
	if event.Status == AttemptStatusFailed {
		level = zap.WarnLevel
		message = "AI provider attempt failed"
	}
	entry := logger.Check(level, message)
	if entry == nil {
		return
	}
	registered, _ := o.registry.lookup(event.Operation)
	fields := make([]zap.Field, 0, 13+min(len(event.Context), len(registered.diagnosticMetadataKeys)))
	fields = append(fields,
		zap.String(appLogger.KeyInvocationID, event.InvocationID),
		zap.Int(appLogger.KeyAttemptNumber, event.AttemptNumber),
		zap.Int(appLogger.KeyMaxAttempts, event.MaxAttempts),
		zap.Stringer(logkeys.Operation, event.Operation),
		zap.String(appLogger.KeyProvider, event.ProviderID),
		zap.String(appLogger.KeyProviderModelID, event.ProviderModelID),
		zap.Uint64(appLogger.KeyInferenceModelID, event.InferenceModelID),
		zap.String(appLogger.KeyOutcome, trimEnumPrefix(event.Status.String(), "INFERENCE_ATTEMPT_STATUS_")),
		zap.Bool(appLogger.KeyRetryable, event.Retryable),
		appLogger.DurationMS("latency_ms", event.Latency()),
	)
	fields = appendDiagnosticMetadata(fields, registered.diagnosticMetadataKeys, event.Context, defaultMaxContextJSONBytes)
	if event.Status == AttemptStatusFailed {
		fallback := "surface_failure_to_caller"
		if event.Retryable && !event.Committed && event.AttemptNumber < event.MaxAttempts {
			fallback = "retry_attempt"
		}
		fields = append(fields,
			zap.String("event_name", "ai_provider_attempt_failed"),
			zap.String("error_class", retryReasonClass(event.RetryReason)),
			zap.String(logkeys.Fallback, fallback),
		)
	} else {
		fields = append(fields, zap.String("event_name", "ai_provider_attempt_completed"))
	}
	requestTPS, generationTPS := event.Throughput()
	if requestTPS != nil {
		fields = append(fields, zap.Float64("request_tokens_per_second", *requestTPS))
	}
	if generationTPS != nil {
		fields = append(fields, zap.Float64("generation_tokens_per_second", *generationTPS))
	}
	entry.Write(fields...)
}

var _ AttemptObserver = (*InferenceLogObserver)(nil)
