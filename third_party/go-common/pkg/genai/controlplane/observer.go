package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// AttemptStatus is the lifecycle state of one admitted execution attempt. It aliases
// the generated protobuf enum whose integer values the kernel already matched, so
// the persisted attempt-ledger status field and this in-memory type are one type.
type AttemptStatus = proto_models.InferenceAttemptStatus

const (
	// AttemptStatusSucceeded means the admitted execution attempt returned no error.
	AttemptStatusSucceeded = proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_SUCCEEDED
	// AttemptStatusFailed means the admitted execution attempt returned an error.
	AttemptStatusFailed = proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_FAILED
)

// RetryOutcome is the typed classification of a failed attempt's disposition,
// aliased to the generated protobuf enum and suitable for bounded metric labels.
type RetryOutcome = proto_models.InferenceRetryOutcome

const (
	// RetryOutcomeRetried means the attempt failed retryably and another attempt follows.
	RetryOutcomeRetried = proto_models.InferenceRetryOutcome_INFERENCE_RETRY_OUTCOME_RETRIED
	// RetryOutcomeExhausted means the attempt failed retryably but was the last attempt.
	RetryOutcomeExhausted = proto_models.InferenceRetryOutcome_INFERENCE_RETRY_OUTCOME_EXHAUSTED
	// RetryOutcomeNotRetryable means the classifier judged the failure terminal.
	RetryOutcomeNotRetryable = proto_models.InferenceRetryOutcome_INFERENCE_RETRY_OUTCOME_NOT_RETRYABLE
)

// ThrottleKind identifies which bucket blocked an attempt before the provider
// call. It aliases the generated protobuf enum.
type ThrottleKind = proto_models.InferenceThrottleKind

const (
	// ThrottleKindRequest marks a request-bucket wait-cap exhaustion.
	ThrottleKindRequest = proto_models.InferenceThrottleKind_INFERENCE_THROTTLE_KIND_REQUEST
	// ThrottleKindToken marks a token-bucket wait-cap exhaustion.
	ThrottleKindToken = proto_models.InferenceThrottleKind_INFERENCE_THROTTLE_KIND_TOKEN
)

// AttemptStartEvent is execution telemetry emitted once after local admission,
// before request preparation and middleware. It is not evidence of provider I/O
// and must not be used as a billing checkpoint.
type AttemptStartEvent struct {
	RuntimePurpose proto_models.InferenceCallPurpose
	// EffectiveSettingsJSON is a non-secret policy snapshot from the resolved route.
	EffectiveSettingsJSON []byte
	API                   proto_models.InferenceAPI
	// DeadlineAt is the actual execution budget deadline; zero means unavailable.
	DeadlineAt       time.Time
	InvocationID     string
	OrganizationID   common_service_models.OrganizationIdType
	Operation        Operation
	AttemptNumber    int
	MaxAttempts      int
	InferenceModelID uint64
	ProviderID       string
	ProviderModelID  string
	Streaming        bool
	StartedAt        time.Time
	// Context is the sanitized usage context supplied on the ExecuteRequest. Recorders
	// must apply their own allowlisting and size bounds before persisting it.
	Context map[string]any
}

// AttemptFinishEvent is emitted exactly once after each admitted execution attempt returns,
// pairing with the AttemptStartEvent for the same attempt number.
type AttemptFinishEvent struct {
	RuntimePurpose proto_models.InferenceCallPurpose
	// EffectiveSettingsJSON is a non-secret policy snapshot from the resolved route.
	EffectiveSettingsJSON []byte
	API                   proto_models.InferenceAPI
	InvocationID          string
	OrganizationID        common_service_models.OrganizationIdType
	Operation             Operation
	AttemptNumber         int
	MaxAttempts           int
	InferenceModelID      uint64
	ProviderID            string
	ProviderModelID       string
	Streaming             bool
	// Committed reports whether a streaming attempt delivered downstream output before
	// finishing; a committed streaming failure is always terminal.
	Committed bool
	StartedAt time.Time
	EndedAt   time.Time
	// Status is the terminal disposition of the attempt (succeeded or failed).
	Status AttemptStatus
	// Result carries the attempt's returned outcome, raw response, and token count.
	Result AttemptResult
	// Err is nil on success, otherwise the admitted execution attempt error.
	Err error
	// Retryable and RetryReason are the classifier's decision for a failed attempt.
	Retryable   bool
	RetryReason RetryReason
	Context     map[string]any
}

// Latency returns the wall-clock duration of the attempt.
func (e AttemptFinishEvent) Latency() time.Duration {
	return e.EndedAt.Sub(e.StartedAt)
}

// RetryDecisionEvent reports the disposition of a failed attempt.
type RetryDecisionEvent struct {
	InvocationID  string
	Operation     Operation
	AttemptNumber int
	MaxAttempts   int
	// ProviderID snapshots the resolved route's provider for bounded metric labeling.
	ProviderID string
	Outcome    RetryOutcome
	Decision   RetryDecision
	Err        error
}

// ThrottleEvent reports a request or token bucket wait that exceeded its cap.
type ThrottleEvent struct {
	InvocationID  string
	Operation     Operation
	AttemptNumber int
	// ProviderID snapshots the resolved route's provider for bounded metric labeling.
	ProviderID string
	Scope      string
	Kind       ThrottleKind
	Err        error
}

// BackoffEvent reports the jittered backoff scheduled before a retry attempt.
type BackoffEvent struct {
	InvocationID  string
	Operation     Operation
	AttemptNumber int
	// ProviderID snapshots the resolved route's provider for bounded metric labeling.
	ProviderID string
	Scope      string
	Backoff    time.Duration
}

// AttemptObserver is the attempt-lifecycle seam used by the recorder and metrics
// bridge. NopObserver disables observation; MultiObserver fans out events.
//
// Implementations must be safe for concurrent use and must never affect generation
// success: the executor invokes every method on a log-and-continue basis and
// recovers from panics, so observer failures are contained. Callbacks are invoked
// synchronously on the executor's goroutine, and AttemptStarted runs before the
// provider call while AttemptFinished runs between retries — so a slow callback does
// not merely "delay" the loop, it consumes the generation's execution budget and can
// push the provider call past its deadline. Callbacks must therefore return promptly:
// an observer that performs I/O (a database-backed recorder, a network export) must
// dispatch that work asynchronously and never block the callback on it.
//
// Observer callbacks receive a cancellation-detached context (context.WithoutCancel
// of the caller's context): request-scoped values such as trace IDs are preserved,
// but the caller's cancellation and deadline are dropped. A recorder can therefore
// persist the terminal event for an attempt even when the caller canceled or timed
// out — which is exactly when a terminal record matters most.
type AttemptObserver interface {
	// AttemptStarted fires once per admitted execution attempt, before request
	// preparation. It is telemetry, not billing evidence of provider execution.
	AttemptStarted(ctx context.Context, event AttemptStartEvent)
	// AttemptFinished fires once per admitted execution attempt, after it returns.
	AttemptFinished(ctx context.Context, event AttemptFinishEvent)
	// RetryDecided fires once per failed attempt with its retry disposition.
	RetryDecided(ctx context.Context, event RetryDecisionEvent)
	// Throttled fires when a bucket wait cap is exceeded before an attempt.
	Throttled(ctx context.Context, event ThrottleEvent)
	// BackoffScheduled fires with the jittered backoff before a retry attempt.
	BackoffScheduled(ctx context.Context, event BackoffEvent)
}

// NopObserver is the default observer: it ignores every event. It lets the executor
// run without a recorder.
type NopObserver struct{}

func (NopObserver) AttemptStarted(context.Context, AttemptStartEvent)   {}
func (NopObserver) AttemptFinished(context.Context, AttemptFinishEvent) {}
func (NopObserver) RetryDecided(context.Context, RetryDecisionEvent)    {}
func (NopObserver) Throttled(context.Context, ThrottleEvent)            {}
func (NopObserver) BackoffScheduled(context.Context, BackoffEvent)      {}

// MultiObserver fans one event out to several observers in order. It lets an
// application install a metrics observer and a recorder observer at the single
// composition boundary without layering them structurally.
//
// Children are panic-isolated from each other: a panicking observer never
// suppresses delivery to its siblings. Recovered child panics are re-raised
// (as []observerPanic) only after every child has run, so they still surface
// to the caller's own recovery — the executor's log-and-continue layer.
type MultiObserver []AttemptObserver

// observerPanic carries a recovered child panic out of the fan-out so the
// executor's recovery layer can log which observer failed and with what value.
type observerPanic struct {
	Observer string // %T of the panicking observer
	Value    any
}

// fanOut invokes fn on every non-nil child, recovering per child so one
// panicking observer cannot suppress its siblings.
func (m MultiObserver) fanOut(fn func(AttemptObserver)) {
	var panics []observerPanic
	for _, o := range m {
		if o == nil {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					panics = append(panics, observerPanic{Observer: fmt.Sprintf("%T", o), Value: r})
				}
			}()
			fn(o)
		}()
	}
	if len(panics) > 0 {
		panic(panics)
	}
}

func (m MultiObserver) AttemptStarted(ctx context.Context, event AttemptStartEvent) {
	m.fanOut(func(o AttemptObserver) { o.AttemptStarted(ctx, event) })
}

func (m MultiObserver) AttemptFinished(ctx context.Context, event AttemptFinishEvent) {
	m.fanOut(func(o AttemptObserver) { o.AttemptFinished(ctx, event) })
}

func (m MultiObserver) RetryDecided(ctx context.Context, event RetryDecisionEvent) {
	m.fanOut(func(o AttemptObserver) { o.RetryDecided(ctx, event) })
}

func (m MultiObserver) Throttled(ctx context.Context, event ThrottleEvent) {
	m.fanOut(func(o AttemptObserver) { o.Throttled(ctx, event) })
}

func (m MultiObserver) BackoffScheduled(ctx context.Context, event BackoffEvent) {
	m.fanOut(func(o AttemptObserver) { o.BackoffScheduled(ctx, event) })
}
