package controlplane

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
)

// ErrInferenceOrganizationRequired means an execution was not attributed to a
// valid organization. Organization attribution is required before any attempt
// lifecycle event, rate-limit charge, or provider call can occur.
var ErrInferenceOrganizationRequired = errors.New("inference organization is required")

// ExecuteRequest is the caller-supplied input to one logical generation: the resolved
// route to execute against and the sanitized usage context to attach to attempt
// records. The caller produces the route ONCE — via ResolveRoute (per drive on the
// agent path, per call on the direct path) or by hand — and every execution against it
// runs under that immutable decision. Actor and request-trace attribution are not
// request fields; the recorder reads them from the trusted request context.
type ExecuteRequest struct {
	// OrganizationID is the organization that owns this logical invocation. It is
	// immutable across retries and provider fallbacks and must be positive.
	OrganizationID common_service_models.OrganizationIdType
	Route          *ResolvedRoute
	Context        map[string]any
	// RuntimePurpose is trusted caller attribution, immutable across retries.
	RuntimePurpose proto_models.InferenceCallPurpose
	// InvocationID optionally supplies the invocation's ledger correlation key. Leave it
	// empty to have the control plane mint one. A caller that must know the key while
	// attempts are still running (the agent bridge reports it to per-attempt admission
	// gates) mints it with NewInvocationID and passes it here, so its attempt callback can
	// close over the value instead of reading it from a context. It must be unique per
	// Execute/ExecuteStream call: attempt records are keyed on invocation ID plus attempt
	// number. ExecutionResult.InvocationID always echoes the effective value.
	InvocationID string
}

// AttemptResult is what one provider attempt reports back to the executor. TokenCount
// is the actual reported token usage the executor charges to the token bucket after
// the attempt; Outcome and RawResponse carry the normalized and raw provider results
// for the caller and the attempt recorder.
type AttemptResult struct {
	Outcome     *commonai.GenerationOutcome
	RawResponse *ai.ModelResponse
	TokenCount  int64
}

// ExecutionResult is returned from Execute and ExecuteStream. InvocationID is the
// ledger correlation key and is populated even when execution returns an error.
// Attempt holds the successful attempt's result, or the last attempt's result on
// failure.
type ExecutionResult struct {
	InvocationID string
	Attempt      AttemptResult
}

// AttemptFunc performs one buffered provider attempt against the resolved route. The
// integer is the one-based attempt number.
type AttemptFunc func(context.Context, *ResolvedRoute, int) (AttemptResult, error)

// StreamAttemptFunc performs one streaming provider attempt. It must call the commit
// function exactly once, immediately before delivering the first downstream chunk. An
// error returned after commit is terminal because a replay could duplicate externally
// visible output; an error before commit follows the registered retry policy.
type StreamAttemptFunc func(context.Context, *ResolvedRoute, int, func()) (AttemptResult, error)

// ControlPlane owns route resolution (ResolveRoute) and the full attempt loop:
// timeout selection, request/token buckets, retry classification, jittered backoff,
// and attempt lifecycle observation. It is safe for concurrent use.
type ControlPlane struct {
	resolver   RouteResolver
	registry   *bucketRegistry
	observer   AttemptObserver
	classifier ShouldRetryFunc
	logger     *zap.Logger

	randMu sync.Mutex
	rand   *rand.Rand
}

// Option configures a ControlPlane at construction.
type Option func(*ControlPlane)

// WithObserver installs the attempt-lifecycle observer. The default is NopObserver.
func WithObserver(observer AttemptObserver) Option {
	return func(c *ControlPlane) {
		if observer != nil {
			c.observer = observer
		}
	}
}

// WithLogger sets the structured logger. The default is a no-op logger.
func WithLogger(logger *zap.Logger) Option {
	return func(c *ControlPlane) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithRand injects the random source used for backoff jitter, making jitter
// deterministic under an injected seed for tests.
func WithRand(r *rand.Rand) Option {
	return func(c *ControlPlane) {
		if r != nil {
			c.rand = r
		}
	}
}

// New builds a ControlPlane over the supplied route resolver.
func New(resolver RouteResolver, opts ...Option) *ControlPlane {
	c := &ControlPlane{
		resolver:   resolver,
		registry:   newBucketRegistry(),
		observer:   NopObserver{},
		classifier: ClassifyRetryableError,
		logger:     zap.NewNop(),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ResolveRoute maps a numeric operation to the immutable route one logical generation
// executes against: it runs the installed resolver and validates the returned route's
// retry and rate-limit policies. Resolution is deliberately separate from execution so
// a caller resolves ONCE and derives everything else — the generator it builds, the
// ledger attribution, and every Execute/ExecuteStream call — from that single
// resolution; a route edit therefore takes effect on the next resolution, never
// mid-decision.
func (c *ControlPlane) ResolveRoute(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error) {
	operation := Operation(request.Operation)

	route, err := c.resolver.Resolve(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("resolve route for operation %s: %w", operation, err)
	}
	if route == nil {
		return nil, fmt.Errorf("resolve route for operation %s: nil route", operation)
	}
	if route.Operation != operation {
		return nil, fmt.Errorf("resolved operation %d does not match request %d", route.Operation, operation)
	}
	if err := request.ModelSelection.Validate(); err != nil {
		return nil, err
	}
	if selection := request.ModelSelection; selection != nil {
		if route.InferenceModelID != selection.InferenceModelID {
			return nil, fmt.Errorf("resolved model does not match explicit model selection")
		}
		if selection.API != nil && *selection.API != 0 && route.API != *selection.API {
			return nil, fmt.Errorf("resolved API does not match explicit selection")
		}
	}
	if err := validateRoutePolicies(route); err != nil {
		return nil, err
	}
	return route, nil
}

// identityRevalidator is implemented by resolvers whose security policy can change
// between resolution and dispatch (LayeredRouteResolver). It owns pinning the request.
type identityRevalidator interface {
	RevalidateIdentity(context.Context, inference.GenerationRequest, *ResolvedRoute) error
}

// RevalidateRoute re-checks a previously resolved route immediately before dispatch.
// Resolvers without dynamic security policy have nothing to re-check.
func (c *ControlPlane) RevalidateRoute(ctx context.Context, request inference.GenerationRequest, route *ResolvedRoute) error {
	if c == nil || route == nil {
		return fmt.Errorf("inference dispatch route is unavailable")
	}
	if revalidator, supported := c.resolver.(identityRevalidator); supported {
		return revalidator.RevalidateIdentity(ctx, request, route)
	}
	return nil
}

// validateRoutePolicies is the struct-only policy validation shared by ResolveRoute
// and the execute path, which repeats it so hand-built routes are guarded too.
func validateRoutePolicies(route *ResolvedRoute) error {
	if err := apishape.ValidateSelection(route.API); err != nil {
		return err
	}
	if err := ValidateRetryPolicy(route.RetryPolicy); err != nil {
		return fmt.Errorf("invalid retry policy for operation %s: %w", route.Operation, err)
	}
	if err := ValidateModelRateLimitPolicy(route.RateLimitPolicy); err != nil {
		return fmt.Errorf("invalid rate-limit policy for operation %s: %w", route.Operation, err)
	}
	return nil
}

// Execute runs a buffered generation through the full attempt loop and returns the
// invocation ID (on success and failure) alongside the successful or last attempt
// result.
func (c *ControlPlane) Execute(ctx context.Context, request ExecuteRequest, fn AttemptFunc) (ExecutionResult, error) {
	return c.execute(ctx, request, false, func(actx context.Context, route *ResolvedRoute, attempt int, _ func()) (AttemptResult, error) {
		return fn(actx, route, attempt)
	})
}

// ExecuteStream runs a streaming generation through the full attempt loop. It is a
// first-class contract: the attempt's commit callback marks the point after which a
// failure can no longer be retried.
func (c *ControlPlane) ExecuteStream(ctx context.Context, request ExecuteRequest, fn StreamAttemptFunc) (ExecutionResult, error) {
	return c.execute(ctx, request, true, fn)
}

// execute is the shared attempt loop for buffered and streaming generation.
func (c *ControlPlane) execute(
	ctx context.Context,
	request ExecuteRequest,
	streaming bool,
	attemptFn StreamAttemptFunc,
) (ExecutionResult, error) {
	// The invocation ID is fixed before route validation so it is available on every
	// exit path, including a rejected route.
	result := ExecutionResult{InvocationID: request.InvocationID}
	if result.InvocationID == "" {
		result.InvocationID = NewInvocationID()
	}
	if request.OrganizationID <= 0 {
		return result, ErrInferenceOrganizationRequired
	}

	// The caller resolved the route once (ResolveRoute) or built it by hand; policy
	// validation is struct-only and repeated here so hand-built routes are guarded.
	route := request.Route
	if route == nil {
		return result, fmt.Errorf("execute: nil resolved route")
	}
	if err := validateRoutePolicies(route); err != nil {
		return result, err
	}

	scopeKey := route.scopeKey()
	requestBucket, tokenBucket, err := c.registry.bucketsForScope(scopeKey, route.RateLimitPolicy)
	if err != nil {
		return result, err
	}

	// Observers receive a cancellation-detached context: a DB-backed recorder must be
	// able to persist the terminal event exactly when the caller cancels or times out.
	// WithoutCancel keeps trace IDs and other request-scoped values while dropping the
	// cancellation signal.
	obsCtx := context.WithoutCancel(ctx)

	budgetCtx, cancel := context.WithTimeout(ctx, route.RetryPolicy.TotalTimeout)
	defer cancel()

	attempts := route.RetryPolicy.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		// Retry backoff happens before either rate-limit gate so an invocation that is
		// canceled while idle does not wait for token debt or consume request capacity.
		if attempt > 1 {
			backoff := c.backoffWithJitter(route.RetryPolicy, attempt)
			c.emitBackoff(obsCtx, route, result.InvocationID, attempt, scopeKey, backoff)
			if err := sleepWithContext(budgetCtx, backoff); err != nil {
				return result, fmt.Errorf("retry backoff interrupted before attempt %d for operation %s: %w", attempt, route.Operation, err)
			}
		}

		// Token bucket: wait until any accumulated token debt is repaid (required = 0)
		// before spending an attempt.
		if route.RateLimitPolicy.HasTokenLimit && tokenBucket != nil {
			if err := tokenBucket.waitUntilAvailable(budgetCtx, route.RateLimitPolicy.TokenLimit.WaitCap, 0); err != nil {
				c.emitThrottle(obsCtx, route, result.InvocationID, attempt, scopeKey, ThrottleKindToken, err)
				return result, fmt.Errorf("token rate-limit bucket unavailable before attempt %d for operation %s: %w", attempt, route.Operation, err)
			}
		}
		// Request bucket: charge one request before the provider call.
		if route.RateLimitPolicy.HasRequestLimit && requestBucket != nil {
			if err := requestBucket.acquire(budgetCtx, route.RateLimitPolicy.RequestLimit.WaitCap, 1); err != nil {
				c.emitThrottle(obsCtx, route, result.InvocationID, attempt, scopeKey, ThrottleKindRequest, err)
				return result, fmt.Errorf("request rate-limit bucket unavailable before attempt %d for operation %s: %w", attempt, route.Operation, err)
			}
		}

		var committed atomic.Bool
		commit := func() { committed.Store(true) }

		deadlineAt, _ := budgetCtx.Deadline()
		startedAt := time.Now()
		c.emitStart(obsCtx, AttemptStartEvent{
			EffectiveSettingsJSON: route.executionSettingsJSON(),
			DeadlineAt:            deadlineAt,
			InvocationID:          result.InvocationID,
			OrganizationID:        request.OrganizationID,
			RuntimePurpose:        request.RuntimePurpose,
			Operation:             route.Operation,
			AttemptNumber:         attempt,
			MaxAttempts:           attempts,
			InferenceModelID:      route.InferenceModelID,
			ProviderID:            route.ProviderID,
			ProviderModelID:       route.ProviderModelID,
			API:                   route.API,
			Streaming:             streaming,
			StartedAt:             startedAt,
			Context:               request.Context,
		})

		attemptResult, attemptErr := attemptFn(budgetCtx, route, attempt, commit)
		attemptResult = normalizeAttemptUsage(attemptResult)
		endedAt := time.Now()

		// Token buckets consume the actual reported tokens after every attempt,
		// success or failure.
		if route.RateLimitPolicy.HasTokenLimit && tokenBucket != nil && attemptResult.TokenCount > 0 {
			tokenBucket.consume(float64(attemptResult.TokenCount))
		}

		didCommit := committed.Load()
		var decision RetryDecision
		if attemptErr != nil {
			if IsProviderCallGateError(attemptErr) {
				// Ownership and checkpoint failures must never enter inference retries.
				decision = RetryDecision{Reason: RetryReasonNotRetryable}
			} else if streaming && didCommit {
				// A post-commit streaming failure is never retried: replay could
				// duplicate already-delivered output.
				decision = RetryDecision{Retryable: false, Reason: RetryReasonStreamCommitted}
			} else {
				decision = c.classifier(attemptErr)
			}
		}

		status := AttemptStatusSucceeded
		if attemptErr != nil {
			status = AttemptStatusFailed
		}
		purpose := request.RuntimePurpose
		if purpose == proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN && attemptResult.Outcome != nil && len(attemptResult.Outcome.ToolRequests) > 0 {
			purpose = proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_TOOL_LOOP
		}
		c.emitFinish(obsCtx, AttemptFinishEvent{
			EffectiveSettingsJSON: route.executionSettingsJSON(),
			InvocationID:          result.InvocationID,
			OrganizationID:        request.OrganizationID,
			RuntimePurpose:        purpose,
			Operation:             route.Operation,
			AttemptNumber:         attempt,
			MaxAttempts:           attempts,
			InferenceModelID:      route.InferenceModelID,
			ProviderID:            route.ProviderID,
			ProviderModelID:       route.ProviderModelID,
			API:                   route.API,
			Streaming:             streaming,
			Committed:             didCommit,
			StartedAt:             startedAt,
			EndedAt:               endedAt,
			Status:                status,
			Result:                attemptResult,
			Err:                   attemptErr,
			Retryable:             decision.Retryable,
			RetryReason:           decision.Reason,
			Context:               request.Context,
		})

		if attemptErr == nil {
			result.Attempt = attemptResult
			return result, nil
		}

		result.Attempt = attemptResult
		lastErr = attemptErr

		outcome := RetryOutcomeRetried
		switch {
		case !decision.Retryable:
			outcome = RetryOutcomeNotRetryable
		case attempt == attempts:
			outcome = RetryOutcomeExhausted
		}
		c.emitRetryDecision(obsCtx, RetryDecisionEvent{
			InvocationID:  result.InvocationID,
			Operation:     route.Operation,
			AttemptNumber: attempt,
			MaxAttempts:   attempts,
			ProviderID:    route.ProviderID,
			Outcome:       outcome,
			Decision:      decision,
			Err:           attemptErr,
		})

		if !decision.Retryable {
			// Preserve the original error for terminal failures.
			return result, attemptErr
		}
		if attempt == attempts {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("retry failed without captured error for operation %s", route.Operation)
	}
	return result, fmt.Errorf("request failed after %d attempt(s) for operation %s: %w", attempts, route.Operation, lastErr)
}

// Outcome's positive provider total takes precedence over TokenCount; absent or
// nonpositive outcome totals use the nonnegative count. Copy the outcome so the
// caller's object remains untouched, and every quota/metric/ledger consumer sees
// the same actual usage total.
func normalizeAttemptUsage(result AttemptResult) AttemptResult {
	total := max(result.TokenCount, 0)
	if result.Outcome != nil {
		if result.Outcome.TotalTokens > 0 {
			total = int64(result.Outcome.TotalTokens)
		}
		outcome := *result.Outcome
		outcome.TotalTokens = int(total)
		result.Outcome = &outcome
	}
	result.TokenCount = total
	return result
}

// backoffWithJitter computes the jittered backoff for one retry attempt under the
// executor's random source.
func (c *ControlPlane) backoffWithJitter(policy RetryPolicy, attempt int) time.Duration {
	c.randMu.Lock()
	jitter := 0.5 + c.rand.Float64() // [0.5, 1.5)
	c.randMu.Unlock()
	return computeBackoff(policy, attempt, jitter)
}

// computeBackoff derives the backoff for an attempt given an explicit jitter
// multiplier, so tests can assert the value deterministically. The first retry
// (attempt 2) uses the initial backoff; each later retry doubles it, capped at the
// policy maximum, then scaled by jitter and re-capped, with a one-millisecond floor.
func computeBackoff(policy RetryPolicy, attempt int, jitter float64) time.Duration {
	exponent := float64(attempt - 2)
	if exponent < 0 {
		exponent = 0
	}
	base := float64(policy.InitialBackoff) * math.Pow(2, exponent)
	if base > float64(policy.MaxBackoff) {
		base = float64(policy.MaxBackoff)
	}
	if base < 1 {
		base = 1
	}
	value := min(time.Duration(base*jitter), policy.MaxBackoff)
	if value < time.Millisecond {
		return time.Millisecond
	}
	return value
}

// sleepWithContext waits for the duration or the context, whichever comes first.
func sleepWithContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
