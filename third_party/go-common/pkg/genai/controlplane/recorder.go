package controlplane

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Default privacy and durability bounds for the attempt recorder.
const (
	// defaultRecorderWriteTimeout bounds each terminal recorder write. The kernel already detaches
	// the observer context from the caller's cancellation; this timeout additionally bounds a write
	// that stalls so a canceled or timed-out execution still finalizes its attempt row.
	defaultRecorderWriteTimeout = 5 * time.Second
	// defaultMaxErrorMessageBytes caps a stored error string (the column is varchar(1024)).
	defaultMaxErrorMessageBytes = 1024
	// defaultMaxContextJSONBytes caps the sanitized context JSON; larger context is dropped rather
	// than stored, so the ledger can never balloon on a single row.
	defaultMaxContextJSONBytes = 8 * 1024
	// defaultRecorderQueueSize bounds the pending-write queue. Ledger writes are dispatched to
	// background workers so they never run on the provider's execution budget; when the queue is
	// full a write is dropped and metered rather than blocking the attempt loop.
	defaultRecorderQueueSize = 2048
	// defaultRecorderWorkers is the number of background goroutines draining the write queue. Writes
	// are idempotent upserts keyed on (invocation_id, attempt_number), so start/finish ordering
	// across workers does not matter.
	defaultRecorderWorkers = 4
)

// Bounded recorder-failure metric stages. Every value is a fixed string so the metric label stays
// low-cardinality. Start and finalize drops are distinct: a dropped finalize can leave a start row
// in flight until the deadline sweep detects the missing outcome, so
// operators must be able to tell the two apart.
const (
	recorderStageStart           = "start"
	recorderStageFinalize        = "finalize"
	recorderStageStartDropped    = "start_dropped"
	recorderStageFinalizeDropped = "finalize_dropped"
	recorderStagePanic           = "panic"
	recorderLoggerName           = "genai.controlplane.recorder"
)

// recorderDiagnosticScope is an immutable snapshot of the correlation available when an
// observer event arrives. Keeping the derived logger and attempt identity together prevents
// background writes from consulting a canceled context or accidentally borrowing another
// concurrent invocation's fields.
type recorderDiagnosticScope struct {
	logger    *zap.Logger
	operation Operation
}

type recorderWriteKind uint8

const (
	recorderWriteStart recorderWriteKind = iota
	recorderWriteFinalize
)

type recorderJob struct {
	scope   recorderDiagnosticScope
	base    context.Context
	attempt *cpm.InferenceAttempt
	kind    recorderWriteKind
}

// AttemptStore is the narrow attempt-write surface the recorder needs. The concrete
// *common_persistence.ControlPlaneStore satisfies it.
//
// Implementations MUST NOT bind to a request-scoped database transaction: the recorder dispatches
// these writes to background workers, so a write can (and often does) execute after the originating
// request has already returned and its transaction has been committed or rolled back. Writes must
// run on the store's own connection/transaction.
type AttemptStore interface {
	CreateAttemptStart(ctx context.Context, attempt *cpm.InferenceAttempt) error
	FinalizeAttempt(ctx context.Context, attempt *cpm.InferenceAttempt) error
	// SweepStaleAttempts updates at most limit rows with deadlines strictly before cutoff.
	SweepStaleAttempts(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// AttemptRecorder persists control-plane attempts to the ledger. It is an AttemptObserver: it
// creates a start row on AttemptStarted and finalizes it on AttemptFinished, always in the store's
// own transaction (decision 13).
//
// Writes are dispatched asynchronously: AttemptStarted/AttemptFinished build the row synchronously
// (capturing trace id, actor, and sanitized context from the live request context) and hand the
// write to a bounded queue drained by background workers. This keeps ledger latency off the
// provider's execution budget — a stalled database can never delay or fail a generation. When the
// queue is full a write is dropped and metered rather than blocking the attempt loop, and each
// worker recovers from panics so a faulty store can never crash the process. Recorder failures are
// logged and metered but never alter inference success.
//
// Close drains the queue and stops the workers; callers should defer it for graceful shutdown.
//
// The request-scoped actor user id and trace id are read from the trusted request context via
// scontext.
type AttemptRecorder struct {
	store    AttemptStore
	registry *Registry
	metrics  Metrics
	logger   *zap.Logger

	writeTimeout    time.Duration
	maxErrorBytes   int
	maxContextBytes int

	// queue carries pending write jobs to the worker pool. mu guards enqueue against Close so a job
	// is never sent on the closed channel.
	queue  chan recorderJob
	mu     sync.Mutex
	closed bool

	workersWG   sync.WaitGroup
	closeOnce   sync.Once
	sweepWG     sync.WaitGroup
	sweepCancel context.CancelFunc
	// shutdownCtx is canceled by Close after a bounded grace period so in-flight and still-queued
	// writes abort promptly instead of holding shutdown open for the full queue-length worth of
	// write timeouts.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// RecorderOption configures an AttemptRecorder.
type RecorderOption func(recorderConfig) recorderConfig

type recorderConfig struct {
	metrics         Metrics
	logger          *zap.Logger
	writeTimeout    time.Duration
	maxErrorBytes   int
	maxContextBytes int
	queueSize       int
	workers         int
}

// WithRecorderMetrics installs the bounded metrics sink for recorder start/finalize failures.
func WithRecorderMetrics(metrics Metrics) RecorderOption {
	return func(c recorderConfig) recorderConfig {
		if metrics != nil {
			c.metrics = metrics
		}
		return c
	}
}

// WithRecorderLogger sets the structured logger used for recorder failures.
func WithRecorderLogger(logger *zap.Logger) RecorderOption {
	return func(c recorderConfig) recorderConfig {
		if logger != nil {
			c.logger = logger
		}
		return c
	}
}

// WithRecorderWriteTimeout overrides the bounded terminal-write timeout.
func WithRecorderWriteTimeout(timeout time.Duration) RecorderOption {
	return func(c recorderConfig) recorderConfig {
		if timeout > 0 {
			c.writeTimeout = timeout
		}
		return c
	}
}

// WithRecorderQueueSize overrides the bounded pending-write queue capacity. A larger queue tolerates
// longer database stalls before dropping writes, at the cost of more buffered memory.
func WithRecorderQueueSize(size int) RecorderOption {
	return func(c recorderConfig) recorderConfig {
		if size > 0 {
			c.queueSize = size
		}
		return c
	}
}

// WithRecorderWorkers overrides the number of background workers draining the write queue.
func WithRecorderWorkers(workers int) RecorderOption {
	return func(c recorderConfig) recorderConfig {
		if workers > 0 {
			c.workers = workers
		}
		return c
	}
}

// NewAttemptRecorder builds an attempt recorder over the given store and registry and starts its
// background write workers. Callers should defer Close to drain the queue and stop the workers.
func NewAttemptRecorder(store AttemptStore, registry *Registry, opts ...RecorderOption) *AttemptRecorder {
	cfg := recorderConfig{
		logger:          zap.NewNop(),
		writeTimeout:    defaultRecorderWriteTimeout,
		maxErrorBytes:   defaultMaxErrorMessageBytes,
		maxContextBytes: defaultMaxContextJSONBytes,
		queueSize:       defaultRecorderQueueSize,
		workers:         defaultRecorderWorkers,
	}
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	r := &AttemptRecorder{
		store:           store,
		registry:        registry,
		metrics:         cfg.metrics,
		logger:          cfg.logger,
		writeTimeout:    cfg.writeTimeout,
		maxErrorBytes:   cfg.maxErrorBytes,
		maxContextBytes: cfg.maxContextBytes,
		queue:           make(chan recorderJob, cfg.queueSize),
		shutdownCtx:     shutdownCtx,
		shutdownCancel:  shutdownCancel,
	}

	r.workersWG.Add(cfg.workers)
	for i := 0; i < cfg.workers; i++ {
		go r.worker()
	}
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	r.sweepCancel = sweepCancel
	r.sweepWG.Add(1)
	go r.sweepLoop(sweepCtx)
	return r
}

// worker drains the write queue until it is closed and empty. Each job runs under its own panic
// recovery so a faulty store can never crash the process; the synchronous caller no longer shields
// these writes.
func (r *AttemptRecorder) worker() {
	defer r.workersWG.Done()
	for job := range r.queue {
		r.runJob(job)
	}
}

// runJob executes one queued write, recovering from panics.
func (r *AttemptRecorder) runJob(job recorderJob) {
	defer func() {
		if rec := recover(); rec != nil {
			job.scope.logger.Error("control plane: attempt recorder write panicked",
				zap.String(logkeys.Invariant, "inference_attempt_recorder_write_does_not_panic"),
				zap.String(logkeys.Stage, recorderStagePanic),
				zap.String(logkeys.Expected, "recorder write returns normally"),
				zap.String(logkeys.Actual, "recorder write panicked"),
				zap.String(logkeys.Fallback, "drop_current_write_and_continue_worker"),
				zap.Any(logkeys.Panic, rec),
			)
			if r.metrics != nil {
				r.metrics.RecordInferenceRecorderFailure(recorderStagePanic)
			}
		}
	}()

	writeCtx, cancel := r.writeContext(job.base)
	defer cancel()

	var stage, detail string
	var err error
	switch job.kind {
	case recorderWriteStart:
		stage = recorderStageStart
		detail = "create attempt start"
		err = r.store.CreateAttemptStart(writeCtx, job.attempt)
	case recorderWriteFinalize:
		stage = recorderStageFinalize
		detail = "finalize attempt"
		err = r.store.FinalizeAttempt(writeCtx, job.attempt)
	default:
		panic("control plane: invalid recorder write kind")
	}
	if err != nil {
		r.recorderFailed(job.scope, stage, err, detail)
	}
}

// enqueue offers a job to the worker pool without blocking. It returns false when the recorder is
// closed or the queue is full, so the caller can meter the drop. The mutex serializes enqueue with
// Close's channel close, so a job is never sent on a closed channel.
func (r *AttemptRecorder) enqueue(job recorderJob) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	select {
	case r.queue <- job:
		return true
	default:
		return false
	}
}

// writeContext derives the bounded write context for one job. It preserves the request-scoped values
// captured in base (trace id, OTel span) while dropping the caller's cancellation, bounds the write
// by writeTimeout, and additionally aborts if the recorder is shutting down so Close cannot hang on a
// stalled database.
func (r *AttemptRecorder) writeContext(base context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(base, r.writeTimeout)
	stop := context.AfterFunc(r.shutdownCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// Close drains the pending write queue, stops the background workers, and returns once they exit. It
// is idempotent. Workers are given a grace period of one write timeout to flush the backlog; after
// that the shutdown context is canceled so any remaining in-flight or queued writes abort promptly
// (a stalled database therefore cannot hold shutdown open for the full queue). Calls to
// AttemptStarted/AttemptFinished after Close are dropped and metered.
func (r *AttemptRecorder) Close() {
	r.closeOnce.Do(func() {
		if r.sweepCancel != nil {
			r.sweepCancel()
		}
		r.sweepWG.Wait()
		r.mu.Lock()
		r.closed = true
		close(r.queue)
		r.mu.Unlock()

		// Cancel outstanding writes if the drain outlasts the grace period, then wait for workers.
		graceTimer := time.AfterFunc(r.writeTimeout, r.shutdownCancel)
		r.workersWG.Wait()
		graceTimer.Stop()
		r.shutdownCancel()
	})
}

// AttemptStarted builds the start row and dispatches its write to the worker pool. The row is
// assembled synchronously so trace id, actor, and sanitized context are captured from the live
// request context; the database write runs asynchronously so it never consumes the provider's
// execution budget.
func (r *AttemptRecorder) AttemptStarted(ctx context.Context, event AttemptStartEvent) {
	_, ok := r.registry.Lookup(event.Operation)
	if !ok {
		scope := r.diagnosticScope(ctx, event.Operation, event.InvocationID, event.AttemptNumber, nil)
		r.recorderFailed(scope, recorderStageStart, nil, "operation not registered")
		return
	}
	scope := r.diagnosticScope(ctx, event.Operation, event.InvocationID, event.AttemptNumber, event.Context)

	startedAt := event.StartedAt
	attempt := &cpm.InferenceAttempt{
		EffectiveSettingsJSON: append([]byte(nil), event.EffectiveSettingsJSON...),
		Operation:             int32(event.Operation),
		InvocationID:          event.InvocationID,
		OrganizationID:        new(event.OrganizationID),
		RuntimePurpose:        event.RuntimePurpose,
		AttemptNumber:         int32(event.AttemptNumber),
		InferenceModelID:      event.InferenceModelID,
		ProviderID:            event.ProviderID,
		ProviderModelID:       event.ProviderModelID,
		API:                   event.API,
		Status:                proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_IN_FLIGHT,
		RequestTraceID:        r.traceID(ctx),
		ActorUserID:           r.actorUserID(ctx),
		SanitizedContextJSON:  r.sanitizeContext(scope, event.Context),
		StartedAt:             &startedAt,
	}

	if !event.DeadlineAt.IsZero() {
		attempt.DeadlineAt = new(event.DeadlineAt)
	}

	// Capture the value-only context and the correlation fields synchronously; the request context
	// is canceled once the response is returned, but its values remain readable for logging.
	base := detachedRecorderContext(ctx)
	enqueued := r.enqueue(recorderJob{
		scope:   scope,
		base:    base,
		attempt: attempt,
		kind:    recorderWriteStart,
	})
	if !enqueued {
		r.recorderFailed(scope, recorderStageStartDropped, nil, "recorder queue full")
	}
}

// AttemptFinished builds the terminal row and dispatches its write to the worker pool. It upserts on
// (invocation_id, attempt_number) so the finish write is correct whether or not the start row landed,
// and is idempotent under concurrent or repeated finalization. A finalize that is dropped (queue
// full) can leave a start row in flight until the deadline sweep, so its drop is metered under a distinct stage.
func (r *AttemptRecorder) AttemptFinished(ctx context.Context, event AttemptFinishEvent) {
	_, ok := r.registry.Lookup(event.Operation)
	if !ok {
		scope := r.diagnosticScope(ctx, event.Operation, event.InvocationID, event.AttemptNumber, nil)
		r.recorderFailed(scope, recorderStageFinalize, nil, "operation not registered")
		return
	}
	scope := r.diagnosticScope(ctx, event.Operation, event.InvocationID, event.AttemptNumber, event.Context)

	startedAt := event.StartedAt
	endedAt := event.EndedAt
	attempt := &cpm.InferenceAttempt{
		EffectiveSettingsJSON: append([]byte(nil), event.EffectiveSettingsJSON...),
		Operation:             int32(event.Operation),
		InvocationID:          event.InvocationID,
		OrganizationID:        new(event.OrganizationID),
		RuntimePurpose:        event.RuntimePurpose,
		AttemptNumber:         int32(event.AttemptNumber),
		InferenceModelID:      event.InferenceModelID,
		ProviderID:            event.ProviderID,
		ProviderModelID:       event.ProviderModelID,
		API:                   event.API,
		Status:                event.Status,
		Retryable:             event.Retryable,
		RetryReason:           event.RetryReason,
		RequestTraceID:        r.traceID(ctx),
		ActorUserID:           r.actorUserID(ctx),
		SanitizedContextJSON:  r.sanitizeContext(scope, event.Context),
		StartedAt:             &startedAt,
		EndedAt:               &endedAt,
		LatencyMs:             event.Latency().Milliseconds(),
	}
	r.applyErrorFields(attempt, event, scope)
	r.applyUsageFields(attempt, event.Result)

	base := detachedRecorderContext(ctx)
	enqueued := r.enqueue(recorderJob{
		scope:   scope,
		base:    base,
		attempt: attempt,
		kind:    recorderWriteFinalize,
	})
	if !enqueued {
		r.recorderFailed(scope, recorderStageFinalizeDropped, nil, "recorder queue full")
	}
}

// RetryDecided, Throttled, and BackoffScheduled are handled by the metrics observer; the recorder
// only persists the attempt start/finish rows.
func (r *AttemptRecorder) RetryDecided(context.Context, RetryDecisionEvent) {}
func (r *AttemptRecorder) Throttled(context.Context, ThrottleEvent)         {}
func (r *AttemptRecorder) BackoffScheduled(context.Context, BackoffEvent)   {}

// applyErrorFields records a bounded classification and a length-capped copy of the provider error
// text for a failed attempt. The class is a stable low-cardinality label. The message is truncated
// but NOT content-sanitized — it can carry provider response fragments — a deliberate trade-off:
// content redaction would put pattern matching on every failure path, and the attempt ledger is
// admin-only. The raw error is also logged here so full diagnostics survive the truncation.
// Intermediate retryable attempts are Warn; the attempt that ends the logical invocation is Error.
func (r *AttemptRecorder) applyErrorFields(attempt *cpm.InferenceAttempt, event AttemptFinishEvent, scope recorderDiagnosticScope) {
	if event.Err == nil {
		return
	}
	level := zap.WarnLevel
	if !event.Retryable || event.Committed || event.MaxAttempts <= 0 || event.AttemptNumber >= event.MaxAttempts {
		level = zap.ErrorLevel
	}
	scope.logger.Log(level, "control plane: provider attempt failed",
		zap.Int(appLogger.KeyMaxAttempts, event.MaxAttempts),
		zap.Uint64(appLogger.KeyInferenceModelID, event.InferenceModelID),
		zap.String(appLogger.KeyProvider, event.ProviderID),
		zap.String(appLogger.KeyProviderModelID, event.ProviderModelID),
		zap.Int64(appLogger.KeyOrganizationID, int64(event.OrganizationID)),
		zap.Stringer(appLogger.KeyOutcome, event.Status),
		zap.Stringer(logkeys.Reason, event.RetryReason),
		zap.Bool(appLogger.KeyRetryable, event.Retryable),
		zap.Error(event.Err),
	)
	attempt.ErrorClass = retryReasonClass(event.RetryReason)
	attempt.ErrorMessage = truncateString(event.Err.Error(), r.maxErrorBytes)
}

// applyUsageFields copies numeric usage counters from the attempt result. Only counts are stored;
// prompts, completions, reasoning, and raw responses are never persisted.
func (r *AttemptRecorder) applyUsageFields(attempt *cpm.InferenceAttempt, result AttemptResult) {
	attempt.TotalTokens = result.TokenCount
	if result.Outcome == nil {
		return
	}
	attempt.TokenSource = proto_models.InferenceTokenSource_INFERENCE_TOKEN_SOURCE_PROVIDER_REPORTED
	attempt.InputTokens = int64(result.Outcome.InputTokens)
	attempt.InputImages = int64(result.Outcome.InputImages)
	attempt.OutputTokens = int64(result.Outcome.OutputTokens)
	if result.Outcome.FirstOutputAt != nil {
		first := *result.Outcome.FirstOutputAt
		attempt.FirstOutputAt = &first
	}
	attempt.CachedTokens = int64(result.Outcome.CachedContentTokens)
	attempt.ThoughtTokens = int64(result.Outcome.ThoughtsTokens)
}

// traceID reads the request trace id from the trusted request context.
func (r *AttemptRecorder) traceID(ctx context.Context) string {
	return scontext.GetTraceID(ctx)
}

// WithAttemptActorUserID binds trusted durable actor attribution for inference attempts.
// It takes precedence over the request user, without changing authentication or
// authorization context. Only pass IDs from trusted server-side state, never model
// output or caller-supplied metadata. Zero explicitly records an unattributed attempt.
func WithAttemptActorUserID(ctx context.Context, actorUserID common_service_models.UserIdType) context.Context {
	return common_context.WithAttemptActorUserID(ctx, actorUserID)
}

// actorUserID prefers explicit durable attribution, falling back to the request user.
func (r *AttemptRecorder) actorUserID(ctx context.Context) *uint64 {
	if id, ok := common_context.GetAttemptActorUserID(ctx); ok {
		if id == 0 {
			return nil
		}
		return &id
	}
	id, ok := scontext.GetUserID[common_service_models.UserIdType](ctx)
	if !ok || uint64(id) == 0 {
		return nil
	}
	actor := uint64(id)
	return &actor
}

// sanitizeContext reduces the caller's usage context to the operation's registered searchable keys
// and returns compact JSON, or nil when there is nothing to store or the payload exceeds the size
// cap. Only allowlisted keys are ever persisted; prompts and secrets carried under other keys are
// dropped.
func (r *AttemptRecorder) sanitizeContext(scope recorderDiagnosticScope, raw map[string]any) []byte {
	if len(raw) == 0 {
		return nil
	}
	registered, _ := r.registry.lookup(scope.operation)
	allowlist := registered.searchableMetadataKeys
	if len(allowlist) == 0 {
		return nil
	}
	filtered := make(map[string]any, min(len(raw), len(allowlist)))
	remaining := r.maxContextBytes - 2
	for _, key := range allowlist {
		value, exists := raw[key.name]
		if !exists {
			continue
		}
		field, cost, ok := safeRecorderMetadataField(key, value, remaining)
		if !ok {
			// Preserve the all-or-nothing persistence contract while rejecting before
			// invoking an encoder or walking arbitrary caller-owned object graphs.
			scope.logger.Error("control plane: failed to marshal sanitized attempt context",
				zap.String(logkeys.Invariant, "inference_searchable_metadata_is_json_serializable"),
				zap.String(logkeys.Stage, "sanitize_attempt_context"),
				zap.String(logkeys.Expected, "allowlisted metadata is bounded scalar JSON"),
				zap.String(logkeys.Actual, "metadata admission failed"),
				zap.String(logkeys.Fallback, "drop_sanitized_context"),
				zap.Error(fmt.Errorf("metadata %q of type %T exceeds the bounded scalar/flat-array contract", key.name, value)))
			return nil
		}
		// Admission accepts only primitives and owned flat arrays, and never calls
		// caller marshal methods or traverses an unbounded object graph.
		switch field.Type {
		case zapcore.StringType:
			filtered[key.name] = field.String
		case zapcore.BoolType:
			filtered[key.name] = field.Integer != 0
		case zapcore.Int64Type:
			filtered[key.name] = field.Integer
		case zapcore.Uint64Type:
			filtered[key.name] = uint64(field.Integer)
		case zapcore.Float64Type:
			filtered[key.name] = math.Float64frombits(uint64(field.Integer))
		default:
			filtered[key.name] = field.Interface
		}
		remaining -= cost
	}
	if len(filtered) == 0 {
		return nil
	}
	encoded, err := json.Marshal(filtered)
	if err != nil {
		scope.logger.Error("control plane: failed to marshal sanitized attempt context",
			zap.String(logkeys.Invariant, "inference_searchable_metadata_is_json_serializable"),
			zap.String(logkeys.Stage, "sanitize_attempt_context"),
			zap.String(logkeys.Expected, "allowlisted metadata is JSON serializable"),
			zap.String(logkeys.Actual, "JSON marshal failed"),
			zap.String(logkeys.Fallback, "drop_sanitized_context"),
			zap.Error(err),
		)
		return nil
	}
	if len(encoded) > r.maxContextBytes {
		scope.logger.Warn("control plane: sanitized attempt context exceeds size cap; dropping",
			zap.String(logkeys.Stage, "sanitize_attempt_context"),
			zap.Int(logkeys.Bytes, len(encoded)),
			zap.Int(appLogger.KeyCapacity, r.maxContextBytes),
			zap.String(logkeys.Fallback, "drop_sanitized_context"))
		return nil
	}
	return encoded
}

// detachedRecorderContext contains only copied correlation identity. Retaining
// the request parent with WithoutCancel would keep its arbitrary values alive
// throughout stalled queued writes.
func detachedRecorderContext(ctx context.Context) context.Context {
	base := scontext.WithTraceID[common_service_models.UserIdType, *common_service_models.User](context.Background(), scontext.GetTraceID(ctx))
	if id, ok := scontext.GetBuildID(ctx); ok {
		base = scontext.WithBuildID[common_service_models.UserIdType, *common_service_models.User](base, id)
	}
	if id, ok := scontext.GetConfigID(ctx); ok {
		base = scontext.WithConfigID[common_service_models.UserIdType, *common_service_models.User](base, id)
	}
	return base
}

// recorderFailed logs and meters a recorder failure without affecting generation. The invocation and
// trace ids are captured synchronously at event time so an asynchronous write failure or drop stays
// correlatable to its originating request even after the request context is gone.
func (r *AttemptRecorder) recorderFailed(scope recorderDiagnosticScope, stage string, err error, msg string) {
	scope.logger.Error("control plane: attempt recorder failure",
		zap.String(logkeys.Stage, stage),
		zap.String(appLogger.KeyDetail, msg),
		zap.String(appLogger.KeyOutcome, "attempt_record_lost"),
		zap.Error(err),
	)
	if r.metrics != nil {
		r.metrics.RecordInferenceRecorderFailure(stage)
	}
}

// diagnosticScope snapshots runtime/request identity and the operation's allowlisted caller
// metadata. Metadata values are admitted one at a time only when they can be safely represented as
// bounded JSON, so a rejected value cannot leak through zap's reflection fallback and does not
// suppress the remaining workflow identifiers.
func (r *AttemptRecorder) diagnosticScope(ctx context.Context, operation Operation, invocationID string, attemptNumber int, raw map[string]any) recorderDiagnosticScope {
	logger := appLogger.For(ctx, r.logger, recorderLoggerName)
	if logger == nil {
		logger = zap.NewNop()
	}
	registered, _ := r.registry.lookup(operation)
	keys := registered.diagnosticMetadataKeys
	capacity := 3 + min(len(raw), len(keys))
	fields := make([]zap.Field, 0, capacity)
	fields = append(fields,
		zap.Stringer(logkeys.Operation, operation),
		zap.String(appLogger.KeyInvocationID, invocationID),
		zap.Int(appLogger.KeyAttemptNumber, attemptNumber),
	)
	fields = appendDiagnosticMetadata(fields, keys, raw, r.maxContextBytes)
	return recorderDiagnosticScope{
		// Values were snapshotted above; defer JSON field encoding until an
		// actual diagnostic occurs. Most successful writes never use this logger.
		logger:    logger.WithLazy(fields...),
		operation: operation,
	}
}

// appendDiagnosticMetadata snapshots only registered, bounded scalar metadata.
func appendDiagnosticMetadata(fields []zap.Field, keys []recorderMetadataKey, raw map[string]any, maxBytes int) []zap.Field {
	if len(raw) == 0 || maxBytes <= 0 {
		return fields
	}
	// The conservative comma charged per field plus this byte covers both braces.
	metadataBytes := 1
	for _, key := range keys {
		value, ok := raw[key.name]
		if !ok {
			continue
		}
		field, cost, ok := safeRecorderMetadataField(key, value, maxBytes-metadataBytes)
		if ok {
			fields = append(fields, field)
			metadataBytes += cost
		}
	}
	return fields
}

// recorderMetadataKey caches the immutable portion of diagnostic admission.
// The byte cost includes the JSON-escaped key, colon, and a conservative comma.
type recorderMetadataKey struct {
	name  string
	bytes int
}

func compileRecorderMetadataKeys(keys []string) []recorderMetadataKey {
	return compileMetadataKeys(keys, true)
}

func compileSearchableMetadataKeys(keys []string) []recorderMetadataKey {
	return compileMetadataKeys(keys, false)
}

func compileMetadataKeys(keys []string, diagnostic bool) []recorderMetadataKey {
	compiled := make([]recorderMetadataKey, 0, len(keys))
	for _, key := range keys {
		if diagnostic && reservedRecorderDiagnosticKey(key) {
			continue
		}
		encoded, err := json.Marshal(key)
		if err == nil {
			compiled = append(compiled, recorderMetadataKey{name: key, bytes: len(encoded) + 2})
		}
	}
	return compiled
}

func reservedRecorderDiagnosticKey(key string) bool {
	switch key {
	case logkeys.TraceID, logkeys.BuildID, logkeys.ConfigID, logkeys.Operation,
		logkeys.UserID, appLogger.KeyInvocationID, appLogger.KeyAttemptNumber,
		appLogger.KeyMaxAttempts, appLogger.KeyInferenceModelID, appLogger.KeyProvider,
		appLogger.KeyProviderModelID, appLogger.KeyOrganizationID, appLogger.KeyOutcome,
		appLogger.KeyRetryable, logkeys.Reason,
		"event_name", "error_class", "latency_ms", "request_tokens_per_second", "generation_tokens_per_second", logkeys.Fallback:
		return true
	default:
		return false
	}
}

// safeRecorderMetadataField accepts only bounded scalar values and flat scalar arrays. It uses
// typed zap constructors for numbers, preserving uint64 identifiers exactly, and never invokes
// caller-provided marshal or String methods while constructing diagnostic fields.
func safeRecorderMetadataField(key recorderMetadataKey, value any, remainingBytes int) (zap.Field, int, bool) {
	// Account for the encoded key, colon, and a conservative comma as well as the value. This keeps
	// the aggregate scope within the configured JSON-size budget without maintaining encoder state.
	keyBytes := key.bytes
	if remainingBytes < keyBytes+1 {
		return zap.Field{}, 0, false
	}
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return zap.Field{}, 0, false
	}
	var field zap.Field
	var normalized any
	switch v.Kind() {
	case reflect.String:
		// The encoded value is never shorter than its raw UTF-8 bytes plus quotes. Reject large
		// strings before JSON escaping can allocate a larger temporary buffer.
		if v.Len()+2 > remainingBytes-keyBytes {
			return zap.Field{}, 0, false
		}
		normalized = v.String()
		field = zap.String(key.name, v.String())
	case reflect.Bool:
		normalized = v.Bool()
		field = zap.Bool(key.name, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		normalized = v.Int()
		field = zap.Int64(key.name, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		normalized = v.Uint()
		field = zap.Uint64(key.name, v.Uint())
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return zap.Field{}, 0, false
		}
		normalized = n
		field = zap.Float64(key.name, n)
	case reflect.Array, reflect.Slice:
		// A flat JSON array needs at least two brackets, one byte per element and one comma between
		// elements. Enforce that lower bound before allocating the normalized snapshot.
		minimumValueBytes := 2
		if v.Len() > 0 {
			valueBudget := remainingBytes - keyBytes
			if valueBudget <= 1 || v.Len() > (valueBudget-1)/2 {
				return zap.Field{}, 0, false
			}
			minimumValueBytes = 2*v.Len() + 1
		}
		if minimumValueBytes > remainingBytes-keyBytes {
			return zap.Field{}, 0, false
		}
		valueBudget := remainingBytes - keyBytes
		lowerBound := 2 // brackets
		for i := range v.Len() {
			if i > 0 {
				lowerBound++ // comma
			}
			item := v.Index(i)
			switch item.Kind() {
			case reflect.String:
				lowerBound += item.Len() + 2
			case reflect.Bool:
				if item.Bool() {
					lowerBound += 4
				} else {
					lowerBound += 5
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				lowerBound++
			default:
				return zap.Field{}, 0, false
			}
			if lowerBound > valueBudget {
				return zap.Field{}, 0, false
			}
		}
		items := make([]any, v.Len())
		for i := range v.Len() {
			item := v.Index(i)
			switch item.Kind() {
			case reflect.String:
				items[i] = item.String()
			case reflect.Bool:
				items[i] = item.Bool()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				items[i] = item.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				items[i] = item.Uint()
			case reflect.Float32, reflect.Float64:
				n := item.Float()
				if math.IsNaN(n) || math.IsInf(n, 0) {
					return zap.Field{}, 0, false
				}
				items[i] = n
			default:
				return zap.Field{}, 0, false
			}
		}
		normalized = items
		field = zap.Any(key.name, items)
	default:
		return zap.Field{}, 0, false
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return zap.Field{}, 0, false
	}
	cost := keyBytes + len(encoded)
	if cost > remainingBytes {
		return zap.Field{}, 0, false
	}
	return field, cost, true
}

// truncateString caps a string to at most maxBytes bytes without splitting a multi-byte rune.
func truncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	truncated := s[:maxBytes]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

// retryReasonClass renders a bounded, stable error-class label from a retry reason.
func retryReasonClass(reason proto_models.InferenceRetryReason) string {
	return trimEnumPrefix(reason.String(), "INFERENCE_RETRY_REASON_")
}
