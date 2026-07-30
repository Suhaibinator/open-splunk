package indexes

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	defaultDeletionPollInterval     = time.Second
	defaultDeletionRecoveryInterval = 30 * time.Second
	defaultDeletionRetryInitial     = time.Second
	defaultDeletionRetryMaximum     = 30 * time.Second
	defaultDeletionStepTimeout      = 2 * time.Minute
	maximumDeletionTenantIDBytes    = 255
)

// DeletionControl is the GORM-backed control-plane boundary used by the
// physical-deletion coordinator. Implementations must preserve the immutable,
// oldest-first contracts of control.DB.
type DeletionControl interface {
	NextIndexDeletionOperation(context.Context) (control.IndexDeletionOperation, error)
	GetIndexDeletionMutationAttempt(
		context.Context,
		string,
	) (control.IndexDeletionMutationAttempt, error)
	EnsureIndexDeletionMutationAttempt(
		context.Context,
		string,
		control.IndexDeletionMutationTarget,
	) (control.IndexDeletionMutationAttempt, error)
	CompleteIndexDataDeletion(
		context.Context,
		control.IndexDeletionMutationAttempt,
	) (control.IndexDataDeletionCompletion, error)
	GetIndexDataDeletionCompletion(
		context.Context,
		string,
	) (control.IndexDataDeletionCompletion, error)
}

// DeletionStore is the native ClickHouse boundary used by the coordinator.
// Every physical-table writer must share the same concrete Store owner.
type DeletionStore interface {
	IndexDataDeletionStatus(
		context.Context,
		clickhouse.IndexDataDeletionRequest,
	) (clickhouse.IndexDataDeletionProgress, error)
	WithWritesFrozen(
		context.Context,
		func(context.Context, clickhouse.FrozenWrites) error,
	) error
}

// IndexDataDeletionCoordinatorConfig bounds polling, recovery, retry, and each
// whole reconciliation step. An ambiguous terminal commit audit receives one
// fresh StepTimeout after the frozen step returns. Zero durations select
// conservative defaults.
type IndexDataDeletionCoordinatorConfig struct {
	// TenantID must equal the immutable tenant on every admitted operation.
	// Drift is rejected before the mutation-attempt read or any native call.
	TenantID         string
	PollInterval     time.Duration
	RecoveryInterval time.Duration
	RetryInitial     time.Duration
	RetryMaximum     time.Duration
	StepTimeout      time.Duration
	// OnError runs asynchronously. At most one callback runs at once; errors
	// observed while it is still running are coalesced. A blocked callback may
	// outlive Close, so callback-owned resources must not assume Close joins it.
	OnError func(error)
}

// IndexDataDeletionCoordinator serially reconciles the oldest outstanding
// physical-deletion operation. The zero value is not usable.
type IndexDataDeletionCoordinator struct {
	control          DeletionControl
	store            DeletionStore
	tenantID         string
	pollInterval     time.Duration
	recoveryInterval time.Duration
	retryInitial     time.Duration
	retryMaximum     time.Duration
	stepTimeout      time.Duration
	onError          func(error)

	workerContext context.Context
	cancelWorker  context.CancelFunc
	workerDone    chan struct{}
	wake          chan struct{}
	closed        atomic.Bool
	closeOnce     sync.Once

	callbackMu    sync.Mutex
	callbackAlive bool
}

type indexDataDeletionWork struct {
	operation control.IndexDeletionOperation
	attempt   *control.IndexDeletionMutationAttempt
}

type indexDataDeletionWait uint8

const (
	indexDataDeletionContinue indexDataDeletionWait = iota
	indexDataDeletionPoll
	indexDataDeletionRecover
)

type frozenDeletionResult struct {
	attempt        *control.IndexDeletionMutationAttempt
	resolvedTarget *control.IndexDeletionMutationTarget
	outcome        frozenDeletionOutcome
	completionErr  error
}

type frozenDeletionOutcome uint8

const (
	frozenDeletionNoOutcome frozenDeletionOutcome = iota
	frozenDeletionPending
	frozenDeletionCompleted
	frozenDeletionCompletionUncertain
	frozenDeletionOperationGone
)

// NewIndexDataDeletionCoordinator starts one immediate, oldest-first recovery
// scan. SQLite remains the durable wake source, so a lost Wake is recovered by
// RecoveryInterval or process restart.
func NewIndexDataDeletionCoordinator(
	controlPlane DeletionControl,
	store DeletionStore,
	config IndexDataDeletionCoordinatorConfig,
) (*IndexDataDeletionCoordinator, error) {
	if interfaceIsNil(controlPlane) {
		return nil, errors.New("index data deletion control plane is required")
	}
	if interfaceIsNil(store) {
		return nil, errors.New("index data deletion ClickHouse store is required")
	}
	if !validDeletionTenantID(config.TenantID) {
		return nil, errors.New("index data deletion tenant ID is invalid")
	}
	pollInterval, err := deletionDuration(
		config.PollInterval,
		defaultDeletionPollInterval,
		"poll interval",
	)
	if err != nil {
		return nil, err
	}
	recoveryInterval, err := deletionDuration(
		config.RecoveryInterval,
		defaultDeletionRecoveryInterval,
		"recovery interval",
	)
	if err != nil {
		return nil, err
	}
	retryInitial, err := deletionDuration(
		config.RetryInitial,
		defaultDeletionRetryInitial,
		"initial retry interval",
	)
	if err != nil {
		return nil, err
	}
	retryMaximum, err := deletionDuration(
		config.RetryMaximum,
		defaultDeletionRetryMaximum,
		"maximum retry interval",
	)
	if err != nil {
		return nil, err
	}
	if retryMaximum < retryInitial {
		return nil, errors.New(
			"index data deletion maximum retry interval must not be less than the initial interval",
		)
	}
	stepTimeout, err := deletionDuration(
		config.StepTimeout,
		defaultDeletionStepTimeout,
		"step timeout",
	)
	if err != nil {
		return nil, err
	}

	// The coordinator owns this cancellation function and invokes it exactly
	// once when shutdown starts.
	workerContext, cancelWorker := context.WithCancel(context.Background()) //nolint:gosec
	coordinator := &IndexDataDeletionCoordinator{
		control:          controlPlane,
		store:            store,
		tenantID:         strings.Clone(config.TenantID),
		pollInterval:     pollInterval,
		recoveryInterval: recoveryInterval,
		retryInitial:     retryInitial,
		retryMaximum:     retryMaximum,
		stepTimeout:      stepTimeout,
		onError:          config.OnError,
		workerContext:    workerContext,
		cancelWorker:     cancelWorker,
		workerDone:       make(chan struct{}),
		wake:             make(chan struct{}, 1),
	}
	go coordinator.run()
	return coordinator, nil
}

// Wake coalesces a best-effort request to check durable work promptly. It is
// nonblocking and remains safe while or after Close races with callers.
func (coordinator *IndexDataDeletionCoordinator) Wake() {
	if coordinator == nil || coordinator.closed.Load() {
		return
	}
	select {
	case coordinator.wake <- struct{}{}:
	default:
	}
}

// Close rejects future wakes, cancels the current dependency call, and waits
// for the sole worker. A caller deadline does not prevent a later caller from
// continuing to wait for the same shutdown.
func (coordinator *IndexDataDeletionCoordinator) Close(ctx context.Context) error {
	if coordinator == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("index data deletion coordinator close context is required")
	}
	coordinator.closeOnce.Do(func() {
		coordinator.closed.Store(true)
		coordinator.cancelWorker()
	})
	select {
	case <-coordinator.workerDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (coordinator *IndexDataDeletionCoordinator) run() {
	defer close(coordinator.workerDone)
	var current *indexDataDeletionWork
	retryDelay := coordinator.retryInitial
	for {
		stepContext, cancelStep := context.WithTimeout(
			coordinator.workerContext,
			coordinator.stepTimeout,
		)
		next, wait, err := coordinator.reconcileStep(stepContext, current)
		cancelStep()
		current = next

		if coordinator.workerContext.Err() != nil {
			return
		}
		if err != nil {
			coordinator.reportError(err)
			if !coordinator.wait(retryDelay, false) {
				return
			}
			retryDelay = nextDeletionRetry(retryDelay, coordinator.retryMaximum)
			continue
		}
		retryDelay = coordinator.retryInitial

		switch wait {
		case indexDataDeletionContinue:
			continue
		case indexDataDeletionPoll:
			if !coordinator.wait(coordinator.pollInterval, false) {
				return
			}
		case indexDataDeletionRecover:
			if !coordinator.wait(coordinator.recoveryInterval, true) {
				return
			}
		default:
			coordinator.reportError(errors.New(
				"index data deletion coordinator produced an invalid wait state",
			))
			if !coordinator.wait(retryDelay, false) {
				return
			}
		}
	}
}

func (coordinator *IndexDataDeletionCoordinator) reconcileStep(
	ctx context.Context,
	current *indexDataDeletionWork,
) (*indexDataDeletionWork, indexDataDeletionWait, error) {
	if current == nil {
		operation, err := coordinator.control.NextIndexDeletionOperation(ctx)
		if errors.Is(err, control.ErrNotFound) {
			return nil, indexDataDeletionRecover, nil
		}
		if err != nil {
			return nil, indexDataDeletionContinue, fmt.Errorf(
				"discover oldest index data deletion operation: %w",
				err,
			)
		}
		current = &indexDataDeletionWork{operation: operation}
	}
	if err := coordinator.validateOperation(current.operation); err != nil {
		return current, indexDataDeletionContinue, err
	}

	if current.attempt == nil {
		attempt, err := coordinator.control.GetIndexDeletionMutationAttempt(
			ctx,
			current.operation.ID,
		)
		switch {
		case err == nil:
			if err := coordinator.validateAttempt(current.operation, attempt); err != nil {
				return current, indexDataDeletionContinue, err
			}
			current.attempt = &attempt
		case errors.Is(err, control.ErrNotFound):
		default:
			return current, indexDataDeletionContinue, coordinator.operationError(
				current.operation,
				"read durable mutation attempt",
				err,
			)
		}
	}

	if current.attempt == nil {
		return coordinator.advanceFrozen(ctx, current)
	}

	request := deletionRequest(*current.attempt)
	progress, err := coordinator.store.IndexDataDeletionStatus(ctx, request)
	if err != nil {
		return current, indexDataDeletionContinue, coordinator.operationError(
			current.operation,
			"poll ClickHouse mutation",
			err,
		)
	}
	switch progress.State {
	case clickhouse.IndexDataDeletionPending:
		return current, indexDataDeletionPoll, nil
	case clickhouse.IndexDataDeletionReady:
		return coordinator.advanceFrozen(ctx, current)
	default:
		return current, indexDataDeletionContinue, coordinator.operationError(
			current.operation,
			"poll ClickHouse mutation",
			fmt.Errorf("unexpected progress state %d", progress.State),
		)
	}
}

func (coordinator *IndexDataDeletionCoordinator) advanceFrozen(
	ctx context.Context,
	current *indexDataDeletionWork,
) (*indexDataDeletionWork, indexDataDeletionWait, error) {
	result := frozenDeletionResult{}
	freezeErr := coordinator.store.WithWritesFrozen(
		ctx,
		func(callbackContext context.Context, frozen clickhouse.FrozenWrites) error {
			if err := frozen.DrainPending(callbackContext); err != nil {
				return coordinator.operationError(
					current.operation,
					"drain pending ClickHouse outbox",
					err,
				)
			}

			attempt := current.attempt
			if attempt == nil {
				target, err := frozen.IndexDataDeletionTarget(callbackContext)
				if err != nil {
					return coordinator.operationError(
						current.operation,
						"resolve ClickHouse deletion target",
						err,
					)
				}
				resolvedTarget := control.IndexDeletionMutationTarget{
					TenantID:  current.operation.TenantID,
					Database:  target.Database,
					Table:     target.Table,
					TableUUID: target.TableUUID,
				}
				result.resolvedTarget = &resolvedTarget
				ensured, err := coordinator.control.EnsureIndexDeletionMutationAttempt(
					callbackContext,
					current.operation.ID,
					resolvedTarget,
				)
				if err != nil {
					if errors.Is(err, control.ErrNotFound) {
						result.outcome = frozenDeletionOperationGone
						return nil
					}
					return coordinator.operationError(
						current.operation,
						"persist durable mutation attempt",
						err,
					)
				}
				if err := coordinator.validateAttempt(
					current.operation,
					ensured,
				); err != nil {
					return err
				}
				attempt = &ensured
				result.attempt = &ensured
			}

			progress, err := frozen.AdvanceIndexDataDeletion(
				callbackContext,
				deletionRequest(*attempt),
			)
			if err != nil {
				return coordinator.operationError(
					current.operation,
					"advance ClickHouse mutation",
					err,
				)
			}
			switch progress.State {
			case clickhouse.IndexDataDeletionPending:
				result.outcome = frozenDeletionPending
				return nil
			case clickhouse.IndexDataDeletionPhysicallyEmpty:
				completion, completionErr := coordinator.control.CompleteIndexDataDeletion(
					callbackContext,
					*attempt,
				)
				result.completionErr = completionErr
				if completionErr == nil &&
					completionMatchesWork(
						completion,
						current.operation,
						*attempt,
					) {
					result.outcome = frozenDeletionCompleted
					return nil
				}
				if completionErr == nil {
					result.completionErr = errors.New(
						"terminal completion does not match the proven operation and mutation attempt",
					)
				}
				result.outcome = frozenDeletionCompletionUncertain
				return coordinator.operationError(
					current.operation,
					"commit terminal control-plane state",
					result.completionErr,
				)
			default:
				return coordinator.operationError(
					current.operation,
					"advance ClickHouse mutation",
					fmt.Errorf("unexpected progress state %d", progress.State),
				)
			}
		},
	)

	if result.attempt != nil {
		current.attempt = result.attempt
	}
	switch result.outcome {
	case frozenDeletionOperationGone:
		completion, completionErr := coordinator.readTerminalAudit(
			current.operation.ID,
		)
		if completionErr == nil &&
			result.resolvedTarget != nil &&
			completionMatchesOperation(
				completion,
				current.operation,
				*result.resolvedTarget,
			) && freezeErr == nil {
			return nil, indexDataDeletionContinue, nil
		}
		if completionErr == nil && freezeErr != nil {
			return current, indexDataDeletionContinue, freezeErr
		}
		if completionErr == nil {
			completionErr = errors.New(
				"terminal audit does not match the concurrently completed operation",
			)
		}
		return current, indexDataDeletionContinue, errors.Join(
			freezeErr,
			coordinator.operationError(
				current.operation,
				"verify concurrently completed operation",
				completionErr,
			),
		)
	case frozenDeletionCompleted:
		if freezeErr != nil {
			return current, indexDataDeletionContinue, freezeErr
		}
		return nil, indexDataDeletionContinue, nil
	case frozenDeletionCompletionUncertain:
		completion, completionErr := coordinator.readTerminalAudit(
			current.operation.ID,
		)
		if completionErr == nil &&
			current.attempt != nil &&
			completionMatchesWork(
				completion,
				current.operation,
				*current.attempt,
			) {
			return nil, indexDataDeletionContinue, nil
		}
		if completionErr == nil {
			completionErr = errors.New(
				"terminal audit does not match the proven mutation attempt",
			)
		}
		return current, indexDataDeletionContinue, errors.Join(
			freezeErr,
			coordinator.operationError(
				current.operation,
				"resolve terminal control-plane commit",
				completionErr,
			),
		)
	case frozenDeletionPending:
		if freezeErr != nil {
			return current, indexDataDeletionContinue, freezeErr
		}
		return current, indexDataDeletionPoll, nil
	case frozenDeletionNoOutcome:
		if freezeErr != nil {
			return current, indexDataDeletionContinue, freezeErr
		}
		return current, indexDataDeletionContinue, coordinator.operationError(
			current.operation,
			"advance ClickHouse mutation",
			errors.New("frozen advancement produced no outcome"),
		)
	default:
		return current, indexDataDeletionContinue, coordinator.operationError(
			current.operation,
			"advance ClickHouse mutation",
			fmt.Errorf("frozen advancement produced invalid outcome %d", result.outcome),
		)
	}
}

func (coordinator *IndexDataDeletionCoordinator) readTerminalAudit(
	operationID string,
) (control.IndexDataDeletionCompletion, error) {
	auditContext, cancelAudit := context.WithTimeout(
		coordinator.workerContext,
		coordinator.stepTimeout,
	)
	defer cancelAudit()
	return coordinator.control.GetIndexDataDeletionCompletion(
		auditContext,
		operationID,
	)
}

func (coordinator *IndexDataDeletionCoordinator) validateAttempt(
	operation control.IndexDeletionOperation,
	attempt control.IndexDeletionMutationAttempt,
) error {
	if attempt.DeletionOperationID != operation.ID ||
		attempt.IndexID != operation.IndexID ||
		attempt.IndexName != operation.IndexName {
		return coordinator.operationError(
			operation,
			"validate durable mutation attempt",
			errors.New("attempt does not match the oldest deletion operation"),
		)
	}
	if attempt.Target.TenantID != operation.TenantID {
		return coordinator.operationError(
			operation,
			"validate durable mutation attempt",
			fmt.Errorf(
				"attempt tenant %q does not match operation tenant %q",
				attempt.Target.TenantID,
				operation.TenantID,
			),
		)
	}
	return nil
}

func (coordinator *IndexDataDeletionCoordinator) validateOperation(
	operation control.IndexDeletionOperation,
) error {
	if operation.TenantID == coordinator.tenantID {
		return nil
	}
	return coordinator.operationError(
		operation,
		"validate durable deletion operation",
		fmt.Errorf(
			"operation tenant %q does not match configured tenant %q",
			operation.TenantID,
			coordinator.tenantID,
		),
	)
}

func (coordinator *IndexDataDeletionCoordinator) operationError(
	operation control.IndexDeletionOperation,
	stage string,
	err error,
) error {
	return fmt.Errorf(
		"coordinate index data deletion operation %q for index %q (%q): %s: %w",
		operation.ID,
		operation.IndexID,
		operation.IndexName,
		stage,
		err,
	)
}

func (coordinator *IndexDataDeletionCoordinator) wait(
	duration time.Duration,
	interruptible bool,
) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	if !interruptible {
		select {
		case <-coordinator.workerContext.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	select {
	case <-coordinator.workerContext.Done():
		return false
	case <-timer.C:
		return true
	case <-coordinator.wake:
		return true
	}
}

func (coordinator *IndexDataDeletionCoordinator) reportError(err error) {
	if err == nil || coordinator.onError == nil ||
		coordinator.workerContext.Err() != nil {
		return
	}
	coordinator.callbackMu.Lock()
	if coordinator.callbackAlive {
		coordinator.callbackMu.Unlock()
		return
	}
	coordinator.callbackAlive = true
	coordinator.callbackMu.Unlock()
	go func() {
		defer func() {
			_ = recover()
			coordinator.callbackMu.Lock()
			coordinator.callbackAlive = false
			coordinator.callbackMu.Unlock()
		}()
		coordinator.onError(err)
	}()
}

func deletionRequest(
	attempt control.IndexDeletionMutationAttempt,
) clickhouse.IndexDataDeletionRequest {
	return clickhouse.IndexDataDeletionRequest{
		OperationID:     attempt.DeletionOperationID,
		CorrelationID:   attempt.CorrelationID,
		TenantID:        attempt.Target.TenantID,
		IndexName:       attempt.IndexName,
		Database:        attempt.Target.Database,
		Table:           attempt.Target.Table,
		TableUUID:       attempt.Target.TableUUID,
		ProtocolVersion: attempt.ProtocolVersion,
	}
}

func completionMatchesWork(
	completion control.IndexDataDeletionCompletion,
	operation control.IndexDeletionOperation,
	attempt control.IndexDeletionMutationAttempt,
) bool {
	return control.IndexDataDeletionCompletionMatchesAttempt(
		completion,
		attempt,
	) &&
		completion.Target.TenantID == operation.TenantID &&
		completion.ArchivedVersion == operation.ArchivedVersion &&
		completion.DeletedVersion == operation.DeletingVersion &&
		completion.OperationCreatedAt.Equal(operation.CreatedAt)
}

func completionMatchesOperation(
	completion control.IndexDataDeletionCompletion,
	operation control.IndexDeletionOperation,
	target control.IndexDeletionMutationTarget,
) bool {
	return completion.DeletionOperationID == operation.ID &&
		completion.IndexID == operation.IndexID &&
		completion.IndexName == operation.IndexName &&
		completion.ArchivedVersion == operation.ArchivedVersion &&
		completion.DeletedVersion == operation.DeletingVersion &&
		completion.OperationCreatedAt.Equal(operation.CreatedAt) &&
		completion.Target.TenantID == operation.TenantID &&
		completion.Target == target &&
		completion.ProtocolVersion ==
			control.IndexDeletionMutationProtocolVersion
}

func deletionDuration(
	configured time.Duration,
	fallback time.Duration,
	name string,
) (time.Duration, error) {
	if configured < 0 {
		return 0, fmt.Errorf("index data deletion %s must not be negative", name)
	}
	if configured == 0 {
		return fallback, nil
	}
	return configured, nil
}

func nextDeletionRetry(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum-current {
		return maximum
	}
	return current * 2
}

func validDeletionTenantID(value string) bool {
	if value == "" || len(value) > maximumDeletionTenantIDBytes ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value ||
		strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func interfaceIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
