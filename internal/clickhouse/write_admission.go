package clickhouse

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrStoreClosed means Store shutdown has begun and no new operation can
	// safely use its ClickHouse connection or visibility owner.
	ErrStoreClosed = errors.New("ClickHouse store is closed")
	// ErrWriteFreezeInactive means a callback-scoped frozen-write capability
	// escaped the callback that owned the exclusive admission lease.
	ErrWriteFreezeInactive = errors.New("ClickHouse write freeze is inactive")
	// ErrWriteFreezeReentrant means a frozen-write callback tried to enter an
	// ordinary writer or acquire another exclusive freeze with its scoped
	// callback context.
	ErrWriteFreezeReentrant = errors.New("ClickHouse write freeze callback cannot enter a normal writer")
	// ErrPendingOutboxNotDrained means an exclusive drain could not prove that
	// every durable replay reservation reached a terminal state.
	ErrPendingOutboxNotDrained = errors.New("ClickHouse pending outbox is not drained")
	// ErrWriteFreezeNotDrained means physical maintenance was attempted before
	// the callback-scoped freeze proved that the durable replay outbox was
	// empty.
	ErrWriteFreezeNotDrained = errors.New("ClickHouse write freeze has not drained the pending outbox")
)

// FrozenWrites is valid only during the callback passed to WithWritesFrozen.
// It is the sole path allowed to replay the durable outbox while ordinary
// Store, ResumeBatch, and reconciliation writers are excluded.
type FrozenWrites interface {
	DrainPending(context.Context) error
	IndexDataDeletionTarget(context.Context) (IndexDataDeletionTarget, error)
	AdvanceIndexDataDeletion(
		context.Context,
		IndexDataDeletionRequest,
	) (IndexDataDeletionProgress, error)
}

// writeAdmission is a writer-preferring shared/exclusive gate. Ordinary
// writers may proceed concurrently until a freeze queues. Once queued, the
// oldest freeze prevents later writers from bypassing it.
type writeAdmission struct {
	mu      sync.Mutex
	changed chan struct{}
	active  uint64
	frozen  bool
	closed  bool
	waiters list.List
}

type writeFreezeWaiter struct {
	element *list.Element
}

func newWriteAdmission() *writeAdmission {
	return &writeAdmission{changed: make(chan struct{})}
}

func (admission *writeAdmission) enter(ctx context.Context) error {
	if ctx == nil {
		return errors.New("acquire ClickHouse write admission: context is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		admission.mu.Lock()
		if admission.closed {
			admission.mu.Unlock()
			return ErrStoreClosed
		}
		if !admission.frozen && admission.waiters.Len() == 0 {
			admission.active++
			admission.mu.Unlock()
			return nil
		}
		changed := admission.changed
		admission.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (admission *writeAdmission) leave() {
	admission.mu.Lock()
	if admission.active == 0 {
		admission.mu.Unlock()
		panic("release ClickHouse write admission without an active writer")
	}
	admission.active--
	if admission.active == 0 && admission.waiters.Len() > 0 {
		admission.notifyLocked()
	}
	admission.mu.Unlock()
}

func (admission *writeAdmission) freeze(ctx context.Context) error {
	if ctx == nil {
		return errors.New("freeze ClickHouse writes: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	waiter := &writeFreezeWaiter{}
	admission.mu.Lock()
	if admission.closed {
		admission.mu.Unlock()
		return ErrStoreClosed
	}
	waiter.element = admission.waiters.PushBack(waiter)
	for {
		if admission.closed {
			admission.removeWaiterLocked(waiter)
			admission.mu.Unlock()
			return ErrStoreClosed
		}
		if err := ctx.Err(); err != nil {
			wasHead := admission.removeWaiterLocked(waiter)
			if wasHead {
				admission.notifyLocked()
			}
			admission.mu.Unlock()
			return err
		}
		if admission.waiters.Front() == waiter.element &&
			admission.active == 0 && !admission.frozen {
			admission.removeWaiterLocked(waiter)
			admission.frozen = true
			admission.mu.Unlock()
			return nil
		}
		changed := admission.changed
		admission.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		admission.mu.Lock()
	}
}

func (admission *writeAdmission) releaseFreeze() {
	admission.mu.Lock()
	if !admission.frozen {
		admission.mu.Unlock()
		panic("release ClickHouse write freeze without an active freeze")
	}
	admission.frozen = false
	admission.notifyLocked()
	admission.mu.Unlock()
}

func (admission *writeAdmission) close() {
	admission.mu.Lock()
	if admission.closed {
		admission.mu.Unlock()
		return
	}
	admission.closed = true
	admission.notifyLocked()
	admission.mu.Unlock()
}

func (admission *writeAdmission) waitingFreezes() int {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.waiters.Len()
}

func (admission *writeAdmission) removeWaiterLocked(waiter *writeFreezeWaiter) bool {
	if waiter.element == nil {
		return false
	}
	wasHead := admission.waiters.Front() == waiter.element
	admission.waiters.Remove(waiter.element)
	waiter.element = nil
	return wasHead
}

func (admission *writeAdmission) notifyLocked() {
	close(admission.changed)
	admission.changed = make(chan struct{})
}

type frozenWrites struct {
	store *Store
	scope context.Context

	mu        sync.Mutex
	valid     bool
	drained   bool
	active    sync.WaitGroup
	drainSlot chan struct{}
}

var _ FrozenWrites = (*frozenWrites)(nil)

func (frozen *frozenWrites) DrainPending(ctx context.Context) error {
	drainContext, finish, err := frozen.beginPrivilegedOperation(
		ctx,
		"drain frozen ClickHouse outbox",
		false,
	)
	if err != nil {
		return err
	}
	defer finish()

	frozen.mu.Lock()
	frozen.drained = false
	frozen.mu.Unlock()
	err = frozen.store.reconcilePending(drainContext, true)
	if err == nil {
		frozen.mu.Lock()
		frozen.drained = true
		frozen.mu.Unlock()
	}
	return frozen.store.operationError(ctx, err)
}

func (frozen *frozenWrites) beginPrivilegedOperation(
	ctx context.Context,
	action string,
	requireDrained bool,
) (context.Context, func(), error) {
	if frozen == nil || frozen.store == nil {
		return nil, nil, ErrWriteFreezeInactive
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%s: context is required", action)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	frozen.mu.Lock()
	if !frozen.valid {
		frozen.mu.Unlock()
		return nil, nil, ErrWriteFreezeInactive
	}
	frozen.active.Add(1)
	frozen.mu.Unlock()

	operationContext, cancelOperation := context.WithCancel(ctx)
	stopScopeCancellation := context.AfterFunc(
		frozen.scope,
		cancelOperation,
	)
	select {
	case <-operationContext.Done():
		stopScopeCancellation()
		cancelOperation()
		frozen.active.Done()
		return nil, nil, frozen.store.operationError(
			ctx,
			operationContext.Err(),
		)
	case <-frozen.drainSlot:
	}
	frozen.mu.Lock()
	valid := frozen.valid
	drained := frozen.drained
	frozen.mu.Unlock()
	if !valid || requireDrained && !drained {
		frozen.drainSlot <- struct{}{}
		stopScopeCancellation()
		cancelOperation()
		frozen.active.Done()
		if !valid {
			return nil, nil, ErrWriteFreezeInactive
		}
		return nil, nil, ErrWriteFreezeNotDrained
	}
	finish := func() {
		frozen.drainSlot <- struct{}{}
		stopScopeCancellation()
		cancelOperation()
		frozen.active.Done()
	}
	return operationContext, finish, nil
}

func (frozen *frozenWrites) invalidate() {
	frozen.mu.Lock()
	frozen.valid = false
	frozen.mu.Unlock()
	frozen.active.Wait()
}

// WithWritesFrozen runs callback while every ClickHouse writer and durable
// replay path on this Store is excluded. All writers for the physical events
// table must use this Store while the callback runs; production satisfies that
// invariant with its single Store owner.
//
// The callback must propagate its supplied context. Store, ResumeBatch,
// ReconcilePending, and nested WithWritesFrozen calls made with that context
// fail with ErrWriteFreezeReentrant; DrainPending is the privileged replay
// path. The callback must not call Close.
func (s *Store) WithWritesFrozen(
	ctx context.Context,
	callback func(context.Context, FrozenWrites) error,
) (resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return ErrWriteFreezeReentrant
	}
	if callback == nil {
		return errors.New("freeze ClickHouse writes: callback is required")
	}
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return err
	}
	defer finishOperation()

	if err := s.writeAdmission.freeze(operationContext); err != nil {
		return err
	}
	callbackContext, cancelCallback := context.WithCancel(operationContext)
	callbackContext = context.WithValue(
		callbackContext,
		frozenCallbackContextKey{store: s},
		true,
	)
	drainSlot := make(chan struct{}, 1)
	drainSlot <- struct{}{}
	capability := &frozenWrites{
		store:     s,
		scope:     callbackContext,
		valid:     true,
		drainSlot: drainSlot,
	}
	defer func() {
		cancelCallback()
		capability.invalidate()
		s.writeAdmission.releaseFreeze()
		s.wakeReconciler()
	}()
	return callback(callbackContext, capability)
}

func (s *Store) beginOperation(
	ctx context.Context,
	resultErr *error,
) (context.Context, func(), error) {
	if s == nil {
		return nil, nil, ErrStoreClosed
	}
	if ctx == nil {
		return nil, nil, errors.New("use ClickHouse store: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.lifecycleMu.Lock()
	if s.closed || s.lifecycleContext == nil || s.writeAdmission == nil {
		s.lifecycleMu.Unlock()
		return nil, nil, ErrStoreClosed
	}
	lifecycleContext := s.lifecycleContext
	s.operations.Add(1)
	s.lifecycleMu.Unlock()

	operationContext, cancelOperation := context.WithCancel(ctx)
	stopLifecycleCancellation := context.AfterFunc(lifecycleContext, cancelOperation)
	finish := func() {
		stopLifecycleCancellation()
		cancelOperation()
		s.operations.Done()
		*resultErr = s.operationError(ctx, *resultErr)
	}
	return operationContext, finish, nil
}

type frozenCallbackContextKey struct {
	store *Store
}

func frozenCallbackActive(ctx context.Context, store *Store) bool {
	return ctx != nil && store != nil &&
		ctx.Value(frozenCallbackContextKey{store: store}) != nil
}

func (s *Store) operationError(callerContext context.Context, err error) error {
	if err == nil || (!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)) {
		return err
	}
	if callerContext != nil {
		if callerErr := callerContext.Err(); callerErr != nil {
			return callerErr
		}
	}
	s.lifecycleMu.Lock()
	closed := s.closed
	s.lifecycleMu.Unlock()
	if closed {
		return ErrStoreClosed
	}
	return err
}

func pendingOutboxError(reservations uint32, outboxBytes uint64) error {
	return fmt.Errorf(
		"%w: %d reservations retain %d bytes",
		ErrPendingOutboxNotDrained,
		reservations,
		outboxBytes,
	)
}
