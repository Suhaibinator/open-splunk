package indexes

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const coordinatorTestTimeout = 5 * time.Second

var errCoordinatorTestPrecommit = errors.New("test completion failed before commit")

func TestIndexDataDeletionCoordinatorCompletesFreshAttemptInsideFrozenSequence(
	t *testing.T,
) {
	operation, attempt, completion, target := coordinatorTestRecords("fresh")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var completionInsideFreeze atomic.Bool
	var ensuredTarget control.IndexDeletionMutationTarget
	var advancedRequest clickhouse.IndexDataDeletionRequest

	store := &coordinatorTestStore{
		recorder: recorder,
		target: clickhouse.IndexDataDeletionTarget{
			Database:  target.Database,
			Table:     target.Table,
			TableUUID: target.TableUUID,
		},
		advanceProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionPhysicallyEmpty,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
		},
		ensureAttempt: func(
			_ context.Context,
			_ string,
			got control.IndexDeletionMutationTarget,
		) (control.IndexDeletionMutationAttempt, error) {
			ensuredTarget = got
			return attempt, nil
		},
		complete: func(
			context.Context,
			control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			completionInsideFreeze.Store(store.isFrozen())
			return completion, nil
		},
	}
	store.advance = func(
		_ context.Context,
		request clickhouse.IndexDataDeletionRequest,
	) (clickhouse.IndexDataDeletionProgress, error) {
		advancedRequest = request
		return store.advanceProgress, nil
	}

	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "terminal completion and next-operation discovery")
	closeCoordinatorTest(t, coordinator)

	wantEvents := []string{
		"control.next",
		"control.get-attempt",
		"store.freeze.enter",
		"frozen.drain",
		"frozen.target",
		"control.ensure-attempt",
		"frozen.advance",
		"control.complete",
		"store.freeze.exit",
		"control.next",
	}
	if got := recorder.snapshot(); !slices.Equal(got, wantEvents) {
		t.Fatalf("coordinator call order = %v, want %v", got, wantEvents)
	}
	if !completionInsideFreeze.Load() {
		t.Fatal("CompleteIndexDataDeletion ran outside WithWritesFrozen callback")
	}
	if ensuredTarget != target {
		t.Fatalf("ensured target = %#v, want %#v", ensuredTarget, target)
	}
	if want := coordinatorTestRequest(attempt); advancedRequest != want {
		t.Fatalf("advance request = %#v, want %#v", advancedRequest, want)
	}
}

func TestIndexDataDeletionCoordinatorPollsExistingAttemptOutsideFreeze(
	t *testing.T,
) {
	operation, attempt, completion, _ := coordinatorTestRecords("pending")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var statusCalls atomic.Int32
	var statusInsideFreeze atomic.Bool
	firstPending := make(chan struct{})
	store := &coordinatorTestStore{
		recorder: recorder,
		advanceProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionPhysicallyEmpty,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
		complete: func(
			context.Context,
			control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			if !store.isFrozen() {
				return control.IndexDataDeletionCompletion{},
					errors.New("completion observed outside freeze")
			}
			return completion, nil
		},
	}
	store.status = func(
		context.Context,
		clickhouse.IndexDataDeletionRequest,
	) (clickhouse.IndexDataDeletionProgress, error) {
		statusInsideFreeze.Store(store.isFrozen())
		if statusInsideFreeze.Load() {
			return clickhouse.IndexDataDeletionProgress{},
				errors.New("status observed inside freeze")
		}
		if statusCalls.Add(1) == 1 {
			close(firstPending)
			return clickhouse.IndexDataDeletionProgress{
				State: clickhouse.IndexDataDeletionPending,
			}, nil
		}
		return clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionReady,
		}, nil
	}

	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		func() IndexDataDeletionCoordinatorConfig {
			config := coordinatorTestConfig()
			config.PollInterval = time.Millisecond
			return config
		}(),
	)
	waitForCoordinatorTestSignal(t, firstPending, "first pending status")
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "completion after the fixed pending poll interval")
	closeCoordinatorTest(t, coordinator)

	wantEvents := []string{
		"control.next",
		"control.get-attempt",
		"store.status",
		"store.status",
		"store.freeze.enter",
		"frozen.drain",
		"frozen.advance",
		"control.complete",
		"store.freeze.exit",
		"control.next",
	}
	if got := recorder.snapshot(); !slices.Equal(got, wantEvents) {
		t.Fatalf("coordinator call order = %v, want %v", got, wantEvents)
	}
	if statusInsideFreeze.Load() {
		t.Fatal("IndexDataDeletionStatus ran while writes were frozen")
	}
}

func TestIndexDataDeletionCoordinatorReprovesZeroAfterPrecommitFailure(
	t *testing.T,
) {
	operation, attempt, completion, _ := coordinatorTestRecords("reproof")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var completeCalls atomic.Int32
	var completionOutsideFreeze atomic.Bool
	reported := make(chan error, 4)
	store := &coordinatorTestStore{
		recorder: recorder,
		statusProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionReady,
		},
		advanceProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionPhysicallyEmpty,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
		complete: func(
			context.Context,
			control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			if !store.isFrozen() {
				completionOutsideFreeze.Store(true)
			}
			if completeCalls.Add(1) == 1 {
				return control.IndexDataDeletionCompletion{},
					errCoordinatorTestPrecommit
			}
			return completion, nil
		},
		getCompletion: func(
			context.Context,
			string,
		) (control.IndexDataDeletionCompletion, error) {
			if store.isFrozen() {
				return control.IndexDataDeletionCompletion{},
					errors.New("completion lookup observed inside freeze")
			}
			return control.IndexDataDeletionCompletion{}, control.ErrNotFound
		},
	}
	config := coordinatorTestConfig()
	config.RetryInitial = time.Millisecond
	config.RetryMaximum = time.Millisecond
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)

	select {
	case err := <-reported:
		if !errors.Is(err, errCoordinatorTestPrecommit) {
			t.Fatalf("reported error = %v, want precommit failure", err)
		}
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("coordinator did not report precommit failure")
	}
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "completion after a fresh frozen zero proof")
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	if completionOutsideFreeze.Load() {
		t.Fatal("CompleteIndexDataDeletion ran outside the frozen callback")
	}
	assertCoordinatorTestCount(t, events, "store.freeze.enter", 2)
	assertCoordinatorTestCount(t, events, "frozen.drain", 2)
	assertCoordinatorTestCount(t, events, "frozen.advance", 2)
	assertCoordinatorTestCount(t, events, "control.complete", 2)
	assertCoordinatorTestCount(t, events, "control.get-completion", 1)
	assertCoordinatorTestSubsequence(t, events, []string{
		"control.complete",
		"store.freeze.exit",
		"control.get-completion",
		"store.freeze.enter",
		"frozen.drain",
		"frozen.advance",
		"control.complete",
	})
}

func TestIndexDataDeletionCoordinatorResolvesCommitThenErrorByExactAudit(
	t *testing.T,
) {
	operation, attempt, completion, _ := coordinatorTestRecords("ambiguous")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var completionLookupInsideFreeze atomic.Bool
	reported := make(chan error, 1)
	store := &coordinatorTestStore{
		recorder: recorder,
		statusProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionReady,
		},
		advanceProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionPhysicallyEmpty,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
		complete: func(
			context.Context,
			control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			if !store.isFrozen() {
				return control.IndexDataDeletionCompletion{},
					errors.New("completion observed outside freeze")
			}
			return control.IndexDataDeletionCompletion{}, io.ErrUnexpectedEOF
		},
		getCompletion: func(
			context.Context,
			string,
		) (control.IndexDataDeletionCompletion, error) {
			completionLookupInsideFreeze.Store(store.isFrozen())
			return completion, nil
		},
	}
	config := coordinatorTestConfig()
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "ambiguous commit resolution")
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	assertCoordinatorTestCount(t, events, "store.freeze.enter", 1)
	assertCoordinatorTestCount(t, events, "control.complete", 1)
	assertCoordinatorTestCount(t, events, "control.get-completion", 1)
	assertCoordinatorTestSubsequence(t, events, []string{
		"control.complete",
		"store.freeze.exit",
		"control.get-completion",
		"control.next",
	})
	if completionLookupInsideFreeze.Load() {
		t.Fatal("GetIndexDataDeletionCompletion ran inside frozen callback")
	}
	select {
	case err := <-reported:
		t.Fatalf("resolved commit-then-error was reported: %v", err)
	default:
	}
}

func TestIndexDataDeletionCoordinatorResolvesCanceledCommitWithFreshContext(
	t *testing.T,
) {
	operation, attempt, completion, _ := coordinatorTestRecords("canceled-commit")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var cancelCallback context.CancelFunc
	var completionLookupLive atomic.Bool
	var completionLookupOutsideCallback atomic.Bool
	type callbackContextKey struct{}
	store := &coordinatorTestStore{
		recorder: recorder,
		freezeContext: func(ctx context.Context) context.Context {
			// #nosec G118 -- Complete invokes the retained cancellation to
			// model a lost response after a callback-scoped commit.
			callbackContext, cancel := context.WithCancel(ctx)
			cancelCallback = cancel
			return context.WithValue(
				callbackContext,
				callbackContextKey{},
				true,
			)
		},
		statusProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionReady,
		},
		advanceProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionPhysicallyEmpty,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
		complete: func(
			ctx context.Context,
			_ control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			if ctx.Value(callbackContextKey{}) != true {
				return control.IndexDataDeletionCompletion{},
					errors.New("completion did not receive callback context")
			}
			cancelCallback()
			if !errors.Is(ctx.Err(), context.Canceled) {
				return control.IndexDataDeletionCompletion{},
					errors.New("callback context did not cancel")
			}
			return control.IndexDataDeletionCompletion{}, context.Canceled
		},
		getCompletion: func(
			ctx context.Context,
			_ string,
		) (control.IndexDataDeletionCompletion, error) {
			completionLookupLive.Store(ctx.Err() == nil)
			completionLookupOutsideCallback.Store(
				ctx.Value(callbackContextKey{}) == nil,
			)
			return completion, nil
		},
	}
	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "canceled commit response resolution")
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	assertCoordinatorTestCount(t, events, "store.freeze.enter", 1)
	assertCoordinatorTestCount(t, events, "control.complete", 1)
	assertCoordinatorTestCount(t, events, "control.get-completion", 1)
	if !completionLookupLive.Load() {
		t.Fatal("terminal audit lookup received a canceled context")
	}
	if !completionLookupOutsideCallback.Load() {
		t.Fatal("terminal audit lookup reused callback-scoped context")
	}
}

func TestIndexDataDeletionCoordinatorResolvesConcurrentEnsureCompletion(
	t *testing.T,
) {
	operation, _, completion, target := coordinatorTestRecords("ensure-completed")
	recorder := newCoordinatorTestRecorder()
	rescanned := make(chan struct{})
	var nextCalls atomic.Int32
	var rescanOnce sync.Once
	var completionLookupLive atomic.Bool
	store := &coordinatorTestStore{
		recorder: recorder,
		target: clickhouse.IndexDataDeletionTarget{
			Database:  target.Database,
			Table:     target.Table,
			TableUUID: target.TableUUID,
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			rescanOnce.Do(func() { close(rescanned) })
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
		},
		ensureAttempt: func(
			context.Context,
			string,
			control.IndexDeletionMutationTarget,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
		},
		getCompletion: func(
			ctx context.Context,
			_ string,
		) (control.IndexDataDeletionCompletion, error) {
			completionLookupLive.Store(ctx.Err() == nil && !store.isFrozen())
			return completion, nil
		},
	}
	reported := make(chan error, 1)
	config := coordinatorTestConfig()
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	select {
	case <-rescanned:
	case err := <-reported:
		t.Fatalf("matching concurrent completion reported an error: %v", err)
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("matching concurrent completion did not clear and rescan")
	}
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	assertCoordinatorTestCount(t, events, "store.freeze.enter", 1)
	assertCoordinatorTestCount(t, events, "control.ensure-attempt", 1)
	assertCoordinatorTestCount(t, events, "frozen.advance", 0)
	assertCoordinatorTestCount(t, events, "control.get-completion", 1)
	assertCoordinatorTestCount(t, events, "control.next", 2)
	if !completionLookupLive.Load() {
		t.Fatal("concurrent completion lookup was frozen or canceled")
	}
}

func TestIndexDataDeletionCoordinatorRetainsConcurrentEnsureWithoutExactAudit(
	t *testing.T,
) {
	tests := []struct {
		name          string
		getCompletion func(
			control.IndexDataDeletionCompletion,
		) (control.IndexDataDeletionCompletion, error)
	}{
		{
			name: "missing audit",
			getCompletion: func(
				control.IndexDataDeletionCompletion,
			) (control.IndexDataDeletionCompletion, error) {
				return control.IndexDataDeletionCompletion{}, control.ErrNotFound
			},
		},
		{
			name: "mismatched audit",
			getCompletion: func(
				completion control.IndexDataDeletionCompletion,
			) (control.IndexDataDeletionCompletion, error) {
				completion.Target.TableUUID =
					"11234567-89ab-4cde-8fab-0123456789ab"
				return completion, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operation, _, completion, target := coordinatorTestRecords(
				strings.ReplaceAll(test.name, " ", "-"),
			)
			recorder := newCoordinatorTestRecorder()
			reported := make(chan error, 1)
			var nextCalls atomic.Int32
			var completionLookupLive atomic.Bool
			store := &coordinatorTestStore{
				recorder: recorder,
				target: clickhouse.IndexDataDeletionTarget{
					Database:  target.Database,
					Table:     target.Table,
					TableUUID: target.TableUUID,
				},
			}
			controlPlane := &coordinatorTestControl{
				recorder: recorder,
				next: func(context.Context) (control.IndexDeletionOperation, error) {
					nextCalls.Add(1)
					return operation, nil
				},
				getAttempt: func(
					context.Context,
					string,
				) (control.IndexDeletionMutationAttempt, error) {
					return control.IndexDeletionMutationAttempt{},
						control.ErrNotFound
				},
				ensureAttempt: func(
					context.Context,
					string,
					control.IndexDeletionMutationTarget,
				) (control.IndexDeletionMutationAttempt, error) {
					return control.IndexDeletionMutationAttempt{},
						control.ErrNotFound
				},
				getCompletion: func(
					ctx context.Context,
					_ string,
				) (control.IndexDataDeletionCompletion, error) {
					completionLookupLive.Store(
						ctx.Err() == nil && !store.isFrozen(),
					)
					return test.getCompletion(completion)
				},
			}
			config := coordinatorTestConfig()
			config.OnError = func(err error) { reported <- err }
			coordinator := startCoordinatorTest(t, controlPlane, store, config)
			select {
			case err := <-reported:
				if err == nil {
					t.Fatal("concurrent Ensure race reported nil error")
				}
			case <-time.After(coordinatorTestTimeout):
				t.Fatal("concurrent Ensure race did not report missing audit")
			}
			closeCoordinatorTest(t, coordinator)

			events := recorder.snapshot()
			assertCoordinatorTestCount(t, events, "control.next", 1)
			assertCoordinatorTestCount(t, events, "control.ensure-attempt", 1)
			assertCoordinatorTestCount(t, events, "frozen.advance", 0)
			assertCoordinatorTestCount(t, events, "control.get-completion", 1)
			if got := nextCalls.Load(); got != 1 {
				t.Fatalf(
					"oldest operation discovery calls = %d, want 1",
					got,
				)
			}
			if !completionLookupLive.Load() {
				t.Fatal("concurrent completion lookup was frozen or canceled")
			}
		})
	}
}

func TestIndexDataDeletionCoordinatorRejectsAttemptTenantBeforeNativeCall(
	t *testing.T,
) {
	operation, attempt, _, _ := coordinatorTestRecords("tenant-drift")
	attempt.Target.TenantID = "other-tenant"
	recorder := newCoordinatorTestRecorder()
	reported := make(chan error, 1)
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	config := coordinatorTestConfig()
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)

	select {
	case err := <-reported:
		if err == nil {
			t.Fatal("tenant mismatch reported a nil error")
		}
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("coordinator did not report tenant mismatch")
	}
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	if !slices.Equal(events, []string{"control.next", "control.get-attempt"}) {
		t.Fatalf("tenant mismatch calls = %v, want no native calls", events)
	}
}

func TestIndexDataDeletionCoordinatorOldestFailureBlocksYoungerOperation(
	t *testing.T,
) {
	oldest, _, _, _ := coordinatorTestRecords("oldest")
	younger, _, _, _ := coordinatorTestRecords("younger")
	younger.ID = "idxdel_younger"
	poison := errors.New("oldest durable operation is unreadable")
	recorder := newCoordinatorTestRecorder()
	reported := make(chan error, 4)
	var youngerTouched atomic.Bool
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return oldest, nil
		},
		getAttempt: func(
			_ context.Context,
			operationID string,
		) (control.IndexDeletionMutationAttempt, error) {
			if operationID == younger.ID {
				youngerTouched.Store(true)
			}
			return control.IndexDeletionMutationAttempt{}, poison
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	config := coordinatorTestConfig()
	config.RetryInitial = time.Millisecond
	config.RetryMaximum = time.Millisecond
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)

	for range 2 {
		select {
		case err := <-reported:
			if !errors.Is(err, poison) {
				t.Fatalf("reported error = %v, want oldest poison", err)
			}
		case <-time.After(coordinatorTestTimeout):
			t.Fatal("coordinator did not retry oldest poison")
		}
	}
	closeCoordinatorTest(t, coordinator)

	if youngerTouched.Load() {
		t.Fatal("coordinator touched younger operation while oldest was poisoned")
	}
	events := recorder.snapshot()
	if countCoordinatorTestEvent(events, "control.next") != 1 {
		t.Fatalf("NextIndexDeletionOperation calls = %v, want cached oldest", events)
	}
	if countCoordinatorTestEvent(events, "control.get-attempt") < 2 {
		t.Fatalf("oldest attempt reads = %v, want at least two", events)
	}
	assertCoordinatorTestCount(t, events, "store.status", 0)
	assertCoordinatorTestCount(t, events, "store.freeze.enter", 0)
}

func TestIndexDataDeletionCoordinatorCloseCancelsBlockedStepAndIsIdempotent(
	t *testing.T,
) {
	started := make(chan struct{})
	canceled := make(chan error, 1)
	var startOnce sync.Once
	controlPlane := &coordinatorTestControl{
		recorder: newCoordinatorTestRecorder(),
		next: func(ctx context.Context) (control.IndexDeletionOperation, error) {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			canceled <- ctx.Err()
			return control.IndexDeletionOperation{}, ctx.Err()
		},
	}
	store := &coordinatorTestStore{recorder: newCoordinatorTestRecorder()}
	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	waitForCoordinatorTestSignal(t, started, "blocked coordinator step")
	closeCoordinatorTest(t, coordinator)

	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked step error = %v, want context.Canceled", err)
		}
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("blocked coordinator step did not observe cancellation")
	}
	closeCoordinatorTest(t, coordinator)
	coordinator.Wake()
}

func TestIndexDataDeletionCoordinatorCoalescesWakeWithoutOverlappingSteps(
	t *testing.T,
) {
	recorder := newCoordinatorTestRecorder()
	firstScan := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondScan := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	var scanCalls atomic.Int32
	var active atomic.Int32
	var maximumActive atomic.Int32
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(ctx context.Context) (control.IndexDeletionOperation, error) {
			nowActive := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximumActive.Load()
				if nowActive <= observed ||
					maximumActive.CompareAndSwap(observed, nowActive) {
					break
				}
			}
			switch scanCalls.Add(1) {
			case 1:
				firstOnce.Do(func() { close(firstScan) })
				select {
				case <-releaseFirst:
				case <-ctx.Done():
					return control.IndexDeletionOperation{}, ctx.Err()
				}
				return control.IndexDeletionOperation{}, control.ErrNotFound
			case 2:
				secondOnce.Do(func() { close(secondScan) })
				<-ctx.Done()
				return control.IndexDeletionOperation{}, ctx.Err()
			default:
				return control.IndexDeletionOperation{}, errors.New(
					"wake storm produced more than one recovery scan",
				)
			}
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	waitForCoordinatorTestSignal(t, firstScan, "first recovery scan")
	for range 64 {
		coordinator.Wake()
	}
	close(releaseFirst)
	waitForCoordinatorTestSignal(t, secondScan, "coalesced wake recovery scan")
	closeCoordinatorTest(t, coordinator)

	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent coordinator steps = %d, want 1", got)
	}
	if got := scanCalls.Load(); got != 2 {
		t.Fatalf("recovery scans after 64 wakes = %d, want 2", got)
	}
}

func TestIndexDataDeletionCoordinatorWakesDoNotBypassPendingPollInterval(
	t *testing.T,
) {
	const pollInterval = 100 * time.Millisecond
	operation, attempt, _, _ := coordinatorTestRecords("pending-wake")
	recorder := newCoordinatorTestRecorder()
	firstStatus := make(chan time.Time, 1)
	secondStatus := make(chan time.Time, 1)
	var statusCalls atomic.Int32
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
	}
	store := &coordinatorTestStore{
		recorder: recorder,
		status: func(
			context.Context,
			clickhouse.IndexDataDeletionRequest,
		) (clickhouse.IndexDataDeletionProgress, error) {
			switch statusCalls.Add(1) {
			case 1:
				firstStatus <- time.Now()
			case 2:
				secondStatus <- time.Now()
			}
			return clickhouse.IndexDataDeletionProgress{
				State: clickhouse.IndexDataDeletionPending,
			}, nil
		},
	}
	config := coordinatorTestConfig()
	config.PollInterval = pollInterval
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	first := waitForCoordinatorTestValue(
		t,
		firstStatus,
		"first pending status",
	)
	for range 64 {
		coordinator.Wake()
	}
	second := waitForCoordinatorTestValue(
		t,
		secondStatus,
		"fixed-interval pending status",
	)
	closeCoordinatorTest(t, coordinator)

	if elapsed := second.Sub(first); elapsed < pollInterval {
		t.Fatalf(
			"pending retry elapsed = %v, want at least %v despite wakes",
			elapsed,
			pollInterval,
		)
	}
	assertCoordinatorTestCount(
		t,
		recorder.snapshot(),
		"store.freeze.enter",
		0,
	)
}

func TestIndexDataDeletionCoordinatorPropagatesFrozenCallbackContext(
	t *testing.T,
) {
	operation, attempt, completion, target := coordinatorTestRecords("callback-context")
	recorder := newCoordinatorTestRecorder()
	var nextCalls atomic.Int32
	var callbackContextCalls atomic.Int32
	type callbackContextKey struct{}
	const callbackContextValue = "frozen-callback"
	checkCallbackContext := func(ctx context.Context) error {
		if got := ctx.Value(callbackContextKey{}); got != callbackContextValue {
			return errors.New("dependency did not receive frozen callback context")
		}
		callbackContextCalls.Add(1)
		return nil
	}
	store := &coordinatorTestStore{
		recorder: recorder,
		freezeContext: func(ctx context.Context) context.Context {
			return context.WithValue(
				ctx,
				callbackContextKey{},
				callbackContextValue,
			)
		},
		drain: checkCallbackContext,
		resolveTarget: func(
			ctx context.Context,
		) (clickhouse.IndexDataDeletionTarget, error) {
			if err := checkCallbackContext(ctx); err != nil {
				return clickhouse.IndexDataDeletionTarget{}, err
			}
			return clickhouse.IndexDataDeletionTarget{
				Database:  target.Database,
				Table:     target.Table,
				TableUUID: target.TableUUID,
			}, nil
		},
		advance: func(
			ctx context.Context,
			_ clickhouse.IndexDataDeletionRequest,
		) (clickhouse.IndexDataDeletionProgress, error) {
			if err := checkCallbackContext(ctx); err != nil {
				return clickhouse.IndexDataDeletionProgress{}, err
			}
			return clickhouse.IndexDataDeletionProgress{
				State: clickhouse.IndexDataDeletionPhysicallyEmpty,
			}, nil
		},
	}
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			if nextCalls.Add(1) == 1 {
				return operation, nil
			}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
		},
		ensureAttempt: func(
			ctx context.Context,
			_ string,
			_ control.IndexDeletionMutationTarget,
		) (control.IndexDeletionMutationAttempt, error) {
			if err := checkCallbackContext(ctx); err != nil {
				return control.IndexDeletionMutationAttempt{}, err
			}
			return attempt, nil
		},
		complete: func(
			ctx context.Context,
			_ control.IndexDeletionMutationAttempt,
		) (control.IndexDataDeletionCompletion, error) {
			if err := checkCallbackContext(ctx); err != nil {
				return control.IndexDataDeletionCompletion{}, err
			}
			return completion, nil
		},
	}
	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.next") == 2
	}, "completion with callback-scoped context")
	closeCoordinatorTest(t, coordinator)

	if got := callbackContextCalls.Load(); got != 5 {
		t.Fatalf("callback-scoped dependency calls = %d, want 5", got)
	}
}

func TestIndexDataDeletionCoordinatorDoesNotCompleteAdvanceResultWithError(
	t *testing.T,
) {
	operation, attempt, _, _ := coordinatorTestRecords("advance-error")
	recorder := newCoordinatorTestRecorder()
	advanceErr := io.ErrUnexpectedEOF
	reported := make(chan error, 1)
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return attempt, nil
		},
	}
	store := &coordinatorTestStore{
		recorder: recorder,
		statusProgress: clickhouse.IndexDataDeletionProgress{
			State: clickhouse.IndexDataDeletionReady,
		},
		advance: func(
			context.Context,
			clickhouse.IndexDataDeletionRequest,
		) (clickhouse.IndexDataDeletionProgress, error) {
			return clickhouse.IndexDataDeletionProgress{
				State: clickhouse.IndexDataDeletionPhysicallyEmpty,
			}, advanceErr
		},
	}
	config := coordinatorTestConfig()
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)

	select {
	case err := <-reported:
		if !errors.Is(err, advanceErr) {
			t.Fatalf("reported error = %v, want advance error", err)
		}
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("coordinator did not report ambiguous advance error")
	}
	closeCoordinatorTest(t, coordinator)

	events := recorder.snapshot()
	assertCoordinatorTestCount(t, events, "frozen.advance", 1)
	assertCoordinatorTestCount(t, events, "control.complete", 0)
	assertCoordinatorTestCount(t, events, "control.get-completion", 0)
}

func TestIndexDataDeletionCoordinatorRetainsEnsuredAttemptAfterAdvanceError(
	t *testing.T,
) {
	operation, attempt, _, target := coordinatorTestRecords("retained-attempt")
	recorder := newCoordinatorTestRecorder()
	reported := make(chan error, 1)
	statusCalled := make(chan struct{})
	var getAttemptCalls atomic.Int32
	var ensureCalls atomic.Int32
	var statusRequest clickhouse.IndexDataDeletionRequest
	var statusOnce sync.Once
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			getAttemptCalls.Add(1)
			return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
		},
		ensureAttempt: func(
			context.Context,
			string,
			control.IndexDeletionMutationTarget,
		) (control.IndexDeletionMutationAttempt, error) {
			ensureCalls.Add(1)
			return attempt, nil
		},
	}
	store := &coordinatorTestStore{
		recorder: recorder,
		target: clickhouse.IndexDataDeletionTarget{
			Database:  target.Database,
			Table:     target.Table,
			TableUUID: target.TableUUID,
		},
		advance: func(
			context.Context,
			clickhouse.IndexDataDeletionRequest,
		) (clickhouse.IndexDataDeletionProgress, error) {
			return clickhouse.IndexDataDeletionProgress{}, io.ErrUnexpectedEOF
		},
		status: func(
			_ context.Context,
			request clickhouse.IndexDataDeletionRequest,
		) (clickhouse.IndexDataDeletionProgress, error) {
			statusRequest = request
			statusOnce.Do(func() { close(statusCalled) })
			return clickhouse.IndexDataDeletionProgress{
				State: clickhouse.IndexDataDeletionPending,
			}, nil
		},
	}
	config := coordinatorTestConfig()
	config.RetryInitial = time.Millisecond
	config.RetryMaximum = time.Millisecond
	config.OnError = func(err error) { reported <- err }
	coordinator := startCoordinatorTest(t, controlPlane, store, config)

	select {
	case err := <-reported:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("reported error = %v, want ambiguous advance error", err)
		}
	case <-time.After(coordinatorTestTimeout):
		t.Fatal("coordinator did not report ambiguous advance error")
	}
	waitForCoordinatorTestSignal(t, statusCalled, "poll of retained attempt")
	closeCoordinatorTest(t, coordinator)

	if got := getAttemptCalls.Load(); got != 1 {
		t.Fatalf("durable attempt reads = %d, want 1", got)
	}
	if got := ensureCalls.Load(); got != 1 {
		t.Fatalf("attempt ensures = %d, want 1", got)
	}
	if want := coordinatorTestRequest(attempt); statusRequest != want {
		t.Fatalf("retained status request = %#v, want %#v", statusRequest, want)
	}
	assertCoordinatorTestSubsequence(t, recorder.snapshot(), []string{
		"control.ensure-attempt",
		"frozen.advance",
		"store.freeze.exit",
		"store.status",
	})
}

func TestIndexDataDeletionCoordinatorRejectsInvalidProgressStates(
	t *testing.T,
) {
	tests := []struct {
		name          string
		status        clickhouse.IndexDataDeletionState
		advance       clickhouse.IndexDataDeletionState
		wantFreeze    int
		wantAdvances  int
		wantCompletes int
	}{
		{
			name:       "zero status",
			status:     0,
			wantFreeze: 0,
		},
		{
			name:       "terminal status outside freeze",
			status:     clickhouse.IndexDataDeletionPhysicallyEmpty,
			wantFreeze: 0,
		},
		{
			name:         "zero frozen advancement",
			status:       clickhouse.IndexDataDeletionReady,
			advance:      0,
			wantFreeze:   1,
			wantAdvances: 1,
		},
		{
			name:         "ready frozen advancement",
			status:       clickhouse.IndexDataDeletionReady,
			advance:      clickhouse.IndexDataDeletionReady,
			wantFreeze:   1,
			wantAdvances: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operation, attempt, _, _ := coordinatorTestRecords(
				strings.ReplaceAll(test.name, " ", "-"),
			)
			recorder := newCoordinatorTestRecorder()
			reported := make(chan error, 1)
			controlPlane := &coordinatorTestControl{
				recorder: recorder,
				next: func(context.Context) (control.IndexDeletionOperation, error) {
					return operation, nil
				},
				getAttempt: func(
					context.Context,
					string,
				) (control.IndexDeletionMutationAttempt, error) {
					return attempt, nil
				},
			}
			store := &coordinatorTestStore{
				recorder: recorder,
				statusProgress: clickhouse.IndexDataDeletionProgress{
					State: test.status,
				},
				advanceProgress: clickhouse.IndexDataDeletionProgress{
					State: test.advance,
				},
			}
			config := coordinatorTestConfig()
			config.OnError = func(err error) { reported <- err }
			coordinator := startCoordinatorTest(t, controlPlane, store, config)

			select {
			case err := <-reported:
				if err == nil {
					t.Fatal("invalid progress state reported nil error")
				}
			case <-time.After(coordinatorTestTimeout):
				t.Fatal("coordinator did not report invalid progress state")
			}
			closeCoordinatorTest(t, coordinator)

			events := recorder.snapshot()
			assertCoordinatorTestCount(
				t,
				events,
				"store.freeze.enter",
				test.wantFreeze,
			)
			assertCoordinatorTestCount(
				t,
				events,
				"frozen.advance",
				test.wantAdvances,
			)
			assertCoordinatorTestCount(
				t,
				events,
				"control.complete",
				test.wantCompletes,
			)
		})
	}
}

func TestNewIndexDataDeletionCoordinatorRejectsInvalidConfiguration(
	t *testing.T,
) {
	recorder := newCoordinatorTestRecorder()
	validControl := &coordinatorTestControl{recorder: recorder}
	validStore := &coordinatorTestStore{recorder: recorder}
	validConfig := coordinatorTestConfig()
	var nilControl *coordinatorTestControl
	var nilStore *coordinatorTestStore

	dependencyTests := []struct {
		name         string
		controlPlane DeletionControl
		store        DeletionStore
	}{
		{name: "nil control", controlPlane: nil, store: validStore},
		{name: "typed nil control", controlPlane: nilControl, store: validStore},
		{name: "nil store", controlPlane: validControl, store: nil},
		{name: "typed nil store", controlPlane: validControl, store: nilStore},
	}
	for _, test := range dependencyTests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, err := NewIndexDataDeletionCoordinator(
				test.controlPlane,
				test.store,
				validConfig,
			)
			if err == nil {
				closeCoordinatorTest(t, coordinator)
				t.Fatal("constructor accepted nil dependency")
			}
			if coordinator != nil {
				t.Fatalf("coordinator = %#v after constructor error, want nil", coordinator)
			}
		})
	}

	tenantTests := []struct {
		name     string
		tenantID string
	}{
		{name: "empty", tenantID: ""},
		{name: "leading whitespace", tenantID: " tenant"},
		{name: "trailing whitespace", tenantID: "tenant "},
		{name: "control character", tenantID: "tenant\nname"},
		{name: "nul", tenantID: "tenant\x00name"},
		{name: "invalid utf8", tenantID: string([]byte{0xff})},
		{name: "too long", tenantID: strings.Repeat("t", 256)},
	}
	for _, test := range tenantTests {
		t.Run("tenant "+test.name, func(t *testing.T) {
			config := validConfig
			config.TenantID = test.tenantID
			coordinator, err := NewIndexDataDeletionCoordinator(
				validControl,
				validStore,
				config,
			)
			if err == nil {
				closeCoordinatorTest(t, coordinator)
				t.Fatal("constructor accepted invalid tenant")
			}
			if coordinator != nil {
				t.Fatalf("coordinator = %#v after constructor error, want nil", coordinator)
			}
		})
	}

	durationTests := []struct {
		name   string
		change func(*IndexDataDeletionCoordinatorConfig)
	}{
		{
			name: "negative poll",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.PollInterval = -time.Nanosecond
			},
		},
		{
			name: "negative recovery",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.RecoveryInterval = -time.Nanosecond
			},
		},
		{
			name: "negative initial retry",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.RetryInitial = -time.Nanosecond
			},
		},
		{
			name: "negative maximum retry",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.RetryMaximum = -time.Nanosecond
			},
		},
		{
			name: "maximum below initial retry",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.RetryInitial = time.Second
				config.RetryMaximum = time.Second - time.Nanosecond
			},
		},
		{
			name: "negative step timeout",
			change: func(config *IndexDataDeletionCoordinatorConfig) {
				config.StepTimeout = -time.Nanosecond
			},
		},
	}
	for _, test := range durationTests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig
			test.change(&config)
			coordinator, err := NewIndexDataDeletionCoordinator(
				validControl,
				validStore,
				config,
			)
			if err == nil {
				closeCoordinatorTest(t, coordinator)
				t.Fatal("constructor accepted invalid duration")
			}
			if coordinator != nil {
				t.Fatalf("coordinator = %#v after constructor error, want nil", coordinator)
			}
		})
	}
}

func TestNewIndexDataDeletionCoordinatorAppliesDurationDefaults(
	t *testing.T,
) {
	recorder := newCoordinatorTestRecorder()
	controlPlane := &coordinatorTestControl{recorder: recorder}
	store := &coordinatorTestStore{recorder: recorder}
	coordinator, err := NewIndexDataDeletionCoordinator(
		controlPlane,
		store,
		IndexDataDeletionCoordinatorConfig{TenantID: "tenant-a"},
	)
	if err != nil {
		t.Fatalf("NewIndexDataDeletionCoordinator(): %v", err)
	}
	closeCoordinatorTest(t, coordinator)

	if coordinator.pollInterval != defaultDeletionPollInterval ||
		coordinator.recoveryInterval != defaultDeletionRecoveryInterval ||
		coordinator.retryInitial != defaultDeletionRetryInitial ||
		coordinator.retryMaximum != defaultDeletionRetryMaximum ||
		coordinator.stepTimeout != defaultDeletionStepTimeout {
		t.Fatalf(
			"default durations = poll %v recovery %v retry %v..%v step %v",
			coordinator.pollInterval,
			coordinator.recoveryInterval,
			coordinator.retryInitial,
			coordinator.retryMaximum,
			coordinator.stepTimeout,
		)
	}
}

func TestIndexDataDeletionCoordinatorCloseDeadlineAllowsLaterJoin(
	t *testing.T,
) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	controlPlane := &coordinatorTestControl{
		recorder: newCoordinatorTestRecorder(),
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			startedOnce.Do(func() { close(started) })
			<-release
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
	}
	store := &coordinatorTestStore{recorder: newCoordinatorTestRecorder()}
	coordinator := startCoordinatorTest(
		t,
		controlPlane,
		store,
		coordinatorTestConfig(),
	)
	waitForCoordinatorTestSignal(t, started, "dependency that ignores cancellation")

	firstContext, cancelFirst := context.WithTimeout(
		context.Background(),
		time.Millisecond,
	)
	defer cancelFirst()
	if err := coordinator.Close(firstContext); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("first Close() error = %v, want context.DeadlineExceeded", err)
	}

	releaseOnce.Do(func() { close(release) })
	closeCoordinatorTest(t, coordinator)
}

func TestIndexDataDeletionCoordinatorBlockedOnErrorDoesNotBlockWorkerOrClose(
	t *testing.T,
) {
	operation, _, _, _ := coordinatorTestRecords("blocked-error-hook")
	recorder := newCoordinatorTestRecorder()
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	hookDone := make(chan struct{})
	poison := errors.New("repeatable coordinator failure")
	var hookCalls atomic.Int32
	var hookStartedOnce sync.Once
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, poison
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	config := coordinatorTestConfig()
	config.RetryInitial = time.Millisecond
	config.RetryMaximum = time.Millisecond
	config.OnError = func(error) {
		hookCalls.Add(1)
		hookStartedOnce.Do(func() { close(hookStarted) })
		<-releaseHook
		close(hookDone)
	}
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	waitForCoordinatorTestSignal(t, hookStarted, "blocked OnError callback")
	recorder.waitFor(t, func(events []string) bool {
		return countCoordinatorTestEvent(events, "control.get-attempt") >= 2
	}, "worker retry while OnError is blocked")
	closeCoordinatorTest(t, coordinator)

	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("concurrent/coalesced OnError calls = %d, want 1", got)
	}
	close(releaseHook)
	waitForCoordinatorTestSignal(t, hookDone, "blocked OnError release")
}

func TestIndexDataDeletionCoordinatorContainsOnErrorPanicAndReportsAgain(
	t *testing.T,
) {
	operation, _, _, _ := coordinatorTestRecords("panic-error-hook")
	recorder := newCoordinatorTestRecorder()
	hookCalls := make(chan struct{}, 16)
	poison := errors.New("repeatable coordinator failure")
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			return control.IndexDeletionMutationAttempt{}, poison
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	config := coordinatorTestConfig()
	config.RetryInitial = time.Millisecond
	config.RetryMaximum = time.Millisecond
	config.OnError = func(error) {
		hookCalls <- struct{}{}
		panic("test OnError panic")
	}
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	for range 2 {
		waitForCoordinatorTestSignal(t, hookCalls, "panic-contained OnError call")
	}
	closeCoordinatorTest(t, coordinator)
}

func TestIndexDataDeletionCoordinatorWakesDoNotBypassErrorBackoff(
	t *testing.T,
) {
	operation, _, _, _ := coordinatorTestRecords("error-backoff")
	recorder := newCoordinatorTestRecorder()
	firstError := make(chan struct{})
	secondAttempt := make(chan struct{})
	var errorOnce sync.Once
	var secondOnce sync.Once
	var attemptCalls atomic.Int32
	poison := errors.New("backed-off coordinator failure")
	controlPlane := &coordinatorTestControl{
		recorder: recorder,
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return operation, nil
		},
		getAttempt: func(
			context.Context,
			string,
		) (control.IndexDeletionMutationAttempt, error) {
			if attemptCalls.Add(1) == 2 {
				secondOnce.Do(func() { close(secondAttempt) })
			}
			return control.IndexDeletionMutationAttempt{}, poison
		},
	}
	store := &coordinatorTestStore{recorder: recorder}
	config := coordinatorTestConfig()
	config.RetryInitial = 500 * time.Millisecond
	config.RetryMaximum = 500 * time.Millisecond
	config.OnError = func(error) {
		errorOnce.Do(func() { close(firstError) })
	}
	coordinator := startCoordinatorTest(t, controlPlane, store, config)
	waitForCoordinatorTestSignal(t, firstError, "first backed-off error")
	for range 64 {
		coordinator.Wake()
	}

	select {
	case <-secondAttempt:
		t.Fatal("Wake bypassed coordinator error backoff")
	case <-time.After(50 * time.Millisecond):
	}
	closeCoordinatorTest(t, coordinator)
	if got := attemptCalls.Load(); got != 1 {
		t.Fatalf("attempt reads during error backoff = %d, want 1", got)
	}
}

type coordinatorTestControl struct {
	recorder      *coordinatorTestRecorder
	next          func(context.Context) (control.IndexDeletionOperation, error)
	getAttempt    func(context.Context, string) (control.IndexDeletionMutationAttempt, error)
	ensureAttempt func(context.Context, string, control.IndexDeletionMutationTarget) (control.IndexDeletionMutationAttempt, error)
	complete      func(context.Context, control.IndexDeletionMutationAttempt) (control.IndexDataDeletionCompletion, error)
	getCompletion func(context.Context, string) (control.IndexDataDeletionCompletion, error)
}

func (fake *coordinatorTestControl) NextIndexDeletionOperation(
	ctx context.Context,
) (control.IndexDeletionOperation, error) {
	fake.recorder.record("control.next")
	if fake.next == nil {
		return control.IndexDeletionOperation{}, control.ErrNotFound
	}
	return fake.next(ctx)
}

func (fake *coordinatorTestControl) GetIndexDeletionMutationAttempt(
	ctx context.Context,
	operationID string,
) (control.IndexDeletionMutationAttempt, error) {
	fake.recorder.record("control.get-attempt")
	if fake.getAttempt == nil {
		return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
	}
	return fake.getAttempt(ctx, operationID)
}

func (fake *coordinatorTestControl) EnsureIndexDeletionMutationAttempt(
	ctx context.Context,
	operationID string,
	target control.IndexDeletionMutationTarget,
) (control.IndexDeletionMutationAttempt, error) {
	fake.recorder.record("control.ensure-attempt")
	if fake.ensureAttempt == nil {
		return control.IndexDeletionMutationAttempt{}, errors.New(
			"unexpected EnsureIndexDeletionMutationAttempt call",
		)
	}
	return fake.ensureAttempt(ctx, operationID, target)
}

func (fake *coordinatorTestControl) CompleteIndexDataDeletion(
	ctx context.Context,
	attempt control.IndexDeletionMutationAttempt,
) (control.IndexDataDeletionCompletion, error) {
	fake.recorder.record("control.complete")
	if fake.complete == nil {
		return control.IndexDataDeletionCompletion{}, errors.New(
			"unexpected CompleteIndexDataDeletion call",
		)
	}
	return fake.complete(ctx, attempt)
}

func (fake *coordinatorTestControl) GetIndexDataDeletionCompletion(
	ctx context.Context,
	operationID string,
) (control.IndexDataDeletionCompletion, error) {
	fake.recorder.record("control.get-completion")
	if fake.getCompletion == nil {
		return control.IndexDataDeletionCompletion{}, control.ErrNotFound
	}
	return fake.getCompletion(ctx, operationID)
}

type coordinatorTestStore struct {
	recorder        *coordinatorTestRecorder
	status          func(context.Context, clickhouse.IndexDataDeletionRequest) (clickhouse.IndexDataDeletionProgress, error)
	statusProgress  clickhouse.IndexDataDeletionProgress
	freezeContext   func(context.Context) context.Context
	drain           func(context.Context) error
	resolveTarget   func(context.Context) (clickhouse.IndexDataDeletionTarget, error)
	target          clickhouse.IndexDataDeletionTarget
	targetError     error
	advance         func(context.Context, clickhouse.IndexDataDeletionRequest) (clickhouse.IndexDataDeletionProgress, error)
	advanceProgress clickhouse.IndexDataDeletionProgress
	freezeDepth     atomic.Int32
}

func (fake *coordinatorTestStore) IndexDataDeletionStatus(
	ctx context.Context,
	request clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	fake.recorder.record("store.status")
	if fake.status != nil {
		return fake.status(ctx, request)
	}
	return fake.statusProgress, nil
}

func (fake *coordinatorTestStore) WithWritesFrozen(
	ctx context.Context,
	callback func(context.Context, clickhouse.FrozenWrites) error,
) error {
	fake.recorder.record("store.freeze.enter")
	fake.freezeDepth.Add(1)
	callbackContext := ctx
	if fake.freezeContext != nil {
		callbackContext = fake.freezeContext(ctx)
	}
	err := callback(callbackContext, (*coordinatorTestFrozenWrites)(fake))
	fake.freezeDepth.Add(-1)
	fake.recorder.record("store.freeze.exit")
	return err
}

func (fake *coordinatorTestStore) isFrozen() bool {
	return fake.freezeDepth.Load() != 0
}

type coordinatorTestFrozenWrites coordinatorTestStore

func (fake *coordinatorTestFrozenWrites) DrainPending(ctx context.Context) error {
	store := (*coordinatorTestStore)(fake)
	store.recorder.record("frozen.drain")
	if store.drain != nil {
		return store.drain(ctx)
	}
	return nil
}

func (fake *coordinatorTestFrozenWrites) IndexDataDeletionTarget(
	ctx context.Context,
) (clickhouse.IndexDataDeletionTarget, error) {
	store := (*coordinatorTestStore)(fake)
	store.recorder.record("frozen.target")
	if store.resolveTarget != nil {
		return store.resolveTarget(ctx)
	}
	return store.target, store.targetError
}

func (fake *coordinatorTestFrozenWrites) AdvanceIndexDataDeletion(
	ctx context.Context,
	request clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	store := (*coordinatorTestStore)(fake)
	store.recorder.record("frozen.advance")
	if store.advance != nil {
		return store.advance(ctx, request)
	}
	return store.advanceProgress, nil
}

type coordinatorTestRecorder struct {
	mutex   sync.Mutex
	events  []string
	changed chan struct{}
}

func newCoordinatorTestRecorder() *coordinatorTestRecorder {
	return &coordinatorTestRecorder{changed: make(chan struct{}, 1)}
}

func (recorder *coordinatorTestRecorder) record(event string) {
	recorder.mutex.Lock()
	recorder.events = append(recorder.events, event)
	recorder.mutex.Unlock()
	select {
	case recorder.changed <- struct{}{}:
	default:
	}
}

func (recorder *coordinatorTestRecorder) snapshot() []string {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return slices.Clone(recorder.events)
}

func (recorder *coordinatorTestRecorder) waitFor(
	t *testing.T,
	condition func([]string) bool,
	description string,
) {
	t.Helper()
	timer := time.NewTimer(coordinatorTestTimeout)
	defer timer.Stop()
	for {
		if condition(recorder.snapshot()) {
			return
		}
		select {
		case <-recorder.changed:
		case <-timer.C:
			t.Fatalf(
				"timed out waiting for %s; calls = %v",
				description,
				recorder.snapshot(),
			)
		}
	}
}

func coordinatorTestConfig() IndexDataDeletionCoordinatorConfig {
	return IndexDataDeletionCoordinatorConfig{
		TenantID:         "tenant-a",
		PollInterval:     time.Hour,
		RecoveryInterval: time.Hour,
		RetryInitial:     time.Hour,
		RetryMaximum:     time.Hour,
		StepTimeout:      time.Minute,
	}
}

func startCoordinatorTest(
	t *testing.T,
	controlPlane DeletionControl,
	store DeletionStore,
	config IndexDataDeletionCoordinatorConfig,
) *IndexDataDeletionCoordinator {
	t.Helper()
	coordinator, err := NewIndexDataDeletionCoordinator(
		controlPlane,
		store,
		config,
	)
	if err != nil {
		t.Fatalf("NewIndexDataDeletionCoordinator(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), coordinatorTestTimeout)
		defer cancel()
		if err := coordinator.Close(ctx); err != nil {
			t.Errorf("Close() during cleanup: %v", err)
		}
	})
	return coordinator
}

func closeCoordinatorTest(t *testing.T, coordinator *IndexDataDeletionCoordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), coordinatorTestTimeout)
	defer cancel()
	if err := coordinator.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func coordinatorTestRecords(
	suffix string,
) (
	control.IndexDeletionOperation,
	control.IndexDeletionMutationAttempt,
	control.IndexDataDeletionCompletion,
	control.IndexDeletionMutationTarget,
) {
	operationCreatedAt := time.Unix(1_800_000_000, 123_456_000).UTC()
	attemptCreatedAt := operationCreatedAt.Add(time.Microsecond)
	completedAt := attemptCreatedAt.Add(time.Microsecond)
	operation := control.IndexDeletionOperation{
		ID:              "idxdel_" + suffix,
		IndexID:         "index-" + suffix,
		IndexName:       "index-" + suffix,
		ArchivedVersion: 4,
		DeletingVersion: 5,
		CreatedAt:       operationCreatedAt,
	}
	target := control.IndexDeletionMutationTarget{
		TenantID:  "tenant-a",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "01234567-89ab-4cde-8fab-0123456789ab",
	}
	attempt := control.IndexDeletionMutationAttempt{
		CorrelationID:       "idxmut_" + suffix,
		DeletionOperationID: operation.ID,
		IndexID:             operation.IndexID,
		IndexName:           operation.IndexName,
		Target:              target,
		ProtocolVersion:     control.IndexDeletionMutationProtocolVersion,
		CreatedAt:           attemptCreatedAt,
	}
	completion := control.IndexDataDeletionCompletion{
		DeletionOperationID: operation.ID,
		CorrelationID:       attempt.CorrelationID,
		IndexID:             operation.IndexID,
		IndexName:           operation.IndexName,
		ArchivedVersion:     operation.ArchivedVersion,
		DeletedVersion:      operation.DeletingVersion,
		Target:              target,
		ProtocolVersion:     attempt.ProtocolVersion,
		OperationCreatedAt:  operation.CreatedAt,
		MutationCreatedAt:   attempt.CreatedAt,
		CompletedAt:         completedAt,
	}
	return operation, attempt, completion, target
}

func coordinatorTestRequest(
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

func waitForCoordinatorTestSignal(
	t *testing.T,
	signal <-chan struct{},
	description string,
) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(coordinatorTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForCoordinatorTestValue[T any](
	t *testing.T,
	values <-chan T,
	description string,
) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(coordinatorTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func countCoordinatorTestEvent(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

func assertCoordinatorTestCount(
	t *testing.T,
	events []string,
	event string,
	want int,
) {
	t.Helper()
	if got := countCoordinatorTestEvent(events, event); got != want {
		t.Fatalf("%s count = %d, want %d; calls = %v", event, got, want, events)
	}
}

func assertCoordinatorTestSubsequence(
	t *testing.T,
	events []string,
	want []string,
) {
	t.Helper()
	index := 0
	for _, event := range events {
		if index < len(want) && event == want[index] {
			index++
		}
	}
	if index != len(want) {
		t.Fatalf("calls %v do not contain ordered subsequence %v", events, want)
	}
}
