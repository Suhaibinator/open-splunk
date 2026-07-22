package searchjobs

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

type jobJournalFunc struct {
	admit    func(context.Context, Job) error
	finalize func(context.Context, Job) error
}

func (journal jobJournalFunc) Admit(ctx context.Context, job Job) error {
	if journal.admit == nil {
		return nil
	}
	return journal.admit(ctx, job)
}

func (journal jobJournalFunc) Finalize(ctx context.Context, job Job) error {
	if journal.finalize == nil {
		return nil
	}
	return journal.finalize(ctx, job)
}

func TestJournalAdmissionFailureIsSafeAndLeavesNoManagerState(t *testing.T) {
	t.Parallel()

	journalCause := errors.New("sqlite password=top-secret")
	reported := make(chan error, 1)
	var executions atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			executions.Add(1)
			return nil
		}),
		Journal: jobJournalFunc{admit: func(_ context.Context, job Job) error {
			if job.State != StateQueued || job.ID == "" || job.CreatedAt.IsZero() || job.VisibilityCutoff != 73 {
				t.Errorf("journal admission snapshot = %#v", job)
			}
			return journalCause
		}},
		OnJournalError: func(err error) { reported <- err },
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 73, nil
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-admission-failure"),
	})

	_, err := manager.Create(context.Background(), validRequest())
	if !errors.Is(err, ErrJournalUnavailable) {
		t.Fatalf("Create() error = %v, want ErrJournalUnavailable", err)
	}
	if strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("Create() leaked journal detail: %v", err)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no admitted job", jobs)
	}
	if executions.Load() != 0 {
		t.Fatalf("executor calls = %d, want 0", executions.Load())
	}
	manager.mu.RLock()
	pendingAdmissions := manager.pendingAdmissions
	reservedIDs := len(manager.reservedIDs)
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	metadataBytes := manager.metadataBytes
	manager.budgetMu.Unlock()
	if pendingAdmissions != 0 || reservedIDs != 0 || metadataBytes != 0 {
		t.Fatalf("failed admission leaked pending=%d IDs=%d metadata=%d", pendingAdmissions, reservedIDs, metadataBytes)
	}

	assertJournalError(t, <-reported, JournalOperationAdmit, "journal-admission-failure-1", StateQueued, journalCause)
	assertJournalError(t, manager.LastJournalError(), JournalOperationAdmit, "journal-admission-failure-1", StateQueued, journalCause)
}

func TestNewRejectsNegativeJournalTimeout(t *testing.T) {
	t.Parallel()

	manager, err := New(Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		}),
		JournalTimeout:  -time.Second,
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	})
	if err == nil {
		_ = manager.Close()
		t.Fatal("New() succeeded with a negative journal timeout")
	}
}

func TestJournalCallbacksAreDetachedAndRunWithoutManagerOrEntryLocks(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	finalized := make(chan Job, 1)
	var manager *Manager
	journal := jobJournalFunc{
		admit: func(_ context.Context, job Job) error {
			// List takes manager.mu. Its completion proves Admit is not invoked
			// while the manager lock is held; the new job is not visible yet.
			if jobs := manager.List(); len(jobs) != 0 {
				t.Errorf("job became visible before journal admission: %#v", jobs)
			}
			job.ID = "mutated"
			job.SPL = "mutated"
			job.RequestedIndexes[0] = "mutated"
			return nil
		},
		finalize: func(_ context.Context, job Job) error {
			// Get takes both manager.mu and entry.mu. It must observe the
			// transition before the callback runs, without deadlocking.
			stored, err := manager.Get(job.ID)
			if err != nil {
				t.Errorf("Get() from Finalize error = %v", err)
			} else if stored.State != job.State {
				t.Errorf("stored state = %v, callback state = %v", stored.State, job.State)
			}
			job.SPL = "mutated"
			job.RequestedIndexes[0] = "mutated"
			job.EffectiveIndexes[0] = "mutated"
			job.Schema.Columns[0].Name = "mutated"
			finalized <- job
			return nil
		},
	}
	var err error
	manager, err = New(Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
			return sink.AddRow([]Value{StringValue("ready")})
		}),
		Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal:         journal,
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-detached"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "journal-detached-1" || created.SPL != validRequest().SPL || !reflect.DeepEqual(created.RequestedIndexes, []string{"main"}) {
		t.Fatalf("Create() retained journal mutation: %#v", created)
	}
	waitForState(t, manager, created.ID, StateRunning)
	close(release)
	select {
	case callback := <-finalized:
		if callback.State != StateCompleted {
			t.Fatalf("Finalize state = %v, want completed", callback.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal journal callback did not run")
	}
	stored := waitForState(t, manager, created.ID, StateCompleted)
	if stored.SPL != validRequest().SPL || !reflect.DeepEqual(stored.RequestedIndexes, []string{"main"}) || !reflect.DeepEqual(stored.EffectiveIndexes, []string{"main"}) {
		t.Fatalf("stored job retained journal mutation: %#v", stored)
	}
	if stored.Schema == nil || stored.Schema.Columns[0].Name != "message" {
		t.Fatalf("stored schema retained journal mutation: %#v", stored.Schema)
	}
}

func TestFailedJournalSnapshotDeepCopiesFailureDiagnostics(t *testing.T) {
	t.Parallel()

	finalized := make(chan Job, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor ran for invalid SPL")
			return nil
		}),
		Journal: jobJournalFunc{finalize: func(_ context.Context, job Job) error {
			if job.Failure == nil || len(job.Failure.Diagnostics) == 0 {
				t.Errorf("failed journal snapshot lacks diagnostics: %#v", job)
				finalized <- job
				return nil
			}
			job.Failure.Message = "mutated"
			job.Failure.Diagnostics[0].Message = "mutated"
			job.Failure.Diagnostics[0].Suggestions = append(job.Failure.Diagnostics[0].Suggestions, "mutated")
			finalized <- job
			return nil
		}},
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-failed-detached"),
	})

	created, err := manager.Create(context.Background(), withSPL(validRequest(), "index=main |"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case callback := <-finalized:
		if callback.State != StateFailed {
			t.Fatalf("Finalize state = %v, want failed", callback.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed journal callback did not run")
	}
	stored := waitForState(t, manager, created.ID, StateFailed)
	if stored.Failure == nil || stored.Failure.Message == "mutated" || len(stored.Failure.Diagnostics) == 0 || stored.Failure.Diagnostics[0].Message == "mutated" {
		t.Fatalf("stored failure retained journal mutation: %#v", stored.Failure)
	}
	if suggestions := stored.Failure.Diagnostics[0].Suggestions; len(suggestions) > 0 && suggestions[len(suggestions)-1] == "mutated" {
		t.Fatalf("stored suggestions retained journal mutation: %#v", suggestions)
	}
}

func TestAdvancePanicReleasesEntryLockAndStillFinalizes(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	var panicNextClock atomic.Bool
	finalized := make(chan Job, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor ran after lifecycle clock panic")
			return nil
		}),
		Journal: jobJournalFunc{
			admit: func(context.Context, Job) error {
				panicNextClock.Store(true)
				return nil
			},
			finalize: func(_ context.Context, job Job) error {
				finalized <- job
				return nil
			},
		},
		Now: func() time.Time {
			if panicNextClock.CompareAndSwap(true, false) {
				panic("injected lifecycle clock panic")
			}
			return fixedNow
		},
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-advance-panic"),
	})

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case terminal := <-finalized:
		if terminal.State != StateFailed || terminal.ID != created.ID {
			t.Fatalf("Finalize snapshot = %#v", terminal)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic path retained entry lock or lost Finalize")
	}
	failed, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Failure == nil || failed.Failure.Code != FailureInternal {
		t.Fatalf("panic failure = %#v", failed.Failure)
	}
}

func TestCloseRacingJournalAdmissionCompensatesBeforeReturning(t *testing.T) {
	t.Parallel()

	admitStarted := make(chan struct{})
	releaseAdmit := make(chan struct{})
	finalizeStarted := make(chan Job, 1)
	releaseFinalize := make(chan struct{})
	var finalizeCalls atomic.Int32
	manager, err := New(Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor ran for an admission rejected by Close")
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal: jobJournalFunc{
			admit: func(context.Context, Job) error {
				close(admitStarted)
				<-releaseAdmit
				return nil
			},
			finalize: func(_ context.Context, job Job) error {
				finalizeCalls.Add(1)
				finalizeStarted <- job
				<-releaseFinalize
				return nil
			},
		},
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-close-race"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := manager.Create(context.Background(), validRequest())
		createDone <- createErr
	}()
	select {
	case <-admitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("journal admission did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	waitForManagerClosed(t, manager)
	close(releaseAdmit)

	var terminal Job
	select {
	case terminal = <-finalizeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("admitted job was orphaned without compensating Finalize")
	}
	if terminal.ID != "journal-close-race-1" || terminal.State != StateCanceled || terminal.FinishedAt.IsZero() {
		t.Fatalf("compensating Finalize snapshot = %#v", terminal)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before compensating Finalize completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case err := <-createDone:
		t.Fatalf("Create returned before compensating Finalize completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseFinalize)
	if err := <-createDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("Create() error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if calls := finalizeCalls.Load(); calls != 1 {
		t.Fatalf("Finalize calls = %d, want 1", calls)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no rejected job", jobs)
	}
}

func TestCloseCancelsAnObservingJournalAdmission(t *testing.T) {
	t.Parallel()

	admitStarted := make(chan struct{})
	admitCanceled := make(chan struct{})
	var finalizeCalls atomic.Int32
	manager, err := New(Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor ran for canceled journal admission")
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal: jobJournalFunc{
			admit: func(ctx context.Context, _ Job) error {
				close(admitStarted)
				<-ctx.Done()
				close(admitCanceled)
				return ctx.Err()
			},
			finalize: func(context.Context, Job) error {
				finalizeCalls.Add(1)
				return nil
			},
		},
		JournalTimeout:  time.Hour,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-close-cancel"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	createDone := make(chan error, 1)
	go func() {
		_, createErr := manager.Create(context.Background(), validRequest())
		createDone <- createErr
	}()
	select {
	case <-admitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("journal admission did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	select {
	case <-admitCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel journal admission context")
	}
	if err := <-createDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("Create() error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if calls := finalizeCalls.Load(); calls != 0 {
		t.Fatalf("Finalize calls = %d for an Admit that returned error", calls)
	}
	if err := manager.LastJournalError(); err != nil {
		t.Fatalf("normal shutdown cancellation was reported as a journal failure: %v", err)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no admitted job", jobs)
	}
}

func TestConcurrentDuplicateIDCannotFinalizeTheWinningAdmission(t *testing.T) {
	t.Parallel()

	admitStarted := make(chan struct{})
	releaseAdmit := make(chan struct{})
	var admitCalls atomic.Int32
	var finalizeCalls atomic.Int32
	finalized := make(chan struct{}, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, _ ResultSink) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		Journal: jobJournalFunc{
			admit: func(context.Context, Job) error {
				if admitCalls.Add(1) == 1 {
					close(admitStarted)
				}
				<-releaseAdmit
				return nil
			},
			finalize: func(_ context.Context, job Job) error {
				if job.State != StateCanceled {
					t.Errorf("Finalize state = %v, want canceled", job.State)
				}
				finalizeCalls.Add(1)
				finalized <- struct{}{}
				return nil
			},
		},
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           func() string { return "duplicate-journal-id" },
	})

	firstDone := make(chan struct {
		job Job
		err error
	}, 1)
	go func() {
		job, err := manager.Create(context.Background(), validRequest())
		firstDone <- struct {
			job Job
			err error
		}{job: job, err: err}
	}()
	select {
	case <-admitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first journal admission did not start")
	}
	if _, err := manager.Create(context.Background(), validRequest()); err == nil || !strings.Contains(err.Error(), "duplicate ID") {
		t.Fatalf("second Create() error = %v, want duplicate ID", err)
	}
	if calls := admitCalls.Load(); calls != 1 {
		t.Fatalf("Admit calls = %d, want only the reserved winner", calls)
	}
	if calls := finalizeCalls.Load(); calls != 0 {
		t.Fatalf("losing Create finalized winner; calls = %d", calls)
	}
	close(releaseAdmit)
	first := <-firstDone
	if first.err != nil {
		t.Fatalf("first Create() error = %v", first.err)
	}
	if err := manager.Cancel(first.job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finalized:
	case <-time.After(2 * time.Second):
		t.Fatal("winner Finalize did not run")
	}
	if calls := finalizeCalls.Load(); calls != 1 {
		t.Fatalf("winner Finalize calls = %d, want 1", calls)
	}
}

func TestTerminalJournalCallbackRunsExactlyOnceAcrossCompletionCancelAndClose(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	var finalizeCalls atomic.Int32
	terminalStates := make(chan State, 4)
	manager, err := New(Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			startOnce.Do(func() { close(started) })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal: jobJournalFunc{finalize: func(_ context.Context, job Job) error {
			finalizeCalls.Add(1)
			terminalStates <- job.State
			return nil
		}},
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-terminal-race"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}

	var racers sync.WaitGroup
	for range 32 {
		racers.Add(1)
		go func() {
			defer racers.Done()
			_ = manager.Cancel(job.ID)
		}()
	}
	racers.Add(2)
	go func() { defer racers.Done(); close(release) }()
	go func() { defer racers.Done(); _ = manager.Close() }()
	racers.Wait()

	if calls := finalizeCalls.Load(); calls != 1 {
		t.Fatalf("Finalize calls = %d, want exactly 1", calls)
	}
	select {
	case state := <-terminalStates:
		if state != StateCompleted && state != StateCanceled {
			t.Fatalf("Finalize state = %v, want completed or canceled", state)
		}
	default:
		t.Fatal("Finalize did not report a terminal state")
	}
	if err := manager.Cancel(job.ID); err != nil {
		t.Fatalf("Cancel(after terminal) = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if calls := finalizeCalls.Load(); calls != 1 {
		t.Fatalf("Finalize calls after idempotent operations = %d, want 1", calls)
	}
}

func TestCloseUsesOneDeadlineForAllShutdownOwnedFinalizations(t *testing.T) {
	t.Parallel()

	const queuedJobs = 6
	executorStarted := make(chan struct{})
	var startOnce sync.Once
	var finalizeCalls atomic.Int32
	var queuedCallbacksWithLiveContext atomic.Int32
	manager, err := New(Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, _ ResultSink) error {
			startOnce.Do(func() { close(executorStarted) })
			<-ctx.Done()
			return ctx.Err()
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal: jobJournalFunc{finalize: func(ctx context.Context, job Job) error {
			finalizeCalls.Add(1)
			if job.StartedAt.IsZero() && ctx.Err() == nil {
				queuedCallbacksWithLiveContext.Add(1)
			}
			<-ctx.Done()
			return ctx.Err()
		}},
		JournalTimeout:  15 * time.Millisecond,
		MaxConcurrent:   1,
		MaxQueued:       queuedJobs,
		MaxJobs:         queuedJobs + 1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-shared-close-deadline"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executorStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}
	for range queuedJobs {
		if _, err := manager.Create(context.Background(), validRequest()); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if calls := finalizeCalls.Load(); calls != queuedJobs+1 {
		t.Fatalf("Finalize calls = %d, want %d", calls, queuedJobs+1)
	}
	if live := queuedCallbacksWithLiveContext.Load(); live > 1 {
		t.Fatalf("%d queued callbacks each received a fresh shutdown deadline; want at most 1", live)
	}
}

func TestTerminalJournalFailureIsObservableAndDoesNotChangeJobOutcome(t *testing.T) {
	t.Parallel()

	journalCause := errors.New("disk I/O detail")
	reported := make(chan error, 1)
	var finalizeCalls atomic.Int32
	var manager *Manager
	journal := jobJournalFunc{finalize: func(context.Context, Job) error {
		finalizeCalls.Add(1)
		return journalCause
	}}
	var err error
	manager, err = New(Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		Journal:     journal,
		OnJournalError: func(err error) {
			// Reading manager and entry state from the error hook must not
			// deadlock either. Panic containment is tested by deliberately
			// panicking after the error has become observable.
			if journalErr, ok := err.(*JournalError); ok {
				if job, getErr := manager.Get(journalErr.JobID); getErr != nil || !job.State.terminal() {
					t.Errorf("Get() from journal error hook = (%#v, %v)", job, getErr)
				}
			}
			reported <- err
			panic("error hook panic must not kill worker")
		},
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-finalize-failure"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	first, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if completed := waitForState(t, manager, first.ID, StateCompleted); completed.Failure != nil {
		t.Fatalf("completed job gained journal failure: %#v", completed.Failure)
	}
	assertJournalError(t, <-reported, JournalOperationFinalize, first.ID, StateCompleted, journalCause)
	assertJournalError(t, manager.LastJournalError(), JournalOperationFinalize, first.ID, StateCompleted, journalCause)
	waitForJournalErrorHookIdle(t, manager)

	// The panicking observability hook did not kill the sole worker.
	second, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, second.ID, StateCompleted)
	assertJournalError(t, <-reported, JournalOperationFinalize, second.ID, StateCompleted, journalCause)
	if calls := finalizeCalls.Load(); calls != 2 {
		t.Fatalf("Finalize calls = %d, want one per job", calls)
	}
	_ = manager.Cancel(first.ID)
	if calls := finalizeCalls.Load(); calls != 2 {
		t.Fatalf("Finalize retried after failure; calls = %d", calls)
	}
}

func assertJournalError(t *testing.T, got error, operation JournalOperation, id string, state State, cause error) {
	t.Helper()
	var journalErr *JournalError
	if !errors.As(got, &journalErr) {
		t.Fatalf("error = %T %v, want *JournalError", got, got)
	}
	if journalErr.Operation != operation || journalErr.JobID != id || journalErr.State != state || !errors.Is(journalErr, cause) {
		t.Fatalf("journal error = %#v, want operation=%v id=%q state=%v cause=%v", journalErr, operation, id, state, cause)
	}
}

func waitForManagerClosed(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.RLock()
		closed := manager.closed
		manager.mu.RUnlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("manager did not begin closing")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForJournalErrorHookIdle(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(manager.journalErrorHookGate) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("journal error hook did not return")
		}
		time.Sleep(time.Millisecond)
	}
}
