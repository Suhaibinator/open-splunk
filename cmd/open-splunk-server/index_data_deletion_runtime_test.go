package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestNewIndexDataDeletionRuntimeStartsImmediateRecoveryScan(t *testing.T) {
	scanned := make(chan struct{}, 1)
	controlPlane := &runtimeDeletionControl{
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			scanned <- struct{}{}
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
	}
	store := &runtimeDeletionStore{}

	runtime, err := newIndexDataDeletionRuntime(
		controlPlane,
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := runtime.Close(context.Background()); closeErr != nil {
			t.Errorf("Close(): %v", closeErr)
		}
	})

	select {
	case <-scanned:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not perform its immediate recovery scan")
	}
}

func TestNewIndexDataDeletionRuntimeForwardsTenantAndErrorReporter(t *testing.T) {
	reported := make(chan error, 1)
	controlPlane := &runtimeDeletionControl{
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			return control.IndexDeletionOperation{
				ID:       "idxdel_operation",
				TenantID: "durable-tenant",
			}, nil
		},
	}
	store := &runtimeDeletionStore{}

	runtime, err := newIndexDataDeletionRuntime(
		controlPlane,
		store,
		"configured-tenant",
		func(err error) {
			reported <- err
		},
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := runtime.Close(context.Background()); closeErr != nil {
			t.Errorf("Close(): %v", closeErr)
		}
	})

	select {
	case reportErr := <-reported:
		if !strings.Contains(reportErr.Error(), "tenant") {
			t.Fatalf("reported error = %q, want tenant mismatch", reportErr)
		}
	case <-time.After(time.Second):
		t.Fatal("configured error reporter was not called")
	}
	if got := store.physicalCalls.Load(); got != 0 {
		t.Fatalf("physical ClickHouse calls = %d, want 0", got)
	}
}

func TestNewIndexDataDeletionRuntimeReturnsConstructionFailure(t *testing.T) {
	store := &runtimeDeletionStore{}

	runtime, err := newIndexDataDeletionRuntime(
		&runtimeDeletionControl{},
		store,
		"",
		nil,
	)
	if err == nil {
		if runtime != nil {
			_ = runtime.Close(context.Background())
		}
		t.Fatal("newIndexDataDeletionRuntime() accepted an empty tenant")
	}
	if runtime != nil {
		t.Fatalf("runtime = %#v after construction failure, want nil", runtime)
	}
	if got := store.closeCalls.Load(); got != 0 {
		t.Fatalf("Store.Close calls = %d after construction failure, want 0", got)
	}
}

func TestIndexDataDeletionRuntimeClosesCoordinatorBeforeStore(t *testing.T) {
	var mutex sync.Mutex
	var events []string
	record := func(event string) {
		mutex.Lock()
		defer mutex.Unlock()
		events = append(events, event)
	}
	started := make(chan struct{})
	controlPlane := &runtimeDeletionControl{
		next: func(ctx context.Context) (control.IndexDeletionOperation, error) {
			close(started)
			<-ctx.Done()
			record("coordinator")
			return control.IndexDeletionOperation{}, ctx.Err()
		},
	}
	store := &runtimeDeletionStore{
		close: func() error {
			record("store")
			return nil
		},
	}
	runtime, err := newIndexDataDeletionRuntime(
		controlPlane,
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not start")
	}

	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(events) != 2 || events[0] != "coordinator" || events[1] != "store" {
		t.Fatalf("close events = %v, want [coordinator store]", events)
	}
}

func TestIndexDataDeletionRuntimeCloseTimeoutLeavesStoreOpenUntilWorkerJoins(
	t *testing.T,
) {
	started := make(chan struct{})
	release := make(chan struct{})
	controlPlane := &runtimeDeletionControl{
		next: func(context.Context) (control.IndexDeletionOperation, error) {
			close(started)
			<-release
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
	}
	storeErr := errors.New("close store")
	store := &runtimeDeletionStore{
		close: func() error {
			return storeErr
		},
	}
	runtime, err := newIndexDataDeletionRuntime(
		controlPlane,
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not start")
	}

	closeContext, cancel := context.WithCancel(context.Background())
	cancel()
	closeErr := runtime.Close(closeContext)
	if !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("Close() error = %v, want context cancellation", closeErr)
	}
	if got := store.closeCalls.Load(); got != 0 {
		t.Fatalf("Store.Close calls = %d after timeout, want 0", got)
	}

	close(release)
	if closeErr := runtime.Close(context.Background()); !errors.Is(closeErr, storeErr) {
		t.Fatalf("second Close() error = %v, want persisted store failure", closeErr)
	}
	if got := store.closeCalls.Load(); got != 1 {
		t.Fatalf("Store.Close calls = %d after second close, want 1", got)
	}
}

func TestIndexDataDeletionRuntimeCloseDeadlineBoundsBlockingStoreClose(
	t *testing.T,
) {
	storeCloseStarted := make(chan struct{})
	releaseStoreClose := make(chan struct{})
	var releaseOnce sync.Once
	releaseStore := func() {
		releaseOnce.Do(func() {
			close(releaseStoreClose)
		})
	}
	t.Cleanup(releaseStore)

	storeErr := errors.New("close store")
	store := &runtimeDeletionStore{
		close: func() error {
			close(storeCloseStarted)
			<-releaseStoreClose
			return storeErr
		},
	}
	runtime, err := newIndexDataDeletionRuntime(
		&runtimeDeletionControl{},
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}

	closeContext, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	firstCloseDone := make(chan error, 1)
	go func() {
		firstCloseDone <- runtime.Close(closeContext)
	}()

	select {
	case <-storeCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("Store.Close did not start")
	}
	select {
	case closeErr := <-firstCloseDone:
		if !errors.Is(closeErr, context.DeadlineExceeded) {
			t.Fatalf(
				"first Close() error = %v, want context deadline",
				closeErr,
			)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close() ignored its deadline while Store.Close blocked")
	}

	secondCloseDone := make(chan error, 1)
	go func() {
		secondCloseDone <- runtime.Close(context.Background())
	}()
	select {
	case closeErr := <-secondCloseDone:
		t.Fatalf(
			"second Close() returned before Store.Close: %v",
			closeErr,
		)
	case <-time.After(20 * time.Millisecond):
	}

	releaseStore()
	select {
	case closeErr := <-secondCloseDone:
		if !errors.Is(closeErr, storeErr) {
			t.Fatalf(
				"second Close() error = %v, want persisted store failure",
				closeErr,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("second Close() did not observe completed Store.Close")
	}
	if closeErr := runtime.Close(context.Background()); !errors.Is(
		closeErr,
		storeErr,
	) {
		t.Fatalf(
			"third Close() error = %v, want persisted store failure",
			closeErr,
		)
	}
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if closeErr := runtime.Close(canceledContext); !errors.Is(
		closeErr,
		storeErr,
	) {
		t.Fatalf(
			"Close() after completion error = %v, want persisted store failure",
			closeErr,
		)
	}
	if got := store.closeCalls.Load(); got != 1 {
		t.Fatalf("Store.Close calls = %d, want exactly 1", got)
	}
}

func TestFinalizeIndexDataDeletionRuntimeRetainsDependenciesUntilJoined(
	t *testing.T,
) {
	var mutex sync.Mutex
	var events []string
	record := func(event string) {
		mutex.Lock()
		defer mutex.Unlock()
		events = append(events, event)
	}
	workerStarted := make(chan struct{})
	workerCanceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	controlPlane := &runtimeDeletionControl{
		next: func(ctx context.Context) (control.IndexDeletionOperation, error) {
			close(workerStarted)
			<-ctx.Done()
			close(workerCanceled)
			<-releaseWorker
			record("coordinator")
			return control.IndexDeletionOperation{}, control.ErrNotFound
		},
	}
	store := &runtimeDeletionStore{
		close: func() error {
			record("store")
			return nil
		},
	}
	runtime, err := newIndexDataDeletionRuntime(
		controlPlane,
		store,
		"tenant-a",
		nil,
	)
	if err != nil {
		t.Fatalf("newIndexDataDeletionRuntime(): %v", err)
	}
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not start")
	}

	finalizeDone := make(chan error, 1)
	go func() {
		closeErr := finalizeIndexDataDeletionRuntime(
			runtime,
			20*time.Millisecond,
		)
		record("sequencer")
		record("control")
		finalizeDone <- closeErr
	}()
	select {
	case <-workerCanceled:
	case <-time.After(time.Second):
		t.Fatal("finalizer did not cancel the coordinator worker")
	}
	select {
	case closeErr := <-finalizeDone:
		t.Fatalf(
			"finalizer returned before coordinator joined: %v",
			closeErr,
		)
	case <-time.After(50 * time.Millisecond):
	}
	mutex.Lock()
	earlyEvents := append([]string(nil), events...)
	mutex.Unlock()
	if len(earlyEvents) != 0 {
		t.Fatalf(
			"dependency close events before join = %v, want none",
			earlyEvents,
		)
	}

	close(releaseWorker)
	select {
	case closeErr := <-finalizeDone:
		if !errors.Is(closeErr, context.DeadlineExceeded) {
			t.Fatalf(
				"finalizer error = %v, want initial deadline",
				closeErr,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("finalizer did not finish after coordinator was released")
	}

	mutex.Lock()
	defer mutex.Unlock()
	want := []string{"coordinator", "store", "sequencer", "control"}
	if len(events) != len(want) {
		t.Fatalf("close events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("close events = %v, want %v", events, want)
		}
	}
}

type runtimeDeletionControl struct {
	next func(context.Context) (control.IndexDeletionOperation, error)
}

func (controlPlane *runtimeDeletionControl) NextIndexDeletionOperation(
	ctx context.Context,
) (control.IndexDeletionOperation, error) {
	if controlPlane.next == nil {
		return control.IndexDeletionOperation{}, control.ErrNotFound
	}
	return controlPlane.next(ctx)
}

func (*runtimeDeletionControl) GetIndexDeletionMutationAttempt(
	context.Context,
	string,
) (control.IndexDeletionMutationAttempt, error) {
	return control.IndexDeletionMutationAttempt{}, control.ErrNotFound
}

func (*runtimeDeletionControl) EnsureIndexDeletionMutationAttempt(
	context.Context,
	string,
	control.IndexDeletionMutationTarget,
) (control.IndexDeletionMutationAttempt, error) {
	return control.IndexDeletionMutationAttempt{}, errors.New(
		"unexpected EnsureIndexDeletionMutationAttempt call",
	)
}

func (*runtimeDeletionControl) CompleteIndexDataDeletion(
	context.Context,
	control.IndexDeletionMutationAttempt,
) (control.IndexDataDeletionCompletion, error) {
	return control.IndexDataDeletionCompletion{}, errors.New(
		"unexpected CompleteIndexDataDeletion call",
	)
}

func (*runtimeDeletionControl) GetIndexDataDeletionCompletion(
	context.Context,
	string,
) (control.IndexDataDeletionCompletion, error) {
	return control.IndexDataDeletionCompletion{}, control.ErrNotFound
}

type runtimeDeletionStore struct {
	physicalCalls atomic.Uint32
	closeCalls    atomic.Uint32
	close         func() error
}

func (store *runtimeDeletionStore) IndexDataDeletionStatus(
	context.Context,
	clickhouse.IndexDataDeletionRequest,
) (clickhouse.IndexDataDeletionProgress, error) {
	store.physicalCalls.Add(1)
	return clickhouse.IndexDataDeletionProgress{}, errors.New(
		"unexpected IndexDataDeletionStatus call",
	)
}

func (store *runtimeDeletionStore) WithWritesFrozen(
	context.Context,
	func(context.Context, clickhouse.FrozenWrites) error,
) error {
	store.physicalCalls.Add(1)
	return errors.New("unexpected WithWritesFrozen call")
}

func (store *runtimeDeletionStore) Close() error {
	store.closeCalls.Add(1)
	if store.close != nil {
		return store.close()
	}
	return nil
}
