package clickhouse

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestSeparateDeletionConnectionRoutesNativeOperations(t *testing.T) {
	t.Parallel()

	t.Run("status", func(t *testing.T) {
		t.Parallel()

		base := newMutationScriptConnection()
		deletion := newMutationScriptConnection()
		deletion.targets = []fakeMutationTarget{validFakeMutationTarget()}
		deletion.summaries = []fakeMutationSummary{{
			matching: 1,
			pending:  1,
		}}
		store := mustTestStoreWithDeletionConnection(t, base, deletion)

		progress, err := store.IndexDataDeletionStatus(
			context.Background(),
			validIndexDataDeletionRequest(),
		)
		if err != nil {
			t.Fatalf("IndexDataDeletionStatus(): %v", err)
		}
		if progress.State != IndexDataDeletionPending {
			t.Fatalf("progress state = %v, want pending", progress.State)
		}
		if deletion.summaryCalls != 1 {
			t.Fatalf("deletion summary calls = %d, want 1", deletion.summaryCalls)
		}
		assertNoNativeDeletionCalls(t, base)
	})

	t.Run("target", func(t *testing.T) {
		t.Parallel()

		base := newMutationScriptConnection()
		deletion := newMutationScriptConnection()
		deletion.targets = []fakeMutationTarget{validFakeMutationTarget()}
		store := mustTestStoreWithDeletionConnection(t, base, deletion)

		err := store.WithWritesFrozen(
			context.Background(),
			func(ctx context.Context, frozen FrozenWrites) error {
				if err := frozen.DrainPending(ctx); err != nil {
					return err
				}
				_, err := frozen.IndexDataDeletionTarget(ctx)
				return err
			},
		)
		if err != nil {
			t.Fatalf("IndexDataDeletionTarget(): %v", err)
		}
		if deletion.targetCalls != 1 {
			t.Fatalf("deletion target calls = %d, want 1", deletion.targetCalls)
		}
		assertNoNativeDeletionCalls(t, base)
	})

	t.Run("advance", func(t *testing.T) {
		t.Parallel()

		base := newMutationScriptConnection()
		deletion := newMutationScriptConnection()
		deletion.targets = []fakeMutationTarget{
			validFakeMutationTarget(),
			validFakeMutationTarget(),
			validFakeMutationTarget(),
		}
		deletion.summaries = []fakeMutationSummary{
			{},
			{matching: 1, pending: 1, latestBlock: 1},
		}
		deletion.existence = []uint64{1}
		store := mustTestStoreWithDeletionConnection(t, base, deletion)

		progress := advanceIndexDataDeletionForTest(
			t,
			store,
			validIndexDataDeletionRequest(),
		)
		if progress.State != IndexDataDeletionPending ||
			!progress.SubmissionAttempted ||
			!progress.SubmissionAccepted {
			t.Fatalf("progress = %#v", progress)
		}
		if deletion.execCalls != 1 ||
			deletion.summaryCalls != 2 ||
			deletion.physicalProofCalls != 1 {
			t.Fatalf(
				"deletion connection calls: exec=%d summary=%d physical=%d",
				deletion.execCalls,
				deletion.summaryCalls,
				deletion.physicalProofCalls,
			)
		}
		assertNoNativeDeletionCalls(t, base)
	})

	t.Run("ordinary write", func(t *testing.T) {
		t.Parallel()

		base := newMutationScriptConnection()
		base.batch = &fakeWriteBatch{}
		deletion := newMutationScriptConnection()
		store := mustTestStoreWithDeletionConnection(t, base, deletion)

		if _, err := store.Store(
			context.Background(),
			validStoreBatch(),
		); err != nil {
			t.Fatalf("Store(): %v", err)
		}
		if base.prepareCalls != 1 {
			t.Fatalf("base prepare calls = %d, want 1", base.prepareCalls)
		}
		if deletion.prepareCalls != 0 {
			t.Fatalf(
				"deletion prepare calls = %d, want 0",
				deletion.prepareCalls,
			)
		}
	})
}

func TestSeparateDeletionConnectionPingAndCloseLifecycle(t *testing.T) {
	t.Parallel()

	basePingErr := errors.New("base ping failed")
	deletionPingErr := errors.New("deletion ping failed")
	baseCloseErr := errors.New("base close failed")
	deletionCloseErr := errors.New("deletion close failed")
	var lifecycle []string
	base := newObservedStoreConnection("base", &lifecycle)
	base.pingErr = basePingErr
	base.closeErr = baseCloseErr
	deletion := newObservedStoreConnection("deletion", &lifecycle)
	deletion.pingErr = deletionPingErr
	deletion.closeErr = deletionCloseErr
	store := mustTestStoreWithDeletionConnection(t, base, deletion)

	err := store.Ping(context.Background())
	if !errors.Is(err, basePingErr) || !errors.Is(err, deletionPingErr) {
		t.Fatalf("Ping() error = %v, want both connection errors", err)
	}
	if base.pingCalls != 1 || deletion.pingCalls != 1 {
		t.Fatalf(
			"Ping calls base/deletion = %d/%d, want 1/1",
			base.pingCalls,
			deletion.pingCalls,
		)
	}

	err = store.Close()
	if !errors.Is(err, baseCloseErr) || !errors.Is(err, deletionCloseErr) {
		t.Fatalf("Close() error = %v, want both connection errors", err)
	}
	if !slices.Equal(lifecycle, []string{"close deletion", "close base"}) {
		t.Fatalf("close order = %v", lifecycle)
	}
	if base.closeCalls != 1 || deletion.closeCalls != 1 {
		t.Fatalf(
			"Close calls base/deletion = %d/%d, want 1/1",
			base.closeCalls,
			deletion.closeCalls,
		)
	}
	if secondErr := store.Close(); !errors.Is(secondErr, baseCloseErr) ||
		!errors.Is(secondErr, deletionCloseErr) ||
		base.closeCalls != 1 ||
		deletion.closeCalls != 1 {
		t.Fatalf(
			"second Close() error=%v calls=%d/%d",
			secondErr,
			base.closeCalls,
			deletion.closeCalls,
		)
	}
}

func TestSeparateDeletionConnectionCloseWaitsForDeletionOperation(t *testing.T) {
	t.Parallel()

	var lifecycle []string
	base := newObservedStoreConnection("base", &lifecycle)
	deletion := &blockingDeletionStoreConnection{
		observedStoreConnection: newObservedStoreConnection(
			"deletion",
			&lifecycle,
		),
		entered:  make(chan struct{}),
		canceled: make(chan struct{}),
		resume:   make(chan struct{}),
	}
	store := mustTestStoreWithDeletionConnection(t, base, deletion)

	statusDone := make(chan error, 1)
	go func() {
		_, err := store.IndexDataDeletionStatus(
			context.Background(),
			validIndexDataDeletionRequest(),
		)
		statusDone <- err
	}()
	select {
	case <-deletion.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("deletion operation did not reach ClickHouse")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()
	select {
	case <-deletion.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel the admitted deletion operation")
	}
	if base.closeCalls != 0 || deletion.closeCalls != 0 ||
		len(lifecycle) != 0 {
		t.Fatalf(
			"connections closed before deletion operation joined: calls=%d/%d order=%v",
			base.closeCalls,
			deletion.closeCalls,
			lifecycle,
		)
	}

	close(deletion.resume)
	select {
	case err := <-statusDone:
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("IndexDataDeletionStatus() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deletion operation did not finish")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close(): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish")
	}
	if !slices.Equal(lifecycle, []string{"close deletion", "close base"}) {
		t.Fatalf("close order = %v", lifecycle)
	}
}

func TestSeparateDeletionConnectionConstructorFailureKeepsCallerOwnership(
	t *testing.T,
) {
	t.Parallel()

	var lifecycle []string
	base := newObservedStoreConnection("base", &lifecycle)
	deletion := newObservedStoreConnection("deletion", &lifecycle)

	store, err := newStoreWithDeletionConnection(
		base,
		deletion,
		"open_splunk",
		"events",
		nil,
		&fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 1},
		},
		time.Now,
		time.Second,
	)
	if err == nil || store != nil {
		t.Fatalf("newStoreWithDeletionConnection() = %#v, %v", store, err)
	}
	if base.closeCalls != 0 || deletion.closeCalls != 0 ||
		len(lifecycle) != 0 {
		t.Fatalf(
			"constructor failure took caller ownership: calls=%d/%d order=%v",
			base.closeCalls,
			deletion.closeCalls,
			lifecycle,
		)
	}
}

func mustTestStoreWithDeletionConnection(
	t *testing.T,
	base storeConnection,
	deletion storeConnection,
) *Store {
	t.Helper()
	store, err := newStoreWithDeletionConnection(
		base,
		deletion,
		"open_splunk",
		"events",
		fixedRetention(time.Hour),
		&fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 1},
		},
		time.Now,
		time.Second,
	)
	if err != nil {
		t.Fatalf("newStoreWithDeletionConnection(): %v", err)
	}
	return store
}

func assertNoNativeDeletionCalls(
	t *testing.T,
	connection *mutationScriptConnection,
) {
	t.Helper()
	if connection.execCalls != 0 ||
		connection.targetCalls != 0 ||
		connection.summaryCalls != 0 ||
		connection.existenceCalls != 0 ||
		connection.physicalProofCalls != 0 {
		t.Fatalf(
			"base connection received deletion calls: exec=%d target=%d summary=%d existence=%d physical=%d",
			connection.execCalls,
			connection.targetCalls,
			connection.summaryCalls,
			connection.existenceCalls,
			connection.physicalProofCalls,
		)
	}
}

type observedStoreConnection struct {
	*fakeStoreConnection

	name      string
	lifecycle *[]string
	pingCalls int
	pingErr   error
	closeErr  error
}

func newObservedStoreConnection(
	name string,
	lifecycle *[]string,
) *observedStoreConnection {
	return &observedStoreConnection{
		fakeStoreConnection: &fakeStoreConnection{},
		name:                name,
		lifecycle:           lifecycle,
	}
}

func (connection *observedStoreConnection) Ping(context.Context) error {
	connection.pingCalls++
	return connection.pingErr
}

func (connection *observedStoreConnection) Close() error {
	connection.closeCalls++
	*connection.lifecycle = append(
		*connection.lifecycle,
		"close "+connection.name,
	)
	return connection.closeErr
}

type blockingDeletionStoreConnection struct {
	*observedStoreConnection

	entered  chan struct{}
	canceled chan struct{}
	resume   chan struct{}
}

func (connection *blockingDeletionStoreConnection) queryRow(
	ctx context.Context,
	_ string,
	_ clickhousedriver.Parameters,
) storeQueryRow {
	return &blockingDeletionStoreQueryRow{
		ctx:      ctx,
		entered:  connection.entered,
		canceled: connection.canceled,
		resume:   connection.resume,
	}
}

type blockingDeletionStoreQueryRow struct {
	ctx      context.Context
	entered  chan struct{}
	canceled chan struct{}
	resume   chan struct{}
}

func (row *blockingDeletionStoreQueryRow) Scan(...any) error {
	close(row.entered)
	<-row.ctx.Done()
	close(row.canceled)
	<-row.resume
	return row.ctx.Err()
}
