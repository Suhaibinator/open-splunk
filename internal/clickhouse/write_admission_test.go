package clickhouse

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestWithWritesFrozenWaitsForAdmittedStoreAndBlocksLaterStore(t *testing.T) {
	t.Parallel()
	connection := &gatedStoreConnection{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	retention := &fakeRetentionProvider{
		periods: map[string]time.Duration{"main": time.Hour},
	}
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
	}
	store := mustTestStoreWithVisibility(t, connection, retention, sequencer)

	activeContext, cancelActive := context.WithCancel(context.Background())
	activeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(activeContext, validStoreBatch())
		activeDone <- err
	}()
	select {
	case <-connection.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Store did not reach ClickHouse")
	}

	freezeEntered := make(chan struct{})
	releaseFreeze := make(chan struct{})
	freezeDone := make(chan error, 1)
	go func() {
		freezeDone <- store.WithWritesFrozen(
			context.Background(),
			func(_ context.Context, _ FrozenWrites) error {
				close(freezeEntered)
				<-releaseFreeze
				return nil
			},
		)
	}()
	waitForWriteFreezeWaiter(t, store)

	laterContext, cancelLater := context.WithCancel(context.Background())
	laterDone := make(chan error, 1)
	go func() {
		batch := distinctStoreBatch("later-frozen", 2)
		_, err := store.Store(laterContext, batch)
		laterDone <- err
	}()
	assertNoErrorResult(t, laterDone, "later Store bypassed the queued write freeze")

	cancelActive()
	if err := <-activeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Store error = %v, want context.Canceled", err)
	}
	select {
	case <-freezeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("write freeze did not enter after the admitted Store stopped")
	}
	assertNoErrorResult(t, laterDone, "later Store ran while writes were frozen")

	cancelLater()
	if err := <-laterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("later Store error = %v, want context.Canceled", err)
	}
	close(releaseFreeze)
	if err := <-freezeDone; err != nil {
		t.Fatalf("WithWritesFrozen() error = %v", err)
	}
	if len(retention.calls) != 1 || len(sequencer.reserveKeys) != 1 {
		t.Fatalf(
			"blocked Store side effects: retention=%v reservations=%v",
			retention.calls,
			sequencer.reserveKeys,
		)
	}
}

func TestCanceledWriteFreezeDoesNotStrandFollowingWriters(t *testing.T) {
	t.Parallel()
	admission := newWriteAdmission()
	if err := admission.enter(context.Background()); err != nil {
		t.Fatal(err)
	}

	freezeContext, cancelFreeze := context.WithCancel(context.Background())
	freezeDone := make(chan error, 1)
	go func() {
		freezeDone <- admission.freeze(freezeContext)
	}()
	waitForAdmissionFreezeWaiter(t, admission)
	cancelFreeze()
	if err := <-freezeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("freeze error = %v, want context.Canceled", err)
	}

	admission.leave()
	nextContext, cancelNext := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelNext()
	if err := admission.enter(nextContext); err != nil {
		t.Fatalf("writer after canceled freeze: %v", err)
	}
	admission.leave()
}

func TestFrozenWritesBlockStoreResumeAndReconciliationBeforeSideEffects(t *testing.T) {
	t.Parallel()
	connection := &fakeStoreConnection{}
	retention := &fakeRetentionProvider{
		periods: map[string]time.Duration{"main": time.Hour},
	}
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
	}
	store := mustTestStoreWithVisibility(t, connection, retention, sequencer)
	batch := validStoreBatch()
	rejected := validStoreBatchRejection()
	identity := ingest.StoreBatchIdentity{
		TenantID:          batch.TenantID,
		CollectorID:       batch.CollectorID,
		BatchID:           batch.BatchID,
		BatchSequence:     batch.BatchSequence,
		SourceBatchSHA256: batch.SourceBatchSHA256,
	}

	err := store.WithWritesFrozen(
		context.Background(),
		func(_ context.Context, _ FrozenWrites) error {
			tests := []struct {
				name string
				call func(context.Context) error
			}{
				{
					name: "Store",
					call: func(ctx context.Context) error {
						_, err := store.Store(ctx, batch)
						return err
					},
				},
				{
					name: "ResumeBatch",
					call: func(ctx context.Context) error {
						_, err := store.ResumeBatch(ctx, identity)
						return err
					},
				},
				{
					name: "RejectBatch",
					call: func(ctx context.Context) error {
						_, err := store.RejectBatch(ctx, rejected)
						return err
					},
				},
				{name: "ReconcilePending", call: store.ReconcilePending},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					done := make(chan error, 1)
					go func() { done <- test.call(ctx) }()
					assertNoErrorResult(t, done, test.name+" bypassed frozen write admission")
					cancel()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Fatalf("%s error = %v, want context.Canceled", test.name, err)
					}
				})
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("WithWritesFrozen() error = %v", err)
	}
	if sequencer.lookupCalls != 0 || sequencer.acquireCalls != 0 ||
		len(sequencer.reserveKeys) != 0 || len(retention.calls) != 0 ||
		connection.prepareCalls != 0 {
		t.Fatalf(
			"frozen dependency calls lookup=%d acquire=%d reserve=%v retention=%v prepare=%d",
			sequencer.lookupCalls,
			sequencer.acquireCalls,
			sequencer.reserveKeys,
			retention.calls,
			connection.prepareCalls,
		)
	}
}

func TestFrozenCallbackContextRejectsReentrantWriters(t *testing.T) {
	t.Parallel()
	connection := &fakeStoreConnection{}
	sequencer := &fakeVisibilitySequencer{}
	store := mustTestStoreWithVisibility(
		t,
		connection,
		fixedRetention(time.Hour),
		sequencer,
	)
	otherConnection := &fakeStoreConnection{}
	otherSequencer := &fakeVisibilitySequencer{}
	otherStore := mustTestStoreWithVisibility(
		t,
		otherConnection,
		fixedRetention(time.Hour),
		otherSequencer,
	)
	batch := validStoreBatch()
	rejected := validStoreBatchRejection()
	identity := ingest.StoreBatchIdentity{
		TenantID:          batch.TenantID,
		CollectorID:       batch.CollectorID,
		BatchID:           batch.BatchID,
		BatchSequence:     batch.BatchSequence,
		SourceBatchSHA256: batch.SourceBatchSHA256,
	}

	if err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			if _, err := store.Store(ctx, batch); !errors.Is(err, ErrWriteFreezeReentrant) {
				t.Fatalf("reentrant Store error = %v, want ErrWriteFreezeReentrant", err)
			}
			if _, err := store.RejectBatch(ctx, rejected); !errors.Is(err, ErrWriteFreezeReentrant) {
				t.Fatalf("reentrant RejectBatch error = %v, want ErrWriteFreezeReentrant", err)
			}
			if _, err := store.ResumeBatch(ctx, identity); !errors.Is(err, ErrWriteFreezeReentrant) {
				t.Fatalf("reentrant ResumeBatch error = %v, want ErrWriteFreezeReentrant", err)
			}
			if err := store.ReconcilePending(ctx); !errors.Is(err, ErrWriteFreezeReentrant) {
				t.Fatalf("reentrant ReconcilePending error = %v, want ErrWriteFreezeReentrant", err)
			}
			if err := store.WithWritesFrozen(
				ctx,
				func(context.Context, FrozenWrites) error { return nil },
			); !errors.Is(err, ErrWriteFreezeReentrant) {
				t.Fatalf("nested freeze error = %v, want ErrWriteFreezeReentrant", err)
			}
			if err := otherStore.WithWritesFrozen(
				ctx,
				func(innerContext context.Context, _ FrozenWrites) error {
					if _, err := store.Store(innerContext, batch); !errors.Is(err, ErrWriteFreezeReentrant) {
						t.Fatalf(
							"outer Store through inner context error = %v, want ErrWriteFreezeReentrant",
							err,
						)
					}
					if _, err := otherStore.Store(innerContext, batch); !errors.Is(err, ErrWriteFreezeReentrant) {
						t.Fatalf(
							"inner Store through inner context error = %v, want ErrWriteFreezeReentrant",
							err,
						)
					}
					if _, err := otherStore.RejectBatch(innerContext, rejected); !errors.Is(err, ErrWriteFreezeReentrant) {
						t.Fatalf(
							"inner RejectBatch through inner context error = %v, want ErrWriteFreezeReentrant",
							err,
						)
					}
					return nil
				},
			); err != nil {
				t.Fatalf("different-Store nested freeze error = %v", err)
			}
			return frozen.DrainPending(ctx)
		},
	); err != nil {
		t.Fatalf("outer WithWritesFrozen() error = %v", err)
	}
	if sequencer.lookupCalls != 0 || sequencer.acquireCalls != 1 ||
		len(sequencer.reserveKeys) != 0 || connection.prepareCalls != 0 {
		t.Fatalf(
			"reentrant dependency calls lookup=%d acquire=%d reserve=%v prepare=%d",
			sequencer.lookupCalls,
			sequencer.acquireCalls,
			sequencer.reserveKeys,
			connection.prepareCalls,
		)
	}
	if otherSequencer.lookupCalls != 0 || len(otherSequencer.reserveKeys) != 0 ||
		otherConnection.prepareCalls != 0 {
		t.Fatalf(
			"nested Store dependency calls lookup=%d reserve=%v prepare=%d",
			otherSequencer.lookupCalls,
			otherSequencer.reserveKeys,
			otherConnection.prepareCalls,
		)
	}
}

func TestFrozenDrainReconcilesAmbiguousReservationAndProvesEmpty(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
	}
	connection := &fakeStoreConnection{
		batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF},
	}
	store := mustTestStoreWithVisibility(
		t,
		connection,
		fixedRetention(time.Hour),
		sequencer,
	)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("ambiguous Store error = %v, want transient", err)
	}

	connection.batch = &fakeWriteBatch{}
	<-store.reconcileSlot
	defer func() { store.reconcileSlot <- struct{}{} }()
	var escaped FrozenWrites
	if err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			escaped = frozen
			return frozen.DrainPending(ctx)
		},
	); err != nil {
		t.Fatalf("frozen drain: %v", err)
	}
	if !sequencer.reservation.AlreadyCommitted || sequencer.pendingCalls != 1 {
		t.Fatalf(
			"drain state committed=%v pending-usage calls=%d",
			sequencer.reservation.AlreadyCommitted,
			sequencer.pendingCalls,
		)
	}
	if connection.batch.sendCalls != 1 || len(connection.batch.rows) != 1 {
		t.Fatalf(
			"replayed ClickHouse batch sends=%d rows=%d",
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	if err := escaped.DrainPending(context.Background()); !errors.Is(err, ErrWriteFreezeInactive) {
		t.Fatalf("escaped DrainPending() error = %v, want ErrWriteFreezeInactive", err)
	}
}

func TestConcurrentFrozenDrainWaitIsContextCancelable(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
	}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}},
		fixedRetention(time.Hour),
		sequencer,
	)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("ambiguous Store error = %v, want transient", err)
	}
	gated := &gatedStoreConnection{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	store.connection = gated

	if err := store.WithWritesFrozen(
		context.Background(),
		func(_ context.Context, frozen FrozenWrites) error {
			firstContext, cancelFirst := context.WithCancel(context.Background())
			firstDone := make(chan error, 1)
			go func() { firstDone <- frozen.DrainPending(firstContext) }()
			select {
			case <-gated.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first frozen drain did not reach ClickHouse")
			}

			secondContext, cancelSecond := context.WithCancel(context.Background())
			secondDone := make(chan error, 1)
			go func() { secondDone <- frozen.DrainPending(secondContext) }()
			cancelSecond()
			select {
			case err := <-secondDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("second DrainPending error = %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("second DrainPending ignored cancellation while waiting")
			}

			cancelFirst()
			if err := <-firstDone; !errors.Is(err, context.Canceled) {
				t.Fatalf("first DrainPending error = %v, want context.Canceled", err)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("WithWritesFrozen() error = %v", err)
	}
}

func TestFrozenDrainFailsClosedWhenPendingReservationIsNotAcquirable(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		acquireBlocked: true,
		pendingUsage: &visibility.PendingUsage{
			Reservations: 1,
			OutboxBytes:  17,
		},
	}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			return frozen.DrainPending(ctx)
		},
	)
	if !errors.Is(err, ErrPendingOutboxNotDrained) {
		t.Fatalf("frozen drain error = %v, want ErrPendingOutboxNotDrained", err)
	}
	if !strings.Contains(err.Error(), "1 reservation") || !strings.Contains(err.Error(), "17 bytes") {
		t.Fatalf("frozen drain error lacks pending usage: %v", err)
	}
	if sequencer.pruneLimit != 0 {
		t.Fatalf("unsafe drain pruned terminal rows with pending usage: limit=%d", sequencer.pruneLimit)
	}
}

func TestFrozenDrainFailsClosedBeyondVisibilityCapacity(t *testing.T) {
	t.Parallel()
	baseSequencer := &fakeVisibilitySequencer{}
	sequencer := &overflowVisibilitySequencer{
		fakeVisibilitySequencer: baseSequencer,
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(
		t,
		connection,
		fixedRetention(time.Hour),
		sequencer,
	)
	batch := validStoreBatch()
	rows, err := store.rowsForBatch(context.Background(), batch, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeReservationMetadata(rows, batch)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := encodeStoreOutbox(batch)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	sequencer.template = visibility.Reservation{
		BatchKey:      deduplicationToken(batch),
		SequenceKey:   sequenceIdentityKey(batch),
		PayloadSHA256: digest,
		IndexTime:     batch.ReceivedAt,
		Metadata:      metadata,
		Outbox:        outbox,
	}

	err = store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			return frozen.DrainPending(ctx)
		},
	)
	if !errors.Is(err, ErrPendingOutboxNotDrained) {
		t.Fatalf("over-capacity drain error = %v, want ErrPendingOutboxNotDrained", err)
	}
	if sequencer.acquired != visibility.MaxPendingReservations+1 {
		t.Fatalf(
			"acquired reservations = %d, want %d",
			sequencer.acquired,
			visibility.MaxPendingReservations+1,
		)
	}
	if connection.batch.sendCalls != int(visibility.MaxPendingReservations) {
		t.Fatalf(
			"ClickHouse sends = %d, want bounded %d",
			connection.batch.sendCalls,
			visibility.MaxPendingReservations,
		)
	}
	if len(baseSequencer.released) != 1 ||
		baseSequencer.released[0] != uint64(visibility.MaxPendingReservations+1) {
		t.Fatalf("over-capacity lease releases = %v", baseSequencer.released)
	}
}

func TestCloseCancelsAndWaitsForAdmittedStoreBeforeClosingConnection(t *testing.T) {
	t.Parallel()
	connection := &gatedStoreConnection{
		entered:          make(chan struct{}),
		resume:           make(chan struct{}),
		exited:           make(chan struct{}),
		closed:           make(chan struct{}),
		closedBeforeExit: make(chan struct{}),
	}
	store := mustTestStore(t, connection, fixedRetention(time.Hour))
	storeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(context.Background(), validStoreBatch())
		storeDone <- err
	}()
	select {
	case <-connection.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Store did not reach ClickHouse")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case <-connection.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the admitted Store")
	}
	if err := <-storeDone; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store error during Close = %v, want ErrStoreClosed", err)
	}
	select {
	case <-connection.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not close after the Store stopped")
	}
	select {
	case <-connection.closedBeforeExit:
		t.Fatal("connection closed before the admitted Store left ClickHouse")
	default:
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}

	if _, err := store.Store(context.Background(), validStoreBatch()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store after Close error = %v, want ErrStoreClosed", err)
	}
	if _, err := store.RejectBatch(context.Background(), validStoreBatchRejection()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("RejectBatch after Close error = %v, want ErrStoreClosed", err)
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("ReconcilePending after Close error = %v, want ErrStoreClosed", err)
	}
	callbackCalled := false
	err := store.WithWritesFrozen(
		context.Background(),
		func(context.Context, FrozenWrites) error {
			callbackCalled = true
			return nil
		},
	)
	if !errors.Is(err, ErrStoreClosed) || callbackCalled {
		t.Fatalf(
			"WithWritesFrozen after Close error=%v callbackCalled=%v, want ErrStoreClosed without callback",
			err,
			callbackCalled,
		)
	}
	if err := store.Ping(context.Background()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Ping after Close error = %v, want ErrStoreClosed", err)
	}
}

func TestCloseCancelsQueuedWriteFreezeWithoutInvokingCallback(t *testing.T) {
	t.Parallel()
	connection := &gatedStoreConnection{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
		exited:  make(chan struct{}),
		closed:  make(chan struct{}),
	}
	store := mustTestStore(t, connection, fixedRetention(time.Hour))
	storeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(context.Background(), validStoreBatch())
		storeDone <- err
	}()
	select {
	case <-connection.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Store did not reach ClickHouse")
	}

	callbackCalled := false
	freezeDone := make(chan error, 1)
	go func() {
		freezeDone <- store.WithWritesFrozen(
			context.Background(),
			func(context.Context, FrozenWrites) error {
				callbackCalled = true
				return nil
			},
		)
	}()
	waitForWriteFreezeWaiter(t, store)

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	if err := <-storeDone; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store error during Close = %v, want ErrStoreClosed", err)
	}
	if err := <-freezeDone; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("queued freeze error during Close = %v, want ErrStoreClosed", err)
	}
	if callbackCalled {
		t.Fatal("queued write-freeze callback ran during Close")
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func waitForWriteFreezeWaiter(t *testing.T, store *Store) {
	t.Helper()
	waitForAdmissionFreezeWaiter(t, store.writeAdmission)
}

func waitForAdmissionFreezeWaiter(t *testing.T, admission *writeAdmission) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for admission.waitingFreezes() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("write freeze did not enter the admission queue")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertNoErrorResult(t *testing.T, result <-chan error, failure string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s: %v", failure, err)
	case <-time.After(25 * time.Millisecond):
	}
}

func distinctStoreBatch(label string, sequence uint64) ingest.StoreBatch {
	batch := validStoreBatch()
	batch.BatchID = label
	batch.BatchSequence = sequence
	batch.SourceBatchSHA256 = testSourceBatchDigest(label)
	for _, event := range batch.Events {
		event.BatchID = label
	}
	return batch
}

type overflowVisibilitySequencer struct {
	*fakeVisibilitySequencer
	template visibility.Reservation
	acquired uint32
}

func (sequencer *overflowVisibilitySequencer) AcquirePending(
	_ context.Context,
	_ string,
) (visibility.Reservation, bool, error) {
	sequencer.acquired++
	reservation := sequencer.template
	reservation.Sequence = uint64(sequencer.acquired)
	reservation.PreviouslyReserved = true
	return reservation, true, nil
}
