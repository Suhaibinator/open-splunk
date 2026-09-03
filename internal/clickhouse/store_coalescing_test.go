package clickhouse

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestGroupedStoreStagesWaitsAndCoalescesOrderedLogicalBatches(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	store.writeGroupLimits.TargetRows = 2
	store.writeGroupLimits.TargetDecodedBytes = visibility.MaxWriteGroupDecodedBytes

	first := distinctStoreBatch("group-first", 1)
	first.Events[0].Event.EventId = "event-first"
	second := distinctStoreBatch("group-second", 2)
	second.Events[0].Event.EventId = "event-second"
	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	results := make(chan outcome, 2)
	go func() {
		result, err := store.Store(context.Background(), first)
		results <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, sequencer, 1)
	go func() {
		result, err := store.Store(context.Background(), second)
		results <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, sequencer, 2)
	if got := store.commitWaiters.size(); got != 2 {
		t.Fatalf("native waiter count = %d, want 2", got)
	}
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("ReconcilePending: %v", err)
	}

	for range 2 {
		outcome := <-results
		if outcome.err != nil || outcome.result.Accepted != 1 || outcome.result.Duplicate != 0 {
			t.Fatalf("Store outcome = %+v error=%v", outcome.result, outcome.err)
		}
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 2 {
		t.Fatalf(
			"physical shape prepares=%d sends=%d rows=%d, want 1/1/2",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	if got := []string{
		connection.batch.rows[0][0].(string),
		connection.batch.rows[1][0].(string),
	}; !slices.Equal(got, []string{"event-first", "event-second"}) {
		t.Fatalf("physical event order = %v", got)
	}
	if token, _ := connection.settings["insert_deduplication_token"].(string); token == "" ||
		token == deduplicationToken(first) || token == deduplicationToken(second) {
		t.Fatalf("physical deduplication token = %q, want stable group identity", token)
	}
	if connection.settings["async_insert"] != uint8(0) ||
		connection.settings["wait_for_async_insert"] != uint8(1) {
		t.Fatalf("physical insert settings = %+v", connection.settings)
	}
	if got := store.commitWaiters.size(); got != 0 {
		t.Fatalf("native waiter count after commit = %d, want 0", got)
	}
	telemetry := store.HECReconciliationTelemetry()
	if telemetry.StagedLogicalBatches != 2 || telemetry.StagedLogicalRows != 2 ||
		telemetry.FormedGroups != 1 || telemetry.PhysicalSends != 1 ||
		telemetry.SuccessfulGroups != 1 || telemetry.GroupMemberBatches != 2 ||
		telemetry.GroupRows != 2 || telemetry.GroupDecodedBytes == 0 ||
		telemetry.GroupMonthlyPartitions != 1 || telemetry.FillRowTarget != 1 ||
		telemetry.NativeWaiters != 0 || telemetry.NativeWaiterWakeups != 2 ||
		telemetry.NativeWaiterCancellations != 0 || telemetry.NativeTerminalLookups != 2 ||
		histogramObservations(telemetry.SealLatency) != 1 ||
		histogramObservations(telemetry.SendLatency) != 1 ||
		histogramObservations(telemetry.CommitLatency) != 1 {
		t.Fatalf("coalescing telemetry = %+v", telemetry)
	}
}

func TestGroupedStoreCancellationLeavesDurableReservationForReconciliation(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.Store(ctx, distinctStoreBatch("cancel-wait", 1))
		done <- err
	}()
	waitForPendingReservations(t, sequencer, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Store error = %v, want context.Canceled", err)
	}
	if got := store.commitWaiters.size(); got != 0 {
		t.Fatalf("waiters after cancellation = %d, want 0", got)
	}
	if got := store.HECReconciliationTelemetry().NativeWaiterCancellations; got != 1 {
		t.Fatalf("native waiter cancellations = %d, want 1", got)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || usage.Reservations != 1 {
		t.Fatalf("pending usage after cancellation = %+v error=%v", usage, err)
	}
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("reconcile canceled native request: %v", err)
	}
	if connection.batch.sendCalls != 1 {
		t.Fatalf("physical sends = %d, want 1", connection.batch.sendCalls)
	}
}

func TestGroupedStorePollsDurableOutcomeWhenCommitWakeIsLost(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{batch: &fakeWriteBatch{}},
		fixedRetention(time.Hour),
		sequencer,
	)
	store.coalescing = true
	store.retryAfter = 10 * time.Millisecond
	batch := distinctStoreBatch("lost-commit-wake", 1)
	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.Store(context.Background(), batch)
		done <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, sequencer, 1)

	attemptID := strings.Repeat("w", 32)
	limits := store.writeGroupLimits
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(),
		attemptID,
		limits,
		time.Now().UTC(),
	)
	if err != nil || !found {
		t.Fatalf("FormOrAcquireWriteGroup found=%v error=%v", found, err)
	}
	if err := sequencer.MarkWriteGroupSending(context.Background(), group.ID, attemptID); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.CommitWriteGroup(context.Background(), group.ID, attemptID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.result.Accepted != 1 || outcome.result.Duplicate != 0 {
			t.Fatalf("Store outcome = %+v error=%v", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Store did not observe the durable commit without a process-local wake")
	}
	if got := store.commitWaiters.size(); got != 0 {
		t.Fatalf("native waiter count after durable poll = %d, want 0", got)
	}
}

func histogramObservations(snapshot CoalescingDurationHistogramSnapshot) uint64 {
	var total uint64
	for _, count := range snapshot {
		total += count
	}
	return total
}

func TestWriteGroupCorruptedMemberOutboxPreventsPhysicalSend(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	staged, err := store.Stage(context.Background(), distinctStoreBatch("corrupt-group", 1))
	if err != nil || staged.State != ingest.StoredBatchPending {
		t.Fatalf("Stage = %+v error=%v", staged, err)
	}
	attemptID := strings.Repeat("a", 32)
	limits := store.writeGroupLimits
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(),
		attemptID,
		limits,
		time.Now().UTC(),
	)
	if err != nil || !found {
		t.Fatalf("FormOrAcquireWriteGroup found=%v error=%v", found, err)
	}
	group.Members[0].Reservation.Outbox[0] ^= 0xff
	if err := store.writeGroup(context.Background(), group, attemptID); err == nil ||
		!strings.Contains(err.Error(), "outbox digest") {
		t.Fatalf("corrupt writeGroup error = %v", err)
	}
	if connection.prepareCalls != 0 || connection.batch.sendCalls != 0 {
		t.Fatalf("corrupt group touched ClickHouse: prepares=%d sends=%d", connection.prepareCalls, connection.batch.sendCalls)
	}
}

func TestWriteGroupSendAmbiguityReplaysSameRowsAndToken(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	firstPhysical := &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}
	connection := &fakeStoreConnection{batch: firstPhysical}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	first := distinctStoreBatch("ambiguous-first", 1)
	first.Events[0].Event.EventId = "ambiguous-event-first"
	second := distinctStoreBatch("ambiguous-second", 2)
	second.Events[0].Event.EventId = "ambiguous-event-second"
	for _, batch := range []ingest.StoreBatch{first, second} {
		if _, err := store.Stage(context.Background(), batch); err != nil {
			t.Fatalf("Stage(%s): %v", batch.BatchID, err)
		}
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first ReconcilePending error = %v, want ambiguous send", err)
	}
	firstToken := connection.settings["insert_deduplication_token"]
	firstRows := firstPhysical.rows
	if firstPhysical.sendCalls != 1 || len(firstRows) != 2 {
		t.Fatalf("first physical send calls=%d rows=%d", firstPhysical.sendCalls, len(firstRows))
	}

	secondPhysical := &fakeWriteBatch{}
	connection.batch = secondPhysical
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("replay ReconcilePending: %v", err)
	}
	if secondPhysical.sendCalls != 1 || len(secondPhysical.rows) != 2 {
		t.Fatalf("replay physical send calls=%d rows=%d", secondPhysical.sendCalls, len(secondPhysical.rows))
	}
	if replayToken := connection.settings["insert_deduplication_token"]; replayToken != firstToken {
		t.Fatalf("replay token = %v, want %v", replayToken, firstToken)
	}
	for index := range firstRows {
		for _, column := range []int{0, 23, 24, eventVisibilitySequenceColumn} {
			if firstRows[index][column] != secondPhysical.rows[index][column] {
				t.Fatalf(
					"replay row %d column %d = %#v, want %#v",
					index,
					column,
					secondPhysical.rows[index][column],
					firstRows[index][column],
				)
			}
		}
	}
}

type uncertainWriteGroupSequencer struct {
	visibility.WriteGroupSequencer
	formationCommittedThenError bool
	markCommittedThenError      bool
	commitCommittedThenError    bool
}

func (sequencer *uncertainWriteGroupSequencer) FormOrAcquireWriteGroup(
	ctx context.Context,
	attemptID string,
	limits visibility.WriteGroupLimits,
	now time.Time,
) (visibility.WriteGroup, bool, time.Time, error) {
	group, found, next, err := sequencer.WriteGroupSequencer.FormOrAcquireWriteGroup(
		ctx,
		attemptID,
		limits,
		now,
	)
	if err == nil && found && sequencer.formationCommittedThenError {
		sequencer.formationCommittedThenError = false
		if releaseErr := sequencer.ReleaseWriteGroup(ctx, group.ID, attemptID); releaseErr != nil {
			return visibility.WriteGroup{}, false, time.Time{}, releaseErr
		}
		return visibility.WriteGroup{}, false, next, io.ErrUnexpectedEOF
	}
	return group, found, next, err
}

func (sequencer *uncertainWriteGroupSequencer) MarkWriteGroupSending(
	ctx context.Context,
	groupID string,
	attemptID string,
) error {
	err := sequencer.WriteGroupSequencer.MarkWriteGroupSending(ctx, groupID, attemptID)
	if err == nil && sequencer.markCommittedThenError {
		sequencer.markCommittedThenError = false
		return io.ErrUnexpectedEOF
	}
	return err
}

func (sequencer *uncertainWriteGroupSequencer) CommitWriteGroup(
	ctx context.Context,
	groupID string,
	attemptID string,
	committedAt time.Time,
) error {
	err := sequencer.WriteGroupSequencer.CommitWriteGroup(ctx, groupID, attemptID, committedAt)
	if err == nil && sequencer.commitCommittedThenError {
		sequencer.commitCommittedThenError = false
		return io.ErrUnexpectedEOF
	}
	return err
}

func TestWriteGroupFormationOutcomeUncertaintyRecoversSealedGroup(t *testing.T) {
	t.Parallel()
	base := openStoreGroupSequencer(t)
	sequencer := &uncertainWriteGroupSequencer{
		WriteGroupSequencer:         base,
		formationCommittedThenError: true,
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	if _, err := store.Stage(context.Background(), distinctStoreBatch("formation-uncertain", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("uncertain formation error = %v", err)
	}
	usage, err := base.PendingUsage(context.Background())
	if err != nil || usage.ReadyGroups != 1 || usage.UngroupedReservations != 0 {
		t.Fatalf("usage after uncertain formation = %+v error=%v", usage, err)
	}
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("formation recovery: %v", err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 {
		t.Fatalf("formation recovery physical shape prepares=%d sends=%d", connection.prepareCalls, connection.batch.sendCalls)
	}
}

func TestWriteGroupMarkOutcomeUncertaintyReplaysSameToken(t *testing.T) {
	t.Parallel()
	base := openStoreGroupSequencer(t)
	sequencer := &uncertainWriteGroupSequencer{
		WriteGroupSequencer:    base,
		markCommittedThenError: true,
	}
	firstPhysical := &fakeWriteBatch{}
	connection := &fakeStoreConnection{batch: firstPhysical}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	if _, err := store.Stage(context.Background(), distinctStoreBatch("mark-uncertain", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("uncertain mark error = %v", err)
	}
	firstToken := connection.settings["insert_deduplication_token"]
	if firstPhysical.sendCalls != 0 || firstPhysical.abortCalls != 1 {
		t.Fatalf("uncertain mark physical batch = %+v", firstPhysical)
	}
	secondPhysical := &fakeWriteBatch{}
	connection.batch = secondPhysical
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("mark recovery: %v", err)
	}
	if secondPhysical.sendCalls != 1 || connection.settings["insert_deduplication_token"] != firstToken {
		t.Fatalf("mark recovery batch=%+v settings=%+v", secondPhysical, connection.settings)
	}
}

func TestWriteGroupCommitOutcomeUncertaintyDoesNotResendCommittedGroup(t *testing.T) {
	t.Parallel()
	base := openStoreGroupSequencer(t)
	sequencer := &uncertainWriteGroupSequencer{
		WriteGroupSequencer:      base,
		commitCommittedThenError: true,
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	if _, err := store.Stage(context.Background(), distinctStoreBatch("commit-uncertain", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("uncertain commit error = %v", err)
	}
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("commit recovery: %v", err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 {
		t.Fatalf("commit recovery resent physical group: prepares=%d sends=%d", connection.prepareCalls, connection.batch.sendCalls)
	}
	usage, err := base.PendingUsage(context.Background())
	if err != nil || !pendingWriteGroupUsageEmpty(usage) {
		t.Fatalf("usage after uncertain committed outcome = %+v error=%v", usage, err)
	}
}

func TestWriteGroupPreSendFailuresRemainReadyForRetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		connection *fakeStoreConnection
	}{
		{name: "prepare", connection: &fakeStoreConnection{prepareErr: io.ErrUnexpectedEOF}},
		{name: "append", connection: &fakeStoreConnection{batch: &fakeWriteBatch{appendErr: io.ErrUnexpectedEOF}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequencer := openStoreGroupSequencer(t)
			store := mustTestStoreWithVisibility(t, test.connection, fixedRetention(time.Hour), sequencer)
			store.coalescing = true
			if _, err := store.Stage(context.Background(), distinctStoreBatch("pre-send-"+test.name, 1)); err != nil {
				t.Fatal(err)
			}
			if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("first reconciliation error = %v", err)
			}
			usage, err := sequencer.PendingUsage(context.Background())
			if err != nil || usage.ReadyGroups != 1 || usage.AmbiguousGroups != 0 {
				t.Fatalf("pre-send failure usage = %+v error=%v", usage, err)
			}
		})
	}
}

func TestWriteGroupCloseFailureLeavesCommittedState(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{closeErr: io.ErrUnexpectedEOF}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	if _, err := store.Stage(context.Background(), distinctStoreBatch("close-failure", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePending(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("close failure error = %v", err)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || !pendingWriteGroupUsageEmpty(usage) {
		t.Fatalf("usage after committed close failure = %+v error=%v", usage, err)
	}
}

func TestGroupedReconcilerReplaysUngroupedAmbiguousReservationBeforeNewerWork(t *testing.T) {
	t.Parallel()
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	store.writeGroupLimits.MaxLinger = time.Hour

	first := distinctStoreBatch("legacy-ambiguous-first", 1)
	first.Events[0].Event.EventId = "legacy-ambiguous-event"
	firstStage, err := store.Stage(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	second := distinctStoreBatch("newer-ungrouped", 2)
	if _, err := store.Stage(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	legacy, found, err := sequencer.AcquirePending(context.Background(), "legacy-sender")
	if err != nil || !found || legacy.Sequence != firstStage.VisibilitySequence {
		t.Fatalf("acquire legacy reservation = %+v found=%v error=%v", legacy, found, err)
	}
	if err := sequencer.MarkSending(context.Background(), legacy.Sequence, "legacy-sender"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(context.Background(), legacy.Sequence, "legacy-sender"); err != nil {
		t.Fatal(err)
	}

	if _, err := store.reconcileWriteGroups(context.Background(), false, false); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first legacy replay error = %v, want ambiguous send", err)
	}
	legacyToken := connection.settings["insert_deduplication_token"]
	if legacyToken != deduplicationToken(first) {
		t.Fatalf("legacy replay token = %v, want original %q", legacyToken, deduplicationToken(first))
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || usage.Reservations != 2 || usage.UngroupedReservations != 2 ||
		usage.ReadyGroups != 0 || usage.AmbiguousGroups != 0 {
		t.Fatalf("usage behind legacy ambiguity = %+v error=%v", usage, err)
	}

	connection.batch = &fakeWriteBatch{}
	nextDeadline, err := store.reconcileWriteGroups(context.Background(), false, false)
	if err != nil {
		t.Fatal(err)
	}
	if connection.batch.sendCalls != 1 || len(connection.batch.rows) != 1 ||
		connection.settings["insert_deduplication_token"] != legacyToken {
		t.Fatalf("successful legacy replay shape=%+v settings=%+v", connection.batch, connection.settings)
	}
	if nextDeadline.IsZero() {
		t.Fatal("newer sparse work did not retain its durable linger deadline")
	}
	if cutoff, cutoffErr := sequencer.Cutoff(context.Background()); cutoffErr != nil ||
		cutoff != firstStage.VisibilitySequence {
		t.Fatalf("cutoff after ordered legacy replay = %d error=%v", cutoff, cutoffErr)
	}
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	usage, err = sequencer.PendingUsage(context.Background())
	if err != nil || !pendingWriteGroupUsageEmpty(usage) {
		t.Fatalf("usage after newer grouped drain = %+v error=%v", usage, err)
	}
}

func TestSparseWriteGroupFlushesAtDurableLingerDeadline(t *testing.T) {
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.writeGroupLimits.MaxLinger = 20 * time.Millisecond
	store.startReconciler()
	t.Cleanup(func() { _ = store.Close() })
	staged, err := store.Stage(context.Background(), distinctStoreBatch("sparse-linger", 1))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		cutoff, cutoffErr := store.VisibilityCutoff(context.Background())
		if cutoffErr != nil {
			t.Fatal(cutoffErr)
		}
		if cutoff == staged.VisibilitySequence {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sparse write group did not flush by its durable linger deadline")
		}
		time.Sleep(time.Millisecond)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 1 {
		t.Fatalf(
			"sparse physical shape prepares=%d sends=%d rows=%d",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
}

func TestGroupedStoreCloseForceSealsAndDrainsAcceptedWork(t *testing.T) {
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	store.writeGroupLimits.MaxLinger = time.Hour
	staged, err := store.Stage(context.Background(), distinctStoreBatch("shutdown-drain", 1))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 1 {
		t.Fatalf(
			"shutdown physical shape prepares=%d sends=%d rows=%d",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	cutoff, err := sequencer.Cutoff(context.Background())
	if err != nil || cutoff != staged.VisibilitySequence {
		t.Fatalf("shutdown cutoff = %d error=%v, want %d", cutoff, err, staged.VisibilitySequence)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || !pendingWriteGroupUsageEmpty(usage) {
		t.Fatalf("shutdown pending usage = %+v error=%v", usage, err)
	}
}

func TestGroupedStoreCloseRetainsAmbiguousWorkWhenDrainFails(t *testing.T) {
	sequencer := openStoreGroupSequencer(t)
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	store.coalescing = true
	if _, err := store.Stage(context.Background(), distinctStoreBatch("shutdown-ambiguous", 1)); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if err := store.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Close error = %v, want retained ambiguous send failure", err)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if usage.Reservations != 1 || usage.UngroupedReservations != 0 ||
		usage.ReadyGroups != 0 || usage.AmbiguousGroups != 1 || usage.LiveGroupLeases != 0 {
		t.Fatalf("pending usage after failed shutdown drain = %+v", usage)
	}
}

func TestStoreCloseContextBoundsConnectionClose(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	connection := &fakeStoreConnection{
		closeStarted: started,
		closeRelease: release,
	}
	store := mustTestStore(t, connection, fixedRetention(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.CloseContext(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("connection close did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CloseContext error = %v, want deadline", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("CloseContext exceeded the caller deadline")
	}
}

func openStoreGroupSequencer(t *testing.T) *visibility.SQLiteSequencer {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	sequencer, err := visibility.NewSQLite(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	return sequencer
}

func waitForPendingReservations(t *testing.T, sequencer visibility.Sequencer, want uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		usage, err := sequencer.PendingUsage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if usage.Reservations == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending reservations = %d, want %d", usage.Reservations, want)
		}
		time.Sleep(time.Millisecond)
	}
}
