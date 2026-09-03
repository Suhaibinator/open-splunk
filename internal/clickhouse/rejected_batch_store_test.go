package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestRejectBatchPersistsOnlyTerminalMetadata(t *testing.T) {
	t.Parallel()
	connection := &fakeStoreConnection{}
	sequencer := &rejectingVisibilitySequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{},
		newlyRejected:           true,
	}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	rejectedAt := time.Date(2026, 7, 31, 12, 13, 14, 987654321, time.UTC)
	store.clock = func() time.Time { return rejectedAt }
	input := validStoreBatchRejection()

	result, err := store.RejectBatch(context.Background(), input)
	if err != nil {
		t.Fatalf("RejectBatch: %v", err)
	}
	if result.BatchRejection == input.Rejection || !proto.Equal(result.BatchRejection, input.Rejection) {
		t.Fatalf("stored rejection = %#v, want independent exact clone %#v", result.BatchRejection, input.Rejection)
	}
	if connection.prepareCalls != 0 || connection.batch != nil {
		t.Fatalf("terminal rejection reached ClickHouse: prepares=%d batch=%#v", connection.prepareCalls, connection.batch)
	}
	if len(sequencer.rejectRequests) != 1 {
		t.Fatalf("Reject calls = %d, want 1", len(sequencer.rejectRequests))
	}
	request := sequencer.rejectRequests[0]
	batch := storeBatchFromIdentity(input.Identity)
	if request.BatchKey != deduplicationToken(batch) || request.SequenceKey != sequenceIdentityKey(batch) {
		t.Fatalf("durable identity keys = %q / %q", request.BatchKey, request.SequenceKey)
	}
	wantDigest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	if request.PayloadSHA256 != wantDigest || !request.IndexTime.Equal(input.ReceivedAt) {
		t.Fatalf("reject request identity/time = %#v", request)
	}
	if !request.RejectedAt.Equal(rejectedAt.UTC().Truncate(time.Microsecond)) {
		t.Fatalf("rejected_at = %v, want %v", request.RejectedAt, rejectedAt.UTC().Truncate(time.Microsecond))
	}
	if len(request.Metadata) == 0 || len(request.Metadata) > visibility.MaxMetadataBytes {
		t.Fatalf("rejection metadata size = %d", len(request.Metadata))
	}
	decoded, err := decodeBatchRejectionMetadata(request.Metadata)
	if err != nil {
		t.Fatalf("decodeBatchRejectionMetadata: %v", err)
	}
	if !proto.Equal(decoded, input.Rejection) {
		t.Fatalf("decoded rejection = %#v, want %#v", decoded, input.Rejection)
	}

	input.Rejection.Message = "mutated after persistence"
	if result.BatchRejection.GetMessage() == input.Rejection.GetMessage() {
		t.Fatal("RejectBatch result aliases caller-owned rejection")
	}
}

func TestRejectBatchReplaysAcrossSQLiteOwnerRestartWithoutPendingUsage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	input := validStoreBatchRejection()

	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := visibility.NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	firstConnection := &fakeStoreConnection{}
	first := mustTestStoreWithVisibility(t, firstConnection, fixedRetention(time.Hour), sequencer)
	first.clock = func() time.Time { return input.ReceivedAt.Add(time.Second) }
	firstResult, err := first.RejectBatch(ctx, input)
	if err != nil {
		t.Fatalf("first RejectBatch: %v", err)
	}
	if firstConnection.prepareCalls != 0 || !proto.Equal(firstResult.BatchRejection, input.Rejection) {
		t.Fatalf("first rejection result=%#v prepares=%d", firstResult, firstConnection.prepareCalls)
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (visibility.PendingUsage{}) {
		t.Fatalf("terminal rejection consumed pending capacity: %+v", usage)
	}
	if cutoff, cutoffErr := sequencer.Cutoff(ctx); cutoffErr != nil || cutoff != 1 {
		t.Fatalf("cutoff after rejection = %d, error=%v, want 1", cutoff, cutoffErr)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedSequencer, err := visibility.NewSQLite(ctx, reopened)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedSequencer.Close() })
	secondConnection := &fakeStoreConnection{}
	second := mustTestStoreWithVisibility(t, secondConnection, fixedRetention(time.Hour), reopenedSequencer)
	state, replay, err := second.LookupBatch(ctx, input.Identity)
	if err != nil {
		t.Fatalf("LookupBatch after restart: %v", err)
	}
	if state != ingest.StoredBatchRejected || !proto.Equal(replay.BatchRejection, input.Rejection) {
		t.Fatalf("replayed state=%v result=%#v, want exact rejection", state, replay)
	}
	if secondConnection.prepareCalls != 0 {
		t.Fatalf("rejected lookup prepared ClickHouse %d times", secondConnection.prepareCalls)
	}
}

func TestRejectedAndAcceptedBatchOutcomesAreFirstWriterWins(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		firstReject bool
	}{
		{name: "rejection wins", firstReject: true},
		{name: "ClickHouse commit wins", firstReject: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			sequencer, err := visibility.NewSQLite(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sequencer.Close() })
			connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
			rejected := validStoreBatchRejection()
			batch := validStoreBatch()

			if test.firstReject {
				if _, err := store.RejectBatch(ctx, rejected); err != nil {
					t.Fatalf("first RejectBatch: %v", err)
				}
				result, err := store.Store(ctx, batch)
				if err != nil {
					t.Fatalf("Store after rejection: %v", err)
				}
				if !proto.Equal(result.BatchRejection, rejected.Rejection) || connection.prepareCalls != 0 {
					t.Fatalf("outcome=%#v prepares=%d, want original rejection and zero writes", result, connection.prepareCalls)
				}
				return
			}

			if _, err := store.Store(ctx, batch); err != nil {
				t.Fatalf("first Store: %v", err)
			}
			result, err := store.RejectBatch(ctx, rejected)
			if err != nil {
				t.Fatalf("RejectBatch after commit: %v", err)
			}
			if result.BatchRejection != nil || result.Accepted != 0 || result.Duplicate != 1 || connection.prepareCalls != 1 {
				t.Fatalf("outcome=%#v prepares=%d, want duplicate accepted result", result, connection.prepareCalls)
			}
		})
	}
}

func TestStoreConvergesWhenRejectionWinsBetweenLookupAndReserve(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	rejected := validStoreBatchRejection()
	metadata, err := encodeBatchRejectionMetadata(rejected.Rejection)
	if err != nil {
		t.Fatal(err)
	}
	sequencer := &rejectionAfterLookupSequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{},
		rejectionMetadata:       metadata,
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)

	result, err := store.Store(context.Background(), batch)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if !proto.Equal(result.BatchRejection, rejected.Rejection) {
		t.Fatalf("Store result = %#v, want concurrent rejection %#v", result, rejected.Rejection)
	}
	if sequencer.lookupCalls != 1 || sequencer.reserveCalls != 1 {
		t.Fatalf("visibility calls = lookup %d reserve %d, want 1/1", sequencer.lookupCalls, sequencer.reserveCalls)
	}
	if connection.prepareCalls != 0 || len(sequencer.abandoned) != 0 || len(sequencer.released) != 0 {
		t.Fatalf(
			"losing Store touched ClickHouse/finalization: prepares=%d abandoned=%v released=%v",
			connection.prepareCalls,
			sequencer.abandoned,
			sequencer.released,
		)
	}
}

func TestRejectBatchDoesNotOverwritePendingClickHouseOutcome(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		reservation:    visibility.Reservation{Sequence: 7},
		hasReservation: true,
	}
	connection := &fakeStoreConnection{}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	result, err := store.RejectBatch(context.Background(), validStoreBatchRejection())
	if !isTransient(err) || !reflect.DeepEqual(result, ingest.StoreResult{}) {
		t.Fatalf("RejectBatch = (%#v, %v), want transient empty result", result, err)
	}
	if connection.prepareCalls != 0 || sequencer.reservation.Rejected {
		t.Fatalf("pending outcome was overwritten: prepares=%d reservation=%#v", connection.prepareCalls, sequencer.reservation)
	}
}

func TestResumeBatchReturnsRejectionAfterSafePendingAbandonment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err := visibility.NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	connection := &fakeStoreConnection{}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	batch := validStoreBatch()

	rows, err := store.rowsForBatch(ctx, batch, nil)
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
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	const attemptID = "accepted-before-safe-abandonment"
	pending, err := sequencer.Reserve(ctx, visibility.ReserveRequest{
		BatchKey:          deduplicationToken(batch),
		SequenceKey:       sequenceIdentityKey(batch),
		AttemptID:         attemptID,
		IndexTime:         batch.ReceivedAt,
		PayloadSHA256:     payloadDigest,
		Metadata:          metadata,
		Outbox:            outbox,
		StoredRowCount:    1,
		DecodedEventBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, pending.Sequence, attemptID); err != nil {
		t.Fatal(err)
	}

	rejected := validStoreBatchRejection()
	first, err := store.RejectBatch(ctx, rejected)
	if err != nil {
		t.Fatalf("RejectBatch after safe abandonment: %v", err)
	}
	resumed, err := store.ResumeBatch(ctx, rejected.Identity)
	if err != nil {
		t.Fatalf("ResumeBatch after durable rejection: %v", err)
	}
	if !proto.Equal(first.BatchRejection, rejected.Rejection) ||
		!proto.Equal(resumed.BatchRejection, rejected.Rejection) {
		t.Fatalf("terminal outcomes = first %#v resumed %#v, want %#v", first, resumed, rejected.Rejection)
	}
	if connection.prepareCalls != 0 || connection.batch != nil {
		t.Fatalf(
			"terminal rejection resume reached ClickHouse: prepares=%d batch=%#v",
			connection.prepareCalls,
			connection.batch,
		)
	}
}

func TestResumeBatchAfterObservedPendingAbandonmentReturnsGoneWithoutRecreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err := visibility.NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	batch := validStoreBatch()
	seedStore := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	rows, err := seedStore.rowsForBatch(ctx, batch, nil)
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
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	const attemptID = "pending-before-resume-abandonment"
	pending, err := sequencer.Reserve(ctx, visibility.ReserveRequest{
		BatchKey:          deduplicationToken(batch),
		SequenceKey:       sequenceIdentityKey(batch),
		AttemptID:         attemptID,
		IndexTime:         batch.ReceivedAt,
		PayloadSHA256:     payloadDigest,
		Metadata:          metadata,
		Outbox:            outbox,
		StoredRowCount:    1,
		DecodedEventBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	recording := &reserveRecordingSequencer{Sequencer: sequencer}
	connection := &fakeStoreConnection{}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), recording)
	identity := ingest.StoreBatchIdentity{
		TenantID:          batch.TenantID,
		CollectorID:       batch.CollectorID,
		BatchID:           batch.BatchID,
		BatchSequence:     batch.BatchSequence,
		SourceBatchSHA256: batch.SourceBatchSHA256,
	}
	state, _, err := store.LookupBatch(ctx, identity)
	if err != nil || state != ingest.StoredBatchPending {
		t.Fatalf("LookupBatch() state=%v error=%v, want pending", state, err)
	}
	if err := sequencer.Abandon(ctx, pending.Sequence, attemptID); err != nil {
		t.Fatal(err)
	}

	_, err = store.ResumeBatch(ctx, identity)
	if _, ok := errors.AsType[*ingest.StoredBatchGoneError](err); !ok {
		t.Fatalf("ResumeBatch() error = %v, want StoredBatchGoneError", err)
	}
	if isTransient(err) {
		t.Fatalf("ResumeBatch() error = %v, must not be a transient retry response", err)
	}
	if !errors.Is(err, visibility.ErrReservationGone) {
		t.Fatalf("ResumeBatch() error = %v, want ErrReservationGone", err)
	}
	if len(recording.requests) != 1 {
		t.Fatalf("Reserve calls = %d, want 1", len(recording.requests))
	}
	request := recording.requests[0]
	if !request.ExistingOnly || !request.IndexTime.IsZero() ||
		request.Metadata != nil || request.Outbox != nil {
		t.Fatalf("ResumeBatch Reserve request = %+v, want identity-only existing acquisition", request)
	}
	if connection.prepareCalls != 0 || connection.batch != nil {
		t.Fatalf(
			"abandoned resume reached ClickHouse: prepares=%d batch=%#v",
			connection.prepareCalls,
			connection.batch,
		)
	}
	var total, active int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT count(*), count(*) FILTER (
			WHERE state IN ('reserved', 'committed', 'rejected')
		)
		FROM ingest_visibility_reservations`).Scan(&total, &active); err != nil {
		t.Fatal(err)
	}
	if total != 1 || active != 0 {
		t.Fatalf("durable reservations = total %d active %d, want 1/0", total, active)
	}
}

func TestRejectBatchCountsOnlyNewTerminalRejection(t *testing.T) {
	t.Parallel()
	encoded, err := encodeBatchRejectionMetadata(validStoreBatchRejection().Rejection)
	if err != nil {
		t.Fatal(err)
	}

	encodedBytes := uint64(len(encoded))
	for _, test := range []struct {
		name              string
		newlyRejected     bool
		wantTerminalCnt   uint64
		wantMetadataBytes uint64
	}{
		{
			name:              "new rejection",
			newlyRejected:     true,
			wantTerminalCnt:   1,
			wantMetadataBytes: encodedBytes,
		},
		{name: "replayed rejection", newlyRejected: false, wantTerminalCnt: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sequencer := &rejectingVisibilitySequencer{
				fakeVisibilitySequencer: &fakeVisibilitySequencer{},
				newlyRejected:           test.newlyRejected,
			}
			store := mustTestStoreWithVisibility(
				t,
				&fakeStoreConnection{},
				fixedRetention(time.Hour),
				sequencer,
			)
			if _, err := store.RejectBatch(context.Background(), validStoreBatchRejection()); err != nil {
				t.Fatal(err)
			}
			if got := store.terminalCount.Load(); got != test.wantTerminalCnt {
				t.Fatalf("terminal count = %d, want %d", got, test.wantTerminalCnt)
			}
			if got := store.rejectionWakeBytes.Load(); got != test.wantMetadataBytes {
				t.Fatalf("rejected metadata bytes = %d, want %d", got, test.wantMetadataBytes)
			}
		})
	}
}

func TestRejectBatchConflictsAcrossBothStableIdentities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err := visibility.NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
	first := validStoreBatchRejection()
	if _, err := store.RejectBatch(ctx, first); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		edit func(*ingest.StoreBatchRejection)
	}{
		{name: "batch ID reused for new source bytes", edit: func(input *ingest.StoreBatchRejection) {
			input.Identity.SourceBatchSHA256 = testSourceBatchDigest("changed")
		}},
		{name: "collector sequence reused for new batch ID", edit: func(input *ingest.StoreBatchRejection) {
			input.Identity.BatchID = "different-batch"
			input.Identity.SourceBatchSHA256 = testSourceBatchDigest("different-batch")
			input.Rejection.BatchId = input.Identity.BatchID
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := validStoreBatchRejection()
			test.edit(&input)
			_, err := store.RejectBatch(ctx, input)
			if _, ok := errors.AsType[*ingest.DurableIdentityConflictError](err); !ok {
				t.Fatalf("RejectBatch error = %v, want durable identity conflict", err)
			}
		})
	}
}

func TestRejectBatchRejectsMismatchedResponseIdentityBeforeLedgerWrite(t *testing.T) {
	t.Parallel()
	sequencer := &rejectingVisibilitySequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{},
	}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
	input := validStoreBatchRejection()
	input.Rejection.BatchSequence++
	if _, err := store.RejectBatch(context.Background(), input); err == nil || isTransient(err) {
		t.Fatalf("RejectBatch error = %v, want permanent validation failure", err)
	}
	if len(sequencer.rejectRequests) != 0 {
		t.Fatalf("mismatched response reached ledger %d times", len(sequencer.rejectRequests))
	}
}

func TestRejectBatchMapsVisibilityFailureAndIdentityConflict(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		err       error
		transient bool
		conflict  bool
	}{
		{name: "unavailable", err: errors.New("SQLite unavailable"), transient: true},
		{name: "identity conflict", err: visibility.ErrConflict, conflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sequencer := &rejectingVisibilitySequencer{
				fakeVisibilitySequencer: &fakeVisibilitySequencer{},
				rejectErr:               test.err,
			}
			store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
			_, err := store.RejectBatch(context.Background(), validStoreBatchRejection())
			if test.transient != isTransient(err) {
				t.Fatalf("RejectBatch error = %v, transient=%v", err, isTransient(err))
			}
			var conflict *ingest.DurableIdentityConflictError
			if test.conflict != errors.As(err, &conflict) {
				t.Fatalf("RejectBatch error = %v, durable conflict=%v", err, errors.As(err, &conflict))
			}
		})
	}
}

func TestRejectBatchVisibilityFailureSchedulesMaintenance(t *testing.T) {
	t.Parallel()
	pruned := make(chan struct{}, 2)
	sequencer := &rejectingVisibilitySequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{pruneNotify: pruned},
		rejectErr:               errors.New("ambiguous SQLite commit"),
	}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	store.startReconciler()
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	select {
	case <-pruned: // startup maintenance
	case <-time.After(5 * time.Second):
		t.Fatal("startup maintenance did not run")
	}
	if _, err := store.RejectBatch(
		context.Background(),
		validStoreBatchRejection(),
	); !isTransient(err) {
		t.Fatalf("RejectBatch error = %v, want transient", err)
	}
	select {
	case <-pruned:
	case <-time.After(5 * time.Second):
		t.Fatal("ambiguous rejection failure did not schedule maintenance")
	}
}

func TestBatchRejectionMetadataRejectsCorruptionAndInvalidMessages(t *testing.T) {
	t.Parallel()
	valid := validStoreBatchRejection().Rejection
	encoded, err := encodeBatchRejectionMetadata(valid)
	if err != nil {
		t.Fatalf("encodeBatchRejectionMetadata: %v", err)
	}
	owned, err := decodeBatchRejectionMetadata(encoded)
	if err != nil {
		t.Fatalf("decodeBatchRejectionMetadata: %v", err)
	}
	for index := range encoded {
		encoded[index] = 0
	}
	if !proto.Equal(owned, valid) {
		t.Fatal("decoded rejection aliases its encoded metadata")
	}
	encoded, err = encodeBatchRejectionMetadata(valid)
	if err != nil {
		t.Fatalf("encodeBatchRejectionMetadata: %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := decodeBatchRejectionMetadata(corrupt); err == nil {
		t.Fatal("decodeBatchRejectionMetadata accepted checksum corruption")
	}

	const headerBytes = len(batchRejectionMetadataMagic) + 1 + 8
	reseal := func(metadata []byte) {
		t.Helper()
		payload := metadata[:len(metadata)-sha256.Size]
		checksum := sha256.Sum256(payload)
		copy(metadata[len(payload):], checksum[:])
	}
	mutateFraming := func(edit func([]byte)) []byte {
		candidate := append([]byte(nil), encoded...)
		edit(candidate[:len(candidate)-sha256.Size])
		reseal(candidate)
		return candidate
	}
	framePayload := func(payload []byte) []byte {
		metadata := make([]byte, headerBytes, headerBytes+len(payload)+sha256.Size)
		copy(metadata, batchRejectionMetadataMagic[:])
		metadata[len(batchRejectionMetadataMagic)] = batchRejectionMetadataVersion

		binary.BigEndian.PutUint64(metadata[len(batchRejectionMetadataMagic)+1:], uint64(len(payload)))
		metadata = append(metadata, payload...)
		checksum := sha256.Sum256(metadata)
		return append(metadata, checksum[:]...)
	}
	canonicalPayload := append(
		[]byte(nil),
		encoded[headerBytes:len(encoded)-sha256.Size]...,
	)
	number, wireType, tagBytes := protowire.ConsumeTag(canonicalPayload)
	valueBytes := -1
	if tagBytes > 0 {
		valueBytes = protowire.ConsumeFieldValue(number, wireType, canonicalPayload[tagBytes:])
	}
	if tagBytes <= 0 || valueBytes <= 0 {
		t.Fatalf("canonical rejection starts with an invalid protobuf field: tag=%d value=%d", tagBytes, valueBytes)
	}
	firstFieldBytes := tagBytes + valueBytes
	reorderedPayload := append([]byte(nil), canonicalPayload[firstFieldBytes:]...)
	reorderedPayload = append(reorderedPayload, canonicalPayload[:firstFieldBytes]...)
	duplicatePayload := append([]byte(nil), canonicalPayload...)
	duplicatePayload = append(duplicatePayload, canonicalPayload[:firstFieldBytes]...)

	for _, test := range []struct {
		name      string
		metadata  []byte
		wantError string
	}{
		{
			name: "valid checksum invalid header",
			metadata: mutateFraming(func(payload []byte) {
				payload[0] ^= 0xff
			}),
			wantError: "invalid header",
		},
		{
			name: "valid checksum invalid version",
			metadata: mutateFraming(func(payload []byte) {
				payload[len(batchRejectionMetadataMagic)]++
			}),
			wantError: "unsupported version",
		},
		{
			name: "valid checksum invalid length",
			metadata: mutateFraming(func(payload []byte) {
				lengthOffset := len(batchRejectionMetadataMagic) + 1
				length := binary.BigEndian.Uint64(payload[lengthOffset:headerBytes])
				binary.BigEndian.PutUint64(payload[lengthOffset:headerBytes], length+1)
			}),
			wantError: "invalid response length",
		},
		{
			name:      "reordered known fields",
			metadata:  framePayload(reorderedPayload),
			wantError: "not canonical",
		},
		{
			name:      "duplicate known field",
			metadata:  framePayload(duplicatePayload),
			wantError: "not canonical",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeBatchRejectionMetadata(test.metadata)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("decodeBatchRejectionMetadata error = %v, want %q", err, test.wantError)
			}
		})
	}

	for _, test := range []struct {
		name string
		edit func(*opensplunk.BatchReject)
	}{
		{name: "nil", edit: func(rejection *opensplunk.BatchReject) { *rejection = opensplunk.BatchReject{} }},
		{name: "unspecified code", edit: func(rejection *opensplunk.BatchReject) {
			rejection.Code = opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_UNSPECIFIED
		}},
		{name: "unknown code", edit: func(rejection *opensplunk.BatchReject) { rejection.Code = 99 }},
		{name: "nil violation", edit: func(rejection *opensplunk.BatchReject) {
			rejection.Violations = append(rejection.Violations, nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rejection := proto.Clone(valid).(*opensplunk.BatchReject)
			test.edit(rejection)
			if _, err := encodeBatchRejectionMetadata(rejection); err == nil {
				t.Fatalf("encodeBatchRejectionMetadata accepted %#v", rejection)
			}
		})
	}
}

func validStoreBatchRejection() ingest.StoreBatchRejection {
	batch := validStoreBatch()
	return ingest.StoreBatchRejection{
		Identity: ingest.StoreBatchIdentity{
			TenantID:          batch.TenantID,
			CollectorID:       batch.CollectorID,
			BatchID:           batch.BatchID,
			BatchSequence:     batch.BatchSequence,
			SourceBatchSHA256: batch.SourceBatchSHA256,
		},
		ReceivedAt: batch.ReceivedAt,
		Rejection: &opensplunk.BatchReject{
			BatchId:       batch.BatchID,
			BatchSequence: batch.BatchSequence,
			Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
			Message:       "batch contains no authorized valid events",
			Violations: []*opensplunk.FieldViolation{{
				FieldPath: "events[0].index_name",
				Code:      "unauthorized_index",
				Message:   "token is not authorized for the requested index",
			}},
		},
	}
}

type rejectingVisibilitySequencer struct {
	*fakeVisibilitySequencer
	rejectRequests []visibility.RejectRequest
	rejectErr      error
	newlyRejected  bool
}

type rejectionAfterLookupSequencer struct {
	*fakeVisibilitySequencer
	rejectionMetadata []byte
	reserveCalls      int
}

type reserveRecordingSequencer struct {
	visibility.Sequencer
	requests []visibility.ReserveRequest
}

func (sequencer *reserveRecordingSequencer) Reserve(
	ctx context.Context,
	request visibility.ReserveRequest,
) (visibility.Reservation, error) {
	sequencer.requests = append(sequencer.requests, request)
	return sequencer.Sequencer.Reserve(ctx, request)
}

func (sequencer *rejectionAfterLookupSequencer) Lookup(
	context.Context,
	string,
	string,
	[32]byte,
) (visibility.Reservation, bool, error) {
	sequencer.lookupCalls++
	return visibility.Reservation{}, false, nil
}

func (sequencer *rejectionAfterLookupSequencer) Reserve(
	_ context.Context,
	request visibility.ReserveRequest,
) (visibility.Reservation, error) {
	sequencer.reserveCalls++
	return visibility.Reservation{
		BatchKey:      request.BatchKey,
		SequenceKey:   request.SequenceKey,
		Sequence:      1,
		Rejected:      true,
		IndexTime:     request.IndexTime.UTC().Truncate(time.Millisecond),
		PayloadSHA256: request.PayloadSHA256,
		Metadata:      append([]byte(nil), sequencer.rejectionMetadata...),
		RejectedAt:    request.IndexTime.UTC().Truncate(time.Microsecond),
	}, nil
}

func (sequencer *rejectingVisibilitySequencer) Reject(
	_ context.Context,
	request visibility.RejectRequest,
) (visibility.Reservation, error) {
	sequencer.rejectRequests = append(sequencer.rejectRequests, request)
	if sequencer.rejectErr != nil {
		return visibility.Reservation{}, sequencer.rejectErr
	}
	reservation := visibility.Reservation{
		BatchKey:      request.BatchKey,
		SequenceKey:   request.SequenceKey,
		Sequence:      1,
		Rejected:      true,
		NewlyRejected: sequencer.newlyRejected,
		IndexTime:     request.IndexTime.UTC().Truncate(time.Millisecond),
		RejectedAt:    request.RejectedAt.UTC().Truncate(time.Microsecond),
		Metadata:      append([]byte(nil), request.Metadata...),
		PayloadSHA256: request.PayloadSHA256,
	}
	sequencer.reservation = reservation
	sequencer.hasReservation = true
	return reservation, nil
}
