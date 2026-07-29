package visibility

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

var testCommittedAt = time.Date(2026, time.July, 22, 4, 5, 6, 789123000, time.UTC)

func TestSQLiteSequencerAdvancesOnlyContiguousTerminalPrefix(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "batch-one", "attempt-one")
	second := reserve(t, sequencer, "batch-two", "attempt-two")
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("sequences = %d, %d; want 1, 2", first.Sequence, second.Sequence)
	}
	if err := sequencer.MarkSending(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, second.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 0)
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-one", testCommittedAt); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 2)
}

func TestSQLiteSequencerTerminalRowsClearOutboxAndPersistCommitTime(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("committed", "attempt")
	reservation, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reservation.Outbox, request.Outbox) {
		t.Fatalf("reserved outbox = %q, want %q", reservation.Outbox, request.Outbox)
	}
	markAndCommit(t, sequencer, reservation.Sequence, request.AttemptID, testCommittedAt)

	retry, found, err := sequencer.Lookup(ctx, request.BatchKey, request.SequenceKey, request.PayloadSHA256)
	if err != nil || !found {
		t.Fatalf("Lookup() found=%v error=%v", found, err)
	}
	if !retry.AlreadyCommitted || len(retry.Outbox) != 0 || !retry.CommittedAt.Equal(testCommittedAt) {
		t.Fatalf("committed reservation = %+v", retry)
	}
	var outboxLength int
	var committedAt int64
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT length(outbox), committed_at_unix_micro
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&outboxLength, &committedAt); err != nil {
		t.Fatal(err)
	}
	if outboxLength != 0 || committedAt != testCommittedAt.UnixMicro() {
		t.Fatalf("terminal storage = outbox %d, committed_at %d", outboxLength, committedAt)
	}
}

func TestSQLiteSequencerAttemptLeaseFencesConcurrentSameBatch(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "same-batch", "attempt-one")
	if _, err := sequencer.Reserve(ctx, reserveRequest("same-batch", "attempt-two")); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("concurrent same-batch error = %v, want ErrAttemptInProgress", err)
	}
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-two", testCommittedAt); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("commit by wrong attempt error = %v, want ErrAttemptLease", err)
	}
	if err := sequencer.Release(ctx, first.Sequence, "attempt-two"); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("release by wrong attempt error = %v, want ErrAttemptLease", err)
	}
	markAndCommit(t, sequencer, first.Sequence, "attempt-one", testCommittedAt)
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-one", testCommittedAt.Add(time.Hour)); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
}

func TestSQLiteSequencerWrongSequenceDoesNotDropAnotherAttemptLease(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "first", "attempt-one")
	second := reserve(t, sequencer, "second", "attempt-two")
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-two", testCommittedAt); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("wrong-sequence Commit() error = %v, want ErrAttemptLease", err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("second", "attempt-three")); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("second reservation after wrong-sequence Commit() error = %v, want ErrAttemptInProgress", err)
	}
	if err := sequencer.Abandon(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, second.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, second.Sequence, "attempt-two"); err != nil {
		t.Fatalf("idempotent Abandon() error = %v", err)
	}
}

func TestSQLiteSequencerFailedFinalizationCanBeTakenOver(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	first := reserve(t, sequencer, "takeover", "attempt-one")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sequencer.Release(canceled, first.Sequence, "attempt-one"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Release error = %v", err)
	}
	retry := reserve(t, sequencer, "takeover", "attempt-two")
	if retry.Sequence != first.Sequence || !retry.PreviouslyReserved {
		t.Fatalf("retry after failed finalization = %+v, want sequence %d", retry, first.Sequence)
	}
	if err := sequencer.Abandon(context.Background(), retry.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSequencerReleasePreservesReplayData(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("retryable", "attempt-one")
	first, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := sequencer.Release(ctx, first.Sequence, request.AttemptID); releaseErr != nil {
		t.Fatal(releaseErr)
	}

	retryRequest := reserveRequest("retryable", "attempt-two")
	retryRequest.IndexTime = request.IndexTime.Add(time.Hour)
	retryRequest.Metadata = []byte("changed policy")
	retryRequest.Outbox = []byte("changed replay payload")
	retry, err := sequencer.Reserve(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Sequence != first.Sequence || !retry.PreviouslyReserved {
		t.Fatalf("retry reservation = %+v, want recovered sequence %d", retry, first.Sequence)
	}
	if !retry.IndexTime.Equal(first.IndexTime) ||
		!slices.Equal(retry.Metadata, first.Metadata) ||
		!slices.Equal(retry.Outbox, first.Outbox) ||
		retry.BatchKey != first.BatchKey || retry.PayloadSHA256 != first.PayloadSHA256 {
		t.Fatalf("retry did not preserve durable replay data: got %+v, want %+v", retry, first)
	}
	markAndCommit(t, sequencer, retry.Sequence, retryRequest.AttemptID, testCommittedAt)
}

func TestSQLiteSequencerNormalizesNilMetadataToEmptyBlob(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("empty-metadata", "attempt")
	request.Metadata = nil
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Metadata == nil || len(reservation.Metadata) != 0 {
		t.Fatalf("reservation metadata = %#v, want non-nil empty bytes", reservation.Metadata)
	}
	var storageType string
	var storageLength int
	if err := db.SQLDB().QueryRowContext(context.Background(), `
		SELECT typeof(metadata), length(metadata)
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&storageType, &storageLength); err != nil {
		t.Fatal(err)
	}
	if storageType != "blob" || storageLength != 0 {
		t.Fatalf("stored metadata = type %q length %d, want empty blob", storageType, storageLength)
	}
}

func TestSQLiteSequencerAbandonFinishesOutOfOrderAndRetryGetsFreshSequence(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "first", "attempt-one")
	second := reserve(t, sequencer, "bad-batch", "attempt-two")
	if err := sequencer.Abandon(ctx, second.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 0)
	if err := sequencer.Abandon(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 2)
	var state string
	var outboxLength int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT state, length(outbox)
		FROM ingest_visibility_reservations WHERE sequence = ?`, second.Sequence).Scan(&state, &outboxLength); err != nil {
		t.Fatal(err)
	}
	if state != reservationAbandoned || outboxLength != 0 {
		t.Fatalf("sequence 2 = state %q outbox length %d", state, outboxLength)
	}
	retry := reserve(t, sequencer, "bad-batch", "attempt-three")
	if retry.Sequence != 3 || retry.PreviouslyReserved {
		t.Fatalf("retry = %+v, want fresh sequence 3", retry)
	}
	if retry.SequenceKey != second.SequenceKey || retry.PayloadSHA256 != second.PayloadSHA256 {
		t.Fatalf("retry identity = %q/%x, want %q/%x", retry.SequenceKey, retry.PayloadSHA256, second.SequenceKey, second.PayloadSHA256)
	}
	var identityCount int
	var firstVisibilitySeq int64
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT count(*), min(first_visibility_seq)
		FROM ingest_batch_identities
		WHERE batch_key = ?`, retry.BatchKey).Scan(&identityCount, &firstVisibilitySeq); err != nil {
		t.Fatal(err)
	}
	decodedFirstVisibilitySeq, err := decodePositiveSequence(firstVisibilitySeq)
	if err != nil {
		t.Fatalf("decode first visibility sequence: %v", err)
	}
	if identityCount != 1 || decodedFirstVisibilitySeq != second.Sequence {
		t.Fatalf("reused identity = count %d first sequence %d", identityCount, firstVisibilitySeq)
	}
}

func TestSQLiteSequencerRejectsSequenceKeyReuseForDifferentBatch(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	first := reserveRequest("batch-one", "attempt-one")
	first.SequenceKey = "collector:sequence:1"
	if _, err := sequencer.Reserve(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	conflict := reserveRequest("batch-two", "attempt-two")
	conflict.SequenceKey = first.SequenceKey
	if _, err := sequencer.Reserve(context.Background(), conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("sequence-key reuse error = %v, want ErrConflict", err)
	}
}

func TestSQLiteSequencerRejectsBatchKeyReuseForDifferentSequence(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	first := reserveRequest("stable-batch", "attempt-one")
	if _, err := sequencer.Reserve(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	conflict := reserveRequest("stable-batch", "attempt-two")
	conflict.SequenceKey = "different-sequence"
	if _, err := sequencer.Reserve(context.Background(), conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("batch-key reuse error = %v, want ErrConflict", err)
	}
}

func TestSQLiteSequencerRejectsCrossWiredExistingIdentities(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	firstRequest := reserveRequest("batch-one", "attempt-one")
	firstRequest.SequenceKey = "sequence-one"
	first, err := sequencer.Reserve(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if abandonErr := sequencer.Abandon(ctx, first.Sequence, firstRequest.AttemptID); abandonErr != nil {
		t.Fatal(abandonErr)
	}
	secondRequest := reserveRequest("batch-two", "attempt-two")
	secondRequest.SequenceKey = "sequence-two"
	second, err := sequencer.Reserve(ctx, secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, second.Sequence, secondRequest.AttemptID); err != nil {
		t.Fatal(err)
	}

	crossWired := reserveRequest(firstRequest.BatchKey, "attempt-three")
	crossWired.SequenceKey = secondRequest.SequenceKey
	if _, err := sequencer.Reserve(ctx, crossWired); !errors.Is(err, ErrConflict) {
		t.Fatalf("cross-wired Reserve() error = %v, want ErrConflict", err)
	}
	if _, found, err := sequencer.Lookup(
		ctx,
		crossWired.BatchKey,
		crossWired.SequenceKey,
		crossWired.PayloadSHA256,
	); !found || !errors.Is(err, ErrConflict) {
		t.Fatalf("cross-wired Lookup() found=%v error=%v, want conflict", found, err)
	}
}

func TestSQLiteSequencerLegacyTombstonePermanentlyRejectsBatchKey(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingest_visibility_legacy_tombstones
			(batch_key, legacy_visibility_seq, created_at_unix_micro)
		VALUES ('legacy-batch', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	request := reserveRequest("legacy-batch", "attempt")
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrConflict) {
		t.Fatalf("Reserve() legacy tombstone error = %v, want ErrConflict", err)
	}
	if _, found, err := sequencer.Lookup(ctx, request.BatchKey, request.SequenceKey, request.PayloadSHA256); !found || !errors.Is(err, ErrConflict) {
		t.Fatalf("Lookup() legacy tombstone found=%v error=%v", found, err)
	}
	if _, err := sequencer.PruneTerminal(ctx, 0, 10); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FROM ingest_visibility_legacy_tombstones WHERE batch_key = 'legacy-batch'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("legacy tombstone count after prune = %d, want 1", count)
	}
}

func TestSQLiteSequencerConcurrentFirstSeenIdentity(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	type outcome struct {
		reservation Reservation
		attemptID   string
		err         error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for i := range 2 {
		go func(i int) {
			request := reserveRequest("concurrent", fmt.Sprintf("attempt-%d", i))
			<-start
			reservation, err := sequencer.Reserve(context.Background(), request)
			outcomes <- outcome{reservation: reservation, attemptID: request.AttemptID, err: err}
		}(i)
	}
	close(start)
	var succeeded *outcome
	for range 2 {
		outcome := <-outcomes
		if outcome.err == nil {
			successfulOutcome := outcome
			succeeded = &successfulOutcome
			continue
		}
		if !errors.Is(outcome.err, ErrAttemptInProgress) {
			t.Fatalf("concurrent first-seen error = %v, want ErrAttemptInProgress", outcome.err)
		}
	}
	if succeeded == nil {
		t.Fatal("neither concurrent first-seen reservation succeeded")
	}
	var identityCount, reservationCount int
	if err := db.SQLDB().QueryRowContext(context.Background(), `SELECT count(*) FROM ingest_batch_identities`).Scan(&identityCount); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDB().QueryRowContext(context.Background(), `SELECT count(*) FROM ingest_visibility_reservations`).Scan(&reservationCount); err != nil {
		t.Fatal(err)
	}
	if identityCount != 1 || reservationCount != 1 {
		t.Fatalf("concurrent rows = identities %d reservations %d, want 1/1", identityCount, reservationCount)
	}
	if err := sequencer.Abandon(context.Background(), succeeded.reservation.Sequence, succeeded.attemptID); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSequencerConcurrentConflictingFirstSeenClassifiesConflict(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	start := make(chan struct{})
	errorsByRequest := make(chan error, 2)
	for i := range 2 {
		go func(i int) {
			request := reserveRequest("same-batch", fmt.Sprintf("attempt-%d", i))
			request.SequenceKey = fmt.Sprintf("sequence-%d", i)
			<-start
			_, err := sequencer.Reserve(context.Background(), request)
			errorsByRequest <- err
		}(i)
	}
	close(start)
	var successes, conflicts int
	for range 2 {
		err := <-errorsByRequest
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("concurrent conflicting identity error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent outcomes = %d successes, %d conflicts", successes, conflicts)
	}
}

func TestSQLiteSequencerCannotAbandonAmbiguousSend(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	reservation := reserve(t, sequencer, "ambiguous", "attempt")
	if err := sequencer.MarkSending(ctx, reservation.Sequence, "attempt"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, reservation.Sequence, "attempt"); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("abandon ambiguous error = %v, want ErrAttemptLease", err)
	}
}

func TestSQLiteSequencerOrphanedAmbiguousSendFreezesNewWorkUntilExactReplay(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	ambiguousRequest := reserveRequest("ambiguous", "ambiguous-owner")
	ambiguous, err := sequencer.Reserve(ctx, ambiguousRequest)
	if err != nil {
		t.Fatal(err)
	}
	later := reserve(t, sequencer, "later", "later-owner")
	if sendingErr := sequencer.MarkSending(ctx, ambiguous.Sequence, ambiguousRequest.AttemptID); sendingErr != nil {
		t.Fatal(sendingErr)
	}
	if releaseErr := sequencer.Release(ctx, ambiguous.Sequence, ambiguousRequest.AttemptID); releaseErr != nil {
		t.Fatal(releaseErr)
	}

	if _, reserveErr := sequencer.Reserve(ctx, reserveRequest("new-identity", "new-owner")); !errors.Is(reserveErr, ErrAmbiguousBarrier) {
		t.Fatalf("Reserve() behind ambiguous orphan error = %v, want ErrAmbiguousBarrier", reserveErr)
	}
	if sendingErr := sequencer.MarkSending(ctx, later.Sequence, "later-owner"); !errors.Is(sendingErr, ErrAmbiguousBarrier) {
		t.Fatalf("MarkSending() behind ambiguous orphan error = %v, want ErrAmbiguousBarrier", sendingErr)
	}

	replayRequest := ambiguousRequest
	replayRequest.AttemptID = "replay-owner"
	replay, err := sequencer.Reserve(ctx, replayRequest)
	if err != nil {
		t.Fatalf("reacquire ambiguous reservation: %v", err)
	}
	if replay.Sequence != ambiguous.Sequence || !replay.PreviouslyReserved || !replay.MayHaveReachedStorage {
		t.Fatalf("ambiguous replay reservation = %+v, want sequence %d", replay, ambiguous.Sequence)
	}
	markAndCommit(t, sequencer, replay.Sequence, replayRequest.AttemptID, testCommittedAt)
	markAndCommit(t, sequencer, later.Sequence, "later-owner", testCommittedAt.Add(time.Second))
	assertCutoff(t, sequencer, 2)
}

func TestSQLiteSequencerSerializesLiveClickHouseSends(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "first-live", "first-owner")
	second := reserve(t, sequencer, "second-live", "second-owner")
	if err := sequencer.MarkSending(ctx, first.Sequence, "first-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.MarkSending(ctx, second.Sequence, "second-owner"); !errors.Is(err, ErrAmbiguousBarrier) {
		t.Fatalf("concurrent MarkSending() error = %v, want ErrAmbiguousBarrier", err)
	}
	if err := sequencer.Commit(ctx, first.Sequence, "first-owner", testCommittedAt); err != nil {
		t.Fatal(err)
	}
	markAndCommit(t, sequencer, second.Sequence, "second-owner", testCommittedAt.Add(time.Second))
}

func TestSQLiteSequencerAcquirePendingPrioritizesAmbiguousBarrier(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	older := reserve(t, sequencer, "older-unsent", "older-owner")
	if err := sequencer.Release(ctx, older.Sequence, "older-owner"); err != nil {
		t.Fatal(err)
	}
	ambiguous := reserve(t, sequencer, "ambiguous", "ambiguous-owner")
	if err := sequencer.MarkSending(ctx, ambiguous.Sequence, "ambiguous-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(ctx, ambiguous.Sequence, "ambiguous-owner"); err != nil {
		t.Fatal(err)
	}

	acquired, found, err := sequencer.AcquirePending(ctx, "reconciler")
	if err != nil || !found {
		t.Fatalf("AcquirePending() found=%v error=%v", found, err)
	}
	if acquired.Sequence != ambiguous.Sequence || !acquired.MayHaveReachedStorage {
		t.Fatalf("AcquirePending() = %+v, want ambiguous sequence %d", acquired, ambiguous.Sequence)
	}
}

func TestSQLiteSequencerMultipleOrphanedAmbiguousSendsReplayOldestFirst(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "first-ambiguous", "first-owner")
	second := reserve(t, sequencer, "second-ambiguous", "second-owner")
	if err := sequencer.Release(ctx, first.Sequence, "first-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(ctx, second.Sequence, "second-owner"); err != nil {
		t.Fatal(err)
	}
	// Simulate recovery of a database written by the earlier concurrent-Send
	// implementation, or multiple in-flight sends orphaned by one crash.
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET phase = 'ambiguous'
		WHERE sequence IN (?, ?)`, first.Sequence, second.Sequence); err != nil {
		t.Fatal(err)
	}

	oldest, found, err := sequencer.AcquirePending(ctx, "oldest-replay")
	if err != nil || !found || oldest.Sequence != first.Sequence {
		t.Fatalf("first AcquirePending() = %+v found=%v error=%v", oldest, found, err)
	}
	markAndCommit(t, sequencer, oldest.Sequence, "oldest-replay", testCommittedAt)
	next, found, err := sequencer.AcquirePending(ctx, "next-replay")
	if err != nil || !found || next.Sequence != second.Sequence {
		t.Fatalf("second AcquirePending() = %+v found=%v error=%v", next, found, err)
	}
	markAndCommit(t, sequencer, next.Sequence, "next-replay", testCommittedAt.Add(time.Second))
	assertCutoff(t, sequencer, 2)
}

func TestSQLiteSequencerAcquirePendingOldestUnowned(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "first", "owner-one")
	second := reserve(t, sequencer, "second", "owner-two")
	if err := sequencer.Release(ctx, first.Sequence, "owner-one"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(ctx, second.Sequence, "owner-two"); err != nil {
		t.Fatal(err)
	}
	acquired, found, err := sequencer.AcquirePending(ctx, "reconciler-one")
	if err != nil || !found {
		t.Fatalf("AcquirePending() found=%v error=%v", found, err)
	}
	if acquired.Sequence != first.Sequence || !acquired.PreviouslyReserved ||
		!slices.Equal(acquired.Outbox, first.Outbox) {
		t.Fatalf("first acquired = %+v, want %+v", acquired, first)
	}
	acquiredSecond, found, err := sequencer.AcquirePending(ctx, "reconciler-two")
	if err != nil || !found || acquiredSecond.Sequence != second.Sequence {
		t.Fatalf("second AcquirePending() = %+v found=%v error=%v", acquiredSecond, found, err)
	}
	if err := sequencer.Abandon(ctx, first.Sequence, "reconciler-one"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, second.Sequence, "reconciler-two"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := sequencer.AcquirePending(ctx, "reconciler-empty"); err != nil || found {
		t.Fatalf("empty AcquirePending() found=%v error=%v", found, err)
	}
}

func TestSQLiteSequencerSecondConstructorDoesNotStealLiveAttempt(t *testing.T) {
	t.Parallel()
	first, db := openTestSequencer(t)
	reservation := reserve(t, first, "live-owner", "attempt-one")
	second, err := NewSQLite(context.Background(), db)
	if second != nil || !errors.Is(err, ErrOwnerExists) {
		t.Fatalf("second NewSQLite = (%p, %v), want nil/ErrOwnerExists", second, err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(context.Background(), `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "attempt-one" {
		t.Fatalf("durable owner after rejected constructor = %q, want attempt-one", durableOwner)
	}
	if err := first.Abandon(context.Background(), reservation.Sequence, "attempt-one"); err != nil {
		t.Fatalf("original owner lost its live lease: %v", err)
	}
}

func TestSQLiteSequencerSecondHandleToSameFileDoesNotStealLiveAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	firstDB, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstDB.Close() })
	first, err := NewSQLite(ctx, firstDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	reservation := reserve(t, first, "same-file", "first-owner")

	secondDB, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := NewSQLite(ctx, secondDB)
	if second != nil || !errors.Is(err, ErrOwnerExists) {
		t.Fatalf("second-file-handle NewSQLite = (%p, %v), want nil/ErrOwnerExists", second, err)
	}
	var durableOwner string
	if err := firstDB.SQLDB().QueryRowContext(ctx, `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "first-owner" {
		t.Fatalf("durable owner after rejected second handle = %q, want first-owner", durableOwner)
	}
	if err := first.Abandon(ctx, reservation.Sequence, "first-owner"); err != nil {
		t.Fatalf("first handle lost its live lease: %v", err)
	}
}

func TestSQLiteSequencerCloseReleasesDurableLeaseAfterCanceledOperation(t *testing.T) {
	t.Parallel()
	first, db := openTestSequencer(t)
	reservation := reserve(t, first, "live", "attempt-one")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := first.Release(
		canceled,
		reservation.Sequence,
		"attempt-one",
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Release error = %v, want context.Canceled", err)
	}
	if first.leases.contains("attempt-one") {
		t.Fatal("canceled Release retained a live process lease")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(context.Background(), `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "" {
		t.Fatalf("durable owner after Close = %q, want empty", durableOwner)
	}
	second, err := NewSQLite(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	recovered, found, err := second.AcquirePending(context.Background(), "attempt-two")
	if err != nil || !found {
		t.Fatalf("reopen acquisition found=%v error=%v", found, err)
	}
	if recovered.Sequence != reservation.Sequence || !recovered.PreviouslyReserved {
		t.Fatalf("recovered reservation = %+v, want sequence %d", recovered, reservation.Sequence)
	}
	if err := second.Abandon(context.Background(), reservation.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSequencerConstructorFailureDoesNotPoisonReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if sequencer, err := NewSQLite(ctx, db); err == nil || sequencer != nil {
		t.Fatalf("NewSQLite(closed database) = (%p, %v), want nil/error", sequencer, err)
	}
	if sequencer, err := NewSQLite(ctx, db); err == nil ||
		sequencer != nil || errors.Is(err, ErrOwnerExists) {
		t.Fatalf(
			"second NewSQLite(closed database) = (%p, %v), want nil/non-owner error",
			sequencer,
			err,
		)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	sequencer, err := NewSQLite(ctx, reopened)
	if err != nil {
		t.Fatalf("NewSQLite(reopened database): %v", err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSequencerShutdownCleansOutcomeAmbiguousLeaseBeforeBind(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("ambiguous-commit", "ambiguous-owner")
	if !sequencer.leases.activate(request.AttemptID) {
		t.Fatal("activate outcome-ambiguous attempt = false, want true")
	}

	tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = 1
		WHERE singleton = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_batch_identities (
			batch_key, sequence_key, payload_sha256,
			first_visibility_seq, created_at_unix_micro
		) VALUES (?, ?, ?, 1, ?)`,
		request.BatchKey,
		request.SequenceKey,
		request.PayloadSHA256[:],
		time.Now().UTC().UnixMicro(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_visibility_reservations (
			sequence, batch_key, state, phase, attempt_id,
			index_time_unix_milli, metadata, outbox,
			created_at_unix_micro, committed_at_unix_micro
		) VALUES (1, ?, 'reserved', 'unsent', ?, ?, ?, ?, ?, NULL)`,
		request.BatchKey,
		request.AttemptID,
		request.IndexTime.UnixMilli(),
		request.Metadata,
		request.Outbox,
		time.Now().UTC().UnixMicro(),
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// This is the state after SQLite persists the transaction but Commit
	// reports an error: Reserve's deferred failure path drops only the live
	// process lease, while shutdown must conservatively clean the durable one.
	sequencer.leases.deactivate(request.AttemptID)

	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = 1`).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "" {
		t.Fatalf("durable owner after ambiguous-commit shutdown = %q, want empty", durableOwner)
	}
}

func TestSQLiteSequencerCloseIsIdempotentAndRejectsOperations(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if sequencer.db != nil || sequencer.leases != nil {
		t.Fatalf("Close retained borrowed database or lease owner: db=%p leases=%p", sequencer.db, sequencer.leases)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	ctx := context.Background()
	request := reserveRequest("closed", "closed-attempt")
	assertClosed := func(operation string, err error) {
		t.Helper()
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("%s error = %v, want ErrClosed", operation, err)
		}
	}
	_, _, err := sequencer.Lookup(ctx, request.BatchKey, request.SequenceKey, request.PayloadSHA256)
	assertClosed("Lookup", err)
	_, err = sequencer.Reserve(ctx, request)
	assertClosed("Reserve", err)
	_, _, err = sequencer.AcquirePending(ctx, request.AttemptID)
	assertClosed("AcquirePending", err)
	assertClosed("MarkSending", sequencer.MarkSending(ctx, 1, request.AttemptID))
	assertClosed("Commit", sequencer.Commit(ctx, 1, request.AttemptID, testCommittedAt))
	assertClosed("Release", sequencer.Release(ctx, 1, request.AttemptID))
	assertClosed("Abandon", sequencer.Abandon(ctx, 1, request.AttemptID))
	_, err = sequencer.Cutoff(ctx)
	assertClosed("Cutoff", err)
	_, err = sequencer.PruneTerminal(ctx, 0, 1)
	assertClosed("PruneTerminal", err)
}

func TestSQLiteSequencerShutdownWithNoActiveLeasesDoesNotTouchDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown with no active leases after database close: %v", err)
	}
	if sequencer.db != nil || sequencer.leases != nil {
		t.Fatalf(
			"Shutdown retained borrowed state: db=%p leases=%p",
			sequencer.db,
			sequencer.leases,
		)
	}
}

func TestSQLiteSequencerZeroValueIsClosed(t *testing.T) {
	t.Parallel()
	var sequencer SQLiteSequencer
	if err := sequencer.Close(); err != nil {
		t.Fatalf("zero-value Close: %v", err)
	}
	if _, err := sequencer.Cutoff(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero-value Cutoff error = %v, want ErrClosed", err)
	}
}

func TestSQLiteSequencerShutdownDeadlineFinalizesAfterBlockedOperationDrains(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	blocker := beginVisibilityWriteBlocker(t, db)
	reserveResult := make(chan error, 1)
	go func() {
		_, err := sequencer.Reserve(
			context.Background(),
			reserveRequest("blocked-operation", "blocked-attempt"),
		)
		reserveResult <- err
	}()
	waitForVisibilityDBInUse(t, db, 2)

	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancelShutdown()
	shutdownStarted := time.Now()
	if err := sequencer.Shutdown(shutdownContext); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("Shutdown behind blocked operation error = %v, want deadline", err)
	}
	if elapsed := time.Since(shutdownStarted); elapsed > time.Second {
		t.Fatalf("Shutdown behind blocked operation returned after %s, want under 1s", elapsed)
	}
	if replacement, err := NewSQLite(context.Background(), db); replacement != nil ||
		!errors.Is(err, ErrOwnerExists) {
		t.Fatalf(
			"replacement during timed-out shutdown = (%p, %v), want nil/ErrOwnerExists",
			replacement,
			err,
		)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-reserveResult; err != nil {
		t.Fatalf("blocked Reserve: %v", err)
	}
	replacement := waitForReplacementVisibilityOwner(t, db)
	t.Cleanup(func() { _ = replacement.Close() })
	recovered, found, err := replacement.AcquirePending(
		context.Background(),
		"recovered-attempt",
	)
	if err != nil || !found || recovered.BatchKey != "blocked-operation" {
		t.Fatalf("recovered reservation = %+v found=%v error=%v", recovered, found, err)
	}
	if err := replacement.Abandon(
		context.Background(),
		recovered.Sequence,
		"recovered-attempt",
	); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSequencerShutdownDeadlineDoesNotAbandonDurableCleanup(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	reservation := reserve(t, sequencer, "blocked-cleanup", "cleanup-attempt")
	blocker := beginVisibilityWriteBlocker(t, db)

	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancelShutdown()
	shutdownStarted := time.Now()
	if err := sequencer.Shutdown(shutdownContext); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("Shutdown with blocked cleanup error = %v, want deadline", err)
	}
	if elapsed := time.Since(shutdownStarted); elapsed > time.Second {
		t.Fatalf("Shutdown with blocked cleanup returned after %s, want under 1s", elapsed)
	}
	if replacement, err := NewSQLite(context.Background(), db); replacement != nil ||
		!errors.Is(err, ErrOwnerExists) {
		t.Fatalf(
			"replacement after cleanup timeout = (%p, %v), want nil/ErrOwnerExists",
			replacement,
			err,
		)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	replacement := waitForReplacementVisibilityOwner(t, db)
	t.Cleanup(func() { _ = replacement.Close() })
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(context.Background(), `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "" {
		t.Fatalf("durable owner after retried cleanup = %q, want empty", durableOwner)
	}
}

func TestSQLiteSequencerConcurrentCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	const callers = 16
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- sequencer.Close()
		}()
	}
	close(start)
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteSequencerRestartClearsLeaseAndPreservesOutbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	pending := reserve(t, sequencer, "ambiguous", "old-process")
	if sendingErr := sequencer.MarkSending(ctx, pending.Sequence, "old-process"); sendingErr != nil {
		t.Fatal(sendingErr)
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if shutdownErr := sequencer.Shutdown(context.Background()); shutdownErr == nil {
		t.Fatal("Shutdown after crash-close succeeded, want cleanup error")
	}

	db, err = control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err = NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	if _, reserveErr := sequencer.Reserve(ctx, reserveRequest("different-after-restart", "blocked-owner")); !errors.Is(reserveErr, ErrAmbiguousBarrier) {
		t.Fatalf("Reserve() after ambiguous restart error = %v, want ErrAmbiguousBarrier", reserveErr)
	}
	recovered, found, err := sequencer.AcquirePending(ctx, "new-process")
	if err != nil || !found {
		t.Fatalf("AcquirePending after restart found=%v error=%v", found, err)
	}
	if recovered.Sequence != pending.Sequence || !recovered.MayHaveReachedStorage ||
		!slices.Equal(recovered.Outbox, pending.Outbox) {
		t.Fatalf("reservation after restart = %+v, want %+v", recovered, pending)
	}
	markAndCommit(t, sequencer, recovered.Sequence, "new-process", testCommittedAt)
	assertCutoff(t, sequencer, 1)
}

func TestSQLiteSequencerEnforcesPendingCapacity(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	for i := 0; i < MaxPendingReservations; i++ {
		attempt := fmt.Sprintf("attempt-%d", i)
		reservation := reserve(t, sequencer, fmt.Sprintf("batch-%d", i), attempt)
		if err := sequencer.Release(ctx, reservation.Sequence, attempt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("over-capacity", "over-capacity")); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("Reserve() error = %v, want ErrPendingCapacity", err)
	}

	first := reserve(t, sequencer, "batch-0", "terminal")
	if err := sequencer.Abandon(ctx, first.Sequence, "terminal"); err != nil {
		t.Fatal(err)
	}
	newReservation := reserve(t, sequencer, "after-terminal", "after-terminal")
	if newReservation.Sequence != MaxPendingReservations+1 {
		t.Fatalf("new sequence = %d, want %d", newReservation.Sequence, MaxPendingReservations+1)
	}
}

func TestPendingCapacityByteBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		count      int64
		stored     int64
		additional int64
		want       bool
	}{
		{name: "just fits", stored: MaxPendingOutboxBytes - MaxOutboxBytes, additional: MaxOutboxBytes},
		{name: "one byte too many", stored: MaxPendingOutboxBytes - MaxOutboxBytes + 1, additional: MaxOutboxBytes, want: true},
		{name: "count full", count: MaxPendingReservations, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := pendingCapacityExceeded(test.count, test.stored, test.additional); got != test.want {
				t.Fatalf("pendingCapacityExceeded() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSQLiteSequencerPruneRetainsPendingRecentAndAboveCutoff(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "old-committed", "attempt-one")
	markAndCommit(t, sequencer, first.Sequence, "attempt-one", testCommittedAt)
	second := reserve(t, sequencer, "recent-committed", "attempt-two")
	markAndCommit(t, sequencer, second.Sequence, "attempt-two", testCommittedAt.Add(time.Second))
	third := reserve(t, sequencer, "pending", "attempt-three")
	if err := sequencer.Release(ctx, third.Sequence, "attempt-three"); err != nil {
		t.Fatal(err)
	}
	fourth := reserve(t, sequencer, "above-cutoff-terminal", "attempt-four")
	if err := sequencer.Abandon(ctx, fourth.Sequence, "attempt-four"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 2)

	deleted, err := sequencer.PruneTerminal(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("PruneTerminal() deleted %d, want 1", deleted)
	}
	assertReservationSequences(t, db, []uint64{second.Sequence, third.Sequence, fourth.Sequence})
	assertIdentityPresence(t, db, first.BatchKey, false)
	assertIdentityPresence(t, db, second.BatchKey, true)
	assertIdentityPresence(t, db, third.BatchKey, true)
	assertIdentityPresence(t, db, fourth.BatchKey, true)

	deleted, err = sequencer.PruneTerminal(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("second PruneTerminal() deleted %d, want 1", deleted)
	}
	assertReservationSequences(t, db, []uint64{third.Sequence, fourth.Sequence})
}

func TestSQLiteSequencerPruneKeepsIdentityReferencedByRetry(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "retried", "attempt-one")
	if err := sequencer.Abandon(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	retry := reserve(t, sequencer, "retried", "attempt-two")
	if retry.Sequence != first.Sequence+1 {
		t.Fatalf("retry sequence = %d, want %d", retry.Sequence, first.Sequence+1)
	}
	deleted, err := sequencer.PruneTerminal(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("PruneTerminal() deleted %d, want 1", deleted)
	}
	assertIdentityPresence(t, db, first.BatchKey, true)
	lookedUp, found, err := sequencer.Lookup(ctx, retry.BatchKey, retry.SequenceKey, retry.PayloadSHA256)
	if err != nil || !found || lookedUp.Sequence != retry.Sequence {
		t.Fatalf("Lookup() after old-attempt prune = %+v found=%v error=%v", lookedUp, found, err)
	}
}

func TestSQLiteSequencerAbandonedAttemptsDoNotAgeCommittedHorizon(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	committed := reserve(t, sequencer, "committed", "committed-owner")
	markAndCommit(t, sequencer, committed.Sequence, "committed-owner", testCommittedAt)
	const retainCommitted = 3
	sequences := []uint64{committed.Sequence}
	for i := 0; i < retainCommitted+2; i++ {
		attemptID := fmt.Sprintf("abandoned-owner-%d", i)
		reservation := reserve(t, sequencer, fmt.Sprintf("abandoned-%d", i), attemptID)
		if err := sequencer.Abandon(ctx, reservation.Sequence, attemptID); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, reservation.Sequence)
	}
	deleted, err := sequencer.PruneTerminal(ctx, retainCommitted, MaxPruneLimit)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("PruneTerminal() deleted %d rows, want 2 old abandoned attempts", deleted)
	}
	wantSequences := append([]uint64{sequences[0]}, sequences[3:]...)
	assertReservationSequences(t, db, wantSequences)
	assertIdentityPresence(t, db, committed.BatchKey, true)
}

func TestSQLiteSequencerPrunesOldAbandonsBehindPendingGap(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	pending := reserve(t, sequencer, "pending-head", "pending-owner")
	if err := sequencer.Release(ctx, pending.Sequence, "pending-owner"); err != nil {
		t.Fatal(err)
	}
	sequences := []uint64{pending.Sequence}
	for i := 0; i < 5; i++ {
		attemptID := fmt.Sprintf("safe-abandon-owner-%d", i)
		reservation := reserve(t, sequencer, fmt.Sprintf("safe-abandon-%d", i), attemptID)
		if err := sequencer.Abandon(ctx, reservation.Sequence, attemptID); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, reservation.Sequence)
	}
	assertCutoff(t, sequencer, 0)
	deleted, err := sequencer.PruneTerminal(ctx, 2, MaxPruneLimit)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("PruneTerminal() deleted %d rows, want 3 old safe abandons", deleted)
	}
	assertReservationSequences(t, db, []uint64{sequences[0], sequences[4], sequences[5]})
	assertCutoff(t, sequencer, 0)
	reacquired := reserve(t, sequencer, "pending-head", "pending-retry")
	if err := sequencer.Abandon(ctx, reacquired.Sequence, "pending-retry"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, sequences[len(sequences)-1])
}

func TestSQLiteSequencerIdentityReusableOnlyAfterExplicitPruneHorizon(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "horizon-batch", "attempt-one")
	markAndCommit(t, sequencer, first.Sequence, "attempt-one", testCommittedAt)

	replacement := reserveRequest(first.BatchKey, "attempt-two")
	replacement.SequenceKey = "new-sequence-key"
	replacement.PayloadSHA256 = sha256.Sum256([]byte("new-payload"))
	if _, err := sequencer.Reserve(ctx, replacement); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity reuse before prune error = %v, want ErrConflict", err)
	}
	if deleted, err := sequencer.PruneTerminal(ctx, 1, 10); err != nil || deleted != 0 {
		t.Fatalf("retaining PruneTerminal() deleted=%d error=%v", deleted, err)
	}
	if _, err := sequencer.Reserve(ctx, replacement); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity reuse inside horizon error = %v, want ErrConflict", err)
	}
	if deleted, err := sequencer.PruneTerminal(ctx, 0, 10); err != nil || deleted != 1 {
		t.Fatalf("horizon PruneTerminal() deleted=%d error=%v", deleted, err)
	}
	assertIdentityPresence(t, db, first.BatchKey, false)
	reused, err := sequencer.Reserve(ctx, replacement)
	if err != nil {
		t.Fatalf("identity reuse beyond horizon: %v", err)
	}
	if reused.Sequence != first.Sequence+1 || reused.SequenceKey != replacement.SequenceKey ||
		reused.PayloadSHA256 != replacement.PayloadSHA256 {
		t.Fatalf("reused identity reservation = %+v", reused)
	}
}

func TestSQLiteSequencerPruneRejectsInvalidLimit(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	for _, limit := range []uint32{0, MaxPruneLimit + 1} {
		if _, err := sequencer.PruneTerminal(context.Background(), 0, limit); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("PruneTerminal(limit=%d) error = %v, want ErrInvalidArgument", limit, err)
		}
	}
}

func TestSQLiteSequencerConcurrentOutOfOrderFinalization(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	const count = 16
	reservations := make([]Reservation, count)
	for i := range reservations {
		reservations[i] = reserve(t, sequencer, fmt.Sprintf("batch-%d", i), fmt.Sprintf("attempt-%d", i))
	}
	// Exercise recovery compatibility with rows produced before Send
	// serialization was introduced.
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET phase = 'ambiguous'
		WHERE state = 'reserved'`); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	errorsByAttempt := make(chan error, count)
	for i := count - 1; i >= 0; i-- {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			errorsByAttempt <- sequencer.Commit(
				ctx,
				reservations[i].Sequence,
				fmt.Sprintf("attempt-%d", i),
				testCommittedAt.Add(time.Duration(i)*time.Microsecond),
			)
		}(i)
	}
	wait.Wait()
	close(errorsByAttempt)
	for err := range errorsByAttempt {
		if err != nil {
			t.Errorf("Commit() error = %v", err)
		}
	}
	assertCutoff(t, sequencer, count)
}

func TestSQLiteSequencerRejectsInvalidInputsAndExhaustion(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	if err := sequencer.Commit(ctx, 99, "attempt", testCommittedAt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("commit unknown error = %v, want ErrNotFound", err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("", "attempt")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("batch", "")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty attempt error = %v", err)
	}
	emptySequenceKey := reserveRequest("batch", "attempt")
	emptySequenceKey.SequenceKey = ""
	if _, err := sequencer.Reserve(ctx, emptySequenceKey); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty sequence key error = %v", err)
	}
	emptyOutbox := reserveRequest("batch", "attempt")
	emptyOutbox.Outbox = nil
	if _, err := sequencer.Reserve(ctx, emptyOutbox); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty outbox error = %v", err)
	}
	//nolint:staticcheck // Deliberately verify that the package rejects a nil context.
	if _, err := sequencer.Reserve(nil, reserveRequest("batch", "attempt")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, _, err := sequencer.AcquirePending(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty reconciliation attempt error = %v", err)
	}
	if err := sequencer.Commit(ctx, 1, "attempt", time.Time{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty committed time error = %v", err)
	}
	if err := sequencer.MarkSending(ctx, math.MaxUint64, "attempt"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("out-of-range sequence error = %v", err)
	}
	retainedCommit := reserve(t, sequencer, "retained-commit", "commit-owner")
	markAndCommit(t, sequencer, retainedCommit.Sequence, "commit-owner", testCommittedAt)
	retainedAbandon := reserve(t, sequencer, "retained-abandon", "abandon-owner")
	if err := sequencer.Abandon(ctx, retainedAbandon.Sequence, "abandon-owner"); err != nil {
		t.Fatalf("abandon retained reservation: %v", err)
	}
	if deleted, err := sequencer.PruneTerminal(ctx, math.MaxUint64, 1); err != nil || deleted != 0 {
		t.Fatalf("huge retention horizon result = %d, error = %v", deleted, err)
	}
	assertReservationSequences(t, db, []uint64{retainedCommit.Sequence, retainedAbandon.Sequence})
	assertIdentityPresence(t, db, retainedCommit.BatchKey, true)
	assertIdentityPresence(t, db, retainedAbandon.BatchKey, true)
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = ?, committed_through = ?
		WHERE singleton = 1`, int64(math.MaxInt64), int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("exhausted", "attempt")); !errors.Is(err, ErrExhausted) {
		t.Fatalf("exhausted error = %v", err)
	}
}

func TestSQLiteSequencerRejectsChangedPayload(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("stable", "attempt-one")
	if _, err := sequencer.Reserve(ctx, request); err != nil {
		t.Fatal(err)
	}
	conflict := reserveRequest("stable", "attempt-two")
	conflict.PayloadSHA256 = sha256.Sum256([]byte("different"))
	if _, err := sequencer.Reserve(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed payload error = %v, want ErrConflict", err)
	}
	if _, found, err := sequencer.Lookup(ctx, request.BatchKey, request.SequenceKey, conflict.PayloadSHA256); !found || !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Lookup() found=%v error=%v", found, err)
	}
}

func openTestSequencer(t *testing.T) (*SQLiteSequencer, *control.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	return sequencer, db
}

func beginVisibilityWriteBlocker(t *testing.T, db *control.DB) *sql.Tx {
	t.Helper()
	tx, err := db.SQLDB().BeginTx(
		context.Background(),
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		UPDATE ingest_visibility_state
		SET last_assigned = last_assigned
		WHERE singleton = 1`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func waitForVisibilityDBInUse(t *testing.T, db *control.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if inUse := db.SQLDB().Stats().InUse; inUse >= want {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf(
				"control database in-use connections = %d, want at least %d",
				db.SQLDB().Stats().InUse,
				want,
			)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForReplacementVisibilityOwner(
	t *testing.T,
	db *control.DB,
) *SQLiteSequencer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		replacement, err := NewSQLite(context.Background(), db)
		if err == nil {
			return replacement
		}
		if !errors.Is(err, ErrOwnerExists) {
			t.Fatalf("construct replacement visibility owner: %v", err)
		}
		if !time.Now().Before(deadline) {
			t.Fatal("visibility owner remained registered after shutdown finalization")
		}
		time.Sleep(time.Millisecond)
	}
}

func reserve(t *testing.T, sequencer *SQLiteSequencer, key, attemptID string) Reservation {
	t.Helper()
	reservation, err := sequencer.Reserve(context.Background(), reserveRequest(key, attemptID))
	if err != nil {
		t.Fatalf("Reserve(%q): %v", key, err)
	}
	return reservation
}

func markAndCommit(t *testing.T, sequencer *SQLiteSequencer, sequence uint64, attemptID string, committedAt time.Time) {
	t.Helper()
	if err := sequencer.MarkSending(context.Background(), sequence, attemptID); err != nil {
		t.Fatalf("MarkSending(%d): %v", sequence, err)
	}
	if err := sequencer.Commit(context.Background(), sequence, attemptID, committedAt); err != nil {
		t.Fatalf("Commit(%d): %v", sequence, err)
	}
}

func reserveRequest(key, attemptID string) ReserveRequest {
	return ReserveRequest{
		BatchKey:      key,
		SequenceKey:   "sequence-" + key,
		AttemptID:     attemptID,
		IndexTime:     time.Date(2026, time.July, 21, 1, 2, 3, 456000000, time.UTC),
		PayloadSHA256: sha256.Sum256([]byte(key)),
		Metadata:      []byte("retention-v1"),
		Outbox:        []byte("clickhouse-block-for-" + key),
	}
}

func assertReservationSequences(t *testing.T, db *control.DB, want []uint64) {
	t.Helper()
	rows, err := db.SQLDB().QueryContext(context.Background(), `
		SELECT sequence
		FROM ingest_visibility_reservations
		ORDER BY sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []uint64
	for rows.Next() {
		var sequence uint64
		if err := rows.Scan(&sequence); err != nil {
			t.Fatal(err)
		}
		got = append(got, sequence)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("reservation sequences = %v, want %v", got, want)
	}
}

func assertIdentityPresence(t *testing.T, db *control.DB, batchKey string, want bool) {
	t.Helper()
	var count int
	if err := db.SQLDB().QueryRowContext(context.Background(), `
		SELECT count(*)
		FROM ingest_batch_identities
		WHERE batch_key = ?`, batchKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if got := count == 1; got != want {
		t.Fatalf("identity %q presence = %v, want %v", batchKey, got, want)
	}
}

func assertCutoff(t *testing.T, sequencer *SQLiteSequencer, want uint64) {
	t.Helper()
	got, err := sequencer.Cutoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("cutoff = %d, want %d", got, want)
	}
}
