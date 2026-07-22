package visibility

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestSQLiteSequencerBlocksLaterBatchUntilHeadCommits(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	first := reserve(t, sequencer, "batch-one", "attempt-one")
	if _, err := sequencer.Reserve(ctx, reserveRequest("batch-two", "attempt-two")); !errors.Is(err, ErrPendingBarrier) {
		t.Fatalf("reserve behind unresolved batch error = %v, want ErrPendingBarrier", err)
	}
	assertCutoff(t, sequencer, 0)
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 1)

	second := reserve(t, sequencer, "batch-two", "attempt-two")
	if second.Sequence != 2 {
		t.Fatalf("second sequence = %d, want 2", second.Sequence)
	}
	if err := sequencer.Commit(ctx, second.Sequence, "attempt-two"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 2)

	retry := reserve(t, sequencer, "batch-one", "attempt-retry")
	if retry.Sequence != first.Sequence || !retry.AlreadyCommitted {
		t.Fatalf("committed retry = %+v, want sequence %d already committed", retry, first.Sequence)
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
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-two"); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("commit by wrong attempt error = %v, want ErrAttemptLease", err)
	}
	if err := sequencer.Release(ctx, first.Sequence, "attempt-two"); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("release by wrong attempt error = %v, want ErrAttemptLease", err)
	}
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	// An ambiguous retry of Commit is idempotent even though the lease was
	// cleared by the first successful transaction.
	if err := sequencer.Commit(ctx, first.Sequence, "attempt-one"); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
}

func TestSQLiteSequencerReleasePreservesSequenceAndMetadata(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("retryable", "attempt-one")
	first, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(ctx, first.Sequence, request.AttemptID); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 0)

	retryRequest := reserveRequest("retryable", "attempt-two")
	retryRequest.IndexTime = request.IndexTime.Add(time.Hour)
	retryRequest.Metadata = []byte("changed policy")
	retry, err := sequencer.Reserve(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Sequence != first.Sequence || !retry.PreviouslyReserved {
		t.Fatalf("retry reservation = %+v, want recovered sequence %d", retry, first.Sequence)
	}
	if !retry.IndexTime.Equal(first.IndexTime) || !slices.Equal(retry.Metadata, first.Metadata) {
		t.Fatalf("retry metadata = %v/%q, want %v/%q", retry.IndexTime, retry.Metadata, first.IndexTime, first.Metadata)
	}
	if err := sequencer.Commit(ctx, retry.Sequence, retryRequest.AttemptID); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 1)
}

func TestSQLiteSequencerRecoversAttemptLeaseAfterRestart(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
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
	retry := reserve(t, sequencer, "ambiguous", "new-process")
	if retry.Sequence != pending.Sequence || retry.AlreadyCommitted != pending.AlreadyCommitted ||
		!retry.IndexTime.Equal(pending.IndexTime) || !slices.Equal(retry.Metadata, pending.Metadata) {
		t.Fatalf("reservation after restart = %+v, want %+v", retry, pending)
	}
	if !retry.PreviouslyReserved {
		t.Fatal("recovered pending reservation was not marked previously reserved")
	}
	if err := sequencer.Commit(ctx, retry.Sequence, "new-process"); err != nil {
		t.Fatal(err)
	}
	assertCutoff(t, sequencer, 1)
}

func TestSQLiteSequencerRejectsInvalidInputsAndExhaustion(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	if err := sequencer.Commit(ctx, 99, "attempt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("commit unknown error = %v, want ErrNotFound", err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("", "attempt")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("batch", "")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty attempt error = %v", err)
	}
	if _, err := sequencer.Reserve(nil, reserveRequest("batch", "attempt")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context error = %v", err)
	}
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
	return sequencer, db
}

func reserve(t *testing.T, sequencer *SQLiteSequencer, key, attemptID string) Reservation {
	t.Helper()
	reservation, err := sequencer.Reserve(context.Background(), reserveRequest(key, attemptID))
	if err != nil {
		t.Fatalf("Reserve(%q): %v", key, err)
	}
	return reservation
}

func reserveRequest(key, attemptID string) ReserveRequest {
	return ReserveRequest{
		BatchKey:      key,
		AttemptID:     attemptID,
		IndexTime:     time.Date(2026, time.July, 21, 1, 2, 3, 456000000, time.UTC),
		PayloadSHA256: sha256.Sum256([]byte(key)),
		Metadata:      []byte("retention-v1"),
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
