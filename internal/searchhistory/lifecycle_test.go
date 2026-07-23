package searchhistory

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func pendingHistoryEntry(id, spl string, created time.Time) *opensplunkv1.SearchHistoryEntry {
	entry := historyEntry(id, spl, "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	entry.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED
	entry.MatchedEvents = 0
	entry.ScannedRows = 0
	entry.ScannedBytes = 0
	entry.ProducedRows = 0
	entry.Duration = nil
	entry.Warnings = nil
	entry.Failure = nil
	entry.StartedAt = nil
	entry.FinishedAt = nil
	return entry
}

func TestBeginAttemptIsDurableIdempotentScopedAndNotYetVisible(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	pending := pendingHistoryEntry("job-pending", "  index=main | head 1\n", time.Now().UTC())
	started, err := store.BeginAttempt(ctx, scope, pending)
	if err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	if started.FinalState != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED || started.FinishedAt != nil || started.Definition.Spl != pending.Definition.Spl {
		t.Fatalf("BeginAttempt() = %+v", started)
	}
	pending.Definition.Spl = "mutated"
	started.Definition.Spl = "mutated result"
	var pendingRows, terminalRows int
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history_pending`).Scan(&pendingRows); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history`).Scan(&terminalRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 1 || terminalRows != 0 {
		t.Fatalf("rows = pending %d terminal %d", pendingRows, terminalRows)
	}
	if _, err := store.Get(ctx, scope, "job-pending"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(pending) error = %v, want ErrNotFound", err)
	}

	original := pendingHistoryEntry("job-pending", "  index=main | head 1\n", started.CreatedAt.AsTime())
	if _, err := store.BeginAttempt(ctx, scope, original); err != nil {
		t.Fatalf("idempotent BeginAttempt() error = %v", err)
	}
	changed := proto.Clone(original).(*opensplunkv1.SearchHistoryEntry)
	changed.Definition.Spl = "index=other"
	if _, err := store.BeginAttempt(ctx, scope, changed); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("changed BeginAttempt() error = %v, want ErrVersionConflict", err)
	}
	if _, err := store.BeginAttempt(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, original); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("cross-owner BeginAttempt() error = %v, want ErrAlreadyExists", err)
	}
}

func TestCompleteAttemptAtomicallyMovesPendingToTerminalHistory(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	pending := pendingHistoryEntry("job-complete", "index=main | head 1", created)
	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatal(err)
	}
	terminal := historyEntry("job-complete", pending.Definition.Spl, "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	completed, err := store.CompleteAttempt(ctx, scope, terminal)
	if err != nil {
		t.Fatalf("CompleteAttempt() error = %v", err)
	}
	if completed.FinalState != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("completed state = %v", completed.FinalState)
	}
	var pendingRows, terminalRows int
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history_pending`).Scan(&pendingRows); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history`).Scan(&terminalRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 0 || terminalRows != 1 {
		t.Fatalf("rows = pending %d terminal %d", pendingRows, terminalRows)
	}
	got, err := store.Get(ctx, scope, "job-complete")
	if err != nil || !proto.Equal(got, completed) {
		t.Fatalf("Get(completed) = (%+v,%v)", got, err)
	}
	if _, err := store.CompleteAttempt(ctx, scope, terminal); err != nil {
		t.Fatalf("idempotent CompleteAttempt() error = %v", err)
	}
}

func TestCompleteAttemptRejectsChangedImmutableAdmission(t *testing.T) {
	_, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	pending := pendingHistoryEntry("job-changed", "index=main", created)
	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatal(err)
	}
	terminal := historyEntry("job-changed", "index=other", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	if _, err := store.CompleteAttempt(ctx, scope, terminal); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("CompleteAttempt(changed) error = %v, want ErrVersionConflict", err)
	}
	if _, err := store.Get(ctx, scope, "job-changed"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("changed completion became visible: %v", err)
	}
}

func TestRecoverInterruptedFinalizesPendingAttempts(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	_, store := openTestStore(t, Options{Clock: func() time.Time { return now }})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	pending := pendingHistoryEntry("job-interrupted", "\nindex=main | head 1\n", now.Add(-time.Minute))
	pending.StartedAt = timestamppb.New(now.Add(-30 * time.Second))
	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.RecoverInterrupted(ctx, scope)
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverInterrupted() = (%d,%v), want (1,nil)", recovered, err)
	}
	got, err := store.Get(ctx, scope, pending.SearchJobId)
	if err != nil {
		t.Fatal(err)
	}
	if got.FinalState != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		got.Failure.GetCode() != opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL ||
		!got.Failure.GetRetryable() || got.FinishedAt.AsTime() != now ||
		got.Duration.AsDuration() != 30*time.Second || got.Definition.Spl != pending.Definition.Spl {
		t.Fatalf("recovered entry = %+v", got)
	}
	if second, err := store.RecoverInterrupted(ctx, scope); err != nil || second != 0 {
		t.Fatalf("second RecoverInterrupted() = (%d,%v)", second, err)
	}
}

func TestRecoverInterruptedPreservesProvenanceAndExactRangeAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, time.March, 8, 19, 0, 0, 987654321, time.UTC)
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	databasePath := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := New(database, Options{Clock: func() time.Time { return now }, CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}

	appID := "search-app"
	savedID := "saved-restart"
	timezone := "America/Los_Angeles"
	pending := pendingHistoryEntry("job-restart-provenance", "index=main | table message", now.Add(-time.Minute))
	pending.Definition.AppId = &appID
	pending.Definition.TimeRange = &opensplunkv1.TimeRangeSpec{
		Earliest: stringPointer("-1d"), Latest: stringPointer("now"), Timezone: &timezone,
	}
	pending.Source = &opensplunkv1.SearchJobSource{
		Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH, SavedSearchId: &savedID,
	}
	pending.ResolvedTimeRange = &opensplunkv1.ResolvedTimeRange{
		Earliest: timestamppb.New(now.Add(-23 * time.Hour)), Latest: timestamppb.New(now), Timezone: timezone,
	}
	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedStore, err := New(reopened, Options{Clock: func() time.Time { return now }, CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	if recovered, err := reopenedStore.RecoverInterrupted(ctx, scope); err != nil || recovered != 1 {
		t.Fatalf("RecoverInterrupted() = (%d, %v), want (1, nil)", recovered, err)
	}
	got, err := reopenedStore.Get(ctx, scope, pending.GetSearchJobId())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetFinalState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		got.GetDefinition().GetSpl() != pending.GetDefinition().GetSpl() || got.GetDefinition().GetAppId() != appID ||
		got.GetDefinition().GetTimeRange().GetEarliest() != "-1d" || got.GetDefinition().GetTimeRange().GetLatest() != "now" ||
		got.GetDefinition().GetTimeRange().GetTimezone() != timezone ||
		got.GetSource().GetOrigin() != opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		got.GetSource().GetSavedSearchId() != savedID ||
		!got.GetResolvedTimeRange().GetEarliest().AsTime().Equal(pending.GetResolvedTimeRange().GetEarliest().AsTime()) ||
		!got.GetResolvedTimeRange().GetLatest().AsTime().Equal(pending.GetResolvedTimeRange().GetLatest().AsTime()) ||
		got.GetResolvedTimeRange().GetTimezone() != timezone {
		t.Fatalf("recovered provenance = %+v", got)
	}
}

func TestPendingAttemptValidation(t *testing.T) {
	_, store := openTestStore(t, Options{})
	base := pendingHistoryEntry("pending-invalid", "index=main", time.Now().UTC())
	for name, mutate := range map[string]func(*opensplunkv1.SearchHistoryEntry){
		"terminal state": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED
		},
		"finished timestamp": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.FinishedAt = timestamppb.Now()
		},
		"failure": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Failure = &opensplunkv1.SearchFailure{Code: opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL, Message: "bad"}
		},
		"duration": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Duration = durationpb.New(time.Second)
		},
		"counter": func(entry *opensplunkv1.SearchHistoryEntry) { entry.ScannedRows = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			entry := proto.Clone(base).(*opensplunkv1.SearchHistoryEntry)
			mutate(entry)
			if _, err := store.BeginAttempt(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, entry); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("BeginAttempt() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestPendingAttemptCapacityIsScopedAndIdempotent(t *testing.T) {
	_, store := openTestStore(t, Options{MaximumEntriesPerOwner: 1})
	ctx := context.Background()
	created := time.Now().UTC()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	first := pendingHistoryEntry("capacity-first", "index=main", created)
	if _, err := store.BeginAttempt(ctx, scope, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginAttempt(ctx, scope, first); err != nil {
		t.Fatalf("idempotent full BeginAttempt() error = %v", err)
	}
	if _, err := store.BeginAttempt(ctx, scope, pendingHistoryEntry("capacity-second", "index=main", created)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("BeginAttempt(over capacity) error = %v, want ErrCapacity", err)
	}
	if _, err := store.BeginAttempt(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, pendingHistoryEntry("capacity-other", "index=main", created)); err != nil {
		t.Fatalf("BeginAttempt(other owner) error = %v", err)
	}
}

func TestRecoverInterruptedRollsBackScopeOnCorruption(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	for _, id := range []string{"a-valid", "z-corrupt"} {
		if _, err := store.BeginAttempt(ctx, scope, pendingHistoryEntry(id, "index=main", created)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE search_history_pending SET entry_sha256 = zeroblob(32)
		WHERE search_job_id = ?`, "z-corrupt"); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverInterrupted(ctx, scope); err == nil || recovered != 0 {
		t.Fatalf("RecoverInterrupted(corrupt) = (%d,%v), want (0,error)", recovered, err)
	}
	var pendingRows, terminalRows int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM search_history_pending`).Scan(&pendingRows); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM search_history`).Scan(&terminalRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 2 || terminalRows != 0 {
		t.Fatalf("rows after rollback = pending %d terminal %d", pendingRows, terminalRows)
	}
}

func TestRecordCompletesExistingPendingAttempt(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created := time.Now().UTC().Add(-time.Minute)
	pending := pendingHistoryEntry("record-completes", "index=main", created)
	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatal(err)
	}
	terminal := historyEntry(pending.SearchJobId, pending.Definition.Spl, "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	if _, err := store.Record(ctx, scope, terminal); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	var pendingRows int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT COUNT(*) FROM search_history_pending`).Scan(&pendingRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 0 {
		t.Fatalf("pending rows = %d, want 0", pendingRows)
	}
}
