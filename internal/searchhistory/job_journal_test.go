package searchhistory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func journalJob(id string, state searchjobs.State, now time.Time) searchjobs.Job {
	return searchjobs.Job{
		ID: id, OwnerID: "owner", TenantID: "tenant",
		SPL:              " \nindex=main | head 1\t",
		RequestedIndexes: []string{"main"},
		Earliest:         now.Add(-time.Hour), Latest: now,
		State: state, CreatedAt: now.Add(-time.Minute),
	}
}

func TestJobJournalAdmitsAndFinalizesDetachedSearchMetadata(t *testing.T) {
	database, store := openTestStore(t, Options{})
	journal, err := NewJobJournal(store, " tier-1-test ")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 22, 12, 0, 0, 123_456_789, time.UTC)
	job := journalJob("journal-complete", searchjobs.StateQueued, now)
	if err := journal.Admit(context.Background(), job); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	job.SPL = "mutated"
	job.RequestedIndexes[0] = "mutated"
	var pendingRows int
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history_pending`).Scan(&pendingRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 1 {
		t.Fatalf("pending rows = %d, want 1", pendingRows)
	}

	terminal := journalJob("journal-complete", searchjobs.StateCompleted, now)
	terminal.EffectiveIndexes = []string{"main"}
	terminal.StartedAt = now.Add(-30 * time.Second)
	terminal.FinishedAt = now.Add(-10 * time.Second)
	terminal.RowCount = 7
	terminal.ResultBytes = 1024
	if err := journal.Finalize(context.Background(), terminal); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	got, err := store.Get(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Definition.GetSpl() != " \nindex=main | head 1\t" || got.Definition.GetTimeRange().GetEarliest() != terminal.Earliest.Format(time.RFC3339Nano) ||
		got.FinalState != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED || got.ProducedRows != 7 ||
		got.Duration.AsDuration() != 20*time.Second || got.CompilerVersion != "tier-1-test" || len(got.EffectiveIndexScope) != 1 {
		t.Fatalf("terminal history = %+v", got)
	}
	if got.MatchedEvents != 0 || got.ScannedRows != 0 || got.ScannedBytes != 0 {
		t.Fatalf("unavailable counters were invented: %+v", got)
	}
}

func TestJobJournalMapsAndBoundsSafeFailure(t *testing.T) {
	_, store := openTestStore(t, Options{})
	journal, err := NewJobJournal(store, "test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	queued := journalJob("journal-failed", searchjobs.StateQueued, now)
	if err := journal.Admit(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	failed := journalJob("journal-failed", searchjobs.StateFailed, now)
	failed.StartedAt = now.Add(-30 * time.Second)
	failed.FinishedAt = now
	failed.Failure = &searchjobs.Failure{
		Code: searchjobs.FailureInvalidSPL, Message: strings.Repeat("é", maximumFailureMessageBytes), Retryable: true,
		Diagnostics: []searchjobs.Diagnostic{{
			Code: " SPL_PARSE ", Message: "unexpected token", Line: 1, Column: 2, EndLine: 1, EndColumn: 3,
			Suggestions: []string{"check syntax"},
		}},
	}
	if err := journal.Finalize(context.Background(), failed); err != nil {
		t.Fatalf("Finalize(failed) error = %v", err)
	}
	got, err := store.Get(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Failure.GetCode() != opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL || !got.Failure.GetRetryable() ||
		len(got.Failure.GetMessage()) > maximumFailureMessageBytes || !strings.HasPrefix(got.Failure.GetMessage(), "é") ||
		got.Failure.GetDiagnostics()[0].GetCode() != "SPL_PARSE" || got.Failure.GetDiagnostics()[0].GetSourceRange().GetStart().GetColumn() != 2 {
		t.Fatalf("failure history = %+v", got.Failure)
	}
}

func TestJobJournalRejectsInvalidConstructionAndTransitions(t *testing.T) {
	_, store := openTestStore(t, Options{})
	if _, err := NewJobJournal(nil, "test"); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewJobJournal(nil) error = %v", err)
	}
	if _, err := NewJobJournal(store, " "); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewJobJournal(empty version) error = %v", err)
	}
	journal, err := NewJobJournal(store, "test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := journal.Admit(context.Background(), journalJob("running", searchjobs.StateRunning, now)); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Admit(running) error = %v", err)
	}
	if err := journal.Finalize(context.Background(), journalJob("queued", searchjobs.StateQueued, now)); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Finalize(queued) error = %v", err)
	}
}
