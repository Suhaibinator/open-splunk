package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type recordingScheduledReportFinisher struct {
	mu              sync.Mutex
	submittedRunID  string
	searchJobID     string
	runID           string
	outcome         scheduledreports.RunOutcome
	failureCategory string
	errors          []error
	markError       error
	calls           chan string
}

func newBoundScheduledReportJournal(t *testing.T, finisher scheduledReportRunFinisher) *runtimeScheduledReportJournal {
	t.Helper()
	journal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{})
	if err != nil {
		t.Fatalf("newRuntimeScheduledReportJournal: %v", err)
	}
	if err := journal.Bind(finisher); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return journal
}

func (finisher *recordingScheduledReportFinisher) MarkSubmitted(_ context.Context, runID, jobID string) error {
	finisher.mu.Lock()
	defer finisher.mu.Unlock()
	finisher.submittedRunID = runID
	finisher.searchJobID = jobID
	return finisher.markError
}

func (finisher *recordingScheduledReportFinisher) Finish(_ context.Context, runID string, outcome scheduledreports.RunOutcome, failureCategory string) error {
	finisher.mu.Lock()
	finisher.runID = runID
	finisher.outcome = outcome
	finisher.failureCategory = failureCategory
	var err error
	if len(finisher.errors) > 0 {
		err = finisher.errors[0]
		finisher.errors = finisher.errors[1:]
	}
	finisher.mu.Unlock()
	if finisher.calls != nil {
		finisher.calls <- runID
	}
	return err
}

func (finisher *recordingScheduledReportFinisher) snapshot() (string, scheduledreports.RunOutcome, string) {
	finisher.mu.Lock()
	defer finisher.mu.Unlock()
	return finisher.runID, finisher.outcome, finisher.failureCategory
}

func (finisher *recordingScheduledReportFinisher) submitted() (string, string) {
	finisher.mu.Lock()
	defer finisher.mu.Unlock()
	return finisher.submittedRunID, finisher.searchJobID
}

func TestRuntimeScheduledReportJournalAttachesBeforeImmediateCompletion(t *testing.T) {
	t.Parallel()
	finisher := &recordingScheduledReportFinisher{}
	journal := newBoundScheduledReportJournal(t, finisher)
	t.Cleanup(journal.Close)
	job := searchjobs.Job{
		ID: "job-instant", State: searchjobs.StateCompleted,
		Source: searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: "run-instant"},
	}
	if err := journal.Admit(context.Background(), job); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if err := journal.Finalize(context.Background(), job); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	submittedRunID, searchJobID := finisher.submitted()
	runID, outcome, _ := finisher.snapshot()
	if submittedRunID != "run-instant" || searchJobID != "job-instant" ||
		runID != "run-instant" || outcome != scheduledreports.RunOutcomeSucceeded {
		t.Fatalf(
			"journal order = submitted (%q, %q), terminal (%q, %v)",
			submittedRunID, searchJobID, runID, outcome,
		)
	}
}

func TestRuntimeScheduledReportJournalFailsAdmissionWithoutDurableAttachment(t *testing.T) {
	t.Parallel()
	want := errors.New("attachment unavailable")
	finisher := &recordingScheduledReportFinisher{markError: want}
	journal := newBoundScheduledReportJournal(t, finisher)
	t.Cleanup(journal.Close)
	err := journal.Admit(context.Background(), searchjobs.Job{
		ID:     "job-rejected",
		Source: searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: "run-rejected"},
	})
	if !errors.Is(err, want) {
		t.Fatalf("Admit() error = %v, want %v", err, want)
	}
	unbound, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{})
	if err != nil {
		t.Fatalf("new unbound journal: %v", err)
	}
	if err := unbound.Admit(context.Background(), searchjobs.Job{
		ID:     "job-unbound",
		Source: searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: "run-unbound"},
	}); err == nil {
		t.Fatal("unbound journal accepted scheduled-report admission")
	}
}

func TestRuntimeScheduledReportJournalBindsExactlyOnce(t *testing.T) {
	t.Parallel()
	journal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{})
	if err != nil {
		t.Fatalf("newRuntimeScheduledReportJournal: %v", err)
	}
	if err := journal.Bind(nil); err == nil {
		t.Fatal("Bind(nil) unexpectedly succeeded")
	}
	if err := journal.Bind(&recordingScheduledReportFinisher{}); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := journal.Bind(&recordingScheduledReportFinisher{}); err == nil {
		t.Fatal("second Bind unexpectedly succeeded")
	}
	journal.Close()
	if err := journal.Bind(&recordingScheduledReportFinisher{}); err == nil {
		t.Fatal("Bind after Close unexpectedly succeeded")
	}
}

type controlledScheduledReportClock struct {
	waits    chan time.Duration
	releases chan struct{}
}

func (clock *controlledScheduledReportClock) Now() time.Time { return time.Now() }

func (clock *controlledScheduledReportClock) Wait(ctx context.Context, duration time.Duration) error {
	select {
	case clock.waits <- duration:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-clock.releases:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRuntimeScheduledReportJournalRecordsTerminalOutcome(t *testing.T) {
	t.Parallel()
	finisher := &recordingScheduledReportFinisher{}
	journal := newBoundScheduledReportJournal(t, finisher)
	t.Cleanup(journal.Close)
	err := journal.Finalize(context.Background(), searchjobs.Job{
		State:   searchjobs.StateFailed,
		Source:  searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: "run-1"},
		Failure: &searchjobs.Failure{Code: searchjobs.FailureTimeout},
	})
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	runID, outcome, failureCategory := finisher.snapshot()
	if runID != "run-1" || outcome != scheduledreports.RunOutcomeFailed || failureCategory != "timeout" {
		t.Fatalf("terminal projection = (%q, %v, %q)", runID, outcome, failureCategory)
	}
}

func TestRuntimeScheduledReportJournalIgnoresOtherOrigins(t *testing.T) {
	t.Parallel()
	finisher := &recordingScheduledReportFinisher{}
	journal := newBoundScheduledReportJournal(t, finisher)
	t.Cleanup(journal.Close)
	if err := journal.Finalize(context.Background(), searchjobs.Job{State: searchjobs.StateCompleted}); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	runID, _, _ := finisher.snapshot()
	if runID != "" {
		t.Fatalf("unexpected completed run %q", runID)
	}
}

func TestRuntimeScheduledReportJournalRetriesTransientFinishWithoutFailingFinalize(t *testing.T) {
	t.Parallel()
	transient := errors.New("database temporarily unavailable")
	finisher := &recordingScheduledReportFinisher{errors: []error{transient, nil}, calls: make(chan string, 2)}
	clock := &controlledScheduledReportClock{waits: make(chan time.Duration), releases: make(chan struct{})}
	journal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{
		clock: clock, initialBackoff: 5 * time.Millisecond, maximumBackoff: 20 * time.Millisecond,
		queueCapacity: 2, workers: 1,
	})
	if err != nil {
		t.Fatalf("newRuntimeScheduledReportJournal: %v", err)
	}
	if err := journal.Bind(finisher); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := journal.Finalize(context.Background(), searchjobs.Job{
		State:  searchjobs.StateCompleted,
		Source: searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: "run-retry"},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := <-finisher.calls; got != "run-retry" {
		t.Fatalf("initial finish run = %q", got)
	}
	if journal.pendingCount() != 1 {
		t.Fatalf("pending retries = %d, want 1", journal.pendingCount())
	}
	if got := <-clock.waits; got != 5*time.Millisecond {
		t.Fatalf("retry wait = %v, want 5ms", got)
	}
	clock.releases <- struct{}{}
	if got := <-finisher.calls; got != "run-retry" {
		t.Fatalf("retried finish run = %q", got)
	}
	journal.Close()
	if journal.pendingCount() != 0 {
		t.Fatalf("pending retries after success = %d, want 0", journal.pendingCount())
	}
}

func TestRuntimeScheduledReportJournalBoundsRetryQueueAndCancelsWaits(t *testing.T) {
	t.Parallel()
	transient := errors.New("database unavailable")
	finisher := &recordingScheduledReportFinisher{errors: []error{transient, transient, transient}, calls: make(chan string, 3)}
	clock := &controlledScheduledReportClock{waits: make(chan time.Duration), releases: make(chan struct{})}
	journal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{
		clock: clock, initialBackoff: time.Millisecond, maximumBackoff: 2 * time.Millisecond,
		queueCapacity: 1, workers: 1,
	})
	if err != nil {
		t.Fatalf("newRuntimeScheduledReportJournal: %v", err)
	}
	if err := journal.Bind(finisher); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	job := func(id string) searchjobs.Job {
		return searchjobs.Job{State: searchjobs.StateFailed, Source: searchjobs.JobSource{Origin: searchjobs.JobOriginScheduledReport, ObjectID: id}}
	}
	if err := journal.Finalize(context.Background(), job("run-one")); err != nil {
		t.Fatalf("Finalize first: %v", err)
	}
	<-finisher.calls
	<-clock.waits
	if err := journal.Finalize(context.Background(), job("run-two")); err != nil {
		t.Fatalf("Finalize second: %v", err)
	}
	<-finisher.calls
	if err := journal.Finalize(context.Background(), job("run-three")); err == nil {
		t.Fatal("Finalize with full retry queue unexpectedly succeeded")
	}
	<-finisher.calls
	journal.Close()
}
