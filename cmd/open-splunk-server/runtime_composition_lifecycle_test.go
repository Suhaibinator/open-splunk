package main

import (
	"bytes"
	"slices"
	"sync"
	"testing"
)

func TestRuntimeAlertLifecycleErasesCursorKeyIdempotently(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x7f}, 32)
	lifecycle := &runtimeAlertLifecycle{appCursorKey: key}
	lifecycle.EraseCursorKey()
	lifecycle.EraseCursorKey()
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("alert lifecycle retained app cursor key material")
	}
}

func TestRuntimeLifecycleCloseHandlesPartialStartupIdempotently(t *testing.T) {
	t.Parallel()
	var searchClosed []string
	searchLifecycle := &runtimeSearchLifecycle{
		closeSearchArtifacts: func() error {
			searchClosed = append(searchClosed, "artifacts")
			return nil
		},
		closeScheduledReports: func() { searchClosed = append(searchClosed, "scheduled reports") },
		closeSearchJobs: func() error {
			searchClosed = append(searchClosed, "search jobs")
			return nil
		},
	}
	searchLifecycle.Close()
	searchLifecycle.Close()
	if expected := []string{"search jobs", "scheduled reports", "artifacts"}; !slices.Equal(searchClosed, expected) {
		t.Fatalf("partial search lifecycle close order = %v, want %v", searchClosed, expected)
	}

	alertLifecycle := &runtimeAlertLifecycle{appCursorKey: bytes.Repeat([]byte{0x7f}, 32)}
	alertLifecycle.Close()
	alertLifecycle.Close()
	if !bytes.Equal(alertLifecycle.appCursorKey, make([]byte, len(alertLifecycle.appCursorKey))) {
		t.Fatal("partial alert lifecycle retained app cursor key material")
	}

	scheduledLifecycle := &runtimeScheduledReportLifecycle{}
	scheduledLifecycle.Close()
	scheduledLifecycle.Close()
}

func TestRuntimeSearchLifecycleClosesInDependencyOrderOnceConcurrently(t *testing.T) {
	t.Parallel()
	var closed []string
	closeStep := func(name string) func() error {
		return func() error {
			closed = append(closed, name)
			return nil
		}
	}
	lifecycle := &runtimeSearchLifecycle{
		closeWebSocket:        closeStep("websocket"),
		closeAnalysis:         closeStep("analysis"),
		closeExports:          closeStep("exports"),
		closeInspection:       closeStep("inspection"),
		closeSearchJobs:       closeStep("search jobs"),
		closeScheduledReports: func() { closed = append(closed, "scheduled reports") },
		closeSearchArtifacts:  closeStep("artifacts"),
	}

	closeConcurrently(32, lifecycle.Close)

	expected := []string{
		"websocket", "analysis", "exports", "inspection", "search jobs", "scheduled reports", "artifacts",
	}
	if !slices.Equal(closed, expected) {
		t.Fatalf("search lifecycle close order = %v, want %v", closed, expected)
	}
}

func TestRuntimeAlertLifecycleClosesCoordinatorBeforeErasingKeyOnceConcurrently(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x7f}, 32)
	coordinatorCloses := 0
	coordinatorObservedErasedKey := false
	lifecycle := &runtimeAlertLifecycle{
		appCursorKey: key,
		closeCoordinator: func() error {
			coordinatorCloses++
			if bytes.Equal(key, make([]byte, len(key))) {
				coordinatorObservedErasedKey = true
			}
			return nil
		},
	}

	closeConcurrently(32, lifecycle.Close)

	if coordinatorCloses != 1 {
		t.Fatalf("alert coordinator close count = %d, want 1", coordinatorCloses)
	}
	if coordinatorObservedErasedKey {
		t.Fatal("alert lifecycle erased cursor key before closing coordinator")
	}
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("alert lifecycle retained app cursor key material")
	}
}

func TestRuntimeScheduledReportLifecycleClosesJournalOnceConcurrently(t *testing.T) {
	t.Parallel()
	journalCloses := 0
	lifecycle := &runtimeScheduledReportLifecycle{
		closeJournal: func() { journalCloses++ },
	}

	closeConcurrently(32, lifecycle.Close)

	if journalCloses != 1 {
		t.Fatalf("scheduled report journal close count = %d, want 1", journalCloses)
	}
}

func closeConcurrently(callers int, closeLifecycle func()) {
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(callers)
	for range callers {
		go func() {
			defer workers.Done()
			<-start
			closeLifecycle()
		}()
	}
	close(start)
	workers.Wait()
}
