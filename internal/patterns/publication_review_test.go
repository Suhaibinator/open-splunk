package patterns

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type publicationExecutor struct{}

func (publicationExecutor) Execute(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}}); err != nil {
		return err
	}
	for _, raw := range []string{"request 1", "request 2", "other 3"} {
		if err := sink.AddRow([]searchjobs.Value{searchjobs.StringValue(raw)}); err != nil {
			return err
		}
	}
	return nil
}

type publicationSnapshotter struct{}

func (publicationSnapshotter) VisibilityCutoff(context.Context) (uint64, error) { return 0, nil }

type publicationBarrierJournal struct {
	*searchartifacts.Store
	beforeFinalize bool
	fail           bool
	entered        chan struct{}
	release        chan struct{}
	once           sync.Once
}

func (journal *publicationBarrierJournal) wait(ctx context.Context) error {
	close(journal.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-journal.release:
	}
	if journal.fail {
		return errors.New("injected publication failure")
	}
	return nil
}

func (journal *publicationBarrierJournal) unblock() {
	journal.once.Do(func() { close(journal.release) })
}

func (journal *publicationBarrierJournal) Finalize(ctx context.Context, job searchjobs.Job) error {
	if journal.beforeFinalize && job.State == searchjobs.StateCompleted {
		if err := journal.wait(ctx); err != nil {
			return err
		}
	}
	return journal.Store.Finalize(ctx, job)
}

func (journal *publicationBarrierJournal) FinalizeResults(ctx context.Context, job searchjobs.Job, lease searchjobs.ResultLease) error {
	if !journal.beforeFinalize {
		lease = &publicationBarrierLease{ResultLease: lease, journal: journal}
	}
	return journal.Store.FinalizeResults(ctx, job, lease)
}

type publicationBarrierLease struct {
	searchjobs.ResultLease
	journal *publicationBarrierJournal
	entered bool
}

func (lease *publicationBarrierLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if !lease.entered {
		lease.entered = true
		if err := lease.journal.wait(ctx); err != nil {
			return searchjobs.ResultRow{}, false, err
		}
	}
	return lease.ResultLease.Next(ctx)
}

func TestPublicationReviewCompletedLiveSearchWaitsForDurableRelation(t *testing.T) {
	for _, phase := range []string{"before-finalize", "during-artifact-write"} {
		t.Run(phase, func(t *testing.T) {
			for _, outcome := range []string{"publish", "cancel", "close", "fail"} {
				t.Run(outcome, func(t *testing.T) { testPublicationReview(t, phase == "before-finalize", outcome) })
			}
		})
	}
}

func testPublicationReview(t *testing.T, beforeFinalize bool, outcome string) {
	t.Helper()
	ctx := t.Context()
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: database.SQLDB(), Directory: filepath.Join(directory, "artifacts"), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	journal := &publicationBarrierJournal{Store: store, beforeFinalize: beforeFinalize, fail: outcome == "fail", entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := searchjobs.New(searchjobs.Config{Executor: publicationExecutor{}, Snapshotter: publicationSnapshotter{}, Journal: searchjobs.NewCompositeJournal(journal), CursorKey: []byte("publication-review-32-byte-key!!!"), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	defer journal.unblock()
	service := newReviewService(t, Config{Source: store})
	timeRange, err := searchtime.NewAbsoluteRange(time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	job, err := manager.Create(ctx, searchjobs.CreateRequest{SPL: "index=main | table _raw", TenantID: access.TenantID, OwnerID: access.OwnerID, AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, TimeRange: timeRange})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-journal.entered:
	case <-time.After(5 * time.Second):
		live, getErr := manager.GetFor(access, job.ID)
		t.Fatalf("publication did not enter barrier: live=%+v, error=%v", live, getErr)
	}
	live, err := manager.GetFor(access, job.ID)
	if err != nil || live.State != searchjobs.StateCompleted {
		t.Fatalf("live job = %+v, %v", live, err)
	}
	page, err := manager.ResultsFor(access, job.ID, searchjobs.PageRequest{})
	if err != nil || page.Generation == 0 || len(page.Rows) != 3 {
		t.Fatalf("live result snapshot = %+v, %v", page, err)
	}
	// This is the exact manager/store mismatch that previously became HTTP 409.
	lease, err := store.Acquire(ctx, access, job.ID)
	if lease != nil {
		_ = lease.Close()
	}
	if !errors.Is(err, searchartifacts.ErrNotReady) {
		t.Fatalf("pending durable acquire = %v", err)
	}
	request := ListRequest{SearchJobID: job.ID, Generation: page.Generation, Sensitivity: Balanced, IncludeTotal: true}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		page ListResult
		err  error
	}
	done := make(chan result, 1)
	go func() { page, err := service.List(callCtx, access, request); done <- result{page, err} }()
	select {
	case result := <-done:
		result.page.Close()
		t.Fatalf("Patterns returned before durable publication: %v", result.err)
	case <-time.After(30 * time.Millisecond):
	}
	switch outcome {
	case "cancel":
		cancel()
	case "close":
		closeCtx, stopClose := context.WithTimeout(ctx, time.Second)
		defer stopClose()
		if err := service.Close(closeCtx); err != nil {
			t.Fatal(err)
		}
	default:
		journal.unblock()
	}
	select {
	case result := <-done:
		defer result.page.Close()
		switch outcome {
		case "cancel", "close":
			if !errors.Is(result.err, context.Canceled) {
				t.Fatalf("canceled analysis = %v", result.err)
			}
		case "fail":
			if !errors.Is(result.err, searchartifacts.ErrNotReady) || len(result.page.Patterns) != 0 {
				t.Fatalf("failed publication = %+v, %v", result.page, result.err)
			}
		default:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.page.EligibleEventCount != 3 || len(result.page.Patterns) != 2 || result.page.Patterns[0].Signature != "request <int>" || result.page.Patterns[0].EventCount != 2 {
				t.Fatalf("published relation = %+v", result.page)
			}
			members, err := service.Members(ctx, access, MemberRequest{SearchJobID: job.ID, Generation: page.Generation, Sensitivity: Balanced, PatternID: result.page.Patterns[0].ID})
			if err != nil {
				t.Fatal(err)
			}
			defer members.Close()
			if len(members.Rows) != 2 {
				t.Fatalf("published member count = %d", len(members.Rows))
			}
			for index, member := range members.Rows {
				expected := page.Rows[index]
				if member.Ordinal != expected.Ordinal || member.TimeBucket != nil || expected.TimeBucket != nil || len(member.Values) != 1 || len(expected.Values) != 1 {
					t.Fatalf("published member metadata = %+v; expected %+v", member, expected)
				}
				actualRaw, actualString := member.Values[0].String()
				expectedRaw, expectedString := expected.Values[0].String()
				if !actualString || !expectedString || actualRaw != expectedRaw {
					t.Fatalf("published member value = %q (%t); expected %q (%t)", actualRaw, actualString, expectedRaw, expectedString)
				}
			}

		}
	case <-time.After(time.Second):
		t.Fatal("publication waiter did not settle")
	}
}
