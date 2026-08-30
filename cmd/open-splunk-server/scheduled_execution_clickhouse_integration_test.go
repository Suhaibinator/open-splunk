package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestScheduledReportExecutionAgainstClickHouse qualifies the complete
// production path that unit tests cannot: a durable due schedule enters the
// scheduler, resolves current app/index authority, runs through Manager and
// queryexec against ClickHouse, persists its immutable artifact, and closes
// the corresponding scheduled-run history row.
func TestScheduledReportExecutionAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	ctx, connection, _ := startLookupClickHouse(t)
	insertLookupEvents(t, ctx, connection)

	clock := &scheduledExecutionClock{now: time.Date(2026, time.August, 8, 12, 0, 30, 0, time.UTC)}
	root := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(root, "control.db"))
	if err != nil {
		t.Fatalf("open scheduled execution control database: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close scheduled execution control database: %v", closeErr)
		}
	})
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)
	appCatalog, appCursorKey, err := newRuntimeAppCatalog(ctx, database, filepath.Join(root, "server.key"))
	if err != nil {
		t.Fatalf("open scheduled execution app catalog: %v", err)
	}
	t.Cleanup(func() { clear(appCursorKey) })

	savedSearchStore, err := savedobjects.New(database, savedobjects.Options{
		Clock:       clock.Now,
		CursorKey:   bytes.Repeat([]byte{0x53}, 32),
		IDGenerator: func() (string, error) { return "saved-scheduled-clickhouse", nil },
	})
	if err != nil {
		t.Fatalf("open saved-search store: %v", err)
	}
	appID := runtimeKnowledgeTestApp
	earliest := "2026-08-08T10:00:00Z"
	latest := "2026-08-08T11:00:00Z"
	timezone := "UTC"
	saved, err := savedSearchStore.Create(ctx, savedobjects.AccessScope{OwnerID: runtimeKnowledgeTestOwner}, &opensplunk.SavedSearchDefinition{
		Name: "Scheduled ClickHouse qualification",
		Search: &opensplunk.SearchDefinition{
			Spl: "index=main | table event_id", AppId: &appID,
			IndexScope: []string{"main"},
			TimeRange:  &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest, Timezone: &timezone},
		},
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	})
	if err != nil {
		t.Fatalf("create scheduled saved search: %v", err)
	}

	artifactStore, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB: database.SQLDB(), Directory: filepath.Join(root, "search-artifacts"),
		Clock: clock.Now, CleanupInterval: -1,
	})
	if err != nil {
		t.Fatalf("open durable search artifacts: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := artifactStore.Close(); closeErr != nil {
			t.Errorf("close durable search artifacts: %v", closeErr)
		}
	})

	scheduledRepository, err := scheduledreports.NewRepository(database.GORMDB(), scheduledreports.RepositoryOptions{
		Clock:     clock.Now,
		CursorKey: bytes.Repeat([]byte{0x52}, 32),
		IDGenerator: func() (string, error) {
			return "run-scheduled-clickhouse", nil
		},
	})
	if err != nil {
		t.Fatalf("open scheduled-report repository: %v", err)
	}
	runJournal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{})
	if err != nil {
		t.Fatalf("create scheduled-report journal: %v", err)
	}
	completed := make(chan struct{}, 1)
	signalingJournal := &scheduledExecutionCompletionJournal{target: runJournal, completed: completed}
	executor, err := queryexec.New(connection, queryexec.Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatalf("create scheduled ClickHouse executor: %v", err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: executor, Snapshotter: scheduledExecutionSnapshotter(42),
		Journal:       searchjobs.NewCompositeJournal(artifactStore, signalingJournal),
		Compiler:      internalclickhouse.Compiler{Database: "open_splunk", Table: "events"},
		MaxConcurrent: 1, MaxQueued: 4, MaxJobs: 8, MaxRows: 100,
		CleanupInterval: -1, Now: clock.Now,
		NewID: func() string { return "job-scheduled-clickhouse" },
	})
	if err != nil {
		t.Fatalf("create scheduled search manager: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close scheduled search manager: %v", closeErr)
		}
	})
	admission := &runtimeTrustedSearchAdmission{
		jobs: manager, indexes: database, apps: appCatalog, clock: clock.Now,
	}
	service, err := scheduledreports.NewService(scheduledreports.ServiceOptions{
		Store: scheduledRepository, Admitter: admission, Clock: clock.Now,
	})
	if err != nil {
		t.Fatalf("create scheduled-report service: %v", err)
	}
	if err := runJournal.Bind(service); err != nil {
		t.Fatalf("bind scheduled-report journal: %v", err)
	}
	schedule, err := service.Configure(ctx, runtimeKnowledgeTestOwner, runtimeKnowledgeTestTenant, saved.GetSavedSearchId(), 0, scheduledreports.Configuration{
		Cron: "* * * * *", Timezone: "UTC", Enabled: true,
	})
	if err != nil {
		t.Fatalf("configure scheduled report: %v", err)
	}
	if schedule.NextRunAt == nil {
		t.Fatal("configured schedule has no next occurrence")
	}
	clock.Set(*schedule.NextRunAt)
	engine, err := scheduler.NewEngine(scheduler.EngineOptions{Clock: clock, Stepper: service})
	if err != nil {
		t.Fatalf("create scheduled-report engine: %v", err)
	}
	if err := engine.Step(ctx); err != nil {
		t.Fatalf("execute due scheduled report: %v", err)
	}

	select {
	case <-completed:
	case <-time.After(15 * time.Second):
		t.Fatal("scheduled execution did not publish its terminal journal record")
	}
	job, artifact, run := completedScheduledExecution(t, ctx, manager, artifactStore, service, saved.GetSavedSearchId())
	if job.State != searchjobs.StateCompleted || job.Failure != nil ||
		job.Source.Origin != searchjobs.JobOriginScheduledReport ||
		job.Source.ObjectID != run.RunID || job.Source.ScheduledAt != run.ScheduledAt ||
		job.AppID != runtimeKnowledgeTestApp || job.TenantID != runtimeKnowledgeTestTenant ||
		job.OwnerID != runtimeKnowledgeTestOwner ||
		!slices.Equal(job.EffectiveIndexes, []string{"main"}) ||
		!slices.Equal(job.RequestedIndexes, []string{"main"}) {
		t.Fatalf("scheduled search job = %#v", job)
	}
	if artifact.State != searchartifacts.StateCompleted || !artifact.ArtifactPresent ||
		artifact.RetentionClass != searchartifacts.RetentionScheduledReport ||
		artifact.Lifetime != 2*time.Minute {
		t.Fatalf("durable scheduled artifact = %#v", artifact)
	}
	if run.Outcome != scheduledreports.RunOutcomeSucceeded || run.SearchJobID != job.ID ||
		run.DefinitionVersion != saved.GetVersion() || run.RetentionLifetime != 2*time.Minute ||
		run.SchedulePeriod != time.Minute || run.FinishedAt == nil {
		t.Fatalf("scheduled run history = %#v", run)
	}

	lease, err := artifactStore.Acquire(ctx, searchjobs.AccessScope{
		TenantID: runtimeKnowledgeTestTenant, OwnerID: runtimeKnowledgeTestOwner,
	}, job.ID)
	if err != nil {
		t.Fatalf("acquire durable scheduled results: %v", err)
	}
	defer func() {
		if closeErr := lease.Close(); closeErr != nil {
			t.Errorf("close durable scheduled result lease: %v", closeErr)
		}
	}()
	if !lease.RowCountExact() || lease.ResultsTruncated() || lease.RowCount() != 5 ||
		len(lease.Schema().Columns) != 1 || lease.Schema().Columns[0].Name != "event_id" {
		t.Fatalf("durable scheduled result metadata: schema=%#v rows=%d exact=%t truncated=%t", lease.Schema(), lease.RowCount(), lease.RowCountExact(), lease.ResultsTruncated())
	}
	identifiers := make([]string, 0, lease.RowCount())
	for {
		row, ok, nextErr := lease.Next(ctx)
		if nextErr != nil {
			t.Fatalf("read durable scheduled result: %v", nextErr)
		}
		if !ok {
			break
		}
		if len(row.Values) != 1 {
			t.Fatalf("durable scheduled row = %#v", row)
		}
		identifier, valid := row.Values[0].String()
		if !valid {
			t.Fatalf("durable scheduled event_id = %#v", row.Values[0])
		}
		identifiers = append(identifiers, identifier)
	}
	slices.Sort(identifiers)
	if !slices.Equal(identifiers, []string{
		"lookup-01-exact", "lookup-02-empty-key", "lookup-03-present-empty",
		"lookup-04-number", "lookup-05-case-mismatch",
	}) {
		t.Fatalf("durable scheduled event IDs = %v", identifiers)
	}
}

type scheduledExecutionClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *scheduledExecutionClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *scheduledExecutionClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (*scheduledExecutionClock) Wait(context.Context, time.Duration) error { return nil }

type scheduledExecutionSnapshotter uint64

func (snapshotter scheduledExecutionSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshotter), nil
}

type scheduledExecutionCompletionJournal struct {
	target    *runtimeScheduledReportJournal
	completed chan<- struct{}
}

func (journal *scheduledExecutionCompletionJournal) Admit(ctx context.Context, job searchjobs.Job) error {
	return journal.target.Admit(ctx, job)
}

func (journal *scheduledExecutionCompletionJournal) Finalize(ctx context.Context, job searchjobs.Job) error {
	err := journal.target.Finalize(ctx, job)
	if err == nil && job.Source.Origin == searchjobs.JobOriginScheduledReport && job.State.Terminal() {
		select {
		case journal.completed <- struct{}{}:
		default:
		}
	}
	return err
}

func completedScheduledExecution(
	t *testing.T,
	ctx context.Context,
	manager *searchjobs.Manager,
	artifacts *searchartifacts.Store,
	service *scheduledreports.Service,
	savedSearchID string,
) (searchjobs.Job, searchartifacts.Record, scheduledreports.Run) {
	t.Helper()
	access := searchjobs.AccessScope{TenantID: runtimeKnowledgeTestTenant, OwnerID: runtimeKnowledgeTestOwner}
	job, jobErr := manager.GetFor(access, "job-scheduled-clickhouse")
	artifact, artifactErr := artifacts.Get(ctx, access, "job-scheduled-clickhouse", searchartifacts.AccessInspect)
	runs, runsErr := service.ListRuns(ctx, runtimeKnowledgeTestOwner, savedSearchID, 1)
	if jobErr != nil || !job.State.Terminal() || artifactErr != nil || !artifact.ArtifactPresent ||
		runsErr != nil || len(runs) != 1 || runs[0].Outcome != scheduledreports.RunOutcomeSucceeded {
		t.Fatalf("scheduled execution did not settle: job=%#v jobErr=%v artifact=%#v artifactErr=%v runs=%#v runsErr=%v", job, jobErr, artifact, artifactErr, runs, runsErr)
	}
	return job, artifact, runs[0]
}
