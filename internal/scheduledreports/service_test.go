package scheduledreports

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type recordingAdmitter struct {
	mu       sync.Mutex
	requests []AdmissionRequest
	err      error
}

type instantCompletionAdmitter struct {
	service *Service
	jobID   string
}

type compensatedFailureAdmitter struct {
	service *Service
}

func (admitter compensatedFailureAdmitter) AdmitScheduledReport(
	ctx context.Context,
	request AdmissionRequest,
) (string, error) {
	if err := admitter.service.MarkSubmitted(ctx, request.RunID, "job-compensated"); err != nil {
		return "", err
	}
	if err := admitter.service.Finish(ctx, request.RunID, RunOutcomeCanceled, ""); err != nil {
		return "", err
	}
	return "", errors.New("later admission projection failed")
}

func (admitter instantCompletionAdmitter) AdmitScheduledReport(
	ctx context.Context,
	request AdmissionRequest,
) (string, error) {
	if err := admitter.service.MarkSubmitted(ctx, request.RunID, admitter.jobID); err != nil {
		return "", err
	}
	if err := admitter.service.Finish(ctx, request.RunID, RunOutcomeSucceeded, ""); err != nil {
		return "", err
	}
	return admitter.jobID, nil
}

func (admitter *recordingAdmitter) AdmitScheduledReport(_ context.Context, request AdmissionRequest) (string, error) {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	admitter.requests = append(admitter.requests, request)
	if admitter.err != nil {
		return "", admitter.err
	}
	return "job-" + request.RunID, nil
}

func (admitter *recordingAdmitter) snapshot() []AdmissionRequest {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	return append([]AdmissionRequest(nil), admitter.requests...)
}

func TestServiceSkipsOlderMissedPeriodsAndSnapshotsDefinition(t *testing.T) {
	t.Parallel()
	configuredAt := time.Date(2026, time.August, 29, 12, 1, 0, 0, time.UTC)
	repository, database := newTestRepository(t, configuredAt)
	seedSavedSearch(t, database, "saved-1", "owner-1", 7, "search index=main | stats count")
	admitter := new(recordingAdmitter)
	service := newTestService(t, repository, admitter, configuredAt)

	created, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-1", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", DispatchTTL: "", Enabled: true,
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if created.NextRunAt == nil || !created.NextRunAt.Equal(time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC)) {
		t.Fatalf("next run = %v", created.NextRunAt)
	}

	stepAt := time.Date(2026, time.August, 29, 12, 17, 0, 0, time.UTC)
	if err := service.Step(context.Background(), stepAt); err != nil {
		t.Fatalf("Step: %v", err)
	}
	requests := admitter.snapshot()
	if len(requests) != 1 {
		t.Fatalf("admissions = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.DefinitionVersion != 7 || request.Definition.GetSearch().GetSpl() != "search index=main | stats count" {
		t.Fatalf("definition snapshot = version %d, SPL %q", request.DefinitionVersion, request.Definition.GetSearch().GetSpl())
	}
	if want := time.Date(2026, time.August, 29, 12, 15, 0, 0, time.UTC); !request.ScheduledAt.Equal(want) {
		t.Fatalf("scheduled at = %v, want %v", request.ScheduledAt, want)
	}
	if request.SchedulePeriod != 5*time.Minute || request.RetentionLifetime != 10*time.Minute {
		t.Fatalf("period/retention = %v/%v, want 5m/10m", request.SchedulePeriod, request.RetentionLifetime)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-1", 0)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].SkippedOccurrenceCount != 2 || runs[0].Outcome != RunOutcomeSubmitted || runs[0].SearchJobID == "" {
		t.Fatalf("runs = %#v", runs)
	}
	updated, err := repository.Get(context.Background(), "owner-1", "saved-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if updated.NextRunAt == nil || !updated.NextRunAt.Equal(time.Date(2026, time.August, 29, 12, 20, 0, 0, time.UTC)) {
		t.Fatalf("advanced next run = %v", updated.NextRunAt)
	}
}

func TestServiceRecordsOverlapWithoutSecondAdmission(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-overlap", "owner-1", 1, "search index=main")
	admitter := new(recordingAdmitter)
	service := newTestService(t, repository, admitter, now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-overlap", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", DispatchTTL: "2p", Enabled: true,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("first Step: %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 10, 0, 0, time.UTC)); err != nil {
		t.Fatalf("second Step: %v", err)
	}
	if got := len(admitter.snapshot()); got != 1 {
		t.Fatalf("admissions = %d, want 1", got)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-overlap", 0)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 || runs[0].Outcome != RunOutcomeSkippedOverlap || runs[1].Outcome != RunOutcomeSubmitted {
		t.Fatalf("run outcomes = %#v", runs)
	}
}

func TestServiceAdmissionFailureIsTerminalAndDoesNotBlockNextRun(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-failure", "owner-1", 1, "search index=main")
	admitter := &recordingAdmitter{err: errors.New("test admission failure")}
	service := newTestService(t, repository, admitter, now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-failure", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: true,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Step should record an expected admission failure without stopping the scheduler: %v", err)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-failure", 0)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].Outcome != RunOutcomeFailed || runs[0].FailureCategory != "admission_failed" || runs[0].FinishedAt == nil {
		t.Fatalf("failed run = %#v", runs)
	}

	admitter.err = nil
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 10, 0, 0, time.UTC)); err != nil {
		t.Fatalf("next Step: %v", err)
	}
	if got := len(admitter.snapshot()); got != 2 {
		t.Fatalf("admissions = %d, want 2", got)
	}
}

func TestServiceAcceptsManagerCompensationAfterJournalAttachment(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-compensated", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	service.admitter = compensatedFailureAdmitter{service: service}
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-compensated", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: true,
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Step() propagated compensated admission failure: %v", err)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-compensated", 1)
	if err != nil || len(runs) != 1 || runs[0].Outcome != RunOutcomeCanceled || runs[0].SearchJobID != "job-compensated" {
		t.Fatalf("compensated history = %#v, %v", runs, err)
	}
}

func TestRunNowReturnsAdmittedSearchJobID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-run-now", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-run-now", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	run, err := service.RunNow(context.Background(), "owner-1", "saved-run-now")
	if err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	if want := "job-" + run.RunID; run.SearchJobID != want || run.Outcome != RunOutcomeSubmitted {
		t.Fatalf("RunNow result = outcome %v job %q, want submitted job %q", run.Outcome, run.SearchJobID, want)
	}
}

func TestRunNowRecordsOverlapAndReturnsConflictWithoutEmptyJobContract(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-run-overlap", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-run-overlap", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if _, err := service.RunNow(context.Background(), "owner-1", "saved-run-overlap"); err != nil {
		t.Fatalf("first RunNow() error = %v", err)
	}
	if run, err := service.RunNow(context.Background(), "owner-1", "saved-run-overlap"); !errors.Is(err, ErrConflict) || run.RunID != "" {
		t.Fatalf("overlapping RunNow() = %#v, %v; want empty result and ErrConflict", run, err)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-run-overlap", 2)
	if err != nil || len(runs) != 2 || runs[0].Outcome != RunOutcomeSkippedOverlap || runs[1].Outcome != RunOutcomeSubmitted {
		t.Fatalf("overlap history = %#v, %v", runs, err)
	}
}

func TestInstantSearchCompletionBeforeAdmissionReturnIsIdempotent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-instant", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	service.admitter = instantCompletionAdmitter{service: service, jobID: "job-instant"}
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-instant", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	run, err := service.RunNow(context.Background(), "owner-1", "saved-instant")
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if run.SearchJobID != "job-instant" {
		t.Fatalf("RunNow() job ID = %q", run.SearchJobID)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-instant", 1)
	if err != nil || len(runs) != 1 || runs[0].Outcome != RunOutcomeSucceeded || runs[0].SearchJobID != "job-instant" {
		t.Fatalf("instant completion history = %#v, %v", runs, err)
	}
	if err := service.MarkSubmitted(context.Background(), runs[0].RunID, "job-other"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting terminal attachment error = %v, want ErrConflict", err)
	}
}

func TestScheduledOccurrenceCanFollowRunNowAtSameTimestamp(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-exact-time", "owner-1", 1, "search index=main")
	admitter := new(recordingAdmitter)
	service := newTestService(t, repository, admitter, now)
	schedule, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-exact-time", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: true,
	})
	if err != nil || schedule.NextRunAt == nil {
		t.Fatalf("Configure() = %+v, %v", schedule, err)
	}
	period := 5 * time.Minute
	manual, claimed, err := repository.ClaimRunNow(context.Background(), schedule, *schedule.NextRunAt, period, 2*period)
	if err != nil || !claimed {
		t.Fatalf("ClaimRunNow() = %+v, %t, %v", manual, claimed, err)
	}
	if err := repository.Finish(context.Background(), manual.RunID, RunOutcomeSucceeded, "", *schedule.NextRunAt); err != nil {
		t.Fatalf("Finish(run now) error = %v", err)
	}
	if err := service.Step(context.Background(), *schedule.NextRunAt); err != nil {
		t.Fatalf("Step(exact timestamp) error = %v", err)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", schedule.SavedSearchID, 0)
	if err != nil || len(runs) != 2 || runs[0].Outcome != RunOutcomeSubmitted {
		t.Fatalf("runs after exact-time claims = %#v, %v", runs, err)
	}
	updated, err := repository.Get(context.Background(), "owner-1", schedule.SavedSearchID)
	if err != nil || updated.NextRunAt == nil || !updated.NextRunAt.After(*schedule.NextRunAt) {
		t.Fatalf("advanced schedule = %+v, %v", updated, err)
	}
}

func TestRunNowOrOneOffSupportsUnscheduledSavedSearch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-one-off", "owner-1", 3, "search index=main | stats count")
	admitter := new(recordingAdmitter)
	service := newTestService(t, repository, admitter, now)

	run, err := service.RunNowOrOneOff(
		context.Background(), "owner-1", "tenant-1", "saved-one-off", DefaultOneOffSchedulePeriod,
	)
	if err != nil {
		t.Fatalf("RunNowOrOneOff: %v", err)
	}
	if want := "job-" + run.RunID; run.SearchJobID != want || run.Outcome != RunOutcomeSubmitted {
		t.Fatalf("one-off result = outcome %v job %q, want submitted job %q", run.Outcome, run.SearchJobID, want)
	}
	if run.Cron != DefaultOneOffCron || run.Timezone != DefaultOneOffTimezone || run.DispatchTTL != DefaultOneOffDispatchTTL {
		t.Fatalf("one-off schedule snapshot = %q, %q, %q", run.Cron, run.Timezone, run.DispatchTTL)
	}
	if run.SchedulePeriod != DefaultOneOffSchedulePeriod || run.RetentionLifetime != 2*DefaultOneOffSchedulePeriod {
		t.Fatalf("one-off retention = period %v lifetime %v", run.SchedulePeriod, run.RetentionLifetime)
	}
	requests := admitter.snapshot()
	if len(requests) != 1 || requests[0].SchedulePeriod != DefaultOneOffSchedulePeriod || requests[0].RetentionLifetime != 2*DefaultOneOffSchedulePeriod {
		t.Fatalf("one-off admission = %#v", requests)
	}
	if _, err := repository.Get(context.Background(), "owner-1", "saved-one-off"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("one-off unexpectedly created a persisted schedule: %v", err)
	}
}

func TestRunHistoryPaginationUsesOwnerScopedKeysetCursor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-pages", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-pages", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	for range 5 {
		run, err := service.RunNow(context.Background(), "owner-1", "saved-pages")
		if err != nil {
			t.Fatalf("RunNow: %v", err)
		}
		if err := service.Finish(context.Background(), run.RunID, RunOutcomeSucceeded, ""); err != nil {
			t.Fatalf("Finish: %v", err)
		}
	}
	first, err := service.ListRunPage(context.Background(), "owner-1", "saved-pages", RunPageRequest{Limit: 2, IncludeTotal: true})
	if err != nil {
		t.Fatalf("ListRunPage first: %v", err)
	}
	if len(first.Runs) != 2 || first.Runs[0].RunID != "run-0005" || first.Runs[1].RunID != "run-0004" || first.NextPageToken == "" || first.TotalSize == nil || *first.TotalSize != 5 {
		t.Fatalf("first page = %#v", first)
	}
	inserted, err := service.RunNow(context.Background(), "owner-1", "saved-pages")
	if err != nil {
		t.Fatalf("RunNow inserted: %v", err)
	}
	if err := service.Finish(context.Background(), inserted.RunID, RunOutcomeSucceeded, ""); err != nil {
		t.Fatalf("Finish inserted: %v", err)
	}
	second, err := service.ListRunPage(context.Background(), "owner-1", "saved-pages", RunPageRequest{Limit: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatalf("ListRunPage second: %v", err)
	}
	if len(second.Runs) != 2 || second.Runs[0].RunID != "run-0003" || second.Runs[1].RunID != "run-0002" || second.NextPageToken == "" {
		t.Fatalf("second page = %#v", second)
	}
	third, err := service.ListRunPage(context.Background(), "owner-1", "saved-pages", RunPageRequest{Limit: 2, PageToken: second.NextPageToken})
	if err != nil {
		t.Fatalf("ListRunPage third: %v", err)
	}
	if len(third.Runs) != 1 || third.Runs[0].RunID != "run-0001" || third.NextPageToken != "" {
		t.Fatalf("third page = %#v", third)
	}
	if _, err := service.ListRunPage(context.Background(), "another-owner", "saved-pages", RunPageRequest{Limit: 2, PageToken: first.NextPageToken}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("cross-owner cursor error = %v, want ErrInvalidArgument", err)
	}
}

func TestFinishIsIdempotentForIdenticalTerminalOutcome(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-finish-retry", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-finish-retry", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	run, err := service.RunNow(context.Background(), "owner-1", "saved-finish-retry")
	if err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	if err := service.Finish(context.Background(), run.RunID, RunOutcomeSucceeded, ""); err != nil {
		t.Fatalf("Finish first: %v", err)
	}
	if err := service.Finish(context.Background(), run.RunID, RunOutcomeSucceeded, ""); err != nil {
		t.Fatalf("Finish retry: %v", err)
	}
	if err := service.Finish(context.Background(), run.RunID, RunOutcomeFailed, "late_failure"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Finish error = %v, want ErrConflict", err)
	}
}

func TestCurrentProjectionsBatchSchedulesLatestRunsAndRetainedResults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-a", "owner-1", 1, "search index=main")
	seedSavedSearch(t, database, "saved-b", "owner-1", 1, "search index=main")
	seedSavedSearch(t, database, "saved-c", "owner-1", 1, "search index=main")
	seedSavedSearch(t, database, "saved-unscheduled", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	for _, id := range []string{"saved-a", "saved-b", "saved-c"} {
		if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", id, 0, Configuration{
			Cron: "*/5 * * * *", Timezone: "UTC", Enabled: false,
		}); err != nil {
			t.Fatalf("Configure(%s): %v", id, err)
		}
	}
	first, err := service.RunNow(context.Background(), "owner-1", "saved-a")
	if err != nil {
		t.Fatalf("RunNow first: %v", err)
	}
	if err := service.Finish(context.Background(), first.RunID, RunOutcomeSucceeded, ""); err != nil {
		t.Fatalf("Finish first: %v", err)
	}
	latest, err := service.RunNow(context.Background(), "owner-1", "saved-a")
	if err != nil {
		t.Fatalf("RunNow latest: %v", err)
	}
	if err := service.Finish(context.Background(), latest.RunID, RunOutcomeFailed, "execution_failed"); err != nil {
		t.Fatalf("Finish latest: %v", err)
	}
	active, err := service.RunNow(context.Background(), "owner-1", "saved-c")
	if err != nil {
		t.Fatalf("RunNow active: %v", err)
	}
	if _, err := service.RunNow(context.Background(), "owner-1", "saved-c"); !errors.Is(err, ErrConflict) {
		t.Fatalf("RunNow overlapping error = %v, want ErrConflict", err)
	}

	projections, err := service.CurrentProjections(context.Background(), "owner-1", []string{
		"saved-a", "saved-b", "saved-c", "saved-unscheduled", "saved-a",
	})
	if err != nil {
		t.Fatalf("CurrentProjections: %v", err)
	}
	if len(projections) != 3 || projections["saved-a"].LatestRun == nil ||
		projections["saved-a"].LatestRun.RunID != latest.RunID ||
		projections["saved-a"].LatestResultRun == nil ||
		projections["saved-a"].LatestResultRun.RunID != first.RunID ||
		projections["saved-b"].LatestRun != nil ||
		projections["saved-c"].LatestRun == nil ||
		projections["saved-c"].LatestRun.Outcome != RunOutcomeSkippedOverlap ||
		projections["saved-c"].LatestResultRun == nil ||
		projections["saved-c"].LatestResultRun.RunID != active.RunID {
		t.Fatalf("current projections = %#v", projections)
	}
	foreign, err := service.CurrentProjections(context.Background(), "owner-2", []string{"saved-a"})
	if err != nil || len(foreign) != 0 {
		t.Fatalf("foreign projections = %#v, %v", foreign, err)
	}
}

func TestConfigureUsesSeparateOperatorAndRuntimeVersions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-version", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	created, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-version", 0, Configuration{
		Cron: "0 * * * *", Timezone: "UTC", Enabled: true,
	})
	if err != nil {
		t.Fatalf("Configure create: %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 13, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Step: %v", err)
	}
	afterRun, err := repository.Get(context.Background(), "owner-1", "saved-version")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if afterRun.ConfigVersion != created.ConfigVersion || afterRun.RuntimeVersion == created.RuntimeVersion {
		t.Fatalf("versions after run = config %d runtime %d; created = %d/%d", afterRun.ConfigVersion, afterRun.RuntimeVersion, created.ConfigVersion, created.RuntimeVersion)
	}
	disabled, err := service.SetEnabled(context.Background(), "owner-1", "tenant-1", "saved-version", created.ConfigVersion, false)
	if err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if disabled.Enabled || disabled.NextRunAt != nil || disabled.ConfigVersion != created.ConfigVersion+1 {
		t.Fatalf("disabled schedule = %#v", disabled)
	}
	if _, err := service.SetEnabled(context.Background(), "owner-1", "tenant-1", "saved-version", created.ConfigVersion, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale SetEnabled error = %v, want ErrConflict", err)
	}
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-version", created.ConfigVersion, Configuration{
		Cron: "30 * * * *", Timezone: "UTC", Enabled: true,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Configure error = %v, want ErrConflict", err)
	}
}

func TestRecoverInterruptsSubmittedRuns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 12, 0, 1, 0, time.UTC)
	repository, database := newTestRepository(t, now)
	seedSavedSearch(t, database, "saved-restart", "owner-1", 1, "search index=main")
	service := newTestService(t, repository, new(recordingAdmitter), now)
	if _, err := service.Configure(context.Background(), "owner-1", "tenant-1", "saved-restart", 0, Configuration{
		Cron: "*/5 * * * *", Timezone: "UTC", Enabled: true,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := service.Step(context.Background(), time.Date(2026, time.August, 29, 12, 5, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Step: %v", err)
	}
	count, err := service.Recover(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("Recover = %d, %v; want 1", count, err)
	}
	runs, err := service.ListRuns(context.Background(), "owner-1", "saved-restart", 0)
	if err != nil || len(runs) != 1 || runs[0].Outcome != RunOutcomeInterrupted {
		t.Fatalf("runs after recovery = %#v, %v", runs, err)
	}
}

func newTestService(t *testing.T, repository *Repository, admitter SearchAdmitter, now time.Time) *Service {
	t.Helper()
	service, err := NewService(ServiceOptions{Store: repository, Admitter: admitter, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

func newTestRepository(t *testing.T, now time.Time) (*Repository, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	for _, statement := range scheduledReportTestSchema {
		if err := database.Exec(statement).Error; err != nil {
			t.Fatalf("create test schema: %v", err)
		}
	}
	counter := 0
	repository, err := NewRepository(database, RepositoryOptions{
		Clock:     func() time.Time { return now },
		CursorKey: []byte("scheduled-report-test-cursor-key-32"),
		IDGenerator: func() (string, error) {
			counter++
			return fmt.Sprintf("run-%04d", counter), nil
		},
	})
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	return repository, database
}

func seedSavedSearch(t *testing.T, database *gorm.DB, id, owner string, version int64, spl string) {
	t.Helper()
	definition := &opensplunk.SavedSearchDefinition{
		Name:    "test report",
		Search:  &opensplunk.SearchDefinition{Spl: spl},
		OwnerId: &owner,
	}
	encoded, err := proto.Marshal(definition)
	if err != nil {
		t.Fatalf("marshal saved search: %v", err)
	}
	if err := database.Exec(`INSERT INTO saved_searches (saved_search_id, version, owner_id, definition_proto) VALUES (?, ?, ?, ?)`, id, version, owner, encoded).Error; err != nil {
		t.Fatalf("seed saved search: %v", err)
	}
}

var scheduledReportTestSchema = []string{
	`CREATE TABLE saved_searches (
		saved_search_id TEXT PRIMARY KEY NOT NULL,
		version INTEGER NOT NULL,
		owner_id TEXT NOT NULL,
		definition_proto BLOB NOT NULL
	)`,
	`CREATE TABLE saved_search_schedules (
		saved_search_id TEXT PRIMARY KEY NOT NULL,
		owner_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL,
		config_version INTEGER NOT NULL,
		runtime_version INTEGER NOT NULL,
		cron_expression TEXT NOT NULL,
		timezone TEXT NOT NULL,
		dispatch_ttl TEXT NOT NULL,
		enabled INTEGER NOT NULL,
		next_run_at_unix_micro INTEGER,
		created_at_unix_micro INTEGER NOT NULL,
		updated_at_unix_micro INTEGER NOT NULL
	)`,
	`CREATE TABLE saved_search_schedule_runs (
		run_id TEXT PRIMARY KEY NOT NULL,
		saved_search_id TEXT NOT NULL,
		owner_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL,
		definition_version INTEGER NOT NULL,
		definition_proto BLOB NOT NULL,
		cron_expression TEXT NOT NULL,
		timezone TEXT NOT NULL,
		dispatch_ttl TEXT NOT NULL,
		schedule_period_microseconds INTEGER NOT NULL,
		retention_lifetime_microseconds INTEGER NOT NULL,
		scheduled_at_unix_micro INTEGER NOT NULL,
		claimed_at_unix_micro INTEGER NOT NULL,
		skipped_occurrence_count INTEGER NOT NULL,
		outcome TEXT NOT NULL,
		search_job_id TEXT,
		failure_category TEXT,
		finished_at_unix_micro INTEGER
	)`,
	`CREATE UNIQUE INDEX saved_search_schedule_runs_active_idx
		ON saved_search_schedule_runs(saved_search_id)
		WHERE outcome IN ('claimed', 'submitted')`,
}
