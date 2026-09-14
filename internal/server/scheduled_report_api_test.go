package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

func TestScheduledReportCallErrorPreservesRequestCancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ctx  context.Context
		err  error
	}{
		{name: "wrapped cancellation", ctx: context.Background(), err: fmt.Errorf("repository: %w", context.Canceled)},
		{name: "wrapped deadline", ctx: context.Background(), err: fmt.Errorf("repository: %w", context.DeadlineExceeded)},
		{name: "canceled request context", ctx: canceledContext(), err: errors.New("persistence stopped")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertHTTPErrorStatus(t, mapScheduledReportCallError(test.ctx, test.err), http.StatusRequestTimeout)
		})
	}
}

func TestScheduledReportCallErrorKeepsSuccessfulCommit(t *testing.T) {
	t.Parallel()
	if err := mapScheduledReportCallError(canceledContext(), nil); err != nil {
		t.Fatalf("successful operation error = %v", err)
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestSetSavedSearchScheduleRejectsContradictoryConfigVersions(t *testing.T) {
	t.Parallel()
	_, err := sanitizeSetSavedSearchScheduleRequest(
		context.Background(),
		&opensplunk.SetSavedSearchScheduleRequest{
			SavedSearchId:           "saved-1",
			ExpectedVersion:         4,
			ExpectedScheduleVersion: 6,
			Schedule: &opensplunk.SavedSearchSchedule{
				Cron: "0 * * * *", Timezone: "UTC", DispatchTtl: "2p", ConfigVersion: 5,
			},
		},
	)
	if err == nil {
		t.Fatal("sanitizeSetSavedSearchScheduleRequest() accepted contradictory config versions")
	}
}

func TestSearchLifecycleCapabilitiesAreServiceControlled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		feature      opensplunk.ServerFeature
		capabilities serviceCapabilities
	}{
		{name: "durable jobs", feature: opensplunk.ServerFeature_SERVER_FEATURE_DURABLE_SEARCH_JOBS, capabilities: serviceCapabilities{durableJobs: true}},
		{name: "scheduled searches", feature: opensplunk.ServerFeature_SERVER_FEATURE_SCHEDULED_SEARCHES, capabilities: serviceCapabilities{scheduledSearches: true}},
		{name: "alerts", feature: opensplunk.ServerFeature_SERVER_FEATURE_ALERTS, capabilities: serviceCapabilities{alerts: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if features := featuresForServices(nil, test.capabilities); !containsFeature(features, test.feature) {
				t.Fatalf("configured service did not advertise %v: %v", test.feature, features)
			}
			if features := featuresForServices([]opensplunk.ServerFeature{test.feature}, serviceCapabilities{}); containsFeature(features, test.feature) {
				t.Fatalf("missing service retained caller-supplied %v: %v", test.feature, features)
			}
		})
	}
}

func TestScheduledReportRunProjectsCurrentRetainedResultState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	run := scheduledreports.Run{
		RunID: "run-1", SavedSearchID: "saved-1", SearchJobID: "job-1",
		ScheduledAt: now.Add(-time.Minute), ClaimedAt: now.Add(-time.Minute),
		Outcome: scheduledreports.RunOutcomeSucceeded, RetentionLifetime: 10 * time.Minute,
	}
	retained := searchartifacts.Record{
		State: searchartifacts.StateCompleted, ArtifactPresent: true,
		ExpiresAt: now.Add(7 * 24 * time.Hour),
	}
	projected := scheduledReportRunToProto(run, retained, true, now)
	if projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE ||
		!projected.GetSearchJobExpiresAt().AsTime().Equal(retained.ExpiresAt) {
		t.Fatalf("scheduled run retained result = %+v", projected)
	}
}

func TestScheduledReportRunOmitsUnknownPendingExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	run := scheduledreports.Run{
		RunID: "run-1", SavedSearchID: "saved-1", SearchJobID: "job-1",
		ScheduledAt: now.Add(-time.Minute), ClaimedAt: now.Add(-time.Minute),
		Outcome: scheduledreports.RunOutcomeSubmitted, RetentionLifetime: 10 * time.Minute,
	}
	projected := scheduledReportRunToProto(run, searchartifacts.Record{State: searchartifacts.StateRunning}, true, now)
	if projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING ||
		projected.SearchJobExpiresAt != nil {
		t.Fatalf("pending scheduled run retained result = %+v", projected)
	}
}

func TestScheduledReportRunDoesNotExposeAvailableWithoutExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	run := scheduledreports.Run{
		RunID: "run-1", SavedSearchID: "saved-1", SearchJobID: "job-1",
		ScheduledAt: now.Add(-time.Minute), ClaimedAt: now.Add(-time.Minute),
		Outcome: scheduledreports.RunOutcomeSucceeded, RetentionLifetime: 10 * time.Minute,
	}
	projected := scheduledReportRunToProto(run, searchartifacts.Record{
		State: searchartifacts.StateCompleted, ArtifactPresent: true,
	}, true, now)
	if projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING ||
		projected.SearchJobExpiresAt != nil {
		t.Fatalf("scheduled run with invalid available expiry = %+v", projected)
	}
}

func TestScheduledReportRunPendingStatePrecedesEstimatedExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	run := scheduledreports.Run{
		RunID: "run-1", SavedSearchID: "saved-1", SearchJobID: "job-1",
		ScheduledAt: now.Add(-time.Hour), ClaimedAt: now.Add(-time.Hour),
		Outcome: scheduledreports.RunOutcomeSubmitted, RetentionLifetime: 10 * time.Minute,
	}
	projected := scheduledReportRunToProto(run, searchartifacts.Record{}, false, now)
	if projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING ||
		projected.SearchJobExpiresAt != nil {
		t.Fatalf("pending scheduled run retained result = %+v", projected)
	}
}

func TestScheduledReportProjectionSeparatesLastRunFromLatestResult(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	latest := &scheduledreports.Run{
		RunID: "run-new", ScheduledAt: now, ClaimedAt: now, Outcome: scheduledreports.RunOutcomeFailed,
	}
	latestResult := &scheduledreports.Run{
		RunID: "run-result", SearchJobID: "job-result", ScheduledAt: now.Add(-time.Hour),
		ClaimedAt: now.Add(-time.Hour), Outcome: scheduledreports.RunOutcomeSucceeded, RetentionLifetime: 2 * time.Hour,
	}
	record := &opensplunk.SavedSearch{Definition: &opensplunk.SavedSearchDefinition{}}
	applyScheduledReportProjection(record, scheduledreports.Schedule{}, latest, latestResult, now)
	if record.GetScheduleStatus().GetLastOutcome() != opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_FAILED ||
		record.GetScheduleStatus().GetLatestSearchJobId() != latestResult.SearchJobID ||
		record.GetScheduleStatus().GetLatestRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING {
		t.Fatalf("scheduled report projection = %+v", record.GetScheduleStatus())
	}
}
