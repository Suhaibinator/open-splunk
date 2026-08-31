package server

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
)

func TestSanitizeSetSavedSearchScheduleRequestRequiresACompleteIdentity(t *testing.T) {
	t.Parallel()
	complete := func() *opensplunk.SetSavedSearchScheduleRequest {
		return &opensplunk.SetSavedSearchScheduleRequest{
			SavedSearchId: " saved-1 ", ExpectedVersion: 4, ExpectedScheduleVersion: 6,
			Schedule: &opensplunk.SavedSearchSchedule{Cron: "0 * * * *", Timezone: "UTC", DispatchTtl: "2p"},
		}
	}

	sanitized, err := sanitizeSetSavedSearchScheduleRequest(t.Context(), complete())
	if err != nil || sanitized.GetSavedSearchId() != "saved-1" {
		t.Fatalf("sanitizeSetSavedSearchScheduleRequest() = %+v, %v", sanitized, err)
	}

	matching := complete()
	matching.Schedule.ConfigVersion = matching.GetExpectedScheduleVersion()
	if _, err := sanitizeSetSavedSearchScheduleRequest(t.Context(), matching); err != nil {
		t.Fatalf("matching config version was rejected: %v", err)
	}

	for _, test := range []struct {
		name    string
		mutate  func(*opensplunk.SetSavedSearchScheduleRequest)
		wantErr string
	}{
		{name: "blank saved search ID", mutate: func(request *opensplunk.SetSavedSearchScheduleRequest) {
			request.SavedSearchId = "   "
		}, wantErr: "saved search ID, expected version, and schedule are required"},
		{name: "control character in saved search ID", mutate: func(request *opensplunk.SetSavedSearchScheduleRequest) {
			request.SavedSearchId = "saved\x001"
		}, wantErr: "saved search ID, expected version, and schedule are required"},
		{name: "absent expected version", mutate: func(request *opensplunk.SetSavedSearchScheduleRequest) {
			request.ExpectedVersion = 0
		}, wantErr: "saved search ID, expected version, and schedule are required"},
		{name: "absent schedule", mutate: func(request *opensplunk.SetSavedSearchScheduleRequest) {
			request.Schedule = nil
		}, wantErr: "saved search ID, expected version, and schedule are required"},
		{name: "contradictory config version", mutate: func(request *opensplunk.SetSavedSearchScheduleRequest) {
			request.Schedule.ConfigVersion = request.GetExpectedScheduleVersion() + 1
		}, wantErr: "schedule config version must match expected schedule version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := complete()
			test.mutate(request)
			if _, err := sanitizeSetSavedSearchScheduleRequest(t.Context(), request); err == nil ||
				!strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("sanitizeSetSavedSearchScheduleRequest() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestSanitizeRunSavedSearchRequestTrimsAndRequiresTheSavedSearchID(t *testing.T) {
	t.Parallel()
	sanitized, err := sanitizeRunSavedSearchRequest(t.Context(), &opensplunk.RunSavedSearchRequest{SavedSearchId: " saved-1 "})
	if err != nil || sanitized.GetSavedSearchId() != "saved-1" {
		t.Fatalf("sanitizeRunSavedSearchRequest() = %+v, %v", sanitized, err)
	}
	if _, err := sanitizeRunSavedSearchRequest(t.Context(), &opensplunk.RunSavedSearchRequest{}); err == nil ||
		!strings.Contains(err.Error(), "saved search ID is required") {
		t.Fatalf("absent saved search ID error = %v", err)
	}
	if _, err := sanitizeRunSavedSearchRequest(t.Context(), &opensplunk.RunSavedSearchRequest{
		SavedSearchId: strings.Repeat("s", maximumSavedSearchIDBytes+1),
	}); err == nil || !strings.Contains(err.Error(), "saved search ID is invalid") {
		t.Fatalf("oversized saved search ID error = %v", err)
	}
}

func TestSanitizeListScheduledSearchRunsRequestResolvesAStablePage(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{maximumPageSize: 1_000}

	sanitized, err := handler.sanitizeListScheduledSearchRunsRequest(t.Context(), &opensplunk.ListScheduledSearchRunsRequest{
		SavedSearchId: " saved-1 ",
	})
	if err != nil {
		t.Fatalf("sanitizeListScheduledSearchRunsRequest() error = %v", err)
	}
	if sanitized.GetSavedSearchId() != "saved-1" || sanitized.GetPage().GetPageSize() != scheduledReportRunPageSize {
		t.Fatalf("sanitized run list = %+v", sanitized)
	}

	again, err := handler.sanitizeListScheduledSearchRunsRequest(t.Context(), sanitized)
	if err != nil || again.GetPage().GetPageSize() != scheduledReportRunPageSize {
		t.Fatalf("re-sanitized run list = %+v, %v", again, err)
	}

	oversize := uint32(scheduledreports.RunHistoryLimit + 1)
	if _, err := handler.sanitizeListScheduledSearchRunsRequest(t.Context(), &opensplunk.ListScheduledSearchRunsRequest{
		SavedSearchId: "saved-1", Page: &opensplunk.PageRequest{PageSize: &oversize},
	}); err == nil || !strings.Contains(err.Error(), "scheduled run page size is invalid") {
		t.Fatalf("oversized page size error = %v", err)
	}

	if _, err := handler.sanitizeListScheduledSearchRunsRequest(t.Context(), &opensplunk.ListScheduledSearchRunsRequest{}); err == nil ||
		!strings.Contains(err.Error(), "saved search ID is required") {
		t.Fatalf("absent saved search ID error = %v", err)
	}
}

func TestSanitizeValidateScheduleRequestRejectsUnusableModes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		mode    opensplunk.ScheduleValidationMode
		wantErr string
	}{
		{name: "unspecified", mode: opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_UNSPECIFIED,
			wantErr: "schedule-validation mode is required"},
		{name: "unsupported", mode: opensplunk.ScheduleValidationMode(127),
			wantErr: "schedule-validation mode is unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := sanitizeValidateScheduleRequest(t.Context(), &opensplunk.ValidateScheduleRequest{Mode: test.mode})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("sanitizeValidateScheduleRequest() error = %v, want %q", err, test.wantErr)
			}
		})
	}

	for _, mode := range []opensplunk.ScheduleValidationMode{
		opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
		opensplunk.ScheduleValidationMode_SCHEDULE_VALIDATION_MODE_WEBHOOK_ALERT,
	} {
		request := &opensplunk.ValidateScheduleRequest{Mode: mode, Cron: "0 * * * *", Timezone: "UTC", DispatchTtl: "2p"}
		sanitized, err := sanitizeValidateScheduleRequest(t.Context(), request)
		if err != nil || sanitized.GetCron() != "0 * * * *" || sanitized.GetTimezone() != "UTC" {
			t.Fatalf("sanitizeValidateScheduleRequest(%v) = %+v, %v", mode, sanitized, err)
		}
	}
}
