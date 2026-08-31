package server

import (
	"net/http"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

func TestSanitizeGetSearchJobSettingsRequestTrimsAndRequiresTheJobID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		want    string
		wantErr string
	}{
		{name: "trims", id: "  job-1 ", want: "job-1"},
		{name: "missing", id: "", wantErr: "search job ID is required"},
		{name: "blank", id: " \t\n ", wantErr: "search job ID is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := sanitizeGetSearchJobSettingsRequest(
				t.Context(),
				&opensplunk.GetSearchJobSettingsRequest{SearchJobId: test.id},
			)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, http.StatusBadRequest, test.wantErr)
				return
			}
			if err != nil || sanitized.GetSearchJobId() != test.want {
				t.Fatalf("sanitize = %q / %v, want %q", sanitized.GetSearchJobId(), err, test.want)
			}
		})
	}
}

func TestSanitizeUpdateSearchJobSettingsRequestBoundsIdentityAndSettings(t *testing.T) {
	tests := []struct {
		name    string
		request *opensplunk.UpdateSearchJobSettingsRequest
		wantID  string
		wantErr string
	}{
		{
			name: "trims a private manual request",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:          " job-1 ",
				ExpectedStateVersion: 3,
				Visibility:           opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE,
				RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL,
			},
			wantID: "job-1",
		},
		{
			name: "accepts an everyone shared request",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:          "job-1",
				ExpectedStateVersion: 1,
				Visibility:           opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE,
				RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED,
			},
			wantID: "job-1",
		},
		{
			name: "missing job ID",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				ExpectedStateVersion: 1,
				RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL,
			},
			wantErr: "search job ID and expected state version are required",
		},
		{
			name: "zero expected state version",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:    "job-1",
				RetentionClass: opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL,
			},
			wantErr: "search job ID and expected state version are required",
		},
		{
			name: "unspecified settings pair",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:          "job-1",
				ExpectedStateVersion: 1,
			},
			wantErr: "visibility and retention class combination is invalid",
		},
		{
			name: "mismatched settings pair",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:          "job-1",
				ExpectedStateVersion: 1,
				Visibility:           opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE,
				RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED,
			},
			wantErr: "visibility and retention class combination is invalid",
		},
		{
			name: "scheduler-owned retention class",
			request: &opensplunk.UpdateSearchJobSettingsRequest{
				SearchJobId:          "job-1",
				ExpectedStateVersion: 1,
				Visibility:           opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE,
				RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SCHEDULED_REPORT,
			},
			wantErr: "visibility and retention class combination is invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := sanitizeUpdateSearchJobSettingsRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, http.StatusBadRequest, test.wantErr)
				return
			}
			if err != nil || sanitized.GetSearchJobId() != test.wantID {
				t.Fatalf("sanitize = %q / %v, want %q", sanitized.GetSearchJobId(), err, test.wantID)
			}
		})
	}
}

func TestRetainedSettingsProjectsEverySanitizedVisibility(t *testing.T) {
	private := retainedSettings(opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE)
	if private != (searchartifacts.Settings{
		Visibility:     searchartifacts.VisibilityPrivate,
		RetentionClass: searchartifacts.RetentionManual,
		Lifetime:       searchretention.ManualLifetime,
	}) {
		t.Fatalf("private settings = %+v", private)
	}
	everyone := retainedSettings(opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE)
	if everyone != (searchartifacts.Settings{
		Visibility:     searchartifacts.VisibilityEveryone,
		RetentionClass: searchartifacts.RetentionShared,
		Lifetime:       searchretention.SharedLifetime,
	}) {
		t.Fatalf("everyone settings = %+v", everyone)
	}
}

func TestSanitizeShareSearchJobRequestRequiresIdentityAndVersion(t *testing.T) {
	tests := []struct {
		name    string
		request *opensplunk.ShareSearchJobRequest
		wantID  string
		wantErr string
	}{
		{
			name:    "trims",
			request: &opensplunk.ShareSearchJobRequest{SearchJobId: " job-1 ", ExpectedStateVersion: 2},
			wantID:  "job-1",
		},
		{
			name:    "missing job ID",
			request: &opensplunk.ShareSearchJobRequest{ExpectedStateVersion: 2},
			wantErr: "search job ID and expected state version are required",
		},
		{
			name:    "blank job ID",
			request: &opensplunk.ShareSearchJobRequest{SearchJobId: "   ", ExpectedStateVersion: 2},
			wantErr: "search job ID and expected state version are required",
		},
		{
			name:    "zero expected state version",
			request: &opensplunk.ShareSearchJobRequest{SearchJobId: "job-1"},
			wantErr: "search job ID and expected state version are required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := sanitizeShareSearchJobRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, http.StatusBadRequest, test.wantErr)
				return
			}
			if err != nil || sanitized.GetSearchJobId() != test.wantID {
				t.Fatalf("sanitize = %q / %v, want %q", sanitized.GetSearchJobId(), err, test.wantID)
			}
		})
	}
}

func TestSanitizeSearchArtifactRequestsDiscardUnknownFields(t *testing.T) {
	get := &opensplunk.GetSearchJobSettingsRequest{SearchJobId: "job-1"}
	get.ProtoReflect().SetUnknown(futureProtobufField("future-settings-get"))
	sanitizedGet, err := sanitizeGetSearchJobSettingsRequest(t.Context(), get)
	if err != nil || len(sanitizedGet.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("get sanitize = %v", err)
	}
	share := &opensplunk.ShareSearchJobRequest{SearchJobId: "job-1", ExpectedStateVersion: 1}
	share.ProtoReflect().SetUnknown(futureProtobufField("future-share"))
	sanitizedShare, err := sanitizeShareSearchJobRequest(t.Context(), share)
	if err != nil || len(sanitizedShare.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("share sanitize = %v", err)
	}
	update := &opensplunk.UpdateSearchJobSettingsRequest{
		SearchJobId:          "job-1",
		ExpectedStateVersion: 1,
		Visibility:           opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE,
		RetentionClass:       opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED,
	}
	update.ProtoReflect().SetUnknown(futureProtobufField("future-settings-update"))
	sanitizedUpdate, err := sanitizeUpdateSearchJobSettingsRequest(t.Context(), update)
	if err != nil || len(sanitizedUpdate.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("update sanitize = %v", err)
	}
}
