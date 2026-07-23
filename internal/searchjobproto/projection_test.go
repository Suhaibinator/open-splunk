package searchjobproto

import (
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestTimeRangePreservesIntentAndLegacyFallback(t *testing.T) {
	t.Parallel()

	earliest := time.Date(2026, 7, 22, 10, 11, 12, 123456789, time.UTC)
	latest := earliest.Add(time.Hour)
	tests := []struct {
		name             string
		intent           searchtime.Intent
		wantEarliest     string
		wantLatest       string
		wantTimezone     string
		wantTimezoneWire bool
	}{
		{
			name: "explicit timezone",
			intent: searchtime.Intent{
				Earliest: "-1d", Latest: "now", Timezone: "America/Los_Angeles", TimezoneSpecified: true,
			},
			wantEarliest: "-1d", wantLatest: "now",
			wantTimezone: "America/Los_Angeles", wantTimezoneWire: true,
		},
		{
			name: "default timezone",
			intent: searchtime.Intent{
				Earliest: "-1h", Latest: "now", Timezone: "UTC",
			},
			wantEarliest: "-1h", wantLatest: "now", wantTimezone: "UTC",
		},
		{
			name:         "legacy zero value",
			wantEarliest: earliest.Format(time.RFC3339Nano),
			wantLatest:   latest.Format(time.RFC3339Nano),
			wantTimezone: "UTC", wantTimezoneWire: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, timezone, err := TimeRange(searchjobs.Job{
				TimeRange: test.intent, Earliest: earliest, Latest: latest,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.GetEarliest() != test.wantEarliest || got.GetLatest() != test.wantLatest ||
				timezone != test.wantTimezone || (got.Timezone != nil) != test.wantTimezoneWire {
				t.Fatalf("TimeRange() = (%+v, %q), want earliest %q latest %q timezone %q present %t",
					got, timezone, test.wantEarliest, test.wantLatest, test.wantTimezone, test.wantTimezoneWire)
			}
			if test.wantTimezoneWire && got.GetTimezone() != test.wantTimezone {
				t.Fatalf("wire timezone = %q, want %q", got.GetTimezone(), test.wantTimezone)
			}
		})
	}
}

func TestTimeRangeRejectsMalformedPresenceAndIntent(t *testing.T) {
	t.Parallel()

	for _, intent := range []searchtime.Intent{
		{TimezoneSpecified: true},
		{Earliest: "-1h", Latest: "now"},
		{Earliest: "-1h", Latest: "now", Timezone: " UTC", TimezoneSpecified: true},
	} {
		if _, _, err := TimeRange(searchjobs.Job{TimeRange: intent}); err == nil {
			t.Fatalf("TimeRange(%+v) error = nil", intent)
		}
	}
}

func TestSourceMapsEveryOriginAndRejectsInvalidShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source searchjobs.JobSource
		origin opensplunkv1.SearchJobOrigin
		id     string
	}{
		{name: "legacy zero value", origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC},
		{name: "ad hoc", source: searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc}, origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC},
		{name: "saved search", source: searchjobs.JobSource{Origin: searchjobs.JobOriginSavedSearch, ObjectID: "saved"}, origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH, id: "saved"},
		{name: "history", source: searchjobs.JobSource{Origin: searchjobs.JobOriginHistoryRerun, ObjectID: "history"}, origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN, id: "history"},
		{name: "dashboard", source: searchjobs.JobSource{Origin: searchjobs.JobOriginDashboard, ObjectID: "dashboard"}, origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD, id: "dashboard"},
		{name: "api", source: searchjobs.JobSource{Origin: searchjobs.JobOriginAPI}, origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_API},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := Source(test.source)
			if err != nil {
				t.Fatal(err)
			}
			if got.GetOrigin() != test.origin {
				t.Fatalf("origin = %v, want %v", got.GetOrigin(), test.origin)
			}
			if (got.SavedSearchId != nil) != (test.origin == opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH) ||
				(got.HistorySearchId != nil) != (test.origin == opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN) ||
				(got.DashboardId != nil) != (test.origin == opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD) {
				t.Fatalf("source pointer shape = %+v", got)
			}
			switch test.origin {
			case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH:
				if got.GetSavedSearchId() != test.id {
					t.Fatalf("saved-search ID = %q, want %q", got.GetSavedSearchId(), test.id)
				}
			case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN:
				if got.GetHistorySearchId() != test.id {
					t.Fatalf("history ID = %q, want %q", got.GetHistorySearchId(), test.id)
				}
			case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD:
				if got.GetDashboardId() != test.id {
					t.Fatalf("dashboard ID = %q, want %q", got.GetDashboardId(), test.id)
				}
			}
		})
	}

	for _, source := range []searchjobs.JobSource{
		{Origin: searchjobs.JobOriginInvalid, ObjectID: "saved"},
		{Origin: searchjobs.JobOriginAdHoc, ObjectID: "saved"},
		{Origin: searchjobs.JobOriginSavedSearch},
		{Origin: searchjobs.JobOriginSavedSearch, ObjectID: " saved "},
	} {
		if _, err := Source(source); err == nil {
			t.Fatalf("Source(%+v) error = nil", source)
		}
	}
}
