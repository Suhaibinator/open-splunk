package searchhistory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestNormalizeEntryUsesOriginSpecificProvenanceIDBounds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		origin     opensplunk.SearchJobOrigin
		objectID   string
		wantErr    bool
		wantObject func(*opensplunk.SearchJobSource) string
	}{
		{
			name:     "history rerun exact limit",
			origin:   opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
			objectID: strings.Repeat("h", maximumSearchJobIDBytes),
			wantObject: func(source *opensplunk.SearchJobSource) string {
				return source.GetHistorySearchId()
			},
		},
		{
			name:     "history rerun over limit",
			origin:   opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
			objectID: strings.Repeat("h", maximumSearchJobIDBytes+1),
			wantErr:  true,
		},
		{
			name:     "saved search exact limit",
			origin:   opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			objectID: strings.Repeat("s", maximumSavedSearchIDBytes),
			wantObject: func(source *opensplunk.SearchJobSource) string {
				return source.GetSavedSearchId()
			},
		},
		{
			name:     "saved search over limit",
			origin:   opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			objectID: strings.Repeat("s", maximumSavedSearchIDBytes+1),
			wantErr:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entry := historyEntry(
				"job-source-boundary",
				"index=main | head 1",
				"search-app",
				opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC),
			)
			entry.Source = provenanceSource(test.origin, test.objectID)

			normalized, _, err := normalizeEntry(entry)
			if test.wantErr {
				if !errors.Is(err, control.ErrInvalidArgument) {
					t.Fatalf("normalizeEntry() error = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeEntry() error = %v", err)
			}
			if normalized.GetSource().GetOrigin() != test.origin ||
				test.wantObject(normalized.GetSource()) != test.objectID {
				t.Fatalf("normalized source = %+v", normalized.GetSource())
			}
		})
	}
}

func TestJobJournalRoundTripsMaximumHistoryRerunProvenanceID(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t, Options{})
	journal, err := NewJobJournal(store)
	if err != nil {
		t.Fatal(err)
	}
	historyID := strings.Repeat("h", maximumSearchJobIDBytes)
	now := time.Date(2026, time.July, 24, 12, 0, 0, 0, time.UTC)
	pending := journalJob("journal-history-rerun", searchjobs.StateQueued, now)
	pending.Source = searchjobs.JobSource{
		Origin:   searchjobs.JobOriginHistoryRerun,
		ObjectID: historyID,
	}
	if err := journal.Admit(context.Background(), pending); err != nil {
		t.Fatalf("Admit(maximum history provenance ID) error = %v", err)
	}

	terminal := journalJob("journal-history-rerun", searchjobs.StateCompleted, now)
	terminal.Source = pending.Source
	terminal.EffectiveIndexes = []string{"main"}
	terminal.StartedAt = now.Add(-30 * time.Second)
	terminal.FinishedAt = now.Add(-10 * time.Second)
	if err := journal.Finalize(context.Background(), terminal); err != nil {
		t.Fatalf("Finalize(maximum history provenance ID) error = %v", err)
	}

	got, err := store.Get(
		context.Background(),
		AccessScope{TenantID: "tenant", OwnerID: "owner"},
		terminal.ID,
	)
	if err != nil {
		t.Fatalf("Get(finalized history rerun) error = %v", err)
	}
	if got.GetSource().GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN ||
		got.GetSource().GetHistorySearchId() != historyID {
		t.Fatalf("finalized history rerun source = %+v", got.GetSource())
	}
}

func TestJobJournalRoundTripsScheduledProvenance(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source searchjobs.JobSource
		assert func(*testing.T, *opensplunk.SearchJobSource, time.Time)
	}{
		{
			name: "scheduled report",
			source: searchjobs.JobSource{
				Origin:   searchjobs.JobOriginScheduledReport,
				ObjectID: "scheduled-run-1",
			},
			assert: func(t *testing.T, source *opensplunk.SearchJobSource, scheduledAt time.Time) {
				t.Helper()
				if source.GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SCHEDULED_REPORT ||
					source.GetScheduledReportRunId() != "scheduled-run-1" ||
					!source.GetScheduledAt().AsTime().Equal(scheduledAt) {
					t.Fatalf("scheduled-report source = %+v", source)
				}
			},
		},
		{
			name: "alert",
			source: searchjobs.JobSource{
				Origin:  searchjobs.JobOriginAlert,
				AlertID: "alert-1", AlertRunID: "alert-run-1",
			},
			assert: func(t *testing.T, source *opensplunk.SearchJobSource, scheduledAt time.Time) {
				t.Helper()
				if source.GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_ALERT ||
					source.GetAlertId() != "alert-1" || source.GetAlertRunId() != "alert-run-1" ||
					!source.GetScheduledAt().AsTime().Equal(scheduledAt) {
					t.Fatalf("alert source = %+v", source)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, store := openTestStore(t, Options{})
			journal, err := NewJobJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			scheduledAt := time.Date(2026, time.August, 30, 12, 34, 56, 789_123_456, time.UTC)
			test.source.ScheduledAt = scheduledAt
			job := journalJob("job-"+strings.ReplaceAll(test.name, " ", "-"), searchjobs.StateQueued, scheduledAt)
			job.Source = test.source
			if err := journal.Admit(context.Background(), job); err != nil {
				t.Fatalf("Admit() error = %v", err)
			}
			job.State = searchjobs.StateCompleted
			job.EffectiveIndexes = []string{"main"}
			job.StartedAt = scheduledAt.Add(time.Second)
			job.FinishedAt = scheduledAt.Add(2 * time.Second)
			if err := journal.Finalize(context.Background(), job); err != nil {
				t.Fatalf("Finalize() error = %v", err)
			}
			got, err := store.Get(
				context.Background(),
				AccessScope{TenantID: "tenant", OwnerID: "owner"},
				job.ID,
			)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			test.assert(t, got.GetSource(), time.UnixMicro(scheduledAt.UnixMicro()).UTC())
		})
	}
}

func provenanceSource(
	origin opensplunk.SearchJobOrigin,
	objectID string,
) *opensplunk.SearchJobSource {
	source := &opensplunk.SearchJobSource{Origin: origin}
	switch origin {
	case opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN:
		source.HistorySearchId = &objectID
	case opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH:
		source.SavedSearchId = &objectID
	}
	return source
}
