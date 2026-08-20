package searchhistory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestNormalizeEntryUsesOriginSpecificProvenanceIDBounds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		origin     opensplunkv1.SearchJobOrigin
		objectID   string
		wantErr    bool
		wantObject func(*opensplunkv1.SearchJobSource) string
	}{
		{
			name:     "history rerun exact limit",
			origin:   opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
			objectID: strings.Repeat("h", maximumSearchJobIDBytes),
			wantObject: func(source *opensplunkv1.SearchJobSource) string {
				return source.GetHistorySearchId()
			},
		},
		{
			name:     "history rerun over limit",
			origin:   opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
			objectID: strings.Repeat("h", maximumSearchJobIDBytes+1),
			wantErr:  true,
		},
		{
			name:     "saved search exact limit",
			origin:   opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			objectID: strings.Repeat("s", maximumSavedSearchIDBytes),
			wantObject: func(source *opensplunkv1.SearchJobSource) string {
				return source.GetSavedSearchId()
			},
		},
		{
			name:     "saved search over limit",
			origin:   opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
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
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
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
	if got.GetSource().GetOrigin() != opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN ||
		got.GetSource().GetHistorySearchId() != historyID {
		t.Fatalf("finalized history rerun source = %+v", got.GetSource())
	}
}

func provenanceSource(
	origin opensplunkv1.SearchJobOrigin,
	objectID string,
) *opensplunkv1.SearchJobSource {
	source := &opensplunkv1.SearchJobSource{Origin: origin}
	switch origin {
	case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN:
		source.HistorySearchId = &objectID
	case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH:
		source.SavedSearchId = &objectID
	}
	return source
}
