package server

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type provenanceIntegrationExecutor struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (executor *provenanceIntegrationExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message", Kind: searchjobs.ValueKindString,
	}}}); err != nil {
		return err
	}
	executor.once.Do(func() { close(executor.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-executor.release:
		return nil
	}
}

type provenanceIntegrationSnapshotter uint64

func (snapshot provenanceIntegrationSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshot), nil
}

func TestSavedSearchProvenanceSurvivesExecutionAndSourceDeletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		ownerID   = "owner-1"
		tenantID  = "tenant-1"
		appID     = "search-app"
		savedID   = "saved-integration"
		jobID     = "job-provenance-integration"
		searchSPL = "index=main | table message"
	)
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, time.March, 8, 12, 0, 0, 123456789, location).UTC()
	wantEarliest := anchor.In(location).AddDate(0, 0, -1).UTC()
	cursorKey := []byte("0123456789abcdef0123456789abcdef")

	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{Name: "main", SearchEnabled: true}); err != nil {
		t.Fatal(err)
	}

	savedSearches, err := savedobjects.New(database, savedobjects.Options{
		Clock:       func() time.Time { return anchor },
		IDGenerator: func() (string, error) { return savedID, nil },
		CursorKey:   cursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := savedSearches.Create(
		ctx,
		savedobjects.AccessScope{OwnerID: ownerID},
		savedSearchDefinition(ownerID, appID, "Errors"),
	)
	if err != nil {
		t.Fatal(err)
	}

	history, err := searchhistory.New(database, searchhistory.Options{
		Clock: func() time.Time { return anchor.Add(time.Minute) }, CursorKey: cursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := searchhistory.NewJobJournal(history, "integration-test")
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	executor := &provenanceIntegrationExecutor{entered: make(chan struct{}), release: release}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     provenanceIntegrationSnapshotter(17),
		Journal:         journal,
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now:             func() time.Time { return anchor },
		NewID:           func() string { return jobID },
		CursorKey:       cursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close search manager: %v", err)
		}
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	handler := newTestHandler(t, Config{
		SearchJobs:    manager,
		Indexes:       database,
		SavedSearches: savedSearches,
		SearchHistory: history,
		WebUI:         testUI(),
		OwnerID:       ownerID,
		TenantID:      tenantID,
		Now:           func() time.Time { return anchor },
	})
	timezone := location.String()
	request := &opensplunkv1.CreateSearchJobRequest{
		Definition: &opensplunkv1.SearchDefinition{
			Spl: searchSPL,
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: stringPointer("-1d"), Latest: stringPointer("now"), Timezone: &timezone,
			},
			IndexScope: []string{"main"},
		},
		Source: &opensplunkv1.SearchJobSource{
			Origin:        opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
			SavedSearchId: stringPointer(saved.GetSavedSearchId()),
		},
	}
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var createdResponse opensplunkv1.CreateSearchJobResponse
	unmarshalResponse(t, response, &createdResponse)
	assertProvenanceSnapshot(t, createdResponse.GetSearchJob(), jobID, appID, savedID, searchSPL, wantEarliest, anchor)

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}
	if err := savedSearches.Delete(ctx, savedobjects.AccessScope{OwnerID: ownerID}, savedID, saved.GetVersion()); err != nil {
		t.Fatal(err)
	}
	if _, err := savedSearches.Get(ctx, savedobjects.AccessScope{OwnerID: ownerID}, savedID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("deleted saved-search lookup error = %v, want ErrNotFound", err)
	}
	response = postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{SearchJobId: jobID})
	if response.Code != http.StatusOK {
		t.Fatalf("post-deletion job status = %d, body = %s", response.Code, response.Body.String())
	}
	var liveResponse opensplunkv1.GetSearchJobResponse
	unmarshalResponse(t, response, &liveResponse)
	assertProvenanceSnapshot(t, liveResponse.GetSearchJob(), jobID, appID, savedID, searchSPL, wantEarliest, anchor)
	releaseOnce.Do(func() { close(release) })

	historyEntry := waitForProvenanceHistory(t, history, searchhistory.AccessScope{TenantID: tenantID, OwnerID: ownerID}, jobID)
	assertHistoryProvenanceSnapshot(t, historyEntry, jobID, appID, savedID, searchSPL, wantEarliest, anchor)

	response = postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: jobID})
	if response.Code != http.StatusOK {
		t.Fatalf("history status = %d, body = %s", response.Code, response.Body.String())
	}
	var historyResponse opensplunkv1.GetSearchHistoryEntryResponse
	unmarshalResponse(t, response, &historyResponse)
	assertHistoryProvenanceSnapshot(t, historyResponse.GetHistoryEntry(), jobID, appID, savedID, searchSPL, wantEarliest, anchor)

	listed, err := history.List(ctx, searchhistory.AccessScope{TenantID: tenantID, OwnerID: ownerID}, searchhistory.ListRequest{
		PageSize: 1, AppIDFilter: stringPointer(appID), SavedSearchIDFilter: stringPointer(savedID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Entries) != 1 || listed.Entries[0].GetSearchJobId() != jobID {
		t.Fatalf("filtered history = %+v", listed.Entries)
	}
}

func assertProvenanceSnapshot(
	t *testing.T,
	job *opensplunkv1.SearchJob,
	jobID, appID, savedID, wantSPL string,
	wantEarliest, wantLatest time.Time,
) {
	t.Helper()
	if job == nil || job.GetSearchJobId() != jobID || job.GetDefinition().GetSpl() != wantSPL ||
		job.GetDefinition().GetAppId() != appID ||
		job.GetDefinition().GetTimeRange().GetEarliest() != "-1d" ||
		job.GetDefinition().GetTimeRange().GetLatest() != "now" ||
		job.GetDefinition().GetTimeRange().Timezone == nil ||
		job.GetDefinition().GetTimeRange().GetTimezone() != "America/Los_Angeles" ||
		job.GetSource().GetOrigin() != opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		job.GetSource().GetSavedSearchId() != savedID ||
		!job.GetResolvedTimeRange().GetEarliest().AsTime().Equal(wantEarliest) ||
		!job.GetResolvedTimeRange().GetLatest().AsTime().Equal(wantLatest) ||
		job.GetResolvedTimeRange().GetTimezone() != "America/Los_Angeles" {
		t.Fatalf("job provenance snapshot = %+v", job)
	}
}

func assertHistoryProvenanceSnapshot(
	t *testing.T,
	entry *opensplunkv1.SearchHistoryEntry,
	jobID, appID, savedID, wantSPL string,
	wantEarliest, wantLatest time.Time,
) {
	t.Helper()
	if entry == nil || entry.GetSearchJobId() != jobID ||
		entry.GetFinalState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
		entry.GetDefinition().GetSpl() != wantSPL ||
		entry.GetDefinition().GetAppId() != appID ||
		entry.GetDefinition().GetTimeRange().GetEarliest() != "-1d" ||
		entry.GetDefinition().GetTimeRange().GetLatest() != "now" ||
		entry.GetDefinition().GetTimeRange().Timezone == nil ||
		entry.GetDefinition().GetTimeRange().GetTimezone() != "America/Los_Angeles" ||
		entry.GetSource().GetOrigin() != opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		entry.GetSource().GetSavedSearchId() != savedID ||
		!entry.GetResolvedTimeRange().GetEarliest().AsTime().Equal(wantEarliest) ||
		!entry.GetResolvedTimeRange().GetLatest().AsTime().Equal(wantLatest) ||
		entry.GetResolvedTimeRange().GetTimezone() != "America/Los_Angeles" ||
		!slices.Equal(entry.GetEffectiveIndexScope(), []string{"main"}) {
		t.Fatalf("history provenance snapshot = %+v", entry)
	}
}

func waitForProvenanceHistory(
	t *testing.T,
	store *searchhistory.Store,
	scope searchhistory.AccessScope,
	jobID string,
) *opensplunkv1.SearchHistoryEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		entry, err := store.Get(context.Background(), scope, jobID)
		if err == nil {
			return entry
		}
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("get completed history: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("completed history was not persisted")
		}
		time.Sleep(time.Millisecond)
	}
}
