package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	runtimeSavedSearchAuditTenantID = "tenant-saved-search-audit"
	runtimeSavedSearchAuditOwnerID  = "single-user"
	runtimeSavedSearchSystemActorID = "open-splunk-server"
)

func TestRuntimeSavedSearchAuditSurvivesStoreReopen(t *testing.T) {
	ctx := context.Background()
	rerunServerTime := time.Date(
		2026,
		time.August,
		5,
		12,
		0,
		0,
		123456000,
		time.UTC,
	)
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")
	bearerToken := bytes.Repeat(
		[]byte("s"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	authenticator := newRuntimeSavedSearchAuditAuthenticator(
		t,
		bearerToken,
	)

	firstDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstDatabase.CreateIndex(
		ctx,
		control.IndexDefinition{Name: "main", SearchEnabled: true},
	); err != nil {
		t.Fatalf("create rerun index: %v", err)
	}
	firstDatabaseClosed := false
	t.Cleanup(func() {
		if !firstDatabaseClosed {
			if err := firstDatabase.Close(); err != nil {
				t.Errorf("close first control database: %v", err)
			}
		}
	})
	firstStores, err := openRuntimeSecurityStores(
		ctx,
		firstDatabase,
		masterKeyPath,
		runtimeSavedSearchAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	firstHandler := newRuntimeSavedSearchAuditHandlerForTest(
		t,
		firstStores.savedSearches,
		firstStores.auditEvents,
		authenticator,
		nil,
	)

	payloadCanary := "saved-search-request-payload-must-not-appear-4821"
	earliest := "-24h"
	latest := "now"
	timezone := "UTC"
	definition := &opensplunk.SavedSearchDefinition{
		Name:        "Runtime audit search",
		Description: &payloadCanary,
		Search: &opensplunk.SearchDefinition{
			Spl:        "index=main | table _raw",
			IndexScope: []string{"main"},
			TimeRange: &opensplunk.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
		},
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	}
	var createdResponse opensplunk.CreateSavedSearchResponse
	postRuntimeProtoOK(
		t,
		firstHandler,
		"/api/saved-searches/create",
		&opensplunk.CreateSavedSearchRequest{Definition: definition},
		&createdResponse,
		bearerToken,
	)
	created := createdResponse.GetSavedSearch()
	if created.GetSavedSearchId() == "" ||
		created.GetVersion() != 1 ||
		created.GetDefinition().GetOwnerId() != runtimeSavedSearchAuditOwnerID {
		t.Fatalf("created saved search = %+v", created)
	}

	if err := firstHandler.Close(ctx); err != nil {
		t.Fatalf("close first handler: %v", err)
	}
	if err := firstDatabase.Close(); err != nil {
		t.Fatalf("close first control database: %v", err)
	}
	firstDatabaseClosed = true

	secondDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDatabase.Close(); err != nil {
			t.Errorf("close second control database: %v", err)
		}
	})
	secondStores, err := openRuntimeSecurityStores(
		ctx,
		secondDatabase,
		masterKeyPath,
		runtimeSavedSearchAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	history, err := openSearchHistoryStore(
		ctx,
		secondDatabase,
		masterKeyPath,
		searchhistory.Options{Clock: func() time.Time { return rerunServerTime }},
	)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor:        runtimeSavedSearchRerunExecutor{},
		Snapshotter:     runtimeSavedSearchRerunSnapshotter(17),
		Journal:         journal,
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now:             func() time.Time { return rerunServerTime },
		NewID: func() string {
			return "job-saved-search-rerun-after-reopen"
		},
		CursorKey: bytes.Repeat([]byte("r"), 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := jobs.Close(); err != nil {
			t.Errorf("close rerun search manager: %v", err)
		}
	})
	secondHandler := newRuntimeSavedSearchAuditHandlerForTest(
		t,
		secondStores.savedSearches,
		secondStores.auditEvents,
		authenticator,
		func(config *server.Config) {
			config.SearchJobs = jobs
			config.SearchHistory = history
			config.Indexes = secondDatabase
			config.Now = func() time.Time { return rerunServerTime }
		},
	)
	t.Cleanup(func() {
		if err := secondHandler.Close(context.Background()); err != nil {
			t.Errorf("close second handler: %v", err)
		}
	})

	var fetchedResponse opensplunk.GetSavedSearchResponse
	postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/saved-searches/get",
		&opensplunk.GetSavedSearchRequest{
			SavedSearchId: created.GetSavedSearchId(),
		},
		&fetchedResponse,
		bearerToken,
	)
	fetched := fetchedResponse.GetSavedSearch()
	if fetched.GetSavedSearchId() != created.GetSavedSearchId() ||
		fetched.GetVersion() != 1 ||
		fetched.GetDefinition().GetName() != definition.GetName() ||
		!proto.Equal(
			fetched.GetDefinition().GetSearch(),
			created.GetDefinition().GetSearch(),
		) {
		t.Fatalf("fetched saved search after reopen = %+v", fetched)
	}

	fetchedID := fetched.GetSavedSearchId()
	rerunDefinition := proto.Clone(
		fetched.GetDefinition().GetSearch(),
	).(*opensplunk.SearchDefinition)
	// Saved searches retain presentation preferences, while job creation
	// accepts execution intent only. This is the same projection a frontend
	// performs after reopening an object.
	rerunDefinition.PreferredResultTab = opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED
	rerunDefinition.SelectedFields = nil
	rerunDefinition.Visualization = nil
	var rerunResponse opensplunk.CreateSearchJobResponse
	postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/search/jobs/create",
		&opensplunk.CreateSearchJobRequest{
			Definition: rerunDefinition,
			Source: &opensplunk.SearchJobSource{
				Origin:        opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
				SavedSearchId: &fetchedID,
			},
		},
		&rerunResponse,
		bearerToken,
	)
	rerunJob := rerunResponse.GetSearchJob()
	assertRuntimeSavedSearchRerunProvenance(
		t,
		rerunJob,
		fetched,
		rerunServerTime,
	)
	historyEntry := waitForRuntimeSavedSearchRerunHistory(
		t,
		history,
		rerunJob.GetSearchJobId(),
	)
	assertRuntimeSavedSearchRerunHistory(
		t,
		historyEntry,
		fetched,
		rerunServerTime,
	)

	var updatedResponse opensplunk.UpdateSavedSearchResponse
	postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/saved-searches/update",
		&opensplunk.UpdateSavedSearchRequest{
			SavedSearchId:   fetched.GetSavedSearchId(),
			ExpectedVersion: fetched.GetVersion(),
			Definition: &opensplunk.SavedSearchDefinition{
				Name: "Runtime audit search updated",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		},
		&updatedResponse,
		bearerToken,
	)
	updated := updatedResponse.GetSavedSearch()
	if updated.GetSavedSearchId() != created.GetSavedSearchId() ||
		updated.GetVersion() != 2 ||
		updated.GetDefinition().GetName() != "Runtime audit search updated" {
		t.Fatalf("updated saved search = %+v", updated)
	}

	var duplicatedResponse opensplunk.DuplicateSavedSearchResponse
	postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/saved-searches/duplicate",
		&opensplunk.DuplicateSavedSearchRequest{
			SavedSearchId: updated.GetSavedSearchId(),
			NewName:       "Runtime audit search copy",
		},
		&duplicatedResponse,
		bearerToken,
	)
	duplicated := duplicatedResponse.GetSavedSearch()
	if duplicated.GetSavedSearchId() == "" ||
		duplicated.GetSavedSearchId() == updated.GetSavedSearchId() ||
		duplicated.GetVersion() != 1 ||
		duplicated.GetDefinition().GetName() != "Runtime audit search copy" {
		t.Fatalf("duplicated saved search = %+v", duplicated)
	}

	var deletedResponse opensplunk.DeleteSavedSearchResponse
	postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/saved-searches/delete",
		&opensplunk.DeleteSavedSearchRequest{
			SavedSearchId:   updated.GetSavedSearchId(),
			ExpectedVersion: updated.GetVersion(),
		},
		&deletedResponse,
		bearerToken,
	)
	if deletedResponse.GetSavedSearchId() != updated.GetSavedSearchId() {
		t.Fatalf("deleted saved-search ID = %q", deletedResponse.GetSavedSearchId())
	}

	pageSize := uint32(20)
	listRequest := &opensplunk.ListAuditEventsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		},
	}
	unauthenticated := postRuntimeAppProto(
		t,
		secondHandler,
		"/api/audit/events/list",
		listRequest,
		nil,
	)
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf(
			"unauthenticated audit list status = %d, body = %s",
			unauthenticated.Code,
			unauthenticated.Body.String(),
		)
	}
	auditResponse := postRuntimeAppProto(
		t,
		secondHandler,
		"/api/audit/events/list",
		listRequest,
		bearerToken,
	)
	if auditResponse.Code != http.StatusOK {
		t.Fatalf(
			"authenticated audit list status = %d, body = %s",
			auditResponse.Code,
			auditResponse.Body.String(),
		)
	}
	if bytes.Contains(auditResponse.Body.Bytes(), []byte(payloadCanary)) {
		t.Fatalf("audit response disclosed saved-search definition payload")
	}
	var listed opensplunk.ListAuditEventsResponse
	unmarshalRuntimeAppResponse(t, auditResponse, &listed)
	expectations := []runtimeSavedSearchAuditExpectation{
		{
			action:   opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE,
			targetID: updated.GetSavedSearchId(),
			version:  updated.GetVersion(),
		},
		{
			action:   opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE,
			targetID: duplicated.GetSavedSearchId(),
			version:  duplicated.GetVersion(),
		},
		{
			action:   opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE,
			targetID: updated.GetSavedSearchId(),
			version:  updated.GetVersion(),
		},
		{
			action:   opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE,
			targetID: created.GetSavedSearchId(),
			version:  created.GetVersion(),
		},
	}
	assertRuntimeSavedSearchAuditEvents(t, &listed, expectations)
}

func TestRuntimeSavedSearchMutationFailsClosedWithoutAuditJournal(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	database, err := control.Open(
		ctx,
		filepath.Join(directory, "control.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	stores, err := openRuntimeSecurityStores(
		ctx,
		database,
		filepath.Join(directory, "server.key"),
		runtimeSavedSearchAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	bearerToken := bytes.Repeat(
		[]byte("f"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	handler := newRuntimeSavedSearchAuditHandlerForTest(
		t,
		stores.savedSearches,
		stores.auditEvents,
		newRuntimeSavedSearchAuditAuthenticator(t, bearerToken),
		nil,
	)
	if _, err := database.SQLDB().ExecContext(
		ctx,
		"DROP TABLE audit_events",
	); err != nil {
		t.Fatalf("make test audit journal unavailable: %v", err)
	}

	payloadCanary := "failed-audit-definition-must-not-leak-9437"
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/saved-searches/create",
		&opensplunk.CreateSavedSearchRequest{
			Definition: &opensplunk.SavedSearchDefinition{
				Name: "Must roll back",
				Search: &opensplunk.SearchDefinition{
					Spl: payloadCanary,
				},
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			},
		},
		bearerToken,
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf(
			"saved-search create without audit journal status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	} else if strings.Contains(response.Body.String(), payloadCanary) ||
		strings.Contains(response.Body.String(), "audit_events") ||
		strings.Contains(response.Body.String(), "no such table") {
		t.Errorf("audit failure response disclosed private detail: %s", response.Body.String())
	}
	var savedSearchRows int64
	if err := database.SQLDB().QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM saved_searches",
	).Scan(&savedSearchRows); err != nil {
		t.Fatal(err)
	}
	if savedSearchRows != 0 {
		t.Errorf(
			"saved-search rows after unavailable audit append = %d, want 0",
			savedSearchRows,
		)
	}
}

type runtimeSavedSearchAuditExpectation struct {
	action   opensplunk.AuditAction
	targetID string
	version  uint64
}

func assertRuntimeSavedSearchAuditEvents(
	t *testing.T,
	response *opensplunk.ListAuditEventsResponse,
	expectations []runtimeSavedSearchAuditExpectation,
) {
	t.Helper()
	page := response.GetPage()
	if len(response.GetAuditEvents()) != len(expectations) ||
		page == nil ||
		page.TotalSize == nil ||
		page.GetTotalSize() != uint64(len(expectations)) ||
		!page.GetTotalSizeExact() ||
		page.GetNextPageToken() != "" {
		t.Fatalf("saved-search audit page = %+v", response)
	}
	for index, expectation := range expectations {
		event := response.GetAuditEvents()[index]
		wantSequence := uint64(len(expectations) - index)
		if event.GetSequence() != wantSequence ||
			event.GetActorKind() != opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_SYSTEM ||
			event.GetActorId() != runtimeSavedSearchSystemActorID ||
			event.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_SYSTEM ||
			event.GetAction() != expectation.action ||
			event.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH ||
			event.GetTargetId() != expectation.targetID ||
			event.GetTargetVersion() != expectation.version ||
			event.GetOccurredAt() == nil ||
			event.GetOccurredAt().CheckValid() != nil {
			t.Fatalf(
				"saved-search audit event %d = %+v, want %+v",
				index,
				event,
				expectation,
			)
		}
	}
}

func newRuntimeSavedSearchAuditAuthenticator(
	t *testing.T,
	bearerToken []byte,
) auth.BrowserAuthenticator {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		bearerToken,
		runtimeSavedSearchAuditTenantID,
		runtimeSavedSearchAuditOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator
}

func newRuntimeSavedSearchAuditHandlerForTest(
	t *testing.T,
	savedSearches server.SavedSearches,
	auditEvents server.AuditEvents,
	authenticator auth.BrowserAuthenticator,
	configure func(*server.Config),
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	config.SavedSearches = savedSearches
	config.AuditEvents = auditEvents
	config.BrowserAuthenticator = authenticator
	config.TenantID = runtimeSavedSearchAuditTenantID
	config.OwnerID = runtimeSavedSearchAuditOwnerID
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	if configure != nil {
		configure(&config)
	}
	handler, err := server.NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

type runtimeSavedSearchRerunSnapshotter uint64

func (snapshot runtimeSavedSearchRerunSnapshotter) VisibilityCutoff(
	context.Context,
) (uint64, error) {
	return uint64(snapshot), nil
}

type runtimeSavedSearchRerunExecutor struct{}

func (runtimeSavedSearchRerunExecutor) Execute(
	_ context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	return sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "_raw",
		Kind: searchjobs.ValueKindString,
	}}})
}

func assertRuntimeSavedSearchRerunProvenance(
	t *testing.T,
	job *opensplunk.SearchJob,
	savedSearch *opensplunk.SavedSearch,
	resolvedAt time.Time,
) {
	t.Helper()
	if job == nil {
		t.Fatalf("saved-search rerun job = %+v", job)
	}
	assertRuntimeSavedSearchRerunRecord(
		t,
		"job",
		job,
		savedSearch,
		resolvedAt,
	)
}

func waitForRuntimeSavedSearchRerunHistory(
	t *testing.T,
	store *searchhistory.Store,
	jobID string,
) *opensplunk.SearchHistoryEntry {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	scope := searchhistory.AccessScope{
		TenantID: runtimeSavedSearchAuditTenantID,
		OwnerID:  runtimeSavedSearchAuditOwnerID,
	}
	for {
		entry, err := store.Get(ctx, scope, jobID)
		if err == nil {
			return entry
		}
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("get saved-search rerun history: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for saved-search rerun history %q", jobID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertRuntimeSavedSearchRerunHistory(
	t *testing.T,
	entry *opensplunk.SearchHistoryEntry,
	savedSearch *opensplunk.SavedSearch,
	resolvedAt time.Time,
) {
	t.Helper()
	if entry == nil ||
		entry.GetFinalState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("saved-search rerun history = %+v", entry)
	}
	assertRuntimeSavedSearchRerunRecord(
		t,
		"history",
		entry,
		savedSearch,
		resolvedAt,
	)
}

type runtimeSavedSearchRerunRecord interface {
	GetSearchJobId() string
	GetDefinition() *opensplunk.SearchDefinition
	GetSource() *opensplunk.SearchJobSource
	GetResolvedTimeRange() *opensplunk.ResolvedTimeRange
}

func assertRuntimeSavedSearchRerunRecord(
	t *testing.T,
	kind string,
	record runtimeSavedSearchRerunRecord,
	savedSearch *opensplunk.SavedSearch,
	resolvedAt time.Time,
) {
	t.Helper()
	wantDefinition := savedSearch.GetDefinition().GetSearch()
	wantEarliest := resolvedAt.Add(-24 * time.Hour)
	if record.GetSearchJobId() != "job-saved-search-rerun-after-reopen" ||
		record.GetDefinition().GetSpl() != wantDefinition.GetSpl() ||
		record.GetDefinition().GetTimeRange().GetEarliest() != "-24h" ||
		record.GetDefinition().GetTimeRange().GetLatest() != "now" ||
		record.GetDefinition().GetTimeRange().GetTimezone() != "UTC" ||
		record.GetSource().GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		record.GetSource().GetSavedSearchId() != savedSearch.GetSavedSearchId() ||
		!record.GetResolvedTimeRange().GetEarliest().AsTime().Equal(wantEarliest) ||
		!record.GetResolvedTimeRange().GetLatest().AsTime().Equal(resolvedAt) ||
		record.GetResolvedTimeRange().GetTimezone() != "UTC" {
		t.Fatalf("saved-search rerun %s = %+v", kind, record)
	}
}
