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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	runtimeSearchAttemptAuditTenantID  = "tenant-search-attempt-audit"
	runtimeSearchAttemptAuditOwnerID   = "single-user"
	runtimeSearchAttemptAuditActorID   = "open-splunk-server"
	runtimeSearchAttemptAuditJobID     = "job-runtime-search-attempt-audit"
	runtimeSearchAttemptAuditSPLCanary = "__runtime_search_attempt_raw_canary_7319__"
)

var runtimeSearchAttemptAuditTime = time.Date(
	2026,
	time.August,
	6,
	12,
	30,
	45,
	123456000,
	time.UTC,
)

func TestRuntimeSearchAttemptAuditSurvivesHistoryDeleteAndStoreReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")
	bearerToken := bytes.Repeat(
		[]byte("a"),
		auth.MinimumBrowserBearerTokenBytes,
	)

	first := openRuntimeSearchAttemptAuditFixture(
		t,
		databasePath,
		masterKeyPath,
		bearerToken,
	)
	createRequest := runtimeSearchAttemptAuditCreateRequest()
	var createdResponse opensplunk.CreateSearchJobResponse
	postRuntimeProtoOK(
		t,
		first.handler,
		"/api/search/jobs/create",
		createRequest,
		&createdResponse,
		bearerToken,
	)
	created := createdResponse.GetSearchJob()
	if created.GetSearchJobId() != runtimeSearchAttemptAuditJobID ||
		created.GetCreatedAt() == nil ||
		created.GetCreatedAt().CheckValid() != nil {
		t.Fatalf("created search job = %+v", created)
	}

	historyEntry := waitForRuntimeSearchAttemptAuditHistory(
		t,
		first.history,
		runtimeSearchAttemptAuditJobID,
	)
	if historyEntry.GetFinalState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		historyEntry.GetFailure().GetCode() != opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL {
		t.Fatalf("parse-invalid search history = %+v", historyEntry)
	}
	select {
	case notification := <-first.failureReported:
		if notification.Coalesced != 0 ||
			notification.Report.JobID != historyEntry.GetSearchJobId() ||
			notification.Report.Code != searchjobs.FailureInvalidSPL ||
			notification.Report.Message != historyEntry.GetFailure().GetMessage() {
			t.Fatalf("failure log/history correlation = notification %+v, history %+v", notification, historyEntry)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for correlated failure log")
	}
	failureLog := first.failureLog.String()
	for _, want := range []string{
		`"msg":"search failed"`,
		`"job_id":"` + runtimeSearchAttemptAuditJobID + `"`,
		`"failure_code":"invalid_spl"`,
		`"cause_class":"spl_parsing"`,
	} {
		if !strings.Contains(failureLog, want) {
			t.Fatalf("runtime failure log %q does not contain %q", failureLog, want)
		}
	}
	if strings.Contains(failureLog, runtimeSearchAttemptAuditSPLCanary) {
		t.Fatalf("runtime failure log disclosed SPL: %q", failureLog)
	}

	var deletedResponse opensplunk.DeleteSearchHistoryEntryResponse
	postRuntimeProtoOK(
		t,
		first.handler,
		"/api/search/history/delete",
		&opensplunk.DeleteSearchHistoryEntryRequest{
			SearchJobId: runtimeSearchAttemptAuditJobID,
		},
		&deletedResponse,
		bearerToken,
	)
	if deletedResponse.GetSearchJobId() != runtimeSearchAttemptAuditJobID {
		t.Fatalf("deleted search-history ID = %q", deletedResponse.GetSearchJobId())
	}
	if _, err := first.history.Get(
		ctx,
		runtimeSearchAttemptAuditScope(),
		runtimeSearchAttemptAuditJobID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(history after delete) error = %v, want control.ErrNotFound", err)
	}

	firstAudit := listRuntimeSearchAttemptAuditEvents(
		t,
		first.handler,
		bearerToken,
	)
	assertSingleRuntimeSearchAttemptAuditEvent(t, firstAudit)

	first.close(t)

	secondDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	secondDatabaseClosed := false
	t.Cleanup(func() {
		if !secondDatabaseClosed {
			if err := secondDatabase.Close(); err != nil {
				t.Errorf("close reopened control database: %v", err)
			}
		}
	})
	secondStores, err := openRuntimeSecurityStores(
		ctx,
		secondDatabase,
		masterKeyPath,
		runtimeSearchAttemptAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if secondStores.searchAttemptAuditEvents == nil {
		t.Fatal("reopened runtime security stores omitted search-attempt audit store")
	}
	secondHandler := newRuntimeSearchAttemptAuditHandler(
		t,
		runtimeSearchAttemptAuditAuthenticator(t, bearerToken),
		secondStores.searchAttemptAuditEvents,
		nil,
		nil,
		nil,
	)
	secondHandlerClosed := false
	t.Cleanup(func() {
		if !secondHandlerClosed {
			if err := secondHandler.Close(context.Background()); err != nil {
				t.Errorf("close reopened runtime handler: %v", err)
			}
		}
	})

	secondAudit := listRuntimeSearchAttemptAuditEvents(
		t,
		secondHandler,
		bearerToken,
	)
	assertSingleRuntimeSearchAttemptAuditEvent(t, secondAudit)

	if err := secondHandler.Close(ctx); err != nil {
		t.Fatalf("close reopened runtime handler: %v", err)
	}
	secondHandlerClosed = true
	if err := secondDatabase.Close(); err != nil {
		t.Fatalf("close reopened control database: %v", err)
	}
	secondDatabaseClosed = true
}

func TestRuntimeSearchAdmissionFailsClosedWithoutAttemptAuditJournal(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	bearerToken := bytes.Repeat(
		[]byte("f"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	fixture := openRuntimeSearchAttemptAuditFixture(
		t,
		filepath.Join(directory, "control.db"),
		filepath.Join(directory, "server.key"),
		bearerToken,
	)
	defer fixture.close(t)

	if _, err := fixture.database.SQLDB().ExecContext(
		ctx,
		"DROP TABLE search_attempt_audit_events",
	); err != nil {
		t.Fatalf("make search-attempt audit journal unavailable: %v", err)
	}
	response := postRuntimeAppProto(
		t,
		fixture.handler,
		"/api/search/jobs/create",
		runtimeSearchAttemptAuditCreateRequest(),
		bearerToken,
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf(
			"search admission without audit journal status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	responseBody := response.Body.String()
	if strings.Contains(responseBody, runtimeSearchAttemptAuditSPLCanary) ||
		strings.Contains(responseBody, "search_attempt_audit_events") ||
		strings.Contains(strings.ToLower(responseBody), "no such table") {
		t.Errorf("search-attempt audit failure disclosed private detail: %s", responseBody)
	}

	for _, table := range []string{"search_history_pending", "search_history"} {
		var rows int64
		if err := fixture.database.SQLDB().QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+table,
		).Scan(&rows); err != nil {
			t.Fatalf("count %s after rejected admission: %v", table, err)
		}
		if rows != 0 {
			t.Errorf("%s rows after rejected admission = %d, want 0", table, rows)
		}
	}
	var tenantStateRows int64
	if err := fixture.database.SQLDB().QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM search_attempt_audit_tenant_state",
	).Scan(&tenantStateRows); err != nil {
		t.Fatal(err)
	}
	if tenantStateRows != 0 {
		t.Errorf(
			"search-attempt audit tenant-state rows after rejected admission = %d, want 0",
			tenantStateRows,
		)
	}
	if _, err := fixture.jobs.GetFor(
		searchjobs.AccessScope{
			TenantID: runtimeSearchAttemptAuditTenantID,
			OwnerID:  runtimeSearchAttemptAuditOwnerID,
		},
		runtimeSearchAttemptAuditJobID,
	); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("GetFor(rejected admission) error = %v, want searchjobs.ErrNotFound", err)
	}
}

type runtimeSearchAttemptAuditFixture struct {
	database        *control.DB
	history         *searchhistory.Store
	jobs            *searchjobs.Manager
	handler         *server.Handler
	failureLog      *bytes.Buffer
	failureReported chan searchjobs.FailureNotification
	closed          bool
}

func openRuntimeSearchAttemptAuditFixture(
	t *testing.T,
	databasePath string,
	masterKeyPath string,
	bearerToken []byte,
) *runtimeSearchAttemptAuditFixture {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &runtimeSearchAttemptAuditFixture{database: database}
	t.Cleanup(func() { fixture.close(t) })
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		RetentionPeriod:  30 * 24 * time.Hour,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("create runtime search-attempt audit index: %v", err)
	}
	stores, err := openRuntimeSecurityStores(
		ctx,
		database,
		masterKeyPath,
		runtimeSearchAttemptAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stores.searchAttemptAuditEvents == nil {
		t.Fatal("runtime security stores omitted search-attempt audit store")
	}
	history, err := openSearchHistoryStore(
		ctx,
		database,
		masterKeyPath,
		searchhistory.Options{
			AuditAppender:             stores.searchAttemptAuditEvents,
			RequireSearchAttemptAudit: true,
			Clock: func() time.Time {
				return runtimeSearchAttemptAuditTime
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.history = history
	journal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		t.Fatal(err)
	}
	failureLog := &bytes.Buffer{}
	failureLogger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(failureLog),
		zap.DebugLevel,
	))
	failureReported := make(chan searchjobs.FailureNotification, 1)
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor:        runtimeSearchAttemptAuditExecutor{},
		Snapshotter:     runtimeSearchAttemptAuditSnapshotter(23),
		Journal:         journal,
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now: func() time.Time {
			return runtimeSearchAttemptAuditTime
		},
		NewID: func() string {
			return runtimeSearchAttemptAuditJobID
		},
		OnFailure: func(notification searchjobs.FailureNotification) {
			logSearchFailure(failureLogger, notification)
			failureReported <- notification
		},
		CursorKey: bytes.Repeat([]byte("j"), 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.jobs = jobs
	fixture.failureLog = failureLog
	fixture.failureReported = failureReported
	fixture.handler = newRuntimeSearchAttemptAuditHandler(
		t,
		runtimeSearchAttemptAuditAuthenticator(t, bearerToken),
		stores.searchAttemptAuditEvents,
		jobs,
		history,
		database,
	)
	return fixture
}

func (fixture *runtimeSearchAttemptAuditFixture) close(t *testing.T) {
	t.Helper()
	if fixture == nil || fixture.closed {
		return
	}
	fixture.closed = true
	if fixture.handler != nil {
		if err := fixture.handler.Close(context.Background()); err != nil {
			t.Errorf("close runtime search-attempt audit handler: %v", err)
		}
	}
	if fixture.jobs != nil {
		if err := fixture.jobs.Close(); err != nil {
			t.Errorf("close runtime search-attempt audit jobs: %v", err)
		}
	}
	if fixture.database != nil {
		if err := fixture.database.Close(); err != nil {
			t.Errorf("close runtime search-attempt audit database: %v", err)
		}
	}
}

func newRuntimeSearchAttemptAuditHandler(
	t *testing.T,
	authenticator auth.BrowserAuthenticator,
	auditEvents server.SearchAttemptAuditEvents,
	jobs server.SearchJobs,
	history server.SearchHistory,
	indexes server.IndexCatalog,
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	if jobs != nil {
		config.SearchJobs = jobs
	}
	if history != nil {
		config.SearchHistory = history
	}
	if indexes != nil {
		config.Indexes = indexes
	}
	config.SearchAttemptAuditEvents = auditEvents
	config.BrowserAuthenticator = authenticator
	config.TenantID = runtimeSearchAttemptAuditTenantID
	config.OwnerID = runtimeSearchAttemptAuditOwnerID
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	handler, err := server.NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func runtimeSearchAttemptAuditAuthenticator(
	t *testing.T,
	bearerToken []byte,
) auth.BrowserAuthenticator {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		bearerToken,
		runtimeSearchAttemptAuditTenantID,
		runtimeSearchAttemptAuditOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator
}

func runtimeSearchAttemptAuditCreateRequest() *opensplunk.CreateSearchJobRequest {
	earliest := "-15m"
	latest := "now"
	timezone := "UTC"
	return &opensplunk.CreateSearchJobRequest{
		Definition: &opensplunk.SearchDefinition{
			Spl: "index=main | where " +
				runtimeSearchAttemptAuditSPLCanary + " =",
			IndexScope: []string{"main"},
			TimeRange: &opensplunk.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
		},
	}
}

func waitForRuntimeSearchAttemptAuditHistory(
	t *testing.T,
	history *searchhistory.Store,
	jobID string,
) *opensplunk.SearchHistoryEntry {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		entry, err := history.Get(ctx, runtimeSearchAttemptAuditScope(), jobID)
		if err == nil {
			return entry
		}
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("get runtime search-attempt history: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for search history %q", jobID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runtimeSearchAttemptAuditScope() searchhistory.AccessScope {
	return searchhistory.AccessScope{
		TenantID: runtimeSearchAttemptAuditTenantID,
		OwnerID:  runtimeSearchAttemptAuditOwnerID,
	}
}

func listRuntimeSearchAttemptAuditEvents(
	t *testing.T,
	handler http.Handler,
	bearerToken []byte,
) *opensplunk.ListSearchAttemptAuditEventsResponse {
	t.Helper()
	pageSize := uint32(10)
	request := &opensplunk.ListSearchAttemptAuditEventsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		},
	}
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/audit/search-attempts/list",
		request,
		bearerToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"search-attempt audit list status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if bytes.Contains(
		response.Body.Bytes(),
		[]byte(runtimeSearchAttemptAuditSPLCanary),
	) {
		t.Fatal("search-attempt audit response disclosed raw SPL")
	}
	var result opensplunk.ListSearchAttemptAuditEventsResponse
	unmarshalRuntimeAppResponse(t, response, &result)
	return &result
}

func assertSingleRuntimeSearchAttemptAuditEvent(
	t *testing.T,
	response *opensplunk.ListSearchAttemptAuditEventsResponse,
) {
	t.Helper()
	page := response.GetPage()
	if len(response.GetEvents()) != 1 ||
		page == nil ||
		page.TotalSize == nil ||
		page.GetTotalSize() != 1 ||
		!page.GetTotalSizeExact() ||
		page.GetNextPageToken() != "" {
		t.Fatalf("search-attempt audit page = %+v", response)
	}
	event := response.GetEvents()[0]
	if event.GetSequence() != 1 ||
		event.GetActorKind() != opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_SYSTEM ||
		event.GetActorId() != runtimeSearchAttemptAuditActorID ||
		event.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_SYSTEM ||
		event.GetOwnerId() != runtimeSearchAttemptAuditOwnerID ||
		event.GetSearchJobId() != runtimeSearchAttemptAuditJobID ||
		event.GetOccurredAt() == nil ||
		event.GetOccurredAt().CheckValid() != nil ||
		!event.GetOccurredAt().AsTime().Equal(runtimeSearchAttemptAuditTime) {
		t.Fatalf("search-attempt audit event = %+v", event)
	}
}

type runtimeSearchAttemptAuditSnapshotter uint64

func (snapshot runtimeSearchAttemptAuditSnapshotter) VisibilityCutoff(
	context.Context,
) (uint64, error) {
	return uint64(snapshot), nil
}

type runtimeSearchAttemptAuditExecutor struct{}

func (runtimeSearchAttemptAuditExecutor) Execute(
	context.Context,
	clickhouse.CompiledQuery,
	searchjobs.ResultSink,
) error {
	return errors.New("runtime search-attempt audit executor ran for parse-invalid SPL")
}
