package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestSearchAdmissionLogsReasonWithoutChangingHTTPContract(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err    error
		reason string
		status int
	}{
		{searchjobs.ErrCapacity, "capacity_exhausted", http.StatusServiceUnavailable},
		{searchjobs.ErrClosed, "manager_closed", http.StatusServiceUnavailable},
		{searchjobs.ErrStorageUnavailable, "storage_unavailable", http.StatusServiceUnavailable},
		{searchjobs.ErrJournalUnavailable, "journal_unavailable", http.StatusServiceUnavailable},
		{searchjobs.ErrKnowledgeUnavailable, "knowledge_unavailable", http.StatusServiceUnavailable},
		{searchjobs.ErrQueueFull, "queue_full", http.StatusTooManyRequests},
	} {
		t.Run(test.reason, func(t *testing.T) {
			route := "/api/search/jobs/create"
			core, logs := observer.New(zapcore.DebugLevel)
			handler := newTestHandler(t, Config{
				Logger: zap.New(core),
				SearchJobs: &fakeSearchJobs{createErr: &searchjobs.AdmissionError{
					Stage: "visibility_snapshot", Err: test.err,
					Cause: &clickhousedriver.Exception{Code: 47, Message: "private-password private-sql"},
				}},
				Indexes: fakeIndexCatalog{indexes: []control.Index{{
					ID: "idx-1", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
				}}},
				WebUI: testUI(),
			})
			response := postProto(t, handler, route, createRequest("-1h", "now", "main"))
			if response.Code != test.status {
				t.Fatalf("%s status = %d, body = %s", route, response.Code, response.Body.String())
			}
			entries := logs.FilterMessage("search admission rejected").All()
			if len(entries) != 1 {
				t.Fatalf("%s admission logs = %v", route, entries)
			}
			fields := entries[0].ContextMap()
			if fields["failure_reason"] != test.reason || fields["failure_stage"] != "visibility_snapshot" ||
				fields["clickhouse_code"] != int32(47) || fields["path"] != route {
				t.Fatalf("unexpected diagnostic fields: %v", fields)
			}
			if strings.Contains(fmt.Sprint(logs.All()), "private-") || strings.Contains(response.Body.String(), "private-") {
				t.Fatal("diagnostics exposed private driver error text")
			}
		})
	}
}

type failingDiagnosticTrustedAdmission struct {
	err error
}

func (admission failingDiagnosticTrustedAdmission) AdmitTrustedSearch(context.Context, TrustedSearchAdmissionRequest) (searchjobs.Job, error) {
	return searchjobs.Job{}, admission.err
}

func TestTrustedAdmissionFailureIsLogged(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zapcore.DebugLevel)
	handler := &apiHandler{
		logger:                 zap.New(core),
		trustedSearchAdmission: failingDiagnosticTrustedAdmission{err: searchjobs.ErrKnowledgeUnavailable},
		ownerID:                "owner-1",
		tenantID:               "tenant-1",
		now:                    func() time.Time { return testNow },
	}
	_, err := handler.createSearchJob(newAPIRequest(t.Context()), createRequest("-1h", "now", "main"))
	entries := logs.FilterMessage("search admission rejected").All()
	if err == nil || len(entries) != 1 || entries[0].ContextMap()["failure_reason"] != "knowledge_unavailable" {
		t.Fatalf("trusted rejection: error=%v logs=%v", err, entries)
	}
}

func TestSearchAdmissionInternalDeadlineRemainsServiceUnavailable(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zapcore.DebugLevel)
	handler := &apiHandler{logger: zap.New(core)}
	err := &searchjobs.AdmissionError{Stage: "visibility_snapshot", Err: searchjobs.ErrStorageUnavailable, Cause: context.DeadlineExceeded}
	handler.logSearchAdmissionFailure(newAPIRequest(t.Context()), "search", err)
	fields := logs.All()[0].ContextMap()
	if fields["cause_class"] != "context_deadline" || fields["cause_deadline"] != true {
		t.Fatalf("internal timeout diagnostics = %v", fields)
	}
	if _, exists := fields["cause_network_timeout"]; exists {
		t.Fatal("internal context deadline was incorrectly labeled a network timeout")
	}
	if got := mapSearchJobError(err).Error(); !strings.Contains(got, "search service is unavailable") {
		t.Fatalf("internal deadline changed HTTP error: %s", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	handler.logSearchAdmissionFailure(newAPIRequest(ctx), "search", err)
	handler.logSearchAdmissionFailure(newAPIRequest(t.Context()), "search", searchjobs.ErrInvalidSPL)
	if logs.Len() != 1 {
		t.Fatal("caller cancellation or invalid SPL emitted an availability warning")
	}
}

func TestSearchAdmissionClassifiesKnowledgeFailuresWithoutPrivateText(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		cause error
	}{
		{"knowledge_catalog_corrupt", knowledgecatalog.ErrCorrupt},
		{"lookup_catalog_corrupt", lookupcatalog.ErrCorrupt},
		{"lookup_asset_corrupt", lookupasset.ErrCorrupt},
		{"knowledge_snapshot_invalid", knowledgesnapshot.ErrInvalidInput},
		{"knowledge_snapshot_limit", knowledgesnapshot.ErrResourceLimit},
		{"catalog_conflict", lookupcatalog.ErrConflict},
		{"dependency_not_found", control.ErrNotFound},
		{"dependency_not_found", lookupcatalog.ErrNotFound},
		{"dependency_not_found", lookupasset.ErrNotFound},
		{"dependency_not_found", lookupservice.ErrNotFound},
		{"dependency_invalid_argument", control.ErrInvalidArgument},
		{"dependency_invalid_argument", lookupcatalog.ErrInvalid},
		{"dependency_invalid_argument", lookupasset.ErrInvalidArgument},
		{"dependency_invalid_argument", lookupservice.ErrInvalid},
		{"capacity_exhausted", control.ErrCapacityExceeded},
		{"capacity_exhausted", lookupcatalog.ErrCapacity},
		{"capacity_exhausted", lookupasset.ErrResourceLimit},
		{"capacity_exhausted", lookupservice.ErrResourceLimit},
		{"capacity_exhausted", searchjobs.ErrCapacity},
		{"lookup_unavailable", lookupservice.ErrUnavailable},
		{"knowledge_contract_invalid", searchjobs.ErrKnowledgeUnavailable},
		{"spl_compilation", &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX", Message: "private-sql", Suggestions: []string{"private-suggestion"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			handler := &apiHandler{logger: zap.New(core)}
			err := &searchjobs.AdmissionError{
				Stage: "knowledge", Err: searchjobs.ErrKnowledgeUnavailable,
				Cause: fmt.Errorf("private-definition private-path: %w", test.cause),
				Step:  "query_compile", Elapsed: 275 * time.Millisecond,
			}
			handler.logSearchAdmissionFailure(newAPIRequest(t.Context()), "search", err)
			fields := logs.All()[0].ContextMap()
			if fields["cause_class"] != test.name || fields["failure_step"] != "query_compile" ||
				fields["admission_elapsed_ms"] != float64(275) {
				t.Fatalf("unexpected safe diagnostic fields: %v", fields)
			}
			if test.name == "spl_compilation" && fields["diagnostic_code"] != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("compiler diagnostic code missing: %v", fields)
			}
			if strings.Contains(fmt.Sprint(logs.All()), "private-") || errors.Is(err, err.Cause) {
				t.Fatal("private cause leaked through logging or the public error chain")
			}
		})
	}
}

func TestSearchAdmissionLogsTypedSQLiteCodeWithoutDriverMessage(t *testing.T) {
	t.Parallel()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	database.SetMaxOpenConns(1)
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE private_definition (secret TEXT UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO private_definition VALUES ('private-value')`); err != nil {
		t.Fatal(err)
	}
	_, cause := database.ExecContext(t.Context(), `INSERT INTO private_definition VALUES ('private-value')`)
	if cause == nil {
		t.Fatal("expected a real SQLite unique-constraint error")
	}
	core, logs := observer.New(zapcore.DebugLevel)
	handler := &apiHandler{logger: zap.New(core)}
	handler.logSearchAdmissionFailure(newAPIRequest(t.Context()), "search", &searchjobs.AdmissionError{
		Stage: "knowledge", Err: searchjobs.ErrKnowledgeUnavailable,
		Cause: fmt.Errorf("private-query: %w", cause),
	})
	fields := logs.All()[0].ContextMap()
	if fields["cause_class"] != "sqlite_error" || fields["sqlite_code"] != int64(19) ||
		fields["sqlite_extended_code"] != int64(2067) {
		t.Fatalf("unexpected SQLite classification: %v", fields)
	}
	if strings.Contains(fmt.Sprint(logs.All()), "private") {
		t.Fatal("SQLite error message exposed a private schema or query")
	}
}
