package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/control"
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
				fields["clickhouse_code"] != int64(47) || fields["path"] != route {
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
