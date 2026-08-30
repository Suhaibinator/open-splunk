package main

import (
	"bytes"
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type searchDiagnosticNetworkError struct{}

func (searchDiagnosticNetworkError) Error() string   { return "private network endpoint" }
func (searchDiagnosticNetworkError) Timeout() bool   { return false }
func (searchDiagnosticNetworkError) Temporary() bool { return true }

func TestClassifySearchExecutionCauseUsesStableSecretFreeClasses(t *testing.T) {
	t.Parallel()

	privateException := &clickhousedriver.Exception{
		Code:       47,
		Name:       "UNKNOWN_IDENTIFIER_WITH_PRIVATE_COLUMN",
		Message:    "private generated SQL and password",
		StackTrace: "private stack trace",
	}
	for _, test := range []struct {
		name     string
		cause    error
		want     string
		wantCode int32
		hasCode  bool
	}{
		{name: "ClickHouse exception", cause: fmt.Errorf("query: %w", privateException), want: searchCauseClickHouseException, wantCode: 47, hasCode: true},
		{name: "pool timeout", cause: fmt.Errorf("query: %w", clickhousedriver.ErrAcquireConnTimeout), want: searchCauseAcquireTimeout},
		{name: "closed pool", cause: fmt.Errorf("query: %w", clickhousedriver.ErrConnectionClosed), want: searchCauseConnectionClosed},
		{name: "bad connection", cause: fmt.Errorf("query: %w", sqldriver.ErrBadConn), want: searchCauseBadConnection},
		{name: "deadline", cause: fmt.Errorf("query: %w", context.DeadlineExceeded), want: searchCauseContextDeadline},
		{name: "canceled", cause: fmt.Errorf("query: %w", context.Canceled), want: searchCauseContextCanceled},
		{name: "EOF", cause: fmt.Errorf("query: %w", io.ErrUnexpectedEOF), want: searchCauseTransportEOF},
		{name: "reset", cause: fmt.Errorf("query: %w", syscall.ECONNRESET), want: searchCauseTransportReset},
		{name: "refused", cause: fmt.Errorf("query: %w", syscall.ECONNREFUSED), want: searchCauseTransportRefused},
		{name: "syscall timeout", cause: fmt.Errorf("query: %w", syscall.ETIMEDOUT), want: searchCauseNetworkTimeout},
		{name: "network", cause: fmt.Errorf("query: %w", searchDiagnosticNetworkError{}), want: searchCauseNetwork},
		{name: "storage sentinel", cause: fmt.Errorf("query: %w", searchjobs.ErrStorageUnavailable), want: searchCauseStorageUnavailable},
		{name: "execution limit", cause: fmt.Errorf("query: %w", searchjobs.ErrExecutionLimit), want: searchCauseExecutionLimit},
		{name: "unsupported value", cause: fmt.Errorf("query: %w", searchjobs.ErrUnsupportedValue), want: searchCauseUnsupportedValue},
		{name: "invalid result", cause: fmt.Errorf("query: %w", searchjobs.ErrInvalidResult), want: searchCauseInvalidResult},
		{name: "generic", cause: errors.New("private generated SQL and password"), want: searchCauseOther},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, code, hasCode := classifySearchExecutionCause(test.cause)
			if got != test.want || code != test.wantCode || hasCode != test.hasCode {
				t.Fatalf(
					"classifySearchExecutionCause() = (%q, %d, %t), want (%q, %d, %t)",
					got,
					code,
					hasCode,
					test.want,
					test.wantCode,
					test.hasCode,
				)
			}
		})
	}
}

func TestSearchFailureLogIncludesOperationalFieldsAndOmitsPrivateDetails(t *testing.T) {
	t.Parallel()

	cause := &clickhousedriver.Exception{
		Code:       47,
		Name:       "PRIVATE_NAME\nINJECTED",
		Message:    "private generated SQL and password",
		StackTrace: "private stack trace",
	}
	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zap.DebugLevel,
	))
	logSearchFailure(logger, searchjobs.FailureNotification{Report: searchjobs.FailureReport{
		JobID:        "job\nidentifier",
		TenantID:     "tenant-a",
		OwnerID:      "owner-a",
		AppID:        "search",
		Source:       searchjobs.JobSource{Origin: searchjobs.JobOriginSavedSearch, ObjectID: "saved-1"},
		Phase:        searchjobs.StateRunning,
		Code:         searchjobs.FailureExecution,
		Message:      "search execution failed",
		Retryable:    true,
		MaxRuntime:   2 * time.Minute,
		QueueWait:    1250 * time.Millisecond,
		Elapsed:      3 * time.Second,
		ScannedRows:  11,
		ScannedBytes: 22,
		ProducedRows: 3,
		ResultBytes:  44,
	}, CauseKind: searchjobs.FailureCauseExecution, Cause: fmt.Errorf("private wrapper: %w", cause)})
	got := output.String()
	for _, private := range []string{
		"PRIVATE_NAME",
		"generated SQL",
		"password",
		"stack trace",
		"private wrapper",
	} {
		if strings.Contains(got, private) {
			t.Fatalf("structured diagnostic leaked %q: %q", private, got)
		}
	}
	if strings.Count(got, "\n") != 1 {
		t.Fatalf("structured diagnostic is not one record: %q", got)
	}
	for _, want := range []string{
		`"level":"error"`,
		`"msg":"search failed"`,
		`"job_id":"job\nidentifier"`,
		`"tenant_id":"tenant-a"`,
		`"owner_id":"owner-a"`,
		`"app_id":"search"`,
		`"search_origin":"saved_search"`,
		`"source_object_id":"saved-1"`,
		`"failure_phase":"running"`,
		`"failure_code":"execution"`,
		`"failure_message":"search execution failed"`,
		`"retryable":true`,
		`"cause_class":"clickhouse_exception"`,
		`"clickhouse_code":47`,
		`"max_runtime_ms":120000`,
		`"queue_wait_ms":1250`,
		`"elapsed_ms":3000`,
		`"scanned_rows":11`,
		`"scanned_bytes":22`,
		`"produced_rows":3`,
		`"result_bytes":44`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("structured diagnostic %q does not contain %q", got, want)
		}
	}
}

func TestSearchFailureSeverityPolicy(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		code searchjobs.FailureCode
		warn bool
	}{
		{code: searchjobs.FailureInvalidSPL, warn: true},
		{code: searchjobs.FailureUnsupportedSPL, warn: true},
		{code: searchjobs.FailureInvalidTimeRange, warn: true},
		{code: searchjobs.FailureIndexForbidden, warn: true},
		{code: searchjobs.FailureResourceLimit, warn: true},
		{code: searchjobs.FailureTimeout},
		{code: searchjobs.FailureStorageUnavailable},
		{code: searchjobs.FailureExecution},
		{code: searchjobs.FailureInternal},
	} {
		if got := searchFailureIsWarning(test.code); got != test.warn {
			t.Errorf("searchFailureIsWarning(%q) = %t, want %t", test.code, got, test.warn)
		}
	}
}

func TestSearchFailureCauseKindsHaveStableClasses(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		kind searchjobs.FailureCauseKind
		want string
	}{
		{kind: searchjobs.FailureCauseParsing, want: searchCauseSPLParsing},
		{kind: searchjobs.FailureCausePlanning, want: searchCauseSPLPlanning},
		{kind: searchjobs.FailureCauseInvariant, want: searchCauseInvariant},
		{kind: searchjobs.FailureCauseRecoveredPanic, want: searchCauseRecoveredPanic},
		{kind: searchjobs.FailureCauseUnknown, want: searchCauseOther},
	} {
		got, code, hasCode := classifySearchFailureCause(searchjobs.FailureNotification{CauseKind: test.kind})
		if got != test.want || code != 0 || hasCode {
			t.Errorf("classifySearchFailureCause(%d) = (%q, %d, %t), want (%q, 0, false)", test.kind, got, code, hasCode, test.want)
		}
	}
}

func TestCoalescedSearchFailuresAreExplicitErrors(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zap.DebugLevel,
	))
	logSearchFailure(logger, searchjobs.FailureNotification{
		Report: searchjobs.FailureReport{
			JobID: "latest", TenantID: "tenant-a", OwnerID: "owner-a",
			Source: searchjobs.JobSource{Origin: searchjobs.JobOriginAPI},
			Phase:  searchjobs.StateRunning, Code: searchjobs.FailureInvalidSPL,
			Message: "search SPL is invalid",
		},
		Coalesced: 7, CauseKind: searchjobs.FailureCauseParsing,
	})
	got := output.String()
	for _, want := range []string{
		`"level":"error"`,
		`"msg":"search failure notifications coalesced"`,
		`"coalesced_failures":7`,
		`"job_id":"latest"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("coalesced diagnostic %q does not contain %q", got, want)
		}
	}
}
