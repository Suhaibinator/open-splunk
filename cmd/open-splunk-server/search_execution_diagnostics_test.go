package main

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
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

func TestFormatSearchExecutionFailureOmitsPrivateCauseDetails(t *testing.T) {
	t.Parallel()

	cause := &clickhousedriver.Exception{
		Code:       47,
		Name:       "PRIVATE_NAME\nINJECTED",
		Message:    "private generated SQL and password",
		StackTrace: "private stack trace",
	}
	got := formatSearchExecutionFailure(
		"job\nidentifier",
		searchjobs.FailureExecution,
		fmt.Errorf("private wrapper: %w", cause),
	)
	for _, private := range []string{
		"PRIVATE_NAME",
		"generated SQL",
		"password",
		"stack trace",
		"private wrapper",
	} {
		if strings.Contains(got, private) {
			t.Fatalf("diagnostic leaked %q: %q", private, got)
		}
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("diagnostic contains an unescaped newline: %q", got)
	}
	for _, want := range []string{
		`job_id="job\nidentifier"`,
		`failure_code="execution"`,
		`cause_class="clickhouse_exception"`,
		"clickhouse_code=47",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnostic %q does not contain %q", got, want)
		}
	}
}
