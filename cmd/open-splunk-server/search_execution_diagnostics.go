package main

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	searchCauseClickHouseException = "clickhouse_exception"
	searchCauseAcquireTimeout      = "clickhouse_acquire_timeout"
	searchCauseConnectionClosed    = "clickhouse_connection_closed"
	searchCauseBadConnection       = "driver_bad_connection"
	searchCauseContextDeadline     = "context_deadline"
	searchCauseContextCanceled     = "context_canceled"
	searchCauseTransportEOF        = "transport_eof"
	searchCauseTransportReset      = "transport_reset"
	searchCauseTransportRefused    = "transport_refused"
	searchCauseNetworkTimeout      = "network_timeout"
	searchCauseNetwork             = "network"
	searchCauseStorageUnavailable  = "storage_unavailable"
	searchCauseExecutionLimit      = "execution_limit"
	searchCauseUnsupportedValue    = "unsupported_value"
	searchCauseInvalidResult       = "invalid_result"
	searchCauseOther               = "other"
)

func formatSearchExecutionFailure(
	jobID string,
	failureCode searchjobs.FailureCode,
	cause error,
) string {
	causeClass, clickHouseCode, hasClickHouseCode := classifySearchExecutionCause(cause)
	diagnostic := fmt.Sprintf(
		"search execution failed: job_id=%q failure_code=%q cause_class=%q",
		jobID,
		failureCode,
		causeClass,
	)
	if hasClickHouseCode {
		diagnostic += fmt.Sprintf(" clickhouse_code=%d", clickHouseCode)
	}
	return diagnostic
}

func classifySearchExecutionCause(cause error) (string, int32, bool) {
	if exception, ok := errors.AsType[*clickhousedriver.Exception](cause); ok {
		return searchCauseClickHouseException, exception.Code, true
	}
	switch {
	case errors.Is(cause, clickhousedriver.ErrAcquireConnTimeout):
		return searchCauseAcquireTimeout, 0, false
	case errors.Is(cause, clickhousedriver.ErrConnectionClosed):
		return searchCauseConnectionClosed, 0, false
	case errors.Is(cause, sqldriver.ErrBadConn):
		return searchCauseBadConnection, 0, false
	case errors.Is(cause, context.DeadlineExceeded):
		return searchCauseContextDeadline, 0, false
	case errors.Is(cause, context.Canceled):
		return searchCauseContextCanceled, 0, false
	case errors.Is(cause, io.EOF), errors.Is(cause, io.ErrUnexpectedEOF):
		return searchCauseTransportEOF, 0, false
	case errors.Is(cause, syscall.ECONNRESET), errors.Is(cause, syscall.EPIPE):
		return searchCauseTransportReset, 0, false
	case errors.Is(cause, syscall.ECONNREFUSED):
		return searchCauseTransportRefused, 0, false
	case errors.Is(cause, syscall.ETIMEDOUT):
		return searchCauseNetworkTimeout, 0, false
	case errors.Is(cause, searchjobs.ErrStorageUnavailable):
		return searchCauseStorageUnavailable, 0, false
	case errors.Is(cause, searchjobs.ErrExecutionLimit):
		return searchCauseExecutionLimit, 0, false
	case errors.Is(cause, searchjobs.ErrUnsupportedValue):
		return searchCauseUnsupportedValue, 0, false
	case errors.Is(cause, searchjobs.ErrInvalidResult), errors.Is(cause, searchjobs.ErrStreamClosed):
		return searchCauseInvalidResult, 0, false
	}
	if networkError, ok := errors.AsType[net.Error](cause); ok {
		if networkError.Timeout() {
			return searchCauseNetworkTimeout, 0, false
		}
		return searchCauseNetwork, 0, false
	}
	return searchCauseOther, 0, false
}
