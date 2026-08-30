package main

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
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
	searchCauseSPLParsing          = "spl_parsing"
	searchCauseSPLPlanning         = "spl_planning"
	searchCauseInvariant           = "internal_invariant"
	searchCauseRecoveredPanic      = "recovered_panic"
	searchCauseOther               = "other"
)

func logSearchFailure(logger *zap.Logger, notification searchjobs.FailureNotification) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fields := searchFailureFields(notification)
	if notification.Coalesced > 0 {
		logger.Error("search failure notifications coalesced", fields...)
		return
	}
	if searchFailureIsWarning(notification.Report.Code) {
		logger.Warn("search failed", fields...)
		return
	}
	logger.Error("search failed", fields...)
}

func searchFailureFields(notification searchjobs.FailureNotification) []zap.Field {
	report := notification.Report
	causeClass, clickHouseCode, hasClickHouseCode := classifySearchFailureCause(notification)
	fields := []zap.Field{
		zap.String("job_id", report.JobID),
		zap.String("tenant_id", report.TenantID),
		zap.String("owner_id", report.OwnerID),
		zap.String("search_origin", report.Source.Origin.String()),
		zap.String("failure_phase", report.Phase.String()),
		zap.String("failure_code", string(report.Code)),
		zap.String("failure_message", report.Message),
		zap.Bool("retryable", report.Retryable),
		zap.String("cause_class", causeClass),
		zap.Int64("max_runtime_ms", report.MaxRuntime.Milliseconds()),
		zap.Int64("queue_wait_ms", report.QueueWait.Milliseconds()),
		zap.Int64("elapsed_ms", report.Elapsed.Milliseconds()),
		zap.Uint64("scanned_rows", report.ScannedRows),
		zap.Uint64("scanned_bytes", report.ScannedBytes),
		zap.Uint64("produced_rows", report.ProducedRows),
		zap.Uint64("result_bytes", report.ResultBytes),
	}
	if report.AppID != "" {
		fields = append(fields, zap.String("app_id", report.AppID))
	}
	if report.Source.ObjectID != "" {
		fields = append(fields, zap.String("source_object_id", report.Source.ObjectID))
	}
	if notification.Coalesced > 0 {
		fields = append(fields, zap.Uint64("coalesced_failures", notification.Coalesced))
	}
	if hasClickHouseCode {
		fields = append(fields, zap.Int32("clickhouse_code", clickHouseCode))
	}
	return fields
}

func searchFailureIsWarning(code searchjobs.FailureCode) bool {
	switch code {
	case searchjobs.FailureInvalidSPL,
		searchjobs.FailureUnsupportedSPL,
		searchjobs.FailureInvalidTimeRange,
		searchjobs.FailureIndexForbidden,
		searchjobs.FailureResourceLimit:
		return true
	default:
		return false
	}
}

func classifySearchFailureCause(
	notification searchjobs.FailureNotification,
) (string, int32, bool) {
	switch notification.CauseKind {
	case searchjobs.FailureCauseParsing:
		return searchCauseSPLParsing, 0, false
	case searchjobs.FailureCausePlanning:
		return searchCauseSPLPlanning, 0, false
	case searchjobs.FailureCauseInvariant:
		return searchCauseInvariant, 0, false
	case searchjobs.FailureCauseRecoveredPanic:
		return searchCauseRecoveredPanic, 0, false
	case searchjobs.FailureCauseExecution:
		return classifySearchExecutionCause(notification.Cause)
	default:
		return searchCauseOther, 0, false
	}
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
