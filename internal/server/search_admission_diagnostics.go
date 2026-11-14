package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"syscall"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"modernc.org/sqlite"
)

// Log before HTTP mapping discards the admission category. Never log the SPL
// or raw error text: driver and resolver errors can contain private data.
func (handler *apiHandler) logSearchAdmissionFailure(request *http.Request, appID string, err error) {
	if handler.logger == nil || request.Context().Err() != nil {
		return
	}
	reason := searchAdmissionReason(err)
	if reason == "" {
		return
	}
	stage := "admission"
	cause := err
	if diagnostic, ok := errors.AsType[*searchjobs.AdmissionError](err); ok {
		stage = diagnostic.Stage
		cause = diagnostic.Cause
	}
	fields := []zap.Field{
		zap.String("failure_reason", reason),
		zap.String("failure_stage", stage),
		zap.String("tenant_id", handler.tenantID),
		zap.String("owner_id", handler.ownerID),
		zap.String("app_id", appID),
		zap.String("path", request.URL.Path),
		zap.String("method", request.Method),
	}
	if diagnostic, ok := errors.AsType[*searchjobs.AdmissionError](err); ok && diagnostic.Step != "" {
		fields = append(fields,
			zap.String("admission_step", diagnostic.Step),
			zap.Float64("admission_elapsed_ms", float64(diagnostic.Elapsed)/1e6),
		)
	}
	if cause != nil {
		fields = append(fields, zap.String("cause_class", searchAdmissionCauseClass(cause)),
			zap.String("cause_type", reflect.TypeOf(cause).String()),
			zap.Bool("cause_deadline", errors.Is(cause, context.DeadlineExceeded)),
			zap.Bool("cause_canceled", errors.Is(cause, context.Canceled)))
		if networkError, ok := errors.AsType[net.Error](cause); ok &&
			!errors.Is(cause, context.DeadlineExceeded) && !errors.Is(cause, context.Canceled) {
			fields = append(fields, zap.Bool("cause_network_timeout", networkError.Timeout()))
		}
		if exception, ok := errors.AsType[*clickhousedriver.Exception](cause); ok {
			fields = append(fields, zap.Int32("clickhouse_code", exception.Code))
		}
		fields = append(fields, searchResolutionDiagnosticFields(cause)...)
	}
	handler.logger.Warn("search admission rejected", fields...)
}

func searchResolutionDiagnosticFields(cause error) []zap.Field {
	var fields []zap.Field
	sqliteCode, hasSQLiteCode := 0, false
	if sqliteError, ok := errors.AsType[*sqlite.Error](cause); ok {
		sqliteCode, hasSQLiteCode = sqliteError.Code(), true
	}
	if diagnostic, ok := knowledgecatalog.ResolutionDiagnosticsFromError(cause); ok {
		fields = append(fields,
			zap.String("resolution_step", diagnostic.Step),
			zap.Float64("resolution_elapsed_ms", float64(diagnostic.Elapsed)/1e6),
			zap.Int("resolution_attempts", diagnostic.Attempts),
			zap.Int("control_db_pool_open", diagnostic.Pool.OpenConnections),
			zap.Int("control_db_pool_in_use", diagnostic.Pool.InUse),
			zap.Int("control_db_pool_idle", diagnostic.Pool.Idle),
			zap.Int("control_db_pool_max_open", diagnostic.Pool.MaxOpenConnections),
			// These deltas include every user of this shared pool during Resolve.
			zap.Int64("control_db_pool_wait_count_delta", diagnostic.PoolWaitCountDelta),
			zap.Float64("control_db_pool_wait_duration_ms_delta", float64(diagnostic.PoolWaitDurationDelta)/1e6),
		)
		if diagnostic.HasSQLiteCode {
			sqliteCode, hasSQLiteCode = diagnostic.SQLiteCode, true
		}
	}
	if hasSQLiteCode {
		fields = append(fields,
			zap.Int("sqlite_code", sqliteCode),
			zap.Int("sqlite_primary_code", sqliteCode&0xff),
		)
	}
	return fields
}

func searchAdmissionCauseClass(cause error) string {
	switch {
	case errors.Is(cause, context.DeadlineExceeded):
		return "context_deadline"
	case errors.Is(cause, context.Canceled):
		return "context_canceled"
	case errors.Is(cause, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(cause, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(cause, io.EOF), errors.Is(cause, io.ErrUnexpectedEOF):
		return "transport_eof"
	case errors.Is(cause, clickhousedriver.ErrAcquireConnTimeout):
		return "clickhouse_acquire_timeout"
	case errors.Is(cause, clickhousedriver.ErrConnectionClosed):
		return "clickhouse_connection_closed"
	case errors.Is(cause, knowledgecatalog.ErrCorrupt), errors.Is(cause, lookupcatalog.ErrCorrupt):
		return "catalog_corrupt"
	case errors.Is(cause, lookupcatalog.ErrConflict):
		return "catalog_conflict"
	case errors.Is(cause, control.ErrNotFound), errors.Is(cause, lookupcatalog.ErrNotFound):
		return "catalog_not_found"
	case errors.Is(cause, control.ErrInvalidArgument), errors.Is(cause, lookupcatalog.ErrInvalid):
		return "invalid_argument"
	case errors.Is(cause, control.ErrCapacityExceeded), errors.Is(cause, lookupcatalog.ErrCapacity):
		return "catalog_capacity"
	default:
		if sqliteError, ok := errors.AsType[*sqlite.Error](cause); ok {
			switch sqliteError.Code() & 0xff {
			case 5:
				return "sqlite_busy"
			case 6:
				return "sqlite_locked"
			default:
				return "sqlite_error"
			}
		}
		if _, ok := errors.AsType[*clickhousedriver.Exception](cause); ok {
			return "clickhouse_exception"
		}
		return "other"
	}
}

func searchAdmissionReason(err error) string {
	switch {
	case errors.Is(err, searchjobs.ErrCapacity):
		return "capacity_exhausted"
	case errors.Is(err, searchjobs.ErrClosed):
		return "manager_closed"
	case errors.Is(err, searchjobs.ErrStorageUnavailable):
		return "storage_unavailable"
	case errors.Is(err, searchjobs.ErrJournalUnavailable):
		return "journal_unavailable"
	case errors.Is(err, searchjobs.ErrKnowledgeUnavailable):
		return "knowledge_unavailable"
	case errors.Is(err, searchjobs.ErrQueueFull):
		return "queue_full"
	default:
		return ""
	}
}
