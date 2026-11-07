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
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"github.com/Suhaibinator/open-splunk/internal/plan"
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
	step := ""
	cause := err
	if diagnostic, ok := errors.AsType[*searchjobs.AdmissionError](err); ok {
		stage = diagnostic.Stage
		step = diagnostic.Step
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
	if step != "" {
		fields = append(fields, zap.String("failure_step", step))
	}
	if cause != nil {
		fields = append(fields, zap.String("cause_class", searchAdmissionCauseClass(cause)),
			zap.String("cause_type", reflect.TypeOf(cause).String()),
			zap.Bool("cause_deadline", errors.Is(cause, context.DeadlineExceeded)),
			zap.Bool("cause_canceled", errors.Is(cause, context.Canceled)))
		if diagnostic, ok := errors.AsType[*plan.Diagnostic](cause); ok {
			fields = append(fields, zap.String("diagnostic_code", diagnostic.Code))
		}
		if databaseError, ok := errors.AsType[*sqlite.Error](cause); ok {
			fields = append(fields, zap.Int("sqlite_code", databaseError.Code()&0xff),
				zap.Int("sqlite_extended_code", databaseError.Code()))
		}
		if networkError, ok := errors.AsType[net.Error](cause); ok {
			fields = append(fields, zap.Bool("cause_network_timeout", networkError.Timeout()))
		}
		if exception, ok := errors.AsType[*clickhousedriver.Exception](cause); ok {
			fields = append(fields, zap.Int32("clickhouse_code", exception.Code))
		}
	}
	handler.logger.Warn("search admission rejected", fields...)
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
	case errors.Is(cause, knowledgecatalog.ErrCorrupt):
		return "knowledge_catalog_corrupt"
	case errors.Is(cause, lookupcatalog.ErrCorrupt):
		return "lookup_catalog_corrupt"
	case errors.Is(cause, lookupasset.ErrCorrupt):
		return "lookup_asset_corrupt"
	case errors.Is(cause, knowledgesnapshot.ErrInvalidInput):
		return "knowledge_snapshot_invalid"
	case errors.Is(cause, knowledgesnapshot.ErrResourceLimit):
		return "knowledge_snapshot_limit"
	case errors.Is(cause, control.ErrCapacityExceeded), errors.Is(cause, lookupcatalog.ErrCapacity),
		errors.Is(cause, lookupasset.ErrResourceLimit), errors.Is(cause, lookupservice.ErrResourceLimit),
		errors.Is(cause, searchjobs.ErrCapacity):
		return "capacity_exhausted"
	case errors.Is(cause, control.ErrNotFound), errors.Is(cause, lookupcatalog.ErrNotFound),
		errors.Is(cause, lookupasset.ErrNotFound), errors.Is(cause, lookupservice.ErrNotFound):
		return "dependency_not_found"
	case errors.Is(cause, control.ErrInvalidArgument), errors.Is(cause, lookupcatalog.ErrInvalid),
		errors.Is(cause, lookupasset.ErrInvalidArgument), errors.Is(cause, lookupservice.ErrInvalid):
		return "dependency_invalid_argument"
	case errors.Is(cause, lookupservice.ErrUnavailable):
		return "lookup_unavailable"
	case errors.Is(cause, searchjobs.ErrKnowledgeUnavailable):
		return "knowledge_contract_invalid"
	default:
		if _, ok := errors.AsType[*plan.Diagnostic](cause); ok {
			return "spl_compilation"
		}
		if _, ok := errors.AsType[*sqlite.Error](cause); ok {
			return "sqlite_error"
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
