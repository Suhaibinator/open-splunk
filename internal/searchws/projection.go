package searchws

import (
	"errors"
	"math"
	"slices"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type targetProjection struct {
	version     uint64
	incarnation time.Time
	terminal    bool
	refreshAt   time.Time
	events      []*opensplunkv1.SearchWebSocketEvent
}

func projectSearch(job searchjobs.Job, now time.Time) (targetProjection, error) {
	if job.ID == "" || job.Version == 0 || job.CreatedAt.IsZero() {
		return targetProjection{}, errors.New("search websocket projection: search snapshot is incomplete")
	}
	if _, err := timestampToProto(job.CreatedAt); err != nil {
		return targetProjection{}, err
	}
	state := searchStateToProto(job.State)
	if state == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED {
		return targetProjection{}, errors.New("search websocket projection: search state is invalid")
	}
	progress, err := searchProgressToProto(job, now)
	if err != nil {
		return targetProjection{}, err
	}
	events := []*opensplunkv1.SearchWebSocketEvent{
		{Payload: &opensplunkv1.SearchWebSocketEvent_SearchStateChanged{SearchStateChanged: &opensplunkv1.SearchJobStateChanged{
			SearchJobId: job.ID, State: state, StateVersion: job.Version,
		}}},
		{Payload: &opensplunkv1.SearchWebSocketEvent_SearchProgress{SearchProgress: progress}},
	}
	if job.Schema != nil {
		schema, schemaErr := schemaToProto(job.ID, *job.Schema, resultKindForSPL(job.SPL))
		if schemaErr != nil {
			return targetProjection{}, schemaErr
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_ResultSchemaAvailable{
			ResultSchemaAvailable: &opensplunkv1.ResultSchemaAvailable{SearchJobId: job.ID, Schema: schema},
		}})
	}
	if job.ResultsTruncated {
		occurredAt := job.FinishedAt
		if occurredAt.IsZero() {
			occurredAt = now
		}
		warningTime, timestampErr := timestampToProto(occurredAt)
		if timestampErr != nil {
			return targetProjection{}, timestampErr
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_Warning{
			Warning: &opensplunkv1.SearchWebSocketWarning{Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: job.ID}}, Warning: &opensplunkv1.ApiWarning{
				Code:       "RESULTS_TRUNCATED",
				Message:    "Retained search results reached the server row boundary; a bounded export can re-execute the same scoped query.",
				OccurredAt: warningTime,
			}},
		}})
	}
	terminal := job.State.Terminal()
	if terminal {
		terminalEvent := &opensplunkv1.SearchJobTerminal{
			SearchJobId: job.ID, State: state, StateVersion: job.Version,
			FinalProgress: proto.Clone(progress).(*opensplunkv1.SearchProgress),
		}
		if job.Failure != nil {
			terminalEvent.Failure = searchFailureToProto(*job.Failure)
			if terminalEvent.Failure.GetCode() == opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED {
				return targetProjection{}, errors.New("search websocket projection: search failure code is invalid")
			}
		}
		if !job.ExpiresAt.IsZero() {
			terminalEvent.ResultsExpireAt, err = timestampToProto(job.ExpiresAt)
			if err != nil {
				return targetProjection{}, err
			}
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_SearchTerminal{
			SearchTerminal: terminalEvent,
		}})
	}
	refreshAt := time.Time{}
	if terminal && !job.ExpiresAt.IsZero() && job.ExpiresAt.After(now) {
		refreshAt = canonicalTime(job.ExpiresAt)
	}
	return targetProjection{version: job.Version, incarnation: canonicalTime(job.CreatedAt), terminal: terminal, refreshAt: refreshAt, events: events}, nil
}

func projectExport(job exportjobs.Job, now time.Time) (targetProjection, error) {
	if job.ID == "" || job.Version == 0 || job.CreatedAt.IsZero() {
		return targetProjection{}, errors.New("search websocket projection: export snapshot is incomplete")
	}
	if _, err := timestampToProto(job.CreatedAt); err != nil {
		return targetProjection{}, err
	}
	state := exportStateToProto(job.State)
	if state == opensplunkv1.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED {
		return targetProjection{}, errors.New("search websocket projection: export state is invalid")
	}
	progress, err := exportProgressToProto(job, now)
	if err != nil {
		return targetProjection{}, err
	}
	events := []*opensplunkv1.SearchWebSocketEvent{
		{Payload: &opensplunkv1.SearchWebSocketEvent_ExportStateChanged{ExportStateChanged: &opensplunkv1.ExportJobStateChanged{
			ExportJobId: job.ID, State: state, StateVersion: job.Version,
		}}},
		{Payload: &opensplunkv1.SearchWebSocketEvent_ExportProgress{ExportProgress: progress}},
	}
	terminal := job.State == exportjobs.StateCompleted || job.State == exportjobs.StateFailed ||
		job.State == exportjobs.StateCanceled || job.State == exportjobs.StateExpired
	if terminal {
		terminalEvent := &opensplunkv1.ExportJobTerminal{
			ExportJobId: job.ID, State: state, StateVersion: job.Version,
			FinalProgress: proto.Clone(progress).(*opensplunkv1.ExportProgress),
		}
		if job.Failure != nil {
			failureCode := exportFailureCodeToProto(job.Failure.Code)
			if failureCode == opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_UNSPECIFIED {
				return targetProjection{}, errors.New("search websocket projection: export failure code is invalid")
			}
			terminalEvent.Failure = &opensplunkv1.ExportFailure{
				Code: failureCode, Message: job.Failure.Message, Retryable: job.Failure.Retryable,
			}
		}
		if job.Artifact != nil {
			if job.Artifact.ExpiresAt.IsZero() {
				return targetProjection{}, errors.New("search websocket projection: export artifact expiry is missing")
			}
			expiresAt, timestampErr := timestampToProto(job.Artifact.ExpiresAt)
			if timestampErr != nil {
				return targetProjection{}, timestampErr
			}
			terminalEvent.Artifact = &opensplunkv1.ExportArtifact{
				FileName: job.Artifact.FileName, MediaType: job.Artifact.MediaType, SizeBytes: job.Artifact.SizeBytes,
				RowCount: job.Artifact.RowCount, ExpiresAt: expiresAt,
			}
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_ExportTerminal{
			ExportTerminal: terminalEvent,
		}})
	}
	refreshAt := time.Time{}
	if terminal && !job.ExpiresAt.IsZero() && job.ExpiresAt.After(now) {
		refreshAt = canonicalTime(job.ExpiresAt)
	}
	return targetProjection{version: job.Version, incarnation: canonicalTime(job.CreatedAt), terminal: terminal, refreshAt: refreshAt, events: events}, nil
}

func searchProgressToProto(job searchjobs.Job, now time.Time) (*opensplunkv1.SearchProgress, error) {
	updatedAt := canonicalTime(now)
	if !job.FinishedAt.IsZero() {
		updatedAt = canonicalTime(job.FinishedAt)
	}
	updated, err := timestampToProto(updatedAt)
	if err != nil {
		return nil, err
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && updatedAt.After(job.StartedAt) {
		elapsed = updatedAt.Sub(job.StartedAt)
	}
	queueWait := time.Duration(0)
	if !job.StartedAt.IsZero() && job.StartedAt.After(job.CreatedAt) {
		queueWait = job.StartedAt.Sub(job.CreatedAt)
	}
	return &opensplunkv1.SearchProgress{
		Phase: searchPhaseToProto(job.State), ProducedRows: job.RowCount, ResultBytes: job.ResultBytes,
		Elapsed: durationpb.New(elapsed), QueueWait: durationpb.New(queueWait), UpdatedAt: updated,
	}, nil
}

func exportProgressToProto(job exportjobs.Job, now time.Time) (*opensplunkv1.ExportProgress, error) {
	updatedAt := job.Progress.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = job.CreatedAt
	}
	updated, err := timestampToProto(updatedAt)
	if err != nil {
		return nil, err
	}
	end := canonicalTime(now)
	if !job.FinishedAt.IsZero() {
		end = canonicalTime(job.FinishedAt)
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && end.After(job.StartedAt) {
		elapsed = end.Sub(job.StartedAt)
	}
	return &opensplunkv1.ExportProgress{
		RowsWritten: job.Progress.RowsWritten, BytesWritten: job.Progress.BytesWritten,
		Elapsed: durationpb.New(elapsed), UpdatedAt: updated,
	}, nil
}

func schemaToProto(id string, schema searchjobs.Schema, resultKind opensplunkv1.ResultSetKind) (*opensplunkv1.ResultSchema, error) {
	columns := make([]*opensplunkv1.ResultColumn, len(schema.Columns))
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return nil, errors.New("search websocket projection: schema contains an invalid column")
		}
		if _, exists := seen[column.Name]; exists {
			return nil, errors.New("search websocket projection: schema contains duplicate columns")
		}
		seen[column.Name] = struct{}{}
		kind := valueKindToProto(column.Kind)
		if kind == opensplunkv1.ValueType_VALUE_TYPE_UNSPECIFIED {
			return nil, errors.New("search websocket projection: schema contains an invalid type")
		}
		semantic := semanticType(column.Name)
		if resultKind == opensplunkv1.ResultSetKind_RESULT_SET_KIND_TIME_SERIES && index > 0 {
			semantic = opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_METRIC
		}
		columns[index] = &opensplunkv1.ResultColumn{
			FieldName: column.Name, DisplayName: column.Name, ValueType: kind, SemanticType: semantic,
			Nullable: column.Nullable, Multivalue: column.Multivalue,
		}
	}
	return &opensplunkv1.ResultSchema{SchemaId: id, Revision: 1, ResultKind: resultKind, Columns: columns}, nil
}

func resultKindForSPL(source string) opensplunkv1.ResultSetKind {
	query, err := spl.Parse(source)
	if err != nil {
		return opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED
	}
	result := opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS
	for _, command := range query.Commands {
		switch command.(type) {
		case *spl.TimechartCommand:
			return opensplunkv1.ResultSetKind_RESULT_SET_KIND_TIME_SERIES
		case *spl.TableCommand, *spl.StatsCommand, *spl.TopCommand, *spl.RareCommand:
			result = opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS
		}
	}
	return result
}

func searchStateToProto(state searchjobs.State) opensplunkv1.SearchJobState {
	switch state {
	case searchjobs.StateQueued:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED
	case searchjobs.StateParsing:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PARSING
	case searchjobs.StatePlanning:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PLANNING
	case searchjobs.StateRunning:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING
	case searchjobs.StateCompleted:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED
	case searchjobs.StateFailed:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED
	case searchjobs.StateCanceled:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED
	case searchjobs.StateExpired:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED
	default:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED
	}
}

func searchPhaseToProto(state searchjobs.State) opensplunkv1.SearchExecutionPhase {
	switch state {
	case searchjobs.StateQueued:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_WAITING_FOR_SLOT
	case searchjobs.StateParsing:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_PARSING
	case searchjobs.StatePlanning:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_OPTIMIZING
	case searchjobs.StateRunning:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING
	case searchjobs.StateCompleted, searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE
	default:
		return opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_UNSPECIFIED
	}
}

func exportStateToProto(state exportjobs.State) opensplunkv1.ExportJobState {
	switch state {
	case exportjobs.StateQueued:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_QUEUED
	case exportjobs.StateRunning:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_RUNNING
	case exportjobs.StateCompleted:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED
	case exportjobs.StateFailed:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_FAILED
	case exportjobs.StateCanceled:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED
	case exportjobs.StateExpired:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED
	default:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED
	}
}

func searchFailureToProto(failure searchjobs.Failure) *opensplunkv1.SearchFailure {
	diagnostics := make([]*opensplunkv1.Diagnostic, len(failure.Diagnostics))
	for index, diagnostic := range failure.Diagnostics {
		converted := &opensplunkv1.Diagnostic{
			Code: diagnostic.Code, Severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message: diagnostic.Message, Suggestions: slices.Clone(diagnostic.Suggestions),
		}
		if diagnostic.Line > 0 || diagnostic.Column > 0 || diagnostic.EndLine > 0 || diagnostic.EndColumn > 0 {
			converted.SourceRange = &opensplunkv1.SourceRange{
				Start: &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.Line), Column: nonnegativeUint32(diagnostic.Column)},
				End:   &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.EndLine), Column: nonnegativeUint32(diagnostic.EndColumn)},
			}
		}
		diagnostics[index] = converted
	}
	return &opensplunkv1.SearchFailure{
		Code: searchFailureCodeToProto(failure.Code), Message: failure.Message,
		Retryable: failure.Retryable, Diagnostics: diagnostics,
	}
}

func searchFailureCodeToProto(code searchjobs.FailureCode) opensplunkv1.SearchFailureCode {
	switch code {
	case searchjobs.FailureInvalidSPL:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL
	case searchjobs.FailureUnsupportedSPL:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSUPPORTED_SPL
	case searchjobs.FailureInvalidTimeRange:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_TIME_RANGE
	case searchjobs.FailureIndexForbidden:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INDEX_FORBIDDEN
	case searchjobs.FailureResourceLimit:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_RESOURCE_LIMIT
	case searchjobs.FailureTimeout:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_TIMEOUT
	case searchjobs.FailureStorageUnavailable:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE
	case searchjobs.FailureExecution:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION
	case searchjobs.FailureInternal:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL
	default:
		return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED
	}
}

func exportFailureCodeToProto(code exportjobs.FailureCode) opensplunkv1.ExportFailureCode {
	switch code {
	case exportjobs.FailureRowLimit:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_ROW_LIMIT
	case exportjobs.FailureByteLimit:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_BYTE_LIMIT
	case exportjobs.FailureSourceUnavailable:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_SEARCH_UNAVAILABLE
	case exportjobs.FailureStorageUnavailable:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_STORAGE_UNAVAILABLE
	case exportjobs.FailureInternal:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_INTERNAL
	default:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_UNSPECIFIED
	}
}

func valueKindToProto(kind searchjobs.ValueKind) opensplunkv1.ValueType {
	switch kind {
	case searchjobs.ValueKindNull:
		return opensplunkv1.ValueType_VALUE_TYPE_NULL
	case searchjobs.ValueKindString:
		return opensplunkv1.ValueType_VALUE_TYPE_STRING
	case searchjobs.ValueKindSigned:
		return opensplunkv1.ValueType_VALUE_TYPE_SINT64
	case searchjobs.ValueKindUnsigned:
		return opensplunkv1.ValueType_VALUE_TYPE_UINT64
	case searchjobs.ValueKindDouble:
		return opensplunkv1.ValueType_VALUE_TYPE_DOUBLE
	case searchjobs.ValueKindBool:
		return opensplunkv1.ValueType_VALUE_TYPE_BOOL
	case searchjobs.ValueKindBytes:
		return opensplunkv1.ValueType_VALUE_TYPE_BYTES
	case searchjobs.ValueKindTime:
		return opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP
	case searchjobs.ValueKindDuration:
		return opensplunkv1.ValueType_VALUE_TYPE_DURATION
	case searchjobs.ValueKindList:
		return opensplunkv1.ValueType_VALUE_TYPE_LIST
	case searchjobs.ValueKindObject:
		return opensplunkv1.ValueType_VALUE_TYPE_OBJECT
	case searchjobs.ValueKindDecimal:
		return opensplunkv1.ValueType_VALUE_TYPE_DECIMAL
	case searchjobs.ValueKindMixed:
		return opensplunkv1.ValueType_VALUE_TYPE_MIXED
	default:
		return opensplunkv1.ValueType_VALUE_TYPE_UNSPECIFIED
	}
}

func semanticType(field string) opensplunkv1.ColumnSemanticType {
	switch field {
	case "_time":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_EVENT_TIME
	case "_indextime":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX_TIME
	case "_raw":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_RAW
	case "index":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_INDEX
	case "host":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_HOST
	case "source":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCE
	case "sourcetype":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SOURCETYPE
	case "level":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_LEVEL
	case "message", "body":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_MESSAGE
	case "trace_id":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_TRACE_ID
	case "span_id":
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_SPAN_ID
	default:
		return opensplunkv1.ColumnSemanticType_COLUMN_SEMANTIC_TYPE_UNSPECIFIED
	}
}

func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(canonicalTime(input))
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("search websocket projection: timestamp is outside protobuf range")
	}
	return result, nil
}

func canonicalTime(input time.Time) time.Time { return input.Round(0).UTC() }

func nonnegativeUint32(value int) uint32 {
	if value <= 0 {
		return 0
	}
	if uint64(value) > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}
