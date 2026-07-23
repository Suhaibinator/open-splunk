package searchws

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type targetProjection struct {
	version            uint64
	incarnation        time.Time
	previewRows        uint32
	invalidatesPreview bool
	terminal           bool
	refreshAt          time.Time
	events             []*opensplunkv1.SearchWebSocketEvent
}

func projectSearch(job searchjobs.Job, now time.Time) (targetProjection, error) {
	return projectSearchWithPreview(context.Background(), job, nil, 0, now)
}

func projectSearchWithPreview(ctx context.Context, job searchjobs.Job, preview *searchjobs.PreviewSnapshot, requestedPreviewRows uint32, now time.Time) (targetProjection, error) {
	if ctx == nil {
		return targetProjection{}, errors.New("search websocket projection: context is required")
	}
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
	progress, err := searchjobproto.Progress(job, now)
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
		schema, schemaErr := schemaToProto(job.ID, *job.Schema, searchjobproto.ResultKindForSPL(job.SPL))
		if schemaErr != nil {
			return targetProjection{}, schemaErr
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_ResultSchemaAvailable{
			ResultSchemaAvailable: &opensplunkv1.ResultSchemaAvailable{SearchJobId: job.ID, Schema: schema},
		}})
	}
	if preview != nil {
		if preview.Revision == 0 || preview.Job.ID != job.ID || preview.Job.Version != job.Version || job.Schema == nil {
			return targetProjection{}, errors.New("search websocket projection: preview snapshot is inconsistent")
		}
		rows, rowsErr := searchjobproto.Rows(ctx, job.ID, *job.Schema, preview.Rows, int(requestedPreviewRows))
		if rowsErr != nil {
			return targetProjection{}, rowsErr
		}
		events = append(events, &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_ResultPreview{
			ResultPreview: &opensplunkv1.ResultPreview{
				SearchJobId: job.ID, SchemaId: job.ID, PreviewRevision: preview.Revision,
				UpdateMode: opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET,
				Rows:       rows, Truncated: preview.Truncated,
			},
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
	invalidatesPreview := job.State == searchjobs.StateFailed ||
		job.State == searchjobs.StateCanceled || job.State == searchjobs.StateExpired
	return targetProjection{
		version: job.Version, incarnation: canonicalTime(job.CreatedAt), previewRows: requestedPreviewRows,
		invalidatesPreview: invalidatesPreview, terminal: terminal, refreshAt: refreshAt, events: events,
	}, nil
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
	return searchjobproto.Schema(id, schema, resultKind)
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
