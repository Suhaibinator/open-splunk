package server

import (
	"context"
	"errors"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func searchJobToProto(job searchjobs.Job, now time.Time) (*opensplunkv1.SearchJob, error) {
	resultKind := resultKindForSPL(job.SPL)
	earliest, err := validTimestamp(job.Earliest)
	if err != nil {
		return nil, err
	}
	latest, err := validTimestamp(job.Latest)
	if err != nil {
		return nil, err
	}
	indexTimeCutoff, err := validTimestamp(job.IndexTimeCutoff)
	if err != nil {
		return nil, err
	}
	createdAt, err := validTimestamp(job.CreatedAt)
	if err != nil {
		return nil, err
	}
	timeRange, timezone, err := searchjobproto.TimeRange(job)
	if err != nil {
		return nil, errors.New("search job contains invalid time-range intent")
	}
	source, err := searchjobproto.Source(job.Source)
	if err != nil {
		return nil, errors.New("search job contains invalid source metadata")
	}
	progress, err := searchjobproto.Progress(job, now)
	if err != nil {
		return nil, errors.New("search job contains invalid progress metadata")
	}
	if err := validateBoundedIdentifier(job.AppID, maximumSavedSearchAppIDBytes, true); err != nil {
		return nil, errors.New("search job contains an invalid app ID")
	}
	definition := &opensplunkv1.SearchDefinition{
		Spl:        job.SPL,
		TimeRange:  timeRange,
		IndexScope: slices.Clone(job.RequestedIndexes),
	}
	if job.AppID != "" {
		definition.AppId = stringPointer(job.AppID)
	}
	result := &opensplunkv1.SearchJob{
		SearchJobId:         job.ID,
		StateVersion:        job.Version,
		Definition:          definition,
		Source:              source,
		NormalizedSpl:       optionalString(job.NormalizedSPL),
		EffectiveIndexScope: slices.Clone(job.EffectiveIndexes),
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: earliest,
			Latest:   latest,
			Timezone: timezone,
		},
		IndexTimeCutoff:  indexTimeCutoff,
		State:            searchStateToProto(job.State),
		ResultKind:       resultKind,
		ResultsTruncated: job.ResultsTruncated,
		Progress:         progress,
		CreatedAt:        createdAt,
	}
	if job.Schema != nil {
		result.ResultSchema, err = schemaToProto(job.ID, *job.Schema, resultKind)
		if err != nil {
			return nil, err
		}
	}
	if job.Failure != nil {
		result.Failure = failureToProto(*job.Failure)
		result.Diagnostics = diagnosticsToProto(job.Failure.Diagnostics)
	}
	if job.ResultsTruncated {
		occurredAt := job.FinishedAt
		if occurredAt.IsZero() {
			occurredAt = now.Round(0).UTC()
		}
		warningTime, timestampErr := validTimestamp(occurredAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
		result.Warnings = append(result.Warnings, &opensplunkv1.ApiWarning{
			Code:       "RESULTS_TRUNCATED",
			Message:    "Retained search results reached the server row boundary; a bounded export can re-execute the same scoped query.",
			OccurredAt: warningTime,
		})
	}
	if !job.StartedAt.IsZero() {
		result.StartedAt, err = validTimestamp(job.StartedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.FinishedAt.IsZero() {
		result.FinishedAt, err = validTimestamp(job.FinishedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(job.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func resultPageToProto(ctx context.Context, jobID string, page searchjobs.ResultPage, resultKind opensplunkv1.ResultSetKind, includeTotal, resultsTruncated bool) (*opensplunkv1.ResultPage, error) {
	return searchjobproto.ResultPage(ctx, jobID, page, resultKind, includeTotal, resultsTruncated)
}

func schemaToProto(schemaID string, schema searchjobs.Schema, resultKind opensplunkv1.ResultSetKind) (*opensplunkv1.ResultSchema, error) {
	return searchjobproto.Schema(schemaID, schema, resultKind)
}

func resultKindForSPL(source string) opensplunkv1.ResultSetKind {
	return searchjobproto.ResultKindForSPL(source)
}

func valueToProto(ctx context.Context, value searchjobs.Value) (*opensplunkv1.TypedValue, error) {
	return searchjobproto.Value(ctx, value)
}

func valueKindToProto(kind searchjobs.ValueKind) (opensplunkv1.ValueType, error) {
	return searchjobproto.ValueKind(kind)
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

func failureToProto(failure searchjobs.Failure) *opensplunkv1.SearchFailure {
	return &opensplunkv1.SearchFailure{
		Code:        failureCodeToProto(failure.Code),
		Message:     failure.Message,
		Retryable:   failure.Retryable,
		Diagnostics: diagnosticsToProto(failure.Diagnostics),
	}
}

func failureCodeToProto(code searchjobs.FailureCode) opensplunkv1.SearchFailureCode {
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

func diagnosticsToProto(diagnostics []searchjobs.Diagnostic) []*opensplunkv1.Diagnostic {
	result := make([]*opensplunkv1.Diagnostic, len(diagnostics))
	for index, diagnostic := range diagnostics {
		converted := &opensplunkv1.Diagnostic{
			Code:        diagnostic.Code,
			Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:     diagnostic.Message,
			Suggestions: slices.Clone(diagnostic.Suggestions),
		}
		if diagnostic.Line > 0 || diagnostic.Column > 0 || diagnostic.EndLine > 0 || diagnostic.EndColumn > 0 {
			converted.SourceRange = &opensplunkv1.SourceRange{
				Start: &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.Line), Column: nonnegativeUint32(diagnostic.Column)},
				End:   &opensplunkv1.SourcePosition{Line: nonnegativeUint32(diagnostic.EndLine), Column: nonnegativeUint32(diagnostic.EndColumn)},
			}
		}
		result[index] = converted
	}
	return result
}

func indexSummaryToProto(index control.Index) *opensplunkv1.IndexSummary {
	return &opensplunkv1.IndexSummary{
		IndexId:         index.ID,
		Name:            index.Definition.Name,
		DisplayName:     index.Definition.DisplayName,
		State:           indexStateToProto(index.State),
		IngestionAccess: accessState(index.Definition.IngestionEnabled),
		SearchAccess:    accessState(index.Definition.SearchEnabled),
	}
}

func indexStateToProto(state control.IndexState) opensplunkv1.IndexState {
	switch state {
	case control.IndexStateActive:
		return opensplunkv1.IndexState_INDEX_STATE_ACTIVE
	case control.IndexStateArchived:
		return opensplunkv1.IndexState_INDEX_STATE_ARCHIVED
	case control.IndexStateDeleting:
		return opensplunkv1.IndexState_INDEX_STATE_DELETING
	default:
		return opensplunkv1.IndexState_INDEX_STATE_UNSPECIFIED
	}
}

func accessState(enabled bool) opensplunkv1.IndexAccessState {
	if enabled {
		return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED
	}
	return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_DISABLED
}

func validTimestamp(input time.Time) (*timestamppb.Timestamp, error) {
	if input.IsZero() {
		return nil, errors.New("required timestamp is zero")
	}
	return timestampToProto(input)
}

// timestampToProto accepts the protobuf minimum instant. In Go that instant is
// time.Time's zero value, which is invalid only for required metadata fields,
// not for a typed search-result cell.
func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(input.Round(0).UTC())
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("timestamp is outside protobuf range")
	}
	return result, nil
}

func cloneApps(input []*opensplunkv1.AppSummary) []*opensplunkv1.AppSummary {
	result := make([]*opensplunkv1.AppSummary, len(input))
	for index, app := range input {
		result[index] = proto.Clone(app).(*opensplunkv1.AppSummary)
	}
	return result
}

func appExists(apps []*opensplunkv1.AppSummary, id string) bool {
	for _, app := range apps {
		if app.GetAppId() == id {
			return true
		}
	}
	return false
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return stringPointer(value)
}

func stringPointer(value string) *string { return &value }

func uint64Pointer(value uint64) *uint64 { return &value }

func nonnegativeUint32(value int) uint32 {
	if value <= 0 {
		return 0
	}
	return uint32(value)
}

func identitySanitizer[T proto.Message](request T) (T, error) { return request, nil }
