// Package searchjobproto projects search-job metadata and typed results into
// the protobuf representation shared by HTTP and live-search APIs.
package searchjobproto

import (
	"errors"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ResultShape describes a completed relation's public columns: the result kind
// clients render, plus whether the columns after the first are named from
// runtime field values rather than from field identities.
type ResultShape struct {
	Kind opensplunk.ResultSetKind
	// RuntimeNamedColumns marks the wide relations — timechart's series and
	// chart's pivot columns — whose public names after the first come from
	// data. Such a column carries no event-metadata meaning even when its name
	// happens to equal one (a chart split value of "host" is an occurrence
	// count, not a host), so it is published with a metric semantic type.
	RuntimeNamedColumns bool
}

// State projects an internal lifecycle state to its wire representation.
// Values unknown to this binary fail closed as UNSPECIFIED so callers can
// reject snapshots whose state cannot be represented safely.
func State(state searchjobs.State) opensplunk.SearchJobState {
	switch state {
	case searchjobs.StateQueued:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED
	case searchjobs.StateParsing:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_PARSING
	case searchjobs.StatePlanning:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_PLANNING
	case searchjobs.StateRunning:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING
	case searchjobs.StateCompleted:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED
	case searchjobs.StateFailed:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED
	case searchjobs.StateCanceled:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED
	case searchjobs.StateExpired:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED
	default:
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED
	}
}

// Failure projects the complete shared HTTP and WebSocket failure surface.
func Failure(failure searchjobs.Failure) *opensplunk.SearchFailure {
	return &opensplunk.SearchFailure{
		Code:        FailureCode(failure.Code),
		Message:     failure.Message,
		Retryable:   failure.Retryable,
		Diagnostics: Diagnostics(failure.Diagnostics),
	}
}

// FailureCode projects an internal failure code to its wire representation.
// Values unknown to this binary remain UNSPECIFIED for caller-side validation.
func FailureCode(code searchjobs.FailureCode) opensplunk.SearchFailureCode {
	switch code {
	case searchjobs.FailureInvalidSPL:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL
	case searchjobs.FailureUnsupportedSPL:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_UNSUPPORTED_SPL
	case searchjobs.FailureInvalidTimeRange:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_TIME_RANGE
	case searchjobs.FailureIndexForbidden:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INDEX_FORBIDDEN
	case searchjobs.FailureResourceLimit:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_RESOURCE_LIMIT
	case searchjobs.FailureTimeout:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_TIMEOUT
	case searchjobs.FailureStorageUnavailable:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE
	case searchjobs.FailureExecution:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION
	case searchjobs.FailureInternal:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL
	default:
		return opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED
	}
}

// ResultShapeForSPL classifies the result shape from the final transforming
// command while preserving event semantics for non-transforming pipelines.
func ResultShapeForSPL(source string) ResultShape {
	query, err := spl.Parse(source)
	if err != nil {
		return ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED}
	}
	classified := spl.ClassifyResultShape(query)
	switch classified.Kind {
	case spl.ResultKindEvents:
		return ResultShape{
			Kind:                opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS,
			RuntimeNamedColumns: classified.RuntimeNamedColumns,
		}
	case spl.ResultKindStatistics:
		return ResultShape{
			Kind:                opensplunk.ResultSetKind_RESULT_SET_KIND_STATISTICS,
			RuntimeNamedColumns: classified.RuntimeNamedColumns,
		}
	case spl.ResultKindTimeSeries:
		return ResultShape{
			Kind:                opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES,
			RuntimeNamedColumns: classified.RuntimeNamedColumns,
		}
	default:
		return ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED}
	}
}

// Progress projects the authoritative counters and timing shared by HTTP and
// WebSocket search representations. Scan counters are exact reported work;
// no matched-event estimate or completion percentage is inferred.
func Progress(job searchjobs.Job, now time.Time) (*opensplunk.SearchProgress, error) {
	updatedAt := now.Round(0).UTC()
	if !job.FinishedAt.IsZero() {
		updatedAt = job.FinishedAt.Round(0).UTC()
	}
	updated := timestamppb.New(updatedAt)
	if err := updated.CheckValid(); err != nil {
		return nil, errors.New("invalid search-job progress timestamp")
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && updatedAt.After(job.StartedAt) {
		elapsed = updatedAt.Sub(job.StartedAt)
	}
	queueWait := time.Duration(0)
	if !job.StartedAt.IsZero() && job.StartedAt.After(job.CreatedAt) {
		queueWait = job.StartedAt.Sub(job.CreatedAt)
	}
	return &opensplunk.SearchProgress{
		StateVersion: job.Version,
		Phase:        executionPhase(job.State),
		ScannedRows:  job.ScannedRows,
		ScannedBytes: job.ScannedBytes,
		ProducedRows: job.RowCount,
		ResultBytes:  job.ResultBytes,
		Elapsed:      durationpb.New(elapsed),
		QueueWait:    durationpb.New(queueWait),
		UpdatedAt:    updated,
	}, nil
}

func executionPhase(state searchjobs.State) opensplunk.SearchExecutionPhase {
	switch state {
	case searchjobs.StateQueued:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_WAITING_FOR_SLOT
	case searchjobs.StateParsing:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_PARSING
	case searchjobs.StatePlanning:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_OPTIMIZING
	case searchjobs.StateRunning:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING
	case searchjobs.StateCompleted, searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE
	default:
		return opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_UNSPECIFIED
	}
}

// TimeRange preserves reusable time intent while returning its effective
// timezone for the separately resolved execution interval.
func TimeRange(job searchjobs.Job) (*opensplunk.TimeRangeSpec, string, error) {
	intent := job.TimeRange
	if intent == (searchtime.Intent{}) {
		earliest := job.Earliest.UTC().Format(time.RFC3339Nano)
		latest := job.Latest.UTC().Format(time.RFC3339Nano)
		timezone := "UTC"
		return &opensplunk.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		}, timezone, nil
	}
	if err := searchtime.ValidateIntent(intent); err != nil {
		return nil, "", errors.New("invalid search-job time-range intent")
	}
	result := &opensplunk.TimeRangeSpec{
		Earliest: new(intent.Earliest),
		Latest:   new(intent.Latest),
	}
	if intent.TimezoneSpecified {
		result.Timezone = new(intent.Timezone)
	}
	return result, intent.Timezone, nil
}

// Source maps normalized internal provenance to its mutually exclusive wire
// representation. A zero value remains a compatibility alias for ad hoc.
func Source(source searchjobs.JobSource) (*opensplunk.SearchJobSource, error) {
	source, err := searchjobs.CanonicalJobSource(source)
	if err != nil {
		return nil, errors.New("invalid search-job source metadata")
	}
	result := &opensplunk.SearchJobSource{}
	switch source.Origin {
	case searchjobs.JobOriginAdHoc:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC
	case searchjobs.JobOriginSavedSearch:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
		result.SavedSearchId = new(source.ObjectID)
	case searchjobs.JobOriginHistoryRerun:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN
		result.HistorySearchId = new(source.ObjectID)
	case searchjobs.JobOriginDashboard:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD
		result.DashboardId = new(source.ObjectID)
	case searchjobs.JobOriginAPI:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_API
	case searchjobs.JobOriginScheduledReport:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SCHEDULED_REPORT
		result.ScheduledReportRunId = new(source.ObjectID)
		result.ScheduledAt = timestamppb.New(source.ScheduledAt)
	case searchjobs.JobOriginAlert:
		result.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_ALERT
		result.AlertId = new(source.AlertID)
		result.AlertRunId = new(source.AlertRunID)
		result.ScheduledAt = timestamppb.New(source.ScheduledAt)
	default:
		return nil, errors.New("invalid search-job source origin")
	}
	return result, nil
}
