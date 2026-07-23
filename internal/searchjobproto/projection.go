// Package searchjobproto projects search-job metadata and typed results into
// the protobuf representation shared by HTTP and live-search APIs.
package searchjobproto

import (
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ResultKindForSPL classifies the result shape from the final transforming
// command while preserving event semantics for non-transforming pipelines.
func ResultKindForSPL(source string) opensplunkv1.ResultSetKind {
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

// Progress projects the authoritative counters and timing shared by HTTP and
// WebSocket search representations. Scan counters are exact reported work;
// no matched-event estimate or completion percentage is inferred.
func Progress(job searchjobs.Job, now time.Time) (*opensplunkv1.SearchProgress, error) {
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
	return &opensplunkv1.SearchProgress{
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

func executionPhase(state searchjobs.State) opensplunkv1.SearchExecutionPhase {
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

// TimeRange preserves reusable time intent while returning its effective
// timezone for the separately resolved execution interval.
func TimeRange(job searchjobs.Job) (*opensplunkv1.TimeRangeSpec, string, error) {
	intent := job.TimeRange
	if intent == (searchtime.Intent{}) {
		earliest := job.Earliest.UTC().Format(time.RFC3339Nano)
		latest := job.Latest.UTC().Format(time.RFC3339Nano)
		timezone := "UTC"
		return &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		}, timezone, nil
	}
	if err := searchtime.ValidateIntent(intent); err != nil {
		return nil, "", errors.New("invalid search-job time-range intent")
	}
	result := &opensplunkv1.TimeRangeSpec{
		Earliest: stringPointer(intent.Earliest),
		Latest:   stringPointer(intent.Latest),
	}
	if intent.TimezoneSpecified {
		result.Timezone = stringPointer(intent.Timezone)
	}
	return result, intent.Timezone, nil
}

// Source maps normalized internal provenance to its mutually exclusive wire
// representation. A zero value remains a compatibility alias for ad hoc.
func Source(source searchjobs.JobSource) (*opensplunkv1.SearchJobSource, error) {
	source, err := searchjobs.CanonicalJobSource(source)
	if err != nil {
		return nil, errors.New("invalid search-job source metadata")
	}
	result := &opensplunkv1.SearchJobSource{}
	switch source.Origin {
	case searchjobs.JobOriginAdHoc:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC
	case searchjobs.JobOriginSavedSearch:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
		result.SavedSearchId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginHistoryRerun:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN
		result.HistorySearchId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginDashboard:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD
		result.DashboardId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginAPI:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_API
	default:
		return nil, errors.New("invalid search-job source origin")
	}
	return result, nil
}

func stringPointer(value string) *string { return &value }
