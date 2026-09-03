package server

import (
	"errors"
	"slices"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func searchJobToProto(job searchjobs.Job, now time.Time) (*opensplunk.SearchJob, error) {
	knowledgeSnapshot, err := projectKnowledgeSnapshotSummary(job.KnowledgeSnapshot)
	if err != nil {
		return nil, err
	}
	resultShape := searchjobproto.ResultShapeForSPL(job.SPL)
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
	definition := &opensplunk.SearchDefinition{
		Spl:        job.SPL,
		TimeRange:  timeRange,
		IndexScope: slices.Clone(job.RequestedIndexes),
	}
	if job.AppID != "" {
		definition.AppId = new(job.AppID)
	}
	result := &opensplunk.SearchJob{
		SearchJobId:         job.ID,
		StateVersion:        job.Version,
		Definition:          definition,
		Source:              source,
		NormalizedSpl:       optionalString(job.NormalizedSPL),
		EffectiveIndexScope: slices.Clone(job.EffectiveIndexes),
		ResolvedTimeRange: &opensplunk.ResolvedTimeRange{
			Earliest: earliest,
			Latest:   latest,
			Timezone: timezone,
		},
		IndexTimeCutoff:   indexTimeCutoff,
		State:             searchjobproto.State(job.State),
		ResultKind:        resultShape.Kind,
		ResultsTruncated:  job.ResultsTruncated,
		Progress:          progress,
		CreatedAt:         createdAt,
		KnowledgeSnapshot: knowledgeSnapshot,
	}
	if job.Schema != nil {
		result.ResultSchema, err = searchjobproto.Schema(job.ID, *job.Schema, resultShape)
		if err != nil {
			return nil, err
		}
	}
	if job.Failure != nil {
		result.Failure = searchjobproto.Failure(*job.Failure)
		result.Diagnostics = searchjobproto.Diagnostics(job.Failure.Diagnostics)
	}
	if job.Failure != nil && job.Failure.Code == searchjobs.FailureResultsNotPersisted {
		// The search itself succeeded; surface the discarded artifact as a
		// stable-code notice so clients can distinguish it from an executor
		// failure and from a job that is still finalizing.
		occurredAt := job.FinishedAt
		if occurredAt.IsZero() {
			occurredAt = now.Round(0).UTC()
		}
		warningTime, timestampErr := validTimestamp(occurredAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
		result.Warnings = append(result.Warnings, &opensplunk.ApiWarning{
			Code: "RESULTS_NOT_PERSISTED",
			// Projected from the constant message set, never from stored text,
			// so a future producer cannot echo a raw error to a client.
			Message:    searchjobs.SafeResultsNotPersistedMessage(*job.Failure),
			OccurredAt: warningTime,
		})
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
		result.Warnings = append(result.Warnings, &opensplunk.ApiWarning{
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

func indexSummaryToProto(index control.Index) *opensplunk.IndexSummary {
	return &opensplunk.IndexSummary{
		IndexId:         index.ID,
		Name:            index.Definition.Name,
		DisplayName:     index.Definition.DisplayName,
		State:           indexStateToProto(index.State),
		IngestionAccess: accessState(index.Definition.IngestionEnabled),
		SearchAccess:    accessState(index.Definition.SearchEnabled),
	}
}

func indexStateToProto(state control.IndexState) opensplunk.IndexState {
	switch state {
	case control.IndexStateActive:
		return opensplunk.IndexState_INDEX_STATE_ACTIVE
	case control.IndexStateArchived:
		return opensplunk.IndexState_INDEX_STATE_ARCHIVED
	case control.IndexStateDeleting:
		return opensplunk.IndexState_INDEX_STATE_DELETING
	default:
		return opensplunk.IndexState_INDEX_STATE_UNSPECIFIED
	}
}

func accessState(enabled bool) opensplunk.IndexAccessState {
	if enabled {
		return opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED
	}
	return opensplunk.IndexAccessState_INDEX_ACCESS_STATE_DISABLED
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

func cloneApps(input []*opensplunk.AppSummary) []*opensplunk.AppSummary {
	result := make([]*opensplunk.AppSummary, len(input))
	for index, app := range input {
		result[index] = proto.Clone(app).(*opensplunk.AppSummary)
	}
	return result
}

func appExists(apps []*opensplunk.AppSummary, id string) bool {
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
	return new(value)
}
