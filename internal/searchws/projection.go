package searchws

import (
	"context"
	"errors"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/exportjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type targetProjection struct {
	version                uint64
	incarnation            time.Time
	previewRows            uint32
	invalidatesPreview     bool
	invalidatesArtifact    bool
	terminal               bool
	refreshAt              time.Time
	revalidateUntilRemoved bool
	events                 []*opensplunk.SearchWebSocketEvent
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
	state := searchjobproto.State(job.State)
	if state == opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED {
		return targetProjection{}, errors.New("search websocket projection: search state is invalid")
	}
	progress, err := searchjobproto.Progress(job, now)
	if err != nil {
		return targetProjection{}, err
	}
	events := []*opensplunk.SearchWebSocketEvent{
		{Payload: &opensplunk.SearchWebSocketEvent_SearchStateChanged{SearchStateChanged: &opensplunk.SearchJobStateChanged{
			SearchJobId: job.ID, State: state, StateVersion: job.Version,
		}}},
		{Payload: &opensplunk.SearchWebSocketEvent_SearchProgress{SearchProgress: progress}},
	}
	if job.Schema != nil {
		schema, schemaErr := schemaToProto(job.ID, *job.Schema, searchjobproto.ResultShapeForSPL(job.SPL))
		if schemaErr != nil {
			return targetProjection{}, schemaErr
		}
		events = append(events, &opensplunk.SearchWebSocketEvent{Payload: &opensplunk.SearchWebSocketEvent_ResultSchemaAvailable{
			ResultSchemaAvailable: &opensplunk.ResultSchemaAvailable{SearchJobId: job.ID, Schema: schema},
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
		events = append(events, &opensplunk.SearchWebSocketEvent{Payload: &opensplunk.SearchWebSocketEvent_ResultPreview{
			ResultPreview: &opensplunk.ResultPreview{
				SearchJobId: job.ID, SchemaId: job.ID, PreviewRevision: preview.Revision,
				UpdateMode: opensplunk.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET,
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
		events = append(events, &opensplunk.SearchWebSocketEvent{Payload: &opensplunk.SearchWebSocketEvent_Warning{
			Warning: &opensplunk.SearchWebSocketWarning{Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: job.ID}}, Warning: &opensplunk.ApiWarning{
				Code:       "RESULTS_TRUNCATED",
				Message:    "Retained search results reached the server row boundary; a bounded export can re-execute the same scoped query.",
				OccurredAt: warningTime,
			}},
		}})
	}
	terminal := job.State.Terminal()
	if terminal {
		terminalEvent := &opensplunk.SearchJobTerminal{
			SearchJobId: job.ID, State: state, StateVersion: job.Version,
			FinalProgress: proto.Clone(progress).(*opensplunk.SearchProgress),
		}
		if job.Failure != nil {
			terminalEvent.Failure = searchjobproto.Failure(*job.Failure)
			if terminalEvent.Failure.GetCode() == opensplunk.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED {
				return targetProjection{}, errors.New("search websocket projection: search failure code is invalid")
			}
		}
		if !job.ExpiresAt.IsZero() {
			terminalEvent.ResultsExpireAt, err = timestampToProto(job.ExpiresAt)
			if err != nil {
				return targetProjection{}, err
			}
		}
		events = append(events, &opensplunk.SearchWebSocketEvent{Payload: &opensplunk.SearchWebSocketEvent_SearchTerminal{
			SearchTerminal: terminalEvent,
		}})
	}
	refreshAt, revalidateUntilRemoved := terminalRefreshSchedule(terminal, job.ExpiresAt, now)
	invalidatesPreview := job.State == searchjobs.StateFailed ||
		job.State == searchjobs.StateCanceled || job.State == searchjobs.StateExpired
	return targetProjection{
		version: job.Version, incarnation: canonicalTime(job.CreatedAt), previewRows: requestedPreviewRows,
		invalidatesPreview: invalidatesPreview, terminal: terminal, refreshAt: refreshAt,
		revalidateUntilRemoved: revalidateUntilRemoved, events: events,
	}, nil
}

func projectExport(job exportjobs.Job, now time.Time) (targetProjection, error) {
	if job.ID == "" || job.Version == 0 || job.CreatedAt.IsZero() {
		return targetProjection{}, errors.New("search websocket projection: export snapshot is incomplete")
	}
	if _, err := timestampToProto(job.CreatedAt); err != nil {
		return targetProjection{}, err
	}
	state := exportjobproto.State(job.State)
	if state == opensplunk.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED {
		return targetProjection{}, errors.New("search websocket projection: export state is invalid")
	}
	progress, err := exportProgressToProto(job, now)
	if err != nil {
		return targetProjection{}, err
	}
	events := []*opensplunk.SearchWebSocketEvent{
		{Payload: &opensplunk.SearchWebSocketEvent_ExportStateChanged{ExportStateChanged: &opensplunk.ExportJobStateChanged{
			ExportJobId: job.ID, State: state, StateVersion: job.Version,
		}}},
		{Payload: &opensplunk.SearchWebSocketEvent_ExportProgress{ExportProgress: progress}},
	}
	terminal := job.State == exportjobs.StateCompleted || job.State == exportjobs.StateFailed ||
		job.State == exportjobs.StateCanceled || job.State == exportjobs.StateExpired
	if terminal {
		terminalEvent := &opensplunk.ExportJobTerminal{
			ExportJobId: job.ID, State: state, StateVersion: job.Version,
			FinalProgress: proto.Clone(progress).(*opensplunk.ExportProgress),
		}
		if job.Failure != nil {
			failureCode := exportjobproto.FailureCode(job.Failure.Code)
			if failureCode == opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_UNSPECIFIED {
				return targetProjection{}, errors.New("search websocket projection: export failure code is invalid")
			}
			terminalEvent.Failure = &opensplunk.ExportFailure{
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
			terminalEvent.Artifact = &opensplunk.ExportArtifact{
				FileName: job.Artifact.FileName, MediaType: job.Artifact.MediaType, SizeBytes: job.Artifact.SizeBytes,
				RowCount: job.Artifact.RowCount, ExpiresAt: expiresAt,
			}
		}
		events = append(events, &opensplunk.SearchWebSocketEvent{Payload: &opensplunk.SearchWebSocketEvent_ExportTerminal{
			ExportTerminal: terminalEvent,
		}})
	}
	refreshAt, revalidateUntilRemoved := terminalRefreshSchedule(terminal, job.ExpiresAt, now)
	return targetProjection{
		version: job.Version, incarnation: canonicalTime(job.CreatedAt),
		invalidatesArtifact: job.State == exportjobs.StateExpired, terminal: terminal,
		refreshAt: refreshAt, revalidateUntilRemoved: revalidateUntilRemoved, events: events,
	}, nil
}

func terminalRefreshSchedule(terminal bool, expiresAt, now time.Time) (time.Time, bool) {
	if !terminal || expiresAt.IsZero() {
		return time.Time{}, false
	}
	if expiresAt.After(now) {
		return canonicalTime(expiresAt), false
	}
	return time.Time{}, true
}

func exportProgressToProto(job exportjobs.Job, now time.Time) (*opensplunk.ExportProgress, error) {
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
	return &opensplunk.ExportProgress{
		RowsWritten: job.Progress.RowsWritten, BytesWritten: job.Progress.BytesWritten,
		Elapsed: durationpb.New(elapsed), UpdatedAt: updated,
	}, nil
}

func schemaToProto(id string, schema searchjobs.Schema, shape searchjobproto.ResultShape) (*opensplunk.ResultSchema, error) {
	return searchjobproto.Schema(id, schema, shape)
}

func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(canonicalTime(input))
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("search websocket projection: timestamp is outside protobuf range")
	}
	return result, nil
}

func canonicalTime(input time.Time) time.Time { return input.Round(0).UTC() }
