package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type nearbyContextSearchJobs interface {
	NearbyContextFor(
		context.Context,
		searchjobs.AccessScope,
		string,
		uint64,
		uint64,
	) (searchjobs.NearbyContext, error)
}

const maximumNearbyContextResponseBytes = 8 << 20

type serializedNearbyContextResponse = boundedProtoResponse[*opensplunk.PrepareNearbyContextResponse]

type serializedNearbyContextCodec = boundedProtoCodec[
	*opensplunk.PrepareNearbyContextRequest,
	*opensplunk.PrepareNearbyContextResponse,
]

func newSerializedNearbyContextCodec() *serializedNearbyContextCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.PrepareNearbyContextRequest,
			*opensplunk.PrepareNearbyContextResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "nearby context serialization state is invalid",
			messageError: "nearby context response is missing",
			maximumBytes: maximumNearbyContextResponseBytes,
			sizeError:    "nearby context response exceeds its byte limit",
		},
	)
}

func (handler *apiHandler) prepareNearbyContext(
	request *http.Request,
	input *opensplunk.PrepareNearbyContextRequest,
) (*serializedNearbyContextResponse, error) {
	generation, err := handler.parseResultSnapshotRef(input.GetSearchJobId(), input.GetSnapshotRef())
	if err != nil {
		return nil, badRequestError("snapshot reference is invalid")
	}
	ordinal, err := parseNearbyRowID(input.GetSearchJobId(), input.GetRowId())
	if err != nil {
		return nil, badRequestError("row ID is invalid")
	}
	ctx := request.Context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if handler.searchArtifacts != nil {
		record, artifactErr := handler.searchArtifacts.Get(
			ctx,
			handler.accessScope(),
			input.GetSearchJobId(),
			searchartifacts.AccessRefresh,
		)
		if artifactErr != nil {
			return nil, mapSearchArtifactError(artifactErr)
		}
		switch record.State {
		case searchartifacts.StateFailed,
			searchartifacts.StateCanceled,
			searchartifacts.StateInterrupted:
			return nil, mapSearchArtifactError(searchartifacts.ErrNotReady)
		}
	}
	var nearby searchjobs.NearbyContext
	if live, ok := handler.jobs.(nearbyContextSearchJobs); ok {
		nearby, err = live.NearbyContextFor(
			ctx,
			handler.accessScope(),
			input.GetSearchJobId(),
			generation,
			ordinal,
		)
		if err == nil {
			return handler.serializedNearbyContext(ctx, input, nearby)
		}
		if !errors.Is(err, searchjobs.ErrNotFound) && !errors.Is(err, searchjobs.ErrExpired) {
			return nil, mapNearbyContextError(err)
		}
	}
	if handler.searchArtifacts == nil {
		if err != nil {
			return nil, mapNearbyContextError(err)
		}
		return nil, unavailableError("retained search service is unavailable")
	}
	nearby, err = handler.durableNearbyContext(
		ctx,
		input.GetSearchJobId(),
		generation,
		ordinal,
	)
	if err != nil {
		return nil, err
	}
	return handler.serializedNearbyContext(ctx, input, nearby)
}

func (handler *apiHandler) serializedNearbyContext(
	ctx context.Context,
	input *opensplunk.PrepareNearbyContextRequest,
	nearby searchjobs.NearbyContext,
) (*serializedNearbyContextResponse, error) {
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("nearby context response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	response, err := nearbyContextToProto(ctx, input, nearby)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedNearbyContextResponse{
		message: response,
		ctx:     ctx,
		release: release,
	}, nil
}

func (handler *apiHandler) durableNearbyContext(
	ctx context.Context,
	jobID string,
	generation uint64,
	ordinal uint64,
) (searchjobs.NearbyContext, error) {
	lease, err := handler.searchArtifacts.Acquire(ctx, handler.accessScope(), jobID)
	if err != nil {
		return searchjobs.NearbyContext{}, mapSearchArtifactError(err)
	}
	defer func() { _ = lease.Close() }()
	if err := ctx.Err(); err != nil {
		return searchjobs.NearbyContext{}, err
	}
	if lease.Generation() != generation || ordinal >= lease.RowCount() {
		return searchjobs.NearbyContext{}, mapNearbyContextError(searchjobs.ErrNearbyContextUnavailable)
	}
	seekable, ok := lease.(searchartifacts.SeekableResultLease)
	if !ok {
		return searchjobs.NearbyContext{}, unavailableError("retained search service is unavailable")
	}
	if err := seekable.Seek(ctx, ordinal); err != nil {
		return searchjobs.NearbyContext{}, mapSearchArtifactError(err)
	}
	row, ok, err := lease.Next(ctx)
	if err != nil {
		return searchjobs.NearbyContext{}, mapSearchArtifactError(err)
	}
	if !ok || row.Ordinal != ordinal {
		return searchjobs.NearbyContext{}, mapNearbyContextError(searchjobs.ErrNearbyContextUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.NearbyContext{}, err
	}
	record, err := handler.searchArtifacts.Get(
		ctx,
		handler.accessScope(),
		jobID,
		searchartifacts.AccessInspect,
	)
	if err != nil {
		return searchjobs.NearbyContext{}, mapSearchArtifactError(err)
	}
	nearby, err := searchjobs.PrepareNearbyContext(
		record.Job.NearbyEventProvenance,
		lease.Schema(),
		row,
	)
	if err != nil {
		return searchjobs.NearbyContext{}, mapNearbyContextError(err)
	}
	return nearby, nil
}

func parseNearbyRowID(jobID, rowID string) (uint64, error) {
	text, ok := strings.CutPrefix(rowID, jobID+":")
	if !ok || text == "" {
		return 0, errors.New("row ID does not belong to search job")
	}
	ordinal, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(ordinal, 10) != text {
		return 0, errors.New("row ID ordinal is invalid")
	}
	return ordinal, nil
}

func nearbyContextToProto(
	ctx context.Context,
	input *opensplunk.PrepareNearbyContextRequest,
	nearby searchjobs.NearbyContext,
) (*opensplunk.PrepareNearbyContextResponse, error) {
	fields := make([]*opensplunk.NearbyContextField, len(nearby.Fields))
	for index, field := range nearby.Fields {
		value, err := searchjobproto.Value(ctx, field.Value)
		if err != nil {
			return nil, internalError()
		}
		fields[index] = &opensplunk.NearbyContextField{
			FieldName: field.Name, Value: value, Suggested: field.Suggested,
		}
	}
	return &opensplunk.PrepareNearbyContextResponse{
		SearchJobId: input.GetSearchJobId(), RowId: input.GetRowId(),
		AnchorTime: nearby.AnchorTime.Format(time.RFC3339Nano),
		Earliest:   nearby.Earliest.Format(time.RFC3339Nano),
		Latest:     nearby.Latest.Format(time.RFC3339Nano),
		Index:      nearby.Index, Host: nearby.Host, Source: nearby.Source,
		Clipped: nearby.Clipped, Fields: fields,
	}, nil
}

func mapNearbyContextError(err error) error {
	if errors.Is(err, searchjobs.ErrNearbyContextUnavailable) {
		return router.NewHTTPError(
			http.StatusConflict,
			searchjobs.ErrNearbyContextUnavailable.Error(),
		)
	}
	return mapSearchJobError(err)
}
