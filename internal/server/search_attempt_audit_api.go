package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"google.golang.org/protobuf/proto"
)

const (
	searchAttemptAuditListRoute             = "/audit/search-attempts/list"
	searchAttemptAuditListPath              = apiPathPrefix + searchAttemptAuditListRoute
	defaultSearchAttemptAuditListPageSize   = uint32(50)
	maximumSearchAttemptAuditResponseBytes  = 2 << 20
	maximumSearchAttemptAuditPageTokenBytes = 2 << 10
	maximumSearchAttemptAuditIdentityBytes  = 255
)

func (handler *apiHandler) searchAttemptAuditRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[
			*opensplunk.ListSearchAttemptAuditEventsRequest,
			*serializedSearchAttemptAuditListResponse,
		]{
			Path:       searchAttemptAuditListRoute,
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedSearchAttemptAuditListCodec(),
			Handler:    handler.listSearchAttemptAuditEvents,
			SourceType: router.Body,
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: smallRequestBytes,
			},
			Sanitizer: discardUnknownProtoFields,
		},
	}
}

func (handler *apiHandler) listSearchAttemptAuditEvents(
	request *http.Request,
	input *opensplunk.ListSearchAttemptAuditEventsRequest,
) (*serializedSearchAttemptAuditListResponse, error) {
	tenantID, err := handler.administratorAuditTenantAccess(request)
	if err != nil {
		return nil, err
	}
	if handler.searchAttemptAuditEvents == nil {
		return nil, unavailableError("search attempt audit service is unavailable")
	}
	listRequest, err := handler.searchAttemptAuditListRequest(input)
	if err != nil {
		return nil, err
	}
	if err := searchAttemptAuditContextError(request.Context()); err != nil {
		return nil, err
	}

	page, operationErr := handler.searchAttemptAuditEvents.List(
		request.Context(),
		tenantID,
		listRequest,
	)
	if mapped := mapSearchAttemptAuditListCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	if err := searchAttemptAuditContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"search attempt audit response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	message, err := searchAttemptAuditListPageToProto(
		tenantID,
		listRequest,
		page,
	)
	if err != nil || proto.Size(message) > maximumSearchAttemptAuditResponseBytes {
		return nil, internalError()
	}
	if err := searchAttemptAuditContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchAttemptAuditListResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) searchAttemptAuditListRequest(
	input *opensplunk.ListSearchAttemptAuditEventsRequest,
) (searchaudit.ListRequest, error) {
	if input == nil || len(input.ProtoReflect().GetUnknown()) != 0 {
		return searchaudit.ListRequest{}, badRequestError(
			"search attempt audit list request is invalid",
		)
	}
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(
		input.Page,
		"search attempt audit",
		defaultSearchAttemptAuditListPageSize,
		searchaudit.MaximumListPageSize,
	)
	if err != nil {
		return searchaudit.ListRequest{}, err
	}

	actorID, err := optionalSearchAttemptAuditFilter(
		input.ActorIdFilter,
		"search attempt audit actor filter is invalid",
	)
	if err != nil {
		return searchaudit.ListRequest{}, err
	}
	ownerID, err := optionalSearchAttemptAuditFilter(
		input.OwnerIdFilter,
		"search attempt audit owner filter is invalid",
	)
	if err != nil {
		return searchaudit.ListRequest{}, err
	}
	return searchaudit.ListRequest{
		PageSize:     pageSize,
		PageToken:    strings.Clone(pageToken),
		IncludeTotal: includeTotal,
		ActorID:      actorID,
		OwnerID:      ownerID,
	}, nil
}

func optionalSearchAttemptAuditFilter(
	input *string,
	message string,
) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := *input
	if strings.TrimSpace(value) != value || validateBoundedIdentifier(
		value,
		maximumSearchAttemptAuditIdentityBytes,
		false,
	) != nil {
		return nil, badRequestError(message)
	}
	return new(strings.Clone(value)), nil
}

func searchAttemptAuditListPageToProto(
	tenantID string,
	request searchaudit.ListRequest,
	page searchaudit.ListPage,
) (*opensplunk.ListSearchAttemptAuditEventsResponse, error) {
	if len(page.Events) > int(request.PageSize) {
		return nil, errors.New(
			"search attempt audit service returned too many rows",
		)
	}
	events := make([]*opensplunk.SearchAttemptAuditEvent, len(page.Events))
	var previous uint64
	for index, event := range page.Events {
		if index > 0 && event.Sequence >= previous {
			return nil, errors.New(
				"search attempt audit service returned unstable ordering",
			)
		}
		converted, err := searchAttemptAuditEventToProto(event, tenantID)
		if err != nil {
			return nil, err
		}
		if !searchAttemptAuditEventMatchesListRequest(event, request) {
			return nil, errors.New(
				"search attempt audit service returned a row outside the requested filters",
			)
		}
		events[index] = converted
		previous = event.Sequence
	}
	if page.TotalSize != nil && *page.TotalSize > searchaudit.MaximumRetainedAttempts {
		return nil, errors.New(
			"search attempt audit service returned an invalid total",
		)
	}
	metadata, err := boundedListPageResponse(
		"search attempt audit",
		boundedListPageMetadata{
			itemCount:     len(events),
			nextPageToken: page.NextPageToken,
			totalSize:     page.TotalSize,
			totalExact:    page.TotalSizeExact,
		},
		int(request.PageSize),
		request.PageToken,
		request.IncludeTotal,
		maximumSearchAttemptAuditPageTokenBytes,
	)
	if err != nil {
		return nil, err
	}
	return &opensplunk.ListSearchAttemptAuditEventsResponse{
		Events: events,
		Page:   metadata,
	}, nil
}

func searchAttemptAuditEventMatchesListRequest(
	event searchaudit.Event,
	request searchaudit.ListRequest,
) bool {
	if request.ActorID != nil && event.Actor.ID != *request.ActorID {
		return false
	}
	return request.OwnerID == nil || event.OwnerID == *request.OwnerID
}

func searchAttemptAuditEventToProto(
	event searchaudit.Event,
	expectedTenantID string,
) (*opensplunk.SearchAttemptAuditEvent, error) {
	if err := event.ValidateForTenant(expectedTenantID); err != nil {
		return nil, errors.New(
			"search attempt audit service returned an invalid event",
		)
	}
	occurredAt, err := validTimestamp(event.OccurredAt)
	if err != nil {
		return nil, errors.New(
			"search attempt audit service returned an invalid time",
		)
	}
	actorKind, ok := auditActorKindToProto(event.Actor.Kind)
	if !ok {
		return nil, errors.New(
			"search attempt audit service returned an invalid actor",
		)
	}
	actorRole, ok := auditActorRoleToProto(event.Actor.Role)
	if !ok {
		return nil, errors.New(
			"search attempt audit service returned an invalid actor",
		)
	}
	var knowledgeSnapshot *opensplunk.KnowledgeSnapshotRef
	if event.KnowledgeSnapshot != nil {
		knowledgeSnapshot, err = knowledgesnapshot.CloneReference(
			event.KnowledgeSnapshot,
		)
		if err != nil {
			return nil, errors.New(
				"search attempt audit service returned an invalid knowledge snapshot",
			)
		}
	}
	return &opensplunk.SearchAttemptAuditEvent{
		Sequence:          event.Sequence,
		OccurredAt:        occurredAt,
		ActorKind:         actorKind,
		ActorId:           strings.Clone(event.Actor.ID),
		ActorRole:         actorRole,
		OwnerId:           strings.Clone(event.OwnerID),
		SearchJobId:       strings.Clone(event.SearchJobID),
		KnowledgeSnapshot: knowledgeSnapshot,
	}, nil
}

func mapSearchAttemptAuditListCallError(
	ctx context.Context,
	operationErr error,
) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"search attempt audit request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, searchaudit.ErrInvalidCursor):
		return badRequestError("search attempt audit page token is invalid")
	case errors.Is(operationErr, searchaudit.ErrCorrupt):
		return unavailableError("search attempt audit service is unavailable")
	default:
		return unavailableError("search attempt audit service is unavailable")
	}
}

func searchAttemptAuditContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "search attempt audit request was canceled")
}

type serializedSearchAttemptAuditListResponse = boundedProtoResponse[*opensplunk.ListSearchAttemptAuditEventsResponse]

type serializedSearchAttemptAuditListCodec = boundedProtoCodec[
	*opensplunk.ListSearchAttemptAuditEventsRequest,
	*opensplunk.ListSearchAttemptAuditEventsResponse,
]

func newSerializedSearchAttemptAuditListCodec() *serializedSearchAttemptAuditListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.ListSearchAttemptAuditEventsRequest,
			*opensplunk.ListSearchAttemptAuditEventsResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "search attempt audit serialization state is invalid",
			messageError: "search attempt audit response is missing",
			contextError: searchAttemptAuditContextError,
			maximumBytes: maximumSearchAttemptAuditResponseBytes,
			sizeError:    "search attempt audit response exceeds its byte limit",
		},
	)
}
