package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"google.golang.org/protobuf/proto"
)

const (
	defaultAuditListPageSize      = uint32(50)
	maximumAuditListResponseBytes = 2 << 20
	maximumAuditPageTokenBytes    = 2 << 10
	maximumAuditActorIDBytes      = 255
)

func (handler *apiHandler) auditEventRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[
			*opensplunkv1.ListAuditEventsRequest,
			*serializedAuditEventListResponse](router.RouteConfig[
			*opensplunkv1.ListAuditEventsRequest,
			*serializedAuditEventListResponse,
		]{
			Path:       auditEventsListRoute,
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedAuditEventListCodec(),
			Handler:    handler.listAuditEvents,
			SourceType: router.Body,
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: smallRequestBytes,
			},
		}),
	}
}

func (handler *apiHandler) listAuditEvents(
	request *http.Request,
	input *opensplunkv1.ListAuditEventsRequest,
) (*serializedAuditEventListResponse, error) {
	tenantID, err := handler.administratorAuditTenantAccess(request)
	if err != nil {
		return nil, err
	}
	if handler.auditEvents == nil {
		return nil, unavailableError("audit event service is unavailable")
	}
	listRequest, err := handler.auditListRequest(input)
	if err != nil {
		return nil, err
	}
	if err := auditListContextError(request.Context()); err != nil {
		return nil, err
	}

	page, operationErr := handler.auditEvents.List(
		request.Context(),
		tenantID,
		listRequest,
	)
	if mapped := mapAuditListCallError(request.Context(), operationErr); mapped != nil {
		return nil, mapped
	}
	if err := auditListContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"audit event response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	message, err := auditListPageToProto(tenantID, listRequest, page)
	if err != nil || proto.Size(message) > maximumAuditListResponseBytes {
		return nil, internalError()
	}
	if err := auditListContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedAuditEventListResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) administratorAuditTenantAccess(
	request *http.Request,
) (string, error) {
	principal, err := handler.administratorPrincipal(request)
	if err != nil {
		return "", err
	}
	return principal.TenantID(), nil
}

func (handler *apiHandler) auditListRequest(
	input *opensplunkv1.ListAuditEventsRequest,
) (audit.ListRequest, error) {
	if input == nil || len(input.ProtoReflect().GetUnknown()) != 0 {
		return audit.ListRequest{}, badRequestError(
			"audit event list request is invalid",
		)
	}
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(
		input.Page,
		"audit event",
		defaultAuditListPageSize,
		audit.MaximumListPageSize,
		maximumAuditPageTokenBytes,
	)
	if err != nil {
		return audit.ListRequest{}, err
	}

	if len(input.GetActionFilters()) > audit.MaximumActionFilters {
		return audit.ListRequest{}, badRequestError(
			"audit event action filters are invalid",
		)
	}
	actions := make([]audit.Action, 0, len(input.GetActionFilters()))
	seenActions := make(map[audit.Action]struct{}, len(input.GetActionFilters()))
	for _, value := range input.GetActionFilters() {
		action, ok := auditActionFromProto(value)
		if !ok {
			return audit.ListRequest{}, badRequestError(
				"audit event action filter is invalid",
			)
		}
		if _, duplicate := seenActions[action]; duplicate {
			return audit.ListRequest{}, badRequestError(
				"audit event action filter is duplicated",
			)
		}
		seenActions[action] = struct{}{}
		actions = append(actions, action)
	}

	var actorID *string
	if input.ActorIdFilter != nil {
		value := input.GetActorIdFilter()
		if strings.TrimSpace(value) != value || validateBoundedIdentifier(
			value,
			maximumAuditActorIDBytes,
			false,
		) != nil {
			return audit.ListRequest{}, badRequestError(
				"audit event actor filter is invalid",
			)
		}
		actorID = new(strings.Clone(value))
	}

	var targetKind *audit.TargetKind
	if input.TargetKindFilter != nil {
		value, ok := auditTargetKindFromProto(input.GetTargetKindFilter())
		if !ok {
			return audit.ListRequest{}, badRequestError(
				"audit event target filter is invalid",
			)
		}
		targetKind = &value
	}

	return audit.ListRequest{
		PageSize:      pageSize,
		PageToken:     strings.Clone(pageToken),
		ActionFilters: actions,
		ActorID:       actorID,
		TargetKind:    targetKind,
		IncludeTotal:  includeTotal,
	}, nil
}

func auditListPageToProto(
	tenantID string,
	request audit.ListRequest,
	page audit.ListPage,
) (*opensplunkv1.ListAuditEventsResponse, error) {
	if len(page.Events) > int(request.PageSize) {
		return nil, errors.New("audit event service returned too many rows")
	}
	events := make([]*opensplunkv1.AuditEvent, len(page.Events))
	var previous uint64
	for index, event := range page.Events {
		if index > 0 && event.Sequence >= previous {
			return nil, errors.New(
				"audit event service returned unstable ordering",
			)
		}
		converted, err := auditEventToProto(event, tenantID)
		if err != nil {
			return nil, err
		}
		if !auditEventMatchesListRequest(event, request) {
			return nil, errors.New(
				"audit event service returned a row outside the requested filters",
			)
		}
		events[index] = converted
		previous = event.Sequence
	}
	if page.TotalSize != nil && *page.TotalSize > audit.MaximumEventsPerTenant {
		return nil, errors.New("audit event service returned an invalid total")
	}
	metadata, err := boundedListPageResponse(
		"audit event",
		boundedListPageMetadata{
			itemCount:     len(events),
			nextPageToken: page.NextPageToken,
			totalSize:     page.TotalSize,
			totalExact:    page.TotalSizeExact,
		},
		int(request.PageSize),
		request.PageToken,
		request.IncludeTotal,
		maximumAuditPageTokenBytes,
	)
	if err != nil {
		return nil, err
	}
	return &opensplunkv1.ListAuditEventsResponse{
		AuditEvents: events,
		Page:        metadata,
	}, nil
}

func auditEventMatchesListRequest(event audit.Event, request audit.ListRequest) bool {
	if len(request.ActionFilters) != 0 &&
		!slices.Contains(request.ActionFilters, event.Action) {
		return false
	}
	if request.ActorID != nil && event.Actor.ID != *request.ActorID {
		return false
	}
	return request.TargetKind == nil || event.TargetKind == *request.TargetKind
}

func auditEventToProto(
	event audit.Event,
	expectedTenantID string,
) (*opensplunkv1.AuditEvent, error) {
	if err := event.ValidateForTenant(expectedTenantID); err != nil {
		return nil, errors.New("audit event service returned an invalid event")
	}
	occurredAt, err := validTimestamp(event.OccurredAt)
	if err != nil {
		return nil, errors.New("audit event service returned an invalid time")
	}
	actorKind, ok := auditActorKindToProto(event.Actor.Kind)
	if !ok {
		return nil, errors.New("audit event service returned an invalid actor")
	}
	actorRole, ok := auditActorRoleToProto(event.Actor.Role)
	if !ok {
		return nil, errors.New("audit event service returned an invalid actor")
	}
	action, ok := auditActionToProto(event.Action)
	if !ok {
		return nil, errors.New("audit event service returned an invalid action")
	}
	targetKind, ok := auditTargetKindToProto(event.TargetKind)
	if !ok {
		return nil, errors.New("audit event service returned an invalid target")
	}
	message := &opensplunkv1.AuditEvent{
		Sequence:      event.Sequence,
		OccurredAt:    occurredAt,
		ActorKind:     actorKind,
		ActorId:       strings.Clone(event.Actor.ID),
		ActorRole:     actorRole,
		Action:        action,
		TargetKind:    targetKind,
		TargetId:      strings.Clone(event.TargetID),
		TargetVersion: event.TargetVersion,
	}
	if event.TargetKind == audit.TargetKindKnowledgeObject {
		appID := strings.Clone(event.KnowledgeObject.AppID)
		objectType, typeOK := auditKnowledgeObjectTypeToProto(
			event.KnowledgeObject.ObjectType,
		)
		sharingScope, scopeOK := auditKnowledgeSharingScopeToProto(
			event.KnowledgeObject.SharingScope,
		)
		if !typeOK || !scopeOK {
			return nil, errors.New("audit event service returned invalid knowledge metadata")
		}
		message.AppId = &appID
		message.ObjectType = &objectType
		message.SharingScope = &sharingScope
	}
	return message, nil
}

func auditKnowledgeObjectTypeToProto(
	value audit.KnowledgeObjectType,
) (opensplunkv1.KnowledgeObjectType, bool) {
	switch value {
	case audit.KnowledgeObjectTypeFieldExtraction:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, true
	case audit.KnowledgeObjectTypeFieldAlias:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, true
	case audit.KnowledgeObjectTypeCalculatedField:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, true
	default:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED, false
	}
}

func auditKnowledgeSharingScopeToProto(
	value audit.KnowledgeSharingScope,
) (opensplunkv1.SharingScope, bool) {
	switch value {
	case audit.KnowledgeSharingScopePrivate:
		return opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, true
	case audit.KnowledgeSharingScopeApp:
		return opensplunkv1.SharingScope_SHARING_SCOPE_APP, true
	case audit.KnowledgeSharingScopeGlobal:
		return opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true
	default:
		return opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED, false
	}
}

func auditActorRoleToProto(
	value audit.ActorRole,
) (opensplunkv1.AuditActorRole, bool) {
	switch value {
	case audit.ActorRoleSystem:
		return opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_SYSTEM, true
	case audit.ActorRoleUser:
		return opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_USER, true
	case audit.ActorRoleAdministrator:
		return opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR, true
	default:
		return opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_UNSPECIFIED, false
	}
}

func auditActorKindToProto(
	value audit.ActorKind,
) (opensplunkv1.AuditActorKind, bool) {
	switch value {
	case audit.ActorKindSystem:
		return opensplunkv1.AuditActorKind_AUDIT_ACTOR_KIND_SYSTEM, true
	case audit.ActorKindBrowser:
		return opensplunkv1.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER, true
	default:
		return opensplunkv1.AuditActorKind_AUDIT_ACTOR_KIND_UNSPECIFIED, false
	}
}

func auditActionFromProto(value opensplunkv1.AuditAction) (audit.Action, bool) {
	switch value {
	case opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE:
		return audit.ActionIngestionTokenCreate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE:
		return audit.ActionIngestionTokenUpdate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE:
		return audit.ActionIngestionTokenRevoke, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_CREATE:
		return audit.ActionIndexCreate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_UPDATE:
		return audit.ActionIndexUpdate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE:
		return audit.ActionIndexActivate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE:
		return audit.ActionIndexArchive, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA:
		return audit.ActionIndexDeleteKeepData, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA:
		return audit.ActionIndexDeleteData, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_APP_CREATE:
		return audit.ActionAppCreate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_APP_UPDATE:
		return audit.ActionAppUpdate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_APP_ACTIVATE:
		return audit.ActionAppActivate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_APP_ARCHIVE:
		return audit.ActionAppArchive, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_APP_DELETE:
		return audit.ActionAppDelete, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE:
		return audit.ActionSavedSearchCreate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE:
		return audit.ActionSavedSearchUpdate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE:
		return audit.ActionSavedSearchDuplicate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE:
		return audit.ActionSavedSearchDelete, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE:
		return audit.ActionKnowledgeObjectCreate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE:
		return audit.ActionKnowledgeObjectUpdate, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE:
		return audit.ActionKnowledgeObjectScopeChange, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE:
		return audit.ActionKnowledgeObjectEnable, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE:
		return audit.ActionKnowledgeObjectDisable, true
	case opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE:
		return audit.ActionKnowledgeObjectDelete, true
	default:
		return "", false
	}
}

func auditActionToProto(value audit.Action) (opensplunkv1.AuditAction, bool) {
	switch value {
	case audit.ActionIngestionTokenCreate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE, true
	case audit.ActionIngestionTokenUpdate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE, true
	case audit.ActionIngestionTokenRevoke:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE, true
	case audit.ActionIndexCreate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_CREATE, true
	case audit.ActionIndexUpdate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_UPDATE, true
	case audit.ActionIndexActivate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE, true
	case audit.ActionIndexArchive:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, true
	case audit.ActionIndexDeleteKeepData:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, true
	case audit.ActionIndexDeleteData:
		return opensplunkv1.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA, true
	case audit.ActionAppCreate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_APP_CREATE, true
	case audit.ActionAppUpdate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_APP_UPDATE, true
	case audit.ActionAppActivate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_APP_ACTIVATE, true
	case audit.ActionAppArchive:
		return opensplunkv1.AuditAction_AUDIT_ACTION_APP_ARCHIVE, true
	case audit.ActionAppDelete:
		return opensplunkv1.AuditAction_AUDIT_ACTION_APP_DELETE, true
	case audit.ActionSavedSearchCreate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE, true
	case audit.ActionSavedSearchUpdate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE, true
	case audit.ActionSavedSearchDuplicate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, true
	case audit.ActionSavedSearchDelete:
		return opensplunkv1.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE, true
	case audit.ActionKnowledgeObjectCreate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE, true
	case audit.ActionKnowledgeObjectUpdate:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE, true
	case audit.ActionKnowledgeObjectScopeChange:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE, true
	case audit.ActionKnowledgeObjectEnable:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE, true
	case audit.ActionKnowledgeObjectDisable:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE, true
	case audit.ActionKnowledgeObjectDelete:
		return opensplunkv1.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE, true
	default:
		return opensplunkv1.AuditAction_AUDIT_ACTION_UNSPECIFIED, false
	}
}

func auditTargetKindFromProto(
	value opensplunkv1.AuditTargetKind,
) (audit.TargetKind, bool) {
	switch value {
	case opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN:
		return audit.TargetKindIngestionToken, true
	case opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_INDEX:
		return audit.TargetKindIndex, true
	case opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_APP:
		return audit.TargetKindApp, true
	case opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH:
		return audit.TargetKindSavedSearch, true
	case opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT:
		return audit.TargetKindKnowledgeObject, true
	default:
		return "", false
	}
}

func auditTargetKindToProto(
	value audit.TargetKind,
) (opensplunkv1.AuditTargetKind, bool) {
	switch value {
	case audit.TargetKindIngestionToken:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN, true
	case audit.TargetKindIndex:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_INDEX, true
	case audit.TargetKindApp:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_APP, true
	case audit.TargetKindSavedSearch:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH, true
	case audit.TargetKindKnowledgeObject:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, true
	default:
		return opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_UNSPECIFIED, false
	}
}

func mapAuditListCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"audit event request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, audit.ErrInvalidCursor):
		return badRequestError("audit event page token is invalid")
	default:
		// Ledger corruption is deliberately reported as 503, not 500.
		return unavailableError("audit event service is unavailable")
	}
}

func auditListContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "audit event request was canceled")
}

type serializedAuditEventListResponse = boundedProtoResponse[*opensplunkv1.ListAuditEventsResponse]

type serializedAuditEventListCodec = boundedProtoCodec[
	*opensplunkv1.ListAuditEventsRequest,
	*opensplunkv1.ListAuditEventsResponse,
]

func newSerializedAuditEventListCodec() *serializedAuditEventListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.ListAuditEventsRequest,
			*opensplunkv1.ListAuditEventsResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "audit event serialization state is invalid",
			messageError: "audit event response is missing",
			contextError: auditListContextError,
			maximumBytes: maximumAuditListResponseBytes,
			sizeError:    "audit event response exceeds its byte limit",
		},
	)
}
