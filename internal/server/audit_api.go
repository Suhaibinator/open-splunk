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
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[
			*opensplunk.ListAuditEventsRequest,
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
			Sanitizer: handler.sanitizeListAuditEventsRequest,
		},
	}
}

func (handler *apiHandler) listAuditEvents(
	request *http.Request,
	input *opensplunk.ListAuditEventsRequest,
) (*serializedAuditEventListResponse, error) {
	tenantID, err := handler.administratorAuditTenantAccess(request)
	if err != nil {
		return nil, err
	}
	if handler.auditEvents == nil {
		return nil, unavailableError("audit event service is unavailable")
	}
	listRequest := auditListRequest(input)
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

// auditListRequest projects a request that sanitizeListAuditEventsRequest has
// already validated and resolved, so every conversion below is total.
func auditListRequest(
	input *opensplunk.ListAuditEventsRequest,
) audit.ListRequest {
	page := input.GetPage()
	actions := make([]audit.Action, 0, len(input.GetActionFilters()))
	for _, value := range input.GetActionFilters() {
		action, _ := auditActionFromProto(value)
		actions = append(actions, action)
	}

	var actorID *string
	if input.ActorIdFilter != nil {
		actorID = new(input.GetActorIdFilter())
	}

	var targetKind *audit.TargetKind
	if input.TargetKindFilter != nil {
		value, _ := auditTargetKindFromProto(input.GetTargetKindFilter())
		targetKind = &value
	}

	return audit.ListRequest{
		PageSize: page.GetPageSize(),
		// The token outlives the decoded request inside the ledger call.
		PageToken:     strings.Clone(page.GetPageToken()),
		ActionFilters: actions,
		ActorID:       actorID,
		TargetKind:    targetKind,
		IncludeTotal:  page.GetIncludeTotalSize(),
	}
}

func auditListPageToProto(
	tenantID string,
	request audit.ListRequest,
	page audit.ListPage,
) (*opensplunk.ListAuditEventsResponse, error) {
	if len(page.Events) > int(request.PageSize) {
		return nil, errors.New("audit event service returned too many rows")
	}
	events := make([]*opensplunk.AuditEvent, len(page.Events))
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
	return &opensplunk.ListAuditEventsResponse{
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
) (*opensplunk.AuditEvent, error) {
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
	message := &opensplunk.AuditEvent{
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
) (opensplunk.KnowledgeObjectType, bool) {
	switch value {
	case audit.KnowledgeObjectTypeFieldExtraction:
		return opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, true
	case audit.KnowledgeObjectTypeFieldAlias:
		return opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, true
	case audit.KnowledgeObjectTypeCalculatedField:
		return opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, true
	default:
		return opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED, false
	}
}

func auditKnowledgeSharingScopeToProto(
	value audit.KnowledgeSharingScope,
) (opensplunk.SharingScope, bool) {
	switch value {
	case audit.KnowledgeSharingScopePrivate:
		return opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, true
	case audit.KnowledgeSharingScopeApp:
		return opensplunk.SharingScope_SHARING_SCOPE_APP, true
	case audit.KnowledgeSharingScopeGlobal:
		return opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, true
	default:
		return opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED, false
	}
}

func auditActorRoleToProto(
	value audit.ActorRole,
) (opensplunk.AuditActorRole, bool) {
	switch value {
	case audit.ActorRoleSystem:
		return opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_SYSTEM, true
	case audit.ActorRoleUser:
		return opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_USER, true
	case audit.ActorRoleAdministrator:
		return opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR, true
	default:
		return opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_UNSPECIFIED, false
	}
}

func auditActorKindToProto(
	value audit.ActorKind,
) (opensplunk.AuditActorKind, bool) {
	switch value {
	case audit.ActorKindSystem:
		return opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_SYSTEM, true
	case audit.ActorKindBrowser:
		return opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER, true
	default:
		return opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_UNSPECIFIED, false
	}
}

func auditActionFromProto(value opensplunk.AuditAction) (audit.Action, bool) {
	switch value {
	case opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE:
		return audit.ActionIngestionTokenCreate, true
	case opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE:
		return audit.ActionIngestionTokenUpdate, true
	case opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE:
		return audit.ActionIngestionTokenRevoke, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE:
		return audit.ActionIndexCreate, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE:
		return audit.ActionIndexUpdate, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE:
		return audit.ActionIndexActivate, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE:
		return audit.ActionIndexArchive, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA:
		return audit.ActionIndexDeleteKeepData, true
	case opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA:
		return audit.ActionIndexDeleteData, true
	case opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE:
		return audit.ActionAppCreate, true
	case opensplunk.AuditAction_AUDIT_ACTION_APP_UPDATE:
		return audit.ActionAppUpdate, true
	case opensplunk.AuditAction_AUDIT_ACTION_APP_ACTIVATE:
		return audit.ActionAppActivate, true
	case opensplunk.AuditAction_AUDIT_ACTION_APP_ARCHIVE:
		return audit.ActionAppArchive, true
	case opensplunk.AuditAction_AUDIT_ACTION_APP_DELETE:
		return audit.ActionAppDelete, true
	case opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE:
		return audit.ActionSavedSearchCreate, true
	case opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE:
		return audit.ActionSavedSearchUpdate, true
	case opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE:
		return audit.ActionSavedSearchDuplicate, true
	case opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE:
		return audit.ActionSavedSearchDelete, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE:
		return audit.ActionKnowledgeObjectCreate, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE:
		return audit.ActionKnowledgeObjectUpdate, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE:
		return audit.ActionKnowledgeObjectScopeChange, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE:
		return audit.ActionKnowledgeObjectEnable, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE:
		return audit.ActionKnowledgeObjectDisable, true
	case opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE:
		return audit.ActionKnowledgeObjectDelete, true
	case opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE:
		return audit.ActionServerSettingsUpdate, true
	default:
		return "", false
	}
}

func auditActionToProto(value audit.Action) (opensplunk.AuditAction, bool) {
	switch value {
	case audit.ActionIngestionTokenCreate:
		return opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE, true
	case audit.ActionIngestionTokenUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE, true
	case audit.ActionIngestionTokenRevoke:
		return opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE, true
	case audit.ActionIndexCreate:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE, true
	case audit.ActionIndexUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE, true
	case audit.ActionIndexActivate:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE, true
	case audit.ActionIndexArchive:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, true
	case audit.ActionIndexDeleteKeepData:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, true
	case audit.ActionIndexDeleteData:
		return opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA, true
	case audit.ActionAppCreate:
		return opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE, true
	case audit.ActionAppUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_APP_UPDATE, true
	case audit.ActionAppActivate:
		return opensplunk.AuditAction_AUDIT_ACTION_APP_ACTIVATE, true
	case audit.ActionAppArchive:
		return opensplunk.AuditAction_AUDIT_ACTION_APP_ARCHIVE, true
	case audit.ActionAppDelete:
		return opensplunk.AuditAction_AUDIT_ACTION_APP_DELETE, true
	case audit.ActionSavedSearchCreate:
		return opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE, true
	case audit.ActionSavedSearchUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE, true
	case audit.ActionSavedSearchDuplicate:
		return opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, true
	case audit.ActionSavedSearchDelete:
		return opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE, true
	case audit.ActionKnowledgeObjectCreate:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE, true
	case audit.ActionKnowledgeObjectUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE, true
	case audit.ActionKnowledgeObjectScopeChange:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE, true
	case audit.ActionKnowledgeObjectEnable:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE, true
	case audit.ActionKnowledgeObjectDisable:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE, true
	case audit.ActionKnowledgeObjectDelete:
		return opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE, true
	case audit.ActionServerSettingsUpdate:
		return opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE, true
	default:
		return opensplunk.AuditAction_AUDIT_ACTION_UNSPECIFIED, false
	}
}

func auditTargetKindFromProto(
	value opensplunk.AuditTargetKind,
) (audit.TargetKind, bool) {
	switch value {
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN:
		return audit.TargetKindIngestionToken, true
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX:
		return audit.TargetKindIndex, true
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_APP:
		return audit.TargetKindApp, true
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH:
		return audit.TargetKindSavedSearch, true
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT:
		return audit.TargetKindKnowledgeObject, true
	case opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SERVER_SETTINGS:
		return audit.TargetKindServerSettings, true
	default:
		return "", false
	}
}

func auditTargetKindToProto(
	value audit.TargetKind,
) (opensplunk.AuditTargetKind, bool) {
	switch value {
	case audit.TargetKindIngestionToken:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN, true
	case audit.TargetKindIndex:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX, true
	case audit.TargetKindApp:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_APP, true
	case audit.TargetKindSavedSearch:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH, true
	case audit.TargetKindKnowledgeObject:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, true
	case audit.TargetKindServerSettings:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SERVER_SETTINGS, true
	default:
		return opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_UNSPECIFIED, false
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

type serializedAuditEventListResponse = boundedProtoResponse[*opensplunk.ListAuditEventsResponse]

type serializedAuditEventListCodec = boundedProtoCodec[
	*opensplunk.ListAuditEventsRequest,
	*opensplunk.ListAuditEventsResponse,
]

func newSerializedAuditEventListCodec() *serializedAuditEventListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.ListAuditEventsRequest,
			*opensplunk.ListAuditEventsResponse,
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
