package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	knowledgeObjectsCreateRoute       = "/knowledge/objects/create"
	knowledgeObjectsGetRoute          = "/knowledge/objects/get"
	knowledgeObjectsListRoute         = "/knowledge/objects/list"
	knowledgeObjectsDependenciesRoute = "/knowledge/objects/dependencies"
	knowledgeObjectsDependentsRoute   = "/knowledge/objects/dependents"
	knowledgeObjectsValidateRoute     = "/knowledge/objects/validate"
	knowledgeObjectsPreviewRoute      = "/knowledge/objects/preview"
	knowledgeObjectsUpdateRoute       = "/knowledge/objects/update"
	knowledgeObjectsSetStateRoute     = "/knowledge/objects/set-state"
	knowledgeObjectsDeleteRoute       = "/knowledge/objects/delete"

	knowledgeObjectsCreatePath       = apiV1PathPrefix + knowledgeObjectsCreateRoute
	knowledgeObjectsGetPath          = apiV1PathPrefix + knowledgeObjectsGetRoute
	knowledgeObjectsListPath         = apiV1PathPrefix + knowledgeObjectsListRoute
	knowledgeObjectsDependenciesPath = apiV1PathPrefix + knowledgeObjectsDependenciesRoute
	knowledgeObjectsDependentsPath   = apiV1PathPrefix + knowledgeObjectsDependentsRoute
	knowledgeObjectsValidatePath     = apiV1PathPrefix + knowledgeObjectsValidateRoute
	knowledgeObjectsPreviewPath      = apiV1PathPrefix + knowledgeObjectsPreviewRoute
	knowledgeObjectsUpdatePath       = apiV1PathPrefix + knowledgeObjectsUpdateRoute
	knowledgeObjectsSetStatePath     = apiV1PathPrefix + knowledgeObjectsSetStateRoute
	knowledgeObjectsDeletePath       = apiV1PathPrefix + knowledgeObjectsDeleteRoute

	maximumKnowledgeApps                 = control.MaximumAppsPerTenant
	maximumKnowledgeSmallRequestBytes    = int64(16 << 10)
	maximumKnowledgeMutationRequestBytes = int64(4<<20 + 64<<10)
	maximumKnowledgePageTokenBytes       = 4 << 10
	maximumKnowledgeIdentityBytes        = 255
	maximumKnowledgeAppIDBytes           = 128
	maximumKnowledgeObjectIDBytes        = 128
	maximumKnowledgeNameBytes            = 255
)

// KnowledgeGraphCatalog is the bounded direct-edge inspection surface used by
// the management handlers. Store satisfies it directly.
type KnowledgeGraphCatalog interface {
	ListDependencies(
		context.Context,
		knowledgecatalog.ReadScope,
		knowledgecatalog.DependencyListRequest,
	) (knowledgecatalog.DependencyPage, error)
	ListDependents(
		context.Context,
		knowledgecatalog.ReadScope,
		knowledgecatalog.DependencyListRequest,
	) (knowledgecatalog.DependencyPage, error)
}

// KnowledgeCatalog is the complete bounded read-only surface used by the
// management handlers. Embedding graph inspection keeps all nine routes on
// one catalog authority and one all-or-none configuration boundary.
type KnowledgeCatalog interface {
	KnowledgeGraphCatalog
	Get(
		context.Context,
		knowledgecatalog.ReadScope,
		string,
		*uint64,
	) (knowledgecatalog.Object, error)
	List(
		context.Context,
		knowledgecatalog.ReadScope,
		knowledgecatalog.ListRequest,
	) (knowledgecatalog.ListPage, error)
}

var _ KnowledgeGraphCatalog = (*knowledgecatalog.Store)(nil)

// KnowledgeWriter is the atomic mutation surface used by the management
// handlers. knowledgecatalog.Writer satisfies it directly.
type KnowledgeWriter interface {
	Create(
		context.Context,
		knowledgecatalog.WriteScope,
		*opensplunkv1.CreateKnowledgeObjectRequest,
	) (*opensplunkv1.CreateKnowledgeObjectResponse, error)
	Update(
		context.Context,
		knowledgecatalog.WriteScope,
		*opensplunkv1.UpdateKnowledgeObjectRequest,
	) (*opensplunkv1.UpdateKnowledgeObjectResponse, error)
	SetState(
		context.Context,
		knowledgecatalog.WriteScope,
		*opensplunkv1.SetKnowledgeObjectStateRequest,
	) (*opensplunkv1.SetKnowledgeObjectStateResponse, error)
	Delete(
		context.Context,
		knowledgecatalog.WriteScope,
		*opensplunkv1.DeleteKnowledgeObjectRequest,
	) (*opensplunkv1.DeleteKnowledgeObjectResponse, error)
}

var _ KnowledgeWriter = (*knowledgecatalog.Writer)(nil)

func (handler *apiHandler) knowledgeManagementConfigured() bool {
	return handler != nil &&
		!isNilDependency(handler.knowledgeCatalog) &&
		replaysUnavailableActiveMutations(handler.knowledgeWriter) &&
		!isNilDependency(handler.knowledgeApps) &&
		!isNilDependency(handler.knowledgeAttempts)
}

func (handler *apiHandler) knowledgePreviewConfigured() bool {
	return handler != nil && handler.knowledgeManagementConfigured() &&
		handler.knowledgePreview != nil && handler.knowledgePreview.Ready()
}

func replaysUnavailableActiveMutations(writer KnowledgeWriter) bool {
	if isNilDependency(writer) {
		return false
	}
	// Keep the exception exact-type-only. A marker interface can be forged by
	// embedding its implementation and overriding mutation methods; only the
	// catalog's concrete Writer owns the receipt-first ACTIVE replay guarantee.
	concrete, ok := writer.(*knowledgecatalog.Writer)
	return ok && concrete.ReadyForManagement()
}

func (handler *apiHandler) knowledgeManagementRoutes(
	noAuth router.AuthLevel,
) []protobufRouteDefinition {
	routes := []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunkv1.CreateKnowledgeObjectRequest, *serializedCreateKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.CreateKnowledgeObjectRequest, *serializedCreateKnowledgeObjectResponse]{
			Path: knowledgeObjectsCreateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedCreateKnowledgeObjectCodec(), Handler: handler.createKnowledgeObject,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeMutationRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.GetKnowledgeObjectRequest, *serializedGetKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.GetKnowledgeObjectRequest, *serializedGetKnowledgeObjectResponse]{
			Path: knowledgeObjectsGetRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedGetKnowledgeObjectCodec(), Handler: handler.getKnowledgeObject,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ListKnowledgeObjectsRequest, *serializedListKnowledgeObjectsResponse](router.RouteConfig[*opensplunkv1.ListKnowledgeObjectsRequest, *serializedListKnowledgeObjectsResponse]{
			Path: knowledgeObjectsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedListKnowledgeObjectsCodec(), Handler: handler.listKnowledgeObjects,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ListKnowledgeObjectDependenciesRequest, *serializedListKnowledgeObjectDependenciesResponse](router.RouteConfig[*opensplunkv1.ListKnowledgeObjectDependenciesRequest, *serializedListKnowledgeObjectDependenciesResponse]{
			Path: knowledgeObjectsDependenciesRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedListKnowledgeObjectDependenciesCodec(), Handler: handler.listKnowledgeObjectDependencies,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ListKnowledgeObjectDependentsRequest, *serializedListKnowledgeObjectDependentsResponse](router.RouteConfig[*opensplunkv1.ListKnowledgeObjectDependentsRequest, *serializedListKnowledgeObjectDependentsResponse]{
			Path: knowledgeObjectsDependentsRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedListKnowledgeObjectDependentsCodec(), Handler: handler.listKnowledgeObjectDependents,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ValidateKnowledgeObjectRequest, *serializedValidateKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.ValidateKnowledgeObjectRequest, *serializedValidateKnowledgeObjectResponse]{
			Path: knowledgeObjectsValidateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newValidateKnowledgeObjectCodec(), Handler: handler.validateKnowledgeObject,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeMutationRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.UpdateKnowledgeObjectRequest, *serializedUpdateKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.UpdateKnowledgeObjectRequest, *serializedUpdateKnowledgeObjectResponse]{
			Path: knowledgeObjectsUpdateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedUpdateKnowledgeObjectCodec(), Handler: handler.updateKnowledgeObject,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeMutationRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.SetKnowledgeObjectStateRequest, *serializedSetKnowledgeObjectStateResponse](router.RouteConfig[*opensplunkv1.SetKnowledgeObjectStateRequest, *serializedSetKnowledgeObjectStateResponse]{
			Path: knowledgeObjectsSetStateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSetKnowledgeObjectStateCodec(), Handler: handler.setKnowledgeObjectState,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.DeleteKnowledgeObjectRequest, *serializedDeleteKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.DeleteKnowledgeObjectRequest, *serializedDeleteKnowledgeObjectResponse]{
			Path: knowledgeObjectsDeleteRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedDeleteKnowledgeObjectCodec(), Handler: handler.deleteKnowledgeObject,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeSmallRequestBytes},
		}),
	}
	if handler.knowledgePreviewConfigured() {
		routes = append(
			routes,
			newForwardCompatibleProtoRoute[*opensplunkv1.PreviewKnowledgeObjectRequest, *serializedPreviewKnowledgeObjectResponse](router.RouteConfig[*opensplunkv1.PreviewKnowledgeObjectRequest, *serializedPreviewKnowledgeObjectResponse]{
				Path: knowledgeObjectsPreviewRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: newPreviewKnowledgeObjectRequestCodec(), Handler: handler.previewKnowledgeObject,
				SourceType: router.Body,
				Overrides:  sroutercommon.RouteOverrides{MaxBodySize: maximumKnowledgeMutationRequestBytes},
			}),
		)
	}
	return routes
}

// KnowledgeAppCatalogResult is one complete, bounded snapshot of every app in
// which an administrator may manage retained knowledge, including archived
// apps. Complete is false whenever more than maximum rows exist.
type KnowledgeAppCatalogResult struct {
	AppIDs   []string
	Complete bool
}

// KnowledgeAppCatalog is deliberately distinct from the active-only browser
// bootstrap catalog. Retained objects in archived apps remain manageable.
type KnowledgeAppCatalog interface {
	ListKnowledgeApps(
		context.Context,
		string,
		uint32,
	) (KnowledgeAppCatalogResult, error)
}

type KnowledgeAttemptJournal = knowledgeattemptaudit.Appender

type knowledgeScopes struct {
	read  knowledgecatalog.ReadScope
	write knowledgecatalog.WriteScope
	apps  []string
}

type knowledgeRejectionBindingKind uint8

const (
	knowledgeRejectionBindingCreate knowledgeRejectionBindingKind = iota + 1
	knowledgeRejectionBindingList
	knowledgeRejectionBindingObject
)

type knowledgeRejectionBinding struct {
	kind     knowledgeRejectionBindingKind
	scopes   knowledgeScopes
	appID    string
	targetID string
}

func (handler *apiHandler) knowledgeScopes(
	request *http.Request,
) (knowledgeScopes, error) {
	principal, ok := browserPrincipalFromRequest(request)
	if !ok || !principal.IsAdministrator() {
		return knowledgeScopes{}, errors.New("knowledge principal is unavailable")
	}
	if handler == nil || isNilDependency(handler.knowledgeApps) {
		return knowledgeScopes{}, errors.New("knowledge app catalog is unavailable")
	}
	result, err := handler.knowledgeApps.ListKnowledgeApps(
		request.Context(),
		principal.TenantID(),
		maximumKnowledgeApps,
	)
	if err != nil {
		return knowledgeScopes{}, err
	}
	if !result.Complete || len(result.AppIDs) == 0 ||
		len(result.AppIDs) > maximumKnowledgeApps {
		return knowledgeScopes{}, errors.New("knowledge app catalog is incomplete")
	}
	apps := slices.Clone(result.AppIDs)
	for _, appID := range apps {
		if !validKnowledgeIdentity(appID, maximumKnowledgeAppIDBytes) {
			return knowledgeScopes{}, errors.New("knowledge app catalog is invalid")
		}
	}
	slices.Sort(apps)
	// The simple adjacent pass is kept separate from normalization so a corrupt
	// trusted catalog cannot be silently repaired by scope compaction.
	for index := 1; index < len(apps); index++ {
		if apps[index-1] == apps[index] {
			return knowledgeScopes{}, errors.New("knowledge app catalog contains duplicates")
		}
	}
	read := knowledgecatalog.ReadScope{
		TenantID:       principal.TenantID(),
		OwnerID:        principal.OwnerID(),
		ReadableAppIDs: slices.Clone(apps),
	}
	write := knowledgecatalog.WriteScope{
		TenantID:       principal.TenantID(),
		OwnerID:        principal.OwnerID(),
		WritableAppIDs: slices.Clone(apps),
	}
	if err := knowledgecatalog.ValidateWriteScope(write); err != nil {
		return knowledgeScopes{}, errors.New("knowledge app catalog write scope is invalid")
	}
	return knowledgeScopes{read: read, write: write, apps: apps}, nil
}

func (scopes knowledgeScopes) containsApp(appID string) bool {
	_, found := slices.BinarySearch(scopes.apps, appID)
	return found
}

// allowsRead is the single sharing-scope read rule. Divergence between the
// catalog, proto, and graph read paths would be an authorization bug.
func (scopes knowledgeScopes) allowsRead(
	scope knowledgecatalog.SharingScope,
	appID string,
	ownerID string,
) bool {
	switch scope {
	case knowledgecatalog.SharingScopePrivate:
		return scopes.containsApp(appID) && ownerID == scopes.read.OwnerID
	case knowledgecatalog.SharingScopeApp:
		return scopes.containsApp(appID)
	case knowledgecatalog.SharingScopeGlobal:
		// Tenant-global objects are intentionally visible from every readable
		// app. Their source app is provenance, not read authority.
		return true
	default:
		return false
	}
}

func (handler *apiHandler) getKnowledgeObject(
	request *http.Request,
	input *opensplunkv1.GetKnowledgeObjectRequest,
) (*serializedGetKnowledgeObjectResponse, error) {
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok ||
		!validKnowledgeIdentity(
			submitted.GetKnowledgeObjectId(),
			maximumKnowledgeObjectIDBytes,
		) || submitted.Version != nil &&
		(submitted.GetVersion() == 0 || submitted.GetVersion() > math.MaxInt64) {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge get request is invalid"),
			nil,
		)
	}
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge response capacity is exhausted",
			),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	if handler == nil || isNilDependency(handler.knowledgeCatalog) {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge catalog is unavailable"),
			nil,
		)
	}
	object, err := handler.knowledgeCatalog.Get(
		request.Context(),
		scopes.read,
		strings.Clone(submitted.GetKnowledgeObjectId()),
		cloneOptionalUint64(submitted.Version),
	)
	binding := knowledgeRejectionBinding{
		kind:     knowledgeRejectionBindingObject,
		scopes:   scopes,
		targetID: strings.Clone(submitted.GetKnowledgeObjectId()),
	}
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			sanitizeKnowledgeGetError(err),
			nil,
			binding,
		)
	}
	if !validKnowledgeCatalogDefinitionAuthority(object) {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge catalog returned an invalid object"),
			nil,
			binding,
		)
	}
	message, err := knowledgecatalog.ObjectToProto(object)
	if err != nil || !knowledgeObjectMatchesReadScopeAfterDefinitionAuthority(message, scopes) ||
		!knowledgeObjectMatchesGetRequest(message, submitted) {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge catalog returned an invalid object"),
			nil,
			binding,
		)
	}
	if err := request.Context().Err(); err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			err,
			knowledgeAuthorizedContext(object),
			binding,
		)
	}
	transferred = true
	return &serializedGetKnowledgeObjectResponse{
		message: &opensplunkv1.GetKnowledgeObjectResponse{
			KnowledgeObject: message,
		},
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) listKnowledgeObjects(
	request *http.Request,
	input *opensplunkv1.ListKnowledgeObjectsRequest,
) (*serializedListKnowledgeObjectsResponse, error) {
	if !knowledgeListRequestPreflight(input) {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge list request is invalid"),
			nil,
		)
	}
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge response capacity is exhausted",
			),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge list request is invalid"),
			nil,
		)
	}
	listRequest, err := knowledgeListRequest(submitted)
	if err != nil {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge list request is invalid"),
			nil,
		)
	}
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return nil, handler.rejectKnowledgeScopeError(request)
	}
	binding := knowledgeRejectionBinding{
		kind:   knowledgeRejectionBindingList,
		scopes: scopes,
	}
	listRequest, err = knowledgecatalog.NormalizeListRequest(
		scopes.read,
		listRequest,
	)
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			err,
			nil,
			binding,
		)
	}
	if listRequest.AppIDFilter != nil {
		binding.appID = strings.Clone(*listRequest.AppIDFilter)
	}
	if handler == nil || isNilDependency(handler.knowledgeCatalog) {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge catalog is unavailable"),
			nil,
		)
	}
	page, err := handler.knowledgeCatalog.List(
		request.Context(),
		scopes.read,
		cloneKnowledgeListRequest(listRequest),
	)
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			sanitizeKnowledgeListError(err),
			nil,
			binding,
		)
	}
	message, err := knowledgeListPageToProto(scopes, listRequest, page)
	if err != nil {
		return nil, handler.rejectKnowledgeOperationError(request, err, nil, binding)
	}
	if err := request.Context().Err(); err != nil {
		return nil, handler.rejectKnowledgeOperationError(request, err, nil, binding)
	}
	transferred = true
	return &serializedListKnowledgeObjectsResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) createKnowledgeObject(
	request *http.Request,
	input *opensplunkv1.CreateKnowledgeObjectRequest,
) (*serializedCreateKnowledgeObjectResponse, error) {
	return knowledgeMutationResponse(
		handler,
		request,
		input,
		func(scopes knowledgeScopes, serviceRequest *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
			if handler.knowledgeWriter == nil {
				return nil, errors.New("knowledge writer is unavailable")
			}
			return handler.knowledgeWriter.Create(request.Context(), scopes.write, serviceRequest)
		},
		func(
			response *opensplunkv1.CreateKnowledgeObjectResponse,
			submitted *opensplunkv1.CreateKnowledgeObjectRequest,
			scopes knowledgeScopes,
		) (*opensplunkv1.CreateKnowledgeObjectResponse, bool) {
			if response == nil ||
				!validKnowledgeProtoDefinitionAuthority(response.GetKnowledgeObject()) {
				return nil, false
			}
			cloned, ok := cloneKnowledgeMessageBounded(
				response,
				maximumKnowledgeObjectResponseBytes,
			)
			return cloned, ok && validKnowledgeCreateResponseAfterDefinitionAuthorityForPolicy(
				cloned,
				submitted,
				scopes,
				replaysUnavailableActiveMutations(handler.knowledgeWriter),
			)
		},
		func(
			message *opensplunkv1.CreateKnowledgeObjectResponse,
			ctx context.Context,
			release func(),
		) *serializedCreateKnowledgeObjectResponse {
			return &serializedCreateKnowledgeObjectResponse{
				message: message,
				ctx:     ctx,
				release: release,
			}
		},
	)
}

func (handler *apiHandler) updateKnowledgeObject(
	request *http.Request,
	input *opensplunkv1.UpdateKnowledgeObjectRequest,
) (*serializedUpdateKnowledgeObjectResponse, error) {
	return knowledgeMutationResponse(
		handler,
		request,
		input,
		func(scopes knowledgeScopes, serviceRequest *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
			if handler.knowledgeWriter == nil {
				return nil, errors.New("knowledge writer is unavailable")
			}
			return handler.knowledgeWriter.Update(request.Context(), scopes.write, serviceRequest)
		},
		func(
			response *opensplunkv1.UpdateKnowledgeObjectResponse,
			submitted *opensplunkv1.UpdateKnowledgeObjectRequest,
			scopes knowledgeScopes,
		) (*opensplunkv1.UpdateKnowledgeObjectResponse, bool) {
			if response == nil ||
				!validKnowledgeProtoDefinitionAuthority(response.GetKnowledgeObject()) {
				return nil, false
			}
			cloned, ok := cloneKnowledgeMessageBounded(
				response,
				maximumKnowledgeObjectResponseBytes,
			)
			return cloned, ok && validKnowledgeUpdateResponseAfterDefinitionAuthorityForPolicy(
				cloned,
				submitted,
				scopes,
				replaysUnavailableActiveMutations(handler.knowledgeWriter),
			)
		},
		func(
			message *opensplunkv1.UpdateKnowledgeObjectResponse,
			ctx context.Context,
			release func(),
		) *serializedUpdateKnowledgeObjectResponse {
			return &serializedUpdateKnowledgeObjectResponse{
				message: message,
				ctx:     ctx,
				release: release,
			}
		},
	)
}

func (handler *apiHandler) setKnowledgeObjectState(
	request *http.Request,
	input *opensplunkv1.SetKnowledgeObjectStateRequest,
) (*serializedSetKnowledgeObjectStateResponse, error) {
	return knowledgeMutationResponse(
		handler,
		request,
		input,
		func(scopes knowledgeScopes, serviceRequest *opensplunkv1.SetKnowledgeObjectStateRequest) (*opensplunkv1.SetKnowledgeObjectStateResponse, error) {
			if handler.knowledgeWriter == nil {
				return nil, errors.New("knowledge writer is unavailable")
			}
			return handler.knowledgeWriter.SetState(request.Context(), scopes.write, serviceRequest)
		},
		func(
			response *opensplunkv1.SetKnowledgeObjectStateResponse,
			submitted *opensplunkv1.SetKnowledgeObjectStateRequest,
			scopes knowledgeScopes,
		) (*opensplunkv1.SetKnowledgeObjectStateResponse, bool) {
			if response == nil ||
				!validKnowledgeProtoDefinitionAuthority(response.GetKnowledgeObject()) {
				return nil, false
			}
			cloned, ok := cloneKnowledgeMessageBounded(
				response,
				maximumKnowledgeObjectResponseBytes,
			)
			return cloned, ok && validKnowledgeSetStateResponseAfterDefinitionAuthorityForPolicy(
				cloned,
				submitted,
				scopes,
				replaysUnavailableActiveMutations(handler.knowledgeWriter),
			)
		},
		func(
			message *opensplunkv1.SetKnowledgeObjectStateResponse,
			ctx context.Context,
			release func(),
		) *serializedSetKnowledgeObjectStateResponse {
			return &serializedSetKnowledgeObjectStateResponse{
				message: message,
				ctx:     ctx,
				release: release,
			}
		},
	)
}

func (handler *apiHandler) deleteKnowledgeObject(
	request *http.Request,
	input *opensplunkv1.DeleteKnowledgeObjectRequest,
) (*serializedDeleteKnowledgeObjectResponse, error) {
	return knowledgeMutationResponse(
		handler,
		request,
		input,
		func(scopes knowledgeScopes, serviceRequest *opensplunkv1.DeleteKnowledgeObjectRequest) (*opensplunkv1.DeleteKnowledgeObjectResponse, error) {
			if handler.knowledgeWriter == nil {
				return nil, errors.New("knowledge writer is unavailable")
			}
			return handler.knowledgeWriter.Delete(request.Context(), scopes.write, serviceRequest)
		},
		func(
			response *opensplunkv1.DeleteKnowledgeObjectResponse,
			submitted *opensplunkv1.DeleteKnowledgeObjectRequest,
			scopes knowledgeScopes,
		) (*opensplunkv1.DeleteKnowledgeObjectResponse, bool) {
			cloned, ok := cloneKnowledgeMessageBounded(
				response,
				maximumKnowledgeObjectResponseBytes,
			)
			return cloned, ok && validKnowledgeDeleteResponse(cloned, submitted, scopes)
		},
		func(
			message *opensplunkv1.DeleteKnowledgeObjectResponse,
			ctx context.Context,
			release func(),
		) *serializedDeleteKnowledgeObjectResponse {
			return &serializedDeleteKnowledgeObjectResponse{
				message: message,
				ctx:     ctx,
				release: release,
			}
		},
	)
}

func knowledgeMutationResponse[
	Request proto.Message,
	Message proto.Message,
	Serialized any,
](
	handler *apiHandler,
	request *http.Request,
	input Request,
	mutate func(knowledgeScopes, Request) (Message, error),
	validate func(Message, Request, knowledgeScopes) (Message, bool),
	serialize func(Message, context.Context, func()) Serialized,
) (Serialized, error) {
	var zero Serialized
	release, ok := handler.acquireSerialization()
	if !ok {
		return zero, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge response capacity is exhausted",
			),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok {
		return zero, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge request is invalid"),
			nil,
		)
	}
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return zero, handler.rejectKnowledgeScopeError(request)
	}
	if err := validateKnowledgeMutationRequest(submitted); err != nil {
		return zero, handler.rejectKnowledgeOperationError(
			request,
			err,
			nil,
		)
	}
	if err := refineKnowledgeMutationAttempt(request, submitted); err != nil {
		return zero, handler.rejectKnowledgeOperationError(
			request,
			err,
			nil,
		)
	}
	binding := knowledgeRejectionBindingForMutation(scopes, submitted)
	if handler == nil || isNilDependency(handler.knowledgeWriter) {
		return zero, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge writer is unavailable"),
			nil,
		)
	}
	if knowledgeMutationRequiresActivePublication(submitted) &&
		!replaysUnavailableActiveMutations(handler.knowledgeWriter) {
		return zero, handler.rejectKnowledgeOperationError(
			request,
			knowledgecatalog.ErrActivePublicationUnavailable,
			nil,
			binding,
		)
	}
	response, err := mutate(scopes, input)
	if err != nil {
		return zero, handler.rejectKnowledgeMutationError(
			request,
			err,
			binding,
		)
	}
	// A nil error from Writer means its transaction committed or an exact
	// replay completed. From here onward, response validation or transport loss
	// must never create a false rejected-attempt row.
	markKnowledgeMutationCommitted(request)
	validated, ok := validate(response, submitted, scopes)
	if !ok {
		return zero, unavailableError("knowledge management is unavailable")
	}
	transferred = true
	return serialize(
		validated,
		context.WithoutCancel(request.Context()),
		release,
	), nil
}

func validateKnowledgeMutationRequest[Request proto.Message](
	request Request,
) error {
	switch typed := any(request).(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		return knowledgecatalog.ValidateCreateKnowledgeObjectRequest(typed)
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		return knowledgecatalog.ValidateUpdateKnowledgeObjectRequest(typed)
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		return knowledgecatalog.ValidateSetKnowledgeObjectStateRequest(typed)
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		return knowledgecatalog.ValidateDeleteKnowledgeObjectRequest(typed)
	default:
		return control.ErrInvalidArgument
	}
}

func refineKnowledgeMutationAttempt[Request proto.Message](
	request *http.Request,
	input Request,
) error {
	switch typed := any(input).(type) {
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		changesScope, err := knowledgecatalog.UpdateChangesSharingScope(
			typed.GetUpdateMask(),
		)
		if err != nil {
			return err
		}
		if changesScope {
			refineKnowledgeAttempt(request, knowledgeattemptaudit.ActionScopeChange)
		}
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		switch typed.GetState() {
		case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
			refineKnowledgeAttempt(request, knowledgeattemptaudit.ActionEnable)
		case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
			refineKnowledgeAttempt(request, knowledgeattemptaudit.ActionDisable)
		}
	}
	return nil
}

func knowledgeMutationRequiresActivePublication[Request proto.Message](
	request Request,
) bool {
	switch typed := any(request).(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		return typed.GetInitialState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		return typed.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	default:
		return false
	}
}

func (handler *apiHandler) rejectKnowledgeRequest(
	request *http.Request,
	reason knowledgeattemptaudit.Reason,
	httpErr error,
	authorized *knowledgecatalog.AuthorizedContext,
) error {
	status, message, ok := knowledgeHTTPError(httpErr)
	if !ok {
		reason = knowledgeattemptaudit.ReasonServiceUnavailable
		status = http.StatusServiceUnavailable
		message = knowledgeManagementUnavailableText
		authorized = nil
	}
	if context, valid := knowledgeAttemptAuthorizedContext(authorized); valid {
		setKnowledgeAttemptAuthorizedContext(request, context)
	} else if authorized != nil {
		reason = knowledgeattemptaudit.ReasonServiceUnavailable
		status = http.StatusServiceUnavailable
		message = knowledgeManagementUnavailableText
	}
	return rejectKnowledgeAttempt(request, reason, status, message)
}

func (handler *apiHandler) rejectKnowledgeScopeError(
	request *http.Request,
) error {
	return handler.rejectKnowledgeRequest(
		request,
		knowledgeattemptaudit.ReasonServiceUnavailable,
		unavailableError(knowledgeManagementUnavailableText),
		nil,
	)
}

func (handler *apiHandler) rejectKnowledgeOperationError(
	request *http.Request,
	operationErr error,
	fallback *knowledgecatalog.AuthorizedContext,
	bindings ...knowledgeRejectionBinding,
) error {
	reason, status, message, includeContext := knowledgeOperationHTTPError(operationErr)
	authorized := fallback
	if carried, found := knowledgecatalog.AuthorizedContextFromError(operationErr); found {
		authorized = &carried
	}
	if !includeContext {
		authorized = nil
	}
	if !knowledgeRejectionContextSatisfies(
		request,
		reason,
		authorized,
		bindings,
	) {
		reason = knowledgeattemptaudit.ReasonServiceUnavailable
		status = http.StatusServiceUnavailable
		message = knowledgeManagementUnavailableText
		authorized = nil
	}
	return handler.rejectKnowledgeRequest(
		request,
		reason,
		router.NewHTTPError(status, message),
		authorized,
	)
}

func knowledgeRejectionContextSatisfies(
	request *http.Request,
	reason knowledgeattemptaudit.Reason,
	authorized *knowledgecatalog.AuthorizedContext,
	bindings []knowledgeRejectionBinding,
) bool {
	validationCreateDependency := false
	if reason == knowledgeattemptaudit.ReasonForbiddenDependency &&
		len(bindings) == 1 &&
		bindings[0].kind == knowledgeRejectionBindingCreate {
		action, found := knowledgeAttemptActionFromRequest(request)
		validationCreateDependency = found &&
			(action == knowledgeattemptaudit.ActionValidate ||
				action == knowledgeattemptaudit.ActionPreview)
	}
	requiresObject := reason == knowledgeattemptaudit.ReasonVersionConflict ||
		reason == knowledgeattemptaudit.ReasonForbiddenDependency &&
			!validationCreateDependency
	requiresContext := requiresObject ||
		validationCreateDependency ||
		reason == knowledgeattemptaudit.ReasonIdempotencyConflict
	if authorized == nil {
		return !requiresContext
	}
	if len(bindings) != 1 || !bindings[0].scopes.containsApp(authorized.AppID) {
		return false
	}
	binding := bindings[0]
	switch binding.kind {
	case knowledgeRejectionBindingCreate:
		if authorized.Object != nil {
			return false
		}
		// An idempotency conflict authorizes the receipt's existing app,
		// which may legitimately differ from the newly submitted app that
		// reused its key. Every other Create rejection binds the attempted
		// normalized definition app exactly.
		if reason != knowledgeattemptaudit.ReasonIdempotencyConflict &&
			authorized.AppID != binding.appID {
			return false
		}
	case knowledgeRejectionBindingList:
		if authorized.Object != nil {
			return false
		}
		if binding.appID != "" && authorized.AppID != binding.appID {
			return false
		}
	case knowledgeRejectionBindingObject:
		if authorized.Object == nil {
			return false
		}
		// An idempotency conflict authorizes the receipt's existing object,
		// which may legitimately differ from the newly submitted target that
		// reused its key. Other object-context reasons must bind the attempt.
		if reason != knowledgeattemptaudit.ReasonIdempotencyConflict &&
			authorized.Object.KnowledgeObjectID != binding.targetID {
			return false
		}
	default:
		return false
	}
	switch reason {
	case knowledgeattemptaudit.ReasonVersionConflict:
		return authorized.Object != nil
	case knowledgeattemptaudit.ReasonForbiddenDependency:
		if validationCreateDependency {
			return authorized.Object == nil
		}
		return authorized.Object != nil
	case knowledgeattemptaudit.ReasonIdempotencyConflict:
		action, found := knowledgeAttemptActionFromRequest(request)
		if !found {
			return false
		}
		if action == knowledgeattemptaudit.ActionCreate {
			return authorized.Object == nil
		}
		switch action {
		case knowledgeattemptaudit.ActionUpdate,
			knowledgeattemptaudit.ActionScopeChange,
			knowledgeattemptaudit.ActionEnable,
			knowledgeattemptaudit.ActionDisable,
			knowledgeattemptaudit.ActionDelete:
			return authorized.Object != nil
		default:
			return false
		}
	default:
		return true
	}
}

func (handler *apiHandler) rejectKnowledgeMutationError(
	request *http.Request,
	operationErr error,
	binding knowledgeRejectionBinding,
) error {
	disposition, found := knowledgecatalog.ErrorDispositionFromError(operationErr)
	if !found || disposition == knowledgecatalog.ErrorDispositionIndeterminate {
		markKnowledgeMutationIndeterminate(request)
		return unavailableError(knowledgeManagementUnavailableText)
	}
	if disposition == knowledgecatalog.ErrorDispositionKnownCommitted {
		markKnowledgeMutationCommitted(request)
		return unavailableError(knowledgeManagementUnavailableText)
	}
	if disposition != knowledgecatalog.ErrorDispositionDefinitiveRejection {
		markKnowledgeMutationIndeterminate(request)
		return unavailableError(knowledgeManagementUnavailableText)
	}
	return handler.rejectKnowledgeOperationError(request, operationErr, nil, binding)
}

func knowledgeRejectionBindingForMutation[Request proto.Message](
	scopes knowledgeScopes,
	request Request,
) knowledgeRejectionBinding {
	binding := knowledgeRejectionBinding{scopes: scopes}
	switch typed := any(request).(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		binding.kind = knowledgeRejectionBindingCreate
		if normalized, err := knowledgedefinition.Normalize(typed.GetDefinition()); err == nil {
			binding.appID = strings.Clone(normalized.AppID)
		}
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		binding.kind = knowledgeRejectionBindingObject
		binding.targetID = strings.Clone(typed.GetKnowledgeObjectId())
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		binding.kind = knowledgeRejectionBindingObject
		binding.targetID = strings.Clone(typed.GetKnowledgeObjectId())
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		binding.kind = knowledgeRejectionBindingObject
		binding.targetID = strings.Clone(typed.GetKnowledgeObjectId())
	}
	return binding
}

func knowledgeHTTPError(err error) (int, string, bool) {
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) || httpErr == nil ||
		httpErr.StatusCode < 400 || httpErr.StatusCode > 599 ||
		httpErr.Message == "" {
		return 0, "", false
	}
	return httpErr.StatusCode, httpErr.Message, true
}

func knowledgeOperationHTTPError(
	err error,
) (knowledgeattemptaudit.Reason, int, string, bool) {
	const (
		notFoundMessage = "knowledge object not found"
		conflictMessage = "knowledge request conflicts with current state"
		invalidMessage  = "knowledge request is invalid"
		capacityMessage = "knowledge capacity is exhausted"
		canceledMessage = "knowledge request was canceled"
	)
	switch {
	case errors.Is(err, control.ErrNotFound),
		errors.Is(err, knowledgecatalog.ErrIdempotentOutcomeRedacted):
		return knowledgeattemptaudit.ReasonNotFoundOrForbidden,
			http.StatusNotFound, notFoundMessage, false
	case errors.Is(err, knowledgecatalog.ErrIdempotencyConflict):
		return knowledgeattemptaudit.ReasonIdempotencyConflict,
			http.StatusConflict, conflictMessage, true
	case errors.Is(err, control.ErrVersionConflict):
		if authorized, found := knowledgecatalog.AuthorizedContextFromError(err); !found || authorized.Object == nil {
			return knowledgeattemptaudit.ReasonServiceUnavailable,
				http.StatusServiceUnavailable,
				knowledgeManagementUnavailableText,
				false
		}
		return knowledgeattemptaudit.ReasonVersionConflict,
			http.StatusConflict, conflictMessage, true
	case errors.Is(err, control.ErrInvalidArgument),
		errors.Is(err, knowledgecatalog.ErrInvalidCursor):
		return knowledgeattemptaudit.ReasonInvalidDefinition,
			http.StatusBadRequest, invalidMessage, true
	case errors.Is(err, control.ErrDependencyConflict):
		return knowledgeattemptaudit.ReasonForbiddenDependency,
			http.StatusConflict, conflictMessage, true
	case errors.Is(err, control.ErrCapacityExceeded):
		return knowledgeattemptaudit.ReasonResourceLimit,
			http.StatusTooManyRequests, capacityMessage, true
	case errors.Is(err, control.ErrPageInvalidated):
		return knowledgeattemptaudit.ReasonServiceUnavailable,
			http.StatusConflict, conflictMessage, false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return knowledgeattemptaudit.ReasonServiceUnavailable,
			http.StatusRequestTimeout, canceledMessage, true
	default:
		return knowledgeattemptaudit.ReasonServiceUnavailable,
			http.StatusServiceUnavailable,
			knowledgeManagementUnavailableText,
			true
	}
}

func sanitizeKnowledgeGetError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, control.ErrInvalidArgument) ||
		errors.Is(err, knowledgecatalog.ErrInvalidCursor) ||
		errors.Is(err, control.ErrPageInvalidated) ||
		errors.Is(err, control.ErrVersionConflict) ||
		errors.Is(err, control.ErrDependencyConflict) ||
		errors.Is(err, control.ErrCapacityExceeded) ||
		errors.Is(err, knowledgecatalog.ErrIdempotencyConflict) ||
		errors.Is(err, knowledgecatalog.ErrIdempotentOutcomeRedacted) {
		return errors.New("knowledge catalog Get returned an impossible error")
	}
	return err
}

func sanitizeKnowledgeListError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, control.ErrInvalidArgument) ||
		errors.Is(err, control.ErrNotFound) ||
		errors.Is(err, control.ErrVersionConflict) ||
		errors.Is(err, control.ErrDependencyConflict) ||
		errors.Is(err, control.ErrCapacityExceeded) ||
		errors.Is(err, knowledgecatalog.ErrIdempotencyConflict) ||
		errors.Is(err, knowledgecatalog.ErrIdempotentOutcomeRedacted) {
		return errors.New("knowledge catalog List returned an impossible error")
	}
	return err
}

func knowledgeAttemptAuthorizedContext(
	value *knowledgecatalog.AuthorizedContext,
) (*knowledgeattemptaudit.AuthorizedContext, bool) {
	if value == nil {
		return nil, true
	}
	if !validKnowledgeIdentity(value.AppID, maximumKnowledgeAppIDBytes) {
		return nil, false
	}
	converted := &knowledgeattemptaudit.AuthorizedContext{
		AppID: strings.Clone(value.AppID),
	}
	if value.Object == nil {
		return converted, true
	}
	if !validKnowledgeIdentity(
		value.Object.KnowledgeObjectID,
		maximumKnowledgeObjectIDBytes,
	) || !value.Object.ObjectType.Valid() ||
		!value.Object.SharingScope.Valid() || value.Object.Version == 0 ||
		value.Object.Version > math.MaxInt64 {
		return nil, false
	}
	converted.Object = &knowledgeattemptaudit.AuthorizedObject{
		KnowledgeObjectID: strings.Clone(value.Object.KnowledgeObjectID),
		ObjectType:        value.Object.ObjectType,
		Version:           value.Object.Version,
		SharingScope:      value.Object.SharingScope,
	}
	return converted, true
}

func validKnowledgeMutationObjectResponse(
	object *opensplunkv1.KnowledgeObject,
	revision uint64,
	token []byte,
	scopes knowledgeScopes,
) bool {
	return validKnowledgeMutationObjectResponseAfterDefinitionAuthority(
		object,
		revision,
		token,
		scopes,
	) && validKnowledgeProtoDefinitionAuthority(object)
}

func validKnowledgeMutationObjectResponseAfterDefinitionAuthority(
	object *opensplunkv1.KnowledgeObject,
	revision uint64,
	token []byte,
	scopes knowledgeScopes,
) bool {
	return revision > 0 && revision <= math.MaxInt64 && len(token) == 32 &&
		validKnowledgeObjectScalarLifecycleEnvelope(object) &&
		revision >= object.GetVersion() &&
		object.GetTenantId() == scopes.write.TenantID &&
		object.GetOwnerId() == scopes.write.OwnerID &&
		scopes.containsApp(object.GetAppId())
}

func validKnowledgeCreateResponseAfterDefinitionAuthorityForPolicy(
	response *opensplunkv1.CreateKnowledgeObjectResponse,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
) bool {
	return validKnowledgeCreateResponseWithPolicy(
		response,
		request,
		scopes,
		allowUnavailableActiveReplay,
		true,
	)
}

func validKnowledgeCreateResponseWithPolicy(
	response *opensplunkv1.CreateKnowledgeObjectResponse,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
	definitionAuthorityValidated bool,
) bool {
	validObject := validKnowledgeMutationObjectResponse
	if definitionAuthorityValidated {
		validObject = validKnowledgeMutationObjectResponseAfterDefinitionAuthority
	}
	if response == nil || request == nil || request.GetDefinition() == nil ||
		request.GetInitialState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE &&
			!allowUnavailableActiveReplay ||
		!validObject(
			response.GetKnowledgeObject(),
			response.GetTenantCatalogRevision(),
			response.GetTenantCatalogStateToken(),
			scopes,
		) {
		return false
	}
	object := response.GetKnowledgeObject()
	normalized, err := knowledgedefinition.Normalize(request.GetDefinition())
	if err != nil {
		return false
	}
	return object.GetVersion() == 1 &&
		object.GetState() == request.GetInitialState() &&
		object.GetAppId() == normalized.AppID &&
		object.GetName() == normalized.Name &&
		object.GetSharingScope() == normalized.SharingScope &&
		object.GetObjectType() == normalized.ObjectType &&
		proto.Equal(object.GetDefinition(), normalized.Definition) &&
		object.GetCreatedAt().AsTime().Equal(object.GetUpdatedAt().AsTime())
}

func validKnowledgeUpdateResponseAfterDefinitionAuthorityForPolicy(
	response *opensplunkv1.UpdateKnowledgeObjectResponse,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
) bool {
	return validKnowledgeUpdateResponseWithPolicy(
		response,
		request,
		scopes,
		allowUnavailableActiveReplay,
		true,
	)
}

func validKnowledgeUpdateResponseWithPolicy(
	response *opensplunkv1.UpdateKnowledgeObjectResponse,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
	definitionAuthorityValidated bool,
) bool {
	validObject := validKnowledgeMutationObjectResponse
	if definitionAuthorityValidated {
		validObject = validKnowledgeMutationObjectResponseAfterDefinitionAuthority
	}
	if response == nil || request == nil ||
		request.GetExpectedVersion() == 0 ||
		request.GetExpectedVersion() >= math.MaxInt64 ||
		!validObject(
			response.GetKnowledgeObject(),
			response.GetTenantCatalogRevision(),
			response.GetTenantCatalogStateToken(),
			scopes,
		) {
		return false
	}
	object := response.GetKnowledgeObject()
	return object.GetKnowledgeObjectId() == request.GetKnowledgeObjectId() &&
		object.GetVersion() == request.GetExpectedVersion()+1 &&
		(object.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT ||
			object.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED ||
			allowUnavailableActiveReplay &&
				object.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE) &&
		knowledgecatalog.UpdateResultMatchesRequest(
			object.GetDefinition(),
			request.GetDefinition(),
			request.GetUpdateMask(),
			object.GetState(),
		)
}

func validKnowledgeSetStateResponseAfterDefinitionAuthorityForPolicy(
	response *opensplunkv1.SetKnowledgeObjectStateResponse,
	request *opensplunkv1.SetKnowledgeObjectStateRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
) bool {
	return validKnowledgeSetStateResponseWithPolicy(
		response,
		request,
		scopes,
		allowUnavailableActiveReplay,
		true,
	)
}

func validKnowledgeSetStateResponseWithPolicy(
	response *opensplunkv1.SetKnowledgeObjectStateResponse,
	request *opensplunkv1.SetKnowledgeObjectStateRequest,
	scopes knowledgeScopes,
	allowUnavailableActiveReplay bool,
	definitionAuthorityValidated bool,
) bool {
	validObject := validKnowledgeMutationObjectResponse
	if definitionAuthorityValidated {
		validObject = validKnowledgeMutationObjectResponseAfterDefinitionAuthority
	}
	if response == nil || request == nil ||
		request.GetExpectedVersion() == 0 ||
		request.GetExpectedVersion() >= math.MaxInt64 ||
		!validObject(
			response.GetKnowledgeObject(),
			response.GetTenantCatalogRevision(),
			response.GetTenantCatalogStateToken(),
			scopes,
		) {
		return false
	}
	object := response.GetKnowledgeObject()
	if object.GetKnowledgeObjectId() != request.GetKnowledgeObjectId() ||
		object.GetVersion() != request.GetExpectedVersion()+1 ||
		object.GetState() != request.GetState() {
		return false
	}
	if request.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		return allowUnavailableActiveReplay
	}
	return request.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED &&
		validKnowledgeLifecycleTimestamp(
			object.GetDisabledAt(),
			object.GetUpdatedAt().AsTime(),
		)
}

func validKnowledgeDeleteResponse(
	response *opensplunkv1.DeleteKnowledgeObjectResponse,
	request *opensplunkv1.DeleteKnowledgeObjectRequest,
	scopes knowledgeScopes,
) bool {
	return response != nil && request != nil &&
		request.GetExpectedVersion() > 0 &&
		request.GetExpectedVersion() < math.MaxInt64 &&
		validKnowledgeIdentity(response.GetKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) &&
		response.GetKnowledgeObjectId() == request.GetKnowledgeObjectId() &&
		response.GetDeletedVersion() == request.GetExpectedVersion()+1 &&
		response.GetTenantCatalogRevision() > 0 &&
		response.GetTenantCatalogRevision() <= math.MaxInt64 &&
		response.GetTenantCatalogRevision() >= response.GetDeletedVersion() &&
		len(response.GetTenantCatalogStateToken()) == 32 &&
		len(scopes.apps) != 0
}

// validKnowledgeObjectScalarLifecycleEnvelope validates every response
// authority except the definition body and digest. Callers may use it only
// after validKnowledgeCatalogDefinitionAuthority or
// validKnowledgeProtoDefinitionAuthority succeeded before a potentially large
// definition was cloned.
func validKnowledgeObjectScalarLifecycleEnvelope(
	object *opensplunkv1.KnowledgeObject,
) bool {
	if object == nil ||
		!validKnowledgeIdentity(object.GetKnowledgeObjectId(), maximumKnowledgeObjectIDBytes) ||
		!validKnowledgeIdentity(object.GetTenantId(), maximumKnowledgeIdentityBytes) ||
		!validKnowledgeIdentity(object.GetAppId(), maximumKnowledgeAppIDBytes) ||
		!validKnowledgeIdentity(object.GetOwnerId(), maximumKnowledgeIdentityBytes) ||
		!validKnowledgeIdentity(object.GetName(), maximumKnowledgeNameBytes) ||
		object.GetVersion() == 0 || object.GetVersion() > math.MaxInt64 {
		return false
	}
	if _, ok := knowledgeObjectTypeFromProto(object.GetObjectType()); !ok {
		return false
	}
	if _, ok := knowledgeStateFromProto(object.GetState()); !ok {
		return false
	}
	if _, ok := knowledgeSharingScopeFromProto(object.GetSharingScope()); !ok {
		return false
	}
	if object.GetCreatedAt() == nil || object.GetUpdatedAt() == nil ||
		object.GetCreatedAt().CheckValid() != nil || object.GetUpdatedAt().CheckValid() != nil ||
		object.GetCreatedAt().GetNanos()%1_000 != 0 || object.GetUpdatedAt().GetNanos()%1_000 != 0 ||
		object.GetUpdatedAt().AsTime().Before(object.GetCreatedAt().AsTime()) {
		return false
	}
	updated := object.GetUpdatedAt().AsTime()
	switch object.GetState() {
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
		if object.GetDisabledAt() != nil || object.GetQuarantinedAt() != nil ||
			object.GetDeletedAt() != nil || object.QuarantineReason != nil {
			return false
		}
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
		if !validKnowledgeLifecycleTimestampRange(
			object.GetDisabledAt(),
			object.GetCreatedAt().AsTime(),
			updated,
		) ||
			object.GetQuarantinedAt() != nil || object.GetDeletedAt() != nil ||
			object.QuarantineReason != nil {
			return false
		}
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED:
		return object.GetDefinition() == nil && len(object.GetDefinitionSha256()) == 0 &&
			object.GetDisabledAt() == nil && object.GetDeletedAt() == nil &&
			validKnowledgeLifecycleTimestamp(object.GetQuarantinedAt(), updated) &&
			object.QuarantineReason != nil &&
			(object.GetQuarantineReason() == "root_corruption" ||
				object.GetQuarantineReason() == "dependency_recovery")
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED:
		if object.GetDisabledAt() != nil || object.GetQuarantinedAt() != nil ||
			!validKnowledgeLifecycleTimestamp(object.GetDeletedAt(), updated) ||
			object.QuarantineReason != nil {
			return false
		}
	default:
		return false
	}
	return true
}

func knowledgeDefinitionObjectType(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (opensplunkv1.KnowledgeObjectType, bool) {
	switch definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, true
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, true
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, true
	default:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED, false
	}
}

func validKnowledgeLifecycleTimestamp(
	value *timestamppb.Timestamp,
	want time.Time,
) bool {
	return value != nil && value.CheckValid() == nil &&
		value.GetNanos()%1_000 == 0 && value.AsTime().Equal(want)
}

func validKnowledgeLifecycleTimestampRange(
	value *timestamppb.Timestamp,
	minimum time.Time,
	maximum time.Time,
) bool {
	return value != nil && value.CheckValid() == nil &&
		value.GetNanos()%1_000 == 0 &&
		!value.AsTime().Before(minimum) && !value.AsTime().After(maximum)
}

func validKnowledgeIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
		trimKnowledgeASCIIWhitespace(value) != value {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return false
		}
	}
	return true
}

func knowledgeListRequest(
	input *opensplunkv1.ListKnowledgeObjectsRequest,
) (knowledgecatalog.ListRequest, error) {
	if !knowledgeListRequestPreflight(input) {
		return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
	}
	request := knowledgecatalog.ListRequest{}
	if input.Page != nil {
		request.IncludeTotal = input.Page.GetIncludeTotalSize()
		if input.Page.PageSize != nil {
			request.PageSize = input.Page.GetPageSize()
		}
		if input.Page.PageToken != nil {
			if len(input.Page.GetPageToken()) > maximumKnowledgePageTokenBytes {
				return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
			}
			request.PageToken = strings.Clone(input.Page.GetPageToken())
		}
	}
	request.AppIDFilter = cloneOptionalString(input.AppIdFilter)
	request.OwnerIDFilter = cloneOptionalString(input.OwnerIdFilter)
	request.TextFilter = cloneOptionalString(input.TextFilter)
	request.SelectorTextFilter = cloneOptionalString(input.SelectorTextFilter)
	for _, objectType := range input.GetObjectTypeFilters() {
		converted, ok := knowledgeObjectTypeFromProto(objectType)
		if !ok {
			return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
		}
		request.ObjectTypeFilters = append(request.ObjectTypeFilters, converted)
	}
	for _, state := range input.GetStateFilters() {
		converted, ok := knowledgeStateFromProto(state)
		if !ok {
			return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
		}
		request.StateFilters = append(request.StateFilters, converted)
	}
	for _, scope := range input.GetSharingScopeFilters() {
		converted, ok := knowledgeSharingScopeFromProto(scope)
		if !ok {
			return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
		}
		request.SharingScopeFilters = append(request.SharingScopeFilters, converted)
	}
	switch input.GetSortBy() {
	case opensplunkv1.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_UNSPECIFIED:
	case opensplunkv1.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_NAME:
		request.SortBy = knowledgecatalog.SortByName
	case opensplunkv1.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_CREATED_AT:
		request.SortBy = knowledgecatalog.SortByCreatedAt
	case opensplunkv1.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_UPDATED_AT:
		request.SortBy = knowledgecatalog.SortByUpdatedAt
	case opensplunkv1.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_OBJECT_TYPE:
		request.SortBy = knowledgecatalog.SortByObjectType
	default:
		return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
	}
	switch input.GetSortDirection() {
	case opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED:
	case opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING:
		request.SortDirection = knowledgecatalog.SortAscending
	case opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING:
		request.SortDirection = knowledgecatalog.SortDescending
	default:
		return knowledgecatalog.ListRequest{}, control.ErrInvalidArgument
	}
	return request, nil
}

func knowledgeListRequestPreflight(
	input *opensplunkv1.ListKnowledgeObjectsRequest,
) bool {
	if input == nil ||
		len(input.GetObjectTypeFilters()) > 3 ||
		len(input.GetStateFilters()) > 5 ||
		len(input.GetSharingScopeFilters()) > 3 ||
		!knowledgeListOptionalFilterWithinLimit(
			input.AppIdFilter,
			maximumKnowledgeAppIDBytes,
		) ||
		!knowledgeListOptionalFilterWithinLimit(
			input.OwnerIdFilter,
			maximumKnowledgeIdentityBytes,
		) ||
		!knowledgeListOptionalFilterWithinLimit(
			input.TextFilter,
			maximumKnowledgeIdentityBytes,
		) ||
		!knowledgeListOptionalFilterWithinLimit(
			input.SelectorTextFilter,
			maximumKnowledgeIdentityBytes,
		) {
		return false
	}
	if input.Page == nil {
		return true
	}
	return (input.Page.PageSize == nil ||
		input.Page.GetPageSize() <= knowledgecatalog.MaximumPageSize) &&
		(input.Page.PageToken == nil ||
			len(input.Page.GetPageToken()) <= maximumKnowledgePageTokenBytes)
}

func knowledgeListOptionalFilterWithinLimit(value *string, maximumBytes int) bool {
	return value == nil || len(trimKnowledgeASCIIWhitespace(*value)) <= maximumBytes
}

func trimKnowledgeASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}

func knowledgeListPageToProto(
	scopes knowledgeScopes,
	request knowledgecatalog.ListRequest,
	page knowledgecatalog.ListPage,
) (*opensplunkv1.ListKnowledgeObjectsResponse, error) {
	minimumTotalSize := uint64(len(page.Objects))
	if request.PageToken != "" {
		minimumTotalSize++
	}
	if page.NextPageToken != "" {
		minimumTotalSize++
	}
	if request.PageSize == 0 || request.PageSize > knowledgecatalog.MaximumPageSize ||
		len(page.Objects) > int(request.PageSize) ||
		page.CatalogRevision > math.MaxInt64 ||
		request.PageToken != "" && !validBoundedListPageToken(
			request.PageToken,
			maximumKnowledgePageTokenBytes,
			false,
		) ||
		(len(page.Objects) != 0 || page.NextPageToken != "" || request.PageToken != "") &&
			page.CatalogRevision == 0 ||
		page.NextPageToken != "" &&
			(len(page.Objects) == 0 ||
				!validBoundedListPageToken(
					page.NextPageToken,
					maximumKnowledgePageTokenBytes,
					false,
				) ||
				page.NextPageToken == request.PageToken) ||
		request.PageToken != "" && len(page.Objects) == 0 ||
		(page.TotalSize != nil) != request.IncludeTotal ||
		page.TotalSizeExact != (page.TotalSize != nil) ||
		page.TotalSize != nil && (*page.TotalSize > math.MaxInt64 ||
			*page.TotalSize > knowledgecatalog.MaximumObjectsPerTenant ||
			*page.TotalSize < minimumTotalSize ||
			request.PageToken == "" && page.NextPageToken == "" &&
				*page.TotalSize != uint64(len(page.Objects))) {
		return nil, errors.New("knowledge catalog returned an invalid page")
	}
	if !knowledgeListPagePreflight(scopes, request, page) {
		return nil, errors.New("knowledge catalog returned an invalid object")
	}
	objects := make([]*opensplunkv1.KnowledgeObject, len(page.Objects))
	for index, object := range page.Objects {
		if !validKnowledgeCatalogDefinitionAuthority(object) {
			return nil, errors.New("knowledge catalog returned an invalid object")
		}
		converted, err := knowledgecatalog.ObjectToProto(object)
		if err != nil || !knowledgeObjectMatchesReadScopeAfterDefinitionAuthority(converted, scopes) ||
			page.CatalogRevision < converted.GetVersion() {
			return nil, errors.New("knowledge catalog returned an invalid object")
		}
		objects[index] = converted
	}
	metadata := &opensplunkv1.PageResponse{TotalSizeExact: page.TotalSizeExact}
	if page.NextPageToken != "" {
		metadata.NextPageToken = new(strings.Clone(page.NextPageToken))
	}
	if page.TotalSize != nil {
		metadata.TotalSize = new(*page.TotalSize)
	}
	response := &opensplunkv1.ListKnowledgeObjectsResponse{
		KnowledgeObjects:      objects,
		Page:                  metadata,
		TenantCatalogRevision: page.CatalogRevision,
	}
	if proto.Size(response) > maximumKnowledgeListResponseBytes {
		return nil, errors.New("knowledge catalog returned an oversized page")
	}
	return response, nil
}

// The catalog interface is an implementation boundary, not a trust boundary.
// Prove page membership and allocation ceilings using caller-owned scalars
// before ObjectToProto clones definitions or the response codec marshals them.
func knowledgeListPagePreflight(
	scopes knowledgeScopes,
	request knowledgecatalog.ListRequest,
	page knowledgecatalog.ListPage,
) bool {
	const knowledgeListObjectWireOverheadBytes = 256
	if (request.SortBy != knowledgecatalog.SortByName &&
		request.SortBy != knowledgecatalog.SortByCreatedAt &&
		request.SortBy != knowledgecatalog.SortByUpdatedAt &&
		request.SortBy != knowledgecatalog.SortByObjectType) ||
		request.SortDirection != knowledgecatalog.SortAscending &&
			request.SortDirection != knowledgecatalog.SortDescending {
		return false
	}

	seen := make(map[string]struct{}, len(page.Objects))
	definitionBytes := 0
	responseBytes := 64
	if !addKnowledgeListPreflightBytes(
		&responseBytes,
		len(page.NextPageToken),
		maximumKnowledgeListResponseBytes,
	) {
		return false
	}
	var previous knowledgecatalog.Object
	for index := range page.Objects {
		object := page.Objects[index]
		if !knowledgeCatalogObjectScalarsPreflight(object, page.CatalogRevision) {
			return false
		}
		objectDefinitionBytes := 0
		if object.Definition != nil {
			if !boundedKnowledgeDefinitionRepeatedShape(object.Definition) {
				return false
			}
			objectDefinitionBytes = proto.Size(object.Definition)
			if objectDefinitionBytes > knowledgecatalog.MaximumListResponseCanonicalDefinitionBytes ||
				!addKnowledgeListPreflightBytes(
					&definitionBytes,
					objectDefinitionBytes,
					knowledgecatalog.MaximumListResponseCanonicalDefinitionBytes,
				) {
				return false
			}
		}
		quarantineReasonBytes := 0
		if object.QuarantineReason != nil {
			quarantineReasonBytes = len(*object.QuarantineReason)
		}
		objectCharges := [...]int{
			objectDefinitionBytes,
			len(object.KnowledgeObjectID),
			len(object.TenantID),
			len(object.AppID),
			len(object.OwnerID),
			len(object.Name),
			len(object.DefinitionSHA256),
			quarantineReasonBytes,
			knowledgeListObjectWireOverheadBytes,
		}
		for _, charge := range objectCharges {
			if !addKnowledgeListPreflightBytes(
				&responseBytes,
				charge,
				maximumKnowledgeListResponseBytes,
			) {
				return false
			}
		}
		if _, duplicate := seen[object.KnowledgeObjectID]; duplicate {
			return false
		}
		seen[object.KnowledgeObjectID] = struct{}{}
		if !knowledgeCatalogObjectMatchesReadScope(object, scopes) ||
			!knowledgeCatalogObjectMatchesListRequest(object, request) ||
			index > 0 && !knowledgeCatalogListOrderValid(previous, object, request) {
			return false
		}
		previous = object
	}
	return true
}

func knowledgeCatalogObjectScalarsPreflight(
	object knowledgecatalog.Object,
	catalogRevision uint64,
) bool {
	if !validKnowledgeIdentity(object.KnowledgeObjectID, maximumKnowledgeObjectIDBytes) ||
		!validKnowledgeIdentity(object.TenantID, maximumKnowledgeIdentityBytes) ||
		!validKnowledgeIdentity(object.AppID, maximumKnowledgeAppIDBytes) ||
		!validKnowledgeIdentity(object.OwnerID, maximumKnowledgeIdentityBytes) ||
		!validKnowledgeIdentity(object.Name, maximumKnowledgeNameBytes) ||
		object.Version == 0 || object.Version > math.MaxInt64 ||
		object.Version > catalogRevision || !object.ObjectType.Valid() ||
		!object.SharingScope.Valid() {
		return false
	}
	switch object.State {
	case knowledgecatalog.StateDraft,
		knowledgecatalog.StateActive,
		knowledgecatalog.StateDisabled,
		knowledgecatalog.StateQuarantined,
		knowledgecatalog.StateDeleted:
		return true
	default:
		return false
	}
}

func addKnowledgeListPreflightBytes(total *int, amount, maximum int) bool {
	if total == nil || amount < 0 || *total < 0 || amount > maximum-*total {
		return false
	}
	*total += amount
	return true
}

func knowledgeCatalogObjectMatchesReadScope(
	object knowledgecatalog.Object,
	scopes knowledgeScopes,
) bool {
	if object.TenantID != scopes.read.TenantID {
		return false
	}
	return scopes.allowsRead(object.SharingScope, object.AppID, object.OwnerID)
}

func knowledgeCatalogObjectMatchesListRequest(
	object knowledgecatalog.Object,
	request knowledgecatalog.ListRequest,
) bool {
	if request.AppIDFilter != nil && object.AppID != *request.AppIDFilter ||
		request.OwnerIDFilter != nil && object.OwnerID != *request.OwnerIDFilter ||
		len(request.ObjectTypeFilters) != 0 &&
			!slices.Contains(request.ObjectTypeFilters, object.ObjectType) ||
		len(request.StateFilters) != 0 &&
			!slices.Contains(request.StateFilters, object.State) ||
		len(request.SharingScopeFilters) != 0 &&
			!slices.Contains(request.SharingScopeFilters, object.SharingScope) {
		return false
	}
	if request.TextFilter != nil &&
		!strings.Contains(object.Name, *request.TextFilter) &&
		(object.Definition == nil ||
			!strings.Contains(object.Definition.GetDescription(), *request.TextFilter)) {
		return false
	}
	return request.SelectorTextFilter == nil || knowledgeSelectorContains(
		object.Definition,
		*request.SelectorTextFilter,
	)
}

func knowledgeSelectorContains(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	filter string,
) bool {
	if definition == nil || definition.GetSelector() == nil {
		return false
	}
	selector := definition.GetSelector()
	for _, patterns := range [][]*opensplunkv1.KnowledgeSelectorPattern{
		selector.GetIndexPatterns(),
		selector.GetHostPatterns(),
		selector.GetSourcePatterns(),
		selector.GetSourcetypePatterns(),
	} {
		for _, pattern := range patterns {
			if pattern != nil && strings.Contains(pattern.GetValue(), filter) {
				return true
			}
		}
	}
	return false
}

func knowledgeCatalogListOrderValid(
	previous knowledgecatalog.Object,
	current knowledgecatalog.Object,
	request knowledgecatalog.ListRequest,
) bool {
	comparison := knowledgeCatalogListObjectCompare(previous, current, request.SortBy)
	if request.SortDirection == knowledgecatalog.SortDescending {
		return comparison > 0
	}
	return comparison < 0
}

func knowledgeCatalogListObjectCompare(
	left knowledgecatalog.Object,
	right knowledgecatalog.Object,
	sortBy knowledgecatalog.SortBy,
) int {
	var comparison int
	switch sortBy {
	case knowledgecatalog.SortByCreatedAt:
		comparison = compareKnowledgeListInt64(left.CreatedAt.UnixMicro(), right.CreatedAt.UnixMicro())
	case knowledgecatalog.SortByUpdatedAt:
		comparison = compareKnowledgeListInt64(left.UpdatedAt.UnixMicro(), right.UpdatedAt.UnixMicro())
	case knowledgecatalog.SortByObjectType:
		comparison = strings.Compare(string(left.ObjectType), string(right.ObjectType))
		if comparison == 0 {
			comparison = strings.Compare(left.Name, right.Name)
		}
	default:
		comparison = strings.Compare(left.Name, right.Name)
	}
	if comparison == 0 {
		comparison = strings.Compare(left.KnowledgeObjectID, right.KnowledgeObjectID)
	}
	return comparison
}

func compareKnowledgeListInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func knowledgeObjectMatchesReadScopeAfterDefinitionAuthority(
	object *opensplunkv1.KnowledgeObject,
	scopes knowledgeScopes,
) bool {
	if !validKnowledgeObjectScalarLifecycleEnvelope(object) ||
		object.GetTenantId() != scopes.read.TenantID {
		return false
	}
	converted, ok := knowledgeSharingScopeFromProto(object.GetSharingScope())
	if !ok {
		return false
	}
	return scopes.allowsRead(converted, object.GetAppId(), object.GetOwnerId())
}

func knowledgeObjectMatchesGetRequest(
	object *opensplunkv1.KnowledgeObject,
	request *opensplunkv1.GetKnowledgeObjectRequest,
) bool {
	if object == nil || request == nil ||
		object.GetKnowledgeObjectId() != request.GetKnowledgeObjectId() {
		return false
	}
	if request.Version == nil ||
		object.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED {
		return true
	}
	return object.GetVersion() == request.GetVersion()
}

func knowledgeObjectTypeFromProto(
	value opensplunkv1.KnowledgeObjectType,
) (knowledgecatalog.ObjectType, bool) {
	switch value {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return knowledgecatalog.ObjectTypeFieldExtraction, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return knowledgecatalog.ObjectTypeFieldAlias, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return knowledgecatalog.ObjectTypeCalculatedField, true
	default:
		return "", false
	}
}

func knowledgeStateFromProto(
	value opensplunkv1.KnowledgeObjectState,
) (knowledgecatalog.State, bool) {
	switch value {
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT:
		return knowledgecatalog.StateDraft, true
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
		return knowledgecatalog.StateActive, true
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
		return knowledgecatalog.StateDisabled, true
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED:
		return knowledgecatalog.StateQuarantined, true
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED:
		return knowledgecatalog.StateDeleted, true
	default:
		return "", false
	}
}

func knowledgeSharingScopeFromProto(
	value opensplunkv1.SharingScope,
) (knowledgecatalog.SharingScope, bool) {
	switch value {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return knowledgecatalog.SharingScopePrivate, true
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return knowledgecatalog.SharingScopeApp, true
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return knowledgecatalog.SharingScopeGlobal, true
	default:
		return "", false
	}
}

func knowledgeAuthorizedContext(
	object knowledgecatalog.Object,
) *knowledgecatalog.AuthorizedContext {
	return &knowledgecatalog.AuthorizedContext{
		AppID: strings.Clone(object.AppID),
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: strings.Clone(object.KnowledgeObjectID),
			ObjectType:        object.ObjectType,
			Version:           object.Version,
			SharingScope:      object.SharingScope,
		},
	}
}

func cloneKnowledgeMessage[Message proto.Message](
	message Message,
) (Message, bool) {
	var zero Message
	if isNilDependency(message) {
		return zero, false
	}
	cloned, ok := proto.Clone(message).(Message)
	return cloned, ok && !isNilDependency(cloned)
}

func cloneKnowledgeMessageBounded[Message proto.Message](
	message Message,
	maximumBytes int,
) (Message, bool) {
	var zero Message
	if maximumBytes < 0 || isNilDependency(message) ||
		proto.Size(message) > maximumBytes {
		return zero, false
	}
	return cloneKnowledgeMessage(message)
}

func cloneOptionalUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneKnowledgeListRequest(
	request knowledgecatalog.ListRequest,
) knowledgecatalog.ListRequest {
	request.PageToken = strings.Clone(request.PageToken)
	request.AppIDFilter = cloneOptionalString(request.AppIDFilter)
	request.OwnerIDFilter = cloneOptionalString(request.OwnerIDFilter)
	request.TextFilter = cloneOptionalString(request.TextFilter)
	request.SelectorTextFilter = cloneOptionalString(request.SelectorTextFilter)
	request.ObjectTypeFilters = slices.Clone(request.ObjectTypeFilters)
	request.StateFilters = slices.Clone(request.StateFilters)
	request.SharingScopeFilters = slices.Clone(request.SharingScopeFilters)
	return request
}
