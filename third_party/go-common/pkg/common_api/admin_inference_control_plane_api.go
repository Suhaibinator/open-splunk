// This file registers the AI control-plane admin endpoints for operation routes,
// model rate-limit policies, and the attempt/usage ledger. Reads require
// PermissionInferenceViewing; writes require PermissionInferenceAdministration.
//
// The write endpoints intentionally carry no transaction middleware: the control-plane store is
// transaction-independent and ignores the ambient request transaction.
package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_api/middleware"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// addAdminInferenceControlPlaneRoutes appends the control-plane admin routes to group, gating
// read endpoints with PermissionInferenceViewing and write endpoints with
// PermissionInferenceAdministration. Write routes deliberately omit transaction middleware (see the
// file header): the control-plane store is transaction-independent.
func (h *CommonAPIHandler[U]) addAdminInferenceControlPlaneRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	viewing := middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceViewing, h.logger)
	administration := middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceAdministration, h.logger)
	group.Route(router.RouteConfig[*proto_models.ListInferenceRoutesRequest, *proto_models.ListInferenceRoutesResponse]{
		Path:        "/inference/routes",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.ListInferenceRoutesRequest, *proto_models.ListInferenceRoutesResponse](),
		Handler:     h.AdminListInferenceRoutes,
		SourceType:  router.Empty,
	})
	// Picker read surface for request-scoped model selection. Gated by the override permission so
	// an actor who may select a model can list the choices without inference viewing.
	group.Route(router.RouteConfig[*proto_models.ListSelectableInferenceModelsRequest, *proto_models.ListSelectableInferenceModelsResponse]{
		Path:        "/inference/selectable-models",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceModelOverride, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.ListSelectableInferenceModelsRequest, *proto_models.ListSelectableInferenceModelsResponse](),
		Handler:     h.AdminListSelectableInferenceModels,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListSelectableInferenceModelsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.UpdateInferenceRouteRequest, *proto_models.UpdateInferenceRouteResponse]{
		Path:        "/inference/routes/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{administration},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateInferenceRouteRequest, *proto_models.UpdateInferenceRouteResponse](),
		Handler:     h.AdminUpdateInferenceRoute,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateInferenceRouteRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListInferenceRateLimitPoliciesRequest, *proto_models.ListInferenceRateLimitPoliciesResponse]{
		Path:        "/inference/rate-limits",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.ListInferenceRateLimitPoliciesRequest, *proto_models.ListInferenceRateLimitPoliciesResponse](),
		Handler:     h.AdminListInferenceRateLimitPolicies,
		SourceType:  router.Empty,
	})
	group.Route(router.RouteConfig[*proto_models.UpdateInferenceRateLimitPolicyRequest, *proto_models.UpdateInferenceRateLimitPolicyResponse]{
		Path:        "/inference/rate-limits/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{administration},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateInferenceRateLimitPolicyRequest, *proto_models.UpdateInferenceRateLimitPolicyResponse](),
		Handler:     h.AdminUpdateInferenceRateLimitPolicy,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateInferenceRateLimitPolicyRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListInferenceAttemptsRequest, *proto_models.ListInferenceAttemptsResponse]{
		Path:        "/inference/attempts",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.ListInferenceAttemptsRequest, *proto_models.ListInferenceAttemptsResponse](),
		Handler:     h.AdminListInferenceAttempts,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListInferenceAttemptsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.GetInferenceAttemptRequest, *proto_models.GetInferenceAttemptResponse]{
		Path:        "/inference/attempts/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.GetInferenceAttemptRequest, *proto_models.GetInferenceAttemptResponse](),
		Handler:     h.AdminGetInferenceAttempt,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetInferenceAttemptRequest,
	})
	group.Route(router.RouteConfig[*proto_models.GetInferenceUsageRequest, *proto_models.GetInferenceUsageResponse]{
		Path:        "/inference/usage",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.GetInferenceUsageRequest, *proto_models.GetInferenceUsageResponse](),
		Handler:     h.AdminGetInferenceUsage,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetInferenceUsageRequest,
	})
}

// AdminListInferenceRoutes returns the current per-operation inference routes, mapping service
// errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminListInferenceRoutes(r *http.Request, req *proto_models.ListInferenceRoutesRequest) (*proto_models.ListInferenceRoutesResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminListInferenceRoutes(ctx)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_inference_routes"))
	}
	return response, nil
}

// AdminListSelectableInferenceModels returns the models available to request-scoped selection and
// the named operation's routed default, mapping service errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminListSelectableInferenceModels(r *http.Request, req *proto_models.ListSelectableInferenceModelsRequest) (*proto_models.ListSelectableInferenceModelsResponse, error) {
	response, err := h.serviceHandler.AdminListSelectableInferenceModels(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_selectable_inference_models"))
	}
	return response, nil
}

// AdminUpdateInferenceRoute applies a validated route patch and returns the updated route, mapping
// service errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminUpdateInferenceRoute(r *http.Request, req *proto_models.UpdateInferenceRouteRequest) (*proto_models.UpdateInferenceRouteResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminUpdateInferenceRoute(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_inference_route"),
			zap.Int32(appLogger.KeyAction, req.GetOperation()),
			zap.Uint64(appLogger.KeyInferenceModelID, req.GetInferenceModelId()))
	}
	return response, nil
}

// AdminListInferenceRateLimitPolicies returns the current per-model rate-limit policies, mapping
// service errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminListInferenceRateLimitPolicies(r *http.Request, req *proto_models.ListInferenceRateLimitPoliciesRequest) (*proto_models.ListInferenceRateLimitPoliciesResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminListInferenceRateLimitPolicies(ctx)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_inference_rate_limit_policies"))
	}
	return response, nil
}

// AdminUpdateInferenceRateLimitPolicy applies a validated policy patch and returns the updated
// policy, mapping service errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminUpdateInferenceRateLimitPolicy(r *http.Request, req *proto_models.UpdateInferenceRateLimitPolicyRequest) (*proto_models.UpdateInferenceRateLimitPolicyResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminUpdateInferenceRateLimitPolicy(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_inference_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, req.GetInferenceModelId()))
	}
	return response, nil
}

// AdminListInferenceAttempts returns a filtered, paginated page of ledger attempts, mapping service
// errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminListInferenceAttempts(r *http.Request, req *proto_models.ListInferenceAttemptsRequest) (*proto_models.ListInferenceAttemptsResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminListInferenceAttempts(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_inference_attempts"))
	}
	return response, nil
}

// AdminGetInferenceAttempt returns a single ledger attempt by id, mapping service errors to HTTP
// status codes.
func (h *CommonAPIHandler[U]) AdminGetInferenceAttempt(r *http.Request, req *proto_models.GetInferenceAttemptRequest) (*proto_models.GetInferenceAttemptResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminGetInferenceAttempt(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_inference_attempt"),
			zap.Uint64(appLogger.KeyInferenceAttemptID, req.GetId()))
	}
	return response, nil
}

// AdminGetInferenceUsage returns the usage summary and breakdown for a filtered attempt set, mapping
// service errors to HTTP status codes.
func (h *CommonAPIHandler[U]) AdminGetInferenceUsage(r *http.Request, req *proto_models.GetInferenceUsageRequest) (*proto_models.GetInferenceUsageResponse, error) {
	ctx := r.Context()
	response, err := h.serviceHandler.AdminGetInferenceUsage(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_get_inference_usage"))
	}
	return response, nil
}
