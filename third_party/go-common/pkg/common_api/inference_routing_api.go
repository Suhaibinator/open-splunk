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
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

func (h *CommonAPIHandler[U]) RegisterInferenceRoutingRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	viewing := middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceViewing, h.logger)
	administration := middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceAdministration, h.logger)
	transaction := h.GetTransactionMiddleware()
	adminGroup := h.featureGroup(group, config.APIView.EnableAdminAPI).Group("/admin").
		Auth(router.AuthRequired).
		Use(middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminAccess, h.logger))
	adminGroup.Route(router.RouteConfig[*proto_models.GetInferenceOperationModelsRequest, *proto_models.GetInferenceOperationModelsResponse]{
		Path:        "/inference/operation-models/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{viewing},
		Codec:       codec.NewProtoCodec[*proto_models.GetInferenceOperationModelsRequest, *proto_models.GetInferenceOperationModelsResponse](),
		Handler:     h.GetInferenceOperationModels,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeGetInferenceOperationModelsRequest,
	})
	adminGroup.Route(router.RouteConfig[*proto_models.UpdateInferenceOperationModelsRequest, *proto_models.UpdateInferenceOperationModelsResponse]{
		Path:        "/inference/operation-models/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{administration, transaction},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateInferenceOperationModelsRequest, *proto_models.UpdateInferenceOperationModelsResponse](),
		Handler:     h.UpdateInferenceOperationModels,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateInferenceOperationModelsRequest,
	})
	organizationGroup := h.featureGroup(group, config.APIView.EnableOrganizations)
	organizationGroup.Route(router.RouteConfig[*proto_models.GetOrganizationInferencePolicyRequest, *proto_models.GetOrganizationInferencePolicyResponse]{
		Path:       "/organizations/inference-policy/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetOrganizationInferencePolicyRequest, *proto_models.GetOrganizationInferencePolicyResponse](),
		Handler:    h.GetOrganizationInferencePolicy,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetOrganizationInferencePolicyRequest,
	})
	organizationGroup.Route(router.RouteConfig[*proto_models.UpdateOrganizationInferencePolicyRequest, *proto_models.UpdateOrganizationInferencePolicyResponse]{
		Path:        "/organizations/inference-policy/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transaction},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateOrganizationInferencePolicyRequest, *proto_models.UpdateOrganizationInferencePolicyResponse](),
		Handler:     h.UpdateOrganizationInferencePolicy,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateOrganizationInferencePolicyRequest,
	})
	organizationGroup.Route(router.RouteConfig[*proto_models.UpdateOrganizationInferenceOperationPolicyRequest, *proto_models.UpdateOrganizationInferenceOperationPolicyResponse]{
		Path:        "/organizations/inference-policy/operation/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transaction},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateOrganizationInferenceOperationPolicyRequest, *proto_models.UpdateOrganizationInferenceOperationPolicyResponse](),
		Handler:     h.UpdateOrganizationInferenceOperationPolicy,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateOrganizationInferenceOperationPolicyRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListInferenceModelChoicesRequest, *proto_models.ListInferenceModelChoicesResponse]{
		Path:       "/inference/model-choices",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListInferenceModelChoicesRequest, *proto_models.ListInferenceModelChoicesResponse](),
		Handler:    h.ListInferenceModelChoices,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListInferenceModelChoicesRequest,
	})
	chatGroup := h.featureGroup(group, config.APIView.EnableChat)
	chatGroup.Route(router.RouteConfig[*proto_models.RetargetInferenceSessionRequest, *proto_models.RetargetInferenceSessionResponse]{
		Path:        "/inference/sessions/retarget",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transaction},
		Codec:       codec.NewProtoCodec[*proto_models.RetargetInferenceSessionRequest, *proto_models.RetargetInferenceSessionResponse](),
		Handler:     h.RetargetInferenceSession,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRetargetInferenceSessionRequest,
	})
	chatGroup.Route(router.RouteConfig[*proto_models.RetargetInferenceSessionsRequest, *proto_models.RetargetInferenceSessionsResponse]{
		Path:        "/inference/sessions/retarget-bulk",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transaction},
		Codec:       codec.NewProtoCodec[*proto_models.RetargetInferenceSessionsRequest, *proto_models.RetargetInferenceSessionsResponse](),
		Handler:     h.RetargetInferenceSessions,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeRetargetInferenceSessionsRequest,
	})
}

func (h *CommonAPIHandler[U]) GetInferenceOperationModels(r *http.Request, req *proto_models.GetInferenceOperationModelsRequest) (*proto_models.GetInferenceOperationModelsResponse, error) {
	response, err := h.serviceHandler.GetInferenceOperationModels(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "get_inference_operation_models"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) UpdateInferenceOperationModels(r *http.Request, req *proto_models.UpdateInferenceOperationModelsRequest) (*proto_models.UpdateInferenceOperationModelsResponse, error) {
	response, err := h.serviceHandler.UpdateInferenceOperationModels(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "update_inference_operation_models"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) GetOrganizationInferencePolicy(r *http.Request, req *proto_models.GetOrganizationInferencePolicyRequest) (*proto_models.GetOrganizationInferencePolicyResponse, error) {
	response, err := h.serviceHandler.GetOrganizationInferencePolicy(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "get_organization_inference_policy"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) UpdateOrganizationInferencePolicy(r *http.Request, req *proto_models.UpdateOrganizationInferencePolicyRequest) (*proto_models.UpdateOrganizationInferencePolicyResponse, error) {
	response, err := h.serviceHandler.UpdateOrganizationInferencePolicy(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "update_organization_inference_policy"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) UpdateOrganizationInferenceOperationPolicy(r *http.Request, req *proto_models.UpdateOrganizationInferenceOperationPolicyRequest) (*proto_models.UpdateOrganizationInferenceOperationPolicyResponse, error) {
	response, err := h.serviceHandler.UpdateOrganizationInferenceOperationPolicy(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "update_organization_inference_operation_policy"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) ListInferenceModelChoices(r *http.Request, req *proto_models.ListInferenceModelChoicesRequest) (*proto_models.ListInferenceModelChoicesResponse, error) {
	response, err := h.serviceHandler.ListInferenceModelChoices(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "list_inference_model_choices"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) RetargetInferenceSession(r *http.Request, req *proto_models.RetargetInferenceSessionRequest) (*proto_models.RetargetInferenceSessionResponse, error) {
	response, err := h.serviceHandler.RetargetInferenceSession(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "retarget_inference_session"))
	}
	return response, nil
}

func (h *CommonAPIHandler[U]) RetargetInferenceSessions(r *http.Request, req *proto_models.RetargetInferenceSessionsRequest) (*proto_models.RetargetInferenceSessionsResponse, error) {
	response, err := h.serviceHandler.RetargetInferenceSessions(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "retarget_inference_sessions"))
	}
	return response, nil
}
