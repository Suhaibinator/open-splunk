package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// RegisterCareersRoutes RegisterCareersRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterCareersRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableCareers)
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.ListCareerJobPostingsRequest, *proto_models.ListCareerJobPostingsResponse]{
		Path:       "/careers/jobs",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.ListCareerJobPostingsRequest, *proto_models.ListCareerJobPostingsResponse](),
		Handler:    h.ListCareerJobPostings,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListCareerJobPostingsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.GetCareerJobPostingBySlugRequest, *proto_models.GetCareerJobPostingBySlugResponse]{
		Path:       "/careers/jobs/get-by-slug",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.GetCareerJobPostingBySlugRequest, *proto_models.GetCareerJobPostingBySlugResponse](),
		Handler:    h.GetCareerJobPostingBySlug,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetCareerJobPostingBySlugRequest,
	})
	group.Route(router.RouteConfig[*proto_models.CreateCareerApplicationRequest, *proto_models.CreateCareerApplicationResponse]{
		Path:        "/careers/applications/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.CreateCareerApplicationRequest, *proto_models.CreateCareerApplicationResponse](),
		Handler:     h.CreateCareerApplication,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeCreateCareerApplicationRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListMyCareerApplicationsRequest, *proto_models.ListMyCareerApplicationsResponse]{
		Path:       "/careers/applications/my",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListMyCareerApplicationsRequest, *proto_models.ListMyCareerApplicationsResponse](),
		Handler:    h.ListMyCareerApplications,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListMyCareerApplicationsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.GetMyCareerApplicationByReferenceIDRequest, *proto_models.GetMyCareerApplicationByReferenceIDResponse]{
		Path:       "/careers/applications/my/get-by-reference",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetMyCareerApplicationByReferenceIDRequest, *proto_models.GetMyCareerApplicationByReferenceIDResponse](),
		Handler:    h.GetMyCareerApplicationByReferenceID,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetMyCareerApplicationByReferenceIDRequest,
	})
	group.Route(router.RouteConfig[*proto_models.WithdrawMyCareerApplicationRequest, *proto_models.WithdrawMyCareerApplicationResponse]{
		Path:        "/careers/applications/my/withdraw",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.WithdrawMyCareerApplicationRequest, *proto_models.WithdrawMyCareerApplicationResponse](),
		Handler:     h.WithdrawMyCareerApplication,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeWithdrawMyCareerApplicationRequest,
	})

}

// ListCareerJobPostings ListCareerJobPostings handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListCareerJobPostings(r *http.Request, req *proto_models.ListCareerJobPostingsRequest) (*proto_models.ListCareerJobPostingsResponse, error) {
	response, err := h.serviceHandler.ListCareerJobPostings(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "list_career_job_postings"))
	}
	return response, nil
}

// GetCareerJobPostingBySlug GetCareerJobPostingBySlug handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetCareerJobPostingBySlug(r *http.Request, req *proto_models.GetCareerJobPostingBySlugRequest) (*proto_models.GetCareerJobPostingBySlugResponse, error) {
	response, err := h.serviceHandler.GetCareerJobPostingBySlug(r.Context(), req.GetSlug())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_career_job_posting_by_slug"),
			zap.String(appLogger.KeySlug, req.GetSlug()))
	}
	return response, nil
}

// CreateCareerApplication CreateCareerApplication handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) CreateCareerApplication(r *http.Request, req *proto_models.CreateCareerApplicationRequest) (*proto_models.CreateCareerApplicationResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
	}

	response, err := h.serviceHandler.CreateCareerApplication(r.Context(), uint64(userID), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_career_application"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	return response, nil
}

// ListMyCareerApplications ListMyCareerApplications handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) ListMyCareerApplications(r *http.Request, req *proto_models.ListMyCareerApplicationsRequest) (*proto_models.ListMyCareerApplicationsResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
	}

	response, err := h.serviceHandler.ListMyCareerApplications(r.Context(), uint64(userID), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_my_career_applications"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	return response, nil
}

// GetMyCareerApplicationByReferenceID GetMyCareerApplicationByReferenceID handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetMyCareerApplicationByReferenceID(r *http.Request, req *proto_models.GetMyCareerApplicationByReferenceIDRequest) (*proto_models.GetMyCareerApplicationByReferenceIDResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
	}

	response, err := h.serviceHandler.GetMyCareerApplicationByReferenceID(r.Context(), uint64(userID), req.GetReferenceId())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_my_career_application_by_reference"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyReferenceID, req.GetReferenceId()))
	}
	return response, nil
}

// WithdrawMyCareerApplication WithdrawMyCareerApplication supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) WithdrawMyCareerApplication(r *http.Request, req *proto_models.WithdrawMyCareerApplicationRequest) (*proto_models.WithdrawMyCareerApplicationResponse, error) {
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
	}

	response, err := h.serviceHandler.WithdrawMyCareerApplication(r.Context(), uint64(userID), req.GetReferenceId())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "withdraw_my_career_application"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyReferenceID, req.GetReferenceId()))
	}
	return response, nil
}
