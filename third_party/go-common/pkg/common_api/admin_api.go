package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_api/middleware"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// RegisterAdminRoutes RegisterAdminRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterAdminRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableAdminAPI)
	log := h.logger.Named(commonAPILoggerName)

	permissionHandler := h.serviceHandler.GetPermissionHandler()

	adminGroup := group.Group("/admin").
		Auth(router.AuthRequired).
		Use(middleware.AdminAuthMiddlewareWithPermission[U](
			permissionHandler,
			permission.PermissionAdminAccess,
			h.logger,
		))

	// User Management Routes
	h.addAdminUserRoutes(adminGroup)

	// Contact Message Routes
	h.addAdminContactRoutes(adminGroup)

	// Careers Routes
	h.addAdminCareerRoutes(adminGroup)

	// Attachment Routes
	h.addAdminAttachmentRoutes(adminGroup)

	// Analytics Routes
	h.addAdminAnalyticsRoutes(adminGroup)

	// Token Management Routes
	h.addAdminTokenRoutes(adminGroup)

	// Inference Model Catalog Routes
	h.addAdminInferenceModelRoutes(adminGroup)

	// Inference Control Plane Routes (routes, rate limits, attempts, usage)
	h.addAdminInferenceControlPlaneRoutes(adminGroup)

	// System Info Routes
	h.addAdminSystemRoutes(adminGroup)

	// Organization Management Routes
	h.addAdminOrganizationRoutes(adminGroup)

	// Chat Channel Management Routes
	h.addAdminChatChannelRoutes(adminGroup)

	log.Debug("Admin API routes registered with authentication middleware")
}

// addAdminUserRoutes addAdminUserRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminUserRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	// Get transaction middleware for write operations. The post-commit-hook
	// variant is required: ban and session revocation defer their auth-cache
	// invalidation until the transaction commits.
	transactionMiddleware := h.GetTransactionMiddleware()
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListUsersRequest, *proto_models.AdminListUsersResponse]{
		Path:        "/users",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserListing, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListUsersRequest, *proto_models.AdminListUsersResponse](),
		Handler:     h.AdminListUsers,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListUsersRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminListUsernameChangeHistoryRequest, *proto_models.AdminListUsernameChangeHistoryResponse]{
		Path:        "/users/username-change-history",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserViewing, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListUsernameChangeHistoryRequest, *proto_models.AdminListUsernameChangeHistoryResponse](),
		Handler:     h.AdminListUsernameChangeHistory,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListUsernameChangeHistoryRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetUserRequest, *proto_models.AdminGetUserResponse]{
		Path:        "/users/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserViewing, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetUserRequest, *proto_models.AdminGetUserResponse](),
		Handler:     h.AdminGetUser,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetUserRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminCreateUserRequest, *proto_models.AdminCreateUserResponse]{
		Path:        "/users/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminCreateUserRequest, *proto_models.AdminCreateUserResponse](),
		Handler:     h.AdminCreateUser,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateUserRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminResendUserInvitationRequest, *proto_models.AdminResendUserInvitationResponse]{
		Path:        "/users/resend-invitation",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminResendUserInvitationRequest, *proto_models.AdminResendUserInvitationResponse](),
		Handler:     h.AdminResendUserInvitation,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminResendUserInvitationRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminBanUserRequest, *proto_models.AdminBanUserResponse]{
		Path:        "/users/ban",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserBanning, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminBanUserRequest, *proto_models.AdminBanUserResponse](),
		Handler:     h.AdminBanUser,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminBanUserRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminChangeUserTypeRequest, *proto_models.AdminChangeUserTypeResponse]{
		Path:        "/users/change-type",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserTypeChange, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminChangeUserTypeRequest, *proto_models.AdminChangeUserTypeResponse](),
		Handler:     h.AdminChangeUserType,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminChangeUserTypeRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminChangeUsernameRequest, *proto_models.AdminChangeUsernameResponse]{
		Path:        "/users/change-username",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminChangeUsernameRequest, *proto_models.AdminChangeUsernameResponse](),
		Handler:     h.AdminChangeUsername,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminChangeUsernameRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminDeleteUserRequest, *proto_models.AdminDeleteUserResponse]{
		Path:        "/users/delete",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserDeletion, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminDeleteUserRequest, *proto_models.AdminDeleteUserResponse](),
		Handler:     h.AdminDeleteUser,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminDeleteUserRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminRevokeUserSessionsRequest, *proto_models.AdminRevokeUserSessionsResponse]{
		Path:        "/users/revoke-sessions",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminRevokeUserSessionsRequest, *proto_models.AdminRevokeUserSessionsResponse](),
		Handler:     h.AdminRevokeUserSessions,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminRevokeUserSessionsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminCreateUserProfilePictureUploadURLRequest, *proto_models.AdminCreateUserProfilePictureUploadURLResponse]{
		Path:        "/users/profile-picture/upload-url",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminCreateUserProfilePictureUploadURLRequest, *proto_models.AdminCreateUserProfilePictureUploadURLResponse](),
		Handler:     h.AdminCreateUserProfilePictureUploadURL,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateUserProfilePictureUploadURLRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminClearUserProfilePictureRequest, *proto_models.AdminClearUserProfilePictureResponse]{
		Path:        "/users/profile-picture/clear",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionUserModification, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminClearUserProfilePictureRequest, *proto_models.AdminClearUserProfilePictureResponse](),
		Handler:     h.AdminClearUserProfilePicture,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminClearUserProfilePictureRequest,
	})
}

// addAdminContactRoutes addAdminContactRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminContactRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListContactMessagesRequest, *proto_models.AdminListContactMessagesResponse]{
		Path:        "/contact-messages",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionContentModeration, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListContactMessagesRequest, *proto_models.AdminListContactMessagesResponse](),
		Handler:     h.AdminListContactMessages,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListContactMessagesRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetContactMessageRequest, *proto_models.AdminGetContactMessageResponse]{
		Path:        "/contact-messages/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionContentModeration, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetContactMessageRequest, *proto_models.AdminGetContactMessageResponse](),
		Handler:     h.AdminGetContactMessage,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetContactMessageRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetContactMessageByReferenceIDRequest, *proto_models.AdminGetContactMessageByReferenceIDResponse]{
		Path:        "/contact-messages/get-by-reference",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionContentModeration, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetContactMessageByReferenceIDRequest, *proto_models.AdminGetContactMessageByReferenceIDResponse](),
		Handler:     h.AdminGetContactMessageByReferenceID,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetContactMessageByReferenceIDRequest,
	})
}

// addAdminCareerRoutes addAdminCareerRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminCareerRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	transactionMiddleware := h.GetTransactionMiddleware()
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListCareerJobPostingsRequest, *proto_models.AdminListCareerJobPostingsResponse]{
		Path:        "/careers/jobs",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListCareerJobPostingsRequest, *proto_models.AdminListCareerJobPostingsResponse](),
		Handler:     h.AdminListCareerJobPostings,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListCareerJobPostingsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetCareerJobPostingRequest, *proto_models.AdminGetCareerJobPostingResponse]{
		Path:        "/careers/jobs/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetCareerJobPostingRequest, *proto_models.AdminGetCareerJobPostingResponse](),
		Handler:     h.AdminGetCareerJobPosting,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetCareerJobPostingRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminCreateCareerJobPostingRequest, *proto_models.AdminCreateCareerJobPostingResponse]{
		Path:        "/careers/jobs/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminCreateCareerJobPostingRequest, *proto_models.AdminCreateCareerJobPostingResponse](),
		Handler:     h.AdminCreateCareerJobPosting,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateCareerJobPostingRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminUpdateCareerJobPostingRequest, *proto_models.AdminUpdateCareerJobPostingResponse]{
		Path:        "/careers/jobs/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateCareerJobPostingRequest, *proto_models.AdminUpdateCareerJobPostingResponse](),
		Handler:     h.AdminUpdateCareerJobPosting,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateCareerJobPostingRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminTransitionCareerJobPostingStatusRequest, *proto_models.AdminTransitionCareerJobPostingStatusResponse]{
		Path:        "/careers/jobs/status",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminTransitionCareerJobPostingStatusRequest, *proto_models.AdminTransitionCareerJobPostingStatusResponse](),
		Handler:     h.AdminTransitionCareerJobPostingStatus,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminTransitionCareerJobPostingStatusRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminListCareerApplicationsRequest, *proto_models.AdminListCareerApplicationsResponse]{
		Path:        "/careers/applications",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListCareerApplicationsRequest, *proto_models.AdminListCareerApplicationsResponse](),
		Handler:     h.AdminListCareerApplications,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListCareerApplicationsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetCareerApplicationRequest, *proto_models.AdminGetCareerApplicationResponse]{
		Path:        "/careers/applications/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetCareerApplicationRequest, *proto_models.AdminGetCareerApplicationResponse](),
		Handler:     h.AdminGetCareerApplication,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetCareerApplicationRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetCareerApplicationByReferenceIDRequest, *proto_models.AdminGetCareerApplicationByReferenceIDResponse]{
		Path:        "/careers/applications/get-by-reference",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetCareerApplicationByReferenceIDRequest, *proto_models.AdminGetCareerApplicationByReferenceIDResponse](),
		Handler:     h.AdminGetCareerApplicationByReferenceID,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetCareerApplicationByReferenceIDRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminUpdateCareerApplicationStatusRequest, *proto_models.AdminUpdateCareerApplicationStatusResponse]{
		Path:        "/careers/applications/status",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionCareersManagement, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateCareerApplicationStatusRequest, *proto_models.AdminUpdateCareerApplicationStatusResponse](),
		Handler:     h.AdminUpdateCareerApplicationStatus,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateCareerApplicationStatusRequest,
	})
}

// addAdminAttachmentRoutes addAdminAttachmentRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminAttachmentRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListAttachmentsRequest, *proto_models.AdminListAttachmentsResponse]{
		Path:        "/attachments",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAttachmentAccess, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListAttachmentsRequest, *proto_models.AdminListAttachmentsResponse](),
		Handler:     h.AdminListAttachments,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListAttachmentsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminUpdateAttachmentStatusRequest, *proto_models.AdminUpdateAttachmentStatusResponse]{
		Path:        "/attachments/update-status",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAttachmentModification, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateAttachmentStatusRequest, *proto_models.AdminUpdateAttachmentStatusResponse](),
		Handler:     h.AdminUpdateAttachmentStatus,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateAttachmentStatusRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminDeleteAttachmentRequest, *proto_models.AdminDeleteAttachmentResponse]{
		Path:        "/attachments/delete",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAttachmentModification, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminDeleteAttachmentRequest, *proto_models.AdminDeleteAttachmentResponse](),
		Handler:     h.AdminDeleteAttachment,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminDeleteAttachmentRequest,
	})
}

// addAdminAnalyticsRoutes addAdminAnalyticsRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminAnalyticsRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminUserAnalyticsRequest, *proto_models.AdminUserAnalyticsResponse]{
		Path:        "/analytics/users",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAnalyticsAccess, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUserAnalyticsRequest, *proto_models.AdminUserAnalyticsResponse](),
		Handler:     h.AdminGetUserAnalytics,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUserAnalyticsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminStorageAnalyticsRequest, *proto_models.AdminStorageAnalyticsResponse]{
		Path:        "/analytics/storage",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAnalyticsAccess, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminStorageAnalyticsRequest, *proto_models.AdminStorageAnalyticsResponse](),
		Handler:     h.AdminGetStorageAnalytics,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminStorageAnalyticsRequest,
	})
}

// addAdminTokenRoutes addAdminTokenRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminTokenRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListTokensRequest, *proto_models.AdminListTokensResponse]{
		Path:        "/tokens",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionSystemAccess, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListTokensRequest, *proto_models.AdminListTokensResponse](),
		Handler:     h.AdminListTokens,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListTokensRequest,
	})
}

// addAdminInferenceModelRoutes addAdminInferenceModelRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminInferenceModelRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.ListAdminInferenceProvidersRequest, *proto_models.ListAdminInferenceProvidersResponse]{
		Path:        "/inference-providers",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceViewing, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.ListAdminInferenceProvidersRequest, *proto_models.ListAdminInferenceProvidersResponse](),
		Handler:     h.AdminListInferenceProviders,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListInferenceProvidersRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListAdminInferenceModelsRequest, *proto_models.ListAdminInferenceModelsResponse]{
		Path:        "/inference-models",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionInferenceViewing, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.ListAdminInferenceModelsRequest, *proto_models.ListAdminInferenceModelsResponse](),
		Handler:     h.AdminListInferenceModels,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListInferenceModelsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.CreateAdminInferenceModelRequest, *proto_models.CreateAdminInferenceModelResponse]{
		Path:        "/inference-models/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionSystemAccess, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.CreateAdminInferenceModelRequest, *proto_models.CreateAdminInferenceModelResponse](),
		Handler:     h.AdminCreateInferenceModel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateInferenceModelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.UpdateAdminInferenceModelRequest, *proto_models.UpdateAdminInferenceModelResponse]{
		Path:        "/inference-models/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionSystemAccess, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.UpdateAdminInferenceModelRequest, *proto_models.UpdateAdminInferenceModelResponse](),
		Handler:     h.AdminUpdateInferenceModel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateInferenceModelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.DeleteAdminInferenceModelRequest, *proto_models.DeleteAdminInferenceModelResponse]{
		Path:        "/inference-models/delete",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionSystemAccess, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.DeleteAdminInferenceModelRequest, *proto_models.DeleteAdminInferenceModelResponse](),
		Handler:     h.AdminDeleteInferenceModel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminDeleteInferenceModelRequest,
	})
}

// addAdminSystemRoutes addAdminSystemRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminSystemRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminSystemStatsRequest, *proto_models.AdminSystemStatsResponse]{
		Path:        "/system/stats",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionSystemAccess, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminSystemStatsRequest, *proto_models.AdminSystemStatsResponse](),
		Handler:     h.AdminGetSystemStats,
		SourceType:  router.Empty,
		Sanitizer:   h.sanitizer.SanitizeAdminSystemStatsRequest,
	})
}

// Handler implementations for admin routes

// AdminListUsers AdminListUsers handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListUsers(r *http.Request, req *proto_models.AdminListUsersRequest) (*proto_models.AdminListUsersResponse, error) {
	ctx := r.Context()
	// Admin authorization is handled by middleware at the route group level

	var page, limit uint32
	if req.Pagination != nil {
		page, limit = req.Pagination.Page, req.Pagination.Limit
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("Admin listing users",
			zap.String(logkeys.Operation, "admin_list_users"),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}

	// Call service layer - URLs are already processed!
	response, err := h.serviceHandler.AdminListUsers(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_users"),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit))
	}

	return response, nil
}

// AdminListUsernameChangeHistory AdminListUsernameChangeHistory handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListUsernameChangeHistory(r *http.Request, req *proto_models.AdminListUsernameChangeHistoryRequest) (*proto_models.AdminListUsernameChangeHistoryResponse, error) {
	ctx := r.Context()

	var page, limit uint32
	if req.Pagination != nil {
		page, limit = req.Pagination.Page, req.Pagination.Limit
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("Admin listing username change history",
			zap.String(logkeys.Operation, "admin_list_username_change_history"),
			zap.Uint32(appLogger.KeyPage, page),
			zap.Uint32(logkeys.Limit, limit),
			zap.Uint64p(appLogger.KeyTargetUserIDFilter, req.TargetUserIdFilter),
			zap.Uint64p(appLogger.KeyActorUserIDFilter, req.ActorUserIdFilter))
	}

	response, err := h.serviceHandler.AdminListUsernameChangeHistory(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_username_change_history"),
			zap.Uint64p(appLogger.KeyTargetUserID, req.TargetUserIdFilter),
			zap.Uint64p(appLogger.KeyActorUserID, req.ActorUserIdFilter))
	}

	return response, nil
}

// AdminGetUser AdminGetUser handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetUser(r *http.Request, req *proto_models.AdminGetUserRequest) (*proto_models.AdminGetUserResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("Admin getting user",
			zap.String(logkeys.Operation, "admin_get_user"),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	// Call service layer
	response, err := h.serviceHandler.AdminGetUser(ctx, req.UserId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_user"),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	return response, nil
}

// AdminCreateUser provisions a human or bot account.
func (h *CommonAPIHandler[U]) AdminCreateUser(r *http.Request, req *proto_models.AdminCreateUserRequest) (*proto_models.AdminCreateUserResponse, error) {
	ctx := r.Context()

	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	response, err := h.serviceHandler.AdminCreateUser(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_create_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())))
	}

	return response, nil
}

// AdminResendUserInvitation replaces a pending account invitation.
func (h *CommonAPIHandler[U]) AdminResendUserInvitation(r *http.Request, req *proto_models.AdminResendUserInvitationRequest) (*proto_models.AdminResendUserInvitationResponse, error) {
	ctx := r.Context()

	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	response, err := h.serviceHandler.AdminResendUserInvitation(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_resend_user_invitation"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())))
	}

	return response, nil
}

// AdminBanUser AdminBanUser handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminBanUser(r *http.Request, req *proto_models.AdminBanUserRequest) (*proto_models.AdminBanUserResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	userID := req.UserId

	// All authorization checks and business logic are handled by the service layer

	// Call service layer
	err := h.serviceHandler.AdminBanUser(ctx, userID, req.Banned, req.Reason)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_change_user_ban_status"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyBanned, req.Banned),
			zap.String(logkeys.Reason, req.Reason))
	}
	return &proto_models.AdminBanUserResponse{}, nil
}

// AdminChangeUserType AdminChangeUserType handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminChangeUserType(r *http.Request, req *proto_models.AdminChangeUserTypeRequest) (*proto_models.AdminChangeUserTypeResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	userID := req.UserId

	// All authorization checks and business logic are handled by the service layer

	// Call service layer
	err := h.serviceHandler.AdminChangeUserType(ctx, userID, req.NewUserType, req.Reason)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_change_user_type"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyNewUserType, req.NewUserType),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminChangeUserTypeResponse{}, nil
}

// AdminChangeUsername AdminChangeUsername handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminChangeUsername(r *http.Request, req *proto_models.AdminChangeUsernameRequest) (*proto_models.AdminChangeUsernameResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer
	err := h.serviceHandler.AdminChangeUsername(ctx, req.UserId, req.NewUsername, req.Reason)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_change_username"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.String(appLogger.KeyNewUsername, req.NewUsername),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminChangeUsernameResponse{}, nil
}

// AdminDeleteUser AdminDeleteUser handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminDeleteUser(r *http.Request, req *proto_models.AdminDeleteUserRequest) (*proto_models.AdminDeleteUserResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// All authorization checks and business logic are handled by the service layer

	// Call service layer
	err := h.serviceHandler.AdminDeleteUser(ctx, req.UserId, req.DeleteAttachments, req.Reason)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_delete_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Bool(appLogger.KeyDeleteAttachments, req.DeleteAttachments),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminDeleteUserResponse{}, nil
}

// AdminRevokeUserSessions AdminRevokeUserSessions handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminRevokeUserSessions(r *http.Request, req *proto_models.AdminRevokeUserSessionsRequest) (*proto_models.AdminRevokeUserSessionsResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// All authorization checks and business logic are handled by the service layer

	// Call service layer
	sessionsRevoked, err := h.serviceHandler.AdminRevokeUserSessions(ctx, req.UserId, req.Reason)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_revoke_user_sessions"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminRevokeUserSessionsResponse{
		SessionsRevoked: sessionsRevoked,
	}, nil
}

// AdminCreateUserProfilePictureUploadURL AdminCreateUserProfilePictureUploadURL handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminCreateUserProfilePictureUploadURL(
	r *http.Request,
	req *proto_models.AdminCreateUserProfilePictureUploadURLRequest,
) (*proto_models.AdminCreateUserProfilePictureUploadURLResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	contentType := "image/jpeg"
	if req.ContentType != nil && *req.ContentType != "" {
		contentType = *req.ContentType
	}

	response, err := h.serviceHandler.AdminCreateUserProfilePictureUploadURL(ctx, req.UserId, req.Filename, contentType)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_create_profile_picture_upload_url"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.String(appLogger.KeyFilename, req.Filename),
			zap.String(appLogger.KeyContentType, contentType))
	}

	return response, nil
}

// AdminClearUserProfilePicture AdminClearUserProfilePicture handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminClearUserProfilePicture(r *http.Request, req *proto_models.AdminClearUserProfilePictureRequest) (*proto_models.AdminClearUserProfilePictureResponse, error) {
	ctx := r.Context()

	// Admin authorization is handled by middleware at the route group level

	// Get admin user from context
	adminUser, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || adminUser == nil {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	if err := h.serviceHandler.AdminClearUserProfilePicture(ctx, req.UserId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_clear_user_profile_picture"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64((*adminUser).GetID())),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	return &proto_models.AdminClearUserProfilePictureResponse{}, nil
}

// AdminListContactMessages AdminListContactMessages handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListContactMessages(r *http.Request, req *proto_models.AdminListContactMessagesRequest) (*proto_models.AdminListContactMessagesResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminListContactMessages(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_contact_messages"))
	}
	return response, nil
}

// AdminGetContactMessage AdminGetContactMessage handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetContactMessage(r *http.Request, req *proto_models.AdminGetContactMessageRequest) (*proto_models.AdminGetContactMessageResponse, error) {
	// Admin authorization is handled by middleware at the route group level

	response, err := h.serviceHandler.AdminGetContactMessage(r.Context(), req.MessageId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_contact_message"),
			zap.Uint64(appLogger.KeyMessageID, req.MessageId))
	}
	return response, nil
}

// AdminGetContactMessageByReferenceID AdminGetContactMessageByReferenceID handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetContactMessageByReferenceID(r *http.Request, req *proto_models.AdminGetContactMessageByReferenceIDRequest) (*proto_models.AdminGetContactMessageByReferenceIDResponse, error) {
	// Admin authorization is handled by middleware at the route group level

	response, err := h.serviceHandler.AdminGetContactMessageByReferenceID(r.Context(), req.ReferenceId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_contact_message_by_reference"),
			zap.String(appLogger.KeyReferenceID, req.ReferenceId))
	}

	return &proto_models.AdminGetContactMessageByReferenceIDResponse{
		Message: response.Message,
	}, nil
}

// AdminListCareerJobPostings AdminListCareerJobPostings handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListCareerJobPostings(r *http.Request, req *proto_models.AdminListCareerJobPostingsRequest) (*proto_models.AdminListCareerJobPostingsResponse, error) {
	response, err := h.serviceHandler.AdminListCareerJobPostings(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_career_job_postings"))
	}
	return response, nil
}

// AdminGetCareerJobPosting AdminGetCareerJobPosting handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetCareerJobPosting(r *http.Request, req *proto_models.AdminGetCareerJobPostingRequest) (*proto_models.AdminGetCareerJobPostingResponse, error) {
	response, err := h.serviceHandler.AdminGetCareerJobPosting(r.Context(), req.JobPostingId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, req.JobPostingId))
	}
	return response, nil
}

// AdminCreateCareerJobPosting AdminCreateCareerJobPosting handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminCreateCareerJobPosting(r *http.Request, req *proto_models.AdminCreateCareerJobPostingRequest) (*proto_models.AdminCreateCareerJobPostingResponse, error) {
	response, err := h.serviceHandler.AdminCreateCareerJobPosting(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_create_career_job_posting"))
	}
	return response, nil
}

// AdminUpdateCareerJobPosting AdminUpdateCareerJobPosting handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateCareerJobPosting(r *http.Request, req *proto_models.AdminUpdateCareerJobPostingRequest) (*proto_models.AdminUpdateCareerJobPostingResponse, error) {
	response, err := h.serviceHandler.AdminUpdateCareerJobPosting(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, req.JobPostingId))
	}
	return response, nil
}

// AdminTransitionCareerJobPostingStatus AdminTransitionCareerJobPostingStatus handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminTransitionCareerJobPostingStatus(r *http.Request, req *proto_models.AdminTransitionCareerJobPostingStatusRequest) (*proto_models.AdminTransitionCareerJobPostingStatusResponse, error) {
	response, err := h.serviceHandler.AdminTransitionCareerJobPostingStatus(r.Context(), req.JobPostingId, req.Status)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_transition_career_job_posting_status"),
			zap.Uint64(appLogger.KeyJobPostingID, req.JobPostingId),
			zap.Stringer(appLogger.KeyNewStatus, req.Status))
	}
	return response, nil
}

// AdminListCareerApplications AdminListCareerApplications handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListCareerApplications(r *http.Request, req *proto_models.AdminListCareerApplicationsRequest) (*proto_models.AdminListCareerApplicationsResponse, error) {
	response, err := h.serviceHandler.AdminListCareerApplications(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_career_applications"))
	}
	return response, nil
}

// AdminGetCareerApplication AdminGetCareerApplication handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetCareerApplication(r *http.Request, req *proto_models.AdminGetCareerApplicationRequest) (*proto_models.AdminGetCareerApplicationResponse, error) {
	response, err := h.serviceHandler.AdminGetCareerApplication(r.Context(), req.ApplicationId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_career_application"),
			zap.Uint64(appLogger.KeyApplicationID, req.ApplicationId))
	}
	return response, nil
}

// AdminGetCareerApplicationByReferenceID AdminGetCareerApplicationByReferenceID handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetCareerApplicationByReferenceID(r *http.Request, req *proto_models.AdminGetCareerApplicationByReferenceIDRequest) (*proto_models.AdminGetCareerApplicationByReferenceIDResponse, error) {
	response, err := h.serviceHandler.AdminGetCareerApplicationByReferenceID(r.Context(), req.ReferenceId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_career_application_by_reference"),
			zap.String(appLogger.KeyReferenceID, req.ReferenceId))
	}
	return response, nil
}

// AdminUpdateCareerApplicationStatus AdminUpdateCareerApplicationStatus handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateCareerApplicationStatus(r *http.Request, req *proto_models.AdminUpdateCareerApplicationStatusRequest) (*proto_models.AdminUpdateCareerApplicationStatusResponse, error) {
	response, err := h.serviceHandler.AdminUpdateCareerApplicationStatus(r.Context(), req.ApplicationId, req.Status, req.AdminNotes)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_career_application_status"),
			zap.Uint64(appLogger.KeyApplicationID, req.ApplicationId),
			zap.Stringer(appLogger.KeyNewStatus, req.Status))
	}
	return response, nil
}

// AdminListAttachments AdminListAttachments handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListAttachments(
	r *http.Request, req *proto_models.AdminListAttachmentsRequest,
) (*proto_models.AdminListAttachmentsResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminListAttachments(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_attachments"))
	}

	return response, nil
}

// AdminUpdateAttachmentStatus AdminUpdateAttachmentStatus handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateAttachmentStatus(
	r *http.Request, req *proto_models.AdminUpdateAttachmentStatusRequest,
) (*proto_models.AdminUpdateAttachmentStatusResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	if err := h.serviceHandler.AdminUpdateAttachmentStatus(r.Context(), req.AttachmentId, req.Status, req.ValidationDetails); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_attachment_status"),
			zap.Uint64(appLogger.KeyAttachmentID, req.AttachmentId),
			zap.String(appLogger.KeyNewStatus, req.Status))
	}

	return &proto_models.AdminUpdateAttachmentStatusResponse{}, nil
}

// AdminDeleteAttachment AdminDeleteAttachment handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminDeleteAttachment(
	r *http.Request, req *proto_models.AdminDeleteAttachmentRequest,
) (*proto_models.AdminDeleteAttachmentResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	if err := h.serviceHandler.AdminDeleteAttachment(r.Context(), req.AttachmentId, req.DeleteFiles, req.Reason); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_delete_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, req.AttachmentId),
			zap.Bool(appLogger.KeyDeleteFiles, req.DeleteFiles),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminDeleteAttachmentResponse{}, nil
}

// AdminGetUserAnalytics AdminGetUserAnalytics handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetUserAnalytics(
	r *http.Request, req *proto_models.AdminUserAnalyticsRequest) (*proto_models.AdminUserAnalyticsResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminGetUserAnalytics(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_get_user_analytics"))
	}
	return response, nil
}

// AdminGetStorageAnalytics AdminGetStorageAnalytics handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetStorageAnalytics(r *http.Request, req *proto_models.AdminStorageAnalyticsRequest) (*proto_models.AdminStorageAnalyticsResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminGetStorageAnalytics(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_get_storage_analytics"))
	}
	return response, nil
}

// AdminListTokens AdminListTokens handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListTokens(
	r *http.Request, req *proto_models.AdminListTokensRequest,
) (*proto_models.AdminListTokensResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminListTokens(r.Context(), req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_tokens"))
	}
	return response, nil
}

// AdminListInferenceModels AdminListInferenceModels handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListInferenceModels(
	r *http.Request, req *proto_models.ListAdminInferenceModelsRequest,
) (*proto_models.ListAdminInferenceModelsResponse, error) {
	models, pagination, err := h.serviceHandler.AdminListInferenceModelsPage(r.Context(), req.GetPagination())
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_inference_models"))
	}
	converter := h.serviceHandler.GetConverter()
	protoModels := make([]*proto_models.AdminInferenceModel, 0, len(models))
	for _, m := range models {
		protoModels = append(protoModels, converter.ServiceInferenceModelToAdminProto(m))
	}
	return &proto_models.ListAdminInferenceModelsResponse{
		InferenceModels: protoModels,
		Pagination:      pagination,
	}, nil
}

// AdminCreateInferenceModel AdminCreateInferenceModel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminCreateInferenceModel(
	r *http.Request, req *proto_models.CreateAdminInferenceModelRequest,
) (*proto_models.CreateAdminInferenceModelResponse, error) {
	model, err := h.serviceHandler.AdminCreateInferenceModel(r.Context(), adminCreateInferenceModelFromProto(req))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_create_inference_model"),
			zap.String(appLogger.KeyProvider, req.ProviderId))
	}
	return &proto_models.CreateAdminInferenceModelResponse{
		InferenceModel: h.serviceHandler.GetConverter().ServiceInferenceModelToAdminProto(model),
	}, nil
}

// AdminUpdateInferenceModel AdminUpdateInferenceModel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateInferenceModel(
	r *http.Request, req *proto_models.UpdateAdminInferenceModelRequest,
) (*proto_models.UpdateAdminInferenceModelResponse, error) {
	model, err := h.serviceHandler.AdminUpdateInferenceModel(r.Context(), adminUpdateInferenceModelPatchFromProto(req))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId))
	}
	return &proto_models.UpdateAdminInferenceModelResponse{
		InferenceModel: h.serviceHandler.GetConverter().ServiceInferenceModelToAdminProto(model),
	}, nil
}

// AdminDeleteInferenceModel AdminDeleteInferenceModel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminDeleteInferenceModel(
	r *http.Request, req *proto_models.DeleteAdminInferenceModelRequest,
) (*proto_models.DeleteAdminInferenceModelResponse, error) {
	if err := h.serviceHandler.AdminDeleteInferenceModel(r.Context(), req.InferenceModelId); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_delete_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId))
	}
	return &proto_models.DeleteAdminInferenceModelResponse{}, nil
}

// adminCreateInferenceModelFromProto adminCreateInferenceModelFromProto supports the backend request flow by performing this package's local processing step.
func adminCreateInferenceModelFromProto(req *proto_models.CreateAdminInferenceModelRequest) *common_service_models.InferenceModel {
	if req == nil {
		return nil
	}

	active := true
	if req.Active != nil {
		active = req.GetActive()
	}

	return &common_service_models.InferenceModel{
		ProviderID:      req.ProviderId,
		ProviderModelID: req.ProviderModelId,
		DisplayName:     req.DisplayName,
		Active:          active,
	}
}

// adminUpdateInferenceModelPatchFromProto adminUpdateInferenceModelPatchFromProto supports the backend request flow by performing this package's local processing step.
func adminUpdateInferenceModelPatchFromProto(req *proto_models.UpdateAdminInferenceModelRequest) *common_service_models.InferenceModelPatch {
	if req == nil {
		return nil
	}

	patch := &common_service_models.InferenceModelPatch{
		ID:         req.InferenceModelId,
		UpdateMask: append([]string(nil), req.GetUpdateMask().GetPaths()...),
	}

	for _, path := range patch.UpdateMask {
		switch path {
		case "provider_id":
			provider := req.ProviderId
			patch.ProviderID = &provider
		case "provider_model_id":
			providerModelID := req.ProviderModelId
			patch.ProviderModelID = &providerModelID
		case "display_name":
			displayName := req.DisplayName
			patch.DisplayName = &displayName
		case "active":
			active := req.Active
			patch.Active = &active
		}
	}

	return patch
}

// AdminGetSystemStats AdminGetSystemStats handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetSystemStats(
	r *http.Request, req *proto_models.AdminSystemStatsRequest,
) (*proto_models.AdminSystemStatsResponse, error) {
	// Admin authorization is handled by middleware at the route group level
	response, err := h.serviceHandler.AdminGetSystemStats(r.Context())
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_get_system_stats"))
	}
	return response, nil
}

// addAdminOrganizationRoutes addAdminOrganizationRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminOrganizationRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	log := h.logger.Named(commonAPILoggerName)
	// Get transaction middleware for write operations
	transactionMiddleware := h.GetTransactionMiddleware()
	permissionHandler := h.serviceHandler.GetPermissionHandler()

	// Admin Create Organization
	adminCreateOrgRoute := router.RouteConfig[*proto_models.AdminCreateOrganizationRequest, *proto_models.AdminCreateOrganizationResponse]{
		Path:        "/organizations/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminCreateOrganizationRequest, *proto_models.AdminCreateOrganizationResponse](),
		Handler:     h.AdminCreateOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateOrganizationRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminCreateOrganization, h.logger), transactionMiddleware},
	}
	group.Route(adminCreateOrgRoute)

	// Admin List All Organizations
	adminListAllOrgsRoute := router.RouteConfig[*proto_models.AdminListAllOrganizationsRequest, *proto_models.AdminListAllOrganizationsResponse]{
		Path:        "/organizations/list-all",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminListAllOrganizationsRequest, *proto_models.AdminListAllOrganizationsResponse](),
		Handler:     h.AdminListAllOrganizationsGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListAllOrganizationsRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminListOrganizations, h.logger)},
	}
	group.Route(adminListAllOrgsRoute)

	group.Route(router.RouteConfig[*proto_models.AdminGetOrganizationDeletionImpactRequest, *proto_models.AdminGetOrganizationDeletionImpactResponse]{
		Path:        "/organizations/deletion-impact",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetOrganizationDeletionImpactRequest, *proto_models.AdminGetOrganizationDeletionImpactResponse](),
		Handler:     h.AdminGetOrganizationDeletionImpactGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetOrganizationDeletionImpactRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminListOrganizations, h.logger)},
	})

	adminListOrgFilterMetadataRoute := router.RouteConfig[*proto_models.AdminListAllOrganizationsFilterMetadataRequest, *proto_models.AdminListAllOrganizationsFilterMetadataResponse]{
		Path:        "/organizations/filter-metadata",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminListAllOrganizationsFilterMetadataRequest, *proto_models.AdminListAllOrganizationsFilterMetadataResponse](),
		Handler:     h.AdminListAllOrganizationsFilterMetadataGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListAllOrganizationsFilterMetadataRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminListOrganizations, h.logger)},
	}
	group.Route(adminListOrgFilterMetadataRoute)

	adminListOrgParentOptionsRoute := router.RouteConfig[*proto_models.AdminListOrganizationParentOptionsRequest, *proto_models.AdminListOrganizationParentOptionsResponse]{
		Path:        "/organizations/parent-options",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminListOrganizationParentOptionsRequest, *proto_models.AdminListOrganizationParentOptionsResponse](),
		Handler:     h.AdminListOrganizationParentOptionsGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListOrganizationParentOptionsRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminListOrganizations, h.logger)},
	}
	group.Route(adminListOrgParentOptionsRoute)

	adminChangeOrgTypeRoute := router.RouteConfig[*proto_models.AdminChangeOrganizationTypeRequest, *proto_models.AdminChangeOrganizationTypeResponse]{
		Path:        "/organizations/change-type",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminChangeOrganizationTypeRequest, *proto_models.AdminChangeOrganizationTypeResponse](),
		Handler:     h.AdminChangeOrganizationTypeGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminChangeOrganizationTypeRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminCreateOrganization, h.logger), transactionMiddleware},
	}
	group.Route(adminChangeOrgTypeRoute)

	// Admin Add User to Organization (direct, no consent required)
	adminAddUserRoute := router.RouteConfig[*proto_models.AdminAddUserToOrganizationRequest, *proto_models.AdminAddUserToOrganizationResponse]{
		Path:        "/organizations/members/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminAddUserToOrganizationRequest, *proto_models.AdminAddUserToOrganizationResponse](),
		Handler:     h.AdminAddUserToOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminAddUserToOrganizationRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger), transactionMiddleware},
	}
	group.Route(adminAddUserRoute)

	adminAddUserByIdentifierRoute := router.RouteConfig[*proto_models.AdminAddUserToOrganizationByIdentifierRequest, *proto_models.AdminAddUserToOrganizationByIdentifierResponse]{
		Path:        "/organizations/members/add-by-identifier",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminAddUserToOrganizationByIdentifierRequest, *proto_models.AdminAddUserToOrganizationByIdentifierResponse](),
		Handler:     h.AdminAddUserToOrganizationByIdentifierGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminAddUserToOrganizationByIdentifierRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger), transactionMiddleware},
	}
	group.Route(adminAddUserByIdentifierRoute)

	adminUpdateUserRoleRoute := router.RouteConfig[*proto_models.AdminUpdateUserRoleInOrganizationRequest, *proto_models.AdminUpdateUserRoleInOrganizationResponse]{
		Path:        "/organizations/members/update-role",
		Methods:     []router.HttpMethod{http.MethodPut, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateUserRoleInOrganizationRequest, *proto_models.AdminUpdateUserRoleInOrganizationResponse](),
		Handler:     h.AdminUpdateUserRoleInOrganizationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateUserRoleInOrganizationRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger), transactionMiddleware},
	}
	group.Route(adminUpdateUserRoleRoute)

	adminGetOrganizationMemberRoute := router.RouteConfig[*proto_models.AdminGetOrganizationMemberRequest, *proto_models.AdminGetOrganizationMemberResponse]{
		Path:        "/organizations/members/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetOrganizationMemberRequest, *proto_models.AdminGetOrganizationMemberResponse](),
		Handler:     h.AdminGetOrganizationMemberGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetOrganizationMemberRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminGetOrganizationMember, h.logger)},
	}
	group.Route(adminGetOrganizationMemberRoute)

	// Admin Create Organization Email Domain
	adminCreateEmailDomainRoute := router.RouteConfig[*proto_models.AdminCreateOrganizationEmailDomainRequest, *proto_models.AdminCreateOrganizationEmailDomainResponse]{
		Path:        "/organizations/email-domains/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminCreateOrganizationEmailDomainRequest, *proto_models.AdminCreateOrganizationEmailDomainResponse](),
		Handler:     h.AdminCreateOrganizationEmailDomain,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminCreateOrganizationEmailDomainRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger), transactionMiddleware},
	}
	group.Route(adminCreateEmailDomainRoute)

	// Admin Delete Organization Email Domain
	adminDeleteEmailDomainRoute := router.RouteConfig[*proto_models.AdminDeleteOrganizationEmailDomainRequest, *proto_models.AdminDeleteOrganizationEmailDomainResponse]{
		Path:        "/organizations/email-domains/delete",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminDeleteOrganizationEmailDomainRequest, *proto_models.AdminDeleteOrganizationEmailDomainResponse](),
		Handler:     h.AdminDeleteOrganizationEmailDomain,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminDeleteOrganizationEmailDomainRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger), transactionMiddleware},
	}
	group.Route(adminDeleteEmailDomainRoute)

	// Admin List Organization Email Domains
	adminListEmailDomainsRoute := router.RouteConfig[*proto_models.AdminListOrganizationEmailDomainsRequest, *proto_models.AdminListOrganizationEmailDomainsResponse]{
		Path:        "/organizations/email-domains/list",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AdminListOrganizationEmailDomainsRequest, *proto_models.AdminListOrganizationEmailDomainsResponse](),
		Handler:     h.AdminListOrganizationEmailDomains,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListOrganizationEmailDomainsRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminManageOrganizationMembers, h.logger)},
	}
	group.Route(adminListEmailDomainsRoute)

	log.Debug("Admin organization routes registered")
}

// addAdminChatChannelRoutes addAdminChatChannelRoutes supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) addAdminChatChannelRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	transactionMiddleware := h.GetTransactionMiddleware()
	permissionHandler := h.serviceHandler.GetPermissionHandler()
	group.Route(router.RouteConfig[*proto_models.AdminListChatChannelsRequest, *proto_models.AdminListChatChannelsResponse]{
		Path:        "/channels",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListChatChannelsRequest, *proto_models.AdminListChatChannelsResponse](),
		Handler:     h.AdminListChatChannels,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListChatChannelsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminGetChatChannelRequest, *proto_models.AdminGetChatChannelResponse]{
		Path:        "/channels/get",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminGetChatChannelRequest, *proto_models.AdminGetChatChannelResponse](),
		Handler:     h.AdminGetChatChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminGetChatChannelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminUpdateChatChannelRequest, *proto_models.AdminUpdateChatChannelResponse]{
		Path:        "/channels/update",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateChatChannelRequest, *proto_models.AdminUpdateChatChannelResponse](),
		Handler:     h.AdminUpdateChatChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateChatChannelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminDeleteChatChannelRequest, *proto_models.AdminDeleteChatChannelResponse]{
		Path:        "/channels/delete",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminDeleteChatChannelRequest, *proto_models.AdminDeleteChatChannelResponse](),
		Handler:     h.AdminDeleteChatChannel,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminDeleteChatChannelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminListChatChannelParticipantsRequest, *proto_models.AdminListChatChannelParticipantsResponse]{
		Path:        "/channels/participants/list",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger)},
		Codec:       codec.NewProtoCodec[*proto_models.AdminListChatChannelParticipantsRequest, *proto_models.AdminListChatChannelParticipantsResponse](),
		Handler:     h.AdminListChatChannelParticipants,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminListChatChannelParticipantsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminAddChatChannelParticipantsRequest, *proto_models.AdminAddChatChannelParticipantsResponse]{
		Path:        "/channels/participants/add",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminAddChatChannelParticipantsRequest, *proto_models.AdminAddChatChannelParticipantsResponse](),
		Handler:     h.AdminAddChatChannelParticipants,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminAddChatChannelParticipantsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminRemoveChatChannelParticipantRequest, *proto_models.AdminRemoveChatChannelParticipantResponse]{
		Path:        "/channels/participants/remove",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminRemoveChatChannelParticipantRequest, *proto_models.AdminRemoveChatChannelParticipantResponse](),
		Handler:     h.AdminRemoveChatChannelParticipant,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminRemoveChatChannelParticipantRequest,
	})
	group.Route(router.RouteConfig[*proto_models.AdminUpdateChatChannelParticipantRoleRequest, *proto_models.AdminUpdateChatChannelParticipantRoleResponse]{
		Path:        "/channels/participants/role",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[U](permissionHandler, permission.PermissionAdminChatChannels, h.logger), transactionMiddleware},
		Codec:       codec.NewProtoCodec[*proto_models.AdminUpdateChatChannelParticipantRoleRequest, *proto_models.AdminUpdateChatChannelParticipantRoleResponse](),
		Handler:     h.AdminUpdateChatChannelParticipantRole,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAdminUpdateChatChannelParticipantRoleRequest,
	})
}

// AdminCreateOrganizationGeneric AdminCreateOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminCreateOrganizationGeneric(r *http.Request, req *proto_models.AdminCreateOrganizationRequest) (*proto_models.AdminCreateOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated admin user ID (admin authentication handled by middleware)
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Convert proto request to service model
	org := &common_service_models.Organization{
		Name:             req.Name,
		Slug:             req.Slug,
		Description:      req.GetDescription(),
		AvatarUrl:        req.GetAvatarUrl(),
		OrganizationType: req.GetOrganizationType(),
	}
	if req.ParentOrganizationId != nil {
		converted := common_service_models.OrganizationIdType(*req.ParentOrganizationId)
		org.ParentOrganizationID = &converted
	}

	// Call service layer with admin creation method
	orgResponse, err := h.serviceHandler.GetOrganizationHandler().CreateOrganizationByAdmin(r.Context(), org)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_create_organization"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.String(appLogger.KeyName, req.Name),
			zap.String(appLogger.KeySlug, req.Slug))
	}

	// Convert service response to proto response using converter
	protoOrg := h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)

	return &proto_models.AdminCreateOrganizationResponse{
		Organization: protoOrg,
	}, nil
}

// AdminListAllOrganizationsGeneric AdminListAllOrganizationsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListAllOrganizationsGeneric(r *http.Request, req *proto_models.AdminListAllOrganizationsRequest) (*proto_models.AdminListAllOrganizationsResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated admin user ID (admin authentication handled by middleware)
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Convert proto request to service request
	// Use sanitized pagination values directly (sanitizer sets defaults and validates limits)
	limit := int(req.Pagination.Limit)
	offset := common_util.PaginationOffset(req.Pagination.Page, req.Pagination.Limit)
	var parentOrganizationIDFilter *common_service_models.OrganizationIdType
	if req.ParentOrganizationIdFilter != nil {
		value := common_service_models.OrganizationIdType(req.GetParentOrganizationIdFilter())
		parentOrganizationIDFilter = &value
	}

	// Call service layer admin method
	orgResponses, total, err := h.serviceHandler.GetOrganizationHandler().ListAllOrganizations(
		r.Context(),
		limit,
		offset,
		req.Filter,
		req.OrganizationTypeFilter,
		parentOrganizationIDFilter,
		req.DepthFilter,
		req.SortBy,
		req.Sort,
		common_util.ShouldIncludeTotalCount(req.Pagination),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_organizations"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	// Convert service responses to proto responses using converter
	protoOrgs := make([]*proto_models.Organization, len(orgResponses))
	for i, orgResponse := range orgResponses {
		protoOrgs[i] = h.serviceHandler.GetConverter().ServiceOrganizationToProto(orgResponse.Organization)
	}
	visibleOrgResponses := orgResponses

	currentPage := req.Pagination.Page
	var paginationResponse *proto_models.PaginationResponse
	if common_util.ShouldIncludeTotalCount(req.Pagination) {
		paginationResponse = common_util.CreatePaginationResponse(total, uint32(limit), currentPage)
	} else {
		hasNextPage := false
		if len(protoOrgs) > limit {
			hasNextPage = true
			protoOrgs = protoOrgs[:limit]
			visibleOrgResponses = visibleOrgResponses[:limit]
		}
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(uint32(limit), currentPage, hasNextPage)
	}

	parentIDs := make([]common_service_models.OrganizationIdType, 0, len(visibleOrgResponses))
	seenParentIDs := make(map[common_service_models.OrganizationIdType]struct{}, len(visibleOrgResponses))
	for _, orgResponse := range visibleOrgResponses {
		if orgResponse == nil || orgResponse.Organization == nil || orgResponse.Organization.ParentOrganizationID == nil {
			continue
		}
		parentID := *orgResponse.Organization.ParentOrganizationID
		if _, exists := seenParentIDs[parentID]; exists {
			continue
		}
		seenParentIDs[parentID] = struct{}{}
		parentIDs = append(parentIDs, parentID)
	}

	parentReferences, err := h.serviceHandler.GetOrganizationHandler().GetAdminOrganizationParentOptionsByIDs(r.Context(), parentIDs)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_resolve_organization_parent_references"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}
	parentOptions := common_util.ApplyToEach(parentReferences, func(option *organization.AdminOrganizationFilterParentOption) *proto_models.AdminOrganizationParentOption {
		if option == nil {
			return nil
		}
		return &proto_models.AdminOrganizationParentOption{
			Id:   int64(option.ID),
			Name: option.Name,
			Slug: option.Slug,
		}
	})

	return &proto_models.AdminListAllOrganizationsResponse{
		Organizations: protoOrgs,
		Pagination:    paginationResponse,
		ParentOptions: parentOptions,
	}, nil
}

func (h *CommonAPIHandler[U]) AdminListOrganizationParentOptionsGeneric(
	r *http.Request,
	req *proto_models.AdminListOrganizationParentOptionsRequest,
) (*proto_models.AdminListOrganizationParentOptionsResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	limit := int(req.Pagination.Limit)
	offset := common_util.PaginationOffset(req.Pagination.Page, req.Pagination.Limit)
	parentOptions, hasMore, err := h.serviceHandler.GetOrganizationHandler().ListAdminOrganizationParentOptions(
		r.Context(),
		limit,
		offset,
		req.Filter,
		req.TableFilter,
		req.OrganizationTypeFilter,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_organization_parent_options"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	protoOptions := common_util.ApplyToEach(parentOptions, func(option *organization.AdminOrganizationFilterParentOption) *proto_models.AdminOrganizationParentOption {
		if option == nil {
			return nil
		}
		return &proto_models.AdminOrganizationParentOption{
			Id:   int64(option.ID),
			Name: option.Name,
			Slug: option.Slug,
		}
	})

	return &proto_models.AdminListOrganizationParentOptionsResponse{
		ParentOptions: protoOptions,
		HasMore:       hasMore,
	}, nil
}

func (h *CommonAPIHandler[U]) AdminListAllOrganizationsFilterMetadataGeneric(
	r *http.Request,
	req *proto_models.AdminListAllOrganizationsFilterMetadataRequest,
) (*proto_models.AdminListAllOrganizationsFilterMetadataResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	metadata, err := h.serviceHandler.GetOrganizationHandler().GetAdminOrganizationFilterMetadata(
		r.Context(),
		req.Filter,
		req.OrganizationTypeFilter,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_organization_filter_metadata"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)))
	}

	return &proto_models.AdminListAllOrganizationsFilterMetadataResponse{
		MaxDepth: metadata.MaxDepth,
	}, nil
}

// AdminChangeOrganizationTypeGeneric AdminChangeOrganizationTypeGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminChangeOrganizationTypeGeneric(r *http.Request, req *proto_models.AdminChangeOrganizationTypeRequest) (*proto_models.AdminChangeOrganizationTypeResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if err := h.serviceHandler.GetOrganizationHandler().ChangeOrganizationType(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.NewOrganizationType,
		req.Reason,
	); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_change_organization_type"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Stringer(appLogger.KeyNewOrganizationType, req.NewOrganizationType),
			zap.String(logkeys.Reason, req.Reason))
	}

	return &proto_models.AdminChangeOrganizationTypeResponse{}, nil
}

// AdminAddUserToOrganizationGeneric AdminAddUserToOrganizationGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminAddUserToOrganizationGeneric(r *http.Request, req *proto_models.AdminAddUserToOrganizationRequest) (*proto_models.AdminAddUserToOrganizationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Get authenticated admin user ID (admin authentication handled by middleware)
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer with admin privilege (bypasses normal permission checks)
	memberResponse, err := h.serviceHandler.GetOrganizationPrivilegedHandler().AddMemberPrivileged(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_add_user_to_organization"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	// Convert service response to proto response using converter
	protoOrgUser := h.serviceHandler.GetConverter().ServiceUserOrganizationToProto(memberResponse.UserOrganization)

	return &proto_models.AdminAddUserToOrganizationResponse{
		OrganizationUser: protoOrgUser,
	}, nil
}

func (h *CommonAPIHandler[U]) AdminAddUserToOrganizationByIdentifierGeneric(r *http.Request, req *proto_models.AdminAddUserToOrganizationByIdentifierRequest) (*proto_models.AdminAddUserToOrganizationByIdentifierResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	result, err := h.serviceHandler.GetOrganizationPrivilegedHandler().AddUserToOrganizationByIdentifierPrivileged(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.IdentifierType,
		req.Identifier,
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_add_user_to_organization_by_identifier"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.String(appLogger.KeyIdentifier, req.Identifier),
			zap.Stringer(appLogger.KeyIdentifierType, req.IdentifierType),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	response := &proto_models.AdminAddUserToOrganizationByIdentifierResponse{
		UserAdded:      result.UserAdded,
		AutoAddPending: result.AutoAddPending,
	}
	if result.OrganizationUser != nil && result.OrganizationUser.UserOrganization != nil {
		response.OrganizationUser = h.convertUserOrganizationToProto(result.OrganizationUser.UserOrganization)
	}
	if result.AutoAddEntry != nil {
		response.AutoAddEntry = h.serviceHandler.GetConverter().ServiceOrganizationInvitationToProto(result.AutoAddEntry, nil)
	}

	return response, nil
}

func (h *CommonAPIHandler[U]) AdminUpdateUserRoleInOrganizationGeneric(r *http.Request, req *proto_models.AdminUpdateUserRoleInOrganizationRequest) (*proto_models.AdminUpdateUserRoleInOrganizationResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	memberResponse, err := h.serviceHandler.GetOrganizationPrivilegedHandler().UpdateMemberRolePrivileged(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Role,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_user_role_in_organization"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewRole, req.Role))
	}

	protoOrgUser := h.serviceHandler.GetConverter().ServiceUserOrganizationToProto(memberResponse.UserOrganization)
	return &proto_models.AdminUpdateUserRoleInOrganizationResponse{
		OrganizationUser: protoOrgUser,
	}, nil
}

func (h *CommonAPIHandler[U]) AdminGetOrganizationMemberGeneric(r *http.Request, req *proto_models.AdminGetOrganizationMemberRequest) (*proto_models.AdminGetOrganizationMemberResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	memberDetails, err := h.serviceHandler.GetOrganizationPrivilegedHandler().GetUserOrganizationMembershipPrivileged(
		r.Context(),
		common_service_models.UserIdType(req.UserId),
		common_service_models.OrganizationIdType(req.OrganizationId),
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_organization_member"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId))
	}

	return &proto_models.AdminGetOrganizationMemberResponse{
		MemberDetails: h.serviceHandler.GetConverter().ServiceOrganizationMemberDetailsToProto(memberDetails),
	}, nil
}

// AdminCreateOrganizationEmailDomain AdminCreateOrganizationEmailDomain handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminCreateOrganizationEmailDomain(r *http.Request, req *proto_models.AdminCreateOrganizationEmailDomainRequest) (*proto_models.AdminCreateOrganizationEmailDomainResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	creatorID := adminID
	serviceDomain, err := h.serviceHandler.GetOrganizationHandler().CreateOrganizationEmailDomain(
		r.Context(),
		common_service_models.OrganizationIdType(req.OrganizationId),
		req.Domain,
		&creatorID,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_create_organization_email_domain"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.String(appLogger.KeyDomain, req.Domain))
	}

	protoDomain := h.serviceHandler.GetConverter().ServiceOrganizationEmailDomainToProto(serviceDomain)

	return &proto_models.AdminCreateOrganizationEmailDomainResponse{
		EmailDomain: protoDomain,
	}, nil
}

// AdminDeleteOrganizationEmailDomain AdminDeleteOrganizationEmailDomain handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminDeleteOrganizationEmailDomain(r *http.Request, req *proto_models.AdminDeleteOrganizationEmailDomainRequest) (*proto_models.AdminDeleteOrganizationEmailDomainResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	err := h.serviceHandler.GetOrganizationHandler().DeleteOrganizationEmailDomain(
		r.Context(),
		0,
		req.OrganizationEmailDomainId,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_delete_organization_email_domain"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Uint64(appLogger.KeyEmailDomainID, req.OrganizationEmailDomainId))
	}

	return &proto_models.AdminDeleteOrganizationEmailDomainResponse{}, nil
}

// AdminListOrganizationEmailDomains AdminListOrganizationEmailDomains handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListOrganizationEmailDomains(r *http.Request, req *proto_models.AdminListOrganizationEmailDomainsRequest) (*proto_models.AdminListOrganizationEmailDomainsResponse, error) {
	adminID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	orgID := req.GetOrganizationId()
	serviceDomains, pagination, err := h.serviceHandler.GetOrganizationHandler().ListOrganizationEmailDomainsPage(
		r.Context(),
		common_service_models.OrganizationIdType(orgID),
		req.Pagination,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_organization_email_domains"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminID)),
			zap.Int64(appLogger.KeyOrganizationID, orgID))
	}

	protoDomains := common_util.ApplyToEach(serviceDomains, func(domain *common_service_models.OrganizationEmailDomain) *proto_models.OrganizationEmailDomain {
		return h.serviceHandler.GetConverter().ServiceOrganizationEmailDomainToProto(domain)
	})

	return &proto_models.AdminListOrganizationEmailDomainsResponse{
		EmailDomains: protoDomains,
		Pagination:   pagination,
	}, nil
}

// AdminListChatChannels AdminListChatChannels handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListChatChannels(r *http.Request, req *proto_models.AdminListChatChannelsRequest) (*proto_models.AdminListChatChannelsResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminListChatChannels(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_chat_channels"))
	}

	return response, nil
}

// AdminGetChatChannel AdminGetChatChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminGetChatChannel(r *http.Request, req *proto_models.AdminGetChatChannelRequest) (*proto_models.AdminGetChatChannelResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminGetChatChannel(ctx, req.GetChannelId(), req.GetIncludeDeleted())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	return response, nil
}

// AdminUpdateChatChannel AdminUpdateChatChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateChatChannel(r *http.Request, req *proto_models.AdminUpdateChatChannelRequest) (*proto_models.AdminUpdateChatChannelResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminUpdateChatChannel(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	return response, nil
}

// AdminDeleteChatChannel AdminDeleteChatChannel handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminDeleteChatChannel(r *http.Request, req *proto_models.AdminDeleteChatChannelRequest) (*proto_models.AdminDeleteChatChannelResponse, error) {
	ctx := r.Context()

	if err := h.serviceHandler.AdminDeleteChatChannel(ctx, req.GetChannelId(), req.GetReason()); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_delete_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.String(logkeys.Reason, req.GetReason()))
	}

	return &proto_models.AdminDeleteChatChannelResponse{}, nil
}

// AdminListChatChannelParticipants AdminListChatChannelParticipants handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminListChatChannelParticipants(r *http.Request, req *proto_models.AdminListChatChannelParticipantsRequest) (*proto_models.AdminListChatChannelParticipantsResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminListChatChannelParticipants(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	return response, nil
}

// AdminAddChatChannelParticipants AdminAddChatChannelParticipants handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminAddChatChannelParticipants(r *http.Request, req *proto_models.AdminAddChatChannelParticipantsRequest) (*proto_models.AdminAddChatChannelParticipantsResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminAddChatChannelParticipants(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_add_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	return response, nil
}

// AdminRemoveChatChannelParticipant AdminRemoveChatChannelParticipant handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminRemoveChatChannelParticipant(r *http.Request, req *proto_models.AdminRemoveChatChannelParticipantRequest) (*proto_models.AdminRemoveChatChannelParticipantResponse, error) {
	ctx := r.Context()

	if err := h.serviceHandler.AdminRemoveChatChannelParticipant(ctx, req.GetChannelId(), req.GetUserId(), req.GetReason()); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_remove_chat_channel_participant"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Uint64(appLogger.KeyTargetUserID, req.GetUserId()),
			zap.String(logkeys.Reason, req.GetReason()))
	}

	return &proto_models.AdminRemoveChatChannelParticipantResponse{}, nil
}

// AdminUpdateChatChannelParticipantRole AdminUpdateChatChannelParticipantRole handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) AdminUpdateChatChannelParticipantRole(r *http.Request, req *proto_models.AdminUpdateChatChannelParticipantRoleRequest) (*proto_models.AdminUpdateChatChannelParticipantRoleResponse, error) {
	ctx := r.Context()

	response, err := h.serviceHandler.AdminUpdateChatChannelParticipantRole(ctx, req)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_chat_channel_participant_role"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Uint64(appLogger.KeyTargetUserID, req.GetUserId()),
			zap.Stringer(appLogger.KeyNewRole, req.GetRole()))
	}

	return response, nil
}

// AdminListInferenceProviders returns the current provider registry's public descriptors.
func (h *CommonAPIHandler[U]) AdminListInferenceProviders(r *http.Request, req *proto_models.ListAdminInferenceProvidersRequest) (*proto_models.ListAdminInferenceProvidersResponse, error) {
	providers, err := h.serviceHandler.AdminListInferenceProviders(r.Context())
	if err != nil {
		return nil, MapServiceErrorWithFields(err, zap.String(logkeys.Operation, "admin_list_inference_providers"))
	}
	return &proto_models.ListAdminInferenceProvidersResponse{Providers: providers}, nil
}
