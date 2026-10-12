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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HandleCreateProfilePictureUploadURL HandleCreateProfilePictureUploadURL handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) HandleCreateProfilePictureUploadURL(r *http.Request, req *proto_models.CreateProfilePictureUploadURLRequest) (*proto_models.CreateProfilePictureUploadURLResponse, error) {
	// Input is already sanitized by SRouter

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer - leverages the attachment system
	// Use content type from request, fallback to "image/jpeg" if not provided
	contentType := "image/jpeg"
	if req.ContentType != nil && *req.ContentType != "" {
		contentType = *req.ContentType
	}
	attachmentResp, err := h.serviceHandler.CreateProfilePictureUploadURL(
		r.Context(),
		uint64(userID),
		req.Filename,
		contentType,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_profile_picture_upload_url"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyFilename, req.Filename),
			zap.String(appLogger.KeyContentType, contentType))
	}

	// Convert service response to proto response
	response := &proto_models.CreateProfilePictureUploadURLResponse{
		UploadUrl:    attachmentResp.URL,
		FormData:     attachmentResp.FormData,
		AttachmentId: attachmentResp.ID,
		Expiry:       timestamppb.New(attachmentResp.Expiry),
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Debug("Created profile picture upload URL",
			zap.String(logkeys.Operation, "create_profile_picture_upload_url"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentResp.ID),
			zap.String(appLogger.KeyFilename, req.Filename),
			zap.String(appLogger.KeyContentType, contentType))
	}

	return response, nil
}

// HandleRemoveProfilePicture HandleRemoveProfilePicture handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) HandleRemoveProfilePicture(r *http.Request, _ *proto_models.RemoveProfilePictureRequest) (*proto_models.RemoveProfilePictureResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer to remove profile picture
	err := h.serviceHandler.RemoveUserProfilePicture(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_profile_picture"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Removed profile picture",
		zap.String(logkeys.Operation, "remove_profile_picture"),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)))

	return &proto_models.RemoveProfilePictureResponse{}, nil
}

// RegisterProfilePictureRoutes RegisterProfilePictureRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterProfilePictureRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableUserProfiles)
	// Get transaction middleware for remove operation
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.CreateProfilePictureUploadURLRequest, *proto_models.CreateProfilePictureUploadURLResponse]{
		Path:       "/user/profile-picture/upload-url",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.CreateProfilePictureUploadURLRequest, *proto_models.CreateProfilePictureUploadURLResponse](),
		Handler:    h.HandleCreateProfilePictureUploadURL,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeCreateProfilePictureUploadURLRequest,
	})
	group.Route(router.RouteConfig[*proto_models.RemoveProfilePictureRequest, *proto_models.RemoveProfilePictureResponse]{
		Path:        "/user/profile-picture",
		Methods:     []router.HttpMethod{http.MethodDelete, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemoveProfilePictureRequest, *proto_models.RemoveProfilePictureResponse](),
		Handler:     h.HandleRemoveProfilePicture,
		SourceType:  router.Empty,
		Middlewares: []common.Middleware{transactionMiddleware}, // DB write operation
	})

	h.logger.Named(commonAPILoggerName).Debug("Profile picture routes registered")
}
