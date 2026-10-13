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

// RegisterUserSettingsRoutes RegisterUserSettingsRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterUserSettingsRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableUserSettings)
	// Get transaction middleware for write operations
	transactionMiddleware := h.GetTransactionMiddleware()

	// Define route for getting user settings
	getUserSettingsRoute := router.RouteConfig[*proto_models.UserSettings, *proto_models.UserSettings]{
		Path:       "/user/settings", // Path relative to the route group's prefix
		Methods:    []router.HttpMethod{http.MethodGet, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.UserSettings, *proto_models.UserSettings](),
		SourceType: router.Empty,
		Handler:    h.GetUserSettingsGeneric,
	}
	group.Route(getUserSettingsRoute)

	// Define route for updating user settings (partial update)
	updateUserSettingsRoute := router.RouteConfig[*proto_models.UpdateUserSettingsRequest, *proto_models.UserSettings]{
		Path:        "/user/settings", // Path relative to the route group's prefix
		Methods:     []router.HttpMethod{http.MethodPut},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateUserSettingsRequest, *proto_models.UserSettings](),
		Handler:     h.UpdateUserSettingsGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateUserSettingsRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(updateUserSettingsRoute)

	// Define route for updating user language preference
	updateLanguageRoute := router.RouteConfig[*proto_models.UpdateLanguagePreferenceRequest, *proto_models.UpdateLanguagePreferenceResponse]{
		Path:        "/user/settings/language",
		Methods:     []router.HttpMethod{http.MethodPut},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateLanguagePreferenceRequest, *proto_models.UpdateLanguagePreferenceResponse](),
		Handler:     h.UpdateUserLanguagePreferenceGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateLanguagePreferenceRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(updateLanguageRoute)

	// Define route for removing user phone number
	removePhoneRoute := router.RouteConfig[*proto_models.RemovePhoneNumberRequest, *proto_models.RemovePhoneNumberResponse]{
		Path:        "/user/phone/remove",
		Methods:     []router.HttpMethod{http.MethodPost},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RemovePhoneNumberRequest, *proto_models.RemovePhoneNumberResponse](),
		Handler:     h.RemovePhoneNumberGeneric,
		SourceType:  router.Body,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(removePhoneRoute)

	// Define route for updating username
	updateUsernameRoute := router.RouteConfig[*proto_models.UpdateUsernameRequest, *proto_models.UpdateUsernameResponse]{
		Path:        pathUserUsername,
		Methods:     []router.HttpMethod{http.MethodPut},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.UpdateUsernameRequest, *proto_models.UpdateUsernameResponse](),
		Handler:     h.UpdateUsernameGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUpdateUsernameRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(updateUsernameRoute)

	h.logger.Named(commonAPILoggerName).Debug("User settings routes registered to route group")
}

// GetUserSettingsGeneric GetUserSettingsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetUserSettingsGeneric(r *http.Request, _ *proto_models.UserSettings) (*proto_models.UserSettings, error) {
	// Get the user ID from the context
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()) // Use generic type
	if !found {
		return nil, ErrUnauthorized
	}

	// Get user settings from service layer using the facade method
	userSettings, err := h.serviceHandler.GetUserSettings(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_settings"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert service model to proto model
	protoSettings := h.serviceHandler.GetConverter().ConvertUserSettingsToProto(userSettings)

	return protoSettings, nil
}

// UpdateUserSettingsGeneric UpdateUserSettingsGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateUserSettingsGeneric(r *http.Request, req *proto_models.UpdateUserSettingsRequest) (*proto_models.UserSettings, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Map the present (optional) proto fields into a partial service update.
	// Absent fields stay nil and are left unchanged by the service layer.
	update := &common_service_models.UserSettingsUpdate{
		DarkMode:               req.DarkMode,
		EmailNotifications:     req.EmailNotifications,
		PushNotifications:      req.PushNotifications,
		MarketingEmails:        req.MarketingEmails,
		Language:               req.Language,
		ChatEmailNotifications: req.ChatEmailNotifications,
		ChatPushNotifications:  req.ChatPushNotifications,
	}

	// Call service layer to apply the partial update; it returns the merged
	// settings, so no second read is needed to build the response.
	updatedSettings, err := h.serviceHandler.UpdateUserSettings(r.Context(), uint64(userID), update)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_user_settings"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert service model to proto model
	return h.serviceHandler.GetConverter().ConvertUserSettingsToProto(updatedSettings), nil
}

// UpdateUserLanguagePreferenceGeneric UpdateUserLanguagePreferenceGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateUserLanguagePreferenceGeneric(r *http.Request, req *proto_models.UpdateLanguagePreferenceRequest) (*proto_models.UpdateLanguagePreferenceResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer to update language preference
	err := h.serviceHandler.UpdateUserLanguagePreference(r.Context(), uint64(userID), req.Language)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_user_language_preference"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Any(appLogger.KeyLanguage, req.Language))
	}

	// Get updated settings to return
	updatedSettings, err := h.serviceHandler.GetUserSettings(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_settings_after_language_update"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert service model to proto model and return
	return &proto_models.UpdateLanguagePreferenceResponse{
		UserSettings: h.serviceHandler.GetConverter().ConvertUserSettingsToProto(updatedSettings),
	}, nil
}

// RemovePhoneNumberGeneric RemovePhoneNumberGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) RemovePhoneNumberGeneric(r *http.Request, req *proto_models.RemovePhoneNumberRequest) (*proto_models.RemovePhoneNumberResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	if _, err := validateRecentSessionFromRequest(r.Context(), r, h.startup.SessionCookieName(), h.serviceHandler); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "remove_user_phone_number"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Call service layer to remove phone number
	err := h.serviceHandler.RemoveUserPhoneNumber(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "remove_user_phone_number"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Get updated user to return
	user, err := h.serviceHandler.GetUserByID(r.Context(), common_service_models.UserIdType(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_after_phone_removal"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert service model to proto model
	protoUser := h.serviceHandler.GetConverter().ConvertUserLikeToProto(r.Context(), *user)

	return &proto_models.RemovePhoneNumberResponse{
		User: protoUser,
	}, nil
}

// UpdateUsernameGeneric UpdateUsernameGeneric handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) UpdateUsernameGeneric(r *http.Request, req *proto_models.UpdateUsernameRequest) (*proto_models.UpdateUsernameResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	// Call service layer to update username
	err := h.serviceHandler.UpdateUserUsername(r.Context(), uint64(userID), req.Username)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "update_user_username"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyNewUsername, req.Username))
	}

	// Get updated user to return
	user, err := h.serviceHandler.GetUserByID(r.Context(), common_service_models.UserIdType(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_after_username_update"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyNewUsername, req.Username))
	}

	// Convert service model to proto model
	protoUser := h.serviceHandler.GetConverter().ConvertUserLikeToProto(r.Context(), *user)

	return &proto_models.UpdateUsernameResponse{
		User: protoUser,
	}, nil
}
