package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetCurrentUserHandler GetCurrentUserHandler handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) GetCurrentUserHandler(r *http.Request, _ *proto_models.User) (*proto_models.GetCurrentUserResponse, error) {
	// Get the user ID from the context
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()) // Use generic type
	if !found {
		invariantErr := appLogger.InvariantError("authenticated_route_has_user_id", nil,
			zap.String(logkeys.Stage, "get_current_user"),
			zap.String(logkeys.Expected, "authenticated user ID in request context"),
			zap.String(logkeys.Actual, "missing user ID"))
		return nil, router.NewHTTPErrorWithCause(http.StatusUnauthorized, "Unauthorized", invariantErr).
			WithFields(appLogger.ErrorFields(invariantErr)...).
			WithLogLevel(zapcore.ErrorLevel)
	}

	// Get the user from the service using the facade method
	user, err := h.serviceHandler.GetUserByID(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_current_user"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	if user == nil {
		return nil, MapServiceErrorWithFields(
			appLogger.InvariantError("get_current_user_returns_user", nil,
				zap.String(logkeys.Stage, "get_current_user"),
				zap.String(logkeys.Expected, "non-nil user"),
				zap.String(logkeys.Actual, "nil user")),
			zap.String(logkeys.Operation, "get_current_user"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// The workspace root is resolved here and not in the converter because it
	// is session-bootstrap data specific to this endpoint, not a user field.
	// A transient failure hard-fails the request; a conflicted workspace is
	// absorbed as absence in the service layer (logged there) so corruption
	// cannot lock the user out of their own profile.
	workspaceRootID, err := h.serviceHandler.GetPersonalWorkspaceRootID(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "resolve_personal_workspace_root"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert to proto using the configured converter
	protoUser := h.serviceHandler.GetConverter().ConvertUserLikeToProto(r.Context(), *user)
	return &proto_models.GetCurrentUserResponse{
		User:                                protoUser,
		PersonalWorkspaceRootOrganizationId: workspaceRootID,
	}, nil
}

// SearchPublicUsersHandler SearchPublicUsersHandler handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) SearchPublicUsersHandler(
	r *http.Request,
	req *proto_models.SearchPublicUsersRequest,
) (*proto_models.SearchPublicUsersResponse, error) {
	params := h.serviceHandler.GetConverter().SearchPublicUsersParamsFromProto(req)
	resp, err := h.serviceHandler.SearchPublicUsers(r.Context(), params)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "search_public_users"),
			zap.String(appLogger.KeySearch, req.GetQuery()))
	}

	return h.serviceHandler.GetConverter().SearchPublicUsersResultToProto(resp), nil
}

// RegisterUserRoutes RegisterUserRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterUserRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	// Define route for getting the current user
	getCurrentUserRoute := router.RouteConfig[*proto_models.User, *proto_models.GetCurrentUserResponse]{
		Path:       pathUserMe, // Path relative to the route group's prefix
		Methods:    []router.HttpMethod{http.MethodGet, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.User, *proto_models.GetCurrentUserResponse](),
		SourceType: router.Empty,
		Handler:    h.GetCurrentUserHandler,
		// Timeout and body-size policy inherit from the route group and router.
		// Route-specific policy can be supplied through Overrides when needed.
	}
	group.Route(getCurrentUserRoute)

	{

		searchUsersRoute := router.RouteConfig[*proto_models.SearchPublicUsersRequest, *proto_models.SearchPublicUsersResponse]{
			Path:       "/user/search",
			Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
			AuthLevel:  new(router.AuthRequired),
			Codec:      codec.NewProtoCodec[*proto_models.SearchPublicUsersRequest, *proto_models.SearchPublicUsersResponse](),
			Handler:    h.SearchPublicUsersHandler,
			SourceType: router.Body,
			Sanitizer:  h.sanitizer.SanitizeSearchPublicUsersRequest,
		}
		h.featureGroup(group, config.APIView.EnableUserSearch).Route(searchUsersRoute)
	}

	h.logger.Named(commonAPILoggerName).Debug("User routes registered to route group")
}
