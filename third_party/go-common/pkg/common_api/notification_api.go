package common_api

import (
	"context"
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

func notificationAPIError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, notification.ErrDisabled):
		return router.NewHTTPError(http.StatusNotFound, "Notifications disabled")
	case errors.Is(err, notification.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Notification not found")
	case errors.Is(err, notification.ErrInvalid), errors.Is(err, notification.ErrUnknownType):
		return ErrInvalidRequest
	default:
		return MapServiceError(err)
	}
}

func (h *CommonAPIHandler[U]) notificationUserID(ctx context.Context) (uint64, error) {
	id, ok := scontext.GetUserID[common_service_models.UserIdType](ctx)
	if !ok || id == 0 {
		return 0, ErrUnauthorized
	}
	return uint64(id), nil
}

// RegisterNotificationRoutes installs authenticated protobuf inbox operations.
// Every recipient is bound to authentication context; requests contain no recipient ID.
func (h *CommonAPIHandler[U]) RegisterNotificationRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, func(c config.APIView) bool { return c.EnableNotifications() })
	group.Route(router.RouteConfig[*proto_models.ListNotificationsRequest, *proto_models.ListNotificationsResponse]{Path: "/notifications/list", Methods: []router.HttpMethod{http.MethodPost}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.ListNotificationsRequest, *proto_models.ListNotificationsResponse](), SourceType: router.Body, Sanitizer: h.sanitizer.SanitizeListNotificationsRequest, Handler: h.ListNotifications})
	group.Route(router.RouteConfig[*proto_models.GetNotificationRequest, *proto_models.Notification]{Path: "/notifications/get", Methods: []router.HttpMethod{http.MethodPost}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.GetNotificationRequest, *proto_models.Notification](), SourceType: router.Body, Sanitizer: h.sanitizer.SanitizeGetNotificationRequest, Handler: h.GetNotification})
	group.Route(router.RouteConfig[*proto_models.NotificationCountRequest, *proto_models.NotificationCountResponse]{Path: "/notifications/count", Methods: []router.HttpMethod{http.MethodGet}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.NotificationCountRequest, *proto_models.NotificationCountResponse](), SourceType: router.Empty, Handler: h.CountNotifications})
	group.Route(router.RouteConfig[*proto_models.MarkNotificationsReadRequest, *proto_models.NotificationMutationResponse]{Path: "/notifications/read", Methods: []router.HttpMethod{http.MethodPost}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.MarkNotificationsReadRequest, *proto_models.NotificationMutationResponse](), SourceType: router.Body, Sanitizer: h.sanitizer.SanitizeMarkNotificationsReadRequest, Handler: h.MarkNotificationsRead, Middlewares: []common.Middleware{h.GetTransactionMiddleware()}})
	group.Route(router.RouteConfig[*proto_models.ClaimNotificationAlertRequest, *proto_models.ClaimNotificationAlertResponse]{Path: "/notifications/alert/claim", Methods: []router.HttpMethod{http.MethodPost}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.ClaimNotificationAlertRequest, *proto_models.ClaimNotificationAlertResponse](), SourceType: router.Body, Sanitizer: h.sanitizer.SanitizeClaimNotificationAlertRequest, Handler: h.ClaimNotificationAlert})
	group.Route(router.RouteConfig[*proto_models.NotificationPreferencesRequest, *proto_models.NotificationPreferencesResponse]{Path: "/notifications/preferences", Methods: []router.HttpMethod{http.MethodGet}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.NotificationPreferencesRequest, *proto_models.NotificationPreferencesResponse](), SourceType: router.Empty, Handler: h.GetNotificationPreferences})
	group.Route(router.RouteConfig[*proto_models.UpdateNotificationPreferencesRequest, *proto_models.NotificationMutationResponse]{Path: "/notifications/preferences", Methods: []router.HttpMethod{http.MethodPut}, AuthLevel: new(router.AuthRequired), Codec: codec.NewProtoCodec[*proto_models.UpdateNotificationPreferencesRequest, *proto_models.NotificationMutationResponse](), SourceType: router.Body, Sanitizer: h.sanitizer.SanitizeUpdateNotificationPreferencesRequest, Handler: h.UpdateNotificationPreferences, Middlewares: []common.Middleware{h.GetTransactionMiddleware()}})
}

func (h *CommonAPIHandler[U]) ListNotifications(r *http.Request, req *proto_models.ListNotificationsRequest) (*proto_models.ListNotificationsResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	out, err := h.serviceHandler.GetNotificationHandler().List(r.Context(), id, req)
	return out, notificationAPIError(err)
}
func (h *CommonAPIHandler[U]) GetNotification(r *http.Request, req *proto_models.GetNotificationRequest) (*proto_models.Notification, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	out, err := h.serviceHandler.GetNotificationHandler().Get(r.Context(), id, req.Id)
	return out, notificationAPIError(err)
}
func (h *CommonAPIHandler[U]) CountNotifications(r *http.Request, _ *proto_models.NotificationCountRequest) (*proto_models.NotificationCountResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	count, err := h.serviceHandler.CountNotifications(r.Context(), id)
	if err != nil {
		return nil, notificationAPIError(err)
	}
	return &proto_models.NotificationCountResponse{UnreadCount: count.UnreadCount, Complete: count.Complete}, nil
}
func (h *CommonAPIHandler[U]) MarkNotificationsRead(r *http.Request, req *proto_models.MarkNotificationsReadRequest) (*proto_models.NotificationMutationResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	err = h.serviceHandler.GetNotificationHandler().MarkRead(r.Context(), id, req.Receipts)
	if err != nil {
		return nil, notificationAPIError(err)
	}
	return &proto_models.NotificationMutationResponse{Success: true}, nil
}
func (h *CommonAPIHandler[U]) ClaimNotificationAlert(r *http.Request, req *proto_models.ClaimNotificationAlertRequest) (*proto_models.ClaimNotificationAlertResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	out, err := h.serviceHandler.GetNotificationHandler().ClaimAlert(r.Context(), id, req.Id, req.Revision)
	return out, notificationAPIError(err)
}
func (h *CommonAPIHandler[U]) GetNotificationPreferences(r *http.Request, _ *proto_models.NotificationPreferencesRequest) (*proto_models.NotificationPreferencesResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	out, err := h.serviceHandler.GetNotificationHandler().Preferences(r.Context(), id)
	return out, notificationAPIError(err)
}
func (h *CommonAPIHandler[U]) UpdateNotificationPreferences(r *http.Request, req *proto_models.UpdateNotificationPreferencesRequest) (*proto_models.NotificationMutationResponse, error) {
	id, err := h.notificationUserID(r.Context())
	if err != nil {
		return nil, err
	}
	err = h.serviceHandler.GetNotificationHandler().UpdatePreferences(r.Context(), id, req)
	if err != nil {
		return nil, notificationAPIError(err)
	}
	return &proto_models.NotificationMutationResponse{Success: true}, nil
}
