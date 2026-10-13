package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// registerPublicReadRoutes registers the owner's public read setter and the logged-out reads of
// CHAT_CHANNEL_PUBLIC_READ_ANYONE channels. The NoAuth reads call the service facade, which charges
// the client IP's public chat read rate limit before delegating to the chat handler.
func (h *CommonAPIHandler[U]) registerPublicReadRoutes(
	group *router.RouteGroup[common_service_models.UserIdType, U],
	txMiddleware common.Middleware,
) {
	group.Route(router.RouteConfig[*proto_models.SetChannelPublicReadRequest, *proto_models.SetChannelPublicReadResponse]{
		Path:        "/chat/channels/public_read/set",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.SetChannelPublicReadRequest, *proto_models.SetChannelPublicReadResponse](),
		Handler:     h.SetChannelPublicRead,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeSetChannelPublicReadRequest,
		Middlewares: []common.Middleware{txMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.GetPublicChannelRequest, *proto_models.GetPublicChannelResponse]{
		Path:       "/chat/public/channels/get",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.GetPublicChannelRequest, *proto_models.GetPublicChannelResponse](),
		Handler:    h.GetPublicChannel,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeGetPublicChannelRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ListPublicMessagesRequest, *proto_models.ListPublicMessagesResponse]{
		Path:       "/chat/public/messages/list",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.ListPublicMessagesRequest, *proto_models.ListPublicMessagesResponse](),
		Handler:    h.ListPublicMessages,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListPublicMessagesRequest,
	})
}

// SetChannelPublicRead sets who beyond participants may read a channel; the service enforces the
// owner gate.
func (h *CommonAPIHandler[U]) SetChannelPublicRead(
	r *http.Request,
	req *proto_models.SetChannelPublicReadRequest,
) (*proto_models.SetChannelPublicReadResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	channel, err := h.serviceHandler.GetChatHandler().SetChannelPublicRead(r.Context(), req.ChannelId, req.PublicRead)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "set_chat_channel_public_read"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Stringer(appLogger.KeyPublicRead, req.PublicRead))
	}
	return &proto_models.SetChannelPublicReadResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// GetPublicChannel reads a public channel with no session.
func (h *CommonAPIHandler[U]) GetPublicChannel(
	r *http.Request,
	req *proto_models.GetPublicChannelRequest,
) (*proto_models.GetPublicChannelResponse, error) {
	channel, err := h.serviceHandler.GetPublicChatChannel(r.Context(), req.ChannelId)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_public_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	return &proto_models.GetPublicChannelResponse{
		Channel: h.serviceHandler.GetConverter().ServiceChatChannelToProto(channel),
	}, nil
}

// ListPublicMessages lists a public channel's messages with no session.
func (h *CommonAPIHandler[U]) ListPublicMessages(
	r *http.Request,
	req *proto_models.ListPublicMessagesRequest,
) (*proto_models.ListPublicMessagesResponse, error) {
	ctx := r.Context()
	rows, page, err := h.serviceHandler.ListPublicChatMessages(ctx, req.ChannelId, req.Pagination, req.ParentMessageId, req.Sort, req.SortBy)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_public_chat_messages"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	converter := h.serviceHandler.GetConverter()
	return &proto_models.ListPublicMessagesResponse{
		Messages: common_util.ApplyToEach(rows, func(m *common_service_models.ChatMessage) *proto_models.Message {
			return converter.ServiceChatMessageToProto(ctx, m)
		}),
		Pagination: page,
	}, nil
}
