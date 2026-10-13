package common_service

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// GetPublicChatChannel charges the client IP's ActionPublicChatRead budget, then runs the chat
// handler's GetPublicChannel. NoAuth routes call it.
func (h *CommonServiceHandler[U]) GetPublicChatChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error) {
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionPublicChatRead); err != nil {
		return nil, err
	}
	return h.chatHandler.GetPublicChannel(ctx, channelID)
}

// ListPublicChatMessages charges the client IP's ActionPublicChatRead budget, then runs the chat
// handler's ListPublicMessages. NoAuth routes call it.
func (h *CommonServiceHandler[U]) ListPublicChatMessages(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
	parentMessageID uint64,
	sort *proto_models.SortRequest,
	sortBy proto_models.ListMessagesSortBy,
) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error) {
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionPublicChatRead); err != nil {
		return nil, nil, err
	}
	return h.chatHandler.ListPublicMessages(ctx, channelID, pagination, parentMessageID, sort, sortBy)
}
