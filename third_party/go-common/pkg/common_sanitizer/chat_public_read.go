package common_sanitizer

import (
	"context"

	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// SanitizeSetChannelPublicReadRequest requires a channel ID and a defined public read level;
// UNSPECIFIED is a valid level (off).
func (h *Handler) SanitizeSetChannelPublicReadRequest(ctx context.Context, req *proto_models.SetChannelPublicReadRequest) (*proto_models.SetChannelPublicReadRequest, error) {
	const stage = "sanitize_set_channel_public_read_request"
	if _, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if _, ok := proto_models.ChatChannelPublicRead_name[int32(req.PublicRead)]; !ok {
		return nil, h.reject(ctx, stage, ErrInvalidChannelPublicRead,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Stringer(appLogger.KeyPublicRead, req.PublicRead))
	}
	return req, nil
}

// SanitizeGetPublicChannelRequest requires a channel ID on the logged-out channel read.
func (h *Handler) SanitizeGetPublicChannelRequest(ctx context.Context, req *proto_models.GetPublicChannelRequest) (*proto_models.GetPublicChannelRequest, error) {
	if _, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired)); err != nil {
		return nil, h.reject(ctx, "sanitize_get_public_channel_request", err)
	}
	return req, nil
}

// SanitizeListPublicMessagesRequest requires a channel ID and normalizes pagination on the
// logged-out message listing.
func (h *Handler) SanitizeListPublicMessagesRequest(ctx context.Context, req *proto_models.ListPublicMessagesRequest) (*proto_models.ListPublicMessagesRequest, error) {
	if _, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired)); err != nil {
		return nil, h.reject(ctx, "sanitize_list_public_messages_request", err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}
