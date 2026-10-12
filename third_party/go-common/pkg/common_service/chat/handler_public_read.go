package chat

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// GetPublicChannel reads a CHAT_CHANNEL_PUBLIC_READ_ANYONE channel with no actor; see
// requirePublicRead for who is refused and how.
func (h *Handler[U]) GetPublicChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error) {
	return h.requirePublicRead(ctx, ports.ChatAnonymousReadRequest{Operation: ports.ChatPermissionGetChannel}, channelID)
}

// ListPublicMessages lists a CHAT_CHANNEL_PUBLIC_READ_ANYONE channel's messages with no actor,
// under the same rules as GetPublicChannel. pagination, parentMessageID, sort and sortBy behave as
// in ListMessages; hidden and deleted messages are never returned. Reactions keep their counts but
// not their reactors: each reaction's UserID is a per-message ordinal (1..n, one per distinct
// reactor) with no user or timestamp. Pre-edit text and tool-call actors and resolution reasons are
// cleared. Mentioned users stay, as part of the message content.
func (h *Handler[U]) ListPublicMessages(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
	parentMessageID uint64,
	sort *proto_models.SortRequest,
	sortBy proto_models.ListMessagesSortBy,
) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error) {
	channel, err := h.requirePublicRead(ctx, ports.ChatAnonymousReadRequest{
		Operation:       ports.ChatPermissionListMessages,
		ParentMessageID: parentMessageID,
	}, channelID)
	if err != nil {
		return nil, nil, err
	}
	readOpts := persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityVisibleToActor}
	messages, page, err := h.listChannelMessages(ctx, channel, pagination, parentMessageID, readOpts, sort, sortBy, 0)
	for _, message := range messages {
		message.OriginalContent = nil
		if message.ToolCall != nil {
			message.ToolCall.TriggeringUserID, message.ToolCall.ResolvedByUserID, message.ToolCall.ResolutionReason = nil, nil, nil
		}
		ordinals := make(map[common_service_models.UserIdType]common_service_models.UserIdType)
		for _, reaction := range message.Reactions {
			if _, seen := ordinals[reaction.UserID]; !seen {
				ordinals[reaction.UserID] = common_service_models.UserIdType(len(ordinals) + 1)
			}
			*reaction = common_service_models.ChatMessageReaction{MessageID: reaction.MessageID, UserID: ordinals[reaction.UserID], Emoji: reaction.Emoji}
		}
	}
	return messages, page, err
}

// requirePublicRead is the anonymous read gate. It loads the channel row fresh (an anonymous reader
// has no membership to cache) and admits it only while enable_public_chat_read is on and the
// channel is CHAT_CHANNEL_PUBLIC_READ_ANYONE; archive state never blocks the read. A missing
// channel, a lower level and the flag being off all answer ErrChatChannelNotFound, so a stranger
// cannot tell them apart. AuthorizeAnonymous then sees req with Channel filled in; without it, an
// installed Authorize fails the read closed with the same ErrChatChannelNotFound.
func (h *Handler[U]) requirePublicRead(ctx context.Context, req ports.ChatAnonymousReadRequest, channelID uint64) (*common_service_models.ChatChannel, error) {
	policy := h.policy()
	if !policy.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if !policy.EnablePublicChatRead() {
		return nil, serviceErrors.ErrChatChannelNotFound
	}
	row, _, _, err := h.loadChannelAccess(ctx, channelID, 0)
	if err != nil {
		return nil, err
	}
	if row.PublicRead != proto_models.ChatChannelPublicRead_CHAT_CHANNEL_PUBLIC_READ_ANYONE {
		return nil, serviceErrors.ErrChatChannelNotFound
	}
	req.Channel = h.converter.PersistenceChatChannelToService(row)
	if cb := h.permissionCallbacks; cb != nil && cb.Callbacks != nil && cb.Callbacks.AuthorizeAnonymous != nil {
		err = cb.Callbacks.AuthorizeAnonymous(ctx, req)
	} else if h.hasPermissionCallback() {
		err = serviceErrors.ErrChatChannelNotFound
	}
	if err != nil {
		return nil, err
	}
	return req.Channel, nil
}
