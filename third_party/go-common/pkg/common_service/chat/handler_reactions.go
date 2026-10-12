package chat

import (
	"context"
	"errors"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// AddReaction executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) AddReaction(ctx context.Context, messageID uint64, emoji string) (*common_service_models.ChatMessageReaction, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	msg, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		return nil, mapChatMessagePersistenceError(getErr)
	}

	if err := requireStandardMutableMessage(msg); err != nil {
		return nil, err
	}

	channel, err := h.requireChannelMember(ctx, msg.ChannelID, actorID)
	if err != nil {
		return nil, err
	}
	if !channel.GetSupportMessageReactions() {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}

	messageForAuth := h.converter.PersistenceChatMessageToService(msg)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionAddReaction,
		Actor:     actor,
		Channel:   channel,
		Message:   messageForAuth,
		Emoji:     emoji,
	}); err != nil {
		return nil, err
	}

	var existingSame *common_persistence_models.ChatMessageReaction
	var existingOther *common_persistence_models.ChatMessageReaction
	for i := range msg.Reactions {
		reaction := &msg.Reactions[i]
		if reaction.UserID != actorID {
			continue
		}
		if reaction.Emoji == emoji {
			existingSame = reaction
			break
		}
		if existingOther == nil {
			existingOther = reaction
		}
	}
	if existingSame != nil {
		return h.converter.PersistenceChatMessageReactionToService(existingSame), nil
	}
	if existingOther != nil && !channel.GetAllowMultipleReactionsPerUser() {
		return nil, serviceErrors.ErrChatReactionAlreadyExists
	}

	var reaction *common_persistence_models.ChatMessageReaction
	var addErr error
	if channel.GetAllowMultipleReactionsPerUser() {
		reaction, addErr = h.persistenceHandler.AddChatMessageReaction(ctx, messageID, actorID, emoji)
	} else {
		// The preloaded-reaction scan above is only a fast path; the single-reaction policy is
		// enforced under a message row lock so concurrent adds of different emojis cannot both
		// slip past a stale snapshot.
		reaction, addErr = h.persistenceHandler.AddChatMessageReactionSingle(ctx, messageID, actorID, emoji)
	}
	if addErr != nil {
		switch {
		case errors.Is(addErr, persistence.ErrChatReactionConflict):
			return nil, serviceErrors.ErrChatReactionAlreadyExists
		case errors.Is(addErr, persistence.ErrChatMessageNotFound):
			return nil, serviceErrors.ErrChatMessageNotFound
		case errors.Is(addErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}

	out := h.converter.PersistenceChatMessageReactionToService(reaction)
	if h.eventPublisher != nil && out != nil {
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_ADDED,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(actorID),
				Reaction:    out,
				Message: &common_service_models.ChatMessage{
					ID:              messageID,
					ChannelID:       msg.ChannelID,
					Kind:            msg.Kind,
					HiddenFromUsers: msg.HiddenFromUsers,
				},
			}
		})
	}
	return out, nil
}

// RemoveReaction executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) RemoveReaction(ctx context.Context, messageID uint64, emoji string) error {
	if h.chatDisabled() {
		return serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return err
	}

	msg, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		return mapChatMessagePersistenceError(getErr)
	}
	if err := requireStandardMutableMessage(msg); err != nil {
		return err
	}

	channel, err := h.requireChannelMember(ctx, msg.ChannelID, actorID)
	if err != nil {
		return err
	}
	if !channel.GetSupportMessageReactions() {
		return serviceErrors.ErrChatFeatureDisabled
	}

	messageForAuth := h.converter.PersistenceChatMessageToService(msg)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionRemoveReaction,
		Actor:     actor,
		Channel:   channel,
		Message:   messageForAuth,
		Emoji:     emoji,
	}); err != nil {
		return err
	}

	if removeErr := h.persistenceHandler.RemoveChatMessageReaction(ctx, messageID, actorID, emoji); removeErr != nil {
		switch {
		// Nothing was removed, so no REACTION_REMOVED event may be published.
		case errors.Is(removeErr, persistence.ErrChatReactionNotFound):
			return serviceErrors.ErrChatReactionNotFound
		case errors.Is(removeErr, persistence.ErrDatabaseError):
			return serviceErrors.ErrDatabaseError
		default:
			return serviceErrors.ErrInternalError
		}
	}

	if h.eventPublisher != nil {
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_REMOVED,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(actorID),
				Reaction: &common_service_models.ChatMessageReaction{
					MessageID: messageID,
					UserID:    common_service_models.UserIdType(actorID),
					Emoji:     emoji,
				},
				Message: &common_service_models.ChatMessage{
					ID:              messageID,
					ChannelID:       msg.ChannelID,
					Kind:            msg.Kind,
					HiddenFromUsers: msg.HiddenFromUsers,
				},
			}
		})
	}

	return nil
}

// ListReactions executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListReactions(
	ctx context.Context,
	messageID uint64,
	pagination *proto_models.PaginationRequest,
	emojiFilter *string,
) ([]*common_service_models.ChatMessageReaction, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}

	msg, getErr := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if getErr != nil {
		return nil, nil, mapChatMessagePersistenceError(getErr)
	}
	if err := requireStandardMutableMessage(msg); err != nil {
		return nil, nil, err
	}

	channel, err := h.requireChannelRead(ctx, msg.ChannelID, actor)
	if err != nil {
		return nil, nil, err
	}
	if !channel.GetSupportMessageReactions() {
		return nil, nil, serviceErrors.ErrChatFeatureDisabled
	}

	emojiValue := ""
	if emojiFilter != nil {
		emojiValue = *emojiFilter
	}

	messageForAuth := h.converter.PersistenceChatMessageToService(msg)
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionListReactions,
		Actor:     actor,
		Channel:   channel,
		Message:   messageForAuth,
		Emoji:     emojiValue,
	}); err != nil {
		return nil, nil, err
	}

	rows, pageResp, listErr := h.persistenceHandler.ListChatMessageReactions(ctx, messageID, pagination, emojiFilter)
	if listErr != nil {
		switch {
		case errors.Is(listErr, persistence.ErrDatabaseError):
			return nil, nil, serviceErrors.ErrDatabaseError
		default:
			return nil, nil, serviceErrors.ErrInternalError
		}
	}

	out := common_util.ApplyToEach(rows, func(r *common_persistence_models.ChatMessageReaction) *common_service_models.ChatMessageReaction {
		return h.converter.PersistenceChatMessageReactionToService(r)
	})
	return out, pageResp, nil
}
