package chat

import (
	"context"
	"fmt"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SetChannelPublicRead sets who beyond participants may read a channel (see ChatChannelPublicRead)
// and returns the updated channel. The caller must be the owner (archived channels are allowed) and
// pass Authorize with ChatPermissionUpdateChannel and ProposedPublicRead set to level. An undefined
// level, or any level but off on a direct channel, wraps ErrInvalidRequest.
// Lowering the level revokes realtime viewers it no longer admits. Requires an ambient transaction.
func (h *Handler[U]) SetChannelPublicRead(
	ctx context.Context,
	channelID uint64,
	level proto_models.ChatChannelPublicRead,
) (*common_service_models.ChatChannel, error) {
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
	channel, err := h.requireChannelOwner(ctx, channelID, actorID, true)
	if err != nil {
		return nil, err
	}
	if channel.ChannelType == proto_models.ChannelType_CHANNEL_TYPE_DIRECT && level != proto_models.ChatChannelPublicRead_CHAT_CHANNEL_PUBLIC_READ_UNSPECIFIED {
		return nil, fmt.Errorf("%w: a direct channel cannot be made public", serviceErrors.ErrInvalidRequest)
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:          ports.ChatPermissionUpdateChannel,
		Actor:              actor,
		Channel:            channel,
		ProposedPublicRead: &level,
	}); err != nil {
		return nil, err
	}

	updated, err := h.persistenceHandler.SetChatChannelPublicRead(ctx, channelID, level)
	if err != nil {
		return nil, channelAccessWriteError(err, persistence.ErrChatChannelPublicReadInvalid)
	}

	// The level is read from the cached channel row, so the change must evict it (now and again
	// after commit). Realtime viewer grants are suspended before the event goes to refreshed
	// participants; viewers the new level still admits catch up on renewal, and viewers it revokes
	// are cut off. The event carries the fresh row, never the cached one, whose last-message
	// preview may be stale.
	h.invalidateChannelAccessCache(ctx, channelID)
	out := h.converter.PersistenceChatChannelToService(updated)
	h.announceAccessChangeAfterCommit(ctx, channelID, func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
			OccurredAt:  time.Now(),
			ActorUserID: actorID,
			Channel:     out,
		}
	})
	return out, nil
}
