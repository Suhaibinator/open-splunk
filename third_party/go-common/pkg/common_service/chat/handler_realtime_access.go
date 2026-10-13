package chat

import (
	"context"
	"errors"
	"fmt"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// SetChatRealtimeAccessInvalidator wires the process-local transport at startup.
func (h *Handler[U]) SetChatRealtimeAccessInvalidator(invalidator ports.ChatRealtimeAccessInvalidator) {
	h.realtimeInvalidator = invalidator
}

func (h *Handler[U]) invalidateRealtimeAfterCommit(ctx context.Context, channelID uint64) {
	ports.InvalidateChatRealtimeAfterCommit(ctx, h.realtimeInvalidator, ports.ChatRealtimeAccessInvalidation{ChannelID: channelID})
}

// announceAccessChangeAfterCommit publishes the channel event of a mutation that may change who
// can read channelID, then invalidates the channel's realtime access. Viewers who still may read
// receive the event before their grants are suspended; viewers the change revokes never do. A
// nil newEvent, or no event publisher, only invalidates.
func (h *Handler[U]) announceAccessChangeAfterCommit(ctx context.Context, channelID uint64, newEvent func(ctx context.Context) *ports.ChatEvent) {
	err := ports.PublishChatAccessChangeAfterCommit(ctx, h.realtimeInvalidator, h.eventPublisher, ports.ChatRealtimeAccessInvalidation{ChannelID: channelID}, newEvent)
	if err != nil && h.logger != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to publish chat event",
			zap.String(logkeys.Stage, "publish_chat_access_change_after_commit"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Error(err))
	}
}

// AuthorizeRealtimeRead authorizes an open-chat subscription for the same readers as
// requireChannelRead (participant, organization reader, signed-in public reader, or site admin),
// using fresh persisted channel and effective organization authority. It grants no writes,
// participation, or hidden-message access. actor must be freshly loaded by the caller.
func (h *Handler[U]) AuthorizeRealtimeRead(ctx context.Context, actor U, channelID uint64) (ports.ChatRealtimeCategory, error) {
	if actor.GetID() == 0 || actor.IsBanned() {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	if h.chatDisabled() {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	access, err := h.persistenceHandler.GetChatChannelAccess(ctx, channelID, uint64(actor.GetID()))
	if err != nil {
		if errors.Is(err, persistence.ErrChatChannelNotFound) {
			return 0, serviceErrors.ErrChatChannelNotFound
		}
		return 0, fmt.Errorf("%w: %w", ports.ErrChatRealtimeUnavailable, err)
	}
	if access == nil || access.Channel == nil {
		return 0, serviceErrors.ErrChatChannelNotFound
	}
	if access.Participant == nil {
		if err := h.requireNonParticipantRead(ctx, actor, access.Channel, channelReadRealtime, false); err != nil {
			return 0, err
		}
	}
	channel := h.converter.PersistenceChatChannelToService(access.Channel)
	check := func(op ports.ChatPermissionOperation) error {
		return h.authorize(ctx, ports.ChatPermissionRequest[U]{Actor: actor, Channel: channel, Operation: op})
	}
	if err := check(ports.ChatPermissionGetChannel); err != nil {
		return 0, err
	}
	grants := ports.ChatRealtimeInference
	// Channel updates may include roster identities as well as metadata.
	if check(ports.ChatPermissionListChannelParticipants) == nil {
		grants |= ports.ChatRealtimeChannel
	}
	// Each broad event capability is conservative: callbacks narrowing individual
	// message/read routes cannot accidentally expose their bodies through a stream.
	if check(ports.ChatPermissionListMessages) == nil && check(ports.ChatPermissionGetMessage) == nil && check(ports.ChatPermissionBatchGetMessages) == nil {
		grants |= ports.ChatRealtimeMessages | ports.ChatRealtimeTyping
	}
	if grants&ports.ChatRealtimeMessages != 0 && check(ports.ChatPermissionListReactions) == nil {
		grants |= ports.ChatRealtimeReactions
	}
	return grants, nil
}

// organizationReadGrantedFresh is organizationReadGranted for realtime subscriptions: it bypasses
// organization role caches so a revoked organization role cannot keep a socket authorized.
func (h *Handler[U]) organizationReadGrantedFresh(ctx context.Context, actor U, channel *common_persistence_models.ChatChannel) (bool, error) {
	resolver, ok := h.organizationRoleResolver.(ports.FreshOrganizationRoleResolver)
	organizationID, scoped := channelOrgReadScope(channel)
	if !ok || !scoped {
		return false, nil
	}
	role, member, err := resolver.EffectiveOrganizationRoleFresh(ctx, actor.GetID(), organizationID)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ports.ErrChatRealtimeUnavailable, err)
	}
	return member && organizationRoleAtLeast(role, channel.OrgMinRoleRead), nil
}
