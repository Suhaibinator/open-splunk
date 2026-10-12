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
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// ===================================================================
// Participants
// ===================================================================

// AddChannelParticipants executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) AddChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_service_models.ChatChannelParticipant, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	channel, err := h.requireChannelAdmin(ctx, channelID, actorID, false)
	if err != nil {
		return nil, err
	}

	// Enforce max participants after add (best-effort; avoids silently creating channels exceeding configured caps).
	if limits.ChatMaxParticipantsPerGroup() > 0 {
		uniqueUserIDs := make([]uint64, 0, len(userIDs))
		seenUserIDs := make(map[uint64]struct{}, len(userIDs))
		for _, id := range userIDs {
			if id == 0 {
				continue
			}
			if _, ok := seenUserIDs[id]; ok {
				continue
			}
			seenUserIDs[id] = struct{}{}
			uniqueUserIDs = append(uniqueUserIDs, id)
		}

		activeCount, countErr := h.persistenceHandler.GetActiveChannelParticipantCount(ctx, channelID)
		if countErr != nil {
			if errors.Is(countErr, persistence.ErrDatabaseError) {
				return nil, serviceErrors.ErrDatabaseError
			}
			return nil, serviceErrors.ErrInternalError
		}

		var existingCount int64
		if len(uniqueUserIDs) > 0 {
			existingCount, countErr = h.persistenceHandler.GetActiveChannelParticipantCountByUserIDs(ctx, channelID, uniqueUserIDs)
			if countErr != nil {
				if errors.Is(countErr, persistence.ErrDatabaseError) {
					return nil, serviceErrors.ErrDatabaseError
				}
				return nil, serviceErrors.ErrInternalError
			}
		}

		netNew := max(int64(len(uniqueUserIDs))-existingCount, 0)
		if activeCount+netNew > int64(limits.ChatMaxParticipantsPerGroup()) {
			return nil, serviceErrors.ErrInvalidRequest
		}
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:          ports.ChatPermissionAddChannelParticipants,
		Actor:              actor,
		Channel:            channel,
		ParticipantUserIDs: userIDs,
	}); err != nil {
		return nil, err
	}

	rows, addErr := h.persistenceHandler.AddChatChannelParticipants(ctx, channelID, userIDs)
	if addErr != nil {
		if errors.Is(addErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	// Re-adding a previously left participant resets their role, and the added
	// users may include bots, so both cache layers must be refreshed.
	h.invalidateChannelAccessCache(ctx, channelID, userIDs...)

	out := common_util.ApplyToEach(rows, func(row *common_persistence_models.ChatChannelParticipant) *common_service_models.ChatChannelParticipant {
		return h.converter.PersistenceChatChannelParticipantToService(row)
	})

	h.announceRosterChangeAfterCommit(ctx, channelID, actorID)
	return out, nil
}

// announceRosterChangeAfterCommit announces a roster change as a CHANNEL_UPDATED event carrying
// the current member set, so clients that hold the roster refresh it, and invalidates the
// channel's realtime access. Shared by the participant management path and the delegation spawn,
// which adds the child bot with persistence authority.
func (h *Handler[U]) announceRosterChangeAfterCommit(ctx context.Context, channelID uint64, actorID uint64) {
	var channelForEvent *common_service_models.ChatChannel
	var memberIDsForEvent []common_service_models.UserIdType
	if h.eventPublisher != nil {
		if ch, getErr := h.persistenceHandler.GetChatChannelByID(ctx, channelID); getErr == nil && ch != nil {
			channelForEvent = h.converter.PersistenceChatChannelToService(ch)
		}
		if channelForEvent != nil {
			if ids, idsErr := h.persistenceHandler.GetActiveChannelParticipantUserIDs(ctx, channelID); idsErr == nil {
				memberIDsForEvent = toMemberUserIDs(ids)
			} else if h.logger != nil {
				appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load channel participants for event fanout", zap.Error(idsErr))
			}
		}
	}
	h.announceAccessChangeAfterCommit(ctx, channelID, func(context.Context) *ports.ChatEvent {
		if channelForEvent == nil {
			return nil
		}
		return &ports.ChatEvent{
			Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
			OccurredAt:    time.Now(),
			ActorUserID:   common_service_models.UserIdType(actorID),
			Channel:       channelForEvent,
			MemberUserIDs: memberIDsForEvent,
		}
	})
}

// channelEventAfterCommit builds a CHANNEL_UPDATED event from the channel row read in the
// transaction, or nil when there is no publisher or the row cannot be loaded.
func (h *Handler[U]) channelEventAfterCommit(ctx context.Context, channelID uint64, actorID uint64) func(context.Context) *ports.ChatEvent {
	if h.eventPublisher == nil {
		return nil
	}
	ch, getErr := h.persistenceHandler.GetChatChannelByID(ctx, channelID)
	if getErr != nil || ch == nil {
		return nil
	}
	channelForEvent := h.converter.PersistenceChatChannelToService(ch)
	if channelForEvent == nil {
		return nil
	}
	return func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
			OccurredAt:  time.Now(),
			ActorUserID: common_service_models.UserIdType(actorID),
			Channel:     channelForEvent,
		}
	}
}

// RemoveChannelParticipant executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) RemoveChannelParticipant(ctx context.Context, channelID uint64, userID common_service_models.UserIdType) error {
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

	var channel *common_service_models.ChatChannel
	if userID == actorID {
		channel, err = h.requireChannelMemberAllowArchived(ctx, channelID, actorID)
	} else {
		channel, err = h.requireChannelAdmin(ctx, channelID, actorID, false)
	}
	if err != nil {
		return err
	}

	target := common_service_models.UserIdType(userID)
	if userID != actorID {
		participant, targetErr := h.persistenceHandler.GetChatChannelParticipant(persistence.WithChatParticipantRoleLock(ctx), channelID, uint64(userID))
		if targetErr != nil {
			if errors.Is(targetErr, persistence.ErrChatParticipantNotFound) {
				return serviceErrors.ErrChatParticipantNotFound
			}
			return serviceErrors.ErrDatabaseError
		}
		if participant.Role == proto_models.ChannelRole_CHANNEL_ROLE_OWNER {
			// Reload the actor's role rather than trusting a positive role-cache entry.
			access, accessErr := h.persistenceHandler.GetChatChannelAccess(ctx, channelID, uint64(actorID))
			if accessErr != nil {
				return serviceErrors.ErrDatabaseError
			}
			if access.Participant == nil || access.Participant.Role != proto_models.ChannelRole_CHANNEL_ROLE_OWNER {
				return serviceErrors.ErrInsufficientPermissions
			}
		}
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:    ports.ChatPermissionRemoveChannelParticipant,
		Actor:        actor,
		Channel:      channel,
		TargetUserID: &target,
	}); err != nil {
		return err
	}

	if removeErr := h.persistenceHandler.RemoveChatChannelParticipant(ctx, channelID, userID); removeErr != nil {
		switch {
		case errors.Is(removeErr, persistence.ErrChatParticipantNotFound):
			return serviceErrors.ErrChatParticipantNotFound
		case errors.Is(removeErr, persistence.ErrDatabaseError):
			return serviceErrors.ErrDatabaseError
		default:
			return serviceErrors.ErrInternalError
		}
	}

	if err := h.retireChannelActivity(ctx, channelID, uint64(userID)); err != nil {
		return err
	}

	h.invalidateChannelAccessCache(ctx, channelID, userID)
	// The removed user keeps the event only if they still read as an organization reader or site admin.
	h.announceAccessChangeAfterCommit(ctx, channelID, h.channelEventAfterCommit(ctx, channelID, actorID))
	return nil
}

// UpdateChannelParticipantRole executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) UpdateChannelParticipantRole(
	ctx context.Context,
	channelID uint64,
	userID common_service_models.UserIdType,
	role proto_models.ChannelRole,
) (*common_service_models.ChatChannelParticipant, error) {
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
	channel, err := h.requireChannelOwner(ctx, channelID, actorID, false)
	if err != nil {
		return nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:    ports.ChatPermissionUpdateChannelParticipantRole,
		Actor:        actor,
		Channel:      channel,
		TargetUserID: &userID,
		NewRole:      &role,
	}); err != nil {
		return nil, err
	}

	updated, updateErr := h.persistenceHandler.UpdateChatChannelParticipantRole(ctx, channelID, userID, role)
	if updateErr != nil {
		switch {
		case errors.Is(updateErr, persistence.ErrChatParticipantNotFound):
			return nil, serviceErrors.ErrChatParticipantNotFound
		case errors.Is(updateErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}

	h.invalidateChannelAccessCache(ctx, channelID, userID)
	h.announceAccessChangeAfterCommit(ctx, channelID, h.channelEventAfterCommit(ctx, channelID, actorID))

	return h.converter.PersistenceChatChannelParticipantToService(updated), nil
}

// UpdateChannelParticipantPreferences executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) UpdateChannelParticipantPreferences(
	ctx context.Context,
	channelID uint64,
	notificationMode *proto_models.ChannelNotificationMode,
	isPinned *bool,
) (*common_service_models.ChatChannelParticipant, error) {
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
	channel, err := h.requireChannelMemberAllowArchived(ctx, channelID, actorID)
	if err != nil {
		return nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                ports.ChatPermissionUpdateChannelParticipantPrefs,
		Actor:                    actor,
		Channel:                  channel,
		ProposedNotificationMode: notificationMode,
		ProposedIsPinned:         isPinned,
	}); err != nil {
		return nil, err
	}

	updated, changed, updateErr := h.persistenceHandler.UpdateChatChannelParticipantPreferences(ctx, channelID, actorID, notificationMode, isPinned)
	if updateErr != nil {
		switch {
		case errors.Is(updateErr, persistence.ErrChatParticipantNotFound):
			return nil, serviceErrors.ErrChatParticipantNotFound
		case errors.Is(updateErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}

	participant := h.converter.PersistenceChatChannelParticipantToService(updated)
	if changed && h.eventPublisher != nil {
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:               proto_models.ChatEventType_CHAT_EVENT_TYPE_PARTICIPANT_PREFERENCES_UPDATED,
				OccurredAt:         time.Now(),
				ActorUserID:        common_service_models.UserIdType(actorID),
				Participant:        participant,
				TargetedRecipients: true,
				MemberUserIDs:      []common_service_models.UserIdType{actorID},
			}
		})
	}
	return participant, nil
}

// ListChannelParticipants executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListChannelParticipants(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.ChatChannelParticipant, *proto_models.PaginationResponse, *common_service_models.ChatChannelParticipant, error) {
	if h.chatDisabled() {
		return nil, nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	channel, err := h.requireChannelRead(ctx, channelID, actor)
	if err != nil {
		return nil, nil, nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionListChannelParticipants,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return nil, nil, nil, err
	}

	rows, page, listErr := h.persistenceHandler.ListChatChannelParticipants(ctx, channelID, pagination)
	if listErr != nil {
		if errors.Is(listErr, persistence.ErrDatabaseError) {
			return nil, nil, nil, serviceErrors.ErrDatabaseError
		}
		return nil, nil, nil, serviceErrors.ErrInternalError
	}

	out := common_util.ApplyToEach(rows, func(row *common_persistence_models.ChatChannelParticipant) *common_service_models.ChatChannelParticipant {
		return h.converter.PersistenceChatChannelParticipantToService(row)
	})
	// Reuse the freshly hydrated page row when it includes the actor. Otherwise
	// load only their membership; never walk additional roster pages.
	for _, participant := range out {
		if participant != nil && participant.UserID == actor.GetID() {
			return out, page, participant, nil
		}
	}
	current, currentErr := h.persistenceHandler.GetChatChannelParticipant(ctx, channelID, actor.GetID())
	if errors.Is(currentErr, persistence.ErrChatParticipantNotFound) {
		return out, page, nil, nil
	}
	if currentErr != nil {
		if errors.Is(currentErr, persistence.ErrDatabaseError) {
			return nil, nil, nil, serviceErrors.ErrDatabaseError
		}
		return nil, nil, nil, serviceErrors.ErrInternalError
	}
	return out, page, h.converter.PersistenceChatChannelParticipantToService(current), nil
}

// ListChannelParticipantsBatch executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListChannelParticipantsBatch(
	ctx context.Context,
	channelIDs []uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.ChatChannelParticipantsBatchItem, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}

	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}

	normalized := common_util.DeduplicateNonZero(channelIDs)

	if len(normalized) == 0 {
		return []*common_service_models.ChatChannelParticipantsBatchItem{}, nil
	}

	for _, channelID := range normalized {
		channel, channelErr := h.requireChannelRead(ctx, channelID, actor)
		if channelErr != nil {
			return nil, channelErr
		}

		if authErr := h.authorize(ctx, ports.ChatPermissionRequest[U]{
			Operation: ports.ChatPermissionListChannelParticipants,
			Actor:     actor,
			Channel:   channel,
		}); authErr != nil {
			return nil, authErr
		}
	}

	rows, listErr := h.persistenceHandler.ListChatChannelParticipantsBatch(ctx, normalized, pagination)
	if listErr != nil {
		if errors.Is(listErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	byChannelID := make(map[uint64]*common_persistence_models.ChatChannelParticipantsBatchItem, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		byChannelID[row.ChannelID] = row
	}

	out := make([]*common_service_models.ChatChannelParticipantsBatchItem, 0, len(normalized))
	for _, channelID := range normalized {
		row, found := byChannelID[channelID]
		if !found {
			out = append(out, &common_service_models.ChatChannelParticipantsBatchItem{
				ChannelID:    common_service_models.ChatChannelIdType(channelID),
				Participants: []*common_service_models.ChatChannelParticipant{},
				Pagination:   nil,
			})
			continue
		}
		participants := common_util.ApplyToEach(row.Participants, func(participant *common_persistence_models.ChatChannelParticipant) *common_service_models.ChatChannelParticipant {
			if participant == nil {
				return nil
			}
			return h.converter.PersistenceChatChannelParticipantToService(participant)
		})
		filteredParticipants := make([]*common_service_models.ChatChannelParticipant, 0, len(participants))
		for _, participant := range participants {
			if participant == nil {
				continue
			}
			filteredParticipants = append(filteredParticipants, participant)
		}
		out = append(out, &common_service_models.ChatChannelParticipantsBatchItem{
			ChannelID:    common_service_models.ChatChannelIdType(channelID),
			Participants: filteredParticipants,
			Pagination:   row.Pagination,
		})
	}

	return out, nil
}
