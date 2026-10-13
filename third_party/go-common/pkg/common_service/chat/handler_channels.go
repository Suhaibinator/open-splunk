package chat

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// canonicalizeMemberTuple executes part of the chat package flow and provides shared helper behavior.
func canonicalizeMemberTuple(actorID uint64, memberUserIDs []uint64) []uint64 {
	unique := make(map[uint64]struct{}, len(memberUserIDs)+1)
	if actorID != 0 {
		unique[actorID] = struct{}{}
	}
	for _, id := range memberUserIDs {
		if id == 0 {
			continue
		}
		unique[id] = struct{}{}
	}
	out := make([]uint64, 0, len(unique))
	for id := range unique {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// memberTupleKey executes part of the chat package flow and provides shared helper behavior.
func memberTupleKey(ids []uint64) string {
	if len(ids) == 0 {
		return ""
	}
	var b strings.Builder
	// Rough capacity: up to 20 digits + comma per id.
	b.Grow(len(ids) * 21)
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}

// toMemberUserIDs executes part of the chat package flow and provides shared helper behavior.
func toMemberUserIDs(ids []uint64) []common_service_models.UserIdType {
	if len(ids) == 0 {
		return nil
	}
	return common_util.ApplyToEach(common_util.DeduplicateNonZero(ids), func(id uint64) common_service_models.UserIdType {
		return common_service_models.UserIdType(id)
	})
}

// GetOrCreateChannelByMembersUnique executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) GetOrCreateChannelByMembersUnique(
	ctx context.Context,
	memberUserIDs []uint64,
	contextID *uint64,
	contextDisplayName *string,
	contextEntityID *uint64,
) (*GetOrCreateUniqueChannelResponse, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}

	ids := canonicalizeMemberTuple(actorID, memberUserIDs)
	if len(ids) < 2 {
		// Must include at least actor + one other member.
		return nil, serviceErrors.ErrInvalidRequest
	}
	if limits.ChatMaxParticipantsPerGroup() > 0 && len(ids) > limits.ChatMaxParticipantsPerGroup() {
		return nil, serviceErrors.ErrInvalidRequest
	}

	channelType := proto_models.ChannelType_CHANNEL_TYPE_GROUP
	if len(ids) == 2 {
		channelType = proto_models.ChannelType_CHANNEL_TYPE_DIRECT
	}

	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                  ports.ChatPermissionGetOrCreateChannelByMembers,
		Actor:                      actor,
		ChannelType:                channelType,
		ParticipantUserIDs:         ids,
		ProposedContextID:          contextID,
		ProposedContextDisplayName: contextDisplayName,
		ProposedContextEntityID:    contextEntityID,
	}); err != nil {
		return nil, err
	}

	// A transaction-scoped advisory lock serializes get-or-create per member tuple: it is held
	// until the request transaction commits, so a concurrent request for the same tuple blocks
	// and then finds the winner's committed channel instead of creating a duplicate. Unlike
	// in-process deduplication, this also holds across replicas.
	if lockErr := h.persistenceHandler.AcquireChatChannelMemberTupleLock(ctx, memberTupleKey(ids)); lockErr != nil {
		if errors.Is(lockErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	existing, findErr := h.persistenceHandler.FindChatChannelByExactActiveMembers(ctx, ids)
	if findErr == nil && existing != nil {
		return &GetOrCreateUniqueChannelResponse{
			Channel: h.converter.PersistenceChatChannelToService(existing),
			Created: false,
		}, nil
	}
	if findErr != nil && !errors.Is(findErr, persistence.ErrChatChannelNotFound) {
		if errors.Is(findErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	// Pass non-actor IDs; persistence always adds the creator as OWNER.
	participantUserIDs := common_util.ApplyToEachFiltered(
		ids,
		func(id uint64) uint64 {
			return id
		},
		func(id uint64) bool {
			return id != actorID
		},
		nil,
	)

	features, err := h.newChannelFeatures(ctx, ports.ChatChannelCreation[U]{
		Operation:          ports.ChatPermissionGetOrCreateChannelByMembers,
		Actor:              actor,
		ChannelType:        channelType,
		ParticipantUserIDs: participantUserIDs,
		ContextID:          contextID,
		ContextDisplayName: contextDisplayName,
		ContextEntityID:    contextEntityID,
	})
	if err != nil {
		return nil, err
	}
	created, createErr := h.persistenceHandler.CreateChatChannel(
		ctx,
		actorID,
		"",
		nil,
		contextID,
		contextDisplayName,
		contextEntityID,
		channelType,
		participantUserIDs,
		common_persistence_models.ChatChannelFeatureFlags{Features: features},
	)
	if createErr != nil {
		if errors.Is(createErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	createdChannel := h.converter.PersistenceChatChannelToService(created)
	if h.eventPublisher != nil {
		memberIDsForEvent := common_util.ApplyToEach(ids, func(id uint64) common_service_models.UserIdType {
			return common_service_models.UserIdType(id)
		})
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_CREATED,
				OccurredAt:    time.Now(),
				ActorUserID:   common_service_models.UserIdType(actorID),
				Channel:       createdChannel,
				MemberUserIDs: memberIDsForEvent,
				Created:       true,
			}
		})
	}

	return &GetOrCreateUniqueChannelResponse{
		Channel: createdChannel,
		Created: true,
	}, nil
}

// ===================================================================
// Channels
// ===================================================================

// CreateChannel executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) CreateChannel(
	ctx context.Context,
	name string,
	description string,
	contextID *uint64,
	contextDisplayName *string,
	contextEntityID *uint64,
	channelType proto_models.ChannelType,
	participantUserIDs []uint64,
	unreadNotificationsEnabled *bool,
	forkRules common_persistence_models.ChatChannelForkRules,
) (*common_service_models.ChatChannel, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if !forkRules.Valid() {
		return nil, serviceErrors.ErrInvalidRequest
	}

	switch channelType {
	case proto_models.ChannelType_CHANNEL_TYPE_DIRECT:
		if len(participantUserIDs) != 1 {
			return nil, serviceErrors.ErrInvalidRequest
		}
	case proto_models.ChannelType_CHANNEL_TYPE_GROUP:
		// ok
	default:
		return nil, serviceErrors.ErrInvalidRequest
	}

	// Enforce max participants (actor is implicitly included).
	if limits.ChatMaxParticipantsPerGroup() > 0 && len(participantUserIDs)+1 > limits.ChatMaxParticipantsPerGroup() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	trimmedName, descPtr, err := newChannelText(limits, name, description, contextDisplayName)
	if err != nil {
		return nil, err
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}

	ids := common_util.ApplyToEachFiltered(
		participantUserIDs,
		func(id uint64) uint64 {
			return id
		},
		func(id uint64) bool {
			return id != 0 && id != actorID
		},
		nil,
	)
	if channelType == proto_models.ChannelType_CHANNEL_TYPE_DIRECT && len(ids) != 1 {
		return nil, serviceErrors.ErrInvalidRequest
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                          ports.ChatPermissionCreateChannel,
		Actor:                              actor,
		ChannelType:                        channelType,
		ParticipantUserIDs:                 ids,
		ProposedUnreadNotificationsEnabled: unreadNotificationsEnabled,
		ProposedChannelName:                &trimmedName,
		ProposedChannelDescription:         descPtr,
		ProposedContextID:                  contextID,
		ProposedContextDisplayName:         contextDisplayName,
		ProposedContextEntityID:            contextEntityID,
	}); err != nil {
		return nil, err
	}

	features, err := h.newChannelFeatures(ctx, ports.ChatChannelCreation[U]{
		Operation:          ports.ChatPermissionCreateChannel,
		Actor:              actor,
		ChannelType:        channelType,
		ParticipantUserIDs: ids,
		ContextID:          contextID,
		ContextDisplayName: contextDisplayName,
		ContextEntityID:    contextEntityID,
	})
	if err != nil {
		return nil, err
	}
	flags := common_persistence_models.ChatChannelFeatureFlags{
		UnreadNotificationsEnabled: unreadNotificationsEnabled,
		Features:                   features,
		ForkRules:                  forkRules,
	}
	created, createErr := h.persistenceHandler.CreateChatChannel(
		ctx,
		actorID,
		trimmedName,
		descPtr,
		contextID,
		contextDisplayName,
		contextEntityID,
		channelType,
		ids,
		flags,
	)
	if createErr != nil {
		if errors.Is(createErr, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	out := h.converter.PersistenceChatChannelToService(created)
	h.publishChannelCreated(ctx, actorID, out, append([]uint64{actorID}, ids...))
	return out, nil
}

// newChannelText trims a new channel's name and description and checks them, and the context
// display name, against the configured length limits. An empty description is nil.
func newChannelText(limits PolicyView, name, description string, contextDisplayName *string) (string, *string, error) {
	trimmedName := strings.TrimSpace(name)
	if limits.ChatMaxChannelNameLength() > 0 && len(trimmedName) > limits.ChatMaxChannelNameLength() {
		return "", nil, serviceErrors.ErrInvalidRequest
	}
	var descPtr *string
	if d := strings.TrimSpace(description); d != "" {
		if limits.ChatMaxChannelDescriptionLength() > 0 && len(d) > limits.ChatMaxChannelDescriptionLength() {
			return "", nil, serviceErrors.ErrInvalidRequest
		}
		descPtr = &d
	}
	if contextDisplayName != nil && limits.ChatMaxChannelNameLength() > 0 && len(strings.TrimSpace(*contextDisplayName)) > limits.ChatMaxChannelNameLength() {
		return "", nil, serviceErrors.ErrInvalidRequest
	}
	return trimmedName, descPtr, nil
}

// publishChannelCreated publishes CHANNEL_CREATED for a channel created in the current
// transaction once it commits. memberIDs include the actor.
func (h *Handler[U]) publishChannelCreated(ctx context.Context, actorID uint64, channel *common_service_models.ChatChannel, memberIDs []uint64) {
	if h.eventPublisher == nil || channel == nil {
		return
	}
	h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_CREATED,
			OccurredAt:    time.Now(),
			ActorUserID:   common_service_models.UserIdType(actorID),
			Channel:       channel,
			MemberUserIDs: toMemberUserIDs(memberIDs),
			Created:       true,
		}
	})
}

// GetChannel executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) GetChannel(ctx context.Context, channelID uint64) (*common_service_models.ChatChannel, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	return h.readChannelAuthorized(ctx, actor, channelID)
}

// UpdateChannel executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) UpdateChannel(
	ctx context.Context,
	channelID uint64,
	name *string,
	description *string,
	avatarAttachmentID *uint64,
	isArchived *bool,
	unreadNotificationsEnabled *bool,
) (*common_service_models.ChatChannel, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if name != nil && limits.ChatMaxChannelNameLength() > 0 && len(strings.TrimSpace(*name)) > limits.ChatMaxChannelNameLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if description != nil && limits.ChatMaxChannelDescriptionLength() > 0 && len(strings.TrimSpace(*description)) > limits.ChatMaxChannelDescriptionLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	channel, err := h.requireChannelAdmin(ctx, channelID, actorID, true)
	if err != nil {
		return nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                          ports.ChatPermissionUpdateChannel,
		Actor:                              actor,
		Channel:                            channel,
		ProposedChannelName:                name,
		ProposedChannelDescription:         description,
		ProposedAvatarAttachmentID:         avatarAttachmentID,
		ProposedIsArchived:                 isArchived,
		ProposedUnreadNotificationsEnabled: unreadNotificationsEnabled,
	}); err != nil {
		return nil, err
	}

	updated, updateErr := h.persistenceHandler.UpdateChatChannel(ctx, channelID, actorID, name, description, avatarAttachmentID, isArchived, unreadNotificationsEnabled)
	if updateErr != nil {
		switch {
		case errors.Is(updateErr, persistence.ErrChatChannelNotFound):
			return nil, serviceErrors.ErrChatChannelNotFound
		case errors.Is(updateErr, persistence.ErrChatAvatarAttachmentInvalid):
			return nil, serviceErrors.ErrChatAvatarAttachmentInvalid
		case errors.Is(updateErr, persistence.ErrDatabaseError):
			return nil, serviceErrors.ErrDatabaseError
		default:
			return nil, serviceErrors.ErrInternalError
		}
	}

	h.invalidateChannelAccessCache(ctx, channelID)

	// Settings never narrow the built-in read gate, but Authorize callbacks may deny on the new
	// state (for example on archive), so viewer grants are suspended and renewed against it.
	out := h.converter.PersistenceChatChannelToService(updated)
	h.announceAccessChangeAfterCommit(ctx, channelID, func(context.Context) *ports.ChatEvent {
		if out == nil {
			return nil
		}
		return &ports.ChatEvent{
			Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
			OccurredAt:  time.Now(),
			ActorUserID: common_service_models.UserIdType(actorID),
			Channel:     out,
		}
	})

	return out, nil
}

// ListChannels executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) ListChannels(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
	includeArchived bool,
) ([]*common_service_models.ChatChannel, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}

	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionListChannels,
		Actor:     actor,
	}); err != nil {
		return nil, nil, err
	}

	rows, page, listErr := h.persistenceHandler.ListChatChannelsForUser(ctx, actorID, pagination, includeArchived)
	if listErr != nil {
		if errors.Is(listErr, persistence.ErrDatabaseError) {
			return nil, nil, serviceErrors.ErrDatabaseError
		}
		return nil, nil, serviceErrors.ErrInternalError
	}

	out := common_util.ApplyToEach(rows, func(row *common_persistence_models.ChatChannel) *common_service_models.ChatChannel {
		return h.converter.PersistenceChatChannelToService(row)
	})
	return out, page, nil
}

// LeaveChannel executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) LeaveChannel(ctx context.Context, channelID uint64) error {
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
	channel, err := h.requireChannelMemberAllowArchived(ctx, channelID, actorID)
	if err != nil {
		return err
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionLeaveChannel,
		Actor:     actor,
		Channel:   channel,
	}); err != nil {
		return err
	}

	if leaveErr := h.persistenceHandler.LeaveChatChannel(ctx, channelID, actorID); leaveErr != nil {
		switch {
		case errors.Is(leaveErr, persistence.ErrChatParticipantNotFound):
			return serviceErrors.ErrChatParticipantNotFound
		case errors.Is(leaveErr, persistence.ErrDatabaseError):
			return serviceErrors.ErrDatabaseError
		default:
			return serviceErrors.ErrInternalError
		}
	}

	if err := h.retireChannelActivity(ctx, channelID, actorID); err != nil {
		return err
	}

	h.invalidateChannelAccessCache(ctx, channelID, actorID)

	// The leaver keeps the event only if they still read as an organization reader or site admin.
	h.announceAccessChangeAfterCommit(ctx, channelID, func(context.Context) *ports.ChatEvent {
		if channel == nil {
			return nil
		}
		return &ports.ChatEvent{
			Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
			OccurredAt:  time.Now(),
			ActorUserID: common_service_models.UserIdType(actorID),
			Channel:     channel,
		}
	})
	return nil
}
