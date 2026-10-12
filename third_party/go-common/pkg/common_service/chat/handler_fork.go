package chat

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// errForkGroupLimit rejects a fork whose members and cloned agents' bots exceed the group limit.
var errForkGroupLimit = fmt.Errorf("%w: fork members and cloned agents exceed the group participant limit", serviceErrors.ErrInvalidRequest)

// ForkChannelInput names a fork of SourceChannelID at ForkPointMessageID by the context actor.
type ForkChannelInput struct {
	SourceChannelID uint64
	// ForkPointMessageID is the requested fork point, on the main timeline or in a thread. The copy
	// runs up to it, but in-flight agent work at the fork point is cut, so it may not be copied.
	ForkPointMessageID uint64
	Name               string
	// MemberUserIDs are the fork's members besides the actor, who is always its OWNER. The
	// source's member pool limits them; bot users of cloned agent sessions join automatically.
	MemberUserIDs []uint64
	// KeepOrgAccess and KeepContext are honored only when the source's matching carry-over rule
	// is FORKER_CHOOSES.
	KeepOrgAccess bool
	KeepContext   bool
	// ClientRequestID is the actor's optional idempotency key; empty disables idempotency. A replay
	// is matched on SourceChannelID and ForkPointMessageID only; its other fields are still
	// validated against current limits but don't change the result, and a key that named a
	// different source or fork point fails with ErrInvalidRequest.
	ClientRequestID string
}

// ForkChannel copies a channel's history up to a message into a new channel the actor owns,
// applying the source's fork rules and the chat_fork_max_copied_messages cap. It requires the
// request transaction. A concurrent fork with the same ClientRequestID that commits first fails
// this one with ErrChatForkRequestKeyConflict; the client's retry then replays it.
//
// The source's who_may_fork rule picks the gate: ANY_READER the read gate (participants,
// organization readers, site chat admins), PARTICIPANTS the participant gate, ADMINS the channel
// admin gate; archived sources pass all three. The read gate runs first, so a user who cannot
// read the source learns nothing about its rules (ErrChatNotMember), and one admitted only by the
// public read level gets ErrInsufficientPermissions whatever the rules. The fork is a private
// GROUP channel with the source's feature flags and fork rules; its org access and context fields
// follow the carry-over rules.
// Kept org access requires the actor to be an effective member of the organization, as
// SetChannelOrganizationAccess does (ErrInsufficientPermissions; ErrInvalidRequest without
// organizations). The actor owns the cloned agent sessions, which keep their organizations, so
// they must also be an effective member of each cloned session's organization
// (ErrChatForkAgentOrganizationNotMember after the copy, which the request's rollback undoes).
func (h *Handler[U]) ForkChannel(ctx context.Context, input ForkChannelInput) (*common_service_models.ChatChannel, error) {
	limits := h.policy()
	if !limits.EnableChat() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if actor.IsBanned() {
		return nil, serviceErrors.ErrUserBanned
	}
	name := strings.TrimSpace(input.Name)
	// The hot key limit applies to new forks only (below); a replay needs a key the store can hold.
	if input.SourceChannelID == 0 || input.ForkPointMessageID == 0 ||
		(limits.ChatMaxChannelNameLength() > 0 && len(name) > limits.ChatMaxChannelNameLength()) ||
		strings.ContainsRune(input.ClientRequestID, '\x00') || len(input.ClientRequestID) > config.MaxChatMaxClientMessageIDLength {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	if input.ClientRequestID != "" {
		if replayed, err := h.replayFork(ctx, actor, actorID, input); replayed != nil || err != nil {
			return replayed, err
		}
	}
	if limits.ChatMaxClientMessageIDLength() > 0 && len(input.ClientRequestID) > limits.ChatMaxClientMessageIDLength() {
		return nil, serviceErrors.ErrInvalidRequest
	}

	// A fork copies hidden rows, so a reader admitted only by the public read level may not fork.
	source, role, isMember, err := h.requireChannelReadAccess(ctx, input.SourceChannelID, actor, true)
	if err != nil {
		return nil, err
	}
	rules := source.ForkRules
	switch rules.WhoMayFork {
	case proto_models.ChatForkWhoMayFork_CHAT_FORK_WHO_MAY_FORK_ANY_READER:
	case proto_models.ChatForkWhoMayFork_CHAT_FORK_WHO_MAY_FORK_PARTICIPANTS, proto_models.ChatForkWhoMayFork_CHAT_FORK_WHO_MAY_FORK_ADMINS:
		// requireChannelRole's participant and admin checks, with archived sources allowed.
		if !isMember {
			return nil, serviceErrors.ErrChatNotMember
		}
		if rules.WhoMayFork == proto_models.ChatForkWhoMayFork_CHAT_FORK_WHO_MAY_FORK_ADMINS && role < proto_models.ChannelRole_CHANNEL_ROLE_ADMIN {
			return nil, serviceErrors.ErrInsufficientPermissions
		}
	default:
		return nil, serviceErrors.ErrChatForkDisabled
	}

	point, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, input.ForkPointMessageID,
		persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityVisibleToActor})
	if errors.Is(err, persistence.ErrChatMessageNotFound) || (err == nil && point.ChannelID != uint64(source.ID)) {
		return nil, serviceErrors.ErrChatForkPointInvalid
	}
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_point_lookup"),
			zap.Uint64(appLogger.KeyChannelID, uint64(source.ID)), zap.Uint64(appLogger.KeyMessageID, input.ForkPointMessageID))
	}

	members, err := h.forkMembers(ctx, limits, source, actorID, input.MemberUserIDs)
	if err != nil {
		return nil, err
	}

	keep := func(rule proto_models.ChatForkCarryOver, requested bool) bool {
		return rule == proto_models.ChatForkCarryOver_CHAT_FORK_CARRY_OVER_ALWAYS_KEEP ||
			(rule == proto_models.ChatForkCarryOver_CHAT_FORK_CARRY_OVER_FORKER_CHOOSES && requested)
	}
	var contextID, contextEntityID *uint64
	var contextDisplayName *string
	keepContext := keep(rules.Context, input.KeepContext)
	if keepContext {
		contextID, contextDisplayName, contextEntityID = source.ContextID, source.ContextDisplayName, source.ContextEntityID
	}
	var orgAccess *common_service_models.ChatChannelOrganizationAccess
	var organizationID common_service_models.OrganizationIdType
	if keep(rules.OrgAccess, input.KeepOrgAccess) && source.OrganizationAccess != nil {
		orgAccess, organizationID = source.OrganizationAccess, source.OrganizationAccess.OrganizationID
		// The forker owns the fork, so keeping the access is SetChannelOrganizationAccess's grant.
		if err := h.requireOrganizationAccessMember(ctx, actorID, organizationID); err != nil {
			return nil, err
		}
	}

	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:                          ports.ChatPermissionForkChannel,
		Actor:                              actor,
		Channel:                            source,
		Message:                            h.converter.PersistenceChatMessageToService(point),
		ChannelType:                        proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		ParticipantUserIDs:                 members,
		ProposedOrganizationID:             organizationID,
		ProposedChannelName:                &name,
		ProposedContextID:                  contextID,
		ProposedContextDisplayName:         contextDisplayName,
		ProposedContextEntityID:            contextEntityID,
		ProposedUnreadNotificationsEnabled: &source.UnreadNotificationsEnabled,
	}); err != nil {
		return nil, err
	}

	// Feature flags and fork rules always carry over; the description and avatar do not.
	flags := common_persistence_models.ChatChannelFeatureFlags{
		UnreadNotificationsEnabled: &source.UnreadNotificationsEnabled,
		Features:                   source.Features,
		ForkRules:                  rules,
	}
	created, err := h.persistenceHandler.CreateChatChannel(ctx, actorID, name, nil, contextID, contextDisplayName, contextEntityID,
		proto_models.ChannelType_CHANNEL_TYPE_GROUP, members, flags)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_create_channel"), zap.Uint64(appLogger.KeyChannelID, uint64(source.ID)))
	}
	if orgAccess != nil {
		// Written before the copy, so the cloned agent sessions (which keep their own
		// organization) meet the same channel organization they had in the source.
		organization := int64(organizationID)
		if err := h.persistenceHandler.SetChatChannelOrganizationAccess(ctx, created.ID, &organization, orgAccess.MinRoleRead); err != nil {
			return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_set_org_access"), zap.Uint64(appLogger.KeyChannelID, created.ID))
		}
		h.invalidateChannelAccessCache(ctx, created.ID)
	}

	var requestKey *string
	if input.ClientRequestID != "" {
		requestKey = &input.ClientRequestID
	}
	copied, err := h.persistenceHandler.CopyChatChannelFork(ctx, persistence.CopyChatChannelForkInput{
		SourceChannelID:      uint64(source.ID),
		DestinationChannelID: created.ID,
		ForkPointMessageID:   input.ForkPointMessageID,
		ForkerUserID:         actorID,
		KeepContext:          keepContext,
		RequestKey:           requestKey,
		MaxCopiedMessages:    limits.ChatForkMaxCopiedMessages(),
	})
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "copy_chat_channel_fork"),
			zap.Uint64(appLogger.KeyChannelID, uint64(source.ID)), zap.Uint64("fork_channel_id", created.ID))
	}
	// The forker owns the cloned sessions, which keep their organizations, so agents of an
	// organization the forker is not an effective member of could never run for them.
	for _, organizationID := range copied.OrganizationIDs {
		err := h.requireOrganizationAccessMember(ctx, actorID, common_service_models.OrganizationIdType(organizationID))
		if errors.Is(err, serviceErrors.ErrInsufficientPermissions) {
			return nil, serviceErrors.ErrChatForkAgentOrganizationNotMember
		}
		if err != nil {
			return nil, err
		}
	}
	// Cloned sessions' bots join beside the requested members; the group limit counts them too.
	memberIDs := common_util.DeduplicateNonZero(slices.Concat([]uint64{actorID}, members, copied.BotUserIDs))
	if limits.ChatMaxParticipantsPerGroup() > 0 && len(memberIDs) > limits.ChatMaxParticipantsPerGroup() {
		return nil, errForkGroupLimit
	}

	// Re-read the fork for the provenance and last message the copy wrote.
	fork, err := h.persistenceHandler.GetChatChannelByID(ctx, created.ID)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_reload"), zap.Uint64(appLogger.KeyChannelID, created.ID))
	}
	out := h.converter.PersistenceChatChannelToService(fork)
	h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_CREATED,
			OccurredAt:    time.Now(),
			ActorUserID:   common_service_models.UserIdType(actorID),
			Channel:       out,
			MemberUserIDs: toMemberUserIDs(memberIDs),
			Created:       true,
		}
	})
	return out, nil
}

// replayFork returns the fork the actor already made with input.ClientRequestID, or nil when
// there is none. The key must name the same source and fork point; the name, members and Keep
// flags are not compared, so a replay returns the original fork as it was made. A deleted fork is
// not found.
func (h *Handler[U]) replayFork(ctx context.Context, actor U, actorID uint64, input ForkChannelInput) (*common_service_models.ChatChannel, error) {
	existing, err := h.persistenceHandler.GetChatChannelByForkRequestKey(ctx, actorID, input.ClientRequestID)
	if errors.Is(err, persistence.ErrChatChannelNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_replay_lookup"))
	}
	if existing.ForkedFromChannelID == nil || *existing.ForkedFromChannelID != input.SourceChannelID ||
		existing.ForkPointMessageID == nil || *existing.ForkPointMessageID != input.ForkPointMessageID {
		return nil, fmt.Errorf("%w: client_request_id already named a different fork", serviceErrors.ErrInvalidRequest)
	}
	if existing.DeletedAt.Valid {
		return nil, serviceErrors.ErrChatChannelNotFound
	}
	return h.readChannelAuthorized(ctx, actor, existing.ID)
}

// forkMembers validates requested fork members against the source's member pool and returns them
// without the actor, zero IDs or duplicates. UNSPECIFIED admits only the actor, SOURCE_PARTICIPANTS
// the source's active participants, and ANYONE anyone; every pool keeps the group size limit
// (ErrInvalidRequest).
func (h *Handler[U]) forkMembers(ctx context.Context, limits PolicyView, source *common_service_models.ChatChannel, actorID uint64, requested []uint64) ([]uint64, error) {
	members := slices.DeleteFunc(common_util.DeduplicateNonZero(requested), func(id uint64) bool { return id == actorID })
	if len(members) == 0 {
		return nil, nil
	}
	if limits.ChatMaxParticipantsPerGroup() > 0 && len(members)+1 > limits.ChatMaxParticipantsPerGroup() {
		return nil, errForkGroupLimit
	}
	switch source.ForkRules.MemberPool {
	case proto_models.ChatForkMemberPool_CHAT_FORK_MEMBER_POOL_ANYONE:
		return members, nil
	case proto_models.ChatForkMemberPool_CHAT_FORK_MEMBER_POOL_SOURCE_PARTICIPANTS:
		active, err := h.persistenceHandler.GetActiveChannelParticipantCountByUserIDs(ctx, uint64(source.ID), members)
		if err != nil {
			return nil, mapChatMessagePersistenceError(err, zap.String(logkeys.Stage, "fork_member_pool"), zap.Uint64(appLogger.KeyChannelID, uint64(source.ID)))
		}
		if active != int64(len(members)) {
			return nil, serviceErrors.ErrChatForkMemberInvalid
		}
		return members, nil
	default:
		return nil, serviceErrors.ErrChatForkMemberInvalid
	}
}
