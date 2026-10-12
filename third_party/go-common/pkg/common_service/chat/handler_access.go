package chat

import (
	"context"
	"errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// newChannelFeatures returns the feature flags of a channel about to be created: the configured
// flags, narrowed by the application's ChannelFeatures hook when one is installed.
func (h *Handler[U]) newChannelFeatures(ctx context.Context, creation ports.ChatChannelCreation[U]) (common_persistence_models.ChatChannelFeatures, error) {
	creation.Configured = h.configuredChannelFeatures()
	if h.permissionCallbacks == nil || h.permissionCallbacks.Callbacks == nil || h.permissionCallbacks.Callbacks.ChannelFeatures == nil {
		return creation.Configured, nil
	}
	features, err := h.permissionCallbacks.Callbacks.ChannelFeatures(ctx, creation)
	if err != nil {
		return 0, err
	}
	return features & creation.Configured, nil
}

// configuredChannelFeatures returns the chat_support_* and chat_allow_* flags that are on now.
func (h *Handler[U]) configuredChannelFeatures() common_persistence_models.ChatChannelFeatures {
	operationalConfig := h.policy()
	var features common_persistence_models.ChatChannelFeatures
	for flag, on := range map[common_persistence_models.ChatChannelFeatures]bool{
		common_persistence_models.ChatChannelSupportReplies:                operationalConfig.ChatSupportReplies(),
		common_persistence_models.ChatChannelSupportThreads:                operationalConfig.ChatSupportThreads(),
		common_persistence_models.ChatChannelSupportMessageEdits:           operationalConfig.ChatSupportMessageEdits(),
		common_persistence_models.ChatChannelSupportMessageDeletes:         operationalConfig.ChatSupportMessageDeletes(),
		common_persistence_models.ChatChannelSupportMessageReactions:       operationalConfig.ChatSupportMessageReactions(),
		common_persistence_models.ChatChannelAllowMultipleReactionsPerUser: operationalConfig.ChatAllowMultipleReactionsPerUser(),
		common_persistence_models.ChatChannelSupportEmbeds:                 operationalConfig.ChatSupportEmbeds(),
		common_persistence_models.ChatChannelSupportAttachments:            operationalConfig.ChatSupportAttachments(),
		common_persistence_models.ChatChannelAllowNativeEmbeds:             operationalConfig.ChatAllowNativeEmbeds(),
		common_persistence_models.ChatChannelAllowCustomEmbeds:             operationalConfig.ChatAllowCustomEmbeds(),
		common_persistence_models.ChatChannelSupportInferenceUsage:         operationalConfig.ChatSupportInferenceUsage(),
	} {
		if on {
			features |= flag
		}
	}
	return features
}

// channelMembershipKey identifies a cached membership entry by channel and user IDs.
type channelMembershipKey struct {
	channelID uint64
	userID    uint64
}

// channelAccessEntry caches channel-scoped access data: the channel row
// (without the participant roster). Cached values are treated as immutable.
type channelAccessEntry struct {
	channel *common_persistence_models.ChatChannel
}

// loadChannelAccess returns the channel (without its participant roster) and the
// requesting user's active membership role, consulting the access caches when
// enabled. Membership is only cached positively, so users gain access
// immediately after being added; removals on this instance invalidate eagerly,
// and removals performed elsewhere (admin API, other instances) converge after
// the cache TTL.
//
// The returned row is the cached value itself, shared with every concurrent
// reader: callers read it and never write to it. Anything that leaves this
// package (permission callbacks, event sinks, responses) goes through
// PersistenceChatChannelToService, which owns every pointer it exposes.
//
// Returns:
// - `result1`: the persistence channel row (participant roster not populated).
// - `result2`: the user's channel role (meaningful only when `result3` is true).
// - `result3`: whether the user is an active member of the channel.
// - `err`: reports any failure encountered while running this operation.
func (h *Handler[U]) loadChannelAccess(
	ctx context.Context,
	channelID uint64,
	userID common_service_models.UserIdType,
) (*common_persistence_models.ChatChannel, proto_models.ChannelRole, bool, error) {
	h.refreshAccessCaches(h.policy())
	h.accessCacheMu.RLock()
	defer h.accessCacheMu.RUnlock()
	cacheEnabled := h.channelAccessCache != nil && h.membershipCache != nil
	if cacheEnabled {
		channelItem := h.channelAccessCache.Get(channelID)
		membershipItem := h.membershipCache.Get(channelMembershipKey{channelID: channelID, userID: uint64(userID)})
		if channelItem != nil && membershipItem != nil {
			entry := channelItem.Value()
			return entry.channel, membershipItem.Value(), true, nil
		}
	}

	access, err := h.persistenceHandler.GetChatChannelAccess(ctx, channelID, uint64(userID))
	if err != nil {
		switch {
		case errors.Is(err, persistence.ErrChatChannelNotFound):
			err = serviceErrors.ErrChatChannelNotFound
		case errors.Is(err, persistence.ErrDatabaseError):
			err = serviceErrors.ErrDatabaseError
		default:
			err = serviceErrors.ErrInternalError
		}
		return nil, proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED, false, err
	}

	role := proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED
	isMember := access.Participant != nil
	if isMember {
		role = access.Participant.Role
	}

	if cacheEnabled {
		h.channelAccessCache.Set(channelID, channelAccessEntry{channel: access.Channel}, ttlcache.DefaultTTL)
		if isMember {
			// Negative membership results are deliberately not cached so that
			// newly added participants are recognized immediately.
			h.membershipCache.Set(channelMembershipKey{channelID: channelID, userID: uint64(userID)}, role, ttlcache.DefaultTTL)
		}
	}

	return access.Channel, role, isMember, nil
}

// invalidateChannelAccessCache drops the cached channel entry and, when user
// IDs are given, their cached membership roles. It invalidates immediately and
// again after the surrounding transaction commits, so a concurrent reader that
// re-caches pre-commit state between the two points is corrected.
//
// Parameters:
// - `ctx`: propagates request-scoped deadlines, cancellation, and trace metadata.
// - `channelID`: supplies the `channelID` input as `uint64` for this operation.
// - `userIDs`: membership cache entries to drop alongside the channel entry.
func (h *Handler[U]) invalidateChannelAccessCache(ctx context.Context, channelID uint64, userIDs ...uint64) {
	invalidate := func() {
		h.accessCacheMu.RLock()
		defer h.accessCacheMu.RUnlock()
		if h.channelAccessCache == nil || h.membershipCache == nil {
			return
		}
		h.channelAccessCache.Delete(channelID)
		for _, id := range userIDs {
			h.membershipCache.Delete(channelMembershipKey{channelID: channelID, userID: id})
		}
	}
	invalidate()
	persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error {
		invalidate()
		return nil
	})
}

// InvalidateAccessCache evicts committed administrative changes from the HTTP read/write gates.
// It also evicts immediately, and repeats after commit to fence readers of pre-commit state.
func (h *Handler[U]) InvalidateAccessCache(ctx context.Context, change ports.ChatRealtimeAccessInvalidation) {
	invalidate := func() {
		h.accessCacheMu.Lock()
		defer h.accessCacheMu.Unlock()
		if h.channelAccessCache == nil || h.membershipCache == nil {
			return
		}
		if change.ChannelID == 0 {
			h.channelAccessCache.DeleteAll()
		} else {
			h.channelAccessCache.Delete(change.ChannelID)
		}
		for key := range h.membershipCache.Items() {
			if (change.ChannelID == 0 || key.channelID == change.ChannelID) && (change.UserID == 0 || key.userID == uint64(change.UserID)) {
				h.membershipCache.Delete(key)
			}
		}
	}
	invalidate()
	persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error { invalidate(); return nil })
}

func (h *Handler[U]) requireChannelMember(ctx context.Context, channelID uint64, userID common_service_models.UserIdType) (*common_service_models.ChatChannel, error) {
	return h.requireChannelRole(ctx, channelID, userID, false, proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED)
}

func (h *Handler[U]) requireChannelMemberAllowArchived(ctx context.Context, channelID uint64, userID common_service_models.UserIdType) (*common_service_models.ChatChannel, error) {
	return h.requireChannelRole(ctx, channelID, userID, true, proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED)
}

func (h *Handler[U]) requireChannelAdmin(ctx context.Context, channelID uint64, userID common_service_models.UserIdType, allowArchived bool) (*common_service_models.ChatChannel, error) {
	return h.requireChannelRole(ctx, channelID, userID, allowArchived, proto_models.ChannelRole_CHANNEL_ROLE_ADMIN)
}

func (h *Handler[U]) requireChannelOwner(ctx context.Context, channelID uint64, userID common_service_models.UserIdType, allowArchived bool) (*common_service_models.ChatChannel, error) {
	return h.requireChannelRole(ctx, channelID, userID, allowArchived, proto_models.ChannelRole_CHANNEL_ROLE_OWNER)
}

// requireChannelRole is the participant gate: the user must be a participant of the channel,
// the channel must not be archived unless allowArchived, and the participant role must be at
// least minRole. Roles are ordinal (MEMBER < ADMIN < OWNER); minRole UNSPECIFIED skips the
// role check so participants with an unset role still pass the member gates.
func (h *Handler[U]) requireChannelRole(
	ctx context.Context,
	channelID uint64,
	userID common_service_models.UserIdType,
	allowArchived bool,
	minRole proto_models.ChannelRole,
) (*common_service_models.ChatChannel, error) {
	channel, role, isMember, err := h.loadChannelAccess(ctx, channelID, userID)
	if err != nil {
		return nil, err
	}
	if channel.IsArchived && !allowArchived {
		return nil, serviceErrors.ErrChatChannelArchived
	}
	if !isMember {
		return nil, serviceErrors.ErrChatNotMember
	}
	if minRole != proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED && role < minRole {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	return h.converter.PersistenceChatChannelToService(channel), nil
}

// authorize executes part of the chat service flow and coordinates work for the *Handler[U] receiver.
func (h *Handler[U]) authorize(ctx context.Context, req ports.ChatPermissionRequest[U]) error {
	if !h.hasPermissionCallback() {
		return nil
	}
	return h.permissionCallbacks.Callbacks.Authorize(ctx, req)
}

// hasPermissionCallback reports whether the consuming app wired a chat permission callback.
// Most operations treat an absent callback as allow (the pre-existing opt-in pattern);
// privileged surfaces such as hidden-row reads treat it as deny (fail closed).
func (h *Handler[U]) hasPermissionCallback() bool {
	return h != nil && h.permissionCallbacks != nil && h.permissionCallbacks.Callbacks != nil && h.permissionCallbacks.Callbacks.Authorize != nil
}

// refreshAccessCaches changes only cache policy, never locks or durable accounting.
// Readers and invalidators finish before replacement; unrelated releases preserve entries.
func (h *Handler[U]) refreshAccessCaches(cfg PolicyView) {
	ttl, capacity := cfg.ChatRoleCacheTTL(), cfg.ChatRoleCacheCapacity()
	h.accessCacheMu.Lock()
	defer h.accessCacheMu.Unlock()
	if ttl == h.accessCacheTTL && capacity == h.accessCacheCapacity {
		return
	}
	h.accessCacheTTL, h.accessCacheCapacity = ttl, capacity
	h.channelAccessCache, h.membershipCache = nil, nil
	if ttl <= 0 || capacity == 0 {
		return
	}
	h.channelAccessCache = ttlcache.New(
		ttlcache.WithTTL[uint64, channelAccessEntry](ttl),
		ttlcache.WithCapacity[uint64, channelAccessEntry](capacity),
		ttlcache.WithDisableTouchOnHit[uint64, channelAccessEntry](),
	)
	h.membershipCache = ttlcache.New(
		ttlcache.WithTTL[channelMembershipKey, proto_models.ChannelRole](ttl),
		ttlcache.WithCapacity[channelMembershipKey, proto_models.ChannelRole](capacity),
		ttlcache.WithDisableTouchOnHit[channelMembershipKey, proto_models.ChannelRole](),
	)
}

// requireChannelRead is the access gate for reads. It admits a participant, at no extra cost
// (membership and the channel row come from the access cache), then the non-participant readers
// admitted by requireNonParticipantRead. Everyone else gets ErrChatNotMember.
//
// Archive state never blocks a read: an archived transcript stays readable by the same people who
// could read it before, and a stranger learns nothing about whether the channel is archived. Only
// the mutation gates (requireChannelMember, requireChannelAdmin, requireChannelOwner) enforce
// ErrChatChannelArchived.
func (h *Handler[U]) requireChannelRead(ctx context.Context, channelID uint64, actor U) (*common_service_models.ChatChannel, error) {
	channel, _, _, err := h.requireChannelReadAccess(ctx, channelID, actor, false)
	return channel, err
}

// requireChannelReadAccess is requireChannelRead that also reports whether the reader participates
// in the channel and, when they do, their role. beyondPublic marks a read of rows the public level
// never serves (hidden or deleted messages): it refuses a reader admitted only by that level with
// ErrInsufficientPermissions, whatever Authorize would return.
func (h *Handler[U]) requireChannelReadAccess(ctx context.Context, channelID uint64, actor U, beyondPublic bool) (*common_service_models.ChatChannel, proto_models.ChannelRole, bool, error) {
	channel, role, isMember, err := h.loadChannelAccess(ctx, channelID, actor.GetID())
	if err != nil {
		return nil, role, false, err
	}
	if !isMember {
		if err := h.requireNonParticipantRead(ctx, actor, channel, channelReadRequest, beyondPublic); err != nil {
			return nil, role, false, err
		}
	}
	return h.converter.PersistenceChatChannelToService(channel), role, isMember, nil
}

// channelReadSurface names the caller of requireNonParticipantRead, which decides how fresh the
// organization authority it consults must be.
type channelReadSurface uint8

const (
	// channelReadRequest serves HTTP requests: the organization role comes from the resolver's
	// cache, like the channel row and membership it was reached through.
	channelReadRequest channelReadSurface = iota
	// channelReadRealtime serves open-chat subscriptions, which outlive a request: the role is
	// read fresh so a revoked organization role cannot keep a socket authorized.
	channelReadRealtime
)

// requireNonParticipantRead is the read gate for a user who is not a participant, shared by the
// request and realtime paths so both admit the same readers in the same order: first any signed-in
// user when public chat reads are enabled and the channel's public read level is at least
// SIGNED_IN (no lookup), then a non-participant granted by the channel-owned organization access
// rule, then a site admin holding PermissionAdminChatChannels. Every grant is read-only and confers no
// participation or mutation rights. Everyone else gets ErrChatNotMember. A read granted only
// through the site-admin permission is logged here, for either surface, for audit. With
// beyondPublic the public grant does not admit, and a reader it alone would admit gets
// ErrInsufficientPermissions.
func (h *Handler[U]) requireNonParticipantRead(
	ctx context.Context,
	actor U,
	channel *common_persistence_models.ChatChannel,
	surface channelReadSurface,
	beyondPublic bool,
) error {
	publicGrant := channel.PublicRead >= proto_models.ChatChannelPublicRead_CHAT_CHANNEL_PUBLIC_READ_SIGNED_IN && h.policy().EnablePublicChatRead()
	if publicGrant && !beyondPublic {
		return nil
	}
	var granted bool
	var err error
	auditMessage := "Site admin chat channel read"
	if surface == channelReadRealtime {
		granted, err = h.organizationReadGrantedFresh(ctx, actor, channel)
		auditMessage = "Site admin chat realtime read"
	} else {
		granted, err = h.organizationReadGranted(ctx, channel, actor.GetID())
	}
	if err != nil {
		return err
	}
	if granted {
		return nil
	}
	if !h.isSiteChatAdmin(ctx, actor) {
		if publicGrant {
			return serviceErrors.ErrInsufficientPermissions
		}
		return serviceErrors.ErrChatNotMember
	}
	appLogger.For(ctx, h.logger, chatLoggerName).Info(auditMessage,
		zap.Uint64(appLogger.KeyActorUserID, uint64(actor.GetID())),
		zap.Uint64(appLogger.KeyChannelID, channel.ID))
	return nil
}

// isSiteChatAdmin reports whether the actor holds the site permission that grants read access to
// every chat channel and its hidden messages.
func (h *Handler[U]) isSiteChatAdmin(ctx context.Context, actor U) bool {
	return h.permissionHandler != nil &&
		h.permissionHandler.HasSitePermission(ctx, actor.GetUserType(), permission.PermissionAdminChatChannels)
}
