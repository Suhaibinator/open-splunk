package chat

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// isStandardKind reports whether the stored kind is explicitly STANDARD.
func isStandardKind(kind proto_models.MessageKind) bool {
	return kind == proto_models.MessageKind_MESSAGE_KIND_STANDARD
}

// requireStandardMutableMessage enforces transcript immutability for the public message-mutation
// paths (edit, delete, reactions, replies/threads). Non-STANDARD transcript rows (TOOL_CALL,
// TOOL_RESULT, SYSTEM_EVENT, COMPACTION_SUMMARY, AGENT_NOTE) and hidden rows are immutable through
// the public API: editing/deleting them would corrupt canonical transcript reconstruction (G1) and
// the audit trail, and reacting/replying to hidden rows would leak them. The only permitted
// mutations to a TOOL_CALL row are the approval/execution state transitions owned by the approval
// lane, which do not go through these public paths.
//
// Parameters:
//   - msg: the persistence message being mutated.
//
// Returns:
//   - err: ErrChatMessageImmutable when the row is non-STANDARD or hidden; nil otherwise.
func requireStandardMutableMessage(msg *common_persistence_models.ChatMessage) error {
	if msg == nil {
		return nil
	}
	if !isStandardKind(msg.Kind) {
		return serviceErrors.ErrChatMessageImmutable
	}
	if msg.HiddenFromUsers != nil && *msg.HiddenFromUsers {
		return serviceErrors.ErrChatMessageImmutable
	}
	return nil
}

// resolveReadOptions converts a public include_hidden request flag into a resolved persistence
// ChatMessageReadOptions, performing the authority check at the service layer.
//
// This is the single place where hidden-message read authority is decided. Persistence never makes
// the authority call (it only applies the resolved filter). include_hidden=false is the default
// browser/user read (VisibleToActor, hidden rows filtered server-side). include_hidden=true is
// admin/debug/bot tooling: it is mapped to IncludeHiddenIfAuthorized ONLY after the actor passes the
// ChatPermissionReadHiddenMessages check; an unauthorized actor's explicit request is REJECTED
// (403 via ErrInsufficientPermissions), never silently downgraded to a visible-only result. When
// the app has configured no chat permission callback at all, hidden reads are DENIED (fail
// closed) except for site admins holding PermissionAdminChatChannels: consuming apps that want
// include_hidden=true for anyone else must wire a callback granting
// ChatPermissionReadHiddenMessages.
//
// Parameters:
//   - ctx: request context carrying the transaction/trace.
//   - actor: the authenticated actor (for the permission callback).
//   - channel: the resolved channel whose hidden rows would be read.
//   - includeHidden: the public request flag.
//
// Returns:
//   - result1: the resolved read options.
//   - err: ErrInsufficientPermissions when include_hidden is requested without authority.
func (h *Handler[U]) resolveReadOptions(
	ctx context.Context,
	actor U,
	channel *common_service_models.ChatChannel,
	includeHidden bool,
) (persistence.ChatMessageReadOptions, error) {
	if !includeHidden {
		return persistence.ChatMessageReadOptions{
			VisibilityMode: persistence.ChatMessageVisibilityVisibleToActor,
		}, nil
	}
	if channel == nil {
		return persistence.ChatMessageReadOptions{}, serviceErrors.ErrInsufficientPermissions
	}

	// Hidden-row read authority FAILS CLOSED: without a site-admin grant or a chat permission
	// callback there is no authority source, and hidden transcript rows (tool payloads, agent
	// notes, compaction summaries) must not default-open to every channel member. Only this surface
	// is stricter — other operations keep the opt-in default-allow callback pattern.
	if !h.hasHiddenReadAuthority(ctx, actor) {
		return persistence.ChatMessageReadOptions{}, serviceErrors.ErrInsufficientPermissions
	}

	req := ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionReadHiddenMessages,
		Actor:     actor,
		Channel:   channel,
	}
	if err := h.authorize(ctx, req); err != nil {
		// Unauthorized explicit include_hidden is a hard failure, not a downgrade.
		return persistence.ChatMessageReadOptions{}, err
	}

	// Bind the read to the channel that was authorized so the payload query can
	// never observe a row from another channel, whichever caller issues it.
	return persistence.ChatMessageReadOptions{
		VisibilityMode: persistence.ChatMessageVisibilityIncludeHiddenIfAuthorized,
		ChannelID:      channel.ID,
	}, nil
}

// hasHiddenReadAuthority reports whether a hidden-row read has an authority source: a site admin
// holding PermissionAdminChatChannels, or an application permission callback. The callback is
// still consulted either way and may veto.
func (h *Handler[U]) hasHiddenReadAuthority(ctx context.Context, actor U) bool {
	return h.hasPermissionCallback() || h.isSiteChatAdmin(ctx, actor)
}
