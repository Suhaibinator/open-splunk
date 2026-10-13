package common_service

import (
	"context"
	"errors"
	"fmt"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetRealtimeSessionCloser wires the realtime transport at construction so session revocation,
// bans and deletions close the affected sockets immediately.
func (h *CommonServiceHandler[U]) SetRealtimeSessionCloser(closer ports.RealtimeSessionCloser) {
	h.realtimeSessions = closer
	if h.authHandler != nil {
		h.authHandler.SetRealtimeSessionCloser(closer)
	}
}

// SetChatRealtimeAccessInvalidator wires the realtime transport at construction.
func (h *CommonServiceHandler[U]) SetChatRealtimeAccessInvalidator(invalidator ports.ChatRealtimeAccessInvalidator) {
	h.chatRealtimeInvalidator = invalidator
	if admin, ok := h.adminHandler.(interface {
		SetChatRealtimeAccessInvalidator(ports.ChatRealtimeAccessInvalidator)
	}); ok {
		admin.SetChatRealtimeAccessInvalidator(invalidator)
	}
	if chat, ok := h.chatHandler.(interface {
		SetChatRealtimeAccessInvalidator(ports.ChatRealtimeAccessInvalidator)
	}); ok {
		chat.SetChatRealtimeAccessInvalidator(invalidator)
	}
	if org, ok := h.organizationHandler.(interface {
		SetChatRealtimeAccessInvalidator(ports.ChatRealtimeAccessInvalidator)
	}); ok {
		org.SetChatRealtimeAccessInvalidator(invalidator)
	}
}

// InvalidateChatRealtimeAccess revokes grants after an application-owned access
// mutation commits. Pass an empty invalidation for broad permission changes.
// Without an ambient transaction, call only after the mutation has committed.
// Transactions without post-commit hooks must call again after committing with
// a context that no longer contains that transaction.
func (h *CommonServiceHandler[U]) InvalidateChatRealtimeAccess(ctx context.Context, invalidation ports.ChatRealtimeAccessInvalidation) {
	ports.InvalidateChatRealtimeAfterCommit(ctx, h.chatRealtimeInvalidator, invalidation)
}

// AuthorizeChatRealtimeRead reloads the actor without the authentication cache.
// Application UserLookup callbacks must return current authority on these calls.
func (h *CommonServiceHandler[U]) AuthorizeChatRealtimeRead(ctx context.Context, actor U, channelID uint64) (ports.ChatRealtimeCategory, error) {
	chat, ok := h.chatHandler.(interface {
		AuthorizeRealtimeRead(context.Context, U, uint64) (ports.ChatRealtimeCategory, error)
	})
	if !ok || actor.GetID() == 0 {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	core, err := h.commonPersistenceHandler.GetUserByID(ctx, uint64(actor.GetID()))
	if err != nil {
		if errors.Is(err, persistence.ErrUserNotFound) {
			return 0, serviceErrors.ErrInsufficientPermissions
		}
		return 0, fmt.Errorf("%w: %w", ports.ErrChatRealtimeUnavailable, err)
	}
	if core == nil || core.Banned || core.DeletedAt.Valid {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	if h.userCallbacks.Validate() != nil {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	current, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(core.ID))
	if err != nil {
		if errors.Is(err, persistence.ErrUserNotFound) || errors.Is(err, serviceErrors.ErrUserNotFound) {
			return 0, serviceErrors.ErrInsufficientPermissions
		}
		return 0, fmt.Errorf("%w: %w", ports.ErrChatRealtimeUnavailable, err)
	}
	if current.GetID() != actor.GetID() {
		return 0, serviceErrors.ErrInsufficientPermissions
	}
	ctx = scontext.WithUser[common_service_models.UserIdType](ctx, &current)
	return chat.AuthorizeRealtimeRead(ctx, current, channelID)
}
