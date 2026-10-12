package notification

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
)

// RetireActivityScopes admits all scope-wide retirements before an operation
// takes any recipient/type lock. Supply every scope together; the store locks
// their canonical order and commits all fences with the source transaction.
func (h *Handler) RetireActivityScopes(ctx context.Context, scopes []string) error {
	if h == nil || h.store == nil {
		return nil
	}
	for _, scope := range scopes {
		if !notificationkey.ValidActivityScope(scope) {
			return ErrInvalid
		}
	}
	maxScopes := h.deps.Config().RetirementMaxRecipients
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		if err := h.store.EnqueueNotificationScopeRetirements(tx, scopes, maxScopes); err != nil {
			return err
		}
		h.nudgeRetirementAfterCommit(tx)
		return nil
	})
}

// RetireActivityScopeForRecipients fences a source-owned audience even before
// async entries exist. It remains active while notification emission is disabled.
// Retirement cutoffs are written under the recipient/type locks.
func (h *Handler) RetireActivityScopeForRecipients(ctx context.Context, scope string, recipients []persistence.NotificationRecipientType) error {
	if h == nil || h.store == nil {
		return nil
	}
	return h.retireActivityScopeForRecipients(ctx, scope, recipients, h.store.RetireActivityScopeForRecipients)
}

// RetireChatActivityScopeForRecipients is the synchronous single-departure fence
// for native chat. It relies on membership generations and writes no cutoffs.
func (h *Handler) RetireChatActivityScopeForRecipients(ctx context.Context, scope string, recipients []persistence.NotificationRecipientType) error {
	if h == nil || h.store == nil {
		return nil
	}
	return h.retireActivityScopeForRecipients(ctx, scope, recipients, h.store.RetireChatActivityScopeForRecipients)
}

// retireActivityScopeForRecipients samples the recipient cap once per call; the
// store rejects a larger unique audience before taking any lock.
func (h *Handler) retireActivityScopeForRecipients(ctx context.Context, scope string, recipients []persistence.NotificationRecipientType, retire func(context.Context, string, []persistence.NotificationRecipientType, int) ([]models.Notification, error)) error {
	if !notificationkey.ValidActivityScope(scope) {
		return ErrInvalid
	}
	for _, recipient := range recipients {
		if recipient.UserID == 0 || recipient.Type == 0 {
			return ErrInvalid
		}
	}
	maxRecipients := h.deps.Config().RetirementMaxRecipients
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		rows, err := retire(tx, scope, recipients, maxRecipients)
		if err != nil {
			return err
		}
		for i := range rows {
			h.publish(tx, &rows[i])
		}
		return nil
	})
}
