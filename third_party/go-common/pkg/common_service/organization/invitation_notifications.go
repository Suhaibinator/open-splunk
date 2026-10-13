package organization

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// SetNotificationSink is construction-time wiring independent of application callbacks.
func (h *OrganizationHandler[U]) SetNotificationSink(sink ports.OrganizationNotificationSink) {
	h.notificationSink = sink
}

func (h *OrganizationHandler[U]) requireInvitationNotificationTransaction(ctx context.Context) error {
	if h.notificationSink == nil {
		return nil
	}
	return h.notificationSink.RequireInvitationNotificationTransaction(ctx)
}

func (h *OrganizationHandler[U]) resolveInvitationNotification(ctx context.Context, invitationID uint64) error {
	if h.notificationSink == nil {
		return nil
	}
	return h.notificationSink.OrganizationInvitationResolved(ctx, invitationID)
}

// NotifyPendingInvitationsForUser only produces acceptance-required inbox
// entries. It does not execute the separate auto-add membership workflow.
func (h *OrganizationHandler[U]) NotifyPendingInvitationsForUser(ctx context.Context, userID uint64, email string) error {
	if h.notificationSink == nil {
		return nil
	}
	if err := h.requireInvitationNotificationTransaction(ctx); err != nil {
		return err
	}
	rows, err := h.persistenceHandler.GetPendingInvitationsByEmail(ctx, email)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.RequiresAcceptance && row.AcceptedAt == nil && row.DeclinedAt == nil && !row.IsExpired() {
			if err := h.notificationSink.OrganizationInvitationRecorded(ctx, row.ID, userID); err != nil {
				return err
			}
		}
	}
	return nil
}
