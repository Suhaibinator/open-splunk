package audit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

var _ control.AppMutationAuditAppender = (*Store)(nil)

// AppendAppMutationInTransaction adapts the control-owned app mutation
// projection to the immutable audit journal. This production control-plane
// boundary requires an explicitly installed successful actor and never falls
// back to the system actor.
func (store *Store) AppendAppMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event control.AppMutationAuditEvent,
) error {
	if err := requireExplicitSuccessfulActor(ctx); err != nil {
		return err
	}
	action, ok := appMutationAction(event.Action)
	if !ok {
		return fmt.Errorf(
			"%w: app mutation audit action is invalid",
			control.ErrInvalidArgument,
		)
	}
	_, err := store.AppendInTransaction(ctx, tx, tenantID, SuccessfulEvent{
		OccurredAt:    event.OccurredAt,
		Action:        action,
		TargetKind:    TargetKindApp,
		TargetID:      event.AppID,
		TargetVersion: event.AppVersion,
	})
	return err
}

func appMutationAction(action control.AppMutationAuditAction) (Action, bool) {
	switch action {
	case control.AppMutationAuditActionCreate:
		return ActionAppCreate, true
	case control.AppMutationAuditActionUpdate:
		return ActionAppUpdate, true
	case control.AppMutationAuditActionActivate:
		return ActionAppActivate, true
	case control.AppMutationAuditActionArchive:
		return ActionAppArchive, true
	case control.AppMutationAuditActionDelete:
		return ActionAppDelete, true
	default:
		return "", false
	}
}
