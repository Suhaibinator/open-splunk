package audit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

var _ control.IndexMutationAuditAppender = (*Store)(nil)

// AppendIndexMutationInTransaction adapts the control-owned index mutation
// projection to the immutable audit journal. Unlike the package-level append
// helper, this production control-plane boundary requires an explicitly
// installed successful actor and never falls back to the system actor.
func (store *Store) AppendIndexMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event control.IndexMutationAuditEvent,
) error {
	actor, explicit := ActorFromContext(ctx)
	if !explicit || !validSuccessfulActor(actor) {
		return fmt.Errorf(
			"%w: audit actor cannot perform successful mutations",
			control.ErrInvalidArgument,
		)
	}
	action, ok := indexMutationAction(event.Action)
	if !ok {
		return fmt.Errorf(
			"%w: index mutation audit action is invalid",
			control.ErrInvalidArgument,
		)
	}
	_, err := store.AppendInTransaction(ctx, tx, tenantID, SuccessfulEvent{
		OccurredAt:    event.OccurredAt,
		Action:        action,
		TargetKind:    TargetKindIndex,
		TargetID:      event.IndexID,
		TargetVersion: event.IndexVersion,
	})
	return err
}

func indexMutationAction(action control.IndexMutationAuditAction) (Action, bool) {
	switch action {
	case control.IndexMutationAuditActionCreate:
		return ActionIndexCreate, true
	case control.IndexMutationAuditActionUpdate:
		return ActionIndexUpdate, true
	case control.IndexMutationAuditActionActivate:
		return ActionIndexActivate, true
	case control.IndexMutationAuditActionArchive:
		return ActionIndexArchive, true
	case control.IndexMutationAuditActionDeleteKeepData:
		return ActionIndexDeleteKeepData, true
	case control.IndexMutationAuditActionDeleteData:
		return ActionIndexDeleteData, true
	default:
		return "", false
	}
}
