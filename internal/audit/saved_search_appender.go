package audit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"gorm.io/gorm"
)

var _ savedobjects.SavedSearchMutationAuditAppender = (*Store)(nil)

// AppendSavedSearchMutationInTransaction adapts the definition-free
// savedobjects projection to the immutable audit journal. The current trusted
// single-user browser surface has no end-user authentication, so an absent
// explicit actor intentionally uses the journal's system actor. A future
// authenticated browser user or administrator is preserved by AppendInTransaction.
func (store *Store) AppendSavedSearchMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event savedobjects.SavedSearchMutationAuditEvent,
) error {
	action, ok := savedSearchMutationAction(event.Action)
	if !ok {
		return fmt.Errorf(
			"%w: saved-search mutation audit action is invalid",
			control.ErrInvalidArgument,
		)
	}
	_, err := store.AppendInTransaction(ctx, tx, tenantID, SuccessfulEvent{
		OccurredAt:    event.OccurredAt,
		Action:        action,
		TargetKind:    TargetKindSavedSearch,
		TargetID:      event.SavedSearchID,
		TargetVersion: event.SavedSearchVersion,
	})
	return err
}

func savedSearchMutationAction(
	action savedobjects.SavedSearchMutationAuditAction,
) (Action, bool) {
	switch action {
	case savedobjects.SavedSearchMutationAuditActionCreate:
		return ActionSavedSearchCreate, true
	case savedobjects.SavedSearchMutationAuditActionUpdate:
		return ActionSavedSearchUpdate, true
	case savedobjects.SavedSearchMutationAuditActionDuplicate:
		return ActionSavedSearchDuplicate, true
	case savedobjects.SavedSearchMutationAuditActionDelete:
		return ActionSavedSearchDelete, true
	default:
		return "", false
	}
}
