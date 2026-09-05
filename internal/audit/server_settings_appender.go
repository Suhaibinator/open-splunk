package audit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

var _ control.ServerSettingsMutationAuditAppender = (*Store)(nil)

func (store *Store) AppendServerSettingsMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event control.ServerSettingsMutationAuditEvent,
) error {
	if err := requireExplicitAdministrativeMutationActor(ctx); err != nil {
		return err
	}
	if !event.Target.Valid() {
		return fmt.Errorf(
			"%w: server settings audit target %q is unknown",
			control.ErrInvalidArgument,
			string(event.Target),
		)
	}
	_, err := store.AppendInTransaction(ctx, tx, tenantID, SuccessfulEvent{
		OccurredAt:    event.OccurredAt,
		Action:        ActionServerSettingsUpdate,
		TargetKind:    TargetKindServerSettings,
		TargetID:      string(event.Target),
		TargetVersion: event.Version,
	})
	return err
}
