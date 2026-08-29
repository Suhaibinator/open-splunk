package audit

import (
	"context"

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
	_, err := store.AppendInTransaction(ctx, tx, tenantID, SuccessfulEvent{
		OccurredAt:    event.OccurredAt,
		Action:        ActionServerSettingsUpdate,
		TargetKind:    TargetKindServerSettings,
		TargetID:      "search-limits",
		TargetVersion: event.Version,
	})
	return err
}
