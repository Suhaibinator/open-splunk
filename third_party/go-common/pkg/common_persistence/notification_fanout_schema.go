package common_persistence

import "gorm.io/gorm"

// notificationFanoutIndexes back the fan-out engine's transaction fences,
// organization audience enumeration, and emission maintenance scans. GORM tags
// cannot express the descending and partial forms. Every table is created by
// AutoMigrate before these statements run.
var notificationFanoutIndexes = []string{
	"CREATE INDEX IF NOT EXISTS idx_notification_emission_deadline ON gocommon_notification_emissions (GREATEST(due_at, lease_expires_at), id) WHERE state = 'pending' AND fingerprint <> ''",
	"CREATE INDEX IF NOT EXISTS idx_chat_participant_notification_join_epoch_xid ON gocommon_chat_channel_participants (notification_join_epoch, notification_join_xid)",
	"CREATE INDEX IF NOT EXISTS idx_notification_org_members ON gocommon_user_organizations (organization_id, user_id)",
	"CREATE INDEX IF NOT EXISTS idx_notification_emission_source_xid ON gocommon_notification_emissions (source_epoch, source_xid DESC)",
	"CREATE INDEX IF NOT EXISTS idx_notification_emission_snapshot_xid ON gocommon_notification_emissions (source_snapshot_epoch, source_snapshot_upper_bound DESC)",
	"CREATE INDEX IF NOT EXISTS idx_notification_emission_pending_age ON gocommon_notification_emissions (created_at) WHERE state = 'pending' AND fingerprint <> ''",
	"CREATE INDEX IF NOT EXISTS idx_notification_emission_compaction ON gocommon_notification_emissions (completed_at, id) WHERE compacted_at IS NULL AND state IN ('completed', 'cancelled')",
}

// ensureNotificationFanoutIndexes runs under the schema bootstrap lock after
// AutoMigrate. Each statement is idempotent.
func ensureNotificationFanoutIndexes(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, statement := range notificationFanoutIndexes {
			if err := executeSharedDDL(tx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}
