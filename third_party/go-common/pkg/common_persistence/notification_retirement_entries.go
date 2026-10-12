package common_persistence

import (
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Retirement only publishes invalidation identities. Never transfer payloads,
// previews, or historical occurrence bodies while settling an entry page.
const retirementEntryColumns = "id,organization_id,user_id,type,group_key,reusable_group,activity_scope,revision,resolved"

func retirementVisibleSQL() string {
	// Unstamped/foreign rows are the pre-fence baseline. A same-incarnation
	// materialization committed after the snapshot must survive older pages.
	return `(materialization_epoch = 0 OR materialization_epoch <> ? OR materialization_xid = '0'::xid8
 OR (materialization_system_identifier <> '' AND ? <> '' AND materialization_system_identifier <> ?)
 OR CASE WHEN materialization_xid = ?::xid8 AND ?::bigint > 0
 THEN materialization_ordinal < ?::bigint
 ELSE pg_visible_in_snapshot(materialization_xid, ?::pg_snapshot) END)`
}

func retirementVisibilityArgs(cutoff models.NotificationVisibilitySnapshot) []any {
	ownXID := cutoff.OwnXID
	if ownXID == "" {
		ownXID = "0"
	}
	return []any{cutoff.Epoch, cutoff.SystemIdentifier, cutoff.SystemIdentifier, ownXID, cutoff.OwnOrdinal, cutoff.OwnOrdinal, cutoff.Snapshot}
}

// retireNotificationTargets assumes recipient locks, then takes bounded group
// and inbox locks in the ordinary order. Its update rechecks materialization
// visibility, and its result carries only fields used for invalidation.
func retireNotificationTargets(db *gorm.DB, targets []models.Notification, cutoff models.NotificationVisibilitySnapshot) ([]models.Notification, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	groups := make([]models.NotificationGroupState, 0, len(targets))
	ids := make(pq.Int64Array, 0, len(targets))
	for _, target := range targets {
		groups = append(groups, notificationGroupIdentity(target))
		ids = append(ids, int64(target.ID))
	}
	if err := lockNotificationGroups(db, groups); err != nil {
		return nil, err
	}
	var locked []models.Notification
	if err := db.Select("id").Where("id = ANY(?)", ids).Order("id").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&locked).Error; err != nil {
		return nil, err
	}
	args := append([]any{ids}, retirementVisibilityArgs(cutoff)...)
	var settled []models.Notification
	if err := db.Select(retirementEntryColumns).Where("id = ANY(?) AND "+retirementVisibleSQL(), args...).Find(&settled).Error; err != nil {
		return nil, err
	}
	if len(settled) == 0 {
		return nil, nil
	}
	ids = ids[:0]
	for _, row := range settled {
		ids = append(ids, int64(row.ID))
	}
	var changed []models.Notification
	columns := []clause.Column{{Name: "id"}, {Name: "organization_id"}, {Name: "user_id"}, {Name: "type"}, {Name: "activity_scope"}, {Name: "revision"}, {Name: "resolved"}}
	if err := db.Model(&changed).Clauses(clause.Returning{Columns: columns}).Where("id = ANY(?) AND resolved = false", ids).Update("resolved", true).Error; err != nil {
		return nil, err
	}
	if err := db.Model(&models.NotificationDelivery{}).Where("notification_id = ANY(?) AND state IN ?", ids, []string{NotificationDeliveryPending, NotificationDeliveryWaiting}).Select("state", "waiting_tag", "lease_owner", "lease_token", "lease_expires_at").Updates(&models.NotificationDelivery{State: NotificationDeliverySkipped}).Error; err != nil {
		return nil, err
	}
	return changed, nil
}

func upsertNotificationRetirementCutoffs(db *gorm.DB, scope string, recipients []NotificationRecipientType, cutoff models.NotificationVisibilitySnapshot) error {
	if len(recipients) == 0 {
		return nil
	}
	users, types := notificationRecipientArrays(recipients)
	table := models.NotificationRetirementCutoff{}.TableName()
	return db.Exec("INSERT INTO "+table+
		" (user_id,type,activity_scope,cutoff_epoch,cutoff_system_identifier,cutoff_snapshot,cutoff_upper_bound,cutoff_own_xid,cutoff_own_ordinal)"+
		" SELECT recipient.user_id,recipient.type,?,?,?,?,?::xid8,?::xid8,?::bigint FROM unnest(?::bigint[],?::bigint[]) AS recipient(user_id,type)"+
		" ON CONFLICT (user_id,type,activity_scope) DO UPDATE SET cutoff_epoch=excluded.cutoff_epoch,cutoff_system_identifier=excluded.cutoff_system_identifier,cutoff_snapshot=excluded.cutoff_snapshot,cutoff_upper_bound=excluded.cutoff_upper_bound,cutoff_own_xid=excluded.cutoff_own_xid,cutoff_own_ordinal=excluded.cutoff_own_ordinal WHERE "+snapshotAtLeastSQL("excluded.", table+"."), scope, cutoff.Epoch, cutoff.SystemIdentifier, cutoff.Snapshot, cutoff.UpperBound, cutoff.OwnXID, cutoff.OwnOrdinal, users, types).Error
}

func enqueueNotificationRecipientRetirements(db *gorm.DB, scope string, recipients []NotificationRecipientType, cutoff models.NotificationVisibilitySnapshot, afterID uint64) error {
	if len(recipients) == 0 {
		return nil
	}
	users, types := notificationRecipientArrays(recipients)
	table := models.NotificationRecipientRetirement{}.TableName()
	return db.Exec("INSERT INTO "+table+
		" (activity_scope,user_id,type,cutoff_epoch,cutoff_system_identifier,cutoff_snapshot,cutoff_upper_bound,cutoff_own_xid,cutoff_own_ordinal,after_notification_id,due_at,created_at,updated_at)"+
		" SELECT ?,r.user_id,r.type,?,?,?,?::xid8,?::xid8,?::bigint,?,now(),now(),now() FROM unnest(?::bigint[],?::bigint[]) r(user_id,type)"+
		" ON CONFLICT (activity_scope,user_id,type) DO UPDATE SET cutoff_epoch=excluded.cutoff_epoch,cutoff_system_identifier=excluded.cutoff_system_identifier,cutoff_snapshot=excluded.cutoff_snapshot,cutoff_upper_bound=excluded.cutoff_upper_bound,cutoff_own_xid=excluded.cutoff_own_xid,cutoff_own_ordinal=excluded.cutoff_own_ordinal,after_notification_id=excluded.after_notification_id,attempts=0,last_error='',exhausted_at=NULL,due_at=now(),updated_at=now() WHERE "+snapshotAtLeastSQL("excluded.", table+"."), scope, cutoff.Epoch, cutoff.SystemIdentifier, cutoff.Snapshot, cutoff.UpperBound, cutoff.OwnXID, cutoff.OwnOrdinal, afterID, users, types).Error
}
