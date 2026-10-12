package common_persistence

import (
	"context"
	"fmt"
	"sort"
	"strings"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	NotificationOccurrenceMaterialized = "materialized"
	NotificationOccurrenceAcknowledged = "acknowledged"
)

// uniqueNotificationRecipients returns recipients sorted by (user, type) with
// duplicates removed, which is the order every recipient lock is taken in. The
// input slice is left untouched.
func uniqueNotificationRecipients(recipients []NotificationRecipientType) []NotificationRecipientType {
	recipients = append([]NotificationRecipientType(nil), recipients...)
	sort.Slice(recipients, func(i, j int) bool {
		if recipients[i].UserID != recipients[j].UserID {
			return recipients[i].UserID < recipients[j].UserID
		}
		return recipients[i].Type < recipients[j].Type
	})
	unique := recipients[:0]
	for i, recipient := range recipients {
		if i > 0 && recipient == recipients[i-1] {
			continue
		}
		unique = append(unique, recipient)
	}
	return unique
}

// notificationRecipientArrays splits recipients into parallel bigint arrays so
// set-based statements bind a constant two parameters for any audience size.
func notificationRecipientArrays(recipients []NotificationRecipientType) (pq.Int64Array, pq.Int64Array) {
	userIDs := make(pq.Int64Array, len(recipients))
	types := make(pq.Int64Array, len(recipients))
	for i, recipient := range recipients {
		userIDs[i], types[i] = int64(recipient.UserID), int64(recipient.Type)
	}
	return userIDs, types
}

func notificationRecipientLockKey(recipient NotificationRecipientType) string {
	return fmt.Sprintf("notification:%d:%d", recipient.UserID, recipient.Type)
}

func lockNotificationRecipients(db *gorm.DB, recipients []NotificationRecipientType) error {
	recipients = uniqueNotificationRecipients(recipients)
	if len(recipients) == 0 {
		return nil
	}
	keys := make(pq.StringArray, len(recipients))
	for i, recipient := range recipients {
		keys[i] = notificationRecipientLockKey(recipient)
	}
	// PostgreSQL evaluates this volatile output expression after the ORDER BY.
	// Keep the numeric recipient/type ordering while acquiring all locks in one
	// statement, rather than paying a network round trip per recipient. One
	// array parameter keeps any audience under the bind-parameter limit.
	return db.Exec("SELECT pg_advisory_xact_lock(hashtextextended(lock_key, 0)) FROM unnest(?::text[]) WITH ORDINALITY AS recipients(lock_key,position) ORDER BY position", keys).Error
}

func tryLockNotificationRecipient(db *gorm.DB, recipient NotificationRecipientType) (bool, error) {
	var locked bool
	err := db.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))", notificationRecipientLockKey(recipient)).Scan(&locked).Error
	return locked, err
}

func notificationGroupIdentity(entry models.Notification) models.NotificationGroupState {
	return models.NotificationGroupState{OrganizationID: entry.OrganizationID, UserID: entry.UserID, Type: entry.Type, GroupKey: entry.GroupKey, ReusableGroup: entry.ReusableGroup}
}

// lockNotificationGroups always precedes notification row locks. Insertion is
// safe because the caller already holds all recipient/type advisory locks.
func lockNotificationGroups(db *gorm.DB, groups []models.NotificationGroupState) error {
	_, err := lockNotificationGroupStates(db, groups)
	return err
}

// lockNotificationGroupStates also returns the locked state for fence checks;
// the recipient/type locks keep it stable through materialization.
func lockNotificationGroupStates(db *gorm.DB, groups []models.NotificationGroupState) ([]models.NotificationGroupState, error) {
	sort.Slice(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
		if a.UserID != b.UserID {
			return a.UserID < b.UserID
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.OrganizationID != b.OrganizationID {
			return a.OrganizationID < b.OrganizationID
		}
		if a.GroupKey != b.GroupKey {
			return a.GroupKey < b.GroupKey
		}
		return !a.ReusableGroup && b.ReusableGroup
	})
	unique := make([]models.NotificationGroupState, 0, len(groups))
	for i, group := range groups {
		if group.GroupKey == "" || (i > 0 && group == groups[i-1]) {
			continue
		}
		unique = append(unique, group)
	}
	if len(unique) == 0 {
		return nil, nil
	}
	organizationIDs := make(pq.Int64Array, len(unique))
	userIDs := make(pq.Int64Array, len(unique))
	types := make(pq.Int64Array, len(unique))
	groupKeys := make(pq.StringArray, len(unique))
	reusable := make(pq.BoolArray, len(unique))
	for i, group := range unique {
		organizationIDs[i], userIDs[i], types[i] = group.OrganizationID, int64(group.UserID), int64(group.Type)
		groupKeys[i], reusable[i] = group.GroupKey, group.ReusableGroup
	}
	// Parallel array parameters bind a constant five values for any number of
	// groups; unset state columns take their schema defaults.
	const keys = "SELECT * FROM unnest(?::bigint[],?::bigint[],?::bigint[],?::text[],?::boolean[])"
	table := models.NotificationGroupState{}.TableName()
	if err := db.Exec("INSERT INTO "+table+" (organization_id,user_id,type,group_key,reusable_group) "+keys+" ON CONFLICT DO NOTHING",
		organizationIDs, userIDs, types, groupKeys, reusable).Error; err != nil {
		return nil, err
	}
	var locked []models.NotificationGroupState
	if err := db.Where("(organization_id,user_id,type,group_key,reusable_group) IN ("+keys+")", organizationIDs, userIDs, types, groupKeys, reusable).
		Order("user_id,type,organization_id,group_key,reusable_group").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&locked).Error; err != nil {
		return nil, err
	}
	return locked, nil
}

// saveNotificationAcknowledgments batches writes after all group/entry locks
// have been acquired. A receipt batch may contain several episodes of one
// nonreusable group; its newest acknowledged entry supplies the target.
func saveNotificationAcknowledgments(db *gorm.DB, entries []models.Notification, snapshot models.NotificationVisibilitySnapshot) error {
	positions := make(map[models.NotificationGroupState]int, len(entries))
	states := make([]models.NotificationGroupState, 0, len(entries))
	for _, entry := range entries {
		if entry.GroupKey == "" {
			continue
		}
		identity := notificationGroupIdentity(entry)
		state := identity
		state.AckSnapshot, state.NotificationID, state.Revision = snapshot, entry.ID, entry.ReadRevision
		if index, ok := positions[identity]; ok {
			if states[index].NotificationID < entry.ID {
				states[index] = state
			}
			continue
		}
		positions[identity] = len(states)
		states = append(states, state)
	}
	if len(states) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "organization_id"}, {Name: "user_id"}, {Name: "type"}, {Name: "group_key"}, {Name: "reusable_group"}},
		DoUpdates: append(clause.AssignmentColumns([]string{"ack_epoch", "ack_system_identifier", "ack_snapshot", "ack_upper_bound", "ack_own_xid", "ack_own_ordinal"}),
			// A valid late receipt still advances the visibility fence, but must
			// not replace a newer episode's occurrence target. Resolution can
			// also have fenced a higher revision than the user's read receipt.
			clause.Assignment{Column: clause.Column{Name: "notification_id"}, Value: gorm.Expr("GREATEST(gocommon_notification_group_states.notification_id, excluded.notification_id)")},
			clause.Assignment{Column: clause.Column{Name: "revision"}, Value: gorm.Expr("CASE WHEN excluded.notification_id > gocommon_notification_group_states.notification_id THEN excluded.revision WHEN excluded.notification_id = gocommon_notification_group_states.notification_id THEN GREATEST(gocommon_notification_group_states.revision, excluded.revision) ELSE gocommon_notification_group_states.revision END")}),
	}).Create(&states).Error
}

// notificationFences checks all visibility comparisons in one statement after
// advisory/group locks and fresh source checks, before inbox rows are touched.
// Group state is reused from its locking read; generic cutoff state is fetched
// once for the batch. Neither can change while the recipient locks are held.
func (s *GormNotificationStore[U]) notificationFences(ctx context.Context, inputs []NotificationRecordInput, eligible []bool, groups []models.NotificationGroupState) ([]*models.NotificationGroupState, []bool, error) {
	fences := make([]*models.NotificationGroupState, len(inputs))
	retired := make([]bool, len(inputs))
	applicable := false
	for i, input := range inputs {
		if eligible[i] && input.SourceStamp.XID != "" && input.SourceStamp.XID != "0" && (input.GroupKey != "" || (!input.SkipRetirementFence && input.ActivityScope != "")) {
			applicable = true
			break
		}
	}
	if !applicable {
		return fences, retired, nil
	}
	groupStates := make(map[models.NotificationGroupState]*models.NotificationGroupState, len(groups))
	for i := range groups {
		key := models.NotificationGroupState{
			OrganizationID: groups[i].OrganizationID,
			UserID:         groups[i].UserID,
			Type:           groups[i].Type,
			GroupKey:       groups[i].GroupKey,
			ReusableGroup:  groups[i].ReusableGroup,
		}
		groupStates[key] = &groups[i]
	}
	type cutoffKey struct {
		userID uint64
		typeID uint32
		scope  string
	}
	cutoffKeys := make([][]any, 0, len(inputs))
	seenCutoffs := make(map[cutoffKey]struct{}, len(inputs))
	for i, input := range inputs {
		if !eligible[i] || input.SourceStamp.XID == "" || input.SourceStamp.XID == "0" || input.SkipRetirementFence || input.ActivityScope == "" {
			continue
		}
		key := cutoffKey{input.UserID, input.Type, input.ActivityScope}
		if _, exists := seenCutoffs[key]; !exists {
			seenCutoffs[key] = struct{}{}
			cutoffKeys = append(cutoffKeys, []any{input.UserID, input.Type, input.ActivityScope})
		}
	}
	db := s.db.DB(ctx)
	cutoffs := make(map[cutoffKey]models.NotificationVisibilitySnapshot, len(cutoffKeys))
	if len(cutoffKeys) > 0 {
		var rows []models.NotificationRetirementCutoff
		if err := db.Where("(user_id,type,activity_scope) IN ?", cutoffKeys).Find(&rows).Error; err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			cutoffs[cutoffKey{row.UserID, row.Type, row.ActivityScope}] = row.Cutoff
		}
	}
	// A scope fence applies immediately, even before its entry pages have
	// finished a recipient/type and written that pair's completion cutoff.
	var scopeCutoffs []models.NotificationScopeRetirementFence
	if scopes := fencedScopes(inputs); len(scopes) > 0 {
		if err := db.Where("activity_scope = ANY(?)", pq.StringArray(scopes)).Find(&scopeCutoffs).Error; err != nil {
			return nil, nil, err
		}
	}
	scopeSnapshots := make(map[string]models.NotificationVisibilitySnapshot, len(scopeCutoffs))
	for _, fence := range scopeCutoffs {
		scopeSnapshots[fence.ActivityScope] = fence.Cutoff
	}
	identity, err := s.CurrentNotificationIdentity(ctx)
	if err != nil {
		return nil, nil, err
	}
	var values strings.Builder
	args := make([]any, 0, len(inputs)*8)
	appendComparison := func(index int, retirement bool, stamp models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) {
		// Foreign/absent restore metadata is a baseline, never a PostgreSQL
		// transaction identity, so it contributes no comparison.
		if !notificationFenceComparable(identity, stamp, snapshot) {
			return
		}
		if values.Len() > 0 {
			values.WriteByte(',')
		}
		values.WriteString("(?::integer,?::boolean,?::xid8,?::bigint,?::pg_snapshot,?::xid8,?::bigint)")
		ownXID := snapshot.OwnXID
		if ownXID == "" {
			ownXID = "0"
		}
		args = append(args, index, retirement, stamp.XID, stamp.Ordinal, snapshot.Snapshot, ownXID, snapshot.OwnOrdinal)
	}
	for i, input := range inputs {
		if !eligible[i] {
			continue
		}
		if !input.SkipRetirementFence && input.ActivityScope != "" {
			appendComparison(i, true, input.SourceStamp, cutoffs[cutoffKey{input.UserID, input.Type, input.ActivityScope}])
			appendComparison(i, true, input.SourceStamp, scopeSnapshots[input.ActivityScope])
		}
		if input.GroupKey != "" {
			group := groupStates[models.NotificationGroupState{OrganizationID: input.OrganizationID, UserID: input.UserID, Type: input.Type, GroupKey: input.GroupKey, ReusableGroup: input.ReusableGroup}]
			if group != nil && group.NotificationID != 0 && group.Revision != 0 {
				fences[i] = group
				appendComparison(i, false, input.SourceStamp, group.AckSnapshot)
			}
		}
	}
	type comparison struct {
		Index      int
		Retirement bool
		Visible    bool
	}
	var comparisons []comparison
	if len(args) > 0 {
		if err := db.Raw("SELECT position AS index, retirement, CASE WHEN source_xid = own_xid AND own_ordinal > 0 THEN source_ordinal < own_ordinal ELSE pg_visible_in_snapshot(source_xid, snapshot) END AS visible FROM (VALUES "+values.String()+") AS fences(position,retirement,source_xid,source_ordinal,snapshot,own_xid,own_ordinal)", args...).Scan(&comparisons).Error; err != nil {
			return nil, nil, err
		}
	}
	acknowledged := make([]bool, len(inputs))
	for _, result := range comparisons {
		if result.Retirement {
			retired[result.Index] = retired[result.Index] || result.Visible
		} else {
			acknowledged[result.Index] = result.Visible
		}
	}
	for i := range fences {
		if !acknowledged[i] || retired[i] {
			fences[i] = nil
		}
	}
	return fences, retired, nil
}
