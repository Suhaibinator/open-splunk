package common_persistence

import (
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type notificationRecordGroup struct {
	organization int64
	user         uint64
	typ          uint32
	key          string
	reusable     bool
}
type notificationRecordOccurrence struct {
	organization int64
	user         uint64
	typ          uint32
	key          string
}
type notificationRecordState struct {
	stamp       models.NotificationTransactionStamp
	groups      map[notificationRecordGroup]*models.Notification
	occurrences map[notificationRecordOccurrence]models.NotificationOccurrence
	entries     map[uint64]*models.Notification
}

func recordGroupKey(in NotificationRecordInput) notificationRecordGroup {
	return notificationRecordGroup{in.OrganizationID, in.UserID, in.Type, in.GroupKey, in.ReusableGroup}
}
func recordOccurrenceKey(in NotificationRecordInput) notificationRecordOccurrence {
	return notificationRecordOccurrence{in.OrganizationID, in.UserID, in.Type, in.SourceEventKey}
}
func cloneRecordedNotification(n *models.Notification) *models.Notification {
	if n == nil {
		return nil
	}
	copy := *n
	return &copy
}

// advanceRecordedNotification is the common state transition for independent
// set writes and dependent positional inputs. Serialization and deduplication
// are established by the caller before this pure transition.
func advanceRecordedNotification(input NotificationRecordInput, entry models.Notification) (models.Notification, bool, bool) {
	newEpisode := entry.ID == 0 || entry.Resolved || entry.ReadRevision >= entry.Revision
	if entry.ID == 0 {
		entry = models.Notification{OrganizationID: input.OrganizationID, UserID: input.UserID, Type: input.Type, GroupKey: input.GroupKey, ReusableGroup: input.ReusableGroup, CreatedAt: input.Now}
	}
	entry.Revision++
	qualifies := input.EmailDueAt != nil && !input.DeliverySuppressed && (input.DeliveryFilter == "" || input.DeliveryFilter == input.DeliveryTag)
	if qualifies {
		entry.DeliveryRearmRevision = entry.Revision
	}
	if newEpisode {
		entry.UnreadEpisodeRevision = entry.Revision
		entry.UnreadEpisodeStartedAt = input.Now
		entry.Resolved = false
	}
	entry.OccurrenceCount++
	entry.Payload, entry.FallbackText, entry.ActivityScope = input.Payload, input.FallbackText, input.ActivityScope
	entry.UpdatedAt, entry.SourceTime = input.Now, input.SourceTime
	if entry.SourceTime.IsZero() {
		entry.SourceTime = input.Now
	}
	return entry, newEpisode, qualifies
}

// Prefetch only after canonical locks and fresh recipient checks. Subsequent
// mutations refresh these maps, so repeated inputs observe their predecessors.
func prefetchNotificationRecordState(db *gorm.DB, inputs []NotificationRecordInput, eligible []bool, fences []*models.NotificationGroupState, locked []models.Notification) (*notificationRecordState, error) {
	state := &notificationRecordState{groups: make(map[notificationRecordGroup]*models.Notification), occurrences: make(map[notificationRecordOccurrence]models.NotificationOccurrence), entries: make(map[uint64]*models.Notification)}
	for i := range locked {
		n := &locked[i]
		state.entries[n.ID] = n
		state.groups[notificationRecordGroup{n.OrganizationID, n.UserID, n.Type, n.GroupKey, n.ReusableGroup}] = n
	}
	seen := make(map[notificationRecordOccurrence]bool)
	keys := make([][]any, 0, len(inputs))
	ids := make(map[uint64]bool)
	for i, in := range inputs {
		if !eligible[i] {
			continue
		}
		key := recordOccurrenceKey(in)
		if !seen[key] {
			keys = append(keys, []any{in.OrganizationID, in.UserID, in.Type, in.SourceEventKey})
			seen[key] = true
		}
		if fences[i] != nil {
			ids[fences[i].NotificationID] = true
		}
	}
	if len(keys) != 0 {
		var occurrences []models.NotificationOccurrence
		if err := db.Where("(organization_id,user_id,type,source_event_key) IN ?", keys).Find(&occurrences).Error; err != nil {
			return nil, err
		}
		for _, occurrence := range occurrences {
			state.occurrences[notificationRecordOccurrence{occurrence.OrganizationID, occurrence.UserID, occurrence.Type, occurrence.SourceEventKey}] = occurrence
			ids[occurrence.NotificationID] = true
		}
	}
	missing := make([]uint64, 0, len(ids))
	for id := range ids {
		if state.entries[id] == nil {
			missing = append(missing, id)
		}
	}
	if len(missing) != 0 {
		var entries []models.Notification
		if err := db.Where("id IN ?", missing).Find(&entries).Error; err != nil {
			return nil, err
		}
		for i := range entries {
			state.entries[entries[i].ID] = &entries[i]
		}
	}
	return state, nil
}

// Independent first occurrences share three writes irrespective of audience
// size. Dependent groups and repeated occurrences use the same prefetched state
// and the ordinary transition logic, retaining all episode/rearm semantics.
func recordIndependentNotifications(db *gorm.DB, inputs []NotificationRecordInput, eligible []bool, fences []*models.NotificationGroupState, state *notificationRecordState, results []NotificationRecordResult) ([]bool, error) {
	occurrenceCounts := make(map[notificationRecordOccurrence]int)
	groupCounts := make(map[notificationRecordGroup]int)
	for _, in := range inputs {
		occurrenceCounts[recordOccurrenceKey(in)]++
		if in.GroupKey != "" {
			groupCounts[recordGroupKey(in)]++
		}
	}
	fast := make([]bool, len(inputs))
	entries := make([]models.Notification, 0, len(inputs))
	indices := make([]int, 0, len(inputs))
	for i, in := range inputs {
		_, exists := state.occurrences[recordOccurrenceKey(in)]
		if !eligible[i] || fences[i] != nil || exists || occurrenceCounts[recordOccurrenceKey(in)] != 1 || (in.GroupKey != "" && groupCounts[recordGroupKey(in)] != 1) {
			continue
		}
		if in.Now.IsZero() {
			in.Now = time.Now().UTC()
			inputs[i].Now = in.Now
		}
		entry := models.Notification{}
		if existing := state.groups[recordGroupKey(in)]; in.GroupKey != "" && existing != nil {
			entry = *existing
		}
		entry, _, _ = advanceRecordedNotification(in, entry)
		entry.MaterializationStamp = state.stamp
		entries = append(entries, entry)
		indices = append(indices, i)
		fast[i] = true
	}
	if len(entries) == 0 {
		return fast, nil
	}
	if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, UpdateAll: true}).Create(&entries).Error; err != nil {
		return nil, err
	}
	occurrences := make([]models.NotificationOccurrence, len(entries))
	deliveries := make([]models.NotificationDelivery, 0, len(entries))
	resets := make([]models.NotificationDelivery, 0, len(entries))
	rearms := make([]models.NotificationDelivery, 0, len(entries))
	disabledIDs := make([]uint64, 0, len(entries))
	for j, index := range indices {
		in, n := inputs[index], &entries[j]
		occurrences[j] = models.NotificationOccurrence{OrganizationID: in.OrganizationID, UserID: in.UserID, Type: in.Type, SourceEventKey: in.SourceEventKey, NotificationID: n.ID, Revision: n.Revision, Disposition: NotificationOccurrenceMaterialized, DeliveryTag: in.DeliveryTag, SourceTime: n.SourceTime, MaterializedAt: in.Now}
		state.entries[n.ID] = n
		state.occurrences[recordOccurrenceKey(in)] = occurrences[j]
		if in.GroupKey != "" {
			state.groups[recordGroupKey(in)] = n
		}
		results[index] = NotificationRecordResult{Entry: cloneRecordedNotification(n), Changed: true}
		newEpisode := n.UnreadEpisodeRevision == n.Revision
		if newEpisode && in.ReusableGroup && in.EmailDueAt == nil {
			disabledIDs = append(disabledIDs, n.ID)
		}
		if in.EmailDueAt != nil {
			due := *in.EmailDueAt
			if in.ReusableGroup {
				due = n.UnreadEpisodeStartedAt.Add(in.EmailDueAt.Sub(in.Now))
			}
			delivery := models.NotificationDelivery{NotificationID: n.ID, UnreadEpisodeRevision: n.UnreadEpisodeRevision, UserID: in.UserID, DueAt: due, State: NotificationDeliveryPending, UpdatedAt: in.Now}
			if newEpisode && in.ReusableGroup {
				resets = append(resets, delivery)
			} else if !newEpisode && n.DeliveryRearmRevision == n.Revision {
				if in.DeliveryFilter != "" {
					delivery.DueAt = *in.EmailDueAt
				}
				rearms = append(rearms, delivery)
			} else {
				deliveries = append(deliveries, delivery)
			}
		}
	}
	if err := db.Create(&occurrences).Error; err != nil {
		return nil, err
	}
	if len(deliveries) != 0 {
		if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "notification_id"}}, DoNothing: true}).Create(&deliveries).Error; err != nil {
			return nil, err
		}
	}
	if len(resets) != 0 {
		if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "notification_id"}}, DoUpdates: clause.AssignmentColumns([]string{"unread_episode_revision", "due_at", "state", "attempts", "lease_owner", "lease_token", "lease_expires_at", "last_error", "waiting_tag", "updated_at"})}).Create(&resets).Error; err != nil {
			return nil, err
		}
	}
	if len(rearms) != 0 {
		conflict := clause.OnConflict{Columns: []clause.Column{{Name: "notification_id"}}, DoUpdates: clause.AssignmentColumns([]string{"state", "due_at", "waiting_tag", "updated_at"}), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "gocommon_notification_deliveries.state IN ? AND gocommon_notification_deliveries.unread_episode_revision = excluded.unread_episode_revision", Vars: []any{[]string{NotificationDeliveryWaiting, NotificationDeliverySuppressed, NotificationDeliveryDisabled}}}}}}
		if err := db.Clauses(conflict).Create(&rearms).Error; err != nil {
			return nil, err
		}
	}
	if len(disabledIDs) != 0 {
		if err := db.Exec("UPDATE gocommon_notification_deliveries AS delivery SET unread_episode_revision = entry.unread_episode_revision, state = ?, attempts = 0, lease_owner = '', lease_token = '', lease_expires_at = NULL, last_error = '', waiting_tag = '', updated_at = entry.updated_at FROM gocommon_notifications AS entry WHERE delivery.notification_id = entry.id AND entry.id IN ?", NotificationDeliveryDisabled, disabledIDs).Error; err != nil {
			return nil, err
		}
	}
	return fast, nil
}
