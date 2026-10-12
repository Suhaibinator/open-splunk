package common_persistence_models

import "time"

// Notification is a recipient's inbox entry. Revision/read revision are independent
// of resolution: seeing an approval never executes or resolves the tool.
type Notification struct {
	OrganizationID         int64  `gorm:"not null;default:0"`
	ID                     uint64 `gorm:"primaryKey;autoIncrement"`
	UserID                 uint64 `gorm:"not null;index:idx_notification_inbox,priority:1"`
	Type                   uint32 `gorm:"not null"`
	ReusableGroup          bool   `gorm:"not null;default:false"`
	UnreadEpisodeRevision  uint64 `gorm:"not null;default:0"`
	UnreadEpisodeStartedAt time.Time
	GroupKey               string `gorm:"type:text;not null;index:idx_notification_group"`
	Payload                []byte `gorm:"type:bytea"`
	FallbackText           string `gorm:"type:text;not null"`
	ActivityScope          string `gorm:"type:text;not null"`
	Revision               uint64 `gorm:"not null"`
	OccurrenceCount        uint64 `gorm:"not null;default:0"`
	// DeliveryRearmRevision records the latest materialized occurrence that
	// was eligible for email under its sampled policy and resource preferences.
	// Delivery rechecks current eligibility before sending.
	DeliveryRearmRevision uint64 `gorm:"not null;default:0"`
	ReadRevision          uint64 `gorm:"not null;default:0"`
	Resolved              bool   `gorm:"not null;default:false"`
	AlertClaimedRevision  uint64 `gorm:"not null;default:0"`
	AlertClaimedAt        *time.Time
	CreatedAt             time.Time `gorm:"not null;index:idx_notification_inbox,priority:2,sort:desc"`
	UpdatedAt             time.Time `gorm:"not null"`
	// SourceTime drives alert freshness; UpdatedAt remains materialization time.
	SourceTime time.Time
	// MaterializationStamp identifies the transaction that last advanced this
	// entry. Retirement pages compare it to their captured visibility snapshot,
	// preserving newer activity even while an older pair is partially settled.
	MaterializationStamp NotificationTransactionStamp `gorm:"embedded;embeddedPrefix:materialization_"`
}

func (Notification) TableName() string { return "gocommon_notifications" }

// UnreadOccurrenceFloor is the exclusive lower revision bound for occurrences
// that may interrupt the current unread episode. Resolution does not mark read.
func (n Notification) UnreadOccurrenceFloor() uint64 {
	floor := n.ReadRevision
	if n.UnreadEpisodeRevision > 0 {
		floor = max(floor, n.UnreadEpisodeRevision-1)
	}
	return floor
}

// NotificationOccurrence makes producer retries idempotent across read episodes.
type NotificationOccurrence struct {
	OrganizationID int64  `gorm:"primaryKey;autoIncrement:false;not null;default:0"`
	UserID         uint64 `gorm:"primaryKey;autoIncrement:false"`
	Type           uint32 `gorm:"primaryKey;autoIncrement:false"`
	SourceEventKey string `gorm:"primaryKey;type:varchar(256)"`
	NotificationID uint64 `gorm:"not null;index:idx_notification_occurrence_tagged_delivery,priority:1,where:delivery_tag <> '' AND disposition = 'materialized'"`
	Revision       uint64 `gorm:"not null;index:idx_notification_occurrence_tagged_delivery,priority:3"`
	Disposition    string `gorm:"type:varchar(16);not null;default:'materialized'"`
	DeliveryTag    string `gorm:"type:varchar(64);not null;default:'';index:idx_notification_occurrence_tagged_delivery,priority:2"`
	SourceTime     time.Time
	MaterializedAt time.Time
}

func (NotificationOccurrence) TableName() string { return "gocommon_notification_occurrences" }

// NotificationDelivery is a durable email outbox. A lease protects each attempt;
// it does not claim exactly-once delivery at an external provider.
type NotificationDelivery struct {
	ID                    uint64    `gorm:"primaryKey;autoIncrement"`
	NotificationID        uint64    `gorm:"not null;uniqueIndex"`
	UnreadEpisodeRevision uint64    `gorm:"not null;default:0"`
	UserID                uint64    `gorm:"not null;index"`
	DueAt                 time.Time `gorm:"not null;index:idx_notification_due,priority:2"`
	State                 string    `gorm:"type:varchar(16);not null;index:idx_notification_due,priority:1"`
	Attempts              uint32    `gorm:"not null;default:0"`
	LeaseOwner            string    `gorm:"type:text;not null;default:''"`
	LeaseToken            string    `gorm:"type:varchar(36);not null;default:''"`
	LeaseExpiresAt        *time.Time
	// WaitingTag is retained while awaiting the first qualifying unread occurrence.
	// WaitingRevision is completion-only: the inbox revision observed by a worker
	// that found no qualifying occurrence. It is never persisted.
	WaitingRevision uint64 `gorm:"-"`
	WaitingTag      string `gorm:"type:varchar(64);not null;default:''"`
	LastError       string `gorm:"type:text;not null;default:''"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (NotificationDelivery) TableName() string { return "gocommon_notification_deliveries" }

// Type 0 stores the global in-app alert switch. Push preferences are unrelated.
type NotificationPreference struct {
	UserID        uint64 `gorm:"primaryKey;autoIncrement:false"`
	Type          uint32 `gorm:"primaryKey;autoIncrement:false"`
	AlertsEnabled *bool
	EmailEnabled  *bool
}

func (NotificationPreference) TableName() string { return "gocommon_notification_preferences" }
