package common_persistence_models

import "time"

// NotificationEmission is one immutable, transactionally accepted source event.
// Recipient work and payloads may be compacted; identity and fingerprint remain.
// EmissionKey separates successive emissions of one source (for example an
// invitation bound to a new account); occurrences and resolution ignore it.
type NotificationEmission struct {
	ID             uint64                         `gorm:"primaryKey;autoIncrement"`
	Producer       string                         `gorm:"type:varchar(128);not null;uniqueIndex:idx_notification_emission_source,priority:4"`
	OrganizationID int64                          `gorm:"not null;uniqueIndex:idx_notification_emission_source,priority:1"`
	Type           uint32                         `gorm:"not null;uniqueIndex:idx_notification_emission_source,priority:2"`
	SourceKey      string                         `gorm:"type:varchar(256);not null;uniqueIndex:idx_notification_emission_source,priority:3"`
	EmissionKey    string                         `gorm:"type:varchar(64);not null;default:'';uniqueIndex:idx_notification_emission_source,priority:5"`
	OperationID    string                         `gorm:"type:varchar(36);not null"`
	Fingerprint    string                         `gorm:"type:varchar(64);not null;default:''"`
	Event          bool                           `gorm:"not null;default:false"`
	Payload        []byte                         `gorm:"type:bytea"`
	FallbackText   string                         `gorm:"type:text;not null"`
	SourceTime     time.Time                      `gorm:"not null"`
	TraceID        string                         `gorm:"type:varchar(128);not null;default:''"`
	SourceStamp    NotificationTransactionStamp   `gorm:"embedded;embeddedPrefix:source_"`
	SourceSnapshot NotificationVisibilitySnapshot `gorm:"embedded;embeddedPrefix:source_snapshot_"`
	State          string                         `gorm:"type:varchar(16);not null;index:idx_notification_emission_due,priority:1"`
	DueAt          time.Time                      `gorm:"not null;default:clock_timestamp();index:idx_notification_emission_due,priority:2"`
	SnapshotReady  bool                           `gorm:"not null;default:false"`
	Cursor         []byte                         `gorm:"type:bytea"`
	AudienceSize   uint64                         `gorm:"not null;default:0"`
	Processed      uint64                         `gorm:"not null;default:0"`
	Accepted       uint64                         `gorm:"not null;default:0"`
	Attempts       uint32                         `gorm:"not null;default:0"`
	LeaseOwner     string                         `gorm:"not null;default:''"`
	LeaseToken     string                         `gorm:"type:varchar(36);not null;default:''"`
	LeaseExpiresAt *time.Time
	LastError      string `gorm:"type:varchar(64);not null;default:''"`
	CompletedAt    *time.Time
	CompactedAt    *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (NotificationEmission) TableName() string { return "gocommon_notification_emissions" }

// NotificationEmissionRecipient stores an audience candidate until its bounded
// materialization step commits. Overrides preserve recipient-specific content.
// DeliveryTag is always recipient-specific, independently of payload overrides.
type NotificationEmissionRecipient struct {
	EmissionID   uint64 `gorm:"primaryKey;autoIncrement:false"`
	UserID       uint64 `gorm:"primaryKey;autoIncrement:false"`
	Override     bool   `gorm:"not null;default:false"`
	Payload      []byte `gorm:"type:bytea"`
	FallbackText string `gorm:"type:text;not null;default:''"`
	Metadata     []byte `gorm:"type:bytea"`
	DeliveryTag  string `gorm:"type:varchar(64);not null;default:''"`
}

func (NotificationEmissionRecipient) TableName() string {
	return "gocommon_notification_emission_recipients"
}
