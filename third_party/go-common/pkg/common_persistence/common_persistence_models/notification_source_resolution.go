package common_persistence_models

import "time"

// NotificationSourceResolution is bounded durable settlement after cancellation
// has committed with the source operation. Its cursor is an immutable inbox ID.
type NotificationSourceResolution struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement"`
	OrganizationID int64  `gorm:"not null;uniqueIndex:idx_notification_resolution_source,priority:1"`
	Type           uint32 `gorm:"not null;uniqueIndex:idx_notification_resolution_source,priority:2"`
	SourceKey      string `gorm:"type:text;not null;uniqueIndex:idx_notification_resolution_source,priority:3"`
	AfterID        uint64 `gorm:"not null;default:0"`
	ThroughID      uint64 `gorm:"not null"`
	Attempts       uint32 `gorm:"not null;default:0"`
	LastError      string `gorm:"type:varchar(64);not null;default:''"`
	ExhaustedAt    *time.Time
	DueAt          time.Time `gorm:"not null;default:now();index"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (NotificationSourceResolution) TableName() string {
	return "gocommon_notification_source_resolutions"
}
