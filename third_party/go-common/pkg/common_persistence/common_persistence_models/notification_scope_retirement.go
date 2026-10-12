package common_persistence_models

import "time"

// NotificationScopeRetirementFence is the permanent scope-wide retirement
// cutoff written in the source transaction of a scope-wide retirement. An
// emission whose source transaction is visible in Cutoff is retired for every
// recipient of the scope; later sources materialize normally. A recipient/type
// whose NotificationRetirementCutoff for the scope is at least as new has
// already had its earlier entries retired only when no recipient continuation
// remains. Touched-entry lazy settlement never writes a completion marker.
type NotificationScopeRetirementFence struct {
	ActivityScope string                         `gorm:"primaryKey;type:varchar(256)"`
	Cutoff        NotificationVisibilitySnapshot `gorm:"embedded;embeddedPrefix:cutoff_"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (NotificationScopeRetirementFence) TableName() string {
	return "gocommon_notification_scope_retirement_fences"
}

// NotificationScopeRetirement is the transient paging job committed beside its
// fence. Each page transaction retires bounded entries after the
// (AfterUserID, AfterType, AfterNotificationID) cursor that still have entries in the scope and no
// cutoff as new as the fence; the final page removes the job. Claims, backoff,
// and exhaustion follow NotificationChatRetirement and share its KMS policy.
type NotificationScopeRetirement struct {
	ActivityScope       string `gorm:"primaryKey;type:varchar(256);index:idx_notification_scope_retirement_due,priority:2"`
	AfterUserID         uint64 `gorm:"not null;default:0"`
	AfterType           uint32 `gorm:"not null;default:0"`
	AfterNotificationID uint64 `gorm:"not null;default:0"`
	Attempts            uint32 `gorm:"not null;default:0"`
	LastError           string `gorm:"type:varchar(64);not null;default:''"`
	ExhaustedAt         *time.Time
	DueAt               time.Time `gorm:"not null;default:now();index:idx_notification_scope_retirement_due,priority:1"`
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (NotificationScopeRetirement) TableName() string {
	return "gocommon_notification_scope_retirements"
}

// NotificationRecipientRetirement retains entry progress for an explicit
// audience's retirement. Its presence means the source cutoff has been written
// but settlement is incomplete. A completed source cutoff alone is never used
// to skip this durable work.
type NotificationRecipientRetirement struct {
	ActivityScope       string                         `gorm:"primaryKey;type:varchar(256)"`
	UserID              uint64                         `gorm:"primaryKey;autoIncrement:false"`
	Type                uint32                         `gorm:"primaryKey;autoIncrement:false"`
	Cutoff              NotificationVisibilitySnapshot `gorm:"embedded;embeddedPrefix:cutoff_"`
	AfterNotificationID uint64                         `gorm:"not null;default:0"`
	Attempts            uint32                         `gorm:"not null;default:0"`
	LastError           string                         `gorm:"type:varchar(64);not null;default:''"`
	ExhaustedAt         *time.Time
	DueAt               time.Time `gorm:"not null;default:now();index:idx_notification_recipient_retirement_due"`
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (NotificationRecipientRetirement) TableName() string {
	return "gocommon_notification_recipient_retirements"
}
