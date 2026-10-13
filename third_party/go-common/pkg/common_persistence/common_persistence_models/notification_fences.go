package common_persistence_models

// NotificationGroupState retains the last acknowledgment across inbox episodes.
// Every writer holds notification:<user>:<type> before locking this row, then
// locks inbox rows only after all affected group states have been locked.
type NotificationGroupState struct {
	OrganizationID int64                          `gorm:"primaryKey;autoIncrement:false;not null;default:0"`
	UserID         uint64                         `gorm:"primaryKey;autoIncrement:false"`
	Type           uint32                         `gorm:"primaryKey;autoIncrement:false"`
	GroupKey       string                         `gorm:"primaryKey;type:varchar(256)"`
	ReusableGroup  bool                           `gorm:"primaryKey;autoIncrement:false"`
	AckSnapshot    NotificationVisibilitySnapshot `gorm:"embedded;embeddedPrefix:ack_"`
	NotificationID uint64                         `gorm:"not null;default:0"`
	Revision       uint64                         `gorm:"not null;default:0"`
}

func (NotificationGroupState) TableName() string { return "gocommon_notification_group_states" }

// NotificationRetirementCutoff exists even when retirement found no entry.
// Native chat uses its membership generation instead of this generic fence.
type NotificationRetirementCutoff struct {
	UserID        uint64                         `gorm:"primaryKey;autoIncrement:false"`
	Type          uint32                         `gorm:"primaryKey;autoIncrement:false"`
	ActivityScope string                         `gorm:"primaryKey;type:varchar(256)"`
	Cutoff        NotificationVisibilitySnapshot `gorm:"embedded;embeddedPrefix:cutoff_"`
}

func (NotificationRetirementCutoff) TableName() string {
	return "gocommon_notification_retirement_cutoffs"
}
