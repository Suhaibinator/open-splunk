package common_persistence_models

import "time"

// UsernameChangeHistory defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UsernameChangeHistory struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	// TargetUserID is the user whose username changed.
	TargetUserID uint64 `gorm:"not null;index"`

	// OldUsername is the previous username value.
	OldUsername string `gorm:"type:varchar(255);not null"`

	// NewUsername is the updated username value.
	NewUsername string `gorm:"type:varchar(255);not null"`

	// ActorUserID is the user that performed the change when applicable.
	ActorUserID *uint64 `gorm:"index"`

	// Reason stores plain-text context for why the change happened (including actor kind).
	Reason string `gorm:"type:text"`

	// CreatedAt is when this audit entry was created.
	CreatedAt time.Time `gorm:"type:timestamptz;index"`

	// TargetUser is the associated target user record when preloaded.
	TargetUser *User `gorm:"foreignKey:TargetUserID;references:ID"`

	// ActorUser is the associated actor user record when preloaded.
	ActorUser *User `gorm:"foreignKey:ActorUserID;references:ID"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (UsernameChangeHistory) TableName() string {
	return TableNameUsernameChangeHistory
}
