package common_persistence_models

import (
	"time"
)

// EmailVerification defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type EmailVerification struct {
	ID         uint64     `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID     uint64     `json:"user_id" gorm:"uniqueIndex;not null"`
	Email      string     `json:"email" gorm:"index;type:varchar(255);not null"`
	VerifiedAt *time.Time `json:"verified_at,omitempty" gorm:"type:timestamptz"`

	// Verification tracking
	LastVerificationSent time.Time `json:"last_verification_sent" gorm:"type:timestamptz;autoCreateTime;not null"`
	VerificationCount    uint      `json:"verification_count" gorm:"default:0;not null"`

	// Timestamps
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// Relations
	User User `json:"-" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (EmailVerification) TableName() string {
	return TableNameEmailVerifications
}
