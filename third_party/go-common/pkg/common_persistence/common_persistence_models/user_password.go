package common_persistence_models

import (
	"time"

	"gorm.io/gorm"
)

// UserPassword defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UserPassword struct {
	gorm.Model
	// UserID is unique among live rows only, so a soft-deleted password does not
	// block adding a new one.
	UserID       uint64 `gorm:"not null;uniqueIndex:idx_gocommon_user_passwords_user_id,where:deleted_at IS NULL" json:"user_id"`
	User         User   `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"-"`
	PasswordHash string `gorm:"not null" json:"-"`
	PasswordSalt string `gorm:"not null" json:"-"`

	// OTP (TOTP) 2FA fields
	OTPSecret     *string    `gorm:"type:varchar(32)" json:"-"`               // Base32 encoded TOTP secret
	OTPEnabled    bool       `gorm:"default:false" json:"otp_enabled"`        // Whether OTP is enabled for this user
	OTPVerifiedAt *time.Time `gorm:"type:timestamptz" json:"otp_verified_at"` // When OTP was successfully set up
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (UserPassword) TableName() string {
	return TableNameUserPasswords
}
