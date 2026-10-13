package common_persistence_models

import (
	"time"
)

// PhoneVerification defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type PhoneVerification struct {
	ID          uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID      uint64 `json:"user_id" gorm:"index;not null"`
	PhoneNumber string `json:"phone_number" gorm:"index;type:varchar(20);not null"`

	// OTP code and expiration
	OTPCode      string    `json:"otp_code" gorm:"type:varchar(6);not null"`
	OTPExpiresAt time.Time `json:"otp_expires_at" gorm:"type:timestamptz;not null"`

	// Verification status
	IsVerified bool       `json:"is_verified" gorm:"default:false;not null;index"`
	VerifiedAt *time.Time `json:"verified_at,omitempty" gorm:"type:timestamptz"`

	// Verification tracking for rate limiting and cooldowns
	LastVerificationSent time.Time `json:"last_verification_sent" gorm:"type:timestamptz;autoCreateTime;not null"`
	VerificationCount    uint      `json:"verification_count" gorm:"default:0;not null"`

	// Timestamps
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// Relations
	User User `json:"-" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (PhoneVerification) TableName() string {
	return TableNamePhoneVerifications
}

// MarkVerified executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (p *PhoneVerification) MarkVerified() {
	if p == nil {
		return
	}
	p.IsVerified = true
	now := time.Now().UTC()
	p.VerifiedAt = &now
}

// IncrementVerificationCount executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (p *PhoneVerification) IncrementVerificationCount() {
	if p == nil {
		return
	}
	p.VerificationCount++
	p.LastVerificationSent = time.Now().UTC()
}

// CanSendVerificationAfter reports whether the duration since the last send
// exceeds cooldown, at full duration precision.
func (p *PhoneVerification) CanSendVerificationAfter(cooldown time.Duration) bool {
	if p == nil {
		return false
	}
	return time.Since(p.LastVerificationSent) > cooldown
}

// IsOTPExpired executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (p *PhoneVerification) IsOTPExpired() bool {
	if p == nil {
		return true
	}
	return time.Now().UTC().After(p.OTPExpiresAt)
}

// SetOTPCodeWithValidity stores an OTP code and sets its expiry using the full
// duration precision supplied by the caller.
func (p *PhoneVerification) SetOTPCodeWithValidity(code string, validity time.Duration) {
	if p == nil {
		return
	}
	p.OTPCode = code
	p.OTPExpiresAt = time.Now().UTC().Add(validity)
}
