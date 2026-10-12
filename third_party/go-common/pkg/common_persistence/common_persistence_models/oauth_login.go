package common_persistence_models

import (
	"time"
)

// Column widths of OAuthLogin's provider-asserted values.
const (
	OAuthSubjectMaxLength    = 255
	OAuthLoginEmailMaxLength = 255
)

// OAuthLogin links one user to one provider account, identified by the
// provider's slug and its stable subject. A user has at most one identity per
// provider, and a (provider, subject) pair belongs to at most one user.
type OAuthLogin struct {
	ID     uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID uint64 `json:"user_id" gorm:"uniqueIndex:idx_oauth_logins_user_provider_slug,priority:1;not null"`
	// ProviderSlug is the configured provider's persisted slug.
	ProviderSlug string `json:"provider_slug" gorm:"type:varchar(64);not null;uniqueIndex:idx_oauth_logins_user_provider_slug,priority:2;uniqueIndex:idx_oauth_logins_provider_subject,priority:1,where:subject IS NOT NULL"`
	// Subject is the provider's stable account identifier. It is NULL only for
	// identities linked before subjects were recorded; the next login through
	// the linked email fills it.
	Subject *string `json:"subject" gorm:"type:varchar(255);uniqueIndex:idx_oauth_logins_provider_subject,priority:2"`
	// Email is the address the provider asserted at the most recent login.
	Email string `json:"email" gorm:"index;type:varchar(255);not null"`

	// Analytics
	LoginCount uint      `json:"login_count" gorm:"default:1;not null"`
	FirstLogin time.Time `json:"first_login" gorm:"type:timestamptz;autoCreateTime;not null"`
	LastLogin  time.Time `json:"last_login" gorm:"type:timestamptz;autoUpdateTime;not null"`

	// Relations
	User User `json:"-" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (OAuthLogin) TableName() string {
	return TableNameOAuthLogins
}
