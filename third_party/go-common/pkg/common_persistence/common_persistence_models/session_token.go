package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SessionToken defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type SessionToken struct {
	// ID is the unique identifier for the token
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// UserID identifies the user this token belongs to
	UserID uint64 `json:"user_id" gorm:"index;not null"`

	// User is the foreign key relationship to the User table
	User User `json:"-" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// Token is the unique token string (securely generated)
	Token string `json:"token" gorm:"uniqueIndex;type:varchar(255);not null"`

	// TokenType specifies the purpose: AUTHENTICATION, EMAIL_VERIFICATION, PASSWORD_RESET
	TokenType proto_models.TokenType `json:"token_type" gorm:"index;not null"`

	// AuthMethod indicates how the token was created: EMAIL, PHONE, or OAUTH.
	AuthMethod proto_models.AuthProvider `json:"auth_method" gorm:"index;not null"`

	// OAuthProviderSlug names the provider of an AUTH_PROVIDER_OAUTH session;
	// it is empty for every other method.
	OAuthProviderSlug string `json:"oauth_provider_slug,omitempty" gorm:"column:oauth_provider_slug;type:varchar(64);not null;default:''"`

	// Client Information (for security auditing)

	// IPAddress of the client that created this token (supports IPv6)
	IPAddress string `json:"ip_address,omitempty" gorm:"type:varchar(45)"`

	// UserAgent string from the client's browser/app
	UserAgent string `json:"user_agent,omitempty" gorm:"type:text"`

	// DeviceID for optional device fingerprinting
	DeviceID string `json:"device_id,omitempty" gorm:"type:varchar(255);index"`

	// BoundEmail binds account invitations and email-verification links to the normalized
	// address present at issuance; it is not exposed in public token models.
	BoundEmail string `json:"-" gorm:"type:varchar(320)"`

	// FrontendOrigin is the validated frontend origin this token's email link targets
	// (empty means the default FrontendURL)
	FrontendOrigin string `json:"frontend_origin,omitempty" gorm:"type:varchar(255)"`

	// Token Lifecycle Timestamps

	// IssuedAt is when the token was created
	IssuedAt time.Time `json:"issued_at" gorm:"type:timestamptz;autoCreateTime;not null"`

	// ExpiresAt is when the token becomes invalid
	ExpiresAt time.Time `json:"expires_at" gorm:"type:timestamptz;index;not null"`

	// UsedAt tracks when a single-use token was consumed
	UsedAt *time.Time `json:"used_at,omitempty" gorm:"type:timestamptz;index"`

	// RevokedAt tracks manual revocation (e.g., logout)
	RevokedAt *time.Time `json:"revoked_at,omitempty" gorm:"type:timestamptz;index"`

	// RecordLogin is an issuance-only instruction. Fresh authentication sets it
	// so token insertion and the user's last-login timestamp commit atomically;
	// refresh and security-token paths leave it false. It is never persisted.
	RecordLogin bool `json:"-" gorm:"-"`

	// Composite index for efficient queries by user and token type
	_ struct{} `gorm:"index:idx_user_type_valid,expression:user_id,token_type,COALESCE(revoked_at, '1970-01-01')"`
}

// IsValid executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
//
// RevokedAt is the instant the token stops being accepted (like ExpiresAt): a
// future RevokedAt is a pending revocation and the token is still valid until
// then. Sliding-session refresh uses this to keep a just-replaced token working
// briefly while clients pick up the new cookie; explicit logout revokes at now
// and takes effect immediately.
func (t *SessionToken) IsValid() bool {
	now := time.Now().UTC()
	return (t.RevokedAt == nil || t.RevokedAt.After(now)) &&
		now.Before(t.ExpiresAt) &&
		(t.TokenType == proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION || t.UsedAt == nil)
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (SessionToken) TableName() string {
	return TableNameSessionTokens
}
