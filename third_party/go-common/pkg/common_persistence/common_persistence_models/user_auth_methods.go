package common_persistence_models

import "time"

// UserAuthMethods defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UserAuthMethods struct {
	UserID         uint64       `json:"user_id"`
	HasPassword    bool         `json:"has_password"`
	OAuthProviders []OAuthLogin `json:"oauth_providers"`
	OTPEnabled     bool         `json:"otp_enabled"`
	OTPVerifiedAt  *time.Time   `json:"otp_verified_at"`
}
