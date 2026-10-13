package common_service_models

import (
	"time"
)

// UserAuthMethods defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserAuthMethods struct {
	UserID         uint64       `json:"user_id"`
	HasPassword    bool         `json:"has_password"`
	OAuthProviders []OAuthLogin `json:"oauth_providers"`
	OTPEnabled     bool         `json:"otp_enabled"`
	OTPVerifiedAt  *time.Time   `json:"otp_verified_at"`
}

// HasAuthMethod evaluates whether the requested capability or state exists for *UserAuthMethods in the common_service_models backend flow.
func (m *UserAuthMethods) HasAuthMethod() bool {
	if m == nil {
		return false
	}
	return m.HasPassword || len(m.OAuthProviders) > 0
}

// CountAuthMethods executes its part of the service logic for *UserAuthMethods in the common_service_models backend flow.
func (m *UserAuthMethods) CountAuthMethods() int {
	if m == nil {
		return 0
	}
	count := 0
	if m.HasPassword {
		count++
	}
	count += len(m.OAuthProviders)
	return count
}

// AuthMethodType represents the type of authentication method for business logic
type AuthMethodType string

const (
	// AuthMethodTypePassword represents password-based authentication
	AuthMethodTypePassword AuthMethodType = "password"
	// AuthMethodTypeOAuth represents OAuth-based authentication (any provider)
	AuthMethodTypeOAuth AuthMethodType = "oauth"
)
