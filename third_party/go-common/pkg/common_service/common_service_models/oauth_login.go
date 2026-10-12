package common_service_models

import (
	"time"
)

// OAuthLogin defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OAuthLogin struct {
	ID     uint64 `json:"id"`
	UserID uint64 `json:"user_id"`
	// ProviderSlug is the linked provider's persisted slug.
	ProviderSlug string `json:"provider_slug"`
	Email        string `json:"email"`

	// Analytics
	LoginCount uint      `json:"login_count"`
	FirstLogin time.Time `json:"first_login"`
	LastLogin  time.Time `json:"last_login"`
}

// GetEmail returns the requested value for *OAuthLogin in the common_service_models backend flow.
func (o *OAuthLogin) GetEmail() string {
	if o == nil {
		return ""
	}
	return o.Email
}
