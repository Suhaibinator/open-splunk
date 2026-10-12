package config

import (
	"fmt"
	"time"
)

// OAuth protocol settings. Provider identity (client IDs, secrets, endpoints)
// belongs to the importing application; these bound go-common's protocol work.
const (
	DefaultOAuthHTTPTimeout = 10 * time.Second
	MinOAuthHTTPTimeout     = time.Second
	MaxOAuthHTTPTimeout     = time.Minute

	DefaultOAuthStateTTL = 10 * time.Minute
	MinOAuthStateTTL     = time.Minute
	MaxOAuthStateTTL     = 30 * time.Minute

	DefaultOAuthStateClockSkew = 30 * time.Second
	MaxOAuthStateClockSkew     = 5 * time.Minute

	DefaultOAuthDiscoveryRefreshInterval = 24 * time.Hour
	MinOAuthDiscoveryRefreshInterval     = time.Hour
	MaxOAuthDiscoveryRefreshInterval     = 7 * 24 * time.Hour

	DefaultOAuthSignedClientSecretLifetime = 5 * time.Minute
	MinOAuthSignedClientSecretLifetime     = time.Minute
	MaxOAuthSignedClientSecretLifetime     = time.Hour

	DefaultOAuthMaxResponseBytes = 1 << 20
	MinOAuthMaxResponseBytes     = 16 << 10
	MaxOAuthMaxResponseBytes     = 8 << 20

	DefaultOAuthMaxStateLength = 4096
	MinOAuthMaxStateLength     = 1024
	MaxOAuthMaxStateLength     = 16384

	DefaultOAuthMaxCodeLength = 2048
	MinOAuthMaxCodeLength     = 256
	MaxOAuthMaxCodeLength     = 8192

	DefaultOAuthFormPostMaxBodyBytes = 16 << 10
	MinOAuthFormPostMaxBodyBytes     = 8 << 10
	MaxOAuthFormPostMaxBodyBytes     = 64 << 10
)

func (c *Config) validateOAuthProtocol() error {
	if err := requiredDurations([]struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"oauth_http_timeout", c.OAuthHTTPTimeout, MinOAuthHTTPTimeout, MaxOAuthHTTPTimeout},
		{"oauth_state_ttl", c.OAuthStateTTL, MinOAuthStateTTL, MaxOAuthStateTTL},
		{"oauth_state_clock_skew", c.OAuthStateClockSkew, 0, MaxOAuthStateClockSkew},
		{"oauth_discovery_refresh_interval", c.OAuthDiscoveryRefreshInterval, MinOAuthDiscoveryRefreshInterval, MaxOAuthDiscoveryRefreshInterval},
		{"oauth_signed_client_secret_lifetime", c.OAuthSignedClientSecretLifetime, MinOAuthSignedClientSecretLifetime, MaxOAuthSignedClientSecretLifetime},
	}); err != nil {
		return err
	}
	if c.OAuthMaxResponseBytes == nil {
		return fmt.Errorf("oauth_max_response_bytes is required")
	}
	if v := *c.OAuthMaxResponseBytes; v < MinOAuthMaxResponseBytes || v > MaxOAuthMaxResponseBytes {
		return fmt.Errorf("oauth_max_response_bytes must be between %d and %d, got: %d", MinOAuthMaxResponseBytes, MaxOAuthMaxResponseBytes, v)
	}
	if c.OAuthMaxStateLength == nil {
		return fmt.Errorf("oauth_max_state_length is required")
	}
	if v := *c.OAuthMaxStateLength; v < MinOAuthMaxStateLength || v > MaxOAuthMaxStateLength {
		return fmt.Errorf("oauth_max_state_length must be between %d and %d, got: %d", MinOAuthMaxStateLength, MaxOAuthMaxStateLength, v)
	}
	if c.OAuthMaxCodeLength == nil {
		return fmt.Errorf("oauth_max_code_length is required")
	}
	if v := *c.OAuthMaxCodeLength; v < MinOAuthMaxCodeLength || v > MaxOAuthMaxCodeLength {
		return fmt.Errorf("oauth_max_code_length must be between %d and %d, got: %d", MinOAuthMaxCodeLength, MaxOAuthMaxCodeLength, v)
	}
	if c.OAuthFormPostMaxBodyBytes == nil {
		return fmt.Errorf("oauth_form_post_max_body_bytes is required")
	}
	if v := *c.OAuthFormPostMaxBodyBytes; v < MinOAuthFormPostMaxBodyBytes || v > MaxOAuthFormPostMaxBodyBytes {
		return fmt.Errorf("oauth_form_post_max_body_bytes must be between %d and %d, got: %d", MinOAuthFormPostMaxBodyBytes, MaxOAuthFormPostMaxBodyBytes, v)
	}
	// The relayed body carries the state next to the code, so a body cap at or
	// below the state cap would reject every valid form_post.
	if *c.OAuthFormPostMaxBodyBytes <= int64(*c.OAuthMaxStateLength) {
		return fmt.Errorf("oauth_form_post_max_body_bytes (%d) must exceed oauth_max_state_length (%d)", *c.OAuthFormPostMaxBodyBytes, *c.OAuthMaxStateLength)
	}
	return nil
}
