package token

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// validateTokenServiceConfig executes this step in the token backend flow.
func validateTokenServiceConfig(config *TokenServiceConfig) error {
	if config.SessionTokenSecret == "" {
		return fmt.Errorf("session token secret is required")
	}

	if config.SessionLifetime == nil {
		return fmt.Errorf("session lifetime provider is required")
	}

	if config.SecurityLifetime == nil {
		return fmt.Errorf("security lifetime provider is required")
	}

	return nil
}

// createSessionTokenRecord executes this step in the token backend flow.
func createSessionTokenRecord(userID uint64, token string, tokenType proto_models.TokenType,
	method SessionAuthMethod, issuedAt, expiresAt time.Time) *common_persistence_models.SessionToken {
	return &common_persistence_models.SessionToken{
		UserID:            userID,
		Token:             token,
		TokenType:         tokenType,
		AuthMethod:        method.Provider,
		OAuthProviderSlug: method.OAuthProviderSlug,
		IssuedAt:          issuedAt,
		ExpiresAt:         expiresAt,
	}
}

// createSessionTokenRecordWithClient executes this step in the token backend flow.
func createSessionTokenRecordWithClient(userID uint64, token string, tokenType proto_models.TokenType,
	method SessionAuthMethod, issuedAt, expiresAt time.Time, clientInfo *ClientInfo) *common_persistence_models.SessionToken {
	sessionToken := createSessionTokenRecord(userID, token, tokenType, method, issuedAt, expiresAt)

	if clientInfo != nil {
		sessionToken.IPAddress = clientInfo.IPAddress
		sessionToken.UserAgent = clientInfo.UserAgent
		sessionToken.DeviceID = clientInfo.DeviceID
	}

	return sessionToken
}

// validateOTPFormat executes this step in the token backend flow.
func validateOTPFormat(provided, stored string) bool {
	// Ensure both codes are exactly 6 digits
	if len(provided) != 6 || len(stored) != 6 {
		return false
	}

	// Validate format - all characters must be digits
	for _, char := range provided {
		if char < '0' || char > '9' {
			return false
		}
	}
	for _, char := range stored {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}

// compareOTPCodes executes this step in the token backend flow.
func compareOTPCodes(provided, stored string) bool {
	// Simple string comparison is sufficient for OTP codes
	// Timing attacks are not a concern for 6-digit codes due to network latency noise
	return provided == stored
}

// getClientInfoFromContext executes this step in the token backend flow.
func getClientInfoFromContext(ctx context.Context) *ClientInfo {
	ip, hasIP := scontext.GetClientIP(ctx)
	userAgent, hasUserAgent := scontext.GetUserAgent(ctx)

	if !hasIP && !hasUserAgent {
		return nil
	}

	return &ClientInfo{
		IPAddress: ip,
		UserAgent: userAgent,
	}
}

// mergeClientInfoFromContext executes this step in the token backend flow.
func mergeClientInfoFromContext(ctx context.Context, provided *ClientInfo) *ClientInfo {
	contextClientInfo := getClientInfoFromContext(ctx)

	if provided == nil {
		return contextClientInfo
	}

	merged := *provided
	if contextClientInfo != nil {
		if merged.IPAddress == "" {
			merged.IPAddress = contextClientInfo.IPAddress
		}
		if merged.UserAgent == "" {
			merged.UserAgent = contextClientInfo.UserAgent
		}
	}

	if merged.IPAddress == "" && merged.UserAgent == "" && merged.DeviceID == "" {
		return nil
	}

	return &merged
}
