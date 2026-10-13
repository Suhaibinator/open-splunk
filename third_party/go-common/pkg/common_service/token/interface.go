// Package token provides a unified token generation and validation system for the go-common library.
// It implements different token types with appropriate security properties for their use cases.
//
// Token Types:
//   - Session Tokens: Small, fast-validating tokens for authentication (sent with every request)
//   - Security Tokens: Large, high-entropy tokens for one-time operations (email verification, password reset)
//   - Temporary Tokens: Short-lived tokens for specific operations (OAuth state, etc.)
//
// The package ensures consistent token handling across all authentication methods,
// eliminating the fragmentation that can lead to security vulnerabilities.
package token

import (
	"context"
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// TokenService defines the interface for unified token generation and validation.
// It provides methods for creating and validating different types of tokens,
// each optimized for their specific use case.
type TokenService interface {
	// Session Token Methods - For authentication tokens sent with every request

	// GenerateSessionToken creates a fresh session token for authenticated users
	// and records the successful login atomically with token persistence.
	// Session tokens are optimized for size and validation speed as they're sent
	GenerateSessionToken(ctx context.Context, userID uint64, method SessionAuthMethod) (string, error)

	// GenerateSessionTokenWithAuthTime creates a new session token while
	// preserving when the user originally authenticated. It is intended for
	// sliding-session refreshes, which must not make authentication look recent.
	GenerateSessionTokenWithAuthTime(ctx context.Context, userID uint64, method SessionAuthMethod, authenticatedAt time.Time) (string, error)

	// ValidateSessionToken performs fast validation of a session token.
	// This method validates the token structure and expiry without database access,
	ValidateSessionToken(token string) (*SessionTokenClaims, error)

	// Security Token Methods - For one-time operations via email

	// GenerateSecurityToken creates a high-entropy token for security operations.
	// These tokens are used for email verification, password reset, and other
	GenerateSecurityToken(ctx context.Context, params SecurityTokenParams) (string, error)

	// ConsumeSecurityToken validates a single-use security token of expectedType against the
	// database and marks it used in the caller's transaction, locking the owning user row
	// before the token row so account flows share one lock order. Requires an ambient
	// transaction; a rollback leaves the token usable.
	ConsumeSecurityToken(ctx context.Context, token string, expectedType proto_models.TokenType) (*SecurityTokenInfo, error)

	// Utility Methods

	// RevokeToken revokes any type of token, preventing further use.
	RevokeToken(ctx context.Context, token string) error

	// RevokeAllUserTokens revokes all tokens of a specific type for a user.
	RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error

	// OTP Code Methods - For phone verification and similar short codes

	// GenerateOTPCode creates a cryptographically secure 6-digit OTP code.
	// OTP codes are short-lived numeric codes used for phone verification,
	// two-factor authentication, and similar quick verification flows.
	//
	// Parameters:
	//   - ctx: Context for logging and tracing
	//
	// Returns:
	//   - code: 6-digit numeric string (e.g., "123456", "000789")
	//   - error: Any error during generation
	GenerateOTPCode(ctx context.Context) (string, error)

	// ValidateOTPCode validates an OTP code using constant-time comparison.
	ValidateOTPCode(provided, stored string) bool
}

// SessionAuthMethod is how a session authenticated. It is persisted on the
// session row, which is authoritative: refresh carries it over from the row.
type SessionAuthMethod struct {
	Provider proto_models.AuthProvider
	// OAuthProviderSlug names the provider when Provider is
	// AUTH_PROVIDER_OAUTH and is empty otherwise.
	OAuthProviderSlug string
}

// SessionTokenClaims are the signed claims embedded in a session token. How the
// session authenticated is not among them: read it from the session row.
type SessionTokenClaims struct {
	// UserID is the authenticated user's identifier
	UserID uint64
	// IssuedAt is when the token was created
	IssuedAt time.Time
	// AuthenticatedAt is when the user last proved their identity. Unlike
	// IssuedAt, sliding-session refreshes preserve this value.
	AuthenticatedAt time.Time
	// ExpiresAt is when the token becomes invalid
	ExpiresAt time.Time
}

// SecurityTokenParams carries state used by the token backend flow.
type SecurityTokenParams struct {
	BoundEmail string
	// UserID is the user this token is for
	UserID uint64
	// TokenType specifies the operation (EMAIL_VERIFICATION, PASSWORD_RESET)
	TokenType proto_models.TokenType
	// AuthMethod indicates the authentication context
	AuthMethod proto_models.AuthProvider
	// ExpiresIn is how long the token remains valid
	ExpiresIn time.Duration
	// ClientInfo contains optional client metadata for auditing
	ClientInfo *ClientInfo
	// FrontendOrigin is the validated frontend origin the token's email link targets
	// (empty means the default FrontendURL)
	FrontendOrigin string
}

// SecurityTokenInfo carries state used by the token backend flow.
type SecurityTokenInfo struct {
	// UserID is the user this token belongs to
	UserID uint64
	// TokenType is the operation this token is for
	TokenType proto_models.TokenType
	// AuthMethod is how the token was created
	AuthMethod proto_models.AuthProvider
	// IssuedAt is when the token was created
	IssuedAt time.Time
	// ExpiresAt is when the token expires
	ExpiresAt time.Time
	// UsedAt indicates if/when the token was used (nil if unused)
	UsedAt *time.Time
	// FrontendOrigin is the validated frontend origin bound to the token at issuance
	// (empty means the default FrontendURL)
	FrontendOrigin string
}

// ClientInfo carries state used by the token backend flow.
type ClientInfo struct {
	// IPAddress of the client (supports IPv6)
	IPAddress string
	// UserAgent from the client's browser/app
	UserAgent string
	// DeviceID for optional device fingerprinting
	DeviceID string
}

// TokenServiceConfig supplies token issuance policy and the startup signing key.
type TokenServiceConfig struct {
	// Lifetimes are sampled for new tokens; existing tokens retain their expiry.
	// Both callbacks are required.
	SessionLifetime    func() time.Duration
	SecurityLifetime   func() time.Duration
	SessionTokenSecret string
}
