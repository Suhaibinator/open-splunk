package token

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// MockTokenService provides deterministic token generation for testing and mock data generation.
// It implements the full TokenService interface but generates predictable tokens based on
// a seed value and time windows, enabling reproducible test scenarios.
//
// SECURITY WARNING: This service should NEVER be used in production environments.
// It is designed specifically for testing, mock data generation, and development workflows.
type MockTokenService struct {
	persistence      common_persistence.CommonPersistenceInterface
	sessionLifetime  func() time.Duration
	securityLifetime func() time.Duration
	seed             int64
	logger           *zap.Logger
}

// MockTokenContent represents the structure embedded in mock tokens.
// This ensures tokens are self-contained and prevent cache collisions.
type MockTokenContent struct {
	UserID           uint64                    `json:"uid"`
	AuthMethod       proto_models.AuthProvider `json:"auth"`
	TokenType        proto_models.TokenType    `json:"type,omitempty"` // For security tokens
	IssuedAt         time.Time                 `json:"iat"`
	AuthenticatedAt  time.Time                 `json:"aat"`
	ExpiresAt        time.Time                 `json:"exp"`
	DeterministicSig string                    `json:"sig"`
}

// NewMockTokenService creates a new mock token service instance.
func NewMockTokenService(
	persistence common_persistence.CommonPersistenceInterface,
	config TokenServiceConfig,
	seed int64,
	logger *zap.Logger,
) (TokenService, error) {
	// Validate configuration using shared utility
	if err := validateTokenServiceConfig(&config); err != nil {
		return nil, err
	}

	service := &MockTokenService{
		persistence:      persistence,
		sessionLifetime:  config.SessionLifetime,
		securityLifetime: config.SecurityLifetime,
		seed:             seed,
		logger:           logger,
	}

	// Log warning about mock service usage
	service.logger.Named(mockTokenServiceLoggerName).Warn("Mock token service initialized - DO NOT USE IN PRODUCTION",
		zap.Int64(appLogger.KeySeed, seed),
		appLogger.DurationSeconds(appLogger.KeySessionTTLSeconds, config.SessionLifetime()),
		appLogger.DurationSeconds(appLogger.KeySecurityTTLSeconds, config.SecurityLifetime()))

	return service, nil
}

// Compile-time interface check
var _ TokenService = (*MockTokenService)(nil)

// generateMockTokenContent generates the common token content without persistence.
// This shared logic creates the deterministic signature and token structure.
func (m *MockTokenService) generateMockTokenContent(
	userID uint64,
	authMethod proto_models.AuthProvider,
	tokenType proto_models.TokenType,
	timeWindow time.Duration,
	expiresIn time.Duration,
	authenticatedAt time.Time,
) (MockTokenContent, string, error) {
	now := time.Now().UTC()

	// Use specified time window for signature stability
	timeWindowTruncated := now.Truncate(timeWindow)

	// Generate deterministic signature
	var sigInput string
	if tokenType == proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
		if authenticatedAt.IsZero() {
			authenticatedAt = timeWindowTruncated
		} else {
			authenticatedAt = authenticatedAt.UTC().Truncate(time.Second)
		}
		sigInput = fmt.Sprintf("%d-%s-%d-%d-%d", userID, authMethod.String(), m.seed, timeWindowTruncated.Unix(), authenticatedAt.Unix())
	} else {
		// Security tokens include token type for uniqueness
		sigInput = fmt.Sprintf("%d-%s-%s-%d-%d", userID, tokenType.String(), authMethod.String(), m.seed, timeWindowTruncated.Unix())
	}

	hash := sha256.Sum256([]byte(sigInput))
	signature := fmt.Sprintf("%x", hash)[:32] // 32 hex characters

	// Create token content
	content := MockTokenContent{
		UserID:           userID,
		AuthMethod:       authMethod,
		TokenType:        tokenType,
		IssuedAt:         timeWindowTruncated,
		AuthenticatedAt:  authenticatedAt,
		ExpiresAt:        timeWindowTruncated.Add(expiresIn),
		DeterministicSig: signature,
	}

	// Serialize and encode
	jsonBytes, err := json.Marshal(content)
	if err != nil {
		return MockTokenContent{}, "", fmt.Errorf("failed to marshal token content: %w", err)
	}

	token := base64.RawURLEncoding.EncodeToString(jsonBytes)
	return content, token, nil
}

// GenerateSessionToken creates a deterministic session token for authenticated users.
// The token is self-contained with embedded user information and a deterministic signature
// based on the user, authentication method, seed, and time window.
func (m *MockTokenService) GenerateSessionToken(ctx context.Context, userID uint64, method SessionAuthMethod) (string, error) {
	return m.generateSessionToken(ctx, userID, method, time.Time{}, true)
}

// GenerateSessionTokenWithAuthTime creates a deterministic session token while
// preserving the original authentication time across a refresh.
func (m *MockTokenService) GenerateSessionTokenWithAuthTime(
	ctx context.Context,
	userID uint64,
	method SessionAuthMethod,
	authenticatedAt time.Time,
) (string, error) {
	return m.generateSessionToken(ctx, userID, method, authenticatedAt, false)
}

func (m *MockTokenService) generateSessionToken(
	ctx context.Context,
	userID uint64,
	method SessionAuthMethod,
	authenticatedAt time.Time,
	recordLogin bool,
) (string, error) {
	content, token, err := m.generateMockTokenContent(
		userID,
		method.Provider,
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		5*time.Minute, // 5-minute time windows for session tokens
		m.sessionLifetime(),
		authenticatedAt,
	)
	if err != nil {
		appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Error("Failed to generate session token content", zap.Error(err))
		return "", ErrTokenGenerationFailed
	}

	// Store in database for session management using shared utility
	authToken := createSessionTokenRecordWithClient(
		userID,
		token,
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		method,
		content.IssuedAt,
		content.ExpiresAt,
		mergeClientInfoFromContext(ctx, nil),
	)
	authToken.RecordLogin = recordLogin

	if err := m.persistence.CreateSessionToken(ctx, authToken); err != nil {
		appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Error("Failed to store session token",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, userID))
		return "", ErrTokenStorageFailed
	}

	operationLogger := appLogger.For(ctx, m.logger, mockTokenServiceLoggerName)
	operationLogger.Warn("Using mock token service for session token generation - NOT SECURE and should NEVER be used in production",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Stringer(appLogger.KeyAuthMethod, method.Provider))

	if ce := operationLogger.Check(zap.DebugLevel, "Generated mock session token"); ce != nil {
		ce.Write(
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, method.Provider),
			zap.Time(appLogger.KeyTimeWindow, content.IssuedAt),
			zap.String(appLogger.KeySignature, content.DeterministicSig[:8]), // Log first 8 chars for debugging
		)
	}

	return token, nil
}

// ValidateSessionToken performs validation of a mock session token.
// It decodes the self-contained token, verifies the deterministic signature,
// and checks expiration without requiring database access for basic validation.
func (m *MockTokenService) ValidateSessionToken(token string) (*SessionTokenClaims, error) {
	m.logger.Named(mockTokenServiceLoggerName).Warn("Using mock token service for session token validation - NOT SECURE and should NEVER be used in production")
	// Decode token content
	jsonBytes, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	var content MockTokenContent
	if err := json.Unmarshal(jsonBytes, &content); err != nil {
		return nil, ErrTokenInvalid
	}

	// Verify signature is correct for this user + time window + original
	// authentication time. Tokens created by an older mock service did not carry
	// AuthenticatedAt, so retain their previous signature and fallback behavior.
	authenticatedAt := content.AuthenticatedAt
	var sigInput string
	if authenticatedAt.IsZero() {
		authenticatedAt = content.IssuedAt
		sigInput = fmt.Sprintf("%d-%s-%d-%d", content.UserID, content.AuthMethod.String(), m.seed, content.IssuedAt.Unix())
	} else {
		sigInput = fmt.Sprintf("%d-%s-%d-%d-%d", content.UserID, content.AuthMethod.String(), m.seed, content.IssuedAt.Unix(), authenticatedAt.Unix())
	}
	expectedHash := sha256.Sum256([]byte(sigInput))
	expectedSig := fmt.Sprintf("%x", expectedHash)[:32]

	if content.DeterministicSig != expectedSig {
		if appLogger.DebugEnabled(m.logger) {
			m.logger.Named(mockTokenServiceLoggerName).Debug("Token signature verification failed",
				zap.String(logkeys.Expected, expectedSig[:8]),
				zap.String(logkeys.Actual, content.DeterministicSig[:8]))
		}
		return nil, ErrTokenInvalid
	}

	// Check expiration
	if time.Now().UTC().After(content.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	// Return claims from embedded data
	return &SessionTokenClaims{
		UserID:          content.UserID,
		IssuedAt:        content.IssuedAt,
		AuthenticatedAt: authenticatedAt,
		ExpiresAt:       content.ExpiresAt,
	}, nil
}

// GenerateSecurityTokenNoPersist creates a deterministic security token without database storage.
// This version generates the token content but does not store it in the database.
func (m *MockTokenService) GenerateSecurityTokenNoPersist(ctx context.Context, params SecurityTokenParams) (string, error) {
	if params.ExpiresIn <= 0 {
		params.ExpiresIn = m.securityLifetime()
	}
	content, token, err := m.generateMockTokenContent(
		params.UserID,
		params.AuthMethod,
		params.TokenType,
		1*time.Minute, // 1-minute time windows for security tokens
		params.ExpiresIn,
		time.Time{},
	)
	if err != nil {
		appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Error("Failed to generate security token content", zap.Error(err))
		return "", ErrTokenGenerationFailed
	}

	operationLogger := appLogger.For(ctx, m.logger, mockTokenServiceLoggerName)
	operationLogger.Warn("Using mock token service for security token generation (no persist) - NOT SECURE and should NEVER be used in production",
		zap.Uint64(appLogger.KeyUserID, params.UserID),
		zap.Stringer(appLogger.KeyTokenType, params.TokenType))

	if ce := operationLogger.Check(zap.DebugLevel, "Generated mock security token (no persist)"); ce != nil {
		ce.Write(
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType),
			zap.Time(appLogger.KeyTimeWindow, content.IssuedAt),
			zap.String(appLogger.KeySignature, content.DeterministicSig[:8]),
		)
	}

	return token, nil
}

// GenerateSecurityToken creates a deterministic security token for one-time operations.
// Unlike session tokens, security tokens are still stored in the database for single-use
// semantics and validation consistency with production behavior.
func (m *MockTokenService) GenerateSecurityToken(ctx context.Context, params SecurityTokenParams) (string, error) {
	if params.ExpiresIn <= 0 {
		params.ExpiresIn = m.securityLifetime()
	}
	content, token, err := m.generateMockTokenContent(
		params.UserID,
		params.AuthMethod,
		params.TokenType,
		1*time.Minute, // 1-minute time windows for security tokens
		params.ExpiresIn,
		time.Time{},
	)
	if err != nil {
		appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Error("Failed to generate security token content", zap.Error(err))
		return "", ErrTokenGenerationFailed
	}

	// Store in database using shared utility
	authToken := createSessionTokenRecordWithClient(
		params.UserID,
		token,
		params.TokenType,
		SessionAuthMethod{Provider: params.AuthMethod},
		content.IssuedAt,
		content.ExpiresAt,
		mergeClientInfoFromContext(ctx, params.ClientInfo),
	)
	authToken.FrontendOrigin = params.FrontendOrigin
	authToken.BoundEmail = params.BoundEmail

	if err := m.persistence.CreateSessionToken(ctx, authToken); err != nil {
		appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Error("Failed to store security token",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType))
		return "", ErrTokenStorageFailed
	}

	operationLogger := appLogger.For(ctx, m.logger, mockTokenServiceLoggerName)
	operationLogger.Warn("Using mock token service for security token generation - NOT SECURE and should NEVER be used in production",
		zap.Uint64(appLogger.KeyUserID, params.UserID),
		zap.Stringer(appLogger.KeyTokenType, params.TokenType))

	if ce := operationLogger.Check(zap.DebugLevel, "Generated mock security token"); ce != nil {
		ce.Write(
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType),
			zap.Time(appLogger.KeyTimeWindow, content.IssuedAt),
			zap.String(appLogger.KeySignature, content.DeterministicSig[:8]),
		)
	}

	return token, nil
}

// ConsumeSecurityToken validates a security token against the database and consumes it under the
// same account lock order as production.
func (m *MockTokenService) ConsumeSecurityToken(ctx context.Context, token string, expectedType proto_models.TokenType) (*SecurityTokenInfo, error) {
	appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Warn("Using mock token service for security token consumption - NOT SECURE and should NEVER be used in production",
		zap.Stringer(appLogger.KeyTokenType, expectedType))
	// First decode and validate token structure
	jsonBytes, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	var content MockTokenContent
	if err := json.Unmarshal(jsonBytes, &content); err != nil {
		return nil, ErrTokenInvalid
	}

	// Verify signature
	sigInput := fmt.Sprintf("%d-%s-%s-%d-%d",
		content.UserID,
		content.TokenType.String(),
		content.AuthMethod.String(),
		m.seed,
		content.IssuedAt.Unix())
	expectedHash := sha256.Sum256([]byte(sigInput))
	expectedSig := fmt.Sprintf("%x", expectedHash)[:32]

	if content.DeterministicSig != expectedSig {
		if appLogger.DebugEnabled(m.logger) {
			m.logger.Named(mockTokenServiceLoggerName).Debug("Security token signature verification failed",
				zap.String(logkeys.Expected, expectedSig[:8]),
				zap.String(logkeys.Actual, content.DeterministicSig[:8]))
		}
		return nil, ErrTokenInvalid
	}

	// Check token type matches expected
	if content.TokenType != expectedType {
		return nil, ErrTokenInvalid
	}

	// Validate and consume against the database (same as production)
	authToken, err := m.persistence.ConsumeSecurityToken(ctx, token, expectedType)
	if err != nil {
		switch {
		case errors.Is(err, common_persistence.ErrTokenAlreadyUsed):
			return nil, ErrTokenAlreadyUsed
		case errors.Is(err, common_persistence.ErrTokenExpired):
			return nil, ErrTokenExpired
		}
		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Debug("Security token not consumable in database",
				zap.Error(err),
				zap.Stringer(appLogger.KeyTokenType, expectedType))
		}
		return nil, ErrTokenInvalid
	}

	return &SecurityTokenInfo{
		UserID:         authToken.UserID,
		TokenType:      authToken.TokenType,
		AuthMethod:     authToken.AuthMethod,
		IssuedAt:       authToken.IssuedAt,
		ExpiresAt:      authToken.ExpiresAt,
		UsedAt:         authToken.UsedAt,
		FrontendOrigin: authToken.FrontendOrigin,
	}, nil
}

// RevokeToken revokes any type of token, preventing further use.
func (m *MockTokenService) RevokeToken(ctx context.Context, token string) error {
	appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Warn("Using mock token service for token revocation - NOT SECURE and should NEVER be used in production")
	return m.persistence.RevokeSessionToken(ctx, token)
}

// RevokeAllUserTokens revokes all tokens of a specific type for a user.
func (m *MockTokenService) RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error {
	appLogger.For(ctx, m.logger, mockTokenServiceLoggerName).Warn("Using mock token service for revoking all user tokens - NOT SECURE and should NEVER be used in production",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Stringer(appLogger.KeyTokenType, tokenType))
	return m.persistence.RevokeAllUserTokens(ctx, userID, tokenType)
}

// GenerateOTPCode creates a deterministic 6-digit OTP code for testing.
// Uses seed and current time window to generate predictable codes that are
// consistent within short time periods but change periodically for realistic testing.
func (m *MockTokenService) GenerateOTPCode(ctx context.Context) (string, error) {
	now := time.Now().UTC()

	// Use 30-second time windows for OTP generation to balance determinism with realistic expiry
	timeWindow := now.Truncate(30 * time.Second)

	// Generate deterministic OTP based on seed + time window
	otpInput := fmt.Sprintf("otp-%d-%d", m.seed, timeWindow.Unix())
	hash := sha256.Sum256([]byte(otpInput))

	// Convert first 4 bytes of hash to number and mod by 1000000 to get 6-digit code
	hashInt := int64(hash[0])<<24 | int64(hash[1])<<16 | int64(hash[2])<<8 | int64(hash[3])
	if hashInt < 0 {
		hashInt = -hashInt // Ensure positive
	}
	otpCode := fmt.Sprintf("%06d", hashInt%1000000)

	operationLogger := appLogger.For(ctx, m.logger, mockTokenServiceLoggerName)
	operationLogger.Warn("Using mock token service for OTP code generation - NOT SECURE and should NEVER be used in production")

	if ce := operationLogger.Check(zap.DebugLevel, "Generated mock OTP code"); ce != nil {
		ce.Write(
			zap.Time(appLogger.KeyTimeWindow, timeWindow),
			zap.String(appLogger.KeyCodePrefix, otpCode[:2]), // Log first 2 digits for debugging
		)
	}

	return otpCode, nil
}

// ValidateOTPCode validates an OTP code with simple string comparison.
// Both the provided and stored codes should be exactly 6 digits.
// Uses the same validation logic as the production service.
func (m *MockTokenService) ValidateOTPCode(provided, stored string) bool {
	m.logger.Named(mockTokenServiceLoggerName).Warn("Using mock token service for OTP code validation - NOT SECURE and should NEVER be used in production")
	// Use shared format validation
	if !validateOTPFormat(provided, stored) {
		return false
	}

	// Use shared comparison logic
	return compareOTPCodes(provided, stored)
}
