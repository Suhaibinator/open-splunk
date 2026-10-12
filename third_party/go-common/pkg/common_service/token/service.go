package token

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// tokenService carries state used by the token backend flow.
type tokenService struct {
	persistence      common_persistence.CommonPersistenceInterface
	sessionLifetime  func() time.Duration
	securityLifetime func() time.Duration
	logger           *zap.Logger
	hmacKey          []byte
}

// NewTokenService executes this step in the token backend flow.
func NewTokenService(
	persistence common_persistence.CommonPersistenceInterface,
	config TokenServiceConfig,
	logger *zap.Logger,
) (TokenService, error) {
	// Validate configuration using shared utility
	if err := validateTokenServiceConfig(&config); err != nil {
		return nil, err
	}

	return &tokenService{
		persistence:      persistence,
		sessionLifetime:  config.SessionLifetime,
		securityLifetime: config.SecurityLifetime,
		logger:           logger,
		hmacKey:          []byte(config.SessionTokenSecret),
	}, nil
}

// GenerateSessionToken executes this step in the token backend flow.
func (s *tokenService) GenerateSessionToken(ctx context.Context, userID uint64, method SessionAuthMethod) (string, error) {
	return s.generateSessionToken(ctx, userID, method, time.Now().UTC(), true)
}

// GenerateSessionTokenWithAuthTime creates a session token that preserves the
// time at which the user originally authenticated. Sliding-session refreshes
// use this path so extending token expiry cannot satisfy recent-authentication
// checks by itself.
func (s *tokenService) GenerateSessionTokenWithAuthTime(
	ctx context.Context,
	userID uint64,
	method SessionAuthMethod,
	authenticatedAt time.Time,
) (string, error) {
	return s.generateSessionToken(ctx, userID, method, authenticatedAt, false)
}

func (s *tokenService) generateSessionToken(
	ctx context.Context,
	userID uint64,
	method SessionAuthMethod,
	authenticatedAt time.Time,
	recordLogin bool,
) (string, error) {
	buffer := make([]byte, SessionTokenSize)
	now := time.Now().UTC()

	// Version byte
	buffer[0] = TokenVersionSession

	// Expiry time (8 bytes)
	expiryTime := now.Add(s.sessionLifetime()).Unix()
	binary.BigEndian.PutUint64(buffer[1:9], uint64(expiryTime))

	// User ID (8 bytes)
	binary.BigEndian.PutUint64(buffer[9:17], userID)

	// Auth method (1 byte). Informational only and never read back: a slug does
	// not fit, so refresh and session listings use the session row instead.
	buffer[17] = byte(method.Provider)

	// Original authentication time (8 bytes)
	binary.BigEndian.PutUint64(
		buffer[sessionTokenAuthenticatedAtOffset:sessionTokenRandomOffset],
		uint64(authenticatedAt.UTC().Unix()),
	)

	// Random bytes (16 bytes)
	if _, err := rand.Read(buffer[sessionTokenRandomOffset:sessionTokenIssuedAtOffset]); err != nil {
		return "", appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", ErrTokenGenerationFailed, err),
			zap.String(logkeys.Stage, "generate_session_token_entropy"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, method.Provider),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
	}

	// Persist issuance independently of live lifetime and original authentication.
	binary.BigEndian.PutUint64(buffer[sessionTokenIssuedAtOffset:sessionTokenHMACOffset], uint64(now.Unix()))

	// Calculate HMAC over all data except HMAC field
	h := hmac.New(sha256.New, s.hmacKey)
	h.Write(buffer[:sessionTokenHMACOffset])
	hmacSum := h.Sum(nil)

	// Copy HMAC (16 bytes)
	copy(buffer[sessionTokenHMACOffset:], hmacSum[:SessionTokenHMACBytes])

	// Encode to base64
	token := base64.RawURLEncoding.EncodeToString(buffer)

	// Store in database for session management using shared utility
	authToken := createSessionTokenRecordWithClient(
		userID,
		token,
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		method,
		now,
		time.Unix(expiryTime, 0).UTC(),
		mergeClientInfoFromContext(ctx, nil),
	)
	authToken.RecordLogin = recordLogin

	if err := s.persistence.CreateSessionToken(ctx, authToken); err != nil {
		return "", appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "store_session_token"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, method.Provider),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
	}

	if appLogger.DebugEnabled(s.logger) {
		appLogger.For(ctx, s.logger, tokenServiceLoggerName).Debug("Generated session token",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, method.Provider),
			zap.Time(appLogger.KeyExpiresAt, authToken.ExpiresAt))
	}

	return token, nil
}

// ValidateSessionToken executes this step in the token backend flow.
func (s *tokenService) ValidateSessionToken(token string) (*SessionTokenClaims, error) {
	// Decode from base64
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	if len(decoded) != SessionTokenSize {
		return nil, ErrTokenInvalid
	}
	if decoded[0] != TokenVersionSession {
		return nil, ErrInvalidTokenVersion
	}
	hmacOffset := sessionTokenHMACOffset

	// Verify HMAC
	h := hmac.New(sha256.New, s.hmacKey)
	h.Write(decoded[:hmacOffset]) // All data except HMAC
	expectedHMAC := h.Sum(nil)[:SessionTokenHMACBytes]

	if !hmac.Equal(decoded[hmacOffset:], expectedHMAC) {
		return nil, ErrInvalidHMAC
	}

	// Extract claims
	expiryTime := int64(binary.BigEndian.Uint64(decoded[1:9]))
	userID := binary.BigEndian.Uint64(decoded[9:17])

	// Check expiry
	expiresAt := time.Unix(expiryTime, 0).UTC()
	if time.Now().UTC().After(expiresAt) {
		return nil, NewExpiredError(expiresAt)
	}

	issuedAt := time.Unix(int64(binary.BigEndian.Uint64(decoded[sessionTokenIssuedAtOffset:sessionTokenHMACOffset])), 0).UTC()
	if issuedAt.After(expiresAt) || issuedAt.Unix() <= 0 {
		return nil, ErrTokenInvalid
	}
	authenticatedAt := time.Unix(int64(binary.BigEndian.Uint64(decoded[sessionTokenAuthenticatedAtOffset:sessionTokenRandomOffset])), 0).UTC()

	return &SessionTokenClaims{
		UserID:          userID,
		IssuedAt:        issuedAt,
		AuthenticatedAt: authenticatedAt,
		ExpiresAt:       expiresAt,
	}, nil
}

// GenerateSecurityToken executes this step in the token backend flow.
func (s *tokenService) GenerateSecurityToken(ctx context.Context, params SecurityTokenParams) (string, error) {
	// Generate pure random bytes for maximum entropy
	tokenBytes := make([]byte, SecurityTokenSize)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", appLogger.WithErrorFields(
			fmt.Errorf("%w: %w", ErrTokenGenerationFailed, err),
			zap.String(logkeys.Stage, "generate_security_token_entropy"),
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyAuthMethod, params.AuthMethod),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType))
	}

	// Encode to base64
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)

	// Calculate expiry
	expiresAt := time.Now().UTC().Add(params.ExpiresIn)
	if params.ExpiresIn <= 0 {
		expiresAt = time.Now().UTC().Add(s.securityLifetime())
	}

	// Store in database using shared utility
	clientInfo := mergeClientInfoFromContext(ctx, params.ClientInfo)
	authToken := createSessionTokenRecordWithClient(
		params.UserID,
		token,
		params.TokenType,
		SessionAuthMethod{Provider: params.AuthMethod},
		time.Now().UTC(),
		expiresAt,
		clientInfo,
	)
	authToken.FrontendOrigin = params.FrontendOrigin
	authToken.BoundEmail = params.BoundEmail

	if err := s.persistence.CreateSessionToken(ctx, authToken); err != nil {
		return "", appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "store_security_token"),
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyAuthMethod, params.AuthMethod),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType),
			zap.String(appLogger.KeyToken, token))
	}

	if appLogger.DebugEnabled(s.logger) {
		appLogger.For(ctx, s.logger, tokenServiceLoggerName).Debug("Generated security token",
			zap.Uint64(appLogger.KeyUserID, params.UserID),
			zap.Stringer(appLogger.KeyTokenType, params.TokenType),
			zap.Time(appLogger.KeyExpiresAt, expiresAt))
	}

	return token, nil
}

// ConsumeSecurityToken executes this step in the token backend flow.
func (s *tokenService) ConsumeSecurityToken(ctx context.Context, token string, expectedType proto_models.TokenType) (*SecurityTokenInfo, error) {
	// Security tokens must always be validated against the database.
	authToken, err := s.persistence.ConsumeSecurityToken(ctx, token, expectedType)
	if err != nil {
		fields := []zap.Field{
			zap.String(logkeys.Stage, "consume_security_token"),
			zap.String(appLogger.KeyToken, token),
			zap.Stringer(appLogger.KeyTokenType, expectedType),
		}
		switch {
		case errors.Is(err, common_persistence.ErrSessionTokenNotFound):
			return nil, appLogger.WithErrorLevel(errors.Join(ErrTokenNotFound, err), zapcore.WarnLevel, fields...)
		case errors.Is(err, common_persistence.ErrTokenExpired):
			return nil, appLogger.WithErrorFields(errors.Join(ErrTokenExpired, err), fields...)
		case errors.Is(err, common_persistence.ErrTokenRevoked):
			return nil, appLogger.WithErrorLevel(errors.Join(ErrTokenRevoked, err), zapcore.WarnLevel, fields...)
		case errors.Is(err, common_persistence.ErrTokenAlreadyUsed):
			return nil, appLogger.WithErrorLevel(errors.Join(ErrTokenUsed, err), zapcore.WarnLevel, fields...)
		case errors.Is(err, common_persistence.ErrInvalidToken):
			return nil, appLogger.WithErrorLevel(errors.Join(ErrTokenInvalid, err), zapcore.WarnLevel, fields...)
		}
		return nil, appLogger.WithErrorFields(err, fields...)
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

// RevokeToken executes this step in the token backend flow.
func (s *tokenService) RevokeToken(ctx context.Context, token string) error {
	err := s.persistence.RevokeSessionToken(ctx, token)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "revoke_security_token"),
			zap.String(appLogger.KeyToken, token))
	}
	return nil
}

// RevokeAllUserTokens executes this step in the token backend flow.
func (s *tokenService) RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error {
	var err error
	if tokenType == proto_models.TokenType_TOKEN_TYPE_UNSPECIFIED {
		// Revoke all authentication tokens (most common case)
		err = s.persistence.RevokeAllSessionTokensForUser(ctx, userID)
	} else {
		// Revoke specific token type
		err = s.persistence.RevokeAllUserTokens(ctx, userID, tokenType)
	}

	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "revoke_all_user_tokens"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyTokenType, tokenType))
	}
	return nil
}

// GenerateOTPCode executes this step in the token backend flow.
func (s *tokenService) GenerateOTPCode(ctx context.Context) (string, error) {
	// Generate a random number between 0 and 999999 (inclusive)
	randomNum, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", appLogger.WithErrorFields(
			fmt.Errorf("failed to generate OTP code: %w", err),
			zap.String(logkeys.Stage, "generate_otp_entropy"),
			zap.String(appLogger.KeyTokenType, "otp"))
	}

	// Format as 6-digit string with leading zeros
	otpCode := fmt.Sprintf("%06d", randomNum.Int64())

	if appLogger.DebugEnabled(s.logger) {
		appLogger.For(ctx, s.logger, tokenServiceLoggerName).Debug("Generated OTP code",
			zap.String(appLogger.KeyCodePrefix, otpCode[:2])) // Log first 2 digits for debugging
	}

	return otpCode, nil
}

// ValidateOTPCode executes this step in the token backend flow.
func (s *tokenService) ValidateOTPCode(provided, stored string) bool {
	// Use shared format validation
	if !validateOTPFormat(provided, stored) {
		return false
	}

	// Use shared comparison logic
	return compareOTPCodes(provided, stored)
}
