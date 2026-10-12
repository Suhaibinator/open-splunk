package email

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/email_client"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	tokenService "github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/argon2"
)

// EmailVerificationPath is the URL path for email verification links.
// This path should be handled by the frontend to extract and validate the token.
const EmailVerificationPath = "/auth/email/verify"

// PasswordResetPath is the URL path for password reset links.
// This path should be handled by the frontend to extract the token and show a password reset form.
const PasswordResetPath = "/auth/email/reset"

// Argon2 parameters for password hashing.
// These values provide a balance between security and performance.
const (
	emailAuthLoggerName = "common_service.auth.email_auth"

	argon2Time    = 3         // Number of iterations
	argon2Memory  = 64 * 1024 // Memory usage in KiB (64MB)
	argon2Threads = 4         // Number of threads
	argon2KeyLen  = 32        // Length of derived key in bytes
	otpAlgorithm  = otp.AlgorithmSHA256
)

func emailVerificationMatchesEmail(ev *common_persistence_models.EmailVerification, email string) bool {
	return ev != nil &&
		ev.VerifiedAt != nil &&
		common_persistence_models.NormalizeUserEmail(ev.Email) == common_persistence_models.NormalizeUserEmail(email)
}

// SaltLength is the length of the salt used for password hashing in bytes.
// A 16-byte (128-bit) salt provides sufficient entropy for security.
const SaltLength = 16

// EmailAuthHandlerInterface defines the interface for email authentication operations.
// It provides methods for user registration, email verification, login, password management, and OTP/2FA.
// The generic type U represents the user model that implements the UserLike interface.
type EmailAuthHandlerInterface[U common_service_models.UserLike] interface {
	SetInitialPassword(ctx context.Context, userID uint64, password string) error
	CreateAccountInvitation(ctx context.Context, userID uint64, frontendOrigin string, resend bool) error
	AcceptAccountInvitation(ctx context.Context, token, password string) (uint64, error)
	CreateEmailVerificationAndSendCode(ctx context.Context, userID uint64, email, password, frontendOrigin string) error
	ResendVerificationEmail(ctx context.Context, userID uint64, frontendOrigin string) error
	VerifyEmailWithToken(ctx context.Context, token string) (VerificationIdentity, error)
	LoginWithEmail(ctx context.Context, email, password string) (*U, string, error)                      // returns user, auth token, error
	LoginWithEmailAndOTP(ctx context.Context, email, password, otpCode string) (*U, string, bool, error) // returns user, auth token, otp_required, error
	CreatePasswordResetToken(ctx context.Context, email, frontendOrigin string) error
	ResetPasswordWithToken(ctx context.Context, token, newPassword string) (uint64, []string, error)
	ChangePassword(ctx context.Context, userID uint64, oldPassword, newPassword string) ([]string, error)
	AddPasswordAuthentication(ctx context.Context, userID uint64, password string) error
	SendAuthMethodChangeEmail(ctx context.Context, email, firstName, changeDescription string) error

	// OTP/TOTP 2FA methods
	SetupOTP(ctx context.Context, userID uint64, currentPassword string) (secret, qrCodeURL string, err error)
	VerifyOTPSetup(ctx context.Context, userID uint64, otpCode, currentPassword string) error
	DisableOTP(ctx context.Context, userID uint64, currentPassword string) error
	VerifyTOTP(secret, code string) bool

	Stop()
}

// VerificationIdentity carries identity learned during email verification.
// Fields remain populated when a later stage fails so the owning boundary can
// include the diagnostic context without performing logging-only lookups.
type VerificationIdentity struct {
	UserID uint64
	Email  string
}

// EmailAuthHandler carries state used by the email backend flow.
type EmailAuthHandler[U common_service_models.UserLike] struct {
	logger             *zap.Logger
	emailClient        *email_client.EmailClient
	persistenceHandler persistence.CommonPersistenceInterface
	userCallbacks      *ports.UserCallbackConfig[U]
	runtimeConfig      RuntimeConfigProvider
	tokenService       tokenService.TokenService
}

// RuntimeConfigView is the hot email-auth policy captured for one operation.
// Values returned by one view must belong to one immutable configuration
// generation.
type RuntimeConfigView interface {
	VerificationTokenValidity() time.Duration
	FrontendURL() string
	VerificationCooldown() time.Duration
	OTPIssuer() string
}

var _ RuntimeConfigView = config.RootView{}

// RuntimeConfigProvider captures one immutable auth configuration generation.
type RuntimeConfigProvider func() RuntimeConfigView

// NewEmailAuthHandler builds the email auth handler. provider is sampled once
// per operation, so verification lifetime, cooldown, link base URL, and OTP
// issuer are hot.
func NewEmailAuthHandler[U common_service_models.UserLike](
	ctx context.Context,
	logger *zap.Logger,
	provider RuntimeConfigProvider,
	emailClient *email_client.EmailClient,
	persistenceHandler persistence.CommonPersistenceInterface,
	userCallbacks *ports.UserCallbackConfig[U],
	tokenService tokenService.TokenService,
) (*EmailAuthHandler[U], error) {
	if provider == nil {
		return nil, fmt.Errorf("email.NewEmailAuthHandler: config provider cannot be nil")
	}
	return &EmailAuthHandler[U]{
		logger:             logger,
		emailClient:        emailClient,
		persistenceHandler: persistenceHandler,
		userCallbacks:      userCallbacks,
		runtimeConfig:      provider,
		tokenService:       tokenService,
	}, nil
}

// CreateEmailVerificationAndSendCode executes this step in the email backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by CreateEmailVerificationAndSendCode.
//   - userID: uint64 input consumed by CreateEmailVerificationAndSendCode.
//   - email: string input consumed by CreateEmailVerificationAndSendCode.
//   - password: string input consumed by CreateEmailVerificationAndSendCode.
//   - frontendOrigin: validated frontend origin for the verification link ("" = default FrontendURL).
func (h *EmailAuthHandler[U]) CreateEmailVerificationAndSendCode(ctx context.Context, userID uint64, email, password, frontendOrigin string) error {
	// Generate password hash and salt
	salt := make([]byte, SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Stage, "generate_password_salt"))
	}

	passwordHash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	// Create email verification record
	ev := &common_persistence_models.EmailVerification{
		UserID: userID,
		Email:  email,
	}

	if err := h.persistenceHandler.CreateEmailVerification(ctx, ev); err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Stage, "create_email_verification"))
	}

	// Create user password record
	if err := h.persistenceHandler.CreateUserPassword(ctx, userID,
		base64.StdEncoding.EncodeToString(passwordHash),
		base64.StdEncoding.EncodeToString(salt)); err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Stage, "create_user_password"))
	}

	// Generate verification token using token service
	view := h.runtimeConfig()
	token, err := h.tokenService.GenerateSecurityToken(ctx, tokenService.SecurityTokenParams{
		UserID:         userID,
		TokenType:      proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION,
		AuthMethod:     proto_models.AuthProvider_AUTH_PROVIDER_EMAIL,
		ExpiresIn:      view.VerificationTokenValidity(),
		FrontendOrigin: frontendOrigin,
		BoundEmail:     common_persistence_models.NormalizeUserEmail(email),
	})
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
			zap.String(logkeys.Stage, "generate_verification_token"))
	}

	// Send verification email
	if err := h.sendVerificationEmail(ctx, view, email, token, frontendOrigin); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Stage, "send_verification_email"))
	}

	return nil
}

// ResendVerificationEmail executes this step in the email backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by ResendVerificationEmail.
//   - userID: uint64 input consumed by ResendVerificationEmail.
//   - frontendOrigin: validated frontend origin for the verification link ("" = default FrontendURL).
func (h *EmailAuthHandler[U]) ResendVerificationEmail(ctx context.Context, userID uint64, frontendOrigin string) error {
	view := h.runtimeConfig()
	// Get user via callback
	user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		return appLogger.WithErrorFields(apperrors.ErrUserNotFound,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_for_verification_resend"))
	}

	ev, err := h.persistenceHandler.GetEmailVerificationByUserID(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(apperrors.ErrUserNotFound,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()),
			zap.String(logkeys.Stage, "load_email_verification_for_resend"))
	}

	if emailVerificationMatchesEmail(ev, user.GetEmail()) {
		return appLogger.WithErrorFields(apperrors.ErrEmailAlreadyVerified,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()))
	}

	// Check for existing active tokens
	tokens, err := h.persistenceHandler.GetActiveTokensByUserID(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()),
			zap.String(logkeys.Stage, "load_active_verification_tokens"))
	}

	// Look for active email verification token
	var activeToken *common_persistence_models.SessionToken
	for _, t := range tokens {
		if t.TokenType == proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION && t.BoundEmail != "" && common_persistence_models.NormalizeUserEmail(t.BoundEmail) == common_persistence_models.NormalizeUserEmail(user.GetEmail()) {
			activeToken = t
			break
		}
	}

	// If no active token, create new one
	if activeToken == nil {
		// Check cooldown - find the most recent verification token (active or not)
		// Note: This uses GetActiveTokensByUserID which might miss very recent expired tokens
		// For a more complete implementation, we'd need a method to get all tokens including expired ones
		allTokens, err := h.persistenceHandler.GetActiveTokensByUserID(ctx, userID)
		if err != nil {
			return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyEmail, user.GetEmail()),
				zap.String(logkeys.Stage, "load_verification_token_history"))
		}

		var mostRecentVerificationToken *common_persistence_models.SessionToken
		for _, t := range allTokens {
			if t.TokenType == proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION {
				if mostRecentVerificationToken == nil || t.IssuedAt.After(mostRecentVerificationToken.IssuedAt) {
					mostRecentVerificationToken = t
				}
			}
		}

		// Check if we're within the cooldown period
		if mostRecentVerificationToken != nil {
			timeSinceLastToken := time.Since(mostRecentVerificationToken.IssuedAt)
			if timeSinceLastToken < view.VerificationCooldown() {
				return appLogger.WithErrorFields(apperrors.ErrVerificationCooldown,
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.String(appLogger.KeyEmail, user.GetEmail()),
					appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, view.VerificationCooldown()-timeSinceLastToken))
			}
		}

		token, err := h.tokenService.GenerateSecurityToken(ctx, tokenService.SecurityTokenParams{
			UserID:         userID,
			TokenType:      proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION,
			AuthMethod:     proto_models.AuthProvider_AUTH_PROVIDER_EMAIL,
			ExpiresIn:      view.VerificationTokenValidity(),
			FrontendOrigin: frontendOrigin,
			BoundEmail:     common_persistence_models.NormalizeUserEmail(user.GetEmail()),
		})
		if err != nil {
			return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyEmail, user.GetEmail()),
				zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
				zap.String(logkeys.Stage, "generate_verification_token"))
		}

		// Note: Token service handles database storage internally, so we need to retrieve it
		// to get the activeToken for sending the email. For now, we'll create a temporary token struct.
		activeToken = &common_persistence_models.SessionToken{
			Token: token,
		}
	}

	// Send email using the current request's origin, even when reusing an active token
	// (the token row keeps its issuance-time origin; the link is what must match the requester).
	if err := h.sendVerificationEmail(ctx, view, user.GetEmail(), activeToken.Token, frontendOrigin); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()),
			zap.String(logkeys.Stage, "send_verification_email"))
	}
	return nil
}

// mapSecurityTokenError translates a token-service validation error into the
// app-level error the email auth handlers return. A revoked token (e.g. its
// owning account was deleted) reads as no-longer-valid to the user, so it is
// surfaced as expired to prompt a fresh link rather than a dead-end error; an
// already-used token is reported distinctly; a missing, malformed, or wrong-type
// token is reported as invalid. Shared by the email-verification and
// password-reset flows so the mapping cannot drift between them.
func mapSecurityTokenError(err error) error {
	switch {
	case errors.Is(err, tokenService.ErrTokenExpired),
		errors.Is(err, tokenService.ErrTokenRevoked):
		return apperrors.ErrTokenExpired
	case errors.Is(err, tokenService.ErrTokenUsed):
		return apperrors.ErrTokenUsed
	case errors.Is(err, tokenService.ErrTokenNotFound),
		errors.Is(err, tokenService.ErrTokenInvalid),
		errors.Is(err, tokenService.ErrTokenTypeMismatch):
		return apperrors.ErrInvalidToken
	default:
		return apperrors.ErrInternalError
	}
}

// VerifyEmailWithToken executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) VerifyEmailWithToken(ctx context.Context, token string) (VerificationIdentity, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return VerificationIdentity{}, apperrors.ErrTransactionNotFound
	}
	// Consuming the token locks the account first, so this flow's later account writes share the
	// lock order of every other account-mutating transaction. A concurrent verification that
	// consumed the token while this one waited reports as already used.
	tokenInfo, err := h.tokenService.ConsumeSecurityToken(ctx, token, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION)
	if err != nil {
		mappedErr := mapSecurityTokenError(err)
		verificationErr := errors.Join(mappedErr, err)
		if errors.Is(mappedErr, apperrors.ErrInvalidToken) || errors.Is(mappedErr, apperrors.ErrTokenUsed) {
			verificationErr = appLogger.WithErrorLevel(verificationErr, zapcore.WarnLevel)
		} else if errors.Is(mappedErr, apperrors.ErrTokenExpired) {
			// A revoked verification token is intentionally presented as expired.
			// Override the token service's generic revoked-token Warn accordingly.
			verificationErr = appLogger.WithErrorLevel(verificationErr, zapcore.InfoLevel)
		}
		return VerificationIdentity{}, appLogger.WithErrorFields(verificationErr,
			zap.String(appLogger.KeyToken, token),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
			zap.String(logkeys.Stage, "consume_token"))
	}
	identity := VerificationIdentity{UserID: tokenInfo.UserID}

	// Mark email as verified
	email, err := h.persistenceHandler.MarkEmailVerified(ctx, tokenInfo.UserID)
	identity.Email = email
	if err != nil {
		fields := []zap.Field{
			zap.String(appLogger.KeyToken, token),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
			zap.Uint64(appLogger.KeyUserID, identity.UserID),
			zap.String(logkeys.Stage, "mark_email_verified"),
		}
		if identity.Email != "" {
			fields = append(fields, zap.String(appLogger.KeyEmail, identity.Email))
		}
		return identity, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err), fields...)
	}
	if identity.Email == "" {
		return identity, appLogger.InvariantError("verified_email_identity_is_known", apperrors.ErrInternalError,
			zap.String(appLogger.KeyToken, token),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
			zap.Uint64(appLogger.KeyUserID, identity.UserID),
			zap.String(logkeys.Stage, "mark_email_verified"),
			zap.String(logkeys.Expected, "normalized verified email"),
			zap.String(logkeys.Actual, "empty email"))
	}

	if h.userCallbacks != nil && h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnEmailVerified != nil {
		if callbackErr := h.userCallbacks.SyncCallbacks.OnEmailVerified(ctx, common_service_models.UserIdType(tokenInfo.UserID), true); callbackErr != nil {
			return identity, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, callbackErr),
				zap.String(appLogger.KeyToken, token),
				zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
				zap.Uint64(appLogger.KeyUserID, tokenInfo.UserID),
				zap.String(appLogger.KeyEmail, identity.Email),
				zap.String(logkeys.Stage, "email_verified_callback"))
		}
	}

	return identity, nil
}

// LoginWithEmail executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) LoginWithEmail(ctx context.Context, email, password string) (*U, string, error) {
	// Get core user by email first
	coreUser, err := h.persistenceHandler.GetUserByEmail(ctx, email)
	if err != nil {
		loginErr := error(apperrors.ErrInvalidCredentials)
		if !errors.Is(err, persistence.ErrUserNotFound) {
			loginErr = appLogger.WithErrorLevel(errors.Join(loginErr, err), zapcore.ErrorLevel)
		}
		return nil, "", appLogger.WithErrorFields(loginErr,
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}

	// Then get the application user via callback
	user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(coreUser.ID))
	if err != nil {
		return nil, "", appLogger.InvariantError("core_user_has_application_user", errors.Join(apperrors.ErrInvalidCredentials, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, coreUser.ID),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "lookup_application_user"),
			zap.String(logkeys.Expected, "application user exists for core user"),
			zap.String(logkeys.Actual, "application user lookup failed"))
	}

	// Get email verification
	ev, err := h.persistenceHandler.GetEmailVerificationByUserID(ctx, user.GetID())
	if err != nil {
		return nil, "", appLogger.InvariantError("email_auth_user_has_verification_record", errors.Join(apperrors.ErrInvalidCredentials, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "load_email_verification"),
			zap.String(logkeys.Expected, "email verification record exists"),
			zap.String(logkeys.Actual, "email verification lookup failed"))
	}

	// Check if the current email is verified
	if !emailVerificationMatchesEmail(ev, email) {
		return nil, "", appLogger.WithErrorFields(apperrors.ErrEmailNotVerified,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}

	// Get user password
	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, user.GetID())
	if err != nil {
		return nil, "", appLogger.WithErrorLevel(errors.Join(apperrors.ErrInvalidCredentials, err), zapcore.ErrorLevel,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}
	if userPassword == nil {
		return nil, "", appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}

	// Verify password
	salt, err := base64.StdEncoding.DecodeString(userPassword.PasswordSalt)
	if err != nil {
		return nil, "", appLogger.InvariantError("password_salt_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.String(logkeys.Stage, "decode_password_salt"),
			zap.String(logkeys.Expected, "valid base64 password salt"),
			zap.String(logkeys.Actual, "password salt decode failed"))
	}

	expectedHash, err := base64.StdEncoding.DecodeString(userPassword.PasswordHash)
	if err != nil {
		return nil, "", appLogger.InvariantError("password_hash_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.String(logkeys.Stage, "decode_password_hash"),
			zap.String(logkeys.Expected, "valid base64 password hash"),
			zap.String(logkeys.Actual, "password hash decode failed"))
	}

	passwordHash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	if subtle.ConstantTimeCompare(passwordHash, expectedHash) != 1 {
		return nil, "", appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}

	// Reject banned accounts only after password verification so account status
	// is not leaked to callers who do not hold the credentials.
	if user.IsBanned() {
		return nil, "", appLogger.WithErrorLevel(apperrors.ErrUserBanned, zapcore.WarnLevel,
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "authorize_email_login"))
	}

	// Generate session token for authentication
	token, err := h.tokenService.GenerateSessionToken(ctx, user.GetID(), tokenService.SessionAuthMethod{Provider: proto_models.AuthProvider_AUTH_PROVIDER_EMAIL})
	if err != nil {
		return nil, "", appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "generate_session_token"))
	}

	return &user, token, nil
}

// CreatePasswordResetToken executes this step in the email backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by CreatePasswordResetToken.
//   - email: string input consumed by CreatePasswordResetToken.
//   - frontendOrigin: validated frontend origin for the reset link ("" = default FrontendURL).
func (h *EmailAuthHandler[U]) CreatePasswordResetToken(ctx context.Context, email, frontendOrigin string) error {
	// Get core user by email first
	coreUser, err := h.persistenceHandler.GetUserByEmail(ctx, email)
	if err != nil {
		// Don't reveal if email exists
		return nil
	}

	// Get email verification to ensure the current email is verified.
	ev, err := h.persistenceHandler.GetEmailVerificationByUserID(ctx, uint64(coreUser.ID))
	if err != nil || !emailVerificationMatchesEmail(ev, email) {
		// Don't reveal if email exists
		return nil
	}

	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, uint64(coreUser.ID))
	if err != nil || userPassword == nil {
		// Don't reveal if email exists
		return nil
	}

	// Generate reset token using token service
	view := h.runtimeConfig()
	token, err := h.tokenService.GenerateSecurityToken(ctx, tokenService.SecurityTokenParams{
		UserID:         common_service_models.UserIdType(coreUser.ID),
		TokenType:      proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET,
		AuthMethod:     proto_models.AuthProvider_AUTH_PROVIDER_EMAIL,
		ExpiresIn:      view.VerificationTokenValidity(),
		FrontendOrigin: frontendOrigin,
	})
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET),
			zap.String(logkeys.Stage, "generate_reset_token"))
	}

	// Send password reset email
	return h.sendPasswordResetEmail(ctx, view, email, token, frontendOrigin)
}

// ResetPasswordWithToken executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) ResetPasswordWithToken(ctx context.Context, token, newPassword string) (uint64, []string, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return 0, nil, apperrors.ErrTransactionNotFound
	}
	// Consume the token in the same transaction as the password update, locking the account
	// first. A reset token is single-use: if the password change does not commit, neither does
	// the consumption, and two concurrent resets with the same token serialize on the account
	// lock so only one mutates the password and receives an auto-login session.
	tokenInfo, err := h.tokenService.ConsumeSecurityToken(ctx, token, proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET)
	if err != nil {
		return 0, nil, appLogger.WithErrorFields(errors.Join(mapSecurityTokenError(err), err),
			zap.String(appLogger.KeyToken, token),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET),
			zap.String(logkeys.Stage, "consume_reset_token"))
	}

	// Generate new password hash
	salt := make([]byte, SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return 0, nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, tokenInfo.UserID),
			zap.String(logkeys.Stage, "generate_password_salt"))
	}

	passwordHash := argon2.IDKey([]byte(newPassword), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	// Update password
	if err := h.persistenceHandler.UpdateUserPassword(ctx, tokenInfo.UserID,
		base64.StdEncoding.EncodeToString(passwordHash),
		base64.StdEncoding.EncodeToString(salt)); err != nil {
		return 0, nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, tokenInfo.UserID),
			zap.String(logkeys.Stage, "update_password"))
	}

	// Revoke every existing session in the same transaction as the password
	// update. Return the raw token values so the service facade can invalidate
	// its in-process token cache only after the transaction commits.
	revokedTokens, err := h.persistenceHandler.RevokeAllSessionTokensForUserAndReturnTokens(ctx, tokenInfo.UserID)
	if err != nil {
		return 0, nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, tokenInfo.UserID),
			zap.String(logkeys.Stage, "revoke_existing_sessions"))
	}

	return tokenInfo.UserID, revokedTokens, nil
}

// ChangePassword executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) ChangePassword(ctx context.Context, userID uint64, oldPassword, newPassword string) ([]string, error) {
	// Get user password
	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, userID)
	if err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_password"))
	}
	if userPassword == nil {
		return nil, appLogger.WithErrorFields(apperrors.ErrPasswordAuthRequired,
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	// Verify old password
	salt, err := base64.StdEncoding.DecodeString(userPassword.PasswordSalt)
	if err != nil {
		return nil, appLogger.InvariantError("password_salt_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "decode_password_salt"),
			zap.String(logkeys.Expected, "valid base64 password salt"),
			zap.String(logkeys.Actual, "password salt decode failed"))
	}

	expectedHash, err := base64.StdEncoding.DecodeString(userPassword.PasswordHash)
	if err != nil {
		return nil, appLogger.InvariantError("password_hash_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "decode_password_hash"),
			zap.String(logkeys.Expected, "valid base64 password hash"),
			zap.String(logkeys.Actual, "password hash decode failed"))
	}

	oldPasswordHash := argon2.IDKey([]byte(oldPassword), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	if subtle.ConstantTimeCompare(oldPasswordHash, expectedHash) != 1 {
		return nil, appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL))
	}

	// Generate new password hash
	newSalt := make([]byte, SaltLength)
	if _, err := rand.Read(newSalt); err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "generate_password_salt"))
	}

	newPasswordHash := argon2.IDKey([]byte(newPassword), newSalt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	// Update password
	if err := h.persistenceHandler.UpdateUserPassword(ctx, userID,
		base64.StdEncoding.EncodeToString(newPasswordHash),
		base64.StdEncoding.EncodeToString(newSalt)); err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "update_password"))
	}

	// Session revocation is part of the password-change transaction. A password
	// change must not commit while any old session remains valid.
	revokedTokens, err := h.persistenceHandler.RevokeAllSessionTokensForUserAndReturnTokens(ctx, userID)
	if err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "revoke_existing_sessions"))
	}

	return revokedTokens, nil
}

// formatDuration executes this step in the email backend flow.
func formatDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60

	var parts []string
	if hours > 0 {
		if hours == 1 {
			parts = append(parts, "1 hour")
		} else {
			parts = append(parts, fmt.Sprintf("%d hours", hours))
		}
	}

	if minutes > 0 {
		if minutes == 1 {
			parts = append(parts, "1 minute")
		} else {
			parts = append(parts, fmt.Sprintf("%d minutes", minutes))
		}
	}

	if len(parts) == 0 {
		return "less than a minute"
	}

	return strings.Join(parts, " and ")
}

// frontendBase selects the base URL for email links: the validated per-request
// frontend origin when one was provided, otherwise the configured default FrontendURL.
func frontendBase(view RuntimeConfigView, frontendOrigin string) string {
	defaultURL := view.FrontendURL()
	if frontendOrigin == "" {
		return defaultURL
	}
	if defaultOrigin, err := origin.FrontendURLOrigin(defaultURL); err == nil && defaultOrigin == frontendOrigin {
		return defaultURL
	}
	return frontendOrigin
}

// sendVerificationEmail executes this step in the email backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by sendVerificationEmail.
//   - email: string input consumed by sendVerificationEmail.
//   - token: string input consumed by sendVerificationEmail.
//   - frontendOrigin: validated frontend origin for the link ("" = default FrontendURL).
func (h *EmailAuthHandler[U]) sendVerificationEmail(ctx context.Context, view RuntimeConfigView, email, token, frontendOrigin string) error {
	verificationURL := fmt.Sprintf("%s%s?token=%s", frontendBase(view, frontendOrigin), EmailVerificationPath, token)

	subject := "Verify your email address"
	body := fmt.Sprintf(`
		<h2>Email Verification</h2>
		<p>Please click the link below to verify your email address:</p>
		<p><a href="%s">Verify Email</a></p>
		<p>If you didn't create an account, please ignore this email.</p>
		<p>This link will expire in %s.</p>
	`, verificationURL, formatDuration(view.VerificationTokenValidity()))

	return h.emailClient.SendHTMLEmail(ctx, email, subject, body)
}

// sendPasswordResetEmail executes this step in the email backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by sendPasswordResetEmail.
//   - email: string input consumed by sendPasswordResetEmail.
//   - token: string input consumed by sendPasswordResetEmail.
//   - frontendOrigin: validated frontend origin for the link ("" = default FrontendURL).
func (h *EmailAuthHandler[U]) sendPasswordResetEmail(ctx context.Context, view RuntimeConfigView, email, token, frontendOrigin string) error {
	resetURL := fmt.Sprintf("%s%s?token=%s", frontendBase(view, frontendOrigin), PasswordResetPath, token)

	subject := "Password Reset Request"
	body := fmt.Sprintf(`
		<h2>Password Reset</h2>
		<p>You requested to reset your password. Click the link below to continue:</p>
		<p><a href="%s">Reset Password</a></p>
		<p>If you didn't request this, please ignore this email.</p>
		<p>This link will expire in %s.</p>
	`, resetURL, formatDuration(view.VerificationTokenValidity()))

	return h.emailClient.SendHTMLEmail(ctx, email, subject, body)
}

// sendPasswordAddedEmail executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) sendPasswordAddedEmail(ctx context.Context, email, firstName string) error {
	subject := "Password Added to Your Account"
	body := fmt.Sprintf(`
		<html>
		<body>
			<h2>Password Authentication Added</h2>
			<p>Hello %s,</p>
			<p>A password has been successfully added to your account. You can now sign in using either:</p>
			<ul>
				<li>Your email address and password</li>
				<li>Your connected social accounts</li>
			</ul>
			<p><strong>If you didn't make this change, please contact support immediately and change your password.</strong></p>
			<p>For your security, we recommend:</p>
			<ul>
				<li>Using a strong, unique password</li>
				<li>Enabling two-factor authentication if available</li>
				<li>Regularly reviewing your account activity</li>
			</ul>
			<p>Best regards,<br>The %s Team</p>
		</body>
		</html>
	`, firstName, h.runtimeConfig().FrontendURL())

	return h.emailClient.SendHTMLEmail(ctx, email, subject, body)
}

// SendAuthMethodChangeEmail executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) SendAuthMethodChangeEmail(ctx context.Context, email, firstName, changeDescription string) error {
	subject := "Authentication Method Changed"
	body := fmt.Sprintf(`
		<html>
		<body>
			<h2>Account Security Update</h2>
			<p>Hello %s,</p>
			<p>We're writing to inform you that the authentication methods for your account have been updated:</p>
			<p><strong>%s</strong></p>
			<p>This change was made on %s.</p>
			<p><strong>If you didn't make this change:</strong></p>
			<ol>
				<li>Secure your account immediately by changing your password</li>
				<li>Review your recent account activity</li>
				<li>Contact our support team</li>
			</ol>
			<p>Your account security is important to us. We send these notifications to help you stay informed about changes to your account.</p>
			<p>Best regards,<br>The %s Team</p>
		</body>
		</html>
	`, firstName, changeDescription, time.Now().UTC().Format("January 2, 2006 at 3:04 PM MST"), h.runtimeConfig().FrontendURL())

	return h.emailClient.SendHTMLEmail(ctx, email, subject, body)
}

// Stop executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) Stop() {
	if appLogger.DebugEnabled(h.logger) {
		h.logger.Named(emailAuthLoggerName).Debug("Email auth handler stopped")
	}
}

// HashPassword executes this step in the email backend flow.
func HashPassword(password string, salt []byte) []byte {
	return argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
}

// GenerateSalt executes this step in the email backend flow.
func GenerateSalt() ([]byte, error) {
	salt := make([]byte, SaltLength)
	_, err := rand.Read(salt)
	return salt, err
}

// VerifyPassword executes this step in the email backend flow.
func VerifyPassword(password string, salt, expectedHash []byte) bool {
	passwordHash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
	return subtle.ConstantTimeCompare(passwordHash, expectedHash) == 1
}

func (h *EmailAuthHandler[U]) getRequiredUserPassword(
	ctx context.Context,
	userID uint64,
	log *zap.Logger,
	operation string,
) (*common_persistence_models.UserPassword, error) {
	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, userID)
	if err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "load_user_password"))
	}

	if userPassword == nil {
		if ce := log.Check(zap.DebugLevel, "Password authentication required"); ce != nil {
			ce.Write(
				zap.String(logkeys.Operation, operation),
				zap.Uint64(appLogger.KeyUserID, userID),
			)
		}
		return nil, apperrors.ErrPasswordAuthRequired
	}

	return userPassword, nil
}

func (h *EmailAuthHandler[U]) verifyCurrentPassword(
	ctx context.Context,
	userID uint64,
	currentPassword string,
	log *zap.Logger,
	operation string,
) (*common_persistence_models.UserPassword, error) {
	userPassword, err := h.getRequiredUserPassword(ctx, userID, log, operation)
	if err != nil {
		return nil, err
	}

	salt, err := base64.StdEncoding.DecodeString(userPassword.PasswordSalt)
	if err != nil {
		return nil, appLogger.InvariantError("password_salt_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "decode_password_salt"),
			zap.String(logkeys.Expected, "valid base64 password salt"),
			zap.String(logkeys.Actual, "password salt decode failed"))
	}

	expectedHash, err := base64.StdEncoding.DecodeString(userPassword.PasswordHash)
	if err != nil {
		return nil, appLogger.InvariantError("password_hash_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "decode_password_hash"),
			zap.String(logkeys.Expected, "valid base64 password hash"),
			zap.String(logkeys.Actual, "password hash decode failed"))
	}

	if !VerifyPassword(currentPassword, salt, expectedHash) {
		if ce := log.Check(zap.DebugLevel, "Invalid password"); ce != nil {
			ce.Write(zap.String(logkeys.Operation, operation))
		}
		return nil, apperrors.ErrInvalidCredentials
	}

	return userPassword, nil
}

// AddPasswordAuthentication executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) AddPasswordAuthentication(
	ctx context.Context,
	userID uint64,
	password string,
) error {
	log := appLogger.For(ctx, h.logger, emailAuthLoggerName).With(
		zap.Uint64(appLogger.KeyUserID, userID),
	)
	log.Debug("AddPasswordAuthentication called")

	// Check if user exists via callback
	user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		return appLogger.WithErrorLevel(errors.Join(apperrors.ErrUserNotFound, err), zapcore.ErrorLevel,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_for_password_addition"))
	}

	// Check if password auth already exists using new method
	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "check_existing_password_authentication"))
	}

	if userPassword != nil {
		return appLogger.WithErrorFields(apperrors.ErrPasswordAuthAlreadyExists,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "check_existing_password_authentication"))
	}

	// Password validation is handled by the sanitizer in the API layer
	// This ensures consistency with initial registration

	// Generate password hash and salt
	salt := make([]byte, SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "generate_password_salt"))
	}

	passwordSalt := base64.StdEncoding.EncodeToString(salt)
	passwordHash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
	passwordHashStr := base64.StdEncoding.EncodeToString(passwordHash)

	// Create password record using new method
	err = h.persistenceHandler.CreateUserPassword(ctx, userID, passwordHashStr, passwordSalt)
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "persist_user_password"))
	}

	// Send confirmation email
	if h.emailClient != nil {
		go func() {
			if err := h.sendPasswordAddedEmail(context.Background(), user.GetEmail(), user.GetFirstName()); err != nil {
				log.Warn("failed to send password added email",
					zap.Error(err),
					zap.String(appLogger.KeyEmail, user.GetEmail()),
					zap.String(logkeys.Stage, "send_password_added_notification"),
					zap.String(logkeys.Fallback, "keep completed password addition without notification"))
			}
		}()
	}

	log.Debug("password authentication added successfully")
	return nil
}

// LoginWithEmailAndOTP executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) LoginWithEmailAndOTP(ctx context.Context, email, password, otpCode string) (*U, string, bool, error) {
	fields := []zap.Field{
		zap.String(appLogger.KeyEmail, email),
		zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
		zap.Bool(appLogger.KeyOTPProvided, otpCode != ""),
	}
	// Get core user by email first
	coreUser, err := h.persistenceHandler.GetUserByEmail(ctx, email)
	if err != nil {
		loginErr := error(apperrors.ErrInvalidCredentials)
		if !errors.Is(err, persistence.ErrUserNotFound) {
			loginErr = appLogger.WithErrorLevel(errors.Join(loginErr, err), zapcore.ErrorLevel)
		}
		return nil, "", false, appLogger.WithErrorFields(loginErr, fields...)
	}

	// Then get the application user via callback
	user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(coreUser.ID))
	if err != nil {
		return nil, "", false, appLogger.InvariantError("core_user_has_application_user", errors.Join(apperrors.ErrInvalidCredentials, err),
			append(fields,
				zap.Uint64(appLogger.KeyUserID, coreUser.ID),
				zap.String(logkeys.Stage, "lookup_application_user"),
				zap.String(logkeys.Expected, "application user exists for core user"),
				zap.String(logkeys.Actual, "application user lookup failed"))...)
	}

	// Get email verification
	ev, err := h.persistenceHandler.GetEmailVerificationByUserID(ctx, user.GetID())
	if err != nil {
		return nil, "", false, appLogger.InvariantError("email_auth_user_has_verification_record", errors.Join(apperrors.ErrInvalidCredentials, err),
			append(fields,
				zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
				zap.String(logkeys.Stage, "load_email_verification"),
				zap.String(logkeys.Expected, "email verification record exists"),
				zap.String(logkeys.Actual, "email verification lookup failed"))...)
	}

	// Check if the current email is verified
	if !emailVerificationMatchesEmail(ev, email) {
		return nil, "", false, appLogger.WithErrorFields(apperrors.ErrEmailNotVerified,
			append(fields, zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())))...)
	}

	// Get user password
	userPassword, err := h.persistenceHandler.GetUserPassword(ctx, user.GetID())
	if err != nil {
		return nil, "", false, appLogger.WithErrorLevel(errors.Join(apperrors.ErrInvalidCredentials, err), zapcore.ErrorLevel,
			append(fields, zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())))...)
	}
	if userPassword == nil {
		return nil, "", false, appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
			append(fields, zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())))...)
	}

	// Verify password first
	salt, err := base64.StdEncoding.DecodeString(userPassword.PasswordSalt)
	if err != nil {
		return nil, "", false, appLogger.InvariantError("password_salt_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.String(logkeys.Stage, "decode_password_salt"),
			zap.String(logkeys.Expected, "valid base64 password salt"),
			zap.String(logkeys.Actual, "password salt decode failed"))
	}

	expectedHash, err := base64.StdEncoding.DecodeString(userPassword.PasswordHash)
	if err != nil {
		return nil, "", false, appLogger.InvariantError("password_hash_is_valid_base64", errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.String(logkeys.Stage, "decode_password_hash"),
			zap.String(logkeys.Expected, "valid base64 password hash"),
			zap.String(logkeys.Actual, "password hash decode failed"))
	}

	passwordHash := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	if subtle.ConstantTimeCompare(passwordHash, expectedHash) != 1 {
		return nil, "", false, appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
			append(fields, zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())))...)
	}

	// Reject banned accounts once the password is verified, before prompting for OTP.
	if user.IsBanned() {
		return nil, "", false, appLogger.WithErrorLevel(apperrors.ErrUserBanned, zapcore.WarnLevel,
			append(fields,
				zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
				zap.String(logkeys.Stage, "authorize_email_otp_login"))...)
	}

	// Password is correct - now check OTP if enabled
	if userPassword.OTPEnabled {
		if otpCode == "" {
			// OTP is enabled but no code provided - request OTP
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, emailAuthLoggerName).With(fields...).Debug("OTP required but not provided")
			}
			return nil, "", true, nil
		}

		// Verify OTP code
		if userPassword.OTPSecret == nil || *userPassword.OTPSecret == "" {
			return nil, "", false, appLogger.InvariantError("otp_enabled_requires_secret", apperrors.ErrInternalError,
				zap.String(appLogger.KeyEmail, email),
				zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
				zap.String(logkeys.Stage, "verify_login_otp"),
				zap.Bool(logkeys.Expected, true),
				zap.String(logkeys.Actual, "OTP enabled with empty secret"))
		}

		if !h.VerifyTOTP(*userPassword.OTPSecret, otpCode) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, emailAuthLoggerName).With(fields...).Debug("Invalid OTP code provided")
			}
			return nil, "", false, appLogger.WithErrorFields(apperrors.ErrInvalidCredentials,
				append(fields, zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())))...)
		}
	}

	// Generate session token for authentication
	token, err := h.tokenService.GenerateSessionToken(ctx, user.GetID(), tokenService.SessionAuthMethod{Provider: proto_models.AuthProvider_AUTH_PROVIDER_EMAIL})
	if err != nil {
		return nil, "", false, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(user.GetID())),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""),
			zap.String(logkeys.Stage, "generate_session_token"))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, emailAuthLoggerName).With(fields...).Debug("Login successful", zap.Bool(appLogger.KeyOTPUsed, userPassword.OTPEnabled))
	}
	return &user, token, false, nil
}

// SetupOTP executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) SetupOTP(ctx context.Context, userID uint64, currentPassword string) (secret, qrCodeURL string, err error) {
	view := h.runtimeConfig()
	log := appLogger.For(ctx, h.logger, emailAuthLoggerName).With(zap.Uint64(appLogger.KeyUserID, userID))

	// Get user via callback to retrieve email for issuer
	user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		return "", "", appLogger.WithErrorLevel(errors.Join(apperrors.ErrUserNotFound, err), zapcore.ErrorLevel,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_for_otp_setup"))
	}

	// Verify current password before generating a new TOTP secret.
	userPassword, err := h.verifyCurrentPassword(ctx, userID, currentPassword, log, "otp_setup")
	if err != nil {
		return "", "", err
	}

	if userPassword.OTPEnabled {
		return "", "", appLogger.WithErrorFields(apperrors.ErrOTPAlreadyEnabled,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "otp_setup"))
	}

	// Generate new TOTP key
	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      view.OTPIssuer(),
		AccountName: user.GetEmail(),
		Period:      30,            // 30 second period (standard)
		Digits:      otp.DigitsSix, // 6 digit codes
		Algorithm:   otpAlgorithm,  // Match verification settings.
	})
	if err != nil {
		return "", "", appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()),
			zap.String(logkeys.Stage, "generate_totp_key"))
	}

	// Store the secret temporarily (not enabled yet)
	err = h.persistenceHandler.UpdateUserOTPSecret(ctx, userID, key.Secret())
	if err != nil {
		return "", "", appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.GetEmail()),
			zap.String(logkeys.Stage, "persist_otp_secret"))
	}

	log.Debug("OTP setup initiated")
	return key.Secret(), key.URL(), nil
}

// VerifyOTPSetup executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) VerifyOTPSetup(ctx context.Context, userID uint64, otpCode, currentPassword string) error {
	log := appLogger.For(ctx, h.logger, emailAuthLoggerName).With(zap.Uint64(appLogger.KeyUserID, userID))

	// Verify current password before enabling the pending TOTP secret.
	userPassword, err := h.verifyCurrentPassword(ctx, userID, currentPassword, log, "otp_verify_setup")
	if err != nil {
		return err
	}

	if userPassword.OTPEnabled {
		return appLogger.WithErrorFields(apperrors.ErrOTPAlreadyEnabled,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "otp_verify_setup"))
	}

	if userPassword.OTPSecret == nil || *userPassword.OTPSecret == "" {
		return appLogger.WithErrorFields(apperrors.ErrOTPNotConfigured,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "otp_verify_setup"))
	}

	// Verify the OTP code
	if !h.VerifyTOTP(*userPassword.OTPSecret, otpCode) {
		return appLogger.WithErrorFields(apperrors.ErrInvalidOTPCode,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Bool(appLogger.KeyOTPProvided, true),
			zap.String(logkeys.Operation, "otp_verify_setup"))
	}

	// Enable OTP
	now := time.Now().UTC()
	err = h.persistenceHandler.EnableUserOTP(ctx, userID, &now)
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "enable_otp"))
	}

	log.Debug("OTP setup completed successfully")
	return nil
}

// DisableOTP executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) DisableOTP(ctx context.Context, userID uint64, currentPassword string) error {
	log := appLogger.For(ctx, h.logger, emailAuthLoggerName).With(zap.Uint64(appLogger.KeyUserID, userID))

	// Verify current password
	userPassword, err := h.verifyCurrentPassword(ctx, userID, currentPassword, log, "otp_disable")
	if err != nil {
		return err
	}

	if !userPassword.OTPEnabled {
		return appLogger.WithErrorFields(apperrors.ErrOTPNotEnabled,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "otp_disable"))
	}

	// Disable OTP and clear secret
	err = h.persistenceHandler.DisableUserOTP(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "disable_otp"))
	}

	log.Debug("OTP disabled successfully")
	return nil
}

// VerifyTOTP executes this step in the email backend flow.
func (h *EmailAuthHandler[U]) VerifyTOTP(secret, code string) bool {
	// Validate with the same algorithm and token shape used during setup.
	valid, _ := totp.ValidateCustom(code, secret, time.Now().UTC(), totp.ValidateOpts{
		Period:    30,
		Skew:      1,
		Digits:    otp.DigitsSix,
		Algorithm: otpAlgorithm,
	})
	return valid
}
