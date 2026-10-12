package common_service

import (
	"context"
	"errors"
	"time"

	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Authentication Operations
// These methods handle user authentication, login, OAuth, OTP, and password management

// ValidateAuthTokenAndGetUser executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) ValidateAuthTokenAndGetUser(ctx context.Context, token string) (*U, error) {
	// Delegate the call to the actual auth handler
	user, err := h.authHandler.GetUserFromAuthToken(ctx, token) // Now returns T

	// Record token validation metrics if metrics handler is available
	if h.metricsHandler != nil {
		h.metricsHandler.RecordTokenValidation(err == nil)
		// Note: Debug logging for token validation metrics (no context available)
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Auth token validation metrics recorded",
				zap.Bool(appLogger.KeyValid, err == nil))
		}
	}

	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyToken, token),
			zap.String(logkeys.Stage, "validate_auth_token_and_get_user"))
	}

	return user, nil
}

// GetUserByID executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) GetUserByID(ctx context.Context, userID common_service_models.UserIdType) (*U, error) {
	user, err := h.authHandler.GetUserByID(ctx, userID)
	if err != nil {
		return nil, err
	}

	return user, nil
}

// GetPersonalWorkspaceRootID returns the user's active personal workspace root
// organization ID for request-path enrichment, or nil when organizations are
// disabled, no active root exists (unprovisioned or archived), or the
// workspace is conflicted — persistent corruption is logged and absorbed as
// absence so it cannot lock the user out of their own profile endpoint.
// Transient lookup failures are returned as errors so callers never mistake
// them for "no workspace".
func (h *CommonServiceHandler[U]) GetPersonalWorkspaceRootID(
	ctx context.Context,
	userID common_service_models.UserIdType,
) (*common_service_models.OrganizationIdType, error) {
	if !h.personalWorkspacesEnabled || h.personalWorkspaceService == nil {
		return nil, nil
	}
	rootID, err := h.personalWorkspaceService.RootIDForUser(ctx, userID)
	if err != nil && errors.Is(err, apperrors.ErrPersonalWorkspaceConflict) {
		// A conflicted workspace is persistent corruption awaiting lifecycle
		// repair — it must not lock the user out of their own profile
		// endpoint. Reporting absence stays truthful (no active root is
		// available), and the classification is never cached, so a repair
		// becomes visible on the next request.
		appLogger.For(ctx, h.logger, loggerName).Error(
			"Personal workspace conflict during profile enrichment; omitting workspace root",
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "resolve_personal_workspace_root"),
			zap.String(logkeys.Fallback, "omit conflicted workspace root from profile"),
			zap.Error(err),
		)
		return nil, nil
	}
	return rootID, err
}

// checkLoginRateLimit enforces the login rate limit for the given email.
// It returns apperrors.ErrRateLimitExceeded when the limit is exceeded and nil
// when no rate limiter is configured or the limiter itself fails (login proceeds).
func (h *CommonServiceHandler[U]) checkLoginRateLimit(ctx context.Context, email string) error {
	if h.rateLimiter == nil {
		return nil
	}
	allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionLogin, email)
	if err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("Login proceeding without rate-limit enforcement",
			zap.Error(err),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAction, rate_limiter.ActionLogin),
			zap.String(logkeys.Stage, "check_rate_limit"),
			zap.String(logkeys.Fallback, "allow login attempt"))
		// Continue with login even if rate limiter fails
		return nil
	}
	if !allowed {
		return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
			zap.Stringer(appLogger.KeyAction, rate_limiter.ActionLogin),
			zap.String(appLogger.KeyEmail, email),
			appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
	}
	return nil
}

// recordLoginFailure records the rate-limiter failure and auth attempt metrics
// for a failed email login.
func (h *CommonServiceHandler[U]) recordLoginFailure(ctx context.Context, email string, err error) {
	// Record failure if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordFailure(ctx, rate_limiter.ActionLogin, email); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record login failure",
				zap.Error(recordErr), zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionLogin),
				zap.String(logkeys.Stage, "record_rate_limit_failure"),
				zap.String(logkeys.Fallback, "continue failed login response"))
		}
	}
	// Record auth attempt metrics
	if h.metricsHandler != nil {
		failureReason := "invalid_credentials"
		switch err {
		case apperrors.ErrEmailNotVerified:
			failureReason = "email_not_verified"
		case apperrors.ErrUserNotFound:
			failureReason = "user_not_found"
		}
		h.metricsHandler.RecordAuthAttempt("email", false, failureReason)
	}
}

// recordLoginSuccess records the rate-limiter success and auth/token metrics
// for a successful email login.
func (h *CommonServiceHandler[U]) recordLoginSuccess(ctx context.Context, email string) {
	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionLogin, email); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record login success",
				zap.Error(recordErr), zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionLogin),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue successful login"))
		}
	}
	// Record successful auth attempt and token generation metrics
	if h.metricsHandler != nil {
		h.metricsHandler.RecordAuthAttempt("email", true, "")
		h.metricsHandler.RecordTokenGeneration()
	}
}

// LoginUserByEmail executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) LoginUserByEmail(ctx context.Context, email, password string) (*U, string, error) {
	if !h.emailAuthenticationEnabled() {
		return nil, "", apperrors.ErrEmailAuthDisabled
	}

	if err := h.checkLoginRateLimit(ctx, email); err != nil {
		return nil, "", err
	}

	user, authToken, err := h.authHandler.LoginUserByEmail(ctx, email, password)
	if err != nil {
		h.recordLoginFailure(ctx, email, err)
		return nil, "", appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Bool(appLogger.KeyOTPProvided, false))
	}

	h.recordLoginSuccess(ctx, email)
	return user, authToken, nil
}

// LoginUserByEmailWithOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) LoginUserByEmailWithOTP(ctx context.Context, email, password, otpCode string) (*U, string, bool, error) {
	if !h.emailAuthenticationEnabled() {
		return nil, "", false, apperrors.ErrEmailAuthDisabled
	}

	if err := h.checkLoginRateLimit(ctx, email); err != nil {
		return nil, "", false, err
	}

	user, authToken, otpRequired, err := h.authHandler.LoginUserByEmailWithOTP(ctx, email, password, otpCode)
	if err != nil {
		h.recordLoginFailure(ctx, email, err)
		return nil, "", false, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""))
	}

	// If OTP is required, don't record as success yet
	if otpRequired {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("OTP required for login", zap.String(appLogger.KeyEmail, email))
		}
		return nil, "", true, nil
	}

	h.recordLoginSuccess(ctx, email)
	return user, authToken, false, nil
}

// oauthMetricsUnknownProvider labels OAuth attempts whose state never
// verified, keeping metric labels bounded by configured provider slugs.
const oauthMetricsUnknownProvider = "unknown"

// LoginUserByOAuth completes an OAuth callback: it verifies the state and
// browser binding, links or creates the account, and mints a session. Each
// call counts toward the client IP's OAuth callback rate limit. It records an
// auth attempt labeled with the provider slug.
func (h *CommonServiceHandler[U]) LoginUserByOAuth(ctx context.Context, callback auth.OAuthCallback) (auth.OAuthLoginResult[U], error) {
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionOAuthCallback); err != nil {
		if h.metricsHandler != nil {
			h.metricsHandler.RecordAuthAttempt(oauthMetricsUnknownProvider, false, "rate_limited")
		}
		return auth.OAuthLoginResult[U]{}, err
	}
	result, err := h.authHandler.LoginUserByOAuth(ctx, callback)
	slug := result.ProviderSlug
	if slug == "" {
		slug = oauthMetricsUnknownProvider
	}
	if err != nil {
		// Record auth attempt metrics
		if h.metricsHandler != nil {
			failureReason := "oauth_failed"
			switch {
			case errors.Is(err, apperrors.ErrInvalidOAuthState):
				failureReason = "invalid_state"
			case errors.Is(err, apperrors.ErrOAuthProviderUnavailable):
				failureReason = "provider_unavailable"
			case errors.Is(err, apperrors.ErrOAuthServiceUnavailable):
				failureReason = "service_unavailable"
			case errors.Is(err, apperrors.ErrOAuthEmailMissing):
				failureReason = "email_missing"
			case errors.Is(err, apperrors.ErrOAuthEmailNotVerified):
				failureReason = "email_not_verified"
			case errors.Is(err, apperrors.ErrOAuthIdentityConflict):
				failureReason = "identity_conflict"
			case errors.Is(err, apperrors.ErrOAuthSubjectMissing):
				failureReason = "subject_missing"
			case errors.Is(err, apperrors.ErrOAuthFailed):
				failureReason = "provider_error"
			}
			h.metricsHandler.RecordAuthAttempt(slug, false, failureReason)
		}
		return result, err
	}

	user := result.User
	if h.organizationHandler != nil && user != nil {
		if assocErr := h.organizationHandler.AutoAssociateUserWithOrganizationByEmail(ctx, (*user).GetID(), (*user).GetEmail()); assocErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Auto-association via email domain failed (OAuth)",
				zap.Error(assocErr),
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.String(appLogger.KeyEmail, (*user).GetEmail()),
				zap.String(appLogger.KeyProvider, result.ProviderSlug),
				zap.String(logkeys.Operation, "login_user_with_oauth"),
				zap.String(logkeys.Stage, "auto_associate_organization"),
				zap.String(logkeys.Fallback, "continue OAuth login without organization association"))
		}
	}

	// Record successful auth attempt and token generation metrics
	if h.metricsHandler != nil {
		h.metricsHandler.RecordAuthAttempt(slug, true, "")
		h.metricsHandler.RecordTokenGeneration()
	}

	return result, nil
}

// CreateOAuthState starts an OAuth attempt. The API layer binds the returned
// Binding to the initiating browser in an HttpOnly cookie and hands the client
// the state and authorization URL.
func (h *CommonServiceHandler[U]) CreateOAuthState(ctx context.Context, req auth.OAuthStateRequest) (*auth.OAuthAuthorization, error) {
	return h.authHandler.CreateOAuthState(ctx, req)
}

// OAuthFormPostRedirect returns the frontend callback URL a provider's
// form_post response is relayed to. Relays share the client IP's OAuth
// callback rate limit.
func (h *CommonServiceHandler[U]) OAuthFormPostRedirect(ctx context.Context, form auth.OAuthFormPost) (string, error) {
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionOAuthCallback); err != nil {
		return "", err
	}
	return h.authHandler.OAuthFormPostRedirect(ctx, form)
}

// ListOAuthProviders describes the currently configured, valid providers.
func (h *CommonServiceHandler[U]) ListOAuthProviders(ctx context.Context) []auth.OAuthProviderDescriptor {
	return h.authHandler.ListOAuthProviders(ctx)
}

// AddPasswordAuthentication executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) AddPasswordAuthentication(ctx context.Context, userID uint64, password string) error {
	if !h.emailAuthenticationEnabled() {
		return apperrors.ErrEmailAuthDisabled
	}

	// We need to expose this through the main service handler, not the auth handler
	// For now, delegate to the email auth handler if available
	if h.emailAuthHandler != nil {
		// Get user to determine their OAuth provider for metrics
		user, err := h.GetUserByID(ctx, common_service_models.UserIdType(userID))
		provider := "unknown"
		if err == nil && user != nil {
			// Try to get auth methods to find the OAuth provider
			methods, _ := h.GetUserAuthMethods(ctx, userID)
			if methods != nil && len(methods.OAuthProviders) > 0 {
				provider = methods.OAuthProviders[0].ProviderSlug
			}
		}

		err = h.emailAuthHandler.AddPasswordAuthentication(ctx, userID, password)

		// Record metrics
		if h.metricsHandler != nil {
			status := "success"
			if err != nil {
				status = "failure"
			}
			h.metricsHandler.RecordPasswordAddition(provider, status)
		}

		// Invalidate user cache after successful password addition to ensure
		// authentication methods list is immediately updated
		if err == nil && h.authHandler != nil {
			h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
		}

		return err
	}
	return apperrors.ErrInternalError
}

// GetUserAuthMethods executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) GetUserAuthMethods(ctx context.Context, userID uint64) (*common_service_models.UserAuthMethods, error) {
	// Get raw data from persistence layer using the standard converter pattern
	persistenceMethods, err := h.commonPersistenceHandler.GetUserAuthMethods(ctx, userID)
	if err != nil {
		return nil, err
	}

	// Convert persistence model to service model using the converter
	methods := h.converter.PersistenceUserAuthMethodsToService(persistenceMethods)

	// Record metrics
	if h.metricsHandler != nil && methods != nil {
		hasPassword := "false"
		if methods.HasPassword {
			hasPassword = "true"
		}

		// Count OAuth providers
		oauthCount := "0"
		if len(methods.OAuthProviders) == 1 {
			oauthCount = "1"
		} else if len(methods.OAuthProviders) >= 2 {
			oauthCount = "2+"
		}

		h.metricsHandler.RecordAuthMethodQuery(hasPassword, oauthCount)
	}

	return methods, err
}

// RequireRecentAuthentication executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) RequireRecentAuthentication(ctx context.Context, claims *token.SessionTokenClaims, maxAge time.Duration) error {
	return h.authHandler.RequireRecentAuthentication(ctx, claims, maxAge)
}

// SetupOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) SetupOTP(ctx context.Context, userID uint64, currentPassword string) (secret, qrCodeURL string, err error) {
	if !h.emailAuthenticationEnabled() {
		return "", "", apperrors.ErrEmailAuthDisabled
	}

	// Check rate limit if configured
	if h.rateLimiter != nil {
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionOTPSetup, fmt.Sprintf("user:%d", userID))
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("OTP setup proceeding without rate-limit enforcement",
				zap.Error(err), zap.Uint64(appLogger.KeyUserID, userID),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPSetup),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "allow OTP setup"))
			// Continue with setup even if rate limiter fails
		} else if !allowed {
			return "", "", appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPSetup),
				zap.Uint64(appLogger.KeyUserID, userID),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	secret, qrCodeURL, err = h.emailAuthHandler.SetupOTP(ctx, userID, currentPassword)
	if err != nil {
		// Record failure if rate limiter is configured
		if h.rateLimiter != nil {
			if recordErr := h.rateLimiter.RecordFailure(ctx, rate_limiter.ActionOTPSetup, fmt.Sprintf("user:%d", userID)); recordErr != nil {
				appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record OTP setup failure",
					zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, userID),
					zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPSetup),
					zap.String(logkeys.Stage, "record_rate_limit_failure"),
					zap.String(logkeys.Fallback, "continue OTP setup failure response"))
			}
		}
		// Record OTP setup failure metrics
		if h.metricsHandler != nil {
			status := "failure"
			if errors.Is(err, apperrors.ErrRateLimitExceeded) {
				status = "rate_limited"
			}
			h.metricsHandler.RecordOTPSetupAttempt(status)
		}
		return "", "", err
	}

	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionOTPSetup, fmt.Sprintf("user:%d", userID)); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record OTP setup success",
				zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, userID),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPSetup),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP setup"))
		}
	}

	// Record OTP setup success metrics
	if h.metricsHandler != nil {
		h.metricsHandler.RecordOTPSetupAttempt("success")
	}

	return secret, qrCodeURL, nil
}

// VerifyOTPSetup executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) VerifyOTPSetup(ctx context.Context, userID uint64, otpCode, currentPassword string) error {
	if !h.emailAuthenticationEnabled() {
		return apperrors.ErrEmailAuthDisabled
	}

	// Check rate limit if configured
	if h.rateLimiter != nil {
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionOTPVerify, fmt.Sprintf("user:%d", userID))
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("OTP verification proceeding without rate-limit enforcement",
				zap.Error(err), zap.Uint64(appLogger.KeyUserID, userID),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPVerify),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "allow OTP verification"))
			// Continue with verification even if rate limiter fails
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPVerify),
				zap.Uint64(appLogger.KeyUserID, userID),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	err := h.emailAuthHandler.VerifyOTPSetup(ctx, userID, otpCode, currentPassword)
	if err != nil {
		// Record failure if rate limiter is configured
		if h.rateLimiter != nil {
			if recordErr := h.rateLimiter.RecordFailure(ctx, rate_limiter.ActionOTPVerify, fmt.Sprintf("user:%d", userID)); recordErr != nil {
				appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record OTP verify failure",
					zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, userID),
					zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPVerify),
					zap.String(logkeys.Stage, "record_rate_limit_failure"),
					zap.String(logkeys.Fallback, "continue OTP verification failure response"))
			}
		}
		// Record OTP verification failure metrics
		if h.metricsHandler != nil {
			status := "failure"
			if errors.Is(err, apperrors.ErrInvalidOTPCode) {
				status = "invalid_code"
			} else if errors.Is(err, apperrors.ErrRateLimitExceeded) {
				status = "rate_limited"
			}
			h.metricsHandler.RecordOTPVerificationAttempt("setup_verification", status)
		}
		return err
	}

	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionOTPVerify, fmt.Sprintf("user:%d", userID)); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record OTP verify success",
				zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, userID),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionOTPVerify),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP verification"))
		}
	}

	// Record OTP verification success and management operation metrics
	if h.metricsHandler != nil {
		h.metricsHandler.RecordOTPVerificationAttempt("setup_verification", "success")
		h.metricsHandler.RecordOTPManagementOperation("setup", "success")
	}

	// Invalidate user cache after successful OTP enable to ensure
	// authentication requirements are immediately updated
	if h.authHandler != nil {
		h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
	}

	return nil
}

// DisableOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) DisableOTP(ctx context.Context, userID uint64, currentPassword string) error {
	if !h.emailAuthenticationEnabled() {
		return apperrors.ErrEmailAuthDisabled
	}

	err := h.emailAuthHandler.DisableOTP(ctx, userID, currentPassword)
	if err != nil {
		// Record OTP disable failure metrics
		if h.metricsHandler != nil {
			status := "failure"
			if errors.Is(err, apperrors.ErrInvalidCredentials) {
				status = "unauthorized"
			}
			h.metricsHandler.RecordOTPManagementOperation("disable", status)
		}
		return err
	}

	// Record OTP disable success metrics
	if h.metricsHandler != nil {
		h.metricsHandler.RecordOTPManagementOperation("disable", "success")
	}

	// Invalidate user cache after successful OTP disable to ensure
	// authentication requirements are immediately updated
	if h.authHandler != nil {
		h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
	}

	return nil
}
