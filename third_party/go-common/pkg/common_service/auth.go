package common_service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_util/origin"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// validateFrontendOrigin resolves an optional per-request frontend origin against the
// configured allowlist. A nil/empty value selects the default FrontendURL and returns "".
// Untrusted or malformed origins are logged and rejected with ErrUntrustedFrontendOrigin —
// never silently replaced with the default.
func (h *CommonServiceHandler[U]) validateFrontendOrigin(frontendOrigin *string) (string, error) {
	if frontendOrigin == nil || strings.TrimSpace(*frontendOrigin) == "" {
		return "", nil
	}
	var allowlist *origin.Allowlist
	if h.frontendOrigins != nil {
		allowlist = h.frontendOrigins()
	}
	if allowlist == nil {
		// Fail closed: a handler without a configured allowlist accepts no explicit origins.
		return "", appLogger.WithErrorLevel(apperrors.ErrUntrustedFrontendOrigin, zapcore.WarnLevel,
			zap.String(logkeys.Operation, "validate_frontend_origin"),
			zap.String(appLogger.KeyFrontendOrigin, *frontendOrigin),
			zap.String(logkeys.Reason, "frontend origin allowlist is not configured"))
	}
	validated, err := allowlist.Validate(*frontendOrigin)
	if err != nil {
		return "", appLogger.WithErrorLevel(errors.Join(apperrors.ErrUntrustedFrontendOrigin, err), zapcore.WarnLevel,
			zap.String(logkeys.Operation, "validate_frontend_origin"),
			zap.String(appLogger.KeyFrontendOrigin, *frontendOrigin))
	}
	return validated, nil
}

// NewUserEmailRegistration executes this step in the common_service backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by NewUserEmailRegistration.
//   - email: string input consumed by NewUserEmailRegistration.
//   - password: string input consumed by NewUserEmailRegistration.
//   - username: *string input consumed by NewUserEmailRegistration.
//   - firstName: *string input consumed by NewUserEmailRegistration.
//   - lastName: *string input consumed by NewUserEmailRegistration.
//   - frontendOrigin: optional frontend origin for the verification link; must be in the configured allowlist.
func (h *CommonServiceHandler[U]) NewUserEmailRegistration(ctx context.Context, email, password string, username, firstName, lastName, frontendOrigin *string) (*U, error) {
	if !h.emailAuthenticationEnabled() {
		return nil, apperrors.ErrEmailAuthDisabled
	}

	signupConfig := h.runtimeView()
	// Check rate limit first so that requests carrying an unlisted or malformed
	// frontend_origin cannot bypass the throttle (which would allow unbounded
	// allowlist probing and log flooding).
	if h.rateLimiter != nil {
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionRegister, email)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Registration proceeding without rate-limit enforcement",
				zap.Error(err), zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionRegister),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "allow registration attempt"))
			// Continue with registration even if rate limiter fails
		} else if !allowed {
			return nil, appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionRegister),
				zap.String(appLogger.KeyEmail, email),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	// Validate the requested frontend origin before doing any persistence work
	validatedOrigin, err := h.validateFrontendOrigin(frontendOrigin)
	if err != nil {
		return nil, err
	}
	if err := h.authHandler.CheckAccountEmailDomain(ctx, email); err != nil {
		return nil, err
	}

	// Ensure transaction context exists
	tx, txRetrievalSuccess := scontext.GetTransaction(ctx)
	if !txRetrievalSuccess || tx == nil {
		return nil, appLogger.InvariantError("email_registration_requires_transaction", apperrors.ErrTransactionNotFound,
			zap.String(logkeys.Operation, "register_user_by_email"),
			zap.String(logkeys.Stage, "resolve_transaction"),
			zap.String(logkeys.Expected, "transaction in request context"),
			zap.String(logkeys.Actual, "transaction absent"))
	}

	// Check if a user already exists with this email
	coreUser, err := h.commonPersistenceHandler.GetUserByEmail(ctx, email)
	var existingUser U
	if err == nil {
		// Get application user via callback
		existingUser, err = h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(coreUser.ID))
	}

	// Handle based on existence and error type
	if err == nil {
		// Existing user found - check email verification status
		return h.handleExistingUserRegistration(ctx, existingUser, email, validatedOrigin)
	} else if !errors.Is(err, persistence.ErrUserNotFound) {
		// Handle unexpected database errors during the initial check
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to check for existing user: %w", err),
			zap.String(logkeys.Operation, "register_user_by_email"),
			zap.String(logkeys.Stage, "lookup_existing_user"),
			zap.String(appLogger.KeyEmail, email))
	}

	// No user found - create new user with email auth
	return h.createNewUserWithEmailAuthWithConfig(ctx, email, password, username, firstName, lastName, validatedOrigin, signupConfig)
}

// handleExistingUserRegistration executes this step in the common_service backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by handleExistingUserRegistration.
//   - existingUser: U input consumed by handleExistingUserRegistration.
//   - email: string input consumed by handleExistingUserRegistration.
//   - validatedOrigin: allowlist-validated frontend origin for the resent link ("" = default FrontendURL).
func (h *CommonServiceHandler[U]) handleExistingUserRegistration(ctx context.Context, existingUser U, email, validatedOrigin string) (*U, error) {
	// Ensure transaction context exists for verification operations
	if err := h.requireTransaction(ctx, "existing user registration"); err != nil {
		return nil, err
	}

	userID := existingUser.GetID()

	// OAuth-only users should add password auth from an authenticated session.
	// Do not infer this state from a missing verification row: OAuth sign-ins
	// also produce verified email state.
	authMethods, methodErr := h.commonPersistenceHandler.GetUserAuthMethods(ctx, uint64(userID))
	if methodErr == nil && authMethods != nil && len(authMethods.OAuthProviders) > 0 && !authMethods.HasPassword {
		return nil, appLogger.WithErrorFields(apperrors.ErrOAuthUserMustAddPassword,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Int(appLogger.KeyOAuthProviders, len(authMethods.OAuthProviders)))
	}
	if methodErr != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("Failed to inspect auth methods for existing registration",
			zap.Error(methodErr),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "inspect_existing_auth_methods"),
			zap.String(logkeys.Fallback, "continue using email verification state"))
	}

	// Check if email is already verified
	ev, err := h.commonPersistenceHandler.GetEmailVerificationByUserID(ctx, userID)
	if err != nil {
		if errors.Is(err, persistence.ErrEmailVerificationNotFound) {
			return nil, appLogger.InvariantError("registered_email_has_verification_record", errors.Join(apperrors.ErrEmailAlreadyExists, err),
				zap.String(appLogger.KeyEmail, email),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.String(logkeys.Stage, "load_existing_registration"),
				zap.String(logkeys.Expected, "email verification record exists"),
				zap.String(logkeys.Actual, "email verification record missing"))
		}
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrInternalError, err),
			zap.String(logkeys.Stage, "load_existing_email_verification"),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	if ev.VerifiedAt != nil && common_persistence_models.NormalizeUserEmail(ev.Email) == common_persistence_models.NormalizeUserEmail(email) {
		// Email is already registered and verified
		return nil, appLogger.WithErrorFields(apperrors.ErrEmailAlreadyExists,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Email exists but is not verified. Attempt to resend verification.
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Registration attempt for unverified email, attempting to resend verification",
			zap.String(appLogger.KeyEmail, email), zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Resend verification email
	if err := h.emailAuthHandler.ResendVerificationEmail(ctx, uint64(userID), validatedOrigin); err != nil {
		if errors.Is(err, apperrors.ErrVerificationCooldown) {
			return nil, appLogger.WithErrorFields(err,
				zap.String(appLogger.KeyEmail, email),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.String(logkeys.Stage, "resend_verification_email"))
		}
		return nil, appLogger.WithErrorLevel(errors.Join(apperrors.ErrEmailNotVerified, err), zapcore.ErrorLevel,
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "resend_verification_email"))
	}

	// Verification email resent successfully
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Verification email resent successfully",
			zap.String(appLogger.KeyEmail, email), zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert U to *U for return
	ptr := new(U)
	*ptr = existingUser
	return ptr, nil
}

// createNewUserWithEmailAuth executes this step in the common_service backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by createNewUserWithEmailAuth.
//   - email: string input consumed by createNewUserWithEmailAuth.
//   - password: string input consumed by createNewUserWithEmailAuth.
//   - username: *string input consumed by createNewUserWithEmailAuth.
//   - firstName: *string input consumed by createNewUserWithEmailAuth.
//   - lastName: *string input consumed by createNewUserWithEmailAuth.
//   - validatedOrigin: allowlist-validated frontend origin for the verification link ("" = default FrontendURL).
func (h *CommonServiceHandler[U]) createNewUserWithEmailAuth(ctx context.Context, email, password string, username, firstName, lastName *string, validatedOrigin string) (*U, error) {
	return h.createNewUserWithEmailAuthWithConfig(
		ctx, email, password, username, firstName, lastName, validatedOrigin, h.runtimeView(),
	)
}

func (h *CommonServiceHandler[U]) createNewUserWithEmailAuthWithConfig(
	ctx context.Context,
	email, password string,
	username, firstName, lastName *string,
	validatedOrigin string,
	signupConfig config.ServiceView,
) (*U, error) {
	// Ensure transaction context exists for multi-step user creation
	if err := h.requireTransaction(ctx, "user creation via email"); err != nil {
		return nil, err
	}
	if h.accountLifecycleCoordinator != nil {
		if err := h.accountLifecycleCoordinator.RequireTransaction(ctx); err != nil {
			return nil, err
		}
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Registering new user via email"); ce != nil {
		ce.Write(zap.String(appLogger.KeyEmail, email))
	}

	// Determine username based on configuration
	var finalUsername string
	if signupConfig.RequireUsernamesOnSignup() {
		// Username is required - use provided username
		if username != nil && strings.TrimSpace(*username) != "" {
			finalUsername = strings.TrimSpace(*username)
		} else {
			return nil, appLogger.WithErrorFields(apperrors.ErrUserCreationFailed,
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Reason, "required username was not provided"))
		}

		available, message, err := h.CheckUsernameAvailability(ctx, finalUsername)
		if err != nil {
			return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUserCreationFailed, err),
				zap.String(appLogger.KeyEmail, email),
				zap.String(appLogger.KeyUsername, finalUsername),
				zap.String(logkeys.Stage, "check_username_availability"))
		}
		if !available {
			return nil, appLogger.WithErrorFields(apperrors.ErrUsernameAlreadyExists,
				zap.String(appLogger.KeyEmail, email),
				zap.String(appLogger.KeyUsername, finalUsername),
				zap.String(logkeys.Reason, message))
		}
	} else {
		// Username not required - use email as username
		finalUsername = email
	}

	// Handle real names based on configuration
	var finalFirstName, finalLastName string
	if signupConfig.RequireRealNamesOnSignup() {
		// Real names are required - use provided names
		if firstName != nil && strings.TrimSpace(*firstName) != "" {
			finalFirstName = strings.TrimSpace(*firstName)
		} else {
			return nil, appLogger.WithErrorFields(apperrors.ErrUserCreationFailed,
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Reason, "required first name was not provided"))
		}
		if lastName != nil && strings.TrimSpace(*lastName) != "" {
			finalLastName = strings.TrimSpace(*lastName)
		} else {
			return nil, appLogger.WithErrorFields(apperrors.ErrUserCreationFailed,
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Reason, "required last name was not provided"))
		}
	} else {
		// Real names not required - use provided names if available, otherwise empty
		if firstName != nil {
			finalFirstName = strings.TrimSpace(*firstName)
		}
		if lastName != nil {
			finalLastName = strings.TrimSpace(*lastName)
		}
	}

	// Create persistence User for initial creation
	persistenceUser := &common_persistence_models.User{
		Username:  finalUsername,
		Email:     email,
		UserType:  signupConfig.DefaultUserType(),
		FirstName: finalFirstName,
		LastName:  finalLastName,
		Banned:    false,
	}

	createErr := h.commonPersistenceHandler.CreateUser(ctx, persistenceUser)
	if createErr != nil {
		// Race window: another concurrent registration may have taken the same
		// email or username between the checks above and this insert. Persistence
		// collapses either unique violation to ErrUserAlreadyExists, so look up
		// the current state before surfacing a stable service error.
		if errors.Is(createErr, persistence.ErrUserAlreadyExists) {
			_, emailErr := h.commonPersistenceHandler.GetUserByEmail(ctx, email)
			if emailErr == nil {
				return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrEmailAlreadyExists, createErr),
					zap.String(appLogger.KeyEmail, email),
					zap.String(logkeys.Stage, "create_user_race"))
			}

			usernameExists, usernameErr := h.commonPersistenceHandler.CheckUsernameExists(ctx, finalUsername)
			if usernameErr == nil && usernameExists {
				return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUsernameAlreadyExists, createErr),
					zap.String(appLogger.KeyUsername, finalUsername),
					zap.String(appLogger.KeyEmail, email),
					zap.String(logkeys.Stage, "create_user_race"))
			}

			return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUserCreationFailed, createErr, emailErr, usernameErr),
				zap.String(appLogger.KeyUsername, finalUsername),
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Stage, "disambiguate_duplicate_user"))
		}
		return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUserCreationFailed, createErr),
			zap.String(appLogger.KeyEmail, email),
			zap.String(appLogger.KeyUsername, finalUsername),
			zap.String(logkeys.Stage, "create_user"))
	}

	// Convert to service model for callback
	coreUser := &common_service_models.User{
		ID:        persistenceUser.ID,
		Username:  persistenceUser.Username,
		Email:     persistenceUser.Email,
		FirstName: persistenceUser.FirstName,
		LastName:  persistenceUser.LastName,
		UserType:  persistenceUser.UserType,
		Banned:    persistenceUser.Banned,
	}

	// Use OnUserCreated callback to create application user
	var createdUser U
	var err error
	if h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnUserCreated != nil {
		createdUser, err = h.userCallbacks.UserCreated(ctx, coreUser)
		if err != nil {
			return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUserCreationFailed, err),
				zap.String(appLogger.KeyEmail, email),
				zap.Uint64(appLogger.KeyUserID, persistenceUser.ID),
				zap.String(logkeys.Stage, "create_application_user"))
		}
	} else {
		// If no callback provided, get user via lookup
		createdUser, err = h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(persistenceUser.ID))
		if err != nil {
			return nil, appLogger.WithErrorFields(errors.Join(apperrors.ErrUserCreationFailed, err),
				zap.String(appLogger.KeyEmail, email),
				zap.Uint64(appLogger.KeyUserID, persistenceUser.ID),
				zap.String(logkeys.Stage, "lookup_application_user"))
		}
	}

	userID := createdUser.GetID()
	if ce := logger.Check(zap.DebugLevel, "User created successfully"); ce != nil {
		ce.Write(zap.String(appLogger.KeyEmail, email), zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Create email verification record and send verification email
	err = h.emailAuthHandler.CreateEmailVerificationAndSendCode(ctx, uint64(userID), email, password, validatedOrigin)
	if err != nil {
		// Transaction will be rolled back by caller
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to create email verification: %w", err),
			zap.String(appLogger.KeyEmail, email),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "create_email_verification"))
	}

	logger.Info("New user registered successfully via email",
		zap.String(appLogger.KeyEmail, email), zap.Uint64(appLogger.KeyUserID, uint64(userID)))

	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionRegister, email); recordErr != nil {
			logger.Warn("Failed to record registration success",
				zap.Error(recordErr),
				zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionRegister),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed registration"))
		}
	}

	// Record user registration metric
	if h.metricsHandler != nil {
		h.metricsHandler.RecordUserRegistration("email")
	}

	// Convert U to *U for return
	ptr := new(U)
	*ptr = createdUser
	return ptr, nil
}

// VerifyNewUserEmail consumes the email proof and verifies its bound address.
// The caller must supply an ambient transaction and roll back on error.
func (h *CommonServiceHandler[U]) VerifyNewUserEmail(ctx context.Context, verificationToken string) (*U, string, error) {
	if !h.emailAuthenticationEnabled() {
		return nil, "", apperrors.ErrEmailAuthDisabled
	}

	if err := h.requireTransaction(ctx, "email verification"); err != nil {
		return nil, "", err
	}
	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Verifying new user email"); ce != nil {
		ce.Write(
			zap.String(appLogger.KeyToken, verificationToken),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
		)
	}

	// Verify the email using the token
	identity, err := h.emailAuthHandler.VerifyEmailWithToken(ctx, verificationToken)
	if err != nil {
		if _, hasLevel := appLogger.ErrorLevel(err); !hasLevel && (errors.Is(err, apperrors.ErrInvalidToken) || errors.Is(err, apperrors.ErrTokenUsed)) {
			err = appLogger.WithErrorLevel(err, zapcore.WarnLevel)
		}
		fields := []zap.Field{
			zap.String(appLogger.KeyToken, verificationToken),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
			zap.String(logkeys.Stage, "verify_email"),
		}
		if errors.Is(err, apperrors.ErrTokenExpired) {
			// Revoked verification tokens intentionally surface as expired. An outer
			// Info override replaces the generic token service's revoked-token Warn.
			err = appLogger.WithErrorLevel(err, zapcore.InfoLevel)
		}
		if identity.UserID != 0 {
			fields = append(fields, zap.Uint64(appLogger.KeyUserID, identity.UserID))
		}
		if identity.Email != "" {
			fields = append(fields, zap.String(appLogger.KeyEmail, identity.Email))
		}
		return nil, "", appLogger.WithErrorFields(err, fields...)
	}
	userID := identity.UserID
	email := identity.Email

	// Get and return the verified user
	user, err := h.GetUserByID(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		return nil, "", appLogger.InvariantError("verified_user_is_resolvable", errors.Join(apperrors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Stage, "load_verified_user"),
			zap.String(logkeys.Expected, "application user exists after email verification"),
			zap.String(logkeys.Actual, "application user lookup failed"))
	}
	if email == "" {
		email = (*user).GetEmail()
	}

	// The email is now verified either way, but a banned account must not
	// receive the auto-login token or the user payload.
	if (*user).IsBanned() {
		fields := []zap.Field{
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "auto_login_after_email_verification"),
		}
		if email != "" {
			fields = append(fields, zap.String(appLogger.KeyEmail, email))
		}
		return nil, "", appLogger.WithErrorLevel(apperrors.ErrUserBanned, zapcore.WarnLevel, fields...)
	}

	if h.organizationHandler != nil {
		if assocErr := h.organizationHandler.AutoAssociateUserWithOrganizationByEmail(ctx, common_service_models.UserIdType(userID), (*user).GetEmail()); assocErr != nil {
			logger.Warn("Auto-association via email domain failed",
				zap.Error(assocErr),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Operation, "verify_new_user_email"),
				zap.String(logkeys.Stage, "auto_associate_organization"),
				zap.String(logkeys.Fallback, "continue without organization association"))
		}
	}

	// Generate auth token for auto-login
	authToken, err := h.authHandler.CreateUserAuth(
		ctx,
		token.SessionAuthMethod{Provider: proto_models.AuthProvider_AUTH_PROVIDER_EMAIL},
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		"", // No external auth code
		common_service_models.UserIdType(userID),
	)
	if err != nil {
		logger.Warn("Failed to create auth token after verification",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
			zap.String(logkeys.Fallback, "complete verification without auto-login token"),
			zap.Bool(appLogger.KeyAutoLogin, false))
		// Return empty token but don't fail the verification
		authToken = ""
	}

	logger.Info("Email verified successfully",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.String(appLogger.KeyEmail, email),
		zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION),
		zap.Bool(appLogger.KeyAutoLogin, authToken != ""))

	return user, authToken, nil
}

// ResetPasswordRequest executes this step in the common_service backend flow.
//
// Parameters:
//   - ctx: context.Context input consumed by ResetPasswordRequest.
//   - email: string input consumed by ResetPasswordRequest.
//   - frontendOrigin: optional frontend origin for the reset link; must be in the configured allowlist.
func (h *CommonServiceHandler[U]) ResetPasswordRequest(ctx context.Context, email string, frontendOrigin *string) error {
	if !h.emailAuthenticationEnabled() {
		return apperrors.ErrEmailAuthDisabled
	}

	// Check rate limit first so that requests carrying an unlisted or malformed
	// frontend_origin cannot bypass the throttle (which would allow unbounded
	// allowlist probing and log flooding).
	if h.rateLimiter != nil {
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionPasswordReset, email)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Password reset proceeding without rate-limit enforcement",
				zap.Error(err), zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionPasswordReset),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "allow password reset attempt"))
			// Continue with password reset even if rate limiter fails
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionPasswordReset),
				zap.String(appLogger.KeyEmail, email),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	// Validate the requested frontend origin before creating any token
	validatedOrigin, err := h.validateFrontendOrigin(frontendOrigin)
	if err != nil {
		return err
	}
	// A synthetic-domain address names a provider account, not a mailbox, so
	// no reset link is sent to it; the API answers it like an unknown email.
	if err := h.authHandler.CheckAccountEmailDomain(ctx, email); err != nil {
		return err
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Password reset requested"); ce != nil {
		ce.Write(zap.String(appLogger.KeyEmail, email))
	}

	// Create password reset token and send email
	err = h.emailAuthHandler.CreatePasswordResetToken(ctx, email, validatedOrigin)
	if err != nil {
		logger.Error("Failed to create password reset token",
			zap.Error(err),
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET),
			zap.String(logkeys.Stage, "create_password_reset_token"),
			zap.String(logkeys.Fallback, "return enumeration-safe success without sending reset email"))
		// Don't reveal if email exists or not for security
		return nil
	}

	// Record success if rate limiter is configured
	if h.rateLimiter != nil {
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionPasswordReset, email); recordErr != nil {
			logger.Warn("Failed to record password reset success",
				zap.Error(recordErr), zap.String(appLogger.KeyEmail, email),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionPasswordReset),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed password reset request"))
		}
	}

	logger.Info("Password reset email sent", zap.String(appLogger.KeyEmail, email))
	return nil
}

// ResetPasswordWithToken consumes the reset proof and changes the password.
// The caller must supply an ambient transaction and roll back on error.
func (h *CommonServiceHandler[U]) ResetPasswordWithToken(ctx context.Context, resetToken, newPassword string) (*U, string, error) {
	if !h.emailAuthenticationEnabled() {
		return nil, "", apperrors.ErrEmailAuthDisabled
	}

	if err := h.requireTransaction(ctx, "password reset"); err != nil {
		return nil, "", err
	}
	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Resetting password with token"); ce != nil {
		ce.Write(zap.String(appLogger.KeyToken, resetToken))
	}

	// Reset password and get user ID
	userID, revokedTokens, err := h.emailAuthHandler.ResetPasswordWithToken(ctx, resetToken, newPassword)
	if err != nil {
		return nil, "", appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyToken, resetToken),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET),
			zap.String(logkeys.Stage, "reset_password"))
	}

	// The database revocation and password mutation share the request
	// transaction. Evict the corresponding auth-cache entries only after commit
	// so an eviction cannot be undone by a rollback and the window in which a
	// concurrent request can repopulate them from pre-commit state is as small
	// as eviction ordering allows (see invalidatePasswordChangeCaches).
	h.invalidatePasswordChangeCaches(ctx, common_service_models.UserIdType(userID), revokedTokens)

	// The password has been reset; notify the application so it can resync
	// downstream state before we deal with auto-login concerns below.
	h.invokeOnPasswordChanged(ctx, common_service_models.UserIdType(userID), common_service_models.UserIdType(userID))

	// Fetch the user object
	user, err := h.GetUserByID(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		logger.Warn("Failed to get user after password reset",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_for_password_reset_auto_login"),
			zap.String(logkeys.Fallback, "complete password reset without user payload or auto-login token"),
			zap.Bool(appLogger.KeyAutoLogin, false))
		// Password was reset successfully, but we can't get user for auto-login
		// Return success without auto-login
		return nil, "", nil
	}

	// Returning an error for a banned account makes transactional callers roll
	// back the password update, session revocations, and reset-token consumption.
	// It also prevents returning an auto-login token or user payload.
	if (*user).IsBanned() {
		return nil, "", appLogger.WithErrorLevel(apperrors.ErrUserBanned, zapcore.WarnLevel,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "auto_login_after_password_reset"))
	}

	// Generate auth token for auto-login
	authToken, err := h.authHandler.CreateUserAuth(
		ctx,
		token.SessionAuthMethod{Provider: proto_models.AuthProvider_AUTH_PROVIDER_EMAIL},
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		"", // No external auth code
		common_service_models.UserIdType(userID),
	)
	if err != nil {
		logger.Warn("Failed to create auth token after password reset",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
			zap.String(logkeys.Stage, "mint_auto_login_session"),
			zap.String(logkeys.Fallback, "complete password reset without auto-login token"),
			zap.Bool(appLogger.KeyAutoLogin, false))
		// Password was reset successfully, but we can't generate token
		// Return user but no token
		return user, "", nil
	}

	logger.Info("Password reset successfully with auto-login",
		zap.Uint64(appLogger.KeyUserID, userID))
	return user, authToken, nil
}

// ChangeUserPassword executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) ChangeUserPassword(ctx context.Context, userID common_service_models.UserIdType, oldPassword, newPassword string) error {
	if !h.emailAuthenticationEnabled() {
		return apperrors.ErrEmailAuthDisabled
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Changing user password"); ce != nil {
		ce.Write(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Change password
	revokedTokens, err := h.emailAuthHandler.ChangePassword(ctx, userID, oldPassword, newPassword)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Stage, "change_password"))
	}

	h.invalidatePasswordChangeCaches(ctx, userID, revokedTokens)

	// Notify the application so it can resync downstream state (e.g. revoke
	// its own external sessions) now that the password has changed.
	h.invokeOnPasswordChanged(ctx, userID, userID)

	logger.Info("Password changed successfully", zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	return nil
}

// invalidatePasswordChangeCaches makes committed password changes visible to
// both tiers of the authentication cache. Without the token-cache eviction, a
// revoked session can continue through GetUserFromAuthToken's cache-hit path
// until the auth cache TTL expires.
//
// TODO(multi-instance-v2): this is process-local; other instances keep their
// own caches until the auth cache TTL.
func (h *CommonServiceHandler[U]) invalidatePasswordChangeCaches(
	ctx context.Context,
	userID common_service_models.UserIdType,
	revokedTokens []string,
) {
	if h.authHandler == nil {
		return
	}

	h.runAfterCommit(ctx, func(hookCtx context.Context) {
		for _, revokedToken := range revokedTokens {
			h.authHandler.InvalidateAuthToken(hookCtx, revokedToken)
		}
		h.authHandler.InvalidateUserCache(userID)
	})
}

// invokeOnPasswordChanged fires the OnPasswordChanged sync callback when one is
// registered. Callback errors are logged but never propagated so a downstream
// synchronization failure cannot abort the completed password mutation, matching
// the semantics of the other user lifecycle hooks.
func (h *CommonServiceHandler[U]) invokeOnPasswordChanged(ctx context.Context, userID, changedBy common_service_models.UserIdType) {
	if h.userCallbacks == nil || h.userCallbacks.SyncCallbacks == nil || h.userCallbacks.SyncCallbacks.OnPasswordChanged == nil {
		return
	}

	if cbErr := h.userCallbacks.SyncCallbacks.OnPasswordChanged(ctx, userID, changedBy); cbErr != nil {
		appLogger.For(ctx, h.logger, loggerName).Error("OnPasswordChanged callback failed",
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "password_changed_callback"),
			zap.String(logkeys.Fallback, "keep completed password mutation"),
			zap.Error(cbErr))
	}
}

// invokeOnUserUpdated fires the OnUserUpdated sync callback when one is
// registered, re-loading the user so the callback receives current core fields.
// Callback errors are logged but never propagated, matching the other user
// lifecycle hooks.
func (h *CommonServiceHandler[U]) invokeOnUserUpdated(ctx context.Context, userID uint64) {
	if h.userCallbacks == nil || h.userCallbacks.SyncCallbacks == nil || h.userCallbacks.SyncCallbacks.OnUserUpdated == nil {
		return
	}

	persistenceUser, err := h.commonPersistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		appLogger.For(ctx, h.logger, loggerName).Error("Failed to load user for OnUserUpdated callback",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_for_updated_callback"),
			zap.String(logkeys.Fallback, "skip user updated callback"),
			zap.Error(err))
		return
	}

	coreUser := &common_service_models.User{
		ID:        persistenceUser.ID,
		Username:  persistenceUser.Username,
		Email:     persistenceUser.Email,
		FirstName: persistenceUser.FirstName,
		LastName:  persistenceUser.LastName,
		UserType:  persistenceUser.UserType,
		Banned:    persistenceUser.Banned,
	}

	if cbErr := h.userCallbacks.SyncCallbacks.OnUserUpdated(ctx, coreUser); cbErr != nil {
		appLogger.For(ctx, h.logger, loggerName).Error("OnUserUpdated callback failed",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "user_updated_callback"),
			zap.String(logkeys.Fallback, "keep completed user mutation"),
			zap.Error(cbErr))
	}
}

// ValidateAuthToken executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) ValidateAuthToken(ctx context.Context, authToken string) (bool, error) {
	// Use the auth handler's validation method
	valid, _, err := h.authHandler.ValidateAuthToken(authToken)
	if err != nil {
		return false, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyToken, authToken),
			zap.String(logkeys.Stage, "validate_auth_token"))
	}
	return valid, nil
}

// ValidateSessionToken executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) ValidateSessionToken(ctx context.Context, authToken string) (*token.SessionTokenClaims, error) {
	claims, err := h.authHandler.ValidateSessionToken(authToken)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyToken, authToken),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
			zap.String(logkeys.Stage, "validate_session_token"))
	}

	return claims, nil
}

// ListActiveSessions returns active authentication sessions for the current user.
func (h *CommonServiceHandler[U]) ListActiveSessions(ctx context.Context, userID common_service_models.UserIdType, currentAuthToken string, pagination *proto_models.PaginationRequest) (*proto_models.ListActiveSessionsResponse, error) {
	activeSessions, paginationResponse, err := h.authHandler.ListActiveSessions(ctx, userID, currentAuthToken, pagination)
	if err != nil {
		return nil, err
	}

	sessions := make([]*proto_models.ActiveSession, 0, len(activeSessions))
	for _, activeSession := range activeSessions {
		session := &proto_models.ActiveSession{
			SessionId:  activeSession.SessionID,
			Current:    activeSession.Current,
			AuthMethod: activeSession.AuthMethod,
			IpAddress:  activeSession.IPAddress,
			UserAgent:  activeSession.UserAgent,
			DeviceId:   activeSession.DeviceID,
			IssuedAt:   timestamppb.New(activeSession.IssuedAt),
			ExpiresAt:  timestamppb.New(activeSession.ExpiresAt),
		}
		if activeSession.AuthMethod == proto_models.AuthProvider_AUTH_PROVIDER_OAUTH {
			session.OauthProviderSlug = &activeSession.OAuthProviderSlug
		}
		if activeSession.LastUsedAt != nil {
			session.LastUsedAt = timestamppb.New(*activeSession.LastUsedAt)
		}
		sessions = append(sessions, session)
	}

	return &proto_models.ListActiveSessionsResponse{
		Sessions:   sessions,
		Pagination: paginationResponse,
	}, nil
}

// RevokeSession revokes an active non-current authentication session for the current user.
func (h *CommonServiceHandler[U]) RevokeSession(ctx context.Context, userID common_service_models.UserIdType, currentAuthToken string, sessionID string) (*proto_models.RevokeSessionResponse, error) {
	sessionsRevoked, err := h.authHandler.RevokeSession(ctx, userID, sessionID, currentAuthToken)
	if err != nil {
		return nil, err
	}

	return &proto_models.RevokeSessionResponse{SessionsRevoked: sessionsRevoked}, nil
}

// RevokeOtherSessions revokes all active authentication sessions except the current one.
func (h *CommonServiceHandler[U]) RevokeOtherSessions(ctx context.Context, userID common_service_models.UserIdType, currentAuthToken string) (*proto_models.RevokeOtherSessionsResponse, error) {
	sessionsRevoked, err := h.authHandler.RevokeOtherSessions(ctx, userID, currentAuthToken)
	if err != nil {
		return nil, err
	}

	return &proto_models.RevokeOtherSessionsResponse{SessionsRevoked: sessionsRevoked}, nil
}

// LogoutUser executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) LogoutUser(ctx context.Context, authToken string) error {
	logger := appLogger.For(ctx, h.logger, loggerName)
	logger.Debug("Logging out user")

	// Revoke the auth token
	err := h.authHandler.RevokeAuthToken(ctx, authToken)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "logout_user"),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
	}

	logger.Info("User logged out successfully")
	return nil
}

// LogoutUserEverywhere executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) LogoutUserEverywhere(ctx context.Context, userID common_service_models.UserIdType) error {
	logger := appLogger.For(ctx, h.logger, loggerName)
	if ce := logger.Check(zap.DebugLevel, "Logging out user from all devices"); ce != nil {
		ce.Write(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Revoke all auth tokens for the user
	err := h.authHandler.RevokeAllUserTokens(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "logout_user_everywhere"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
	}

	logger.Info("User logged out from all devices successfully",
		zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	return nil
}

// CheckUsernameAvailability executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) CheckUsernameAvailability(ctx context.Context, username string) (available bool, message string, err error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Checking username availability",
			zap.String(appLogger.KeyUsername, username))
	}

	// Check if username already exists
	exists, err := h.commonPersistenceHandler.CheckUsernameExists(ctx, username)
	if err != nil {
		return false, "Unable to check username availability", appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "check_username_availability"),
			zap.String(appLogger.KeyUsername, username))
	}

	if exists {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Username already taken",
				zap.String(appLogger.KeyUsername, username))
		}
		return false, "Username is already taken", nil
	}

	// Check for reserved usernames if needed
	if isReservedUsername(username) {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Username is reserved",
				zap.String(appLogger.KeyUsername, username))
		}
		return false, "Username is reserved and cannot be used", nil
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Username is available",
			zap.String(appLogger.KeyUsername, username))
	}
	return true, "Username is available", nil
}

// ===================================================================
// Phone Verification Service Methods
// ===================================================================

// SendPhoneVerificationOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) SendPhoneVerificationOTP(ctx context.Context, userID common_service_models.UserIdType, phoneNumber string) error {
	if h.phoneAuthHandler == nil {
		return appLogger.InvariantError("phone_auth_route_requires_handler", fmt.Errorf("phone authentication not configured"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber),
			zap.String(logkeys.Stage, "send_phone_verification_otp"),
			zap.String(logkeys.Expected, "phone authentication handler configured"),
			zap.String(logkeys.Actual, "phone authentication handler absent"))
	}

	// Check rate limit if configured
	if h.rateLimiter != nil {
		// Rate limit by user ID for authenticated requests
		userIDStr := fmt.Sprintf("%d", userID)
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionSendPhoneOTP, userIDStr)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Phone OTP send proceeding without user rate-limit enforcement",
				zap.Error(err), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.String(logkeys.Stage, "check_user_rate_limit"),
				zap.String(logkeys.Fallback, "send OTP"))
			// Continue with SMS sending even if rate limiter fails
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}

		// Also rate limit by phone number to prevent abuse
		allowed, retryAfter, err = h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionSendPhoneOTP, phoneNumber)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Phone OTP send proceeding without phone rate-limit enforcement",
				zap.Error(err), zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.String(logkeys.Stage, "check_phone_rate_limit"),
				zap.String(logkeys.Fallback, "send OTP"))
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	err := h.phoneAuthHandler.SendPhoneVerificationOTP(ctx, userID, phoneNumber)

	// Record success if SMS was sent successfully
	if err == nil && h.rateLimiter != nil {
		userIDStr := fmt.Sprintf("%d", userID)
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionSendPhoneOTP, userIDStr); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record phone OTP send success",
				zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP send"))
		}
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionSendPhoneOTP, phoneNumber); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record phone OTP send success for phone number",
				zap.Error(recordErr), zap.String(appLogger.KeyPhoneNumber, phoneNumber),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionSendPhoneOTP),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP send"))
		}
	}

	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyPhoneNumber, phoneNumber),
			zap.String(logkeys.Stage, "send_phone_verification_otp"))
	}
	return nil
}

// VerifyPhoneOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) VerifyPhoneOTP(ctx context.Context, userID common_service_models.UserIdType, otpCode string) error {
	if h.phoneAuthHandler == nil {
		return appLogger.InvariantError("phone_auth_route_requires_handler", fmt.Errorf("phone authentication not configured"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "verify_phone_otp"),
			zap.String(logkeys.Expected, "phone authentication handler configured"),
			zap.String(logkeys.Actual, "phone authentication handler absent"))
	}

	// Check rate limit if configured
	if h.rateLimiter != nil {
		userIDStr := fmt.Sprintf("%d", uint64(userID))
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionVerifyPhoneOTP, userIDStr)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Phone OTP verification proceeding without rate-limit enforcement",
				zap.Error(err), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionVerifyPhoneOTP),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "verify OTP"))
			// Continue with verification even if rate limiter fails
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionVerifyPhoneOTP),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	err := h.phoneAuthHandler.VerifyPhoneOTP(ctx, uint64(userID), otpCode)

	// Record success if verification was successful
	if err == nil && h.rateLimiter != nil {
		userIDStr := fmt.Sprintf("%d", uint64(userID))
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionVerifyPhoneOTP, userIDStr); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record phone OTP verification success",
				zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionVerifyPhoneOTP),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP verification"))
		}
	}

	// A successful verification persists the verified phone number on the core
	// user, so notify the application to resync.
	if err == nil {
		h.invokeOnUserUpdated(ctx, uint64(userID))
	}

	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""),
			zap.String(logkeys.Stage, "verify_phone_otp"))
	}
	return nil
}

// ResendPhoneVerificationOTP executes this step in the common_service backend flow.
func (h *CommonServiceHandler[U]) ResendPhoneVerificationOTP(ctx context.Context, userID common_service_models.UserIdType) error {
	if h.phoneAuthHandler == nil {
		return appLogger.InvariantError("phone_auth_route_requires_handler", fmt.Errorf("phone authentication not configured"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "resend_phone_verification_otp"),
			zap.String(logkeys.Expected, "phone authentication handler configured"),
			zap.String(logkeys.Actual, "phone authentication handler absent"))
	}

	// Check rate limit if configured
	if h.rateLimiter != nil {
		userIDStr := fmt.Sprintf("%d", uint64(userID))
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionResendPhoneOTP, userIDStr)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Phone OTP resend proceeding without rate-limit enforcement",
				zap.Error(err), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionResendPhoneOTP),
				zap.String(logkeys.Stage, "check_rate_limit"),
				zap.String(logkeys.Fallback, "resend OTP"))
			// Continue with resend even if rate limiter fails
		} else if !allowed {
			return appLogger.WithErrorLevel(apperrors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionResendPhoneOTP),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	err := h.phoneAuthHandler.ResendPhoneVerificationOTP(ctx, uint64(userID))

	// Record success if resend was successful
	if err == nil && h.rateLimiter != nil {
		userIDStr := fmt.Sprintf("%d", uint64(userID))
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionResendPhoneOTP, userIDStr); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to record phone OTP resend success",
				zap.Error(recordErr), zap.Uint64(appLogger.KeyUserID, uint64(userID)),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionResendPhoneOTP),
				zap.String(logkeys.Stage, "record_rate_limit_success"),
				zap.String(logkeys.Fallback, "continue completed OTP resend"))
		}
	}

	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "resend_phone_verification_otp"))
	}
	return nil
}

// isReservedUsername executes this step in the common_service backend flow.
func isReservedUsername(username string) bool {
	// Convert to lowercase for case-insensitive comparison
	lower := strings.ToLower(username)

	// List of reserved usernames
	reserved := []string{
		"admin", "administrator", "root", "system", "support",
		"help", "info", "contact", "about", "api",
		"www", "mail", "email", "blog", "news",
		"user", "users", "account", "accounts", "profile",
		"login", "logout", "register", "signup", "signin",
		"dashboard", "settings", "config", "configuration",
		"public", "private", "test", "demo", "example",
	}

	return slices.Contains(reserved, lower)
}
