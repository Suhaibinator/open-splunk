// Package common_sanitizer provides comprehensive input validation and sanitization
// for the go-common library. It offers protection against common security threats
// including SQL injection, XSS attacks, and email abuse.
//
// The package includes:
//   - Email validation with RFC compliance and disposable email detection
//   - Password strength validation
//   - Contact form input sanitization
//   - OAuth request validation
//   - Verification code validation
//
// All sanitization methods are designed to work with Protocol Buffer message types
// and integrate seamlessly with the SRouter framework's sanitizer middleware.
//
// Example usage:
//
//	handler := common_sanitizer.NewHandler(config.Static(mainConfig).Sanitization(), logger, oauthProviders)
//	// Use handler methods in SRouter route configuration
package common_sanitizer

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
	"go.uber.org/zap"
)

// Handler defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - Handler is read or populated by backend components in this package to move requests through this stage.
type Handler struct {
	// requestPolicy samples one hot configuration generation and returns its
	// immutable policy. Sampling is per call, not per request: a sanitizer that
	// nests another exported sanitizer may observe two generations if a
	// configuration publish lands between the samples. That window is
	// accepted; validation limits are harmless to straddle, and it keeps the
	// policy an explicit local value rather than hidden request-context state.
	requestPolicy func() *RequestPolicy
	logger        *zap.Logger
}

// NewHandler constructs the sanitizer facade from its typed provider. Request
// validation policy is hot: each operation samples the provider and the
// compiled policy is memoized per configuration generation
// (config.GenerationMemo), so regex and domain state is rebuilt only when a
// new generation is observed. A generation whose policy fails to compile is
// logged once and the last valid policy keeps serving until the next
// generation is published.
func NewHandler(cfg config.SanitizationProvider, logger *zap.Logger) *Handler {
	startupView := cfg()
	policy, err := NewRequestPolicyFromView(startupView)
	if err != nil {
		panic(fmt.Sprintf("sanitizer request policy initialization failed: %v", err))
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	handler := &Handler{logger: logger}
	policies := config.NewGenerationMemo(NewRequestPolicyFromView, func(err error) {
		handler.logger.Named(sanitizerLoggerName).Warn("sanitizer policy recompilation failed; keeping last valid policy",
			zap.String(logkeys.Stage, "compile_sanitizer_request_policy"),
			zap.String(logkeys.Fallback, "last_valid_policy"),
			zap.Error(err))
	})
	policies.Seed(startupView, policy)
	handler.requestPolicy = policies.Provider(cfg)
	return handler
}

// ValidateEmail validates and normalizes an email using one hot policy capture.
// It is the only supported entry point for email validation outside a larger
// sanitizer request, so every operation observes a single policy snapshot.
func (h *Handler) ValidateEmail(email string) (string, error) {
	return h.requestPolicy().ValidateEmail(email)
}

// SanitizeUserEmailRegistrationRequest validates and sanitizes email registration requests.
// It performs the following validations:
//   - Email format validation and normalization
//   - Email domain restrictions (blocked/allowed lists)
//   - Disposable email detection (if configured)
//   - Password length validation
//
// The email address is normalized to lowercase and trimmed of whitespace.
// This method is designed to be used as a sanitizer in SRouter route configuration.

// validatePasswordComplexity performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validatePasswordComplexity(password string, passwordConfig PasswordConfig) error {
	if passwordConfig.RequireUppercase && !strings.ContainsAny(password, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		return ErrPasswordMissingUppercase
	}
	if passwordConfig.RequireLowercase && !strings.ContainsAny(password, "abcdefghijklmnopqrstuvwxyz") {
		return ErrPasswordMissingLowercase
	}
	if passwordConfig.RequireNumber && !strings.ContainsAny(password, "0123456789") {
		return ErrPasswordMissingNumber
	}
	if passwordConfig.RequireSpecialChar && !strings.ContainsAny(password, passwordConfig.SpecialChars) {
		return ErrPasswordMissingSpecialChar
	}
	return nil
}

// validateRealNames performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validateRealNames(firstName, lastName *string, realNameConfig RealNameConfig) error {
	// Check if first name is provided and meets length requirements
	if firstName == nil || strings.TrimSpace(*firstName) == "" {
		return ErrFirstNameRequired
	}

	firstNameTrimmed := strings.TrimSpace(*firstName)
	if len(firstNameTrimmed) < realNameConfig.MinLength {
		return configuredMinimumLengthError(ErrFirstNameTooShort, realNameConfig.MinLength)
	}
	if len(firstNameTrimmed) > realNameConfig.MaxLength {
		return configuredMaximumLengthError(ErrFirstNameTooLong, realNameConfig.MaxLength)
	}

	// Check if last name is provided and meets length requirements
	if lastName == nil || strings.TrimSpace(*lastName) == "" {
		return ErrLastNameRequired
	}

	lastNameTrimmed := strings.TrimSpace(*lastName)
	if len(lastNameTrimmed) < realNameConfig.MinLength {
		return configuredMinimumLengthError(ErrLastNameTooShort, realNameConfig.MinLength)
	}
	if len(lastNameTrimmed) > realNameConfig.MaxLength {
		return configuredMaximumLengthError(ErrLastNameTooLong, realNameConfig.MaxLength)
	}

	return nil
}

// validateUsernameFormat performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validateUsernameFormat(username string, usernameConfig UsernameConfig) (string, error) {
	// Trim whitespace
	trimmedUsername := strings.TrimSpace(username)

	// Check if username is empty
	if trimmedUsername == "" {
		return "", ErrUsernameEmpty
	}

	// Check length
	if len(trimmedUsername) < usernameConfig.MinLength {
		return "", configuredMinimumLengthError(ErrUsernameTooShort, usernameConfig.MinLength)
	}
	if len(trimmedUsername) > usernameConfig.MaxLength {
		return "", configuredMaximumLengthError(ErrUsernameTooLong, usernameConfig.MaxLength)
	}

	// Check if username starts with letter or number
	if len(trimmedUsername) > 0 {
		firstChar := rune(trimmedUsername[0])
		if !unicode.IsLetter(firstChar) && !unicode.IsDigit(firstChar) {
			return "", ErrUsernameStartsWithInvalidChar
		}
	}

	// Check for valid characters (letters, numbers, underscore, hyphen)
	for _, char := range trimmedUsername {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '_' && char != '-' {
			return "", ErrUsernameInvalidCharacters
		}
	}

	return trimmedUsername, nil
}

// sanitizeSlug performs its package-specific work in the request sanitization and validation flow before service handlers run.
func sanitizeSlug(slug string, organizationConfig OrganizationConfig) (string, error) {
	// Trim whitespace and convert to lowercase
	sanitizedSlug := strings.ToLower(strings.TrimSpace(slug))

	// Check if slug is empty
	if sanitizedSlug == "" {
		return "", fmt.Errorf("slug cannot be empty")
	}

	// Get min and max length from config
	minLength := organizationConfig.SlugMinLength
	maxLength := organizationConfig.SlugMaxLength

	// Check length against configured limits
	if len(sanitizedSlug) < minLength {
		return "", fmt.Errorf("slug must be at least %d characters long", minLength)
	}
	if len(sanitizedSlug) > maxLength {
		return "", fmt.Errorf("slug must be no more than %d characters long", maxLength)
	}

	// Check for valid slug format: alphanumeric and hyphens, but not starting/ending with hyphen
	// Pattern: must start with alphanumeric, can contain alphanumeric and hyphens, must end with alphanumeric
	for i, char := range sanitizedSlug {
		isAlphaNumeric := (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')
		isHyphen := char == '-'

		if !isAlphaNumeric && !isHyphen {
			return "", fmt.Errorf("slug can only contain lowercase letters, numbers, and hyphens")
		}

		// First and last characters must be alphanumeric
		if (i == 0 || i == len(sanitizedSlug)-1) && !isAlphaNumeric {
			return "", fmt.Errorf("slug must start and end with a letter or number")
		}
	}

	return sanitizedSlug, nil
}

// validateSlugFormat performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validateSlugFormat(slug string, organizationConfig OrganizationConfig) (string, error) {
	return sanitizeSlug(slug, organizationConfig)
}

// validatePhoneNumber performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) validatePhoneNumber(ctx context.Context, stage string, phoneNumber *string, defaultRegion string) (string, error) {
	// Phone number is optional - if nil or empty, return empty string
	if phoneNumber == nil || strings.TrimSpace(*phoneNumber) == "" {
		return "", nil
	}

	phoneStr := strings.TrimSpace(*phoneNumber)
	// The number itself is never logged; only its length and the failing check.
	lengthField := zap.Int("phone_number_length", len(phoneStr))

	// Parse the phone number with configurable default region
	parsedNumber, err := phonenumbers.Parse(phoneStr, defaultRegion)
	if err != nil {
		return "", h.reject(ctx, stage, ErrPhoneNumberInvalid, lengthField,
			zap.String(logkeys.Reason, "parse_failed"),
			zap.String(appLogger.KeyDefaultRegion, defaultRegion))
	}

	// Validate the parsed number
	if !phonenumbers.IsValidNumber(parsedNumber) {
		return "", h.reject(ctx, stage, ErrPhoneNumberInvalidRegion, lengthField,
			zap.String(logkeys.Reason, "not_a_valid_number"),
			zap.String(appLogger.KeyDefaultRegion, defaultRegion))
	}

	// Format to E.164 international format for consistent storage
	e164Number := phonenumbers.Format(parsedNumber, phonenumbers.E164)

	// Additional safety check - E.164 numbers should not exceed 15 digits
	if len(e164Number) > 16 { // +15 digits = 16 characters max
		return "", h.reject(ctx, stage, ErrPhoneNumberTooLong, lengthField,
			zap.String(logkeys.Reason, "e164_too_long"),
			zap.Int("e164_length", len(e164Number)))
	}

	return e164Number, nil
}

// SanitizeUserEmailRegistrationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUserEmailRegistrationRequest(ctx context.Context, req *proto_models.EmailRegistrationRequest) (*proto_models.EmailRegistrationRequest, error) {
	const stage = "sanitize_user_email_registration_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	// Validate and normalize email
	normalizedEmail, err := policy.ValidateEmail(req.Email)
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.Email)))
	}
	req.Email = normalizedEmail

	// Validate password
	if len(req.Password) < policy.password.MinLength || len(req.Password) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.Int("password_length", len(req.Password)))
	}

	if err := validatePasswordComplexity(req.Password, policy.password); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("password_length", len(req.Password)))
	}

	// Validate and normalize phone number (optional field)
	normalizedPhone, err := h.validatePhoneNumber(ctx, stage, req.PhoneNumber, policy.phoneNumber.DefaultRegion)
	if err != nil {
		return nil, err
	}
	// Update the request with the normalized phone number (E.164 format or empty)
	if normalizedPhone != "" {
		req.PhoneNumber = &normalizedPhone
	} else {
		req.PhoneNumber = nil
	}

	// Validate real names if required
	if policy.signup.RequireRealNames {
		if err := validateRealNames(req.FirstName, req.LastName, policy.realName); err != nil {
			return nil, h.reject(ctx, stage, err,
				zap.Int("first_name_length", len(strings.TrimSpace(req.GetFirstName()))),
				zap.Int("last_name_length", len(strings.TrimSpace(req.GetLastName()))))
		}
	}

	// Syntactically validate and normalize the optional frontend origin
	// (the allowlist check happens in the service layer)
	if err := normalizeFrontendOrigin(&req.FrontendOrigin); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("frontend_origin_length", len(req.GetFrontendOrigin())))
	}

	// When usernames are not required, registration uses the normalized email as
	// the username. Drop any supplied username so service behavior stays explicit.
	if !policy.signup.RequireUsernames {
		req.Username = nil
		return req, nil
	}

	// Validate required username
	if req.Username == nil || strings.TrimSpace(*req.Username) == "" {
		return nil, h.reject(ctx, stage, ErrUsernameRequired, zap.String("field", "username"))
	}
	validatedUsername, err := validateUsernameFormat(*req.Username, policy.username)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, *req.Username, policy.logInputMaxLength)...)
	}
	req.Username = &validatedUsername

	return req, nil
}

// SanitizePasswordResetRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizePasswordResetRequest(ctx context.Context, req *proto_models.PasswordResetRequest) (*proto_models.PasswordResetRequest, error) {
	const stage = "sanitize_password_reset_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	// Validate and normalize email
	normalizedEmail, err := policy.ValidateEmail(req.Email)
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.Email)))
	}
	req.Email = normalizedEmail

	// Syntactically validate and normalize the optional frontend origin
	// (the allowlist check happens in the service layer)
	if err := normalizeFrontendOrigin(&req.FrontendOrigin); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("frontend_origin_length", len(req.GetFrontendOrigin())))
	}

	return req, nil
}

// normalizeFrontendOrigin syntactically validates an optional frontend_origin field in
// place: whitespace-only values become nil, valid origins are replaced with their
// normalized form, and anything else fails with ErrInvalidFrontendOrigin. Whether the
// origin is trusted is decided against the configured allowlist in the service layer.
func normalizeFrontendOrigin(field **string) error {
	if *field == nil {
		return nil
	}
	if strings.TrimSpace(**field) == "" {
		*field = nil
		return nil
	}
	normalized, err := origin.Normalize(**field)
	if err != nil {
		return ErrInvalidFrontendOrigin
	}
	*field = &normalized
	return nil
}

// SanitizePasswordChangeRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizePasswordChangeRequest(ctx context.Context, req *proto_models.PasswordChangeRequest) (*proto_models.PasswordChangeRequest, error) {
	const stage = "sanitize_password_change_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if len(req.OldPassword) < policy.password.MinLength || len(req.OldPassword) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.String("field", "old_password"),
			zap.Int("old_password_length", len(req.OldPassword)))
	}
	if len(req.NewPassword) < policy.password.MinLength || len(req.NewPassword) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.String("field", "new_password"),
			zap.Int("new_password_length", len(req.NewPassword)))
	}
	if err := validatePasswordComplexity(req.NewPassword, policy.password); err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.String("field", "new_password"),
			zap.Int("new_password_length", len(req.NewPassword)))
	}
	return req, nil
}

// SanitizeEmailVerificationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeEmailVerificationRequest(ctx context.Context, req *proto_models.EmailVerificationRequest) (*proto_models.EmailVerificationRequest, error) {
	const stage = "sanitize_email_verification_request"
	policy := h.requestPolicy()
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.VerificationCode == "" {
		return nil, h.reject(ctx, stage, ErrEmptyVerificationCode, zap.String("field", "verification_code"))
	}
	// Validate verification code length
	codeLen := len(req.VerificationCode)
	if codeLen < policy.verification.MinCodeLength || codeLen > policy.verification.MaxCodeLength {
		return nil, h.reject(ctx, stage, ErrInvalidVerificationCodeLen,
			zap.Int("verification_code_length", codeLen),
			zap.Int("min_code_length", policy.verification.MinCodeLength),
			zap.Int("max_code_length", policy.verification.MaxCodeLength))
	}
	return req, nil
}

// SanitizePasswordResetCompleteRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizePasswordResetCompleteRequest(ctx context.Context, req *proto_models.PasswordResetCompleteRequest) (*proto_models.PasswordResetCompleteRequest, error) {
	const stage = "sanitize_password_reset_complete_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if req.VerificationCode == "" {
		return nil, h.reject(ctx, stage, ErrEmptyVerificationCode, zap.String("field", "verification_code"))
	}
	// Validate verification code length
	codeLen := len(req.VerificationCode)
	if codeLen < policy.verification.MinCodeLength || codeLen > policy.verification.MaxCodeLength {
		return nil, h.reject(ctx, stage, ErrInvalidVerificationCodeLen,
			zap.Int("verification_code_length", codeLen),
			zap.Int("min_code_length", policy.verification.MinCodeLength),
			zap.Int("max_code_length", policy.verification.MaxCodeLength))
	}

	// Validate password
	if len(req.NewPassword) < policy.password.MinLength || len(req.NewPassword) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.Int("new_password_length", len(req.NewPassword)))
	}

	if err := validatePasswordComplexity(req.NewPassword, policy.password); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("new_password_length", len(req.NewPassword)))
	}

	return req, nil
}

// SanitizeOAuthStateRequest validates the provider slug of an OAuth state
// request and bounds its URLs by the state length. URL policy (return URL,
// exact redirect URI, frontend callback URL) is checked by the service against
// the current provider sample.
func (h *Handler) SanitizeOAuthStateRequest(ctx context.Context, req *proto_models.OAuthStateRequest) (*proto_models.OAuthStateRequest, error) {
	const stage = "sanitize_oauth_state_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if !oauth.ValidSlug(req.ProviderSlug) {
		return nil, h.reject(ctx, stage, ErrInvalidOAuthProviderSlug,
			zap.String("field", "provider_slug"),
			zap.Int(appLogger.KeyLength, len(req.ProviderSlug)))
	}
	// Each URL is signed into the state, so one longer than the state bound
	// can never be issued. Only lengths are logged.
	maxLength := h.requestPolicy().oauthMaxStateLength
	for _, field := range []struct{ name, value string }{
		{"return_url", req.ReturnUrl},
		{"redirect_uri", req.RedirectUri},
		{"frontend_callback_url", req.GetFrontendCallbackUrl()},
	} {
		if len(field.value) > maxLength {
			return nil, h.reject(ctx, stage, ErrOAuthURLTooLong,
				zap.String("field", field.name),
				zap.Int(appLogger.KeyLength, len(field.value)),
				zap.Int("max_length", maxLength))
		}
	}
	return req, nil
}

// SanitizeOAuthCodeRequest validates the code (non-empty, within
// OAuthMaxCodeLength) and the opaque state of an OAuth callback.
func (h *Handler) SanitizeOAuthCodeRequest(ctx context.Context, req *proto_models.OAuthCodeRequest) (*proto_models.OAuthCodeRequest, error) {
	const stage = "sanitize_oauth_code_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.Code == "" {
		return nil, h.reject(ctx, stage, ErrEmptyOAuthCode, zap.String("field", "code"))
	}
	if maxLength := h.requestPolicy().oauthMaxCodeLength; len(req.Code) > maxLength {
		return nil, h.reject(ctx, stage, ErrOAuthCodeTooLong,
			zap.String("field", "code"),
			zap.Int(appLogger.KeyLength, len(req.Code)),
			zap.Int("max_length", maxLength))
	}
	if req.State == "" {
		return nil, h.reject(ctx, stage, ErrEmptyOAuthState, zap.String("field", "state"))
	}
	if err := h.validateOAuthStateFormat(ctx, stage, req.State); err != nil {
		return nil, err
	}
	return req, nil
}

// validateOAuthStateFormat checks only the shape of the opaque state: raw
// base64url within the configured length. The service verifies its signature.
// The state itself is never logged.
func (h *Handler) validateOAuthStateFormat(ctx context.Context, stage string, state string) error {
	maxLength := h.requestPolicy().oauthMaxStateLength
	reason := ""
	switch {
	case len(state) > maxLength:
		reason = "too_long"
	case strings.ContainsFunc(state, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_')
	}):
		reason = "not_base64url"
	}
	if reason == "" {
		return nil
	}
	return h.reject(ctx, stage, ErrInvalidOAuthState,
		zap.String("field", "state"),
		zap.Int("state_length", len(state)),
		zap.Int("max_state_length", maxLength),
		zap.String(logkeys.Reason, reason))
}

// SanitizeUserEmailAuthRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUserEmailAuthRequest(ctx context.Context, req *proto_models.EmailAuthRequest) (*proto_models.EmailAuthRequest, error) {
	const stage = "sanitize_user_email_auth_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	// Validate and normalize email
	normalizedEmail, err := policy.ValidateEmail(req.Email)
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.Email)))
	}
	req.Email = normalizedEmail

	// Validate password
	if req.Password == "" || len(req.Password) < policy.password.MinLength || len(req.Password) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.Int("password_length", len(req.Password)))
	}

	// Validate OTP code format if provided
	if req.OtpCode != nil {
		otpCode := strings.TrimSpace(*req.OtpCode)
		if otpCode == "" {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.String("field", "otp_code"),
				zap.String(logkeys.Reason, "otp_code_empty"))
		}
		if len(otpCode) != 6 {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.String("field", "otp_code"),
				zap.String(logkeys.Reason, "otp_code_length"),
				zap.Int("otp_code_length", len(otpCode)))
		}
		for _, char := range otpCode {
			if char < '0' || char > '9' {
				return nil, h.reject(ctx, stage, ErrInvalidRequest,
					zap.String("field", "otp_code"),
					zap.String(logkeys.Reason, "otp_code_not_numeric"),
					zap.Int("otp_code_length", len(otpCode)))
			}
		}
		req.OtpCode = &otpCode
	}

	return req, nil
}

// SanitizeLogoutRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeLogoutRequest(ctx context.Context, req *proto_models.LogoutRequest) (*proto_models.LogoutRequest, error) {
	// LogoutRequest only has a boolean field, no validation needed
	// The boolean will default to false if not provided
	return req, nil
}

// SanitizeListActiveSessionsRequest defaults and bounds the session-list page
// from default_pagination_limit and max_pagination_limit.
func (h *Handler) SanitizeListActiveSessionsRequest(ctx context.Context, req *proto_models.ListActiveSessionsRequest) (*proto_models.ListActiveSessionsRequest, error) {
	const stage = "sanitize_list_active_sessions_request"
	if _, err := guardRequest(req); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAddPasswordRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAddPasswordRequest(ctx context.Context, req *proto_models.AddPasswordRequest) (*proto_models.AddPasswordRequest, error) {
	const stage = "sanitize_add_password_request"
	policy := h.requestPolicy()
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Check password presence
	if req.Password == "" {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.String("field", "password"),
			zap.String(logkeys.Reason, "password_empty"))
	}

	// Check confirmation presence
	if req.ConfirmPassword == "" {
		return nil, h.reject(ctx, stage, ErrPasswordConfirmationRequired, zap.String("field", "confirm_password"))
	}

	// Use the SAME password validation as registration
	if len(req.Password) < policy.password.MinLength ||
		len(req.Password) > policy.password.MaxLength {
		return nil, h.reject(ctx, stage,
			configuredLengthRangeError(ErrInvalidPasswordFormat, policy.password.MinLength, policy.password.MaxLength),
			zap.Int("password_length", len(req.Password)))
	}

	if err := validatePasswordComplexity(req.Password, policy.password); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("password_length", len(req.Password)))
	}

	// Passwords must match
	if req.Password != req.ConfirmPassword {
		return nil, h.reject(ctx, stage, ErrPasswordMismatch,
			zap.Int("password_length", len(req.Password)),
			zap.Int("confirm_password_length", len(req.ConfirmPassword)))
	}

	return req, nil
}

// SanitizeCheckUsernameAvailabilityRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCheckUsernameAvailabilityRequest(ctx context.Context, req *proto_models.CheckUsernameAvailabilityRequest) (*proto_models.CheckUsernameAvailabilityRequest, error) {
	const stage = "sanitize_check_username_availability_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	// Validate and sanitize username using the common validation logic
	validatedUsername, err := validateUsernameFormat(req.Username, policy.username)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, req.Username, policy.logInputMaxLength)...)
	}

	// Update the request with the validated username
	req.Username = validatedUsername
	return req, nil
}

// SanitizeCheckOrganizationSlugAvailabilityRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCheckOrganizationSlugAvailabilityRequest(ctx context.Context, req *proto_models.CheckOrganizationSlugAvailabilityRequest) (*proto_models.CheckOrganizationSlugAvailabilityRequest, error) {
	const stage = "sanitize_check_organization_slug_availability_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	// Validate and sanitize slug using organization slug validation logic
	validatedSlug, err := validateSlugFormat(req.Slug, policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeySlug, req.Slug, policy.logInputMaxLength)...)
	}

	// Update the request with the validated slug
	req.Slug = validatedSlug
	return req, nil
}

// SanitizeUpdateUserSettingsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateUserSettingsRequest(ctx context.Context, req *proto_models.UpdateUserSettingsRequest) (*proto_models.UpdateUserSettingsRequest, error) {
	const stage = "sanitize_update_user_settings_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// A present language must be a real language; an absent (nil) language leaves
	// the stored preference unchanged, so it needs no validation.
	if req.Language != nil && *req.Language == proto_models.Language_LANGUAGE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidLanguage, zap.Stringer(appLogger.KeyLanguage, req.Language))
	}

	// Boolean fields are strongly typed optionals; a nil value simply leaves the
	// stored preference unchanged and needs no validation.

	return req, nil
}

// SanitizeUpdateLanguagePreferenceRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateLanguagePreferenceRequest(ctx context.Context, req *proto_models.UpdateLanguagePreferenceRequest) (*proto_models.UpdateLanguagePreferenceRequest, error) {
	const stage = "sanitize_update_language_preference_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Validate language enum - must not be UNSPECIFIED
	if req.Language == proto_models.Language_LANGUAGE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidLanguage, zap.Stringer(appLogger.KeyLanguage, req.Language))
	}

	return req, nil
}

// Note: UpdatePhoneNumberRequest sanitizer was removed as we no longer support
// insecure direct phone number updates. Use the phone verification flow for
// adding/changing phone numbers and the removePhoneNumber API for removal.

// ===================================================================
// Phone Verification Sanitizers
// ===================================================================

// SanitizeSendPhoneOTPRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeSendPhoneOTPRequest(ctx context.Context, req *proto_models.SendPhoneOTPRequest) (*proto_models.SendPhoneOTPRequest, error) {
	const stage = "sanitize_send_phone_otp_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Validate and normalize phone number
	policy := h.requestPolicy()
	normalizedPhone, err := h.validatePhoneNumber(ctx, stage, &req.PhoneNumber, policy.phoneNumber.DefaultRegion)
	if err != nil {
		return nil, err
	}

	// Phone number is required for OTP sending
	if normalizedPhone == "" {
		return nil, h.reject(ctx, stage, ErrPhoneNumberInvalid,
			zap.String("field", "phone_number"),
			zap.Int("phone_number_length", len(strings.TrimSpace(req.PhoneNumber))))
	}

	req.PhoneNumber = normalizedPhone
	return req, nil
}

// SanitizeVerifyPhoneOTPRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeVerifyPhoneOTPRequest(ctx context.Context, req *proto_models.VerifyPhoneOTPRequest) (*proto_models.VerifyPhoneOTPRequest, error) {
	const stage = "sanitize_verify_phone_otp_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Sanitize OTP code
	otpCode := strings.TrimSpace(req.OtpCode)
	if otpCode == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String("field", "otp_code"),
			zap.String(logkeys.Reason, "otp_code_empty"))
	}

	// Validate OTP format (6 digits)
	if len(otpCode) != 6 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String("field", "otp_code"),
			zap.String(logkeys.Reason, "otp_code_length"),
			zap.Int("otp_code_length", len(otpCode)))
	}

	// Check if all characters are digits
	for _, char := range otpCode {
		if char < '0' || char > '9' {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.String("field", "otp_code"),
				zap.String(logkeys.Reason, "otp_code_not_numeric"),
				zap.Int("otp_code_length", len(otpCode)))
		}
	}

	req.OtpCode = otpCode
	return req, nil
}

// SanitizeVerifyOTPSetupRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeVerifyOTPSetupRequest(ctx context.Context, req *proto_models.VerifyOTPSetupRequest) (*proto_models.VerifyOTPSetupRequest, error) {
	const stage = "sanitize_verify_otp_setup_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.CurrentPassword == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String("field", "current_password"),
			zap.String(logkeys.Reason, "current_password_empty"))
	}

	otpCode := strings.TrimSpace(req.OtpCode)
	if otpCode == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String("field", "otp_code"),
			zap.String(logkeys.Reason, "otp_code_empty"))
	}
	if len(otpCode) != 6 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String("field", "otp_code"),
			zap.String(logkeys.Reason, "otp_code_length"),
			zap.Int("otp_code_length", len(otpCode)))
	}
	for _, char := range otpCode {
		if char < '0' || char > '9' {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.String("field", "otp_code"),
				zap.String(logkeys.Reason, "otp_code_not_numeric"),
				zap.Int("otp_code_length", len(otpCode)))
		}
	}

	req.OtpCode = otpCode
	return req, nil
}

// SanitizeResendPhoneOTPRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeResendPhoneOTPRequest(ctx context.Context, req *proto_models.ResendPhoneOTPRequest) (*proto_models.ResendPhoneOTPRequest, error) {
	// The phone number is resolved from the authenticated user context, so the resend
	// request itself carries no fields to sanitize.
	const stage = "sanitize_resend_phone_otp_request"
	sanitized, err := guardRequest(req)
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeUpdateUsernameRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateUsernameRequest(ctx context.Context, req *proto_models.UpdateUsernameRequest) (*proto_models.UpdateUsernameRequest, error) {
	const stage = "sanitize_update_username_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Validate and sanitize username using the common validation logic
	policy := h.requestPolicy()
	validatedUsername, err := validateUsernameFormat(req.Username, policy.username)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, req.Username, policy.logInputMaxLength)...)
	}

	// Update the request with the validated username
	req.Username = validatedUsername
	return req, nil
}

// SanitizeGetOrganizationMemberRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetOrganizationMemberRequest(ctx context.Context, req *proto_models.GetOrganizationMemberRequest) (*proto_models.GetOrganizationMemberRequest, error) {
	const stage = "sanitize_get_organization_member_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetOrganizationId(), ErrInvalidOrganizationID),
		requiredID(req.GetUserId(), ErrInvalidUserID),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()))
	}
	return sanitized, nil
}

// SanitizePaginationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizePaginationRequest(ctx context.Context, req *proto_models.PaginationRequest) (*proto_models.PaginationRequest, error) {
	const stage = "sanitize_pagination_request"
	policy := h.requestPolicy()
	sanitized, err := sanitizePaginationRequest(req, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Uint32(logkeys.Limit, req.GetLimit()),
			zap.Int("max_limit", policy.pagination.MaxLimit))
	}
	return sanitized, nil
}

// sanitizePaginationRequest is the single place a request page size is defaulted
// and bounded. A nil request takes the same path as an empty one: an omitted page
// becomes 1, an omitted limit becomes default_pagination_limit, and a limit above
// max_pagination_limit is rejected. Downstream layers read the result as-is.
func sanitizePaginationRequest(req *proto_models.PaginationRequest, pagination PaginationConfig) (*proto_models.PaginationRequest, error) {
	if req == nil {
		req = &proto_models.PaginationRequest{}
	}
	if req.Page == 0 {
		req.Page = 1
	}
	if req.Limit == 0 {
		req.Limit = uint32(pagination.DefaultLimit)
	}
	if req.Limit > uint32(pagination.MaxLimit) {
		return nil, configuredMaximumError(ErrPaginationLimitTooHigh, pagination.MaxLimit)
	}
	return req, nil
}

// GetDefaultPagination performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) GetDefaultPagination() *proto_models.PaginationRequest {
	policy := h.requestPolicy()
	return &proto_models.PaginationRequest{
		Page:  1,
		Limit: uint32(policy.pagination.DefaultLimit),
	}
}
