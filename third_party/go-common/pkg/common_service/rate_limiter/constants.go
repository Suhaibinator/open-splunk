package rate_limiter

// RateLimitAction represents a rate-limited action in the system.
// Actions are kept as a compact enum for indexed lookup in hot paths.
type RateLimitAction uint8

const (
	ActionUnknown RateLimitAction = iota
	ActionLogin
	ActionRegister
	ActionPasswordReset
	ActionVerifyEmail
	ActionSendPhoneOTP
	ActionVerifyPhoneOTP
	ActionResendPhoneOTP
	ActionContact
	ActionCareerApplication
	ActionCareerJobLookup
	ActionOrganizationSlugAvailability
	ActionUserSearch
	ActionOAuthCallback
	ActionAddPassword
	ActionRemoveAuthMethod
	ActionOTPSetup
	ActionOTPVerify
	ActionAdminBanUser
	ActionAdminChangeUserType
	ActionAdminChangeUsername
	ActionAdminDeleteUser
	ActionAdminRevokeSession
	ActionAdminCreateUser
	ActionAdminUpdateProfilePicture
	ActionAdminBulkOperation
	ActionNotificationCount
	ActionAgentSessionControl
	// ActionPublicChatRead counts logged-out public chat reads per client IP.
	ActionPublicChatRead
	ActionChatFork

	rateLimitActionCount
)

var rateLimitActionNames = [...]string{
	ActionUnknown:                      "unknown",
	ActionLogin:                        "login",
	ActionRegister:                     "register",
	ActionPasswordReset:                "password_reset",
	ActionVerifyEmail:                  "verify_email",
	ActionSendPhoneOTP:                 "send_phone_otp",
	ActionVerifyPhoneOTP:               "verify_phone_otp",
	ActionResendPhoneOTP:               "resend_phone_otp",
	ActionContact:                      "contact",
	ActionCareerApplication:            "career_application",
	ActionCareerJobLookup:              "career_job_lookup",
	ActionOrganizationSlugAvailability: "organization_slug_availability",
	ActionUserSearch:                   "user_search",
	ActionOAuthCallback:                "oauth_callback",
	ActionAddPassword:                  "add_password",
	ActionRemoveAuthMethod:             "remove_auth_method",
	ActionOTPSetup:                     "otp_setup",
	ActionOTPVerify:                    "otp_verify",
	ActionAdminBanUser:                 "admin_ban_user",
	ActionAdminChangeUserType:          "admin_change_user_type",
	ActionAdminChangeUsername:          "admin_change_username",
	ActionAdminDeleteUser:              "admin_delete_user",
	ActionAdminRevokeSession:           "admin_revoke_session",
	ActionAdminCreateUser:              "admin_create_user",
	ActionAdminUpdateProfilePicture:    "admin_update_profile_picture",
	ActionAdminBulkOperation:           "admin_bulk_operation",
	ActionNotificationCount:            "notification_count",
	ActionAgentSessionControl:          "agent_session_control",
	ActionPublicChatRead:               "public_chat_read",
	ActionChatFork:                     "chat_fork",
}

// String executes this step in the rate_limiter backend flow.
func (a RateLimitAction) String() string {
	if a >= rateLimitActionCount {
		return rateLimitActionNames[ActionUnknown]
	}
	return rateLimitActionNames[a]
}

// valid executes this step in the rate_limiter backend flow.
func (a RateLimitAction) valid() bool {
	return a > ActionUnknown && a < rateLimitActionCount
}

// IdentifierType represents the type of identifier used for rate limiting.
// Different identifier types allow rate limiting by various attributes
// such as IP address, email, or user ID.
type IdentifierType string

// Predefined identifier types for rate limiting.
const (
	// IdentifierTypeEmail uses email address as the rate limit identifier.
	// Useful for limiting actions per email address (e.g., registration).
	IdentifierTypeEmail IdentifierType = "email"

	// IdentifierTypeIP uses IP address as the rate limit identifier.
	// Useful for limiting unauthenticated actions (e.g., login attempts).
	IdentifierTypeIP IdentifierType = "ip"

	// IdentifierTypeUserID uses user ID as the rate limit identifier.
	// Useful for limiting authenticated user actions.
	IdentifierTypeUserID IdentifierType = "user_id"
)
