package common_api

import (
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/router"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// Common HTTP errors define standard error responses for the API.
// These errors use SRouter's HTTPError type to ensure consistent error
// formatting and proper HTTP status codes in responses.
var (
	// ErrInvalidCredentials indicates that the provided authentication credentials are incorrect.
	ErrInvalidCredentials = router.NewHTTPError(http.StatusUnauthorized, "Invalid credentials")
	// ErrEmailNotVerified indicates that the user's email address has not been verified.
	ErrEmailNotVerified = router.NewHTTPError(http.StatusForbidden, "Email not verified")
	// ErrUserNotFound indicates that the requested user does not exist in the system.
	ErrUserNotFound = router.NewHTTPError(http.StatusNotFound, "User not found")
	// ErrEmailAlreadyExists indicates that the email address is already registered.
	ErrEmailAlreadyExists = router.NewHTTPError(http.StatusConflict, "Email already exists")
	// ErrInvalidRequest indicates that the request format or data is invalid.
	ErrInvalidRequest = router.NewHTTPError(http.StatusBadRequest, "Invalid request")
	// ErrInternalServer indicates an unexpected server error occurred.
	ErrInternalServer = router.NewHTTPError(http.StatusInternalServerError, "Internal server error")
	// ErrUnauthorized indicates that the request lacks valid authentication.
	ErrUnauthorized = router.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
	// ErrInvalidToken indicates that the provided token is malformed or invalid.
	ErrInvalidToken = router.NewHTTPError(http.StatusBadRequest, "Invalid token")
)

// MapServiceError translates a service error into a public HTTP error while
// retaining its private cause and any contextual diagnostics for the router's
// single owning log record.
func MapServiceError(err error) error {
	return MapServiceErrorWithFields(err)
}

// MapServiceErrorWithFields adds boundary context while translating err. The
// fields and cause are available to SRouter logging but are never disclosed in
// the HTTP response.
func MapServiceErrorWithFields(err error, fields ...zap.Field) error {
	mapped := mapServiceHTTPError(err)
	var block *inference.BlockError
	if errors.As(err, &block) {
		fields = append(fields, zap.Stringer("inference_block_reason", block.Reason))
	}
	contextFields := append(appLogger.ErrorFields(err), fields...)
	result := router.NewHTTPErrorWithCause(mapped.StatusCode, mapped.Message, err).
		WithFields(contextFields...)
	if level, ok := appLogger.ErrorLevel(err); ok {
		result = result.WithLogLevel(level)
	}
	return result
}

func mapServiceHTTPError(err error) *router.HTTPError {
	var block *inference.BlockError
	if errors.As(err, &block) {
		return router.NewHTTPError(http.StatusForbidden, "Inference is blocked by the current routing policy: "+block.Reason.String())
	}
	if errors.Is(err, serviceErrors.ErrInferencePolicyVersionConflict) {
		return router.NewHTTPError(http.StatusConflict, "Inference policy changed; reload and retry")
	}
	if errors.Is(err, serviceErrors.ErrInferencePolicyInvalid) {
		return router.NewHTTPError(http.StatusBadRequest, "Invalid inference policy")
	}
	if errors.Is(err, serviceErrors.ErrInvalidOAuthState) {
		return router.NewHTTPError(http.StatusBadRequest, "Invalid OAuth state")
	}
	// Personal workspace lifecycle errors have public messages that differ from
	// the other policy errors, so keep their translations grouped explicitly.
	if errors.Is(err, serviceErrors.ErrPersonalWorkspaceNotFound) {
		return router.NewHTTPError(http.StatusNotFound, "Personal workspace not found")
	}
	if errors.Is(err, serviceErrors.ErrPersonalWorkspaceArchived) {
		return router.NewHTTPError(http.StatusGone, "Personal workspace is archived")
	}
	if errors.Is(err, serviceErrors.ErrPersonalWorkspaceConflict) {
		return router.NewHTTPError(http.StatusConflict, "Personal workspace state conflicts with managed invariants")
	}
	switch {
	// 400 Bad Request errors
	case errors.Is(err, serviceErrors.ErrInvalidRequest):
		return ErrInvalidRequest
	case errors.Is(err, serviceErrors.ErrInvalidToken):
		return ErrInvalidToken
	case errors.Is(err, serviceErrors.ErrInferenceAncestorProviderBlocked):
		return router.NewHTTPError(http.StatusBadRequest, "A parent organization's provider allowlist blocks this model")
	case errors.Is(err, serviceErrors.ErrInvalidUserID):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid user ID")
	case errors.Is(err, serviceErrors.ErrCannotRevokeCurrentSession):
		return router.NewHTTPError(http.StatusBadRequest, "Use logout to end your current session")
	case errors.Is(err, serviceErrors.ErrInvalidUserIDFormat):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid user ID format")
	case errors.Is(err, serviceErrors.ErrInvalidUsernameFormat):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid username format")
	case errors.Is(err, serviceErrors.ErrUsernameRequired):
		return router.NewHTTPError(http.StatusBadRequest, "Username is required")
	case errors.Is(err, serviceErrors.ErrOAuthSignupDisabled):
		return router.NewHTTPError(http.StatusForbidden, "OAuth signup is disabled")
	case errors.Is(err, serviceErrors.ErrEmailAuthDisabled):
		return router.NewHTTPError(http.StatusForbidden, "Email authentication is disabled")
	case errors.Is(err, serviceErrors.ErrInvalidEmailFormat):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid email format")
	case errors.Is(err, serviceErrors.ErrUntrustedFrontendOrigin):
		return router.NewHTTPError(http.StatusBadRequest, "Frontend origin not allowed")
	case errors.Is(err, serviceErrors.ErrTokenExpired):
		return router.NewHTTPError(http.StatusBadRequest, "Token has expired")
	case errors.Is(err, serviceErrors.ErrTokenUsed):
		return router.NewHTTPError(http.StatusBadRequest, "Token has already been used")
	case errors.Is(err, serviceErrors.ErrInvalidOTPCode):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid OTP code")
	case errors.Is(err, serviceErrors.ErrOrganizationParentInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid parent organization ID")
	case errors.Is(err, serviceErrors.ErrOrganizationHierarchyDepthExceeded):
		return router.NewHTTPError(http.StatusBadRequest, "Organization hierarchy depth exceeds configured limit")
	case errors.Is(err, serviceErrors.ErrOrganizationParentImmutable):
		return router.NewHTTPError(http.StatusBadRequest, "Parent organization cannot be changed after creation")
	case errors.Is(err, serviceErrors.ErrOrganizationLineageImmutable):
		return router.NewHTTPError(http.StatusBadRequest, "Organization lineage cannot be changed after creation")
	case errors.Is(err, serviceErrors.ErrOrganizationAncestorMembershipRequired):
		return router.NewHTTPError(http.StatusBadRequest, "Membership in all ancestor organizations is required")
	case errors.Is(err, serviceErrors.ErrOrganizationSlugReserved):
		return router.NewHTTPError(http.StatusBadRequest, "Organization slug is reserved for personal workspaces")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceOwnershipImmutable):
		return router.NewHTTPError(http.StatusBadRequest, "Personal workspace ownership cannot be set or changed")
	case errors.Is(err, serviceErrors.ErrInvalidAttachmentStatus):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid attachment status")
	case errors.Is(err, serviceErrors.ErrChatAvatarAttachmentInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid attachment")

	// 401 Unauthorized errors
	case errors.Is(err, serviceErrors.ErrInvalidCredentials):
		return ErrInvalidCredentials
	case errors.Is(err, serviceErrors.ErrUserBanned):
		return router.NewHTTPError(http.StatusUnauthorized, "User account is banned")
	case errors.Is(err, serviceErrors.ErrLoginFailed):
		return router.NewHTTPError(http.StatusUnauthorized, "Login failed")
	case errors.Is(err, serviceErrors.ErrOAuthFailed):
		return router.NewHTTPError(http.StatusUnauthorized, "OAuth authentication failed")
	case errors.Is(err, serviceErrors.ErrAuthenticationRequired):
		return ErrUnauthorized

	// 403 Forbidden errors
	case errors.Is(err, serviceErrors.ErrEmailNotVerified):
		return ErrEmailNotVerified
	case errors.Is(err, serviceErrors.ErrChatDisabled):
		return router.NewHTTPError(http.StatusForbidden, "Chat feature is disabled")
	case errors.Is(err, serviceErrors.ErrUserSearchDisabled):
		return router.NewHTTPError(http.StatusForbidden, "User search feature is disabled")
	case errors.Is(err, serviceErrors.ErrCareersDisabled):
		return router.NewHTTPError(http.StatusForbidden, "Careers feature is disabled")
	case errors.Is(err, serviceErrors.ErrInsufficientPermissions):
		return router.NewHTTPError(http.StatusForbidden, "Insufficient permissions for this operation")
	case errors.Is(err, serviceErrors.ErrChatMessageImmutable):
		return router.NewHTTPError(http.StatusForbidden, "Chat message is immutable")
	case errors.Is(err, serviceErrors.ErrChatForkDisabled):
		return router.NewHTTPError(http.StatusForbidden, "This chat channel cannot be forked")
	case errors.Is(err, serviceErrors.ErrChatForkAgentOrganizationNotMember):
		return router.NewHTTPError(http.StatusForbidden, "This chat has agents of an organization you are not a member of; join that organization to fork it")
	case errors.Is(err, serviceErrors.ErrChatAgentStopNotAllowed):
		return router.NewHTTPError(http.StatusForbidden, "This agent cannot be stopped from the chat")
	case errors.Is(err, serviceErrors.ErrInferenceSessionRetargetNotAllowed):
		return router.NewHTTPError(http.StatusForbidden, "Re-targeting is not allowed for this operation")
	case errors.Is(err, serviceErrors.ErrCannotModifyOwnAccount):
		return router.NewHTTPError(http.StatusForbidden, "Cannot modify your own account")
	case errors.Is(err, serviceErrors.ErrCannotModifyHigherTierUser):
		return router.NewHTTPError(http.StatusForbidden, "Cannot modify user with higher admin tier")
	case errors.Is(err, serviceErrors.ErrUnauthorizedAttachmentAccess):
		return router.NewHTTPError(http.StatusForbidden, "You don't have permission to access one or more attachments")
	case errors.Is(err, serviceErrors.ErrAttachmentSecurityViolation):
		return router.NewHTTPError(http.StatusForbidden, "Attachment failed security validation")
	case errors.Is(err, serviceErrors.ErrReauthenticationRequired):
		// NOTE: This message is a stable contract that the frontend matches to
		// trigger the confirm-password prompt (see isReauthenticationRequiredError
		// in typescript/packages/core/src/error-handling.ts). The error transport
		// (SRouter) does not carry a machine-readable code, so do not reword or
		// translate this message without updating the frontend matcher.
		return router.NewHTTPError(http.StatusForbidden, "Please reauthenticate to continue")
	case errors.Is(err, serviceErrors.ErrCannotRemoveLastAuthMethod):
		return router.NewHTTPError(http.StatusForbidden, "Cannot remove last authentication method")
	case errors.Is(err, serviceErrors.ErrOrganizationEmailDomainsDisabled):
		return router.NewHTTPError(http.StatusForbidden, "Organization email domains feature is disabled")
	case errors.Is(err, serviceErrors.ErrOrganizationEmailDomainManagementDisabled):
		return router.NewHTTPError(http.StatusForbidden, "Organization email domain management is disabled")
	case errors.Is(err, serviceErrors.ErrInsufficientOrganizationPermissions):
		return router.NewHTTPError(http.StatusForbidden, "Insufficient organization permissions")
	case errors.Is(err, serviceErrors.ErrOrganizationCreateChildNotAllowed):
		return router.NewHTTPError(http.StatusForbidden, "Insufficient permissions to create child organization")
	case errors.Is(err, serviceErrors.ErrOrganizationSelfJoinNotAllowed):
		return router.NewHTTPError(http.StatusForbidden, "Insufficient permissions to self-join organization")
	case errors.Is(err, serviceErrors.ErrOrganizationTypeCapabilityDenied):
		return router.NewHTTPError(http.StatusForbidden, "Organization type does not allow this operation")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceOwnerRequired):
		return router.NewHTTPError(http.StatusForbidden, "Only the personal workspace owner may perform this operation")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceMembershipRestricted):
		return router.NewHTTPError(http.StatusForbidden, "Personal workspace membership is restricted to its owner")

	// 404 Not Found errors
	case errors.Is(err, serviceErrors.ErrUserNotFound):
		return ErrUserNotFound
	case errors.Is(err, serviceErrors.ErrUserSettingsNotFound):
		return router.NewHTTPError(http.StatusNotFound, "User settings not found")
	case errors.Is(err, serviceErrors.ErrContactMessageNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Contact message not found")
	case errors.Is(err, serviceErrors.ErrCareerJobPostingNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Career job posting not found")
	case errors.Is(err, serviceErrors.ErrCareerApplicationNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Career application not found")
	case errors.Is(err, serviceErrors.ErrInferenceModelNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Inference model not found")
	case errors.Is(err, serviceErrors.ErrInferenceOperationNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Inference operation not found")
	case errors.Is(err, serviceErrors.ErrInferenceAttemptNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Inference attempt not found")
	case errors.Is(err, serviceErrors.ErrInferenceControlPlaneUnavailable):
		return router.NewHTTPError(http.StatusServiceUnavailable, "Inference control plane is not configured")
	case errors.Is(err, serviceErrors.ErrAttachmentNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Attachment not found")
	case errors.Is(err, serviceErrors.ErrTokenNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Token not found")
	case errors.Is(err, serviceErrors.ErrSessionNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Session not found")
	case errors.Is(err, serviceErrors.ErrAuthMethodNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Authentication method not found")
	case errors.Is(err, serviceErrors.ErrOAuthEmailMissing):
		return router.NewHTTPError(http.StatusBadRequest, "Email not provided by OAuth provider")
	case errors.Is(err, serviceErrors.ErrOAuthEmailNotVerified):
		return router.NewHTTPError(http.StatusForbidden, "Email not verified by OAuth provider")
	case errors.Is(err, serviceErrors.ErrEmailDomainReserved):
		return router.NewHTTPError(http.StatusBadRequest, "Email domain is not allowed")
	case errors.Is(err, serviceErrors.ErrOAuthProviderUnavailable):
		return router.NewHTTPError(http.StatusBadRequest, "OAuth provider unavailable")
	case errors.Is(err, serviceErrors.ErrOAuthServiceUnavailable):
		return router.NewHTTPError(http.StatusServiceUnavailable, "OAuth sign-in temporarily unavailable")
	case errors.Is(err, serviceErrors.ErrOAuthSubjectMissing):
		return router.NewHTTPError(http.StatusBadGateway, "OAuth provider did not identify the account")
	case errors.Is(err, serviceErrors.ErrOAuthIdentityConflict):
		return router.NewHTTPError(http.StatusConflict, "A different account from this OAuth provider is already linked")
	case errors.Is(err, serviceErrors.ErrOrganizationNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Organization not found")
	case errors.Is(err, serviceErrors.ErrOrganizationParentNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Parent organization not found")
	case errors.Is(err, serviceErrors.ErrOrganizationEmailDomainNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Organization email domain not found")
	case errors.Is(err, serviceErrors.ErrInvitationNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Invitation not found")
	case errors.Is(err, serviceErrors.ErrChatChannelNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat channel not found")
	case errors.Is(err, serviceErrors.ErrChatMessageNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat message not found")
	case errors.Is(err, serviceErrors.ErrChatParticipantNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat participant not found")
	case errors.Is(err, serviceErrors.ErrChatReactionNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat reaction not found")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat agent session not found")
	case errors.Is(err, serviceErrors.ErrChatAgentRunNotFound):
		return router.NewHTTPError(http.StatusNotFound, "Chat agent run not found")
	case errors.Is(err, serviceErrors.ErrUserNotMember):
		return router.NewHTTPError(http.StatusNotFound, "User is not a member of this organization")

	case errors.Is(err, serviceErrors.ErrToolCallNotResolvable):
		return router.NewHTTPError(http.StatusNotFound, "Tool call is not awaiting approval")

	// 409 Conflict errors
	case errors.Is(err, serviceErrors.ErrChatAgentSessionVersionConflict):
		return router.NewHTTPError(http.StatusConflict, "Chat agent session config version conflict")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionLocationConflict):
		return router.NewHTTPError(http.StatusConflict, "Chat agent session location is already occupied")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionIdempotencyConflict):
		return router.NewHTTPError(http.StatusConflict, "Chat agent session create idempotency conflict")
	case errors.Is(err, serviceErrors.ErrChatForkRequestKeyConflict):
		return router.NewHTTPError(http.StatusConflict, "A fork with this request ID is already in progress; retry to get it")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionBusy):
		return router.NewHTTPError(http.StatusConflict, "The agent is busy; try again when it has finished")
	case errors.Is(err, serviceErrors.ErrChatAgentRunNotActive):
		return router.NewHTTPError(http.StatusConflict, "The agent run has already ended")
	// NOTE: The next two messages are stable contracts that the frontend matches
	// (isAgentBroadcastWouldWaitError and isAgentQueuedInputNotQueuedError in
	// typescript/packages/core/src/error-handling.ts): the error transport (SRouter) does not
	// carry a machine-readable code, so do not reword them without updating the matchers.
	case errors.Is(err, serviceErrors.ErrChatAgentBroadcastWouldWait):
		return router.NewHTTPError(http.StatusConflict, "The agent is busy, so this reply cannot also be sent to the channel")
	case errors.Is(err, serviceErrors.ErrChatAgentQueuedInputNotQueued):
		return router.NewHTTPError(http.StatusConflict, "This message is no longer queued for the agent")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionScopeInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Chat agent session scope is invalid")
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationParentInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Chat agent session cannot delegate")
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationChildInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Chat agent delegation child type is invalid")
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationNotAllowed):
		return router.NewHTTPError(http.StatusBadRequest, "Chat agent delegation is not allowed for this agent type")
	case errors.Is(err, serviceErrors.ErrChatAgentSessionNotDelegated):
		return router.NewHTTPError(http.StatusBadRequest, "Chat agent session is not a delegated child")
	case errors.Is(err, serviceErrors.ErrChatAgentDelegationLimitExceeded):
		return router.NewHTTPError(http.StatusConflict, "Chat agent delegation active-children limit reached")
	case errors.Is(err, serviceErrors.ErrGraphBudgetExhausted):
		return router.NewHTTPError(http.StatusConflict, "Chat agent delegation graph budget exhausted")
	case errors.Is(err, serviceErrors.ErrChatMessageAgentSessionRootPinned):
		return router.NewHTTPError(http.StatusConflict, "Chat message anchors an agent session and cannot be deleted")
	case errors.Is(err, serviceErrors.ErrChatChannelAgentSessionThreadsRequired):
		return router.NewHTTPError(http.StatusConflict, "Chat channel has agent sessions that require threads")
	case errors.Is(err, serviceErrors.ErrToolCallResolutionConflict):
		return router.NewHTTPError(http.StatusConflict, "Tool call resolution conflicts with its recorded final state")
	case errors.Is(err, serviceErrors.ErrToolCallApprovalExpired):
		return router.NewHTTPError(http.StatusConflict, "Tool call approval has expired")
	case errors.Is(err, serviceErrors.ErrNotImplemented):
		return router.NewHTTPError(http.StatusNotImplemented, "Not implemented")
	case errors.Is(err, serviceErrors.ErrEmailAlreadyExists):
		return ErrEmailAlreadyExists
	case errors.Is(err, serviceErrors.ErrUsernameAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Username already exists")
	case errors.Is(err, serviceErrors.ErrUserAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "User already exists")
	case errors.Is(err, serviceErrors.ErrEmailAlreadyVerified):
		return router.NewHTTPError(http.StatusConflict, "Email is already verified")
	case errors.Is(err, serviceErrors.ErrActiveVerificationToken):
		return router.NewHTTPError(http.StatusConflict, "Active verification token already exists")
	case errors.Is(err, serviceErrors.ErrPasswordAuthAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Password authentication already exists for this user")
	case errors.Is(err, serviceErrors.ErrOAuthUserCannotRegister):
		return router.NewHTTPError(http.StatusConflict, "This email is already registered, please sign in with your social account")
	case errors.Is(err, serviceErrors.ErrOAuthUserMustAddPassword):
		return router.NewHTTPError(http.StatusConflict, "You're already signed up with a social account, sign in and add a password from settings")
	case errors.Is(err, serviceErrors.ErrNoPasswordToReset):
		return router.NewHTTPError(http.StatusConflict, "No password set for this account, sign in with your social account")
	case errors.Is(err, serviceErrors.ErrPasswordAuthRequired):
		return router.NewHTTPError(http.StatusConflict, "Add a password before enabling or managing 2FA")
	case errors.Is(err, serviceErrors.ErrOTPAlreadyEnabled):
		return router.NewHTTPError(http.StatusConflict, "OTP authentication is already enabled")
	case errors.Is(err, serviceErrors.ErrOTPNotEnabled):
		return router.NewHTTPError(http.StatusConflict, "OTP authentication is not enabled")
	case errors.Is(err, serviceErrors.ErrOTPNotConfigured):
		return router.NewHTTPError(http.StatusConflict, "OTP authentication not configured")
	case errors.Is(err, serviceErrors.ErrUserAlreadyBanned):
		return router.NewHTTPError(http.StatusConflict, "User is already banned")
	case errors.Is(err, serviceErrors.ErrUserNotBanned):
		return router.NewHTTPError(http.StatusConflict, "User is not banned")
	case errors.Is(err, serviceErrors.ErrCannotDeleteLastAdmin):
		return router.NewHTTPError(http.StatusConflict, "Cannot delete the last admin user")
	case errors.Is(err, serviceErrors.ErrCannotRemoveLastOwner):
		return router.NewHTTPError(http.StatusConflict, "Cannot remove the last owner of the organization")
	case errors.Is(err, serviceErrors.ErrCannotDemoteLastOwner):
		return router.NewHTTPError(http.StatusConflict, "Cannot demote the last owner of the organization")
	case errors.Is(err, serviceErrors.ErrOrganizationAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Organization already exists")
	case errors.Is(err, serviceErrors.ErrCareerJobPostingAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Career job posting already exists")
	case errors.Is(err, serviceErrors.ErrInferenceModelAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Inference model already exists")
	case errors.Is(err, serviceErrors.ErrInferenceModelInUse):
		return router.NewHTTPError(http.StatusConflict, "Inference model is referenced by an operation route; reassign the route before deleting it")
	case errors.Is(err, serviceErrors.ErrOrganizationSlugTaken):
		return router.NewHTTPError(http.StatusConflict, "Organization slug is already taken")
	case errors.Is(err, serviceErrors.ErrOrganizationEmailDomainExists):
		return router.NewHTTPError(http.StatusConflict, "Organization email domain already exists")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceManagedFieldsImmutable):
		return router.NewHTTPError(http.StatusConflict, "Personal workspace slug and description are system-managed")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceDeletionRestricted):
		return router.NewHTTPError(http.StatusConflict, "Personal workspace organizations cannot be deleted directly")
	case errors.Is(err, serviceErrors.ErrPersonalWorkspaceOwnerMembershipProtected):
		return router.NewHTTPError(http.StatusConflict, "Personal workspace owner membership is protected")
	case errors.Is(err, serviceErrors.ErrInvitationAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Invitation already exists")
	case errors.Is(err, serviceErrors.ErrInvitationAlreadyProcessed):
		return router.NewHTTPError(http.StatusConflict, "Invitation already processed")
	case errors.Is(err, serviceErrors.ErrUserAlreadyMember):
		return router.NewHTTPError(http.StatusConflict, "User is already a member of this organization")
	case errors.Is(err, serviceErrors.ErrChatChannelArchived):
		return router.NewHTTPError(http.StatusConflict, "Chat channel is archived")
	case errors.Is(err, serviceErrors.ErrChatReactionAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "Reaction already exists for this message")

	// 429 Too Many Requests errors
	case errors.Is(err, serviceErrors.ErrRateLimitExceeded):
		return router.NewHTTPError(http.StatusTooManyRequests, "Too many requests. Please try again later.")
	case errors.Is(err, serviceErrors.ErrVerificationCooldown):
		return router.NewHTTPError(http.StatusTooManyRequests, "Please wait before requesting another verification email")
	case errors.Is(err, serviceErrors.ErrChatAgentPendingInputLimitExceeded):
		return router.NewHTTPError(http.StatusTooManyRequests, "Too many messages are waiting for the agent")
	case errors.Is(err, serviceErrors.ErrCareerApplicationClosed):
		return router.NewHTTPError(http.StatusConflict, "This job is not accepting applications")

	// 410 Gone / expired resources
	case errors.Is(err, serviceErrors.ErrInvitationExpired):
		return router.NewHTTPError(http.StatusGone, "Invitation has expired")

	// 500 Internal Server Error - System/operational errors
	case errors.Is(err, serviceErrors.ErrInternalError):
		return ErrInternalServer
	case errors.Is(err, serviceErrors.ErrUserCreationFailed):
		return router.NewHTTPError(http.StatusInternalServerError, "Failed to create user")
	case errors.Is(err, serviceErrors.ErrTransactionNotFound):
		return router.NewHTTPError(http.StatusInternalServerError, "Database transaction not found")
	case errors.Is(err, serviceErrors.ErrDatabaseError):
		return router.NewHTTPError(http.StatusInternalServerError, "Database operation failed")
	case errors.Is(err, serviceErrors.ErrStorageError):
		return router.NewHTTPError(http.StatusInternalServerError, "Storage operation failed")
	case errors.Is(err, serviceErrors.ErrEmailSendFailed):
		return router.NewHTTPError(http.StatusInternalServerError, "Failed to send email")
	case errors.Is(err, serviceErrors.ErrCareerDraftTokenInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Career draft token is invalid or expired")
	case errors.Is(err, serviceErrors.ErrCareerResumeRequired):
		return router.NewHTTPError(http.StatusBadRequest, "Resume attachment is required")
	case errors.Is(err, serviceErrors.ErrCareerResumeAttachmentInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "Resume attachment does not match the application draft")
	case errors.Is(err, serviceErrors.ErrCareerInvalidStatusTransition):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid career status transition")
	case errors.Is(err, serviceErrors.ErrSystemStatsUnavailable):
		return router.NewHTTPError(http.StatusInternalServerError, "System statistics unavailable")
	case errors.Is(err, serviceErrors.ErrChatInvalidThread):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid thread reference")
	case errors.Is(err, serviceErrors.ErrChatInvalidReply):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid reply reference")
	case errors.Is(err, serviceErrors.ErrChatForkPointInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "The fork point is not a message of this channel you can see")
	case errors.Is(err, serviceErrors.ErrChatForkMemberInvalid):
		return router.NewHTTPError(http.StatusBadRequest, "A fork member is not allowed by this channel's fork rules")
	case errors.Is(err, serviceErrors.ErrChatForkTooLarge):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "The fork would copy more messages than allowed; fork from an earlier message")
	case errors.Is(err, serviceErrors.ErrChatInvalidBroadcast):
		return router.NewHTTPError(http.StatusBadRequest, "Only thread replies in threaded channels can also be sent to the channel")
	case errors.Is(err, serviceErrors.ErrChatMentionLimitExceeded):
		return router.NewHTTPError(http.StatusBadRequest, "Too many mentions in message")
	case errors.Is(err, serviceErrors.ErrChatInvalidMention):
		return router.NewHTTPError(http.StatusBadRequest, "Invalid mention in message")
	case errors.Is(err, serviceErrors.ErrChatNotMember):
		return router.NewHTTPError(http.StatusForbidden, "You are not a member of this chat channel")
	case errors.Is(err, serviceErrors.ErrChatFeatureDisabled):
		return router.NewHTTPError(http.StatusBadRequest, "Chat feature is disabled")

	// Default fallback for unmapped errors
	default:
		return ErrInternalServer
	}
}

// MapChangePasswordError overrides shared error translations for the
// change-password endpoint, where OAuth-only accounts should be told to add
// a password instead of receiving the 2FA-specific password-auth message.
func MapChangePasswordError(err error) error {
	return MapChangePasswordErrorWithFields(err)
}

// MapChangePasswordErrorWithFields is the contextual variant used by the
// change-password HTTP boundary.
func MapChangePasswordErrorWithFields(err error, fields ...zap.Field) error {
	if errors.Is(err, serviceErrors.ErrPasswordAuthRequired) {
		mapped := router.NewHTTPErrorWithCause(
			http.StatusConflict,
			"No password set for this account. Add a password before trying to change it.",
			err,
		).WithFields(append(appLogger.ErrorFields(err), fields...)...)
		if level, ok := appLogger.ErrorLevel(err); ok {
			mapped = mapped.WithLogLevel(level)
		}
		return mapped
	}

	return MapServiceErrorWithFields(err, fields...)
}
