// Package errors defines common error variables used throughout the service layer.
// These errors provide consistent error handling and allow callers to use errors.Is()
// for error type checking.
package errors

import (
	"errors"
	"fmt"
)

// User-related errors that may occur during user operations.
var (
	// ErrUserNotFound indicates the requested user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrUserCreationFailed indicates user creation failed for any reason.
	ErrUserCreationFailed = errors.New("failed to create user")

	// ErrUserBanned indicates the user account has been banned.
	ErrUserBanned = errors.New("user is banned")

	// ErrUserSettingsNotFound indicates user settings record does not exist.
	ErrUserSettingsNotFound = errors.New("user settings not found")

	// ErrInvalidUserID indicates the provided user ID is invalid or malformed.
	ErrInvalidUserID = errors.New("invalid user ID")

	// ErrInvalidCredentials indicates incorrect username/password combination.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrInvalidToken indicates the provided token is invalid or malformed.
	ErrInvalidToken = errors.New("invalid token")

	// ErrAuthenticationRequired indicates the request requires authentication.
	ErrAuthenticationRequired = errors.New("authentication required")

	// ErrUserAlreadyExists indicates that a user with the given email already exists.
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrTokenExpired indicates the token has passed its expiration time.
	ErrTokenExpired = errors.New("token expired")

	// ErrTokenRevoked indicates the token has been revoked.
	ErrTokenRevoked = errors.New("token revoked")

	// ErrRefreshChainTooLong indicates a refresh replacement chain exceeded the
	// revocation walk's bound, so a live replacement token may remain unrevoked.
	ErrRefreshChainTooLong = errors.New("refresh replacement chain exceeded revocation bound")

	// ErrTokenUsed indicates a single-use token has already been consumed.
	ErrTokenUsed = errors.New("token already used")

	// ErrLoginFailed indicates a general login failure.
	ErrLoginFailed = errors.New("login failed")

	// ErrInternalError indicates an unexpected internal error occurred.
	ErrInternalError = errors.New("internal error")

	// ErrTransactionNotFound indicates no database transaction in context.
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrInvalidRequest indicates the request parameters are invalid.
	ErrInvalidRequest = errors.New("invalid request")

	// ErrRateLimitExceeded indicates the rate limit has been exceeded.
	ErrRateLimitExceeded = errors.New("rate limit exceeded")

	// ErrUntrustedFrontendOrigin indicates a request named a frontend origin that is
	// not in the configured allowed frontend origins.
	ErrUntrustedFrontendOrigin = errors.New("frontend origin is not allowed")

	// ErrUserAlreadyBanned indicates the user is already banned.
	ErrUserAlreadyBanned = errors.New("user is already banned")

	// ErrUserNotBanned indicates the user is not currently banned.
	ErrUserNotBanned = errors.New("user is not banned")

	// ErrCannotModifyOwnAccount indicates admin cannot modify their own account.
	ErrCannotModifyOwnAccount = errors.New("cannot modify your own account")

	// ErrCannotModifyHigherTierUser indicates admin cannot modify user with higher tier.
	ErrCannotModifyHigherTierUser = errors.New("cannot modify user with higher admin tier")

	// ErrCannotDeleteLastAdmin indicates the last admin user cannot be deleted.
	ErrCannotDeleteLastAdmin = errors.New("cannot delete the last admin user")

	// ErrInsufficientPermissions indicates the user lacks required permissions.
	ErrInsufficientPermissions = errors.New("insufficient permissions for this operation")

	// ErrUserSearchDisabled indicates the user search feature is disabled.
	ErrUserSearchDisabled = errors.New("user search is disabled")

	// ErrDatabaseError indicates a database operation failed.
	ErrDatabaseError = errors.New("database operation failed")

	// ErrStorageError indicates a storage operation failed.
	ErrStorageError = errors.New("storage operation failed")

	// ErrEmailSendFailed indicates email sending failed.
	ErrEmailSendFailed = errors.New("failed to send email")

	// ErrContactMessageNotFound indicates the contact message does not exist.
	ErrContactMessageNotFound = errors.New("contact message not found")

	// ErrAttachmentNotFound indicates the attachment does not exist.
	ErrAttachmentNotFound = errors.New("attachment not found")

	// ErrInvalidAttachmentStatus indicates the requested attachment status is not a recognized value.
	ErrInvalidAttachmentStatus = errors.New("invalid attachment status")

	// ErrTokenNotFound indicates the token does not exist.
	ErrTokenNotFound = errors.New("token not found")

	// ErrSessionNotFound indicates the requested active session does not exist.
	ErrSessionNotFound = errors.New("session not found")

	// ErrCannotRevokeCurrentSession indicates the current session cannot be revoked through session management.
	ErrCannotRevokeCurrentSession = errors.New("cannot revoke current session")

	// ErrSystemStatsUnavailable indicates system statistics are unavailable.
	ErrSystemStatsUnavailable = errors.New("system statistics unavailable")

	// ErrRateLimiterConfigurationError indicates a rate limiter configuration or internal state error.
	ErrRateLimiterConfigurationError = errors.New("rate limiter configuration error")

	// ErrCareersDisabled indicates the careers feature is disabled.
	ErrCareersDisabled = errors.New("careers feature is disabled")

	// ErrCareerJobPostingNotFound indicates the requested career job posting does not exist.
	ErrCareerJobPostingNotFound = errors.New("career job posting not found")

	// ErrCareerJobPostingAlreadyExists indicates a job posting with the same slug already exists.
	ErrCareerJobPostingAlreadyExists = errors.New("career job posting already exists")

	// ErrCareerApplicationNotFound indicates the requested career application does not exist.
	ErrCareerApplicationNotFound = errors.New("career application not found")

	// ErrInferenceModelNotFound indicates the requested inference model does not exist.
	ErrInferenceModelNotFound = errors.New("inference model not found")

	// ErrInferenceModelAlreadyExists indicates the provider/model pair already exists.
	ErrInferenceModelAlreadyExists = errors.New("inference model already exists")

	// ErrInferenceModelInUse indicates an operation route still references the model.
	ErrInferenceModelInUse = errors.New("inference model is in use")

	// ErrInferenceOperationNotFound indicates the numeric operation reference has no registry row.
	ErrInferenceOperationNotFound = errors.New("inference operation not found")

	// ErrInferenceAttemptNotFound indicates the requested attempt-ledger row does not exist.
	ErrInferenceAttemptNotFound = errors.New("inference attempt not found")

	// ErrInferenceControlPlaneUnavailable indicates the application has not wired the AI
	// control-plane store into the admin service.
	ErrInferenceControlPlaneUnavailable = errors.New("inference control plane is not configured")

	// ErrInferenceSessionRetargetNotAllowed indicates the operation's application re-target gate,
	// or the organization's member re-target policy, forbids this actor's kind of session move.
	ErrInferenceSessionRetargetNotAllowed = errors.New("inference session re-targeting is not allowed for this operation")

	// ErrInferencePolicyInvalid indicates an inference routing policy, policy session action, or
	// session inference pin request that fails validation.
	ErrInferencePolicyInvalid = errors.New("invalid inference policy")

	// ErrInferencePolicyVersionConflict indicates an inference routing policy or session pin
	// changed after the caller read the version it expected.
	ErrInferencePolicyVersionConflict = errors.New("inference policy version conflict")

	// ErrInferenceAncestorProviderBlocked indicates an organization policy names a model whose
	// provider a parent organization's provider allowlist excludes.
	ErrInferenceAncestorProviderBlocked = errors.New("a parent organization's provider allowlist blocks this model")

	// ErrAgentOperationRouteUnavailable indicates the application has not wired the control-plane
	// route store into the agent runtime, so an agent's operation cannot select a model.
	ErrAgentOperationRouteUnavailable = errors.New("agent operation route store is not configured")

	// ErrAgentOperationRouteUnresolved indicates an agent's operation did not resolve to a usable
	// inference-catalog row: no route, no model, or an inactive/incomplete model.
	ErrAgentOperationRouteUnresolved = errors.New("agent operation route could not be resolved")

	// ErrCareerApplicationClosed indicates the job is not accepting applications.
	ErrCareerApplicationClosed = errors.New("career application window is closed")

	// ErrCareerDraftTokenInvalid indicates the resume upload draft token is invalid or expired.
	ErrCareerDraftTokenInvalid = errors.New("career draft token is invalid")

	// ErrCareerResumeRequired indicates an application cannot be submitted without a resume attachment.
	ErrCareerResumeRequired = errors.New("career resume attachment is required")

	// ErrCareerResumeAttachmentInvalid indicates the provided resume attachment does not belong to the draft.
	ErrCareerResumeAttachmentInvalid = errors.New("career resume attachment is invalid")

	// ErrCareerInvalidStatusTransition indicates the requested status transition is not allowed.
	ErrCareerInvalidStatusTransition = errors.New("invalid career status transition")
)

// Chat-related errors for chat/messaging operations.
var (
	// ErrChatDisabled indicates the chat feature is disabled by configuration.
	ErrChatDisabled = errors.New("chat is disabled")

	// ErrChatChannelNotFound indicates the requested channel does not exist.
	ErrChatChannelNotFound = errors.New("chat channel not found")

	// ErrChatMessageNotFound indicates the requested message does not exist.
	ErrChatMessageNotFound = errors.New("chat message not found")

	// ErrChatParticipantNotFound indicates the requested participant record does not exist.
	ErrChatParticipantNotFound = errors.New("chat participant not found")

	// ErrChatAvatarAttachmentInvalid indicates that a requested channel avatar is
	// not an available public attachment. All invalid attachment states share this
	// error so callers cannot distinguish missing, internal, or unvetted rows.
	ErrChatAvatarAttachmentInvalid = errors.New("chat avatar attachment is invalid")

	// ErrChatAgentSessionNotFound indicates the requested agent chat session does not exist.
	ErrChatAgentSessionNotFound = errors.New("chat agent session not found")

	ErrChatAgentSessionLocationConflict       = errors.New("chat agent session location is already occupied")
	ErrChatAgentSessionScopeInvalid           = errors.New("chat agent session scope is invalid")
	ErrChatMessageAgentSessionRootPinned      = errors.New("chat message anchors an agent session and cannot be deleted")
	ErrChatChannelAgentSessionThreadsRequired = errors.New("chat channel has thread sessions and requires threads")

	// ErrChatAgentSessionVersionConflict indicates an optimistic-concurrency conflict updating an
	// agent chat session config (the expected active version was stale).
	ErrChatAgentSessionVersionConflict = errors.New("chat agent session config version conflict")

	// ErrChatAgentSessionIdempotencyConflict indicates a create idempotency key was replayed with
	// request inputs that do not match the original create.
	ErrChatAgentSessionIdempotencyConflict = errors.New("chat agent session create idempotency conflict")

	// ErrChatAgentSessionBusy indicates a BLOCKED-mode agent session refused a message because one
	// of its runs is active or the session is suspended (nothing was sent), or a Resume found the
	// stopped run still being cancelled.
	ErrChatAgentSessionBusy = errors.New("chat agent session is busy")

	// ErrChatAgentPendingInputLimitExceeded indicates an agent session already holds the maximum
	// number of queued inputs. Nothing was sent.
	ErrChatAgentPendingInputLimitExceeded = errors.New("chat agent session pending input limit exceeded")

	// ErrChatAgentBroadcastWouldWait indicates a thread reply also sent to the channel was refused
	// because an agent session it reaches is busy or suspended: one send never splits into a message
	// now for one session and queued input for another. Nothing was sent; the reply can be sent to
	// the thread alone.
	ErrChatAgentBroadcastWouldWait = errors.New("chat agent session would make a broadcast reply wait")

	// ErrChatAgentQueuedInputNotQueued indicates an edit or removal named queued agent input that
	// has left its session's queue: the agent received it (it is a chat message now), or it was
	// removed or discarded.
	ErrChatAgentQueuedInputNotQueued = errors.New("chat agent input is no longer queued")

	// ErrChatAgentRunNotFound indicates the requested agent run does not exist.
	ErrChatAgentRunNotFound = errors.New("chat agent run not found")

	// ErrChatAgentRunNotActive indicates a Stop named a run that is no longer its session's active
	// run: it already ended, so a newer run is not stopped in its place.
	ErrChatAgentRunNotActive = errors.New("chat agent run is not active")

	// ErrChatAgentStopNotAllowed indicates a Stop named a run whose captured session policy does not
	// allow people to stop it, or a run of a workflow-owned agent.
	ErrChatAgentStopNotAllowed = errors.New("chat agent run does not allow stop")

	// ErrChatAgentDelegationNotAllowed indicates the parent agent does not delegate at all or does
	// not declare the requested child agent type.
	ErrChatAgentDelegationNotAllowed = errors.New("chat agent delegation is not allowed for this agent type")

	// ErrChatAgentDelegationChildInvalid indicates the requested child agent type cannot be
	// delegated to: unregistered, workflow-owned, itself delegating, or sharing the parent's bot.
	ErrChatAgentDelegationChildInvalid = errors.New("chat agent delegation child type is invalid")

	// ErrChatAgentDelegationLimitExceeded indicates the parent already has its maximum number of
	// children with an active run.
	ErrChatAgentDelegationLimitExceeded = errors.New("chat agent delegation active-children limit exceeded")

	// ErrChatForkDisabled indicates the source channel's fork rules do not allow forking.
	ErrChatForkDisabled = errors.New("chat channel forking is disabled")

	// ErrChatForkTooLarge indicates a fork would copy more source messages than
	// chat_fork_max_copied_messages allows.
	ErrChatForkTooLarge = errors.New("chat channel fork is too large")

	// ErrChatForkPointInvalid indicates the fork point is not a message of the source channel
	// that the forker can see.
	ErrChatForkPointInvalid = errors.New("chat channel fork point is invalid")

	// ErrChatForkMemberInvalid indicates a requested fork member is outside the source's member
	// pool or rejected by the channel member rules.
	ErrChatForkMemberInvalid = errors.New("chat channel fork member is invalid")

	// ErrChatForkRequestKeyConflict indicates a concurrent fork with the same client request ID
	// committed first; retrying replays that fork.
	ErrChatForkRequestKeyConflict = errors.New("chat channel fork request key conflict")

	// ErrChatForkAgentOrganizationNotMember indicates the fork would clone an agent session of an
	// organization the forker is not an effective member of: the forker would own agents that
	// could never run.
	ErrChatForkAgentOrganizationNotMember = errors.New("chat channel fork clones an agent of an organization the forker is not a member of")

	// ErrGraphBudgetExhausted indicates a limit of the delegation graph's budget pool is used up,
	// so the graph admits no new spawn, send, or resume until SetGraphBudget raises it.
	ErrGraphBudgetExhausted = errors.New("delegation graph budget exhausted")

	// ErrChatAgentDelegationChildrenExhausted indicates the parent session has created as many
	// children over its lifetime as its delegation allows (MaxChildrenPerSession). It wraps
	// ErrChatAgentDelegationLimitExceeded.
	ErrChatAgentDelegationChildrenExhausted = fmt.Errorf("%w: lifetime children limit reached", ErrChatAgentDelegationLimitExceeded)

	// ErrChatAgentDelegationParentInvalid indicates the named parent session cannot spawn: it is
	// not a main-timeline session, is itself a delegated child, is archived, is suspended by a
	// person's Stop, or the named parent run does not belong to it.
	ErrChatAgentDelegationParentInvalid = errors.New("chat agent delegation parent session is invalid")

	// ErrChatAgentSessionNotDelegated indicates the session is not a delegated child of the named
	// parent (or of any parent) when the operation requires one.
	ErrChatAgentSessionNotDelegated = errors.New("chat agent session is not a delegated child")

	// ErrChatAgentChildRunLimit indicates a delegated child has used
	// chat_agent_delegation_max_runs_per_child runs, so a continue send cannot start another.
	ErrChatAgentChildRunLimit = errors.New("delegated child reached its run limit")

	// ErrChatAgentChildUnavailable indicates a delegated child cannot receive a parent message: it
	// is archived, its channel or thread is unavailable, its parent cannot wake children, or (for a
	// continue send) it does not run automatically.
	ErrChatAgentChildUnavailable = errors.New("delegated child cannot receive messages")

	// ErrChatAgentMessageInvalid indicates a parent-to-child message send was malformed or its
	// written row landed outside the child's thread. Both are service bugs, not caller input.
	ErrChatAgentMessageInvalid = errors.New("delegated agent message is invalid")

	// ErrChatAgentMessagePageInvalid indicates a delegated transcript read named an invalid page.
	ErrChatAgentMessagePageInvalid = errors.New("delegated agent message page is invalid")

	// ErrGraphRevisionAdvanced indicates ParkGraph was refused because a resume event committed
	// after the revision the application read; the graph may be drivable, so keep driving it.
	ErrGraphRevisionAdvanced = errors.New("delegation graph resume revision advanced")

	// ErrCompactionWatermarkInvalid indicates a compaction checkpoint's covers_through watermark is
	// out of range: below the previous checkpoint's watermark (would un-compact), or above the
	// checkpoint's own anchor/id (would claim to cover not-yet-written messages). Decision 3 integrity.
	ErrCompactionWatermarkInvalid = errors.New("compaction watermark is invalid")

	// ErrToolCallNotResolvable indicates the message is not a TOOL_CALL awaiting approval (e.g. a
	// non-tool message, a NOT_REQUIRED call, or a missing tool-call side row).
	ErrToolCallNotResolvable = errors.New("tool call is not awaiting approval")

	// ErrToolCallResolutionConflict indicates a resolution request contradicts the recorded final
	// approval state (e.g. approving an already-denied/expired call). Re-resolving to the same final
	// state is idempotent and not a conflict.
	ErrToolCallResolutionConflict = errors.New("tool call resolution conflicts with its recorded final state")

	// ErrToolCallApprovalExpired indicates an approval request arrived after the TOOL_CALL's durable
	// approval deadline. Expiry is enforced by the approval transition itself, independently of the
	// optional background sweep.
	ErrToolCallApprovalExpired = errors.New("tool call approval has expired")

	// ErrNotImplemented indicates a service method whose behavior has not been wired yet.
	ErrNotImplemented = errors.New("not implemented")

	// ErrChatMessageImmutable indicates an attempt to edit/delete/react-to/reply-to a non-STANDARD
	// transcript row (TOOL_CALL, TOOL_RESULT, SYSTEM_EVENT, COMPACTION_SUMMARY, AGENT_NOTE) or a
	// hidden message. Those rows are immutable through the public message API; only the well-defined
	// approval/execution transitions on a TOOL_CALL row are permitted, and those go through the
	// approval lane, not edit/delete.
	ErrChatMessageImmutable = errors.New("chat message is immutable")

	// ErrChatNotMember indicates the user is not a participant in the channel.
	ErrChatNotMember = errors.New("user is not a member of this chat channel")

	// ErrChatChannelArchived indicates the channel is archived and disallows the operation.
	ErrChatChannelArchived = errors.New("chat channel is archived")

	// ErrChatInvalidThread indicates thread semantics are invalid (e.g., parent root missing / wrong timeline).
	ErrChatInvalidThread = errors.New("invalid chat thread reference")

	// ErrChatInvalidReply indicates reply-to semantics are invalid for the current timeline/thread.
	ErrChatInvalidReply = errors.New("invalid chat reply reference")

	// ErrChatInvalidBroadcast indicates "also send to channel" was requested for a message that
	// is not a thread reply. A thread reply in a channel without threads reports
	// ErrChatFeatureDisabled instead, since the broadcast is checked before the thread gate.
	ErrChatInvalidBroadcast = errors.New("invalid chat thread broadcast")

	// ErrChatFeatureDisabled indicates a requested chat capability is disabled by configuration.
	ErrChatFeatureDisabled = errors.New("chat feature is disabled")

	// ErrChatReactionAlreadyExists indicates the user already reacted and additional reactions are not allowed.
	ErrChatReactionAlreadyExists = errors.New("chat reaction already exists")

	// ErrChatReactionNotFound indicates the reaction to remove does not exist.
	ErrChatReactionNotFound = errors.New("chat reaction not found")

	// ErrChatMentionLimitExceeded indicates a message mentions too many users.
	ErrChatMentionLimitExceeded = errors.New("chat mention limit exceeded")

	// ErrChatInvalidMention indicates a message mention is invalid (e.g., not a channel participant).
	ErrChatInvalidMention = errors.New("invalid chat mention")
)

// Email-specific errors for email verification and authentication.
var (
	// ErrEmailNotVerified indicates the user's email address has not been verified.
	ErrEmailNotVerified = errors.New("email not verified")

	// ErrEmailAlreadyExists indicates an account with this email already exists.
	ErrEmailAlreadyExists = errors.New("email already exists")

	// ErrEmailAlreadyVerified indicates the email is already verified.
	ErrEmailAlreadyVerified = errors.New("email already verified")

	// ErrUsernameAlreadyExists indicates the username is already taken by another user.
	ErrUsernameAlreadyExists = errors.New("username already exists")

	// ErrActiveVerificationToken indicates a verification token is already active.
	ErrActiveVerificationToken = errors.New("active verification token already exists")

	// ErrVerificationCooldown indicates the cooldown period hasn't elapsed.
	ErrVerificationCooldown = errors.New("please wait before requesting another verification email")
)

// Format validation errors for input validation.
var (
	// ErrInvalidUserIDFormat indicates the user ID doesn't match expected format.
	ErrInvalidUserIDFormat = errors.New("invalid user ID format")

	// ErrInvalidUsernameFormat indicates the username doesn't match requirements.
	ErrInvalidUsernameFormat = errors.New("invalid username format")

	// ErrInvalidEmailFormat indicates the email address is malformed.
	ErrInvalidEmailFormat = errors.New("invalid email format")

	// ErrUsernameRequired indicates a username is required but was not provided.
	ErrUsernameRequired = errors.New("username is required")

	// ErrOAuthSignupDisabled indicates OAuth signup is disabled for new accounts.
	ErrOAuthSignupDisabled = errors.New("oauth signup is disabled")
	// ErrEmailAuthDisabled indicates password/email authentication is disabled by policy.
	ErrEmailAuthDisabled = errors.New("email authentication is disabled")
)

// Attachment-related errors for file operations and attachments.
var (
	// ErrUnauthorizedAttachmentAccess indicates the user doesn't have permission to access the attachment.
	ErrUnauthorizedAttachmentAccess = errors.New("unauthorized attachment access")

	// ErrAttachmentSecurityViolation indicates a security validation failure such as MIME type spoofing.
	ErrAttachmentSecurityViolation = errors.New("attachment security validation failed")
)

// Multi-auth specific errors for handling multiple authentication methods.
var (
	// ErrPasswordAuthAlreadyExists indicates the user already has password authentication set up.
	ErrPasswordAuthAlreadyExists = errors.New("password authentication already exists for this user")

	// ErrOAuthUserCannotRegister indicates an OAuth user is trying to register with email/password.
	ErrOAuthUserCannotRegister = errors.New("this email is already registered, please sign in with your social account")

	// ErrOAuthUserMustAddPassword provides guidance for OAuth users trying email registration.
	ErrOAuthUserMustAddPassword = errors.New("you're already signed up with a social account, sign in and add a password from settings")

	// ErrNoPasswordToReset indicates a user without password auth is trying to reset password.
	ErrNoPasswordToReset = errors.New("no password set for this account, sign in with your social account")

	// ErrPasswordAuthRequired indicates the requested operation requires password authentication first.
	ErrPasswordAuthRequired = errors.New("password authentication is required for this operation")

	// ErrCannotRemoveLastAuthMethod prevents users from removing their only auth method.
	ErrCannotRemoveLastAuthMethod = errors.New("cannot remove last authentication method")

	// ErrReauthenticationRequired indicates the user needs to authenticate again for sensitive operations.
	ErrReauthenticationRequired = errors.New("please sign in again to continue")

	// ErrAuthMethodNotFound indicates the requested auth method doesn't exist for the user.
	ErrAuthMethodNotFound = errors.New("authentication method not found")
)

// OTP/TOTP 2FA errors for OTP authentication and setup.
var (
	// ErrOTPAlreadyEnabled indicates OTP is already enabled for the user.
	ErrOTPAlreadyEnabled = errors.New("OTP authentication is already enabled")

	// ErrOTPNotEnabled indicates OTP is not enabled for the user.
	ErrOTPNotEnabled = errors.New("OTP authentication is not enabled")

	// ErrOTPNotConfigured indicates OTP setup was not initiated (no secret found).
	ErrOTPNotConfigured = errors.New("OTP authentication not configured")

	// ErrInvalidOTPCode indicates the provided OTP code is invalid or expired.
	ErrInvalidOTPCode = errors.New("invalid OTP code")
)

// Organization-related errors for organization management and membership operations.
var (
	// ErrOrganizationNotFound indicates the requested organization does not exist.
	ErrOrganizationNotFound = errors.New("organization not found")

	// ErrOrganizationAlreadyExists indicates an organization with the same name or slug already exists.
	ErrOrganizationAlreadyExists = errors.New("organization already exists")

	// ErrOrganizationNameRequired indicates the organization name is required but was not provided.
	ErrOrganizationNameRequired = errors.New("organization name is required")

	// ErrOrganizationSlugRequired indicates the organization slug is required but was not provided.
	ErrOrganizationSlugRequired = errors.New("organization slug is required")

	// ErrInvalidOrganizationSlug indicates the organization slug contains invalid characters or format.
	ErrInvalidOrganizationSlug = errors.New("invalid organization slug format")

	// ErrOrganizationSlugTaken indicates the requested organization slug is already in use.
	ErrOrganizationSlugTaken = errors.New("organization slug is already taken")

	// ErrOrganizationSlugReserved indicates the requested slug belongs to a system-managed namespace.
	ErrOrganizationSlugReserved = errors.New("organization slug is reserved")

	// ErrOrganizationCallbackFailed indicates an organization lifecycle callback failed.
	ErrOrganizationCallbackFailed = errors.New("organization callback failed")

	// ErrPersonalWorkspaceNotFound indicates that no personal workspace has ever
	// been provisioned for the requested user.
	ErrPersonalWorkspaceNotFound = errors.New("personal workspace not found")

	// ErrPersonalWorkspaceArchived indicates that the user's lifetime personal
	// workspace exists but is currently soft-deleted and must be restored
	// explicitly rather than replaced.
	ErrPersonalWorkspaceArchived = errors.New("personal workspace is archived")

	// ErrPersonalWorkspaceConflict indicates that retained ownership, hierarchy,
	// slug, or membership state cannot be reconciled safely.
	ErrPersonalWorkspaceConflict = errors.New("personal workspace state conflicts with managed invariants")

	// ErrPersonalWorkspaceOwnershipImmutable indicates an ordinary organization
	// request attempted to set or mutate authoritative personal ownership.
	ErrPersonalWorkspaceOwnershipImmutable = errors.New("personal workspace ownership is immutable")

	// ErrPersonalWorkspaceOwnerRequired indicates an operation on a personal
	// workspace tree may only be performed by its authoritative owner.
	ErrPersonalWorkspaceOwnerRequired = errors.New("personal workspace operation requires its owner")

	// ErrPersonalWorkspaceManagedFieldsImmutable indicates an ordinary request
	// attempted to change a system-managed personal root slug or description.
	ErrPersonalWorkspaceManagedFieldsImmutable = errors.New("personal workspace slug and description are system-managed")

	// ErrPersonalWorkspaceDeletionRestricted indicates personal organizations
	// may only be archived through the owning user's account lifecycle.
	ErrPersonalWorkspaceDeletionRestricted = errors.New("personal workspace organizations cannot be deleted directly")

	// ErrOrganizationNameTooLong indicates the organization name exceeds maximum length.
	ErrOrganizationNameTooLong = errors.New("organization name is too long")

	// ErrOrganizationDescriptionTooLong indicates the organization description exceeds maximum length.
	ErrOrganizationDescriptionTooLong = errors.New("organization description is too long")

	// ErrInvalidOrganizationID indicates the provided organization ID is invalid or malformed.
	ErrInvalidOrganizationID = errors.New("invalid organization ID")

	// ErrOrganizationParentInvalid indicates the provided parent organization ID is invalid.
	ErrOrganizationParentInvalid = errors.New("invalid parent organization ID")

	// ErrOrganizationParentNotFound indicates the parent organization does not exist.
	ErrOrganizationParentNotFound = errors.New("parent organization not found")

	// ErrOrganizationParentImmutable indicates parent organization cannot be changed after creation.
	ErrOrganizationParentImmutable = errors.New("parent organization is immutable")

	// ErrOrganizationLineageImmutable indicates lineage cannot be changed after creation.
	ErrOrganizationLineageImmutable = errors.New("organization lineage is immutable")

	// ErrOrganizationHierarchyDepthExceeded indicates org hierarchy depth exceeds configured limit.
	ErrOrganizationHierarchyDepthExceeded = errors.New("organization hierarchy depth exceeds configured limit")

	// ErrOrganizationCreateChildNotAllowed indicates the user lacks permission to create a child org.
	ErrOrganizationCreateChildNotAllowed = errors.New("insufficient permissions to create child organization")

	// ErrOrganizationSelfJoinNotAllowed indicates the user lacks permission to self-join an organization.
	ErrOrganizationSelfJoinNotAllowed = errors.New("insufficient permissions to self-join organization")

	// ErrOrganizationAncestorMembershipRequired indicates missing ancestor membership for hierarchical orgs.
	ErrOrganizationAncestorMembershipRequired = errors.New("membership in all ancestor organizations is required")

	// ErrOrganizationEmailDomainExists indicates the email domain is already associated with an organization.
	ErrOrganizationEmailDomainExists = errors.New("organization email domain already exists")

	// ErrOrganizationEmailDomainNotFound indicates the requested email domain mapping does not exist.
	ErrOrganizationEmailDomainNotFound = errors.New("organization email domain not found")

	// ErrOrganizationEmailDomainsDisabled indicates the email domain feature is disabled.
	ErrOrganizationEmailDomainsDisabled = errors.New("organization email domains feature is disabled")

	// ErrOrganizationEmailDomainManagementDisabled indicates organization-managed domain changes are disabled.
	ErrOrganizationEmailDomainManagementDisabled = errors.New("organization email domain management is disabled")

	// ErrOrganizationTypeCapabilityDenied indicates the organization's type is below the required capability threshold.
	ErrOrganizationTypeCapabilityDenied = errors.New("organization type does not allow this operation")
)

// Organization membership errors for user-organization relationship management.
var (
	// ErrUserNotMember indicates the user is not a member of the organization.
	ErrUserNotMember = errors.New("user is not a member of this organization")

	// ErrUserAlreadyMember indicates the user is already a member of the organization.
	ErrUserAlreadyMember = errors.New("user is already a member of this organization")

	// ErrPersonalWorkspaceMembershipRestricted indicates personal trees do not
	// allow explicit memberships for anyone other than their authoritative owner.
	ErrPersonalWorkspaceMembershipRestricted = errors.New("personal workspace membership is restricted to its owner")

	// ErrPersonalWorkspaceOwnerMembershipProtected indicates the personal owner
	// membership cannot be removed or assigned a role other than the configured role.
	ErrPersonalWorkspaceOwnerMembershipProtected = errors.New("personal workspace owner membership is protected")

	// ErrInvalidOrganizationRole indicates the provided organization role is invalid.
	ErrInvalidOrganizationRole = errors.New("invalid organization role")

	// ErrInsufficientOrganizationPermissions indicates the user lacks required organization permissions.
	ErrInsufficientOrganizationPermissions = errors.New("insufficient organization permissions")

	// ErrCannotPromoteAboveOwnRole indicates a user cannot promote someone to a role higher than their own.
	ErrCannotPromoteAboveOwnRole = errors.New("cannot promote user to a role higher than your own")

	// ErrCannotManageHigherRole indicates a user cannot manage someone with a higher role.
	ErrCannotManageHigherRole = errors.New("cannot manage user with higher organization role")

	// ErrCannotRemoveLastOwner indicates the last owner of an organization cannot be removed.
	ErrCannotRemoveLastOwner = errors.New("cannot remove the last owner of an organization")

	// ErrCannotDemoteLastOwner indicates the last owner of an organization cannot be demoted below owner.
	ErrCannotDemoteLastOwner = errors.New("cannot demote the last owner of an organization")

	// ErrInvitationNotFound indicates the requested invitation does not exist.
	ErrInvitationNotFound = errors.New("invitation not found")

	// ErrInvitationAlreadyExists indicates a pending invitation already exists for this user and organization.
	ErrInvitationAlreadyExists = errors.New("pending invitation already exists")

	// ErrInvitationExpired indicates the invitation has passed its expiry time.
	ErrInvitationExpired = errors.New("invitation has expired")

	// ErrInvitationAlreadyProcessed indicates the invitation has already been accepted or declined.
	ErrInvitationAlreadyProcessed = errors.New("invitation has already been processed")
)
