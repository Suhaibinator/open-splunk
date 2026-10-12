package common_sanitizer

import (
	"errors"
	"fmt"
)

// configuredLengthRangeError keeps the package sentinel stable while reporting
// the bounds from the request's sampled configuration generation.
func configuredLengthRangeError(err error, minLength, maxLength int) error {
	return fmt.Errorf("%w: length must be between %d and %d characters", err, minLength, maxLength)
}

// configuredMinimumLengthError keeps the package sentinel stable while
// reporting the lower bound from the request's sampled configuration generation.
func configuredMinimumLengthError(err error, minLength int) error {
	return fmt.Errorf("%w: minimum %d characters", err, minLength)
}

// configuredMaximumLengthError keeps the package sentinel stable while
// reporting the upper bound from the request's sampled configuration generation.
func configuredMaximumLengthError(err error, maxLength int) error {
	return fmt.Errorf("%w: maximum %d characters", err, maxLength)
}

// configuredMaximumError is the non-character-count equivalent used for hot
// numeric request limits such as pagination page size.
func configuredMaximumError(err error, maximum int) error {
	return fmt.Errorf("%w: maximum %d", err, maximum)
}

func configuredSlugFormatError(err error, minLength, maxLength int) error {
	return fmt.Errorf(
		"%w: must be between %d and %d characters and contain only lowercase letters, numbers, and hyphens",
		err,
		minLength,
		maxLength,
	)
}

// Error definitions for sanitization provide specific error messages for various
// validation failures. These errors help clients understand exactly what validation
// rule was violated.
var (
	// ErrUsernameEmpty indicates that an empty username was provided.
	ErrUsernameEmpty = errors.New("username cannot be empty")
	// ErrUsernameTooShort indicates that the username is shorter than the minimum allowed length.
	ErrUsernameTooShort = errors.New("username is too short")
	// ErrUsernameTooLong indicates that the username exceeds the maximum allowed length.
	ErrUsernameTooLong = errors.New("username is too long")
	// ErrUsernameInvalidCharacters indicates that the username contains invalid characters.
	ErrUsernameInvalidCharacters = errors.New("username can only contain letters, numbers, underscores, and hyphens")
	// ErrUsernameStartsWithInvalidChar indicates that the username starts with an invalid character.
	ErrUsernameStartsWithInvalidChar = errors.New("username must start with a letter or number")
	// ErrInvalidEmailFormat indicates that the email address has an invalid format.
	ErrInvalidEmailFormat = errors.New("invalid email format")
	// ErrInvalidPasswordFormat indicates that the password does not meet length requirements.
	ErrInvalidPasswordFormat = errors.New("invalid password format")
	// ErrInvalidOAuthProviderSlug indicates an OAuth provider slug that is not
	// lowercase alphanumeric words joined by single hyphens, at most 64 bytes.
	ErrInvalidOAuthProviderSlug = errors.New("invalid OAuth provider slug")
	// ErrInvalidFrontendOrigin indicates that a frontend_origin value is not a bare
	// http(s) origin of the form scheme://host[:port].
	ErrInvalidFrontendOrigin = errors.New("invalid frontend origin: must be an http(s) origin without path, query, or fragment")
	// ErrEmptyVerificationCode indicates that a verification code was not provided.
	ErrEmptyVerificationCode = errors.New("verification code cannot be empty")
	// ErrInvalidVerificationCodeLen indicates that the verification code length is outside acceptable bounds.
	ErrInvalidVerificationCodeLen = errors.New("invalid verification code length")
	// ErrEmptyOAuthCode indicates that an OAuth authorization code was not provided.
	ErrEmptyOAuthCode = errors.New("OAuth code cannot be empty")
	// ErrEmptyOAuthState indicates that an OAuth state parameter was not provided.
	ErrEmptyOAuthState = errors.New("OAuth state parameter cannot be empty")
	// ErrInvalidOAuthState indicates that the OAuth state parameter is invalid.
	ErrInvalidOAuthState = errors.New("invalid OAuth state parameter")
	// ErrOAuthCodeTooLong indicates an OAuth authorization code longer than the
	// configured maximum.
	ErrOAuthCodeTooLong = errors.New("OAuth code is too long")
	// ErrOAuthURLTooLong indicates an OAuth state request URL longer than the
	// configured maximum state length, which would make the state unissuable.
	ErrOAuthURLTooLong = errors.New("OAuth URL is too long")

	// Email-specific validation errors provide detailed feedback about email validation failures.
	// ErrEmailEmpty indicates that an empty email address was provided.
	ErrEmailEmpty = errors.New("email address cannot be empty")
	// ErrEmailTooLong indicates that the email address exceeds the maximum allowed length.
	ErrEmailTooLong = errors.New("email address is too long")
	// ErrEmailLocalPartTooLong indicates that the local part of the email (before @) is too long.
	ErrEmailLocalPartTooLong = errors.New("email username is too long")
	// ErrEmailDomainTooLong indicates that the domain part of the email (after @) is too long.
	ErrEmailDomainTooLong = errors.New("email domain is too long")
	// ErrEmailMissingAtSign indicates that the email address lacks the required @ symbol.
	ErrEmailMissingAtSign = errors.New("email address must contain an @ symbol")
	// ErrEmailMissingDomain indicates that the email address has no domain after the @ symbol.
	ErrEmailMissingDomain = errors.New("email address must have a domain after the @ symbol")
	// ErrEmailMissingLocalPart indicates that the email address has no local part before the @ symbol.
	ErrEmailMissingLocalPart = errors.New("email address must have a username before the @ symbol")
	// ErrEmailInvalidDomain indicates that the email domain does not meet validation requirements.
	ErrEmailInvalidDomain = errors.New("email domain is invalid (must contain at least one dot)")
	// ErrEmailInvalidCharacters indicates that the email contains characters not allowed by the validation rules.
	ErrEmailInvalidCharacters = errors.New("email contains invalid characters")
	// ErrEmailStartsWithDot indicates that the local part of the email starts with a dot, which is invalid.
	ErrEmailStartsWithDot = errors.New("email username cannot start with a dot")
	// ErrEmailEndsWithDot indicates that the local part of the email ends with a dot, which is invalid.
	ErrEmailEndsWithDot = errors.New("email username cannot end with a dot")
	// ErrEmailConsecutiveDots indicates that the local part contains consecutive dots, which is invalid.
	ErrEmailConsecutiveDots = errors.New("email username cannot contain consecutive dots")
	// ErrEmailDisposableNotAllowed indicates that the email domain is from a known disposable email provider.
	ErrEmailDisposableNotAllowed = errors.New("disposable email addresses are not allowed")
	// ErrEmailDomainBlocked indicates that the email domain is in the blocked domains list.
	ErrEmailDomainBlocked = errors.New("email domain is not allowed")
	// ErrEmailDomainNotAllowed indicates that the email domain is not in the allowed domains list.
	ErrEmailDomainNotAllowed = errors.New("email domain is not in the list of allowed domains")

	// Contact form validation errors provide specific feedback for contact form field validation.
	// ErrInvalidName indicates that the contact form name field does not meet length requirements.
	ErrInvalidName = errors.New("invalid name length")
	// ErrInvalidSubject indicates that the contact form subject field does not meet length requirements.
	ErrInvalidSubject = errors.New("invalid subject length")
	// ErrInvalidMessage indicates that the contact form message field does not meet length requirements.
	ErrInvalidMessage = errors.New("invalid message length")

	// Multi-auth validation errors provide specific feedback for multi-authentication operations.
	// ErrInvalidRequest indicates that the request is missing required fields or is malformed.
	ErrInvalidRequest = errors.New("invalid request")
	// ErrPasswordConfirmationRequired indicates that password confirmation is required but was not provided.
	ErrPasswordConfirmationRequired = errors.New("password confirmation is required")
	// ErrPasswordMismatch indicates that the password and confirmation password do not match.
	ErrPasswordMismatch = errors.New("passwords do not match")
	// ErrPasswordMissingUppercase indicates that the password does not contain an uppercase letter.
	ErrPasswordMissingUppercase = errors.New("password must contain at least one uppercase letter")
	// ErrPasswordMissingLowercase indicates that the password does not contain a lowercase letter.
	ErrPasswordMissingLowercase = errors.New("password must contain at least one lowercase letter")
	// ErrPasswordMissingNumber indicates that the password does not contain a number.
	ErrPasswordMissingNumber = errors.New("password must contain at least one number")
	// ErrPasswordMissingSpecialChar indicates that the password does not contain a special character.
	ErrPasswordMissingSpecialChar = errors.New("password must contain at least one special character")

	// Admin validation errors provide specific feedback for admin API operations.
	// ErrSearchTermTooLong indicates that the search term exceeds the maximum allowed length.
	ErrSearchTermTooLong = errors.New("search term is too long")
	// ErrUserIDRequired indicates that a user ID is required but was not provided.
	ErrUserIDRequired = errors.New("user ID is required")
	// ErrAttachmentIDRequired indicates that an attachment ID is required but was not provided.
	ErrAttachmentIDRequired = errors.New("attachment ID is required")
	// ErrInvalidUserIDFormat indicates that the user ID has an invalid format.
	ErrInvalidUserIDFormat = errors.New("invalid user ID format")
	// ErrReasonTooLong indicates that the reason field exceeds the maximum allowed length.
	ErrReasonTooLong = errors.New("reason is too long (maximum 500 characters)")
	// ErrInvalidUserType indicates that an invalid or unspecified user type was provided.
	ErrInvalidUserType = errors.New("invalid user type specified")
	// ErrReasonRequired indicates that a reason is required but was not provided.
	ErrReasonRequired = errors.New("reason is required")
	// ErrInvalidStatus indicates that an invalid status value was provided.
	ErrInvalidStatus = errors.New("invalid status value")
	// ErrMessageIDRequired indicates that a message ID is required but was not provided.
	ErrMessageIDRequired = errors.New("message ID is required")
	// ErrReferenceIDRequired indicates that a reference ID is required but was not provided.
	ErrReferenceIDRequired = errors.New("reference ID is required")
	// ErrInvalidAttachmentStatus indicates that an invalid attachment status was provided.
	ErrInvalidAttachmentStatus = errors.New("invalid attachment status")
	// ErrContentTypeTooLong indicates that the content type filter exceeds the maximum allowed length.
	ErrContentTypeTooLong = errors.New("content type filter is too long (maximum 100 characters)")
	// ErrFilterValueTooLong indicates that a retained filter value exceeds the allowed length.
	ErrFilterValueTooLong = errors.New("filter value is too long")
	// ErrInvalidSizeRange indicates that the size range is invalid (min > max).
	ErrInvalidSizeRange = errors.New("invalid size range: minimum size cannot be greater than maximum size")
	// ErrInvalidGroupBy indicates that an invalid group by parameter was provided.
	ErrInvalidGroupBy = errors.New("invalid group by parameter")
	// ErrInvalidTokenType indicates that an invalid token type was provided.
	ErrInvalidTokenType = errors.New("invalid token type")
	// ErrInvalidTokenStatus indicates that an invalid token status was provided.
	ErrInvalidTokenStatus = errors.New("invalid token status")

	// Career validation errors provide specific feedback for career/job posting operations.
	// ErrInvalidCareerJobPostingSlug indicates that the job posting slug has an invalid format.
	ErrInvalidCareerJobPostingSlug = errors.New("invalid job posting slug format")
	// ErrJobPostingIDRequired indicates that a job posting ID is required but was not provided.
	ErrJobPostingIDRequired = errors.New("job posting ID is required")
	// ErrInvalidResumeContentType indicates that the resume file type is not supported.
	ErrInvalidResumeContentType = errors.New("unsupported resume file type (accepted: PDF, DOC, DOCX, TXT)")
	// ErrPaginationLimitTooHigh indicates that the pagination limit exceeds the maximum allowed value.
	ErrPaginationLimitTooHigh = errors.New("pagination limit is too high")
	// ErrOrganizationParentOptionsLimitTooHigh indicates that a parent-option request exceeds its bounded page size.
	ErrOrganizationParentOptionsLimitTooHigh = errors.New("organization parent options limit is too high")
	// ErrInvalidDateRange indicates that the date range is invalid (from > to).
	ErrInvalidDateRange = errors.New("invalid date range: from date cannot be after to date")
	// ErrAnalyticsDateRangeTooLarge indicates that an analytics query exceeds
	// the bounded historical window accepted by the admin API.
	ErrAnalyticsDateRangeTooLarge = errors.New("analytics date range exceeds maximum of 366 days")

	// User settings validation errors provide specific feedback for user settings operations.
	// ErrInvalidLanguage indicates that an invalid or unspecified language was provided.
	ErrInvalidLanguage = errors.New("invalid language: must specify a valid language preference")

	// Attachment validation errors provide specific feedback for attachment operations.
	// ErrAttachmentMetadataNil indicates that attachment metadata was nil.
	ErrAttachmentMetadataNil = errors.New("attachment metadata cannot be nil")
	// ErrAttachmentContentHashEmpty indicates that attachment content hash was empty.
	ErrAttachmentContentHashEmpty = errors.New("attachment content hash cannot be empty")
	// ErrAttachmentContentHashInvalid indicates that attachment content hash format is invalid.
	ErrAttachmentContentHashInvalid = errors.New("attachment content hash must be a valid SHA-256 hex string (64 characters, hex-only)")
	// ErrAttachmentFilenameEmpty indicates that attachment filename was empty.
	ErrAttachmentFilenameEmpty = errors.New("attachment filename cannot be empty")
	// ErrAttachmentFilenameEmptyAfterSanitization indicates that attachment filename was empty after sanitization.
	ErrAttachmentFilenameEmptyAfterSanitization = errors.New("attachment filename cannot be empty after sanitization")
	// ErrAttachmentContentTypeEmpty indicates that attachment content type was empty.
	ErrAttachmentContentTypeEmpty = errors.New("attachment content type cannot be empty")
	// ErrAttachmentContentTypeEmptyAfterSanitization indicates that attachment content type was empty after sanitization.
	ErrAttachmentContentTypeEmptyAfterSanitization = errors.New("attachment content type cannot be empty after sanitization")

	// Real name validation errors provide specific feedback for real name field validation.
	// ErrFirstNameRequired indicates that first name is required but was not provided.
	ErrFirstNameRequired = errors.New("first name is required")
	// ErrLastNameRequired indicates that last name is required but was not provided.
	ErrLastNameRequired = errors.New("last name is required")
	// ErrFirstNameTooShort indicates that the first name is shorter than the minimum allowed length.
	ErrFirstNameTooShort = errors.New("first name is too short")
	// ErrFirstNameTooLong indicates that the first name exceeds the maximum allowed length.
	ErrFirstNameTooLong = errors.New("first name is too long")
	// ErrLastNameTooShort indicates that the last name is shorter than the minimum allowed length.
	ErrLastNameTooShort = errors.New("last name is too short")
	// ErrLastNameTooLong indicates that the last name exceeds the maximum allowed length.
	ErrLastNameTooLong = errors.New("last name is too long")

	// Username requirement validation errors
	// ErrUsernameRequired indicates that username is required but was not provided.
	ErrUsernameRequired = errors.New("username is required")

	// Phone number validation errors provide specific feedback for phone number field validation.
	// ErrPhoneNumberInvalid indicates that the phone number format is invalid.
	ErrPhoneNumberInvalid = errors.New("invalid phone number format")
	// ErrPhoneNumberTooLong indicates that the phone number exceeds maximum length.
	ErrPhoneNumberTooLong = errors.New("phone number is too long")
	// ErrPhoneNumberInvalidRegion indicates that the phone number is not valid for any known region.
	ErrPhoneNumberInvalidRegion = errors.New("phone number is not valid for any known region")

	// Organization validation errors provide specific feedback for organization operations.
	// ErrOrganizationIDRequired indicates that an organization ID is required but was not provided.
	ErrOrganizationIDRequired = errors.New("organization ID is required")
	// ErrInvalidOrganizationID indicates that the organization ID has an invalid format.
	ErrInvalidOrganizationID = errors.New("invalid organization ID format")
	// ErrOrganizationNameRequired indicates that an organization name is required but was not provided.
	ErrOrganizationNameRequired = errors.New("organization name is required")
	// ErrOrganizationNameTooLong indicates that the organization name exceeds the maximum allowed length.
	ErrOrganizationNameTooLong = errors.New("organization name is too long (maximum 100 characters)")
	// ErrOrganizationDescriptionTooLong indicates that the organization description exceeds the maximum allowed length.
	ErrOrganizationDescriptionTooLong = errors.New("organization description is too long (maximum 500 characters)")
	// ErrOrganizationSlugRequired indicates that an organization slug is required but was not provided.
	ErrOrganizationSlugRequired = errors.New("organization slug is required")
	// ErrInvalidOrganizationSlug indicates that the organization slug has an invalid format.
	ErrInvalidOrganizationSlug = errors.New("invalid organization slug format")
	// ErrInvalidOrganizationRole indicates that an invalid or unspecified organization role was provided.
	ErrInvalidOrganizationRole = errors.New("invalid organization role specified")
	// ErrInvalidUserID indicates that the user ID has an invalid format.
	ErrInvalidUserID = errors.New("invalid user ID format")
	// ErrOrganizationEmailDomainRequired indicates that an organization email domain value is required.
	ErrOrganizationEmailDomainRequired = errors.New("organization email domain is required")
	// ErrOrganizationEmailDomainInvalid indicates that the provided email domain is invalid.
	ErrOrganizationEmailDomainInvalid = errors.New("organization email domain is invalid")
	// ErrOrganizationEmailDomainTooLong indicates that the organization email domain exceeds maximum length.
	ErrOrganizationEmailDomainTooLong = errors.New("organization email domain is too long")
	// ErrOrganizationEmailDomainIDRequired indicates that an organization email domain ID is required.
	ErrOrganizationEmailDomainIDRequired = errors.New("organization email domain ID is required")

	// Organization invitation validation errors provide specific feedback for invitation operations.
	// ErrInvitationIDRequired indicates that an invitation ID is required but was not provided.
	ErrInvitationIDRequired = errors.New("invitation ID is required")
	// ErrInvitationIdentifierRequired indicates that an identifier (email/username) is required but was not provided.
	ErrInvitationIdentifierRequired = errors.New("identifier is required")
	// ErrInvalidInvitationIdentifierType indicates that an invalid identifier type was provided.
	ErrInvalidInvitationIdentifierType = errors.New("identifier type must be EMAIL or USERNAME")

	// Chat channel validation errors provide specific feedback for chat channel operations.
	// ErrChannelIDRequired indicates that a channel ID is required but was not provided.
	ErrChannelIDRequired = errors.New("channel ID is required")
	// ErrChannelNameRequired indicates that a channel name is required for group channels.
	ErrChannelNameRequired = errors.New("channel name is required for group channels")
	// ErrChannelNameTooLong indicates that the channel name exceeds the maximum allowed length.
	ErrChannelNameTooLong = errors.New("channel name exceeds maximum length")
	// ErrChannelDescriptionTooLong indicates that the channel description exceeds the maximum allowed length.
	ErrChannelDescriptionTooLong = errors.New("channel description exceeds maximum length")
	// ErrInvalidChannelType indicates that an invalid or unspecified channel type was provided.
	ErrInvalidChannelType = errors.New("invalid channel type")
	// ErrTooManyParticipants indicates that too many participants were specified for a group channel.
	ErrTooManyParticipants = errors.New("too many participants for group channel")
	// ErrNoParticipantsProvided indicates that at least one participant is required but none were provided.
	ErrNoParticipantsProvided = errors.New("at least one participant is required")
	// ErrChannelContextIDRequired indicates that a context ID was expected but missing.
	ErrChannelContextIDRequired = errors.New("channel context id is required")
	// ErrChannelContextDisplayNameRequired indicates that a context display name was expected but missing.
	ErrChannelContextDisplayNameRequired = errors.New("channel context display name is required")
	// ErrChannelContextDisplayNameTooLong indicates that the context display name exceeds the maximum allowed length.
	ErrChannelContextDisplayNameTooLong = errors.New("channel context display name exceeds maximum length")
	// ErrInvalidChannelContextID indicates that a provided context ID is invalid.
	ErrInvalidChannelContextID = errors.New("invalid channel context id")
	// ErrInvalidChannelContextEntityID indicates that a provided context entity ID is invalid.
	ErrInvalidChannelContextEntityID = errors.New("invalid channel context entity id")
	// ErrInvalidChannelPublicRead indicates a public read level outside the defined enum values.
	ErrInvalidChannelPublicRead = errors.New("invalid channel public read level")

	// Chat participant validation errors provide specific feedback for participant operations.
	// ErrParticipantUserIDRequired indicates that a participant user ID is required but was not provided.
	ErrParticipantUserIDRequired = errors.New("participant user ID is required")
	// ErrInvalidChannelRole indicates that an invalid or unspecified channel role was provided.
	ErrInvalidChannelRole = errors.New("invalid channel role")

	// ErrInvalidChannelNotificationMode indicates an unsupported participant interruption preference.
	ErrInvalidChannelNotificationMode = errors.New("invalid channel notification mode")

	// Chat message validation errors provide specific feedback for message operations.
	// ErrChatMessageIDRequired indicates that a message ID is required but was not provided.
	ErrChatMessageIDRequired = errors.New("message ID is required")
	// ErrBroadcastRequiresThread indicates broadcast_to_channel was set on a main-timeline message.
	ErrBroadcastRequiresThread = errors.New("broadcast_to_channel requires parent_message_id")
	// ErrMessageContentRequired indicates that message content is required but was not provided.
	ErrMessageContentRequired = errors.New("message content is required")
	// ErrMessageContentTooLong indicates that the message content exceeds the maximum allowed length.
	ErrMessageContentTooLong = errors.New("message content exceeds maximum length")
	// ErrClientMessageIDTooLong indicates that a send's idempotency key exceeds the maximum length.
	ErrClientMessageIDTooLong = errors.New("client message ID exceeds maximum length")
	// ErrInvalidClientMessageID indicates that a send's idempotency key contains a NUL character.
	ErrInvalidClientMessageID = errors.New("client message ID must not contain NUL characters")
	// ErrTooManyEmbeds indicates that too many embeds were specified for a message.
	ErrTooManyEmbeds = errors.New("too many embeds per message")
	// ErrTooManyBatchMessages indicates that too many message IDs were specified in a batch request.
	ErrTooManyBatchMessages = errors.New("too many message IDs in batch request")
	// ErrTooManyAgentRunStatusLocations indicates that an agent-run status read named too many thread locations.
	ErrTooManyAgentRunStatusLocations = errors.New("too many thread locations in agent run status request")
	// ErrAgentRunStatusHistoryLimitTooLarge indicates that an agent-run status read asked for too much run history.
	ErrAgentRunStatusHistoryLimitTooLarge = errors.New("agent run status history limit is too large")
	// ErrTooManyParticipantBatchChannels indicates that a participant batch contained too many channel ID entries.
	ErrTooManyParticipantBatchChannels = errors.New("too many channel IDs in participant batch request")
	// ErrNoMessageIDsProvided indicates that at least one message ID is required but none were provided.
	ErrNoMessageIDsProvided = errors.New("at least one message ID is required")
	// ErrAgentSessionIDRequired indicates that an agent session ID is required but was not provided.
	ErrAgentSessionIDRequired = errors.New("agent session ID is required")
	// ErrAgentRunIDRequired indicates that an agent run ID is required but was not provided.
	ErrAgentRunIDRequired = errors.New("agent run ID is required")
	// ErrAgentQueuedInputIDRequired indicates that a queued agent input ID is required but was not provided.
	ErrAgentQueuedInputIDRequired = errors.New("queued agent input ID is required")
	// ErrResolutionReasonTooLong indicates the tool-call resolution reason exceeds the maximum length.
	ErrResolutionReasonTooLong = errors.New("resolution reason exceeds maximum length")

	// Chat embed validation errors provide specific feedback for embed operations.
	// ErrEmbedDataRequired indicates that embed data is required but was not provided.
	ErrEmbedDataRequired = errors.New("embed data is required")
	// ErrEmbedDataTooLarge indicates that the embed data exceeds the maximum allowed size.
	ErrEmbedDataTooLarge = errors.New("embed data exceeds maximum size")
	// ErrEmbedTypeInvalid indicates that the embed type is invalid or not allowed.
	ErrEmbedTypeInvalid = errors.New("embed type is invalid or not allowed")
	// ErrFallbackTextTooLong indicates that the fallback text exceeds the maximum allowed length.
	ErrFallbackTextTooLong = errors.New("fallback text exceeds maximum length")

	// Chat reaction validation errors provide specific feedback for reaction operations.
	// ErrEmojiRequired indicates that an emoji is required but was not provided.
	ErrEmojiRequired = errors.New("emoji is required")
	// ErrEmojiTooLong indicates that the emoji exceeds the maximum allowed length.

	ErrEmojiTooLong = errors.New("emoji exceeds maximum length")
)
