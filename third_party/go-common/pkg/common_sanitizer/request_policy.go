package common_sanitizer

import (
	"fmt"
	"slices"

	"github.com/Suhaibinator/go-common/pkg/config"
)

// RequestPolicy is one immutable, operation-scoped sanitizer policy. It owns
// the derived email-validation state so regexes and domain lookup tables are
// compiled once when a configuration snapshot is activated, not per request.
//
// Handler memoizes one policy per configuration generation, built with
// NewRequestPolicyFromView.
type RequestPolicy struct {
	logInputMaxLength    int
	oauthMaxStateLength  int
	oauthMaxCodeLength   int
	emailValidator       *EmailValidator
	emailMaxDomainLength int
	password             PasswordConfig
	verification         VerificationConfig
	contact              ContactConfig
	username             UsernameConfig
	realName             RealNameConfig
	signup               SignupConfig
	phoneNumber          PhoneNumberConfig
	pagination           PaginationConfig
	organization         OrganizationConfig
	chat                 ChatConfig
	notification         notificationRequestPolicy
	userSearch           UserSearchConfig
	inference            inferenceRoutingRequestPolicy
}

// notificationRequestPolicy holds the notification request bounds as validated
// by pkg/config: the default page size never exceeds the maximum.
type notificationRequestPolicy struct {
	listDefaultLimit     int
	listMaxLimit         int
	maxReadReceipts      int
	maxPreferenceUpdates int
}

type inferenceRoutingRequestPolicy struct {
	retargetBatchSize int
}

// NewRequestPolicyFromView builds one immutable request policy from one
// coherent configuration generation.
func NewRequestPolicyFromView(view config.SanitizationView) (*RequestPolicy, error) {
	if view == nil {
		return nil, fmt.Errorf("sanitization view is required")
	}

	email := EmailValidationConfig{
		MaxEmailLength:        view.SanitizerMaxEmailLength(),
		MaxLocalPartLength:    view.SanitizerMaxLocalPartLength(),
		MaxDomainLength:       view.SanitizerMaxDomainLength(),
		AllowDisposableEmails: view.SanitizerAllowDisposableEmails(),
		BlockSQLPatterns:      view.SanitizerBlockSQLPatterns(),
		RequireDotInDomain:    view.SanitizerRequireDotInDomain(),
		BlockedDomains:        slices.Clone(view.SanitizerBlockedDomains()),
		AllowedDomains:        slices.Clone(view.SanitizerAllowedDomains()),
		DisposableDomains:     slices.Clone(view.SanitizerDisposableDomains()),
		UseStrictRFC5322:      view.SanitizerUseStrictRFC5322(),
		CustomRegexPattern:    view.SanitizerCustomRegexPattern(),
	}
	emailValidator, err := newEmailValidator(&email)
	if err != nil {
		return nil, err
	}
	return &RequestPolicy{
		inference: inferenceRoutingRequestPolicy{
			retargetBatchSize: view.InferenceRetargetBatchSize(),
		},
		emailValidator:       emailValidator,
		emailMaxDomainLength: email.MaxDomainLength,
		logInputMaxLength:    view.SanitizerLogInputMaxLength(),
		oauthMaxStateLength:  view.OAuthMaxStateLength(),
		oauthMaxCodeLength:   view.OAuthMaxCodeLength(),
		password: PasswordConfig{
			MinLength:          view.SanitizerPasswordMinLength(),
			MaxLength:          view.SanitizerPasswordMaxLength(),
			RequireUppercase:   view.SanitizerPasswordRequireUppercase(),
			RequireLowercase:   view.SanitizerPasswordRequireLowercase(),
			RequireNumber:      view.SanitizerPasswordRequireNumber(),
			RequireSpecialChar: view.SanitizerPasswordRequireSpecial(),
			SpecialChars:       view.SanitizerPasswordSpecialChars(),
		},
		verification: VerificationConfig{
			MinCodeLength: view.SanitizerVerificationMinLength(),
			MaxCodeLength: view.SanitizerVerificationMaxLength(),
		},
		contact: ContactConfig{
			MinNameLength:    view.SanitizerContactMinNameLength(),
			MaxNameLength:    view.SanitizerContactMaxNameLength(),
			MinSubjectLength: view.SanitizerContactMinSubjectLength(),
			MaxSubjectLength: view.SanitizerContactMaxSubjectLength(),
			MinMessageLength: view.SanitizerContactMinMessageLength(),
			MaxMessageLength: view.SanitizerContactMaxMessageLength(),
		},
		username: UsernameConfig{
			MinLength: view.SanitizerUsernameMinLength(),
			MaxLength: view.SanitizerUsernameMaxLength(),
		},
		realName: RealNameConfig{
			MinLength: view.SanitizerRealNameMinLength(),
			MaxLength: view.SanitizerRealNameMaxLength(),
		},
		signup: SignupConfig{
			RequireRealNames: view.RequireRealNamesOnSignup(),
			RequireUsernames: view.RequireUsernamesOnSignup(),
		},
		phoneNumber: PhoneNumberConfig{
			DefaultRegion: view.PhoneNumberDefaultRegion(),
		},
		pagination: PaginationConfig{
			DefaultLimit:                 view.DefaultPaginationLimit(),
			MaxLimit:                     view.MaxPaginationLimit(),
			InferenceAttemptDefaultLimit: view.InferenceAttemptListDefaultLimit(),
			InferenceAttemptMaxLimit:     view.InferenceAttemptListMaxLimit(),
		},
		organization: OrganizationConfig{
			SlugMinLength:         view.OrganizationSlugMinLength(),
			SlugMaxLength:         view.OrganizationSlugMaxLength(),
			HierarchyMaxDepth:     view.OrgHierarchyMaxDepth(),
			ParentOptionsMaxLimit: view.OrganizationParentOptionsMaxLimit(),
		},
		chat: ChatConfig{
			MaxChannelNameLength:               view.ChatMaxChannelNameLength(),
			MaxChannelDescriptionLength:        view.ChatMaxChannelDescriptionLength(),
			MaxMessageLength:                   view.ChatMaxMessageLength(),
			MaxClientMessageIDLength:           view.ChatMaxClientMessageIDLength(),
			MaxEmbedsPerMessage:                view.ChatMaxEmbedsPerMessage(),
			MaxEmbedDataBytes:                  view.ChatMaxEmbedDataBytes(),
			AllowNativeEmbeds:                  view.ChatAllowNativeEmbeds(),
			AllowCustomEmbeds:                  view.ChatAllowCustomEmbeds(),
			MaxParticipantsPerGroup:            view.ChatMaxParticipantsPerGroup(),
			MaxBatchGetMessages:                view.ChatMaxBatchGetMessages(),
			MaxListParticipantsBatchChannelIDs: view.ChatMaxListParticipantsBatchChannelIDs(),
			MaxEmojiLength:                     view.ChatMaxEmojiLength(),
			MaxFallbackTextLength:              view.ChatMaxFallbackTextLength(),
			MaxAgentRunStatusLocations:         view.ChatAgentRunStatusMaxLocations(),
			MaxAgentRunStatusHistory:           view.ChatAgentRunStatusMaxHistory(),
		},
		notification: notificationRequestPolicy{
			listDefaultLimit:     view.NotificationListDefaultLimit(),
			listMaxLimit:         view.NotificationListMaxLimit(),
			maxReadReceipts:      view.NotificationMaxReadReceipts(),
			maxPreferenceUpdates: view.NotificationMaxPreferenceUpdates(),
		},
		userSearch: UserSearchConfig{
			MaxQueryLength: view.UserSearchMaxQueryLength(),
		},
	}, nil
}

// ValidateEmail validates and normalizes an email with this immutable policy.
func (p *RequestPolicy) ValidateEmail(email string) (string, error) {
	if p == nil || p.emailValidator == nil {
		return "", ErrInvalidEmailFormat
	}
	return p.emailValidator.ValidateEmail(email)
}
