package common_sanitizer

// PasswordConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - PasswordConfig is read or populated by backend components in this package to move requests through this stage.
type PasswordConfig struct {
	MinLength          int    `json:"min_length" yaml:"min_length" toml:"min_length"`
	MaxLength          int    `json:"max_length" yaml:"max_length" toml:"max_length"`
	RequireUppercase   bool   `json:"require_uppercase" yaml:"require_uppercase" toml:"require_uppercase"`
	RequireLowercase   bool   `json:"require_lowercase" yaml:"require_lowercase" toml:"require_lowercase"`
	RequireNumber      bool   `json:"require_number" yaml:"require_number" toml:"require_number"`
	RequireSpecialChar bool   `json:"require_special_char" yaml:"require_special_char" toml:"require_special_char"`
	SpecialChars       string `json:"special_chars" yaml:"special_chars" toml:"special_chars"`
}

// VerificationConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - VerificationConfig is read or populated by backend components in this package to move requests through this stage.
type VerificationConfig struct {
	MinCodeLength int `json:"min_code_length" yaml:"min_code_length" toml:"min_code_length"`
	MaxCodeLength int `json:"max_code_length" yaml:"max_code_length" toml:"max_code_length"`
}

// ContactConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - ContactConfig is read or populated by backend components in this package to move requests through this stage.
type ContactConfig struct {
	MinNameLength    int `json:"min_name_length" yaml:"min_name_length" toml:"min_name_length"`
	MaxNameLength    int `json:"max_name_length" yaml:"max_name_length" toml:"max_name_length"`
	MinSubjectLength int `json:"min_subject_length" yaml:"min_subject_length" toml:"min_subject_length"`
	MaxSubjectLength int `json:"max_subject_length" yaml:"max_subject_length" toml:"max_subject_length"`
	MinMessageLength int `json:"min_message_length" yaml:"min_message_length" toml:"min_message_length"`
	MaxMessageLength int `json:"max_message_length" yaml:"max_message_length" toml:"max_message_length"`
}

// UsernameConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - UsernameConfig is read or populated by backend components in this package to move requests through this stage.
type UsernameConfig struct {
	MinLength int `json:"min_length" yaml:"min_length" toml:"min_length"`
	MaxLength int `json:"max_length" yaml:"max_length" toml:"max_length"`
}

// RealNameConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - RealNameConfig is read or populated by backend components in this package to move requests through this stage.
type RealNameConfig struct {
	MinLength int `json:"min_length" yaml:"min_length" toml:"min_length"`
	MaxLength int `json:"max_length" yaml:"max_length" toml:"max_length"`
}

// SignupConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - SignupConfig is read or populated by backend components in this package to move requests through this stage.
type SignupConfig struct {
	RequireRealNames bool `json:"require_real_names" yaml:"require_real_names" toml:"require_real_names"`
	RequireUsernames bool `json:"require_usernames" yaml:"require_usernames" toml:"require_usernames"`
}

// PhoneNumberConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - PhoneNumberConfig is read or populated by backend components in this package to move requests through this stage.
type PhoneNumberConfig struct {
	DefaultRegion string `json:"default_region" yaml:"default_region" toml:"default_region"`
}

// EmailValidationConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - EmailValidationConfig is read or populated by backend components in this package to move requests through this stage.
type EmailValidationConfig struct {
	// Length limits
	MaxEmailLength     int `json:"max_email_length" yaml:"max_email_length" toml:"max_email_length"`
	MaxLocalPartLength int `json:"max_local_part_length" yaml:"max_local_part_length" toml:"max_local_part_length"`
	MaxDomainLength    int `json:"max_domain_length" yaml:"max_domain_length" toml:"max_domain_length"`

	// Feature toggles
	AllowDisposableEmails bool `json:"allow_disposable_emails" yaml:"allow_disposable_emails" toml:"allow_disposable_emails"`
	BlockSQLPatterns      bool `json:"block_sql_patterns" yaml:"block_sql_patterns" toml:"block_sql_patterns"`
	RequireDotInDomain    bool `json:"require_dot_in_domain" yaml:"require_dot_in_domain" toml:"require_dot_in_domain"`

	// Domain restrictions
	BlockedDomains    []string `json:"blocked_domains" yaml:"blocked_domains" toml:"blocked_domains"`
	AllowedDomains    []string `json:"allowed_domains" yaml:"allowed_domains" toml:"allowed_domains"`
	DisposableDomains []string `json:"disposable_domains" yaml:"disposable_domains" toml:"disposable_domains"`

	// Advanced options
	UseStrictRFC5322   bool   `json:"use_strict_rfc5322" yaml:"use_strict_rfc5322" toml:"use_strict_rfc5322"`
	CustomRegexPattern string `json:"custom_regex_pattern" yaml:"custom_regex_pattern" toml:"custom_regex_pattern"`
}

// PaginationConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - PaginationConfig is read or populated by backend components in this package to move requests through this stage.
type PaginationConfig struct {
	DefaultLimit int `json:"default_limit" yaml:"default_limit" toml:"default_limit"`
	MaxLimit     int `json:"max_limit" yaml:"max_limit" toml:"max_limit"`
	// InferenceAttemptDefaultLimit and InferenceAttemptMaxLimit size admin
	// inference-attempt pages (inference_attempt_list_default_limit / _max_limit).
	InferenceAttemptDefaultLimit int `json:"inference_attempt_default_limit" yaml:"inference_attempt_default_limit" toml:"inference_attempt_default_limit"`
	InferenceAttemptMaxLimit     int `json:"inference_attempt_max_limit" yaml:"inference_attempt_max_limit" toml:"inference_attempt_max_limit"`
}

// OrganizationConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - OrganizationConfig is read or populated by backend components in this package to move requests through this stage.
type OrganizationConfig struct {
	SlugMinLength     int `json:"slug_min_length" yaml:"slug_min_length" toml:"slug_min_length"`
	SlugMaxLength     int `json:"slug_max_length" yaml:"slug_max_length" toml:"slug_max_length"`
	HierarchyMaxDepth int `json:"hierarchy_max_depth" yaml:"hierarchy_max_depth" toml:"hierarchy_max_depth"`
	// ParentOptionsMaxLimit caps one admin parent-option page (organization_parent_options_max_limit).
	ParentOptionsMaxLimit int `json:"parent_options_max_limit" yaml:"parent_options_max_limit" toml:"parent_options_max_limit"`
}

// ChatConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - ChatConfig is read or populated by backend components in this package to move requests through this stage.
type ChatConfig struct {
	MaxChannelNameLength               int  `json:"max_channel_name_length" yaml:"max_channel_name_length" toml:"max_channel_name_length"`
	MaxChannelDescriptionLength        int  `json:"max_channel_description_length" yaml:"max_channel_description_length" toml:"max_channel_description_length"`
	MaxMessageLength                   int  `json:"max_message_length" yaml:"max_message_length" toml:"max_message_length"`
	MaxClientMessageIDLength           int  `json:"max_client_message_id_length" yaml:"max_client_message_id_length" toml:"max_client_message_id_length"`
	MaxEmbedsPerMessage                int  `json:"max_embeds_per_message" yaml:"max_embeds_per_message" toml:"max_embeds_per_message"`
	MaxEmbedDataBytes                  int  `json:"max_embed_data_bytes" yaml:"max_embed_data_bytes" toml:"max_embed_data_bytes"`
	AllowNativeEmbeds                  bool `json:"allow_native_embeds" yaml:"allow_native_embeds" toml:"allow_native_embeds"`
	AllowCustomEmbeds                  bool `json:"allow_custom_embeds" yaml:"allow_custom_embeds" toml:"allow_custom_embeds"`
	MaxParticipantsPerGroup            int  `json:"max_participants_per_group" yaml:"max_participants_per_group" toml:"max_participants_per_group"`
	MaxBatchGetMessages                int  `json:"max_batch_get_messages" yaml:"max_batch_get_messages" toml:"max_batch_get_messages"`
	MaxListParticipantsBatchChannelIDs int  `json:"max_list_participants_batch_channel_ids" yaml:"max_list_participants_batch_channel_ids" toml:"max_list_participants_batch_channel_ids"`
	MaxEmojiLength                     int  `json:"max_emoji_length" yaml:"max_emoji_length" toml:"max_emoji_length"`
	MaxFallbackTextLength              int  `json:"max_fallback_text_length" yaml:"max_fallback_text_length" toml:"max_fallback_text_length"`
	// MaxAgentRunStatusLocations and MaxAgentRunStatusHistory are the same keys the chat service
	// enforces (chat_agent_run_status_max_locations / _max_history), validated by pkg/config.
	MaxAgentRunStatusLocations int `json:"max_agent_run_status_locations" yaml:"max_agent_run_status_locations" toml:"max_agent_run_status_locations"`
	MaxAgentRunStatusHistory   int `json:"max_agent_run_status_history" yaml:"max_agent_run_status_history" toml:"max_agent_run_status_history"`
}

// UserSearchConfig defines structured state used in the request sanitization and validation flow before service handlers run.
//
// Used in flow:
// - UserSearchConfig is read or populated by backend components in this package to move requests through this stage.
type UserSearchConfig struct {
	MaxQueryLength int `json:"max_query_length" yaml:"max_query_length" toml:"max_query_length"`
}
