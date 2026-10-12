package common_persistence_models

// Table name constants for all database models.
// These constants ensure consistent table naming across the application
// and can be used for raw SQL queries, migrations, and other database operations.
const (
	// TableNameChatAgentMessageReceipts stores delegation send replay outcomes.
	TableNameChatAgentMessageReceipts = "gocommon_chat_agent_message_receipts"
	// TableNameChatAgentGraphBudgets stores the optional budget pool of each delegation graph root.
	TableNameChatAgentGraphBudgets = "gocommon_chat_agent_graph_budgets"

	// TableNameUsers is the table name for the User model
	TableNameUsers = "gocommon_users"

	// TableNameUserSettings is the table name for the UserSettings model
	TableNameUserSettings = "gocommon_user_settings"

	// TableNameAttachments is the table name for the Attachment model
	TableNameAttachments = "gocommon_attachments"

	// TableNameSessionTokens is the table name for the SessionToken model
	TableNameSessionTokens = "gocommon_auth_tokens"

	// TableNameContactMessages is the table name for the ContactMessage model
	TableNameContactMessages = "gocommon_contact_messages"

	// TableNameCareerJobPostings is the table name for careers job postings
	TableNameCareerJobPostings = "gocommon_career_job_postings"

	// TableNameCareerApplications is the table name for careers applications
	TableNameCareerApplications = "gocommon_career_applications"

	// TableNameEmailVerifications is the table name for the EmailVerification model
	TableNameEmailVerifications = "gocommon_email_verifications"

	// TableNameOAuthLogins is the table name for the OAuthLogin model
	TableNameOAuthLogins = "gocommon_oauth_logins"

	// TableNameOrganizations is the table name for the Organization model
	TableNameOrganizations = "gocommon_organizations"

	// TableNameUserOrganizations is the table name for the UserOrganization model
	TableNameUserOrganizations = "gocommon_user_organizations"

	// TableNamePhoneVerifications is the table name for the PhoneVerification model
	TableNamePhoneVerifications = "gocommon_phone_verifications"

	// TableNameUserAuthMethods is the table name for the UserAuthMethod model
	TableNameUserAuthMethods = "gocommon_user_auth_methods"

	// TableNameUserPasswords is the table name for the UserPassword model
	TableNameUserPasswords = "gocommon_user_passwords"

	// TableNameOrganizationEmailDomains is the table name for the OrganizationEmailDomain model
	TableNameOrganizationEmailDomains = "gocommon_organization_email_domains"

	// TableNameOrganizationInvitations is the table name for the OrganizationInvitation model
	TableNameOrganizationInvitations = "gocommon_organization_invitations"

	// TableNameChatChannels is the table name for chat channels
	TableNameChatChannels = "gocommon_chat_channels"

	// TableNameChatChannelParticipants is the table name for chat channel participants
	TableNameChatChannelParticipants = "gocommon_chat_channel_participants"

	// TableNameChatMessages is the table name for chat messages
	TableNameChatMessages = "gocommon_chat_messages"

	// TableNameChatMessageEmbeds is the table name for chat message embeds
	TableNameChatMessageEmbeds = "gocommon_chat_message_embeds"

	// TableNameChatMessageReactions is the table name for chat message reactions
	TableNameChatMessageReactions = "gocommon_chat_message_reactions"

	// TableNameChatMessageMentions is the table name for chat message mentions
	TableNameChatMessageMentions = "gocommon_chat_message_mentions"

	// TableNameChatMessageEditHistory is the table name for chat message edit history
	TableNameChatMessageEditHistory = "gocommon_chat_message_edit_history"

	// TableNameChatMessageToolCalls is the table name for the 1:1 agent/tool-call side table
	TableNameChatMessageToolCalls = "gocommon_chat_message_tool_calls"

	// TableNameUsernameChangeHistory is the table name for username change audit entries
	TableNameUsernameChangeHistory = "gocommon_username_change_history"

	// TableNameInferenceModels is the table name for supported GenAI models
	TableNameInferenceModels = "gocommon_inference_models"

	// TableNameChatAgentSessions is the table name for chat agent sessions (one per agent-enabled channel)
	TableNameChatAgentSessions = "gocommon_chat_agent_sessions"

	// TableNameChatAgentSessionConfigSnapshots is the table name for append-only agent session config snapshots
	TableNameChatAgentSessionConfigSnapshots = "gocommon_chat_agent_session_config_snapshots"

	// TableNameChatAgentSessionToolConfigs is the table name for normalized per-version agent session tool configs
	TableNameChatAgentSessionToolConfigs = "gocommon_chat_agent_session_tool_configs"

	// TableNameChatAgentRuns is the table name for chat agent runs (one logical agent turn/resume chain)
	TableNameChatAgentRuns = "gocommon_chat_agent_runs"

	// TableNameChatAgentQueuedInputs is the table name for human input queued for an agent session until it is delivered
	TableNameChatAgentQueuedInputs = "gocommon_chat_agent_queued_inputs"

	// TableNameChatInferenceCalls is the table name for the append-only inference-call ledger
	TableNameChatInferenceCalls = "gocommon_chat_inference_calls"

	// TableNameChatInferenceTotals is the table name for the per-channel inference totals cache
	TableNameChatInferenceTotals = "gocommon_chat_inference_totals"

	// TableNameInferenceOperationRoutes is the table name for per-operation control-plane routes
	TableNameInferenceOperationRoutes = "gocommon_inference_operation_routes"

	// TableNameInferenceModelRateLimitPolicies is the table name for model-level rate-limit policies
	TableNameInferenceModelRateLimitPolicies = "gocommon_inference_model_rate_limit_policies"

	// TableNameInferenceAttempts is the table name for the global inference-attempt ledger
	TableNameInferenceAttempts = "gocommon_inference_attempts"
)
