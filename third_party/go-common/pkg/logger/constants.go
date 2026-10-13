package logger

// Log key constants for structured logging.
// All log statements should use these constants for consistency.
// Key values use snake_case convention.

// Entity ID keys - used to identify specific records
const (
	KeyUserID               = "user_id"
	KeyAttachmentID         = "attachment_id"
	KeyOrganizationID       = "organization_id"
	KeyMessageID            = "message_id"
	KeyChannelID            = "channel_id"
	KeyTargetUserID         = "target_user_id"
	KeyActorUserID          = "actor_user_id"
	KeyAdminUserID          = "admin_id"
	KeyInvitationID         = "invitation_id"
	KeyBotUserID            = "bot_user_id"
	KeyAgentRunID           = "agent_run_id"
	KeyWorkerID             = "worker_id"
	KeyTokenID              = "token_id"
	KeySenderID             = "sender_id"
	KeyContactID            = "contact_id"
	KeySessionID            = "session_id"
	KeyVerificationID       = "verification_id"
	KeyReferenceID          = "reference_id"
	KeyCreatorID            = "creator_id"
	KeyProviderUserID       = "provider_user_id"
	KeyAgentSessionID       = "agent_session_id"
	KeyInferenceModelID     = "inference_model_id"
	KeyProviderModelID      = "provider_model_id"
	KeyInvocationID         = "invocation_id"
	KeyAttemptID            = "attempt_id"
	KeyInferenceCallID      = "inference_call_id"
	KeyToolCallID           = "tool_call_id"
	KeyRequestID            = "request_id"
	KeyCallID               = "call_id"
	KeyInitialRunID         = "initial_run_id"
	KeyInitialMessageID     = "initial_message_id"
	KeyParentOrganizationID = "parent_organization_id"
	KeyQueuedInputID        = "queued_input_id"
)

// Notification diagnostics use stable fields without putting IDs in metric labels.
const (
	KeyNotificationID           = "notification_id"
	KeyNotificationType         = "notification_type"
	KeyNotificationRevision     = "notification_revision"
	KeyDeliveryJobID            = "delivery_job_id"
	KeyNotificationEmissionID   = "notification_emission_id"
	KeyDeliveryState            = "delivery_state"
	KeyFailureKind              = "failure_kind"
	KeyNotificationConnectionID = "notification_connection_id"
	KeyNotificationMode         = "notification_mode"
)

// User and contact field keys
const (
	KeyEmail              = "email"
	KeyUsername           = "username"
	KeyPhoneNumber        = "phone_number"
	KeyUserType           = "user_type"
	KeyNewUserType        = "new_user_type"
	KeyOldUserType        = "old_user_type"
	KeyRequestedUserType  = "requested_user_type"
	KeyAdminUserType      = "admin_user_type"
	KeyTargetUserType     = "target_user_type"
	KeyActorUserType      = "actor_user_type"
	KeyRequiredType       = "required_type"
	KeyMinRequiredType    = "min_required_type"
	KeyOAuthProviders     = "oauth_providers"
	KeyOAuthProviderCount = "oauth_provider_count"
	KeyOAuthIdentityID    = "oauth_identity_id"
	KeyTotalAuthMethods   = "total_auth_methods"
)

// Storage and attachment keys
const (
	KeyObjectName            = "object_name"
	KeyObjectType            = "object_type"
	KeyEntityID              = "entity_id"
	KeyEntityType            = "entity_type"
	KeyImpliedEntityType     = "implied_entity_type"
	KeyFilename              = "filename"
	KeyOriginalFilename      = "original_filename"
	KeyContentType           = "content_type"
	KeyDeclaredContentType   = "declared_content_type"
	KeyVerifiedPath          = "verified_path"
	KeySize                  = "size"
	KeyMinSize               = "min_size"
	KeyMaxSize               = "max_size"
	KeySourceObjectName      = "source_object_name"
	KeyDestinationObjectName = "destination_object_name"
	KeyTempObjectName        = "temp_object_name"
	KeyETag                  = "etag"
	KeyFinalObjectName       = "final_object_name"
	KeyOriginalURL           = "original_url"
	KeyPublicBaseURL         = "public_base_url"
	KeyAttachmentCount       = "attachment_count"
	KeyAttachmentUserID      = "attachment_user_id"
	KeyAttachmentOwnerID     = "attachment_owner_id"
	KeyUnsaltedHash          = "unsalted_hash"
	KeySaltedHash            = "salted_hash"
	KeyTotalBytes            = "total_bytes"
)

// Token and auth keys
const (
	KeyToken      = "token"
	KeyTokenType  = "token_type"
	KeyAuthMethod = "auth_method"
	KeyProvider   = "provider"
)

// Organization keys
const (
	KeyRole              = "role"
	KeyNewRole           = "new_role"
	KeyMinRole           = "min_role"
	KeyActorRole         = "actor_role"
	KeyUserRole          = "user_role"
	KeyRequiredRole      = "required_role"
	KeyOrgDepth          = "org_depth"
	KeyGrantedBy         = "granted_by"
	KeyCurrentRole       = "current_role"
	KeyCurrentTargetRole = "current_target_role"
	KeyCreatorRole       = "creator_role"
	KeyActorMemberRole   = "actor_member_role"
	KeyName              = "name"
	KeySlug              = "slug"
	KeyDomain            = "domain"
	KeyEmailDomainID     = "email_domain_id"
)

// Message and chat keys
const (
	KeyMessageType      = "message_type"
	KeyMessagePreview   = "message_preview"
	KeyEventType        = "event_type"
	KeyParentMessageID  = "parent_message_id"
	KeyReplyToMessageID = "reply_to_message_id"
	KeyMessageCount     = "message_count"
	KeyMentionCount     = "mention_count"
	KeyParticipantCount = "participant_count"
	KeyChannelCount     = "channel_count"
	KeyEmbedCount       = "embed_count"
	KeyEmoji            = "emoji"
	KeyToolName         = "tool_name"
	KeyToolCallDelta    = "tool_call_delta"
)

// Pagination and query keys
const (
	KeyPage            = "page"
	KeyOffset          = "offset"
	KeyCount           = "count"
	KeyTotal           = "total"
	KeyTotalFiltered   = "total_filtered"
	KeySearch          = "search"
	KeyProcessed       = "processed"
	KeyProcessedCount  = "processed_count"
	KeyPrunedCount     = "pruned_count"
	KeyFoundCount      = "found_count"
	KeyCleanedCount    = "cleaned_count"
	KeyDroppedCount    = "dropped_count"
	KeyCandidateCount  = "candidate_count"
	KeyDeletedCount    = "deleted_count"
	KeyTotalFound      = "total_found"
	KeyResultsCount    = "results_count"
	KeyEntityGroups    = "entity_groups"
	KeyValidatorCount  = "validator_count"
	KeyGroupBy         = "group_by"
	KeyEmailFilter     = "email_filter"
	KeyTokenTypeFilter = "token_type_filter"
)

// Rate limiting keys
const (
	KeyAction         = "action"
	KeyIdentifier     = "identifier"
	KeyIdentifierType = "identifier_type"
	KeyMaxAttempts    = "max_attempts"
	KeyRetryAfterAt   = "retry_after_at"
)

// Cache keys
const (
	KeyHits      = "hits"
	KeyMisses    = "misses"
	KeyEvictions = "evictions"
	KeyCapacity  = "capacity"
)

// General keys
const (
	KeyStatusFilter              = "status_filter"
	KeyNewStatus                 = "new_status"
	KeyOutcome                   = "outcome"
	KeyConsumer                  = "consumer"
	KeyProviderBacked            = "provider_backed"
	KeyConfigurationRole         = "configuration_role"
	KeyMethodType                = "method_type"
	KeyQueueCapacity             = "queue_capacity"
	KeySubject                   = "subject"
	KeyTo                        = "to"
	KeyValidationDetails         = "validation_details"
	KeyValidationFailure         = "validation_failure"
	KeyValidator                 = "validator"
	KeyIsMember                  = "is_member"
	KeyAutoLogin                 = "auto_login"
	KeyOTPUsed                   = "otp_used"
	KeyOTPProvided               = "otp_provided"
	KeyValid                     = "valid"
	KeyErrorMessage              = "error_message"
	KeyAdminNotes                = "admin_notes"
	KeyHasPermission             = "has_permission"
	KeyHasPassword               = "has_password"
	KeyHasAuthMethod             = "has_auth_method"
	KeyUserAdded                 = "user_added"
	KeyUserExists                = "user_exists"
	KeyInvitationPending         = "invitation_pending"
	KeyDeleteAttachments         = "delete_attachments"
	KeyDeleteFiles               = "delete_files"
	KeyAllSuccess                = "all_success"
	KeyReturnURL                 = "return_url"
	KeyFrontendOrigin            = "frontend_origin"
	KeyNewUsername               = "new_username"
	KeyOldUsername               = "old_username"
	KeyBanned                    = "banned"
	KeyBannedStatus              = "banned_status"
	KeyHandlerErrorSet           = "handler_error_set"
	KeyHandlerError              = "handler_error"
	KeyHookIndex                 = "hook_index"
	KeyHookCount                 = "hook_count"
	KeyResponseBufferLimitBytes  = "response_buffer_limit_bytes"
	KeyResponseBytesAttempted    = "response_bytes_attempted"
	KeyRollbackError             = "rollback_error"
	KeyCleanupStage              = "cleanup_stage"
	KeyStep                      = "step"
	KeyVerificationTokenValidity = "verification_token_validity"
	KeyUserIDFilter              = "user_id_filter"
	KeyHasMetadataSearch         = "has_metadata_search"
	KeyHasRawTokenFilter         = "has_raw_token_filter"
	KeyAgentStepIndex            = "agent_step_index"
	KeyAgentType                 = "agent_type"
	KeyAttemptNumber             = "attempt_number"
	KeyAttachmentIndex           = "attachment_index"
	KeyAvailable                 = "available"
	KeyApprove                   = "approve"
	KeyCause                     = "cause"
	KeyCode                      = "code"
	KeyCodePrefix                = "code_prefix"
	KeyConfigVersion             = "config_version"
	KeyCursor                    = "cursor"
	KeyDefaultRegion             = "default_region"
	KeyElapsedFraction           = "elapsed_fraction"
	KeyExpectedConfigVersion     = "expected_config_version"
	KeyGenerationDelta           = "generation_delta"
	KeyGenerationMode            = "generation_mode"
	KeyIncludeArchived           = "include_archived"
	KeyIncludeDeleted            = "include_deleted"
	KeyLane                      = "lane"
	KeyLanguage                  = "language"
	KeyLeaseExpiresAt            = "lease_expires_at"
	KeyMetadataFilterCount       = "metadata_filter_count"
	KeyNewConfigVersion          = "new_config_version"
	KeyNewOrganizationType       = "new_organization_type"
	KeyOrigin                    = "origin"
	KeyOutstandingLimit          = "outstanding_limit"
	KeyPhase                     = "phase"
	KeyPhone                     = "phone"
	KeyRelease                   = "release"
	KeyResendID                  = "resend_id"
	KeyRetryable                 = "retryable"
	KeySavepoint                 = "savepoint"
	KeySeed                      = "seed"
	KeySetup                     = "setup"
	KeySourceStatusCount         = "source_status_count"
	KeyStacktrace                = "stacktrace"
	KeySuccess                   = "success"
	KeyTargetStatus              = "target_status"
	KeyUploadTime                = "uploadTime"
	KeyVersion                   = "version"
	KeyVisibilityMode            = "visibility_mode"
	KeyPanicType                 = "panic_type"
	KeyProcessingStartTime       = "processingStartTime"
	KeyAddress                   = "address"
	KeyAddr                      = "addr"
	KeySink                      = "sink"
	KeySetting                   = "setting"
	KeyAttemptCount              = "attempt_count"
)

// GORM and database keys
const (
	KeySQL         = "sql"
	KeyRows        = "rows"
	KeyTable       = "table"
	KeyGORMMessage = "gorm_message"
)

// Faro and metrics keys
const (
	// KeyFaroTimestamp is the key for a Faro/Grafana frontend event's own timestamp.
	// It is deliberately distinct from the zap entry TimeKey ("timestamp", set in
	// logger.go) so a payload's event time never collides with the backend log-line
	// time. Always log Faro timestamps under this key — never the bare "timestamp".
	KeyFaroTimestamp = "faro_timestamp"
	KeyUser          = "user"
	KeyEnv           = "env"
	KeyApp           = "app"
	KeyAppName       = "app_name"
	KeyAppEnv        = "app_env"
	KeyBrowserName   = "browser_name"
	KeyPagePath      = "page_path"
	KeyPageURL       = "page_url"
	KeyReferer       = "referer"
	KeyLevel         = "level"
	KeyMessage       = "message"
	KeyAttributes    = "attributes"
	KeyValueType     = "value_type"

	// Faro payload collection counts (summary log fields)
	KeyMeasurementsCount = "measurements_count"
	KeyEventsCount       = "events_count"
	KeyLogsCount         = "logs_count"
	KeyExceptionsCount   = "exceptions_count"
	KeyValuesCount       = "values_count"
	KeyTraceSize         = "trace_size"

	// Faro payload collections (debug detail fields)
	KeyMeasurements        = "measurements"
	KeyEvents              = "events"
	KeyLogs                = "logs"
	KeyExceptions          = "exceptions"
	KeyDerivedMeasurements = "derived_measurements"
)

// WebSocket keys
const (
	KeyCommandType = "command_type"
	KeyTopic       = "topic"
)

// Connection and database keys
const (
	KeyOpenConnections = "open_connections"
	KeyIdleConnections = "idle_connections"
	KeyCurrentLocks    = "current_locks"
	KeyPreviousCount   = "previous_count"
	KeySessionsRevoked = "sessions_revoked"
	KeyTotalMethods    = "total_methods"
)

// Careers keys
const (
	KeyJobPostingID    = "job_posting_id"
	KeyApplicationID   = "application_id"
	KeyApplicantUserID = "applicant_user_id"
)

// Attachment lifecycle keys
const (
	KeyAttachmentIDs = "attachment_ids"
	KeyCutoffTime    = "cutoff_time"
)

// Token and expiry keys
const (
	KeyExpiresAt  = "expires_at"
	KeyIssuedAt   = "issued_at"
	KeyUsedAt     = "used_at"
	KeyRevokedAt  = "revoked_at"
	KeyTimeWindow = "time_window"
	KeySignature  = "signature"
)

// Client identity keys. RequestIP/RequestUserAgent describe the client making
// the current request; TokenIP/TokenUserAgent describe the client captured on a
// stored token row at issuance time.
const (
	KeyTokenIP        = "token_ip"
	KeyTokenUserAgent = "token_user_agent"
)

// Configuration generation keys. Every log line stamped with these can be
// correlated with the exact KMS release in effect when it was written: look the
// activation revision up in the release history (`parameter-store release
// show`) or in the SDK's `kms config group` dump, which shares it.
const (
	// KeyConfigRelease is the release identity in "namespace/name" form.
	KeyConfigRelease = "config_release"
	// KeyConfigReleaseVersion is the immutable release version.
	KeyConfigReleaseVersion = "config_release_version"
	// KeyConfigActivationRevision is the store's activation counter; it
	// distinguishes re-activations of one release version.
	KeyConfigActivationRevision = "config_activation_revision"
)

// Additional general keys
const (
	KeyType      = "type"
	KeyValue     = "value"
	KeyFilter    = "filter"
	KeyFrom      = "from"
	KeyEndpoint  = "endpoint"
	KeyLayer     = "layer"
	KeyComponent = "component"
)

// Domain-specific structured logging keys.
const (
	KeyActorUserIDFilter               = "actorUserIDFilter"
	KeyActualOrganizationID            = "actual_organization_id"
	KeyAdminAPIEnabled                 = "admin_api_enabled"
	KeyAfterID                         = "after_id"
	KeyAfterMessageID                  = "after_message_id"
	KeyAgentRunCount                   = "agent_run_count"
	KeyApplicationSchemaSetupCount     = "application_schema_setup_count"
	KeyAttachmentsEnabled              = "attachments_enabled"
	KeyAttempt                         = "attempt"
	KeyAttemptCap                      = "attempt_cap"
	KeyAutoRunPolicy                   = "auto_run_policy"
	KeyBacklogPresent                  = "backlog_present"
	KeyBatchSize                       = "batch_size"
	KeyBodyLength                      = "body_length"
	KeyBodyPreview                     = "body_preview"
	KeyCallback                        = "callback"
	KeyCareersEnabled                  = "careers_enabled"
	KeyCategory                        = "category"
	KeyChanged                         = "changed"
	KeyChannelType                     = "channel_type"
	KeyChatEnabled                     = "chat_enabled"
	KeyConnections                     = "connections"
	KeyContactsEnabled                 = "contacts_enabled"
	KeyCreatedByUserID                 = "created_by_user_id"
	KeyCustomResponsePresent           = "custom_response_present"
	KeyDatabaseHealthCheckerEnabled    = "database_health_checker_enabled"
	KeyDatabaseHost                    = "database_host"
	KeyDatabaseMetricsCollectorEnabled = "database_metrics_collector_enabled"
	KeyDatabaseName                    = "database_name"
	KeyDatabasePort                    = "database_port"
	KeyDedicatedCredentials            = "dedicated_credentials"
	KeyDefaultDivergent                = "default_divergent"
	KeyDetail                          = "detail"
	KeyDifferences                     = "differences"
	KeyDisposition                     = "disposition"
	KeyEmailAuthEnabled                = "email_auth_enabled"
	KeyErrorDetail                     = "error_detail"
	KeyErrorInstance                   = "error_instance"
	KeyErrorTitle                      = "error_title"
	KeyErrorType                       = "error_type"
	KeyExecutedAt                      = "executed_at"
	KeyExecutionState                  = "execution_state"
	KeyFailed                          = "failed"
	KeyFaroEnabled                     = "faro_enabled"
	KeyFileHash                        = "file_hash"
	KeyFinishMessage                   = "finish_message"
	KeyFinishReason                    = "finish_reason"
	KeyFound                           = "found"
	KeyFromStatus                      = "from_status"
	KeyGenkitFinalResponsePresent      = "genkit_final_response_present"
	KeyGenkitStreamChunkCount          = "genkit_stream_chunk_count"
	KeyGenkitStreamDoneReceived        = "genkit_stream_done_received"
	KeyHasCustomValidators             = "hasCustomValidators"
	KeyHasPasswordLegacy               = "hasPassword"
	KeyHasCursor                       = "has_cursor"
	KeyHiddenEvent                     = "hidden_event"
	KeyHTMLBodyLength                  = "html_body_length"
	KeyHTMLBodyPreview                 = "html_body_preview"
	KeyIncludeLeft                     = "include_left"
	KeyIndex                           = "index"
	KeyInferenceAttemptID              = "inference_attempt_id"
	KeyInputTokens                     = "input_tokens"
	KeyInsertions                      = "insertions"
	KeyInternalOnly                    = "internal_only"
	KeyJob                             = "job"
	KeyLastSent                        = "last_sent"
	KeyLength                          = "length"
	KeyLockExpires                     = "lockExpires"
	KeyLockExpiresAt                   = "lockExpiresAt"
	KeyLockOwner                       = "lock_owner"
	KeyMarked                          = "marked"
	KeyMemberCount                     = "member_count"
	KeyMessageKind                     = "message_kind"
	KeyMessageLength                   = "message_length"
	KeyMessagePartCount                = "message_part_count"
	KeyMessagePresent                  = "message_present"
	KeyMode                            = "mode"
	KeyNormalizedResponseBytes         = "normalized_response_bytes"
	KeyNormalizedResponseSHA256        = "normalized_response_sha256"
	KeyOAuthEnabled                    = "oauth_enabled"
	KeyOrganizationIDColumn            = "organization_id_column"
	KeyOrganizationsEnabled            = "organizations_enabled"
	KeyOTPCode                         = "otp_code"
	KeyOTPExpiresAt                    = "otp_expires_at"
	KeyOutputTokens                    = "output_tokens"
	KeyOutstanding                     = "outstanding"
	KeyOutstandingPublishLimit         = "outstanding_publish_limit"
	KeyOwnsDatabasePool                = "owns_database_pool"
	KeyPages                           = "pages"
	KeyPaths                           = "paths"
	KeyPhoneAuthEnabled                = "phone_auth_enabled"
	KeyPublicOrigin                    = "public_origin"
	KeyPublicRead                      = "public_read"
	KeyPublicVisibility                = "public_visibility"
	KeyRawResponsePresent              = "raw_response_present"
	KeyRealtimeWebSocketEnabled        = "realtime_websocket_enabled"
	KeyRedactedResponseShapeJSON       = "redacted_response_shape_json"
	KeyRedactedResponseShapeTruncated  = "redacted_response_shape_truncated"
	KeyRefreshMappingPreserved         = "refresh_mapping_preserved"
	KeyRegion                          = "region"
	KeyRejectedCount                   = "rejectedCount"
	KeyRemainingMinutes                = "remaining_minutes"
	KeyRequestIndex                    = "request_index"
	KeyRequestOmitted                  = "request_omitted"
	KeyRequested                       = "requested"
	KeyResolved                        = "resolved"
	KeyResponse                        = "response"
	KeyResponsePresent                 = "response_present"
	KeyResults                         = "results"
	KeyResumeOnDenial                  = "resume_on_denial"
	KeyReturned                        = "returned"
	KeyRunCount                        = "run_count"
	KeyScanned                         = "scanned"
	KeySearchScope                     = "search_scope"
	KeySnapshotCount                   = "snapshot_count"
	KeySourceTable                     = "source_table"
	KeySSLMode                         = "ssl_mode"
	KeySuggestionsOnly                 = "suggestions_only"
	KeySummaryID                       = "summary_id"
	KeyTargetUserIDFilter              = "targetUserIDFilter"
	KeyToStatus                        = "to_status"
	KeyTokenIndex                      = "token_index"
	KeyToolCallCount                   = "tool_call_count"
	KeyToolConfigCount                 = "tool_config_count"
	KeyTotalTokens                     = "total_tokens"
	KeyTwilioSID                       = "twilio_sid"
	KeyUpdatedAt                       = "updated_at"
	KeyUsagePresent                    = "usage_present"
	KeyUserCount                       = "user_count"
	KeyUserProfilesEnabled             = "user_profiles_enabled"
	KeyUserSettingsEnabled             = "user_settings_enabled"
	KeyVisibility                      = "visibility"
	KeyVonageMessageUUID               = "vonage_message_uuid"
	KeyVonageWorkflowID                = "vonage_workflow_id"
	KeyWasExpiringAt                   = "wasExpiringAt"
)

// Complete model-facing diagnostic payloads (DEBUG only).
const (
	KeyPayloadJSON                = "payload_json"
	KeyPayloadSerializationFailed = "payload_serialization_failed"
)

// Duration keys include their fixed output unit. Use the matching duration field helper.
const (
	KeyActualSeconds               = "actual_s"
	KeyAdditionalTimeSeconds       = "additional_time_s"
	KeyCacheTTLSeconds             = "cache_ttl_s"
	KeyCommandAuthBudgetMS         = "command_auth_budget_ms"
	KeyElapsedMS                   = "elapsed_ms"
	KeyExpirySeconds               = "expiry_s"
	KeyFallbackSeconds             = "fallback_s"
	KeyFlushWaitMS                 = "flush_wait_ms"
	KeyHookTimeoutSeconds          = "hook_timeout_s"
	KeyIntervalSeconds             = "interval_s"
	KeyLeaseDurationSeconds        = "lease_duration_s"
	KeyLockTTLSeconds              = "lock_ttl_s"
	KeyMaxAgeSeconds               = "max_age_s"
	KeyMaxWindowSeconds            = "max_window_s"
	KeyNewTTLSeconds               = "new_ttl_s"
	KeyOlderThanSeconds            = "older_than_s"
	KeyPendingUploadTimeoutSeconds = "pending_upload_timeout_s"
	KeyProcessingTimeoutSeconds    = "processing_timeout_s"
	KeyPublishTimeoutSeconds       = "publish_timeout_s"
	KeyQuantumSeconds              = "quantum_s"
	KeyRequestedTTLSeconds         = "requested_ttl_s"
	KeyRetryAfterSeconds           = "retry_after_s"
	KeySecurityTTLSeconds          = "security_ttl_s"
	KeySessionAgeSeconds           = "session_age_s"
	KeySessionTTLSeconds           = "session_ttl_s"
	KeyShutdownGraceSeconds        = "shutdown_grace_s"
	KeyStuckDurationSeconds        = "stuck_duration_s"
	KeyTimeoutSeconds              = "timeout_s"
	KeyWindowSeconds               = "window_s"
)
