// Package config provides centralized default values and validation for go-common configuration.
// This file consolidates all default values that were previously scattered throughout the codebase
// and provides validation functions to ensure configuration consistency.
package config

import (
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	// DefaultPaginationPageSize and DefaultMaxPaginationLimit are the default_pagination_limit and
	// max_pagination_limit defaults.
	DefaultPaginationPageSize = 20
	DefaultMaxPaginationLimit = 100
	// MaxPaginationLimitCeiling is the largest accepted max_pagination_limit (and default_pagination_limit).
	MaxPaginationLimitCeiling = 1000
	// DefaultInferenceAttemptListDefaultLimit and DefaultInferenceAttemptListMaxLimit are the
	// inference_attempt_list_default_limit and inference_attempt_list_max_limit defaults.
	DefaultInferenceAttemptListDefaultLimit = 100
	DefaultInferenceAttemptListMaxLimit     = 500
	// MaxInferenceAttemptListLimit is the largest accepted inference attempt page size setting.
	MaxInferenceAttemptListLimit = 1000
	// DefaultOrganizationParentOptionsMaxLimit caps one admin organization parent-option page.
	DefaultOrganizationParentOptionsMaxLimit = 20
	MaxOrganizationParentOptionsMaxLimit     = 1000
	// DefaultSanitizerLogInputMaxLength is the default diagnostic preview limit in Unicode code points.
	DefaultSanitizerLogInputMaxLength = 256
	// MaxSanitizerLogInputLength is the hard safety ceiling for sanitizer diagnostic previews.
	MaxSanitizerLogInputLength = 4096

	// DefaultChatAgentDelegationStateMaxChildren is the default number of delegated children the
	// delegation state read returns to a parent's prompt state block and unscoped subagent_status.
	// Validation keeps it at least both active-children limits, so a parent never loses a working
	// child to the cap: active children are read first.
	DefaultChatAgentDelegationStateMaxChildren = 64
	// MaxChatAgentDelegationStateMaxChildren bounds the per-step read and the prompt block it feeds.
	MaxChatAgentDelegationStateMaxChildren = 256
	// MinChatAgentDelegationMetadataValueMaxBytes fits a one-rune title; validation requires
	// utf8.UTFMax bytes per rune of chat_agent_delegation_title_max_runes.
	MinChatAgentDelegationMetadataValueMaxBytes = utf8.UTFMax
	// MaxChatAgentDelegationMetadataValueMaxBytes caps one delegation metadata value.
	MaxChatAgentDelegationMetadataValueMaxBytes = 65536
	// DefaultChatAgentDelegationMetadataMaxBytes caps a delegated child's encoded metadata.
	DefaultChatAgentDelegationMetadataMaxBytes = 8192
	// MinChatAgentDelegationMetadataMaxBytes fits the encoded metadata of a one-rune title;
	// validation requires room for the encoded longest title.
	MinChatAgentDelegationMetadataMaxBytes = delegationTitleMetadataOverheadBytes + jsonEscapedRuneMaxBytes
	MaxChatAgentDelegationMetadataMaxBytes = 262144
	// delegationTitleMetadataOverheadBytes is the encoded metadata of every delegated child around
	// its title, and jsonEscapedRuneMaxBytes the widest JSON encoding of one title rune, a control
	// character's \u escape.
	delegationTitleMetadataOverheadBytes = len(`{"delegation.title":""}`)
	jsonEscapedRuneMaxBytes              = len(`\u0000`)

	// DefaultChatAgentDelegatedChildrenPageMax caps the delegated children one ListDelegatedChildren
	// page returns; a request for more, or for no particular size, gets this many.
	DefaultChatAgentDelegatedChildrenPageMax = 50
	MaxChatAgentDelegatedChildrenPageMax     = 500

	// DefaultChatAgentSessionsByChannelMax caps the agent sessions one channel listing returns;
	// the bundled UI mirrors the default as CHANNEL_AGENT_SESSIONS_LIST_CAP.
	DefaultChatAgentSessionsByChannelMax = 200
	MaxChatAgentSessionsByChannel        = 1000
	// DefaultChatAgentRunRecoveryScanPageSize caps recoverable agent runs listed per recovery scan page.
	DefaultChatAgentRunRecoveryScanPageSize = 200
	MaxChatAgentRunRecoveryScanPageSize     = 1000
	// DefaultChatAgentStreamRecoveryMaxAttempts is the number of stream recovery attempts per agent run step.
	DefaultChatAgentStreamRecoveryMaxAttempts = 2
	MaxChatAgentStreamRecoveryMaxAttempts     = 10
	// DefaultChatAgentRunSettleTimeout bounds detached agent drive settlement and accounting writes.
	DefaultChatAgentRunSettleTimeout = 10 * time.Second
	MinChatAgentRunSettleTimeout     = time.Second
	MaxChatAgentRunSettleTimeout     = 5 * time.Minute
	// DefaultChatAgentWorkflowResultRecoveryFloor is the admission/recovery floor for workflow drives with durable results.
	DefaultChatAgentWorkflowResultRecoveryFloor = 30 * time.Second
	MinChatAgentWorkflowResultRecoveryFloor     = time.Second
	MaxChatAgentWorkflowResultRecoveryFloor     = 10 * time.Minute
	// DefaultChatAgentRunDriveTimeout is the wall-clock budget of one managed or recovery agent drive.
	DefaultChatAgentRunDriveTimeout = 10 * time.Minute
	MinChatAgentRunDriveTimeout     = 10 * time.Second
	MaxChatAgentRunDriveTimeout     = 24 * time.Hour
	// DefaultChatAgentRunDriveLeaseDuration is the durable drive lease a claim or renewal grants. It
	// must outlast one heartbeat interval plus one renewal's database timeout.
	DefaultChatAgentRunDriveLeaseDuration = 2 * time.Minute
	MinChatAgentRunDriveLeaseDuration     = 10 * time.Second
	MaxChatAgentRunDriveLeaseDuration     = time.Hour
	// DefaultChatAgentRunDriveHeartbeatInterval is how often a drive renews its lease.
	DefaultChatAgentRunDriveHeartbeatInterval = 20 * time.Second
	MinChatAgentRunDriveHeartbeatInterval     = time.Second
	MaxChatAgentRunDriveHeartbeatInterval     = 10 * time.Minute
	// DefaultChatAgentRunDriveRetryBackoff delays a retryable drive and an in-drive stream recovery.
	DefaultChatAgentRunDriveRetryBackoff = 10 * time.Second
	MinChatAgentRunDriveRetryBackoff     = time.Second
	MaxChatAgentRunDriveRetryBackoff     = 10 * time.Minute
	// DefaultChatAgentRunDriveDBTimeout bounds each detached drive and recovery-sweep database call.
	DefaultChatAgentRunDriveDBTimeout = 5 * time.Second
	MinChatAgentRunDriveDBTimeout     = time.Second
	MaxChatAgentRunDriveDBTimeout     = time.Minute
	// DefaultChatAgentRunRecoveryScanInterval is the stale-run recovery sweep period.
	DefaultChatAgentRunRecoveryScanInterval = 15 * time.Second
	MinChatAgentRunRecoveryScanInterval     = time.Second
	MaxChatAgentRunRecoveryScanInterval     = 10 * time.Minute
	// DefaultChatAgentRunRecoveryBatchSize caps the stale runs one recovery sweep dispatches.
	DefaultChatAgentRunRecoveryBatchSize = 50
	MaxChatAgentRunRecoveryBatchSize     = 1000
	// DefaultChatAgentRunRecoveryConcurrency is the recovery worker count (restart-bound).
	DefaultChatAgentRunRecoveryConcurrency = 8
	MaxChatAgentRunRecoveryConcurrency     = 256
	// DefaultChatAgentRunManagedAttemptCapacity caps concurrent managed drive attempts (restart-bound).
	DefaultChatAgentRunManagedAttemptCapacity = 32
	MaxChatAgentRunManagedAttemptCapacity     = 1024
	// DefaultChatAgentRunDriveAbandonGrace is the wait for a drive call that ignores cancellation.
	DefaultChatAgentRunDriveAbandonGrace = 2 * time.Second
	MinChatAgentRunDriveAbandonGrace     = 100 * time.Millisecond
	MaxChatAgentRunDriveAbandonGrace     = time.Minute
	// DefaultChatAgentRunMaxDriveAbandonments is the abandoned drive attempts a run survives.
	DefaultChatAgentRunMaxDriveAbandonments = 1
	MaxChatAgentRunMaxDriveAbandonments     = 10
	// DefaultChatAgentRunAbandonedDriveCapacity caps abandoned drive attempts still running
	// (restart-bound).
	DefaultChatAgentRunAbandonedDriveCapacity = 32
	MaxChatAgentRunAbandonedDriveCapacity     = 4096
	// DefaultChatAgentWorkflowWaitPollInterval is the workflow agent Wait polling backstop.
	DefaultChatAgentWorkflowWaitPollInterval = 15 * time.Second
	MinChatAgentWorkflowWaitPollInterval     = time.Second
	MaxChatAgentWorkflowWaitPollInterval     = 10 * time.Minute
	// DefaultChatAgentGraphDriveClaimPollInterval is how often an idle DriveGraph worker looks for
	// newly drivable runs while other drives of its graph are in flight.
	DefaultChatAgentGraphDriveClaimPollInterval = time.Second
	MinChatAgentGraphDriveClaimPollInterval     = 10 * time.Millisecond
	MaxChatAgentGraphDriveClaimPollInterval     = time.Minute
	// DefaultChatToolWorkScanTimeout bounds one claimable tool-work listing.
	DefaultChatToolWorkScanTimeout = 15 * time.Second
	MinChatToolWorkScanTimeout     = time.Second
	MaxChatToolWorkScanTimeout     = 5 * time.Minute
	// DefaultChatToolWorkSettleTimeout bounds a detached tool call claim release or result commit.
	DefaultChatToolWorkSettleTimeout = 15 * time.Second
	MinChatToolWorkSettleTimeout     = time.Second
	MaxChatToolWorkSettleTimeout     = 5 * time.Minute
	// DefaultChatToolWorkScanLimit caps the tool calls one tool-work scan considers.
	DefaultChatToolWorkScanLimit = 100
	MaxChatToolWorkScanLimit     = 1000
	// DefaultChatToolWorkMaxConcurrency is the handler-wide tool-work worker count (restart-bound).
	DefaultChatToolWorkMaxConcurrency = 8
	MaxChatToolWorkMaxConcurrency     = 256
	// DefaultChatToolWorkAttemptCapacity caps running and abandoned tool executions (restart-bound);
	// it must admit at least one execution per worker.
	DefaultChatToolWorkAttemptCapacity = 32
	MaxChatToolWorkAttemptCapacity     = 4096
	// DefaultChatToolWorkExecutionLeaseDuration is the tool call claim lease. It must outlast the
	// execution timeout plus the settle timeout, so a timed-out call releases its own claim.
	DefaultChatToolWorkExecutionLeaseDuration = 5 * time.Minute
	MinChatToolWorkExecutionLeaseDuration     = 10 * time.Second
	MaxChatToolWorkExecutionLeaseDuration     = 2 * time.Hour
	// DefaultChatToolWorkExecutionTimeout bounds one tool-work execution.
	DefaultChatToolWorkExecutionTimeout = 4*time.Minute + 30*time.Second
	MinChatToolWorkExecutionTimeout     = time.Second
	MaxChatToolWorkExecutionTimeout     = time.Hour
	// DefaultChatToolWorkShutdownGrace is the shutdown wait for canceled tool executions.
	DefaultChatToolWorkShutdownGrace = 2 * time.Second
	MinChatToolWorkShutdownGrace     = 100 * time.Millisecond
	MaxChatToolWorkShutdownGrace     = time.Minute
	// DefaultChatAgentRunStatusMaxLocations caps the thread locations in one agent run status read;
	// the bundled UI mirrors the default as DELEGATED_RUN_STATUS_MAX_LOCATIONS and sends up to that
	// many roots in one unchunked request, so the key is floored there: a lower value would reject
	// every delegated status read with more known roots instead of degrading.
	DefaultChatAgentRunStatusMaxLocations = 50
	MinChatAgentRunStatusMaxLocations     = 50
	MaxChatAgentRunStatusMaxLocations     = 500
	// DefaultChatAgentAnthropicDefaultMaxOutputTokens is the max_tokens sent on Anthropic Messages
	// agent generations when neither the model policy nor the native config sets one; the API
	// requires the field. The ceiling keeps a mere default below the largest documented model
	// output windows, which an explicit policy or native config still selects freely.
	DefaultChatAgentAnthropicDefaultMaxOutputTokens = 4096
	MaxChatAgentAnthropicDefaultMaxOutputTokens     = 32768
	// DefaultChatAgentRunStatusMaxHistory caps the older runs returned per status location. The
	// bundled UI's useAgentRunStatus sends historyLimit 5 by default, so the key is floored there: a
	// lower value would reject every bundled status read instead of degrading.
	DefaultChatAgentRunStatusMaxHistory = 10
	MinChatAgentRunStatusMaxHistory     = 5
	MaxChatAgentRunStatusMaxHistory     = 100
	// DefaultChatAgentDelegationReportExcerptRunes is the child report excerpt length in a parent's delegation state.
	DefaultChatAgentDelegationReportExcerptRunes = 160
	MinChatAgentDelegationReportExcerptRunes     = 8
	MaxChatAgentDelegationReportExcerptRunes     = 2000
	// DefaultChatAgentDelegationTitleMaxRunes caps a delegated task title.
	DefaultChatAgentDelegationTitleMaxRunes = 200
	MaxChatAgentDelegationTitleRunes        = 200
	// DefaultChatAgentMaxPendingInputs caps the inputs queued for one agent session, so a busy or
	// stopped agent cannot accumulate unbounded paid follow-up context.
	DefaultChatAgentMaxPendingInputs = 20
	MaxChatAgentMaxPendingInputs     = 200
	// DefaultChatTypingRefreshWindow sits below the bundled UI's 2.5s typing send interval and the
	// 3s BotTypingInterval, so every legitimate refresh passes while a flood of identical starts
	// collapses to one per window; the maximum keeps the bundled UI's refreshes deliverable.
	DefaultChatTypingRefreshWindow = 2 * time.Second
	MinChatTypingRefreshWindow     = 100 * time.Millisecond
	MaxChatTypingRefreshWindow     = 2400 * time.Millisecond
	// DefaultChatMaxClientMessageIDLength bounds a send's sender-chosen idempotency key in bytes;
	// a UUID or ULID fits with room to spare. The key is a column of a btree unique index, whose
	// entries PostgreSQL caps near 2.7 KB, so the maximum stays well below that.
	DefaultChatMaxClientMessageIDLength = 128
	MaxChatMaxClientMessageIDLength     = 1024
	// DefaultAgentSessionControlMaxAttempts and DefaultAgentSessionControlWindowDuration bound one
	// person's agent Stop and Resume requests (rate_limits group).
	DefaultAgentSessionControlMaxAttempts    = 30
	DefaultAgentSessionControlWindowDuration = time.Minute
	// DefaultChatForkMaxAttempts and DefaultChatForkWindowDuration bound one person's chat channel
	// forks, each a synchronous copy of up to chat_fork_max_copied_messages rows (rate_limits group).
	DefaultChatForkMaxAttempts    = 10
	DefaultChatForkWindowDuration = time.Minute
	// DefaultOAuthCallbackMaxAttempts and DefaultOAuthCallbackWindowDuration bound one
	// client IP's OAuth callbacks and form_post relays (rate_limits group).
	DefaultOAuthCallbackMaxAttempts    = 20
	DefaultOAuthCallbackWindowDuration = time.Minute
	// DefaultChatAgentDelegationReadDefaultLimit is the page size of a subagent transcript read
	// that names no limit. It may not exceed the configured read max limit.
	DefaultChatAgentDelegationReadDefaultLimit = 20
	MinChatAgentDelegationReadDefaultLimit     = 1
	MaxChatAgentDelegationReadDefaultLimit     = 500
	// DefaultChatAgentDelegationReadMaxLimit is the largest page size a subagent transcript read may request.
	DefaultChatAgentDelegationReadMaxLimit = 100
	MinChatAgentDelegationReadMaxLimit     = 1
	MaxChatAgentDelegationReadMaxLimit     = 500
	// DefaultChatAgentDelegationReadMaxBytes caps the serialized bytes one subagent transcript read returns.
	DefaultChatAgentDelegationReadMaxBytes = 32768
	MinChatAgentDelegationReadMaxBytes     = 1024
	MaxChatAgentDelegationReadMaxBytes     = 1048576
	// DefaultChatAgentDelegationReadMaxContentChars caps the Unicode characters in each content
	// field a subagent transcript read returns.
	DefaultChatAgentDelegationReadMaxContentChars = 4000
	MinChatAgentDelegationReadMaxContentChars     = 128
	MaxChatAgentDelegationReadMaxContentChars     = 65536
	// DefaultAttachmentXMLMaxTokenBytes caps the bytes one XML token, and the open start tags together, may span during upload validation.
	DefaultAttachmentXMLMaxTokenBytes = 1 << 20
	MinAttachmentXMLMaxTokenBytes     = 64 << 10
	MaxAttachmentXMLMaxTokenBytes     = 64 << 20
	// DefaultObjectStorageStreamPartSize is the part size for unknown-length object uploads;
	// the bounds are the S3 minimum and maximum part sizes.
	DefaultObjectStorageStreamPartSize = 16 << 20
	MinObjectStorageStreamPartSize     = 5 << 20
	MaxObjectStorageStreamPartSize     = 5 << 30
	// DefaultAttachmentInternalReclaimGrace is how long an unreferenced internal attachment row
	// stays untouched before the cleanup routine reclaims it.
	DefaultAttachmentInternalReclaimGrace = time.Hour
	MinAttachmentInternalReclaimGrace     = time.Minute
	MaxAttachmentInternalReclaimGrace     = 720 * time.Hour
	// DefaultAttachmentInternalReclaimBatchSize caps the internal rows one cleanup tick reclaims.
	DefaultAttachmentInternalReclaimBatchSize = 100
	MinAttachmentInternalReclaimBatchSize     = 1
	MaxAttachmentInternalReclaimBatchSize     = 200
	// DefaultAttachmentTemporaryObjectCleanupTimeout bounds one post-commit temporary-object deletion.
	DefaultAttachmentTemporaryObjectCleanupTimeout = 3 * time.Second
	MinAttachmentTemporaryObjectCleanupTimeout     = 100 * time.Millisecond
	MaxAttachmentTemporaryObjectCleanupTimeout     = time.Minute
	// DefaultAttachmentTemporaryUploadPruneAge is how old a still-pending upload must be to be pruned.
	DefaultAttachmentTemporaryUploadPruneAge = 24 * time.Hour
	MinAttachmentTemporaryUploadPruneAge     = time.Hour
	MaxAttachmentTemporaryUploadPruneAge     = 720 * time.Hour
	// DefaultAttachmentTemporaryObjectSweepBatchSize caps the leftover temporary objects one cleanup tick deletes.
	DefaultAttachmentTemporaryObjectSweepBatchSize = 200
	MinAttachmentTemporaryObjectSweepBatchSize     = 1
	MaxAttachmentTemporaryObjectSweepBatchSize     = 1000
	// DefaultChatAgentDelegationMaxRunsPerChild is the maximum lifetime number of runs one
	// delegated child may have; orchestration cannot start another run once a child reaches it.
	DefaultChatAgentDelegationMaxRunsPerChild = 8
	MinChatAgentDelegationMaxRunsPerChild     = 1
	MaxChatAgentDelegationMaxRunsPerChild     = 1000
	// DefaultChatForkMaxCopiedMessages caps the source messages one channel fork copies in its
	// synchronous request transaction.
	DefaultChatForkMaxCopiedMessages = 5000
	MinChatForkMaxCopiedMessages     = 1
	MaxChatForkMaxCopiedMessages     = 100000
)

// DefaultPasswordSpecialChars is the default set of accepted special characters
// for password complexity validation. It is used as the config default for
// SanitizerPasswordSpecialChars and must stay in
// sync with DEFAULT_PASSWORD_SPECIAL_CHARS in the TypeScript core package.
const DefaultPasswordSpecialChars = "!@#$%^&*()_+-=[]{}|;:,.<>?"

// GetDefaults executes its operation in the configuration loading, defaulting, and validation flow.
func GetDefaults() *Config {

	defaults := &Config{}

	// Database defaults
	defaults.DBMaxOpenConns = new(25)
	defaults.DBMaxIdleConns = new(10)
	defaults.DBConnMaxLifetime = new(3600)                 // 1 hour in seconds
	defaults.DBConnMaxIdleTime = new(1800)                 // 30 minutes in seconds
	defaults.DBQueryTimeout = new(30 * time.Second)        // 30 seconds
	defaults.DBTransactionTimeout = new(60 * time.Second)  // 60 seconds
	defaults.DBHealthCheckInterval = new(30 * time.Second) // 30 seconds
	defaults.DBMetricsInterval = new(1 * time.Hour)        // 1 hour
	defaults.DBPort = new(5432)                            // PostgreSQL default port
	defaults.DBLogLevel = new(DBLogLevelWarn)              // balanced verbosity

	// Authentication defaults
	defaults.AuthCacheTtl = new(15 * time.Minute)                // 15 minutes
	defaults.AuthTokenCacheCapacity = new(uint64(10000))         // 10k tokens
	defaults.AuthUserCacheCapacity = new(uint64(5000))           // 5k users
	defaults.OrganizationRoleCacheCapacity = new(uint64(10000))  // 10k role mappings
	defaults.OrganizationRoleCacheTTL = new(15 * time.Minute)    // 15 minutes
	defaults.VerificationTokenValidity = new(24 * time.Hour)     // 24 hours
	defaults.VerificationCooldown = new(1 * time.Minute)         // 1 minute
	defaults.UnverifiedUserCleanupInterval = new(1 * time.Hour)  // 1 hour
	defaults.UnverifiedUserCleanupBatchSize = new(250)           // 250 users
	defaults.UnverifiedUserCleanupTimeout = new(2 * time.Minute) // 2 minutes
	defaults.PhoneVerificationOTPValidity = new(5 * time.Minute) // 5 minutes for OTP codes
	defaults.PhoneVerificationCooldown = new(1 * time.Minute)    // 1 minute between SMS requests
	defaults.PhoneNumberDefaultRegion = new("US")                // Default region for phone number parsing
	defaults.OTPIssuer = new("go-common")                        // Default TOTP issuer name
	defaults.SessionValidity = new(15 * time.Minute)             // 15 minutes
	defaults.EnableSessionRefresh = new(false)                   // opt-in
	defaults.SessionRefreshSoftThreshold = new(0.5)              // never refresh before half the TTL has elapsed
	defaults.SessionRefreshHardThreshold = new(0.9)              // always refresh past 90% of the TTL
	defaults.SessionRefreshRampExponent = new(1.0)               // linear ramp between the thresholds
	defaults.SessionRefreshRevocationGrace = new(30 * time.Second)

	// Sanitizer defaults
	defaults.SanitizerLogInputMaxLength = new(DefaultSanitizerLogInputMaxLength)
	defaults.SanitizerPasswordMinLength = new(8)
	defaults.SanitizerPasswordMaxLength = new(128)
	defaults.SanitizerUsernameMinLength = new(3)
	defaults.SanitizerUsernameMaxLength = new(50)
	defaults.SanitizerMaxEmailLength = new(100)
	defaults.SanitizerMaxLocalPartLength = new(64)     // RFC 5321
	defaults.SanitizerMaxDomainLength = new(253)       // RFC 5321
	defaults.SanitizerVerificationMinLength = new(127) // expected length for security tokens
	defaults.SanitizerVerificationMaxLength = new(129) // allow slight variance

	// Contact form defaults
	defaults.SanitizerContactMinNameLength = new(2)
	defaults.SanitizerContactMaxNameLength = new(100)
	defaults.SanitizerContactMinSubjectLength = new(5)
	defaults.SanitizerContactMaxSubjectLength = new(200)
	defaults.SanitizerContactMinMessageLength = new(10)
	defaults.SanitizerContactMaxMessageLength = new(5000)

	// Real name validation defaults
	defaults.SanitizerRealNameMinLength = new(2)
	defaults.SanitizerRealNameMaxLength = new(50)

	// User signup enforcement defaults
	defaults.RequireRealNamesOnSignup = new(true)
	defaults.RequireUsernamesOnSignup = new(true)
	defaults.OAuthSignupMode = new(OAuthSignupModeLegacy)
	defaults.OAuthDefaultUsernamePrefix = new("^")
	defaults.OAuthHTTPTimeout = new(DefaultOAuthHTTPTimeout)
	defaults.OAuthStateTTL = new(DefaultOAuthStateTTL)
	defaults.OAuthStateClockSkew = new(DefaultOAuthStateClockSkew)
	defaults.OAuthDiscoveryRefreshInterval = new(DefaultOAuthDiscoveryRefreshInterval)
	defaults.OAuthSignedClientSecretLifetime = new(DefaultOAuthSignedClientSecretLifetime)
	defaults.OAuthMaxResponseBytes = new(int64(DefaultOAuthMaxResponseBytes))
	defaults.OAuthMaxStateLength = new(DefaultOAuthMaxStateLength)
	defaults.OAuthMaxCodeLength = new(DefaultOAuthMaxCodeLength)
	defaults.OAuthFormPostMaxBodyBytes = new(int64(DefaultOAuthFormPostMaxBodyBytes))

	// Mock token service defaults
	defaults.UseTokenServiceMock = new(false)         // NEVER enable by default - explicit opt-in only
	defaults.TokenServiceMockSeed = new(int64(12345)) // Default seed for deterministic testing

	// File attachment defaults
	defaults.AttachmentUploadURLExpiry = new(30 * time.Minute)        // 30 minutes
	defaults.AttachmentDownloadURLExpiry = new(15 * time.Minute)      // 15 minutes
	defaults.AttachmentPresignHost = new("")                          // SDK presign + host rewrite
	defaults.AttachmentPresignQuantum = new(time.Duration(0))         // quantization disabled
	defaults.AttachmentPresignRegion = new("us-east-1")               // SigV4 credential-scope region
	defaults.AttachmentPresignAccessKey = new("")                     // fall back to the MinIO credential
	defaults.AttachmentDefaultMaxSize = new(int64(100 * 1024 * 1024)) // 100MB
	defaults.StreamingHashChunkSize = new(int64(50 * 1024 * 1024))    // 50MB
	defaults.AttachmentProcessingTimeout = new(10 * time.Minute)      // 10 minutes
	defaults.AttachmentCleanupInterval = new(5 * time.Minute)         // 5 minutes
	defaults.AttachmentEnableAutoCleanup = new(true)                  // enable auto cleanup
	defaults.AttachmentPendingUploadTimeout = new(1 * time.Hour)      // 1 hour
	defaults.AttachmentLockTTL = new(10 * time.Minute)                // 10 minutes
	defaults.AttachmentLockCapacity = new(uint64(30000))              // 30k concurrent locks
	defaults.AttachmentXMLMaxTokenBytes = new(int64(DefaultAttachmentXMLMaxTokenBytes))
	defaults.AttachmentInternalReclaimGrace = new(DefaultAttachmentInternalReclaimGrace)
	defaults.AttachmentInternalReclaimBatchSize = new(DefaultAttachmentInternalReclaimBatchSize)
	defaults.AttachmentTemporaryObjectCleanupTimeout = new(DefaultAttachmentTemporaryObjectCleanupTimeout)
	defaults.AttachmentTemporaryUploadPruneAge = new(DefaultAttachmentTemporaryUploadPruneAge)
	defaults.AttachmentTemporaryObjectSweepBatchSize = new(DefaultAttachmentTemporaryObjectSweepBatchSize)
	defaults.ObjectStorageStreamPartSize = new(int64(DefaultObjectStorageStreamPartSize))

	// Rate limiting defaults
	defaults.RateLimitCacheCapacity = new(uint64(30000))         // in-memory limiter cardinality
	defaults.LoginMaxAttempts = new(5)                           // attempts per window
	defaults.LoginWindowDuration = new(15 * time.Minute)         // attempt window
	defaults.RegisterMaxAttempts = new(3)                        // attempts per window
	defaults.RegisterWindowDuration = new(10 * time.Minute)      // attempt window
	defaults.PasswordResetMaxAttempts = new(5)                   // attempts per window
	defaults.PasswordResetWindowDuration = new(15 * time.Minute) // attempt window
	defaults.VerifyPhoneOTPMaxAttempts = new(5)                  // attempts per OTP lifetime
	defaults.VerifyPhoneOTPWindowDuration = new(5 * time.Minute) // matches the default OTP validity
	defaults.ContactMaxAttempts = new(5)                         // attempts per window
	defaults.ContactWindowDuration = new(15 * time.Minute)       // attempt window
	defaults.CareerApplicationMaxAttempts = new(5)               // attempts per window
	defaults.CareerApplicationWindowDuration = new(15 * time.Minute)
	defaults.CareerJobLookupMaxAttempts = new(20)
	defaults.CareerJobLookupWindowDuration = new(1 * time.Minute)
	defaults.PublicChatReadMaxAttempts = new(60)
	defaults.PublicChatReadWindowDuration = new(1 * time.Minute)
	defaults.OrganizationSlugAvailabilityMaxAttempts = new(10)
	defaults.OrganizationSlugAvailabilityWindowDuration = new(1 * time.Minute)
	defaults.NotificationCountMaxAttempts = new(DefaultNotificationCountMaxAttempts)
	defaults.NotificationCountWindowDuration = new(DefaultNotificationCountWindowDuration)
	defaults.AgentSessionControlMaxAttempts = new(DefaultAgentSessionControlMaxAttempts)
	defaults.AgentSessionControlWindowDuration = new(DefaultAgentSessionControlWindowDuration)
	defaults.ChatForkMaxAttempts = new(DefaultChatForkMaxAttempts)
	defaults.ChatForkWindowDuration = new(DefaultChatForkWindowDuration)
	defaults.OAuthCallbackMaxAttempts = new(DefaultOAuthCallbackMaxAttempts)
	defaults.OAuthCallbackWindowDuration = new(DefaultOAuthCallbackWindowDuration)

	// Server defaults
	defaults.ReadTimeout = new(30 * time.Second)                               // read timeout
	defaults.WriteTimeout = new(30 * time.Second)                              // write timeout
	defaults.IdleTimeout = new(120 * time.Second)                              // idle timeout
	defaults.PostCommitHookTimeout = new(30 * time.Second)                     // deadline on post-commit hook contexts
	defaults.PostCommitHookFlushWait = new(5 * time.Second)                    // max time a committed response waits on hooks
	defaults.TransactionResponseBufferLimitBytes = new(int64(4 * 1024 * 1024)) // 4 MiB buffered transaction response
	defaults.MaxPayloadSize = new(int64(50 * 1024 * 1024))                     // 50MB max payload

	// Metrics defaults
	defaults.MetricsPushInterval = new(60) // 60 seconds

	// Permission defaults (updated for new tier structure: tier 10 = highest, tier 0 = lowest)
	defaults.SiteMinUserTypeAdminAccess = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_2)                    // Basic admin access
	defaults.SiteMinUserTypeUserListing = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_2)                    // List users
	defaults.SiteMinUserTypeUserViewing = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_2)                    // View user details
	defaults.SiteMinUserTypeUserBanning = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5)                    // Ban/unban users
	defaults.SiteMinUserTypeUserModification = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5)               // Modify users
	defaults.SiteMinUserTypeUserDeletion = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_8)                   // Delete users (more restrictive)
	defaults.SiteMinUserTypeUserTypeChange = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_7)                 // Change user types
	defaults.SiteMinUserTypeSystemAccess = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5)                   // System settings access
	defaults.SiteMinUserTypeContentModeration = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_3)              // Content moderation
	defaults.SiteMinUserTypeAttachmentAccess = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_4)               // Attachment management
	defaults.SiteMinUserTypeAttachmentModification = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5)         // Attachment modification (must be >= attachment access; matches user modification)
	defaults.SiteMinUserTypeAnalyticsAccess = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_4)                // Analytics access
	defaults.SiteMinUserTypeAdminCreateOrganization = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_7)        // Create organization via admin
	defaults.SiteMinUserTypeAdminListOrganizations = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_3)         // List all organizations via admin
	defaults.SiteMinUserTypeAdminManageOrganizationMembers = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5) // Directly add members to organizations (bypassing invitations)
	defaults.SiteMinUserTypeAdminGetOrganizationMember = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_2)     // Get individual organization member details
	defaults.SiteMinUserTypeAdminChatChannels = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_4)              // Manage chat channels via admin
	defaults.SiteMinUserTypeCareersManagement = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_3)              // Manage careers via admin
	defaults.SiteMinUserTypeChatToolApproval = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_1)               // Resolve agentic chat tool-call approvals (Decision 6)
	defaults.SiteMinUserTypeInferenceModelOverride = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_2)
	defaults.SiteMinUserTypeInferenceViewing = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_3)        // View inference routes/policies/attempts (read side)
	defaults.SiteMinUserTypeInferenceAdministration = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5) // Update inference routes/policies (write side, matches system access)
	defaults.EnableOrganizationEmailDomains = new(false)                                                // Domain mapping disabled by default
	defaults.EnableOrganizationEmailAutoAssociation = new(false)                                        // Auto association disabled by default
	defaults.AllowOrganizationManagedEmailDomains = new(false)                                          // Org-managed domains disabled by default
	// BUSINESS_TIER_0 matches the OrgMinTypeCreateChildOrganizations default so
	// sub-organization creation inside personal workspaces works out of the box.
	defaults.PersonalWorkspaceOrganizationType = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinUserTypeManageEmailDomains = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5) // Admin tier 5 required by default
	defaults.OrgHierarchyMaxDepth = new(5)                                                        // Default max depth for org hierarchy
	defaults.OrgHierarchyMaxNodes = new(500)                                                      // Defensive cap for full hierarchy responses
	defaults.OrgRoleInheritanceEnabled = new(false)                                               // Role inheritance disabled by default
	defaults.OrgAncestorMembershipAutoAdd = new(false)                                            // Require explicit ancestor memberships by default

	defaultHierarchyMaxDepth := *defaults.OrgHierarchyMaxDepth
	makeRoleSlice := func(role proto_models.OrganizationRole) []proto_models.OrganizationRole {
		values := make([]proto_models.OrganizationRole, defaultHierarchyMaxDepth+1)
		for i := range values {
			values[i] = role
		}
		return values
	}
	makeBoolSlice := func(value bool) []bool {
		values := make([]bool, defaultHierarchyMaxDepth+1)
		for i := range values {
			values[i] = value
		}
		return values
	}
	makeOrgUserAttributePermissionsSlice := func(perms map[string]AttributeVisibility) []map[string]AttributeVisibility {
		values := make([]map[string]AttributeVisibility, defaultHierarchyMaxDepth+1)
		for i := range values {
			values[i] = maps.Clone(perms)
		}
		return values
	}

	// Default roles that vary by org depth
	defaults.DefaultOrgCreatorRole = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_2)                   // Default owner tier for creators
	defaults.PersonalWorkspaceOwnerRole = new(defaults.DefaultOrgCreatorRole[0])                                                   // Derived from DefaultOrgCreatorRole[0] so both stay aligned
	defaults.OrganizationAutoAssociationDefaultRole = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0) // Auto-associated users become member tier 0
	defaults.UserSearchMinOrgRole = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_1)                   // Org-scoped search requires member tier 1 by default

	defaults.OrgMinRoleManageEmailDomains = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_1) // Org admin tier 1 required by default

	// User attribute permission defaults - configure what user attributes are visible by organization role
	allAttributes := (PermViewEmail | PermViewPhoneNumber | PermViewFirstName | PermViewLastName | PermViewUsername | PermViewAvatarUrl | PermViewLastActivity).Visibility()
	defaultOrgUserAttributePermissions := map[string]AttributeVisibility{
		OrganizationRoleAttributeKey(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_1): (PermViewFirstName | PermViewLastName | PermViewUsername | PermViewAvatarUrl).Visibility(),
		OrganizationRoleAttributeKey(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_1):  allAttributes,
		OrganizationRoleAttributeKey(proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_2):  allAttributes,
	}
	defaults.OrgUserAttributePermissions = makeOrgUserAttributePermissionsSlice(defaultOrgUserAttributePermissions)

	// Global user attribute permission defaults - primarily for site staff to override org-level restrictions
	defaults.GlobalUserAttributePermissions = map[string]AttributeVisibility{
		// Public/unauthenticated access - extremely restrictive (only public, non-sensitive info)
		PublicAttributeKey: (PermViewUsername | PermViewAvatarUrl).Visibility(),
		UserTypeAttributeKey(proto_models.UserType_USER_TYPE_ADMIN_TIER_1): allAttributes,
		UserTypeAttributeKey(proto_models.UserType_USER_TYPE_ADMIN_TIER_5): allAttributes,
	}
	defaults.PublicPrimaryUserIdentifier = new(proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USERNAME)
	defaults.PublicSecondaryUserIdentifier = new(proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_UNSPECIFIED)
	defaults.OrgMinUserTypeCreate = new(proto_models.UserType_USER_TYPE_REGULAR_TIER_5)          // Organization creation (mid-tier regular users)
	defaults.OrgMinUserTypeCreateChild = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_0)       // Global override for creating children in other organizations
	defaults.OrgMinUserTypeView = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_0)              // Site admins can view any organization; regular users require an organization role
	defaults.OrgMinUserTypeListMembers = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_3)       // List organization members (global admin check)
	defaults.OrgMinUserTypeDelete = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_7)            // Delete organization (high privilege admin operation)
	defaults.OrgMinUserTypeUpdate = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_4)            // Update organization (mid-level admin operation)
	defaults.OrgMinUserTypeInviteMembers = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_4)     // Invite org members (admin operation)
	defaults.OrgMinUserTypeAddMembers = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_5)        // Add org member (admin operation)
	defaults.OrgMinUserTypeRemoveMembers = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_6)     // Remove org member (admin operation)
	defaults.OrgMinUserTypeUpdateMemberRoles = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_6) // Update org role (admin operation)

	// Organization role-specific defaults (used by organization config)
	defaults.OrgMinRoleInviteMembers = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_1)     // Admin tier 1 can invite members
	defaults.OrgMinRoleAddMembers = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_0)        // Only owners can directly add members without consent
	defaults.OrgMinRoleRemoveMembers = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_2)     // Admin tier 2 can remove members
	defaults.OrgMinRoleUpdateMemberRoles = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_2) // Admin tier 2 can update member roles
	defaults.OrgMinRoleListMembers = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_1)      // Member tier 1 can list members
	defaults.OrgMinRoleView = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED)               // Any member can view organization details
	defaults.OrgMinRoleUpdate = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0)            // Admin tier 0 can update org details
	defaults.OrgMinRoleDelete = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_0)            // Owner tier 0 can delete orgs
	defaults.OrgMinRoleCancelInvitation = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_1)  // Admin tier 1 can cancel invitations
	defaults.OrgMinRoleViewInvitations = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_1)   // Admin tier 1 can view invitations
	defaults.OrgMinRoleCreateChild = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0)       // Admin tier 0 can create child orgs
	defaults.OrgMinRoleSelfJoin = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0)          // Admin tier 0 can self-join descendants
	defaults.OrgMinRoleViewInferencePolicy = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
	defaults.OrgMinRoleManageInferencePolicy = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_0)
	defaults.OrgMinRoleChooseInferenceModel = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0)
	// Transcripts stay participant-only unless a create names a role.
	defaults.OrgMinRoleReadAgentTranscript = makeRoleSlice(proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED)
	defaults.OrgAutoAddCreatorOnChildCreate = makeBoolSlice(false)                              // Do not auto-add creator by default
	defaults.OrgMinUserTypeCancelInvitation = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_6) // Site admins can cancel any org invitation
	defaults.OrgMinUserTypeViewInvitations = new(proto_models.UserType_USER_TYPE_ADMIN_TIER_6)  // Site admins can view any org's invitations

	// User creation defaults
	defaults.DefaultUserType = new(proto_models.UserType_USER_TYPE_REGULAR_TIER_2) // Default for new user registrations
	defaults.DefaultOrganizationType = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeListMembers = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BASIC_TIER_0)
	defaults.OrgMinTypeInviteMembers = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeAddMembers = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeRemoveMembers = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeUpdateMemberRoles = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeCancelInvitation = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeViewInvitations = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeManageEmailDomains = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeWorkspaceNavigation = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeCreateChildOrganizations = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)
	defaults.OrgMinTypeBulkMemberManagement = new(proto_models.OrganizationType_ORGANIZATION_TYPE_BUSINESS_TIER_0)

	// Email and SMS defaults
	defaults.EmailEnabled = new(false)                // disabled by default
	defaults.EmailProvider = new(EmailProviderResend) // default provider
	defaults.EmailFromAddress = new("")               // empty by default
	defaults.EmailFromName = new("")                  // empty by default
	defaults.SMSEnabled = new(false)                  // disabled by default
	defaults.SMSProvider = new(SMSProviderTwilio)     // default provider
	defaults.SMSFromNumber = new("")                  // empty by default

	// Metrics defaults
	defaults.MetricsEnabled = new(false)           // disabled by default
	defaults.MetricsPort = new(":9090")            // default port
	defaults.MetricsEndpoint = new("/metrics")     // default endpoint
	defaults.MetricsNamespace = new("gocommon")    // default namespace
	defaults.MetricsPushEnabled = new(false)       // disabled by default
	defaults.MetricsPushEndpoint = new("")         // empty by default
	defaults.MetricsPushJobName = new("go-common") // default job name
	defaults.MetricsPushBasicAuth = new(false)     // disabled by default
	defaults.MetricsPushUsername = new("")         // empty by default

	// CORS defaults
	defaults.EnableCORS = new(false)                                                  // disabled by default
	defaults.CorsAllowCredentials = new(false)                                        // disabled by default
	defaults.CorsMaxAge = new(12 * time.Hour)                                         // 12 hours
	defaults.CorsAllowedOrigins = []string{"*"}                                       // allow all by default
	defaults.CorsAllowedMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"} // common methods
	defaults.CorsAllowedHeaders = []string{"Content-Type", "Authorization"}           // common headers
	defaults.CorsExposeHeaders = []string{}                                           // empty by default

	// Module enablement defaults
	defaults.EnableEmailAuth = new(true)                  // enabled by default
	defaults.EnableOAuth = new(true)                      // enabled by default
	defaults.EnableContacts = new(true)                   // enabled by default
	defaults.EnableAttachments = new(true)                // enabled by default
	defaults.EnableUserSettings = new(true)               // enabled by default
	defaults.EnableUserProfiles = new(true)               // enabled by default
	defaults.EnableUserSearch = new(false)                // disabled by default
	defaults.EnableFaro = new(false)                      // disabled by default
	defaults.FaroMaxPayloadSize = new(int64(1024 * 1024)) // 1MB; Faro batches are typically a few KB
	defaults.EnableAdminAPI = new(false)                  // disabled by default
	defaults.EnableOrganizations = new(false)             // disabled by default
	defaults.EnableCareers = new(false)                   // disabled by default

	// User search defaults
	defaults.UserSearchRequireSameOrganization = new(true)
	defaults.UserSearchMinUserType = new(proto_models.UserType_USER_TYPE_REGULAR_TIER_0)
	defaults.UserSearchMinQueryLength = new(2)
	defaults.UserSearchMaxQueryLength = new(100)
	defaults.UserSearchEnableSuggestions = new(false)
	defaults.UserSearchAllowEmailLookup = new(false)
	defaults.UserSearchAllowUsernameLookup = new(true)
	defaults.UserSearchAllowRealNameLookup = new(false)
	defaults.UserSearchMaxAttempts = new(60)
	defaults.UserSearchWindowDuration = new(1 * time.Minute)

	// Chat module defaults
	defaults.EnableChat = new(false) // disabled by default
	defaults.EnablePublicChatRead = new(false)
	defaults.NotificationInboxFetchRows = new(DefaultNotificationInboxFetchRows)
	defaults.NotificationListCandidateBudget = new(DefaultNotificationListCandidateBudget)
	defaults.NotificationCountOperationTimeout = new(DefaultNotificationCountOperationTimeout)
	defaults.NotificationCountUnreadLimit = new(DefaultNotificationCountUnreadLimit)
	defaults.NotificationCountCandidateBudget = new(DefaultNotificationCountCandidateBudget)
	defaults.NotificationListDefaultLimit = new(DefaultNotificationListDefaultLimit)
	defaults.NotificationListMaxLimit = new(DefaultNotificationListMaxLimit)
	defaults.NotificationMaxReadReceipts = new(DefaultNotificationMaxReadReceipts)
	defaults.NotificationMaxPreferenceUpdates = new(DefaultNotificationMaxPreferenceUpdates)
	defaults.EnableNotifications = new(false) // opt in
	defaults.NotificationFanoutAsync = new(false)
	defaults.NotificationFanoutWorkerCount = new(DefaultNotificationFanoutWorkerCount)
	defaults.NotificationFanoutPaused = new(false)
	defaults.NotificationFanoutStepBudget = new(DefaultNotificationFanoutStepBudget)
	defaults.NotificationFanoutLease = new(DefaultNotificationFanoutLease)
	defaults.NotificationFanoutBatchRecipients = new(DefaultNotificationFanoutBatchRecipients)
	defaults.NotificationFanoutMaintenanceInterval = new(DefaultNotificationFanoutMaintenanceInterval)
	defaults.NotificationFanoutPollInterval = new(DefaultNotificationFanoutPollInterval)
	defaults.NotificationFanoutMaxAttempts = new(DefaultNotificationFanoutMaxAttempts)
	defaults.NotificationChatRetirementPageSize = new(DefaultNotificationChatRetirementPageSize)
	defaults.NotificationRetirementMaxRecipients = new(DefaultNotificationRetirementMaxRecipients)
	defaults.NotificationSourceResolutionPageSize = new(DefaultNotificationSourceResolutionPageSize)
	defaults.NotificationRetirementClaimCandidates = new(DefaultNotificationRetirementClaimCandidates)
	defaults.NotificationFanoutCompactionRetention = new(DefaultNotificationFanoutCompactionRetention)
	defaults.NotificationFanoutCompactionBatch = new(DefaultNotificationFanoutCompactionBatch)
	defaults.NotificationInlineSnapshotMaxPages = new(DefaultNotificationInlineSnapshotMaxPages)
	defaults.NotificationFanoutBacklogCountLimit = new(DefaultNotificationFanoutBacklogCountLimit)
	defaults.NotificationDeliveryWorkerCount = new(DefaultNotificationDeliveryWorkerCount)
	defaults.NotificationDeliveryPollInterval = new(DefaultNotificationDeliveryPollInterval)
	defaults.NotificationDeliveryClaimErrorMaxBackoff = new(DefaultNotificationDeliveryClaimErrorMaxBackoff)
	defaults.NotificationWorkerShutdownTimeout = new(DefaultNotificationWorkerShutdownTimeout)
	defaults.NotificationDeliveryClaimTimeout = new(DefaultNotificationDeliveryClaimTimeout)
	defaults.NotificationDeliveryLease = new(DefaultNotificationDeliveryLease)
	defaults.NotificationDeliveryMaxAttempts = new(DefaultNotificationDeliveryMaxAttempts)
	defaults.NotificationDeliveryPollJitterFraction = new(DefaultNotificationDeliveryPollJitterFraction)
	defaults.NotificationDeliveryWorkBudget = new(DefaultNotificationDeliveryWorkBudget)
	defaults.NotificationDeliveryLeaseCompletionMargin = new(DefaultNotificationDeliveryLeaseCompletionMargin)
	defaults.NotificationDeliveryRetryBackoffBase = new(DefaultNotificationDeliveryRetryBackoffBase)
	defaults.NotificationDeliveryRetryBackoffMax = new(DefaultNotificationDeliveryRetryBackoffMax)
	defaults.NotificationDeliveryActiveViewDefer = new(DefaultNotificationDeliveryActiveViewDefer)
	defaults.NotificationDeliveryDisabledDefer = new(DefaultNotificationDeliveryDisabledDefer)
	defaults.NotificationDeliveryTypeUnavailableDefer = new(DefaultNotificationDeliveryTypeUnavailableDefer)
	defaults.NotificationDeliveryRevisionChangedDefer = new(DefaultNotificationDeliveryRevisionChangedDefer)
	defaults.NotificationAlertMinAge = new(DefaultNotificationAlertMinAge)
	defaults.NotificationAlertMaxAge = new(DefaultNotificationAlertMaxAge)
	defaults.NotificationDeliveryCompletionTimeout = new(DefaultNotificationDeliveryCompletionTimeout)
	defaults.NotificationDeliveryCompletionRetryBackoffBase = new(DefaultNotificationDeliveryCompletionRetryBackoffBase)
	defaults.NotificationDeliveryCompletionRetryBackoffMax = new(DefaultNotificationDeliveryCompletionRetryBackoffMax)
	defaults.NotificationFanoutClaimErrorMaxBackoff = new(DefaultNotificationFanoutClaimErrorMaxBackoff)
	defaults.NotificationFanoutCompactionInterval = new(DefaultNotificationFanoutCompactionInterval)
	defaults.NotificationFanoutRetryBackoffBase = new(DefaultNotificationFanoutRetryBackoffBase)
	defaults.NotificationFanoutRetryBackoffMax = new(DefaultNotificationFanoutRetryBackoffMax)
	defaults.NotificationFanoutDeferredRetryDelay = new(DefaultNotificationFanoutDeferredRetryDelay)
	defaults.NotificationFanoutFinishTimeout = new(DefaultNotificationFanoutFinishTimeout)
	defaults.NotificationSourceBatchRows = new(DefaultNotificationSourceBatchRows)
	defaults.NotificationSourceBatchBytes = new(DefaultNotificationSourceBatchBytes)
	defaults.NotificationMaxPayloadBytes = new(DefaultNotificationMaxPayloadBytes)
	defaults.NotificationMaxFallbackTextBytes = new(DefaultNotificationMaxFallbackTextBytes)
	defaults.NotificationMaxMetadataBytes = new(DefaultNotificationMaxMetadataBytes)
	defaults.NotificationEmitMaxTotalBytes = new(DefaultNotificationEmitMaxTotalBytes)
	defaults.NotificationChatRetirementMaxAttempts = new(DefaultNotificationChatRetirementMaxAttempts)
	defaults.NotificationChatRetirementExhaustedDelay = new(DefaultNotificationChatRetirementExhaustedDelay)
	defaults.NotificationChatRetirementRetryBackoffBase = new(DefaultNotificationChatRetirementRetryBackoffBase)
	defaults.NotificationChatRetirementRetryBackoffMax = new(DefaultNotificationChatRetirementRetryBackoffMax)
	defaults.NotificationChatRetirementRecordFailureTimeout = new(DefaultNotificationChatRetirementRecordFailureTimeout)

	// Chat capability defaults
	defaults.ChatSupportReplies = new(false)               // enable when reply hydration exists
	defaults.ChatSupportThreads = new(false)               // enable when threading is implemented
	defaults.ChatSupportMessageEdits = new(false)          // enable when edit API exists
	defaults.ChatSupportMessageDeletes = new(false)        // enable when delete API exists
	defaults.ChatSupportMessageReactions = new(true)       // reactions enabled by default
	defaults.ChatAllowMultipleReactionsPerUser = new(true) // allow multiple different emoji reactions
	defaults.ChatSupportEmbeds = new(true)                 // embeds enabled by default
	defaults.ChatSupportAttachments = new(true)            // attachments enabled by default
	defaults.ChatAllowNativeEmbeds = new(true)             // native embeds enabled by default
	defaults.ChatAllowCustomEmbeds = new(true)             // custom embeds enabled by default
	defaults.ChatSupportInferenceUsage = new(false)        // token usage hidden by default

	// Chat limits defaults
	defaults.ChatMaxChannelNameLength = new(100)
	defaults.ChatMaxChannelDescriptionLength = new(500)
	defaults.ChatMaxMessageLength = new(4000)
	defaults.ChatMaxClientMessageIDLength = new(DefaultChatMaxClientMessageIDLength)
	defaults.ChatMaxEmbedsPerMessage = new(10)
	defaults.ChatMaxEmbedDataBytes = new(65536)
	defaults.ChatMaxMentionsPerMessage = new(10)
	defaults.ChatMaxParticipantsPerGroup = new(100)
	defaults.ChatMaxBatchGetMessages = new(200)
	defaults.ChatMaxListParticipantsBatchChannelIDs = new(50)
	defaults.ChatMaxEmojiLength = new(32)
	defaults.ChatMaxFallbackTextLength = new(500)
	defaults.ChatAgentDelegationReadDefaultLimit = new(DefaultChatAgentDelegationReadDefaultLimit)
	defaults.ChatAgentDelegationReadMaxLimit = new(DefaultChatAgentDelegationReadMaxLimit)
	defaults.ChatAgentDelegationReadMaxBytes = new(DefaultChatAgentDelegationReadMaxBytes)
	defaults.ChatAgentDelegationReadMaxContentChars = new(DefaultChatAgentDelegationReadMaxContentChars)
	defaults.ChatAgentDelegationMaxRunsPerChild = new(DefaultChatAgentDelegationMaxRunsPerChild)
	defaults.ChatForkMaxCopiedMessages = new(DefaultChatForkMaxCopiedMessages)
	defaults.ChatAgentRunMaxActiveDuration = new(5 * time.Minute)
	defaults.ChatAgentRunMaxGenerations = new(12)
	defaults.ChatAgentRunMaxToolCalls = new(32)
	defaults.ChatAgentDelegationStateMaxChildren = new(DefaultChatAgentDelegationStateMaxChildren)
	defaults.ChatAgentDelegationActiveChildrenLimitInteractive = new(32)
	defaults.ChatAgentDelegationActiveChildrenLimitOwnerDriven = new(64)
	defaults.ChatAgentDelegationMetadataValueMaxBytes = new(1024)
	defaults.ChatAgentDelegationMetadataMaxBytes = new(DefaultChatAgentDelegationMetadataMaxBytes)
	defaults.ChatAgentDelegatedChildrenPageMax = new(DefaultChatAgentDelegatedChildrenPageMax)
	defaults.ChatAgentSessionsByChannelMax = new(DefaultChatAgentSessionsByChannelMax)
	defaults.ChatAgentRunRecoveryScanPageSize = new(DefaultChatAgentRunRecoveryScanPageSize)
	defaults.ChatAgentStreamRecoveryMaxAttempts = new(DefaultChatAgentStreamRecoveryMaxAttempts)
	defaults.ChatAgentRunSettleTimeout = new(DefaultChatAgentRunSettleTimeout)
	defaults.ChatAgentWorkflowResultRecoveryFloor = new(DefaultChatAgentWorkflowResultRecoveryFloor)
	defaults.ChatAgentRunDriveTimeout = new(DefaultChatAgentRunDriveTimeout)
	defaults.ChatAgentRunDriveLeaseDuration = new(DefaultChatAgentRunDriveLeaseDuration)
	defaults.ChatAgentRunDriveHeartbeatInterval = new(DefaultChatAgentRunDriveHeartbeatInterval)
	defaults.ChatAgentRunDriveRetryBackoff = new(DefaultChatAgentRunDriveRetryBackoff)
	defaults.ChatAgentRunDriveDBTimeout = new(DefaultChatAgentRunDriveDBTimeout)
	defaults.ChatAgentRunRecoveryScanInterval = new(DefaultChatAgentRunRecoveryScanInterval)
	defaults.ChatAgentRunRecoveryBatchSize = new(DefaultChatAgentRunRecoveryBatchSize)
	defaults.ChatAgentRunRecoveryConcurrency = new(DefaultChatAgentRunRecoveryConcurrency)
	defaults.ChatAgentRunManagedAttemptCapacity = new(DefaultChatAgentRunManagedAttemptCapacity)
	defaults.ChatAgentRunDriveAbandonGrace = new(DefaultChatAgentRunDriveAbandonGrace)
	defaults.ChatAgentRunMaxDriveAbandonments = new(DefaultChatAgentRunMaxDriveAbandonments)
	defaults.ChatAgentRunAbandonedDriveCapacity = new(DefaultChatAgentRunAbandonedDriveCapacity)
	defaults.ChatAgentWorkflowWaitPollInterval = new(DefaultChatAgentWorkflowWaitPollInterval)
	defaults.ChatAgentGraphDriveClaimPollInterval = new(DefaultChatAgentGraphDriveClaimPollInterval)
	defaults.ChatAgentRunStatusMaxLocations = new(DefaultChatAgentRunStatusMaxLocations)
	defaults.ChatAgentRunStatusMaxHistory = new(DefaultChatAgentRunStatusMaxHistory)
	defaults.ChatAgentDelegationReportExcerptRunes = new(DefaultChatAgentDelegationReportExcerptRunes)
	defaults.ChatAgentDelegationTitleMaxRunes = new(DefaultChatAgentDelegationTitleMaxRunes)
	defaults.ChatAgentAnthropicDefaultMaxOutputTokens = new(DefaultChatAgentAnthropicDefaultMaxOutputTokens)
	defaults.ChatAgentMaxPendingInputs = new(DefaultChatAgentMaxPendingInputs)

	// Bot typing indicator defaults
	defaults.BotTypingDelay = new(5 * time.Millisecond)
	defaults.BotTypingInterval = new(3 * time.Second)
	defaults.ChatTypingRefreshWindow = new(DefaultChatTypingRefreshWindow)

	// Chat caching defaults
	defaults.ChatRoleCacheCapacity = new(uint64(10000))
	defaults.ChatRoleCacheTTL = new(5 * time.Minute)

	// Agentic tool-approval lane background-task intervals (Decision 6).
	defaults.ChatApprovalExpiryScanInterval = new(1 * time.Minute)
	defaults.ChatApprovedWorkScanInterval = new(30 * time.Second)
	defaults.ChatToolWorkScanTimeout = new(DefaultChatToolWorkScanTimeout)
	defaults.ChatToolWorkSettleTimeout = new(DefaultChatToolWorkSettleTimeout)
	defaults.ChatToolWorkScanLimit = new(DefaultChatToolWorkScanLimit)
	defaults.ChatToolWorkMaxConcurrency = new(DefaultChatToolWorkMaxConcurrency)
	defaults.ChatToolWorkAttemptCapacity = new(DefaultChatToolWorkAttemptCapacity)
	defaults.ChatToolWorkExecutionLeaseDuration = new(DefaultChatToolWorkExecutionLeaseDuration)
	defaults.ChatToolWorkExecutionTimeout = new(DefaultChatToolWorkExecutionTimeout)
	defaults.ChatToolWorkShutdownGrace = new(DefaultChatToolWorkShutdownGrace)

	// Realtime WebSocket defaults
	defaults.EnableRealtimeWebSocket = new(false) // disabled by default (explicit opt-in)
	defaults.RealtimeWSAllowedOrigins = []string{}
	defaults.RealtimeWSPingInterval = new(defaultRealtimeWSPingInterval)
	defaults.RealtimeWSPongTimeout = new(defaultRealtimeWSPongTimeout)
	defaults.RealtimeWSReadTimeout = new(60 * time.Second)
	defaults.RealtimeWSMaxMessageSize = new(int64(4096))
	defaults.RealtimeWSSendQueueSize = new(64)
	defaults.RealtimeWSMaxConnectionsPerUser = new(8)
	defaults.RealtimeWSMaxSubscriptionsPerConnection = new(256)
	defaults.RealtimeWSCommandAuthBudget = new(defaultRealtimeWSCommandAuthBudget)

	// Rate limiting defaults
	defaults.RateLimitEnabled = new(false) // disabled by default

	// Server configuration defaults
	defaults.ServerPort = new(8080)                        // default port
	defaults.SessionCookieName = new("auth_token")         // standard session cookie
	defaults.SessionCookieDomain = new("")                 // host-only by default
	defaults.SessionCookieSameSite = new("Strict")         // CSRF defense by default
	defaults.SessionCookieSecure = new(true)               // production-safe by default
	defaults.SessionCookieTTL = defaults.SessionValidity   // match session validity by default
	defaults.PathPrefix = new("/api/v1/common")            // Common API endpoints prefix
	defaults.RemoteAddrHeaderName = new("X-Forwarded-For") // standard proxy header
	defaults.FrontendURL = new("")                         // empty by default
	defaults.AllowedFrontendOrigins = []string{}           // no additional origins by default
	defaults.Env = new(EnvironmentDevelopment)             // default environment
	defaults.ServiceName = new("go-common-service")        // default service name

	// Logging defaults
	defaults.LogOutputPaths = []string{"stdout"}      // stdout by default
	defaults.LogErrorOutputPaths = []string{"stderr"} // stderr by default

	// MinIO defaults
	defaults.MinioUseSSL = new(false) // disabled by default

	// Sanitizer password policy defaults
	defaults.SanitizerPasswordRequireUppercase = new(true)                    // enabled by default
	defaults.SanitizerPasswordRequireLowercase = new(true)                    // enabled by default
	defaults.SanitizerPasswordRequireNumber = new(true)                       // enabled by default
	defaults.SanitizerPasswordRequireSpecial = new(true)                      // enabled by default
	defaults.SanitizerPasswordSpecialChars = new(DefaultPasswordSpecialChars) // common special chars

	// Sanitizer email policy defaults
	defaults.SanitizerAllowDisposableEmails = new(false) // disabled by default
	defaults.SanitizerBlockSQLPatterns = new(true)       // enabled by default
	defaults.SanitizerRequireDotInDomain = new(true)     // enabled by default
	defaults.SanitizerUseStrictRFC5322 = new(false)      // disabled by default

	// Sanitizer pattern defaults
	defaults.SanitizerCustomRegexPattern = new("")   // empty by default (disabled)
	defaults.SanitizerBlockedDomains = []string{}    // empty by default
	defaults.SanitizerAllowedDomains = []string{}    // empty by default (allow all)
	defaults.SanitizerDisposableDomains = []string{} // empty by default

	// Organization slug defaults
	defaults.OrganizationSlugMinLength = new(3)   // minimum 1 character
	defaults.OrganizationSlugMaxLength = new(100) // maximum 100 characters

	// API pagination defaults
	defaults.DefaultPaginationLimit = new(DefaultPaginationPageSize)
	defaults.MaxPaginationLimit = new(DefaultMaxPaginationLimit)
	defaults.InferenceAttemptListDefaultLimit = new(DefaultInferenceAttemptListDefaultLimit)
	defaults.InferenceAttemptListMaxLimit = new(DefaultInferenceAttemptListMaxLimit)
	defaults.OrganizationParentOptionsMaxLimit = new(DefaultOrganizationParentOptionsMaxLimit)

	// Organization invitation defaults
	defaults.OrganizationInvitationExpiryDays = new(45 * 24 * time.Hour) // 45 days before pending invitations expire
	defaults.OrganizationAutoAddExpiryDays = new(45 * 24 * time.Hour)    // Align auto-add pending expiry with invitations by default

	// Applications add account-specific providers and bind individually managed credentials.
	defaults.InferenceProviders = []ProviderConfig{OpenAIProvider(), OpenRouterProvider(), AnthropicProvider(), BasetenProvider(), OllamaCloudProvider(), GoogleAIStudioProvider()}
	defaults.InferenceMaxOperationModels = new(DefaultInferenceMaxOperationModels)
	defaults.InferenceMaxOrganizationProviders = new(DefaultInferenceMaxOrganizationProviders)
	defaults.InferencePreviewSessionLimit = new(DefaultInferencePreviewSessionLimit)
	defaults.InferenceRetargetBatchSize = new(DefaultInferenceRetargetBatchSize)
	defaults.InferenceMaxModelSteps = new(DefaultInferenceMaxModelSteps)

	return defaults
}

// ValidateConfig executes its operation in the configuration loading, defaulting, and validation flow.
func (c *Config) ValidateConfig() error {
	if c == nil {
		return fmt.Errorf("config invariant config_root_present violated: config is nil")
	}
	if c.Env != nil && (*c.Env < EnvironmentDevelopment || *c.Env > EnvironmentProduction) {
		return fmt.Errorf("environment must be a known value, got: %d", *c.Env)
	}
	if c.Env != nil && c.Env.IsProduction() && c.UseTokenServiceMock != nil && *c.UseTokenServiceMock {
		return fmt.Errorf(
			"config invariant production_token_service_is_not_mocked violated: use token service mock must be disabled in %s",
			c.Env.String(),
		)
	}
	if err := c.validateLivePersistenceAndAuthPolicy(); err != nil {
		return err
	}
	if err := c.validateOperationalClients(); err != nil {
		return err
	}
	if err := c.validateAuthorizationEnums(); err != nil {
		return err
	}
	if err := c.validateOAuthProtocol(); err != nil {
		return err
	}
	if err := c.validateInferenceRouting(); err != nil {
		return err
	}
	if err := c.validateNotificationInbox(); err != nil {
		return err
	}
	if err := c.validateNotificationRequestLimits(); err != nil {
		return err
	}
	if err := c.validateNotificationFanout(); err != nil {
		return err
	}
	if err := c.validateNotificationDelivery(); err != nil {
		return err
	}
	if err := validateNotificationPolicies(c.NotificationPolicies); err != nil {
		return err
	}

	chatLimits := [...]struct {
		name  string
		value *int
	}{
		{name: "chat max channel name length", value: c.ChatMaxChannelNameLength},
		{name: "chat max channel description length", value: c.ChatMaxChannelDescriptionLength},
		{name: "chat max message length", value: c.ChatMaxMessageLength},
		{name: "chat max embeds per message", value: c.ChatMaxEmbedsPerMessage},
		{name: "chat max embed data bytes", value: c.ChatMaxEmbedDataBytes},
		{name: "chat max mentions per message", value: c.ChatMaxMentionsPerMessage},
		{name: "chat max participants per group", value: c.ChatMaxParticipantsPerGroup},
		{name: "chat max batch get messages", value: c.ChatMaxBatchGetMessages},
		{name: "chat max emoji length", value: c.ChatMaxEmojiLength},
		{name: "chat max fallback text length", value: c.ChatMaxFallbackTextLength},
	}
	for _, limit := range chatLimits {
		if limit.value != nil && *limit.value <= 0 {
			return fmt.Errorf("%s must be positive, got: %d", limit.name, *limit.value)
		}
	}

	if c.ChatMaxListParticipantsBatchChannelIDs != nil && *c.ChatMaxListParticipantsBatchChannelIDs <= 0 {
		return fmt.Errorf(
			"chat max list participants batch channel IDs must be positive, got: %d",
			*c.ChatMaxListParticipantsBatchChannelIDs,
		)
	}
	if err := validateRealtimeWebSocketOrigins(c.RealtimeWSAllowedOrigins); err != nil {
		return err
	}
	if c.RealtimeWSPingInterval != nil && *c.RealtimeWSPingInterval < 0 {
		return fmt.Errorf("realtime websocket ping interval must be non-negative, got: %v", *c.RealtimeWSPingInterval)
	}
	if c.RealtimeWSPongTimeout != nil && *c.RealtimeWSPongTimeout < 0 {
		return fmt.Errorf("realtime websocket pong timeout must be non-negative, got: %v", *c.RealtimeWSPongTimeout)
	}
	if c.RealtimeWSReadTimeout != nil && *c.RealtimeWSReadTimeout < 0 {
		return fmt.Errorf("realtime websocket read timeout must be non-negative, got: %v", *c.RealtimeWSReadTimeout)
	}
	if c.RealtimeWSMaxMessageSize != nil && *c.RealtimeWSMaxMessageSize < 0 {
		return fmt.Errorf("realtime websocket max message size must be non-negative, got: %d", *c.RealtimeWSMaxMessageSize)
	}
	if c.RealtimeWSPingInterval != nil && *c.RealtimeWSPingInterval > 0 &&
		c.RealtimeWSPongTimeout != nil && *c.RealtimeWSPongTimeout <= 0 {
		return fmt.Errorf("realtime websocket pong timeout must be positive when pinging is enabled, got: %v", *c.RealtimeWSPongTimeout)
	}
	if c.RealtimeWSSendQueueSize != nil && *c.RealtimeWSSendQueueSize < 0 {
		return fmt.Errorf("realtime websocket send queue size must be non-negative, got: %d", *c.RealtimeWSSendQueueSize)
	}
	if c.RealtimeWSMaxConnectionsPerUser != nil && *c.RealtimeWSMaxConnectionsPerUser < 0 {
		return fmt.Errorf("realtime websocket max connections per user must be non-negative, got: %d", *c.RealtimeWSMaxConnectionsPerUser)
	}
	if c.RealtimeWSMaxSubscriptionsPerConnection != nil && *c.RealtimeWSMaxSubscriptionsPerConnection < 0 {
		return fmt.Errorf("realtime websocket max subscriptions per connection must be non-negative, got: %d", *c.RealtimeWSMaxSubscriptionsPerConnection)
	}
	if c.RealtimeWSCommandAuthBudget != nil && *c.RealtimeWSCommandAuthBudget < 0 {
		return fmt.Errorf("realtime websocket command auth budget must be non-negative, got: %v", *c.RealtimeWSCommandAuthBudget)
	}
	if err := validateRealtimeCommandAuthBudget(
		c.RealtimeWSCommandAuthBudget,
		c.RealtimeWSPingInterval,
		c.RealtimeWSPongTimeout,
	); err != nil {
		return err
	}
	if _, err := requiredDuration("chat typing refresh window", c.ChatTypingRefreshWindow, MinChatTypingRefreshWindow, MaxChatTypingRefreshWindow); err != nil {
		return err
	}
	if c.ChatAgentRunMaxActiveDuration != nil && *c.ChatAgentRunMaxActiveDuration < time.Millisecond {
		return fmt.Errorf("chat agent run max active duration must be at least 1ms")
	}
	if c.ChatAgentRunMaxGenerations != nil && *c.ChatAgentRunMaxGenerations <= 0 {
		return fmt.Errorf("chat agent run max generations must be positive")
	}
	if c.ChatAgentRunMaxToolCalls != nil && *c.ChatAgentRunMaxToolCalls <= 0 {
		return fmt.Errorf("chat agent run max tool calls must be positive")
	}
	// Every agent bound is required: consumers read the sampled value as-is.
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"chat agent delegation state max children", c.ChatAgentDelegationStateMaxChildren, 1, MaxChatAgentDelegationStateMaxChildren},
		{"chat agent delegation active children limit interactive", c.ChatAgentDelegationActiveChildrenLimitInteractive, 1, MaxChatAgentDelegationStateMaxChildren},
		{"chat agent delegation active children limit owner driven", c.ChatAgentDelegationActiveChildrenLimitOwnerDriven, 1, MaxChatAgentDelegationStateMaxChildren},
		{"chat agent delegation metadata value max bytes", c.ChatAgentDelegationMetadataValueMaxBytes, MinChatAgentDelegationMetadataValueMaxBytes, MaxChatAgentDelegationMetadataValueMaxBytes},
		{"chat agent delegation metadata max bytes", c.ChatAgentDelegationMetadataMaxBytes, MinChatAgentDelegationMetadataMaxBytes, MaxChatAgentDelegationMetadataMaxBytes},
		{"chat agent delegated children page max", c.ChatAgentDelegatedChildrenPageMax, 1, MaxChatAgentDelegatedChildrenPageMax},
		{"chat agent sessions by channel max", c.ChatAgentSessionsByChannelMax, 1, MaxChatAgentSessionsByChannel},
		{"chat agent run recovery scan page size", c.ChatAgentRunRecoveryScanPageSize, 1, MaxChatAgentRunRecoveryScanPageSize},
		{"chat agent stream recovery max attempts", c.ChatAgentStreamRecoveryMaxAttempts, 1, MaxChatAgentStreamRecoveryMaxAttempts},
		{"chat agent run status max locations", c.ChatAgentRunStatusMaxLocations, MinChatAgentRunStatusMaxLocations, MaxChatAgentRunStatusMaxLocations},
		{"chat agent run status max history", c.ChatAgentRunStatusMaxHistory, MinChatAgentRunStatusMaxHistory, MaxChatAgentRunStatusMaxHistory},
		{"chat agent delegation report excerpt runes", c.ChatAgentDelegationReportExcerptRunes, MinChatAgentDelegationReportExcerptRunes, MaxChatAgentDelegationReportExcerptRunes},
		{"chat agent delegation title max runes", c.ChatAgentDelegationTitleMaxRunes, 1, MaxChatAgentDelegationTitleRunes},
		{"chat agent anthropic default max output tokens", c.ChatAgentAnthropicDefaultMaxOutputTokens, 1, MaxChatAgentAnthropicDefaultMaxOutputTokens},
		{"chat agent max pending inputs", c.ChatAgentMaxPendingInputs, 1, MaxChatAgentMaxPendingInputs},
		{"chat max client message id length", c.ChatMaxClientMessageIDLength, 1, MaxChatMaxClientMessageIDLength},
		{"chat agent delegation read default limit", c.ChatAgentDelegationReadDefaultLimit, MinChatAgentDelegationReadDefaultLimit, MaxChatAgentDelegationReadDefaultLimit},
		{"chat agent delegation read max limit", c.ChatAgentDelegationReadMaxLimit, MinChatAgentDelegationReadMaxLimit, MaxChatAgentDelegationReadMaxLimit},
		{"chat agent delegation read max bytes", c.ChatAgentDelegationReadMaxBytes, MinChatAgentDelegationReadMaxBytes, MaxChatAgentDelegationReadMaxBytes},
		{"chat agent delegation read max content chars", c.ChatAgentDelegationReadMaxContentChars, MinChatAgentDelegationReadMaxContentChars, MaxChatAgentDelegationReadMaxContentChars},
		{"chat agent delegation max runs per child", c.ChatAgentDelegationMaxRunsPerChild, MinChatAgentDelegationMaxRunsPerChild, MaxChatAgentDelegationMaxRunsPerChild},
		{"chat fork max copied messages", c.ChatForkMaxCopiedMessages, MinChatForkMaxCopiedMessages, MaxChatForkMaxCopiedMessages},
		{"chat agent run recovery batch size", c.ChatAgentRunRecoveryBatchSize, 1, MaxChatAgentRunRecoveryBatchSize},
		{"chat agent run recovery concurrency", c.ChatAgentRunRecoveryConcurrency, 1, MaxChatAgentRunRecoveryConcurrency},
		{"chat agent run managed attempt capacity", c.ChatAgentRunManagedAttemptCapacity, 1, MaxChatAgentRunManagedAttemptCapacity},
		{"chat agent run max drive abandonments", c.ChatAgentRunMaxDriveAbandonments, 0, MaxChatAgentRunMaxDriveAbandonments},
		{"chat agent run abandoned drive capacity", c.ChatAgentRunAbandonedDriveCapacity, 1, MaxChatAgentRunAbandonedDriveCapacity},
		{"chat tool work scan limit", c.ChatToolWorkScanLimit, 1, MaxChatToolWorkScanLimit},
		{"chat tool work max concurrency", c.ChatToolWorkMaxConcurrency, 1, MaxChatToolWorkMaxConcurrency},
		{"chat tool work attempt capacity", c.ChatToolWorkAttemptCapacity, 1, MaxChatToolWorkAttemptCapacity},
	}); err != nil {
		return err
	}
	if err := requiredDurations([]struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"chat agent run settle timeout", c.ChatAgentRunSettleTimeout, MinChatAgentRunSettleTimeout, MaxChatAgentRunSettleTimeout},
		{"chat agent workflow result recovery floor", c.ChatAgentWorkflowResultRecoveryFloor, MinChatAgentWorkflowResultRecoveryFloor, MaxChatAgentWorkflowResultRecoveryFloor},
		{"chat agent run drive timeout", c.ChatAgentRunDriveTimeout, MinChatAgentRunDriveTimeout, MaxChatAgentRunDriveTimeout},
		{"chat agent run drive lease duration", c.ChatAgentRunDriveLeaseDuration, MinChatAgentRunDriveLeaseDuration, MaxChatAgentRunDriveLeaseDuration},
		{"chat agent run drive heartbeat interval", c.ChatAgentRunDriveHeartbeatInterval, MinChatAgentRunDriveHeartbeatInterval, MaxChatAgentRunDriveHeartbeatInterval},
		{"chat agent run drive retry backoff", c.ChatAgentRunDriveRetryBackoff, MinChatAgentRunDriveRetryBackoff, MaxChatAgentRunDriveRetryBackoff},
		{"chat agent run drive db timeout", c.ChatAgentRunDriveDBTimeout, MinChatAgentRunDriveDBTimeout, MaxChatAgentRunDriveDBTimeout},
		{"chat agent run recovery scan interval", c.ChatAgentRunRecoveryScanInterval, MinChatAgentRunRecoveryScanInterval, MaxChatAgentRunRecoveryScanInterval},
		{"chat agent run drive abandon grace", c.ChatAgentRunDriveAbandonGrace, MinChatAgentRunDriveAbandonGrace, MaxChatAgentRunDriveAbandonGrace},
		{"chat agent workflow wait poll interval", c.ChatAgentWorkflowWaitPollInterval, MinChatAgentWorkflowWaitPollInterval, MaxChatAgentWorkflowWaitPollInterval},
		{"chat agent graph drive claim poll interval", c.ChatAgentGraphDriveClaimPollInterval, MinChatAgentGraphDriveClaimPollInterval, MaxChatAgentGraphDriveClaimPollInterval},
		{"chat tool work scan timeout", c.ChatToolWorkScanTimeout, MinChatToolWorkScanTimeout, MaxChatToolWorkScanTimeout},
		{"chat tool work settle timeout", c.ChatToolWorkSettleTimeout, MinChatToolWorkSettleTimeout, MaxChatToolWorkSettleTimeout},
		{"chat tool work execution lease duration", c.ChatToolWorkExecutionLeaseDuration, MinChatToolWorkExecutionLeaseDuration, MaxChatToolWorkExecutionLeaseDuration},
		{"chat tool work execution timeout", c.ChatToolWorkExecutionTimeout, MinChatToolWorkExecutionTimeout, MaxChatToolWorkExecutionTimeout},
		{"chat tool work shutdown grace", c.ChatToolWorkShutdownGrace, MinChatToolWorkShutdownGrace, MaxChatToolWorkShutdownGrace},
	}); err != nil {
		return err
	}
	// A renewal starts one heartbeat after the previous one and may take up to the database
	// timeout, so the lease must outlast both or a healthy drive loses it between beats.
	if *c.ChatAgentRunDriveHeartbeatInterval+*c.ChatAgentRunDriveDBTimeout >= *c.ChatAgentRunDriveLeaseDuration {
		return fmt.Errorf("chat agent run drive heartbeat interval (%s) plus drive db timeout (%s) must be less than drive lease duration (%s)",
			*c.ChatAgentRunDriveHeartbeatInterval, *c.ChatAgentRunDriveDBTimeout, *c.ChatAgentRunDriveLeaseDuration)
	}
	if *c.ChatToolWorkAttemptCapacity < *c.ChatToolWorkMaxConcurrency {
		return fmt.Errorf("chat tool work attempt capacity (%d) cannot be less than chat tool work max concurrency (%d)",
			*c.ChatToolWorkAttemptCapacity, *c.ChatToolWorkMaxConcurrency)
	}
	// A timed-out execution settles its claim (release or result commit) within the settle timeout,
	// and must do so before the claim lease lets another worker reclaim the call.
	if *c.ChatToolWorkExecutionTimeout+*c.ChatToolWorkSettleTimeout >= *c.ChatToolWorkExecutionLeaseDuration {
		return fmt.Errorf("chat tool work execution timeout (%s) plus settle timeout (%s) must be less than execution lease duration (%s)",
			*c.ChatToolWorkExecutionTimeout, *c.ChatToolWorkSettleTimeout, *c.ChatToolWorkExecutionLeaseDuration)
	}
	if *c.ChatAgentDelegationStateMaxChildren < max(*c.ChatAgentDelegationActiveChildrenLimitInteractive, *c.ChatAgentDelegationActiveChildrenLimitOwnerDriven) {
		return fmt.Errorf("chat agent delegation state max children (%d) must be at least both active children limits (interactive %d, owner driven %d)",
			*c.ChatAgentDelegationStateMaxChildren, *c.ChatAgentDelegationActiveChildrenLimitInteractive, *c.ChatAgentDelegationActiveChildrenLimitOwnerDriven)
	}
	if *c.ChatAgentDelegationMetadataMaxBytes < *c.ChatAgentDelegationMetadataValueMaxBytes {
		return fmt.Errorf("chat agent delegation metadata max bytes (%d) cannot be less than chat agent delegation metadata value max bytes (%d)",
			*c.ChatAgentDelegationMetadataMaxBytes, *c.ChatAgentDelegationMetadataValueMaxBytes)
	}
	// Every delegated child stores its title as metadata, so the caps must fit a max-length title:
	// the value cap counts raw UTF-8 bytes, the total counts the JSON encoding.
	titleRunes := *c.ChatAgentDelegationTitleMaxRunes
	if valueBytes := utf8.UTFMax * titleRunes; *c.ChatAgentDelegationMetadataValueMaxBytes < valueBytes {
		return fmt.Errorf("chat agent delegation metadata value max bytes (%d) must be at least %d (%d bytes per rune of chat agent delegation title max runes %d)",
			*c.ChatAgentDelegationMetadataValueMaxBytes, valueBytes, utf8.UTFMax, titleRunes)
	}
	if encodedBytes := delegationTitleMetadataOverheadBytes + jsonEscapedRuneMaxBytes*titleRunes; *c.ChatAgentDelegationMetadataMaxBytes < encodedBytes {
		return fmt.Errorf("chat agent delegation metadata max bytes (%d) must be at least %d (%d bytes plus %d per rune of chat agent delegation title max runes %d)",
			*c.ChatAgentDelegationMetadataMaxBytes, encodedBytes, delegationTitleMetadataOverheadBytes, jsonEscapedRuneMaxBytes, titleRunes)
	}
	if *c.ChatAgentDelegationReadDefaultLimit > *c.ChatAgentDelegationReadMaxLimit {
		return fmt.Errorf("chat agent delegation read default limit (%d) cannot exceed chat agent delegation read max limit (%d)",
			*c.ChatAgentDelegationReadDefaultLimit, *c.ChatAgentDelegationReadMaxLimit)
	}

	// Validate database connection pool settings
	if c.DBMaxIdleConns != nil && c.DBMaxOpenConns != nil && *c.DBMaxOpenConns > 0 && *c.DBMaxIdleConns > *c.DBMaxOpenConns {
		return fmt.Errorf("DBMaxIdleConns (%d) cannot be greater than DBMaxOpenConns (%d)",
			*c.DBMaxIdleConns, *c.DBMaxOpenConns)
	}

	// Validate sanitizer length constraints. Zero remains meaningful for minimum
	// lengths. Maximums are consumed directly and must stay positive.
	nonNegativeSanitizerLengths := [...]struct {
		name  string
		value *int
	}{
		{name: "password min length", value: c.SanitizerPasswordMinLength},
		{name: "username min length", value: c.SanitizerUsernameMinLength},
		{name: "verification code min length", value: c.SanitizerVerificationMinLength},
		{name: "real name min length", value: c.SanitizerRealNameMinLength},
		{name: "contact name min length", value: c.SanitizerContactMinNameLength},
		{name: "contact subject min length", value: c.SanitizerContactMinSubjectLength},
		{name: "contact message min length", value: c.SanitizerContactMinMessageLength},
	}
	for _, length := range nonNegativeSanitizerLengths {
		if length.value != nil && *length.value < 0 {
			return fmt.Errorf("sanitizer %s must be non-negative, got: %d", length.name, *length.value)
		}
	}
	positiveSanitizerLengths := [...]struct {
		name  string
		value *int
	}{
		{name: "password max length", value: c.SanitizerPasswordMaxLength},
		{name: "username max length", value: c.SanitizerUsernameMaxLength},
		{name: "email max length", value: c.SanitizerMaxEmailLength},
		{name: "verification code max length", value: c.SanitizerVerificationMaxLength},
		{name: "real name max length", value: c.SanitizerRealNameMaxLength},
		{name: "contact name max length", value: c.SanitizerContactMaxNameLength},
		{name: "contact subject max length", value: c.SanitizerContactMaxSubjectLength},
		{name: "contact message max length", value: c.SanitizerContactMaxMessageLength},
	}
	if c.SanitizerPasswordSpecialChars == nil || *c.SanitizerPasswordSpecialChars == "" {
		return fmt.Errorf("sanitizer password special chars is required")
	}
	// Diagnostic previews and the RFC email part limits are read as-is per request.
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"sanitizer log input max length", c.SanitizerLogInputMaxLength, 1, MaxSanitizerLogInputLength},
		{"sanitizer email local part max length", c.SanitizerMaxLocalPartLength, 1, 0},
		{"sanitizer email domain max length", c.SanitizerMaxDomainLength, 1, 0},
	}); err != nil {
		return err
	}
	for _, length := range positiveSanitizerLengths {
		if length.value != nil && *length.value <= 0 {
			return fmt.Errorf("sanitizer %s must be positive, got: %d", length.name, *length.value)
		}
	}
	for _, domains := range [...]struct {
		name   string
		values []string
	}{
		{name: "SanitizerBlockedDomains", values: c.SanitizerBlockedDomains},
		{name: "SanitizerAllowedDomains", values: c.SanitizerAllowedDomains},
		{name: "SanitizerDisposableDomains", values: c.SanitizerDisposableDomains},
	} {
		if err := validateSanitizerDomains(domains.name, domains.values); err != nil {
			return err
		}
	}
	if c.SanitizerPasswordMinLength != nil && c.SanitizerPasswordMaxLength != nil && *c.SanitizerPasswordMinLength > *c.SanitizerPasswordMaxLength {
		return fmt.Errorf("password min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerPasswordMinLength, *c.SanitizerPasswordMaxLength)
	}
	if c.SanitizerUsernameMinLength != nil && c.SanitizerUsernameMaxLength != nil && *c.SanitizerUsernameMinLength > *c.SanitizerUsernameMaxLength {
		return fmt.Errorf("username min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerUsernameMinLength, *c.SanitizerUsernameMaxLength)
	}
	if c.SanitizerVerificationMinLength != nil && c.SanitizerVerificationMaxLength != nil && *c.SanitizerVerificationMinLength > *c.SanitizerVerificationMaxLength {
		return fmt.Errorf("verification code min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerVerificationMinLength, *c.SanitizerVerificationMaxLength)
	}
	if c.SanitizerCustomRegexPattern != nil && *c.SanitizerCustomRegexPattern != "" {
		if _, err := regexp.Compile(*c.SanitizerCustomRegexPattern); err != nil {
			return fmt.Errorf("sanitizer custom regex pattern is invalid: %w", err)
		}
	}

	positiveDurations := [...]struct {
		name  string
		value *time.Duration
	}{
		{name: "verification cooldown", value: c.VerificationCooldown},
		{name: "phone verification OTP validity", value: c.PhoneVerificationOTPValidity},
		{name: "phone verification cooldown", value: c.PhoneVerificationCooldown},
	}
	for _, duration := range positiveDurations {
		if duration.value != nil && *duration.value <= 0 {
			return fmt.Errorf("%s must be positive, got: %v", duration.name, *duration.value)
		}
	}
	if c.OTPIssuer != nil && strings.TrimSpace(*c.OTPIssuer) == "" {
		return fmt.Errorf("OTP issuer cannot be blank")
	}
	if c.PhoneNumberDefaultRegion != nil {
		if err := validatePhoneNumberDefaultRegion(*c.PhoneNumberDefaultRegion); err != nil {
			return err
		}
	}

	// Validate contact form constraints
	if c.SanitizerContactMinNameLength != nil && c.SanitizerContactMaxNameLength != nil && *c.SanitizerContactMinNameLength > *c.SanitizerContactMaxNameLength {
		return fmt.Errorf("contact name min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerContactMinNameLength, *c.SanitizerContactMaxNameLength)
	}
	if c.SanitizerContactMinSubjectLength != nil && c.SanitizerContactMaxSubjectLength != nil && *c.SanitizerContactMinSubjectLength > *c.SanitizerContactMaxSubjectLength {
		return fmt.Errorf("contact subject min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerContactMinSubjectLength, *c.SanitizerContactMaxSubjectLength)
	}
	if c.SanitizerContactMinMessageLength != nil && c.SanitizerContactMaxMessageLength != nil && *c.SanitizerContactMinMessageLength > *c.SanitizerContactMaxMessageLength {
		return fmt.Errorf("contact message min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerContactMinMessageLength, *c.SanitizerContactMaxMessageLength)
	}

	// Validate real name constraints
	if c.SanitizerRealNameMinLength != nil && c.SanitizerRealNameMaxLength != nil && *c.SanitizerRealNameMinLength > *c.SanitizerRealNameMaxLength {
		return fmt.Errorf("real name min length (%d) cannot be greater than max length (%d)",
			*c.SanitizerRealNameMinLength, *c.SanitizerRealNameMaxLength)
	}

	// Validate OAuth signup configuration
	if c.OAuthSignupMode != nil {
		switch *c.OAuthSignupMode {
		case OAuthSignupModeLegacy, OAuthSignupModeDisabled, OAuthSignupModePrefixed, OAuthSignupModeEmailLocalPart:
		default:
			return fmt.Errorf("oauth signup mode must be one of %q, %q, %q, or %q", OAuthSignupModeLegacy, OAuthSignupModeDisabled, OAuthSignupModePrefixed, OAuthSignupModeEmailLocalPart)
		}
	}
	// The API layer issues and reads the session cookie under the configured name
	// verbatim, so a name that needs trimming would never match its own cookie.
	if c.SessionCookieName == nil {
		return fmt.Errorf("session cookie name is required")
	}
	if name := *c.SessionCookieName; name == "" || strings.ContainsFunc(name, unicode.IsSpace) {
		return fmt.Errorf("session cookie name must be non-empty and contain no whitespace, got: %q", name)
	}
	if c.SessionCookieSameSite != nil {
		switch strings.ToLower(strings.TrimSpace(*c.SessionCookieSameSite)) {
		case "strict", "lax", "none":
		default:
			return fmt.Errorf("session cookie same_site must be one of %q, %q, or %q", "Strict", "Lax", "None")
		}
	}
	if c.SessionCookieTTL != nil && *c.SessionCookieTTL <= 0 {
		return fmt.Errorf("session cookie ttl must be positive")
	}

	// Permission relationships must hold for every admitted generation. These
	// fields are hot-reloadable, so startup-only permission construction is too
	// late to protect an active process from an incoherent candidate.
	if c.SiteMinUserTypeUserDeletion != nil && c.SiteMinUserTypeUserModification != nil &&
		*c.SiteMinUserTypeUserDeletion < *c.SiteMinUserTypeUserModification {
		return fmt.Errorf("user deletion permission (%s) should require at least as much privilege as user modification (%s)",
			*c.SiteMinUserTypeUserDeletion, *c.SiteMinUserTypeUserModification)
	}
	if c.SiteMinUserTypeUserTypeChange != nil && c.SiteMinUserTypeUserModification != nil &&
		*c.SiteMinUserTypeUserTypeChange < *c.SiteMinUserTypeUserModification {
		return fmt.Errorf("user type change permission (%s) should require at least as much privilege as user modification (%s)",
			*c.SiteMinUserTypeUserTypeChange, *c.SiteMinUserTypeUserModification)
	}
	if c.SiteMinUserTypeInferenceAdministration != nil && c.SiteMinUserTypeInferenceViewing != nil &&
		*c.SiteMinUserTypeInferenceAdministration < *c.SiteMinUserTypeInferenceViewing {
		return fmt.Errorf("inference administration permission (%s) should require at least as much privilege as inference viewing (%s)",
			*c.SiteMinUserTypeInferenceAdministration, *c.SiteMinUserTypeInferenceViewing)
	}
	validUserIdentifier := func(value proto_models.UserIdentifierType) bool {
		_, ok := proto_models.UserIdentifierType_name[int32(value)]
		return ok
	}
	if c.PublicPrimaryUserIdentifier != nil &&
		(*c.PublicPrimaryUserIdentifier == proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_UNSPECIFIED ||
			!validUserIdentifier(*c.PublicPrimaryUserIdentifier)) {
		return fmt.Errorf("public primary user identifier must be a specified UserIdentifierType, got: %d", *c.PublicPrimaryUserIdentifier)
	}
	if c.PublicSecondaryUserIdentifier != nil && !validUserIdentifier(*c.PublicSecondaryUserIdentifier) {
		return fmt.Errorf("public secondary user identifier must be a valid UserIdentifierType, got: %d", *c.PublicSecondaryUserIdentifier)
	}

	// Validate session refresh configuration (only meaningful when the feature is enabled)
	if c.EnableSessionRefresh != nil && *c.EnableSessionRefresh {
		soft, hard := float64(0), float64(1)
		if c.SessionRefreshSoftThreshold != nil {
			soft = *c.SessionRefreshSoftThreshold
		}
		if c.SessionRefreshHardThreshold != nil {
			hard = *c.SessionRefreshHardThreshold
		}
		if soft < 0 || soft >= hard || hard > 1 {
			return fmt.Errorf("session refresh thresholds must satisfy 0 <= soft (%v) < hard (%v) <= 1", soft, hard)
		}
		if c.SessionRefreshRampExponent != nil && *c.SessionRefreshRampExponent <= 0 {
			return fmt.Errorf("session refresh ramp exponent must be positive")
		}
		if c.SessionRefreshRevocationGrace != nil && *c.SessionRefreshRevocationGrace <= 0 {
			return fmt.Errorf("session refresh revocation grace must be positive")
		}
	}
	if c.OAuthDefaultUsernamePrefix != nil {
		prefix := strings.TrimSpace(*c.OAuthDefaultUsernamePrefix)
		if prefix == "" {
			if *c.OAuthSignupMode == OAuthSignupModePrefixed {
				return fmt.Errorf("oauth default username prefix cannot be empty when oauth signup mode is %q", OAuthSignupModePrefixed)
			}
		} else if !isInvalidUsernamePrefix(prefix) {
			return fmt.Errorf("oauth default username prefix must be invalid under user-supplied username rules")
		}
	}

	// Validate reasonable bounds on cache sizes
	if c.AuthTokenCacheCapacity != nil && *c.AuthTokenCacheCapacity == 0 {
		return fmt.Errorf("auth token cache capacity cannot be zero")
	}
	// Zero user-cache capacity is supported by the auth consumer: session
	// identities remain cached while application user projections stay fresh.
	if c.OrganizationRoleCacheCapacity != nil && *c.OrganizationRoleCacheCapacity == 0 {
		return fmt.Errorf("organization role cache capacity cannot be zero")
	}
	if c.RateLimitCacheCapacity != nil && *c.RateLimitCacheCapacity == 0 {
		return fmt.Errorf("rate limit cache capacity cannot be zero")
	}

	// Validate unverified-user cleanup configuration when enabled
	if c.UnverifiedUserCleanupInterval != nil && *c.UnverifiedUserCleanupInterval < 0 {
		return fmt.Errorf("unverified user cleanup interval cannot be negative, got: %v", *c.UnverifiedUserCleanupInterval)
	}
	if c.UnverifiedUserCleanupInterval != nil && *c.UnverifiedUserCleanupInterval > 0 {
		if c.VerificationTokenValidity != nil && *c.VerificationTokenValidity <= 0 {
			return fmt.Errorf("verification token validity must be positive when unverified user cleanup is enabled, got: %v", *c.VerificationTokenValidity)
		}
		if c.UnverifiedUserCleanupBatchSize != nil && *c.UnverifiedUserCleanupBatchSize <= 0 {
			return fmt.Errorf("unverified user cleanup batch size must be positive when cleanup is enabled, got: %d", *c.UnverifiedUserCleanupBatchSize)
		}
		if c.UnverifiedUserCleanupTimeout != nil && *c.UnverifiedUserCleanupTimeout <= 0 {
			return fmt.Errorf("unverified user cleanup timeout must be positive when cleanup is enabled, got: %v", *c.UnverifiedUserCleanupTimeout)
		}
	}

	// Validate attachment sizes
	if c.AttachmentUploadURLExpiry != nil && *c.AttachmentUploadURLExpiry <= 0 {
		return fmt.Errorf("attachment upload URL expiry must be positive, got: %v", *c.AttachmentUploadURLExpiry)
	}
	// The download expiry is the presign validity every download reads as-is;
	// SigV4 caps a presign at 7 days.
	if _, err := requiredDuration("attachment download URL expiry", c.AttachmentDownloadURLExpiry, time.Nanosecond, 7*24*time.Hour); err != nil {
		return err
	}
	if c.AttachmentPresignQuantum != nil && *c.AttachmentPresignQuantum < 0 {
		return fmt.Errorf("attachment presign quantum cannot be negative, got: %v", *c.AttachmentPresignQuantum)
	}
	if c.AttachmentPresignAccessKey != nil && *c.AttachmentPresignAccessKey != "" && c.AttachmentPresignSecretKey.StringValue() == "" {
		return fmt.Errorf("attachment presign access key is set but attachment_presign_secret_key is not resolved; the dedicated presign credential requires both")
	}
	if (c.AttachmentPresignAccessKey == nil || *c.AttachmentPresignAccessKey == "") && c.AttachmentPresignSecretKey.StringValue() != "" {
		return fmt.Errorf("attachment_presign_secret_key is resolved but attachment presign access key is empty; the dedicated presign credential requires both")
	}
	if c.AttachmentDefaultMaxSize != nil && *c.AttachmentDefaultMaxSize <= 0 {
		return fmt.Errorf("attachment default max size must be positive, got: %d", *c.AttachmentDefaultMaxSize)
	}
	if c.AttachmentXMLMaxTokenBytes == nil {
		return fmt.Errorf("attachment XML max token bytes is required")
	}
	if *c.AttachmentXMLMaxTokenBytes < MinAttachmentXMLMaxTokenBytes || *c.AttachmentXMLMaxTokenBytes > MaxAttachmentXMLMaxTokenBytes {
		return fmt.Errorf("attachment XML max token bytes must be between %d and %d, got: %d", MinAttachmentXMLMaxTokenBytes, MaxAttachmentXMLMaxTokenBytes, *c.AttachmentXMLMaxTokenBytes)
	}
	if err := requiredDurations([]struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"attachment internal reclaim grace", c.AttachmentInternalReclaimGrace, MinAttachmentInternalReclaimGrace, MaxAttachmentInternalReclaimGrace},
		{"attachment temporary object cleanup timeout", c.AttachmentTemporaryObjectCleanupTimeout, MinAttachmentTemporaryObjectCleanupTimeout, MaxAttachmentTemporaryObjectCleanupTimeout},
		{"attachment temporary upload prune age", c.AttachmentTemporaryUploadPruneAge, MinAttachmentTemporaryUploadPruneAge, MaxAttachmentTemporaryUploadPruneAge},
	}); err != nil {
		return err
	}
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"attachment internal reclaim batch size", c.AttachmentInternalReclaimBatchSize, MinAttachmentInternalReclaimBatchSize, MaxAttachmentInternalReclaimBatchSize},
		{"attachment temporary object sweep batch size", c.AttachmentTemporaryObjectSweepBatchSize, MinAttachmentTemporaryObjectSweepBatchSize, MaxAttachmentTemporaryObjectSweepBatchSize},
	}); err != nil {
		return err
	}
	if c.StreamingHashChunkSize != nil && *c.StreamingHashChunkSize <= 0 {
		return fmt.Errorf("streaming hash chunk size must be positive, got: %d", *c.StreamingHashChunkSize)
	}
	if c.ObjectStorageStreamPartSize == nil {
		return fmt.Errorf("object storage stream part size is required")
	}
	if *c.ObjectStorageStreamPartSize < MinObjectStorageStreamPartSize || *c.ObjectStorageStreamPartSize > MaxObjectStorageStreamPartSize {
		return fmt.Errorf("object storage stream part size must be between %d and %d, got: %d", MinObjectStorageStreamPartSize, MaxObjectStorageStreamPartSize, *c.ObjectStorageStreamPartSize)
	}

	// Validate attachment cleanup configuration
	if c.AttachmentProcessingTimeout != nil && *c.AttachmentProcessingTimeout <= 0 {
		return fmt.Errorf("attachment processing timeout must be positive, got: %v", *c.AttachmentProcessingTimeout)
	}
	if c.AttachmentCleanupInterval != nil && *c.AttachmentCleanupInterval <= 0 {
		return fmt.Errorf("attachment cleanup interval must be positive, got: %v", *c.AttachmentCleanupInterval)
	}
	if c.AttachmentCleanupInterval != nil && c.AttachmentProcessingTimeout != nil && *c.AttachmentCleanupInterval >= *c.AttachmentProcessingTimeout {
		return fmt.Errorf("attachment cleanup interval (%v) should be less than processing timeout (%v)",
			*c.AttachmentCleanupInterval, *c.AttachmentProcessingTimeout)
	}
	if c.AttachmentPendingUploadTimeout != nil && *c.AttachmentPendingUploadTimeout <= 0 {
		return fmt.Errorf("attachment pending upload timeout must be positive, got: %v", *c.AttachmentPendingUploadTimeout)
	}
	if c.AttachmentLockTTL != nil && *c.AttachmentLockTTL <= 0 {
		return fmt.Errorf("attachment lock TTL must be positive, got: %v", *c.AttachmentLockTTL)
	}
	if c.AttachmentLockCapacity != nil && *c.AttachmentLockCapacity == 0 {
		return fmt.Errorf("attachment lock capacity must be positive, got: %d", *c.AttachmentLockCapacity)
	}

	// Validate rate limiting
	if c.RateLimitEnabled != nil && *c.RateLimitEnabled {
		validateRateLimitAttempts := func(name string, value *int) error {
			if value != nil && *value <= 0 {
				return fmt.Errorf("%s must be positive, got: %d", name, *value)
			}
			return nil
		}
		validateRateLimitDuration := func(name string, value *time.Duration) error {
			if value != nil && *value <= 0 {
				return fmt.Errorf("%s must be positive, got: %v", name, *value)
			}
			return nil
		}

		if err := validateRateLimitAttempts("login max attempts", c.LoginMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("login window duration", c.LoginWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("register max attempts", c.RegisterMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("register window duration", c.RegisterWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("password reset max attempts", c.PasswordResetMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("password reset window duration", c.PasswordResetWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("verify phone OTP max attempts", c.VerifyPhoneOTPMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("verify phone OTP window duration", c.VerifyPhoneOTPWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("contact max attempts", c.ContactMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("contact window duration", c.ContactWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("career application max attempts", c.CareerApplicationMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("career application window duration", c.CareerApplicationWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("career job lookup max attempts", c.CareerJobLookupMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("career job lookup window duration", c.CareerJobLookupWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("public chat read max attempts", c.PublicChatReadMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("public chat read window duration", c.PublicChatReadWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("organization slug availability max attempts", c.OrganizationSlugAvailabilityMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("organization slug availability window duration", c.OrganizationSlugAvailabilityWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("user search max attempts", c.UserSearchMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("user search window duration", c.UserSearchWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("notification count max attempts", c.NotificationCountMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("notification count window duration", c.NotificationCountWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("agent session control max attempts", c.AgentSessionControlMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("agent session control window duration", c.AgentSessionControlWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("chat fork max attempts", c.ChatForkMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("chat fork window duration", c.ChatForkWindowDuration); err != nil {
			return err
		}
		if err := validateRateLimitAttempts("oauth callback max attempts", c.OAuthCallbackMaxAttempts); err != nil {
			return err
		}
		if err := validateRateLimitDuration("oauth callback window duration", c.OAuthCallbackWindowDuration); err != nil {
			return err
		}
	}

	// Validate timeout values
	if c.ReadTimeout != nil && *c.ReadTimeout <= 0 {
		return fmt.Errorf("read timeout must be positive, got: %v", *c.ReadTimeout)
	}
	if c.WriteTimeout != nil && *c.WriteTimeout <= 0 {
		return fmt.Errorf("write timeout must be positive, got: %v", *c.WriteTimeout)
	}
	if _, err := requiredDuration("post-commit hook timeout", c.PostCommitHookTimeout, time.Nanosecond, 0); err != nil {
		return err
	}
	if c.PostCommitHookFlushWait != nil && *c.PostCommitHookFlushWait <= 0 {
		return fmt.Errorf("post-commit hook flush wait must be positive, got: %v", *c.PostCommitHookFlushWait)
	}
	if c.TransactionResponseBufferLimitBytes != nil && *c.TransactionResponseBufferLimitBytes <= 0 {
		return fmt.Errorf("transaction response buffer limit must be positive, got: %d", *c.TransactionResponseBufferLimitBytes)
	}

	if _, err := requiredCount("org hierarchy max nodes", c.OrgHierarchyMaxNodes, 1, MaxOrganizationHierarchyNodes); err != nil {
		return err
	}
	for depth, byRole := range c.OrgUserAttributePermissions {
		if err := validateOrganizationRoleAttributeKeys(fmt.Sprintf("OrgUserAttributePermissions[%d]", depth), byRole); err != nil {
			return err
		}
	}
	if err := validateUserTypeAttributeKeys("GlobalUserAttributePermissions", c.GlobalUserAttributePermissions); err != nil {
		return err
	}
	if c.OrgHierarchyMaxDepth != nil {
		maxDepth := *c.OrgHierarchyMaxDepth
		if maxDepth < 0 {
			return fmt.Errorf("org hierarchy max depth must be non-negative, got: %d", maxDepth)
		}
		expectedLen := maxDepth + 1
		validateRoleSlice := func(name string, roles []proto_models.OrganizationRole) error {
			if roles == nil {
				return nil
			}
			if len(roles) != expectedLen {
				return fmt.Errorf("%s length must be %d for org hierarchy depth %d", name, expectedLen, maxDepth)
			}
			return nil
		}

		if err := validateRoleSlice("OrgMinRoleInviteMembers", c.OrgMinRoleInviteMembers); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleAddMembers", c.OrgMinRoleAddMembers); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleRemoveMembers", c.OrgMinRoleRemoveMembers); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleUpdateMemberRoles", c.OrgMinRoleUpdateMemberRoles); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleListMembers", c.OrgMinRoleListMembers); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleView", c.OrgMinRoleView); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleUpdate", c.OrgMinRoleUpdate); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleDelete", c.OrgMinRoleDelete); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleCancelInvitation", c.OrgMinRoleCancelInvitation); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleViewInvitations", c.OrgMinRoleViewInvitations); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleCreateChild", c.OrgMinRoleCreateChild); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleSelfJoin", c.OrgMinRoleSelfJoin); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleManageEmailDomains", c.OrgMinRoleManageEmailDomains); err != nil {
			return err
		}
		if err := validateRoleSlice("DefaultOrgCreatorRole", c.DefaultOrgCreatorRole); err != nil {
			return err
		}
		if err := validateRoleSlice("OrganizationAutoAssociationDefaultRole", c.OrganizationAutoAssociationDefaultRole); err != nil {
			return err
		}
		if err := validateRoleSlice("UserSearchMinOrgRole", c.UserSearchMinOrgRole); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleViewInferencePolicy", c.OrgMinRoleViewInferencePolicy); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleManageInferencePolicy", c.OrgMinRoleManageInferencePolicy); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleChooseInferenceModel", c.OrgMinRoleChooseInferenceModel); err != nil {
			return err
		}
		if err := validateRoleSlice("OrgMinRoleReadAgentTranscript", c.OrgMinRoleReadAgentTranscript); err != nil {
			return err
		}
		if c.OrgUserAttributePermissions != nil && len(c.OrgUserAttributePermissions) != expectedLen {
			return fmt.Errorf("OrgUserAttributePermissions length must be %d for org hierarchy depth %d", expectedLen, maxDepth)
		}
		if c.OrgAutoAddCreatorOnChildCreate != nil && len(c.OrgAutoAddCreatorOnChildCreate) != expectedLen {
			return fmt.Errorf("OrgAutoAddCreatorOnChildCreate length must be %d for org hierarchy depth %d", expectedLen, maxDepth)
		}
	}

	// Validate organization slug configuration
	if c.OrganizationSlugMinLength != nil && *c.OrganizationSlugMinLength < 1 {
		return fmt.Errorf("organization slug minimum length must be at least 1, got: %d", *c.OrganizationSlugMinLength)
	}
	if c.OrganizationSlugMaxLength != nil && *c.OrganizationSlugMaxLength < 1 {
		return fmt.Errorf("organization slug maximum length must be at least 1, got: %d", *c.OrganizationSlugMaxLength)
	}
	if c.OrganizationSlugMinLength != nil && c.OrganizationSlugMaxLength != nil && *c.OrganizationSlugMinLength > *c.OrganizationSlugMaxLength {
		return fmt.Errorf("organization slug minimum length (%d) cannot be greater than maximum length (%d)", *c.OrganizationSlugMinLength, *c.OrganizationSlugMaxLength)
	}
	if c.OrganizationInvitationExpiryDays != nil && *c.OrganizationInvitationExpiryDays <= 0 {
		return fmt.Errorf("organization invitation expiry must be positive, got: %v", *c.OrganizationInvitationExpiryDays)
	}
	if c.OrganizationAutoAddExpiryDays != nil && *c.OrganizationAutoAddExpiryDays <= 0 {
		return fmt.Errorf("organization auto-add expiry must be positive, got: %v", *c.OrganizationAutoAddExpiryDays)
	}

	// Validate pagination configuration
	if c.DefaultPaginationLimit != nil && (*c.DefaultPaginationLimit < 1 || *c.DefaultPaginationLimit > MaxPaginationLimitCeiling) {
		return fmt.Errorf("default pagination limit must be between 1 and %d, got: %d", MaxPaginationLimitCeiling, *c.DefaultPaginationLimit)
	}
	if c.OrganizationParentOptionsMaxLimit != nil && (*c.OrganizationParentOptionsMaxLimit < 1 || *c.OrganizationParentOptionsMaxLimit > MaxOrganizationParentOptionsMaxLimit) {
		return fmt.Errorf("organization parent options max limit must be between 1 and %d, got: %d", MaxOrganizationParentOptionsMaxLimit, *c.OrganizationParentOptionsMaxLimit)
	}
	if c.MaxPaginationLimit != nil && (*c.MaxPaginationLimit < 1 || *c.MaxPaginationLimit > MaxPaginationLimitCeiling) {
		return fmt.Errorf("max pagination limit must be between 1 and %d, got: %d", MaxPaginationLimitCeiling, *c.MaxPaginationLimit)
	}
	if c.DefaultPaginationLimit != nil && c.MaxPaginationLimit != nil && *c.DefaultPaginationLimit > *c.MaxPaginationLimit {
		return fmt.Errorf("default pagination limit (%d) cannot exceed max pagination limit (%d)", *c.DefaultPaginationLimit, *c.MaxPaginationLimit)
	}
	if c.InferenceAttemptListDefaultLimit != nil && (*c.InferenceAttemptListDefaultLimit < 1 || *c.InferenceAttemptListDefaultLimit > MaxInferenceAttemptListLimit) {
		return fmt.Errorf("inference attempt list default limit must be between 1 and %d, got: %d", MaxInferenceAttemptListLimit, *c.InferenceAttemptListDefaultLimit)
	}
	if c.InferenceAttemptListMaxLimit != nil && (*c.InferenceAttemptListMaxLimit < 1 || *c.InferenceAttemptListMaxLimit > MaxInferenceAttemptListLimit) {
		return fmt.Errorf("inference attempt list max limit must be between 1 and %d, got: %d", MaxInferenceAttemptListLimit, *c.InferenceAttemptListMaxLimit)
	}
	if c.InferenceAttemptListDefaultLimit != nil && c.InferenceAttemptListMaxLimit != nil && *c.InferenceAttemptListDefaultLimit > *c.InferenceAttemptListMaxLimit {
		return fmt.Errorf("inference attempt list default limit (%d) cannot exceed inference attempt list max limit (%d)", *c.InferenceAttemptListDefaultLimit, *c.InferenceAttemptListMaxLimit)
	}
	// Parent-option pages are page-number pagination: a first page larger than the
	// global maximum could not be continued with the same limit.
	if c.OrganizationParentOptionsMaxLimit != nil && c.MaxPaginationLimit != nil && *c.OrganizationParentOptionsMaxLimit > *c.MaxPaginationLimit {
		return fmt.Errorf("organization parent options max limit (%d) cannot exceed max pagination limit (%d)", *c.OrganizationParentOptionsMaxLimit, *c.MaxPaginationLimit)
	}

	// Validate user search configuration
	if c.UserSearchMinQueryLength != nil && *c.UserSearchMinQueryLength < 0 {
		return fmt.Errorf("user search min query length cannot be negative, got: %d", *c.UserSearchMinQueryLength)
	}
	if c.UserSearchMaxQueryLength != nil && *c.UserSearchMaxQueryLength < 1 {
		return fmt.Errorf("user search max query length must be at least 1, got: %d", *c.UserSearchMaxQueryLength)
	}
	if c.UserSearchMinQueryLength != nil && c.UserSearchMaxQueryLength != nil && *c.UserSearchMinQueryLength > *c.UserSearchMaxQueryLength {
		return fmt.Errorf("user search min query length (%d) cannot exceed max query length (%d)", *c.UserSearchMinQueryLength, *c.UserSearchMaxQueryLength)
	}
	if c.UserSearchMaxAttempts != nil && *c.UserSearchMaxAttempts < 0 {
		return fmt.Errorf("user search max attempts cannot be negative, got: %d", *c.UserSearchMaxAttempts)
	}
	if c.CareerApplicationMaxAttempts != nil && *c.CareerApplicationMaxAttempts < 0 {
		return fmt.Errorf("career application max attempts cannot be negative, got: %d", *c.CareerApplicationMaxAttempts)
	}
	if c.UserSearchWindowDuration != nil && *c.UserSearchWindowDuration < 0 {
		return fmt.Errorf("user search window duration cannot be negative, got: %v", *c.UserSearchWindowDuration)
	}
	if c.CareerApplicationWindowDuration != nil && *c.CareerApplicationWindowDuration < 0 {
		return fmt.Errorf("career application window duration cannot be negative, got: %v", *c.CareerApplicationWindowDuration)
	}
	if c.CareerJobLookupMaxAttempts != nil && *c.CareerJobLookupMaxAttempts < 0 {
		return fmt.Errorf("career job lookup max attempts cannot be negative, got: %d", *c.CareerJobLookupMaxAttempts)
	}
	if c.CareerJobLookupWindowDuration != nil && *c.CareerJobLookupWindowDuration < 0 {
		return fmt.Errorf("career job lookup window duration cannot be negative, got: %v", *c.CareerJobLookupWindowDuration)
	}
	if c.PublicChatReadMaxAttempts != nil && *c.PublicChatReadMaxAttempts < 0 {
		return fmt.Errorf("public chat read max attempts cannot be negative, got: %d", *c.PublicChatReadMaxAttempts)
	}
	if c.PublicChatReadWindowDuration != nil && *c.PublicChatReadWindowDuration < 0 {
		return fmt.Errorf("public chat read window duration cannot be negative, got: %v", *c.PublicChatReadWindowDuration)
	}
	if c.OrganizationSlugAvailabilityMaxAttempts != nil && *c.OrganizationSlugAvailabilityMaxAttempts < 0 {
		return fmt.Errorf("organization slug availability max attempts cannot be negative, got: %d", *c.OrganizationSlugAvailabilityMaxAttempts)
	}
	if c.OrganizationSlugAvailabilityWindowDuration != nil && *c.OrganizationSlugAvailabilityWindowDuration < 0 {
		return fmt.Errorf("organization slug availability window duration cannot be negative, got: %v", *c.OrganizationSlugAvailabilityWindowDuration)
	}
	if c.NotificationCountMaxAttempts != nil && *c.NotificationCountMaxAttempts < 0 {
		return fmt.Errorf("notification count max attempts cannot be negative, got: %d", *c.NotificationCountMaxAttempts)
	}
	if c.NotificationCountWindowDuration != nil && *c.NotificationCountWindowDuration < 0 {
		return fmt.Errorf("notification count window duration cannot be negative, got: %v", *c.NotificationCountWindowDuration)
	}
	if c.AgentSessionControlMaxAttempts != nil && *c.AgentSessionControlMaxAttempts < 0 {
		return fmt.Errorf("agent session control max attempts cannot be negative, got: %d", *c.AgentSessionControlMaxAttempts)
	}
	if c.AgentSessionControlWindowDuration != nil && *c.AgentSessionControlWindowDuration < 0 {
		return fmt.Errorf("agent session control window duration cannot be negative, got: %v", *c.AgentSessionControlWindowDuration)
	}
	if c.ChatForkMaxAttempts != nil && *c.ChatForkMaxAttempts < 0 {
		return fmt.Errorf("chat fork max attempts cannot be negative, got: %d", *c.ChatForkMaxAttempts)
	}
	if c.ChatForkWindowDuration != nil && *c.ChatForkWindowDuration < 0 {
		return fmt.Errorf("chat fork window duration cannot be negative, got: %v", *c.ChatForkWindowDuration)
	}
	if c.OAuthCallbackMaxAttempts != nil && *c.OAuthCallbackMaxAttempts < 0 {
		return fmt.Errorf("oauth callback max attempts cannot be negative, got: %d", *c.OAuthCallbackMaxAttempts)
	}
	if c.OAuthCallbackWindowDuration != nil && *c.OAuthCallbackWindowDuration < 0 {
		return fmt.Errorf("oauth callback window duration cannot be negative, got: %v", *c.OAuthCallbackWindowDuration)
	}

	if c.BotTypingDelay != nil && *c.BotTypingDelay < 0 {
		return fmt.Errorf("bot typing delay cannot be negative, got: %v", *c.BotTypingDelay)
	}
	if c.BotTypingInterval != nil && *c.BotTypingInterval <= 0 {
		return fmt.Errorf("bot typing interval must be positive, got: %v", *c.BotTypingInterval)
	}

	return nil
}

// isInvalidUsernamePrefix executes its operation in the configuration loading, defaulting, and validation flow.
func isInvalidUsernamePrefix(prefix string) bool {
	if prefix == "" {
		return false
	}

	for i, r := range prefix {
		if i == 0 && !isUsernameAlphaNumeric(r) {
			return true
		}
		if !isUsernameAllowedChar(r) {
			return true
		}
	}

	return false
}

// isUsernameAllowedChar executes its operation in the configuration loading, defaulting, and validation flow.
func isUsernameAllowedChar(r rune) bool {
	return isUsernameAlphaNumeric(r) || r == '_' || r == '-'
}

// isUsernameAlphaNumeric executes its operation in the configuration loading, defaulting, and validation flow.
func isUsernameAlphaNumeric(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}
