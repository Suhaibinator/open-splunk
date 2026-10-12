// Package config defines go-common's typed managed-configuration contract.
// The package defines a comprehensive Config struct that holds all application settings,
// including database connections, authentication parameters, email providers, observability
// settings, and more. Applications provide code-owned defaults; KMS supplies
// complete typed parameter groups and exact secret-version pins.
package config

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap/zapcore"
)

// MaxOrganizationHierarchyNodes is the hard safety ceiling for a full
// organization hierarchy response.
const MaxOrganizationHierarchyNodes = 10_000

// Config defines data used in the configuration loading, defaulting, and validation flow.
// It is consumed by handlers and helpers that participate in this backend call path.
//
//go:generate go run ../../cmd/gen-config-views -type Config -output views_gen.go
type Config struct {
	// Database configuration

	// DBHost is the hostname or IP address of the PostgreSQL database server
	DBHost string `json:"db_host" yaml:"db_host" toml:"db_host" kms:"group=database,reload=restart" kms_views:"database"`
	// DBPort is the port number on which PostgreSQL is listening
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set to 0" (*0)
	DBPort *int `json:"db_port" yaml:"db_port,omitempty" toml:"db_port,omitempty" kms:"group=database,reload=restart" kms_views:"database"`
	// DBUser is the username for database authentication
	DBUser string `json:"db_user" yaml:"db_user" toml:"db_user" kms:"group=database,reload=restart" kms_views:"database"`
	// DBPassword is the password for database authentication (resolved from KMS)
	DBPassword Secret `json:"-" yaml:"-" toml:"-" kms:"secret=db_password,reload=restart" kms_views:"database"`
	// DBName is the name of the PostgreSQL database to connect to
	DBName string `json:"db_name" yaml:"db_name" toml:"db_name" kms:"group=database,reload=restart" kms_views:"database"`
	// DBSSLMode controls the SSL/TLS behavior for database connections (disable, require, verify-ca, verify-full)
	DBSSLMode string `json:"db_ssl_mode" yaml:"db_ssl_mode" toml:"db_ssl_mode" kms:"group=database,reload=restart" kms_views:"database"`
	// DBMaxOpenConns is the maximum number of open connections to the database
	// Use nil to apply default, 0 to disable connection limit
	DBMaxOpenConns *int `json:"db_max_open_conns" yaml:"db_max_open_conns,omitempty" toml:"db_max_open_conns,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBMaxIdleConns is the maximum number of connections in the idle connection pool
	// Use nil to apply default, 0 to disable idle connections
	DBMaxIdleConns *int `json:"db_max_idle_conns" yaml:"db_max_idle_conns,omitempty" toml:"db_max_idle_conns,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBConnMaxLifetime is the maximum amount of time a connection may be reused (in seconds)
	// Use nil to apply default, 0 to disable connection lifetime limit
	DBConnMaxLifetime *int `json:"db_conn_max_lifetime" yaml:"db_conn_max_lifetime,omitempty" toml:"db_conn_max_lifetime,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBConnMaxIdleTime is the maximum amount of time a connection may be idle (in seconds)
	// Use nil to apply default, 0 to disable idle timeout
	DBConnMaxIdleTime *int `json:"db_conn_max_idle_time" yaml:"db_conn_max_idle_time,omitempty" toml:"db_conn_max_idle_time,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBLogLevel controls the verbosity of GORM's logger (silent, error, warn, info)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set" (*DBLogLevelSilent)
	DBLogLevel *DBLogLevel `json:"db_log_level" yaml:"db_log_level,omitempty" toml:"db_log_level,omitempty" kms:"group=database,reload=restart" kms_views:"database"`
	// Database timeout configurations

	// DBQueryTimeout bounds session-token cleanup queries. Other database operations
	// use their caller context; this is not a global SQL query timeout.
	// Use nil to apply default, 0 to disable query timeout
	DBQueryTimeout *time.Duration `json:"db_query_timeout" yaml:"db_query_timeout,omitempty" toml:"db_query_timeout,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBTransactionTimeout is currently diagnostic only: BeginTransaction logs it
	// but uses the caller context without imposing a separate transaction deadline.
	DBTransactionTimeout *time.Duration `json:"db_transaction_timeout" yaml:"db_transaction_timeout,omitempty" toml:"db_transaction_timeout,omitempty" kms:"group=database,reload=restart" kms_views:"database"`
	// DBHealthCheckInterval is the interval between database health check pings
	// Use nil to apply default, 0 to disable health checks
	DBHealthCheckInterval *time.Duration `json:"db_health_check_interval" yaml:"db_health_check_interval,omitempty" toml:"db_health_check_interval,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// DBMetricsInterval is the interval for collecting database performance metrics
	// Use nil to apply default, 0 to disable metrics collection
	DBMetricsInterval *time.Duration `json:"db_metrics_interval" yaml:"db_metrics_interval,omitempty" toml:"db_metrics_interval,omitempty" kms:"group=database,reload=hot" kms_views:"database"`
	// MinIO configuration

	// MinioEndpoint is the endpoint URL for the MinIO server (e.g., "localhost:9000")
	MinioEndpoint string `json:"minio_endpoint" yaml:"minio_endpoint" toml:"minio_endpoint" kms:"group=object_storage,reload=restart" kms_views:"object_storage"`
	// MinioAccessKey is the access key for MinIO authentication
	MinioAccessKey string `json:"minio_access_key" yaml:"minio_access_key" toml:"minio_access_key" kms:"group=object_storage,reload=restart" kms_views:"object_storage"`
	// MinioSecretKey is the secret key for MinIO authentication (resolved from KMS)
	MinioSecretKey Secret `json:"-" yaml:"-" toml:"-" kms:"secret=minio_secret_key,reload=restart" kms_views:"object_storage"`
	// MinioUseSSL determines whether to use SSL/TLS for MinIO connections
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	MinioUseSSL *bool `json:"minio_use_ssl" yaml:"minio_use_ssl,omitempty" toml:"minio_use_ssl,omitempty" kms:"group=object_storage,reload=restart" kms_views:"object_storage"`
	// MinioBucket is the name of the MinIO bucket to use for object storage
	MinioBucket string `json:"minio_bucket" yaml:"minio_bucket" toml:"minio_bucket" kms:"group=object_storage,reload=restart" kms_views:"object_storage"`
	// MinioPublicBucket is the name of the MinIO bucket that holds anonymously readable
	// attachment objects. An empty value disables public attachment storage.
	MinioPublicBucket string `json:"minio_public_bucket" yaml:"minio_public_bucket" toml:"minio_public_bucket" kms:"group=object_storage,reload=restart" kms_views:"object_storage"`
	// MinioPublicBaseURL is the public origin fronting the MinIO server root, or a CDN of
	// it (e.g., "https://cdn.example.com"). It is not bucket-scoped: presigned URLs keep
	// the private bucket segment in their path and public attachment URLs are composed as
	// <MinioPublicBaseURL>/<MinioPublicBucket>/<object name>, so one origin serves both.
	MinioPublicBaseURL string `json:"minio_public_base_url" yaml:"minio_public_base_url" toml:"minio_public_base_url" kms:"group=object_storage,reload=hot" kms_views:"object_storage"`
	// ObjectHashSalt is the salt used for hashing object names in MinIO (resolved from KMS)
	ObjectHashSalt Secret `json:"-" yaml:"-" toml:"-" kms:"secret=object_hash_salt,reload=restart" kms_views:"object_storage"`
	// StreamingHashChunkSize is the size of chunks (in bytes) used when calculating hashes for large files.
	// Files are read and hashed in chunks of this size to avoid loading entire files into memory.
	// Default is 50MB (50 * 1024 * 1024). Larger chunks may improve performance but use more memory.
	// Use nil to apply the default; configured chunk sizes must be positive.
	StreamingHashChunkSize *int64 `json:"streaming_hash_chunk_size" yaml:"streaming_hash_chunk_size,omitempty" toml:"streaming_hash_chunk_size,omitempty" kms:"group=object_storage,reload=hot" kms_views:"object_storage"`
	// ObjectStorageStreamPartSize is the multipart part size (in bytes) for unknown-length
	// PutObjectStream uploads. Each such upload holds one buffer of this size while it runs, and
	// S3 allows at most 10,000 parts, so it also caps the object at part size x 10,000 (16 MiB
	// allows about 156 GiB). Known-length uploads size their parts automatically. Sampled once
	// per upload. Required; must be between 5 MiB and 5 GiB, the S3 part-size limits.
	ObjectStorageStreamPartSize *int64 `json:"object_storage_stream_part_size" yaml:"object_storage_stream_part_size,omitempty" toml:"object_storage_stream_part_size,omitempty" kms:"group=object_storage,reload=hot" kms_views:"object_storage"`
	// AttachmentUploadURLExpiry is the duration for which attachment upload URLs remain valid.
	// Default is 30 minutes if not specified.
	// Use nil to apply the default; configured values must be positive.
	AttachmentUploadURLExpiry *time.Duration `json:"attachment_upload_url_expiry" yaml:"attachment_upload_url_expiry,omitempty" toml:"attachment_upload_url_expiry,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentDownloadURLExpiry is the duration for which presigned attachment download URLs remain valid.
	// Default is 15 minutes if not specified.
	// Use nil to apply the default; configured values must be positive.
	AttachmentDownloadURLExpiry *time.Duration `json:"attachment_download_url_expiry" yaml:"attachment_download_url_expiry,omitempty" toml:"attachment_download_url_expiry,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPresignHost is the public origin attachment download presigns are signed for,
	// as "host[:port]" or "scheme://host[:port]" (scheme defaults to https). When set, download
	// URLs are signed offline for this origin so an edge SigV4 verifier fronting the private
	// bucket accepts them without a host rewrite.
	// Empty selects the SDK presign against the MinIO endpoint plus host rewrite.
	AttachmentPresignHost *string `json:"attachment_presign_host" yaml:"attachment_presign_host,omitempty" toml:"attachment_presign_host,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPresignQuantum floors the presign signing time to this boundary when it is
	// positive and smaller than the effective expiry, making download URLs byte-identical
	// within a window (browser caching and stable edge cache keys); the expiry counts from
	// the quantum start. Only applies when AttachmentPresignHost is set.
	// Use nil to apply the default; 0 disables quantization.
	AttachmentPresignQuantum *time.Duration `json:"attachment_presign_quantum" yaml:"attachment_presign_quantum,omitempty" toml:"attachment_presign_quantum,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPresignRegion is the SigV4 credential-scope region for host-scoped download presigns.
	// Default is "us-east-1" if not specified.
	AttachmentPresignRegion *string `json:"attachment_presign_region" yaml:"attachment_presign_region,omitempty" toml:"attachment_presign_region,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPresignAccessKey is the access key ID embedded in host-scoped download
	// presigns (the X-Amz-Credential prefix). When set together with
	// AttachmentPresignSecretKey, download URLs are signed with this dedicated
	// credential instead of the MinIO credential, so an offline edge verifier can hold
	// a signing secret that has no standing at MinIO. Empty falls back to the MinIO
	// credential. Both fields are hot: rotation takes effect on the next URL mint.
	AttachmentPresignAccessKey *string `json:"attachment_presign_access_key" yaml:"attachment_presign_access_key,omitempty" toml:"attachment_presign_access_key,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPresignSecretKey is the dedicated signing secret paired with
	// AttachmentPresignAccessKey (resolved from KMS). URLs signed with it verify only
	// at an edge configured with the same pair; MinIO does not recognize the
	// credential, so origin fetches must be authorized by deployment topology.
	AttachmentPresignSecretKey Secret `json:"-" yaml:"-" toml:"-" kms:"secret=attachment_presign_secret_key,reload=hot" kms_views:"attachments"`
	// AttachmentDefaultMaxSize is the default maximum file size (in bytes) for attachments when not specified by object type.
	// Default is 100MB (100 * 1024 * 1024) if not specified.
	// Use nil to apply the default; configured values must be positive.
	AttachmentDefaultMaxSize *int64 `json:"attachment_default_max_size" yaml:"attachment_default_max_size,omitempty" toml:"attachment_default_max_size,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentXMLMaxTokenBytes caps the bytes one XML token (tag, text run,
	// comment), and the start tags of all open elements together, may span while
	// a declared application/xml or text/xml upload is scanned, bounding the
	// parser's buffer and retained element state (65,536–67,108,864; default
	// 1,048,576). Documents exceeding either cap are rejected.
	AttachmentXMLMaxTokenBytes *int64 `json:"attachment_xml_max_token_bytes" yaml:"attachment_xml_max_token_bytes,omitempty" toml:"attachment_xml_max_token_bytes,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentProcessingTimeout is the duration after which an attachment in processing state is considered stuck.
	// Default is 10 minutes if not specified.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AttachmentProcessingTimeout *time.Duration `json:"attachment_processing_timeout" yaml:"attachment_processing_timeout,omitempty" toml:"attachment_processing_timeout,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentCleanupInterval is the interval at which the cleanup routine runs to find and mark stuck attachments.
	// Default is 5 minutes if not specified.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AttachmentCleanupInterval *time.Duration `json:"attachment_cleanup_interval" yaml:"attachment_cleanup_interval,omitempty" toml:"attachment_cleanup_interval,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentEnableAutoCleanup determines whether automatic cleanup of stuck attachments is enabled.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	AttachmentEnableAutoCleanup *bool `json:"attachment_enable_auto_cleanup" yaml:"attachment_enable_auto_cleanup,omitempty" toml:"attachment_enable_auto_cleanup,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentPendingUploadTimeout is how long after its latest upload URL expired an attachment in pending_upload state is considered stuck.
	// Default is 1 hour if not specified.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AttachmentPendingUploadTimeout *time.Duration `json:"attachment_pending_upload_timeout" yaml:"attachment_pending_upload_timeout,omitempty" toml:"attachment_pending_upload_timeout,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentInternalReclaimGrace is how long an internal attachment row must stay unchanged and
	// unreferenced by any message before the cleanup routine reclaims it.
	// Default is 1 hour; bounded to 1 minute–720 hours.
	AttachmentInternalReclaimGrace *time.Duration `json:"attachment_internal_reclaim_grace" yaml:"attachment_internal_reclaim_grace,omitempty" toml:"attachment_internal_reclaim_grace,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentInternalReclaimBatchSize caps the internal attachment rows one cleanup tick reclaims.
	// Default is 100; bounded to 1–200.
	AttachmentInternalReclaimBatchSize *int `json:"attachment_internal_reclaim_batch_size" yaml:"attachment_internal_reclaim_batch_size,omitempty" toml:"attachment_internal_reclaim_batch_size,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentTemporaryObjectCleanupTimeout bounds one best-effort deletion of an upload's
	// temporary object after its transaction commits.
	// Default is 3 seconds; bounded to 100 milliseconds–1 minute.
	AttachmentTemporaryObjectCleanupTimeout *time.Duration `json:"attachment_temporary_object_cleanup_timeout" yaml:"attachment_temporary_object_cleanup_timeout,omitempty" toml:"attachment_temporary_object_cleanup_timeout,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentTemporaryUploadPruneAge is how old a still-pending upload must be before the
	// cleanup routine rejects it and deletes its temporary object.
	// Default is 24 hours; bounded to 1–720 hours.
	AttachmentTemporaryUploadPruneAge *time.Duration `json:"attachment_temporary_upload_prune_age" yaml:"attachment_temporary_upload_prune_age,omitempty" toml:"attachment_temporary_upload_prune_age,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentTemporaryObjectSweepBatchSize caps the vetted, rejected, or deleted attachments
	// whose leftover temporary object one cleanup tick deletes.
	// Default is 200; bounded to 1–1000.
	AttachmentTemporaryObjectSweepBatchSize *int `json:"attachment_temporary_object_sweep_batch_size" yaml:"attachment_temporary_object_sweep_batch_size,omitempty" toml:"attachment_temporary_object_sweep_batch_size,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// AttachmentLockTTL is the duration after which attachment processing locks automatically expire.
	// Default is 10 minutes if not specified.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AttachmentLockTTL *time.Duration `json:"attachment_lock_ttl" yaml:"attachment_lock_ttl,omitempty" toml:"attachment_lock_ttl,omitempty" kms:"group=attachments,reload=restart" kms_views:"attachments"`
	// AttachmentLockCapacity is the maximum number of concurrent attachment processing locks.
	// Default is 30000 if not specified.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AttachmentLockCapacity *uint64 `json:"attachment_lock_capacity" yaml:"attachment_lock_capacity,omitempty" toml:"attachment_lock_capacity,omitempty" kms:"group=attachments,reload=restart" kms_views:"attachments"`

	// Auth configuration

	// SessionValidity is the duration for which session tokens remain valid
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SessionValidity *time.Duration `json:"session_validity" yaml:"session_validity,omitempty" toml:"session_validity,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// EnableSessionRefresh determines whether the probabilistic sliding-session refresh middleware is active
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableSessionRefresh *bool `json:"enable_session_refresh" yaml:"enable_session_refresh,omitempty" toml:"enable_session_refresh,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// SessionRefreshSoftThreshold is the fraction of SessionValidity elapsed below which a session token is never refreshed
	// Use pointer to distinguish between "not configured" (nil) and "explicitly zero" (*0)
	SessionRefreshSoftThreshold *float64 `json:"session_refresh_soft_threshold" yaml:"session_refresh_soft_threshold,omitempty" toml:"session_refresh_soft_threshold,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// SessionRefreshHardThreshold is the fraction of SessionValidity elapsed at or above which a session token is always refreshed
	// Use pointer to distinguish between "not configured" (nil) and "explicitly zero" (*0)
	SessionRefreshHardThreshold *float64 `json:"session_refresh_hard_threshold" yaml:"session_refresh_hard_threshold,omitempty" toml:"session_refresh_hard_threshold,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// SessionRefreshRampExponent shapes the refresh probability ramp between the soft and hard
	// thresholds: p = x^k where x is the normalized position in the window (1 = linear,
	// >1 back-loads refreshes toward the hard threshold)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly zero" (*0)
	SessionRefreshRampExponent *float64 `json:"session_refresh_ramp_exponent" yaml:"session_refresh_ramp_exponent,omitempty" toml:"session_refresh_ramp_exponent,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// SessionRefreshRevocationGrace is how long a session token revoked by a refresh keeps
	// authenticating requests, so in-flight requests still carrying it are not rejected
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SessionRefreshRevocationGrace *time.Duration `json:"session_refresh_revocation_grace" yaml:"session_refresh_revocation_grace,omitempty" toml:"session_refresh_revocation_grace,omitempty" kms:"group=auth,reload=restart" kms_views:"auth"`
	// SessionTokenSecret is the secret used for signing session tokens (resolved from KMS)
	SessionTokenSecret Secret `json:"-" yaml:"-" toml:"-" kms:"secret=session_token_secret,reload=restart" kms_views:"auth"`
	// AuthCacheTtl is the time-to-live for cached authentication tokens and users.
	// Zero disables authorization cache reads and admissions; negative values are invalid.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AuthCacheTtl *time.Duration `json:"auth_cache_ttl" yaml:"auth_cache_ttl,omitempty" toml:"auth_cache_ttl,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// OTPIssuer is the issuer name displayed in TOTP authenticator apps (e.g., "MyApp" or "MyCompany")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	OTPIssuer *string `json:"otp_issuer" yaml:"otp_issuer,omitempty" toml:"otp_issuer,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// AuthTokenCacheCapacity is the maximum number of tokens to store in the auth cache
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AuthTokenCacheCapacity *uint64 `json:"auth_token_cache_capacity" yaml:"auth_token_cache_capacity,omitempty" toml:"auth_token_cache_capacity,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// AuthUserCacheCapacity is the maximum number of users to store in the user cache
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	AuthUserCacheCapacity *uint64 `json:"auth_user_cache_capacity" yaml:"auth_user_cache_capacity,omitempty" toml:"auth_user_cache_capacity,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// OrganizationRoleCacheCapacity is the maximum number of user-organization role mappings to store in the cache
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	OrganizationRoleCacheCapacity *uint64 `json:"organization_role_cache_capacity" yaml:"organization_role_cache_capacity,omitempty" toml:"organization_role_cache_capacity,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrganizationRoleCacheTTL is the duration for which organization role mappings remain cached
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	OrganizationRoleCacheTTL *time.Duration `json:"organization_role_cache_ttl" yaml:"organization_role_cache_ttl,omitempty" toml:"organization_role_cache_ttl,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// VerificationTokenValidity is the duration for which email verification tokens remain valid
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	VerificationTokenValidity *time.Duration `json:"verification_token_validity" yaml:"verification_token_validity,omitempty" toml:"verification_token_validity,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// VerificationCooldown is the minimum time between verification email resend requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	VerificationCooldown *time.Duration `json:"verification_cooldown" yaml:"verification_cooldown,omitempty" toml:"verification_cooldown,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// UnverifiedUserCleanupInterval is how often to sweep unverified users for cleanup.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	UnverifiedUserCleanupInterval *time.Duration `json:"unverified_user_cleanup_interval" yaml:"unverified_user_cleanup_interval,omitempty" toml:"unverified_user_cleanup_interval,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// UnverifiedUserCleanupBatchSize limits how many users are processed per cleanup run.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	UnverifiedUserCleanupBatchSize *int `json:"unverified_user_cleanup_batch_size" yaml:"unverified_user_cleanup_batch_size,omitempty" toml:"unverified_user_cleanup_batch_size,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// UnverifiedUserCleanupTimeout is the per-run timeout for unverified user cleanup.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	UnverifiedUserCleanupTimeout *time.Duration `json:"unverified_user_cleanup_timeout" yaml:"unverified_user_cleanup_timeout,omitempty" toml:"unverified_user_cleanup_timeout,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`

	// Phone verification configuration

	// PhoneVerificationOTPValidity is the duration for which phone verification OTP codes remain valid
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PhoneVerificationOTPValidity *time.Duration `json:"phone_verification_otp_validity" yaml:"phone_verification_otp_validity,omitempty" toml:"phone_verification_otp_validity,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`
	// PhoneVerificationCooldown is the minimum time between phone verification OTP resend requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PhoneVerificationCooldown *time.Duration `json:"phone_verification_cooldown" yaml:"phone_verification_cooldown,omitempty" toml:"phone_verification_cooldown,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`
	// PhoneNumberDefaultRegion is the default region/country code used for parsing phone numbers (e.g., "US", "GB", "CA")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	PhoneNumberDefaultRegion *string `json:"phone_number_default_region" yaml:"phone_number_default_region,omitempty" toml:"phone_number_default_region,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`

	// Email configuration

	// EmailEnabled determines whether email sending functionality is active
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EmailEnabled *bool `json:"email_enabled" yaml:"email_enabled,omitempty" toml:"email_enabled,omitempty" kms:"group=email,reload=hot" kms_views:"email"`
	// EmailProvider specifies which email service to use (postal, gmail, resend, or mockmail)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	EmailProvider *EmailProvider `json:"email_provider" yaml:"email_provider,omitempty" toml:"email_provider,omitempty" kms:"group=email,reload=hot" kms_views:"email"`
	// EmailFromAddress is the sender email address for outgoing emails
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	EmailFromAddress *string `json:"email_from_address" yaml:"email_from_address,omitempty" toml:"email_from_address,omitempty" kms:"group=email,reload=hot" kms_views:"email"`
	// EmailFromName is the sender display name for outgoing emails
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	EmailFromName *string `json:"email_from_name" yaml:"email_from_name,omitempty" toml:"email_from_name,omitempty" kms:"group=email,reload=hot" kms_views:"email"`

	// Postal configuration (self-hosted email server)

	// PostalAPIEndpoint is the URL of the Postal API server
	PostalAPIEndpoint string `json:"postal_api_endpoint" yaml:"postal_api_endpoint" toml:"postal_api_endpoint" kms:"group=email,reload=hot" kms_views:"email"`
	// PostalAPIKey is the API key for Postal authentication (resolved from KMS)
	PostalAPIKey Secret `json:"-" yaml:"-" toml:"-" kms:"secret=postal_api_key,reload=hot" kms_views:"email"`

	// Gmail configuration (OAuth-based email sending)

	// GmailCredentials contains the OAuth2 credentials for Gmail API access
	GmailCredentials Secret `json:"-" yaml:"-" toml:"-" kms:"secret=gmail_credentials,reload=hot" kms_views:"email"`
	// GmailToken contains the OAuth2 token for Gmail API access (resolved from KMS)
	GmailToken Secret `json:"-" yaml:"-" toml:"-" kms:"secret=gmail_token,reload=hot" kms_views:"email"`
	// GmailUserID is the Gmail user identifier (typically "me" for the authenticated user)
	GmailUserID string `json:"gmail_user_id" yaml:"gmail_user_id" toml:"gmail_user_id" kms:"group=email,reload=hot" kms_views:"email"`

	// Resend configuration (modern transactional email service)

	// ResendAPIKey is the API key for authenticating with Resend
	ResendAPIKey Secret `json:"-" yaml:"-" toml:"-" kms:"secret=resend_api_key,reload=hot" kms_views:"email"`

	// SMS configuration

	// SMSEnabled determines whether SMS sending functionality is active
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	SMSEnabled *bool `json:"sms_enabled" yaml:"sms_enabled,omitempty" toml:"sms_enabled,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`
	// SMSProvider specifies which SMS service to use ("twilio", "vonage", "mocksms")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set" (*SMSProviderTwilio)
	SMSProvider *SMSProvider `json:"sms_provider" yaml:"sms_provider,omitempty" toml:"sms_provider,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`
	// SMSFromNumber is the sender phone number for outgoing SMS messages (E.164 format)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	SMSFromNumber *string `json:"sms_from_number" yaml:"sms_from_number,omitempty" toml:"sms_from_number,omitempty" kms:"group=sms,reload=hot" kms_views:"sms"`

	// Twilio configuration (cloud SMS service)

	// TwilioAccountSID is the Account SID for Twilio authentication (resolved from KMS)
	TwilioAccountSID Secret `json:"-" yaml:"-" toml:"-" kms:"secret=twilio_account_sid,reload=hot" kms_views:"sms"`
	// TwilioAuthToken is the Auth Token for Twilio authentication (resolved from KMS)
	TwilioAuthToken Secret `json:"-" yaml:"-" toml:"-" kms:"secret=twilio_auth_token,reload=hot" kms_views:"sms"`

	// Vonage configuration (formerly Nexmo - cloud SMS service)

	// VonageAPIKey is the API Key for Vonage authentication (resolved from KMS)
	VonageAPIKey Secret `json:"-" yaml:"-" toml:"-" kms:"secret=vonage_api_key,reload=hot" kms_views:"sms"`
	// VonageAPISecret is the API Secret for Vonage authentication (resolved from KMS)
	VonageAPISecret Secret `json:"-" yaml:"-" toml:"-" kms:"secret=vonage_api_secret,reload=hot" kms_views:"sms"`

	// Log configuration

	// LogLevel sets the minimum log level (debug, info, warn, error, dpanic, panic, fatal)
	LogLevel zapcore.Level `json:"log_level" yaml:"log_level" toml:"log_level" kms:"group=logging,reload=hot" kms_views:"logging"`
	// LogSink specifies where logs should be sent (stdout, file, graylog, etc.)
	LogSink SinkType `json:"log_sink" yaml:"log_sink" toml:"log_sink" kms:"group=logging,reload=restart" kms_views:"logging"`
	// LogSinkConfig contains additional configuration for the selected log sink
	LogSinkConfig map[string]string `json:"log_sink_config" yaml:"log_sink_config" toml:"log_sink_config" kms:"group=logging,reload=restart" kms_views:"logging"`
	// LogOutputPaths specifies the output paths for regular logs (files or stdout)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	LogOutputPaths []string `json:"log_output_paths" yaml:"log_output_paths,omitempty" toml:"log_output_paths,omitempty" kms:"group=logging,reload=restart" kms_views:"logging"` // TODO: Consolidate with LogSinkConfig?
	// LogErrorOutputPaths specifies the output paths for error logs (files or stderr)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	LogErrorOutputPaths []string `json:"log_error_output_paths" yaml:"log_error_output_paths,omitempty" toml:"log_error_output_paths,omitempty" kms:"group=logging,reload=restart" kms_views:"logging"` // TODO: Consolidate with LogSinkConfig?

	// InferenceProviders defines integrations; importing applications bind their fixed secrets at runtime.
	InferenceProviders []ProviderConfig `json:"inference_providers" yaml:"inference_providers" toml:"inference_providers" kms:"group=genai,reload=hot" kms_views:"genai,chat"`
	// InferenceMaxOperationModels caps models in one operation's routing list (1–256; default 64).
	InferenceMaxOperationModels *int `json:"inference_max_operation_models" yaml:"inference_max_operation_models,omitempty" toml:"inference_max_operation_models,omitempty" kms:"group=genai,reload=hot" kms_views:"genai,chat"`
	// InferenceMaxOrganizationProviders caps providers in one organization's allowlist (1–256; default 32).
	InferenceMaxOrganizationProviders *int `json:"inference_max_organization_providers" yaml:"inference_max_organization_providers,omitempty" toml:"inference_max_organization_providers,omitempty" kms:"group=genai,reload=hot" kms_views:"genai,chat"`
	// InferencePreviewSessionLimit caps sessions examined by one routing impact preview (1–10,000; default 1,000).
	InferencePreviewSessionLimit *int `json:"inference_preview_session_limit" yaml:"inference_preview_session_limit,omitempty" toml:"inference_preview_session_limit,omitempty" kms:"group=genai,reload=hot" kms_views:"genai,chat"`
	// InferenceRetargetBatchSize caps sessions changed in one routing retarget batch (1–1,000; default 100).
	InferenceRetargetBatchSize *int `json:"inference_retarget_batch_size" yaml:"inference_retarget_batch_size,omitempty" toml:"inference_retarget_batch_size,omitempty" kms:"group=genai,reload=hot" kms_views:"genai,chat,sanitizer"`
	// InferenceMaxModelSteps bounds session model steps and application overrides (1–10,000; default 128).
	InferenceMaxModelSteps *uint32 `json:"inference_max_model_steps" yaml:"inference_max_model_steps,omitempty" toml:"inference_max_model_steps,omitempty" kms:"group=genai,reload=hot" kms_views:"genai,chat"`

	// Metrics configuration (Prometheus metrics)

	// MetricsEnabled determines whether Prometheus metrics collection is active
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	MetricsEnabled *bool `json:"metrics_enabled" yaml:"metrics_enabled,omitempty" toml:"metrics_enabled,omitempty" kms:"group=metrics,reload=restart" kms_views:"metrics"`
	// MetricsPort is the port number for the Prometheus metrics endpoint (e.g., ":9090")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsPort *string `json:"metrics_port" yaml:"metrics_port,omitempty" toml:"metrics_port,omitempty" kms:"group=metrics,reload=restart" kms_views:"metrics"`
	// MetricsEndpoint is the HTTP path for the metrics endpoint (e.g., "/metrics")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsEndpoint *string `json:"metrics_endpoint" yaml:"metrics_endpoint,omitempty" toml:"metrics_endpoint,omitempty" kms:"group=metrics,reload=restart" kms_views:"metrics"`
	// MetricsNamespace is the prefix for all metric names (e.g., "myapp")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsNamespace *string `json:"metrics_namespace" yaml:"metrics_namespace,omitempty" toml:"metrics_namespace,omitempty" kms:"group=metrics,reload=restart" kms_views:"metrics"`
	// Metrics Push configuration (for environments without pull-based scraping)

	// MetricsPushEnabled determines whether metrics are pushed to a remote endpoint
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	MetricsPushEnabled *bool `json:"metrics_push_enabled" yaml:"metrics_push_enabled,omitempty" toml:"metrics_push_enabled,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushEndpoint is the URL of the push gateway or remote endpoint
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsPushEndpoint *string `json:"metrics_push_endpoint" yaml:"metrics_push_endpoint,omitempty" toml:"metrics_push_endpoint,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushInterval is the interval between metric pushes (in seconds)
	MetricsPushInterval *int `json:"metrics_push_interval" yaml:"metrics_push_interval,omitempty" toml:"metrics_push_interval,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushJobName is the job label value for pushed metrics
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsPushJobName *string `json:"metrics_push_job_name" yaml:"metrics_push_job_name,omitempty" toml:"metrics_push_job_name,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushBasicAuth determines whether to use HTTP basic authentication
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	MetricsPushBasicAuth *bool `json:"metrics_push_basic_auth" yaml:"metrics_push_basic_auth,omitempty" toml:"metrics_push_basic_auth,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushUsername is the username for basic authentication
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	MetricsPushUsername *string `json:"metrics_push_username" yaml:"metrics_push_username,omitempty" toml:"metrics_push_username,omitempty" kms:"group=metrics,reload=hot" kms_views:"metrics"`
	// MetricsPushPassword is the password for basic authentication (resolved from KMS)
	MetricsPushPassword Secret `json:"-" yaml:"-" toml:"-" kms:"secret=metrics_push_password,reload=hot" kms_views:"metrics"`

	// Server configuration

	// ServerPort is the port on which the HTTP server listens (e.g., ":8080")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	ServerPort *int `json:"server_port" yaml:"server_port,omitempty" toml:"server_port,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// Env specifies the deployment environment (development, test, staging, production)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set" (*EnvironmentDevelopment)
	Env *Environment `json:"env" yaml:"env,omitempty" toml:"env,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// ServiceName is the name of this service instance (used in logs and metrics)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	ServiceName *string `json:"service_name" yaml:"service_name,omitempty" toml:"service_name,omitempty" kms:"group=server,reload=restart" kms_views:"server"`

	// Sanitizer configuration (input validation and sanitization)

	// SanitizerPasswordMinLength is the minimum allowed password length
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerPasswordMinLength *int `json:"sanitizer_password_min_length" yaml:"sanitizer_password_min_length,omitempty" toml:"sanitizer_password_min_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerLogInputMaxLength bounds user-controlled text included in sanitizer diagnostics.
	// Values are measured in Unicode code points and must not exceed 4096.
	SanitizerLogInputMaxLength *int `json:"sanitizer_log_input_max_length" yaml:"sanitizer_log_input_max_length,omitempty" toml:"sanitizer_log_input_max_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordMaxLength is the maximum allowed password length
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerPasswordMaxLength *int `json:"sanitizer_password_max_length" yaml:"sanitizer_password_max_length,omitempty" toml:"sanitizer_password_max_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordRequireUppercase determines whether passwords must contain an uppercase letter
	SanitizerPasswordRequireUppercase *bool `json:"sanitizer_password_require_uppercase" yaml:"sanitizer_password_require_uppercase,omitempty" toml:"sanitizer_password_require_uppercase,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordRequireLowercase determines whether passwords must contain a lowercase letter
	SanitizerPasswordRequireLowercase *bool `json:"sanitizer_password_require_lowercase" yaml:"sanitizer_password_require_lowercase,omitempty" toml:"sanitizer_password_require_lowercase,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordRequireNumber determines whether passwords must contain a number
	SanitizerPasswordRequireNumber *bool `json:"sanitizer_password_require_number" yaml:"sanitizer_password_require_number,omitempty" toml:"sanitizer_password_require_number,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordRequireSpecial determines whether passwords must contain a special character
	SanitizerPasswordRequireSpecial *bool `json:"sanitizer_password_require_special" yaml:"sanitizer_password_require_special,omitempty" toml:"sanitizer_password_require_special,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerPasswordSpecialChars is the set of characters considered special
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	SanitizerPasswordSpecialChars *string `json:"sanitizer_password_special_chars" yaml:"sanitizer_password_special_chars,omitempty" toml:"sanitizer_password_special_chars,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerUsernameMinLength is the minimum allowed username length
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerUsernameMinLength *int `json:"sanitizer_username_min_length" yaml:"sanitizer_username_min_length,omitempty" toml:"sanitizer_username_min_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerUsernameMaxLength is the maximum allowed username length
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerUsernameMaxLength *int `json:"sanitizer_username_max_length" yaml:"sanitizer_username_max_length,omitempty" toml:"sanitizer_username_max_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerMaxEmailLength is the maximum allowed email address length
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerMaxEmailLength *int `json:"sanitizer_max_email_length" yaml:"sanitizer_max_email_length,omitempty" toml:"sanitizer_max_email_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerMaxLocalPartLength is the maximum length for the local part of an email (before @)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerMaxLocalPartLength *int `json:"sanitizer_max_local_part_length" yaml:"sanitizer_max_local_part_length,omitempty" toml:"sanitizer_max_local_part_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerMaxDomainLength is the maximum length for the domain part of an email (after @)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerMaxDomainLength *int `json:"sanitizer_max_domain_length" yaml:"sanitizer_max_domain_length,omitempty" toml:"sanitizer_max_domain_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerAllowDisposableEmails determines whether to accept disposable email addresses
	SanitizerAllowDisposableEmails *bool `json:"sanitizer_allow_disposable_emails" yaml:"sanitizer_allow_disposable_emails,omitempty" toml:"sanitizer_allow_disposable_emails,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerBlockSQLPatterns determines whether to block potential SQL injection patterns
	SanitizerBlockSQLPatterns *bool `json:"sanitizer_block_sql_patterns" yaml:"sanitizer_block_sql_patterns,omitempty" toml:"sanitizer_block_sql_patterns,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerRequireDotInDomain determines whether email domains must contain at least one dot
	SanitizerRequireDotInDomain *bool `json:"sanitizer_require_dot_in_domain" yaml:"sanitizer_require_dot_in_domain,omitempty" toml:"sanitizer_require_dot_in_domain,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerBlockedDomains is a list of email domains to explicitly block
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	SanitizerBlockedDomains []string `json:"sanitizer_blocked_domains" yaml:"sanitizer_blocked_domains,omitempty" toml:"sanitizer_blocked_domains,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerAllowedDomains is a list of email domains to exclusively allow (blocks all others)
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	SanitizerAllowedDomains []string `json:"sanitizer_allowed_domains" yaml:"sanitizer_allowed_domains,omitempty" toml:"sanitizer_allowed_domains,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerDisposableDomains is a list of known disposable email domains
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	SanitizerDisposableDomains []string `json:"sanitizer_disposable_domains" yaml:"sanitizer_disposable_domains,omitempty" toml:"sanitizer_disposable_domains,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerUseStrictRFC5322 determines whether to use strict RFC 5322 email validation
	SanitizerUseStrictRFC5322 *bool `json:"sanitizer_use_strict_rfc5322" yaml:"sanitizer_use_strict_rfc5322,omitempty" toml:"sanitizer_use_strict_rfc5322,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerCustomRegexPattern is a custom regex pattern for email validation
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	SanitizerCustomRegexPattern *string `json:"sanitizer_custom_regex_pattern" yaml:"sanitizer_custom_regex_pattern,omitempty" toml:"sanitizer_custom_regex_pattern,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerVerificationMinLength is the minimum length for verification codes
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerVerificationMinLength *int `json:"sanitizer_verification_min_length" yaml:"sanitizer_verification_min_length,omitempty" toml:"sanitizer_verification_min_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerVerificationMaxLength is the maximum length for verification codes
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerVerificationMaxLength *int `json:"sanitizer_verification_max_length" yaml:"sanitizer_verification_max_length,omitempty" toml:"sanitizer_verification_max_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerRealNameMinLength is the minimum length for first_name and last_name when required
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerRealNameMinLength *int `json:"sanitizer_real_name_min_length" yaml:"sanitizer_real_name_min_length,omitempty" toml:"sanitizer_real_name_min_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerRealNameMaxLength is the maximum length for first_name and last_name
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerRealNameMaxLength *int `json:"sanitizer_real_name_max_length" yaml:"sanitizer_real_name_max_length,omitempty" toml:"sanitizer_real_name_max_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// Contact form sanitizer configuration

	// SanitizerContactMinNameLength is the minimum length for contact form names
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMinNameLength *int `json:"sanitizer_contact_min_name_length" yaml:"sanitizer_contact_min_name_length,omitempty" toml:"sanitizer_contact_min_name_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerContactMaxNameLength is the maximum length for contact form names
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMaxNameLength *int `json:"sanitizer_contact_max_name_length" yaml:"sanitizer_contact_max_name_length,omitempty" toml:"sanitizer_contact_max_name_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerContactMinSubjectLength is the minimum length for contact form subjects
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMinSubjectLength *int `json:"sanitizer_contact_min_subject_length" yaml:"sanitizer_contact_min_subject_length,omitempty" toml:"sanitizer_contact_min_subject_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerContactMaxSubjectLength is the maximum length for contact form subjects
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMaxSubjectLength *int `json:"sanitizer_contact_max_subject_length" yaml:"sanitizer_contact_max_subject_length,omitempty" toml:"sanitizer_contact_max_subject_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerContactMinMessageLength is the minimum length for contact form messages
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMinMessageLength *int `json:"sanitizer_contact_min_message_length" yaml:"sanitizer_contact_min_message_length,omitempty" toml:"sanitizer_contact_min_message_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`
	// SanitizerContactMaxMessageLength is the maximum length for contact form messages
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	SanitizerContactMaxMessageLength *int `json:"sanitizer_contact_max_message_length" yaml:"sanitizer_contact_max_message_length,omitempty" toml:"sanitizer_contact_max_message_length,omitempty" kms:"group=sanitizer,reload=hot" kms_views:"sanitizer"`

	// API Layer configuration

	// ReadTimeout is the maximum duration for reading the entire request, including the body
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	ReadTimeout *time.Duration `json:"read_timeout" yaml:"read_timeout,omitempty" toml:"read_timeout,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// WriteTimeout is the maximum duration before timing out writes of the response
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	WriteTimeout *time.Duration `json:"write_timeout" yaml:"write_timeout,omitempty" toml:"write_timeout,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// IdleTimeout is the maximum duration to wait for the next request when keep-alives are enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	IdleTimeout *time.Duration `json:"idle_timeout" yaml:"idle_timeout,omitempty" toml:"idle_timeout,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// PostCommitHookTimeout is the deadline on the context passed to post-commit hooks
	// (cooperative: a hook that ignores its context can outlive it)
	// Use pointer to distinguish between "not configured" (nil) and explicit configuration
	PostCommitHookTimeout *time.Duration `json:"post_commit_hook_timeout" yaml:"post_commit_hook_timeout,omitempty" toml:"post_commit_hook_timeout,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// PostCommitHookFlushWait is how long the buffering transaction middleware holds a
	// committed response waiting for post-commit hooks before flushing it anyway and
	// leaving the hooks to finish in the background
	// Use pointer to distinguish between "not configured" (nil) and explicit configuration
	PostCommitHookFlushWait *time.Duration `json:"post_commit_hook_flush_wait" yaml:"post_commit_hook_flush_wait,omitempty" toml:"post_commit_hook_flush_wait,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// TransactionResponseBufferLimitBytes is the maximum response body retained in
	// memory by the default commit-before-flush transaction middleware. Responses
	// above the limit are discarded and rolled back. Large/export/streaming routes
	// must use the streaming transaction middleware instead.
	// Use pointer to distinguish between "not configured" (nil) and explicit configuration.
	TransactionResponseBufferLimitBytes *int64 `json:"transaction_response_buffer_limit_bytes" yaml:"transaction_response_buffer_limit_bytes,omitempty" toml:"transaction_response_buffer_limit_bytes,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// EnableCORS determines whether Cross-Origin Resource Sharing is enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableCORS *bool `json:"enable_cors" yaml:"enable_cors,omitempty" toml:"enable_cors,omitempty" kms:"group=features,reload=restart" kms_views:"features"`
	// SessionCookieName is the name of the HttpOnly session cookie.
	SessionCookieName *string `json:"session_cookie_name" yaml:"session_cookie_name,omitempty" toml:"session_cookie_name,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// SessionCookieDomain is the optional domain for the session cookie.
	SessionCookieDomain *string `json:"session_cookie_domain" yaml:"session_cookie_domain,omitempty" toml:"session_cookie_domain,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// SessionCookieSameSite controls the SameSite attribute for the session cookie (Strict, Lax, or None).
	SessionCookieSameSite *string `json:"session_cookie_same_site" yaml:"session_cookie_same_site,omitempty" toml:"session_cookie_same_site,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// SessionCookieSecure controls whether the session cookie is marked Secure.
	SessionCookieSecure *bool `json:"session_cookie_secure" yaml:"session_cookie_secure,omitempty" toml:"session_cookie_secure,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// SessionCookieTTL controls the Max-Age for the session cookie. Defaults to SessionValidity.
	SessionCookieTTL *time.Duration `json:"session_cookie_ttl" yaml:"session_cookie_ttl,omitempty" toml:"session_cookie_ttl,omitempty" kms:"group=server,reload=hot" kms_views:"server"`
	// PathPrefix is the URL path prefix for all API endpoints (e.g., "/api/v1")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	PathPrefix *string `json:"path_prefix" yaml:"path_prefix,omitempty" toml:"path_prefix,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// RemoteAddrHeaderName is the header to check for the real client IP (e.g., "X-Forwarded-For")
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	RemoteAddrHeaderName *string `json:"remote_addr_header_name" yaml:"remote_addr_header_name,omitempty" toml:"remote_addr_header_name,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// MaxPayloadSize is the maximum allowed size for request payloads in bytes
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	MaxPayloadSize *int64 `json:"max_payload_size" yaml:"max_payload_size,omitempty" toml:"max_payload_size,omitempty" kms:"group=server,reload=restart" kms_views:"server"`

	// CorsAllowedOrigins is a list of allowed origins for CORS requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	CorsAllowedOrigins []string `json:"cors_allowed_origins" yaml:"cors_allowed_origins,omitempty" toml:"cors_allowed_origins,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// CorsAllowedMethods is a list of allowed HTTP methods for CORS requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	CorsAllowedMethods []string `json:"cors_allowed_methods" yaml:"cors_allowed_methods,omitempty" toml:"cors_allowed_methods,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// CorsAllowedHeaders is a list of allowed headers in CORS requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	CorsAllowedHeaders []string `json:"cors_allowed_headers" yaml:"cors_allowed_headers,omitempty" toml:"cors_allowed_headers,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// CorsExposeHeaders is a list of headers exposed to the browser in CORS responses
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	CorsExposeHeaders []string `json:"cors_expose_headers" yaml:"cors_expose_headers,omitempty" toml:"cors_expose_headers,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// CorsAllowCredentials determines whether cookies and auth headers are allowed in CORS requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	CorsAllowCredentials *bool `json:"cors_allow_credentials" yaml:"cors_allow_credentials,omitempty" toml:"cors_allow_credentials,omitempty" kms:"group=server,reload=restart" kms_views:"server"`
	// CorsMaxAge is the duration for which CORS preflight requests can be cached
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	CorsMaxAge *time.Duration `json:"cors_max_age" yaml:"cors_max_age,omitempty" toml:"cors_max_age,omitempty" kms:"group=server,reload=restart" kms_views:"server"`

	// FrontendURL is the default frontend base URL used for authentication links and trusted
	// redirect/origin validation. Browser CORS is configured separately.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*"")
	FrontendURL *string `json:"frontend_url" yaml:"frontend_url,omitempty" toml:"frontend_url,omitempty" kms:"group=server,reload=hot" kms_views:"server"`

	// AllowedFrontendOrigins lists additional trusted frontend origins (e.g. "https://students.example.com")
	// accepted in per-request frontend_origin auth fields and OAuth state returnUrl values.
	// The origin of FrontendURL is always implicitly allowed.
	// This trust list does not configure browser CORS or session-cookie scope; multi-frontend
	// deployments must configure those independently.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly empty" (*[]string{})
	AllowedFrontendOrigins []string `json:"allowed_frontend_origins" yaml:"allowed_frontend_origins,omitempty" toml:"allowed_frontend_origins,omitempty" kms:"group=server,reload=hot" kms_views:"server"`

	// Rate Limiting configuration

	// RateLimitEnabled determines whether rate limiting is active
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	RateLimitEnabled *bool `json:"rate_limit_enabled" yaml:"rate_limit_enabled,omitempty" toml:"rate_limit_enabled,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// RateLimitCacheCapacity bounds in-memory cardinality for tracked rate-limit identifiers.
	RateLimitCacheCapacity *uint64 `json:"rate_limit_cache_capacity" yaml:"rate_limit_cache_capacity,omitempty" toml:"rate_limit_cache_capacity,omitempty" kms:"group=rate_limits,reload=restart" kms_views:"rate_limits"`
	// Login rate limiting

	// LoginMaxAttempts is the maximum number of login attempts allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	LoginMaxAttempts *int `json:"login_max_attempts" yaml:"login_max_attempts,omitempty" toml:"login_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// LoginWindowDuration is the time window for counting login attempts
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	LoginWindowDuration *time.Duration `json:"login_window_duration" yaml:"login_window_duration,omitempty" toml:"login_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// Registration rate limiting

	// RegisterMaxAttempts is the maximum number of registration attempts allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	RegisterMaxAttempts *int `json:"register_max_attempts" yaml:"register_max_attempts,omitempty" toml:"register_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// RegisterWindowDuration is the time window for counting registration attempts
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	RegisterWindowDuration *time.Duration `json:"register_window_duration" yaml:"register_window_duration,omitempty" toml:"register_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// Password reset rate limiting

	// PasswordResetMaxAttempts is the maximum number of password reset requests allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PasswordResetMaxAttempts *int `json:"password_reset_max_attempts" yaml:"password_reset_max_attempts,omitempty" toml:"password_reset_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// PasswordResetWindowDuration is the time window for counting password reset requests
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PasswordResetWindowDuration *time.Duration `json:"password_reset_window_duration" yaml:"password_reset_window_duration,omitempty" toml:"password_reset_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// Phone OTP verification rate limiting

	// VerifyPhoneOTPMaxAttempts is the maximum number of phone OTP verification attempts allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	VerifyPhoneOTPMaxAttempts *int `json:"verify_phone_otp_max_attempts" yaml:"verify_phone_otp_max_attempts,omitempty" toml:"verify_phone_otp_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// VerifyPhoneOTPWindowDuration is the time window for counting phone OTP verification attempts
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	VerifyPhoneOTPWindowDuration *time.Duration `json:"verify_phone_otp_window_duration" yaml:"verify_phone_otp_window_duration,omitempty" toml:"verify_phone_otp_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// Contact form rate limiting

	// ContactMaxAttempts is the maximum number of contact form submissions allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	ContactMaxAttempts *int `json:"contact_max_attempts" yaml:"contact_max_attempts,omitempty" toml:"contact_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// ContactWindowDuration is the time window for counting contact form submissions
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	ContactWindowDuration *time.Duration `json:"contact_window_duration" yaml:"contact_window_duration,omitempty" toml:"contact_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// CareerApplicationMaxAttempts is the maximum number of career application submissions allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	CareerApplicationMaxAttempts *int `json:"career_application_max_attempts" yaml:"career_application_max_attempts,omitempty" toml:"career_application_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// CareerApplicationWindowDuration is the time window for counting career application submissions
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	CareerApplicationWindowDuration *time.Duration `json:"career_application_window_duration" yaml:"career_application_window_duration,omitempty" toml:"career_application_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// CareerJobLookupMaxAttempts is the maximum number of public career job lookups allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	CareerJobLookupMaxAttempts *int `json:"career_job_lookup_max_attempts" yaml:"career_job_lookup_max_attempts,omitempty" toml:"career_job_lookup_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// CareerJobLookupWindowDuration is the time window for counting public career job lookups
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	CareerJobLookupWindowDuration *time.Duration `json:"career_job_lookup_window_duration" yaml:"career_job_lookup_window_duration,omitempty" toml:"career_job_lookup_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// PublicChatReadMaxAttempts is the maximum number of logged-out public chat reads allowed per IP within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PublicChatReadMaxAttempts *int `json:"public_chat_read_max_attempts" yaml:"public_chat_read_max_attempts,omitempty" toml:"public_chat_read_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// PublicChatReadWindowDuration is the time window for counting logged-out public chat reads
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	PublicChatReadWindowDuration *time.Duration `json:"public_chat_read_window_duration" yaml:"public_chat_read_window_duration,omitempty" toml:"public_chat_read_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// OrganizationSlugAvailabilityMaxAttempts is the maximum number of public slug checks allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	OrganizationSlugAvailabilityMaxAttempts *int `json:"organization_slug_availability_max_attempts" yaml:"organization_slug_availability_max_attempts,omitempty" toml:"organization_slug_availability_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// OrganizationSlugAvailabilityWindowDuration is the time window for counting public slug checks
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	OrganizationSlugAvailabilityWindowDuration *time.Duration `json:"organization_slug_availability_window_duration" yaml:"organization_slug_availability_window_duration,omitempty" toml:"organization_slug_availability_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// NotificationCountMaxAttempts is the maximum number of unread-count requests one user may make within the window (default 60).
	NotificationCountMaxAttempts *int `json:"notification_count_max_attempts" yaml:"notification_count_max_attempts,omitempty" toml:"notification_count_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// NotificationCountWindowDuration is the time window for counting unread-count requests (default 1m).
	NotificationCountWindowDuration *time.Duration `json:"notification_count_window_duration" yaml:"notification_count_window_duration,omitempty" toml:"notification_count_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// AgentSessionControlMaxAttempts is the maximum number of agent Stop and Resume requests one user may make within the window (default 30).
	AgentSessionControlMaxAttempts *int `json:"agent_session_control_max_attempts" yaml:"agent_session_control_max_attempts,omitempty" toml:"agent_session_control_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// AgentSessionControlWindowDuration is the time window for counting agent Stop and Resume requests (default 1m).
	AgentSessionControlWindowDuration *time.Duration `json:"agent_session_control_window_duration" yaml:"agent_session_control_window_duration,omitempty" toml:"agent_session_control_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// ChatForkMaxAttempts is the maximum number of chat channel forks one user may request within the window (default 10).
	ChatForkMaxAttempts *int `json:"chat_fork_max_attempts" yaml:"chat_fork_max_attempts,omitempty" toml:"chat_fork_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// ChatForkWindowDuration is the time window for counting chat channel fork requests (default 1m).
	ChatForkWindowDuration *time.Duration `json:"chat_fork_window_duration" yaml:"chat_fork_window_duration,omitempty" toml:"chat_fork_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// OAuthCallbackMaxAttempts is the maximum number of OAuth callbacks and form_post relays one client IP may make within the window (default 20).
	OAuthCallbackMaxAttempts *int `json:"oauth_callback_max_attempts" yaml:"oauth_callback_max_attempts,omitempty" toml:"oauth_callback_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// OAuthCallbackWindowDuration is the time window for counting OAuth callbacks and form_post relays (default 1m).
	OAuthCallbackWindowDuration *time.Duration `json:"oauth_callback_window_duration" yaml:"oauth_callback_window_duration,omitempty" toml:"oauth_callback_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`

	// Module enablement configuration
	// These flags allow selective enabling/disabling of go-common modules

	// EnableEmailAuth determines whether email-based authentication endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableEmailAuth *bool `json:"enable_email_auth" yaml:"enable_email_auth,omitempty" toml:"enable_email_auth,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableOAuth determines whether OAuth authentication endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableOAuth *bool `json:"enable_oauth" yaml:"enable_oauth,omitempty" toml:"enable_oauth,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableContacts determines whether contact form endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableContacts *bool `json:"enable_contacts" yaml:"enable_contacts,omitempty" toml:"enable_contacts,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableAttachments determines whether file attachment endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableAttachments *bool `json:"enable_attachments" yaml:"enable_attachments,omitempty" toml:"enable_attachments,omitempty" kms:"group=attachments,reload=hot" kms_views:"attachments"`
	// EnableUserSettings determines whether user settings endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableUserSettings *bool `json:"enable_user_settings" yaml:"enable_user_settings,omitempty" toml:"enable_user_settings,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableUserProfiles determines whether user profile endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableUserProfiles *bool `json:"enable_user_profiles" yaml:"enable_user_profiles,omitempty" toml:"enable_user_profiles,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableUserSearch determines whether the user search endpoint is enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableUserSearch *bool `json:"enable_user_search" yaml:"enable_user_search,omitempty" toml:"enable_user_search,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// EnableFaro determines whether Faro telemetry endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableFaro *bool `json:"enable_faro" yaml:"enable_faro,omitempty" toml:"enable_faro,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// FaroMaxPayloadSize is the maximum allowed size in bytes for a single Faro telemetry
	// payload. The Faro collect endpoint is unauthenticated, so it gets its own cap instead
	// of inheriting the much larger MaxPayloadSize sized for attachment uploads.
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	FaroMaxPayloadSize *int64 `json:"faro_max_payload_size" yaml:"faro_max_payload_size,omitempty" toml:"faro_max_payload_size,omitempty" kms:"group=telemetry,reload=restart" kms_views:"telemetry"`
	// EnableAdminAPI determines whether admin API endpoints are registered
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableAdminAPI *bool `json:"enable_admin_api" yaml:"enable_admin_api,omitempty" toml:"enable_admin_api,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableOrganizations determines whether organization functionality is enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableOrganizations *bool `json:"enable_organizations" yaml:"enable_organizations,omitempty" toml:"enable_organizations,omitempty" kms:"group=organizations,reload=restart" kms_views:"organizations"`
	// EnablePersonalWorkspaces provisions a managed personal organization on account creation.
	// Organizations may remain enabled while automatic personal-workspace creation is disabled.
	EnablePersonalWorkspaces *bool `json:"enable_personal_workspaces" yaml:"enable_personal_workspaces,omitempty" toml:"enable_personal_workspaces,omitempty" kms:"group=organizations,reload=restart" kms_views:"organizations"`
	// EnableCareers determines whether careers functionality is enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableCareers *bool `json:"enable_careers" yaml:"enable_careers,omitempty" toml:"enable_careers,omitempty" kms:"group=careers,reload=hot" kms_views:"careers"`

	// User search configuration

	// UserSearchRequireSameOrganization controls org-scoped searches only.
	// When true, users must be a member of the target organization to search within it.
	// When false, any user meeting UserSearchMinUserType can search any organization.
	// This setting has no effect on global-scope searches.
	UserSearchRequireSameOrganization *bool `json:"user_search_require_same_organization" yaml:"user_search_require_same_organization,omitempty" toml:"user_search_require_same_organization,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchMinUserType controls global-scope searches only.
	// Users with a UserType below this threshold cannot perform global searches.
	// They can still perform org-scoped searches if they meet the org role requirements.
	// Set to a high tier (e.g., ADMIN_TIER_0) to restrict global search to admins.
	// Note: There is no admin bypass; all users are subject to these permission checks.
	UserSearchMinUserType *proto_models.UserType `json:"user_search_min_user_type" yaml:"user_search_min_user_type,omitempty" toml:"user_search_min_user_type,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchMinOrgRole controls org-scoped searches for organization members by org depth.
	// When a user IS a member of the target organization, their role must be >= this threshold.
	// If they are NOT a member, UserSearchRequireSameOrganization determines access instead.
	// Index i represents the threshold at org depth i.
	UserSearchMinOrgRole []proto_models.OrganizationRole `json:"user_search_min_org_role" yaml:"user_search_min_org_role,omitempty" toml:"user_search_min_org_role,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchMinQueryLength is the minimum query length for searches
	UserSearchMinQueryLength *int `json:"user_search_min_query_length" yaml:"user_search_min_query_length,omitempty" toml:"user_search_min_query_length,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchMaxQueryLength is the maximum query length for searches
	UserSearchMaxQueryLength *int `json:"user_search_max_query_length" yaml:"user_search_max_query_length,omitempty" toml:"user_search_max_query_length,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchEnableSuggestions enables suggestions when query is empty
	UserSearchEnableSuggestions *bool `json:"user_search_enable_suggestions" yaml:"user_search_enable_suggestions,omitempty" toml:"user_search_enable_suggestions,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchAllowEmailLookup controls whether email is searchable
	UserSearchAllowEmailLookup *bool `json:"user_search_allow_email_lookup" yaml:"user_search_allow_email_lookup,omitempty" toml:"user_search_allow_email_lookup,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchAllowUsernameLookup controls whether username is searchable
	UserSearchAllowUsernameLookup *bool `json:"user_search_allow_username_lookup" yaml:"user_search_allow_username_lookup,omitempty" toml:"user_search_allow_username_lookup,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchAllowRealNameLookup controls whether real name is searchable
	UserSearchAllowRealNameLookup *bool `json:"user_search_allow_real_name_lookup" yaml:"user_search_allow_real_name_lookup,omitempty" toml:"user_search_allow_real_name_lookup,omitempty" kms:"group=user_search,reload=hot" kms_views:"user_search"`
	// UserSearchMaxAttempts is the maximum number of search attempts allowed within the window
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	UserSearchMaxAttempts *int `json:"user_search_max_attempts" yaml:"user_search_max_attempts,omitempty" toml:"user_search_max_attempts,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`
	// UserSearchWindowDuration is the time window for counting search attempts
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*0)
	UserSearchWindowDuration *time.Duration `json:"user_search_window_duration" yaml:"user_search_window_duration,omitempty" toml:"user_search_window_duration,omitempty" kms:"group=rate_limits,reload=hot" kms_views:"rate_limits"`

	// Chat Module Configuration

	// EnableChat determines whether chat/messaging functionality is enabled
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	EnableChat *bool `json:"enable_chat" yaml:"enable_chat,omitempty" toml:"enable_chat,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// EnablePublicChatRead lets channels whose public_read level is SIGNED_IN or ANYONE be read beyond
	// their participants. Off, every channel's public_read is ignored; turning it off cuts off public
	// readers on their next request or subscription without rewriting any channel. Existing signed-in
	// realtime viewers lose access at their next grant renewal (within 60 seconds); lower the channel's
	// level to revoke them immediately. Defaults to false. Anonymous reads are limited per IP by
	// public_chat_read only while rate_limits.rate_limit_enabled is on (default false); enable it first.
	EnablePublicChatRead *bool `json:"enable_public_chat_read" yaml:"enable_public_chat_read,omitempty" toml:"enable_public_chat_read,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// EnableNotifications gates the durable notification inbox and API at each operation.
	EnableNotifications *bool `json:"enable_notifications" yaml:"enable_notifications,omitempty" toml:"enable_notifications,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationPolicies override interruption policy by canonical decimal type ID.
	NotificationPolicies map[string]NotificationPolicy `json:"notification_policies" yaml:"notification_policies,omitempty" toml:"notification_policies,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutAsync sets the default durable inbox fan-out mode; false or unset is inline, and types may override it.
	// Inline paging retains recipient advisory locks across the entire source transaction; large audiences can exhaust
	// PostgreSQL lock memory and roll back the source operation. Use true for large audiences: async workers commit
	// bounded batches separately, releasing their locks after each batch. Per-type inline overrides retain this limitation.
	NotificationFanoutAsync *bool `json:"notification_fanout_async" yaml:"notification_fanout_async,omitempty" toml:"notification_fanout_async,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatUnreadFanoutMode selects inline or async for unread chat; empty inherits NotificationFanoutAsync.
	// Inline retains recipient locks until source commit; use async for large audiences to avoid the lock-memory limitation described on NotificationFanoutAsync.
	NotificationChatUnreadFanoutMode string `json:"notification_chat_unread_fanout_mode" yaml:"notification_chat_unread_fanout_mode,omitempty" toml:"notification_chat_unread_fanout_mode,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatApprovalRequiredFanoutMode selects inline or async for tool approvals; empty inherits NotificationFanoutAsync.
	// Inline retains recipient locks until source commit; use async for large audiences to avoid the lock-memory limitation described on NotificationFanoutAsync.
	NotificationChatApprovalRequiredFanoutMode string `json:"notification_chat_approval_required_fanout_mode" yaml:"notification_chat_approval_required_fanout_mode,omitempty" toml:"notification_chat_approval_required_fanout_mode,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutWorkerCount sets process fan-out capacity (1–64; default 1).
	NotificationFanoutWorkerCount *int `json:"notification_fanout_worker_count" yaml:"notification_fanout_worker_count,omitempty" toml:"notification_fanout_worker_count,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutPaused stops new durable fan-out claims without disabling realtime or email delivery.
	NotificationFanoutPaused *bool `json:"notification_fanout_paused" yaml:"notification_fanout_paused,omitempty" toml:"notification_fanout_paused,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutStepBudget bounds one claim, materialization step, backlog sample, compaction pass, retirement page, and retirement pass (1s–60s; default 10s).
	NotificationFanoutStepBudget *time.Duration `json:"notification_fanout_step_budget" yaml:"notification_fanout_step_budget,omitempty" toml:"notification_fanout_step_budget,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutLease is the database lease taken by each fan-out claim (at least the step budget plus 30s, at most 30m; default 2m).
	NotificationFanoutLease *time.Duration `json:"notification_fanout_lease" yaml:"notification_fanout_lease,omitempty" toml:"notification_fanout_lease,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutBatchRecipients caps recipients materialized per step before the payload byte bound (1–1,000; default 100).
	NotificationFanoutBatchRecipients *int `json:"notification_fanout_batch_recipients" yaml:"notification_fanout_batch_recipients,omitempty" toml:"notification_fanout_batch_recipients,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutMaintenanceInterval spaces backlog samples independently of queue deadlines (1s–5m; default 5m).
	NotificationFanoutMaintenanceInterval *time.Duration `json:"notification_fanout_maintenance_interval" yaml:"notification_fanout_maintenance_interval,omitempty" toml:"notification_fanout_maintenance_interval,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutPollInterval is the coordinator's memory-only configuration check and minimum contention/error retry delay (100ms–30s; default 1s).
	NotificationFanoutPollInterval *time.Duration `json:"notification_fanout_poll_interval" yaml:"notification_fanout_poll_interval,omitempty" toml:"notification_fanout_poll_interval,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutMaxAttempts is the number of failed claims after which an emission is exhausted (1–20; default 5).
	NotificationFanoutMaxAttempts *int `json:"notification_fanout_max_attempts" yaml:"notification_fanout_max_attempts,omitempty" toml:"notification_fanout_max_attempts,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementPageSize is the number of members retired per chat retirement page transaction, and of recipient/type pairs per scope-wide retirement page (1–1,000; default 100). The other notification_chat_retirement_* keys also govern scope-wide retirement jobs.
	NotificationChatRetirementPageSize *int `json:"notification_chat_retirement_page_size" yaml:"notification_chat_retirement_page_size,omitempty" toml:"notification_chat_retirement_page_size,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationRetirementMaxRecipients caps the unique recipient/type pairs one activity retirement transaction may lock; larger calls are rejected before any lock (100–100,000; default 2,000). Each pair holds an advisory lock until commit in PostgreSQL's shared lock table of max_locks_per_transaction × (max_connections + max_prepared_transactions) slots (6,400 at defaults) that every session shares, so raising it requires raising max_locks_per_transaction. Chat and scope-wide retirement pages shrink to fit it; scope-wide retirement itself is not capped.
	NotificationRetirementMaxRecipients *int `json:"notification_retirement_max_recipients" yaml:"notification_retirement_max_recipients,omitempty" toml:"notification_retirement_max_recipients,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationSourceResolutionPageSize is the number of inbox entries one source resolution settles synchronously; a larger audience persists a resolution job instead, and each job page settles at most this many entries, also bounded by the chat retirement page size and recipient cap (1–1,000; default 64).
	NotificationSourceResolutionPageSize *int `json:"notification_source_resolution_page_size" yaml:"notification_source_resolution_page_size,omitempty" toml:"notification_source_resolution_page_size,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationRetirementClaimCandidates is the number of due recipient retirement jobs one claim probes, rotating past jobs whose recipient is locked so a busy recipient never blocks the rest (1–1,000; default 32).
	NotificationRetirementClaimCandidates *int `json:"notification_retirement_claim_candidates" yaml:"notification_retirement_claim_candidates,omitempty" toml:"notification_retirement_claim_candidates,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutCompactionRetention is how long completed or cancelled emissions keep their payloads before compaction (at least 24h; default 720h).
	NotificationFanoutCompactionRetention *time.Duration `json:"notification_fanout_compaction_retention" yaml:"notification_fanout_compaction_retention,omitempty" toml:"notification_fanout_compaction_retention,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutCompactionBatch is the number of emissions compacted per transaction (1–1,000; default 100).
	NotificationFanoutCompactionBatch *int `json:"notification_fanout_compaction_batch" yaml:"notification_fanout_compaction_batch,omitempty" toml:"notification_fanout_compaction_batch,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutBacklogCountLimit bounds the pending-emission count query run on each backlog sample; a backlog above it reports a saturated lower bound (1,000–10,000,000; default 100,000).
	NotificationFanoutBacklogCountLimit *int `json:"notification_fanout_backlog_count_limit" yaml:"notification_fanout_backlog_count_limit,omitempty" toml:"notification_fanout_backlog_count_limit,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationInlineSnapshotMaxPages caps the producer snapshot pages one inline event emission may enumerate before it fails and rolls back its source operation (1–10,000; default 1,000).
	NotificationInlineSnapshotMaxPages *int `json:"notification_inline_snapshot_max_pages" yaml:"notification_inline_snapshot_max_pages,omitempty" toml:"notification_inline_snapshot_max_pages,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryWorkerCount sets the per-process delivery (email and live-alert) worker count, captured when the notification workers start (1–64; default 4).
	NotificationDeliveryWorkerCount *int `json:"notification_delivery_worker_count" yaml:"notification_delivery_worker_count,omitempty" toml:"notification_delivery_worker_count,omitempty" kms:"group=notifications,reload=restart" kms_views:"notifications"`
	// NotificationDeliveryPollInterval is the delivery coordinator's memory-only configuration check and minimum contention/error retry delay (100ms–30s; default 1s).
	NotificationDeliveryPollInterval *time.Duration `json:"notification_delivery_poll_interval" yaml:"notification_delivery_poll_interval,omitempty" toml:"notification_delivery_poll_interval,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryClaimErrorMaxBackoff caps the exponential backoff after failed delivery claims (1s–5m and at least the poll interval; default 30s).
	NotificationDeliveryClaimErrorMaxBackoff *time.Duration `json:"notification_delivery_claim_error_max_backoff" yaml:"notification_delivery_claim_error_max_backoff,omitempty" toml:"notification_delivery_claim_error_max_backoff,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationWorkerShutdownTimeout bounds how long Stop waits for delivery and fan-out workers to drain, captured at construction (1s–5m; default 5s).
	NotificationWorkerShutdownTimeout *time.Duration `json:"notification_worker_shutdown_timeout" yaml:"notification_worker_shutdown_timeout,omitempty" toml:"notification_worker_shutdown_timeout,omitempty" kms:"group=notifications,reload=restart" kms_views:"notifications"`
	// NotificationDeliveryClaimTimeout bounds each delivery claim database call (1s–1m; default 10s).
	NotificationDeliveryClaimTimeout *time.Duration `json:"notification_delivery_claim_timeout" yaml:"notification_delivery_claim_timeout,omitempty" toml:"notification_delivery_claim_timeout,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryLease is the lease taken by each delivery claim (at least the work budget plus the completion margin, at most 30m; default 2m).
	NotificationDeliveryLease *time.Duration `json:"notification_delivery_lease" yaml:"notification_delivery_lease,omitempty" toml:"notification_delivery_lease,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryMaxAttempts is the number of delivery attempts after which a job is exhausted, used at claim and at completion (1–20; default 5).
	NotificationDeliveryMaxAttempts *int `json:"notification_delivery_max_attempts" yaml:"notification_delivery_max_attempts,omitempty" toml:"notification_delivery_max_attempts,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryPollJitterFraction is the positive jitter added to delivery contention/error retries as a fraction of the retry interval (0–1; default 0.2).
	NotificationDeliveryPollJitterFraction *float64 `json:"notification_delivery_poll_jitter_fraction" yaml:"notification_delivery_poll_jitter_fraction,omitempty" toml:"notification_delivery_poll_jitter_fraction,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryWorkBudget bounds rendering and sending one delivery before the lease-derived clamp (1s–10m; default 10s).
	NotificationDeliveryWorkBudget *time.Duration `json:"notification_delivery_work_budget" yaml:"notification_delivery_work_budget,omitempty" toml:"notification_delivery_work_budget,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryLeaseCompletionMargin is the time reserved before lease expiry for the delivery completion write (1s–1m; default 5s).
	NotificationDeliveryLeaseCompletionMargin *time.Duration `json:"notification_delivery_lease_completion_margin" yaml:"notification_delivery_lease_completion_margin,omitempty" toml:"notification_delivery_lease_completion_margin,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryRetryBackoffBase is the delay after the first failed delivery attempt, doubled per later failure (100ms–1m; default 1s).
	NotificationDeliveryRetryBackoffBase *time.Duration `json:"notification_delivery_retry_backoff_base" yaml:"notification_delivery_retry_backoff_base,omitempty" toml:"notification_delivery_retry_backoff_base,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryRetryBackoffMax caps the delivery retry delay (at least the base, at most 1h; default 64s).
	NotificationDeliveryRetryBackoffMax *time.Duration `json:"notification_delivery_retry_backoff_max" yaml:"notification_delivery_retry_backoff_max,omitempty" toml:"notification_delivery_retry_backoff_max,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryActiveViewDefer defers an email while its recipient is actively viewing the scope (1s–10m; default 30s).
	NotificationDeliveryActiveViewDefer *time.Duration `json:"notification_delivery_active_view_defer" yaml:"notification_delivery_active_view_defer,omitempty" toml:"notification_delivery_active_view_defer,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryDisabledDefer defers a delivery claimed while notifications are disabled (1s–10m; default 30s).
	NotificationDeliveryDisabledDefer *time.Duration `json:"notification_delivery_disabled_defer" yaml:"notification_delivery_disabled_defer,omitempty" toml:"notification_delivery_disabled_defer,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryTypeUnavailableDefer defers a delivery whose type this worker has not registered, as during a rolling deploy (1s–10m; default 30s).
	NotificationDeliveryTypeUnavailableDefer *time.Duration `json:"notification_delivery_type_unavailable_defer" yaml:"notification_delivery_type_unavailable_defer,omitempty" toml:"notification_delivery_type_unavailable_defer,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryRevisionChangedDefer defers a delivery whose notification revision changed while it rendered (100ms–1m; default 1s).
	NotificationDeliveryRevisionChangedDefer *time.Duration `json:"notification_delivery_revision_changed_defer" yaml:"notification_delivery_revision_changed_defer,omitempty" toml:"notification_delivery_revision_changed_defer,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationAlertMinAge is the grace a live alert's source must age before it may be claimed (1ms–5s; default 1s).
	NotificationAlertMinAge *time.Duration `json:"notification_alert_min_age" yaml:"notification_alert_min_age,omitempty" toml:"notification_alert_min_age,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationAlertMaxAge is the staleness after which a live alert is no longer delivered (greater than the min age, 1s–10m; default 30s).
	NotificationAlertMaxAge *time.Duration `json:"notification_alert_max_age" yaml:"notification_alert_max_age,omitempty" toml:"notification_alert_max_age,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryCompletionTimeout bounds each delivery completion write and the shutdown cleanup budget (1s–1m; default 5s).
	NotificationDeliveryCompletionTimeout *time.Duration `json:"notification_delivery_completion_timeout" yaml:"notification_delivery_completion_timeout,omitempty" toml:"notification_delivery_completion_timeout,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryCompletionRetryBackoffBase is the first delay between retries of the completion write after a successful send (10ms–5s; default 50ms).
	NotificationDeliveryCompletionRetryBackoffBase *time.Duration `json:"notification_delivery_completion_retry_backoff_base" yaml:"notification_delivery_completion_retry_backoff_base,omitempty" toml:"notification_delivery_completion_retry_backoff_base,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationDeliveryCompletionRetryBackoffMax caps the completion-write retry delay (at least the base, at most 30s; default 500ms).
	NotificationDeliveryCompletionRetryBackoffMax *time.Duration `json:"notification_delivery_completion_retry_backoff_max" yaml:"notification_delivery_completion_retry_backoff_max,omitempty" toml:"notification_delivery_completion_retry_backoff_max,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutClaimErrorMaxBackoff caps the exponential backoff after failed fan-out claims (1s–5m and at least the fan-out poll interval; default 30s).
	NotificationFanoutClaimErrorMaxBackoff *time.Duration `json:"notification_fanout_claim_error_max_backoff" yaml:"notification_fanout_claim_error_max_backoff,omitempty" toml:"notification_fanout_claim_error_max_backoff,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutCompactionInterval spaces fan-out compaction runs once the compaction backlog is drained (1s–1h; default 5m).
	NotificationFanoutCompactionInterval *time.Duration `json:"notification_fanout_compaction_interval" yaml:"notification_fanout_compaction_interval,omitempty" toml:"notification_fanout_compaction_interval,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutRetryBackoffBase is the delay after the first failed fan-out emission attempt, doubled per later failure (100ms–1m; default 1s).
	NotificationFanoutRetryBackoffBase *time.Duration `json:"notification_fanout_retry_backoff_base" yaml:"notification_fanout_retry_backoff_base,omitempty" toml:"notification_fanout_retry_backoff_base,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutRetryBackoffMax caps the fan-out emission retry delay (at least the base, at most 1h; default 64s).
	NotificationFanoutRetryBackoffMax *time.Duration `json:"notification_fanout_retry_backoff_max" yaml:"notification_fanout_retry_backoff_max,omitempty" toml:"notification_fanout_retry_backoff_max,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutDeferredRetryDelay is the retry delay for emissions deferred because notifications are disabled or the producer is unavailable (1s–10m; default 30s).
	NotificationFanoutDeferredRetryDelay *time.Duration `json:"notification_fanout_deferred_retry_delay" yaml:"notification_fanout_deferred_retry_delay,omitempty" toml:"notification_fanout_deferred_retry_delay,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationFanoutFinishTimeout bounds the write that finishes one fan-out emission attempt (1s–1m; default 5s).
	NotificationFanoutFinishTimeout *time.Duration `json:"notification_fanout_finish_timeout" yaml:"notification_fanout_finish_timeout,omitempty" toml:"notification_fanout_finish_timeout,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationSourceBatchRows caps candidates per application source-authorization callback batch (1–1,000; default 64).
	NotificationSourceBatchRows *int `json:"notification_source_batch_rows" yaml:"notification_source_batch_rows,omitempty" toml:"notification_source_batch_rows,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationSourceBatchBytes caps payload bytes per application source-authorization callback batch (at least the max payload bytes, at most 16 MiB; default 4 MiB).
	NotificationSourceBatchBytes *int `json:"notification_source_batch_bytes" yaml:"notification_source_batch_bytes,omitempty" toml:"notification_source_batch_bytes,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationMaxPayloadBytes caps one notification payload in bytes (1–1 MiB; default 1 MiB; the ceiling keeps inbox hydration within 16 MiB).
	NotificationMaxPayloadBytes *int `json:"notification_max_payload_bytes" yaml:"notification_max_payload_bytes,omitempty" toml:"notification_max_payload_bytes,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationMaxFallbackTextBytes caps one notification's fallback text in bytes (1–4,096; default 4,096).
	NotificationMaxFallbackTextBytes *int `json:"notification_max_fallback_text_bytes" yaml:"notification_max_fallback_text_bytes,omitempty" toml:"notification_max_fallback_text_bytes,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationMaxMetadataBytes caps the metadata bytes of one producer candidate (1–65,536; default 65,536). Producer cursors use the fixed MaxNotificationProducerCursorBytes protocol cap, not this key.
	NotificationMaxMetadataBytes *int `json:"notification_max_metadata_bytes" yaml:"notification_max_metadata_bytes,omitempty" toml:"notification_max_metadata_bytes,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationEmitMaxTotalBytes caps the stored bytes (payload, fallback text, delivery tag) of one EmitMany call in either fan-out mode (at least max payload + max fallback text + the 64-byte delivery tag maximum, at most 16 MiB; default 16 MiB).
	NotificationEmitMaxTotalBytes *int `json:"notification_emit_max_total_bytes" yaml:"notification_emit_max_total_bytes,omitempty" toml:"notification_emit_max_total_bytes,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementMaxAttempts is the number of consecutive failed pages after which a chat retirement job is exhausted (1–20; default 5).
	NotificationChatRetirementMaxAttempts *int `json:"notification_chat_retirement_max_attempts" yaml:"notification_chat_retirement_max_attempts,omitempty" toml:"notification_chat_retirement_max_attempts,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementExhaustedDelay is the retry delay for an exhausted chat retirement job (1m–24h; default 1h).
	NotificationChatRetirementExhaustedDelay *time.Duration `json:"notification_chat_retirement_exhausted_delay" yaml:"notification_chat_retirement_exhausted_delay,omitempty" toml:"notification_chat_retirement_exhausted_delay,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementRetryBackoffBase is the first chat retirement failure retry delay, doubled per failure (100ms–1m; default 1s).
	NotificationChatRetirementRetryBackoffBase *time.Duration `json:"notification_chat_retirement_retry_backoff_base" yaml:"notification_chat_retirement_retry_backoff_base,omitempty" toml:"notification_chat_retirement_retry_backoff_base,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementRetryBackoffMax caps the chat retirement failure retry delay (at least the base, at most 1h; default 60s).
	NotificationChatRetirementRetryBackoffMax *time.Duration `json:"notification_chat_retirement_retry_backoff_max" yaml:"notification_chat_retirement_retry_backoff_max,omitempty" toml:"notification_chat_retirement_retry_backoff_max,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationChatRetirementRecordFailureTimeout bounds the write that records a chat retirement page failure (1s–1m; default 5s).
	NotificationChatRetirementRecordFailureTimeout *time.Duration `json:"notification_chat_retirement_record_failure_timeout" yaml:"notification_chat_retirement_record_failure_timeout,omitempty" toml:"notification_chat_retirement_record_failure_timeout,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationInboxFetchRows sets the maximum rows hydrated per inbox fetch (1–16; at most 16 MiB of persisted payloads).
	NotificationInboxFetchRows *int `json:"notification_inbox_fetch_rows" yaml:"notification_inbox_fetch_rows,omitempty" toml:"notification_inbox_fetch_rows,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationListCandidateBudget sets the maximum candidates examined per inbox list operation.
	NotificationListCandidateBudget *int `json:"notification_list_candidate_budget" yaml:"notification_list_candidate_budget,omitempty" toml:"notification_list_candidate_budget,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationCountOperationTimeout sets the time budget for one unread count operation.
	NotificationCountOperationTimeout *time.Duration `json:"notification_count_operation_timeout" yaml:"notification_count_operation_timeout,omitempty" toml:"notification_count_operation_timeout,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationCountUnreadLimit caps the unread count; reaching it returns a confirmed lower bound.
	NotificationCountUnreadLimit *int `json:"notification_count_unread_limit" yaml:"notification_count_unread_limit,omitempty" toml:"notification_count_unread_limit,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationCountCandidateBudget sets the maximum candidates examined per unread count operation.
	NotificationCountCandidateBudget *int `json:"notification_count_candidate_budget" yaml:"notification_count_candidate_budget,omitempty" toml:"notification_count_candidate_budget,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationListDefaultLimit is the inbox page size used when a list request omits its limit (1–1,000, at most notification_list_max_limit; default 30).
	NotificationListDefaultLimit *int `json:"notification_list_default_limit" yaml:"notification_list_default_limit,omitempty" toml:"notification_list_default_limit,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationListMaxLimit is the largest inbox page size a list request may ask for (1–1,000; default 100).
	NotificationListMaxLimit *int `json:"notification_list_max_limit" yaml:"notification_list_max_limit,omitempty" toml:"notification_list_max_limit,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationMaxReadReceipts caps read receipts accepted in one mark-read request (1–1,000; default 100).
	NotificationMaxReadReceipts *int `json:"notification_max_read_receipts" yaml:"notification_max_read_receipts,omitempty" toml:"notification_max_read_receipts,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`
	// NotificationMaxPreferenceUpdates caps preference entries accepted in one update request (1–1,000; default 100).
	NotificationMaxPreferenceUpdates *int `json:"notification_max_preference_updates" yaml:"notification_max_preference_updates,omitempty" toml:"notification_max_preference_updates,omitempty" kms:"group=notifications,reload=hot" kms_views:"notifications"`

	// Chat capability flags (for API gating and UI feature discovery)

	// ChatSupportReplies enables Discord-style reply references
	ChatSupportReplies *bool `json:"chat_support_replies" yaml:"chat_support_replies,omitempty" toml:"chat_support_replies,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportThreads enables thread grouping
	ChatSupportThreads *bool `json:"chat_support_threads" yaml:"chat_support_threads,omitempty" toml:"chat_support_threads,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportMessageEdits enables message editing
	ChatSupportMessageEdits *bool `json:"chat_support_message_edits" yaml:"chat_support_message_edits,omitempty" toml:"chat_support_message_edits,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportMessageDeletes enables message soft deletion
	ChatSupportMessageDeletes *bool `json:"chat_support_message_deletes" yaml:"chat_support_message_deletes,omitempty" toml:"chat_support_message_deletes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportMessageReactions enables emoji reactions on messages
	ChatSupportMessageReactions *bool `json:"chat_support_message_reactions" yaml:"chat_support_message_reactions,omitempty" toml:"chat_support_message_reactions,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAllowMultipleReactionsPerUser controls whether a user may react with multiple different emojis on the same message
	ChatAllowMultipleReactionsPerUser *bool `json:"chat_allow_multiple_reactions_per_user" yaml:"chat_allow_multiple_reactions_per_user,omitempty" toml:"chat_allow_multiple_reactions_per_user,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportEmbeds enables rich embeds in messages
	ChatSupportEmbeds *bool `json:"chat_support_embeds" yaml:"chat_support_embeds,omitempty" toml:"chat_support_embeds,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportAttachments enables file attachments in chat messages
	ChatSupportAttachments *bool `json:"chat_support_attachments" yaml:"chat_support_attachments,omitempty" toml:"chat_support_attachments,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAllowNativeEmbeds controls whether native embed types (1-999) are accepted
	ChatAllowNativeEmbeds *bool `json:"chat_allow_native_embeds" yaml:"chat_allow_native_embeds,omitempty" toml:"chat_allow_native_embeds,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAllowCustomEmbeds controls whether custom embed types (1000+) are accepted
	ChatAllowCustomEmbeds *bool `json:"chat_allow_custom_embeds" yaml:"chat_allow_custom_embeds,omitempty" toml:"chat_allow_custom_embeds,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatSupportInferenceUsage lets new channels' readers see token usage (badges, usage routes, realtime usage events)
	ChatSupportInferenceUsage *bool `json:"chat_support_inference_usage" yaml:"chat_support_inference_usage,omitempty" toml:"chat_support_inference_usage,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// Chat limits

	// ChatMaxChannelNameLength is the maximum length for channel names
	ChatMaxChannelNameLength *int `json:"chat_max_channel_name_length" yaml:"chat_max_channel_name_length,omitempty" toml:"chat_max_channel_name_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxChannelDescriptionLength is the maximum length for channel descriptions
	ChatMaxChannelDescriptionLength *int `json:"chat_max_channel_description_length" yaml:"chat_max_channel_description_length,omitempty" toml:"chat_max_channel_description_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxMessageLength is the maximum length for message content
	ChatMaxMessageLength *int `json:"chat_max_message_length" yaml:"chat_max_message_length,omitempty" toml:"chat_max_message_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxClientMessageIDLength bounds sender-chosen idempotency keys in bytes (1–1024; default 128).
	ChatMaxClientMessageIDLength *int `json:"chat_max_client_message_id_length" yaml:"chat_max_client_message_id_length,omitempty" toml:"chat_max_client_message_id_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxEmbedsPerMessage is the maximum number of embeds per message
	ChatMaxEmbedsPerMessage *int `json:"chat_max_embeds_per_message" yaml:"chat_max_embeds_per_message,omitempty" toml:"chat_max_embeds_per_message,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxEmbedDataBytes is the maximum size of embed_data in bytes for a single embed
	ChatMaxEmbedDataBytes *int `json:"chat_max_embed_data_bytes" yaml:"chat_max_embed_data_bytes,omitempty" toml:"chat_max_embed_data_bytes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxMentionsPerMessage is the maximum number of user mentions per message
	ChatMaxMentionsPerMessage *int `json:"chat_max_mentions_per_message" yaml:"chat_max_mentions_per_message,omitempty" toml:"chat_max_mentions_per_message,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxParticipantsPerGroup is the maximum participants in a group channel
	ChatMaxParticipantsPerGroup *int `json:"chat_max_participants_per_group" yaml:"chat_max_participants_per_group,omitempty" toml:"chat_max_participants_per_group,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxBatchGetMessages is the maximum messages that can be fetched in a batch request
	ChatMaxBatchGetMessages *int `json:"chat_max_batch_get_messages" yaml:"chat_max_batch_get_messages,omitempty" toml:"chat_max_batch_get_messages,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxListParticipantsBatchChannelIDs is the maximum number of channel ID entries accepted by one participant batch request
	ChatMaxListParticipantsBatchChannelIDs *int `json:"chat_max_list_participants_batch_channel_ids" yaml:"chat_max_list_participants_batch_channel_ids,omitempty" toml:"chat_max_list_participants_batch_channel_ids,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxEmojiLength is the maximum length in bytes of one reaction emoji
	ChatMaxEmojiLength *int `json:"chat_max_emoji_length" yaml:"chat_max_emoji_length,omitempty" toml:"chat_max_emoji_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatMaxFallbackTextLength is the maximum length of an embed's fallback text
	ChatMaxFallbackTextLength *int `json:"chat_max_fallback_text_length" yaml:"chat_max_fallback_text_length,omitempty" toml:"chat_max_fallback_text_length,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationReadDefaultLimit is the page size of a subagent transcript read that names no
	// limit (1–500; default 20; must not exceed ChatAgentDelegationReadMaxLimit).
	ChatAgentDelegationReadDefaultLimit *int `json:"chat_agent_delegation_read_default_limit" yaml:"chat_agent_delegation_read_default_limit,omitempty" toml:"chat_agent_delegation_read_default_limit,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationReadMaxLimit is the largest page size a subagent transcript read may request (1–500; default 100).
	ChatAgentDelegationReadMaxLimit *int `json:"chat_agent_delegation_read_max_limit" yaml:"chat_agent_delegation_read_max_limit,omitempty" toml:"chat_agent_delegation_read_max_limit,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationReadMaxBytes caps the serialized bytes one subagent transcript read returns (1,024–1,048,576; default 32,768).
	ChatAgentDelegationReadMaxBytes *int `json:"chat_agent_delegation_read_max_bytes" yaml:"chat_agent_delegation_read_max_bytes,omitempty" toml:"chat_agent_delegation_read_max_bytes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationReadMaxContentChars caps the Unicode characters in each content field a subagent
	// transcript read returns (128–65,536; default 4,000).
	ChatAgentDelegationReadMaxContentChars *int `json:"chat_agent_delegation_read_max_content_chars" yaml:"chat_agent_delegation_read_max_content_chars,omitempty" toml:"chat_agent_delegation_read_max_content_chars,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationMaxRunsPerChild is the maximum lifetime number of runs one delegated child may have;
	// orchestration cannot start another run once a child reaches it (1–1,000; default 8).
	ChatAgentDelegationMaxRunsPerChild *int `json:"chat_agent_delegation_max_runs_per_child" yaml:"chat_agent_delegation_max_runs_per_child,omitempty" toml:"chat_agent_delegation_max_runs_per_child,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatForkMaxCopiedMessages caps the source messages one channel fork may copy; a larger fork is
	// rejected (1–100,000; default 5,000).
	ChatForkMaxCopiedMessages *int `json:"chat_fork_max_copied_messages" yaml:"chat_fork_max_copied_messages,omitempty" toml:"chat_fork_max_copied_messages,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// ChatAgentRunMaxActiveDuration limits cumulative active processing time across one logical
	// agent run. Time spent waiting for tool approval does not count toward this budget.
	ChatAgentRunMaxActiveDuration *time.Duration `json:"chat_agent_run_max_active_duration" yaml:"chat_agent_run_max_active_duration,omitempty" toml:"chat_agent_run_max_active_duration,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunMaxGenerations limits model generations across one logical agent run.
	ChatAgentRunMaxGenerations *int `json:"chat_agent_run_max_generations" yaml:"chat_agent_run_max_generations,omitempty" toml:"chat_agent_run_max_generations,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunMaxToolCalls limits model-requested tool calls across one logical agent run.
	ChatAgentRunMaxToolCalls *int `json:"chat_agent_run_max_tool_calls" yaml:"chat_agent_run_max_tool_calls,omitempty" toml:"chat_agent_run_max_tool_calls,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationStateMaxChildren caps the delegated children one delegation state read returns
	// with active runs in a parent's prompt state block and unscoped subagent_status (1–256; default 64;
	// must be at least both active-children limits, so a parent never loses an active child from the read).
	ChatAgentDelegationStateMaxChildren *int `json:"chat_agent_delegation_state_max_children" yaml:"chat_agent_delegation_state_max_children,omitempty" toml:"chat_agent_delegation_state_max_children,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationActiveChildrenLimitInteractive is the largest MaxActiveChildren an interactive
	// delegating agent type may register (1–256; default 32). Captured when the agent registry is
	// constructed, so registered caps never change under a running process.
	ChatAgentDelegationActiveChildrenLimitInteractive *int `json:"chat_agent_delegation_active_children_limit_interactive" yaml:"chat_agent_delegation_active_children_limit_interactive,omitempty" toml:"chat_agent_delegation_active_children_limit_interactive,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatAgentDelegationActiveChildrenLimitOwnerDriven is the largest MaxActiveChildren a
	// workflow-owned (owner-driven) delegating agent type may register (1–256; default 64). Captured
	// when the agent registry is constructed.
	ChatAgentDelegationActiveChildrenLimitOwnerDriven *int `json:"chat_agent_delegation_active_children_limit_owner_driven" yaml:"chat_agent_delegation_active_children_limit_owner_driven,omitempty" toml:"chat_agent_delegation_active_children_limit_owner_driven,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatAgentDelegationMetadataValueMaxBytes caps the bytes of one delegation metadata value, such as
	// a child's normalized scope (4–65,536; default 1,024; at least 4 bytes per rune of
	// ChatAgentDelegationTitleMaxRunes, so a max-length title fits).
	ChatAgentDelegationMetadataValueMaxBytes *int `json:"chat_agent_delegation_metadata_value_max_bytes" yaml:"chat_agent_delegation_metadata_value_max_bytes,omitempty" toml:"chat_agent_delegation_metadata_value_max_bytes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationMetadataMaxBytes caps the encoded bytes of a delegated child's metadata
	// (29–262,144; default 8,192; must be at least the per-value limit and the encoded title
	// metadata, 23 bytes plus 6 per rune of ChatAgentDelegationTitleMaxRunes).
	ChatAgentDelegationMetadataMaxBytes *int `json:"chat_agent_delegation_metadata_max_bytes" yaml:"chat_agent_delegation_metadata_max_bytes,omitempty" toml:"chat_agent_delegation_metadata_max_bytes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegatedChildrenPageMax caps the delegated children one ListDelegatedChildren page returns (1–500; default 50).
	ChatAgentDelegatedChildrenPageMax *int `json:"chat_agent_delegated_children_page_max" yaml:"chat_agent_delegated_children_page_max,omitempty" toml:"chat_agent_delegated_children_page_max,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentSessionsByChannelMax caps the agent sessions one channel session listing returns (1–1,000; default 200).
	ChatAgentSessionsByChannelMax *int `json:"chat_agent_sessions_by_channel_max" yaml:"chat_agent_sessions_by_channel_max,omitempty" toml:"chat_agent_sessions_by_channel_max,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunRecoveryScanPageSize caps the recoverable agent runs listed per recovery scan page (1–1,000; default 200).
	ChatAgentRunRecoveryScanPageSize *int `json:"chat_agent_run_recovery_scan_page_size" yaml:"chat_agent_run_recovery_scan_page_size,omitempty" toml:"chat_agent_run_recovery_scan_page_size,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentStreamRecoveryMaxAttempts is the number of stream recovery attempts per agent run step before it is exhausted (1–10; default 2).
	ChatAgentStreamRecoveryMaxAttempts *int `json:"chat_agent_stream_recovery_max_attempts" yaml:"chat_agent_stream_recovery_max_attempts,omitempty" toml:"chat_agent_stream_recovery_max_attempts,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunSettleTimeout bounds the detached writes that settle an agent drive after its own context may be gone: terminal settlement and the active-duration record (1s–5m; default 10s).
	ChatAgentRunSettleTimeout *time.Duration `json:"chat_agent_run_settle_timeout" yaml:"chat_agent_run_settle_timeout,omitempty" toml:"chat_agent_run_settle_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentWorkflowResultRecoveryFloor is the minimum admission and recovery time a workflow-owned drive with durable result completion gets after its active-duration budget is spent, so it can repair a tool acknowledgement and observe an accepted result; generation stays budget-gated (1s–10m; default 30s).
	ChatAgentWorkflowResultRecoveryFloor *time.Duration `json:"chat_agent_workflow_result_recovery_floor" yaml:"chat_agent_workflow_result_recovery_floor,omitempty" toml:"chat_agent_workflow_result_recovery_floor,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunDriveTimeout is the wall-clock budget of one managed or recovery agent drive, sampled when the drive starts (10s–24h; default 10m).
	ChatAgentRunDriveTimeout *time.Duration `json:"chat_agent_run_drive_timeout" yaml:"chat_agent_run_drive_timeout,omitempty" toml:"chat_agent_run_drive_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunDriveLeaseDuration is how long a claimed or renewed agent run drive lease lasts, sampled once per drive for its claim and every renewal (10s–1h; default 2m; must exceed the heartbeat interval plus the drive database timeout).
	ChatAgentRunDriveLeaseDuration *time.Duration `json:"chat_agent_run_drive_lease_duration" yaml:"chat_agent_run_drive_lease_duration,omitempty" toml:"chat_agent_run_drive_lease_duration,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunDriveHeartbeatInterval is how often a drive renews its lease and re-publishes its activity, sampled once per drive (1s–10m; default 20s).
	ChatAgentRunDriveHeartbeatInterval *time.Duration `json:"chat_agent_run_drive_heartbeat_interval" yaml:"chat_agent_run_drive_heartbeat_interval,omitempty" toml:"chat_agent_run_drive_heartbeat_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunDriveRetryBackoff is the wait before a retryable drive is due again and before an in-drive stream recovery retries (1s–10m; default 10s).
	ChatAgentRunDriveRetryBackoff *time.Duration `json:"chat_agent_run_drive_retry_backoff" yaml:"chat_agent_run_drive_retry_backoff,omitempty" toml:"chat_agent_run_drive_retry_backoff,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunDriveDBTimeout bounds each detached drive database call: lease renewal and release, and the recovery sweep's reads (1s–1m; default 5s).
	ChatAgentRunDriveDBTimeout *time.Duration `json:"chat_agent_run_drive_db_timeout" yaml:"chat_agent_run_drive_db_timeout,omitempty" toml:"chat_agent_run_drive_db_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunRecoveryScanInterval is how often the stale-run recovery sweep runs; a change reschedules the next sweep (1s–10m; default 15s).
	ChatAgentRunRecoveryScanInterval *time.Duration `json:"chat_agent_run_recovery_scan_interval" yaml:"chat_agent_run_recovery_scan_interval,omitempty" toml:"chat_agent_run_recovery_scan_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunRecoveryBatchSize caps the stale runs one recovery sweep dispatches (1–1,000; default 50).
	ChatAgentRunRecoveryBatchSize *int `json:"chat_agent_run_recovery_batch_size" yaml:"chat_agent_run_recovery_batch_size,omitempty" toml:"chat_agent_run_recovery_batch_size,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunRecoveryConcurrency is the number of concurrent recovery drive workers, captured when the recovery sweep starts (1–256; default 8).
	ChatAgentRunRecoveryConcurrency *int `json:"chat_agent_run_recovery_concurrency" yaml:"chat_agent_run_recovery_concurrency,omitempty" toml:"chat_agent_run_recovery_concurrency,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatAgentRunManagedAttemptCapacity caps concurrent managed (fresh and resume) drives, captured when managed drives start (1–1,024; default 32).
	ChatAgentRunManagedAttemptCapacity *int `json:"chat_agent_run_managed_attempt_capacity" yaml:"chat_agent_run_managed_attempt_capacity,omitempty" toml:"chat_agent_run_managed_attempt_capacity,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatAgentRunDriveAbandonGrace is how long a drive past its deadline, cancellation, or shutdown waits for a call that ignores cancellation before abandoning the attempt, sampled when the drive starts (100ms–1m; default 2s).
	ChatAgentRunDriveAbandonGrace *time.Duration `json:"chat_agent_run_drive_abandon_grace" yaml:"chat_agent_run_drive_abandon_grace,omitempty" toml:"chat_agent_run_drive_abandon_grace,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunMaxDriveAbandonments is how many abandoned drive attempts (not counting shutdown) a run survives; the next one fails it with PROVIDER_TIMEOUT. Sampled when the drive starts (0–10; default 1).
	ChatAgentRunMaxDriveAbandonments *int `json:"chat_agent_run_max_drive_abandonments" yaml:"chat_agent_run_max_drive_abandonments,omitempty" toml:"chat_agent_run_max_drive_abandonments,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunAbandonedDriveCapacity caps abandoned drive attempts whose calls are still running, across every drive path; when it is full an abandoned run fails and its drive keeps its worker slot until the call returns. Captured when managed drives start (1–4,096; default 32).
	ChatAgentRunAbandonedDriveCapacity *int `json:"chat_agent_run_abandoned_drive_capacity" yaml:"chat_agent_run_abandoned_drive_capacity,omitempty" toml:"chat_agent_run_abandoned_drive_capacity,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatAgentWorkflowWaitPollInterval is the polling backstop between drives of agentkit.WorkflowAgentStep.Wait, which the application supplies from this key, and DriveGraph's wait before it drives again a run whose last drive changed nothing; sampled once per wait (1s–10m; default 15s).
	ChatAgentWorkflowWaitPollInterval *time.Duration `json:"chat_agent_workflow_wait_poll_interval" yaml:"chat_agent_workflow_wait_poll_interval,omitempty" toml:"chat_agent_workflow_wait_poll_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentGraphDriveClaimPollInterval is how often a DriveGraph worker with nothing to claim looks again for a newly drivable run of its graph while other drives of it are in flight (a child spawned or an approval resolved mid-drive); a finished drive wakes the workers at once. Sampled once per DriveGraph call (10ms–1m; default 1s).
	ChatAgentGraphDriveClaimPollInterval *time.Duration `json:"chat_agent_graph_drive_claim_poll_interval" yaml:"chat_agent_graph_drive_claim_poll_interval,omitempty" toml:"chat_agent_graph_drive_claim_poll_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunStatusMaxLocations caps the thread locations one agent run status read accepts (50–500; default 50; floored at the bundled UI request size).
	ChatAgentRunStatusMaxLocations *int `json:"chat_agent_run_status_max_locations" yaml:"chat_agent_run_status_max_locations,omitempty" toml:"chat_agent_run_status_max_locations,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentRunStatusMaxHistory caps the older runs returned per location in an agent run status read (5–100; default 10; floored at the bundled UI default historyLimit).
	ChatAgentRunStatusMaxHistory *int `json:"chat_agent_run_status_max_history" yaml:"chat_agent_run_status_max_history,omitempty" toml:"chat_agent_run_status_max_history,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationReportExcerptRunes is the length in runes of the child report excerpt included in a parent's delegation state (8–2,000; default 160).
	ChatAgentDelegationReportExcerptRunes *int `json:"chat_agent_delegation_report_excerpt_runes" yaml:"chat_agent_delegation_report_excerpt_runes,omitempty" toml:"chat_agent_delegation_report_excerpt_runes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentDelegationTitleMaxRunes caps the rune length of a delegated task title (1–200; default 200).
	ChatAgentDelegationTitleMaxRunes *int `json:"chat_agent_delegation_title_max_runes" yaml:"chat_agent_delegation_title_max_runes,omitempty" toml:"chat_agent_delegation_title_max_runes,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentAnthropicDefaultMaxOutputTokens is the max_tokens an Anthropic Messages agent generation sends when neither the model policy's MaxOutputTokens nor the native Anthropic config sets one; the Messages API requires the field (1–32,768; default 4,096). Sampled once per generation preparation.
	ChatAgentAnthropicDefaultMaxOutputTokens *int `json:"chat_agent_anthropic_default_max_output_tokens" yaml:"chat_agent_anthropic_default_max_output_tokens,omitempty" toml:"chat_agent_anthropic_default_max_output_tokens,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatAgentMaxPendingInputs caps the inputs queued for one agent session; a send past it is rejected with 429 (1–200; default 20). Sampled once per send to a busy or suspended session, and once per queued-input list request as its largest page.
	ChatAgentMaxPendingInputs *int `json:"chat_agent_max_pending_inputs" yaml:"chat_agent_max_pending_inputs,omitempty" toml:"chat_agent_max_pending_inputs,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// Bot typing indicators

	// BotTypingDelay is the delay before emitting bot typing indicators
	BotTypingDelay *time.Duration `json:"bot_typing_delay" yaml:"bot_typing_delay,omitempty" toml:"bot_typing_delay,omitempty" kms:"group=bot,reload=hot" kms_views:"bot"`
	// BotTypingInterval is the interval for refreshing bot typing indicators
	BotTypingInterval *time.Duration `json:"bot_typing_interval" yaml:"bot_typing_interval,omitempty" toml:"bot_typing_interval,omitempty" kms:"group=bot,reload=hot" kms_views:"bot"`
	// ChatTypingRefreshWindow drops a repeated typing start from one participant in one channel
	// within this window, before recipient lookup; stops always pass. Keep it below the client's
	// typing send interval (bundled UI: 2.5s) and BotTypingInterval (default 3s). Range 100ms–2.4s;
	// default 2s.
	ChatTypingRefreshWindow *time.Duration `json:"chat_typing_refresh_window" yaml:"chat_typing_refresh_window,omitempty" toml:"chat_typing_refresh_window,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// Chat caching

	// ChatRoleCacheCapacity is the capacity of the channel role cache
	ChatRoleCacheCapacity *uint64 `json:"chat_role_cache_capacity" yaml:"chat_role_cache_capacity,omitempty" toml:"chat_role_cache_capacity,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatRoleCacheTTL is the TTL for cached channel role entries
	ChatRoleCacheTTL *time.Duration `json:"chat_role_cache_ttl" yaml:"chat_role_cache_ttl,omitempty" toml:"chat_role_cache_ttl,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// ChatApprovalExpiryScanInterval is how often the agentic tool-approval lane sweeps for pending
	// TOOL_CALL approvals that have timed out (Decision 6). <=0 disables the sweep.
	// Use pointer to distinguish "not configured" (nil) from "explicitly disabled" (*0).
	ChatApprovalExpiryScanInterval *time.Duration `json:"chat_approval_expiry_scan_interval" yaml:"chat_approval_expiry_scan_interval,omitempty" toml:"chat_approval_expiry_scan_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatApprovedWorkScanInterval is how often the agentic tool-approval lane's background backstop
	// scans for approved-but-unexecuted TOOL_CALLs (and reclaims expired execution leases). <=0
	// disables the backstop (the post-approval nudge still triggers immediate execution).
	// Use pointer to distinguish "not configured" (nil) from "explicitly disabled" (*0).
	ChatApprovedWorkScanInterval *time.Duration `json:"chat_approved_work_scan_interval" yaml:"chat_approved_work_scan_interval,omitempty" toml:"chat_approved_work_scan_interval,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkScanTimeout bounds one claimable tool-work listing by the approved-work scanner, sampled once per scan (1s–5m; default 15s).
	ChatToolWorkScanTimeout *time.Duration `json:"chat_tool_work_scan_timeout" yaml:"chat_tool_work_scan_timeout,omitempty" toml:"chat_tool_work_scan_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkSettleTimeout bounds the detached write that releases a tool call claim or commits its result after the execution context ended, sampled with the execution timeout when the call is admitted (1s–5m; default 15s).
	ChatToolWorkSettleTimeout *time.Duration `json:"chat_tool_work_settle_timeout" yaml:"chat_tool_work_settle_timeout,omitempty" toml:"chat_tool_work_settle_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkScanLimit caps the claimable or recoverable tool calls one tool-work scan considers; the rest stay durable for the next scan (1–1,000; default 100).
	ChatToolWorkScanLimit *int `json:"chat_tool_work_scan_limit" yaml:"chat_tool_work_scan_limit,omitempty" toml:"chat_tool_work_scan_limit,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkMaxConcurrency is the handler-wide number of concurrent tool-work executions, captured when the tool-work scheduler starts (1–256; default 8).
	ChatToolWorkMaxConcurrency *int `json:"chat_tool_work_max_concurrency" yaml:"chat_tool_work_max_concurrency,omitempty" toml:"chat_tool_work_max_concurrency,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatToolWorkAttemptCapacity caps tool executions still running, including ones abandoned at their deadline that ignore cancellation, captured when the tool-work scheduler starts (1–4,096; default 32; at least ChatToolWorkMaxConcurrency).
	ChatToolWorkAttemptCapacity *int `json:"chat_tool_work_attempt_capacity" yaml:"chat_tool_work_attempt_capacity,omitempty" toml:"chat_tool_work_attempt_capacity,omitempty" kms:"group=chat,reload=restart" kms_views:"chat"`
	// ChatToolWorkExecutionLeaseDuration is how long a tool call claim lasts before a crashed worker's call becomes reclaimable, sampled with the execution timeout when the call is admitted (10s–2h; default 5m; must exceed the execution timeout plus the settle timeout).
	ChatToolWorkExecutionLeaseDuration *time.Duration `json:"chat_tool_work_execution_lease_duration" yaml:"chat_tool_work_execution_lease_duration,omitempty" toml:"chat_tool_work_execution_lease_duration,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkExecutionTimeout bounds one tool-work execution, sampled when it is admitted; it stays inside the claim lease so a timed-out call can release its claim first (1s–1h; default 4m30s).
	ChatToolWorkExecutionTimeout *time.Duration `json:"chat_tool_work_execution_timeout" yaml:"chat_tool_work_execution_timeout,omitempty" toml:"chat_tool_work_execution_timeout,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`
	// ChatToolWorkShutdownGrace is how long shutdown waits for canceled tool executions to unwind and settle before abandoning them, sampled when the scheduler stops (100ms–1m; default 2s).
	ChatToolWorkShutdownGrace *time.Duration `json:"chat_tool_work_shutdown_grace" yaml:"chat_tool_work_shutdown_grace,omitempty" toml:"chat_tool_work_shutdown_grace,omitempty" kms:"group=chat,reload=hot" kms_views:"chat"`

	// Realtime WebSocket settings
	// The hot keys below are re-read for every handshake through RealtimeView, so a reload
	// changes the policy applied to connections accepted afterwards, and each handshake captures
	// all of them from one generation. Existing connections keep their captured policy.

	// EnableRealtimeWebSocket determines whether go-common should expose the built-in realtime websocket
	// endpoint (GET /realtime/ws) and wire up the in-process hub + default publisher. It is independent
	// of EnableChat: an application with no chat still gets the realtime transport for its own topics.
	EnableRealtimeWebSocket *bool `json:"enable_realtime_websocket" yaml:"enable_realtime_websocket,omitempty" toml:"enable_realtime_websocket,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSAllowedOrigins is an optional allowlist of Origin values permitted to connect.
	// If empty, the websocket upgrader uses the default same-origin check.
	// Use ["*"] to allow any origin (not recommended for production).
	RealtimeWSAllowedOrigins []string `json:"realtime_ws_allowed_origins" yaml:"realtime_ws_allowed_origins,omitempty" toml:"realtime_ws_allowed_origins,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSPingInterval is the interval between server-initiated WebSocket pings
	RealtimeWSPingInterval *time.Duration `json:"realtime_ws_ping_interval" yaml:"realtime_ws_ping_interval,omitempty" toml:"realtime_ws_ping_interval,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSPongTimeout is the time to wait for a pong response after sending a ping
	RealtimeWSPongTimeout *time.Duration `json:"realtime_ws_pong_timeout" yaml:"realtime_ws_pong_timeout,omitempty" toml:"realtime_ws_pong_timeout,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSReadTimeout is the maximum time to wait for any client message
	RealtimeWSReadTimeout *time.Duration `json:"realtime_ws_read_timeout" yaml:"realtime_ws_read_timeout,omitempty" toml:"realtime_ws_read_timeout,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSMaxMessageSize is the maximum size in bytes for incoming WebSocket messages
	RealtimeWSMaxMessageSize *int64 `json:"realtime_ws_max_message_size" yaml:"realtime_ws_max_message_size,omitempty" toml:"realtime_ws_max_message_size,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSSendQueueSize bounds the per-connection outbound queue. A connection whose queue
	// fills is disconnected (at-most-once delivery); clients reconnect and catch up over REST.
	// The queue is allocated using the generation captured when the connection is accepted.
	RealtimeWSSendQueueSize *int `json:"realtime_ws_send_queue_size" yaml:"realtime_ws_send_queue_size,omitempty" toml:"realtime_ws_send_queue_size,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSMaxConnectionsPerUser caps concurrent realtime connections for a single user
	// (0 = common_ws.DefaultMaxConnectionsPerUser; the hub reserves negative values for "unlimited"
	// and this key rejects them, so a configured deployment always has a cap).
	// The cap is enforced inside the hub's registration critical section using the handshake policy.
	RealtimeWSMaxConnectionsPerUser *int `json:"realtime_ws_max_connections_per_user" yaml:"realtime_ws_max_connections_per_user,omitempty" toml:"realtime_ws_max_connections_per_user,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSMaxSubscriptionsPerConnection caps entity-topic subscriptions held by one connection.
	// Subscribes past the cap fail with the "subscription_limit" per-topic error code.
	RealtimeWSMaxSubscriptionsPerConnection *int `json:"realtime_ws_max_subscriptions_per_connection" yaml:"realtime_ws_max_subscriptions_per_connection,omitempty" toml:"realtime_ws_max_subscriptions_per_connection,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`
	// RealtimeWSCommandAuthBudget is the single context budget shared by every topic authorizer call
	// of one subscribe/unsubscribe command. Authorizers run synchronously on the read goroutine and
	// gorilla dispatches pong control frames from inside ReadMessage, so this budget must stay below
	// RealtimeWSPingInterval + RealtimeWSPongTimeout or the ping loop disconnects the connection it
	// is serving. ValidateConfig rejects any generation that breaks that invariant.
	//
	// It is hot for the same reason ping and pong are: the three form one invariant, and each
	// handshake captures all three from a single generation. Were this key restart-bound, a
	// generation that lowered the keepalive window and the budget together would validate cleanly and
	// still leave live handshakes spending the old, now-too-long budget against the new window.
	RealtimeWSCommandAuthBudget *time.Duration `json:"realtime_ws_command_auth_budget" yaml:"realtime_ws_command_auth_budget,omitempty" toml:"realtime_ws_command_auth_budget,omitempty" kms:"group=realtime,reload=hot" kms_views:"realtime"`

	// Permission configuration

	// SiteMinUserTypeAdminAccess is the minimum user type required for admin panel access
	SiteMinUserTypeAdminAccess *proto_models.UserType `json:"site_min_user_type_admin_access" yaml:"site_min_user_type_admin_access" toml:"site_min_user_type_admin_access" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserListing is the minimum user type required to list users
	SiteMinUserTypeUserListing *proto_models.UserType `json:"site_min_user_type_user_listing" yaml:"site_min_user_type_user_listing,omitempty" toml:"site_min_user_type_user_listing,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserViewing is the minimum user type required to view user details
	SiteMinUserTypeUserViewing *proto_models.UserType `json:"site_min_user_type_user_viewing" yaml:"site_min_user_type_user_viewing,omitempty" toml:"site_min_user_type_user_viewing,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserBanning is the minimum user type required to ban or unban users
	SiteMinUserTypeUserBanning *proto_models.UserType `json:"site_min_user_type_user_banning" yaml:"site_min_user_type_user_banning,omitempty" toml:"site_min_user_type_user_banning,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserModification is the minimum user type required for modifying users
	SiteMinUserTypeUserModification *proto_models.UserType `json:"site_min_user_type_user_modification" yaml:"site_min_user_type_user_modification" toml:"site_min_user_type_user_modification" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserDeletion is the minimum user type required for deleting users
	SiteMinUserTypeUserDeletion *proto_models.UserType `json:"site_min_user_type_user_deletion" yaml:"site_min_user_type_user_deletion" toml:"site_min_user_type_user_deletion" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeUserTypeChange is the minimum user type required to change user types
	SiteMinUserTypeUserTypeChange *proto_models.UserType `json:"site_min_user_type_user_type_change" yaml:"site_min_user_type_user_type_change,omitempty" toml:"site_min_user_type_user_type_change,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeSystemAccess is the minimum user type required for system settings access
	SiteMinUserTypeSystemAccess *proto_models.UserType `json:"site_min_user_type_system_access" yaml:"site_min_user_type_system_access,omitempty" toml:"site_min_user_type_system_access,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeContentModeration is the minimum user type required for content moderation
	SiteMinUserTypeContentModeration *proto_models.UserType `json:"site_min_user_type_content_moderation" yaml:"site_min_user_type_content_moderation,omitempty" toml:"site_min_user_type_content_moderation,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAttachmentAccess is the minimum user type required for attachment admin access
	SiteMinUserTypeAttachmentAccess *proto_models.UserType `json:"site_min_user_type_attachment_access" yaml:"site_min_user_type_attachment_access,omitempty" toml:"site_min_user_type_attachment_access,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAttachmentModification is the minimum user type required for attachment modification
	SiteMinUserTypeAttachmentModification *proto_models.UserType `json:"site_min_user_type_attachment_modification" yaml:"site_min_user_type_attachment_modification,omitempty" toml:"site_min_user_type_attachment_modification,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAnalyticsAccess is the minimum user type required for analytics access
	SiteMinUserTypeAnalyticsAccess *proto_models.UserType `json:"site_min_user_type_analytics_access" yaml:"site_min_user_type_analytics_access,omitempty" toml:"site_min_user_type_analytics_access,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAdminCreateOrganization is the minimum user type required to create an organization via admin
	SiteMinUserTypeAdminCreateOrganization *proto_models.UserType `json:"site_min_user_type_admin_create_organization" yaml:"site_min_user_type_admin_create_organization,omitempty" toml:"site_min_user_type_admin_create_organization,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAdminListOrganizations is the minimum user type required to list all organizations via admin
	SiteMinUserTypeAdminListOrganizations *proto_models.UserType `json:"site_min_user_type_admin_list_organizations" yaml:"site_min_user_type_admin_list_organizations,omitempty" toml:"site_min_user_type_admin_list_organizations,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAdminManageOrganizationMembers is the minimum user type required to directly add members to organizations (bypassing invitations)
	SiteMinUserTypeAdminManageOrganizationMembers *proto_models.UserType `json:"site_min_user_type_admin_manage_organization_members" yaml:"site_min_user_type_admin_manage_organization_members,omitempty" toml:"site_min_user_type_admin_manage_organization_members,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAdminGetOrganizationMember is the minimum user type required to get individual organization member details via admin
	SiteMinUserTypeAdminGetOrganizationMember *proto_models.UserType `json:"site_min_user_type_admin_get_organization_member" yaml:"site_min_user_type_admin_get_organization_member,omitempty" toml:"site_min_user_type_admin_get_organization_member,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeAdminChatChannels is the minimum user type required to manage chat channels via admin
	SiteMinUserTypeAdminChatChannels *proto_models.UserType `json:"site_min_user_type_admin_chat_channels" yaml:"site_min_user_type_admin_chat_channels,omitempty" toml:"site_min_user_type_admin_chat_channels,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeCareersManagement is the minimum user type required to manage careers via admin
	SiteMinUserTypeCareersManagement *proto_models.UserType `json:"site_min_user_type_careers_management" yaml:"site_min_user_type_careers_management,omitempty" toml:"site_min_user_type_careers_management,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeChatToolApproval is the minimum user type required to resolve agentic chat tool-call approvals (Decision 6)
	SiteMinUserTypeChatToolApproval *proto_models.UserType `json:"site_min_user_type_chat_tool_approval" yaml:"site_min_user_type_chat_tool_approval,omitempty" toml:"site_min_user_type_chat_tool_approval,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeInferenceViewing is the minimum user type required to view inference routes, rate-limit policies, and attempt/usage data
	SiteMinUserTypeInferenceViewing *proto_models.UserType `json:"site_min_user_type_inference_viewing" yaml:"site_min_user_type_inference_viewing,omitempty" toml:"site_min_user_type_inference_viewing,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeInferenceModelOverride permits request-scoped selection; admin tiers only.
	SiteMinUserTypeInferenceModelOverride *proto_models.UserType `json:"site_min_user_type_inference_model_override" yaml:"site_min_user_type_inference_model_override,omitempty" toml:"site_min_user_type_inference_model_override,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// SiteMinUserTypeInferenceAdministration is the minimum user type required to update inference routes and rate-limit policies
	SiteMinUserTypeInferenceAdministration *proto_models.UserType `json:"site_min_user_type_inference_administration" yaml:"site_min_user_type_inference_administration,omitempty" toml:"site_min_user_type_inference_administration,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`

	// DefaultOrgCreatorRole is the default role assigned to organization creators by org depth.
	// Index i represents the role assigned at org depth i.
	DefaultOrgCreatorRole []proto_models.OrganizationRole `json:"default_org_creator_role" yaml:"default_org_creator_role,omitempty" toml:"default_org_creator_role,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// PersonalWorkspaceOrganizationType is the capability tier assigned to managed personal workspaces.
	// Sub-organization creation inside a personal workspace is gated by
	// OrgMinTypeCreateChildOrganizations; raising that threshold above this type
	// silently disables personal sub-organization creation.
	PersonalWorkspaceOrganizationType *proto_models.OrganizationType `json:"personal_workspace_organization_type" yaml:"personal_workspace_organization_type,omitempty" toml:"personal_workspace_organization_type,omitempty" kms:"group=organizations,reload=restart" kms_views:"organizations"`
	// PersonalWorkspaceOwnerRole is the explicit role assigned to each personal workspace owner.
	PersonalWorkspaceOwnerRole *proto_models.OrganizationRole `json:"personal_workspace_owner_role" yaml:"personal_workspace_owner_role,omitempty" toml:"personal_workspace_owner_role,omitempty" kms:"group=organizations,reload=restart" kms_views:"organizations"`
	// EnableOrganizationEmailDomains determines whether organization email domain mapping is enabled
	EnableOrganizationEmailDomains *bool `json:"enable_organization_email_domains" yaml:"enable_organization_email_domains,omitempty" toml:"enable_organization_email_domains,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// EnableOrganizationEmailAutoAssociation toggles automatic membership based on email domain
	EnableOrganizationEmailAutoAssociation *bool `json:"enable_organization_email_auto_association" yaml:"enable_organization_email_auto_association,omitempty" toml:"enable_organization_email_auto_association,omitempty" kms:"group=features,reload=hot" kms_views:"features"`
	// AllowOrganizationManagedEmailDomains determines whether organizations can self-manage domain mappings
	AllowOrganizationManagedEmailDomains *bool `json:"allow_organization_managed_email_domains" yaml:"allow_organization_managed_email_domains,omitempty" toml:"allow_organization_managed_email_domains,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrganizationAutoAssociationDefaultRole is the role automatically assigned when auto-associating a user via domain by org depth.
	// Index i represents the role assigned at org depth i.
	OrganizationAutoAssociationDefaultRole []proto_models.OrganizationRole `json:"organization_auto_association_default_role" yaml:"organization_auto_association_default_role,omitempty" toml:"organization_auto_association_default_role,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeManageEmailDomains is the minimum global user type required to manage organization email domains
	OrgMinUserTypeManageEmailDomains *proto_models.UserType `json:"org_min_user_type_manage_email_domains" yaml:"org_min_user_type_manage_email_domains,omitempty" toml:"org_min_user_type_manage_email_domains,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleManageEmailDomains is the minimum organization role required to manage email domains within an organization
	OrgMinRoleManageEmailDomains []proto_models.OrganizationRole `json:"org_min_role_manage_email_domains" yaml:"org_min_role_manage_email_domains,omitempty" toml:"org_min_role_manage_email_domains,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// OrganizationInvitationExpiryDays is the duration before a pending organization invitation expires.
	// When a user is invited to an organization but doesn't exist yet (email invite), the invitation is stored
	// and processed when the user registers. This setting controls how long those invitations remain valid.
	// Default: 45 days
	OrganizationInvitationExpiryDays *time.Duration `json:"organization_invitation_expiry_days" yaml:"organization_invitation_expiry_days,omitempty" toml:"organization_invitation_expiry_days,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrganizationAutoAddExpiryDays is the duration before a pending auto-add entry (direct add of non-existent user) expires.
	// Default aligns with invitation expiry unless overridden.
	OrganizationAutoAddExpiryDays *time.Duration `json:"organization_auto_add_expiry_days" yaml:"organization_auto_add_expiry_days,omitempty" toml:"organization_auto_add_expiry_days,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// OrgUserAttributePermissions defines which member attributes an organization role may view, by org depth.
	// Index i is the policy at org depth i and must have OrgHierarchyMaxDepth+1 entries. Each entry maps a
	// role key to that role's visibility; the key is the OrganizationRole name without its ORGANIZATION_ROLE_
	// prefix, lowercased ("member_tier_1", "moderator_tier_3", "admin_tier_1", "owner_tier_2"). Roles absent
	// from the map are unconfigured and fall back to the viewer's global user-type policy. Unknown keys are
	// rejected at validation. Hot: a published generation applies to the next request.
	OrgUserAttributePermissions []map[string]AttributeVisibility `json:"org_user_attribute_permissions_by_depth" yaml:"org_user_attribute_permissions_by_depth,omitempty" toml:"org_user_attribute_permissions_by_depth,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// GlobalUserAttributePermissions defines which user attributes a viewer may see based on their site user type,
	// regardless of organization role; intended primarily to grant site staff (admin tiers) elevated visibility.
	// Keys are the UserType name without its USER_TYPE_ prefix, lowercased ("regular_tier_1", "admin_tier_5"),
	// plus "public" for unauthenticated viewers. User types absent from the map are unconfigured. Unknown keys
	// are rejected at validation. Hot: a published generation applies to the next request.
	GlobalUserAttributePermissions map[string]AttributeVisibility `json:"global_user_attribute_permissions" yaml:"global_user_attribute_permissions,omitempty" toml:"global_user_attribute_permissions,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`

	// PublicPrimaryUserIdentifier controls which identifier type populates PublicUser.display_primary.
	PublicPrimaryUserIdentifier *proto_models.UserIdentifierType `json:"public_primary_user_identifier" yaml:"public_primary_user_identifier,omitempty" toml:"public_primary_user_identifier,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// PublicSecondaryUserIdentifier controls which identifier type populates PublicUser.display_secondary (optional).
	PublicSecondaryUserIdentifier *proto_models.UserIdentifierType `json:"public_secondary_user_identifier" yaml:"public_secondary_user_identifier,omitempty" toml:"public_secondary_user_identifier,omitempty" kms:"group=permissions,reload=hot" kms_views:"permissions"`

	// User creation configuration

	// DefaultUserType is the user type assigned to newly created users (email registration, OAuth, etc.)
	DefaultUserType *proto_models.UserType `json:"default_user_type" yaml:"default_user_type" toml:"default_user_type" kms:"group=permissions,reload=hot" kms_views:"permissions"`
	// DefaultOrganizationType is the organization type assigned to newly created organizations when not specified.
	DefaultOrganizationType *proto_models.OrganizationType `json:"default_organization_type" yaml:"default_organization_type,omitempty" toml:"default_organization_type,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// User signup enforcement configuration

	// RequireRealNamesOnSignup determines whether first_name and last_name are required during registration
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	RequireRealNamesOnSignup *bool `json:"require_real_names_on_signup" yaml:"require_real_names_on_signup,omitempty" toml:"require_real_names_on_signup,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`
	// RequireUsernamesOnSignup determines whether username is required during registration
	// When false, the system will use the user's email as their username
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	RequireUsernamesOnSignup *bool `json:"require_usernames_on_signup" yaml:"require_usernames_on_signup,omitempty" toml:"require_usernames_on_signup,omitempty" kms:"group=auth,reload=hot" kms_views:"auth"`

	// OAuthSignupMode controls how OAuth account creation behaves when no user exists.
	// Supported values: "legacy", "disabled", "prefixed", "email_local_part"
	OAuthSignupMode *OAuthSignupMode `json:"oauth_signup_mode" yaml:"oauth_signup_mode,omitempty" toml:"oauth_signup_mode,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthDefaultUsernamePrefix is prefixed to system-generated usernames in prefixed mode.
	OAuthDefaultUsernamePrefix *string `json:"oauth_default_username_prefix" yaml:"oauth_default_username_prefix,omitempty" toml:"oauth_default_username_prefix,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthHTTPTimeout bounds each request to an OAuth provider: discovery, key set, token exchange, and userinfo (1s–60s; default 10s).
	OAuthHTTPTimeout *time.Duration `json:"oauth_http_timeout" yaml:"oauth_http_timeout,omitempty" toml:"oauth_http_timeout,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthStateTTL is how long an issued OAuth state and its browser binding stay redeemable (1m–30m; default 10m).
	OAuthStateTTL *time.Duration `json:"oauth_state_ttl" yaml:"oauth_state_ttl,omitempty" toml:"oauth_state_ttl,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthStateClockSkew tolerates OAuth state issued-at times this far in the future (0–5m; default 30s).
	OAuthStateClockSkew *time.Duration `json:"oauth_state_clock_skew" yaml:"oauth_state_clock_skew,omitempty" toml:"oauth_state_clock_skew,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthDiscoveryRefreshInterval is how long a provider's OIDC discovery document and key set are reused, and how long an unused issuer stays cached (1h–7d; default 24h).
	OAuthDiscoveryRefreshInterval *time.Duration `json:"oauth_discovery_refresh_interval" yaml:"oauth_discovery_refresh_interval,omitempty" toml:"oauth_discovery_refresh_interval,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthSignedClientSecretLifetime is the lifetime of client secrets signed per token exchange, such as Apple's ES256 assertion (1m–1h; default 5m).
	OAuthSignedClientSecretLifetime *time.Duration `json:"oauth_signed_client_secret_lifetime" yaml:"oauth_signed_client_secret_lifetime,omitempty" toml:"oauth_signed_client_secret_lifetime,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthMaxResponseBytes caps userinfo and provider API response bodies read during identity resolution (16 KiB–8 MiB; default 1 MiB).
	OAuthMaxResponseBytes *int64 `json:"oauth_max_response_bytes" yaml:"oauth_max_response_bytes,omitempty" toml:"oauth_max_response_bytes,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`
	// OAuthMaxStateLength bounds the opaque OAuth state, in characters, that is issued, accepted by callback sanitizers, and relayed (1024–16384; default 4096).
	OAuthMaxStateLength *int `json:"oauth_max_state_length" yaml:"oauth_max_state_length,omitempty" toml:"oauth_max_state_length,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth,sanitizer"`
	// OAuthMaxCodeLength bounds the authorization code, in characters, accepted by the callback sanitizer (256–8192; default 2048).
	OAuthMaxCodeLength *int `json:"oauth_max_code_length" yaml:"oauth_max_code_length,omitempty" toml:"oauth_max_code_length,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth,sanitizer"`
	// OAuthFormPostMaxBodyBytes caps the provider's form_post body read by the OAuth relay; it must exceed OAuthMaxStateLength (8 KiB–64 KiB; default 16 KiB).
	OAuthFormPostMaxBodyBytes *int64 `json:"oauth_form_post_max_body_bytes" yaml:"oauth_form_post_max_body_bytes,omitempty" toml:"oauth_form_post_max_body_bytes,omitempty" kms:"group=oauth,reload=hot" kms_views:"oauth"`

	// Mock Token Service configuration

	// UseTokenServiceMock determines whether to use the mock token service instead of the production service
	// CRITICAL: This should NEVER be true in production environments
	// Use pointer to distinguish between "not configured" (nil) and "explicitly disabled" (*false)
	UseTokenServiceMock *bool `json:"use_token_service_mock" yaml:"use_token_service_mock,omitempty" toml:"use_token_service_mock,omitempty" kms:"group=auth,reload=restart" kms_views:"auth"`
	// TokenServiceMockSeed is the seed value for deterministic token generation
	// This allows reproducible token generation across test runs
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set" (*12345)
	TokenServiceMockSeed *int64 `json:"token_service_mock_seed" yaml:"token_service_mock_seed,omitempty" toml:"token_service_mock_seed,omitempty" kms:"group=auth,reload=restart" kms_views:"auth"`

	// Organization creation configuration

	// OrgMinUserTypeCreate is the minimum user type allowed to create organizations through user endpoint
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	OrgMinUserTypeCreate *proto_models.UserType `json:"org_min_user_type_create" yaml:"org_min_user_type_create,omitempty" toml:"org_min_user_type_create,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// OrgMinUserTypeCreateChild allows creating children without a qualifying parent role.
	// The user creation endpoint also requires OrgMinUserTypeCreate.
	OrgMinUserTypeCreateChild *proto_models.UserType `json:"org_min_user_type_create_child" yaml:"org_min_user_type_create_child,omitempty" toml:"org_min_user_type_create_child,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgHierarchyMaxDepth is the maximum allowed depth for organization hierarchy
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	OrgHierarchyMaxDepth *int `json:"org_hierarchy_max_depth" yaml:"org_hierarchy_max_depth,omitempty" toml:"org_hierarchy_max_depth,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgHierarchyMaxNodes is the maximum number of nodes returned by a full hierarchy request.
	OrgHierarchyMaxNodes *int `json:"org_hierarchy_max_nodes" yaml:"org_hierarchy_max_nodes,omitempty" toml:"org_hierarchy_max_nodes,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgRoleInheritanceEnabled toggles downward role inheritance in org hierarchies
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	OrgRoleInheritanceEnabled *bool `json:"org_role_inheritance_enabled" yaml:"org_role_inheritance_enabled,omitempty" toml:"org_role_inheritance_enabled,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// Organization operation access control configuration

	// OrgMinUserTypeView is the minimum user type allowed to view organization details
	OrgMinUserTypeView *proto_models.UserType `json:"org_min_user_type_view" yaml:"org_min_user_type_view,omitempty" toml:"org_min_user_type_view,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleView is the minimum role within an organization required to view its details
	OrgMinRoleView []proto_models.OrganizationRole `json:"org_min_role_view" yaml:"org_min_role_view,omitempty" toml:"org_min_role_view,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeListMembers is the minimum user type allowed to list organization members
	OrgMinUserTypeListMembers *proto_models.UserType `json:"org_min_user_type_list_members" yaml:"org_min_user_type_list_members,omitempty" toml:"org_min_user_type_list_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeListMembers is the minimum organization type required to list organization members.
	OrgMinTypeListMembers *proto_models.OrganizationType `json:"org_min_type_list_members" yaml:"org_min_type_list_members,omitempty" toml:"org_min_type_list_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleListMembers is the minimum role within an organization required to list its members
	OrgMinRoleListMembers []proto_models.OrganizationRole `json:"org_min_role_list_members" yaml:"org_min_role_list_members,omitempty" toml:"org_min_role_list_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeDelete is the minimum user type allowed to delete organizations
	OrgMinUserTypeDelete *proto_models.UserType `json:"org_min_user_type_delete" yaml:"org_min_user_type_delete,omitempty" toml:"org_min_user_type_delete,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeUpdate is the minimum user type allowed to update organizations
	OrgMinUserTypeUpdate *proto_models.UserType `json:"org_min_user_type_update" yaml:"org_min_user_type_update,omitempty" toml:"org_min_user_type_update,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleUpdate is the minimum role within an organization required to update it
	OrgMinRoleUpdate []proto_models.OrganizationRole `json:"org_min_role_update" yaml:"org_min_role_update,omitempty" toml:"org_min_role_update,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleDelete is the minimum role within an organization required to delete it
	OrgMinRoleDelete []proto_models.OrganizationRole `json:"org_min_role_delete" yaml:"org_min_role_delete,omitempty" toml:"org_min_role_delete,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeInviteMembers is the minimum user type allowed to invite members to any organization
	OrgMinUserTypeInviteMembers *proto_models.UserType `json:"org_min_user_type_invite_members" yaml:"org_min_user_type_invite_members,omitempty" toml:"org_min_user_type_invite_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeInviteMembers is the minimum organization type required to invite members.
	OrgMinTypeInviteMembers *proto_models.OrganizationType `json:"org_min_type_invite_members" yaml:"org_min_type_invite_members,omitempty" toml:"org_min_type_invite_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeAddMembers is the minimum user type allowed to add members to any organization
	OrgMinUserTypeAddMembers *proto_models.UserType `json:"org_min_user_type_add_members" yaml:"org_min_user_type_add_members,omitempty" toml:"org_min_user_type_add_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeAddMembers is the minimum organization type required to add members directly.
	OrgMinTypeAddMembers *proto_models.OrganizationType `json:"org_min_type_add_members" yaml:"org_min_type_add_members,omitempty" toml:"org_min_type_add_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleInviteMembers is the minimum role within an organization required to invite members
	OrgMinRoleInviteMembers []proto_models.OrganizationRole `json:"org_min_role_invite_members" yaml:"org_min_role_invite_members,omitempty" toml:"org_min_role_invite_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleAddMembers is the minimum role within an organization required to add members
	OrgMinRoleAddMembers []proto_models.OrganizationRole `json:"org_min_role_add_members" yaml:"org_min_role_add_members,omitempty" toml:"org_min_role_add_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeRemoveMembers is the minimum user type allowed to remove members from any organization
	OrgMinUserTypeRemoveMembers *proto_models.UserType `json:"org_min_user_type_remove_members" yaml:"org_min_user_type_remove_members,omitempty" toml:"org_min_user_type_remove_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeRemoveMembers is the minimum organization type required to remove members.
	OrgMinTypeRemoveMembers *proto_models.OrganizationType `json:"org_min_type_remove_members" yaml:"org_min_type_remove_members,omitempty" toml:"org_min_type_remove_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleRemoveMembers is the minimum role within an organization required to remove members
	OrgMinRoleRemoveMembers []proto_models.OrganizationRole `json:"org_min_role_remove_members" yaml:"org_min_role_remove_members,omitempty" toml:"org_min_role_remove_members,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeUpdateMemberRoles is the minimum user type allowed to update member roles in any organization
	OrgMinUserTypeUpdateMemberRoles *proto_models.UserType `json:"org_min_user_type_update_member_roles" yaml:"org_min_user_type_update_member_roles,omitempty" toml:"org_min_user_type_update_member_roles,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeUpdateMemberRoles is the minimum organization type required to update member roles.
	OrgMinTypeUpdateMemberRoles *proto_models.OrganizationType `json:"org_min_type_update_member_roles" yaml:"org_min_type_update_member_roles,omitempty" toml:"org_min_type_update_member_roles,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleUpdateMemberRoles is the minimum role within an organization required to update member roles
	OrgMinRoleUpdateMemberRoles []proto_models.OrganizationRole `json:"org_min_role_update_member_roles" yaml:"org_min_role_update_member_roles,omitempty" toml:"org_min_role_update_member_roles,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeCancelInvitation is the minimum user type allowed to cancel invitations in any organization
	OrgMinUserTypeCancelInvitation *proto_models.UserType `json:"org_min_user_type_cancel_invitation" yaml:"org_min_user_type_cancel_invitation,omitempty" toml:"org_min_user_type_cancel_invitation,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeCancelInvitation is the minimum organization type required to cancel invitations.
	OrgMinTypeCancelInvitation *proto_models.OrganizationType `json:"org_min_type_cancel_invitation" yaml:"org_min_type_cancel_invitation,omitempty" toml:"org_min_type_cancel_invitation,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleCancelInvitation is the minimum role within an organization required to cancel pending invitations
	OrgMinRoleCancelInvitation []proto_models.OrganizationRole `json:"org_min_role_cancel_invitation" yaml:"org_min_role_cancel_invitation,omitempty" toml:"org_min_role_cancel_invitation,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinUserTypeViewInvitations is the minimum user type allowed to view invitations in any organization
	OrgMinUserTypeViewInvitations *proto_models.UserType `json:"org_min_user_type_view_invitations" yaml:"org_min_user_type_view_invitations,omitempty" toml:"org_min_user_type_view_invitations,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeViewInvitations is the minimum organization type required to view invitations.
	OrgMinTypeViewInvitations *proto_models.OrganizationType `json:"org_min_type_view_invitations" yaml:"org_min_type_view_invitations,omitempty" toml:"org_min_type_view_invitations,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleViewInvitations is the minimum role within an organization required to view pending invitations
	OrgMinRoleViewInvitations []proto_models.OrganizationRole `json:"org_min_role_view_invitations" yaml:"org_min_role_view_invitations,omitempty" toml:"org_min_role_view_invitations,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeManageEmailDomains is the minimum organization type required to manage email domains.
	OrgMinTypeManageEmailDomains *proto_models.OrganizationType `json:"org_min_type_manage_email_domains" yaml:"org_min_type_manage_email_domains,omitempty" toml:"org_min_type_manage_email_domains,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeWorkspaceNavigation is the minimum organization type required to surface workspace/member navigation.
	OrgMinTypeWorkspaceNavigation *proto_models.OrganizationType `json:"org_min_type_workspace_navigation" yaml:"org_min_type_workspace_navigation,omitempty" toml:"org_min_type_workspace_navigation,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeCreateChildOrganizations is the minimum organization type required to create child organizations.
	OrgMinTypeCreateChildOrganizations *proto_models.OrganizationType `json:"org_min_type_create_child_organizations" yaml:"org_min_type_create_child_organizations,omitempty" toml:"org_min_type_create_child_organizations,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinTypeBulkMemberManagement is the minimum organization type required for bulk/admin member-management flows.
	OrgMinTypeBulkMemberManagement *proto_models.OrganizationType `json:"org_min_type_bulk_member_management" yaml:"org_min_type_bulk_member_management,omitempty" toml:"org_min_type_bulk_member_management,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// OrgMinRoleCreateChild is the minimum role within an organization required to create child orgs
	OrgMinRoleCreateChild []proto_models.OrganizationRole `json:"org_min_role_create_child" yaml:"org_min_role_create_child,omitempty" toml:"org_min_role_create_child,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleSelfJoin is the minimum role within an organization required for self-join into descendants
	OrgMinRoleSelfJoin []proto_models.OrganizationRole `json:"org_min_role_self_join" yaml:"org_min_role_self_join,omitempty" toml:"org_min_role_self_join,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleViewInferencePolicy requires an organization role at each hierarchy depth, without a user-type bypass.
	OrgMinRoleViewInferencePolicy []proto_models.OrganizationRole `json:"org_min_role_view_inference_policy" yaml:"org_min_role_view_inference_policy,omitempty" toml:"org_min_role_view_inference_policy,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleManageInferencePolicy requires an organization role at each hierarchy depth, without a user-type bypass.
	OrgMinRoleManageInferencePolicy []proto_models.OrganizationRole `json:"org_min_role_manage_inference_policy" yaml:"org_min_role_manage_inference_policy,omitempty" toml:"org_min_role_manage_inference_policy,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleChooseInferenceModel requires an organization role at each hierarchy depth, without a user-type bypass.
	OrgMinRoleChooseInferenceModel []proto_models.OrganizationRole `json:"org_min_role_choose_inference_model" yaml:"org_min_role_choose_inference_model,omitempty" toml:"org_min_role_choose_inference_model,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgMinRoleReadAgentTranscript is, by hierarchy depth, the minimum effective organization role
	// that may read a server-created transcript channel whose create leaves the role UNSPECIFIED:
	// the channel of a new agent session (CreateAgentChatSessionInput.OrganizationReadMinRole) and a
	// workflow channel with organization access (CreateWorkflowChannelInput.OrganizationAccess). The
	// entry at the owning organization's depth is stored on the channel when it is created and is
	// never part of a session's create fingerprint. An UNSPECIFIED entry keeps an agent session's
	// channel participant-only and fails a workflow channel's create.
	OrgMinRoleReadAgentTranscript []proto_models.OrganizationRole `json:"org_min_role_read_agent_transcript" yaml:"org_min_role_read_agent_transcript,omitempty" toml:"org_min_role_read_agent_transcript,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations,chat"`
	// OrgAutoAddCreatorOnChildCreate controls auto-adding the creator to child orgs by depth
	OrgAutoAddCreatorOnChildCreate []bool `json:"org_auto_add_creator_on_child_create" yaml:"org_auto_add_creator_on_child_create,omitempty" toml:"org_auto_add_creator_on_child_create,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrgAncestorMembershipAutoAdd controls whether missing ancestor memberships are auto-added
	OrgAncestorMembershipAutoAdd *bool `json:"org_ancestor_membership_auto_add" yaml:"org_ancestor_membership_auto_add,omitempty" toml:"org_ancestor_membership_auto_add,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// OrganizationParentOptionsMaxLimit caps one admin organization parent-option page; larger requests are rejected (1–1,000 and at most MaxPaginationLimit; default 20).
	OrganizationParentOptionsMaxLimit *int `json:"organization_parent_options_max_limit" yaml:"organization_parent_options_max_limit,omitempty" toml:"organization_parent_options_max_limit,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// Organization slug configuration

	// OrganizationSlugMinLength is the minimum length for organization slugs
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	OrganizationSlugMinLength *int `json:"organization_slug_min_length" yaml:"organization_slug_min_length,omitempty" toml:"organization_slug_min_length,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`
	// OrganizationSlugMaxLength is the maximum length for organization slugs
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set"
	OrganizationSlugMaxLength *int `json:"organization_slug_max_length" yaml:"organization_slug_max_length,omitempty" toml:"organization_slug_max_length,omitempty" kms:"group=organizations,reload=hot" kms_views:"organizations"`

	// API Pagination configuration

	// DefaultPaginationLimit is the page size a list request gets when it omits its limit (1–1,000, at most MaxPaginationLimit; default 20).
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set to 0" (*0)
	DefaultPaginationLimit *int `json:"default_pagination_limit" yaml:"default_pagination_limit,omitempty" toml:"default_pagination_limit,omitempty" kms:"group=pagination,reload=hot" kms_views:"pagination"`
	// MaxPaginationLimit is the largest page size a list request may ask for; larger requests are rejected (1–1,000; default 100).
	// Use pointer to distinguish between "not configured" (nil) and "explicitly set to 0" (*0)
	MaxPaginationLimit *int `json:"max_pagination_limit" yaml:"max_pagination_limit,omitempty" toml:"max_pagination_limit,omitempty" kms:"group=pagination,reload=hot" kms_views:"pagination"`
	// InferenceAttemptListDefaultLimit is the admin inference-attempt page size used when a list request omits its limit (1–1,000, at most InferenceAttemptListMaxLimit; default 100).
	InferenceAttemptListDefaultLimit *int `json:"inference_attempt_list_default_limit" yaml:"inference_attempt_list_default_limit,omitempty" toml:"inference_attempt_list_default_limit,omitempty" kms:"group=pagination,reload=hot" kms_views:"pagination"`
	// InferenceAttemptListMaxLimit is the largest admin inference-attempt page size a list request may ask for; larger requests are rejected (1–1,000; default 500).
	InferenceAttemptListMaxLimit *int `json:"inference_attempt_list_max_limit" yaml:"inference_attempt_list_max_limit,omitempty" toml:"inference_attempt_list_max_limit,omitempty" kms:"group=pagination,reload=hot" kms_views:"pagination"`

	// Vertex AI Configuration

}

// Validate implements the managed KMS configuration contract.
func (c *Config) Validate() error {
	return c.ValidateConfig()
}
