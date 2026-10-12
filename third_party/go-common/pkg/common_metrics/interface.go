// Package common_metrics provides comprehensive Prometheus metrics collection and
// management for the go-common library. It supports both pull-based and push-based
// metrics collection strategies.
//
// The package includes specialized collectors for:
//   - Database operations and connection pooling
//   - Authentication events and cache performance
//   - MinIO object storage operations
//   - Rate limiting decisions
//   - HTTP request tracking
//   - Frontend telemetry (via Grafana Faro integration)
//   - System uptime monitoring
//
// Metrics can be exposed via an HTTP endpoint for Prometheus scraping or pushed
// to a remote gateway for environments where pull-based collection is not feasible.
//
// Example usage:
//
//	metricsHandler := common_metrics.NewCommonMetricsHandler(mainConfig, logger)
//	metricsServer := common_metrics.NewHTTPServer(metricsHandler, logger)
//	if err := metricsServer.Start(); err != nil {
//		panic(err)
//	}
//	defer metricsServer.Close()
//	defer metricsHandler.Close()
//
//	// Record a database query
//	metricsHandler.RecordDBQuery("select", queryDuration, true)
//
//	// Track authentication attempt
//	metricsHandler.RecordAuthAttempt("email", true, "")
package common_metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsHandlerInterface defines the contract for metrics collection and management
// in the go-common library. Implementations of this interface provide methods for
// recording various operational metrics and managing the metrics infrastructure.
//
// The interface is designed to be mockable for testing and allows for different
// implementations if needed (e.g., for different metrics backends).
type MetricsHandlerInterface interface {
	// Core metrics infrastructure methods

	// GetRegistry returns the underlying Prometheus registry used for metric registration.
	// This allows external components to register custom metrics if needed.
	GetRegistry() *prometheus.Registry
	// GetNamespace returns the stable namespace used to register metrics.
	GetNamespace() string
	// IsPushEnabled indicates whether metrics push mode is enabled.
	// When true, metrics are periodically pushed to a remote endpoint.
	IsPushEnabled() bool
	// IsEnabled indicates whether the metrics pull endpoint is enabled.
	// When true, metrics can be scraped via the HTTP endpoint.
	IsEnabled() bool
	// GetEndpoint returns the HTTP path where metrics are exposed (e.g., "/metrics").
	GetEndpoint() string
	// GetPort returns the port number for the metrics HTTP server.
	GetPort() string
	// GetHandler returns the HTTP handler for serving metrics in Prometheus format.
	GetHandler() http.Handler

	// Database metrics methods

	// RecordDBStats records current database connection pool statistics.
	// Parameters:
	//   - openConnections: Number of currently open connections
	//   - idleConnections: Number of idle connections in the pool
	RecordDBStats(openConnections, idleConnections int)
	// RecordDBQuery records metrics for a database query execution.
	// Parameters:
	//   - queryType: Type of query (e.g., "select", "insert", "update", "delete")
	//   - duration: Time taken to execute the query
	//   - success: Whether the query executed successfully
	RecordDBQuery(queryType string, duration time.Duration, success bool)

	// Auth metrics methods

	// RecordAuthAttempt records an authentication attempt.
	// Parameters:
	//   - provider: Authentication provider (e.g., "email", "google", "github")
	//   - success: Whether the authentication was successful
	//   - failureReason: Reason for failure if success is false (e.g., "invalid_credentials", "user_not_found")
	RecordAuthAttempt(provider string, success bool, failureReason string)
	// RecordTokenGeneration increments the counter for authentication tokens generated.
	RecordTokenGeneration()
	// RecordTokenValidation records a token validation attempt.
	// Parameters:
	//   - valid: Whether the token was valid
	RecordTokenValidation(valid bool)
	// RecordAuthCacheAccess records authentication cache access.
	// Parameters:
	//   - hit: Whether the cache lookup was a hit (true) or miss (false)
	RecordAuthCacheAccess(hit bool)
	// RecordPasswordAddition records password additions to OAuth accounts.
	// Parameters:
	//   - provider: The OAuth provider the user originally signed up with
	//   - status: Either "success" or "failure"
	RecordPasswordAddition(provider string, status string)
	// RecordAuthMethodQuery records authentication method queries.
	// Parameters:
	//   - hasPassword: "true" if user has password, "false" otherwise
	//   - oauthProvidersCount: Number of OAuth providers linked (e.g., "0", "1", "2+")
	RecordAuthMethodQuery(hasPassword string, oauthProvidersCount string)
	// RecordEmailVerificationRequest records email verification requests for existing users.
	// Parameters:
	//   - status: Either "success", "failure", or "rate_limited"
	//   - reason: Reason for the status (e.g., "sent", "already_verified", "cooldown", "error")
	RecordEmailVerificationRequest(status string, reason string)

	// OTP/2FA metrics methods

	// RecordOTPSetupAttempt records OTP setup attempts.
	// Parameters:
	//   - status: Either "success", "failure", or "rate_limited"
	RecordOTPSetupAttempt(status string)
	// RecordOTPVerificationAttempt records OTP verification attempts.
	// Parameters:
	//   - context: Context where OTP was used ("login", "setup_verification")
	//   - status: Either "success", "failure", or "invalid_code"
	RecordOTPVerificationAttempt(context string, status string)
	// RecordOTPManagementOperation records OTP management operations.
	// Parameters:
	//   - operation: Operation performed ("setup", "disable", "re_setup")
	//   - status: Either "success", "failure", or "unauthorized"
	RecordOTPManagementOperation(operation string, status string)
	// RecordOTPSetupDuration records time taken to complete OTP setup.
	// Parameters:
	//   - duration: Duration in seconds
	RecordOTPSetupDuration(duration float64)

	// MinIO metrics methods

	// RecordMinioOperation records metrics for a MinIO object storage operation.
	// Parameters:
	//   - operation: Type of operation (e.g., "get", "put", "delete", "list")
	//   - duration: Time taken to complete the operation
	//   - bytesTransferred: Number of bytes transferred (0 for operations without data transfer)
	//   - direction: Data flow direction ("upload" or "download", empty for non-transfer operations)
	//   - success: Whether the operation completed successfully
	RecordMinioOperation(operation string, duration time.Duration, bytesTransferred int64, direction string, success bool)

	// Rate limiter metrics methods

	// RecordRateLimitCheck records the result and duration of a rate limit check.
	// Parameters:
	//   - action: The action being rate limited (e.g., "login", "register", "password_reset")
	//   - identifierType: Type of identifier used (e.g., "ip", "user_id", "email")
	//   - allowed: Whether the request was allowed by the rate limiter
	//   - duration: Time taken to perform the rate limit check in seconds
	RecordRateLimitCheck(action string, identifierType string, allowed bool, duration float64)
	// RecordRateLimitThrottle records when a request is throttled by the rate limiter.
	// This is a convenience method that only records throttled requests.
	// Parameters:
	//   - action: The action that was throttled
	//   - identifierType: Type of identifier that triggered the throttle
	RecordRateLimitThrottle(action string, identifierType string)
	// RecordRateLimitAllow records when a request is allowed by the rate limiter.
	// This is a convenience method that only records allowed requests.
	// Parameters:
	//   - action: The action that was allowed
	//   - identifierType: Type of identifier for the allowed request
	RecordRateLimitAllow(action string, identifierType string)

	// Contact form metrics methods

	// RecordContactSubmission records metrics for a contact form submission.
	// Parameters:
	//   - status: The submission status ("success", "failure", "rate_limited")
	//   - duration: Time taken to process the submission in seconds
	RecordContactSubmission(status string, duration float64)
	// RecordContactSpamDetected increments the counter for contact submissions detected as spam.
	RecordContactSpamDetected()

	// Attachment metrics methods

	// RecordAttachmentUpload records metrics for an attachment upload attempt.
	// Parameters:
	//   - objectType: The type of attachment ("profile-picture", "attachment", "document", etc.)
	//   - status: The upload status ("success", "failure", "validation_error")
	RecordAttachmentUpload(objectType string, status string)
	// RecordAttachmentProcessing records the processing duration for an attachment.
	// Parameters:
	//   - objectType: The type of attachment being processed
	//   - duration: Processing time in seconds
	RecordAttachmentProcessing(objectType string, duration float64)
	// RecordAttachmentDeduplication tracks when an attachment is deduplicated.
	// Parameters:
	//   - saved: Whether the upload was saved (false) or deduplicated (true)
	RecordAttachmentDeduplication(saved bool)
	// RecordAttachmentBytes tracks the total bytes uploaded for attachments.
	// Parameters:
	//   - objectType: The type of attachment
	//   - bytes: Number of bytes uploaded
	RecordAttachmentBytes(objectType string, bytes int64)
	// RecordAttachmentValidationFailure tracks validation failures for attachments.
	// Parameters:
	//   - objectType: The type of attachment that failed validation
	//   - reason: The reason for failure ("too_large", "invalid_type", "virus_detected", "processing_error")
	RecordAttachmentValidationFailure(objectType string, reason string)

	// User metrics methods

	// RecordUserProfileUpdate records metrics for user profile updates.
	// Parameters:
	//   - category: The category of profile update ("identity", "preferences", "avatar")
	RecordUserProfileUpdate(category string)
	// RecordUserRegistration records metrics for new user registrations.
	// Parameters:
	//   - method: The registration method ("email", "oauth")
	RecordUserRegistration(method string)
	// RecordUserSettingsUpdate increments the counter for user settings updates.
	RecordUserSettingsUpdate()

	// Email service metrics methods

	// RecordEmailSent records metrics for email sending attempts.
	// Parameters:
	//   - provider: The email service provider ("gmail", "postal", "resend")
	//   - emailType: The type of email ("verification", "password_reset", "notification")
	//   - status: The sending status ("success", "failure")
	//   - duration: Time taken to send the email in seconds
	RecordEmailSent(provider string, emailType string, status string, duration float64)
	// RecordEmailTemplateError records template rendering errors.
	// Parameters:
	//   - category: The template category ("auth", "notification", "admin")
	RecordEmailTemplateError(category string)

	// Background-worker metrics methods

	// ObserveWorkerStageDuration records execution time and increments outcome totals for a worker stage.
	// Parameters:
	//   - worker: The logical worker label
	//   - stage: The worker stage label
	//   - outcome: The outcome label for the completed stage execution
	//   - d: The elapsed execution duration to observe
	ObserveWorkerStageDuration(worker, stage, outcome string, d time.Duration)
	// ObserveWorkerStageQueueWait records queue-wait time before a worker stage claim.
	// Parameters:
	//   - worker: The logical worker label
	//   - stage: The worker stage label
	//   - d: The elapsed queue-wait duration to observe
	ObserveWorkerStageQueueWait(worker, stage string, d time.Duration)
	// AddWorkerStageInFlight adjusts the in-flight gauge for a worker stage.
	// Parameters:
	//   - worker: The logical worker label
	//   - stage: The worker stage label
	//   - delta: The signed adjustment applied to the in-flight gauge
	AddWorkerStageInFlight(worker, stage string, delta float64)
	// SetWorkerStageQueueDepth sets the queued-job gauge for a worker stage.
	// Parameters:
	//   - worker: The logical worker label
	//   - stage: The worker stage label
	//   - depth: The absolute queue depth value to publish
	SetWorkerStageQueueDepth(worker, stage string, depth int64)
	// SetWorkerBacklog sets a worker-specific reconciliation backlog gauge.
	// Parameters:
	//   - worker: The logical worker label
	//   - kind: The low-cardinality backlog kind label
	//   - count: The absolute backlog count to publish
	SetWorkerBacklog(worker, kind string, count int64)
	// IncWorkerStaleRecovered increments the stale-recovery counter for a worker stage by n.
	// Parameters:
	//   - worker: The logical worker label
	//   - stage: The worker stage label
	//   - n: The number of recovered jobs to add (non-positive values are ignored)
	IncWorkerStaleRecovered(worker, stage string, n int)
	// IncWorkerHeartbeatFailure increments the heartbeat-failure counter for a worker.
	// Parameters:
	//   - worker: The logical worker label
	IncWorkerHeartbeatFailure(worker string)
	// IncWorkerPoll increments the poll counter for a worker by result label.
	// Parameters:
	//   - worker: The logical worker label
	//   - result: The poll-outcome label (e.g. "claimed", "idle", "error")
	IncWorkerPoll(worker, result string)
	// Realtime websocket metrics methods
	//
	// These are the surface the realtime hub records through (common_ws.RealtimeMetrics). Every
	// label value below is server-owned — a fixed reason/code constant or a registered topic
	// namespace — never a raw client-supplied topic.

	// RecordRealtimeConnectionOpened records an accepted realtime websocket connection.
	RecordRealtimeConnectionOpened()
	// RecordRealtimeConnectionClosed records a realtime websocket disconnect.
	// Parameters:
	//   - reason: Why the connection ended ("client_eof", "read_error", "backpressure", "pong_timeout", ...)
	RecordRealtimeConnectionClosed(reason string)
	// RecordRealtimeSubscribed records a successful entity-topic subscription.
	// Parameters:
	//   - namespace: The registered authorizer namespace covering the topic (e.g. "grading.submission")
	RecordRealtimeSubscribed(namespace string)
	// RecordRealtimeUnsubscribed records an entity-topic subscription being dropped.
	// Parameters:
	//   - namespace: The registered authorizer namespace covering the topic
	RecordRealtimeUnsubscribed(namespace string)
	// RecordRealtimeSubscribeDenied records a rejected topic subscribe.
	// Parameters:
	//   - errorCode: The per-topic denial code ("invalid_topic", "reserved_topic", "unknown_namespace",
	//     "unauthorized_topic", "subscription_limit")
	RecordRealtimeSubscribeDenied(errorCode string)
	// RecordRealtimeAuthorizerCall records one topic-authorizer invocation.
	// Parameters:
	//   - namespace: The registered authorizer namespace that was consulted
	//   - duration: Time the authorizer took in seconds
	RecordRealtimeAuthorizerCall(namespace string, duration float64)
	// RecordRealtimePublish records the fan-out of a single realtime frame.
	// Parameters:
	//   - delivered: Number of connections the frame was enqueued on
	//   - dropped: Number of connections that could not accept the frame
	RecordRealtimePublish(delivered int, dropped int)
	// RecordRealtimePublishFailure records a realtime frame that never reached fan-out. These are
	// server faults rather than client behavior: a frame go-common could not build, or one that
	// arrived at a hub which is shutting down.
	// Parameters:
	//   - cause: Why the frame was not published ("marshal_payload", "marshal_envelope",
	//     "marshal_command_result", "hub_closed")
	RecordRealtimePublishFailure(cause string)
}

// Verify that CommonMetricsHandler implements the interface
var _ MetricsHandlerInterface = (*CommonMetricsHandler)(nil)
