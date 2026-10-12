// Package common_metrics provides Prometheus metrics functionality for applications.
// See interface.go for the main package documentation.
package common_metrics

import (
	"context"
	"github.com/Suhaibinator/go-common/pkg/config"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	commonMetricsLoggerName     = "common_metrics"
	faroLoggerName              = "faro"
	metricsHTTPServerLoggerName = "metrics_http_server"
)

// CommonMetricsHandler defines structured state used in the metrics instrumentation and export flow.
//
// Used in flow:
// - CommonMetricsHandler is read or populated by backend components in this package to move requests through this stage.
type CommonMetricsHandler struct {
	provider config.MetricsProvider
	// startup pins the immutable generation used for registered namespace and listener identity.
	startup config.MetricsView
	// logger is used for error reporting and debugging
	logger *zap.Logger

	// registry is the Prometheus registry for metric registration
	registry *prometheus.Registry
	pushDone chan struct{}
	// pushCancelCtx and pushCancel control the push goroutine lifecycle
	pushCancelCtx context.Context
	pushCancel    context.CancelFunc

	// Database metrics
	// dbConnectionsOpen tracks the number of open database connections
	dbConnectionsOpen prometheus.Gauge
	// dbConnectionsIdle tracks the number of idle database connections
	dbConnectionsIdle prometheus.Gauge
	// dbQueryDuration measures query execution time by query type
	dbQueryDuration *prometheus.HistogramVec
	// dbQueriesTotal counts total queries by type and status
	dbQueriesTotal *prometheus.CounterVec
	// dbTransactionsOpen tracks currently open database transactions
	dbTransactionsOpen prometheus.Gauge

	// MinIO metrics
	// minioOperationsTotal counts MinIO operations by type and status
	minioOperationsTotal *prometheus.CounterVec
	// minioOperationDuration measures MinIO operation latency
	minioOperationDuration *prometheus.HistogramVec
	// minioBytesTransferred tracks bytes uploaded/downloaded
	minioBytesTransferred *prometheus.CounterVec

	// Auth metrics
	// authAttemptsTotal counts all authentication attempts by provider
	authAttemptsTotal *prometheus.CounterVec
	// sessionRefreshesTotal counts probabilistic session token refresh attempts by outcome
	sessionRefreshesTotal *prometheus.CounterVec
	// authSuccessTotal counts successful authentications by provider
	authSuccessTotal *prometheus.CounterVec
	// authFailureTotal counts failed authentications by provider and reason
	authFailureTotal *prometheus.CounterVec
	// authTokensGenerated counts total authentication tokens created
	authTokensGenerated prometheus.Counter
	// authTokensValidated counts total token validation attempts
	authTokensValidated prometheus.Counter
	// authTokensInvalidated counts tokens found to be invalid
	authTokensInvalidated prometheus.Counter
	// authCacheHits counts successful cache lookups
	authCacheHits prometheus.Counter
	// authCacheMisses counts cache lookup failures
	authCacheMisses prometheus.Counter
	// authPasswordAdditionsTotal counts password additions to OAuth accounts
	authPasswordAdditionsTotal *prometheus.CounterVec
	// authMethodQueriesTotal counts authentication method queries
	authMethodQueriesTotal *prometheus.CounterVec
	// emailVerificationRequestsTotal counts email verification requests for existing users
	emailVerificationRequestsTotal *prometheus.CounterVec

	// OTP/2FA metrics
	// otpSetupAttemptsTotal counts OTP setup attempts by status
	otpSetupAttemptsTotal *prometheus.CounterVec
	// otpVerificationAttemptsTotal counts OTP verification attempts by context and status
	otpVerificationAttemptsTotal *prometheus.CounterVec
	// otpManagementOperationsTotal counts OTP management operations
	otpManagementOperationsTotal *prometheus.CounterVec
	// otpSetupDuration measures time to complete OTP setup process
	otpSetupDuration prometheus.Histogram

	// System metrics
	// uptimeSeconds tracks application uptime in seconds
	uptimeSeconds prometheus.Counter
	// uptimeCancel controls the uptime tracking goroutine
	uptimeCancel context.CancelFunc

	// Rate limiter metrics
	// rateLimitThrottledTotal counts throttled requests by action and identifier type
	rateLimitThrottledTotal *prometheus.CounterVec
	// rateLimitAllowedTotal counts allowed requests by action and identifier type
	rateLimitAllowedTotal *prometheus.CounterVec
	// rateLimitCheckDuration measures rate limit check latency
	rateLimitCheckDuration *prometheus.HistogramVec

	// Contact form metrics
	// contactSubmissionsTotal counts contact form submissions by status
	contactSubmissionsTotal *prometheus.CounterVec
	// contactSpamDetectedTotal counts spam detections
	contactSpamDetectedTotal prometheus.Counter
	// contactProcessingDuration measures contact form processing time
	contactProcessingDuration prometheus.Histogram

	// Attachment metrics
	// attachmentUploadsTotal counts attachment uploads by type and status
	attachmentUploadsTotal *prometheus.CounterVec
	// attachmentProcessingDuration measures attachment processing time
	attachmentProcessingDuration *prometheus.HistogramVec
	// attachmentDeduplicationTotal tracks deduplicated attachments
	attachmentDeduplicationTotal *prometheus.CounterVec
	// attachmentBytesUploaded tracks total bytes uploaded
	attachmentBytesUploaded *prometheus.CounterVec
	// attachmentValidationFailures tracks validation failures
	attachmentValidationFailures *prometheus.CounterVec

	// User metrics
	// userProfileUpdatesTotal counts profile updates by category
	userProfileUpdatesTotal *prometheus.CounterVec
	// userRegistrationsTotal counts new registrations by method
	userRegistrationsTotal *prometheus.CounterVec
	// userSettingsUpdatesTotal counts settings updates
	userSettingsUpdatesTotal prometheus.Counter

	// Email metrics
	// emailSentTotal counts emails sent by provider, type and status
	emailSentTotal *prometheus.CounterVec
	// emailSendDuration measures email sending latency
	emailSendDuration *prometheus.HistogramVec
	// emailTemplateErrors counts template rendering errors
	emailTemplateErrors *prometheus.CounterVec

	// Tool-approval expiry sweep metrics. These expose catch-up health without adding another
	// backlog-count query to the already database-sensitive worker.
	toolApprovalExpirySweepDurationSeconds prometheus.Histogram
	toolApprovalExpiryCandidatesScanned    prometheus.Counter
	toolApprovalExpiryCandidatesResolved   prometheus.Counter
	toolApprovalExpiryCandidatesFailed     prometheus.Counter
	toolApprovalExpiryBacklogPresent       prometheus.Gauge
	toolApprovalExpiryOldestOverdueSeconds prometheus.Gauge

	// Agent-session input metrics (metrics_agent_input.go).
	chatAgentInputPendingDepth    prometheus.Histogram
	chatAgentInputAdmissions      *prometheus.CounterVec
	chatAgentInputAdmissionWait   *prometheus.HistogramVec
	chatAgentRunCancelPropagation prometheus.Histogram

	// Agent delegation metrics (metrics_agent_delegation.go).
	chatAgentChildrenCreated       *prometheus.CounterVec
	chatAgentChildrenActive        *prometheus.GaugeVec
	chatAgentChildrenTerminal      *prometheus.CounterVec
	chatAgentGraphDriveOutcomes    *prometheus.CounterVec
	chatAgentGraphBudgetExhausted  *prometheus.CounterVec
	chatAgentGraphEvents           *prometheus.CounterVec
	chatAgentGraphParked           *prometheus.CounterVec
	chatAgentGraphParkRefused      prometheus.Counter
	chatAgentGraphDriveSuspensions *prometheus.CounterVec
	chatAgentWakeGroupCompletions  prometheus.Counter

	notificationDeliveriesTotal         *prometheus.CounterVec
	notificationAttemptDuration         prometheus.Histogram
	notificationSendsTotal              prometheus.Counter
	notificationEnqueueToSend           prometheus.Histogram
	notificationSuppressionsTotal       *prometheus.CounterVec
	notificationClaimLateness           prometheus.Histogram
	notificationEmitBatchRecipients     *prometheus.HistogramVec
	notificationInboxCandidatesExamined *prometheus.HistogramVec
	notificationCountCompletions        *prometheus.CounterVec
	notificationDeferralsTotal          *prometheus.CounterVec
	notificationFanoutPending           prometheus.Gauge
	notificationFanoutPendingSaturated  prometheus.Gauge
	notificationFanoutOldestPendingAge  prometheus.Gauge
	notificationFanoutPaused            prometheus.Gauge
	notificationFanoutActiveSteps       prometheus.Gauge
	notificationFanoutSampleTimestamp   prometheus.Gauge
	notificationFanoutSampleSuccess     prometheus.Gauge
	notificationFanoutSampleErrors      prometheus.Counter
	notificationFanoutEnqueues          *prometheus.CounterVec
	notificationFanoutSteps             *prometheus.CounterVec
	notificationFanoutStepDuration      *prometheus.HistogramVec
	notificationFanoutRecipients        prometheus.Counter
	notificationFanoutLateness          prometheus.Histogram
	notificationFanoutCompletion        prometheus.Histogram

	// AI control-plane metrics. All labels are bounded (provider, registered operation symbolic
	// name, status/outcome/kind, recorder stage); trace IDs, user IDs, and error strings
	// are never used as labels.
	inferenceRequestTPS                   *prometheus.HistogramVec
	inferenceGenerationTPS                *prometheus.HistogramVec
	inferenceAttemptsTotal                *prometheus.CounterVec
	inferenceAttemptLatencySeconds        *prometheus.HistogramVec
	inferenceAttemptTokensTotal           *prometheus.CounterVec
	inferenceRetriesTotal                 *prometheus.CounterVec
	inferenceThrottleWaitsTotal           *prometheus.CounterVec
	inferenceBackoffSeconds               *prometheus.HistogramVec
	inferenceRouteResolutionFailuresTotal prometheus.Counter
	inferenceRecorderFailuresTotal        *prometheus.CounterVec
	inferencePolicyBlocksTotal            *prometheus.CounterVec

	// Background-worker metrics. Generic collectors that apply to every instrumented
	// worker built on pkg/worker, keyed by a low-cardinality "worker" label plus a
	// "stage" label (and bounded outcome/result/kind labels). Trace IDs, user IDs, and
	// error strings are never used as labels.
	workerStageDuration     *prometheus.HistogramVec
	workerStageJobsTotal    *prometheus.CounterVec
	workerStageQueueWait    *prometheus.HistogramVec
	workerStageInFlight     *prometheus.GaugeVec
	workerStageQueueDepth   *prometheus.GaugeVec
	workerBacklog           *prometheus.GaugeVec
	workerStaleRecovered    *prometheus.CounterVec
	workerHeartbeatFailures *prometheus.CounterVec
	workerPollTotal         *prometheus.CounterVec
	// Realtime websocket metrics
	// realtimeConnectionsActive tracks currently served realtime websocket connections
	realtimeConnectionsActive prometheus.Gauge
	// realtimeConnectionsTotal counts accepted realtime websocket connections
	realtimeConnectionsTotal prometheus.Counter
	// realtimeDisconnectsTotal counts closed realtime connections by disconnect reason
	realtimeDisconnectsTotal *prometheus.CounterVec
	// realtimeSubscriptionsActive tracks live entity-topic subscriptions by registered namespace
	realtimeSubscriptionsActive *prometheus.GaugeVec
	// realtimeSubscribeDenialsTotal counts denied topic subscribes by error code
	realtimeSubscribeDenialsTotal *prometheus.CounterVec
	// realtimeAuthorizerDuration measures topic-authorizer latency by registered namespace
	realtimeAuthorizerDuration *prometheus.HistogramVec
	// realtimeEventsPublishedTotal counts realtime frames handed to the hub
	realtimeEventsPublishedTotal prometheus.Counter
	// realtimeEventsDeliveredTotal counts realtime frames enqueued onto a connection
	realtimeEventsDeliveredTotal prometheus.Counter
	// realtimeEventsDroppedTotal counts realtime frames dropped by an unavailable connection
	realtimeEventsDroppedTotal prometheus.Counter
	// realtimePublishFailuresTotal counts realtime frames that never reached fan-out, by cause
	realtimePublishFailuresTotal   *prometheus.CounterVec
	chatRealtimeGrantsActive       prometheus.Gauge
	chatRealtimeRenewalsTotal      *prometheus.CounterVec
	chatRealtimeInvalidationsTotal *prometheus.CounterVec
	chatRecipientCacheTotal        *prometheus.CounterVec
}

// NewCommonMetricsHandler constructs a metrics handler from an already
// sampled provider; listener identity is captured and export policy remains hot.
func NewCommonMetricsHandler(provider config.MetricsProvider, logger *zap.Logger) *CommonMetricsHandler {
	view := provider()
	namespace := view.MetricsNamespace()
	registry := prometheus.NewRegistry()

	h := &CommonMetricsHandler{
		startup:  view,
		provider: provider,
		logger:   logger,
		registry: registry,
	}

	h.registerDBMetrics(namespace)
	h.registerMinioMetrics(namespace)
	h.registerAuthMetrics(namespace)
	h.registerSystemMetrics(namespace)
	h.registerRateLimiterMetrics(namespace)
	h.registerContactMetrics(namespace)
	h.registerAttachmentMetrics(namespace)
	h.registerUserMetrics(namespace)
	h.registerEmailMetrics(namespace)
	h.registerToolApprovalMetrics(namespace)
	h.registerAgentInputMetrics(namespace)
	h.registerAgentDelegationMetrics(namespace)
	h.registerNotificationMetrics(namespace)
	h.registerInferenceControlPlaneMetrics(namespace)
	h.registerWorkerMetrics(namespace)
	h.registerRealtimeMetrics(namespace)

	h.startPushing()

	return h
}

// Close performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) Close() error {
	if h.pushCancel != nil {
		h.pushCancel()
		if h.pushDone != nil {
			<-h.pushDone
		}
	}
	if h.uptimeCancel != nil {
		h.uptimeCancel()
	}
	return nil
}

// GetHandler performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) GetHandler() http.Handler {
	return promhttp.HandlerFor(h.registry, promhttp.HandlerOpts{})
}

// Handler performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) Handler() http.Handler {
	return h.GetHandler()
}

// GetRegistry performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) GetRegistry() *prometheus.Registry {
	return h.registry
}

// GetNamespace returns the namespace used to register this handler's metrics.
func (h *CommonMetricsHandler) GetNamespace() string {
	if h == nil || h.startup == nil {
		return ""
	}
	return h.startup.MetricsNamespace()
}

// IsEnabled performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) IsEnabled() bool {
	if h == nil || h.startup == nil {
		return false
	}
	return h.startup.MetricsEnabled()
}

// GetPort performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) GetPort() string {
	if h == nil || h.startup == nil {
		return ""
	}
	return h.startup.MetricsPort()
}

// GetEndpoint performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) GetEndpoint() string {
	if h == nil || h.startup == nil {
		return "/metrics"
	}
	return h.startup.MetricsEndpoint()
}

// IsPushEnabled returns whether metrics push mode is enabled.

// IsPushEnabled performs its package-specific work in the metrics instrumentation and export flow.
func (h *CommonMetricsHandler) IsPushEnabled() bool {
	if h == nil || h.provider == nil {
		return false
	}
	return h.provider().MetricsPushEnabled()
}
