package common_ws

// Disconnect reasons recorded on RecordRealtimeConnectionClosed. They are a closed, server-owned set
// precisely so the metric's "reason" label can never be influenced by a client.
const (
	// disconnectReasonHandlerExit is the normal end of ServeConnection.
	disconnectReasonHandlerExit = "handler_exit"
	// disconnectReasonContextDone means the request context that owns the connection was cancelled.
	disconnectReasonContextDone = "context_done"
	// disconnectReasonClientEOF means the client closed the socket.
	disconnectReasonClientEOF = "client_eof"
	// disconnectReasonReadError means a read failed for any reason other than a clean EOF.
	disconnectReasonReadError = "read_error"
	// disconnectReasonWriteError means a frame could not be written to the socket.
	disconnectReasonWriteError = "write_error"
	// disconnectReasonBackpressure means the connection's bounded send queue overflowed.
	disconnectReasonBackpressure = "backpressure"
	// disconnectReasonPongTimeout means the client stopped answering keepalive pings.
	disconnectReasonPongTimeout = "pong_timeout"
	// disconnectReasonPingFailed means a keepalive ping could not be written.
	disconnectReasonPingFailed = "ping_failed"
	// disconnectReasonHubClose means the hub is shutting down.
	disconnectReasonHubClose = "hub_close"
	// disconnectReasonSessionInvalid means session expiry, revocation, or a ban ended access.
	disconnectReasonSessionInvalid = "session_invalid"
)

// Causes recorded on RecordRealtimePublishFailure. Like the disconnect reasons above they are a
// closed, server-owned set: every one of them means go-common could not hand a frame to a client for
// a reason the client had no part in.
const (
	// publishFailureMarshalPayload means the application's payload message could not be serialized,
	// so no frame of the publish was built.
	publishFailureMarshalPayload = "marshal_payload"
	// publishFailureMarshalEnvelope means the RealtimeEvent envelope around an already-serialized
	// payload could not be built for one topic.
	publishFailureMarshalEnvelope = "marshal_envelope"
	// publishFailureMarshalCommandResult means a subscribe/unsubscribe ack could not be serialized,
	// so the client never learns the outcome of a command the hub already applied.
	publishFailureMarshalCommandResult = "marshal_command_result"
	// publishFailureHubClosed means a serialized frame reached a hub that is shutting down and was
	// dropped. Unlike the marshal causes this one is expected during shutdown; it is counted so a
	// steady rate outside shutdown is visible.
	publishFailureHubClosed = "hub_closed"
)

// metricNamespaceUnknown labels subscription accounting for a topic with no registered authorizer.
// It exists only as a safety valve: a topic cannot become subscribed without a matching authorizer,
// so this value should never appear in practice.
const metricNamespaceUnknown = "unknown"

// RealtimeMetrics is the optional instrumentation surface the hub records through. It is an
// interface rather than a concrete dependency so common_ws stays free of a metrics-backend import,
// and so applications can supply their own recorder; common_metrics.MetricsHandlerInterface
// satisfies it as-is.
//
// Cardinality contract: implementations may assume every label value they receive is server-owned.
// Reasons and error codes are package constants, and a namespace is always a *registered* authorizer
// prefix — never the raw topic a client asked to subscribe to.
type RealtimeMetrics interface {
	// RecordRealtimeConnectionOpened records an accepted connection.
	RecordRealtimeConnectionOpened()
	// RecordRealtimeConnectionClosed records a disconnect and its reason.
	RecordRealtimeConnectionClosed(reason string)
	// RecordRealtimeSubscribed records a successful entity-topic subscription.
	RecordRealtimeSubscribed(namespace string)
	// RecordRealtimeUnsubscribed records an entity-topic subscription being dropped.
	RecordRealtimeUnsubscribed(namespace string)
	// RecordRealtimeSubscribeDenied records a per-topic subscribe denial by error code.
	RecordRealtimeSubscribeDenied(errorCode string)
	// RecordRealtimeAuthorizerCall records one authorizer invocation and its duration in seconds.
	RecordRealtimeAuthorizerCall(namespace string, duration float64)
	// RecordRealtimePublish records one published frame and how it fanned out.
	RecordRealtimePublish(delivered int, dropped int)
	// RecordRealtimePublishFailure records a frame that never reached fan-out, by cause. The causes
	// are a closed server-owned set ("marshal_payload", "marshal_envelope", "marshal_command_result",
	// "hub_closed") and describe server faults, not client behavior.
	RecordRealtimePublishFailure(cause string)
}

// noopRealtimeMetrics is what NewHub substitutes when no recorder is supplied, so every call site
// can record unconditionally instead of guarding a nil interface.
type noopRealtimeMetrics struct{}

var _ RealtimeMetrics = noopRealtimeMetrics{}

// RecordRealtimeConnectionOpened discards the observation.
func (noopRealtimeMetrics) RecordRealtimeConnectionOpened() {}

// RecordRealtimeConnectionClosed discards the observation.
func (noopRealtimeMetrics) RecordRealtimeConnectionClosed(string) {}

// RecordRealtimeSubscribed discards the observation.
func (noopRealtimeMetrics) RecordRealtimeSubscribed(string) {}

// RecordRealtimeUnsubscribed discards the observation.
func (noopRealtimeMetrics) RecordRealtimeUnsubscribed(string) {}

// RecordRealtimeSubscribeDenied discards the observation.
func (noopRealtimeMetrics) RecordRealtimeSubscribeDenied(string) {}

// RecordRealtimeAuthorizerCall discards the observation.
func (noopRealtimeMetrics) RecordRealtimeAuthorizerCall(string, float64) {}

// RecordRealtimePublish discards the observation.
func (noopRealtimeMetrics) RecordRealtimePublish(int, int) {}

// RecordRealtimePublishFailure discards the observation.
func (noopRealtimeMetrics) RecordRealtimePublishFailure(string) {}
