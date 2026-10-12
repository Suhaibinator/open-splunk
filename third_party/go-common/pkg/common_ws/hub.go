package common_ws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

var (
	ErrHubClosed                = errors.New("realtime hub is closed")
	ErrBackpressure             = errors.New("realtime connection backpressure")
	ErrNilConnection            = errors.New("nil websocket connection")
	ErrInvalidUserID            = errors.New("invalid user id")
	ErrTooManyConns             = errors.New("too many active realtime connections for user")
	ErrHubStarted               = errors.New("realtime hub already started; registration is construction-time only")
	ErrNilTopicAuthorizer       = errors.New("nil realtime topic authorizer")
	ErrInvalidTopic             = errors.New("invalid realtime topic")
	ErrReservedTopic            = errors.New("reserved realtime topic namespace")
	ErrAuthorizerAlreadyBound   = errors.New("realtime topic authorizer already registered")
	ErrCommandAuthBudgetTooLong = errors.New("realtime command auth budget must be shorter than the keepalive budget")
	ErrNilRealtimeProvider      = errors.New("nil realtime configuration provider")
)

const (
	chatRecipientResolverLoggerName     = "chat_recipient_resolver"
	realtimeHubLoggerName               = "realtime_hub"
	realtimePublisherLoggerName         = "realtime_publisher"
	realtimeTypingBroadcasterLoggerName = "realtime_typing_broadcaster"
)

// HubConfig defines the data structure used in the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
//
// Queue sizes and connection settings are captured per handshake. Admission and subscription caps
// are sampled for each new admission; lowering a cap does not evict existing connections or topics.
// Keepalive and auth settings are captured together to preserve their timing invariant.
type HubConfig struct {
	// SendQueueSize bounds memory per connection (0 = DefaultSendQueueSize).
	SendQueueSize int
	// DisconnectOnFullQueue closes slow connections when their send queue fills.
	// If nil, defaults to true.
	DisconnectOnFullQueue *bool
	// MaxConnectionsPerUser rejects new connections when exceeded
	// (0 = DefaultMaxConnectionsPerUser, negative = unlimited). Unlimited has to be spelled out
	// because an uncapped per-user connection count is an unbounded memory commitment driven by
	// client behavior.
	MaxConnectionsPerUser int
	// MaxSubscriptionsPerConnection caps entity-topic subscriptions held by one connection
	// (0 = DefaultMaxSubscriptionsPerConnection). Subscribes past the cap fail with
	// TopicErrorSubscriptionLimit.
	MaxSubscriptionsPerConnection int
	// CommandAuthBudget is the single context budget shared by every authorizer call of one command
	// (0 = DefaultCommandAuthBudget). It must stay below PingInterval + PongTimeout; the hub validates
	// that at construction, every handshake captures the three together from one configuration
	// generation, and config.ValidateConfig re-checks it for every candidate generation.
	CommandAuthBudget time.Duration

	// AllowedOrigins is the browser origin allowlist captured for a handshake.
	// A wildcard allows every origin; an empty list uses same-host validation.
	AllowedOrigins []string
	// PingInterval is the interval between server-initiated pings (0 = disabled).
	PingInterval time.Duration
	// PongTimeout is the time to wait for a pong response after sending a ping.
	PongTimeout time.Duration
	// ReadTimeout is the maximum time to wait for any client message (0 = disabled).
	ReadTimeout time.Duration
	// MaxMessageSize is the maximum size in bytes for incoming messages (0 = disabled).
	MaxMessageSize int64
}

// RealtimeView supplies immutable connection policy sampled once per handshake.
// Admission limits are required on every runtime view.
type RealtimeView interface {
	RealtimeWSSendQueueSize() int
	RealtimeWSMaxConnectionsPerUser() int
	RealtimeWSMaxSubscriptionsPerConnection() int
	// RealtimeWSAllowedOrigins returns a slice owned by the caller.
	RealtimeWSAllowedOrigins() []string
	RealtimeWSPingInterval() time.Duration
	RealtimeWSPongTimeout() time.Duration
	RealtimeWSReadTimeout() time.Duration
	RealtimeWSMaxMessageSize() int64
	RealtimeWSCommandAuthBudget() time.Duration
}

// RealtimeViewProvider returns the current immutable realtime configuration view.
// Providers can be called concurrently and must therefore be concurrency-safe.
type RealtimeViewProvider func() RealtimeView

func (c HubConfig) RealtimeWSAllowedOrigins() []string {
	return append([]string(nil), c.AllowedOrigins...)
}
func (c HubConfig) RealtimeWSPingInterval() time.Duration      { return c.PingInterval }
func (c HubConfig) RealtimeWSPongTimeout() time.Duration       { return c.PongTimeout }
func (c HubConfig) RealtimeWSReadTimeout() time.Duration       { return c.ReadTimeout }
func (c HubConfig) RealtimeWSMaxMessageSize() int64            { return c.MaxMessageSize }
func (c HubConfig) RealtimeWSCommandAuthBudget() time.Duration { return c.CommandAuthBudget }
func (c HubConfig) RealtimeWSSendQueueSize() int               { return c.SendQueueSize }
func (c HubConfig) RealtimeWSMaxConnectionsPerUser() int       { return c.MaxConnectionsPerUser }
func (c HubConfig) RealtimeWSMaxSubscriptionsPerConnection() int {
	return c.MaxSubscriptionsPerConnection
}

// NewHubConfigFromView captures all hot transport settings from one immutable view.
// The backpressure setting is construction-owned; a nil view yields an empty config.
func NewHubConfigFromView(view RealtimeView) HubConfig {
	if view == nil {
		return HubConfig{}
	}
	return HubConfig{
		AllowedOrigins:                view.RealtimeWSAllowedOrigins(),
		PingInterval:                  view.RealtimeWSPingInterval(),
		PongTimeout:                   view.RealtimeWSPongTimeout(),
		ReadTimeout:                   view.RealtimeWSReadTimeout(),
		MaxMessageSize:                view.RealtimeWSMaxMessageSize(),
		CommandAuthBudget:             view.RealtimeWSCommandAuthBudget(),
		SendQueueSize:                 view.RealtimeWSSendQueueSize(),
		MaxConnectionsPerUser:         view.RealtimeWSMaxConnectionsPerUser(),
		MaxSubscriptionsPerConnection: view.RealtimeWSMaxSubscriptionsPerConnection(),
	}
}

// withDefaults fills in every tunable a caller left at its zero value, so the rest of the package
// can read the configuration without repeating "or the default" at each use.
//
// Zero means "give me the default" for all of them. MaxConnectionsPerUser therefore cannot express
// "no cap" with a zero, and spells it with a negative value instead; both enforcement sites
// (CanAcceptConnection and the registration critical section) read negative as unlimited.
//
// Returns:
//   - result: A copy of the configuration with unset tunables filled in.
func (c HubConfig) withDefaults() HubConfig {
	out := c
	if out.SendQueueSize <= 0 {
		out.SendQueueSize = DefaultSendQueueSize
	}
	if out.MaxConnectionsPerUser == 0 {
		out.MaxConnectionsPerUser = DefaultMaxConnectionsPerUser
	}
	if out.MaxSubscriptionsPerConnection <= 0 {
		out.MaxSubscriptionsPerConnection = DefaultMaxSubscriptionsPerConnection
	}
	if out.CommandAuthBudget <= 0 {
		out.CommandAuthBudget = DefaultCommandAuthBudget
	}
	if out.DisconnectOnFullQueue == nil {
		disconnect := true
		out.DisconnectOnFullQueue = &disconnect
	}
	return out
}

// validate rejects a configuration whose command auth budget could outlive the keepalive budget.
// Authorizers run synchronously on the read goroutine, and gorilla dispatches pong control frames
// from inside ReadMessage — a command that can block longer than PingInterval + PongTimeout would
// have the ping loop disconnect the very connection it is serving.
func (c HubConfig) validate() error {
	if c.PingInterval <= 0 {
		return nil
	}
	keepalive := c.PingInterval + c.PongTimeout
	if c.CommandAuthBudget >= keepalive {
		return fmt.Errorf("%w: budget %s, keepalive %s", ErrCommandAuthBudgetTooLong, c.CommandAuthBudget, keepalive)
	}
	return nil
}

// disconnectOnFullQueue reports the backpressure policy for this configuration: whether a connection
// whose bounded send queue overflows is torn down (the at-most-once default, after which the client
// reconnects and catches up over REST) or merely has the frame dropped.
//
// Returns:
//   - result: True when a full send queue must disconnect the connection; the nil-pointer default.
func (c HubConfig) disconnectOnFullQueue() bool {
	if c.DisconnectOnFullQueue == nil {
		return true
	}
	return *c.DisconnectOnFullQueue
}

// Hub defines the data structure used in the realtime WebSocket flow for connection lifecycle,
// command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
//
// It owns three routing indexes. connsByUser doubles as the "user.{id}" topic index — registration
// *is* the user-topic subscription, which removes the registered-but-not-yet-subscribed window by
// construction. connsByTopic indexes explicitly subscribed entity topics only. connsBySession
// indexes sockets by session token so revocation closes them without scanning the hub.
//
// Delivery is at-most-once with no replay: send queues are bounded and a connection under sustained
// backpressure is disconnected, after which the client reconnects, resubscribes, and catches up over
// REST. Per-connection frame order is preserved by the single writer goroutine; there is no
// cross-connection or cross-topic ordering guarantee.
type Hub[U common_service_models.UserLike] struct {
	// realtimeViewProvider is never nil and never returns nil: construction wraps the caller's
	// provider with the validated startup fallback. Each connection retains its own captured
	// policy; the hub retains only its backpressure mode.
	realtimeViewProvider  RealtimeViewProvider
	disconnectOnFullQueue bool

	logger *zap.Logger

	// metrics is never nil: NewHub substitutes a no-op recorder when none is supplied, so call sites
	// record unconditionally.
	metrics RealtimeMetrics

	// commandRouter and authorizers are wiring, written only before the first connection is served
	// and read without a lock from connection goroutines afterwards. Registration after start is
	// rejected precisely so that stays safe.
	commandRouter        *CommandRouter
	notificationActivity *NotificationActivityStore[U]
	authorizers          map[string]TopicAuthorizer[U]

	closed atomic.Bool

	// testHookBeforeAckEnqueue is nil in production. Tests install it to observe the instant between
	// a subscription becoming live and its ack reaching the send queue; it is fired from
	// enqueueAckLocked, i.e. it is anchored to the ack rather than to a line number, so moving the ack
	// out of the membership critical section moves the hook out with it and the D5 ordering test fails.
	testHookBeforeAckEnqueue func()

	mu           sync.RWMutex
	started      bool
	connsByUser  map[common_service_models.UserIdType]map[*connection[U]]struct{}
	connsByTopic map[string]map[*connection[U]]struct{}
	// connsBySession holds several sockets per token when tabs share a session cookie.
	connsBySession map[string]map[*connection[U]]struct{}
	chat           *chatSubscriptions[U]

	wg sync.WaitGroup
}

// NewHub constructs the realtime hub from its typed configuration provider. Startup settings are
// captured once from the sampled generation; each new handshake re-captures the hot settings from
// the same provider.
//
// Parameters:
//   - cfg: Provider sampled once here for startup settings and once per handshake for hot settings.
//   - logger: Logger for transport diagnostics; a no-op logger is used when nil.
//   - metrics: Optional instrumentation recorder; nil installs a no-op recorder.
//
// Returns:
//   - result1: The constructed hub, or nil when the configuration is rejected.
//   - result2: ErrNilRealtimeProvider for a nil provider, or ErrCommandAuthBudgetTooLong when the
//     command auth budget would outlive keepalive.
func NewHub[U common_service_models.UserLike](cfg config.RealtimeProvider, logger *zap.Logger, metrics RealtimeMetrics) (*Hub[U], error) {
	if cfg == nil {
		return nil, ErrNilRealtimeProvider
	}
	return NewHubWithRealtimeViewProvider[U](
		NewHubConfigFromView(cfg()),
		func() RealtimeView { return cfg() },
		logger,
		metrics,
	)
}

// NewHubWithRealtimeViewProvider constructs a hub whose new connections capture the current origin,
// ping, pong, read-timeout, and message-size settings from the provider. Existing connections retain
// their captured connection settings. Live providers own every hot transport setting.
// A nil provider is ignored: the hub then serves the startup HubConfig, so the installed provider is
// always non-nil.
//
// Parameters:
//   - cfg: Startup tunables; unset values fall back to package defaults.
//   - provider: Source of the per-handshake hot settings; nil pins them to cfg.
//   - logger: Logger for transport diagnostics; a no-op logger is used when nil.
//   - metrics: Optional instrumentation recorder; nil installs a no-op recorder.
//
// Returns:
//   - result1: The constructed hub, or nil when the configuration is rejected.
//   - result2: ErrCommandAuthBudgetTooLong when the command auth budget would outlive keepalive.
func NewHubWithRealtimeViewProvider[U common_service_models.UserLike](cfg HubConfig, provider RealtimeViewProvider, logger *zap.Logger, metrics RealtimeMetrics) (*Hub[U], error) {
	cfg.AllowedOrigins = append([]string(nil), cfg.AllowedOrigins...)
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// The validated startup config serves handshakes when the provider is absent or returns nil.
	source, fallback := provider, RealtimeView(cfg)
	provider = func() RealtimeView {
		if source != nil {
			if view := source(); view != nil {
				return view
			}
		}
		return fallback
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if metrics == nil {
		metrics = noopRealtimeMetrics{}
	}
	return &Hub[U]{
		realtimeViewProvider:  provider,
		disconnectOnFullQueue: cfg.disconnectOnFullQueue(),
		logger:                logger,
		metrics:               metrics,
		authorizers:           make(map[string]TopicAuthorizer[U]),
		connsByUser:           make(map[common_service_models.UserIdType]map[*connection[U]]struct{}),
		connsByTopic:          make(map[string]map[*connection[U]]struct{}),
		connsBySession:        make(map[string]map[*connection[U]]struct{}),
	}, nil
}

// CaptureConnectionConfig samples the provider once and returns the coherent policy
// shared by origin validation and the accepted connection. Pass the result to
// CanAcceptConnectionWithConfig and ServeConnectionWithConfig so one handshake never samples twice.
func (h *Hub[U]) CaptureConnectionConfig() HubConfig {
	cfg := NewHubConfigFromView(h.realtimeViewProvider())
	cfg.DisconnectOnFullQueue = new(h.disconnectOnFullQueue)
	return cfg.withDefaults()
}

func (h *Hub[U]) subscriptionLimit() int {
	limit := h.realtimeViewProvider().RealtimeWSMaxSubscriptionsPerConnection()
	if limit <= 0 {
		return DefaultMaxSubscriptionsPerConnection
	}
	return limit
}

// SetCommandRouter installs the router that handles every command type the hub does not own itself.
// Registration is wiring, not runtime behavior: it is rejected once the hub has started serving.
//
// Parameters:
//   - router: Router to install; nil leaves the hub with built-in commands only.
//
// Returns:
//   - result: ErrHubStarted when a connection has already been served; nil on success.
func (h *Hub[U]) SetCommandRouter(router *CommandRouter) error {
	if h == nil {
		return ErrHubClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return ErrHubStarted
	}
	h.commandRouter = router
	return nil
}

// RegisterRealtimeCommandHandler binds a handler to a command type, creating the command router on
// first use. Subscribe and unsubscribe belong to the hub and cannot be overridden.
//
// Parameters:
//   - commandType: The RealtimeCommand type string the handler serves.
//   - handler: Handler invoked for that type.
//
// Returns:
//   - result: ErrHubStarted after the hub has started, otherwise the router's registration error.
func (h *Hub[U]) RegisterRealtimeCommandHandler(commandType string, handler CommandHandler) error {
	if h == nil {
		return ErrHubClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return ErrHubStarted
	}
	if h.commandRouter == nil {
		h.commandRouter = NewCommandRouter()
	}
	return h.commandRouter.Register(commandType, handler)
}

// RegisterTopicAuthorizer binds an authorizer to a topic namespace. Matching is by segment-boundary
// longest prefix, so "grading.submission" covers "grading.submission.42" but never
// "grading.submissionfoo.42".
//
// The namespaces go-common reserves — user.*, realtime.* and chat.* — cannot be claimed.
// Chat's internal leased subscriptions are installed through RegisterChatReadAuthorizer.
//
// Parameters:
//   - namespace: Segment prefix the authorizer covers, e.g. "grading" or "grading.submission".
//   - authorizer: Authorizer consulted for topics under that prefix.
func (h *Hub[U]) RegisterTopicAuthorizer(namespace string, authorizer TopicAuthorizer[U]) error {
	if h == nil {
		return ErrHubClosed
	}
	if authorizer == nil {
		return ErrNilTopicAuthorizer
	}
	if !ValidTopicName(namespace) {
		return ErrInvalidTopic
	}
	if isRegistrationReservedTopic(namespace) {
		return ErrReservedTopic
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return ErrHubStarted
	}
	if _, exists := h.authorizers[namespace]; exists {
		return ErrAuthorizerAlreadyBound
	}
	h.authorizers[namespace] = authorizer
	return nil
}

// CommandRouter executes this method as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
//
// Returns:
//   - result: The installed command router, or nil when none is configured.
func (h *Hub[U]) CommandRouter() *CommandRouter {
	if h == nil {
		return nil
	}
	return h.commandRouter
}

// authorizerFor resolves the authorizer covering a topic by segment-boundary longest-prefix match,
// returning it alongside the namespace it was registered under. Unregistered namespaces resolve to
// nil, which the subscribe path turns into a deny.
func (h *Hub[U]) authorizerFor(topic string) (TopicAuthorizer[U], string) {
	if h == nil || len(h.authorizers) == 0 {
		return nil, ""
	}
	for i := len(topic); i > 0; i-- {
		if i < len(topic) && topic[i] != '.' {
			continue
		}
		if authorizer, ok := h.authorizers[topic[:i]]; ok {
			return authorizer, topic[:i]
		}
	}
	return nil, ""
}

// topicMetricNamespace returns the registered authorizer namespace covering a topic. Subscription
// accounting is labeled by that prefix and never by the raw topic name, which is what keeps the
// metric's cardinality bounded by application wiring instead of by client traffic.
func (h *Hub[U]) topicMetricNamespace(topic string) string {
	if _, err := ParseChatChannelTopic(topic); err == nil && h.chat != nil {
		return "chat.channel"
	}
	if _, namespace := h.authorizerFor(topic); namespace != "" {
		return namespace
	}
	return metricNamespaceUnknown
}

// Closed reports whether the hub has been shut down. A route can use it to tell "this server is
// going away" (503) apart from "this user is at their connection cap" (429) before the handshake;
// both otherwise show up as a refused CanAcceptConnection.
//
// Returns:
//   - result: True once Close has been called, and for a nil hub.
func (h *Hub[U]) Closed() bool {
	return h == nil || h.closed.Load()
}

// CanAcceptConnection reports whether a new connection for this user would fit under the per-user
// cap. It is an optimistic pre-upgrade fast path for answering 429 before the handshake; the
// authoritative check happens inside ServeConnection's registration critical section. A negative
// MaxConnectionsPerUser means unlimited, so only a closed hub or an invalid user refuses then.
//
// Parameters:
//   - userID: Identifier of the connecting user.
func (h *Hub[U]) CanAcceptConnection(userID common_service_models.UserIdType) bool {
	if h == nil {
		return false
	}
	return h.CanAcceptConnectionWithConfig(userID, h.CaptureConnectionConfig())
}

// CanAcceptConnectionWithConfig reuses the handshake snapshot for the optimistic admission check.
func (h *Hub[U]) CanAcceptConnectionWithConfig(userID common_service_models.UserIdType, cfg HubConfig) bool {
	if h == nil || h.closed.Load() {
		return false
	}
	if userID == 0 {
		return false
	}
	limit := cfg.MaxConnectionsPerUser
	if limit == 0 {
		limit = DefaultMaxConnectionsPerUser
	}
	if limit <= 0 {
		return true
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connsByUser[userID]) < limit
}

// ServeConnection registers a connection and runs its read/write/keepalive loops until it closes.
// It takes the full user, not just an ID, because subscribe-time authorizers receive U.
//
// The per-user cap is enforced inside the registration critical section: check-then-insert across
// two lock acquisitions is a TOCTOU race under concurrent handshakes. Inserting into the per-user
// index is also the connection's user-topic subscription, so there is no window in which a
// registered connection can miss its own events.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - user: The authenticated user owning the connection.
//   - conn: The websocket transport to serve.
//   - session: The authenticated session; the socket closes at its expiry or revocation.
//
// Returns:
//   - result: ErrTooManyConns when the user is at their cap, or another registration error; nil
//     once the connection has been served to completion.
func (h *Hub[U]) ServeConnection(ctx context.Context, user U, conn WSConn, session Session) error {
	if err := h.validateConnection(user, conn, session); err != nil {
		return err
	}

	// Standalone hub users capture the provider exactly once. API callers that already captured a
	// handshake view use ServeConnectionWithConfig instead, so origin validation and connection
	// setup share one coherent view.
	return h.serveConnection(ctx, user, conn, h.CaptureConnectionConfig(), session)
}

// ServeConnectionWithConfig accepts a connection using an already captured per-handshake
// configuration. It never consults the hub's provider, allowing origin validation and connection
// setup to share one coherent RealtimeView.
//
// The supplied configuration is re-validated here. Configurations built by CaptureConnectionConfig
// are coherent by construction, but this entry point also accepts hand-built ones, and a connection
// whose auth budget outlives its keepalive window would have its own ping loop disconnect it
// mid-command — a failure worth refusing at the door rather than debugging in production.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - user: The authenticated user owning the connection.
//   - conn: The websocket transport to serve.
//   - cfg: The configuration captured for this handshake.
//   - session: The authenticated session; the socket closes at its expiry or revocation.
//
// Returns:
//   - result: ErrCommandAuthBudgetTooLong for an incoherent configuration, ErrTooManyConns when the
//     user is at their cap, or another registration error; nil once the connection has been served
//     to completion.
func (h *Hub[U]) ServeConnectionWithConfig(ctx context.Context, user U, conn WSConn, cfg HubConfig, session Session) error {
	if err := h.validateConnection(user, conn, session); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	return h.serveConnection(ctx, user, conn, cfg, session)
}

func (h *Hub[U]) validateConnection(user U, conn WSConn, session Session) error {
	if h == nil {
		return ErrHubClosed
	}
	if h.closed.Load() {
		return ErrHubClosed
	}
	if user.GetID() == 0 {
		return ErrInvalidUserID
	}
	if conn == nil {
		return ErrNilConnection
	}
	if session.ExpiresAt.IsZero() {
		return ErrInvalidSession
	}
	return nil
}

func (h *Hub[U]) serveConnection(ctx context.Context, user U, conn WSConn, cfg HubConfig, session Session) error {
	userID := user.GetID()
	c := newConnection(ctx, h, user, conn, cfg, session)

	h.mu.Lock()
	// Re-check closed under the lock: Close snapshots the connection set under the same mutex, so a
	// connection registered after that snapshot would never be torn down, and its wait-group
	// increment would race a Wait that has already reached zero.
	if h.closed.Load() {
		h.mu.Unlock()
		return ErrHubClosed
	}
	// The hub is now serving traffic, so wiring registration is closed even if this particular
	// handshake is about to be rejected.
	h.started = true
	// Count and insert atomically so concurrent handshakes for the same user cannot race past the
	// per-user limit. Use the cap captured for this handshake; existing connections are retained.
	set := h.connsByUser[userID]
	limit := cfg.MaxConnectionsPerUser
	if limit > 0 && len(set) >= limit {
		h.mu.Unlock()
		return ErrTooManyConns
	}
	if set == nil {
		set = make(map[*connection[U]]struct{})
		h.connsByUser[userID] = set
	}
	set[c] = struct{}{}
	if token := c.session.Token; token != "" {
		if h.connsBySession[token] == nil {
			h.connsBySession[token] = make(map[*connection[U]]struct{})
		}
		h.connsBySession[token][c] = struct{}{}
	}
	h.wg.Add(1)
	h.mu.Unlock()

	h.metrics.RecordRealtimeConnectionOpened()

	defer h.wg.Done()
	defer c.close(disconnectReasonHandlerExit)

	if session.Check != nil {
		// Bounded like an authorizer call and by the session's expiry; closing the socket cancels it.
		checkCtx, cancel := context.WithTimeout(c.activityCtx, c.cfg.CommandAuthBudget)
		checkCtx, cancelExpiry := context.WithDeadline(checkCtx, session.ExpiresAt)
		err := session.Check(checkCtx)
		cancelExpiry()
		cancel()
		if err != nil {
			c.close(disconnectReasonSessionInvalid)
			return nil
		}
	}
	c.serve()
	return nil
}

// Publish delivers an already-serialized frame to every connection subscribed to a topic.
// Per-user topics route through the per-user index (that index is the subscription); everything
// else routes through the entity-topic registry.
//
// Parameters:
//   - topic: Topic to publish on.
//   - payload: Serialized RealtimeEvent frame.
//
// Returns:
//   - result: ErrHubClosed when the hub is shut down; nil otherwise.
func (h *Hub[U]) Publish(topic string, payload []byte) error {
	if h == nil || h.closed.Load() {
		return ErrHubClosed
	}
	if topic == "" || len(payload) == 0 {
		return nil
	}
	if topic == TopicNamespaceChat || strings.HasPrefix(topic, TopicNamespaceChat+".") {
		return ErrReservedTopic
	}
	if userID, ok := parseUserTopic(topic); ok {
		return h.BroadcastToUsers([]common_service_models.UserIdType{userID}, payload)
	}
	h.deliver(h.snapshotTopicConnections(topic), payload)
	return nil
}

// BroadcastToUsers delivers an already-serialized frame to every connection held by the given users.
// It is the user-topic fan-out primitive: one frame is shared by every recipient, which is why
// user-topic envelopes carry the recipient-relative literal topic "user".
//
// Parameters:
//   - userIDs: Recipients of the frame.
//   - payload: Serialized RealtimeEvent frame.
//
// Returns:
//   - result: ErrHubClosed when the hub is shut down; nil otherwise.
func (h *Hub[U]) BroadcastToUsers(userIDs []common_service_models.UserIdType, payload []byte) error {
	if h == nil || h.closed.Load() {
		return ErrHubClosed
	}
	if len(userIDs) == 0 || len(payload) == 0 {
		return nil
	}

	h.deliver(h.snapshotConnections(userIDs), payload)
	return nil
}

// deliver enqueues a frame on already-snapshotted connections, disconnecting any that are under
// sustained backpressure. It must never be called while holding h.mu: connection.close() re-enters
// the hub through unregister, which takes the write lock.
func (h *Hub[U]) deliver(connections []*connection[U], payload []byte) {
	disconnect := h.disconnectOnFullQueue
	delivered, dropped := 0, 0
	for _, c := range connections {
		if c == nil {
			continue
		}
		err := c.enqueue(payload)
		if err == nil {
			delivered++
			continue
		}
		dropped++
		if errors.Is(err, ErrBackpressure) && disconnect {
			c.close(disconnectReasonBackpressure)
		}
	}
	h.metrics.RecordRealtimePublish(delivered, dropped)
}

// snapshotConnections copies the connection sets of the given users so the caller can enqueue and
// close outside the lock.
//
// Recipient lists are deduplicated here rather than trusted. They arrive from application resolvers
// and from pre-stamped member lists, and a user that appears twice would otherwise have every one of
// their connections enqueued twice: a duplicated frame for the client and, against a bounded queue,
// an avoidable step towards a backpressure disconnect.
//
// Parameters:
//   - userIDs: Recipient user IDs, in any order and with any duplicates.
//
// Returns:
//   - result: Each recipient's connections, once per connection.
func (h *Hub[U]) snapshotConnections(userIDs []common_service_models.UserIdType) []*connection[U] {
	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[common_service_models.UserIdType]struct{}, len(userIDs))
	out := make([]*connection[U], 0, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, duplicate := seen[userID]; duplicate {
			continue
		}
		seen[userID] = struct{}{}
		for c := range h.connsByUser[userID] {
			out = append(out, c)
		}
	}
	return out
}

// snapshotTopicConnections copies an entity topic's subscriber set so the caller can enqueue and
// close outside the lock.
func (h *Hub[U]) snapshotTopicConnections(topic string) []*connection[U] {
	h.mu.RLock()
	defer h.mu.RUnlock()

	set := h.connsByTopic[topic]
	if len(set) == 0 {
		return nil
	}
	out := make([]*connection[U], 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	return out
}

// unregister removes a closing connection from the per-user index and from every topic set it
// joined, deleting emptied entries so neither map grows monotonically.
//
// The subscription metric is resolved under the lock (the label is a registered namespace) but
// recorded after it: a recorder is application-supplied code, and calling into it while holding the
// hub's write lock would let a slow or locking implementation stall every publish and handshake.
func (h *Hub[U]) unregister(c *connection[U]) {
	if h == nil || c == nil {
		return
	}
	h.mu.Lock()

	if set := h.connsByUser[c.userID]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(h.connsByUser, c.userID)
		}
	}
	if set := h.connsBySession[c.session.Token]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(h.connsBySession, c.session.Token)
		}
	}

	namespaces := make([]string, 0, len(c.topics))
	for topic := range c.topics {
		namespaces = append(namespaces, h.topicMetricNamespace(topic))
		set := h.connsByTopic[topic]
		if set == nil {
			continue
		}
		delete(set, c)
		h.pruneChatGrantLocked(c, topic)
		if len(set) == 0 {
			delete(h.connsByTopic, topic)
		}
	}
	c.topics = nil
	h.mu.Unlock()

	for _, namespace := range namespaces {
		h.metrics.RecordRealtimeUnsubscribed(namespace)
	}
}

// isRegisteredLocked reports whether a connection is still in the per-user index. Callers must hold
// h.mu. Subscription commits consult it so a connection that closed mid-command cannot resurrect
// itself in the topic registry after unregister already cleaned up.
func (h *Hub[U]) isRegisteredLocked(c *connection[U]) bool {
	_, ok := h.connsByUser[c.userID][c]
	return ok
}

// Close disconnects every connection and waits for their handlers to unwind.
//
// Parameters:
//   - timeout: How long to wait for handlers to exit; <=0 waits indefinitely.
func (h *Hub[U]) Close(timeout time.Duration) {
	if h == nil {
		return
	}
	if h.closed.Swap(true) {
		return
	}
	h.mu.Lock()
	if h.chat != nil {
		h.chat.cancel()
	}
	h.mu.Unlock()
	if m, ok := h.metrics.(chatRealtimeMetrics); ok {
		m.RecordChatRealtimeGrants(0)
	}

	connections := h.snapshotAllConnections()
	for _, c := range connections {
		c.close(disconnectReasonHubClose)
	}

	if timeout <= 0 {
		h.wg.Wait()
		return
	}

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		h.logger.Named(realtimeHubLoggerName).Warn("hub close timed out",
			zap.String(logkeys.Operation, "close_realtime_hub"),
			zap.String(logkeys.Stage, "wait_for_connections"),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, timeout),
			zap.Int(appLogger.KeyConnections, len(connections)),
			zap.String(logkeys.Fallback, "return_with_connections_still_draining"),
		)
	}
}

// snapshotAllConnections copies every registered connection so Close can tear them down outside the
// lock. Closing inline would self-deadlock: connection.close re-enters the hub through unregister,
// which takes the write lock this snapshot is holding.
//
// Returns:
//   - result: Every connection currently in the per-user index.
func (h *Hub[U]) snapshotAllConnections() []*connection[U] {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*connection[U], 0, len(h.connsByUser))
	for _, set := range h.connsByUser {
		for c := range set {
			out = append(out, c)
		}
	}
	return out
}
