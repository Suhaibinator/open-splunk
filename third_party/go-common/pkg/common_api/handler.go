// Package common_api provides a comprehensive HTTP API layer for the go-common library.
// It implements RESTful endpoints for authentication, user management, file handling,
// and other common backend services using the SRouter framework.
//
// The package features:
//   - Email and OAuth authentication endpoints
//   - User profile and settings management
//   - File upload with direct-to-storage capabilities
//   - Contact form handling
//   - Frontend telemetry collection (Grafana Faro)
//   - Request logging and metrics
//   - Automatic input sanitization
//   - Consistent error handling
//
// The API layer is designed to be generic and reusable across different applications
// by using type parameters for user models.
//
// Example usage:
//
//	handler, err := common_api.NewCommonAPIHandler(
//		provider.API(),
//		router,
//		serviceHandler,
//		logger,
//		metricsHandler,
//		sanitizerHandler,
//	)
//	if err != nil {
//		// handle error
//	}
//	// Register all common API routes on the injected router.
//	handler.RegisterAllRoutes()
//	if err := mainRouter.Build(); err != nil {
//		// handle invalid or conflicting route definitions
//	}
package common_api

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_api/middleware"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_sanitizer"
	"github.com/Suhaibinator/go-common/pkg/common_service"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models" // Import the adapter
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_ws"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const commonAPILoggerName = "common_api"

// CommonAPIHandler CommonAPIHandler defines data/state used by this package and is consumed as part of the backend request and service integration flow.
type CommonAPIHandler[U common_service_models.UserLike] struct {
	provider config.APIProvider
	// startup pins the immutable generation used to install routes and cookie identity.
	startup config.APIView
	// serviceHandler manages business logic and service operations
	serviceHandler *common_service.CommonServiceHandler[U]
	// logger is used for structured logging throughout the API layer
	logger *zap.Logger
	// metricsHandler collects and exposes Prometheus metrics
	metricsHandler common_metrics.MetricsHandlerInterface
	// router is the SRouter instance for registering HTTP endpoints
	router *router.Router[common_service_models.UserIdType, U]
	// sanitizer validates and sanitizes all incoming requests
	sanitizer *common_sanitizer.Handler

	// realtimeHub provides the optional websocket connection registry for GET /realtime/ws.
	// If nil, the handler returns 501 (Not Implemented) for the websocket endpoint.
	realtimeHub *common_ws.Hub[U]
	// realtimeUpgrader allows the consuming application to customize the websocket upgrade behavior
	// (e.g. origin policy, compression). SetRealtimeUpgrader guarantees the installed upgrader
	// advertises common_ws.Subprotocol, without which browser clients fail the handshake.
	//
	// It is written by SetRealtimeUpgrader before serving and read without synchronization by every
	// request goroutine, which is why installation is a startup-time operation.
	//
	// Construction leaves it nil on purpose: realtime_ws_allowed_origins is hot, so the built-in
	// upgrader is rebuilt for every handshake from the same view the accepted connection is
	// configured from. Only an application-supplied upgrader is a startup-time fixture, and it owns
	// the origin policy from then on.
	realtimeUpgrader *websocket.Upgrader

	// realtimePublisher is the construction-time publisher selected for service events.
	realtimePublisher           ports.RealtimePublisher
	realtimePublisherFactory    func(ports.RealtimePublisher) ports.RealtimePublisher
	notificationActivityBackend common_ws.NotificationActivityBackend
}

// HandlerOption customizes optional runtime integrations while preserving the
// startup-only behavior of the existing constructors.
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	realtimePublisherFactory    func(ports.RealtimePublisher) ports.RealtimePublisher
	notificationActivityBackend common_ws.NotificationActivityBackend
}

// WithNotificationActivityBackend shares active notification scopes between
// instances. The backend receives only server-generated connection identities.
func WithNotificationActivityBackend(backend common_ws.NotificationActivityBackend) HandlerOption {
	return func(options *handlerOptions) { options.notificationActivityBackend = backend }
}

// WithRealtimePublisherFactory selects a publisher at construction while giving
// the factory the local hub publisher for single-instance fallback/composition.
// Chat-enabled deployments must return ChatAudiencePublisher too, preserving
// connection-scoped viewer delivery and revocation fencing.
func WithRealtimePublisherFactory(factory func(ports.RealtimePublisher) ports.RealtimePublisher) HandlerOption {
	return func(options *handlerOptions) { options.realtimePublisherFactory = factory }
}

// NewCommonAPIHandler constructs the common API handler from the typed
// configuration provider. Startup configuration is captured from one
// generation sampled at construction; the WebSocket origin and connection
// policy stays hot by sampling cfg per handshake.
//
// Returns:
// - result1 (*CommonAPIHandler[U]): contains the operation result that is returned to the caller/HTTP response encoder.
// - result2 (error): is non-nil when the operation fails and should be propagated to the caller; an
// invalid realtime transport configuration fails construction here rather than degrading the
// endpoint at runtime.
func NewCommonAPIHandler[U common_service_models.UserLike](
	cfg config.APIProvider,
	srouter *router.Router[common_service_models.UserIdType, U],
	serviceHandler *common_service.CommonServiceHandler[U],
	logger *zap.Logger,
	metricsHandler common_metrics.MetricsHandlerInterface,
	sanitizerHandler *common_sanitizer.Handler,
	options ...HandlerOption,
) (*CommonAPIHandler[U], error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	view := cfg()
	// A zero RootView is the only nil-shaped value the config package hands out.
	if view == nil || view == config.APIView(config.RootView{}) {
		return nil, fmt.Errorf("configuration view is required")
	}
	hubCfg := common_ws.NewHubConfigFromView(view)
	if srouter == nil {
		return nil, fmt.Errorf("router is required")
	}

	var handlerOptions handlerOptions
	for _, option := range options {
		if option != nil {
			option(&handlerOptions)
		}
	}

	handler := &CommonAPIHandler[U]{
		provider:                    cfg,
		startup:                     view,
		serviceHandler:              serviceHandler,
		logger:                      logger,
		metricsHandler:              metricsHandler,
		router:                      srouter,
		sanitizer:                   sanitizerHandler,
		realtimeHub:                 nil,
		realtimeUpgrader:            nil,
		realtimePublisher:           nil,
		realtimePublisherFactory:    handlerOptions.realtimePublisherFactory,
		notificationActivityBackend: handlerOptions.notificationActivityBackend,
	}

	if err := handler.enableBuiltInRealtime(hubCfg); err != nil {
		return nil, err
	}

	if serviceHandler != nil {
		serviceHandler.Initialize()
	}
	if err := handler.wireNotificationsIntoRealtime(); err != nil {
		return nil, err
	}

	return handler, nil
}

// enableBuiltInRealtime enableBuiltInRealtime supports the backend request flow by performing this package's local processing step.
//
// It constructs the realtime transport and chat wiring before startup.
// Current configuration gates handshakes and commands. The transport is deliberately
// independent of chat: an application with no chat still gets the multiplexed socket for its own
// topics.
//
// Parameters:
// - hubCfg (common_ws.HubConfig): provides the immutable WebSocket startup settings; the hot half is
// re-sampled from the realtime view for every handshake.
//
// Returns:
// - result1 (error): is non-nil when the transport configuration is rejected, which fails handler
// construction outright.
func (h *CommonAPIHandler[U]) enableBuiltInRealtime(hubCfg common_ws.HubConfig) error {

	hub, err := common_ws.NewHubWithRealtimeViewProvider[U](hubCfg, func() common_ws.RealtimeView { return h.provider() }, h.logger, h.metricsHandler)
	if err != nil {
		// The hub only rejects a command auth budget that would outlive the keepalive window,
		// which is a startup-time operator mistake in realtime_ws_ping_interval /
		// realtime_ws_pong_timeout. Failing construction surfaces it at deploy time instead of
		// leaving an endpoint that answers 501 for the life of the process.
		return fmt.Errorf("realtime websocket transport configuration rejected: %w", err)
	}
	h.realtimeHub = hub
	if h.serviceHandler != nil {
		h.serviceHandler.SetRealtimeSessionCloser(h.realtimeHub)
	}

	local := common_ws.NewHubRealtimePublisher(h.realtimeHub, h.logger)
	h.realtimePublisher = local
	if h.realtimePublisherFactory != nil {
		h.realtimePublisher = h.realtimePublisherFactory(local)
		if h.realtimePublisher == nil {
			return fmt.Errorf("realtime publisher factory returned nil")
		}
		if _, ok := h.realtimePublisher.(common_ws.ChatAudiencePublisher); !ok {
			return fmt.Errorf("realtime publisher must implement ChatAudiencePublisher when chat may be enabled")
		}
	}

	if h.serviceHandler == nil {
		return nil
	}
	h.wireChatIntoRealtime()
	return nil
}

// wireChatIntoRealtime wireChatIntoRealtime supports the backend request flow by performing this package's local processing step.
//
// Chat is the first consumer of the realtime transport, not its definition: everything here is
// gated at operation boundaries, and anything the application installed itself is left alone.
func (h *CommonAPIHandler[U]) wireChatIntoRealtime() {
	log := h.logger.Named(commonAPILoggerName)
	metrics, _ := h.metricsHandler.(interface{ RecordChatRecipientCache(string) })
	cache := common_ws.NewChatRecipientCache(h.serviceHandler.GetPersistenceHandler(), metrics)
	h.serviceHandler.SetChatRealtimeAccessInvalidator(&chatRealtimeInvalidator[U]{cache: cache, hub: h.realtimeHub})
	if err := h.realtimeHub.RegisterChatReadAuthorizer(func(ctx context.Context, user U, channelID uint64) (ports.ChatRealtimeCategory, error) {
		if !h.provider().EnableChat() {
			return 0, common_ws.ErrReservedTopic
		}
		return h.serviceHandler.AuthorizeChatRealtimeRead(ctx, user, channelID)
	}); err != nil {
		log.Error("Failed to register chat read subscriptions", zap.Error(err))
	}

	// The setter also fans the publisher into the bot dispatcher, so installation goes through it
	// rather than assigning the field directly.
	if h.serviceHandler.GetChatEventPublisher() == nil {
		h.serviceHandler.SetChatEventPublisher(common_ws.NewChatRealtimeEventPublisher(
			h.realtimePublisher,
			h.serviceHandler.GetConverter(),
			common_ws.ChatRealtimeEventPublisherOptions{
				Persistence: cache,
				Logger:      h.logger,
			},
		))
	}

	// Typing and durable/stream events share the same invalidated participant cache.
	typingBroadcaster := common_ws.NewTypingBroadcaster(
		h.realtimePublisher,
		cache,
		h.logger,
		func() time.Duration {
			return h.provider().ChatTypingRefreshWindow()
		},
	)

	if err := h.realtimeHub.RegisterRealtimeCommandHandler(
		common_ws.ChatTypingCommandType,
		h.liveChatCommand(common_ws.NewTypingCommandHandler(typingBroadcaster)),
	); err != nil {
		log.Warn("Failed to register the chat typing realtime command handler",
			zap.String(logkeys.Operation, "register_realtime_command_handler"),
			zap.String(appLogger.KeyCommandType, common_ws.ChatTypingCommandType),
			zap.String(logkeys.Fallback, "chat_typing_command_unavailable"),
			zap.Error(err))
	}

	if h.serviceHandler.GetAgentTypingEmitter() == nil {
		h.serviceHandler.SetAgentTypingEmitter(common_ws.NewChatTypingEmitter(typingBroadcaster))
	}
}

// SetRealtimeUpgrader SetRealtimeUpgrader supports the backend request flow by performing this package's local processing step.
//
// It installs an application-supplied websocket upgrader for GET /realtime/ws, replacing the
// built-in one. Three things follow from that, all of them load-bearing:
//
//   - The upgrader is mutated: common_ws.Subprotocol is appended to its Subprotocols when absent.
//     gorilla only echoes a subprotocol it was told about and a browser fails any connection whose
//     requested subprotocol is not confirmed, so an upgrader without it would break every browser
//     client while passing every Go test. Adding it is safer than documenting it and is done in
//     place, on the caller's value.
//   - Call it before serving begins. The field is written here without synchronization and read by
//     every request goroutine, so installing an upgrader once at startup is safe and swapping one
//     while requests are in flight is a data race.
//   - Supplying an upgrader opts the application out of hot origin policy. The built-in upgrader is
//     rebuilt per handshake from the current realtime view, so realtime_ws_allowed_origins reloads;
//     a supplied upgrader is a startup fixture and its CheckOrigin becomes the whole origin policy
//     from then on, reload or not.
//
// Passing nil restores the built-in per-handshake upgrader.
//
// Parameters:
// - upgrader (*websocket.Upgrader): the upgrader to install; nil restores the built-in one.
func (h *CommonAPIHandler[U]) SetRealtimeUpgrader(upgrader *websocket.Upgrader) {
	if upgrader != nil && !slices.Contains(upgrader.Subprotocols, common_ws.Subprotocol) {
		upgrader.Subprotocols = append(upgrader.Subprotocols, common_ws.Subprotocol)
	}
	h.realtimeUpgrader = upgrader
}

// RegisterTopicAuthorizer RegisterTopicAuthorizer registers the application authorizer consulted when a client subscribes to a topic in the given namespace.
//
// Registration is construction-time wiring: it is rejected once the hub has served its first
// connection. Namespaces go-common reserves (user, realtime, chat) cannot be claimed.
//
// Parameters:
// - namespace (string): segment prefix the authorizer covers, e.g. "grading" or "grading.submission".
// - authorizer (common_ws.TopicAuthorizer[U]): authorizer consulted for topics under that prefix.
func (h *CommonAPIHandler[U]) RegisterTopicAuthorizer(namespace string, authorizer common_ws.TopicAuthorizer[U]) error {
	if h == nil || h.realtimeHub == nil {
		return ErrRealtimeNotEnabled
	}
	return h.realtimeHub.RegisterTopicAuthorizer(namespace, authorizer)
}

// RegisterRealtimeCommandHandler RegisterRealtimeCommandHandler registers an application handler for a client-to-server realtime command type.
//
// Registration is construction-time wiring: it is rejected once the hub has served its first
// connection. The hub-owned subscribe and unsubscribe types cannot be overridden.
//
// Parameters:
// - commandType (string): the RealtimeCommand type string the handler serves.
// - handler (common_ws.CommandHandler): handler invoked for that type.
func (h *CommonAPIHandler[U]) RegisterRealtimeCommandHandler(commandType string, handler common_ws.CommandHandler) error {
	if h == nil || h.realtimeHub == nil {
		return ErrRealtimeNotEnabled
	}
	return h.realtimeHub.RegisterRealtimeCommandHandler(commandType, handler)
}

// RealtimePublisher RealtimePublisher exposes the hub-backed publisher applications publish their own events through.
//
// Returns:
// - result1 (ports.RealtimePublisher): the publisher, or nil when the realtime transport is disabled.
func (h *CommonAPIHandler[U]) RealtimePublisher() ports.RealtimePublisher {
	if h == nil || h.realtimePublisher == nil {
		return nil
	}
	return h.realtimePublisher
}

// GetTransactionMiddleware returns THE default transaction middleware for unary
// request/response routes. The returned middleware buffers the bounded HTTP response
// and flushes it to the client only after the transaction has durably committed, so
// a commit failure cannot leak a success response. Registered post-commit hooks run
// after commit with a bounded budget (PostCommitHookTimeout / PostCommitHookFlushWait
// from config), so a slow or hung hook cannot withhold the committed response.
// TransactionResponseBufferLimitBytes (4 MiB by default) bounds the buffered body;
// exceeding it rolls back the transaction and returns a generic 500. This middleware
// must not be used on streaming/SSE/WebSocket or intentionally large/export routes; see
// middleware.TransactionMiddleware for the full contract and
// GetStreamingTransactionMiddleware for the streaming variant.
func (h *CommonAPIHandler[U]) GetTransactionMiddleware() common.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middleware.TransactionMiddleware[U](h.serviceHandler.GetPersistenceHandler(), h.logger, h.postCommitHookOptions())(next).ServeHTTP(w, r)
		})
	}
}

// postCommitHookOptions translates the configured hook and response-buffer bounds
// into middleware options shared by both transaction middleware variants. The
// streaming variant ignores the response-buffer fields.
func (h *CommonAPIHandler[U]) postCommitHookOptions() middleware.PostCommitHookOptions {
	v := h.provider()
	return middleware.PostCommitHookOptions{
		HookTimeout:              v.PostCommitHookTimeout(),
		FlushWait:                v.PostCommitHookFlushWait(),
		MaxBufferedResponseBytes: v.TransactionResponseBufferLimitBytes(),
	}
}

// GetStreamingTransactionMiddleware returns the non-buffering transaction middleware
// with post-commit hooks, for streaming/SSE/WebSocket and intentionally large/export
// routes where response buffering is impossible or inappropriate. Unlike
// GetTransactionMiddleware it lets the handler write
// directly to the client, so a commit failure after the response has streamed can
// leak a success status; see middleware.StreamingTransactionMiddleware for the full
// contract. Every bounded unary route should use GetTransactionMiddleware instead.
func (h *CommonAPIHandler[U]) GetStreamingTransactionMiddleware() common.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middleware.StreamingTransactionMiddleware[U](h.serviceHandler.GetPersistenceHandler(), h.logger, h.postCommitHookOptions())(next).ServeHTTP(w, r)
		})
	}
}

// RegisterAllRoutes registers the enabled common API modules directly on the
// Router supplied to NewCommonAPIHandler. Applications should call it during
// startup before adding any remaining application routes and calling Router.Build.
func (h *CommonAPIHandler[U]) RegisterAllRoutes() {
	log := h.logger.Named(commonAPILoggerName)
	log.Debug("Registering common API route group")

	commonAPIGroup := h.router.Group(h.startup.PathPrefix()).AuthToken(&common.AuthTokenConfig{
		Source:     common.AuthTokenSourceCookie,
		CookieName: h.startup.SessionCookieName(),
	})

	// Sliding-session refresh: keep active users signed in by re-issuing the
	// session cookie as it nears expiry. Attached unconditionally — the
	// middleware no-ops when session refresh is disabled, skips OPTIONS and
	// auth routes, and re-wrapping the auth routes' per-route cookie writer is
	// idempotent.
	commonAPIGroup.Use(h.GetSessionRefreshMiddleware())

	{
		pathPrefix := strings.TrimRight(h.startup.PathPrefix(), "/")
		joinPath := func(path string) string {
			if pathPrefix == "" {
				return path
			}
			return pathPrefix + path
		}
		// Pre-compute paths once at initialization
		getUserMe := joinPath(pathUserMe)
		putUserUsername := joinPath(pathUserUsername)
		postAuthCheckUsername := joinPath(pathAuthCheckUsername)
		postAuthLogout := joinPath(pathAuthLogout)

		// Switch on method provides a compiler jump table, direct string comparison avoids map hashing
		isAllowedPath := func(method, path string) bool {
			switch method {
			case http.MethodGet:
				return path == getUserMe
			case http.MethodPut:
				return path == putUserUsername
			case http.MethodPost:
				return path == postAuthCheckUsername || path == postAuthLogout
			}
			return false
		}
		commonAPIGroup.Use(
			func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					v := h.provider()
					enabled, prefix := v.EnableUserSettings(), v.OAuthDefaultUsernamePrefix()
					if !enabled {
						next.ServeHTTP(w, r)
						return
					}
					middleware.UsernamePrefixEnforcementMiddleware[U](prefix, isAllowedPath, h.logger)(next).ServeHTTP(w, r)
				})
			},
		)
	}

	h.RegisterHealthRoutes(commonAPIGroup)
	log.Debug("Health route registered to common API group")

	// Install module routes once; request middleware applies the current feature policy.
	h.RegisterAttachmentRoutes(commonAPIGroup)
	log.Debug("Attachment routes registered")

	// User routes - always enabled for now (basic user info)
	h.RegisterUserRoutes(commonAPIGroup)
	log.Debug("User routes registered to common API group")

	// Profile picture routes - enabled when user profiles are enabled
	h.RegisterProfilePictureRoutes(commonAPIGroup)
	log.Debug("Profile picture routes registered")

	h.RegisterUserSettingsRoutes(commonAPIGroup)
	log.Debug("User settings routes registered")

	h.RegisterAuthRoutes(commonAPIGroup)
	log.Debug("Authentication routes registered")

	h.RegisterContactRoutes(commonAPIGroup)
	log.Debug("Contact routes registered")

	h.RegisterCareersRoutes(commonAPIGroup)
	log.Debug("Careers routes registered")

	h.RegisterFaroRoutes(commonAPIGroup)
	log.Debug("Faro telemetry routes registered")

	h.RegisterAdminRoutes(commonAPIGroup)
	h.RegisterInferenceRoutingRoutes(commonAPIGroup)
	log.Debug("Admin API routes registered")

	h.RegisterOrganizationRoutes(commonAPIGroup)
	log.Debug("Organization routes registered")

	h.RegisterChatRoutes(commonAPIGroup)
	log.Debug("Chat routes registered")
	h.RegisterNotificationRoutes(commonAPIGroup)

	// Registered independently of chat: the realtime transport carries application topics too, so a
	// chat-less application still gets the socket.
	h.RegisterRealtimeRoutes(commonAPIGroup)
	log.Debug("Realtime websocket route registered")

	v := h.provider()
	log.Info("Common API route group registered",
		zap.String(logkeys.Path, h.startup.PathPrefix()),
		zap.Bool(appLogger.KeyAttachmentsEnabled, v.EnableAttachments()),
		zap.Bool(appLogger.KeyUserProfilesEnabled, v.EnableUserProfiles()),
		zap.Bool(appLogger.KeyUserSettingsEnabled, v.EnableUserSettings()),
		zap.Bool(appLogger.KeyEmailAuthEnabled, v.EnableEmailAuth()),
		zap.Bool(appLogger.KeyPhoneAuthEnabled, v.SMSEnabled()),
		zap.Bool(appLogger.KeyOAuthEnabled, v.EnableOAuth()),
		zap.Bool(appLogger.KeyContactsEnabled, v.EnableContacts()),
		zap.Bool(appLogger.KeyCareersEnabled, v.EnableCareers()),
		zap.Bool(appLogger.KeyFaroEnabled, v.EnableFaro()),
		zap.Bool(appLogger.KeyAdminAPIEnabled, v.EnableAdminAPI()),
		zap.Bool(appLogger.KeyOrganizationsEnabled, v.EnableOrganizations()),
		zap.Bool(appLogger.KeyChatEnabled, v.EnableChat()),
		zap.Bool(appLogger.KeyRealtimeWebSocketEnabled, v.EnableRealtimeWebSocket()))
}

// Stop Stop handles a backend operation in the API/request flow and delegates business logic to the service layer.
//
// It also stops the service handler's background workers, since constructing
// this API handler is what starts them. An application builds exactly one API
// handler per service handler.
func (h *CommonAPIHandler[U]) Stop() {
	if h == nil {
		return
	}

	h.CloseRealtimeHub(realtimeHubCloseTimeout)

	if h.serviceHandler != nil {
		h.serviceHandler.Stop()
	}
}

// liveChatCommand applies the feature gate to existing websocket connections,
// whose commands bypass the HTTP route middleware after the handshake.
func (h *CommonAPIHandler[U]) liveChatCommand(next common_ws.CommandHandler) common_ws.CommandHandler {
	return common_ws.CommandHandlerFunc(func(ctx context.Context, userID common_service_models.UserIdType, requestID string, payload []byte) error {
		enabled := h.provider().EnableChat()
		if !enabled {
			return common_ws.ErrUnsupportedCommand
		}
		return next.HandleRealtimeCommand(ctx, userID, requestID, payload)
	})
}
