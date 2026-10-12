package common_service

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/admin"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth/email"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth/phone"
	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/email_client"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_service/sms_client"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// Initialization Methods
// These methods handle the initialization of all sub-handlers and components

type handlerOptions[U common_service_models.UserLike] struct {
	notificationTypes                []notification.Type
	notificationProducers            []notification.Producer
	notificationStore                persistence.NotificationStore
	notificationPolicyProvider       notification.PolicyProvider
	notificationWorkerCount          *int
	notificationFanoutWorkersEnabled *bool
	organizationCallbacks            *ports.OrganizationCallbackConfig[U]
	organizationOptions              []organization.HandlerOption
	chatPermissionCallbacks          *ports.ChatPermissionCallbackConfig[U]
	notificationApprovers            func(context.Context, uint64) ([]uint64, error)
	notificationApproverPage         func(context.Context, uint64, uint64, int) ([]uint64, error)
	notificationLinkBuilder          func(context.Context, uint64, uint32, *proto_models.ChatNotificationPayload) (string, error)
	agentGeneratorFactory            AgentGeneratorFactory
	agentRunObserver                 AgentRunObserver
	inferenceControlPlaneStore       admin.InferenceControlPlaneStore
	inferenceControlPlaneRegistry    *controlplane.Registry
	converter                        common_converter.ConverterInterface
	oauthSource                      oauth.Source
}

// HandlerOption customizes CommonServiceHandler construction.
type HandlerOption[U common_service_models.UserLike] func(*handlerOptions[U])

// WithOrganizationCallbacks installs lifecycle hooks for organization events.
func WithOrganizationCallbacks[U common_service_models.UserLike](callbacks *ports.OrganizationCallbackConfig[U]) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.organizationCallbacks = callbacks
	}
}

// WithOrganizationOptions installs construction-time membership and creation policies.
func WithOrganizationOptions[U common_service_models.UserLike](options ...organization.HandlerOption) HandlerOption[U] {
	return func(settings *handlerOptions[U]) {
		settings.organizationOptions = append(settings.organizationOptions, options...)
	}
}

// WithChatPermissionCallbacks installs the application's chat permission hooks.
func WithChatPermissionCallbacks[U common_service_models.UserLike](callbacks *ports.ChatPermissionCallbackConfig[U]) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.chatPermissionCallbacks = callbacks
	}
}

// WithAgentGeneratorFactory replaces the default generator factory for agent runs (for example
// with a fake generator in tests). A factory installed this way takes precedence over the default
// one SetAgentModelRuntimeFactory would install.
func WithAgentGeneratorFactory[U common_service_models.UserLike](factory AgentGeneratorFactory) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.agentGeneratorFactory = factory
	}
}

// WithAgentRunObserver installs the outbound run-lifecycle observer. The replica that wins a
// reported durable transition (RETRYABLE or a terminal state) invokes it after commit; RUNNING and
// WAITING_FOR_APPROVAL are not reported. Callbacks must be idempotent because process failure can
// still occur in the narrow post-commit delivery window.
func WithAgentRunObserver[U common_service_models.UserLike](observer AgentRunObserver) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.agentRunObserver = observer
	}
}

// WithInferenceControlPlane wires the AI control-plane store and code-owned operation registry into
// the admin endpoints. Without it those endpoints answer 503.
func WithInferenceControlPlane[U common_service_models.UserLike](store admin.InferenceControlPlaneStore, registry *controlplane.Registry) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.inferenceControlPlaneStore = store
		options.inferenceControlPlaneRegistry = registry
	}
}

// WithOAuthProviders installs the application's OAuth provider source. The
// constructor samples it once and fails on a source error or an invalid
// provider; afterwards every OAuth operation samples it once, leaving out (and
// logging) providers invalid in that sample. Without it no provider is
// configured.
func WithOAuthProviders[U common_service_models.UserLike](source oauth.Source) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		options.oauthSource = source
	}
}

// WithConverter replaces the converter the constructor would otherwise build
// from its configuration provider. A nil converter is ignored.
func WithConverter[U common_service_models.UserLike](converter common_converter.ConverterInterface) HandlerOption[U] {
	return func(options *handlerOptions[U]) {
		if converter != nil {
			options.converter = converter
		}
	}
}

// NewCommonServiceHandler constructs the service graph from the typed
// configuration provider. Restart-bound settings are captured from one
// generation sampled at construction; every hot boundary (rate limits, user
// search, session refresh, unverified-user cleanup, signup policy, permissions,
// auth, SMS, chat, bot, organization, and attachment runtime policy) keeps
// sampling cfg per operation; there are no per-leaf provider overrides.
// inferenceModelCatalog is an explicit construction-time
// choice: pass the application's runtime-backed catalog, or nil to disable the
// catalog-backed model administration, provider listings, and selectable-model
// queries. This does not disable separately wired control-plane APIs.
func NewCommonServiceHandler[U common_service_models.UserLike](
	cfg config.ServiceProvider,
	commonPersistenceHandler persistence.CommonPersistenceInterface,
	userCallbacks *ports.UserCallbackConfig[U],
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	inferenceModelCatalog *genai.Catalog,
	options ...HandlerOption[U],
) (*CommonServiceHandler[U], error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	if err := userCallbacks.Validate(); err != nil {
		return nil, fmt.Errorf("invalid user callbacks: %w", err)
	}
	settings := handlerOptions[U]{}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}

	view := cfg()
	if view == nil || view == config.ServiceView(config.RootView{}) {
		return nil, fmt.Errorf("configuration view is required")
	}

	converter := settings.converter
	if converter == nil {
		converter = common_converter.NewConverterHandler[U](func() config.ConverterView { return cfg() }, logger)
	}

	handler := &CommonServiceHandler[U]{
		runtimeConfig:            cfg,
		commonPersistenceHandler: commonPersistenceHandler,
		userCallbacks:            userCallbacks,
		organizationCallbacks:    settings.organizationCallbacks,
		chatPermissionCallbacks:  settings.chatPermissionCallbacks,
		notificationApprovers:    settings.notificationApprovers,
		notificationApproverPage: settings.notificationApproverPage,
		notificationLinkBuilder:  settings.notificationLinkBuilder,
		metricsHandler:           metricsHandler,
		converter:                converter,
		logger:                   logger,
		sessionRefreshRand:       rand.Float64,
	}
	constructionComplete := false
	defer func() {
		if !constructionComplete {
			handler.Stop()
		}
	}()

	// Initialize permission handler first since other components may depend on it
	if err := handler.initializePermissionHandler(logger, view, cfg); err != nil {
		return nil, fmt.Errorf("failed to initialize permission handler: %w", err)
	}

	// Initialize all sub-components
	handler.initializeUserSettings()
	handler.initializeEmailClient(func() config.EmailView { return cfg() }, logger)
	if err := handler.initializeRateLimiter(view, func() config.RateLimitsView { return cfg() }, logger); err != nil {
		return nil, fmt.Errorf("failed to initialize rate limiter: %w", err)
	}
	tokenService, err := handler.initializeTokenService(cfg, view, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize token service: %w", err)
	}
	// Organization and personal-workspace lifecycle services must exist before
	// auth/admin handlers capture the internally coordinated user callbacks.
	if err := handler.initializeOrganizationHandler(logger, cfg, settings.organizationOptions...); err != nil {
		return nil, fmt.Errorf("failed to initialize organization handler: %w", err)
	}
	if err := handler.initializePersonalWorkspaceLifecycle(view); err != nil {
		return nil, fmt.Errorf("failed to initialize personal workspace lifecycle: %w", err)
	}
	if err := handler.initializeAuthHandlers(cfg, view, logger, handler.GetEmailClient(), tokenService, settings.oauthSource); err != nil {
		return nil, fmt.Errorf("failed to initialize auth handlers: %w", err)
	}
	handler.configureCacheInvalidation()
	handler.initializeChatHandler(logger, cfg)
	if err := handler.initializeBotDispatcher(logger, cfg); err != nil {
		return nil, fmt.Errorf("failed to initialize bot dispatcher: %w", err)
	}
	if err := handler.initializeAttachmentRegistry(); err != nil {
		return nil, fmt.Errorf("failed to initialize attachment registry: %w", err)
	}
	if err := handler.initializeAttachmentLifecycleManager(logger, func() attachmentpkg.RuntimeConfigView { return cfg() }); err != nil {
		return nil, fmt.Errorf("failed to initialize attachment lifecycle manager: %w", err)
	}
	if err := handler.configureProfilePictureCallback(); err != nil {
		return nil, fmt.Errorf("failed to configure profile picture callback: %w", err)
	}
	if err := handler.configureChatAttachmentCallback(); err != nil {
		return nil, fmt.Errorf("failed to configure chat attachment callback: %w", err)
	}
	if err := handler.configureCareerResumeAttachmentType(); err != nil {
		return nil, fmt.Errorf("failed to configure career resume attachment type: %w", err)
	}
	if err := handler.initializeAdminHandler(logger, inferenceModelCatalog); err != nil {
		return nil, fmt.Errorf("failed to initialize admin handler: %w", err)
	}
	if settings.inferenceControlPlaneStore != nil || settings.inferenceControlPlaneRegistry != nil {
		handler.adminHandler.SetInferenceControlPlane(settings.inferenceControlPlaneStore, settings.inferenceControlPlaneRegistry)
	}
	if settings.agentGeneratorFactory != nil {
		handler.setAgentGeneratorFactory(settings.agentGeneratorFactory)
		handler.agentRuntime.customGeneratorFactory = true
	}
	handler.setAgentRunObserver(settings.agentRunObserver)

	if err := handler.initializeNotifications(settings); err != nil {
		return nil, fmt.Errorf("initialize notifications: %w", err)
	}

	constructionComplete = true
	return handler, nil
}

// initializeChatHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `cfg`: supplies the hot runtime configuration provider.
func (h *CommonServiceHandler[U]) initializeChatHandler(logger *zap.Logger, cfg config.ServiceProvider) {
	chatHandler := chatpkg.NewHandler[U](h.commonPersistenceHandler, h.converter, func() chatpkg.PolicyView { return cfg() }, logger)
	if h.chatEventPublisher != nil {
		chatHandler.SetEventPublisher(h.chatEventPublisher)
	}
	chatHandler.SetPermissionCallbacks(h.chatPermissionCallbacks)
	// The permission handler is built before the chat handler (initializePermissionHandler runs
	// first), so it is available here to authorize site-admin tool-call resolution (Decision 6).
	if h.permissionHandler != nil {
		chatHandler.SetPermissionHandler(h.permissionHandler)
	}
	// initializePermissionHandler also builds the shared organization role provider, so channels
	// that own org-derived access are evaluated against its cached, lineage-aware effective roles.
	// Without a provider the resolver stays unset and no org-derived access is granted.
	if resolver := newOrgRoleProviderResolver(h.orgRoleProvider); resolver != nil {
		chatHandler.SetOrganizationRoleResolver(resolver)
	}
	chatHandler.SetAttachmentHydrator(h)
	chatHandler.SetAttachmentCoordinator(h)
	// The facade reads gates from the inference policy handler at call time, so routing configured
	// after construction still reaches session summaries.
	chatHandler.SetInferenceSessionRetargetGates(h)
	// Persistence mints delegation wake and drained follow-up runs on its own and has no view of
	// the chat configuration group; the provider samples the live config per minted run so a hot
	// budget change reaches those runs like service-minted ones.
	if h.commonPersistenceHandler != nil {
		h.commonPersistenceHandler.SetChatAgentRunBudgetCeilingsProvider(chatHandler.AgentRunBudgetCeilings)
		h.commonPersistenceHandler.SetChatAgentChildWakeLimitsProvider(chatHandler.ChildWakeLimits)
		h.commonPersistenceHandler.SetChatAgentGraphEventNotifier(h.notifyChatAgentGraphEvent)
		h.commonPersistenceHandler.SetChatAgentWorkflowTypesProvider(chatHandler.WorkflowAgentTypes)
	}
	h.chatHandler = chatHandler
}

// initializeBotDispatcher executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `cfg`: supplies the hot runtime configuration provider.
func (h *CommonServiceHandler[U]) initializeBotDispatcher(logger *zap.Logger, cfg config.ServiceProvider) error {
	dispatcher := botpkg.NewDispatcher[U](func() botpkg.RuntimeConfigView { return cfg() }, h.commonPersistenceHandler, logger)

	// The active-children bounds are restart-bound: one generation, captured here, serves every
	// registration, so a registrar's plan and apply phases check the same limits.
	h.agentRegistry = botpkg.NewAgentRegistry(cfg())
	h.agentToolRegistry = botpkg.NewAgentToolRegistry()
	// The delegation tools are library-owned: their executors need this handler's chat service and
	// persistence, and an application only lists the names on its agents (the agentkit registrar
	// does that from a DelegationSpec). Registering them here, before any application registrar
	// runs, is what lets the registrar's plan phase find them live.
	for _, def := range botpkg.NewDelegationToolDefinitions(h.delegationToolExecutors()) {
		if err := h.agentToolRegistry.RegisterTool(def); err != nil {
			return fmt.Errorf("register delegation tool %s: %w", def.Name, err)
		}
	}
	h.botDispatcher = dispatcher
	dispatcher.SetTypingEmitter(h.agentTypingEmitter)
	dispatcher.SetChatEventPublisher(h.chatEventPublisher)
	dispatcher.SetConverter(h.converter)
	h.wireInternalAttachmentStore()

	if h.chatHandler != nil {
		h.chatHandler.SetAgentRegistries(h.agentRegistry, h.agentToolRegistry)
		// Inject the pure tool executor (runtime lane, #8) that the approval lane's durable
		// approved-work scanner (#7) calls to run claimed tool calls. The scanner is a no-op until
		// this is set, so wiring it here activates approved-work execution.
		h.chatHandler.SetToolCallExecutor(h.newToolCallExecutor())
		// Wire the agent runtime as the fresh-turn starter + approval-resume driver. The chat handler
		// calls it after creating a ChatAgentRun (initial user message) and after a resume nudge; the
		// runtime drives the transcript-native Genkit continuation loop. The generator factory is set
		// separately via SetAgentModelRuntimeFactory; until then the runtime is inert.
		h.chatHandler.SetAgentRunDriver(func(ctx context.Context, runID uint64) {
			if err := h.RunAgentTurn(ctx, runID); err != nil {
				appLogger.LogOperationWarn(appLogger.For(ctx, logger, agentRuntimeLoggerName), "Agent runtime turn failed", "run_agent_turn", err,
					zap.Uint64(appLogger.KeyAgentRunID, runID))
			}
		})
	}

	// Bind the runtime's seams now that the dispatcher + registries exist. Any generator factory
	// already injected via WithAgentGeneratorFactory/SetAgentModelRuntimeFactory is preserved.
	h.initializeAgentRuntime(logger)
	if h.chatHandler != nil {
		h.chatHandler.SetAgentRunObserver(h.agentRuntime.runObserver)
	}

	return nil
}

// Initialize executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) Initialize() {
	if h == nil || h.runtimeConfig == nil {
		return
	}
	if h.notificationHandler != nil {
		h.notificationHandler.Start()
	}
	// Keep pollers available so their live gates can enable work after startup.
	if h.attachmentWorkerCtx == nil {
		h.StartAttachmentWorkers()
	}
	if h.unverifiedUserCleanupCtx == nil {
		h.startUnverifiedUserCleanup()
	}
	// Start the agentic tool-approval lane background tasks (expiry sweep + approved-work backstop).
	// Gated internally by their configured intervals (<=0 disables each).
	if h.chatHandler != nil {
		h.chatHandler.StartApprovalBackgroundTasks()
	}
	if h.agentRuntime != nil {
		h.agentRuntime.startRecovery()
	}
}

// initializePermissionHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `view`: supplies the coherent startup configuration generation.
// - `cfg`: supplies the hot runtime configuration provider.
func (h *CommonServiceHandler[U]) initializePermissionHandler(logger *zap.Logger, view config.ServiceView, cfg config.ServiceProvider) error {
	orgRoleProvider, err := organization.NewOrgRoleProvider(h.commonPersistenceHandler, func() config.OrganizationsView { return cfg() })
	if err != nil {
		return fmt.Errorf("failed to create organization role provider: %w", err)
	}
	h.orgRoleProvider = orgRoleProvider
	startupPolicy, err := projectPermissionPolicy(view)
	if err != nil {
		return err
	}
	currentPolicy := newCachingPermissionConfigProvider(cfg, view, startupPolicy, logger)
	permissionHandler, err := permission.NewReloadingPermissionHandler(
		startupPolicy, currentPolicy, logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create permission handler: %w", err)
	}
	h.permissionHandler = permissionHandler
	return nil
}

// initializeUserSettings executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) initializeUserSettings() {
	h.userSettingsHandler = NewUserSettingsHandler(h.commonPersistenceHandler)
}

// initializeEmailClient executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `view`: supplies the email startup view.
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
func (h *CommonServiceHandler[U]) initializeEmailClient(provider func() config.EmailView, logger *zap.Logger) {
	h.emailClient = email_client.CreateReloadingEmailClient(provider, logger)
}

// initializeRateLimiter executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `view`: supplies restart-bound rate-limit settings.
// - `provider`: supplies one coherent hot configuration view per operation.
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
func (h *CommonServiceHandler[U]) initializeRateLimiter(view config.RateLimitsView, provider rate_limiter.ConfigProvider, logger *zap.Logger) error {
	h.rateLimiter = rate_limiter.NewReloadingSimpleRateLimiterFromView(logger, view, provider, h.metricsHandler)
	if h.rateLimiter == nil {
		return fmt.Errorf("failed to create rate limiter")
	}
	return nil
}

// initializeTokenService executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `view`: supplies one coherent startup generation.
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
func (h *CommonServiceHandler[U]) initializeTokenService(cfg config.ServiceProvider, view config.ServiceView, logger *zap.Logger) (token.TokenService, error) {
	// Prepare token service configuration
	tokenConfig := token.TokenServiceConfig{
		SecurityLifetime:   func() time.Duration { return cfg().VerificationTokenValidity() },
		SessionLifetime:    func() time.Duration { return cfg().SessionValidity() },
		SessionTokenSecret: view.SessionTokenSecret().StringValue(),
	}

	// Check if mock token service is requested
	if view.UseTokenServiceMock() {
		// Safety check - ensure mock service is only used in appropriate environments
		if !view.Env().IsMockingAllowed() {
			return nil, fmt.Errorf("mock token service cannot be used in %s environment - only allowed in development, test, or mocking environments", view.Env())
		}

		// Log warning about mock service usage
		logger.Named(loggerName).Warn("Using mock token service - DO NOT USE IN PRODUCTION",
			zap.Stringer(appLogger.KeyEnv, view.Env()),
			zap.Int64(appLogger.KeySeed, view.TokenServiceMockSeed()))

		// Create mock token service
		return token.NewMockTokenService(
			h.commonPersistenceHandler,
			tokenConfig,
			view.TokenServiceMockSeed(),
			logger,
		)
	}

	// Default to production token service
	tokenService, err := token.NewTokenService(
		h.commonPersistenceHandler,
		tokenConfig,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize token service: %w", err)
	}
	return tokenService, nil
}

// initializeAuthHandlers executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `cfg`: supplies the hot runtime configuration provider.
// - `view`: supplies the coherent startup configuration generation.
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `emailClient`: supplies the `emailClient` input as `*email_client.EmailClient` for this operation.
// - `tokenService`: supplies the `tokenService` input as `token.TokenService` for this operation.
// - `oauthSource`: supplies the application's OAuth providers; nil configures none.
func (h *CommonServiceHandler[U]) initializeAuthHandlers(
	cfg config.ServiceProvider,
	view config.ServiceView,
	logger *zap.Logger,
	emailClient *email_client.EmailClient,
	tokenService token.TokenService,
	oauthSource oauth.Source,
) error {
	// Compile and validate trusted frontend origins once per generation.
	origins := config.NewStrictGenerationMemo(func(v config.ServiceView) (*origin.Allowlist, error) {
		current, invalidOrigins := origin.NewAllowlistWithDefault(v.AllowedFrontendOrigins(), v.FrontendURL())
		policyLogger := logger.Named(loggerName)
		for _, entry := range invalidOrigins {
			policyLogger.Warn("Ignoring invalid "+entry.Key+" entry", zap.String(appLogger.KeyOrigin, entry.Value))
		}
		for _, entry := range current.Entries() {
			if !origin.IsSecureForProduction(entry) {
				policyLogger.Warn("Allowed frontend origin is not HTTPS; only use plain HTTP origins in local development", zap.String(appLogger.KeyOrigin, entry))
			}
		}
		return current, nil
	}, nil)
	_, _ = origins.Get(view)
	h.frontendOrigins = func() *origin.Allowlist {
		current, _ := origins.Get(cfg())
		return current
	}

	// Initialize the EmailAuthHandler
	emailAuthHandler, err := email.NewEmailAuthHandler(
		context.Background(),
		logger,
		func() email.RuntimeConfigView { return cfg() },
		emailClient,
		h.commonPersistenceHandler,
		h.userCallbacks,
		tokenService,
	)
	if err != nil {
		return fmt.Errorf("failed to create email auth handler: %w", err)
	}
	h.emailAuthHandler = emailAuthHandler

	// Keep the phone handler available so SMS can enable after startup.
	var smsService sms_client.SMSService = sms_client.CreateReloadingSMSClient(func() config.SmsView { return cfg() }, logger)

	// Initialize the PhoneAuthHandler
	phoneAuthHandler, err := phone.NewPhoneAuthHandler(
		logger,
		smsService,
		h.commonPersistenceHandler,
		h.userCallbacks,
		tokenService,
		func() phone.RuntimeConfigView { return cfg() },
	)
	if err != nil {
		return fmt.Errorf("failed to create phone auth handler: %w", err)
	}
	h.phoneAuthHandler = phoneAuthHandler

	// Initialize the AuthHandler
	authHandler, err := auth.NewAuthHandler(
		context.Background(),
		logger,
		func() auth.ConfigView { return cfg() },
		h.commonPersistenceHandler,
		h.userCallbacks,
		h.emailAuthHandler,
		tokenService,
		h.converter,
		auth.WithAccountLifecyclePreflight(h.accountLifecycleCoordinator.RequireTransaction),
		auth.WithOAuthProviders(oauthSource),
	)
	if err != nil {
		return fmt.Errorf("failed to create auth handler: %w", err)
	}
	h.authHandler = authHandler
	return nil
}

// configureCacheInvalidation executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) configureCacheInvalidation() {
	// Wire up cache invalidation so the persistence layer can evict users from
	// the auth cache after updates. SetUserCacheInvalidator is part of
	// CommonPersistenceInterface, so this also reaches decorated handlers.
	h.commonPersistenceHandler.SetUserCacheInvalidator(func(userID uint64) error {
		h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
		return nil
	})
}

// initializeAdminHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) initializeAdminHandler(logger *zap.Logger, inferenceModelCatalog *genai.Catalog) error {
	adminHandler := admin.NewAdminHandler(
		h.commonPersistenceHandler,
		h.permissionHandler,
		h.rateLimiter,
		h.attachmentLifecycleManager,
		h.converter,
		h.userCallbacks,
		&genai.DefaultModelResolver{},
		logger,
		admin.WithAccountLifecyclePreflight(h.accountLifecycleCoordinator.RequireTransaction),
		admin.WithAccountDeletionLifecyclePreflight(h.accountLifecycleCoordinator.RequireDeletionTransaction),
		admin.WithInferenceModelCatalog(inferenceModelCatalog),
	)
	if adminHandler == nil {
		return fmt.Errorf("failed to create admin handler")
	}
	h.adminHandler = adminHandler
	if h.chatHandler != nil {
		adminHandler.SetChatAccessCacheInvalidator(h.chatHandler.InvalidateAccessCache)
	}
	return nil
}

// initializeOrganizationHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `cfg`: supplies the hot runtime configuration provider.
func (h *CommonServiceHandler[U]) initializeOrganizationHandler(
	logger *zap.Logger,
	cfg config.ServiceProvider,
	options ...organization.HandlerOption,
) error {
	organizationHandler, err := organization.NewOrganizationHandler(
		h.commonPersistenceHandler,
		h.permissionHandler,
		h.orgRoleProvider,
		h.converter,
		func() organization.PolicyView { return cfg() },
		h.organizationCallbacks,
		logger,
		options...,
	)
	if err != nil {
		return fmt.Errorf("failed to create organization handler: %w", err)
	}
	h.organizationHandler = organizationHandler
	h.organizationPrivilegedHandler = organizationHandler
	h.personalWorkspaceCacheInvalidator = organizationHandler
	return nil
}

// initializePersonalWorkspaceLifecycle constructs the trusted workspace
// service, then replaces the handler's user callback pointer with an immutable
// clone coordinated around that service.
func (h *CommonServiceHandler[U]) initializePersonalWorkspaceLifecycle(
	view config.ServiceView,
) error {
	if view == nil {
		return fmt.Errorf("configuration view is required")
	}
	// Personal workspaces are always on whenever organizations are enabled;
	// applications that do not want them simply do not render them. Failing
	// fast here turns what would otherwise surface as an opaque
	// ErrUserCreationFailed at first signup into a construction-time error
	// that names the fix.
	organizationsEnabled := view.EnableOrganizations()
	if organizationsEnabled && h.userCallbacks == nil {
		return fmt.Errorf("no ports.UserCallbackConfig provided; personal workspace provisioning requires user callbacks when organizations are enabled")
	}
	personalWorkspaceService, err := organization.NewPersonalWorkspaceService(
		h.commonPersistenceHandler,
		h.converter,
		h.organizationCallbacks,
		organization.PersonalWorkspaceServiceConfig{
			OrganizationType: view.PersonalWorkspaceOrganizationType(),
			OwnerRole:        view.PersonalWorkspaceOwnerRole(),
			// The root-ID cache deliberately shares the organization role-cache
			// knobs rather than introducing new config surface: both memoize
			// small per-user organization facts with post-commit eviction.
			RootIDCacheCapacity: view.OrganizationRoleCacheCapacity(),
			RootIDCacheTTL:      view.OrganizationRoleCacheTTL(),
		},
		h.personalWorkspaceCacheInvalidator,
	)
	if err != nil {
		return err
	}
	h.personalWorkspaceService = personalWorkspaceService
	h.personalWorkspacesEnabled = organizationsEnabled
	h.accountLifecycleCoordinator = newAccountLifecycleCoordinator(h.userCallbacks, h.personalWorkspaceService, organizationsEnabled)
	// With organizations disabled an application may legitimately run without
	// user callbacks; keep the nil pointer so downstream handlers apply their
	// historical nil-callback behavior instead of a hollow coordinator clone
	// with a nil user lookup.
	if h.userCallbacks != nil {
		h.userCallbacks = h.accountLifecycleCoordinator.InternalCallbacks()
	}
	return nil
}

// initializeAttachmentRegistry executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) initializeAttachmentRegistry() error {
	h.objectTypeRegistry = attachmentpkg.NewObjectTypeRegistry()
	if h.objectTypeRegistry == nil {
		return fmt.Errorf("failed to create object type registry")
	}
	// Identity-only applications do not publish public avatars or expose uploads.
	// Keep the registry for private internal artifacts without requiring a public bucket.
	if view := h.runtimeView(); view != nil && !view.EnableAttachments() && !view.EnableUserProfiles() {
		return nil
	}
	// Built-in object types are registered by the registry constructor, which has no
	// persistence reference. Validate their visibility here so a deployment that asks
	// for public attachments without a public bucket fails at construction.
	for _, objectType := range h.objectTypeRegistry.ListObjectTypes() {
		if err := h.validateObjectTypeVisibility(h.objectTypeRegistry.Get(objectType)); err != nil {
			return err
		}
	}
	return nil
}

// initializeAttachmentLifecycleManager executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `runtimeConfigProvider`: supplies one coherent hot policy per operation.
func (h *CommonServiceHandler[U]) initializeAttachmentLifecycleManager(
	logger *zap.Logger,
	runtimeConfigProvider attachmentpkg.RuntimeConfigProvider,
) error {
	manager, err := attachmentpkg.NewAttachmentLifecycleManager(h.commonPersistenceHandler, logger, h.objectTypeRegistry, runtimeConfigProvider)
	if err != nil {
		return fmt.Errorf("failed to create attachment lifecycle manager: %w", err)
	}
	h.attachmentLifecycleManager = manager
	h.wireInternalAttachmentStore()
	return nil
}

// wireInternalAttachmentStore binds private provider-state and tool-media storage once both
// consumers and the attachment lifecycle manager exist. Initialization currently constructs the
// dispatcher before the lifecycle manager, so keeping this helper order-independent prevents a
// permanently nil-backed adapter from being captured during service construction.
func (h *CommonServiceHandler[U]) wireInternalAttachmentStore() {
	if h == nil || h.attachmentLifecycleManager == nil {
		return
	}
	store := attachmentpkg.NewInternalStoreAdapter(h.attachmentLifecycleManager)
	if h.botDispatcher != nil {
		h.botDispatcher.SetInternalAttachmentStore(store)
	}
	if h.chatHandler != nil {
		h.chatHandler.SetInternalAttachmentStore(store)
	}
}

// configureProfilePictureCallback executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) configureProfilePictureCallback() error {
	if profilePicConfig := h.objectTypeRegistry.Get("profile-picture"); profilePicConfig != nil {
		// Configure entity validation for profile pictures
		profilePicConfig.ValidateEntity = func(entityID string) error {
			// Validate user ID format
			_, err := strconv.ParseUint(entityID, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid user ID format: %w", err)
			}
			return nil
		}

		// Use EntityBatchCallback for efficient batch processing of profile pictures
		// This ensures optimal performance when multiple profile pictures are processed together
		profilePicConfig.EntityBatchCallback = func(ctx context.Context, attachments []*common_persistence_models.Attachment, entityID string) error {
			// Entity type is implied to be "user" for profile pictures
			// Parse user ID from entity ID
			userID, err := strconv.ParseUint(entityID, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid user ID format: %w", err)
			}

			// For profile pictures, we typically expect only one attachment per user
			// If multiple are provided, use the latest one (highest ID)
			var latestAttachment *common_persistence_models.Attachment
			for _, attachment := range attachments {
				if attachment == nil {
					return appLogger.InvariantError(
						"profile_picture_callback_attachment_non_nil",
						nil,
						zap.String(logkeys.Stage, "profile_picture_entity_callback"),
						zap.Uint64(appLogger.KeyUserID, userID),
						zap.String(appLogger.KeyEntityID, entityID),
						zap.String(logkeys.Expected, "non-nil attachment row"),
						zap.String(logkeys.Actual, "nil"),
					)
				}
				// Verify ownership for all attachments
				if userID != attachment.UserID {
					appLogger.For(ctx, h.logger, loggerName).Warn("Profile picture entity doesn't match owner",
						zap.Uint64(appLogger.KeyAttachmentUserID, attachment.UserID),
						zap.Uint64(appLogger.KeyUserID, userID),
						zap.String(appLogger.KeyEntityID, entityID),
						zap.String(appLogger.KeyObjectType, attachment.ObjectType))
					return fmt.Errorf("users can only set their own profile picture")
				}

				// Select the latest attachment (highest ID indicates most recent)
				if latestAttachment == nil || attachment.ID > latestAttachment.ID {
					latestAttachment = attachment
				}
			}

			if latestAttachment == nil {
				return appLogger.InvariantError(
					"profile_picture_callback_attachment_available",
					nil,
					zap.String(logkeys.Stage, "profile_picture_entity_callback"),
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.String(appLogger.KeyEntityID, entityID),
					zap.String(logkeys.Expected, "at least one profile picture attachment"),
					zap.Int(logkeys.Actual, len(attachments)),
				)
			}

			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Setting profile picture from batch",
					zap.Uint64(appLogger.KeyUserID, userID),
					zap.Uint64(appLogger.KeyAttachmentID, latestAttachment.ID),
					zap.Int(appLogger.KeyAttachmentCount, len(attachments)))
			}

			defer h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))

			// Update the user's profile picture using persistence layer
			// Note: UpdateUserProfilePicture will handle cache invalidation
			return h.commonPersistenceHandler.UpdateUserProfilePicture(ctx, latestAttachment.UserID, &latestAttachment.ID)
		}
	}
	return nil
}

// configureChatAttachmentCallback executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) configureChatAttachmentCallback() error {
	if h.objectTypeRegistry == nil {
		return fmt.Errorf("object type registry not initialized")
	}

	securityConstraints := attachmentpkg.DefaultSecurityConstraints()
	chatAttachmentConfig := &attachmentpkg.ObjectTypeConfig{
		Name:              attachmentpkg.ObjectTypeChatAttachment,
		ImpliedEntityType: "chat-message",
		MaxSize:           50 * 1024 * 1024, // 50MB
		AllowedMimeTypes: []string{
			"image/jpeg",
			"image/jpg",
			"image/png",
			"image/gif",
			"image/webp",
		},
		ForbiddenMimeTypes: securityConstraints.DangerousTypes,
		StreamingValidators: []attachmentpkg.StreamingAttachmentValidator{
			attachmentpkg.NewStreamingSizeValidator(50 * 1024 * 1024),
			attachmentpkg.NewStreamingImageDimensionValidator(8192, 8192, 1, 1),
			attachmentpkg.NewStreamingSecurityValidator(),
		},
		RequiresEntity:   true,
		SingleEntityOnly: true,
		Description:      "Chat image attachments",
	}
	archivedChatAttachmentConfig := &attachmentpkg.ObjectTypeConfig{
		Name:               attachmentpkg.ObjectTypeArchivedChatAttachment,
		ImpliedEntityType:  "chat-message",
		MaxSize:            chatAttachmentConfig.MaxSize,
		AllowedMimeTypes:   append([]string(nil), chatAttachmentConfig.AllowedMimeTypes...),
		ForbiddenMimeTypes: append([]string(nil), chatAttachmentConfig.ForbiddenMimeTypes...),
		StreamingValidators: append([]attachmentpkg.StreamingAttachmentValidator(nil),
			chatAttachmentConfig.StreamingValidators...),
		RequiresEntity:   true,
		SingleEntityOnly: true,
		Description:      "Archived chat image attachments retained for message history",
	}

	// A queued agent input's uploads are chat attachments that belong to the input until delivery
	// moves them to the message it becomes, after which the chat-attachment flow applies.
	queuedInputAttachmentConfig := &attachmentpkg.ObjectTypeConfig{
		Name:               attachmentpkg.ObjectTypeChatQueuedInputAttachment,
		ImpliedEntityType:  "chat-agent-queued-input",
		MaxSize:            chatAttachmentConfig.MaxSize,
		AllowedMimeTypes:   append([]string(nil), chatAttachmentConfig.AllowedMimeTypes...),
		ForbiddenMimeTypes: append([]string(nil), chatAttachmentConfig.ForbiddenMimeTypes...),
		StreamingValidators: append([]attachmentpkg.StreamingAttachmentValidator(nil),
			chatAttachmentConfig.StreamingValidators...),
		RequiresEntity:   true,
		SingleEntityOnly: true,
		Description:      "Chat image attachments of agent input queued until the agent receives it",
	}

	chatAttachmentConfig.ValidateEntity = func(entityID string) error {
		if _, err := strconv.ParseUint(entityID, 10, 64); err != nil {
			return fmt.Errorf("invalid message ID format: %w", err)
		}
		return nil
	}
	archivedChatAttachmentConfig.ValidateEntity = chatAttachmentConfig.ValidateEntity
	queuedInputAttachmentConfig.ValidateEntity = func(entityID string) error {
		if _, err := strconv.ParseUint(entityID, 10, 64); err != nil {
			return fmt.Errorf("invalid queued input ID format: %w", err)
		}
		return nil
	}
	chatAttachmentConfig.EntityBatchCallback = h.chatAttachmentsVetted
	queuedInputAttachmentConfig.EntityBatchCallback = h.chatQueuedInputAttachmentsVetted

	if err := h.RegisterObjectType(chatAttachmentConfig); err != nil {
		return err
	}
	if err := h.RegisterObjectType(archivedChatAttachmentConfig); err != nil {
		return err
	}
	return h.RegisterObjectType(queuedInputAttachmentConfig)
}

// chatAttachmentsVetted is the entity callback of a chat message's vetted uploads (entityID is the
// message). After commit, once every attachment of the message is vetted, it tells the channel
// the message's attachments are ready (MESSAGE_ATTACHMENTS_READY).
func (h *CommonServiceHandler[U]) chatAttachmentsVetted(ctx context.Context, attachments []*common_persistence_models.Attachment, entityID string) error {
	runCallback := func(baseCtx context.Context) error {
		messageID, err := strconv.ParseUint(entityID, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid message ID format: %w", err)
		}

		// Detach from request/worker cancellation so bot events mirror post-commit behavior.
		eventCtx := context.WithoutCancel(baseCtx)
		eventCtx = scontext.CopySRouterContext[common_service_models.UserIdType, U](eventCtx, eventCtx)
		scontext.ClearTransaction[common_service_models.UserIdType, U](eventCtx)

		message, err := h.commonPersistenceHandler.GetChatMessageByID(eventCtx, messageID)
		if err != nil {
			return fmt.Errorf("failed to load message for attachments: %w", err)
		}

		attachmentIDSet := make(map[uint64]struct{}, len(message.AttachmentIDs))
		for _, id := range message.AttachmentIDs {
			if id == 0 {
				continue
			}
			attachmentIDSet[uint64(id)] = struct{}{}
		}

		for _, attachment := range attachments {
			if attachment.UserID != message.SenderID {
				appLogger.For(eventCtx, h.logger, loggerName).Warn("Chat attachment owner mismatch",
					zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
					zap.Uint64(appLogger.KeyAttachmentUserID, attachment.UserID),
					zap.Uint64(appLogger.KeySenderID, message.SenderID),
					zap.String(appLogger.KeyEntityID, entityID))
				return fmt.Errorf("attachment owner does not match message sender")
			}
			if _, exists := attachmentIDSet[attachment.ID]; !exists {
				appLogger.LogInvariantViolation(appLogger.For(eventCtx, h.logger, loggerName), "chat_attachment_referenced_by_message",
					nil,
					zap.String(logkeys.Stage, "chat_attachment_entity_callback"),
					zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
					zap.Uint64(appLogger.KeyMessageID, messageID),
					zap.String(logkeys.Expected, "attachment ID present on message"),
					zap.String(logkeys.Actual, "missing"),
					zap.String(logkeys.Fallback, "continue vetting remaining message attachments"))
			}
		}

		// Only emit attachments-ready when all message attachments are vetted.
		attachmentIDs := make([]uint64, 0, len(message.AttachmentIDs))
		for _, id := range message.AttachmentIDs {
			if id == 0 {
				continue
			}
			attachmentIDs = append(attachmentIDs, uint64(id))
		}
		if len(attachmentIDs) > 0 {
			allAttachments, fetchErr := h.commonPersistenceHandler.GetAttachmentsByIDs(eventCtx, attachmentIDs)
			if fetchErr != nil {
				return fmt.Errorf("failed to load attachments for message: %w", fetchErr)
			}

			attachmentByID := make(map[uint64]*common_persistence_models.Attachment, len(allAttachments))
			for _, attachment := range allAttachments {
				attachmentByID[attachment.ID] = attachment
			}
			for _, id := range attachmentIDs {
				attachment := attachmentByID[id]
				if attachment == nil {
					appLogger.LogInvariantViolation(appLogger.For(eventCtx, h.logger, loggerName), "chat_message_attachment_row_available",
						nil,
						zap.String(logkeys.Stage, "chat_attachment_vetting_check"),
						zap.Uint64(appLogger.KeyAttachmentID, id),
						zap.Uint64(appLogger.KeyMessageID, messageID),
						zap.String(logkeys.Expected, "attachment row for every message attachment ID"),
						zap.String(logkeys.Actual, "missing"),
						zap.String(logkeys.Fallback, "suppress attachments-ready event"))
					return nil
				}
				if attachment.UploadStatus != common_persistence_models.UploadStatusVetted {
					if appLogger.DebugEnabled(h.logger) {
						appLogger.For(eventCtx, h.logger, loggerName).Debug("Chat attachment not yet vetted",
							zap.Uint64(appLogger.KeyAttachmentID, id),
							zap.Uint64(appLogger.KeyMessageID, messageID),
							zap.Stringer(logkeys.Status, attachment.UploadStatus))
					}
					return nil
				}
			}
		}

		updatedMessage := h.converter.PersistenceChatMessageToService(message)
		if updatedMessage == nil {
			appLogger.LogInvariantViolation(appLogger.For(eventCtx, h.logger, loggerName), "chat_message_conversion_non_nil",
				nil,
				zap.String(logkeys.Stage, "chat_attachment_entity_callback"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.String(logkeys.Expected, "converted chat message"),
				zap.String(logkeys.Actual, "nil"),
				zap.String(logkeys.Fallback, "suppress attachments-ready event"))
			return nil
		}
		if err := h.HydrateChatMessageAttachments(eventCtx, updatedMessage); err != nil {
			return fmt.Errorf("failed to hydrate chat message attachments: %w", err)
		}
		if h.chatEventPublisher != nil {
			event := &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_ATTACHMENTS_READY,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(message.SenderID),
				Message:     updatedMessage,
			}
			if publishErr := h.chatEventPublisher.PublishChatEvent(eventCtx, event); publishErr != nil {
				appLogger.For(eventCtx, h.logger, loggerName).Warn("Failed to publish chat attachment update event",
					zap.Error(publishErr),
					zap.String(logkeys.Stage, "publish_chat_attachment_update"),
					zap.Uint64(appLogger.KeyMessageID, messageID),
					zap.Uint64(appLogger.KeyUserID, message.SenderID),
					zap.String(logkeys.Fallback, "retain persisted state without realtime notification"))
			}
		}
		return nil
	}

	if persistence.AddPostCommitHookFromContext(ctx, runCallback) {
		return nil
	}
	return runCallback(ctx)
}

// chatQueuedInputAttachmentsVetted is the entity callback of a queued agent input's vetted uploads.
// The composer's queue tray shows the input's attachments, so after commit the input's session
// tells its readers to read the queue again. Once delivery moved the uploads to the message the
// input became, they are that message's (chatQueuedInputAttachmentsMoved).
func (h *CommonServiceHandler[U]) chatQueuedInputAttachmentsVetted(ctx context.Context, attachments []*common_persistence_models.Attachment, entityID string) error {
	queuedInputID, err := strconv.ParseUint(entityID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid queued input ID format: %w", err)
	}
	announce := func(baseCtx context.Context) error {
		eventCtx := persistence.WithoutRequestTransaction[U](context.WithoutCancel(baseCtx))
		queued, err := h.commonPersistenceHandler.GetChatAgentQueuedInput(eventCtx, queuedInputID)
		if errors.Is(err, persistence.ErrChatAgentQueuedInputNotQueued) {
			return h.chatQueuedInputAttachmentsMoved(eventCtx, attachments)
		}
		if err != nil {
			return fmt.Errorf("failed to load queued input for attachments: %w", err)
		}
		for _, attachment := range attachments {
			if attachment.UserID != queued.SenderID {
				appLogger.For(eventCtx, h.logger, loggerName).Warn("Queued agent input attachment owner mismatch",
					zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
					zap.Uint64(appLogger.KeyAttachmentUserID, attachment.UserID),
					zap.Uint64(appLogger.KeySenderID, queued.SenderID),
					zap.Uint64(appLogger.KeyQueuedInputID, queuedInputID))
				return fmt.Errorf("attachment owner does not match queued input sender")
			}
		}
		if h.chatHandler != nil {
			h.chatHandler.PublishAgentSessionInteractionAfterCommit(eventCtx, queued.AgentSessionID, queued.ChannelID, queued.ParentMessageID)
		}
		return nil
	}
	if persistence.AddPostCommitHookFromContext(ctx, announce) {
		return nil
	}
	return announce(ctx)
}

// chatQueuedInputAttachmentsMoved finishes vetted uploads of a queued input that left the queue.
// Processing reads an upload's owner when it starts, so an upload that delivery moved to the
// input's message while it was processed still reaches the queued-input callback: it is the
// message's now, and the message's attachments become ready through the chat-attachment flow
// (chatAttachmentsVetted). An input delivered after its uploads were vetted was announced with
// them, and the uploads of a removed or discarded input stay with it, so neither needs anything.
func (h *CommonServiceHandler[U]) chatQueuedInputAttachmentsMoved(ctx context.Context, attachments []*common_persistence_models.Attachment) error {
	current, err := h.commonPersistenceHandler.GetAttachmentsByIDs(ctx, common_util.ApplyToEach(attachments, func(attachment *common_persistence_models.Attachment) uint64 {
		return attachment.ID
	}))
	if err != nil {
		return fmt.Errorf("failed to reload delivered queued input attachments: %w", err)
	}
	var moved []*common_persistence_models.Attachment
	for _, attachment := range current {
		if attachment.ObjectType == attachmentpkg.ObjectTypeChatAttachment {
			moved = append(moved, attachment)
		}
	}
	if len(moved) == 0 {
		return nil
	}
	// Delivery moves every upload of an input to the one message it becomes.
	return h.chatAttachmentsVetted(ctx, moved, moved[0].EntityID)
}

// configureCareerResumeAttachmentType executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) configureCareerResumeAttachmentType() error {
	if h.objectTypeRegistry == nil {
		return fmt.Errorf("object type registry not initialized")
	}

	securityConstraints := attachmentpkg.DefaultSecurityConstraints()
	return h.RegisterObjectType(&attachmentpkg.ObjectTypeConfig{
		Name:              "career-resume",
		ImpliedEntityType: "career-application",
		MaxSize:           common_persistence_models.MaxCareerResumeSize,
		AllowedMimeTypes: []string{
			"application/pdf",
			"application/msword",
			"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			"text/plain",
		},
		ForbiddenMimeTypes: securityConstraints.DangerousTypes,
		StreamingValidators: []attachmentpkg.StreamingAttachmentValidator{
			attachmentpkg.NewStreamingSizeValidator(common_persistence_models.MaxCareerResumeSize),
			attachmentpkg.NewStreamingSecurityValidator(),
		},
		RequiresEntity:   true,
		SingleEntityOnly: true,
		Description:      "Career application resume uploads",
		PostProcessCallback: func(ctx context.Context, attachment *common_persistence_models.Attachment) error {
			if attachment == nil || strings.TrimSpace(attachment.EntityID) == "" {
				return fmt.Errorf("career resume attachment missing linked application reference")
			}
			return h.transitionCareerApplicationToSubmittedIfPending(ctx, attachment.EntityID)
		},
	})
}

// Stop executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (handler *CommonServiceHandler[U]) Stop() {
	if handler.notificationHandler != nil {
		handler.notificationHandler.Stop()
	}
	logger := appLogger.For(context.Background(), handler.logger, loggerName)
	if handler.agentRuntime != nil {
		handler.agentRuntime.stopManagedDrives()
		handler.agentRuntime.stopRecovery()
	}
	handler.stopOwnedPostCommitHooks()
	// Join the transcript writers' bounded post-commit runners before shutting down chat or
	// attachment dependencies they may still use.
	if handler.botDispatcher != nil {
		handler.botDispatcher.StopPostCommitHooks()
	}

	// Stop the agentic tool-approval lane before closing dependencies used by its post-commit hooks.
	if handler.chatHandler != nil {
		handler.chatHandler.StopApprovalBackgroundTasks()
		handler.chatHandler.StopAgentRuns()
	}

	// Stop attachment workers if they're running
	if handler.attachmentWorkerCancel != nil {
		handler.attachmentWorkerCancel()
		handler.attachmentWorkerWg.Wait()
		logger.Info("Attachment workers stopped")
	}

	// Stop unverified user cleanup if it's running
	if handler.unverifiedUserCleanupCancel != nil {
		handler.unverifiedUserCleanupCancel()
		handler.unverifiedUserCleanupWg.Wait()
		logger.Info("Unverified user cleanup stopped")
	}

	if handler.attachmentLifecycleManager != nil {
		if err := handler.attachmentLifecycleManager.Close(); err != nil {
			logger.Error("Failed to close attachment lifecycle manager", zap.Error(err))
		}
	}

	// The organization handler owns the role, depth, and metrics cache
	// goroutines. If construction failed before that handler was installed, the
	// role provider may still have been started by permission initialization.
	if handler.organizationHandler != nil {
		handler.organizationHandler.Stop()
	} else if handler.orgRoleProvider != nil {
		handler.orgRoleProvider.Stop()
	}

	if handler.authHandler != nil {
		handler.authHandler.Stop()
	}

	if handler.emailAuthHandler != nil {
		handler.emailAuthHandler.Stop()
	}
	if handler.emailClient != nil {
		if err := handler.emailClient.Stop(); err != nil {
			logger.Error("Failed to close email client", zap.Error(err))
		}
	}

	if handler.phoneAuthHandler != nil {
		handler.phoneAuthHandler.Stop()
	}

	if handler.userSettingsHandler != nil {
		handler.userSettingsHandler.Stop()
	}

	if handler.rateLimiter != nil {
		if err := handler.rateLimiter.Close(); err != nil {
			logger.Error("Failed to close rate limiter", zap.Error(err))
		}
	}
}
