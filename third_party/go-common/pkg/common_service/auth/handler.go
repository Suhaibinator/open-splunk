package auth

import (
	"context"
	stdErrors "errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth/email"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// AuthHandlerInterface defines the interface for authentication operations.
// It provides methods for token validation, user authentication via email and OAuth,
// token revocation, and cache management. The generic type U represents the user model
// that implements the UserLike interface.
type AuthHandlerInterface[U common_service_models.UserLike] interface {
	ValidateAuthToken(authCode string) (bool, time.Time, error)
	GetUserIDFromAuthToken(ctx context.Context, authCode string) (common_service_models.UserIdType, error)
	GetUserByID(ctx context.Context, userID common_service_models.UserIdType) (*U, error)
	GetUserFromAuthToken(ctx context.Context, authCode string) (*U, error)
	CreateUserAuth(ctx context.Context, method token.SessionAuthMethod, tokenType proto_models.TokenType, obtainedAuthCode string, userId common_service_models.UserIdType) (string, error)
	ListActiveSessions(ctx context.Context, userID common_service_models.UserIdType, currentToken string, pagination *proto_models.PaginationRequest) ([]*ActiveSession, *proto_models.PaginationResponse, error)

	// Authentication methods
	LoginUserByEmail(ctx context.Context, email, password string) (*U, string, error)
	LoginUserByEmailWithOTP(ctx context.Context, email, password, otpCode string) (*U, string, bool, error)
	CreateOAuthState(ctx context.Context, req OAuthStateRequest) (*OAuthAuthorization, error)
	LoginUserByOAuth(ctx context.Context, callback OAuthCallback) (OAuthLoginResult[U], error)
	OAuthFormPostRedirect(ctx context.Context, form OAuthFormPost) (string, error)
	ListOAuthProviders(ctx context.Context) []OAuthProviderDescriptor
	CheckAccountEmailDomain(ctx context.Context, email string) error

	// RefreshSessionToken mints a replacement session token for a DB-valid old token,
	// revokes the old token, and returns the new one. Returns ErrTokenRevoked /
	// ErrInvalidToken / ErrTokenExpired when the old token must not be refreshed.
	RefreshSessionToken(ctx context.Context, oldToken string) (string, error)

	// Logout methods
	RevokeAuthToken(ctx context.Context, authToken string) error
	RevokeAllUserTokens(ctx context.Context, userID common_service_models.UserIdType) error
	RevokeSession(ctx context.Context, userID common_service_models.UserIdType, sessionID string, currentToken string) (uint32, error)
	RevokeOtherSessions(ctx context.Context, userID common_service_models.UserIdType, currentToken string) (uint32, error)
	InvalidateAuthToken(ctx context.Context, authToken string)
	InvalidateUserCache(userID common_service_models.UserIdType)
	SetRealtimeSessionCloser(closer ports.RealtimeSessionCloser)

	// Multi-auth methods
	GetUserAuthMethods(ctx context.Context, userID uint64) (*common_service_models.UserAuthMethods, error)
	ValidateSessionToken(authCode string) (*token.SessionTokenClaims, error)
	RequireRecentAuthentication(ctx context.Context, claims *token.SessionTokenClaims, maxAge time.Duration) error

	Stop()
}

// AuthHandler carries state used by the auth backend flow.
type AuthHandler[U common_service_models.UserLike] struct {
	logger                    *zap.Logger
	config                    startupConfig
	runtimeConfigProvider     RuntimeConfigProvider
	cachePolicyMu             sync.Mutex
	cachePolicyTTL            time.Duration
	cacheGeneration           uint64 // bumped by every invalidation; guarded by cachePolicyMu
	realtimeSessions          ports.RealtimeSessionCloser
	cacheTokenCapacity        uint64
	cacheUserCapacity         uint64
	accountLifecyclePreflight func(context.Context) error
	sessionAuthMethodPolicy   SessionAuthMethodPolicy
	persistenceHandler        persistence.CommonPersistenceInterface
	authTokenToUserIDCache    *ttlcache.Cache[string, common_service_models.UserIdType]
	userIDToUserCache         *ttlcache.Cache[common_service_models.UserIdType, U] // Store U directly (User values)
	userCallbacks             *ports.UserCallbackConfig[U]
	emailAuthHandler          email.EmailAuthHandlerInterface[U]
	tokenService              token.TokenService // Unified token service
	converter                 common_converter.ConverterInterface

	// Sliding-session refresh state: refreshGroup collapses concurrent refreshes of
	// the same token into one mint; refreshedTokens maps a just-replaced token to its
	// replacement for the grace window so parallel tabs re-receive the same new token.
	// refreshedTokens is nil when session refresh is disabled.
	refreshGroup    singleflight.Group
	refreshedTokens *ttlcache.Cache[string, string]

	// frontendOriginMemo holds the trusted frontend origins used to validate
	// the returnUrl carried in OAuth state (FrontendURL's origin included)
	frontendOriginMemo *config.StrictGenerationMemo[RuntimeConfigView, *origin.Allowlist]

	// oauthSource is the application's provider source, sampled once per OAuth
	// operation; nil configures no providers. oauthRuntime performs protocol
	// work and caches OIDC discovery; Stop closes it.
	oauthSource     oauth.Source
	oauthRuntime    *oauth.Runtime
	oauthStateKey   []byte
	oauthBindingKey []byte
	// oauthExclusionsLogged is the last logged set of invalid providers.
	oauthExclusionsMu     sync.Mutex
	oauthExclusionsLogged string

	// stopCh ends the cache-metrics goroutine when Stop is called; stopOnce
	// keeps Stop idempotent.
	stopCh   chan struct{}
	stopOnce sync.Once

	// Cache statistics for both caches
	tokenCacheHits      atomic.Int64
	tokenCacheMisses    atomic.Int64
	tokenCacheEvictions atomic.Int64
	userCacheHits       atomic.Int64
	userCacheMisses     atomic.Int64
	userCacheEvictions  atomic.Int64
}

type handlerOptions struct {
	accountLifecyclePreflight func(context.Context) error
	sessionAuthMethodPolicy   SessionAuthMethodPolicy
	oauthSource               oauth.Source
}

// HandlerOption configures optional AuthHandler integrations at construction.
type HandlerOption func(*handlerOptions)

// WithAccountLifecyclePreflight configures the transaction capability check
// that runs only when OAuth is about to create a new core user.
func WithAccountLifecyclePreflight(preflight func(context.Context) error) HandlerOption {
	return func(options *handlerOptions) {
		options.accountLifecyclePreflight = preflight
	}
}

// RuntimeConfigView is the hot signup policy consumed by the auth package.
// Values returned by one view must belong to one immutable configuration
// generation.
type RuntimeConfigView interface {
	AuthCacheTtl() time.Duration
	AuthTokenCacheCapacity() uint64
	AuthUserCacheCapacity() uint64
	OAuthSignupMode() config.OAuthSignupMode
	OAuthDefaultUsernamePrefix() string
	OAuthHTTPTimeout() time.Duration
	OAuthStateTTL() time.Duration
	OAuthStateClockSkew() time.Duration
	OAuthDiscoveryRefreshInterval() time.Duration
	OAuthSignedClientSecretLifetime() time.Duration
	OAuthMaxResponseBytes() int64
	OAuthMaxStateLength() int
	FrontendURL() string
	AllowedFrontendOrigins() []string
	SessionValidity() time.Duration
	DefaultUserType() proto_models.UserType
	RequireUsernamesOnSignup() bool
	SanitizerUsernameMinLength() int
	SanitizerUsernameMaxLength() int
}

var _ RuntimeConfigView = config.RootView{}

// RuntimeConfigProvider captures one immutable auth configuration generation.
type RuntimeConfigProvider func() RuntimeConfigView

// ConfigView supplies startup authentication state and current operational policy
// from one immutable configuration generation.
type ConfigView interface {
	RuntimeConfigView
	config.OauthView
	SessionTokenSecret() config.Secret
	SessionRefreshRevocationGrace() time.Duration
	EnableSessionRefresh() bool
}

var _ ConfigView = config.RootView{}

// ConfigProvider returns the current immutable authentication generation.
type ConfigProvider func() ConfigView

// startupConfig retains only restart-bound authentication state.
type startupConfig struct {
	sessionTokenSecret  string
	sessionRefreshGrace time.Duration
}

// ActiveSession is the user-facing session metadata returned by session management APIs.
// Raw bearer tokens are intentionally excluded.
type ActiveSession struct {
	SessionID  string
	Current    bool
	AuthMethod proto_models.AuthProvider
	// OAuthProviderSlug names the provider of an AUTH_PROVIDER_OAUTH session.
	OAuthProviderSlug string
	IPAddress         string
	UserAgent         string
	DeviceID          string
	IssuedAt          time.Time
	ExpiresAt         time.Time
	LastUsedAt        *time.Time
}

const (
	authLoggerName = "common_service.auth"

	oauthUsernameMaxAttempts = 50
)

func NewAuthHandler[U common_service_models.UserLike](
	ctx context.Context,
	logger *zap.Logger,
	provider ConfigProvider,
	persistenceHandler persistence.CommonPersistenceInterface,
	userCallbacks *ports.UserCallbackConfig[U],
	emailHandler email.EmailAuthHandlerInterface[U],
	tokenService token.TokenService,
	converter common_converter.ConverterInterface,
	options ...HandlerOption,
) (AuthHandlerInterface[U], error) {
	if provider == nil {
		return nil, fmt.Errorf("auth.NewAuthHandler: config provider cannot be nil")
	}
	if tokenService == nil {
		return nil, fmt.Errorf("auth.NewAuthHandler: tokenService cannot be nil")
	}
	view := provider()
	authHandlerConfig := startupConfig{sessionTokenSecret: view.SessionTokenSecret().StringValue()}
	if view.EnableSessionRefresh() {
		authHandlerConfig.sessionRefreshGrace = view.SessionRefreshRevocationGrace()
	}
	cacheTTL := view.AuthCacheTtl()
	tokenCapacity, userCapacity := view.AuthTokenCacheCapacity(), view.AuthUserCacheCapacity()
	settings := handlerOptions{}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.oauthSource != nil {
		providers, err := settings.oauthSource()
		if err != nil {
			return nil, fmt.Errorf("auth.NewAuthHandler: OAuth provider source: %w", err)
		}
		if err := oauth.ValidateProviders(providers); err != nil {
			return nil, fmt.Errorf("auth.NewAuthHandler: OAuth providers: %w", err)
		}
	}

	// Create two TTL caches:
	// 1. Auth token to user ID cache
	authTokenToUserIDCache := ttlcache.New(
		ttlcache.WithTTL[string, common_service_models.UserIdType](cacheTTL),
		ttlcache.WithCapacity[string, common_service_models.UserIdType](tokenCapacity),
		ttlcache.WithDisableTouchOnHit[string, common_service_models.UserIdType](),
	)

	// 2. User ID to user object cache - stores U directly (User values)
	userIDToUserCache := ttlcache.New(
		ttlcache.WithTTL[common_service_models.UserIdType, U](cacheTTL),
		ttlcache.WithCapacity[common_service_models.UserIdType, U](userCapacity),
		ttlcache.WithDisableTouchOnHit[common_service_models.UserIdType, U](),
	)

	// Initialize the main handler struct
	h := &AuthHandler[U]{
		logger:                    logger,
		config:                    authHandlerConfig,
		accountLifecyclePreflight: settings.accountLifecyclePreflight,
		sessionAuthMethodPolicy:   settings.sessionAuthMethodPolicy,

		persistenceHandler:     persistenceHandler,
		authTokenToUserIDCache: authTokenToUserIDCache,
		userIDToUserCache:      userIDToUserCache,
		userCallbacks:          userCallbacks,
		emailAuthHandler:       emailHandler,
		tokenService:           tokenService,
		converter:              converter,

		cachePolicyTTL:     cacheTTL,
		cacheTokenCapacity: tokenCapacity,
		cacheUserCapacity:  userCapacity,

		oauthSource:     settings.oauthSource,
		oauthRuntime:    oauth.NewRuntime(nil),
		oauthStateKey:   deriveOAuthKey(authHandlerConfig.sessionTokenSecret, oauthStateKeyLabel),
		oauthBindingKey: deriveOAuthKey(authHandlerConfig.sessionTokenSecret, oauthBindingKeyLabel),

		stopCh: make(chan struct{}),
	}

	h.setRuntimeConfigProvider(func() RuntimeConfigView { return provider() })
	_, _ = h.frontendOriginMemo.Get(view)

	if authHandlerConfig.sessionRefreshGrace > 0 {
		h.refreshedTokens = newRefreshedTokensCache(
			authHandlerConfig.sessionRefreshGrace,
			tokenCapacity,
		)
		go h.refreshedTokens.Start()
	}

	// Define callbacks for expired items
	authTokenToUserIDCache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, common_service_models.UserIdType]) {
		h.tokenCacheEvictions.Add(1)
	})

	userIDToUserCache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[common_service_models.UserIdType, U]) {
		h.userCacheEvictions.Add(1)
	})

	// Start the cache cleanups
	go authTokenToUserIDCache.Start()
	go userIDToUserCache.Start()

	// Start cache metrics collection
	go h.collectCacheMetrics()

	return h, nil
}

// setRuntimeConfigProvider installs the hot runtime provider and the
// frontend-origin memo derived from it. Call it before publishing the handler
// to concurrent callers. A nil provider is ignored, keeping the current
// provider.
func (h *AuthHandler[U]) setRuntimeConfigProvider(provider RuntimeConfigProvider) {
	if h == nil || provider == nil {
		return
	}
	h.runtimeConfigProvider = provider
	h.frontendOriginMemo = config.NewStrictGenerationMemo(func(v RuntimeConfigView) (*origin.Allowlist, error) {
		a, invalid := origin.NewAllowlistWithDefault(v.AllowedFrontendOrigins(), v.FrontendURL())
		for _, entry := range invalid {
			h.logger.Named(authLoggerName).Warn("Ignoring invalid "+entry.Key+" entry", zap.String(appLogger.KeyFrontendOrigin, entry.Value))
		}
		return a, nil
	}, func(err error) {
		h.logger.Named(authLoggerName).Error("active frontend origin configuration unreadable", zap.Error(err))
	})
}

func newRefreshedTokensCache(ttl time.Duration, capacity uint64) *ttlcache.Cache[string, string] {
	return ttlcache.New(
		ttlcache.WithTTL[string, string](ttl),
		ttlcache.WithCapacity[string, string](capacity),
		// Re-issue mappings are valid only for the fixed refresh grace window.
		// Reads from parallel requests must not turn that window into sliding TTL.
		ttlcache.WithDisableTouchOnHit[string, string](),
	)
}

// collectCacheMetrics executes this step in the auth backend flow.
// It runs until Stop closes stopCh.
func (h *AuthHandler[U]) collectCacheMetrics() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			// Retired cleaners are stopped during resize. The existing maintenance
			// loop expires replacement caches without spawning new goroutines.
			tokens, users := h.tokenCache(), h.userCache()
			tokens.DeleteExpired()
			users.DeleteExpired()
			tokenCacheSize := tokens.Len()
			userCacheSize := users.Len()

			if appLogger.DebugEnabled(h.logger) {
				logger := appLogger.For(context.Background(), h.logger, authLoggerName)
				h.cachePolicyMu.Lock()
				tokenCapacity, userCapacity := h.cacheTokenCapacity, h.cacheUserCapacity
				h.cachePolicyMu.Unlock()

				logger.Debug("Auth token cache stats",
					zap.Int(appLogger.KeySize, tokenCacheSize),
					zap.Uint64(appLogger.KeyCapacity, tokenCapacity),
					zap.Int64(appLogger.KeyHits, h.tokenCacheHits.Load()),
					zap.Int64(appLogger.KeyMisses, h.tokenCacheMisses.Load()),
					zap.Int64(appLogger.KeyEvictions, h.tokenCacheEvictions.Load()))

				logger.Debug("User cache stats",
					zap.Int(appLogger.KeySize, userCacheSize),
					zap.Uint64(appLogger.KeyCapacity, userCapacity),
					zap.Int64(appLogger.KeyHits, h.userCacheHits.Load()),
					zap.Int64(appLogger.KeyMisses, h.userCacheMisses.Load()),
					zap.Int64(appLogger.KeyEvictions, h.userCacheEvictions.Load()))
			}
		}
	}
}

// ValidateAuthToken executes this step in the auth backend flow.
func (h *AuthHandler[U]) ValidateAuthToken(authCode string) (bool, time.Time, error) {
	// Use the token service for validation
	claims, err := h.tokenService.ValidateSessionToken(authCode)
	if err != nil {
		// Check if it's an ExpiredError and extract the expiry time
		if expErr, ok := stdErrors.AsType[token.ExpiredError](err); ok {
			// Return false (invalid) but preserve the expiry time
			return false, expErr.ExpiresAt, nil
		}
		// For other errors (invalid format, etc.), return the error
		return false, time.Time{}, err
	}

	// Token is valid, return true with expiry time
	return true, claims.ExpiresAt, nil
}

// ValidateSessionToken executes this step in the auth backend flow.
func (h *AuthHandler[U]) ValidateSessionToken(authCode string) (*token.SessionTokenClaims, error) {
	claims, err := h.tokenService.ValidateSessionToken(authCode)
	if err != nil {
		if _, ok := stdErrors.AsType[token.ExpiredError](err); ok {
			return nil, errors.ErrTokenExpired
		}
		return nil, errors.ErrInvalidToken
	}

	return claims, nil
}

// GetUserFromAuthToken executes this step in the auth backend flow.
func (h *AuthHandler[U]) GetUserFromAuthToken(ctx context.Context, authCode string) (*U, error) {
	caches := h.authCaches()
	if h.sessionAuthMethodPolicy != nil {
		caches.tokens = nil
	} // session methods are persisted authority
	var tokenItem *ttlcache.Item[string, common_service_models.UserIdType]

	if caches.tokens != nil {
		// First check if the auth token is in the token-to-userID cache
		tokenItem = caches.tokens.Get(authCode)
	}
	if tokenItem != nil {
		// Increment token cache hit counter
		h.tokenCacheHits.Add(1)

		// Validate the auth token and remove from cache atomically if invalid
		userID, valid := h.validateAndCleanupCachedToken(ctx, authCode, tokenItem)
		if !valid {
			return nil, errors.ErrTokenExpired // Return zero value of T
		}

		var userItem *ttlcache.Item[common_service_models.UserIdType, U] // Store U values
		if caches.users != nil {
			// Now check if the user is in the userID-to-user cache
			userItem = caches.users.Get(userID)
		}
		if userItem != nil {
			// Increment user cache hit counter
			h.userCacheHits.Add(1)
			// userItem.Value() is of type U, allocate on heap and copy
			userPtr := new(U)
			*userPtr = userItem.Value()
			if (*userPtr).IsBot() {
				return nil, errors.ErrInvalidCredentials
			}
			if (*userPtr).IsBanned() {
				return nil, errors.ErrUserBanned
			}
			return userPtr, nil
		}

		// User not in cache, increment user cache miss counter
		h.userCacheMisses.Add(1)

		// Fetch user via callback using the userID
		userObject, err := h.userCallbacks.UserLookup(ctx, userID) // Returns U
		if err != nil {
			return nil, errors.ErrUserNotFound
		}

		// Store user value directly in the userID-to-user cache
		caches.store("", userID, nil, &userObject)

		if userObject.IsBot() {
			return nil, errors.ErrInvalidCredentials
		}
		if userObject.IsBanned() {
			return nil, errors.ErrUserBanned
		}

		// Allocate on heap and copy
		userPtr := new(U)
		*userPtr = userObject
		return userPtr, nil
	}

	// Token not in cache, increment token cache miss counter
	h.tokenCacheMisses.Add(1)

	// If not in cache, check the database for a valid AUTHENTICATION token.
	// A token pending revocation (future revoked_at, written by session refresh)
	// still validates until its revocation instant.
	authToken, err := h.persistenceHandler.GetValidSessionToken(ctx, authCode)
	if err != nil {
		// Map persistence errors to service errors
		if stdErrors.Is(err, persistence.ErrSessionTokenNotFound) {
			return nil, appLogger.WithErrorCause(errors.ErrInvalidToken, err) // Return zero value of T
		}
		if stdErrors.Is(err, persistence.ErrTokenExpired) {
			return nil, appLogger.WithErrorCause(errors.ErrTokenExpired, err) // Return zero value of T
		}
		return nil, appLogger.WithErrorCause(errors.ErrInvalidToken, err) // Return zero value of T
	}

	// Verify token type
	if authToken.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
		return nil, errors.ErrInvalidToken
	}

	if h.sessionAuthMethodPolicy != nil {
		if err := h.sessionAuthMethodPolicy(ctx, token.SessionAuthMethod{Provider: authToken.AuthMethod, OAuthProviderSlug: authToken.OAuthProviderSlug}); err != nil {
			return nil, err
		}
	}
	// Get the user ID
	userID := common_service_models.UserIdType(authToken.UserID)

	// Fetch user via callback
	userObject, err := h.userCallbacks.UserLookup(ctx, userID) // Returns U
	if err != nil {
		return nil, errors.ErrUserNotFound
	}

	// Store in both caches with appropriate TTLs
	caches.store(authCode, userID, authToken, &userObject)

	if userObject.IsBot() {
		return nil, errors.ErrInvalidCredentials
	}
	if userObject.IsBanned() {
		return nil, errors.ErrUserBanned
	}

	// Allocate on heap and copy
	userPtr := new(U)
	*userPtr = userObject
	return userPtr, nil
}

// CreateUserAuth issues a session or security token. Email-verification and
// account-invitation tokens bind the current persisted account address rather
// than a cached user projection. AUTH_PROVIDER_OAUTH is accepted only for
// session tokens and only with a provider slug.
func (h *AuthHandler[U]) CreateUserAuth(
	ctx context.Context,
	method token.SessionAuthMethod,
	tokenType proto_models.TokenType,
	obtainedAuthCode string,
	userId common_service_models.UserIdType,
) (string, error) {
	// An OAuth method names its provider by slug, which only session rows
	// carry: never mint an OAuth token without its slug or a slug without OAuth.
	isOAuth := method.Provider == proto_models.AuthProvider_AUTH_PROVIDER_OAUTH
	if isOAuth != (method.OAuthProviderSlug != "") || (isOAuth && tokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION) {
		return "", appLogger.InvariantError("oauth_token_method_carries_slug", errors.ErrInvalidRequest,
			zap.Stringer(appLogger.KeyAuthMethod, method.Provider),
			zap.String(appLogger.KeyProvider, method.OAuthProviderSlug),
			zap.Stringer(appLogger.KeyTokenType, tokenType),
			zap.String(logkeys.Expected, "AUTH_PROVIDER_OAUTH only for session tokens, always with a provider slug"),
			zap.String(logkeys.Actual, "OAuth method and slug disagree or name a non-session token"))
	}
	// Use token service to generate appropriate token type
	if tokenType == proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
		if h.sessionAuthMethodPolicy != nil {
			if err := h.sessionAuthMethodPolicy(ctx, method); err != nil {
				return "", err
			}
		}
		// Generate session token for authentication
		return h.tokenService.GenerateSessionToken(ctx, uint64(userId), method)
	} else {
		ip, _ := scontext.GetClientIP(ctx)
		userAgent, _ := scontext.GetUserAgent(ctx)
		// Generate security token for other purposes (verification, reset, etc.)
		params := token.SecurityTokenParams{
			UserID:     uint64(userId),
			TokenType:  tokenType,
			AuthMethod: method.Provider,
			ExpiresIn:  h.sessionLifetime(),
			ClientInfo: &token.ClientInfo{
				IPAddress: ip,
				UserAgent: userAgent,
			},
		}
		if tokenType == proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION || tokenType == proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION {
			user, err := h.persistenceHandler.GetUserByID(ctx, uint64(userId))
			if err != nil {
				return "", fmt.Errorf("load email-bound token account: %w", err)
			}
			if user == nil || common_persistence_models.NormalizeUserEmail(user.Email) == "" {
				return "", errors.ErrInvalidRequest
			}
			params.BoundEmail = common_persistence_models.NormalizeUserEmail(user.Email)
		}
		return h.tokenService.GenerateSecurityToken(ctx, params)
	}
}

// Stop executes this step in the auth backend flow.
func (h *AuthHandler[U]) Stop() {
	// End the cache-metrics goroutine; sync.Once keeps Stop idempotent and the
	// nil check keeps Stop safe on handlers built without NewAuthHandler.
	h.stopOnce.Do(func() {
		if h.stopCh != nil {
			close(h.stopCh)
		}
	})

	// Shutdown must not read a configuration provider that may already be closed.
	h.cachePolicyMu.Lock()
	tokens, users := h.authTokenToUserIDCache, h.userIDToUserCache
	h.cachePolicyMu.Unlock()
	if tokens != nil {
		tokens.Stop()
	}
	if users != nil {
		users.Stop()
	}
	if h.refreshedTokens != nil {
		h.refreshedTokens.Stop()
	}

	// Release the OAuth discovery cache.
	if h.oauthRuntime != nil {
		h.oauthRuntime.Close()
	}

	// Stop the email auth handler
	if h.emailAuthHandler != nil {
		h.emailAuthHandler.Stop()
	}
}

// GetUserIDFromAuthToken executes this step in the auth backend flow.
//
// SECURITY: this resolves the token to a user ID without loading the user, so
// it performs NO ban check. Callers making authorization decisions must use
// GetUserFromAuthToken, which rejects banned users.
func (h *AuthHandler[U]) GetUserIDFromAuthToken(ctx context.Context, authCode string) (common_service_models.UserIdType, error) {
	caches := h.authCaches()
	var tokenItem *ttlcache.Item[string, common_service_models.UserIdType]
	if caches.tokens != nil {
		// First check if the auth token is in the token-to-userID cache
		tokenItem = caches.tokens.Get(authCode)
	}
	if tokenItem != nil {
		// Increment token cache hit counter
		h.tokenCacheHits.Add(1)

		// Validate the auth token and remove from cache atomically if invalid
		userID, valid := h.validateAndCleanupCachedToken(ctx, authCode, tokenItem)
		if !valid {
			return 0, errors.ErrTokenExpired
		}

		// Return the validated user ID
		return userID, nil
	}

	// Token not in cache, increment token cache miss counter
	h.tokenCacheMisses.Add(1)

	// If not in cache, check the database for a valid AUTHENTICATION token.
	// See GetUserFromAuthToken for the pending-revocation semantics.
	authToken, err := h.persistenceHandler.GetValidSessionToken(ctx, authCode)
	if err != nil {
		// Map persistence errors to service errors
		if stdErrors.Is(err, persistence.ErrSessionTokenNotFound) {
			return 0, appLogger.WithErrorCause(errors.ErrInvalidToken, err)
		}
		if stdErrors.Is(err, persistence.ErrTokenExpired) {
			return 0, appLogger.WithErrorCause(errors.ErrTokenExpired, err)
		}
		return 0, appLogger.WithErrorCause(errors.ErrInvalidToken, err)
	}

	// Verify token type
	if authToken.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
		return 0, errors.ErrInvalidToken
	}

	// Get the user ID
	userID := common_service_models.UserIdType(authToken.UserID)

	// Store in the token-to-userID cache
	caches.store(authCode, userID, authToken, nil)

	return userID, nil
}

// GetUserByID executes this step in the auth backend flow.
func (h *AuthHandler[U]) GetUserByID(ctx context.Context, userID common_service_models.UserIdType) (*U, error) {
	caches := h.authCaches()
	var userItem *ttlcache.Item[common_service_models.UserIdType, U] // Store U values
	if caches.users != nil {
		// Check if the user is in the userID-to-user cache
		userItem = caches.users.Get(userID)
	}
	if userItem != nil {
		// Increment user cache hit counter
		h.userCacheHits.Add(1)
		// userItem.Value() is of type U, allocate on heap and copy
		userPtr := new(U)
		*userPtr = userItem.Value()
		return userPtr, nil
	}

	// User not in cache, increment user cache miss counter
	h.userCacheMisses.Add(1)

	// Fetch user via callback using the userID
	userObject, err := h.userCallbacks.UserLookup(ctx, userID) // Returns U
	if err != nil {
		return nil, errors.ErrUserNotFound
	}

	// Store value directly in the userID-to-user cache
	caches.store("", userID, nil, &userObject)

	// Allocate on heap and copy
	userPtr := new(U)
	*userPtr = userObject
	return userPtr, nil
}

// LoginUserByEmail executes this step in the auth backend flow.
func (h *AuthHandler[U]) LoginUserByEmail(ctx context.Context, email, password string) (*U, string, error) {
	// Get the user and auth token from the email auth handler
	user, authToken, err := h.emailAuthHandler.LoginWithEmail(ctx, email, password) // Returns user, token, error
	if err != nil {
		return nil, "", appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Bool(appLogger.KeyOTPProvided, false))
	}

	// Email auth handler already generated the token, so just return the user and token
	return user, authToken, nil
}

// LoginUserByEmailWithOTP executes this step in the auth backend flow.
func (h *AuthHandler[U]) LoginUserByEmailWithOTP(ctx context.Context, email, password, otpCode string) (*U, string, bool, error) {
	// Use the email auth handler's new OTP-aware login method
	user, authToken, otpRequired, err := h.emailAuthHandler.LoginWithEmailAndOTP(ctx, email, password, otpCode)
	if err != nil {
		return nil, "", false, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyEmail, email),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""))
	}

	// If OTP is required, return early with the flag
	if otpRequired {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, authLoggerName).Debug("OTP required for login", zap.String(appLogger.KeyEmail, email))
		}
		return nil, "", true, nil
	}

	// Successful login - email auth handler already generated the token
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).Debug("User logged in via email with OTP",
			zap.String(appLogger.KeyEmail, email),
			zap.Bool(appLogger.KeyOTPUsed, otpCode != ""))
	}
	return user, authToken, false, nil
}

// resolveOAuthSignupUsername executes this step in the auth backend flow.
func (h *AuthHandler[U]) resolveOAuthSignupUsername(ctx context.Context, email string) (string, error) {
	return h.resolveOAuthSignupUsernameWithConfig(ctx, email, h.runtimeConfigProvider())
}

func (h *AuthHandler[U]) resolveOAuthSignupUsernameWithConfig(ctx context.Context, email string, runtime RuntimeConfigView) (string, error) {
	mode := runtime.OAuthSignupMode()
	switch mode {
	case config.OAuthSignupModeLegacy:
		if runtime.RequireUsernamesOnSignup() {
			return "", errors.ErrUsernameRequired
		}
		return email, nil
	case config.OAuthSignupModeDisabled:
		return "", errors.ErrOAuthSignupDisabled
	case config.OAuthSignupModePrefixed:
		return h.generatePrefixedUsername(ctx, runtime)
	case config.OAuthSignupModeEmailLocalPart:
		return h.generateEmailLocalUsername(ctx, email, runtime)
	default:
		return "", appLogger.InvariantError("oauth_signup_mode_is_supported", errors.ErrUserCreationFailed,
			zap.Stringer(appLogger.KeyMode, mode),
			zap.String(logkeys.Expected, "configured OAuth signup mode is recognized"),
			zap.String(logkeys.Actual, "unsupported OAuth signup mode"))
	}
}

// generatePrefixedUsername returns an unused temporary username of the form
// prefix+UUID. Configuration requires the prefix to be invalid under
// user-supplied username rules; that is how onboarding recognizes accounts that
// still need a username, so the candidate deliberately bypasses username
// normalization and length limits, which would strip or truncate the marker.
func (h *AuthHandler[U]) generatePrefixedUsername(ctx context.Context, runtime RuntimeConfigView) (string, error) {
	prefix := strings.TrimSpace(runtime.OAuthDefaultUsernamePrefix())
	if prefix == "" {
		return "", appLogger.InvariantError("prefixed_oauth_signup_requires_prefix", errors.ErrUserCreationFailed,
			zap.String(logkeys.Expected, "non-empty OAuth username prefix"),
			zap.String(logkeys.Actual, "empty OAuth username prefix"))
	}

	for range oauthUsernameMaxAttempts {
		candidate := prefix + uuid.New().String()
		available, err := h.isUsernameAvailable(ctx, candidate)
		if err != nil {
			return "", err
		}
		if available {
			return candidate, nil
		}
	}

	return "", errors.ErrUserCreationFailed
}

// generateEmailLocalUsername executes this step in the auth backend flow.
func (h *AuthHandler[U]) generateEmailLocalUsername(ctx context.Context, email string, runtime RuntimeConfigView) (string, error) {
	local := email
	if before, _, ok := strings.Cut(email, "@"); ok {
		local = before
	}

	base := normalizeEmailLocalPart(local)
	if base == "" {
		base = "user"
	}

	return h.findAvailableUsername(ctx, base, runtime)
}

// findAvailableUsername executes this step in the auth backend flow.
func (h *AuthHandler[U]) findAvailableUsername(ctx context.Context, base string, runtime RuntimeConfigView) (string, error) {
	minLength := runtime.SanitizerUsernameMinLength()
	maxLength := runtime.SanitizerUsernameMaxLength()
	if minLength <= 0 {
		minLength = 3
	}
	if maxLength <= 0 {
		maxLength = 50
	}

	base = trimUsernameBoundaries(base)
	if base == "" {
		base = "user"
	}
	base = enforceUsernameLength(base, minLength, maxLength)
	if base == "" {
		base = "user"
		base = enforceUsernameLength(base, minLength, maxLength)
	}

	available, err := h.isUsernameAvailable(ctx, base)
	if err != nil {
		return "", err
	}
	if available {
		return base, nil
	}

	for i := 1; i <= oauthUsernameMaxAttempts; i++ {
		suffix := strconv.Itoa(i)
		candidateBase := base
		if maxLength <= len(suffix) {
			continue
		}
		if len(candidateBase)+len(suffix) > maxLength {
			candidateBase = candidateBase[:maxLength-len(suffix)]
			candidateBase = trimUsernameBoundaries(candidateBase)
		}
		if candidateBase == "" {
			candidateBase = "user"
		}

		candidate := candidateBase + suffix
		if len(candidate) < minLength {
			candidate += strings.Repeat("0", minLength-len(candidate))
		}
		if len(candidate) > maxLength {
			candidate = candidate[:maxLength]
		}
		candidate = trimUsernameBoundaries(candidate)
		if candidate == "" {
			continue
		}

		available, err := h.isUsernameAvailable(ctx, candidate)
		if err != nil {
			return "", err
		}
		if available {
			return candidate, nil
		}
	}

	return "", errors.ErrUserCreationFailed
}

// isUsernameAvailable executes this step in the auth backend flow.
func (h *AuthHandler[U]) isUsernameAvailable(ctx context.Context, username string) (bool, error) {
	exists, err := h.persistenceHandler.CheckUsernameExists(ctx, username)
	if err != nil {
		return false, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyUsername, username),
			zap.String(logkeys.Stage, "check_oauth_username_availability"))
	}
	return !exists, nil
}

// normalizeEmailLocalPart executes this step in the auth backend flow.
func normalizeEmailLocalPart(local string) string {
	normalized := strings.ToLower(strings.TrimSpace(local))
	if normalized == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(normalized))
	lastWasSeparator := false

	for i := 0; i < len(normalized); i++ {
		char := normalized[i]
		if isUsernameAllowedChar(char) {
			builder.WriteByte(char)
			lastWasSeparator = false
			continue
		}
		if !lastWasSeparator {
			builder.WriteByte('_')
			lastWasSeparator = true
		}
	}

	return strings.Trim(builder.String(), "_-")
}

// enforceUsernameLength executes this step in the auth backend flow.
func enforceUsernameLength(value string, minLength, maxLength int) string {
	if maxLength > 0 && len(value) > maxLength {
		value = value[:maxLength]
	}

	value = trimUsernameBoundaries(value)
	if value == "" {
		return ""
	}

	if minLength > 0 && len(value) < minLength {
		value += strings.Repeat("0", minLength-len(value))
	}

	if maxLength > 0 && len(value) > maxLength {
		value = value[:maxLength]
	}

	return trimUsernameBoundaries(value)
}

// trimUsernameBoundaries executes this step in the auth backend flow.
func trimUsernameBoundaries(value string) string {
	if value == "" {
		return ""
	}

	start := 0
	for start < len(value) && !isUsernameAlphaNumeric(value[start]) {
		start++
	}
	end := len(value)
	for end > start && !isUsernameAlphaNumeric(value[end-1]) {
		end--
	}
	if start >= end {
		return ""
	}
	return value[start:end]
}

// isUsernameAllowedChar executes this step in the auth backend flow.
func isUsernameAllowedChar(char byte) bool {
	return isUsernameAlphaNumeric(char) || char == '_' || char == '-'
}

// isUsernameAlphaNumeric executes this step in the auth backend flow.
func isUsernameAlphaNumeric(char byte) bool {
	return (char >= 'a' && char <= 'z') ||
		(char >= 'A' && char <= 'Z') ||
		(char >= '0' && char <= '9')
}

// ListActiveSessions returns active authentication sessions for the user, newest first.
func (h *AuthHandler[U]) ListActiveSessions(ctx context.Context, userID common_service_models.UserIdType, currentToken string, pagination *proto_models.PaginationRequest) ([]*ActiveSession, *proto_models.PaginationResponse, error) {
	tokens, paginationResponse, err := h.persistenceHandler.ListActiveSessionTokensByUserID(ctx, uint64(userID), pagination)
	if err != nil {
		return nil, nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrDatabaseError, err),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "list_active_sessions"))
	}

	sessions := make([]*ActiveSession, 0, len(tokens))
	for _, token := range tokens {
		sessions = append(sessions, &ActiveSession{
			SessionID:         strconv.FormatUint(token.ID, 10),
			Current:           token.Token == currentToken,
			AuthMethod:        token.AuthMethod,
			OAuthProviderSlug: token.OAuthProviderSlug,
			IPAddress:         token.IPAddress,
			UserAgent:         token.UserAgent,
			DeviceID:          token.DeviceID,
			IssuedAt:          token.IssuedAt,
			ExpiresAt:         token.ExpiresAt,
			LastUsedAt:        token.UsedAt,
		})
	}

	return sessions, paginationResponse, nil
}

// RefreshSessionToken executes this step in the auth backend flow.
//
// It mints a replacement session token for a DB-valid old token, revokes the old
// token, and returns the new one. Concurrent refreshes of the same token collapse
// into a single mint, and a token already refreshed within the grace window
// re-receives its existing replacement instead of minting again.
func (h *AuthHandler[U]) RefreshSessionToken(ctx context.Context, oldToken string) (string, error) {
	// Fast path: this token was already refreshed within the grace window
	// (parallel tab, or a lost Set-Cookie being healed) — re-issue the same
	// replacement token.
	if h.refreshedTokens != nil {
		if item := h.refreshedTokens.Get(oldToken); item != nil {
			return item.Value(), nil
		}
	}

	// Stateless validation up front for the claims (AuthenticatedAt carries
	// over); the session row supplies the authentication method.
	claims, err := h.ValidateSessionToken(oldToken)
	if err != nil {
		return "", err
	}

	result, err, _ := h.refreshGroup.Do(oldToken, func() (any, error) {
		// Double-check inside the flight: a concurrent caller may have just
		// completed the refresh.
		if h.refreshedTokens != nil {
			if item := h.refreshedTokens.Get(oldToken); item != nil {
				return item.Value(), nil
			}
		}

		// Strict DB verification: a token with any revocation on record — even a
		// pending (future-dated) one — must never be refreshed, so logout wins
		// and refresh chains from switchover-window tokens are impossible.
		rec, err := h.persistenceHandler.GetValidSessionToken(ctx, oldToken)
		if err != nil {
			switch {
			case stdErrors.Is(err, persistence.ErrTokenRevoked):
				return nil, appLogger.WithErrorCause(errors.ErrTokenRevoked, err)
			case stdErrors.Is(err, persistence.ErrTokenExpired):
				return nil, appLogger.WithErrorCause(errors.ErrTokenExpired, err)
			default:
				return nil, appLogger.WithErrorCause(errors.ErrInvalidToken, err)
			}
		}
		if rec.RevokedAt != nil {
			return nil, errors.ErrTokenRevoked
		}
		if rec.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION {
			return nil, errors.ErrInvalidToken
		}
		if h.sessionAuthMethodPolicy != nil {
			if err := h.sessionAuthMethodPolicy(ctx, token.SessionAuthMethod{Provider: rec.AuthMethod, OAuthProviderSlug: rec.OAuthProviderSlug}); err != nil {
				return nil, err
			}
		}

		newToken, err := h.tokenService.GenerateSessionTokenWithAuthTime(
			ctx,
			rec.UserID,
			token.SessionAuthMethod{Provider: rec.AuthMethod, OAuthProviderSlug: rec.OAuthProviderSlug},
			claims.AuthenticatedAt,
		)
		if err != nil {
			return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrInternalError, err),
				zap.Uint64(appLogger.KeyUserID, rec.UserID),
				zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
				zap.String(logkeys.Stage, "mint_refresh_replacement"))
		}

		// Publish the re-issue mapping before scheduling the old token's
		// revocation. An immediate logout force-revokes the old token and then
		// walks the re-issue mappings, so publishing first guarantees that walk
		// reaches the replacement instead of missing a mapping a later Set would
		// have added (which would leave the replacement alive past the logout).
		if h.refreshedTokens != nil {
			h.refreshedTokens.Set(oldToken, newToken, ttlcache.DefaultTTL)
		}

		// Revoke the old token effective a grace period from now: in-flight
		// requests and parallel tabs still carrying it survive the cookie
		// switchover, while explicit logout (RevokeSessionToken) stays instant.
		revokeAt := time.Now().UTC().Add(h.config.sessionRefreshGrace)
		if err := h.persistenceHandler.RevokeSessionTokenAt(ctx, oldToken, revokeAt); err != nil {
			// Never leave two long-lived tokens from one refresh: drop the mapping
			// we just published and best-effort revoke the token we just minted
			// before failing, so the fast path cannot hand out the doomed token.
			if h.refreshedTokens != nil {
				h.refreshedTokens.Delete(oldToken)
			}
			if revokeErr := h.persistenceHandler.RevokeSessionToken(ctx, newToken); revokeErr != nil {
				appLogger.For(ctx, h.logger, authLoggerName).Error("Failed to revoke replacement token after refresh failure",
					zap.Error(revokeErr),
					zap.Uint64(appLogger.KeyUserID, rec.UserID),
					zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
					zap.String(logkeys.Stage, "revoke_failed_refresh_replacement"),
					zap.String(logkeys.Fallback, "continue failing refresh with replacement revocation unconfirmed"))
			}
			return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrDatabaseError, err),
				zap.Uint64(appLogger.KeyUserID, rec.UserID),
				zap.String(logkeys.Stage, "schedule_old_token_revocation"))
		}
		// Sockets stay bound to the token they opened with, so close them when its
		// revocation takes effect; clients reconnect with the new cookie.
		if h.realtimeSessions != nil {
			time.AfterFunc(time.Until(revokeAt), func() { h.realtimeSessions.CloseSessions(oldToken) })
		}

		// Re-verify against the DB after scheduling the revocation: a logout/ban
		// landing between the strict verification above and here force-revokes the
		// old token. Immediate revocation paths always update the DB before walking
		// the re-issue mappings, and the mapping was published above, so either
		// this re-read observes the forced revocation (and we clean up here) or the
		// logout's chain walk revokes the replacement through the mapping. A
		// future-dated revocation surviving the re-read can only be the one
		// scheduled above, since immediate paths pull revoked_at to now and
		// RevokeSessionTokenAt never overrides an existing revocation. The old
		// token's auth-cache entry is written only after this check confirms the
		// row is clean, so a forced revocation is never masked by a stale hit.
		if h.config.sessionRefreshGrace > 0 {
			admission := h.cacheAdmission()
			verifyRec, verifyErr := h.persistenceHandler.GetValidSessionToken(ctx, oldToken)
			if stdErrors.Is(verifyErr, persistence.ErrDatabaseError) {
				// A read failure is not evidence of a revocation; retry once
				// before deciding the fate of a durably completed refresh.
				verifyRec, verifyErr = h.persistenceHandler.GetValidSessionToken(ctx, oldToken)
			}
			switch {
			case stdErrors.Is(verifyErr, persistence.ErrDatabaseError):
				// Transient DB trouble on a read that exists only to detect races:
				// keep the completed refresh (a concurrent logout still revokes the
				// replacement through the mapping published above). Drop only the
				// old token's auth-cache entry — never re-add it — so a forced
				// revocation that already pulled revoked_at to now cannot be masked
				// by a grace-capped cache hit; the next use re-checks the DB. The
				// re-issue mapping is deliberately left in place for the logout walk.
				h.evictAuthTokenCacheOnly(oldToken)
				appLogger.For(ctx, h.logger, authLoggerName).Warn("Post-refresh verification read failed; keeping completed refresh",
					zap.Error(verifyErr),
					zap.Uint64(appLogger.KeyUserID, rec.UserID),
					zap.String(logkeys.Stage, "verify_refresh_revocation_race"),
					zap.Int(appLogger.KeyAttempt, 2),
					zap.String(logkeys.Fallback, "keep completed refresh and evict old token cache entry"))
			case verifyErr != nil || verifyRec == nil || verifyRec.RevokedAt == nil:
				h.InvalidateAuthToken(ctx, oldToken)
				// The concurrent revocation may never have seen the replacement:
				// best-effort revoke it so the refresh leaves nothing alive. A
				// not-found result means it was already force-revoked (e.g. the
				// logout followed the re-issue mapping), which is the goal state.
				if revokeErr := h.persistenceHandler.RevokeSessionToken(ctx, newToken); revokeErr != nil &&
					!stdErrors.Is(revokeErr, persistence.ErrSessionTokenNotFound) {
					appLogger.For(ctx, h.logger, authLoggerName).Error("Failed to revoke replacement token after refresh raced a revocation",
						zap.Error(revokeErr),
						zap.Uint64(appLogger.KeyUserID, rec.UserID),
						zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
						zap.String(logkeys.Stage, "revoke_raced_refresh_replacement"),
						zap.String(logkeys.Fallback, "return revoked outcome with replacement revocation unconfirmed"))
				}
				if appLogger.DebugEnabled(h.logger) {
					appLogger.For(ctx, h.logger, authLoggerName).Debug("Session refresh aborted: token was revoked mid-refresh",
						zap.Uint64(appLogger.KeyUserID, rec.UserID))
				}
				return nil, errors.ErrTokenRevoked
			default:
				// Only our own scheduled (future-dated) revocation remains: cap the
				// old token's cache entry at that revocation instead of its original
				// TTL, so parallel tabs still using it survive the switch.
				if remaining := time.Until(revokeAt); remaining > 0 {
					h.cacheToken(oldToken, common_service_models.UserIdType(rec.UserID), remaining, admission)
				} else {
					h.evictAuthTokenCacheOnly(oldToken)
				}
			}
		} else {
			h.InvalidateAuthToken(ctx, oldToken)
		}

		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, authLoggerName).Debug("Session token refreshed",
				zap.Uint64(appLogger.KeyUserID, rec.UserID))
		}

		return newToken, nil
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// RevokeAuthToken executes this step in the auth backend flow.
func (h *AuthHandler[U]) RevokeAuthToken(ctx context.Context, authToken string) error {
	// First revoke in the database
	err := h.persistenceHandler.RevokeSessionToken(ctx, authToken)
	if err != nil && !stdErrors.Is(err, persistence.ErrSessionTokenNotFound) {
		return err
	}
	// An already-revoked head is expected on a retry after replacement-chain
	// revocation failed. Continue into the retained mappings so the retry can
	// finish revoking the live tail.

	// A refresh that completed concurrently already minted a replacement keyed
	// under this token: revoke the replacement chain in the DB before dropping
	// the mappings, so a logout landing after the refresh's own race check still
	// ends the session the refresh handed out.
	if err := h.revokeRefreshedReplacements(ctx, authToken); err != nil {
		// Keep the re-issue mapping so a retried revocation can still reach the
		// replacement, but never leave the revoked token authenticating from cache.
		h.invalidateAuthTokenPreservingRefreshMapping(ctx, authToken)
		return err
	}

	// Clear from cache after the revocation commits. The replacement-chain
	// mappings were already dropped inline by revokeRefreshedReplacements above,
	// so this only evicts the head token's cache entry.
	h.invalidateAuthTokensAfterCommit(ctx, []string{authToken})

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).Debug("Auth token revoked successfully")
	}
	return nil
}

// maxRefreshChainRevocations bounds the replacement-chain walk in
// revokeRefreshedReplacements. Chains longer than one link require multiple
// refreshes inside a single grace window, so this is a defensive limit rather
// than an expected depth.
const maxRefreshChainRevocations = 8

// revokeRefreshedReplacements force-revokes every replacement token the refresh
// path minted for authToken (following old→new re-issue mappings across chained
// refreshes) and drops each replacement from the caches. Mappings are deleted
// only after the full chain's DB revocation succeeds, so a mid-chain failure
// leaves the tail reachable from the original token for a retry.
func (h *AuthHandler[U]) revokeRefreshedReplacements(ctx context.Context, authToken string) error {
	if h.refreshedTokens == nil {
		return nil
	}
	current := authToken
	traversed := make([]string, 0, maxRefreshChainRevocations)
	for range maxRefreshChainRevocations {
		item := h.refreshedTokens.Get(current)
		if item == nil {
			for _, token := range traversed {
				h.refreshedTokens.Delete(token)
			}
			return nil
		}
		replacement := item.Value()
		// Not-found means the replacement is already revoked (or was cleaned
		// up), which is exactly the state this walk exists to reach.
		if err := h.persistenceHandler.RevokeSessionToken(ctx, replacement); err != nil &&
			!stdErrors.Is(err, persistence.ErrSessionTokenNotFound) {
			return appLogger.WithErrorFields(err,
				zap.String(logkeys.Stage, "revoke_refresh_replacement"),
				zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION))
		}
		h.invalidateAuthTokenPreservingRefreshMapping(ctx, replacement)
		traversed = append(traversed, current)
		current = replacement
	}
	// The bound is exhausted. If a further mapping still hangs off current, a live
	// replacement remains unrevoked: leave the mappings in place (as a mid-chain
	// failure does) so a retry keeps a path to the tail, and report the failure so
	// logout is never claimed clean while a replacement may still be valid.
	if item := h.refreshedTokens.Get(current); item != nil {
		return appLogger.InvariantError("refresh_replacement_chain_within_bound", errors.ErrRefreshChainTooLong,
			zap.String(logkeys.Stage, "revoke_refresh_replacement_chain"),
			zap.Int(logkeys.Limit, maxRefreshChainRevocations),
			zap.String(logkeys.Expected, "replacement chain ends within configured bound"),
			zap.String(logkeys.Actual, "replacement mapping remains after bound"))
	}
	for _, token := range traversed {
		h.refreshedTokens.Delete(token)
	}
	return nil
}

// RevokeAllUserTokens executes this step in the auth backend flow.
func (h *AuthHandler[U]) RevokeAllUserTokens(ctx context.Context, userID common_service_models.UserIdType) error {
	// Revoke all tokens in the database
	revokedTokens, err := h.persistenceHandler.RevokeAllSessionTokensForUserAndReturnTokens(ctx, uint64(userID))
	if err != nil {
		return err
	}

	h.invalidateAuthTokensAfterCommit(ctx, revokedTokens)

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).Debug("All user tokens revoked successfully",
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	return nil
}

// RevokeSession revokes one active session owned by a user.
func (h *AuthHandler[U]) RevokeSession(ctx context.Context, userID common_service_models.UserIdType, sessionID string, currentToken string) (uint32, error) {
	parsedSessionID, err := strconv.ParseUint(strings.TrimSpace(sessionID), 10, 64)
	if err != nil || parsedSessionID == 0 {
		return 0, errors.ErrInvalidRequest
	}

	activeTokens, err := h.persistenceHandler.GetActiveSessionTokensByUserID(ctx, uint64(userID))
	if err != nil {
		return 0, appLogger.WithErrorFields(stdErrors.Join(errors.ErrDatabaseError, err),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(logkeys.Stage, "load_sessions_for_revocation"))
	}

	found := false
	for _, token := range activeTokens {
		if token.ID != parsedSessionID {
			continue
		}
		found = true
		if token.Token == currentToken {
			return 0, errors.ErrCannotRevokeCurrentSession
		}
		break
	}
	if !found {
		return 0, errors.ErrSessionNotFound
	}

	revokedToken, err := h.persistenceHandler.RevokeSessionTokenByIDForUser(ctx, uint64(userID), parsedSessionID)
	if err != nil {
		if stdErrors.Is(err, persistence.ErrSessionTokenNotFound) {
			return 0, errors.ErrSessionNotFound
		}
		return 0, errors.ErrDatabaseError
	}

	h.invalidateAuthTokensAfterCommit(ctx, []string{revokedToken})
	return 1, nil
}

// RevokeOtherSessions revokes every active session except the current bearer token.
func (h *AuthHandler[U]) RevokeOtherSessions(ctx context.Context, userID common_service_models.UserIdType, currentToken string) (uint32, error) {
	revokedTokens, err := h.persistenceHandler.RevokeOtherSessionTokensForUser(ctx, uint64(userID), currentToken)
	if err != nil {
		return 0, errors.ErrDatabaseError
	}

	h.invalidateAuthTokensAfterCommit(ctx, revokedTokens)
	return uint32(len(revokedTokens)), nil
}

// InvalidateAuthToken is the public entry point for complete auth-token
// invalidation after a token becomes unusable.
func (h *AuthHandler[U]) InvalidateAuthToken(ctx context.Context, authToken string) {
	h.invalidateAuthToken(ctx, authToken, false)
}

// invalidateAuthTokenPreservingRefreshMapping invalidates a token while
// retaining its outgoing refresh mapping so a retry can still reach and revoke
// the replacement chain.
func (h *AuthHandler[U]) invalidateAuthTokenPreservingRefreshMapping(ctx context.Context, authToken string) {
	h.invalidateAuthToken(ctx, authToken, true)
}

// evictAuthTokenCacheOnly removes a token from the authentication fast path
// while retaining its refresh mapping for concurrent logout. Post-refresh
// verification uses it when a read fails or the grace window has elapsed.
func (h *AuthHandler[U]) evictAuthTokenCacheOnly(authToken string) {
	h.bumpCacheGeneration()
	h.tokenCache().Delete(authToken)
}

// invalidateAuthToken is the single extension point for auth-token
// invalidation side effects. preserveRefreshMapping is reserved for refresh
// and logout race handling, where the token must stop authenticating locally
// while its old-to-new mapping remains reachable for a retry or chain walk.
//
// MULTI-INSTANCE: authentication caches are process-local because the current
// architecture supports only a single service instance. Propagating this
// invalidation to every replica is one of many process-local assumptions that
// must be updated when multi-instance support is introduced; changing this
// method alone will not make the broader architecture multi-instance safe.
func (h *AuthHandler[U]) invalidateAuthToken(
	ctx context.Context,
	authToken string,
	preserveRefreshMapping bool,
) {
	h.bumpCacheGeneration()
	h.tokenCache().Delete(authToken)
	if h.realtimeSessions != nil {
		h.realtimeSessions.CloseSessions(authToken)
	}
	if !preserveRefreshMapping && h.refreshedTokens != nil {
		h.refreshedTokens.Delete(authToken)
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).Debug("Auth token invalidated",
			zap.Bool(appLogger.KeyRefreshMappingPreserved, preserveRefreshMapping))
	}
}

func (h *AuthHandler[U]) invalidateAuthTokens(ctx context.Context, authTokens []string) {
	for _, authToken := range authTokens {
		h.InvalidateAuthToken(ctx, authToken)
	}
}

// invalidateAuthTokensAfterCommit evicts revoked tokens from the auth caches
// once the request transaction commits, falling back to an immediate eviction
// when ctx carries no post-commit-capable transaction (no transaction, or a
// transaction that cannot register hooks). Deferring past commit is what stops
// a concurrent request that read the pre-revocation row from repopulating the
// cache with a token this call just revoked — which would otherwise keep
// authenticating from GetUserFromAuthToken's cache-hit path until its TTL. All
// of the explicit revocation entry points (logout, logout-everywhere, revoke
// one/other sessions) route their terminal eviction through here. The
// session-refresh path is deliberately excluded: it evicts synchronously as
// part of its own grace-window cache-cap ordering, not through the request
// transaction.
func (h *AuthHandler[U]) invalidateAuthTokensAfterCommit(ctx context.Context, authTokens []string) {
	if len(authTokens) == 0 {
		return
	}
	if persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		h.invalidateAuthTokens(hookCtx, authTokens)
		return nil
	}) {
		return
	}
	h.invalidateAuthTokens(ctx, authTokens)
}

// SetRealtimeSessionCloser wires the realtime transport at construction so every invalidated
// session also loses its sockets.
func (h *AuthHandler[U]) SetRealtimeSessionCloser(closer ports.RealtimeSessionCloser) {
	h.realtimeSessions = closer
}

// InvalidateUserCache executes this step in the auth backend flow.
func (h *AuthHandler[U]) InvalidateUserCache(userID common_service_models.UserIdType) {
	// Remove from user cache
	h.bumpCacheGeneration()
	h.userCache().Delete(userID)
}

// requireTransaction executes this step in the auth backend flow.
func (h *AuthHandler[U]) requireTransaction(ctx context.Context, operation string) error {
	tx, success := scontext.GetTransaction(ctx)
	if !success || tx == nil {
		return appLogger.InvariantError("auth_operation_requires_transaction", errors.ErrTransactionNotFound,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "resolve_transaction"),
			zap.String(logkeys.Expected, "transaction in request context"),
			zap.String(logkeys.Actual, "transaction absent"))
	}
	return nil
}

// validateAndCleanupCachedToken executes this step in the auth backend flow.
//
// Parameters:
//   - ctx: context carrying request cancellation and tracing.
//   - authCode: string input consumed by validateAndCleanupCachedToken.
//   - tokenItem: *ttlcache.Item[string, common_service_models.UserIdType] input consumed by validateAndCleanupCachedToken.
func (h *AuthHandler[U]) validateAndCleanupCachedToken(
	ctx context.Context,
	authCode string,
	tokenItem *ttlcache.Item[string, common_service_models.UserIdType],
) (common_service_models.UserIdType, bool) {
	// Validate the auth token
	valid, expiry, err := h.ValidateAuthToken(authCode)
	if err != nil {
		// Invalid token format - remove from cache immediately
		h.InvalidateAuthToken(ctx, authCode)
		return 0, false
	}

	if !valid || time.Now().UTC().After(expiry) {
		// Expired or invalid token - remove from cache immediately
		h.InvalidateAuthToken(ctx, authCode)
		return 0, false
	}

	// Token is valid, return the user ID
	// TODO(multi-instance-v2): other instances' revocations are not observed here.
	return tokenItem.Value(), true
}

// GetUserAuthMethods executes this step in the auth backend flow.
func (h *AuthHandler[U]) GetUserAuthMethods(
	ctx context.Context,
	userID uint64,
) (*common_service_models.UserAuthMethods, error) {
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, userID),
		).Debug("GetUserAuthMethods called")
	}

	// Get raw data from persistence layer
	persistenceMethods, err := h.persistenceHandler.GetUserAuthMethods(ctx, userID)
	if err != nil {
		return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrInternalError, err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "load_user_auth_methods"))
	}

	// Convert persistence model to service model using the converter
	serviceMethods := h.converter.PersistenceUserAuthMethodsToService(persistenceMethods)

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).With(
			zap.Uint64(appLogger.KeyUserID, userID),
		).Debug("retrieved user auth methods",
			zap.Bool(appLogger.KeyHasPassword, serviceMethods.HasPassword),
			zap.Int(appLogger.KeyOAuthProviderCount, len(serviceMethods.OAuthProviders)),
			zap.Int(appLogger.KeyTotalAuthMethods, serviceMethods.CountAuthMethods()),
			zap.Bool(appLogger.KeyHasAuthMethod, serviceMethods.HasAuthMethod()),
		)
	}

	return serviceMethods, nil
}

// RequireRecentAuthentication executes this step in the auth backend flow.
func (h *AuthHandler[U]) RequireRecentAuthentication(
	ctx context.Context,
	claims *token.SessionTokenClaims,
	maxAge time.Duration,
) error {
	if claims == nil {
		return errors.ErrInvalidToken
	}

	sessionAge := time.Since(claims.AuthenticatedAt)
	if sessionAge > maxAge {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, authLoggerName).Debug("session too old for sensitive operation",
				appLogger.DurationSeconds(appLogger.KeySessionAgeSeconds, sessionAge),
				appLogger.DurationSeconds(appLogger.KeyMaxAgeSeconds, maxAge),
			)
		}
		return errors.ErrReauthenticationRequired
	}

	return nil
}

func (h *AuthHandler[U]) currentFrontendAllowlist() *origin.Allowlist {
	a, _ := h.frontendOriginMemo.Get(h.runtimeConfigProvider())
	return a
}
