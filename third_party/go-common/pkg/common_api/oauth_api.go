package common_api

import (
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	pathOAuthPrefix    = "/auth/oauth"
	pathOAuthProviders = pathOAuthPrefix + "/providers"
	pathOAuthState     = pathOAuthPrefix + "/state"
	pathOAuthCallback  = pathOAuthPrefix + "/callback"
	pathOAuthFormPost  = pathOAuthPrefix + "/form-post"
)

// addOAuthRoutes registers the provider list, state issuance, the form_post
// relay, and the callback.
func (h *CommonAPIHandler[U]) addOAuthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.ListOAuthProvidersResponse, *proto_models.ListOAuthProvidersResponse]{
		Path:       pathOAuthProviders,
		Methods:    []router.HttpMethod{http.MethodGet, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.ListOAuthProvidersResponse, *proto_models.ListOAuthProvidersResponse](),
		Handler:    h.ListOAuthProviders,
		SourceType: router.Empty,
	})
	group.Route(router.RouteConfig[*proto_models.OAuthStateRequest, *proto_models.OAuthStateResponse]{
		Path:        pathOAuthState,
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.OAuthStateRequest, *proto_models.OAuthStateResponse](),
		Handler:     h.CreateOAuthState,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeOAuthStateRequest,
		Middlewares: []common.Middleware{h.sessionCookieMiddleware},
	})
	group.Route(router.RouteConfigBase{
		Path:      pathOAuthFormPost,
		Methods:   []router.HttpMethod{http.MethodPost},
		AuthLevel: new(router.NoAuth),
		Handler:   h.OAuthFormPostRelay,
		Overrides: common.RouteOverrides{
			// The hot per-request cap is applied in the handler; this is its
			// configuration ceiling, so the route never inherits the global
			// body limit sized for uploads.
			MaxBodySize: config.MaxOAuthFormPostMaxBodyBytes,
		},
	})
	group.Route(router.RouteConfig[*proto_models.OAuthCodeRequest, *proto_models.OAuthTokenResponse]{
		Path:        pathOAuthCallback,
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.OAuthCodeRequest, *proto_models.OAuthTokenResponse](),
		Handler:     h.OAuthCallback,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeOAuthCodeRequest,
		Middlewares: []common.Middleware{transactionMiddleware, h.sessionCookieMiddleware},
	})
}

// ListOAuthProviders lists the providers usable right now, for sign-in buttons.
func (h *CommonAPIHandler[U]) ListOAuthProviders(r *http.Request, _ *proto_models.ListOAuthProvidersResponse) (*proto_models.ListOAuthProvidersResponse, error) {
	providers := h.serviceHandler.ListOAuthProviders(r.Context())
	response := &proto_models.ListOAuthProvidersResponse{Providers: make([]*proto_models.OAuthProviderDescriptor, 0, len(providers))}
	for _, provider := range providers {
		response.Providers = append(response.Providers, &proto_models.OAuthProviderDescriptor{
			Slug:        provider.Slug,
			DisplayName: provider.DisplayName,
			IconKey:     provider.IconKey,
			FormPost:    provider.FormPost,
		})
	}
	return response, nil
}

// CreateOAuthState starts an attempt and binds it to the initiating browser
// with an HttpOnly cookie; the client navigates to the authorization URL.
func (h *CommonAPIHandler[U]) CreateOAuthState(r *http.Request, req *proto_models.OAuthStateRequest) (*proto_models.OAuthStateResponse, error) {
	authorization, err := h.serviceHandler.CreateOAuthState(r.Context(), auth.OAuthStateRequest{
		ProviderSlug:        req.GetProviderSlug(),
		ReturnURL:           req.GetReturnUrl(),
		RedirectURI:         req.GetRedirectUri(),
		FrontendCallbackURL: req.GetFrontendCallbackUrl(),
	})
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "create_oauth_state"),
			zap.String(appLogger.KeyProvider, req.GetProviderSlug()),
			zap.String(appLogger.KeyReturnURL, req.GetReturnUrl()))
	}
	markIssueOAuthStateCookie(r, authorization.Binding)
	return &proto_models.OAuthStateResponse{
		State:            authorization.State,
		RedirectUri:      authorization.RedirectURI,
		AuthorizationUrl: authorization.AuthorizationURL,
	}, nil
}

// OAuthFormPostRelay receives a provider's form_post response and redirects
// the browser (303) to the frontend callback URL signed into the state, so the
// frontend completes the callback with its same-site cookie. A state that does
// not verify gets a 400 (a rate-limited client a 429) without a Location
// header: the relay is never an open redirect.
func (h *CommonAPIHandler[U]) OAuthFormPostRelay(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	r.Body = http.MaxBytesReader(w, r.Body, h.provider().OAuthFormPostMaxBodyBytes())
	if err := r.ParseForm(); err != nil {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Rejected unreadable OAuth form_post body",
			zap.String(logkeys.Operation, "relay_oauth_form_post"),
			zap.Error(err))
		status := http.StatusBadRequest
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	target, err := h.serviceHandler.OAuthFormPostRedirect(r.Context(), auth.OAuthFormPost{
		State: r.PostForm.Get("state"),
		Code:  r.PostForm.Get("code"),
		Error: r.PostForm.Get("error"),
	})
	if err != nil {
		h.logOAuthRelayRejection(r, err)
		status := http.StatusBadRequest
		if errors.Is(err, apperrors.ErrRateLimitExceeded) {
			status = http.StatusTooManyRequests
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (h *CommonAPIHandler[U]) logOAuthRelayRejection(r *http.Request, err error) {
	level := zapcore.WarnLevel
	if configured, ok := appLogger.ErrorLevel(err); ok {
		level = configured
	}
	if ce := appLogger.For(r.Context(), h.logger, commonAPILoggerName).Check(level, "Rejected OAuth form_post relay"); ce != nil {
		ce.Write(append(appLogger.ErrorFields(err), zap.String(logkeys.Operation, "relay_oauth_form_post"), zap.Error(err))...)
	}
}

// OAuthCallback completes the attempt bound to this browser and sets the
// session cookie. The binding cookie is cleared on every outcome, so a state
// replayed after this response, or copied from another browser, cannot
// install a session.
func (h *CommonAPIHandler[U]) OAuthCallback(r *http.Request, req *proto_models.OAuthCodeRequest) (*proto_models.OAuthTokenResponse, error) {
	markClearOAuthStateCookie(r)
	binding := ""
	if cookie, err := r.Cookie(h.oauthStateCookieName()); err == nil {
		binding = cookie.Value
	}
	result, err := h.serviceHandler.LoginUserByOAuth(r.Context(), auth.OAuthCallback{
		Code:    req.GetCode(),
		State:   req.GetState(),
		Binding: binding,
	})
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "login_user_with_oauth"),
			zap.String(appLogger.KeyProvider, result.ProviderSlug),
			zap.String(appLogger.KeyAuthMethod, "oauth"))
	}
	if result.User == nil {
		return nil, MapServiceErrorWithFields(
			appLogger.InvariantError("successful_oauth_login_returns_user", nil,
				zap.String(logkeys.Stage, "login_user_with_oauth"),
				zap.String(logkeys.Expected, "non-nil authenticated user"),
				zap.String(logkeys.Actual, "nil authenticated user")),
			zap.String(logkeys.Operation, "login_user_with_oauth"),
			zap.String(appLogger.KeyProvider, result.ProviderSlug),
			zap.String(appLogger.KeyAuthMethod, "oauth"))
	}

	markIssueSessionCookie(r, result.SessionToken)
	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("OAuth login successful",
		zap.String(logkeys.Operation, "login_user_with_oauth"),
		zap.String(appLogger.KeyProvider, result.ProviderSlug),
		zap.String(appLogger.KeyAuthMethod, "oauth"),
		zap.Uint64(appLogger.KeyUserID, uint64((*result.User).GetID())))

	return &proto_models.OAuthTokenResponse{
		AccountCreated: result.AccountCreated,
		ReturnUrl:      result.ReturnURL,
		ProviderSlug:   result.ProviderSlug,
	}, nil
}
