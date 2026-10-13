package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	json "encoding/json/v2"
	stdErrors "errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/oauth2"
)

// OAuth state and the browser binding cookie are sealed as
// base64url(payload JSON || HMAC-SHA256), each under its own key derived from
// the session token secret, so neither can stand in for the other.
const (
	oauthStateVersion    = 2
	oauthStateKeyLabel   = "go-common/oauth/state/v2"
	oauthBindingKeyLabel = "go-common/oauth/binding/v2"
	oauthStateCSRFBytes  = 32
	oauthSealedMACBytes  = sha256.Size
)

// OAuthStateRequest begins one authorization attempt.
type OAuthStateRequest struct {
	ProviderSlug string
	ReturnURL    string
	// RedirectURI must be one of the provider's exact RedirectURLs. For a
	// form_post provider it is the backend relay.
	RedirectURI string
	// FrontendCallbackURL is the frontend page the form_post relay redirects
	// to; required exactly when the provider uses form_post.
	FrontendCallbackURL string
}

// OAuthAuthorization is a started attempt. State goes to the provider through
// AuthorizationURL; Binding is the HttpOnly cookie value that ties the attempt
// to the initiating browser and carries the PKCE verifier and OIDC nonce.
type OAuthAuthorization struct {
	State            string
	Binding          string
	AuthorizationURL string
	RedirectURI      string
}

// OAuthFormPost is a provider's form_post response received by the relay.
type OAuthFormPost struct {
	State string
	Code  string
	Error string
}

type oauthStatePayload struct {
	Version             int    `json:"v"`
	Slug                string `json:"slug"`
	ReturnURL           string `json:"return_url"`
	RedirectURI         string `json:"redirect_uri"`
	FrontendCallbackURL string `json:"frontend_callback_url,omitempty"`
	CSRF                string `json:"csrf"`
	IssuedAt            int64  `json:"iat"`
}

type oauthBindingPayload struct {
	Version      int    `json:"v"`
	StateHash    string `json:"state_sha256"`
	CodeVerifier string `json:"pkce_verifier"`
	Nonce        string `json:"nonce,omitempty"`
	IssuedAt     int64  `json:"iat"`
}

// deriveOAuthKey derives a purpose-specific HMAC key from the session secret.
func deriveOAuthKey(secret, label string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(label))
	return mac.Sum(nil)
}

func sealOAuthValue[T oauthStatePayload | oauthBindingPayload](key []byte, payload T) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(encoded)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(encoded)), nil
}

// openOAuthValue verifies the MAC in constant time before decoding anything.
func openOAuthValue[T oauthStatePayload | oauthBindingPayload](key []byte, sealed string, out *T) error {
	raw, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil || len(raw) <= oauthSealedMACBytes {
		return fmt.Errorf("%w: malformed sealed value", errors.ErrInvalidOAuthState)
	}
	payload, sum := raw[:len(raw)-oauthSealedMACBytes], raw[len(raw)-oauthSealedMACBytes:]
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(sum, mac.Sum(nil)) {
		return fmt.Errorf("%w: signature invalid", errors.ErrInvalidOAuthState)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("%w: payload unreadable", errors.ErrInvalidOAuthState)
	}
	return nil
}

func oauthStateHash(state string) string {
	sum := sha256.Sum256([]byte(state))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// checkOAuthIssuedAt applies the sampled TTL and future skew tolerance.
func checkOAuthIssuedAt(issuedAt int64, now time.Time, ttl, skew time.Duration) error {
	issued := time.Unix(issuedAt, 0)
	if issuedAt <= 0 || issued.After(now.Add(skew)) || now.Sub(issued) > ttl {
		return fmt.Errorf("%w: expired", errors.ErrInvalidOAuthState)
	}
	return nil
}

func oauthSettings(view RuntimeConfigView) oauth.Settings {
	return oauth.Settings{
		HTTPTimeout:                view.OAuthHTTPTimeout(),
		DiscoveryRefreshInterval:   view.OAuthDiscoveryRefreshInterval(),
		SignedClientSecretLifetime: view.OAuthSignedClientSecretLifetime(),
		MaxResponseBytes:           view.OAuthMaxResponseBytes(),
	}
}

// openOAuthState verifies a state's MAC, version, and age under view.
func (h *AuthHandler[U]) openOAuthState(view RuntimeConfigView, state string, now time.Time) (*oauthStatePayload, error) {
	if len(state) > view.OAuthMaxStateLength() {
		return nil, fmt.Errorf("%w: too long", errors.ErrInvalidOAuthState)
	}
	var payload oauthStatePayload
	if err := openOAuthValue(h.oauthStateKey, state, &payload); err != nil {
		return nil, err
	}
	if payload.Version != oauthStateVersion || payload.CSRF == "" || payload.Slug == "" {
		return nil, fmt.Errorf("%w: unsupported state", errors.ErrInvalidOAuthState)
	}
	if err := checkOAuthIssuedAt(payload.IssuedAt, now, view.OAuthStateTTL(), view.OAuthStateClockSkew()); err != nil {
		return nil, err
	}
	return &payload, nil
}

// openOAuthBinding verifies that binding was issued with state to this browser.
func (h *AuthHandler[U]) openOAuthBinding(view RuntimeConfigView, binding, state string, now time.Time) (*oauthBindingPayload, error) {
	if binding == "" {
		return nil, fmt.Errorf("%w: browser binding missing", errors.ErrInvalidOAuthState)
	}
	var payload oauthBindingPayload
	if err := openOAuthValue(h.oauthBindingKey, binding, &payload); err != nil {
		return nil, err
	}
	if payload.Version != oauthStateVersion || payload.CodeVerifier == "" ||
		!hmac.Equal([]byte(payload.StateHash), []byte(oauthStateHash(state))) {
		return nil, fmt.Errorf("%w: browser binding does not match state", errors.ErrInvalidOAuthState)
	}
	if err := checkOAuthIssuedAt(payload.IssuedAt, now, view.OAuthStateTTL(), view.OAuthStateClockSkew()); err != nil {
		return nil, err
	}
	return &payload, nil
}

// validateFrontendCallbackURL requires an absolute http(s) URL on an allowed
// frontend origin: the relay redirects the browser to it.
func (h *AuthHandler[U]) validateFrontendCallbackURL(raw string) error {
	if raw == "" || strings.ContainsFunc(raw, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return fmt.Errorf("%w: frontend callback URL missing or malformed", errors.ErrInvalidOAuthState)
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" ||
		parsed.User != nil || parsed.Fragment != "" || parsed.Opaque != "" {
		return fmt.Errorf("%w: frontend callback URL must be an absolute http(s) URL", errors.ErrInvalidOAuthState)
	}
	allowlist := h.currentFrontendAllowlist()
	if allowlist == nil {
		return fmt.Errorf("%w: active frontend origin configuration unavailable", errors.ErrInvalidOAuthState)
	}
	if err := allowlist.ValidateReturnURL(raw); err != nil {
		return appLogger.WithErrorCause(fmt.Errorf("%w: frontend callback URL not allowed", errors.ErrInvalidOAuthState), err)
	}
	return nil
}

// CreateOAuthState starts an authorization attempt with one provider sample:
// it validates the request, issues the signed state and browser binding, and
// builds the provider authorization URL (S256 PKCE, OIDC nonce, response mode).
func (h *AuthHandler[U]) CreateOAuthState(ctx context.Context, req OAuthStateRequest) (*OAuthAuthorization, error) {
	fields := []zap.Field{zap.String(appLogger.KeyProvider, req.ProviderSlug), zap.String(logkeys.Stage, "create_oauth_state")}
	view := h.runtimeConfigProvider()
	provider, _, err := h.oauthProvider(ctx, req.ProviderSlug)
	if err != nil {
		return nil, appLogger.WithErrorFields(err, fields...)
	}
	allowlist := h.currentFrontendAllowlist()
	if allowlist == nil {
		return nil, appLogger.InvariantError("oauth_state_requires_frontend_allowlist", errors.ErrInternalError,
			append(fields,
				zap.String(logkeys.Expected, "frontend origin allowlist configured"),
				zap.String(logkeys.Actual, "frontend origin allowlist unavailable"))...)
	}
	if err := allowlist.ValidateReturnURL(req.ReturnURL); err != nil {
		return nil, appLogger.WithErrorLevel(appLogger.WithErrorCause(fmt.Errorf("%w: OAuth state returnUrl not allowed", errors.ErrInvalidOAuthState), err), zapcore.WarnLevel,
			append(fields, zap.String(appLogger.KeyReturnURL, req.ReturnURL))...)
	}
	if !provider.AllowsRedirectURL(req.RedirectURI) {
		return nil, appLogger.WithErrorLevel(fmt.Errorf("%w: OAuth redirect URI not allowed", errors.ErrInvalidOAuthState), zapcore.WarnLevel, fields...)
	}
	if provider.ResponseMode == oauth.ResponseModeFormPost {
		if err := h.validateFrontendCallbackURL(req.FrontendCallbackURL); err != nil {
			return nil, appLogger.WithErrorLevel(err, zapcore.WarnLevel, fields...)
		}
	} else if req.FrontendCallbackURL != "" {
		return nil, appLogger.WithErrorLevel(fmt.Errorf("%w: frontend callback URL applies only to form_post providers", errors.ErrInvalidOAuthState), zapcore.WarnLevel, fields...)
	}

	csrf := make([]byte, oauthStateCSRFBytes)
	if _, err := rand.Read(csrf); err != nil {
		return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrInternalError, err), fields...)
	}
	issuedAt := time.Now().Unix()
	state, err := sealOAuthValue(h.oauthStateKey, oauthStatePayload{
		Version:             oauthStateVersion,
		Slug:                provider.Slug,
		ReturnURL:           req.ReturnURL,
		RedirectURI:         req.RedirectURI,
		FrontendCallbackURL: req.FrontendCallbackURL,
		CSRF:                base64.RawURLEncoding.EncodeToString(csrf),
		IssuedAt:            issuedAt,
	})
	if err != nil {
		return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrInternalError, err), fields...)
	}
	// Never issue a state the callback sanitizer or the relay would refuse.
	if len(state) > view.OAuthMaxStateLength() {
		return nil, appLogger.WithErrorLevel(fmt.Errorf("%w: OAuth state would exceed the configured maximum length", errors.ErrInvalidOAuthState), zapcore.WarnLevel,
			append(fields, zap.Int(appLogger.KeyLength, len(state)))...)
	}
	verifier := oauth2.GenerateVerifier()
	var nonce string
	if provider.Issuer != "" {
		nonce = oauth.NewNonce()
	}
	binding, err := sealOAuthValue(h.oauthBindingKey, oauthBindingPayload{
		Version:      oauthStateVersion,
		StateHash:    oauthStateHash(state),
		CodeVerifier: verifier,
		Nonce:        nonce,
		IssuedAt:     issuedAt,
	})
	if err != nil {
		return nil, appLogger.WithErrorFields(stdErrors.Join(errors.ErrInternalError, err), fields...)
	}
	authorizationURL, err := h.oauthRuntime.AuthorizationURL(ctx, oauthSettings(view), provider, oauth.AuthorizationRequest{
		State:        state,
		RedirectURI:  req.RedirectURI,
		CodeVerifier: verifier,
		Nonce:        nonce,
	})
	if err != nil {
		return nil, appLogger.WithErrorFields(mapOAuthRuntimeError(err), fields...)
	}
	return &OAuthAuthorization{State: state, Binding: binding, AuthorizationURL: authorizationURL, RedirectURI: req.RedirectURI}, nil
}

// OAuthFormPostRedirect turns a provider's form_post response into the
// frontend callback URL carrying code (or error) and state. It verifies only
// the state's signature and age; the callback verifies everything else. A
// state without a frontend callback URL is rejected, so the relay never
// redirects anywhere the state did not name.
func (h *AuthHandler[U]) OAuthFormPostRedirect(ctx context.Context, form OAuthFormPost) (string, error) {
	view := h.runtimeConfigProvider()
	payload, err := h.openOAuthState(view, form.State, time.Now())
	if err != nil {
		return "", appLogger.WithErrorLevel(err, zapcore.WarnLevel, zap.String(logkeys.Stage, "relay_oauth_form_post"))
	}
	fields := []zap.Field{zap.String(appLogger.KeyProvider, payload.Slug), zap.String(logkeys.Stage, "relay_oauth_form_post")}
	if payload.FrontendCallbackURL == "" || (form.Code == "") == (form.Error == "") {
		return "", appLogger.WithErrorLevel(fmt.Errorf("%w: form_post relay needs a form_post state and exactly one of code or error", errors.ErrInvalidOAuthState), zapcore.WarnLevel, fields...)
	}
	// Re-check against the current allowlist: origins may have been removed
	// since the state was issued.
	if err := h.validateFrontendCallbackURL(payload.FrontendCallbackURL); err != nil {
		return "", appLogger.WithErrorLevel(err, zapcore.WarnLevel, fields...)
	}
	target, err := url.Parse(payload.FrontendCallbackURL)
	if err != nil {
		return "", appLogger.WithErrorFields(fmt.Errorf("%w: frontend callback URL unreadable", errors.ErrInvalidOAuthState), fields...)
	}
	query := target.Query()
	if form.Code != "" {
		query.Set("code", form.Code)
	} else {
		query.Set("error", form.Error)
	}
	query.Set("state", form.State)
	target.RawQuery = query.Encode()
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, authLoggerName).Debug("Relaying OAuth form_post response", fields...)
	}
	return target.String(), nil
}
