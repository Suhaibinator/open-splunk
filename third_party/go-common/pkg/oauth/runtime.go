package oauth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"
)

// Settings are the protocol knobs for one OAuth operation. Callers sample
// them from configuration once per operation and pass them as-is.
type Settings struct {
	// HTTPTimeout bounds every request to the provider.
	HTTPTimeout time.Duration
	// DiscoveryRefreshInterval is how long a discovery document (and its key
	// set) is reused, and how long an unused issuer stays cached.
	DiscoveryRefreshInterval time.Duration
	// SignedClientSecretLifetime is the lifetime of client secrets minted per
	// exchange (Apple).
	SignedClientSecretLifetime time.Duration
	// MaxResponseBytes caps userinfo and provider API response bodies.
	MaxResponseBytes int64
}

// AuthorizationRequest carries the per-attempt values bound to the browser.
type AuthorizationRequest struct {
	State       string
	RedirectURI string
	// CodeVerifier is the PKCE verifier (oauth2.GenerateVerifier); the URL
	// carries its S256 challenge.
	CodeVerifier string
	// Nonce is required for OIDC providers and ignored otherwise.
	Nonce string
}

// ExchangeRequest completes one authorization attempt.
type ExchangeRequest struct {
	Code         string
	RedirectURI  string
	CodeVerifier string
	// Nonce must equal the ID token's nonce for OIDC providers.
	Nonce string
}

// Runtime performs OAuth protocol work and caches OIDC discovery per issuer.
// It is safe for concurrent use; Close releases it.
type Runtime struct {
	transport http.RoundTripper
	now       func() time.Time

	// ctx bounds discovery fetches. They are shared between concurrent callers
	// through singleflight, so no single request context may cancel them.
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	issuers map[string]*discoveredIssuer
	flights singleflight.Group
}

type discoveredIssuer struct {
	provider  *oidc.Provider
	fetchedAt time.Time
	lastUsed  time.Time
}

// NewRuntime returns a Runtime whose provider requests use transport (nil
// selects http.DefaultTransport).
func NewRuntime(transport http.RoundTripper) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		transport: transport,
		now:       time.Now,
		ctx:       ctx,
		cancel:    cancel,
		issuers:   make(map[string]*discoveredIssuer),
	}
}

// Close drops the discovery cache and aborts in-flight discovery. Later calls
// return ErrRuntimeClosed. Close is idempotent.
func (r *Runtime) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.issuers = nil
	r.cancel()
}

func (r *Runtime) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// NewNonce returns a random OIDC nonce with 128 bits of entropy.
func NewNonce() string { return rand.Text() }

// AuthorizationURL builds the provider authorization URL with an S256 PKCE
// challenge, the OIDC nonce (OIDC providers only), response_mode=form_post
// when configured, and the provider's AuthParams.
func (r *Runtime) AuthorizationURL(ctx context.Context, settings Settings, p Provider, req AuthorizationRequest) (string, error) {
	if r.isClosed() {
		return "", ErrRuntimeClosed
	}
	if err := p.Validate(); err != nil {
		return "", err
	}
	if req.State == "" || req.CodeVerifier == "" || (p.Issuer != "" && req.Nonce == "") {
		return "", fmt.Errorf("%w: state, PKCE verifier, and (for OIDC) nonce are required", ErrInvalidRequest)
	}
	if !p.AllowsRedirectURL(req.RedirectURI) {
		return "", ErrRedirectURINotAllowed
	}
	endpoint, _, err := r.endpoint(ctx, settings, p)
	if err != nil {
		return "", err
	}
	cfg := oauth2.Config{ClientID: p.ClientID, Endpoint: endpoint, RedirectURL: req.RedirectURI, Scopes: p.Scopes}
	opts := make([]oauth2.AuthCodeOption, 0, len(p.AuthParams)+3)
	opts = append(opts, oauth2.S256ChallengeOption(req.CodeVerifier))
	if p.Issuer != "" {
		opts = append(opts, oidc.Nonce(req.Nonce))
	}
	if p.ResponseMode == ResponseModeFormPost {
		opts = append(opts, oauth2.SetAuthURLParam("response_mode", "form_post"))
	}
	for _, param := range p.AuthParams {
		opts = append(opts, oauth2.SetAuthURLParam(param.Name, param.Value))
	}
	return cfg.AuthCodeURL(req.State, opts...), nil
}

// Exchange redeems the authorization code with the PKCE verifier, verifies
// the ID token and nonce for OIDC providers, and resolves the identity.
func (r *Runtime) Exchange(ctx context.Context, settings Settings, p Provider, req ExchangeRequest) (Identity, error) {
	if r.isClosed() {
		return Identity{}, ErrRuntimeClosed
	}
	if err := p.Validate(); err != nil {
		return Identity{}, err
	}
	if req.Code == "" || req.CodeVerifier == "" || (p.Issuer != "" && req.Nonce == "") {
		return Identity{}, fmt.Errorf("%w: code, PKCE verifier, and (for OIDC) nonce are required", ErrInvalidRequest)
	}
	if !p.AllowsRedirectURL(req.RedirectURI) {
		return Identity{}, ErrRedirectURINotAllowed
	}
	endpoint, discovered, err := r.endpoint(ctx, settings, p)
	if err != nil {
		return Identity{}, err
	}

	cfg := oauth2.Config{ClientID: p.ClientID, Endpoint: endpoint, RedirectURL: req.RedirectURI, Scopes: p.Scopes}
	if p.ClientSecret == nil {
		// A public client identifies itself with client_id in the body.
		cfg.Endpoint.AuthStyle = oauth2.AuthStyleInParams
	} else {
		secret, err := p.ClientSecret(ctx, SecretInput{
			ClientID:             p.ClientID,
			Now:                  r.now(),
			SignedSecretLifetime: settings.SignedClientSecretLifetime,
		})
		if err != nil {
			if errors.Is(err, ErrClientSecret) {
				return Identity{}, err
			}
			return Identity{}, fmt.Errorf("%w: %w", ErrClientSecret, err)
		}
		if secret == "" {
			return Identity{}, fmt.Errorf("%w: source returned an empty secret", ErrClientSecret)
		}
		cfg.ClientSecret = secret
	}

	client := r.httpClient(settings)
	exchangeCtx := context.WithValue(ctx, oauth2.HTTPClient, client)
	token, err := cfg.Exchange(exchangeCtx, req.Code, oauth2.VerifierOption(req.CodeVerifier))
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %w", ErrExchange, err)
	}

	in := ResolveInput{Token: token, HTTPClient: client, MaxResponseBytes: settings.MaxResponseBytes}
	if discovered != nil {
		if in.IDToken, err = r.verifyIDToken(ctx, discovered, p, token, req.Nonce); err != nil {
			return Identity{}, err
		}
	}

	id, err := p.Identity(ctx, in)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %w", ErrIdentity, err)
	}
	if id.Subject == "" {
		return Identity{}, ErrSubjectMissing
	}
	return id, nil
}

func (r *Runtime) verifyIDToken(ctx context.Context, discovered *oidc.Provider, p Provider, token *oauth2.Token, nonce string) (*oidc.IDToken, error) {
	raw, _ := token.Extra("id_token").(string)
	if raw == "" {
		return nil, ErrIDTokenMissing
	}
	verified, err := discovered.Verifier(&oidc.Config{ClientID: p.ClientID, Now: r.now}).Verify(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIDTokenInvalid, err)
	}
	if subtle.ConstantTimeCompare([]byte(verified.Nonce), []byte(nonce)) != 1 {
		return nil, ErrNonceMismatch
	}
	if verified.AccessTokenHash != "" {
		if err := verified.VerifyAccessToken(token.AccessToken); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrIDTokenInvalid, err)
		}
	}
	return verified, nil
}

// httpClient bounds every provider request by HTTPTimeout and every response
// body (discovery, key sets, token, userinfo) by MaxResponseBytes.
func (r *Runtime) httpClient(settings Settings) *http.Client {
	base := r.transport
	if base == nil {
		base = http.DefaultTransport
	}
	return &http.Client{
		Transport: cappedTransport{base: base, maxBytes: settings.MaxResponseBytes},
		Timeout:   settings.HTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// endpoint returns the provider's OAuth 2.0 endpoints and, for OIDC
// providers, the discovered provider used to verify ID tokens.
func (r *Runtime) endpoint(ctx context.Context, settings Settings, p Provider) (oauth2.Endpoint, *oidc.Provider, error) {
	style := oauth2.AuthStyleInHeader
	if p.AuthStyle == AuthStyleInParams {
		style = oauth2.AuthStyleInParams
	}
	if p.Issuer == "" {
		return oauth2.Endpoint{AuthURL: p.Endpoints.Authorization, TokenURL: p.Endpoints.Token, AuthStyle: style}, nil, nil
	}
	discovered, err := r.discover(ctx, settings, p.Issuer)
	if err != nil {
		return oauth2.Endpoint{}, nil, err
	}
	endpoint := discovered.Endpoint()
	endpoint.AuthStyle = style
	return endpoint, discovered, nil
}

// discover returns the cached discovery for issuer, fetching it (once across
// concurrent callers) when absent or older than the refresh interval.
// Failures are never cached; the next call retries. A failed refresh keeps
// serving the previous discovery until it is retired for disuse.
func (r *Runtime) discover(ctx context.Context, settings Settings, issuer string) (*oidc.Provider, error) {
	now := r.now()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRuntimeClosed
	}
	r.retireLocked(now, settings.DiscoveryRefreshInterval)
	entry, cached := r.issuers[issuer]
	if cached && now.Sub(entry.fetchedAt) < settings.DiscoveryRefreshInterval {
		entry.lastUsed = now
		r.mu.Unlock()
		return entry.provider, nil
	}
	r.mu.Unlock()

	result := r.flights.DoChan(issuer, func() (any, error) {
		return r.fetch(settings, issuer)
	})
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrDiscovery, ctx.Err())
	case res := <-result:
		if res.Err == nil {
			return res.Val.(*oidc.Provider), nil
		}
		if !cached || errors.Is(res.Err, ErrRuntimeClosed) {
			return nil, res.Err
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed {
			return nil, ErrRuntimeClosed
		}
		entry.lastUsed = now
		return entry.provider, nil
	}
}

func (r *Runtime) fetch(settings Settings, issuer string) (*oidc.Provider, error) {
	// go-oidc keeps this client for later JWKS refreshes of the provider's
	// key set, detached from any request context.
	discovered, err := oidc.NewProvider(oidc.ClientContext(r.ctx, r.httpClient(settings)), issuer)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrDiscovery, issuer, err)
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRuntimeClosed
	}
	r.issuers[issuer] = &discoveredIssuer{provider: discovered, fetchedAt: now, lastUsed: now}
	return discovered, nil
}

// retireLocked drops issuers no operation has used within interval.
func (r *Runtime) retireLocked(now time.Time, interval time.Duration) {
	for issuer, entry := range r.issuers {
		if now.Sub(entry.lastUsed) >= interval {
			delete(r.issuers, issuer)
		}
	}
}

// cappedTransport limits every response body to maxBytes; reading past the
// cap fails with ErrResponseTooLarge.
type cappedTransport struct {
	base     http.RoundTripper
	maxBytes int64
}

func (t cappedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &cappedBody{ReadCloser: resp.Body, maxBytes: t.maxBytes, remaining: t.maxBytes}
	return resp, nil
}

type cappedBody struct {
	io.ReadCloser
	maxBytes, remaining int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		// Probe one byte past the cap: end of body means it fit exactly.
		var probe [1]byte
		n, err := b.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("%w: more than %d bytes", ErrResponseTooLarge, b.maxBytes)
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
