// Package oauth implements provider-generic OAuth 2.0 / OpenID Connect sign-in.
//
// go-common does not own provider configuration. The importing application
// describes each provider as a plain Provider value (secrets included) and
// hands go-common a Source that returns the current set. Brand adapters such as
// Google or GitHub only fill in a Provider; every field stays editable.
//
// Runtime performs the protocol work: authorization URLs with S256 PKCE and an
// OIDC nonce, the code exchange, ID-token verification, and identity
// resolution. Its knobs arrive as a sampled Settings value on every call.
package oauth

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// Provider describes one OAuth 2.0 or OpenID Connect provider.
type Provider struct {
	// Slug identifies the provider in URLs, sessions, and linked identities.
	// It is persisted, so it must never be renamed once users have linked it.
	Slug string
	// DisplayName is the human-readable name shown on sign-in buttons.
	DisplayName string
	// IconKey selects a frontend icon; empty selects the generic icon.
	IconKey string

	ClientID string
	// ClientSecret returns the client secret per exchange; nil marks a public
	// client that authenticates with PKCE only.
	ClientSecret ClientSecretSource

	// Issuer enables OIDC discovery and ID-token verification. Exactly one of
	// Issuer and Endpoints is set.
	Issuer    string
	Endpoints Endpoints
	AuthStyle AuthStyle

	Scopes []string
	// AuthParams are extra authorization request parameters. Names the
	// runtime owns (client_id, state, nonce, PKCE, ...) are rejected.
	AuthParams   []Param
	ResponseMode ResponseMode
	// RedirectURLs is the exact callback allowlist for this provider.
	RedirectURLs []string

	// Identity resolves the signed-in identity after the code exchange.
	Identity IdentityResolver
	// EmailVerification decides whether an email without a verification
	// claim is trusted.
	EmailVerification EmailVerification
	// SyntheticEmailDomain, when set, gives identities without a usable email
	// the address "<subject>@<domain>". Addresses on it identify a provider
	// account rather than a mailbox.
	SyntheticEmailDomain string
}

// Endpoints are explicit OAuth 2.0 endpoints for providers without discovery.
type Endpoints struct {
	Authorization string
	Token         string
}

// Param is one extra authorization request parameter.
type Param struct {
	Name  string
	Value string
}

// AuthStyle selects how the client authenticates to the token endpoint.
// It is internal Go configuration and is never persisted or sent on the wire.
type AuthStyle int

const (
	// AuthStyleInHeader sends HTTP Basic credentials (client_secret_basic).
	AuthStyleInHeader AuthStyle = iota
	// AuthStyleInParams sends client_id and client_secret in the form body
	// (client_secret_post).
	AuthStyleInParams
)

func (s AuthStyle) String() string {
	switch s {
	case AuthStyleInHeader:
		return "header"
	case AuthStyleInParams:
		return "params"
	default:
		return fmt.Sprintf("AuthStyle(%d)", int(s))
	}
}

func (s AuthStyle) valid() bool { return s == AuthStyleInHeader || s == AuthStyleInParams }

// ResponseMode selects how the provider returns the authorization response.
type ResponseMode int

const (
	// ResponseModeQuery redirects back with code and state in the query.
	ResponseModeQuery ResponseMode = iota
	// ResponseModeFormPost POSTs code and state to the redirect URL (Apple).
	ResponseModeFormPost
)

func (m ResponseMode) String() string {
	switch m {
	case ResponseModeQuery:
		return "query"
	case ResponseModeFormPost:
		return "form_post"
	default:
		return fmt.Sprintf("ResponseMode(%d)", int(m))
	}
}

func (m ResponseMode) valid() bool { return m == ResponseModeQuery || m == ResponseModeFormPost }

// EmailVerification decides how an identity's email verification is judged.
type EmailVerification int

const (
	// EmailVerificationRequireClaim trusts an email only when the provider
	// asserts it is verified.
	EmailVerificationRequireClaim EmailVerification = iota
	// EmailVerificationTrustWhenAbsent also trusts an email the provider
	// returns without any verification claim; an explicit false still fails.
	EmailVerificationTrustWhenAbsent
)

func (v EmailVerification) String() string {
	switch v {
	case EmailVerificationRequireClaim:
		return "require_verified_claim"
	case EmailVerificationTrustWhenAbsent:
		return "trust_when_absent"
	default:
		return fmt.Sprintf("EmailVerification(%d)", int(v))
	}
}

func (v EmailVerification) valid() bool {
	return v == EmailVerificationRequireClaim || v == EmailVerificationTrustWhenAbsent
}

// EmailTrusted reports whether id carries an email this provider's
// EmailVerification policy accepts.
func (p Provider) EmailTrusted(id Identity) bool {
	if id.Email == "" {
		return false
	}
	if id.EmailVerified != nil {
		return *id.EmailVerified
	}
	return p.EmailVerification == EmailVerificationTrustWhenAbsent
}

// SecretInput is what the runtime supplies when it needs a client secret.
type SecretInput struct {
	ClientID string
	Now      time.Time
	// SignedSecretLifetime bounds secrets minted per exchange (Apple's ES256
	// client assertion); static secrets ignore it.
	SignedSecretLifetime time.Duration
}

// ClientSecretSource returns the client secret for one token exchange.
type ClientSecretSource func(ctx context.Context, in SecretInput) (string, error)

// IdentityResolver turns a completed code exchange into an Identity.
type IdentityResolver func(ctx context.Context, in ResolveInput) (Identity, error)

// ResolveInput is the verified exchange result passed to an IdentityResolver.
type ResolveInput struct {
	Token *oauth2.Token
	// IDToken is the verified ID token (signature, issuer, audience, expiry,
	// and nonce already checked); nil unless the provider sets Issuer.
	IDToken *oidc.IDToken
	// HTTPClient is timeout-bound and does not follow redirects.
	HTTPClient *http.Client
	// MaxResponseBytes caps every response body the resolver reads.
	MaxResponseBytes int64
}

// Identity is the provider account that completed sign-in.
type Identity struct {
	Subject    string
	Email      string
	GivenName  string
	FamilyName string
	Username   string
	// EmailVerified is nil when the provider made no verification claim.
	EmailVerified *bool
}

// Source returns the current provider set. go-common samples it once per
// OAuth operation, so every value used by one operation comes from one sample.
type Source func() ([]Provider, error)

// Static returns a Source that always yields providers.
func Static(providers ...Provider) Source {
	return func() ([]Provider, error) { return slices.Clone(providers), nil }
}
