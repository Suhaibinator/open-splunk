package oauth

import "errors"

var (
	// ErrInvalidProvider reports a structurally invalid Provider.
	ErrInvalidProvider = errors.New("oauth: invalid provider")
	// ErrDuplicateSlug reports two providers sharing one slug.
	ErrDuplicateSlug = errors.New("oauth: duplicate provider slug")
	// ErrDuplicateSyntheticEmailDomain reports two providers sharing one
	// synthetic email domain.
	ErrDuplicateSyntheticEmailDomain = errors.New("oauth: duplicate synthetic email domain")

	// ErrInvalidRequest reports a missing state, code, PKCE verifier, or nonce.
	ErrInvalidRequest = errors.New("oauth: invalid request")
	// ErrRedirectURINotAllowed reports a redirect URI outside the provider's
	// exact allowlist.
	ErrRedirectURINotAllowed = errors.New("oauth: redirect URI not allowed")
	// ErrDiscovery reports a failed OIDC discovery.
	ErrDiscovery = errors.New("oauth: provider discovery failed")
	// ErrClientSecret reports a client secret source failure.
	ErrClientSecret = errors.New("oauth: client secret unavailable")
	// ErrExchange reports a failed authorization code exchange.
	ErrExchange = errors.New("oauth: code exchange failed")
	// ErrIDTokenMissing reports an OIDC token response without an ID token.
	ErrIDTokenMissing = errors.New("oauth: token response has no ID token")
	// ErrIDTokenInvalid reports an ID token that failed verification.
	ErrIDTokenInvalid = errors.New("oauth: ID token invalid")
	// ErrNonceMismatch reports an ID token nonce that differs from the one
	// bound to the browser.
	ErrNonceMismatch = errors.New("oauth: ID token nonce mismatch")
	// ErrIdentity reports an identity resolver failure.
	ErrIdentity = errors.New("oauth: identity resolution failed")
	// ErrSubjectMissing reports an identity without a provider subject.
	ErrSubjectMissing = errors.New("oauth: identity has no subject")
	// ErrSubjectMismatch reports a userinfo subject that differs from the
	// verified ID token subject.
	ErrSubjectMismatch = errors.New("oauth: userinfo subject differs from ID token subject")
	// ErrInvalidClaim reports a claim with an unusable JSON type.
	ErrInvalidClaim = errors.New("oauth: invalid claim")
	// ErrResponseTooLarge reports a provider response above MaxResponseBytes.
	ErrResponseTooLarge = errors.New("oauth: provider response too large")
	// ErrUnexpectedStatus reports a non-2xx provider API response.
	ErrUnexpectedStatus = errors.New("oauth: unexpected provider response status")
	// ErrRuntimeClosed reports use of a closed Runtime.
	ErrRuntimeClosed = errors.New("oauth: runtime closed")
)
