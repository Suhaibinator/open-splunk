package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"strings"
)

type bearerTokenAuthenticator struct {
	tokenDigest [sha256.Size]byte
	principal   BrowserPrincipal
}

var _ BrowserAuthenticator = (*bearerTokenAuthenticator)(nil)

// NewBearerTokenAuthenticator constructs a fixed browser authentication
// boundary. token must be generated with a cryptographically secure random
// source. The constructor stores only its SHA-256 digest and detached identity
// strings; it never retains token plaintext.
func NewBearerTokenAuthenticator(
	token []byte,
	tenantID string,
	ownerID string,
	role BrowserRole,
) (BrowserAuthenticator, error) {
	if !validBrowserBearerToken(token) ||
		!validBrowserIdentity(tenantID) ||
		!validBrowserIdentity(ownerID) ||
		!role.Valid() {
		return nil, ErrInvalidBrowserAuthenticator
	}

	return &bearerTokenAuthenticator{
		tokenDigest: sha256.Sum256(token),
		principal: BrowserPrincipal{
			tenantID: strings.Clone(tenantID),
			ownerID:  strings.Clone(ownerID),
			role:     role,
		},
	}, nil
}

// Authenticate resolves one already-extracted bearer token. Every bounded
// candidate is hashed before its fixed-size digest is compared in constant
// time. Oversized candidates fail before hashing to preserve the admission
// bound. Context cancellation is checked both before and after credential
// work.
func (authenticator *bearerTokenAuthenticator) Authenticate(
	ctx context.Context,
	token []byte,
) (BrowserPrincipal, error) {
	if ctx == nil {
		return BrowserPrincipal{}, ErrInvalidBrowserAuthenticationRequest
	}
	if err := ctx.Err(); err != nil {
		return BrowserPrincipal{}, err
	}
	if authenticator == nil || len(token) > MaximumBrowserBearerTokenBytes {
		return BrowserPrincipal{}, ErrBrowserUnauthorized
	}

	candidateDigest := sha256.Sum256(token)
	matched := subtle.ConstantTimeCompare(
		candidateDigest[:],
		authenticator.tokenDigest[:],
	)
	valid := validBrowserBearerToken(token)

	if err := ctx.Err(); err != nil {
		return BrowserPrincipal{}, err
	}
	if matched != 1 || !valid {
		return BrowserPrincipal{}, ErrBrowserUnauthorized
	}
	return authenticator.principal.detached(), nil
}

// String and GoString prevent diagnostic formatting from exposing even the
// stored credential digest.
func (*bearerTokenAuthenticator) String() string {
	return "BrowserAuthenticator{credential:[REDACTED]}"
}

func (*bearerTokenAuthenticator) GoString() string {
	return "BrowserAuthenticator{credential:[REDACTED]}"
}

// MarshalJSON prevents reflective serializers from exposing implementation
// details such as the credential digest.
func (*bearerTokenAuthenticator) MarshalJSON() ([]byte, error) {
	return json.Marshal(redactedValue)
}

const (
	// MinimumBrowserBearerTokenBytes prevents trivially short configured
	// credentials. Length does not substitute for entropy: callers must still
	// generate the token with a cryptographically secure random source.
	MinimumBrowserBearerTokenBytes = 32
	// MaximumBrowserBearerTokenBytes bounds authentication work and the
	// credential material accepted from a transport.
	MaximumBrowserBearerTokenBytes = 512
)

// BrowserAuthenticator establishes a detached browser principal from an
// already-extracted bearer token. HTTP header parsing is intentionally outside
// this contract. The token buffer is borrowed for the duration of Authenticate
// and must not be retained.
type BrowserAuthenticator interface {
	Authenticate(context.Context, []byte) (BrowserPrincipal, error)
}

// ValidateBrowserBearerToken validates the syntax and admission bounds of an
// already-extracted bearer token. It does not authenticate the token. All
// invalid values return the same detail-free ErrBrowserUnauthorized sentinel.
func ValidateBrowserBearerToken(token []byte) error {
	if !validBrowserBearerToken(token) {
		return ErrBrowserUnauthorized
	}
	return nil
}

// validBrowserBearerToken implements the token68 value grammar used for HTTP
// bearer credentials without accepting an Authorization scheme or whitespace.
// Padding is permitted only after at least one token character and only at the
// end.
func validBrowserBearerToken(token []byte) bool {
	if len(token) < MinimumBrowserBearerTokenBytes ||
		len(token) > MaximumBrowserBearerTokenBytes {
		return false
	}

	padding := false
	for index, character := range token {
		if character == '=' {
			if index == 0 {
				return false
			}
			padding = true
			continue
		}
		if padding || !browserBearerTokenCharacter(character) {
			return false
		}
	}
	return true
}

func browserBearerTokenCharacter(character byte) bool {
	switch {
	case character >= 'a' && character <= 'z':
		return true
	case character >= 'A' && character <= 'Z':
		return true
	case character >= '0' && character <= '9':
		return true
	}
	switch character {
	case '-', '.', '_', '~', '+', '/':
		return true
	default:
		return false
	}
}
