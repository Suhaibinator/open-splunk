package auth

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaximumBrowserIdentityBytes matches the browser server's canonical
	// tenant and owner identity boundary.
	MaximumBrowserIdentityBytes = 255
)

var (
	// ErrInvalidBrowserAuthenticator means the configured credential,
	// identity, or role cannot form a browser authentication boundary. It is
	// intentionally detail-free so a rejected secret is never formatted.
	ErrInvalidBrowserAuthenticator = errors.New("auth: invalid browser authenticator configuration")
	// ErrInvalidBrowserAuthenticationRequest means Authenticate was called
	// without a context. Credential failures deliberately use
	// ErrBrowserUnauthorized instead.
	ErrInvalidBrowserAuthenticationRequest = errors.New("auth: invalid browser authentication request")
	// ErrBrowserUnauthorized intentionally combines absent, malformed, and
	// incorrect browser credentials into one safe externally reportable
	// error.
	ErrBrowserUnauthorized = errors.New("auth: browser authentication failed")
)

// BrowserRole is the authorization role established for a browser principal.
// Its zero value is invalid and therefore never grants access.
type BrowserRole uint8

const (
	BrowserRoleInvalid BrowserRole = iota
	// BrowserRoleUser is an authenticated ordinary user without
	// administrator privileges.
	BrowserRoleUser
	// BrowserRoleAdministrator may cross administrator-only authorization
	// boundaries.
	BrowserRoleAdministrator
)

// Valid reports whether role is one of the roles accepted by an
// authenticator.
func (role BrowserRole) Valid() bool {
	switch role {
	case BrowserRoleUser, BrowserRoleAdministrator:
		return true
	default:
		return false
	}
}

// String returns a bounded canonical role name. Unknown numeric values are
// collapsed to "invalid" rather than formatted from untrusted input.
func (role BrowserRole) String() string {
	switch role {
	case BrowserRoleUser:
		return "user"
	case BrowserRoleAdministrator:
		return "administrator"
	default:
		return "invalid"
	}
}

// BrowserPrincipal is the immutable, safe result of browser authentication.
// Its fields are deliberately private. Accessors return detached strings so a
// caller cannot retain aliases to an authenticator's configured identity.
type BrowserPrincipal struct {
	tenantID string
	ownerID  string
	role     BrowserRole
}

// NewBrowserPrincipal projects an already authenticated user onto the browser transport scope.
func NewBrowserPrincipal(tenantID, ownerID string, role BrowserRole) (BrowserPrincipal, error) {
	principal := BrowserPrincipal{tenantID: strings.Clone(tenantID), ownerID: strings.Clone(ownerID), role: role}
	if !principal.Valid() {
		return BrowserPrincipal{}, ErrBrowserUnauthorized
	}
	return principal, nil
}

// TenantID returns the exact configured tenant identity without normalizing it.
func (principal BrowserPrincipal) TenantID() string {
	return strings.Clone(principal.tenantID)
}

// OwnerID returns the exact configured owner identity without normalizing it.
func (principal BrowserPrincipal) OwnerID() string {
	return strings.Clone(principal.ownerID)
}

// Role returns the principal's explicit browser authorization role.
func (principal BrowserPrincipal) Role() BrowserRole {
	return principal.role
}

// Valid reports whether the principal contains complete canonical identity and
// role data.
func (principal BrowserPrincipal) Valid() bool {
	return validBrowserIdentity(principal.tenantID) &&
		validBrowserIdentity(principal.ownerID) &&
		principal.role.Valid()
}

// IsAdministrator reports whether this principal may cross an
// administrator-only authorization boundary. Invalid and ordinary principals
// fail closed.
func (principal BrowserPrincipal) IsAdministrator() bool {
	return principal.Valid() && principal.role == BrowserRoleAdministrator
}

func (principal BrowserPrincipal) detached() BrowserPrincipal {
	return BrowserPrincipal{
		tenantID: strings.Clone(principal.tenantID),
		ownerID:  strings.Clone(principal.ownerID),
		role:     principal.role,
	}
}

func validBrowserIdentity(value string) bool {
	if value == "" ||
		len(value) > MaximumBrowserIdentityBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
