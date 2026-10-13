package oauth

import (
	"fmt"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"
)

// Claims is a decoded JSON object from an ID token, userinfo endpoint, or
// provider API. Values stay raw so each claim is coerced only when read.
type Claims map[string]jsontext.Value

// DecodeClaims decodes a JSON object with JSON v2 semantics (duplicate names
// are rejected).
func DecodeClaims(data []byte) (Claims, error) {
	var claims Claims
	if err := json.Unmarshal(data, &claims); err != nil {
		return nil, fmt.Errorf("%w: decode claims: %w", ErrInvalidClaim, err)
	}
	return claims, nil
}

// rawClaims captures the bytes go-oidc hands to its (JSON v1) Claims
// decoder so they can be decoded again with JSON v2.
type rawClaims []byte

// UnmarshalJSON implements the JSON v1 unmarshaler go-oidc calls.
func (r *rawClaims) UnmarshalJSON(data []byte) error {
	*r = append((*r)[:0], data...)
	return nil
}

// present reports whether name holds a non-null value.
func (c Claims) present(name string) (jsontext.Value, bool) {
	value, ok := c[name]
	if !ok || value.Kind() == 'n' {
		return nil, false
	}
	return value, true
}

// String returns a string claim; absent and null claims return "".
func (c Claims) String(name string) (string, error) {
	value, ok := c.present(name)
	if !ok {
		return "", nil
	}
	if value.Kind() != '"' {
		return "", fmt.Errorf("%w: %q must be a string", ErrInvalidClaim, name)
	}
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return "", fmt.Errorf("%w: %q: %w", ErrInvalidClaim, name, err)
	}
	return s, nil
}

// Subject returns an identifier claim that may be a JSON string or a JSON
// integer (GitHub and Facebook-style numeric IDs). Absent returns "".
func (c Claims) Subject(name string) (string, error) {
	value, ok := c.present(name)
	if !ok {
		return "", nil
	}
	switch value.Kind() {
	case '"':
		return c.String(name)
	case '0':
		digits := string(value)
		if digits == "" || strings.TrimLeft(digits, "0123456789") != "" {
			return "", fmt.Errorf("%w: %q must be a non-negative integer or a string", ErrInvalidClaim, name)
		}
		return digits, nil
	default:
		return "", fmt.Errorf("%w: %q must be a string or an integer", ErrInvalidClaim, name)
	}
}

// Bool returns a boolean claim, accepting JSON true/false and the strings
// "true"/"false" (Apple). Absent and null claims return nil.
func (c Claims) Bool(name string) (*bool, error) {
	value, ok := c.present(name)
	if !ok {
		return nil, nil
	}
	var b bool
	switch value.Kind() {
	case 't':
		b = true
	case 'f':
		b = false
	case '"':
		s, err := c.String(name)
		if err != nil {
			return nil, err
		}
		switch s {
		case "true":
			b = true
		case "false":
			b = false
		default:
			return nil, fmt.Errorf("%w: %q must be a boolean", ErrInvalidClaim, name)
		}
	default:
		return nil, fmt.Errorf("%w: %q must be a boolean", ErrInvalidClaim, name)
	}
	return &b, nil
}

// ClaimMapping names the claims that fill an Identity. An empty name leaves
// the field unset.
type ClaimMapping struct {
	Subject       string
	Email         string
	EmailVerified string
	GivenName     string
	FamilyName    string
	Username      string
}

// StandardClaims maps the OpenID Connect standard claims.
func StandardClaims() ClaimMapping {
	return ClaimMapping{
		Subject:       "sub",
		Email:         "email",
		EmailVerified: "email_verified",
		GivenName:     "given_name",
		FamilyName:    "family_name",
		Username:      "preferred_username",
	}
}

// Identity builds an Identity from claims. A missing subject is reported by
// the runtime, not here, so custom resolvers can fill it in afterwards.
func (m ClaimMapping) Identity(claims Claims) (Identity, error) {
	var id Identity
	var err error
	if m.Subject != "" {
		if id.Subject, err = claims.Subject(m.Subject); err != nil {
			return Identity{}, err
		}
	}
	for _, field := range []struct {
		name   string
		target *string
	}{
		{m.Email, &id.Email},
		{m.GivenName, &id.GivenName},
		{m.FamilyName, &id.FamilyName},
		{m.Username, &id.Username},
	} {
		if field.name == "" {
			continue
		}
		if *field.target, err = claims.String(field.name); err != nil {
			return Identity{}, err
		}
	}
	if m.EmailVerified != "" {
		if id.EmailVerified, err = claims.Bool(m.EmailVerified); err != nil {
			return Identity{}, err
		}
	}
	return id, nil
}
