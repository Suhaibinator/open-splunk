package oauth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/coreos/go-oidc/v3/oidc"
)

// GitHubAPIBaseURL is the public GitHub REST API root.
const GitHubAPIBaseURL = "https://api.github.com"

// IDTokenClaims decodes every claim of a verified ID token with JSON v2.
func IDTokenClaims(token *oidc.IDToken) (Claims, error) {
	if token == nil {
		return nil, ErrIDTokenMissing
	}
	var raw rawClaims
	if err := token.Claims(&raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidClaim, err)
	}
	return DecodeClaims(raw)
}

// GetJSON performs a bearer-authenticated GET with the resolver's HTTP client
// and returns the body, capped at in.MaxResponseBytes (the runtime's client
// enforces the same cap on every provider response). header adds or
// overrides request headers; Accept defaults to application/json.
func GetJSON(ctx context.Context, in ResolveInput, url string, header http.Header) (jsontext.Value, error) {
	if in.Token == nil || in.Token.AccessToken == "" {
		return nil, fmt.Errorf("%w: no access token", ErrIdentity)
	}
	if in.HTTPClient == nil {
		return nil, fmt.Errorf("%w: no HTTP client", ErrIdentity)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %w", ErrIdentity, err)
	}
	req.Header.Set("Accept", "application/json")
	for name, values := range header {
		req.Header[name] = values
	}
	req.Header.Set("Authorization", "Bearer "+in.Token.AccessToken)
	resp, err := in.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIdentity, err)
	}
	defer resp.Body.Close()
	body, err := readCapped(resp.Body, in.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%w: GET %s returned %d", ErrUnexpectedStatus, req.URL.Redacted(), resp.StatusCode)
	}
	return body, nil
}

// GetClaims is GetJSON decoded as a JSON object.
func GetClaims(ctx context.Context, in ResolveInput, url string, header http.Header) (Claims, error) {
	body, err := GetJSON(ctx, in, url, header)
	if err != nil {
		return nil, err
	}
	return DecodeClaims(body)
}

func readCapped(body io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if errors.Is(err, ErrResponseTooLarge) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read response: %w", ErrIdentity, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrResponseTooLarge, maxBytes)
	}
	return data, nil
}

// OIDCIdentity resolves the identity from the verified ID token's claims.
// An empty mapping.Subject uses the token's verified subject.
func OIDCIdentity(mapping ClaimMapping) IdentityResolver {
	return func(_ context.Context, in ResolveInput) (Identity, error) {
		claims, err := IDTokenClaims(in.IDToken)
		if err != nil {
			return Identity{}, err
		}
		id, err := mapping.Identity(claims)
		if err != nil {
			return Identity{}, err
		}
		if mapping.Subject == "" {
			id.Subject = in.IDToken.Subject
		}
		return id, nil
	}
}

// UserInfoIdentity resolves the identity from a bearer GET of url. When the
// provider also returned a verified ID token, the userinfo subject must match
// it (and fills in for an absent userinfo subject).
func UserInfoIdentity(url string, mapping ClaimMapping) IdentityResolver {
	return func(ctx context.Context, in ResolveInput) (Identity, error) {
		claims, err := GetClaims(ctx, in, url, nil)
		if err != nil {
			return Identity{}, err
		}
		id, err := mapping.Identity(claims)
		if err != nil {
			return Identity{}, err
		}
		if in.IDToken != nil {
			if id.Subject == "" {
				id.Subject = in.IDToken.Subject
			} else if id.Subject != in.IDToken.Subject {
				return Identity{}, ErrSubjectMismatch
			}
		}
		return id, nil
	}
}

// GitHubIdentity resolves the identity from the public GitHub API.
func GitHubIdentity() IdentityResolver { return GitHubIdentityAt(GitHubAPIBaseURL) }

// GitHubIdentityAt resolves the identity from a GitHub-compatible REST API
// root (GitHub Enterprise Server or a test fake): GET /user for the numeric
// account ID, login, and name, and GET /user/emails for the primary verified
// address.
func GitHubIdentityAt(apiBaseURL string) IdentityResolver {
	base := strings.TrimSuffix(apiBaseURL, "/")
	header := http.Header{
		"Accept":               {"application/vnd.github+json"},
		"User-Agent":           {"go-common-oauth"},
		"X-Github-Api-Version": {"2022-11-28"},
	}
	return func(ctx context.Context, in ResolveInput) (Identity, error) {
		user, err := GetClaims(ctx, in, base+"/user", header)
		if err != nil {
			return Identity{}, err
		}
		id, err := ClaimMapping{Subject: "id", Username: "login"}.Identity(user)
		if err != nil {
			return Identity{}, err
		}
		name, err := user.String("name")
		if err != nil {
			return Identity{}, err
		}
		given, family, _ := strings.Cut(strings.TrimSpace(name), " ")
		id.GivenName, id.FamilyName = given, strings.TrimSpace(family)

		body, err := GetJSON(ctx, in, base+"/user/emails", header)
		if err != nil {
			return Identity{}, err
		}
		var emails []gitHubEmail
		if err := json.Unmarshal(body, &emails); err != nil {
			return Identity{}, fmt.Errorf("%w: decode GitHub emails: %w", ErrInvalidClaim, err)
		}
		if chosen, ok := chooseGitHubEmail(emails); ok {
			verified := chosen.Verified
			id.Email, id.EmailVerified = chosen.Email, &verified
		}
		return id, nil
	}
}

type gitHubEmail struct {
	Email    string `json:"email"`
	Primary  bool   `json:"primary"`
	Verified bool   `json:"verified"`
}

// chooseGitHubEmail prefers the primary verified address, then any verified
// address, then the unverified primary (which EmailTrusted rejects).
func chooseGitHubEmail(emails []gitHubEmail) (gitHubEmail, bool) {
	var fallback *gitHubEmail
	var primary *gitHubEmail
	for i := range emails {
		e := &emails[i]
		if e.Email == "" {
			continue
		}
		switch {
		case e.Primary && e.Verified:
			return *e, true
		case e.Verified && fallback == nil:
			fallback = e
		case e.Primary:
			primary = e
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	if primary != nil {
		return *primary, true
	}
	return gitHubEmail{}, false
}
