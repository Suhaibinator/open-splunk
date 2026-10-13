// Package origin validates and normalizes frontend web origins against a configured allowlist.
// It backs the per-request frontend_origin auth fields and OAuth state returnUrl checks,
// preventing open redirects by only accepting explicitly configured origins.
package origin

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

var (
	// ErrInvalidOrigin indicates a value that is not a bare http(s) origin (scheme://host[:port]).
	ErrInvalidOrigin = errors.New("invalid frontend origin")
	// ErrOriginNotAllowed indicates a syntactically valid origin that is not in the allowlist.
	ErrOriginNotAllowed = errors.New("frontend origin not allowed")
	// ErrInvalidReturnURL indicates a return URL that is neither a same-site relative path
	// nor an absolute URL on an allowlisted origin.
	ErrInvalidReturnURL = errors.New("invalid return URL")
)

// Normalize parses raw as a web origin and returns its canonical form "scheme://host[:port]".
// The scheme must be http or https and the host non-empty; userinfo, a path other than a
// bare trailing "/", query parameters, and fragments are all rejected. Scheme and host are
// lowercased and default ports (:80 for http, :443 for https) are stripped.
func Normalize(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: empty value", ErrInvalidOrigin)
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidOrigin, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%w: scheme must be http or https", ErrInvalidOrigin)
	}
	if u.User != nil {
		return "", fmt.Errorf("%w: userinfo not allowed", ErrInvalidOrigin)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%w: missing host", ErrInvalidOrigin)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: must not contain a path, query, or fragment", ErrInvalidOrigin)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", fmt.Errorf("%w: missing host", ErrInvalidOrigin)
	}
	if strings.Contains(host, ":") {
		// IPv6 literals keep their brackets in the canonical form.
		host = "[" + host + "]"
	}
	// Canonicalize the port so non-canonical numeric forms (e.g. ":0080") compare
	// equal to their canonical spelling, and drop the scheme's default port.
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return "", fmt.Errorf("%w: invalid port", ErrInvalidOrigin)
		}
		if !((scheme == "http" && p == 80) || (scheme == "https" && p == 443)) {
			host += ":" + strconv.Itoa(p)
		}
	}
	return scheme + "://" + host, nil
}

// IsSecureForProduction reports whether a normalized origin is https or points at a
// loopback host, i.e. whether it is safe to configure outside local development.
func IsSecureForProduction(normalized string) bool {
	if strings.HasPrefix(normalized, "https://") {
		return true
	}
	host := strings.TrimPrefix(normalized, "http://")
	if i := strings.LastIndex(host, ":"); i != -1 && !strings.HasSuffix(host, "]") {
		host = host[:i]
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// Allowlist is a set of normalized origins that per-request frontend targets are matched against.
type Allowlist struct {
	set map[string]struct{}
}

// NewAllowlist normalizes the given origins into an allowlist. Empty entries are skipped;
// entries that fail normalization are excluded and returned so the caller can log them
// (this package deliberately has no logger dependency).
func NewAllowlist(origins []string) (*Allowlist, []string) {
	set := make(map[string]struct{}, len(origins))
	var invalid []string
	for _, o := range origins {
		if strings.TrimSpace(o) == "" {
			continue
		}
		normalized, err := Normalize(o)
		if err != nil {
			invalid = append(invalid, o)
			continue
		}
		set[normalized] = struct{}{}
	}
	return &Allowlist{set: set}, invalid
}

// InvalidConfigEntry identifies the setting that supplied a rejected URL.
type InvalidConfigEntry struct {
	Key   string
	Value string
}

// FrontendURLOrigin returns the normalized origin of a configured frontend base URL.
func FrontendURLOrigin(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return "", ErrInvalidOrigin
	}
	return Normalize(u.Scheme + "://" + u.Host)
}

// NewAllowlistWithDefault builds an allowlist from configured origins plus the
// default frontend URL's origin (the base URL may carry a path). Rejected entries
// identify their originating setting so the caller can log actionable diagnostics.
func NewAllowlistWithDefault(origins []string, defaultFrontendURL string) (*Allowlist, []InvalidConfigEntry) {
	allowlist, invalidOrigins := NewAllowlist(origins)
	invalid := make([]InvalidConfigEntry, 0, len(invalidOrigins)+1)
	for _, entry := range invalidOrigins {
		invalid = append(invalid, InvalidConfigEntry{Key: "allowed_frontend_origins", Value: entry})
	}
	trimmed := strings.TrimSpace(defaultFrontendURL)
	if trimmed == "" {
		return allowlist, invalid
	}
	normalized, err := FrontendURLOrigin(trimmed)
	if err != nil {
		return allowlist, append(invalid, InvalidConfigEntry{Key: "frontend_url", Value: defaultFrontendURL})
	}
	allowlist.set[normalized] = struct{}{}
	return allowlist, invalid
}

// AllowsRequestOrigin reports whether an Origin header is permitted for a
// request host. A missing Origin is accepted for non-browser clients. A "*"
// entry accepts every origin, while an empty configured allowlist uses the
// same-host policy used by gorilla/websocket. Any non-empty configured
// allowlist is normalized and matched with the same rules as Allowlist.
func AllowsRequestOrigin(requestOrigin, requestHost string, configuredOrigins []string) bool {
	if requestOrigin == "" {
		return true
	}

	hasAllowlist := false
	for _, configuredOrigin := range configuredOrigins {
		configuredOrigin = strings.TrimSpace(configuredOrigin)
		if configuredOrigin == "" {
			continue
		}
		hasAllowlist = true
		if configuredOrigin == "*" {
			return true
		}
	}

	if !hasAllowlist {
		parsed, err := url.Parse(requestOrigin)
		return err == nil && strings.EqualFold(parsed.Host, requestHost)
	}

	allowlist, _ := NewAllowlist(configuredOrigins)
	normalized, err := Normalize(requestOrigin)
	return err == nil && allowlist.contains(normalized)
}

// Entries returns the normalized origins in the allowlist, in unspecified order.
func (a *Allowlist) Entries() []string {
	if a == nil {
		return nil
	}
	entries := make([]string, 0, len(a.set))
	for o := range a.set {
		entries = append(entries, o)
	}
	return entries
}

// contains reports whether a normalized origin is in the allowlist.
// A nil allowlist contains nothing, so all methods fail closed.
func (a *Allowlist) contains(normalized string) bool {
	if a == nil {
		return false
	}
	_, ok := a.set[normalized]
	return ok
}

// Validate checks a per-request origin value. An empty value means "use the default frontend"
// and returns ("", nil). Otherwise the value is normalized and matched against the allowlist,
// returning the normalized origin, ErrInvalidOrigin, or ErrOriginNotAllowed.
func (a *Allowlist) Validate(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	normalized, err := Normalize(raw)
	if err != nil {
		return "", err
	}
	if !a.contains(normalized) {
		return "", fmt.Errorf("%w: %s", ErrOriginNotAllowed, normalized)
	}
	return normalized, nil
}

// ValidateReturnURL checks an OAuth state returnUrl. Empty values and same-site relative
// paths (a single leading "/", not "//") are allowed. Absolute URLs must have an
// allowlisted origin. Anything else is rejected with ErrInvalidReturnURL.
func (a *Allowlist) ValidateReturnURL(raw string) error {
	// Browsers strip ASCII tab, LF, and CR from anywhere in a URL before navigating,
	// which can turn "/\t/evil.com" into the protocol-relative "//evil.com". Mirror
	// that removal so validation sees the same authority the browser will act on.
	cleaned := strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return -1
		}
		return r
	}, raw)
	trimmed := strings.TrimSpace(cleaned)
	if trimmed == "" {
		return nil
	}
	if strings.HasPrefix(trimmed, "/") {
		// Browsers normalize backslashes to slashes, so "/\host" navigates like "//host".
		if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "/\\") {
			return fmt.Errorf("%w: protocol-relative URLs are not allowed", ErrInvalidReturnURL)
		}
		return nil
	}
	u, err := url.Parse(trimmed)
	if err != nil || !u.IsAbs() {
		return fmt.Errorf("%w: must be a relative path or an absolute URL", ErrInvalidReturnURL)
	}
	normalized, err := Normalize(u.Scheme + "://" + u.Host)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidReturnURL, err)
	}
	if !a.contains(normalized) {
		return fmt.Errorf("%w: origin %s is not allowed", ErrInvalidReturnURL, normalized)
	}
	return nil
}
