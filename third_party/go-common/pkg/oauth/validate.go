package oauth

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

// MaxSlugLength bounds Provider.Slug; it matches the persisted column width.
const MaxSlugLength = 64

var slugPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// reservedAuthParams are authorization request parameters the runtime owns.
var reservedAuthParams = map[string]struct{}{
	"client_id":             {},
	"redirect_uri":          {},
	"response_type":         {},
	"scope":                 {},
	"state":                 {},
	"nonce":                 {},
	"code_challenge":        {},
	"code_challenge_method": {},
	"response_mode":         {},
}

// ValidSlug reports whether slug is a well-formed provider slug.
func ValidSlug(slug string) bool {
	return len(slug) <= MaxSlugLength && slugPattern.MatchString(slug)
}

// Validate checks the provider's structure. It does not contact the provider.
func (p Provider) Validate() error {
	if err := p.validate(); err != nil {
		return fmt.Errorf("%w %q: %s", ErrInvalidProvider, p.Slug, err)
	}
	return nil
}

func (p Provider) validate() error {
	if !ValidSlug(p.Slug) {
		return errors.New("slug must match ^[a-z0-9]+(-[a-z0-9]+)*$ and be at most 64 bytes")
	}
	if strings.TrimSpace(p.DisplayName) == "" {
		return errors.New("display name is required")
	}
	if p.ClientID == "" {
		return errors.New("client ID is required")
	}
	if p.Identity == nil {
		return errors.New("identity resolver is required")
	}
	if !p.AuthStyle.valid() {
		return fmt.Errorf("unknown auth style %s", p.AuthStyle)
	}
	if !p.ResponseMode.valid() {
		return fmt.Errorf("unknown response mode %s", p.ResponseMode)
	}
	if !p.EmailVerification.valid() {
		return fmt.Errorf("unknown email verification policy %s", p.EmailVerification)
	}

	hasEndpoints := p.Endpoints.Authorization != "" || p.Endpoints.Token != ""
	switch {
	case p.Issuer != "" && hasEndpoints:
		return errors.New("issuer and explicit endpoints are mutually exclusive")
	case p.Issuer != "":
		if err := validateProviderURL(p.Issuer, false); err != nil {
			return fmt.Errorf("issuer: %w", err)
		}
	default:
		if err := validateProviderURL(p.Endpoints.Authorization, true); err != nil {
			return fmt.Errorf("authorization endpoint: %w", err)
		}
		if err := validateProviderURL(p.Endpoints.Token, true); err != nil {
			return fmt.Errorf("token endpoint: %w", err)
		}
	}

	if err := p.validateScopes(); err != nil {
		return err
	}
	seenParams := make(map[string]struct{}, len(p.AuthParams))
	for _, param := range p.AuthParams {
		if param.Name == "" || strings.ContainsFunc(param.Name, unsafeRune) {
			return fmt.Errorf("auth param name %q is malformed", param.Name)
		}
		if _, reserved := reservedAuthParams[param.Name]; reserved {
			return fmt.Errorf("auth param %q is reserved", param.Name)
		}
		if _, dup := seenParams[param.Name]; dup {
			return fmt.Errorf("auth param %q is duplicated", param.Name)
		}
		seenParams[param.Name] = struct{}{}
	}

	if len(p.RedirectURLs) == 0 {
		return errors.New("at least one redirect URL is required")
	}
	seenRedirects := make(map[string]struct{}, len(p.RedirectURLs))
	for _, redirect := range p.RedirectURLs {
		if err := validateProviderURL(redirect, true); err != nil {
			return fmt.Errorf("redirect URL %q: %w", redirect, err)
		}
		if _, dup := seenRedirects[redirect]; dup {
			return fmt.Errorf("redirect URL %q is duplicated", redirect)
		}
		seenRedirects[redirect] = struct{}{}
	}

	if p.SyntheticEmailDomain != "" && !validDomain(p.SyntheticEmailDomain) {
		return fmt.Errorf("synthetic email domain %q must be a lowercase DNS name with at least two labels", p.SyntheticEmailDomain)
	}
	return nil
}

func (p Provider) validateScopes() error {
	seen := make(map[string]struct{}, len(p.Scopes))
	for _, scope := range p.Scopes {
		if scope == "" || strings.ContainsFunc(scope, unsafeRune) {
			return fmt.Errorf("scope %q is malformed", scope)
		}
		if _, dup := seen[scope]; dup {
			return fmt.Errorf("scope %q is duplicated", scope)
		}
		seen[scope] = struct{}{}
	}
	return nil
}

// AllowsRedirectURL reports whether redirectURL is in the exact allowlist.
func (p Provider) AllowsRedirectURL(redirectURL string) bool {
	for _, allowed := range p.RedirectURLs {
		if allowed == redirectURL {
			return true
		}
	}
	return false
}

// ValidateProviders validates every provider and the uniqueness of slugs and
// synthetic email domains across the set.
func ValidateProviders(providers []Provider) error {
	_, errs := ValidProviders(providers)
	return errors.Join(errs...)
}

// ValidProviders returns the providers that are individually valid and do not
// share a slug or synthetic email domain, plus one error per exclusion.
// Ambiguity fails closed: every provider involved in a collision is excluded.
func ValidProviders(providers []Provider) ([]Provider, []error) {
	var errs []error
	slugCount := make(map[string]int, len(providers))
	domainCount := make(map[string]int, len(providers))
	for _, p := range providers {
		slugCount[p.Slug]++
		if p.SyntheticEmailDomain != "" {
			domainCount[strings.ToLower(p.SyntheticEmailDomain)]++
		}
	}
	valid := make([]Provider, 0, len(providers))
	for _, p := range providers {
		if err := p.Validate(); err != nil {
			errs = append(errs, err)
			continue
		}
		if slugCount[p.Slug] > 1 {
			errs = append(errs, fmt.Errorf("%w: %q", ErrDuplicateSlug, p.Slug))
			continue
		}
		if p.SyntheticEmailDomain != "" && domainCount[p.SyntheticEmailDomain] > 1 {
			errs = append(errs, fmt.Errorf("%w: provider %q uses %q", ErrDuplicateSyntheticEmailDomain, p.Slug, p.SyntheticEmailDomain))
			continue
		}
		valid = append(valid, p)
	}
	return valid, errs
}

func unsafeRune(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }

// validateProviderURL requires an absolute https URL, or http on a loopback
// host so tests and local development can use plain listeners.
func validateProviderURL(raw string, allowQuery bool) error {
	if raw == "" {
		return errors.New("is required")
	}
	if strings.ContainsFunc(raw, unsafeRune) || strings.ContainsAny(raw, "\\#") {
		return errors.New("contains whitespace, control characters, a backslash, or a fragment")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is malformed: %w", err)
	}
	if parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" {
		return errors.New("must be an absolute URL with a host and no user info")
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if !loopbackHost(parsed.Hostname()) {
			return errors.New("must use https (http is allowed only on loopback hosts)")
		}
	default:
		return errors.New("must use https")
	}
	if !allowQuery && (parsed.RawQuery != "" || parsed.ForceQuery) {
		return errors.New("must not contain a query")
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validDomain(domain string) bool {
	if len(domain) > 253 {
		return false
	}
	labels := strings.Split(domain, ".")
	if len(labels) < 2 {
		return false
	}
	for _, label := range labels {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}
