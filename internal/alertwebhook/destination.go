package alertwebhook

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ParseDestination validates URL syntax without performing network I/O. DNS
// policy is deliberately re-evaluated immediately before every delivery.
func ParseDestination(rawURL string) (ParsedDestination, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed == nil {
		return ParsedDestination{}, fmt.Errorf("%w: webhook URL is invalid", ErrInvalidArgument)
	}
	if parsed.Scheme != "https" || parsed.Opaque != "" {
		return ParsedDestination{}, fmt.Errorf("%w: webhook URL must use HTTPS", ErrInvalidArgument)
	}
	if parsed.User != nil {
		return ParsedDestination{}, fmt.Errorf("%w: webhook URL credentials are forbidden", ErrInvalidArgument)
	}
	if parsed.Fragment != "" {
		return ParsedDestination{}, fmt.Errorf("%w: webhook URL fragments are forbidden", ErrInvalidArgument)
	}
	hostname, err := canonicalHostname(parsed.Hostname())
	if err != nil {
		return ParsedDestination{}, err
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	} else if number, conversionErr := strconv.ParseUint(port, 10, 16); conversionErr != nil || number == 0 {
		return ParsedDestination{}, fmt.Errorf("%w: webhook URL port is invalid", ErrInvalidArgument)
	}
	return ParsedDestination{URL: parsed, Hostname: hostname, Port: port}, nil
}

func ResolveDestination(ctx context.Context, resolver Resolver, rawURL string, policy DestinationPolicy) (ResolvedDestination, error) {
	parsed, err := ParseDestination(rawURL)
	if err != nil {
		return ResolvedDestination{}, err
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", parsed.Hostname)
	if err != nil {
		if ctx.Err() != nil {
			return ResolvedDestination{}, ctx.Err()
		}
		return ResolvedDestination{}, &DeliveryError{Category: DeliveryDNSFailure}
	}
	if len(addresses) == 0 {
		return ResolvedDestination{}, &DeliveryError{Category: DeliveryDNSFailure}
	}
	allowPrivate := privateHostAllowed(parsed.Hostname, policy.PrivateHostAllowlist)
	validated := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !address.IsValid() || address.IsUnspecified() || address.IsMulticast() {
			return ResolvedDestination{}, &DeliveryError{Category: DeliveryDestinationRejected}
		}
		if !publicAddress(address) && !allowPrivate {
			return ResolvedDestination{}, &DeliveryError{Category: DeliveryDestinationRejected}
		}
		validated = append(validated, address)
	}
	sort.Slice(validated, func(left, right int) bool {
		return validated[left].Compare(validated[right]) < 0
	})
	return ResolvedDestination{ParsedDestination: parsed, Address: validated[0]}, nil
}

func canonicalHostname(hostname string) (string, error) {
	hostname = strings.ToLower(strings.TrimSuffix(hostname, "."))
	if hostname == "" || len(hostname) > 253 || !utf8.ValidString(hostname) {
		return "", fmt.Errorf("%w: webhook URL hostname is required", ErrInvalidArgument)
	}
	for _, character := range hostname {
		if character > 127 || character == '\x00' {
			return "", fmt.Errorf("%w: webhook URL hostname must be ASCII", ErrInvalidArgument)
		}
	}
	return hostname, nil
}

func privateHostAllowed(hostname string, configured []string) bool {
	for _, candidate := range configured {
		canonical, err := canonicalHostname(candidate)
		if err == nil && canonical == hostname {
			return true
		}
	}
	return false
}

func ValidateDestinationPolicy(policy DestinationPolicy) error {
	for _, candidate := range policy.PrivateHostAllowlist {
		if _, err := canonicalHostname(candidate); err != nil {
			return fmt.Errorf("%w: private webhook host allowlist contains an invalid hostname", ErrInvalidArgument)
		}
	}
	return nil
}

func publicAddress(address netip.Addr) bool {
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	// Reject IPv4-compatible, NAT64, Teredo, and 6to4 addresses even when the
	// outer IPv6 address is global unicast. Their embedded IPv4 destination can
	// otherwise cross the private-address boundary after validation.
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}
