package alertwebhook

import (
	"context"
	"errors"
	"net/netip"
	"testing"
)

type staticResolver struct {
	addresses []netip.Addr
	err       error
}

func (resolver staticResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), resolver.addresses...), resolver.err
}

func TestParseDestinationRejectsUnsafeURLs(t *testing.T) {
	t.Parallel()
	unsafe := []string{
		"http://hooks.example.com/path",
		"https://user:secret@hooks.example.com/path",
		"https://hooks.example.com/path#fragment",
		"https:///missing-host",
		"https://hooks.éxample/path",
		"https://hooks.example.com:0/path",
	}
	for _, rawURL := range unsafe {
		t.Run(rawURL, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseDestination(rawURL); err == nil {
				t.Fatal("ParseDestination() accepted an unsafe URL")
			}
		})
	}
}

func TestResolveDestinationPublicAndPrivatePolicy(t *testing.T) {
	t.Parallel()
	public := staticResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}}
	resolved, err := ResolveDestination(context.Background(), public, "https://hooks.example.com/path", DestinationPolicy{})
	if err != nil {
		t.Fatalf("ResolveDestination(public) error = %v", err)
	}
	if resolved.Address.String() != "8.8.8.8" || resolved.Port != "443" {
		t.Fatalf("resolved = %#v", resolved)
	}

	private := staticResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	if _, err := ResolveDestination(context.Background(), private, "https://internal.example.com/hook", DestinationPolicy{}); err == nil {
		t.Fatal("ResolveDestination() accepted private DNS without an allowlist")
	}
	allowed, err := ResolveDestination(
		context.Background(),
		private,
		"https://internal.example.com/hook",
		DestinationPolicy{PrivateHostAllowlist: []string{"INTERNAL.EXAMPLE.COM."}},
	)
	if err != nil {
		t.Fatalf("ResolveDestination(allowlisted) error = %v", err)
	}
	if allowed.Address.String() != "127.0.0.1" {
		t.Fatalf("allowed address = %s", allowed.Address)
	}
}

func TestResolveDestinationRejectsMixedDNSAnswers(t *testing.T) {
	t.Parallel()
	resolver := staticResolver{addresses: []netip.Addr{
		netip.MustParseAddr("8.8.8.8"),
		netip.MustParseAddr("169.254.169.254"),
	}}
	if _, err := ResolveDestination(context.Background(), resolver, "https://hooks.example.com/path", DestinationPolicy{}); err == nil {
		t.Fatal("ResolveDestination() accepted a mixed public/link-local answer")
	}
}

func TestPublicAddressRejectsReservedRanges(t *testing.T) {
	t.Parallel()
	addresses := []string{
		"10.0.0.1", "100.64.0.1", "192.0.2.1", "198.18.0.1", "203.0.113.1",
		"2001:db8::1", "::1", "::7f00:1", "64:ff9b::a9fe:a9fe", "2001::1", "2002:7f00:1::",
	}
	for _, raw := range addresses {
		if publicAddress(netip.MustParseAddr(raw)) {
			t.Fatalf("publicAddress(%s) = true", raw)
		}
	}
}

func TestResolveDestinationRejectsIPv4TransitionAddresses(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"::7f00:1", "64:ff9b::a9fe:a9fe", "2001::1", "2002:7f00:1::"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			resolver := staticResolver{addresses: []netip.Addr{netip.MustParseAddr(raw)}}
			if _, err := ResolveDestination(
				context.Background(), resolver, "https://hooks.example.com/path", DestinationPolicy{},
			); err == nil {
				t.Fatal("ResolveDestination() accepted an IPv4 transition address")
			}
		})
	}
}

func TestPublicAddressSRv6AllocationBoundary(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		address string
		public  bool
	}{
		{address: "5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", public: true},
		{address: "5f00::"},
		{address: "5f00::1"},
		{address: "5F00:1234:5678:9ABC:DEF0:0000:0000:0001"},
		{address: "5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
		{address: "5f01::", public: true},
		{address: "2606:4700:4700::1111", public: true},
	} {
		t.Run(test.address, func(t *testing.T) {
			t.Parallel()
			if got := publicAddress(netip.MustParseAddr(test.address)); got != test.public {
				t.Fatalf("publicAddress(%s) = %v; want %v", test.address, got, test.public)
			}
		})
	}
}

func TestResolveDestinationSRv6Policy(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		url       string
		addresses []netip.Addr
	}{
		{
			name: "DNS", url: "https://hooks.example.com/path",
			addresses: []netip.Addr{netip.MustParseAddr("5f00::1")},
		},
		{
			name: "literal", url: "https://[5f00::1]/path",
			addresses: []netip.Addr{netip.MustParseAddr("5f00::1")},
		},
		{
			name: "expanded literal", url: "https://[5F00:0000:0000:0000:0000:0000:0000:0001]/path",
			addresses: []netip.Addr{netip.MustParseAddr("5F00:0000:0000:0000:0000:0000:0000:0001")},
		},
		{
			name: "mixed DNS", url: "https://hooks.example.com/path",
			addresses: []netip.Addr{netip.MustParseAddr("2606:4700:4700::1111"), netip.MustParseAddr("5f00::1")},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resolver := staticResolver{addresses: test.addresses}
			_, err := ResolveDestination(context.Background(), resolver, test.url, DestinationPolicy{})
			var deliveryError *DeliveryError
			if !errors.As(err, &deliveryError) || deliveryError.Category != DeliveryDestinationRejected {
				t.Fatalf("ResolveDestination() error = %v; want destination rejected", err)
			}
		})
	}
	resolver := staticResolver{addresses: []netip.Addr{netip.MustParseAddr("5f00::1")}}
	allowed, err := ResolveDestination(context.Background(), resolver, "https://hooks.example.com/path", DestinationPolicy{
		PrivateHostAllowlist: []string{"HOOKS.EXAMPLE.COM."},
	})
	if err != nil || allowed.Address != resolver.addresses[0] {
		t.Fatalf("ResolveDestination(allowlisted) = %#v, %v", allowed, err)
	}
}

func TestValidateDestinationPolicyRejectsInvalidEntries(t *testing.T) {
	t.Parallel()
	if err := ValidateDestinationPolicy(DestinationPolicy{PrivateHostAllowlist: []string{""}}); err == nil {
		t.Fatal("ValidateDestinationPolicy() accepted an empty host")
	}
	if err := ValidateDestinationPolicy(DestinationPolicy{PrivateHostAllowlist: []string{"internal.example.com"}}); err != nil {
		t.Fatalf("ValidateDestinationPolicy() error = %v", err)
	}
}
