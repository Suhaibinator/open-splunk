package authz

import (
	"slices"
	"testing"
)

func TestCapabilitySetRejectsNonCanonicalMembers(t *testing.T) {
	t.Parallel()

	set, ok := NewCapabilitySet(CapabilitySearch, Capability("search.everything"))
	if ok || !set.Empty() {
		t.Fatalf("NewCapabilitySet accepted a non-canonical id: (%v, %t)", set.Strings(), ok)
	}
	if CapabilitiesFor(OrgRoleOwner, true).Has(Capability("root")) {
		t.Fatal("a non-canonical capability was reported held")
	}
	if (CapabilitySet{}).Has(CapabilitySearch) {
		t.Fatal("the zero set holds a capability")
	}
}

func TestCapabilitySetIsImmutable(t *testing.T) {
	t.Parallel()

	member := CapabilitiesFor(OrgRoleMember, false)
	capabilities := member.Capabilities()
	capabilities[0] = CapabilityServerAdmin
	strs := member.Strings()
	strs[0] = "server.admin"
	all := AllCapabilities()
	all[0] = CapabilityOrgsAdmin

	again := CapabilitiesFor(OrgRoleMember, false)
	if again != member || again.Has(CapabilityServerAdmin) {
		t.Fatalf("mutating returned slices changed the role table: %v", again.Strings())
	}
	if !slices.Equal(again.Strings(), []string{"search", "objects.own"}) {
		t.Fatalf("member set = %v", again.Strings())
	}
	if AllCapabilities()[0] != CapabilitySearch {
		t.Fatal("mutating AllCapabilities result changed the canonical order")
	}
}

func TestCapabilitySetPreservesCanonicalOrder(t *testing.T) {
	t.Parallel()

	set, ok := NewCapabilitySet(CapabilityOrgsAdmin, CapabilitySearch, CapabilityAuditRead, CapabilitySearch)
	if !ok {
		t.Fatal("canonical capabilities rejected")
	}
	if got := set.Strings(); !slices.Equal(got, []string{"search", "audit.read", "orgs.admin"}) {
		t.Fatalf("Strings() = %v", got)
	}
	if set.Len() != 3 {
		t.Fatalf("Len() = %d", set.Len())
	}
}
