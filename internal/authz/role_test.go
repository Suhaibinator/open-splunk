package authz

import (
	"slices"
	"testing"
)

// minimumTier spells every canonical id literally so a renamed constant fails
// here instead of silently drifting from the TypeScript client.
type tier int

const (
	tierMember tier = iota + 1
	tierModerator
	tierAdmin
	tierOwner
	tierSite
)

var expectedMinimumTier = map[string]tier{
	"search":              tierMember,
	"objects.own":         tierMember,
	"knowledge.write":     tierModerator,
	"lookups.write":       tierModerator,
	"alerts.write":        tierModerator,
	"reports.write":       tierModerator,
	"indexes.admin":       tierAdmin,
	"ingest_tokens.admin": tierAdmin,
	"collectors.admin":    tierAdmin,
	"hec.admin":           tierAdmin,
	"apps.admin":          tierAdmin,
	"audit.read":          tierAdmin,
	"knowledge.admin":     tierAdmin,
	"members.admin":       tierAdmin,
	"org.admin":           tierOwner,
	"server.admin":        tierSite,
	"users.admin":         tierSite,
	"orgs.admin":          tierSite,
}

var roleTiers = map[OrgRole]tier{
	OrgRoleMember:    tierMember,
	OrgRoleModerator: tierModerator,
	OrgRoleAdmin:     tierAdmin,
	OrgRoleOwner:     tierOwner,
}

func TestCanonicalCapabilityIDs(t *testing.T) {
	t.Parallel()

	all := AllCapabilities()
	if len(all) != len(expectedMinimumTier) {
		t.Fatalf("capability count = %d, want %d", len(all), len(expectedMinimumTier))
	}
	seen := make(map[Capability]struct{}, len(all))
	for _, capability := range all {
		if _, ok := expectedMinimumTier[string(capability)]; !ok {
			t.Errorf("unexpected capability id %q", capability)
		}
		if _, duplicate := seen[capability]; duplicate {
			t.Errorf("duplicate capability id %q", capability)
		}
		seen[capability] = struct{}{}
		if !capability.Valid() {
			t.Errorf("%q is not valid", capability)
		}
		parsed, ok := ParseCapability(string(capability))
		if !ok || parsed != capability {
			t.Errorf("ParseCapability(%q) = (%q, %t)", capability, parsed, ok)
		}
	}
	for _, invalid := range []string{"", "Search", "search ", "admin", "server.Admin", "objects"} {
		if _, ok := ParseCapability(invalid); ok {
			t.Errorf("ParseCapability(%q) accepted a non-canonical id", invalid)
		}
		if Capability(invalid).Valid() {
			t.Errorf("Capability(%q).Valid() = true", invalid)
		}
	}
}

func TestRoleCapabilityMatrix(t *testing.T) {
	t.Parallel()

	for role, roleTier := range roleTiers {
		for id, minimum := range expectedMinimumTier {
			t.Run(role.String()+"/"+id, func(t *testing.T) {
				t.Parallel()
				got := CapabilitiesFor(role, false).Has(Capability(id))
				want := minimum <= roleTier
				if got != want {
					t.Fatalf("CapabilitiesFor(%s, false).Has(%q) = %t, want %t", role, id, got, want)
				}
			})
		}
	}
}

func TestRoleCapabilitySetsAreExact(t *testing.T) {
	t.Parallel()

	want := map[OrgRole][]string{
		OrgRoleMember: {"search", "objects.own"},
		OrgRoleModerator: {
			"search", "objects.own",
			"knowledge.write", "lookups.write", "alerts.write", "reports.write",
		},
		OrgRoleAdmin: {
			"search", "objects.own",
			"knowledge.write", "lookups.write", "alerts.write", "reports.write",
			"indexes.admin", "ingest_tokens.admin", "collectors.admin", "hec.admin",
			"apps.admin", "audit.read", "knowledge.admin", "members.admin",
		},
		OrgRoleOwner: {
			"search", "objects.own",
			"knowledge.write", "lookups.write", "alerts.write", "reports.write",
			"indexes.admin", "ingest_tokens.admin", "collectors.admin", "hec.admin",
			"apps.admin", "audit.read", "knowledge.admin", "members.admin",
			"org.admin",
		},
	}
	for role, ids := range want {
		got := CapabilitiesFor(role, false).Strings()
		if !slices.Equal(got, ids) {
			t.Errorf("CapabilitiesFor(%s, false) = %v, want %v", role, got, ids)
		}
	}
}

func TestRoleTiersAreStrictlyNested(t *testing.T) {
	t.Parallel()

	previous := CapabilitySet{}
	for _, role := range []OrgRole{OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner} {
		current := CapabilitiesFor(role, false)
		if !previous.IsSubsetOf(current) || previous.Len() >= current.Len() {
			t.Fatalf("%s (%v) does not strictly extend the tier below (%v)", role, current.Strings(), previous.Strings())
		}
		previous = current
	}
}

func TestNoOrganizationRoleHoldsSiteCapabilities(t *testing.T) {
	t.Parallel()

	for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner, OrgRole(200)} {
		set := CapabilitiesFor(role, false)
		for _, site := range []Capability{CapabilityServerAdmin, CapabilityUsersAdmin, CapabilityOrgsAdmin} {
			if set.Has(site) {
				t.Errorf("%s holds site capability %q", role, site)
			}
		}
	}
}

func TestSiteAdminHoldsEveryCapabilityInAnyRole(t *testing.T) {
	t.Parallel()

	all, ok := NewCapabilitySet(AllCapabilities()...)
	if !ok {
		t.Fatal("canonical capabilities rejected")
	}
	for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner, OrgRole(200)} {
		site := CapabilitiesFor(role, true)
		if site != all {
			t.Errorf("CapabilitiesFor(%s, true) = %v, want every capability", role, site.Strings())
		}
		if role.Valid() && !CapabilitiesFor(role, false).IsSubsetOf(site) {
			t.Errorf("site admin set is not a superset of %s", role)
		}
		owner := CapabilitiesFor(OrgRoleOwner, false)
		if !owner.IsSubsetOf(site) || owner == site {
			t.Errorf("site admin set is not a strict superset of owner")
		}
	}
}

func TestInvalidRolesGrantNothing(t *testing.T) {
	t.Parallel()

	for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleOwner + 1, OrgRole(255)} {
		if role.Valid() {
			t.Errorf("%d reported valid", role)
		}
		if set := CapabilitiesFor(role, false); !set.Empty() {
			t.Errorf("CapabilitiesFor(%d, false) = %v, want empty", role, set.Strings())
		}
	}
}

func TestOrgRoleNames(t *testing.T) {
	t.Parallel()

	for name, role := range map[string]OrgRole{
		"member":    OrgRoleMember,
		"moderator": OrgRoleModerator,
		"admin":     OrgRoleAdmin,
		"owner":     OrgRoleOwner,
	} {
		if role.String() != name {
			t.Errorf("%d.String() = %q, want %q", role, role.String(), name)
		}
		parsed, ok := ParseOrgRole(name)
		if !ok || parsed != role {
			t.Errorf("ParseOrgRole(%q) = (%s, %t)", name, parsed, ok)
		}
	}
	for _, invalid := range []string{"", "unspecified", "Member", "ADMIN", "site_admin", " owner"} {
		if role, ok := ParseOrgRole(invalid); ok || role != OrgRoleUnspecified {
			t.Errorf("ParseOrgRole(%q) = (%s, %t), want rejection", invalid, role, ok)
		}
	}
	if got := OrgRole(99).String(); got != "unspecified" {
		t.Errorf("out-of-range role string = %q", got)
	}
}
