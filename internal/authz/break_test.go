package authz

import (
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"testing"
)

var allOrgRoles = []OrgRole{OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner}

// TestRouteAccessIsUpwardClosed walks every route: once a role may call it,
// every higher role and every site administrator may too, and the principal
// with no role may call nothing but the public bootstrap.
func TestRouteAccessIsUpwardClosed(t *testing.T) {
	t.Parallel()

	for _, path := range Routes() {
		requirement, _ := RouteRequirement(path)
		allowedBelow := false
		for _, role := range allOrgRoles {
			allowed := Allowed(CapabilitiesFor(role, false), requirement.Method, path)
			if allowedBelow && !allowed {
				t.Errorf("%s %s: allowed for a lower role but denied for %s", requirement.Method, path, role)
			}
			allowedBelow = allowedBelow || allowed
		}
		for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleOwner + 1, OrgRole(math.MaxUint8)} {
			allowed := Allowed(CapabilitiesFor(role, false), requirement.Method, path)
			if allowed != requirement.Public {
				t.Errorf("%s %s: role %d allowed = %t, want %t", requirement.Method, path, role, allowed, requirement.Public)
			}
		}
	}
}

// TestRoleGrantTiersPartitionTheVocabulary makes the per-tier grant lists the
// single source of the table: every capability is granted by exactly one tier,
// in canonical order, so a capability added to two tiers or to none fails.
func TestRoleGrantTiersPartitionTheVocabulary(t *testing.T) {
	t.Parallel()

	var concatenated []Capability
	for _, grants := range [][]Capability{memberGrants, moderatorGrants, adminGrants, ownerGrants, siteAdminGrants} {
		if len(grants) == 0 {
			t.Error("a role tier grants nothing new")
		}
		concatenated = append(concatenated, grants...)
	}
	if !slices.Equal(concatenated, AllCapabilities()) {
		t.Fatalf("tier grants %v are not the canonical vocabulary %v in order", concatenated, AllCapabilities())
	}
	if len(allCapabilities) > 32 {
		t.Fatalf("%d capabilities do not fit the uint32 bitset", len(allCapabilities))
	}
}

// TestSiteAdminBreakGlassIgnoresMembership: the site administrator's set does
// not depend on the role it holds in the active organization, so a site admin
// who is a plain member of one org and a stranger to another gets the same
// full set in both, and every capability-gated route is open to it.
func TestSiteAdminBreakGlassIgnoresMembership(t *testing.T) {
	t.Parallel()

	stranger := CapabilitiesFor(OrgRoleUnspecified, true)
	for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner, OrgRoleOwner + 1, OrgRole(math.MaxUint8)} {
		if got := CapabilitiesFor(role, true); got != stranger {
			t.Errorf("site admin as %d = %v, want %v", role, got.Strings(), stranger.Strings())
		}
	}
	if got, want := stranger.Len(), len(AllCapabilities()); got != want {
		t.Fatalf("site admin holds %d capabilities, want %d", got, want)
	}
	for _, path := range Routes() {
		requirement, _ := RouteRequirement(path)
		if !Allowed(stranger, requirement.Method, path) {
			t.Errorf("break-glass site admin denied %s %s", requirement.Method, path)
		}
	}
}

// TestCapabilitySetsCannotBeMutatedThroughResults hands each role's set out
// many times, mutates every slice it returns and every copy it holds, and
// checks that concurrent readers keep seeing the original table.
func TestCapabilitySetsCannotBeMutatedThroughResults(t *testing.T) {
	t.Parallel()

	baseline := make(map[OrgRole][]string, len(allOrgRoles))
	for _, role := range allOrgRoles {
		baseline[role] = CapabilitiesFor(role, false).Strings()
	}
	siteBaseline := CapabilitiesFor(OrgRoleUnspecified, true).Strings()

	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 200 {
				for _, role := range allOrgRoles {
					set := CapabilitiesFor(role, false)
					capabilities := set.Capabilities()
					for index := range capabilities {
						capabilities[index] = CapabilityOrgsAdmin
					}
					ids := set.Strings()
					for index := range ids {
						ids[index] = "server.admin"
					}
					// Extending a copy must not reach the table either.
					extended := set.with(CapabilityServerAdmin)
					if !extended.Has(CapabilityServerAdmin) {
						t.Error("with() did not extend its copy")
					}
				}
			}
		})
	}
	group.Wait()

	for _, role := range allOrgRoles {
		if got := CapabilitiesFor(role, false).Strings(); !slices.Equal(got, baseline[role]) {
			t.Errorf("%s set changed to %v, want %v", role, got, baseline[role])
		}
		if CapabilitiesFor(role, false).Has(CapabilityServerAdmin) {
			t.Errorf("%s gained server.admin", role)
		}
	}
	if got := CapabilitiesFor(OrgRoleUnspecified, true).Strings(); !slices.Equal(got, siteBaseline) {
		t.Errorf("site admin set changed to %v", got)
	}
}

func TestRouteRequirementResultsAreCopies(t *testing.T) {
	t.Parallel()

	before := Routes()
	requirement, ok := RouteRequirement("/api/server/settings/update")
	if !ok {
		t.Fatal("server settings route missing")
	}
	requirement.Public = true
	requirement.Capability = ""
	routes := Routes()
	for index := range routes {
		routes[index] = "/api/system/bootstrap"
	}

	if Allowed(CapabilitiesFor(OrgRoleOwner, false), http.MethodPost, "/api/server/settings/update") {
		t.Fatal("mutating a returned Requirement opened a site-admin route to owners")
	}
	if again, _ := RouteRequirement("/api/server/settings/update"); again.Public || again.Capability != CapabilityServerAdmin {
		t.Fatalf("route table changed: %+v", again)
	}
	if !slices.Equal(Routes(), before) {
		t.Fatal("mutating the Routes result changed later results")
	}
}

// TestNonCanonicalCapabilitiesAreNeverHeld covers forged ids reaching Has and
// Allowed: case variants, padding, the empty id, and prefixes of real ids.
func TestNonCanonicalCapabilitiesAreNeverHeld(t *testing.T) {
	t.Parallel()

	site := CapabilitiesFor(OrgRoleOwner, true)
	for _, forged := range []Capability{"", " ", "SEARCH", "search\x00", "server", "server.admin.", "orgs.admin ", "ingest_tokens", "ingestion.admin", "*"} {
		if site.Has(forged) {
			t.Errorf("site admin set holds forged capability %q", forged)
		}
		if _, ok := NewCapabilitySet(forged); ok {
			t.Errorf("NewCapabilitySet accepted forged capability %q", forged)
		}
	}
	if set, ok := NewCapabilitySet(); !ok || !set.Empty() {
		t.Fatal("NewCapabilitySet() is not the empty set")
	}
}

// TestOrgRoleFromGoCommonBands uses go-common's literal OrganizationRole enum
// numbers (proto/organization.proto): four bands of eleven tiers from 1.
func TestOrgRoleFromGoCommonBands(t *testing.T) {
	t.Parallel()

	cases := []struct {
		value int32
		want  OrgRole
	}{
		{0, OrgRoleUnspecified}, // ORGANIZATION_ROLE_UNSPECIFIED
		{1, OrgRoleMember},      // MEMBER_TIER_0
		{2, OrgRoleMember},      // MEMBER_TIER_1, which a raw OrgRole(2) cast reads as moderator
		{3, OrgRoleMember},
		{4, OrgRoleMember},
		{11, OrgRoleMember},    // MEMBER_TIER_10
		{12, OrgRoleModerator}, // MODERATOR_TIER_0
		{22, OrgRoleModerator}, // MODERATOR_TIER_10
		{23, OrgRoleAdmin},     // ADMIN_TIER_0
		{33, OrgRoleAdmin},     // ADMIN_TIER_10
		{34, OrgRoleOwner},     // OWNER_TIER_0
		{44, OrgRoleOwner},     // OWNER_TIER_10
		{45, OrgRoleUnspecified},
		{100, OrgRoleUnspecified},
		{-1, OrgRoleUnspecified},
		{math.MinInt32, OrgRoleUnspecified},
		{math.MaxInt32, OrgRoleUnspecified},
	}
	for _, testCase := range cases {
		if got := OrgRoleFromGoCommon(testCase.value); got != testCase.want {
			t.Errorf("OrgRoleFromGoCommon(%d) = %s, want %s", testCase.value, got, testCase.want)
		}
	}
	// Every in-range value maps to a valid role, monotonically.
	previous := OrgRoleMember
	for value := int32(1); value <= 44; value++ {
		role := OrgRoleFromGoCommon(value)
		if !role.Valid() || role < previous {
			t.Fatalf("OrgRoleFromGoCommon(%d) = %s after %s", value, role, previous)
		}
		previous = role
	}
	// Unknown values fail closed all the way to the capability set.
	for _, value := range []int32{0, -1, 45, math.MaxInt32} {
		if set := CapabilitiesFor(OrgRoleFromGoCommon(value), false); !set.Empty() {
			t.Errorf("go-common role %d grants %v", value, set.Strings())
		}
	}
}

// TestSiteAdminFromGoCommonUserType uses go-common's literal UserType enum
// numbers (proto/user.proto): only the Admin band (34-44) is a site admin.
func TestSiteAdminFromGoCommonUserType(t *testing.T) {
	t.Parallel()

	for value := int32(-2); value <= 50; value++ {
		want := value >= 34 && value <= 44
		if got := SiteAdminFromGoCommonUserType(value); got != want {
			t.Errorf("SiteAdminFromGoCommonUserType(%d) = %t, want %t", value, got, want)
		}
	}
	for _, value := range []int32{math.MinInt32, math.MaxInt32} {
		if SiteAdminFromGoCommonUserType(value) {
			t.Errorf("SiteAdminFromGoCommonUserType(%d) = true", value)
		}
	}
}

// TestTypeScriptCapabilityTableMatches compares the browser's copy of the role
// and capability table in lib/api/identity.ts with this package. It skips
// only while that file is absent from the checkout.
func TestTypeScriptCapabilityTableMatches(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "lib", "api", "identity.ts")
	source, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("%s is not in this checkout", path)
	}
	if err != nil {
		t.Fatal(err)
	}

	want := map[string][]Capability{
		"MEMBER_CAPABILITIES":    memberGrants,
		"MODERATOR_CAPABILITIES": moderatorGrants,
		"ADMIN_CAPABILITIES":     adminGrants,
		"OWNER_CAPABILITIES":     ownerGrants,
		"SITE_CAPABILITIES":      siteAdminGrants,
	}
	for name, grants := range want {
		got := typeScriptStringArray(t, source, name)
		ids := make([]string, len(grants))
		for index, capability := range grants {
			ids[index] = string(capability)
		}
		if !slices.Equal(got, ids) {
			t.Errorf("TypeScript %s = %v, Go = %v", name, got, ids)
		}
	}

	roles := typeScriptStringArray(t, source, "ORG_ROLES")
	var goRoles []string
	for _, role := range allOrgRoles {
		goRoles = append(goRoles, role.String())
	}
	if !slices.Equal(roles, goRoles) {
		t.Errorf("TypeScript ORG_ROLES = %v, Go = %v", roles, goRoles)
	}
	if bands := typeScriptStringArray(t, source, "USER_TYPE_BANDS"); len(bands) != goCommonBands || bands[goCommonSiteAdminBand] != "admin" {
		t.Errorf("TypeScript USER_TYPE_BANDS = %v, want %d bands with admin at %d", bands, goCommonBands, goCommonSiteAdminBand)
	}
	for name, value := range map[string]int{
		"GO_COMMON_BAND_WIDTH":  goCommonBandWidth,
		"GO_COMMON_BAND_ORIGIN": goCommonBandOrigin,
	} {
		match := regexp.MustCompile(`const ` + name + ` = (\d+);`).FindSubmatch(source)
		if match == nil {
			t.Errorf("TypeScript %s not found", name)
			continue
		}
		if got, _ := strconv.Atoi(string(match[1])); got != value {
			t.Errorf("TypeScript %s = %d, Go = %d", name, got, value)
		}
	}
}

func typeScriptStringArray(t *testing.T, source []byte, name string) []string {
	t.Helper()
	match := regexp.MustCompile(`export const ` + name + ` = \[([^\]]*)\] as const;`).FindSubmatch(source)
	if match == nil {
		t.Fatalf("TypeScript %s not found", name)
	}
	var result []string
	for _, item := range regexp.MustCompile(`"([^"]*)"`).FindAllSubmatch(match[1], -1) {
		result = append(result, string(item[1]))
	}
	return result
}
