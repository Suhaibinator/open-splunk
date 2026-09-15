package clickhouse

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompileSearchCIDRUsesSubnetMembership(t *testing.T) {
	t.Parallel()
	// Search equality with CIDR notation matches addresses in the subnet:
	// https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.1/search-commands/search
	for _, source := range []string{
		`index=gradethis ip="192.0.2.56/24"`,
		`index=gradethis | search ip="2001:db8::/32"`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, source)
			if !strings.Contains(compiled.SQL, "isIPAddressInRange") {
				t.Fatalf("SQL does not use CIDR membership:\n%s", compiled.SQL)
			}
			want := "192.0.2.0/24"
			if strings.Contains(source, "2001:") {
				want = "2001:db8::/32"
			}
			found := false
			for _, argument := range compiled.Args {
				found = found || reflect.DeepEqual(argument, want)
			}
			if !found {
				t.Fatalf("args = %#v, want CIDR %q", compiled.Args, want)
			}
		})
	}
}

func TestCompileSearchCIDRNotEqualNegatesSubnetMembership(t *testing.T) {
	t.Parallel()
	compiled := compileSPL(t, `index=gradethis ip!="192.0.2.0/24"`)
	if !strings.Contains(compiled.SQL, "NOT ifNull") || !strings.Contains(compiled.SQL, "isIPAddressInRange") {
		t.Fatalf("SQL does not negate CIDR membership:\n%s", compiled.SQL)
	}
}

func TestCompileSearchMalformedCIDRRemainsTextEquality(t *testing.T) {
	t.Parallel()
	compiled := compileSPL(t, `index=gradethis ip="192.0.2.0/99"`)
	if strings.Contains(compiled.SQL, "isIPAddressInRange") {
		t.Fatalf("malformed CIDR unexpectedly uses membership:\n%s", compiled.SQL)
	}
}
