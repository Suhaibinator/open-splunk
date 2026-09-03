package clickhouse

import (
	"strconv"
	"strings"
	"testing"
)

func TestCompileDynamicNumericStringEqualityIsConsistent(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "base search",
			source: `index=gradethis runtime_numeric_text=100`,
		},
		{
			name:   "pipeline search",
			source: `index=gradethis | search runtime_numeric_text=100`,
		},
		{
			name:   "where",
			source: `index=gradethis | where runtime_numeric_text=100`,
		},
		{
			name:   "eval",
			source: `index=gradethis | eval matched=if(runtime_numeric_text=100, "yes", "no") | table matched`,
		},
		{
			name:   "singleton IN",
			source: `index=gradethis | where runtime_numeric_text IN (100)`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			for _, required := range []string{
				"__os_exact_order_text",
				"__os_exact_order_bounded",
				"isValidUTF8(",
				"<= " + strconv.Itoa(MaximumExactNumericOrderingInputTextBytes),
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("Dynamic numeric-String equality SQL missing %q:\n%s", required, compiled.SQL)
				}
			}

			stringEligibility := []string{
				`dynamicType("__os_fields"."runtime_numeric_text") = 'String'`,
				`dynamicType(__os_membership_left) = 'String'`,
				`__os_membership_left_type = 'String'`,
				`dynamicType(left_value) = 'String'`,
			}
			eligible := false
			for _, guard := range stringEligibility {
				eligible = eligible || strings.Contains(compiled.SQL, guard)
			}
			if !eligible {
				t.Fatalf("Dynamic numeric equality has no String eligibility guard:\n%s", compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", got, want, compiled.Args, compiled.SQL)
			}
		})
	}
}

func TestCompileDynamicQuotedStringEqualityRemainsStringOnly(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | where runtime_numeric_text="100"`,
	)
	for _, required := range []string{
		`dynamicType("__os_fields"."runtime_numeric_text") = 'String'`,
		`dynamicElement("__os_fields"."runtime_numeric_text", 'String') = CAST(? AS String)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("quoted Dynamic String equality SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"__os_exact_order_",
		`startsWith(dynamicType("__os_fields"."runtime_numeric_text"), 'Float')`,
		`dynamicType("__os_fields"."runtime_numeric_text") IN ('Int8'`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("quoted Dynamic String equality retained numeric path %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", got, want, compiled.Args, compiled.SQL)
	}
}
