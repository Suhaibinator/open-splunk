package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileSortKeepsAbsentValuesLastInBothDirections(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		source        string
		presenceOrder string
		valueOrder    string
		nullPlacement string
	}{
		{
			name: "ascending", source: `index=gradethis | sort 0 + status`,
			presenceOrder: "ASC", valueOrder: "ASC", nullPlacement: "LAST",
		},
		{
			name: "descending", source: `index=gradethis | sort 0 - status`,
			presenceOrder: "ASC", valueOrder: "DESC", nullPlacement: "LAST",
		},
		{
			name:          "tail reverses complete established order",
			source:        `index=gradethis | sort 0 + status | tail 3`,
			presenceOrder: "DESC", valueOrder: "DESC", nullPlacement: "FIRST",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			order := compiled.SQL[strings.LastIndex(compiled.SQL, "ORDER BY "):]
			for _, required := range []string{
				`tupleElement("__os_order_2_0", 1) ` + test.presenceOrder + ` NULLS ` + test.nullPlacement,
				`tupleElement("__os_order_2_0", 2) ` + test.valueOrder + ` NULLS ` + test.nullPlacement,
			} {
				if !strings.Contains(order, required) {
					t.Fatalf("final order missing %q:\n%s", required, order)
				}
			}
		})
	}
}

func TestCompileSortValueModesUseDistinctTotalOrderKeys(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		required  []string
		forbidden []string
	}{
		{
			name: "lexical", source: `index=gradethis | sort 0 str(value)`,
			required:  []string{`dynamicType("__os_fields"."value")`, `__os_sort_string_member`},
			forbidden: []string{`toIPv4OrNull(`, `__os_sort_numeric_key`},
		},
		{
			name: "numeric", source: `index=gradethis | sort 0 num(value)`,
			required:  []string{`__os_sort_numeric_key`, `__os_exact_order_text`, `translate(`},
			forbidden: []string{`toIPv4OrNull(`},
		},
		{
			name: "ip", source: `index=gradethis | sort 0 ip(value)`,
			required:  []string{`toIPv4OrNull(`, `toIPv6OrNull(`, `IPv4ToIPv6(`},
			forbidden: []string{`__os_sort_leading_text`},
		},
		{
			name: "auto", source: `index=gradethis | sort 0 auto(value)`,
			required: []string{`__os_sort_exact_key`, `__os_sort_ip_key`, `__os_sort_leading_text`, `extract(`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%s sort SQL missing %q:\n%s", test.name, required, compiled.SQL)
				}
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("%s sort SQL contains %q:\n%s", test.name, forbidden, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("%s placeholders = %d, args = %d", test.name, got, want)
			}
		})
	}
}

func TestCompileSortSupportsNativeAndRuntimeMultivalueFields(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		required []string
	}{
		{
			name:     "native dynamic array",
			source:   `index=gradethis | eval values=mvappend("10","2") | sort 0 values | table event_id values`,
			required: []string{`arrayMap(__os_sort_mv_member ->`, `__os_sort_leading_text`},
		},
		{
			name:     "fixed aggregate string array",
			source:   `index=gradethis | stats list(user) AS users BY service | sort 0 str(users)`,
			required: []string{`arrayMap(__os_sort_mv_member ->`, `((notEmpty("users")) AND isNotNull("users"))`},
		},
		{
			name:     "runtime event array",
			source:   `index=gradethis | sort 0 runtime_values | table event_id runtime_values`,
			required: []string{`dynamicType("__os_fields"."runtime_values") = 'Array(String)'`, `'Array(Dynamic)'`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("multivalue sort SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
		})
	}
}

func TestCompileSortRejectsUnknownPlannerValueMode(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | sort value`)
	for _, operator := range logical.Operators {
		if sortOperator, ok := operator.(*plan.Sort); ok {
			sortOperator.Keys[0].Mode = plan.SortValueMode(255)
		}
	}
	if _, err := (Compiler{}).Compile(logical); err == nil ||
		!strings.Contains(err.Error(), "invalid value mode 255") {
		t.Fatalf("Compile() error = %v, want invalid sort mode", err)
	}
}

func TestAutoSortKeyAvoidsDriverBindMetacharacters(t *testing.T) {
	t.Parallel()

	key := autoSortOrderingKeySQL("numeric_text", "lexical_text")
	if strings.ContainsAny(key, "?{}") {
		t.Fatalf("auto sort key contains driver bind metacharacters: %s", key)
	}
}
