package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileStatsQuotedInputsGroupsAndLiteralOutputs(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats avg('Product Name') AS "Revenue" `+
			`sparkline(sum('request-bytes'),30m) AS ".com" BY 'HTTP Status'`,
	)
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"HTTP Status", "Revenue", ".com"},
	) {
		t.Fatalf("output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		`AS "Revenue"`,
		`AS ".com"`,
		`AS "__os_group_0"`,
		statsSparklineMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("quoted stats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, argument := range []any{"HTTP Status", "Product Name", "request-bytes"} {
		if !slices.Contains(compiled.Args, argument) {
			t.Fatalf("quoted field argument %q is missing: %#v", argument, compiled.Args)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsImplicitEvalOutputKeepsAuthoredInvocation(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats SuM(eval(bytes * price)) count(eval(status=500))`,
	)
	want := []string{
		`SuM(eval(bytes * price))`,
		`count(eval(status=500))`,
	}
	if !slices.Equal(compiled.OutputFields, want) {
		t.Fatalf("output fields = %#v, want %#v", compiled.OutputFields, want)
	}
	for _, output := range want {
		if !strings.Contains(compiled.SQL, " AS "+quoteIdentifier(output)) {
			t.Fatalf("implicit eval output %q is missing:\n%s", output, compiled.SQL)
		}
	}
}

func TestCompileStatsLiteralOutputCanBeReferencedDownstream(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count AS ".com" | where isnotnull('.com') | table '.com'`,
	)
	if !slices.Equal(compiled.OutputFields, []string{".com"}) {
		t.Fatalf("literal downstream output fields = %#v", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `".com"`) {
		t.Fatalf("literal downstream field was not resolved by exact visible name:\n%s", compiled.SQL)
	}
}
