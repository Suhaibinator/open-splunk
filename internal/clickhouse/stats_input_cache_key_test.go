package clickhouse

import (
	"strings"
	"testing"
)

func TestCompileStatsSeparatesFieldNamesFromExpressionCacheIdentity(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | stats min(@expression:0) AS direct min(eval(other)) AS calculated`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	for _, alias := range []string{
		`AS "__os_measure_extrema_0"`,
		`AS "__os_measure_extrema_1"`,
	} {
		if !strings.Contains(compiled.SQL, alias) {
			t.Fatalf("compiled SQL does not materialize distinct field/expression extrema input %q:\n%s", alias, compiled.SQL)
		}
	}
}
