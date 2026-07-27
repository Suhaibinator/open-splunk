package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEvalCoalesceFixedScalarsPreserveBindOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval selected=coalesce(null, replace("first", "i", "1"), "fallback") | table selected`,
	)
	for _, required := range []string{
		`coalesce(`,
		`CAST(NULL AS Nullable(String))`,
		`replaceRegexpAll(`,
		`AS "selected"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("coalesce SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"first", "i", "1", "fallback"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("coalesce argument prefix = %#v, want %#v\nSQL: %s", compiled.Args, wantPrefix, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("ordinary coalesce introduced row expansion:\n%s", compiled.SQL)
	}
}

func TestCompileEvalCoalesceSupportsTypedNullsAndBooleanConsumers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		required []string
	}{
		{
			name:   "all null becomes nullable String",
			source: `index=gradethis | eval value=coalesce(null, null) | where isnull(value)`,
			required: []string{
				`coalesce(CAST(NULL AS Nullable(String)), CAST(NULL AS Nullable(String)))`,
				`isNotNull("value")`,
			},
		},
		{
			name:   "null adopts Int64",
			source: `index=gradethis | eval value=coalesce(null, 1) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name:   "null adopts UInt64",
			source: `index=gradethis | eval value=coalesce(null, 18446744073709551615) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(UInt64))`,
				`CAST(? AS UInt64)`,
			},
		},
		{
			name:   "nullable Float64",
			source: `index=gradethis | eval value=coalesce(tonumber(optional), tonumber("1.5")) | table value`,
			required: []string{
				`coalesce(ifNotFinite(toFloat64OrNull(`,
				`CAST(NULL AS Nullable(Float64))`,
			},
		},
		{
			name:   "fixed Bool assignment",
			source: `index=gradethis | eval value=coalesce(null, false, true) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Bool))`,
				`CAST(? AS Bool)`,
			},
		},
		{
			name:   "Boolean result consumed by where",
			source: `index=gradethis | where coalesce(isnull(optional), false)`,
			required: []string{
				`coalesce(CAST(NOT ifNull(`,
				`CAST(? AS Bool)`,
			},
		},
		{
			name:   "nested all-null if adopts String",
			source: `index=gradethis | eval value=coalesce(if(isnull(optional), null, null), "fallback") | table value`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`CAST(? AS String)`,
			},
		},
		{
			name: "projected-away field adopts String",
			source: `index=gradethis` +
				` | fields optional` +
				` | eval value=coalesce(removed, "fallback")` +
				` | table value`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`CAST(? AS String)`,
			},
		},
		{
			name:   "matching raw provenance",
			source: `index=gradethis | eval value=coalesce(null, _raw, _raw) | spath input=value output=selected path=value | table selected`,
			required: []string{
				`coalesce(CAST(NULL AS Nullable(String)), "_raw", "_raw")`,
				`"__os_raw_encoding" = 1`,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%q SQL missing %q:\n%s", test.source, required, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
			}
		})
	}

}

func TestCompileEvalCoalesceRejectsUnstableValueTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "Dynamic",
			source: `index=gradethis | eval value=coalesce(first, second)`,
		},
		{
			name:   "Dynamic and fixed String",
			source: `index=gradethis | eval value=coalesce(first, "fallback")`,
		},
		{
			name:   "fixed multivalue",
			source: `index=gradethis | stats values(user) AS users | eval value=coalesce(users, users)`,
		},
		{
			name:   "time",
			source: `index=gradethis | eval value=coalesce(_time, _indextime)`,
		},
		{
			name:   "mixed String and number",
			source: `index=gradethis | eval value=coalesce("one", 1)`,
		},
		{
			name:   "differing numeric types",
			source: `index=gradethis | eval value=coalesce(1, 18446744073709551615)`,
		},
		{
			name:   "null plus Dynamic",
			source: `index=gradethis | eval value=coalesce(null, first)`,
		},
		{
			name:   "incompatible raw text provenance",
			source: `index=gradethis | eval value=coalesce(_raw, "{}")`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, test.source)
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_COALESCE_VALUE_TYPE" {
				t.Fatalf(
					"Compile(%q) error = %#v, want SPL_UNSUPPORTED_COALESCE_VALUE_TYPE",
					test.source,
					err,
				)
			}
			if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
				!strings.HasPrefix(
					test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset],
					"coalesce(",
				) {
				t.Fatalf("Compile(%q) diagnostic range = %#v", test.source, diagnostic.Range)
			}
		})
	}

}

func TestCompileEvalCoalesceRetainsCalculatedFieldMaterialization(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value | eval value=coalesce(tonumber(selected), tonumber("1")) | where value>0`,
	)
	if !strings.Contains(compiled.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(compiled.SQL, `"__os_filter_bound_`) {
		t.Fatalf("coalesce output lost calculated-field materialization:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEvalCoalesceRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	stringLiteral := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: value},
		}
	}
	compileAssignment := func(expression plan.ScalarExpression) error {
		t.Helper()
		candidate := *base
		candidate.Operators = append(
			append([]plan.Operator(nil), base.Operators...),
			&plan.Extend{Assignments: []plan.ExtendAssignment{{
				Output:     plan.FieldRef{Name: "value"},
				Expression: expression,
			}}},
		)
		_, compileErr := (Compiler{}).Compile(&candidate)
		return compileErr
	}

	var typedNil *plan.ScalarLiteralExpression
	tooMany := make([]plan.ScalarExpression, spl.MaximumCoalesceArguments+1)
	for index := range tooMany {
		tooMany[index] = stringLiteral("value")
	}
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionCoalesce}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name: "zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionCoalesce,
			},
			want: "requires at least one argument",
		},
		{
			name: "too many arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionCoalesce,
				Arguments: tooMany,
			},
			want: "more than 32 arguments",
		},
		{
			name: "typed nil argument",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionCoalesce,
				Arguments: []plan.ScalarExpression{
					stringLiteral("value"),
					typedNil,
				},
			},
			want: "missing argument",
		},
		{
			name:       "cyclic expression",
			expression: cyclic,
			want:       "contains a cycle",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compileErr := compileAssignment(test.expression)
			if compileErr == nil || !strings.Contains(compileErr.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", compileErr, test.want)
			}
		})
	}

	t.Run("cyclic direct predicate is rejected before materialization discovery", func(t *testing.T) {
		t.Parallel()
		candidate := *base
		candidate.Operators = append(
			append([]plan.Operator(nil), base.Operators...),
			&plan.Filter{Expression: &plan.ScalarPredicateExpression{Value: cyclic}},
		)
		_, compileErr := (Compiler{}).Compile(&candidate)
		if compileErr == nil || !strings.Contains(compileErr.Error(), "contains a cycle") {
			t.Fatalf("Compile cyclic predicate error = %v, want cycle diagnostic", compileErr)
		}
	})
}
