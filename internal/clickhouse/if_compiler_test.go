package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEvalIfFixedScalarsPreserveBindOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval label=if(isnull(condition), replace(first, "a", "b"), replace(second, "c", "d")) | table label`,
	)
	for _, required := range []string{
		`if(ifNull(`,
		`replaceRegexpAll(`,
		`AS "label"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("if SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"condition", "condition.", "first", "a", "b", "second", "c", "d"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("if argument prefix = %#v, want %#v\nSQL: %s", compiled.Args, wantPrefix, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("ordinary if introduced row expansion:\n%s", compiled.SQL)
	}
}

func TestCompileEvalIfSupportsTypedNullsNumbersAndBooleanConsumers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		required []string
	}{
		{
			name:   "null then Int64",
			source: `index=gradethis | eval value=if(isnull(optional), null, 1) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name:   "Int64 then null",
			source: `index=gradethis | eval value=if(isnotnull(optional), 1, null) | table value`,
			required: []string{
				`CAST(? AS Int64)`,
				`CAST(NULL AS Nullable(Int64))`,
			},
		},
		{
			name:   "same Float64 conversion",
			source: `index=gradethis | eval value=if(isnull(optional), tonumber("1"), tonumber("2")) | table value`,
			required: []string{
				`ifNotFinite(toFloat64OrNull(`,
				`CAST(NULL AS Nullable(Float64))`,
			},
		},
		{
			name:   "null and null",
			source: `index=gradethis | eval value=if(isnull(optional), null, null) | where isnull(value)`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`isNotNull("value")`,
			},
		},
		{
			name:   "Boolean result consumed by where",
			source: `index=gradethis | where if(isnull(optional), true, false)`,
			required: []string{
				`CAST(? AS Bool)`,
				`ifNull(if(ifNull(`,
			},
		},
		{
			name:   "Boolean functions consumed by where",
			source: `index=gradethis | where if(isnull(optional), isnull(left), isnotnull(right))`,
			required: []string{
				`CAST(NOT ifNull(`,
				`CAST(ifNull(`,
			},
		},
		{
			name:   "nested if remains nullable",
			source: `index=gradethis | where isnull(if(isnull(optional), null, if(isnull(other), "x", "y")))`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`CAST(NOT ifNull(`,
			},
		},
		{
			name:   "nested all-null if adopts Int64",
			source: `index=gradethis | eval value=if(isnull(optional), if(isnull(other), null, null), 1) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name:   "nested all-null if adopts Bool",
			source: `index=gradethis | eval value=if(isnull(optional), if(isnull(other), null, null), true) | where value=true`,
			required: []string{
				`CAST(NULL AS Nullable(Bool))`,
				`CAST(? AS Bool)`,
			},
		},
		{
			name: "all-null field adopts Int64 after eval projection and rename",
			source: `index=gradethis` +
				` | eval selected=if(isnull(optional), null, null)` +
				` | fields selected other` +
				` | rename selected AS renamed` +
				` | eval value=if(isnull(other), renamed, 1)` +
				` | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name: "all-null field adopts Bool after eval boundary",
			source: `index=gradethis` +
				` | eval selected=if(isnull(optional), null, null)` +
				` | eval value=if(isnull(other), selected, true)` +
				` | where value=true`,
			required: []string{
				`CAST(NULL AS Nullable(Bool))`,
				`CAST(? AS Bool)`,
			},
		},
		{
			name: "projected-away field adopts Int64",
			source: `index=gradethis` +
				` | fields optional` +
				` | eval value=if(isnull(optional), removed, 1)` +
				` | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name: "synthetic table field adopts Bool",
			source: `index=gradethis` +
				` | fields optional` +
				` | table optional removed` +
				` | eval value=if(isnull(optional), removed, true)` +
				` | where value=true`,
			required: []string{
				`CAST(NULL AS Nullable(Bool))`,
				`CAST(? AS Bool)`,
			},
		},
		{
			name: "all-null stats group adopts Int64",
			source: `index=gradethis` +
				` | eval selected=if(isnull(optional), null, null)` +
				` | stats count BY selected` +
				` | eval value=if(isnull(selected), selected, 1)` +
				` | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
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

func TestCompileEvalIfRejectsUnstableBranchTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "Dynamic",
			source: `index=gradethis | eval value=if(isnull(condition), first, second)`,
		},
		{
			name:   "fixed multivalue",
			source: `index=gradethis | stats values(user) AS users | eval value=if(isnull(users), users, users)`,
		},
		{
			name:   "time",
			source: `index=gradethis | eval value=if(isnull(condition), _time, _time)`,
		},
		{
			name:   "mixed String and number",
			source: `index=gradethis | eval value=if(isnull(condition), "one", 1)`,
		},
		{
			name:   "differing numeric types",
			source: `index=gradethis | eval value=if(isnull(condition), 1, 18446744073709551615)`,
		},
		{
			name:   "null plus Dynamic",
			source: `index=gradethis | eval value=if(isnull(condition), null, first)`,
		},
		{
			name:   "incompatible raw text provenance",
			source: `index=gradethis | eval value=if(isnull(condition), _raw, "{}")`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logical := buildPlan(t, test.source)
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_IF_BRANCH_TYPE" {
				t.Fatalf("Compile(%q) error = %#v, want SPL_UNSUPPORTED_IF_BRANCH_TYPE", test.source, err)
			}
			if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
				!strings.HasPrefix(test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset], "if(") {
				t.Fatalf("Compile(%q) diagnostic range = %#v", test.source, diagnostic.Range)
			}
		})
	}
}

func TestCompileEvalIfRetainsCalculatedFieldMaterialization(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value | eval label=if(isnull(selected), "missing", "present") | where label="missing"`,
	)
	if !strings.Contains(compiled.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(compiled.SQL, `"__os_filter_bound_`) {
		t.Fatalf("if output lost calculated-field materialization:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}

	direct := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value | where if(isnull(selected), true, false)`,
	)
	if !strings.Contains(direct.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(direct.SQL, `"__os_filter_bound_`) {
		t.Fatalf("direct if predicate lost calculated-field materialization:\n%s", direct.SQL)
	}

	raw := compileSPL(
		t,
		`index=gradethis | eval selected=if(isnull(optional), _raw, _raw) | spath input=selected output=value path=value | table value`,
	)
	if !strings.Contains(raw.SQL, `"__os_raw_encoding" = 1`) {
		t.Fatalf("if lost identical raw text provenance:\n%s", raw.SQL)
	}
}

func TestCompileEvalIfBoundsNestedSQLGrowth(t *testing.T) {
	t.Parallel()

	expression := "1"
	for range 24 {
		expression = "if(" + expression + "=dynamic_probe, 1, 1)"
	}
	source := "index=gradethis | eval value=" + expression
	logical := buildPlan(t, source)
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "if scalar SQL") {
		t.Fatalf("Compile(max nested if growth) error = %#v, want scalar SQL budget", err)
	}

	small := "1"
	for range 1 {
		small = "if(" + small + "=dynamic_probe, 1, 1)"
	}
	compiled := compileSPL(t, "index=gradethis | eval value="+small)
	if len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("bounded nested if compiled to %d bytes", len(compiled.SQL))
	}
}

func TestCompileEvalIfRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	field, err := plan.ResolveField("optional", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	stringLiteral := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: value},
		}
	}
	validCondition := func() plan.Expression {
		return &plan.ScalarPredicateExpression{Value: &plan.ScalarCallExpression{
			Function: plan.ScalarFunctionIsNull,
			Arguments: []plan.ScalarExpression{
				&plan.ScalarFieldExpression{Field: field},
			},
		}}
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

	var nilIf *plan.ScalarIfExpression
	var nilComparison *plan.EvalComparisonExpression
	var nilLiteral *plan.ScalarLiteralExpression
	var nilCall *plan.ScalarCallExpression
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "typed nil if",
			expression: nilIf,
			want:       "missing if expression",
		},
		{
			name: "nil condition",
			expression: &plan.ScalarIfExpression{
				True:  stringLiteral("yes"),
				False: stringLiteral("no"),
			},
			want: "missing condition",
		},
		{
			name: "typed nil condition",
			expression: &plan.ScalarIfExpression{
				Condition: nilComparison,
				True:      stringLiteral("yes"),
				False:     stringLiteral("no"),
			},
			want: "missing condition",
		},
		{
			name: "base-search condition",
			expression: &plan.ScalarIfExpression{
				Condition: &plan.TextExpression{Value: "error"},
				True:      stringLiteral("yes"),
				False:     stringLiteral("no"),
			},
			want: "eval/where predicate",
		},
		{
			name: "invalid Boolean operator",
			expression: &plan.ScalarIfExpression{
				Condition: &plan.BooleanExpression{
					Op:    plan.BooleanOpInvalid,
					Left:  validCondition(),
					Right: validCondition(),
				},
				True:  stringLiteral("yes"),
				False: stringLiteral("no"),
			},
			want: "invalid Boolean operator",
		},
		{
			name: "typed nil comparison operand",
			expression: &plan.ScalarIfExpression{
				Condition: &plan.EvalComparisonExpression{
					Left:  nilLiteral,
					Op:    plan.ComparisonOpEqual,
					Right: stringLiteral("x"),
				},
				True:  stringLiteral("yes"),
				False: stringLiteral("no"),
			},
			want: "missing scalar operand",
		},
		{
			name: "typed nil scalar predicate",
			expression: &plan.ScalarIfExpression{
				Condition: &plan.ScalarPredicateExpression{Value: nilCall},
				True:      stringLiteral("yes"),
				False:     stringLiteral("no"),
			},
			want: "scalar condition is missing",
		},
		{
			name: "invalid comparison operator",
			expression: &plan.ScalarIfExpression{
				Condition: &plan.EvalComparisonExpression{
					Left:  stringLiteral("x"),
					Op:    plan.ComparisonOpInvalid,
					Right: stringLiteral("x"),
				},
				True:  stringLiteral("yes"),
				False: stringLiteral("no"),
			},
			want: "invalid comparison operator",
		},
		{
			name: "nil true branch",
			expression: &plan.ScalarIfExpression{
				Condition: validCondition(),
				False:     stringLiteral("no"),
			},
			want: "missing true branch",
		},
		{
			name: "typed nil false branch",
			expression: &plan.ScalarIfExpression{
				Condition: validCondition(),
				True:      stringLiteral("yes"),
				False:     nilLiteral,
			},
			want: "missing false branch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compileErr := compileAssignment(test.expression)
			if compileErr == nil || !strings.Contains(compileErr.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", compileErr, test.want)
			}
		})
	}

	t.Run("Boolean function result assigned by eval", func(t *testing.T) {
		nullCall := func(function plan.ScalarFunction) plan.ScalarExpression {
			return &plan.ScalarCallExpression{
				Function: function,
				Arguments: []plan.ScalarExpression{
					&plan.ScalarFieldExpression{Field: field},
				},
			}
		}
		compileErr := compileAssignment(&plan.ScalarIfExpression{
			Condition: validCondition(),
			True:      nullCall(plan.ScalarFunctionIsNull),
			False:     nullCall(plan.ScalarFunctionIsNotNull),
		})
		if compileErr == nil || !strings.Contains(compileErr.Error(), "cannot directly assign a Boolean") {
			t.Fatalf("Compile error = %v, want Boolean assignment rejection", compileErr)
		}
	})

	t.Run("tonumber consumes Boolean if result", func(t *testing.T) {
		nullCall := func(function plan.ScalarFunction) plan.ScalarExpression {
			return &plan.ScalarCallExpression{
				Function: function,
				Arguments: []plan.ScalarExpression{
					&plan.ScalarFieldExpression{Field: field},
				},
			}
		}
		compileErr := compileAssignment(&plan.ScalarCallExpression{
			Function: plan.ScalarFunctionToNumber,
			Arguments: []plan.ScalarExpression{&plan.ScalarIfExpression{
				Condition: validCondition(),
				True:      nullCall(plan.ScalarFunctionIsNull),
				False:     nullCall(plan.ScalarFunctionIsNotNull),
			}},
		})
		if compileErr == nil || !strings.Contains(compileErr.Error(), "cannot consume a Boolean") {
			t.Fatalf("Compile error = %v, want Boolean consumer rejection", compileErr)
		}
	})
}
