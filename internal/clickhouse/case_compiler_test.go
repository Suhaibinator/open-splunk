package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEvalCasePreservesOrderedConditionsValuesAndBindings(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval label=case(severity=9, replace("first", "i", "1"), severity=13, "fallback") | table label`,
	)
	for _, required := range []string{
		`multiIf(ifNull(`,
		`replaceRegexpAll(`,
		`CAST(NULL AS Nullable(String))`,
		`AS "label"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("case SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{int64(9), "first", "i", "1", int64(13), "fallback"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"case argument prefix = %#v, want %#v\nSQL: %s",
			compiled.Args,
			wantPrefix,
			compiled.SQL,
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("ordinary case introduced row expansion:\n%s", compiled.SQL)
	}

	allNull := compileSPL(
		t,
		`index=gradethis | eval selected=case(severity=9, if(source="match", null, null)) | where isnull(selected)`,
	)
	wantAllNullPrefix := []any{int64(9), "match"}
	if len(allNull.Args) < len(wantAllNullPrefix) ||
		!slices.Equal(allNull.Args[:len(wantAllNullPrefix)], wantAllNullPrefix) {
		t.Fatalf(
			"all-null case argument prefix = %#v, want %#v\nSQL: %s",
			allNull.Args,
			wantAllNullPrefix,
			allNull.SQL,
		)
	}
}

func TestCompileEvalCaseSupportsTypedNullsAndBooleanConsumers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		required []string
	}{
		{
			name:   "implicit nullable String default",
			source: `index=gradethis | eval value=case(severity=9, "selected") | where isnull(value)`,
			required: []string{
				`multiIf(ifNull(`,
				`CAST(NULL AS Nullable(String))`,
				`isNotNull("value")`,
			},
		},
		{
			name:   "all null becomes nullable String",
			source: `index=gradethis | eval value=case(severity=9, null, severity=13, null) | where isnull(value)`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
			},
		},
		{
			name:   "null adopts Int64",
			source: `index=gradethis | eval value=case(severity=9, null, severity=13, 1) | table value`,
			required: []string{
				`CAST(NULL AS Nullable(Int64))`,
				`CAST(? AS Int64)`,
			},
		},
		{
			name:   "null adopts UInt64",
			source: `index=gradethis | eval value=case(severity=9, 18446744073709551615) | table value`,
			required: []string{
				`CAST(? AS UInt64)`,
				`CAST(NULL AS Nullable(UInt64))`,
			},
		},
		{
			name:   "nullable Float64",
			source: `index=gradethis | eval value=case(severity=9, tonumber("1.5"), severity=13, tonumber("2.5")) | table value`,
			required: []string{
				`ifNotFinite(toFloat64OrNull(`,
				`CAST(NULL AS Nullable(Float64))`,
			},
		},
		{
			name:   "fixed Bool assignment",
			source: `index=gradethis | eval value=case(severity=9, false, severity=13, true) | table value`,
			required: []string{
				`CAST(? AS Bool)`,
				`CAST(NULL AS Nullable(Bool))`,
			},
		},
		{
			name:   "Boolean result consumed by where",
			source: `index=gradethis | where case(isnull(optional), false, isnotnull(other), true)`,
			required: []string{
				`multiIf(ifNull(`,
				`CAST(? AS Bool)`,
				`CAST(NULL AS Nullable(Bool))`,
			},
		},
		{
			name:   "nested all-null if adopts String",
			source: `index=gradethis | eval value=case(severity=9, if(isnull(optional), null, null), severity=13, "fallback") | table value`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`CAST(? AS String)`,
			},
		},
		{
			name: "projected-away field adopts String",
			source: `index=gradethis` +
				` | fields optional` +
				` | eval value=case(severity=9, removed, severity=13, "fallback")` +
				` | table value`,
			required: []string{
				`CAST(NULL AS Nullable(String))`,
				`CAST(? AS String)`,
			},
		},
		{
			name:   "matching raw provenance",
			source: `index=gradethis | eval value=case(severity=9, _raw, severity=13, _raw) | spath input=value output=selected path=value | table selected`,
			required: []string{
				`ifNull("__os_string_or_bytes_`,
				`isValidUTF8(assumeNotNull("value"))`,
				`CAST(NULL AS Nullable(String))`,
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
				t.Fatalf(
					"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
					got,
					want,
					compiled.SQL,
					compiled.Args,
				)
			}
		})
	}
}

func TestCompileEvalCaseRejectsUnstableValueTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "Dynamic",
			source: `index=gradethis | eval value=case(severity=9, first, severity=13, second)`,
		},
		{
			name:   "Dynamic and fixed String",
			source: `index=gradethis | eval value=case(severity=9, first, severity=13, "fallback")`,
		},
		{
			name:   "fixed multivalue",
			source: `index=gradethis | stats values(user) AS users | eval value=case(count=1, users)`,
		},
		{
			name:   "time",
			source: `index=gradethis | eval value=case(severity=9, _time, severity=13, _indextime)`,
		},
		{
			name:   "mixed String and number",
			source: `index=gradethis | eval value=case(severity=9, "one", severity=13, 1)`,
		},
		{
			name:   "differing numeric types",
			source: `index=gradethis | eval value=case(severity=9, 1, severity=13, 18446744073709551615)`,
		},
		{
			name:   "null plus Dynamic",
			source: `index=gradethis | eval value=case(severity=9, null, severity=13, first)`,
		},
		{
			name:   "incompatible raw text provenance",
			source: `index=gradethis | eval value=case(severity=9, _raw, severity=13, "{}")`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, test.source)
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_CASE_VALUE_TYPE" {
				t.Fatalf(
					"Compile(%q) error = %#v, want SPL_UNSUPPORTED_CASE_VALUE_TYPE",
					test.source,
					err,
				)
			}
			if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
				!strings.HasPrefix(
					test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset],
					"case(",
				) {
				t.Fatalf("Compile(%q) diagnostic range = %#v", test.source, diagnostic.Range)
			}
		})
	}
}

func TestCompileEvalCaseRetainsCalculatedFieldMaterialization(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value | eval value=case(isnull(selected), "missing", selected="1", "one") | where value="missing"`,
	)
	if !strings.Contains(compiled.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(compiled.SQL, `"__os_filter_bound_`) {
		t.Fatalf("case output lost calculated-field materialization:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}

	direct := compileSPL(
		t,
		`index=gradethis | spath input=payload output=selected path=value | where case(isnull(selected), false, selected="1", true)`,
	)
	if !strings.Contains(direct.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(direct.SQL, `"__os_filter_bound_`) {
		t.Fatalf("direct case predicate lost calculated-field materialization:\n%s", direct.SQL)
	}
}

func TestCompileEvalCaseBoundsVariadicSQLGrowth(t *testing.T) {
	t.Parallel()

	nested := "1"
	for range 1 {
		nested = "if(" + nested + "=dynamic_probe, 1, 1)"
	}
	seed := buildPlan(t, "index=gradethis | eval seed="+nested)
	seedExtend := seed.Operators[len(seed.Operators)-1].(*plan.Extend)
	largeValue := seedExtend.Assignments[0].Expression

	conditionSeed := buildPlan(
		t,
		`index=gradethis | eval seed=case(optional=1, "value")`,
	)
	conditionCase := conditionSeed.Operators[len(conditionSeed.Operators)-1].(*plan.Extend).Assignments[0].Expression.(*plan.ScalarCaseExpression)
	condition := conditionCase.Branches[0].Condition

	branches := make([]plan.ScalarCaseBranch, spl.MaximumCaseBranches)
	for index := range branches {
		branches[index] = plan.ScalarCaseBranch{
			Condition: condition,
			Value:     largeValue,
		}
	}
	base := buildPlan(t, `index=gradethis`)
	candidate := *base
	candidate.Operators = append(
		append([]plan.Operator(nil), base.Operators...),
		&plan.Extend{Assignments: []plan.ExtendAssignment{{
			Output: plan.FieldRef{Name: "value"},
			Expression: &plan.ScalarCaseExpression{
				Branches: branches,
			},
		}}},
	)
	_, err := (Compiler{}).Compile(&candidate)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "case scalar SQL") {
		t.Fatalf("Compile variadic case growth error = %#v, want case SQL budget", err)
	}

	small := *base
	small.Operators = append(
		append([]plan.Operator(nil), base.Operators...),
		&plan.Extend{Assignments: []plan.ExtendAssignment{{
			Output: plan.FieldRef{Name: "value"},
			Expression: &plan.ScalarCaseExpression{
				Branches: branches[:2],
			},
		}}},
	)
	compiled, err := (Compiler{}).Compile(&small)
	if err != nil {
		t.Fatalf("Compile bounded case: %v", err)
	}
	if len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("bounded case compiled to %d bytes", len(compiled.SQL))
	}
}

func TestCompileEvalCaseRejectsForgedPlans(t *testing.T) {
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

	var typedNilCondition *plan.ScalarPredicateExpression
	var typedNilValue *plan.ScalarLiteralExpression
	tooMany := make([]plan.ScalarCaseBranch, spl.MaximumCaseBranches+1)
	for index := range tooMany {
		tooMany[index] = plan.ScalarCaseBranch{
			Condition: validCondition(),
			Value:     stringLiteral("value"),
		}
	}
	cyclic := &plan.ScalarCaseExpression{}
	cyclic.Branches = []plan.ScalarCaseBranch{{
		Condition: validCondition(),
		Value:     cyclic,
	}}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "zero branches",
			expression: &plan.ScalarCaseExpression{},
			want:       "requires at least one condition/value pair",
		},
		{
			name: "too many branches",
			expression: &plan.ScalarCaseExpression{
				Branches: tooMany,
			},
			want: "more than 16 condition/value pairs",
		},
		{
			name: "nil condition",
			expression: &plan.ScalarCaseExpression{
				Branches: []plan.ScalarCaseBranch{{
					Value: stringLiteral("value"),
				}},
			},
			want: "missing condition",
		},
		{
			name: "typed nil condition",
			expression: &plan.ScalarCaseExpression{
				Branches: []plan.ScalarCaseBranch{{
					Condition: typedNilCondition,
					Value:     stringLiteral("value"),
				}},
			},
			want: "missing condition",
		},
		{
			name: "nil value",
			expression: &plan.ScalarCaseExpression{
				Branches: []plan.ScalarCaseBranch{{
					Condition: validCondition(),
				}},
			},
			want: "missing value",
		},
		{
			name: "typed nil value",
			expression: &plan.ScalarCaseExpression{
				Branches: []plan.ScalarCaseBranch{{
					Condition: validCondition(),
					Value:     typedNilValue,
				}},
			},
			want: "missing value",
		},
		{
			name:       "cycle",
			expression: cyclic,
			want:       "contains a cycle",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := compileAssignment(test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile forged case error = %v, want containing %q", err, test.want)
			}
		})
	}
}
