package clickhouse

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEvalConcatenationFixedScalarsPreserveOrderAndNulls(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval joined="prefix:" . source . ":" . severity . ":" . 42 . ":" . 18446744073709551615 . ":" . 1.5 . ":" . tostring(true) | table joined`,
	)
	for _, required := range []string{
		`concat(`,
		`CAST(? AS String)`,
		`"source"`,
		`toString("severity")`,
		`toString(CAST(? AS Int64))`,
		`toString(CAST(? AS UInt64))`,
		`toString(CAST(? AS Float64))`,
		`transform(CAST(? AS Bool), [true, false], ['True', 'False']`,
		`AS "joined"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("concatenation SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{`toString("source")`, `toString(CAST(? AS String))`, "ARRAY JOIN"} {
		if strings.Contains(strings.ToUpper(compiled.SQL), strings.ToUpper(forbidden)) {
			t.Fatalf("fixed concatenation retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
	wantPrefix := []any{
		"prefix:", ":", ":", int64(42), ":", uint64(math.MaxUint64),
		":", float64(1.5), ":", true,
	}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"concatenation argument prefix = %#v, want %#v\nSQL: %s",
			compiled.Args,
			wantPrefix,
			compiled.SQL,
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}

	nullValue := compileSPL(
		t,
		`index=gradethis | eval absent="left" . null . replace("right", "i", "1") | table absent`,
	)
	if !strings.Contains(nullValue.SQL, `CAST(NULL AS Nullable(String))`) ||
		!strings.Contains(nullValue.SQL, `concat(`) {
		t.Fatalf("null-propagating concatenation SQL:\n%s", nullValue.SQL)
	}
	wantNullPrefix := []any{"left", "right", "i", "1"}
	if len(nullValue.Args) < len(wantNullPrefix) ||
		!slices.Equal(nullValue.Args[:len(wantNullPrefix)], wantNullPrefix) {
		t.Fatalf("null concatenation args = %#v, want prefix %#v", nullValue.Args, wantNullPrefix)
	}

	missing := compileSPL(
		t,
		`index=gradethis | fields source | eval absent=removed . "suffix" | table absent`,
	)
	if !strings.Contains(missing.SQL, `CAST(NULL AS Nullable(String))`) ||
		!strings.Contains(missing.SQL, `concat(`) {
		t.Fatalf("missing-field concatenation SQL:\n%s", missing.SQL)
	}
}

func TestCompileEvalConcatenationDynamicDomainsAreScalarAndBoundOnce(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		domain    dynamicScalarDomain
		required  []string
		forbidden []string
	}{
		{
			name:   "Any accepts String numeric and bounded decimal",
			domain: dynamicScalarDomainAny,
			required: []string{
				`'String'`, `toString(`, `'decimal/v1'`, `Map(String, String)`,
				`length(`, `match(`, `CAST(NULL AS Nullable(String))`,
			},
			forbidden: []string{`= 'Bool'`, `Array(String)`},
		},
		{
			name:      "Text accepts scalar String only",
			domain:    dynamicScalarDomainText,
			required:  []string{`'String'`, `dynamicElement(`},
			forbidden: []string{`'decimal/v1'`, `Map(String, String)`, `startsWith(`},
		},
		{
			name:     "Numeric accepts physical numbers only",
			domain:   dynamicScalarDomainNumeric,
			required: []string{`'Int8'`, `'Float'`, `'Decimal'`, `toString(`},
			forbidden: []string{
				`'decimal/v1'`, `Map(String, String)`,
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const source = "dynamic_input"
			state := concatenationCompilerTestState(map[string]fieldState{
				"value": {
					valueSQL:       source,
					dynamicTypeSQL: "dynamicType(" + source + ")",
					maxStringBytes: 64,
					existsSQL:      "1",
					kind:           fieldKindDynamic,
					dynamicDomain:  test.domain,
				},
			})
			compiled, err := compileScalarValue(
				concatenationCall(
					concatenationField("value"),
					concatenationStringLiteral("suffix"),
				),
				state,
			)
			if err != nil {
				t.Fatalf("compile Dynamic concatenation: %v", err)
			}
			for _, required := range test.required {
				if !strings.Contains(compiled.valueSQL, required) {
					t.Fatalf("Dynamic concatenation SQL missing %q:\n%s", required, compiled.valueSQL)
				}
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.valueSQL, forbidden) {
					t.Fatalf("Dynamic concatenation SQL retained %q:\n%s", forbidden, compiled.valueSQL)
				}
			}
			if got := strings.Count(compiled.valueSQL, source); got != 1 {
				t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.valueSQL)
			}
			if strings.Contains(strings.ToUpper(compiled.valueSQL), "ARRAY JOIN") {
				t.Fatalf("Dynamic concatenation introduced row expansion:\n%s", compiled.valueSQL)
			}
		})
	}
}

func TestStringConversionDynamicDecimalBudgetIsSharedAndDomainAware(
	t *testing.T,
) {
	t.Parallel()

	fields := map[string]fieldState{}
	for name, domain := range map[string]dynamicScalarDomain{
		"any":     dynamicScalarDomainAny,
		"text":    dynamicScalarDomainText,
		"numeric": dynamicScalarDomainNumeric,
	} {
		fields[name] = fieldState{
			valueSQL:       name + "_sql",
			dynamicTypeSQL: "dynamicType(" + name + "_sql)",
			maxStringBytes: 64,
			existsSQL:      "1",
			kind:           fieldKindDynamic,
			dynamicDomain:  domain,
		}
	}
	state := concatenationCompilerTestState(fields)
	reservations := int(
		MaximumStringConversionQueryDynamicDecimalBytes /
			uint64(MaximumStringConversionDynamicDecimalBytes),
	)
	for index := range reservations {
		var expression plan.ScalarExpression
		if index%2 == 0 {
			expression = concatenationCall(
				concatenationField("any"),
				concatenationStringLiteral("x"),
			)
		} else {
			expression = concatenationToStringCall(
				concatenationField("any"),
			)
		}
		if _, err := compileScalarValue(expression, state); err != nil {
			t.Fatalf("compile at shared Dynamic decimal budget: %v", err)
		}
	}
	if got := state.context.stringConversionBudget.dynamicDecimalBytes; got !=
		MaximumStringConversionQueryDynamicDecimalBytes {
		t.Fatalf(
			"shared Dynamic decimal budget = %d, want %d",
			got,
			MaximumStringConversionQueryDynamicDecimalBytes,
		)
	}
	_, overBudgetErr := compileScalarValue(
		concatenationToStringCall(concatenationField("any")),
		state,
	)
	assertConcatenationComplexityError(t, overBudgetErr)

	for _, field := range []string{"text", "numeric"} {
		compiled, err := compileScalarValue(
			concatenationToStringCall(concatenationField(field)),
			state,
		)
		if err != nil {
			t.Fatalf("compile %s domain at exhausted decimal budget: %v", field, err)
		}
		if strings.Contains(compiled.valueSQL, `'decimal/v1'`) ||
			strings.Contains(compiled.valueSQL, `= 'Bool'`) {
			t.Fatalf(
				"%s domain retained unrestricted dispatch:\n%s",
				field,
				compiled.valueSQL,
			)
		}
		if strings.Count(compiled.valueSQL, field+"_sql") != 1 {
			t.Fatalf(
				"%s source was not bound once:\n%s",
				field,
				compiled.valueSQL,
			)
		}
	}
}

func TestCompileEvalConcatenationPropagatesStringMetadata(t *testing.T) {
	t.Parallel()

	state := concatenationCompilerTestState(map[string]fieldState{
		"first": {
			valueSQL: "first_sql", maxStringBytes: 7, existsSQL: "1",
			textEligibleSQL: "guard_a", kind: fieldKindString,
		},
		"second": {
			valueSQL: "second_sql", maxStringBytes: 11, existsSQL: "1",
			textEligibleSQL: "guard_a", kind: fieldKindString,
			materializeForPredicate: true,
		},
		"third": {
			valueSQL: "third_sql", maxStringBytes: 13, existsSQL: "1",
			textEligibleSQL: "guard_b", kind: fieldKindString,
		},
	})
	compiled, err := compileScalarValue(
		concatenationCall(
			concatenationField("first"),
			concatenationField("second"),
			concatenationField("third"),
		),
		state,
	)
	if err != nil {
		t.Fatalf("compile metadata concatenation: %v", err)
	}
	if compiled.kind != fieldKindString || compiled.maxStringBytes != 31 ||
		!compiled.materializeForPredicate {
		t.Fatalf("concatenation metadata = %#v", compiled)
	}
	for _, marker := range []string{"first_sql", "second_sql", "third_sql"} {
		if strings.Count(compiled.valueSQL, marker) != 1 {
			t.Fatalf("operand %q was not evaluated once:\n%s", marker, compiled.valueSQL)
		}
	}
	if strings.Index(compiled.valueSQL, "first_sql") >=
		strings.Index(compiled.valueSQL, "second_sql") ||
		strings.Index(compiled.valueSQL, "second_sql") >=
			strings.Index(compiled.valueSQL, "third_sql") {
		t.Fatalf("operand order changed:\n%s", compiled.valueSQL)
	}
	if strings.Count(compiled.textEligibleSQL, "guard_a") != 1 ||
		strings.Count(compiled.textEligibleSQL, "guard_b") != 1 ||
		!strings.Contains(compiled.textEligibleSQL, "AND") {
		t.Fatalf("text provenance was not ANDed and deduplicated: %q", compiled.textEligibleSQL)
	}
}

func TestCompileEvalConcatenationRejectsUnsupportedFixedTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		code   string
	}{
		{
			name:   "time",
			source: `index=gradethis | eval value="at=" . _time`,
			code:   "SPL_UNSUPPORTED_CONCATENATION_VALUE_TYPE",
		},
		{
			name:   "fixed multivalue",
			source: `index=gradethis | stats values(user) AS users | eval value="users=" . users`,
			code:   "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := (Compiler{}).Compile(buildPlan(t, test.source))
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Compile(%q) error = %#v, want %s", test.source, err, test.code)
			}
		})
	}
}

func TestCompileEvalConcatenationEnforcesIndependentBudgets(t *testing.T) {
	t.Parallel()

	boundedFields := make(map[string]fieldState, 5)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		boundedFields[name] = fieldState{
			valueSQL: name, maxStringBytes: 1 << 20, existsSQL: "1",
			kind: fieldKindString,
		}
	}
	atCallLimit, err := compileScalarValue(
		concatenationCall(
			concatenationField("a"), concatenationField("b"),
			concatenationField("c"), concatenationField("d"),
		),
		concatenationCompilerTestState(boundedFields),
	)
	if err != nil || atCallLimit.maxStringBytes != 4<<20 {
		t.Fatalf("compile at 4 MiB call limit = (%#v, %v)", atCallLimit, err)
	}
	assertConcatenationComplexityError(
		t,
		compileConcatenationForTest(
			concatenationCompilerTestState(boundedFields),
			concatenationField("a"), concatenationField("b"),
			concatenationField("c"), concatenationField("d"),
			concatenationField("e"),
		),
	)

	queryState := concatenationCompilerTestState(boundedFields)
	for range 4 {
		if _, err := compileScalarValue(
			concatenationCall(
				concatenationField("a"), concatenationField("b"),
				concatenationField("c"), concatenationField("d"),
			),
			queryState,
		); err != nil {
			t.Fatalf("compile at 16 MiB query limit: %v", err)
		}
	}
	assertConcatenationComplexityError(
		t,
		compileConcatenationForTest(
			queryState,
			concatenationStringLiteral("x"),
			concatenationStringLiteral("y"),
		),
	)

	operandState := concatenationCompilerTestState(nil)
	maximumOperands := make([]plan.ScalarExpression, spl.MaximumConcatenationOperands)
	for index := range maximumOperands {
		maximumOperands[index] = concatenationStringLiteral("x")
	}
	for range spl.MaximumConcatenationOperandsPerQuery / spl.MaximumConcatenationOperands {
		if _, err := compileScalarValue(
			concatenationCall(maximumOperands...),
			operandState,
		); err != nil {
			t.Fatalf("compile at query operand limit: %v", err)
		}
	}
	assertConcatenationComplexityError(
		t,
		compileConcatenationForTest(
			operandState,
			concatenationStringLiteral("x"),
			concatenationStringLiteral("y"),
		),
	)

	largeState := concatenationCompilerTestState(map[string]fieldState{
		"large": {
			valueSQL: strings.Repeat("x", 2100), maxStringBytes: 1,
			existsSQL: "1", kind: fieldKindString,
		},
	})
	largeOperands := make([]plan.ScalarExpression, spl.MaximumConcatenationOperands)
	for index := range largeOperands {
		largeOperands[index] = concatenationField("large")
	}
	assertConcatenationComplexityError(
		t,
		compileConcatenationForTest(largeState, largeOperands...),
	)
}

func TestCompileEvalConcatenationRejectsForgedPlansAndWorksInPredicates(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	var typedNil *plan.ScalarLiteralExpression
	tooMany := make([]plan.ScalarExpression, spl.MaximumConcatenationOperands+1)
	for index := range tooMany {
		tooMany[index] = concatenationStringLiteral("x")
	}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "one operand",
			expression: concatenationCall(concatenationStringLiteral("x")),
			want:       "at least two operands",
		},
		{
			name:       "too many operands",
			expression: concatenationCall(tooMany...),
			want:       "more than 32 operands",
		},
		{
			name: "typed nil operand",
			expression: concatenationCall(
				concatenationStringLiteral("x"),
				typedNil,
			),
			want: "missing operand",
		},
		{
			name: "Boolean operand",
			expression: concatenationCall(
				concatenationStringLiteral("x"),
				&plan.ScalarLiteralExpression{
					Value: plan.Value{Kind: plan.ValueKindBool, Bool: true},
				},
			),
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid function enum operand",
			expression: concatenationCall(
				concatenationStringLiteral("x"),
				&plan.ScalarCallExpression{Function: plan.ScalarFunctionInvalid},
			),
			want: "unsupported function 0",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}

	predicate := compileSPL(
		t,
		`index=gradethis | eval selected=if("prefix:" . category = "prefix:value", "yes", "no") | table selected`,
	)
	if strings.Count(predicate.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("predicate concatenation duplicated its source:\n%s", predicate.SQL)
	}
	if strings.Contains(strings.ToUpper(predicate.SQL), "ARRAY JOIN") {
		t.Fatalf("predicate concatenation introduced row expansion:\n%s", predicate.SQL)
	}
}

func concatenationCall(arguments ...plan.ScalarExpression) *plan.ScalarCallExpression {
	return &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionConcat,
		Arguments: arguments,
	}
}

func concatenationStringLiteral(value string) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{Kind: plan.ValueKindString, String: value},
	}
}

func concatenationField(name string) plan.ScalarExpression {
	return &plan.ScalarFieldExpression{Field: plan.FieldRef{Name: name}}
}

func concatenationToStringCall(
	argument plan.ScalarExpression,
) *plan.ScalarCallExpression {
	return &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionToString,
		Arguments: []plan.ScalarExpression{argument},
	}
}

func concatenationCompilerTestState(fields map[string]fieldState) compileState {
	scope := testChartScope()
	return compileState{
		visible:      fields,
		context:      newCompileContext(scope.SearchStart, scope.SearchTimezone),
		blocked:      make(map[string]struct{}),
		allowDynamic: false,
	}
}

func compileConcatenationForTest(
	state compileState,
	arguments ...plan.ScalarExpression,
) error {
	_, err := compileScalarValue(concatenationCall(arguments...), state)
	return err
}

func assertConcatenationComplexityError(t *testing.T, err error) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}
