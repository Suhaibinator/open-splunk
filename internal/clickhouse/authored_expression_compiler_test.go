package clickhouse

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileAuthoredArithmeticNormalizesFixedOperandsAndOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         plan.ScalarBinaryOp
		required   string
		normalizes bool
	}{
		{name: "add", op: plan.ScalarBinaryOpAdd, required: "plus(", normalizes: true},
		{name: "subtract", op: plan.ScalarBinaryOpSubtract, required: "minus(", normalizes: true},
		{name: "multiply", op: plan.ScalarBinaryOpMultiply, required: "multiply(", normalizes: true},
		{name: "divide", op: plan.ScalarBinaryOpDivide, required: "divideOrNull("},
		{name: "remainder", op: plan.ScalarBinaryOpRemainder, required: "moduloOrNull(", normalizes: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			state := authoredExpressionCompilerTestState(nil)
			compiled, err := compileScalarValue(
				&plan.ScalarBinaryExpression{
					Op:    test.op,
					Left:  authoredExpressionIntLiteral(5),
					Right: authoredExpressionFloatLiteral(-2),
				},
				state,
			)
			if err != nil {
				t.Fatalf("compile binary arithmetic: %v", err)
			}
			if compiled.kind != fieldKindNumber || compiled.numberType != "Float64" ||
				compiled.existsSQL != "1" || !compiled.ieeeComparison {
				t.Fatalf("compiled arithmetic type = %#v", compiled)
			}
			if !strings.Contains(compiled.valueSQL, test.required) {
				t.Fatalf("compiled SQL missing %q:\n%s", test.required, compiled.valueSQL)
			}
			if got := strings.Count(compiled.valueSQL, "?"); got != 2 {
				t.Fatalf("operand placeholder occurrences = %d, want 2:\n%s", got, compiled.valueSQL)
			}
			if len(compiled.valueArgs) != 2 || compiled.valueArgs[0] != int64(5) ||
				compiled.valueArgs[1] != float64(-2) {
				t.Fatalf("operand arguments = %#v", compiled.valueArgs)
			}
			if test.normalizes != strings.Contains(compiled.valueSQL, "toFloat64(0)") {
				t.Fatalf("negative-zero normalization = %v, want %v:\n%s",
					strings.Contains(compiled.valueSQL, "toFloat64(0)"),
					test.normalizes,
					compiled.valueSQL,
				)
			}
			if strings.Contains(strings.ToUpper(compiled.valueSQL), "ARRAY JOIN") {
				t.Fatalf("arithmetic introduced row expansion:\n%s", compiled.valueSQL)
			}
			if !state.context.atomicResult {
				t.Fatal("arithmetic did not require atomic result execution")
			}
		})
	}
}

func TestCompileAuthoredArithmeticDynamicInputIsBoundAndBoundedOnce(t *testing.T) {
	t.Parallel()

	const source = "dynamic_arithmetic_source"
	state := authoredExpressionCompilerTestState(map[string]fieldState{
		"value": {
			valueSQL:       source,
			dynamicTypeSQL: "dynamicType(" + source + ")",
			existsSQL:      "1",
			kind:           fieldKindDynamic,
		},
	})
	compiled, err := compileScalarValue(
		&plan.ScalarUnaryExpression{
			Op: plan.ScalarUnaryOpPositive,
			Operand: &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "value"},
			},
		},
		state,
	)
	if err != nil {
		t.Fatalf("compile Dynamic arithmetic: %v", err)
	}
	for _, required := range []string{
		"accurateCastOrNull(__os_arithmetic_dynamic, 'Float64')",
		"isValidUTF8(",
		"length(dynamicElement(__os_arithmetic_dynamic, 'String')) <= 4096",
		"'decimal/v1'",
		UnsupportedExpressionValueMarker,
	} {
		if !strings.Contains(compiled.valueSQL, required) {
			t.Fatalf("Dynamic arithmetic SQL missing %q:\n%s", required, compiled.valueSQL)
		}
	}
	if got := strings.Count(compiled.valueSQL, source); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.valueSQL)
	}
	if strings.Contains(compiled.valueSQL, "ifNotFinite") {
		t.Fatalf("Dynamic arithmetic discarded non-finite values:\n%s", compiled.valueSQL)
	}
}

func TestCompileAuthoredArithmeticRejectsFixedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value compiledScalar
	}{
		{name: "String", value: compiledScalar{valueSQL: "value", existsSQL: "1", kind: fieldKindString}},
		{name: "Boolean", value: compiledScalar{valueSQL: "value", existsSQL: "1", kind: fieldKindBool}},
		{name: "time", value: compiledScalar{valueSQL: "value", existsSQL: "1", kind: fieldKindTime}},
		{name: "multivalue", value: compiledScalar{valueSQL: "value", existsSQL: "1", kind: fieldKindStringArray}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeArithmeticOperand(test.value, plan.FieldRef{}.Range)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE" {
				t.Fatalf("fixed %s error = %v", test.name, err)
			}
		})
	}

	state := authoredExpressionCompilerTestState(nil)
	_, err := compileScalarValue(
		&plan.ScalarBinaryExpression{
			Op:    plan.ScalarBinaryOpAdd,
			Left:  authoredExpressionStringLiteral("12"),
			Right: authoredExpressionIntLiteral(1),
		},
		state,
	)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE" {
		t.Fatalf("fixed numeric-looking String error = %v", err)
	}
}

func TestCompileAuthoredMembershipBindsEveryOperandAndUsesExplicitThreeValuedLogic(t *testing.T) {
	t.Parallel()

	state := authoredExpressionCompilerTestState(nil)
	expression := &plan.MembershipExpression{
		Value: authoredExpressionIntLiteral(2),
		Candidates: []plan.ScalarExpression{
			authoredExpressionIntLiteral(1),
			authoredExpressionIntLiteral(2),
			&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindNull}},
		},
	}
	predicate, args, err := compileExpression(expression, state)
	if err != nil {
		t.Fatalf("compile membership: %v", err)
	}
	if len(args) != 3 || args[0] != int64(2) || args[1] != int64(1) ||
		args[2] != int64(2) {
		t.Fatalf("membership arguments = %#v", args)
	}
	if got := strings.Count(predicate, "?"); got != len(args) {
		t.Fatalf("membership placeholders = %d, arguments = %d:\n%s", got, len(args), predicate)
	}
	for _, required := range []string{
		"__os_membership_left",
		"__os_membership_candidate_1",
		"__os_membership_equal_1",
		"ifNull(__os_membership_equal_1, 0)",
		"isNull(__os_membership_equal_3)",
		"CAST(NULL AS Nullable(Bool))",
	} {
		if !strings.Contains(predicate, required) {
			t.Fatalf("membership SQL missing %q:\n%s", required, predicate)
		}
	}
	if strings.Contains(predicate, " IN (") {
		t.Fatalf("fixed membership delegated equality to backend IN:\n%s", predicate)
	}
	if strings.Contains(strings.ToUpper(predicate), "ARRAY JOIN") {
		t.Fatalf("membership introduced row expansion:\n%s", predicate)
	}
	if !state.context.atomicResult {
		t.Fatal("membership did not require atomic result execution")
	}
}

func TestCompileAuthoredMembershipEveryPredicateConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
	}{
		{
			name:   "if",
			source: `index=gradethis | eval class=if(status IN ("A", null), "match", "other") | table class`,
		},
		{
			name:   "case",
			source: `index=gradethis | eval class=case(status NOT IN ("A", "B"), "other", status IN ("A", null), "match") | table class`,
		},
		{
			name:   "stats count eval",
			source: `index=gradethis | stats count(eval(status IN ("A", null))) AS matches count(eval(status NOT IN ("A", null))) AS misses`,
		},
		{
			name:   "eventstats count eval",
			source: `index=gradethis | eventstats count(eval(status IN ("A", null))) AS matches | table event_id matches`,
		},
		{
			name:   "streamstats count eval",
			source: `index=gradethis | sort event_id | streamstats count(eval(status IN ("A", null))) AS matches | table event_id matches`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !compiled.RequiresAtomicResult() {
				t.Fatal("membership consumer did not retain atomic-result evidence")
			}
			if !strings.Contains(compiled.SQL, "__os_membership_left") ||
				!strings.Contains(compiled.SQL, "__os_membership_equal_") {
				t.Fatalf("membership consumer omitted explicit membership lowering:\n%s", compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), " ARRAY JOIN ") ||
				strings.Contains(compiled.SQL, "status IN (") {
				t.Fatalf("membership consumer used backend IN or row expansion:\n%s", compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, "?"); got != len(compiled.Args) {
				t.Fatalf("membership consumer placeholders = %d, args = %d", got, len(compiled.Args))
			}
		})
	}
}

func TestCompileAuthoredMembershipDynamicOperandValidatesMalformedTagsOnce(t *testing.T) {
	t.Parallel()

	const source = "dynamic_membership_source"
	state := authoredExpressionCompilerTestState(map[string]fieldState{
		"value": {
			valueSQL:       source,
			dynamicTypeSQL: "dynamicType(" + source + ")",
			existsSQL:      "1",
			kind:           fieldKindDynamic,
		},
	})
	predicate, _, err := compileExpression(
		&plan.MembershipExpression{
			Value: &plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "value"}},
			Candidates: []plan.ScalarExpression{
				authoredExpressionStringLiteral("ok"),
			},
		},
		state,
	)
	if err != nil {
		t.Fatalf("compile Dynamic membership: %v", err)
	}
	if got := strings.Count(predicate, source); got != 1 {
		t.Fatalf("Dynamic membership source references = %d, want 1:\n%s", got, predicate)
	}
	if !strings.Contains(predicate, UnsupportedExpressionValueMarker) ||
		!strings.Contains(predicate, "mapContains(") {
		t.Fatalf("Dynamic membership omitted malformed-tag validation:\n%s", predicate)
	}
}

func TestCompileAuthoredMembershipBindsOnlyRequiredDynamicClassifiers(t *testing.T) {
	t.Parallel()

	const source = "dynamic_membership_classifier_source"
	compile := func(t *testing.T, candidates ...plan.ScalarExpression) string {
		t.Helper()
		state := authoredExpressionCompilerTestState(map[string]fieldState{
			"value": {
				valueSQL:       source,
				dynamicTypeSQL: "dynamicType(" + source + ")",
				existsSQL:      "1",
				kind:           fieldKindDynamic,
			},
		})
		predicate, _, err := compileExpression(
			&plan.MembershipExpression{
				Value:      &plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "value"}},
				Candidates: candidates,
			},
			state,
		)
		if err != nil {
			t.Fatalf("compile Dynamic membership: %v", err)
		}
		return predicate
	}

	nonnumeric := compile(
		t,
		authoredExpressionStringLiteral("ok"),
		authoredExpressionBoolLiteral(true),
	)
	if !strings.Contains(nonnumeric, "__os_membership_left_type") {
		t.Fatalf("String/Bool membership omitted Dynamic type binding:\n%s", nonnumeric)
	}
	for _, forbidden := range []string{
		"__os_membership_left_numeric",
		"__os_membership_left_key",
		"__os_exact_order_",
	} {
		if strings.Contains(nonnumeric, forbidden) {
			t.Fatalf("String/Bool membership retained unused %q helper:\n%s", forbidden, nonnumeric)
		}
	}

	numeric := compile(t, authoredExpressionIntLiteral(2))
	for _, required := range []string{
		"__os_membership_left_type",
		"__os_membership_left_numeric",
		"__os_membership_left_key",
		"__os_exact_order_",
	} {
		if !strings.Contains(numeric, required) {
			t.Fatalf("numeric membership omitted required %q helper:\n%s", required, numeric)
		}
	}
}

func TestBindCompiledScalarForComparisonClearsDerivedMetadata(t *testing.T) {
	t.Parallel()

	literal := &plan.Value{Kind: plan.ValueKindInt64, Int64: 7}
	bound := bindCompiledScalarForComparison(
		compiledScalar{
			valueSQL:                  "authored_value",
			valueArgs:                 []any{7},
			existsSQL:                 "authored_present",
			existsArgs:                []any{true},
			dynamicTypeSQL:            "authored_type",
			dynamicNumericEligibleSQL: "authored_numeric",
			exactNumericKeySQL:        "authored_key",
			kind:                      fieldKindDynamic,
			dynamicDomain:             dynamicScalarDomainNumeric,
			literal:                   literal,
			ieeeComparison:            true,
			materializeForPredicate:   true,
		},
		"bound_value",
		"bound_present",
	)
	if bound.valueSQL != "bound_value" || bound.existsSQL != "bound_present" {
		t.Fatalf("comparison bindings = value %q, present %q", bound.valueSQL, bound.existsSQL)
	}
	if len(bound.valueArgs) != 0 || len(bound.existsArgs) != 0 {
		t.Fatalf("comparison binding retained arguments: value=%#v present=%#v", bound.valueArgs, bound.existsArgs)
	}
	if bound.dynamicTypeSQL != "" || bound.dynamicNumericEligibleSQL != "" ||
		bound.exactNumericKeySQL != "" || !bound.comparisonAtomic {
		t.Fatalf("comparison binding retained derived metadata: %#v", bound)
	}
	if bound.kind != fieldKindDynamic ||
		bound.dynamicDomain != dynamicScalarDomainNumeric ||
		bound.literal != literal || !bound.ieeeComparison ||
		!bound.materializeForPredicate {
		t.Fatalf("comparison binding lost semantic metadata: %#v", bound)
	}
}

func TestEvalAuthoredComparisonAppliesExplicitNaNRulesWithoutDuplicatingOperands(t *testing.T) {
	t.Parallel()

	left := arithmeticCompiledScalar("left_arithmetic", nil, false, false)
	right := compiledScalar{
		valueSQL:   "right_float",
		existsSQL:  "1",
		kind:       fieldKindNumber,
		numberType: "Float64",
		valueArgs:  nil,
		alwaysNull: false,
		literal:    &plan.Value{Kind: plan.ValueKindFloat64, Float64: math.NaN()},
	}
	for _, operator := range []string{"=", "!=", "<", "<="} {
		predicate, args := evalComparisonCore(left, right, operator)
		if len(args) != 0 {
			t.Fatalf("%s comparison arguments = %#v", operator, args)
		}
		if !strings.Contains(predicate, "isNaN(") {
			t.Fatalf("%s comparison omitted NaN guard:\n%s", operator, predicate)
		}
		if got := strings.Count(predicate, "left_arithmetic"); got != 1 {
			t.Fatalf("%s left operand references = %d, want 1:\n%s", operator, got, predicate)
		}
		if got := strings.Count(predicate, "right_float"); got != 1 {
			t.Fatalf("%s right operand references = %d, want 1:\n%s", operator, got, predicate)
		}
		want := "CAST(0 AS Nullable(Bool))"
		if operator == "!=" {
			want = "CAST(1 AS Nullable(Bool))"
		}
		if !strings.Contains(predicate, want) {
			t.Fatalf("%s NaN result missing %q:\n%s", operator, want, predicate)
		}
	}
}

func TestAuthoredExpressionRoundingPreservesIEEEComparisonSemantics(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"round", "ceil", "floor"} {
		t.Run(function, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(
				t,
				`index=gradethis | eval nan_value=(1e308*1e308)-(1e308*1e308)`+
					` | where `+function+`(nan_value)=`+function+`(nan_value)`+
					` | table nan_value`,
			)
			if !strings.Contains(compiled.SQL, "isNaN(") {
				t.Fatalf("%s around arithmetic lost IEEE NaN comparison guard:\n%s", function, compiled.SQL)
			}
		})
	}
}

func TestCompileAuthoredExpressionDefensiveBudgets(t *testing.T) {
	t.Parallel()

	state := authoredExpressionCompilerTestState(nil)
	for range 256 {
		if _, err := compileScalarValue(
			&plan.ScalarUnaryExpression{
				Op:      plan.ScalarUnaryOpPositive,
				Operand: authoredExpressionIntLiteral(1),
			},
			state,
		); err != nil {
			t.Fatalf("compile at arithmetic budget: %v", err)
		}
	}
	_, err := compileScalarValue(
		&plan.ScalarUnaryExpression{
			Op:      plan.ScalarUnaryOpPositive,
			Operand: authoredExpressionIntLiteral(1),
		},
		state,
	)
	assertAuthoredExpressionComplexityError(t, err)

	membershipState := authoredExpressionCompilerTestState(nil)
	for range 8 {
		candidates := make([]plan.ScalarExpression, 32)
		for index := range candidates {
			candidates[index] = authoredExpressionIntLiteral(int64(index))
		}
		if _, _, err := compileExpression(
			&plan.MembershipExpression{
				Value:      authoredExpressionIntLiteral(0),
				Candidates: candidates,
			},
			membershipState,
		); err != nil {
			t.Fatalf("compile at membership budget: %v", err)
		}
	}
	_, _, err = compileExpression(
		&plan.MembershipExpression{
			Value:      authoredExpressionIntLiteral(0),
			Candidates: []plan.ScalarExpression{authoredExpressionIntLiteral(1)},
		},
		membershipState,
	)
	assertAuthoredExpressionComplexityError(t, err)
}

func TestCompileAuthoredExpressionParserPlanCompilerPipeline(t *testing.T) {
	t.Parallel()

	arithmetic := compileSPL(
		t,
		`index=gradethis | eval adjusted=(latency * 2) + 1.5 | table adjusted`,
	)
	if !arithmetic.RequiresAtomicResult() {
		t.Fatal("authored arithmetic did not retain atomic-result evidence")
	}
	if !strings.Contains(arithmetic.SQL, "multiply(") ||
		!strings.Contains(arithmetic.SQL, "plus(") {
		t.Fatalf("authored arithmetic did not reach compiler lowering:\n%s", arithmetic.SQL)
	}
	if len(arithmetic.OutputFields) != 1 || arithmetic.OutputFields[0] != "adjusted" {
		t.Fatalf("arithmetic output fields = %v", arithmetic.OutputFields)
	}

	membership := compileSPL(
		t,
		`index=gradethis | where status NOT IN (200, 201, 204) | table status`,
	)
	if !membership.RequiresAtomicResult() {
		t.Fatal("authored membership did not retain atomic-result evidence")
	}
	if !strings.Contains(membership.SQL, "__os_membership_left") ||
		!strings.Contains(membership.SQL, "__os_membership_equal_3") {
		t.Fatalf("authored membership did not reach compiler lowering:\n%s", membership.SQL)
	}
	if got := strings.Count(membership.SQL, "status NOT IN"); got != 0 {
		t.Fatalf("authored membership leaked source syntax into backend SQL: %d", got)
	}
}

func TestCompileAuthoredExpressionTerminalConsumersRetainAtomicResult(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "chart after chronological and bin consumers",
			source: `index=gradethis` +
				` | eval weighted=duration_ms+1` +
				` | eventstats avg(weighted) AS mean` +
				` | streamstats sum(mean) AS running` +
				` | bin running span=2` +
				` | chart avg(running) OVER path BY service`,
		},
		{
			name: "timechart",
			source: `index=gradethis` +
				` | eval weighted=duration_ms+1` +
				` | timechart span=5m avg(weighted) AS mean`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !compiled.RequiresAtomicResult() {
				t.Fatal("terminal consumer lost arithmetic atomic-result authority")
			}
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("terminal consumer did not seal arithmetic atomic-result authority")
			}
		})
	}
}

func TestCompileAuthoredExpressionMaximumAuthoredShapesStayWithinSQLBudget(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		name   string
		source string
	}{
		{
			name:   "fixed arithmetic 256",
			source: authoredExpressionArithmeticBenchmarkSource("severity", 256),
		},
		{
			name:   "Dynamic arithmetic 256",
			source: authoredExpressionArithmeticBenchmarkSource("duration_ms", 256),
		},
		{
			name:   "membership 32",
			source: authoredExpressionMembershipBenchmarkSource(32),
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, fixture.source)
			if len(compiled.SQL) > maxCompiledQueryBytes {
				t.Fatalf("compiled SQL = %d bytes, want at most %d", len(compiled.SQL), maxCompiledQueryBytes)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("storage scans = %d, want 1", got)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
				t.Fatalf("maximum expression introduced row expansion:\n%s", compiled.SQL)
			}
		})
	}
}

func assertAuthoredExpressionComplexityError(t *testing.T, err error) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("complexity error = %v", err)
	}
}

func authoredExpressionCompilerTestState(fields map[string]fieldState) compileState {
	if fields == nil {
		fields = make(map[string]fieldState)
	}
	return compileState{
		visible: fields,
		context: newCompileContext(
			time.Unix(1_700_000_000, 0).UTC(),
			"UTC",
		),
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
}

func authoredExpressionIntLiteral(value int64) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{Kind: plan.ValueKindInt64, Int64: value},
	}
}

func authoredExpressionFloatLiteral(value float64) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{Kind: plan.ValueKindFloat64, Float64: value},
	}
}

func authoredExpressionStringLiteral(value string) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{Kind: plan.ValueKindString, String: value, Quoted: true},
	}
}

func authoredExpressionBoolLiteral(value bool) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{Kind: plan.ValueKindBool, Bool: value},
	}
}
