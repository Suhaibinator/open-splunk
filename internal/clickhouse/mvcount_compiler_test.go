package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalMVCountFixedScalarsAreOneOrNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval text_count=mvcount("one"), number_count=mvcount(42), bool_count=mvcount(true), predicate_count=mvcount(isnull(optional)), null_count=mvcount(null) | table text_count,number_count,bool_count,predicate_count,null_count`,
	)
	for _, required := range []string{
		`toUInt64(1)`,
		`CAST(NULL AS Nullable(UInt64)) AS "null_count"`,
		`AS "text_count"`,
		`AS "number_count"`,
		`AS "bool_count"`,
		`AS "predicate_count"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed mvcount SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `length(CAST(? AS String))`) {
		t.Fatalf("fixed String was counted by characters instead of as one value:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalMVCountFixedMultivalueCountsMembersAndNullsEmpty(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(status) AS values | eval count=mvcount(values) | table count`,
	)
	if !strings.Contains(
		compiled.SQL,
		`nullIf(toUInt64(length("values")), toUInt64(0)) AS "count"`,
	) {
		t.Fatalf("fixed multivalue mvcount SQL is not cardinality-or-null:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVCountDynamicInputIsBoundOnceAndCountsNonNullMembers(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | where mvcount(category)>1`,
	)
	for _, required := range []string{
		`arrayElement(arrayMap((value, present) ->`,
		`dynamicType(value) = 'Array(Dynamic)'`,
		`arrayCount(element -> dynamicType(element) != 'None'`,
		`startsWith(dynamicType(value), 'Array(')`,
		`nullIf(toUInt64(length(value)), toUInt64(0))`,
		`'bytes/v1', 'timestamp/v1', 'duration/v1', 'decimal/v1'`,
		`CAST(NULL AS Nullable(UInt64))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("Dynamic mvcount SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		`ARRAY JOIN`,
		`arrayJoin(`,
		`toString(value)`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("Dynamic mvcount retained forbidden branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalMVCountTextOnlyDynamicInputSkipsGeneralDispatch(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval count=mvcount(lower(category)) | table count`,
	)
	for _, required := range []string{
		`dynamicType(value) = 'String'`,
		`dynamicType(value) = 'Array(String)'`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("text-only mvcount SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`open_splunk_type`,
		`startsWith(dynamicType(value), 'Int')`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("text-only mvcount retained general branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(
		compiled.SQL,
		`dynamicType(value) = 'Array(Dynamic)'`,
	); got != 1 {
		t.Fatalf(
			"text-only mvcount added general Array(Dynamic) dispatch: count=%d\n%s",
			got,
			compiled.SQL,
		)
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("text-only source references = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEvalMVCountClosedSchemaMissingInputIsNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count | eval count=mvcount(absent) | table count`,
	)
	if !strings.Contains(
		compiled.SQL,
		`CAST(NULL AS Nullable(UInt64)) AS "count"`,
	) {
		t.Fatalf("closed-schema missing mvcount did not become null:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVCountNestedSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 24 {
		expression = "mvcount(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval count=`+expression+` | table count`,
	)
	if len(compiled.SQL) > maxCompiledMVCountScalarSQLBytes {
		t.Fatalf(
			"nested mvcount SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledMVCountScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVCountRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "status", Path: []string{"status"}},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionMVCount}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "zero arguments",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionMVCount},
			want:       "expected one argument",
		},
		{
			name: "two arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMVCount,
				Arguments: []plan.ScalarExpression{value(), value()},
			},
			want: "expected one argument",
		},
		{
			name: "typed nil value",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMVCount,
				Arguments: []plan.ScalarExpression{typedNil},
			},
			want: "missing scalar expression",
		},
		{
			name:       "cyclic expression",
			expression: cyclic,
			want:       "contains a cycle",
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
}
