package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func testUnaryScalarCompilerTrustBoundary(
	t *testing.T,
	firstFunction plan.ScalarFunction,
	secondFunction plan.ScalarFunction,
) {
	t.Helper()
	testUnaryScalarCompilerStructuralTrustBoundary(t, firstFunction, secondFunction)

	base := buildPlan(t, `index=gradethis`)
	literal := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "value"},
		}
	}

	boolean := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{literal()},
	}
	t.Run("Boolean null predicate", func(t *testing.T) {
		t.Parallel()
		err := compileForgedScalarAssignment(
			t,
			base,
			&plan.ScalarCallExpression{
				Function:  secondFunction,
				Arguments: []plan.ScalarExpression{boolean},
			},
		)
		if err == nil || !strings.Contains(err.Error(), "cannot consume a Boolean") {
			t.Fatalf("Compile error = %v, want %q", err, "cannot consume a Boolean")
		}
	})
}

func testUnaryScalarCompilerStructuralTrustBoundary(
	t *testing.T,
	firstFunction plan.ScalarFunction,
	secondFunction plan.ScalarFunction,
) {
	t.Helper()

	base := buildPlan(t, `index=gradethis`)
	literal := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "value"},
		}
	}

	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: firstFunction}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "zero arguments",
			expression: &plan.ScalarCallExpression{Function: firstFunction},
			want:       "expected one argument",
		},
		{
			name: "two arguments",
			expression: &plan.ScalarCallExpression{
				Function:  secondFunction,
				Arguments: []plan.ScalarExpression{literal(), literal()},
			},
			want: "expected one argument",
		},
		{
			name: "typed nil argument",
			expression: &plan.ScalarCallExpression{
				Function:  firstFunction,
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

func compileForgedScalarAssignment(
	t *testing.T,
	base *plan.Query,
	expression plan.ScalarExpression,
) error {
	t.Helper()
	candidate := *base
	candidate.Operators = append(
		append([]plan.Operator(nil), base.Operators...),
		&plan.Extend{Assignments: []plan.ExtendAssignment{{
			Output:     plan.FieldRef{Name: "value"},
			Expression: expression,
		}}},
	)
	_, err := (Compiler{}).Compile(&candidate)
	return err
}
