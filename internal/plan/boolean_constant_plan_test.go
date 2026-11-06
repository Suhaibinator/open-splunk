package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildBooleanConstantsAndRejectForgedArguments(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		function ScalarFunction
	}{
		{"true", ScalarFunctionTrue},
		{"false", ScalarFunctionFalse},
	} {
		query := mustParse(t, "index=gradethis | where "+test.name+"()")
		logical, err := Build(query, testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Fatal(err)
		}
		filter := logical.Operators[len(logical.Operators)-1].(*Filter)
		call := filter.Expression.(*ScalarPredicateExpression).Value.(*ScalarCallExpression)
		if call.Function != test.function || len(call.Arguments) != 0 {
			t.Fatalf("planned constant = %#v", call)
		}
		parsedCall := query.Commands[0].(*spl.WhereCommand).Expression.(*spl.WhereScalarPredicateExpr).Value.(*spl.ScalarCallExpr)
		parsedCall.Arguments = forgedScalarArguments(1)
		_, err = Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_INVALID_EVAL_ARITY")
	}
}
