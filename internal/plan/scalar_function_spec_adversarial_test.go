package plan

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

var forgedScalarRange = spl.Range{
	Start: spl.Position{Line: 1, Column: 1},
	End:   spl.Position{Line: 1, Column: 5},
}

func forgedScalarLiteral() spl.ScalarExpr {
	return &spl.ScalarLiteralExpr{
		Value: spl.Literal{
			Kind:   spl.LiteralKindString,
			Text:   "value",
			Quoted: true,
			Range:  forgedScalarRange,
		},
		Range: forgedScalarRange,
	}
}

func forgedScalarArguments(count int) []spl.ScalarExpr {
	if count == 0 {
		return nil
	}
	arguments := make([]spl.ScalarExpr, count)
	for index := range arguments {
		arguments[index] = forgedScalarLiteral()
	}
	return arguments
}

// buildForgedScalar plans `index=gradethis | eval selected=<expression>` from a
// hand-built syntax tree the parser could never produce and returns the
// resulting diagnostic.
func buildForgedScalar(t *testing.T, expression spl.ScalarExpr) *Diagnostic {
	t.Helper()
	base := mustParse(t, `index=gradethis`)
	query := &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.EvalCommand{
			Assignments: []spl.EvalAssignment{{
				Field:      "selected",
				FieldRange: forgedScalarRange,
				Expression: expression,
				Range:      forgedScalarRange,
			}},
			Range: forgedScalarRange,
		}},
		Range: base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	if err == nil {
		t.Fatalf("Build unexpectedly succeeded for %#v", expression)
	}
	diagnostic := &Diagnostic{}
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %#v, want *plan.Diagnostic", err)
	}
	return diagnostic
}

// TestConvertScalarCallRejectsForgedFunctionEnumsWithoutPanicking drives every
// ScalarFunction value that carries no scalarFunctionSpecs entry through plan
// conversion. The table-driven switches deliberately have no default, so an
// unknown function must fall through argument conversion and only then report
// SPL_UNSUPPORTED_EVAL_FUNCTION.
func TestConvertScalarCallRejectsForgedFunctionEnumsWithoutPanicking(t *testing.T) {
	t.Parallel()

	unknown := []spl.ScalarFunction{
		spl.ScalarFunctionInvalid,
		spl.ScalarFunctionCount,
		spl.ScalarFunction(200),
		spl.ScalarFunction(255),
	}
	for _, function := range unknown {
		for _, arity := range []int{0, 1, 2, 3} {
			diagnostic := buildForgedScalar(t, &spl.ScalarCallExpr{
				Function:  function,
				Arguments: forgedScalarArguments(arity),
				Range:     forgedScalarRange,
			})
			if diagnostic.Code != "SPL_UNSUPPORTED_EVAL_FUNCTION" ||
				diagnostic.Message != "unsupported scalar function" {
				t.Fatalf("function %d arity %d diagnostic = %#v, want SPL_UNSUPPORTED_EVAL_FUNCTION",
					function, arity, diagnostic)
			}
		}
	}

	// A typed-nil operand of an unknown function must still surface the
	// argument-conversion diagnostic first: argument recursion precedes the
	// unsupported-function report.
	var typedNil *spl.ScalarLiteralExpr
	diagnostic := buildForgedScalar(t, &spl.ScalarCallExpr{
		Function:  spl.ScalarFunction(200),
		Arguments: []spl.ScalarExpr{typedNil},
		Range:     forgedScalarRange,
	})
	if diagnostic.Code != "SPL_UNSUPPORTED_EVAL_EXPRESSION" {
		t.Fatalf("unknown function with typed-nil operand = %#v, want SPL_UNSUPPORTED_EVAL_EXPRESSION",
			diagnostic)
	}

	// No per-function guard may leak onto an unknown function: a Boolean
	// operand and a coalesce-sized operand list stay unsupported-function
	// errors rather than borrowing another function's diagnostic.
	booleanOperand := &spl.ScalarCallExpr{
		Function:  spl.ScalarFunctionIsNull,
		Arguments: forgedScalarArguments(1),
		Range:     forgedScalarRange,
	}
	for _, arguments := range [][]spl.ScalarExpr{
		{booleanOperand},
		{forgedScalarLiteral(), booleanOperand},
		forgedScalarArguments(spl.MaximumCoalesceArguments + 1),
	} {
		diagnostic := buildForgedScalar(t, &spl.ScalarCallExpr{
			Function:  spl.ScalarFunction(203),
			Arguments: arguments,
			Range:     forgedScalarRange,
		})
		if diagnostic.Code != "SPL_UNSUPPORTED_EVAL_FUNCTION" {
			t.Fatalf("unknown function with %d operands = %#v, want SPL_UNSUPPORTED_EVAL_FUNCTION",
				len(arguments), diagnostic)
		}
	}
}

// TestConvertScalarCallNestedUnknownFunctionsSurfaceInnermostDiagnostic pins
// the recursion order between the known-function checks and the unknown
// fall-through.
func TestConvertScalarCallNestedUnknownFunctionsSurfaceInnermostDiagnostic(t *testing.T) {
	t.Parallel()

	unknownInner := &spl.ScalarCallExpr{
		Function:  spl.ScalarFunction(201),
		Arguments: forgedScalarArguments(1),
		Range:     forgedScalarRange,
	}
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name: "unknown nested in known",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionLower,
				Arguments: []spl.ScalarExpr{unknownInner},
				Range:     forgedScalarRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
		{
			name: "known arity failure nested in unknown",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunction(202),
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionMatch,
					Arguments: forgedScalarArguments(1),
					Range:     forgedScalarRange,
				}},
				Range: forgedScalarRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "unknown nested three levels deep",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionCoalesce,
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionToString,
					Arguments: []spl.ScalarExpr{unknownInner},
					Range:     forgedScalarRange,
				}},
				Range: forgedScalarRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
	} {
		if diagnostic := buildForgedScalar(t, test.expression); diagnostic.Code != test.code {
			t.Fatalf("%s diagnostic = %#v, want %s", test.name, diagnostic, test.code)
		}
	}
}

// TestScalarFunctionSpecsStayInjectiveAndNamed guards the table itself: a
// copy-paste slip that pointed two spl functions at one plan function, dropped
// a diagnostic name, or admitted a non-eval enum would otherwise mistranslate
// silently rather than fail a conversion test.
func TestScalarFunctionSpecsStayInjectiveAndNamed(t *testing.T) {
	t.Parallel()

	seen := make(map[ScalarFunction]spl.ScalarFunction, len(scalarFunctionSpecs))
	for function, spec := range scalarFunctionSpecs {
		if function == spl.ScalarFunctionInvalid || function == spl.ScalarFunctionCount {
			t.Fatalf("scalarFunctionSpecs contains non-eval enum %d", function)
		}
		if spec.name == "" || spec.plan == ScalarFunctionInvalid {
			t.Fatalf("spec for %d = %#v, want a name and a plan function", function, spec)
		}
		if spec.exactArity && spec.arguments < 0 {
			t.Fatalf("spec for %s has negative arity %d", spec.name, spec.arguments)
		}
		if !spec.exactArity && spec.arguments != 0 {
			t.Fatalf("spec for %s carries arity %d without exactArity", spec.name, spec.arguments)
		}
		if previous, duplicate := seen[spec.plan]; duplicate {
			t.Fatalf("plan function %d mapped from both %d and %d", spec.plan, previous, function)
		}
		seen[spec.plan] = function
	}
}

// TestConvertScalarCallExactArityMessagesSurviveTheSpecTable checks each
// fixed-arity entry of scalarFunctionSpecs at arity-1 and arity+1 so the
// singular/plural noun and the spec's diagnostic name stay pinned.
func TestConvertScalarCallExactArityMessagesSurviveTheSpecTable(t *testing.T) {
	t.Parallel()

	for function, spec := range scalarFunctionSpecs {
		if !spec.exactArity {
			continue
		}
		for _, arity := range []int{spec.arguments - 1, spec.arguments + 1} {
			if arity < 0 {
				continue
			}
			want := spec.name + " requires exactly 1 argument"
			switch spec.arguments {
			case 0:
				want = spec.name + " requires no arguments"
			case 2:
				want = spec.name + " requires exactly 2 arguments"
			case 3:
				want = spec.name + " requires exactly 3 arguments"
			}
			var expression spl.ScalarExpr = &spl.ScalarCallExpr{
				Function:  function,
				Arguments: forgedScalarArguments(arity),
				Range:     forgedScalarRange,
			}
			if function.ReturnsBoolean() {
				// Eval refuses a top-level Boolean assignment before the
				// arity check runs, so nest the call in a scalar consumer.
				expression = &spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionToString,
					Arguments: []spl.ScalarExpr{expression},
					Range:     forgedScalarRange,
				}
			}
			diagnostic := buildForgedScalar(t, expression)
			if diagnostic.Code != "SPL_INVALID_EVAL_ARITY" || diagnostic.Message != want {
				t.Fatalf("%s at arity %d diagnostic = %#v, want SPL_INVALID_EVAL_ARITY/%q",
					spec.name, arity, diagnostic, want)
			}
		}
	}
}
