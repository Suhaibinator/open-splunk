package spl

import "fmt"

func (p *parser) parseNumericScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "round":
		function = ScalarFunctionRound
		if len(arguments) < 1 || len(arguments) > 2 {
			return nil, invalidEvalArity(name.sourceRange, "round requires one or two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"round",
				arguments[0],
				"use isnull or isnotnull directly with where",
				"convert a numeric value before rounding it",
			)
		}
		if len(arguments) == 2 && !SupportedRoundPrecision(arguments[1]) {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_ROUND_PRECISION",
				Message: fmt.Sprintf(
					"round precision must be a literal integer from 0 through %d",
					MaximumRoundPrecision,
				),
				Range: arguments[1].SourceRange(),
				Suggestions: []string{
					"round(value)",
					"round(value, 2)",
				},
			}
		}
	case "ceil", "ceiling", "floor":
		if functionName == "floor" {
			function = ScalarFunctionFloor
		} else {
			function = ScalarFunctionCeil
		}
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires exactly one argument")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				functionName,
				arguments[0],
				"use isnull or isnotnull directly with where",
				"convert a numeric value before rounding it",
			)
		}
	case "abs", "sqrt", "exp", "ln":
		function = map[string]ScalarFunction{
			"abs":  ScalarFunctionAbs,
			"sqrt": ScalarFunctionSqrt,
			"exp":  ScalarFunctionExp,
			"ln":   ScalarFunctionLn,
		}[functionName]
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires exactly one argument")
		}
		if err := rejectBooleanMathArgument(functionName, arguments[0]); err != nil {
			return nil, err
		}
	case "log":
		function = ScalarFunctionLog
		if len(arguments) != 1 && len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "log requires one or two arguments")
		}
		for _, argument := range arguments {
			if err := rejectBooleanMathArgument("log", argument); err != nil {
				return nil, err
			}
		}
	case "pow":
		function = ScalarFunctionPow
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "pow requires exactly two arguments")
		}
		for _, argument := range arguments {
			if err := rejectBooleanMathArgument("pow", argument); err != nil {
				return nil, err
			}
		}
	case "pi":
		function = ScalarFunctionPi
		if len(arguments) != 0 {
			return nil, invalidEvalArity(name.sourceRange, "pi requires no arguments")
		}
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
