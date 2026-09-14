package spl

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func (p *parser) parseMultivalueScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "mvcount":
		function = ScalarFunctionMVCount
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "mvcount requires exactly one argument")
		}
	case "mvsort":
		function = ScalarFunctionMVSort
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "mvsort requires exactly one argument")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"mvsort",
				arguments[0],
				"mvsort(multivalue_field)",
			)
		}
	case "split":
		function = ScalarFunctionSplit
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "split requires exactly two arguments")
		}
		if err := validateMVDelimiterLiteral("split", arguments[1]); err != nil {
			return nil, err
		}
	case "mvappend":
		function = ScalarFunctionMVAppend
		if len(arguments) == 0 {
			return nil, invalidEvalArity(name.sourceRange, "mvappend requires at least one argument")
		}
	case "mvdedup":
		function = ScalarFunctionMVDedup
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, "mvdedup requires exactly one argument")
		}
	case "mvindex":
		function = ScalarFunctionMVIndex
		if len(arguments) < 2 || len(arguments) > 3 {
			return nil, invalidEvalArity(name.sourceRange, "mvindex requires two or three arguments")
		}
		for index := 1; index < len(arguments); index++ {
			literal, ok := arguments[index].(*ScalarLiteralExpr)
			if !ok || literal == nil || literal.Value.Kind != LiteralKindInteger {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_MV_INDEX",
					Message: "mvindex start and end must be literal signed 32-bit integers",
					Range:   arguments[index].SourceRange(),
					Suggestions: []string{
						"mvindex(multivalue_field, 0)",
						"mvindex(multivalue_field, -2, -1)",
					},
				}
			}
			if !SupportedMVIndexLiteral(arguments[index]) {
				return nil, &Diagnostic{
					Code:    "SPL_NUMBER_OUT_OF_RANGE",
					Message: "mvindex start and end must fit a signed 32-bit integer",
					Range:   literal.Range,
				}
			}
		}
	case "mvjoin":
		function = ScalarFunctionMVJoin
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "mvjoin requires exactly two arguments")
		}
		if err := validateMVDelimiterLiteral("mvjoin", arguments[1]); err != nil {
			return nil, err
		}
	case "mvzip":
		function = ScalarFunctionMVZip
		if len(arguments) < 2 || len(arguments) > 3 {
			return nil, invalidEvalArity(name.sourceRange, "mvzip requires two or three arguments")
		}
		if len(arguments) == 3 {
			if err := validateMVDelimiterLiteral("mvzip", arguments[2]); err != nil {
				return nil, err
			}
		}
	case "mvfind":
		function = ScalarFunctionMVFind
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "mvfind requires exactly two arguments")
		}
		pattern, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || pattern == nil || pattern.Value.Kind != LiteralKindString ||
			!pattern.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "mvfind regular expression must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`mvfind(multivalue_field, "pattern")`,
				},
			}
		}
		_, err := splregex.CompileMatchPattern(pattern.Value.Text)
		if err != nil {
			if splregex.IsMatchComplexityError(err) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"mvfind regular expression exceeds the %d-byte or %d-work-unit limit",
						splregex.MaximumMatchPatternBytes,
						splregex.MaximumMatchProgramWorkUnits,
					),
					Range: pattern.Range,
				}
			}
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REGEX",
				Message: "mvfind regular expression is outside the supported RE2-compatible subset",
				Range:   pattern.Range,
				Suggestions: []string{
					"use an RE2-compatible regular expression",
				},
			}
		}
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
