package spl

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

func (p *parser) parsePredicateScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "match":
		function = ScalarFunctionMatch
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "match requires exactly two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"match",
				arguments[0],
				"use the Boolean directly with where",
				`match(value, "pattern")`,
			)
		}
		pattern, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || pattern == nil || pattern.Value.Kind != LiteralKindString ||
			!pattern.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "match regular expression must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`match(value, "pattern")`,
				},
			}
		}
		_, err := splregex.CompileMatchPattern(pattern.Value.Text)
		if err != nil {
			if splregex.IsMatchComplexityError(err) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"match regular expression exceeds the %d-byte or %d-work-unit limit",
						splregex.MaximumMatchPatternBytes,
						splregex.MaximumMatchProgramWorkUnits,
					),
					Range: pattern.Range,
				}
			}
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REGEX",
				Message: "match regular expression is outside the supported RE2-compatible subset",
				Range:   pattern.Range,
				Suggestions: []string{
					"use an RE2-compatible regular expression",
				},
			}
		}
	case "like":
		function = ScalarFunctionLike
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "like requires exactly two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"like",
				arguments[0],
				"use the Boolean directly with where",
				`like(value, "pattern")`,
			)
		}
		pattern, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || pattern == nil || pattern.Value.Kind != LiteralKindString ||
			!pattern.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "like pattern must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`like(value, "pattern%")`,
				},
			}
		}
		if _, err := splwildcard.CompileLikePattern(pattern.Value.Text); err != nil {
			if splwildcard.IsLikeComplexityError(err) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"like pattern exceeds the %d-byte or %d-work-unit limit",
						splwildcard.MaximumLikePatternBytes,
						splwildcard.MaximumLikePatternWorkUnits,
					),
					Range: pattern.Range,
				}
			}
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LIKE_PATTERN",
				Message: "like pattern must be valid UTF-8 without NUL bytes or an unpaired terminal backslash",
				Range:   pattern.Range,
				Suggestions: []string{
					`like(value, "prefix%")`,
				},
			}
		}
	case "cidrmatch":
		function = ScalarFunctionCIDRMatch
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "cidrmatch requires exactly two arguments")
		}
		if err := validateCIDRPrefixLiteral(arguments[0]); err != nil {
			return nil, err
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[1]) {
			return nil, booleanArgumentDiagnostic(
				"cidrmatch",
				arguments[1],
				"use isnull or isnotnull directly with where",
				"pass the field that holds the IP address text",
			)
		}
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
