package spl

import "github.com/Suhaibinator/open-splunk/internal/splregex"

func (p *parser) parseTextScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "replace":
		function = ScalarFunctionReplace
		if len(arguments) != 3 {
			return nil, invalidEvalArity(name.sourceRange, "replace requires exactly three arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"replace",
				arguments[0],
				"use isnull or isnotnull directly with where",
				"consume the Boolean with a supported conditional or conversion function",
			)
		}
		for index := 1; index < 3; index++ {
			literal, ok := arguments[index].(*ScalarLiteralExpr)
			if !ok || literal.Value.Kind != LiteralKindString || !literal.Value.Quoted {
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message:     "replace regex and replacement arguments must be quoted string literals",
					Range:       arguments[index].SourceRange(),
					Suggestions: []string{`replace(field, "pattern", "replacement")`},
				}
			}
		}
		pattern := arguments[1].(*ScalarLiteralExpr)
		if pattern.Value.Text == "" {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REGEX",
				Message:     "replace does not support an empty regular expression",
				Range:       pattern.Range,
				Suggestions: []string{"use a non-empty RE2-compatible regular expression"},
			}
		}
		if err := splregex.ValidateReplacePattern(pattern.Value.Text); err != nil {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REGEX",
				Message:     "replace regular expression is outside the supported always-consuming RE2-compatible subset",
				Range:       pattern.Range,
				Suggestions: []string{"use an RE2-compatible regular expression"},
			}
		}
	case "lower", "upper":
		if functionName == "lower" {
			function = ScalarFunctionLower
		} else {
			function = ScalarFunctionUpper
		}
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires exactly one argument")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				functionName,
				arguments[0],
				"use isnull or isnotnull directly with where",
				"consume the Boolean with a supported conditional or conversion function",
			)
		}
	case "len", "length":
		function = ScalarFunctionLength
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires exactly one argument")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				functionName,
				arguments[0],
				"use isnull or isnotnull directly with where",
				"consume the Boolean with a supported conditional or conversion function",
			)
		}
	case "substr":
		function = ScalarFunctionSubstring
		if len(arguments) < 2 || len(arguments) > 3 {
			return nil, invalidEvalArity(name.sourceRange, "substr requires two or three arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, booleanArgumentDiagnostic(
				"substr",
				arguments[0],
				"use isnull or isnotnull directly with where",
				"consume the Boolean with a supported conditional or conversion function",
			)
		}
		for index := 1; index < len(arguments); index++ {
			literal, ok := arguments[index].(*ScalarLiteralExpr)
			if !ok || literal == nil || literal.Value.Kind != LiteralKindInteger {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_SUBSTRING_INDEX",
					Message: "substr start and length must be literal integers",
					Range:   arguments[index].SourceRange(),
					Suggestions: []string{
						`substr(value, 1)`,
						`substr(value, -3, 2)`,
					},
				}
			}
		}
	case "trim", "ltrim", "rtrim":
		function = map[string]ScalarFunction{
			"trim":  ScalarFunctionTrim,
			"ltrim": ScalarFunctionLTrim,
			"rtrim": ScalarFunctionRTrim,
		}[functionName]
		if len(arguments) != 1 && len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires one or two arguments")
		}
		if err := rejectBooleanTextArgument(functionName, arguments[0]); err != nil {
			return nil, err
		}
		if len(arguments) == 2 {
			if err := validateTrimCharactersLiteral(functionName, arguments[1]); err != nil {
				return nil, err
			}
		}
	case "urldecode", "md5", "sha1", "sha256", "sha512":
		function = map[string]ScalarFunction{
			"urldecode": ScalarFunctionURLDecode,
			"md5":       ScalarFunctionMD5,
			"sha1":      ScalarFunctionSHA1,
			"sha256":    ScalarFunctionSHA256,
			"sha512":    ScalarFunctionSHA512,
		}[functionName]
		if len(arguments) != 1 {
			return nil, invalidEvalArity(name.sourceRange, functionName+" requires exactly one argument")
		}
		if err := rejectBooleanTextArgument(functionName, arguments[0]); err != nil {
			return nil, err
		}
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
