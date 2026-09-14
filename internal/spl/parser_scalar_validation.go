package spl

import (
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"unicode/utf8"
)

func unsupportedScalarIdentifier(value string) bool {
	return strings.ContainsAny(value, "+-*/%'`")
}

// invalidEvalArity reports an eval-language call whose argument count is
// outside the function's supported arity.
func invalidEvalArity(sourceRange Range, message string) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_INVALID_EVAL_ARITY",
		Message: message,
		Range:   sourceRange,
	}
}

func validateMVDelimiterLiteral(function string, expression ScalarExpr) error {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil || literal.Value.Kind != LiteralKindString ||
		!literal.Value.Quoted {
		example := function + `(value, ",")`
		switch function {
		case "mvjoin":
			example = `mvjoin(multivalue_field, ",")`
		case "mvzip":
			example = `mvzip(left, right, ",")`
		}
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message:     function + " delimiter must be a quoted string literal",
			Range:       expression.SourceRange(),
			Suggestions: []string{example},
		}
	}
	if !utf8.ValidString(literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: function + " delimiter must be valid UTF-8",
			Range:   literal.Range,
		}
	}
	if len(literal.Value.Text) > MaximumMVDelimiterBytes {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s delimiter exceeds the %d-byte limit",
				function,
				MaximumMVDelimiterBytes,
			),
			Range: literal.Range,
		}
	}
	return nil
}

// booleanArgumentDiagnostic reports the shared search-mode restriction that a
// Boolean-returning function result cannot be consumed as a scalar value.
func booleanArgumentDiagnostic(function string, argument ScalarExpr, suggestions ...string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		Message:     function + " cannot consume a Boolean result in search-mode expressions",
		Range:       argument.SourceRange(),
		Suggestions: suggestions,
	}
}

// rejectBooleanMathArgument reports a Boolean-returning operand handed to a
// numeric eval function; the math functions share the arithmetic operand
// model, which never consumes a Boolean.
func rejectBooleanMathArgument(function string, argument ScalarExpr) error {
	if !scalarExpressionMayReturnBooleanFunction(argument) {
		return nil
	}
	return booleanArgumentDiagnostic(
		function,
		argument,
		"use isnull or isnotnull directly with where",
		"convert a numeric value before passing it to "+function,
	)
}

// rejectBooleanTextArgument reports a Boolean-returning operand handed to a
// text-transform eval function.
func rejectBooleanTextArgument(function string, argument ScalarExpr) error {
	if !scalarExpressionMayReturnBooleanFunction(argument) {
		return nil
	}
	return booleanArgumentDiagnostic(
		function,
		argument,
		"use isnull or isnotnull directly with where",
		"consume the Boolean with a supported conditional or conversion function",
	)
}

// SupportedToStringFormats lists the tostring second-argument formats that
// have an exact lowering. "hex" stays rejected because Splunk's hexadecimal
// rendering of non-integral input is undocumented.
var SupportedToStringFormats = []string{"commas", "duration"}

// validateToStringFormatLiteral requires the tostring format to be one of the
// supported quoted literals so the planner never sees a dynamic format.
func validateToStringFormatLiteral(expression ScalarExpr) error {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil || literal.Value.Kind != LiteralKindString ||
		!literal.Value.Quoted || !slices.Contains(SupportedToStringFormats, literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TOSTRING_FORMAT",
			Message: `tostring supports only the quoted "commas" and "duration" formats`,
			Range:   expression.SourceRange(),
			Suggestions: []string{
				`tostring(value, "commas")`,
				`tostring(value, "duration")`,
				"tostring(value)",
			},
		}
	}
	return nil
}

// MaximumTrimCharactersBytes bounds the explicit character set accepted by
// trim, ltrim, and rtrim so the bound SQL argument stays small.
const MaximumTrimCharactersBytes = 256

// validateTrimCharactersLiteral requires the trim character set to be a
// bounded, non-empty, valid UTF-8 quoted string literal.
func validateTrimCharactersLiteral(function string, expression ScalarExpr) error {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil || literal.Value.Kind != LiteralKindString ||
		!literal.Value.Quoted {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TRIM_CHARACTERS",
			Message:     function + " characters must be a quoted string literal",
			Range:       expression.SourceRange(),
			Suggestions: []string{function + `(value, "xy")`},
		}
	}
	if literal.Value.Text == "" || !utf8.ValidString(literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TRIM_CHARACTERS",
			Message: function + " characters must be a non-empty valid UTF-8 string",
			Range:   literal.Range,
		}
	}
	if len(literal.Value.Text) > MaximumTrimCharactersBytes {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s characters exceed the %d-byte limit",
				function,
				MaximumTrimCharactersBytes,
			),
			Range: literal.Range,
		}
	}
	return nil
}

// validateCIDRPrefixLiteral requires the cidrmatch prefix to be a quoted
// literal that parses as an IPv4 or IPv6 CIDR block.
func validateCIDRPrefixLiteral(expression ScalarExpr) error {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil || literal.Value.Kind != LiteralKindString ||
		!literal.Value.Quoted {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_CIDR_PREFIX",
			Message:     "cidrmatch prefix must be a quoted string literal",
			Range:       expression.SourceRange(),
			Suggestions: []string{`cidrmatch("10.0.0.0/8", ip)`},
		}
	}
	if _, err := netip.ParsePrefix(literal.Value.Text); err != nil {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_CIDR_PREFIX",
			Message:     "cidrmatch prefix must be an IPv4 or IPv6 CIDR block such as 10.0.0.0/8",
			Range:       literal.Range,
			Suggestions: []string{`cidrmatch("10.0.0.0/8", ip)`, `cidrmatch("2001:db8::/32", ip)`},
		}
	}
	return nil
}
