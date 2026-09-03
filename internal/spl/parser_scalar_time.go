package spl

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func (p *parser) parseTimeScalarCall(name token, functionName string, arguments []ScalarExpr) (ScalarExpr, error) {
	var function ScalarFunction
	switch functionName {
	case "now":
		function = ScalarFunctionNow
		if len(arguments) != 0 {
			return nil, invalidEvalArity(name.sourceRange, "now requires no arguments")
		}
	case "strftime":
		function = ScalarFunctionStrftime
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "strftime requires exactly two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strftime cannot consume a Boolean time value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
		format, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || format == nil || format.Value.Kind != LiteralKindString ||
			!format.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strftime format must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
		if _, err := spltimeformat.CompileStrftimeFormat(format.Value.Text); err != nil {
			if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"strftime format exceeds the %d-byte, %d-work-unit, or %d-output-byte limit",
						spltimeformat.MaximumStrftimeFormatBytes,
						spltimeformat.MaximumStrftimeWorkUnits,
						spltimeformat.MaximumStrftimeOutputBytes,
					),
					Range: format.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIME_FORMAT",
				Message: "strftime format is outside the supported locale-stable " +
					"date/time variable subset",
				Range: format.Range,
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
	case "strptime":
		function = ScalarFunctionStrptime
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "strptime requires exactly two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strptime cannot consume a Boolean text value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
		format, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || format == nil || format.Value.Kind != LiteralKindString ||
			!format.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strptime format must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
		if _, err := spltimeformat.CompileStrptimeFormat(format.Value.Text); err != nil {
			if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"strptime format exceeds the %d-byte or %d-work-unit limit",
						spltimeformat.MaximumStrptimeFormatBytes,
						spltimeformat.MaximumStrptimeWorkUnits,
					),
					Range: format.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIME_FORMAT",
				Message: "strptime format is outside the supported deterministic " +
					"full-date parsing subset",
				Range: format.Range,
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
	case "relative_time":
		function = ScalarFunctionRelativeTime
		if len(arguments) != 2 {
			return nil, invalidEvalArity(name.sourceRange, "relative_time requires exactly two arguments")
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "relative_time cannot consume a Boolean time value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
		specifier, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || specifier == nil ||
			specifier.Value.Kind != LiteralKindString ||
			!specifier.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "relative_time specifier must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
		if _, err := splrelativetime.CompileSpecifier(
			specifier.Value.Text,
		); err != nil {
			if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"relative_time specifier exceeds the %d-byte or %d-work-unit limit",
						splrelativetime.MaximumSpecifierBytes,
						splrelativetime.MaximumSpecifierWorkUnits,
					),
					Range: specifier.Range,
				}
			}
			if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
				return nil, &Diagnostic{
					Code: "SPL_NUMBER_OUT_OF_RANGE",
					Message: "relative_time magnitude exceeds the supported " +
						searchtimebounds.YearRangeDescription + " timestamp span",
					Range: specifier.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
				Message: "relative_time specifier is outside the supported " +
					"bounded offset-and-snap subset",
				Range: specifier.Range,
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
	}
	return parsedScalarCall(p, name, function, arguments), nil
}
