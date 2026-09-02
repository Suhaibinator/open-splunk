package clickhouse

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// maxToStringFormatBytes bounds both formatted spellings: a 19-digit
// magnitude with six separators, a sign, and two decimals for "commas", or a
// five-digit day count for "duration".
const maxToStringFormatBytes = 40

// int64FloatCeilingSQL is the first Float64 that no longer fits Int64; both
// formats null out magnitudes at or beyond it instead of saturating.
const int64FloatCeilingSQL = "toFloat64(9223372036854775808)"

// compileToStringFormatScalar lowers tostring(x, "commas") and
// tostring(x, "duration"). Numeric inputs follow the arithmetic operand
// contract (Dynamic numeric text and tagged decimals are numbers; malformed
// semantics abort the query) and the call is charged as one arithmetic
// operator so that contract's atomic-result requirement applies.
func compileToStringFormatScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse tostring: expected two arguments")
	}
	format, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok || !slices.Contains(spl.SupportedToStringFormats, format) {
		return compiledScalar{}, errors.New("compile ClickHouse tostring: unsupported format")
	}
	input, err := compileScalarInputArgument(expression.Arguments[0], state, "tostring")
	if err != nil {
		return compiledScalar{}, err
	}
	switch {
	case compiledScalarIsAlwaysNull(input), input.kind == fieldKindInvalid, input.kind == fieldKindBool:
		// Booleans spell True/False regardless of format, and null stays null:
		// the default conversion already implements both.
		return compileLexicalStringScalar(
			input,
			state,
			scalarStringConversion{
				operation:           "tostring",
				unsupportedTypeCode: "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
				allowBoolean:        true,
				maximumSQLBytes:     maxCompiledToStringFormatScalarSQLBytes,
			},
			expression.Range,
		)
	case isNativeMultivalueKind(input.kind):
		return compiledScalar{}, unsupportedMultivalueUsage("tostring", expression.Range)
	case input.kind == fieldKindNumber, input.kind == fieldKindDynamic:
	default:
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			Message: fmt.Sprintf(
				"tostring %q requires a numeric value; use tonumber before formatting",
				format,
			),
			Range: expression.Range,
		}
	}
	if err := chargeCompiledArithmeticOperator(state.context, expression.Range); err != nil {
		return compiledScalar{}, err
	}
	operand, err := normalizeArithmeticOperand(input, expression.Arguments[0].SourceRange())
	if err != nil {
		return compiledScalar{}, err
	}
	const valueAlias = "__os_tostring_value"
	var body string
	switch format {
	case "commas":
		body = toStringCommasSQL(valueAlias)
	case "duration":
		body = toStringDurationSQL(valueAlias)
	default:
		return compiledScalar{}, errors.New("compile ClickHouse tostring: unsupported format")
	}
	valueSQL := bindSQLExpressions([]string{valueAlias}, []string{operand.valueSQL}, body)
	if len(valueSQL) > maxCompiledToStringFormatScalarSQLBytes {
		return compiledScalar{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"tostring scalar SQL exceeds %d bytes",
				maxCompiledToStringFormatScalarSQLBytes,
			),
			Range: expression.Range,
		}
	}
	return compiledScalar{
		valueSQL:                valueSQL,
		valueArgs:               append([]any(nil), operand.valueArgs...),
		maxStringBytes:          maxToStringFormatBytes,
		existsSQL:               "1",
		kind:                    fieldKindString,
		alwaysNull:              operand.alwaysNull,
		materializeForPredicate: operand.materializeForPredicate,
	}, nil
}

const nullStringSQL = "CAST(NULL AS Nullable(String))"

// toStringCommasSQL groups the integral digits in threes and, when the value
// has a fractional part, appends exactly two decimals of the value rounded to
// hundredths, matching Splunk's documented "commas" examples.
func toStringCommasSQL(value string) string {
	const (
		safeAlias     = "__os_tostring_safe"
		roundedAlias  = "__os_tostring_rounded"
		reversedAlias = "__os_tostring_reversed"
	)
	inRange := "(isFinite(" + value + ") AND abs(" + value + ") < " + int64FloatCeilingSQL + ")"
	// Every branch is evaluated for every row, so integer conversion must only
	// ever see a finite, in-range magnitude.
	safe := "if(" + inRange + ", " + value + ", toFloat64(0))"
	rounded := "round(abs(" + safeAlias + "), 2)"
	reversed := "reverse(toString(toInt64(floor(" + roundedAlias + "))))"
	grouped := "reverse(arrayStringConcat(arrayMap(k -> substring(" + reversedAlias +
		", k * 3 + 1, 3), range(intDiv(length(" + reversedAlias + ") + 2, 3))), ','))"
	fraction := "if(" + value + " = floor(" + value + "), CAST('' AS String), " +
		"concat('.', leftPad(toString(toInt64(round((" + roundedAlias + " - floor(" +
		roundedAlias + ")) * 100))), 2, '0')))"
	formatted := "concat(if(" + value + " < 0, '-', ''), " + grouped + ", " + fraction + ")"
	formatted = bindSQLExpressions([]string{reversedAlias}, []string{reversed}, formatted)
	formatted = bindSQLExpressions([]string{roundedAlias}, []string{rounded}, formatted)
	formatted = bindSQLExpressions([]string{safeAlias}, []string{safe}, formatted)
	return "multiIf(isNull(" + value + "), " + nullStringSQL + ", NOT " + inRange + ", " +
		nullStringSQL + ", " + formatted + ")"
}

// toStringDurationSQL spells whole seconds as HH:MM:SS, prefixed by the day
// count as D+ once the value reaches one day. Negative and non-finite values
// have no duration spelling and become null.
func toStringDurationSQL(value string) string {
	const totalAlias = "__os_tostring_total"
	inRange := "(isFinite(" + value + ") AND " + value + " >= 0 AND " + value + " < " +
		int64FloatCeilingSQL + ")"
	total := "toInt64(floor(if(" + inRange + ", " + value + ", toFloat64(0))))"
	days := "intDiv(" + totalAlias + ", 86400)"
	pad := func(component string) string {
		return "leftPad(toString(" + component + "), 2, '0')"
	}
	formatted := "concat(if(" + days + " > 0, concat(toString(" + days + "), '+'), CAST('' AS String)), " +
		pad("modulo(intDiv("+totalAlias+", 3600), 24)") + ", ':', " +
		pad("modulo(intDiv("+totalAlias+", 60), 60)") + ", ':', " +
		pad("modulo("+totalAlias+", 60)") + ")"
	formatted = bindSQLExpressions([]string{totalAlias}, []string{total}, formatted)
	return "multiIf(isNull(" + value + "), " + nullStringSQL + ", NOT " + inRange + ", " +
		nullStringSQL + ", " + formatted + ")"
}
