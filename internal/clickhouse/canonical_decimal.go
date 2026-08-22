package clickhouse

import "strconv"

// canonicalDecimalPayloadPattern is the ClickHouse spelling of the public
// searchjobs Decimal grammar. Optional pieces use empty alternatives because a
// question mark in generated SQL is reserved for a driver argument.
const canonicalDecimalPayloadPattern = `'^([+]|-|)[0-9]+([.][0-9]+|)([eE]([+]|-|)[0-9]+|)$'`

const (
	canonicalDecimalMagnitudeChunkDigits = 18
	canonicalDecimalMagnitudeChunkBase   = "1000000000000000000"
)

// canonicalDecimalPayloadTextSQL returns one bounded String expression whose
// result matches searchjobs.CanonicalDecimal. Invalid input, invalid UTF-8, and
// input above MaximumExactNumericBinTextBytes produce the empty String. The
// exponent is manipulated lexically: even a maximum-sized exponent never
// passes through a fixed-width numeric conversion.
//
// This helper intentionally does not inspect or construct a decimal/v1
// envelope. Callers retain responsibility for selecting the envelope payload.
func canonicalDecimalPayloadTextSQL(valueSQL string) string {
	return canonicalDecimalPayloadTextSQLWithLimit(
		valueSQL,
		MaximumExactNumericBinTextBytes,
	)
}

func canonicalDecimalPayloadTextSQLWithLimit(valueSQL string, maximumBytes int) string {
	raw := "__os_canonical_decimal_raw"
	bounded := "__os_canonical_decimal_bounded"
	valid := "__os_canonical_decimal_valid"
	lowered := "__os_canonical_decimal_lowered"
	body := "__os_canonical_decimal_body"
	exponentPosition := "__os_canonical_decimal_exponent_position"
	significand := "__os_canonical_decimal_significand"
	exponentText := "__os_canonical_decimal_exponent_text"
	dotPosition := "__os_canonical_decimal_dot_position"
	integer := "__os_canonical_decimal_integer"
	digits := "__os_canonical_decimal_digits"
	significant := "__os_canonical_decimal_significant"
	coefficient := "__os_canonical_decimal_coefficient"
	leadingZeros := "__os_canonical_decimal_leading_zeros"
	exponentDigits := "__os_canonical_decimal_exponent_digits"
	exponentMagnitude := "__os_canonical_decimal_exponent_magnitude"
	exponentSign := "__os_canonical_decimal_exponent_sign"
	adjustment := "__os_canonical_decimal_adjustment"
	adjustedExponent := "__os_canonical_decimal_adjusted_exponent"
	scientificSign := "__os_canonical_decimal_scientific_sign"
	scientificMagnitude := "__os_canonical_decimal_scientific_magnitude"
	smallExponent := "__os_canonical_decimal_small_exponent"
	decimalPoint := "__os_canonical_decimal_point"

	sign := "if(startsWith(" + lowered +
		", '-'), CAST('-' AS String), CAST('' AS String))"
	exponentValue := "toInt16OrZero(if(length(" + scientificMagnitude +
		") <= 2, " + scientificMagnitude + ", CAST('0' AS String))) * toInt16(" +
		scientificSign + ")"
	plainEligible := "length(" + scientificMagnitude + ") <= 2 AND " +
		smallExponent + " >= -6 AND " + smallExponent + " < 21"
	plain := "multiIf(" +
		decimalPoint + " <= 0, concat(" + sign + ", '0.', repeat('0', toUInt64(" +
		"greatest(-(" + decimalPoint + "), toInt64(0)))), " + coefficient + "), " +
		decimalPoint + " >= toInt64(length(" + coefficient + ")), concat(" + sign +
		", " + coefficient + ", repeat('0', toUInt64(greatest(" + decimalPoint +
		" - toInt64(length(" + coefficient + ")), toInt64(0))))), concat(" + sign +
		", substring(" + coefficient + ", 1, toUInt64(greatest(" + decimalPoint +
		", toInt64(0)))), '.', substring(" + coefficient + ", toUInt64(greatest(" +
		decimalPoint + " + 1, toInt64(1))))))"
	formattedExponent := "if(" + scientificSign + " < 0, concat('-', " +
		scientificMagnitude + "), " + scientificMagnitude + ")"
	scientific := "concat(" + sign + ", if(length(" + coefficient +
		") = 1, " + coefficient + ", concat(substring(" + coefficient +
		", 1, 1), '.', substring(" + coefficient + ", 2))), 'e', " +
		formattedExponent + ")"
	canonical := "if(" + plainEligible + ", " + plain + ", " + scientific + ")"
	result := "if(" + valid + " = 0, CAST('' AS String), if(empty(" +
		significant + "), CAST('0' AS String), " + canonical + "))"

	result = bindSQLExpressions(
		[]string{decimalPoint},
		[]string{"toInt64(" + smallExponent + ") + 1"},
		result,
	)
	result = bindSQLExpressions(
		[]string{smallExponent},
		[]string{exponentValue},
		result,
	)
	result = bindSQLExpressions(
		[]string{scientificSign, scientificMagnitude},
		[]string{
			"tupleElement(" + adjustedExponent + ", 1)",
			"tupleElement(" + adjustedExponent + ", 2)",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{adjustedExponent},
		[]string{canonicalDecimalAdjustedExponentSQL(
			exponentSign,
			exponentMagnitude,
			adjustment,
		)},
		result,
	)
	result = bindSQLExpressions(
		[]string{coefficient, adjustment},
		[]string{
			"replaceRegexpOne(" + significant + ", '0+$', '')",
			"toInt64(length(" + integer + ")) - " + leadingZeros + " - 1",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{leadingZeros, exponentMagnitude, exponentSign},
		[]string{
			"toInt64(length(" + digits + ") - length(" + significant + "))",
			"if(empty(" + exponentDigits + "), CAST('0' AS String), " +
				exponentDigits + ")",
			"multiIf(empty(" + exponentDigits + "), toInt8(0), startsWith(" +
				exponentText + ", '-'), toInt8(-1), toInt8(1))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{significant, exponentDigits},
		[]string{
			"replaceRegexpOne(" + digits + ", '^0+', '')",
			"replaceRegexpOne(substring(" + exponentText + ", if(startsWith(" +
				exponentText + ", '-') OR startsWith(" + exponentText +
				", '+'), 2, 1)), '^0+', '')",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{integer, digits},
		[]string{
			"if(" + dotPosition + " = 0, " + significand + ", substring(" +
				significand + ", 1, " + dotPosition + " - 1))",
			"replaceAll(" + significand + ", '.', '')",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{dotPosition},
		[]string{"position(" + significand + ", '.')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{significand, exponentText},
		[]string{
			"if(" + exponentPosition + " = 0, " + body + ", substring(" + body +
				", 1, " + exponentPosition + " - 1))",
			"if(" + exponentPosition + " = 0, CAST('0' AS String), substring(" +
				body + ", " + exponentPosition + " + 1))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentPosition},
		[]string{"position(" + body + ", 'e')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{body},
		[]string{"substring(" + lowered + ", if(startsWith(" + lowered +
			", '-') OR startsWith(" + lowered + ", '+'), 2, 1))"},
		result,
	)
	result = bindSQLExpressions(
		[]string{valid, lowered},
		[]string{
			"toUInt8(match(" + bounded + ", " + canonicalDecimalPayloadPattern + "))",
			"lower(" + bounded + ")",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{bounded},
		[]string{"if(length(" + raw + ") <= " +
			strconv.Itoa(maximumBytes) + " AND isValidUTF8(" + raw +
			"), " + raw + ", CAST('' AS String))"},
		result,
	)
	return bindSQLExpressions([]string{raw}, []string{valueSQL}, result)
}

// canonicalDecimalAdjustedExponentSQL adds a bounded signed mantissa offset to
// an arbitrary-precision normalized exponent. Its tuple result is (Int8 sign,
// String magnitude), where zero is always represented as (0, "0").
func canonicalDecimalAdjustedExponentSQL(signSQL, magnitudeSQL, adjustmentSQL string) string {
	sign := "__os_canonical_decimal_add_sign"
	magnitude := "__os_canonical_decimal_add_magnitude"
	adjustment := "__os_canonical_decimal_add_adjustment"
	adjustmentSign := "__os_canonical_decimal_add_adjustment_sign"
	adjustmentMagnitude := "__os_canonical_decimal_add_adjustment_magnitude"
	comparison := "__os_canonical_decimal_add_comparison"

	added := canonicalDecimalMagnitudeAddSmallSQL(
		magnitude,
		"toInt128OrZero("+adjustmentMagnitude+")",
	)
	subtracted := canonicalDecimalMagnitudeSubtractSmallSQL(
		magnitude,
		"toInt128OrZero("+adjustmentMagnitude+")",
	)
	reverseMagnitude := "toInt128OrZero(if(length(" + magnitude + ") <= " +
		strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", " + magnitude +
		", CAST('0' AS String)))"
	reverseSubtracted := "toString(if(" + comparison + " < 0, toInt128OrZero(" +
		adjustmentMagnitude + ") - " + reverseMagnitude + ", toInt128(0)))"
	result := "multiIf(" +
		adjustmentSign + " = 0, tuple(" + sign + ", " + magnitude + "), " +
		sign + " = 0, tuple(" + adjustmentSign + ", " + adjustmentMagnitude + "), " +
		sign + " = " + adjustmentSign + ", tuple(" + sign + ", " + added + "), " +
		comparison + " = 0, tuple(toInt8(0), CAST('0' AS String)), " +
		comparison + " > 0, tuple(" + sign + ", " + subtracted + "), tuple(" +
		adjustmentSign + ", " + reverseSubtracted + "))"
	result = bindSQLExpressions(
		[]string{comparison},
		[]string{canonicalDecimalMagnitudeComparisonSQL(magnitude, adjustmentMagnitude)},
		result,
	)
	result = bindSQLExpressions(
		[]string{adjustmentSign, adjustmentMagnitude},
		[]string{
			"multiIf(" + adjustment + " < 0, toInt8(-1), " + adjustment +
				" > 0, toInt8(1), toInt8(0))",
			"toString(abs(" + adjustment + "))",
		},
		result,
	)
	return bindSQLExpressions(
		[]string{sign, magnitude, adjustment},
		[]string{signSQL, magnitudeSQL, adjustmentSQL},
		result,
	)
}

func canonicalDecimalMagnitudeComparisonSQL(left, right string) string {
	return "multiIf(length(" + left + ") < length(" + right + "), toInt8(-1), " +
		"length(" + left + ") > length(" + right + "), toInt8(1), " + left +
		" < " + right + ", toInt8(-1), " + left + " > " + right +
		", toInt8(1), toInt8(0))"
}

// canonicalDecimalMagnitudeAddSmallSQL adds one bounded Int128 adjustment to an
// arbitrary-precision normalized magnitude. Only the final 18 digits are
// converted; carry into the prefix is handled lexically.
func canonicalDecimalMagnitudeAddSmallSQL(magnitudeSQL, adjustmentSQL string) string {
	magnitude := "__os_canonical_decimal_magnitude_add_value"
	adjustment := "__os_canonical_decimal_magnitude_add_adjustment"
	short := "__os_canonical_decimal_magnitude_add_short"
	prefix := "__os_canonical_decimal_magnitude_add_prefix"
	suffix := "__os_canonical_decimal_magnitude_add_suffix"
	suffixValue := "__os_canonical_decimal_magnitude_add_suffix_value"
	total := "__os_canonical_decimal_magnitude_add_total"

	// Keep the addition in an explicitly unsigned wide domain. When this
	// helper is nested under Dynamic lambdas, ClickHouse may otherwise infer
	// incompatible signed and unsigned widths even though both operands are
	// bounded and non-negative.
	base := "toInt128('" + canonicalDecimalMagnitudeChunkBase + "')"
	paddedTotal := "leftPad(toString(" + total + "), " +
		strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", '0')"
	paddedCarry := "leftPad(toString(if(" + total + " >= " + base + ", " +
		total + " - " + base + ", toInt128(0))), " +
		strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", '0')"
	result := "if(" + short + " != 0, toString(" + total + "), if(" + total +
		" >= " + base + ", concat(" + canonicalDecimalMagnitudeIncrementSQL(prefix) +
		", " + paddedCarry + "), concat(" + prefix + ", " + paddedTotal + ")))"
	result = bindSQLExpressions(
		[]string{total},
		[]string{"toInt128(" + suffixValue + ") + toInt128(" + adjustment + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{suffixValue},
		[]string{"toInt128OrZero(" + suffix + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{prefix, suffix},
		[]string{
			"if(" + short + " != 0, CAST('' AS String), substring(" + magnitude +
				", 1, toUInt64(greatest(toInt64(length(" + magnitude + ")) - " +
				strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", toInt64(0)))))",
			"if(" + short + " != 0, " + magnitude + ", substring(" + magnitude +
				", toUInt64(greatest(toInt64(length(" + magnitude + ")) - " +
				strconv.Itoa(canonicalDecimalMagnitudeChunkDigits-1) + ", toInt64(1)))))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{short},
		[]string{"toUInt8(length(" + magnitude + ") <= " +
			strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ")"},
		result,
	)
	return bindSQLExpressions(
		[]string{magnitude, adjustment},
		[]string{magnitudeSQL, adjustmentSQL},
		result,
	)
}

func canonicalDecimalMagnitudeIncrementSQL(magnitudeSQL string) string {
	magnitude := "__os_canonical_decimal_magnitude_increment_value"
	head := "__os_canonical_decimal_magnitude_increment_head"
	trailingNines := "__os_canonical_decimal_magnitude_increment_trailing_nines"
	lastDigit := "toUInt8OrZero(substring(" + head +
		", toUInt64(greatest(toInt64(length(" + head + ")), toInt64(1))), 1))"
	result := "if(empty(" + head + "), concat('1', repeat('0', " + trailingNines +
		")), concat(substring(" + head + ", 1, toUInt64(greatest(toInt64(length(" +
		head + ")) - 1, toInt64(0)))), toString(" + lastDigit +
		" + toUInt8(1)), repeat('0', " + trailingNines + ")))"
	result = bindSQLExpressions(
		[]string{head, trailingNines},
		[]string{
			"replaceRegexpOne(" + magnitude + ", '9+$', '')",
			"length(" + magnitude + ") - length(replaceRegexpOne(" + magnitude +
				", '9+$', ''))",
		},
		result,
	)
	return bindSQLExpressions([]string{magnitude}, []string{magnitudeSQL}, result)
}

// canonicalDecimalMagnitudeSubtractSmallSQL subtracts one bounded Int128
// adjustment from a normalized magnitude known by the caller to be at least as
// large. Borrow from an arbitrary-sized prefix is handled lexically.
func canonicalDecimalMagnitudeSubtractSmallSQL(magnitudeSQL, adjustmentSQL string) string {
	magnitude := "__os_canonical_decimal_magnitude_subtract_value"
	adjustment := "__os_canonical_decimal_magnitude_subtract_adjustment"
	short := "__os_canonical_decimal_magnitude_subtract_short"
	prefix := "__os_canonical_decimal_magnitude_subtract_prefix"
	suffix := "__os_canonical_decimal_magnitude_subtract_suffix"
	suffixValue := "__os_canonical_decimal_magnitude_subtract_suffix_value"
	borrow := "__os_canonical_decimal_magnitude_subtract_borrow"
	candidate := "__os_canonical_decimal_magnitude_subtract_candidate"

	base := "toInt128('" + canonicalDecimalMagnitudeChunkBase + "')"
	wideSuffix := "toInt128(" + suffixValue + ")"
	wideAdjustment := "toInt128(" + adjustment + ")"
	difference := "if(" + wideSuffix + " >= " + wideAdjustment + ", " + wideSuffix +
		" - " + wideAdjustment + ", toInt128(0))"
	borrowedDifference := "if(" + wideSuffix + " < " + wideAdjustment + ", " + base +
		" + " + wideSuffix + " - " + wideAdjustment + ", toInt128(0))"
	longResult := "if(" + borrow + " = 0, concat(" + prefix +
		", leftPad(toString(" + difference + "), " +
		strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", '0')), concat(" +
		canonicalDecimalMagnitudeDecrementSQL(prefix) + ", leftPad(toString(" +
		borrowedDifference + "), " + strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) +
		", '0')))"
	result := "if(" + short + " != 0, toString(" + difference + "), if(empty(" +
		candidate + "), CAST('0' AS String), " + candidate + "))"
	result = bindSQLExpressions(
		[]string{candidate},
		[]string{"replaceRegexpOne(" + longResult + ", '^0+', '')"},
		result,
	)
	result = bindSQLExpressions(
		[]string{borrow},
		[]string{"toUInt8(" + wideSuffix + " < " + wideAdjustment + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{suffixValue},
		[]string{"toInt128OrZero(" + suffix + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{prefix, suffix},
		[]string{
			"if(" + short + " != 0, CAST('' AS String), substring(" + magnitude +
				", 1, toUInt64(greatest(toInt64(length(" + magnitude + ")) - " +
				strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ", toInt64(0)))))",
			"if(" + short + " != 0, " + magnitude + ", substring(" + magnitude +
				", toUInt64(greatest(toInt64(length(" + magnitude + ")) - " +
				strconv.Itoa(canonicalDecimalMagnitudeChunkDigits-1) + ", toInt64(1)))))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{short},
		[]string{"toUInt8(length(" + magnitude + ") <= " +
			strconv.Itoa(canonicalDecimalMagnitudeChunkDigits) + ")"},
		result,
	)
	return bindSQLExpressions(
		[]string{magnitude, adjustment},
		[]string{magnitudeSQL, adjustmentSQL},
		result,
	)
}

func canonicalDecimalMagnitudeDecrementSQL(magnitudeSQL string) string {
	magnitude := "__os_canonical_decimal_magnitude_decrement_value"
	head := "__os_canonical_decimal_magnitude_decrement_head"
	trailingZeros := "__os_canonical_decimal_magnitude_decrement_trailing_zeros"
	lastDigit := "toInt16(toUInt8OrZero(substring(" + head +
		", toUInt64(greatest(toInt64(length(" + head + ")), toInt64(1))), 1)))"
	result := "concat(substring(" + head +
		", 1, toUInt64(greatest(toInt64(length(" + head + ")) - 1, toInt64(0)))), " +
		"toString(" + lastDigit + " - toInt16(1)), repeat('9', " + trailingZeros + "))"
	result = bindSQLExpressions(
		[]string{head, trailingZeros},
		[]string{
			"replaceRegexpOne(" + magnitude + ", '0+$', '')",
			"length(" + magnitude + ") - length(replaceRegexpOne(" + magnitude +
				", '0+$', ''))",
		},
		result,
	)
	return bindSQLExpressions([]string{magnitude}, []string{magnitudeSQL}, result)
}
