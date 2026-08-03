package clickhouse

import (
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// MaximumExactNumericOrderingInputTextBytes is the attacker-controlled raw
// text bound shared with exact binning and runtime extrema. A compiler-created
// decimal/v1 envelope may be one byte longer when ".5" is normalized to
// "0.5", so the parser has one reserved publication byte. Raw inputs are
// explicitly bounded before they reach that larger parser limit.
const (
	MaximumExactNumericOrderingInputTextBytes = MaximumExactNumericBinTextBytes
	MaximumExactNumericOrderingTextBytes      = MaximumExactNumericBinTextBytes + 1
)

func bindSQLExpressions(parameters, values []string, body string) string {
	if len(parameters) != len(values) {
		panic("bind SQL expressions: parameter/value count mismatch")
	}
	arrays := make([]string, len(values))
	for index, value := range values {
		arrays[index] = "[" + value + "]"
	}
	return "arrayElement(arrayMap((" + strings.Join(parameters, ", ") + ") -> " +
		body + ", " + strings.Join(arrays, ", ") + "), 1)"
}

// exactNumericOrderingKeySQL returns a total ascending key for one complete
// decimal spelling:
//
//	(eligible, sign class, decimal order, normalized coefficient)
//
// Sign classes are negative, zero, positive. For a nonzero positive value the
// decimal order is the number of coefficient digits that precede the decimal
// point after applying the exponent. Equal orders compare by coefficient
// digits with insignificant leading/trailing zeroes removed. Negative values
// reverse both magnitude components; ':' terminates the complemented
// coefficient above every decimal digit so prefix values reverse correctly.
//
// Parsing is lexical and linear in at most 4 KiB of raw input plus the one
// reserved publication byte. Exponents are never expanded. Nonzero values
// accept exponent magnitudes through 10,000, which covers every finite Float64
// spelling and the existing bounded exact-decimal operations. Larger nonzero
// spellings receive eligible=0 and remain in their caller's established
// nonnumeric/null path; every syntactically valid zero spelling normalizes to
// the same key without parsing its exponent magnitude.
func exactNumericOrderingKeySQL(valueSQL string) string {
	raw := "__os_exact_order_text"
	bounded := "__os_exact_order_bounded"
	valid := "__os_exact_order_valid"
	negative := "__os_exact_order_negative"
	body := "__os_exact_order_body"
	exponentPosition := "__os_exact_order_exponent_position"
	significand := "__os_exact_order_significand"
	exponentText := "__os_exact_order_exponent_text"
	exponentNegative := "__os_exact_order_exponent_negative"
	exponentDigits := "__os_exact_order_exponent_digits"
	exponentTrimmed := "__os_exact_order_exponent_trimmed"
	exponentEligible := "__os_exact_order_exponent_eligible"
	exponentMagnitude := "__os_exact_order_exponent_magnitude"
	fractionDigits := "__os_exact_order_fraction_digits"
	significant := "__os_exact_order_significant"
	coefficient := "__os_exact_order_coefficient"
	decimalOrder := "__os_exact_order_decimal_order"

	limit := strconv.Itoa(MaximumExactNumericOrderingTextBytes)
	maximumExponent := strconv.Itoa(jsonnumber.MaximumExponentMagnitude)
	maximumExponentDigits := strconv.Itoa(len(maximumExponent))
	boundedSQL := "if(length(" + raw + ") <= " + limit + ", " + raw +
		", CAST('' AS String))"
	validSQL := "toUInt8(isValidUTF8(" + bounded + ") AND length(" + raw + ") <= " +
		limit + " AND match(" + bounded + ", " + decimalNumericStringPattern + "))"
	signOffset := "if(startsWith(" + bounded + ", '-') OR startsWith(" +
		bounded + ", '+'), 2, 1)"
	bodySQL := "substring(" + bounded + ", " + signOffset + ")"
	exponentPositionSQL := "greatest(position(" + body + ", 'e'), position(" +
		body + ", 'E'))"
	significandSQL := "if(" + exponentPosition + " = 0, " + body +
		", substring(" + body + ", 1, " + exponentPosition + " - 1))"
	exponentTextSQL := "if(" + exponentPosition + " = 0, CAST('0' AS String), substring(" +
		body + ", " + exponentPosition + " + 1))"
	exponentOffset := "if(startsWith(" + exponentText + ", '-') OR startsWith(" +
		exponentText + ", '+'), 2, 1)"
	exponentDigitsSQL := "substring(" + exponentText + ", " + exponentOffset + ")"
	exponentTrimmedSQL := "replaceRegexpOne(" + exponentDigits + ", '^0+', '')"
	exponentEligibleSQL := "toUInt8(empty(" + exponentTrimmed + ") OR length(" +
		exponentTrimmed + ") < " + maximumExponentDigits + " OR (length(" +
		exponentTrimmed + ") = " + maximumExponentDigits + " AND " +
		exponentTrimmed + " <= '" + maximumExponent + "'))"
	exponentMagnitudeSQL := "toInt64OrZero(if(" + exponentEligible +
		" != 0 AND NOT empty(" + exponentTrimmed + "), " + exponentTrimmed +
		", CAST('0' AS String)))"
	fractionDigitsSQL := "toInt64(if(position(" + significand +
		", '.') = 0, 0, length(" + significand + ") - position(" + significand + ", '.')))"
	significantSQL := "replaceRegexpOne(replaceAll(" + significand +
		", '.', ''), '^0+', '')"
	coefficientSQL := "replaceRegexpOne(" + significant + ", '0+$', '')"
	signedExponent := "if(" + exponentNegative + " != 0, -(" +
		exponentMagnitude + "), " + exponentMagnitude + ")"
	decimalOrderSQL := "toInt64(length(" + significant + ")) + " +
		signedExponent + " - " + fractionDigits

	zero := "empty(" + significant + ")"
	eligible := "toUInt8(" + valid + " != 0 AND (" + zero + " OR " +
		exponentEligible + " != 0))"
	signClass := "toUInt8(multiIf(" + zero + ", 1, " + negative + " != 0, 0, 2))"
	orderKey := "multiIf(" + zero + ", toInt64(0), " + negative +
		" != 0, -(" + decimalOrder + "), " + decimalOrder + ")"
	coefficientKey := "multiIf(" + zero + ", CAST('' AS String), " +
		negative + " != 0, concat(translate(" + coefficient +
		", '0123456789', '9876543210'), ':'), " + coefficient + ")"
	result := "tuple(" + eligible + ", " + signClass + ", " +
		orderKey + ", " + coefficientKey + ")"

	result = bindSQLExpressions(
		[]string{coefficient, decimalOrder},
		[]string{coefficientSQL, decimalOrderSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{fractionDigits, significant},
		[]string{fractionDigitsSQL, significantSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentMagnitude},
		[]string{exponentMagnitudeSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentEligible},
		[]string{exponentEligibleSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentTrimmed},
		[]string{exponentTrimmedSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentNegative, exponentDigits},
		[]string{
			"toUInt8(startsWith(" + exponentText + ", '-'))",
			exponentDigitsSQL,
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{significand, exponentText},
		[]string{significandSQL, exponentTextSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentPosition},
		[]string{exponentPositionSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{negative, body},
		[]string{
			"toUInt8(startsWith(" + bounded + ", '-'))",
			bodySQL,
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{valid},
		[]string{validSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{bounded},
		[]string{boundedSQL},
		result,
	)
	return bindSQLExpressions([]string{raw}, []string{valueSQL}, result)
}

// trustedFiniteFloatOrderingKeySQL returns the same key shape as
// exactNumericOrderingKeySQL for one non-null, finite Float64 expression. It
// is deliberately not a String helper: callers must establish the Float64
// type and finite/null handling before entering this trusted path.
//
// The pinned ClickHouse server formats every finite Float64 as a short ASCII
// decimal accepted by the generic parser. That lets this publication-only
// path omit attacker-facing byte, UTF-8, grammar, and exponent-work bounds
// while retaining the sign, exponent, and coefficient normalization required
// for exact key equality. Raw event text must continue to use
// exactNumericOrderingKeySQL.
func trustedFiniteFloatOrderingKeySQL(valueSQL string) string {
	raw := "__os_trusted_float_order_text"
	negative := "__os_trusted_float_order_negative"
	body := "__os_trusted_float_order_body"
	exponentPosition := "__os_trusted_float_order_exponent_position"
	significand := "__os_trusted_float_order_significand"
	exponent := "__os_trusted_float_order_exponent"
	fractionDigits := "__os_trusted_float_order_fraction_digits"
	significant := "__os_trusted_float_order_significant"
	coefficient := "__os_trusted_float_order_coefficient"
	decimalOrder := "__os_trusted_float_order_decimal_order"

	bodySQL := "if(startsWith(" + raw + ", '-'), substring(" + raw + ", 2), " + raw + ")"
	exponentPositionSQL := "position(" + body + ", 'e')"
	significandSQL := "if(" + exponentPosition + " = 0, " + body +
		", substring(" + body + ", 1, " + exponentPosition + " - 1))"
	exponentTextSQL := "if(" + exponentPosition + " = 0, CAST('0' AS String), substring(" +
		body + ", " + exponentPosition + " + 1))"
	exponentSQL := "toInt16OrZero(" + exponentTextSQL + ")"
	fractionDigitsSQL := "toInt64(if(position(" + significand +
		", '.') = 0, 0, length(" + significand + ") - position(" + significand + ", '.')))"
	significantSQL := "trimLeft(replaceAll(" + significand + ", '.', ''), '0')"
	coefficientSQL := "trimRight(" + significant + ", '0')"
	decimalOrderSQL := "toInt64(length(" + significant + ")) + toInt64(" +
		exponent + ") - " + fractionDigits

	zero := "empty(" + significant + ")"
	signClass := "toUInt8(multiIf(" + zero + ", 1, " + negative + " != 0, 0, 2))"
	orderKey := "multiIf(" + zero + ", toInt64(0), " + negative +
		" != 0, -(" + decimalOrder + "), " + decimalOrder + ")"
	coefficientKey := "multiIf(" + zero + ", CAST('' AS String), " +
		negative + " != 0, concat(translate(" + coefficient +
		", '0123456789', '9876543210'), ':'), " + coefficient + ")"
	result := "tuple(toUInt8(1), " + signClass + ", " + orderKey + ", " +
		coefficientKey + ")"

	result = bindSQLExpressions(
		[]string{coefficient, decimalOrder},
		[]string{coefficientSQL, decimalOrderSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{fractionDigits, significant},
		[]string{fractionDigitsSQL, significantSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{significand, exponent},
		[]string{significandSQL, exponentSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentPosition},
		[]string{exponentPositionSQL},
		result,
	)
	result = bindSQLExpressions(
		[]string{negative, body},
		[]string{
			"toUInt8(startsWith(" + raw + ", '-'))",
			bodySQL,
		},
		result,
	)
	return bindSQLExpressions(
		[]string{raw},
		[]string{"toString(" + valueSQL + ")"},
		result,
	)
}

func exactNumericKeyEligibleSQL(keySQL string) string {
	return "tupleElement(" + keySQL + ", 1) != 0"
}

func exactNumericKeyValueSQL(keySQL string) string {
	return "tuple(tupleElement(" + keySQL + ", 2), tupleElement(" + keySQL +
		", 3), tupleElement(" + keySQL + ", 4))"
}

func exactNumericKeyComparisonSQL(leftKeySQL, rightKeySQL, operator string) string {
	left := "__os_exact_order_left"
	right := "__os_exact_order_right"
	body := "if(" + exactNumericKeyEligibleSQL(left) + " AND " +
		exactNumericKeyEligibleSQL(right) + ", " +
		exactNumericKeyValueSQL(left) + " " + operator + " " +
		exactNumericKeyValueSQL(right) + ", CAST(NULL AS Nullable(Bool)))"
	return bindSQLExpressions(
		[]string{left, right},
		[]string{leftKeySQL, rightKeySQL},
		body,
	)
}

func exactNumericScalarTextSQL(value compiledScalar) string {
	if value.kind != fieldKindDynamic {
		text := exactNumericScalarLexicalTextSQL(value)
		if value.kind == fieldKindString {
			return boundedExactNumericOrderingInputSQL(text)
		}
		return text
	}
	if value.dynamicDomain == dynamicScalarDomainNumeric {
		return "toString(" + value.valueSQL + ")"
	}
	decimal, payload := dynamicTaggedOrderingDecimalText(value)
	raw := "toString(" + value.valueSQL + ")"
	return "if(" + decimal + ", " + payload + ", " +
		boundedExactNumericOrderingInputSQL(raw) + ")"
}

// exactNumericScalarLexicalTextSQL retains the complete logical scalar text
// for nonnumeric ordering. Numeric classification is independently bounded by
// exactNumericScalarTextSQL; callers must not reuse that sanitized classifier
// input as the lexical fallback or distinct over-limit Strings would collapse.
func exactNumericScalarLexicalTextSQL(value compiledScalar) string {
	raw := "toString(" + value.valueSQL + ")"
	if value.kind != fieldKindDynamic {
		return raw
	}
	decimal, payload := dynamicTaggedOrderingDecimalText(value)
	return "if(" + decimal + ", " + payload + ", " + raw + ")"
}

func exactNumericScalarKeySQL(value compiledScalar) string {
	if value.exactNumericKeySQL != "" {
		return value.exactNumericKeySQL
	}
	if literal, ok := exactNumericLiteralOrderingKeySQL(value.literal); ok {
		return literal
	}
	return exactNumericOrderingKeySQL(exactNumericScalarTextSQL(value))
}

type exactNumericLiteralKey struct {
	eligible     bool
	signClass    uint8
	decimalOrder int64
	coefficient  string
}

func exactNumericLiteralOrderingKeySQL(
	value *plan.Value,
) (string, bool) {
	if value == nil || (value.Kind != plan.ValueKindInt64 &&
		value.Kind != plan.ValueKindUint64 &&
		value.Kind != plan.ValueKindFloat64) {
		return "", false
	}
	key := parseExactNumericLiteralKey(comparisonSourceText(*value))
	keySQL := "tuple(toUInt8(" + boolSQL(key.eligible) + "), toUInt8(" +
		strconv.FormatUint(uint64(key.signClass), 10) + "), toInt64(" +
		strconv.FormatInt(key.decimalOrder, 10) + "), CAST('" +
		key.coefficient + "' AS String))"
	// The trusted compiler derives this constant from the typed literal's
	// source spelling. Only normalized digits produced by the parser are
	// embedded; the original query lexeme remains outside generated SQL.
	return keySQL, true
}

func exactNumericLiteralFieldComparisonSQL(
	dynamic compiledScalar,
	value *plan.Value,
	operator string,
) string {
	literalKey, ok := exactNumericLiteralOrderingKeySQL(value)
	if !ok {
		panic("exact numeric literal comparison: nonnumeric literal")
	}
	return exactNumericKeyComparisonSQL(
		exactNumericScalarKeySQL(dynamic),
		literalKey,
		operator,
	)
}

func parseExactNumericLiteralKey(text string) exactNumericLiteralKey {
	ineligible := exactNumericLiteralKey{}
	if text == "" {
		return ineligible
	}
	negative := false
	switch text[0] {
	case '-', '+':
		negative = text[0] == '-'
		text = text[1:]
		if text == "" {
			return ineligible
		}
	}

	exponentText := ""
	if position := strings.IndexAny(text, "eE"); position >= 0 {
		if strings.ContainsAny(text[position+1:], "eE") {
			return ineligible
		}
		exponentText = text[position+1:]
		text = text[:position]
		if exponentText == "" {
			return ineligible
		}
	}

	dot := strings.IndexByte(text, '.')
	if dot >= 0 && strings.IndexByte(text[dot+1:], '.') >= 0 {
		return ineligible
	}
	digits := text
	fractionDigits := 0
	if dot >= 0 {
		fractionDigits = len(text) - dot - 1
		digits = text[:dot] + text[dot+1:]
	}
	if digits == "" || !asciiDecimalDigits(digits) {
		return ineligible
	}
	significant := strings.TrimLeft(digits, "0")
	if significant == "" {
		return exactNumericLiteralKey{
			eligible:  true,
			signClass: 1,
		}
	}
	if len(significant) > MaximumExactNumericOrderingTextBytes {
		return ineligible
	}

	exponent, ok := boundedExactNumericLiteralExponent(exponentText)
	if !ok {
		return ineligible
	}
	decimalOrder := int64(len(significant)-fractionDigits) + exponent
	coefficient := strings.TrimRight(significant, "0")
	signClass := uint8(2)
	if negative {
		signClass = 0
		decimalOrder = -decimalOrder
		var complemented strings.Builder
		complemented.Grow(len(coefficient) + 1)
		for index := range len(coefficient) {
			complemented.WriteByte('9' - (coefficient[index] - '0'))
		}
		complemented.WriteByte(':')
		coefficient = complemented.String()
	}
	return exactNumericLiteralKey{
		eligible:     true,
		signClass:    signClass,
		decimalOrder: decimalOrder,
		coefficient:  coefficient,
	}
}

func boundedExactNumericLiteralExponent(text string) (int64, bool) {
	if text == "" {
		return 0, true
	}
	negative := false
	switch text[0] {
	case '-', '+':
		negative = text[0] == '-'
		text = text[1:]
	}
	if text == "" || !asciiDecimalDigits(text) {
		return 0, false
	}
	text = strings.TrimLeft(text, "0")
	if text == "" {
		return 0, true
	}
	maximumExponent := strconv.Itoa(jsonnumber.MaximumExponentMagnitude)
	if len(text) > len(maximumExponent) ||
		(len(text) == len(maximumExponent) && text > maximumExponent) {
		return 0, false
	}
	exponent, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	if negative {
		exponent = -exponent
	}
	return exponent, true
}

func asciiDecimalDigits(text string) bool {
	for index := range len(text) {
		if text[index] < '0' || text[index] > '9' {
			return false
		}
	}
	return true
}

func boolSQL(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func boundedExactNumericOrderingInputSQL(valueSQL string) string {
	return "if(length(" + valueSQL + ") <= " +
		strconv.Itoa(MaximumExactNumericOrderingInputTextBytes) + ", " +
		valueSQL + ", CAST('' AS String))"
}

func dynamicTaggedOrderingDecimalText(
	value compiledScalar,
) (condition, payload string) {
	return dynamicTaggedDecimalTextWithLimit(
		value,
		MaximumExactNumericOrderingTextBytes,
	)
}

func decimalEnvelopeTextSQL(valueSQL string) string {
	// The runtime String grammar additionally permits a leading plus, a missing
	// integer before '.', and a trailing '.'. Normalize only those syntax
	// differences here; transport canonicalization removes insignificant zeros
	// and exponent spellings after the query boundary.
	canonical := "__os_decimal_envelope_canonical"
	unsigned := "__os_decimal_envelope_unsigned"
	integer := "__os_decimal_envelope_integer"
	exponentPosition := "__os_decimal_envelope_exponent_position"
	significand := "__os_decimal_envelope_significand"
	exponent := "__os_decimal_envelope_exponent"
	result := "concat(if(endsWith(" + significand + ", '.'), substring(" +
		significand + ", 1, length(" + significand + ") - 1), " +
		significand + "), " + exponent + ")"
	result = bindSQLExpressions(
		[]string{significand, exponent},
		[]string{
			"if(" + exponentPosition + " = 0, " + integer + ", substring(" +
				integer + ", 1, " + exponentPosition + " - 1))",
			"if(" + exponentPosition + " = 0, CAST('' AS String), substring(" +
				integer + ", " + exponentPosition + "))",
		},
		result,
	)
	result = bindSQLExpressions(
		[]string{exponentPosition},
		[]string{"greatest(position(" + integer + ", 'e'), position(" +
			integer + ", 'E'))"},
		result,
	)
	result = bindSQLExpressions(
		[]string{integer},
		[]string{"multiIf(startsWith(" + unsigned + ", '.'), concat('0', " +
			unsigned + "), startsWith(" + unsigned + ", '-.'), concat('-0', substring(" +
			unsigned + ", 2)), " + unsigned + ")"},
		result,
	)
	result = bindSQLExpressions(
		[]string{unsigned},
		[]string{"if(startsWith(" + canonical + ", '+'), substring(" +
			canonical + ", 2), " + canonical + ")"},
		result,
	)
	return bindSQLExpressions(
		[]string{canonical},
		[]string{canonicalNumericTextSQL(valueSQL)},
		result,
	)
}

func decimalEnvelopeDynamicSQL(valueSQL string) string {
	return decimalEnvelopePayloadDynamicSQL(decimalEnvelopeTextSQL(valueSQL))
}

// decimalEnvelopePayloadDynamicSQL wraps an already-normalized decimal/v1
// payload. Lexical extraction uses this narrower constructor so its collector-
// canonical spelling is not coupled to computed-result normalization rules.
func decimalEnvelopePayloadDynamicSQL(payloadSQL string) string {
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	return "CAST(map(" + typeKey + ", 'decimal/v1', " + valueKey + ", " +
		payloadSQL + ") AS Dynamic)"
}
