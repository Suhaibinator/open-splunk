package clickhouse

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// splAutoLeadingNumericPattern extracts the numeric prefix used by Splunk's
// automatic ordering for strings that begin with a number. The complete
// numeric parser remains authoritative: this expression only selects the
// prefix and exactNumericOrderingKeySQL validates and orders it.
// Avoid '?' and brace regex quantifiers because the driver treats those SQL
// characters as bind syntax. The first capture is the complete match; the
// empty alternatives express optional sign, decimal fraction, and exponent.
const splAutoLeadingNumericPattern = `^(([+]|-|)(([0-9]+([.][0-9]*|))|([.][0-9]+))([eE]([+]|-|)[0-9]+|))`

// dynamicSortValue is retained for chart/timechart row-domain ordering. Those
// callers have already excluded missing rows and currently promise only the
// legacy numeric-or-lexical behavior. Pipeline sort uses sortFieldValueSQL so
// it can additionally honor explicit modes, IPs, leading numbers, and MVs.
func dynamicSortValue(valueSQL string, dynamicValue bool) string {
	scalar := compiledScalar{valueSQL: valueSQL, kind: fieldKindString}
	if dynamicValue {
		scalar = compiledScalar{
			valueSQL:       valueSQL,
			dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
			kind:           fieldKindDynamic,
		}
	}
	numericText := exactNumericScalarTextSQL(scalar)
	lexicalText := exactNumericScalarLexicalTextSQL(scalar)
	textVariable := "__os_sort_exact_text"
	lexicalVariable := "__os_sort_lexical_text"
	nullVariable := "__os_sort_exact_null"
	keyVariable := "__os_sort_exact_key"
	numeric := exactNumericKeyEligibleSQL(keyVariable)
	keyValue := exactNumericKeyValueSQL(keyVariable)
	body := "tuple(" +
		"if(" + nullVariable + " != 0, toUInt8(2), if(" + numeric +
		", toUInt8(0), toUInt8(1))), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 1), toUInt8(1)), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 2), toInt64(0)), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 3), CAST('' AS String)), " +
		"ifNull(" + lexicalVariable + ", CAST('' AS String)))"
	boundKey := bindSQLExpressions(
		[]string{keyVariable},
		[]string{exactNumericOrderingKeySQL(textVariable)},
		body,
	)
	return bindSQLExpressions(
		[]string{textVariable, lexicalVariable, nullVariable},
		[]string{numericText, lexicalText, "toUInt8(isNull(" + valueSQL + "))"},
		boundKey,
	)
}

// sortFieldPresenceSQL returns zero/false for both a missing field and an
// explicit null. Sort materializes its negation as a separately directed key,
// so absent values remain last for direct ascending and descending sorts. A
// downstream tail/reversal deliberately reverses this key with the value key.
func sortFieldPresenceSQL(field fieldState) (string, []any) {
	if isNativeMultivalueKind(field.kind) {
		return logicalFieldPresenceSQL(field)
	}
	return "isNotNull(" + field.valueSQL + ")", nil
}

// sortFieldValueSQL emits an Array key for every value. Singleton scalar keys
// retain scalar ordering, while native and runtime multivalue fields use
// ClickHouse's deterministic lexicographic array comparison: members are
// compared left-to-right and a shorter equal-prefix array sorts first. Splunk's
// public sort reference does not define event-level multivalue comparison, so
// this is an explicit compatibility approximation rather than an oracle claim.
func sortFieldValueSQL(field fieldState, mode plan.SortValueMode) (string, error) {
	if !validSortValueMode(mode) {
		return "", fmt.Errorf("compile ClickHouse sort: invalid value mode %d", mode)
	}

	if field.kind == fieldKindTime && mode == plan.SortValueModeAuto {
		// DateTime64 has the desired chronological total order and avoids turning
		// canonical event time into display text before ordering.
		return "[" + field.valueSQL + "]", nil
	}
	if field.kind == fieldKindTime && mode == plan.SortValueModeNumeric {
		text := "toString(toUnixTimestamp64Nano(" + field.valueSQL + "))"
		return "[" + sortScalarOrderingKeySQL(text, text, mode) + "]", nil
	}

	switch field.kind {
	case fieldKindStringArray:
		member := "__os_sort_mv_member"
		memberText := "ifNull(toString(" + member + "), CAST('' AS String))"
		key := sortScalarOrderingKeySQL(memberText, memberText, mode)
		return "arrayMap(" + member + " -> " + key + ", " + field.valueSQL + ")", nil
	case fieldKindDynamicArray:
		member := "__os_sort_mv_member"
		memberText := nativeMVCanonicalTextSQL(member)
		key := sortScalarOrderingKeySQL(memberText, memberText, mode)
		return "arrayMap(" + member + " -> " + key + ", " + field.valueSQL + ")", nil
	case fieldKindDynamic:
		return sortDynamicFieldValueSQL(field, mode), nil
	default:
		scalar := compiledScalarFromField(field)
		numericText := exactNumericScalarTextSQL(scalar)
		lexicalText := exactNumericScalarLexicalTextSQL(scalar)
		return "[" + sortScalarOrderingKeySQL(numericText, lexicalText, mode) + "]", nil
	}
}

func validSortValueMode(mode plan.SortValueMode) bool {
	switch mode {
	case plan.SortValueModeAuto,
		plan.SortValueModeLexical,
		plan.SortValueModeNumeric,
		plan.SortValueModeIP:
		return true
	default:
		return false
	}
}

func sortDynamicFieldValueSQL(field fieldState, mode plan.SortValueMode) string {
	scalar := compiledScalarFromField(field)
	scalarKey := sortScalarOrderingKeySQL(
		exactNumericScalarTextSQL(scalar),
		exactNumericScalarLexicalTextSQL(scalar),
		mode,
	)
	stringMember := "__os_sort_string_member"
	stringKey := sortScalarOrderingKeySQL(stringMember, stringMember, mode)
	dynamicMember := "__os_sort_dynamic_member"
	dynamicText := nativeMVCanonicalTextSQL(dynamicMember)
	dynamicKey := sortScalarOrderingKeySQL(dynamicText, dynamicText, mode)
	typeSQL := field.dynamicTypeSQL
	if typeSQL == "" {
		typeSQL = "dynamicType(" + field.valueSQL + ")"
	}
	return "multiIf(" +
		typeSQL + " = 'Array(String)', arrayMap(" + stringMember + " -> " +
		stringKey + ", dynamicElement(" + field.valueSQL + ", 'Array(String)')), " +
		typeSQL + " = 'Array(Dynamic)', arrayMap(" + dynamicMember + " -> " +
		dynamicKey + ", dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')), " +
		"[" + scalarKey + "])"
}

func sortScalarOrderingKeySQL(
	numericTextSQL string,
	lexicalTextSQL string,
	mode plan.SortValueMode,
) string {
	lexical := "ifNull(" + lexicalTextSQL + ", CAST('' AS String))"
	switch mode {
	case plan.SortValueModeLexical:
		return lexical
	case plan.SortValueModeNumeric:
		return numericSortOrderingKeySQL(numericTextSQL, lexical)
	case plan.SortValueModeIP:
		return ipSortOrderingKeySQL(lexical)
	case plan.SortValueModeAuto:
		return autoSortOrderingKeySQL(numericTextSQL, lexical)
	default:
		panic("sort scalar ordering key: invalid value mode")
	}
}

// numericSortOrderingKeySQL keeps exact decimal order for every supported
// integer/decimal spelling. Values that cannot be interpreted numerically use
// a deterministic lexical fallback rather than collapsing to one key.
func numericSortOrderingKeySQL(numericTextSQL, lexicalTextSQL string) string {
	textVariable := "__os_sort_numeric_text"
	lexicalVariable := "__os_sort_lexical_text"
	keyVariable := "__os_sort_numeric_key"
	numeric := exactNumericKeyEligibleSQL(keyVariable)
	keyValue := exactNumericKeyValueSQL(keyVariable)
	body := "tuple(" +
		"if(" + numeric + ", toUInt8(0), toUInt8(1)), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 1), toUInt8(1)), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 2), toInt64(0)), " +
		"if(" + numeric + ", tupleElement(" + keyValue + ", 3), CAST('' AS String)), " +
		lexicalVariable + ")"
	body = bindSQLExpressions(
		[]string{keyVariable},
		[]string{exactNumericOrderingKeySQL(textVariable)},
		body,
	)
	return bindSQLExpressions(
		[]string{textVariable, lexicalVariable},
		[]string{numericTextSQL, lexicalTextSQL},
		body,
	)
}

// ipSortOrderingKeySQL maps IPv4 into IPv4-mapped IPv6 space and compares the
// resulting 128-bit addresses. Invalid text follows valid addresses and is
// ordered lexically, which preserves a deterministic total order for mixed
// fields even though Splunk documents only the valid-IP interpretation.
func ipSortOrderingKeySQL(lexicalTextSQL string) string {
	textVariable := "__os_sort_ip_text"
	ipv4Variable := "__os_sort_ipv4"
	ipv6Variable := "__os_sort_ipv6"
	ipv4 := "toIPv4OrNull(if(length(" + textVariable + ") <= 15, " +
		textVariable + ", CAST('' AS String)))"
	ipv6 := "toIPv6OrNull(if(length(" + textVariable + ") <= 45, " +
		textVariable + ", CAST('' AS String)))"
	valid := "isNotNull(" + ipv4Variable + ") OR isNotNull(" + ipv6Variable + ")"
	address := "if(isNotNull(" + ipv4Variable + "), IPv4ToIPv6(ifNull(" +
		ipv4Variable + ", toIPv4('0.0.0.0'))), ifNull(" + ipv6Variable +
		", toIPv6('::')))"
	body := "tuple(if(" + valid + ", toUInt8(0), toUInt8(1)), " + address +
		", " + textVariable + ")"
	body = bindSQLExpressions(
		[]string{ipv4Variable, ipv6Variable},
		[]string{ipv4, ipv6},
		body,
	)
	return bindSQLExpressions(
		[]string{textVariable},
		[]string{lexicalTextSQL},
		body,
	)
}

// autoSortOrderingKeySQL implements a deterministic transitive approximation
// of Splunk's documented pairwise automatic comparator. A fixed SQL ORDER BY
// key cannot select a different comparison mode for each pair, so mixed-domain
// sets necessarily use stable domain ranks. Within those ranks, complete
// numbers and simple number-leading alphanumeric strings sort by the same exact
// numeric key, valid IPs sort by address, and remaining values sort
// lexicographically. The dotted-alphanumeric guard preserves Splunk's explicit
// `9.1.a, 10.1.a` descending example rather than treating `9.1`/`10.1` as
// decimal prefixes. The final lexical component breaks equal numeric/IP keys.
func autoSortOrderingKeySQL(numericTextSQL, lexicalTextSQL string) string {
	numericTextVariable := "__os_sort_exact_text"
	lexicalVariable := "__os_sort_lexical_text"
	numericKeyVariable := "__os_sort_exact_key"
	leadingTextVariable := "__os_sort_leading_text"
	leadingKeyVariable := "__os_sort_leading_key"
	ipKeyVariable := "__os_sort_ip_key"

	numeric := exactNumericKeyEligibleSQL(numericKeyVariable)
	dottedAlphanumeric := "position(" + leadingTextVariable + ", '.') != 0 AND " +
		"startsWith(substring(" + lexicalVariable + ", length(" +
		leadingTextVariable + ") + 1), '.')"
	leading := "NOT (" + numeric + ") AND NOT (" +
		"tupleElement(" + ipKeyVariable + ", 1) = toUInt8(0)) AND " +
		exactNumericKeyEligibleSQL(leadingKeyVariable) + " AND NOT (" +
		dottedAlphanumeric + ")"
	ip := "NOT (" + numeric + ") AND tupleElement(" + ipKeyVariable + ", 1) = toUInt8(0)"
	chosenNumericKey := "if(" + numeric + ", " + numericKeyVariable + ", " + leadingKeyVariable + ")"
	chosenNumericValue := exactNumericKeyValueSQL(chosenNumericKey)
	numericLike := numeric + " OR " + leading
	category := "multiIf(" + numericLike + ", toUInt8(0), " + ip +
		", toUInt8(1), toUInt8(2))"
	body := "tuple(" + category + ", " +
		"if(" + numericLike + ", tupleElement(" + chosenNumericValue + ", 1), toUInt8(1)), " +
		"if(" + numericLike + ", tupleElement(" + chosenNumericValue + ", 2), toInt64(0)), " +
		"if(" + numericLike + ", tupleElement(" + chosenNumericValue + ", 3), CAST('' AS String)), " +
		"tupleElement(" + ipKeyVariable + ", 2), " + lexicalVariable + ")"
	body = bindSQLExpressions(
		[]string{numericKeyVariable, leadingKeyVariable, ipKeyVariable},
		[]string{
			exactNumericOrderingKeySQL(numericTextVariable),
			exactNumericOrderingKeySQL(leadingTextVariable),
			ipSortOrderingKeySQL(lexicalVariable),
		},
		body,
	)
	leadingText := "if(length(" + lexicalVariable + ") <= " +
		fmt.Sprintf("%d", MaximumExactNumericOrderingInputTextBytes) +
		", extract(" + lexicalVariable + ", '" + splAutoLeadingNumericPattern +
		"'), CAST('' AS String))"
	body = bindSQLExpressions(
		[]string{leadingTextVariable},
		[]string{leadingText},
		body,
	)
	return bindSQLExpressions(
		[]string{numericTextVariable, lexicalVariable},
		[]string{numericTextSQL, lexicalTextSQL},
		body,
	)
}
