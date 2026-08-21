package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

// nativeMVState is a row-local normalization tuple:
//
//	(Array(Dynamic) values, UInt8 exists, UInt8 present, UInt8 invalid)
//
// Keeping the validation bit beside the value lets every consumer fail the
// complete result atomically instead of silently dropping an unsupported
// member. The independent existence and list-presence bits preserve all three
// public states: missing, explicit null, and a present (possibly empty) list.
type nativeMVState struct {
	sql  string
	args []any
}

type nativeMVScalarCompiler func(
	*plan.ScalarCallExpression,
	compileState,
) (compiledScalar, error)

func compileBoundedNativeMVScalar(
	expression *plan.ScalarCallExpression,
	state compileState,
	compile nativeMVScalarCompiler,
) (compiledScalar, error) {
	if compile == nil {
		return compiledScalar{}, errors.New("compile ClickHouse native multivalue: missing compiler")
	}
	result, err := compile(expression, state)
	if err != nil {
		return compiledScalar{}, err
	}
	size := len(result.valueSQL)
	for _, sql := range []string{
		result.existsSQL,
		result.optionalMultivaluePresentSQL,
		result.textEligibleSQL,
		result.semanticBytesSQL,
	} {
		if len(sql) > maxCompiledNativeMVScalarSQLBytes-size {
			return compiledScalar{}, &plan.Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("native multivalue scalar SQL exceeds %d bytes", maxCompiledNativeMVScalarSQLBytes),
				Range:   expression.Range,
			}
		}
		size += len(sql)
	}
	return result, nil
}

func emptyNativeMVSQL() string { return "CAST([], 'Array(Dynamic)')" }

func nativeMVPreflightSQL(
	resultSQL, invalidSQL, membersSQL, payloadSQL, emptySQL string,
) string {
	members := strconv.Itoa(spl.MaximumNativeMVValues)
	bytes := strconv.Itoa(spl.MaximumNativeMVPayloadBytes)
	return "if(throwIf(toUInt8(" + invalidSQL + "), '" +
		UnsupportedNativeMVValueMarker + "') = 0, " +
		"if(throwIf(toUInt8(" + membersSQL + " > toUInt64(" + members +
		")), '" + NativeMVMembersLimitMarker + "') = 0, " +
		"if(throwIf(toUInt8(" + payloadSQL + " > toUInt128(" + bytes +
		")), '" + NativeMVPayloadLimitMarker + "') = 0, " + resultSQL +
		", " + emptySQL + "), " + emptySQL + "), " + emptySQL + ")"
}

func nativeMVLimitsGuardSQL(valuesSQL, invalidSQL string) string {
	empty := emptyNativeMVSQL()
	return nativeMVPreflightSQL(
		valuesSQL,
		invalidSQL,
		"length("+valuesSQL+")",
		nativeMVArrayPayloadBytesSQL(valuesSQL),
		empty,
	)
}

func stringMVLimitsGuardSQL(valuesSQL, invalidSQL string) string {
	empty := "CAST([], 'Array(String)')"
	payload := "arrayFold((bytes, member) -> bytes + toUInt128(length(member)), " +
		valuesSQL + ", toUInt128(0))"
	return nativeMVPreflightSQL(
		valuesSQL,
		invalidSQL,
		"length("+valuesSQL+")",
		payload,
		empty,
	)
}

func markNativeMVRuntimeValidation(state compileState) {
	if state.context == nil {
		return
	}
	state.context.atomicResult = true
	state.context.requiresMaterializedValidationSettings = true
}

func nativeMVElementSQL(valueSQL string) compiledScalar {
	return compiledScalar{
		valueSQL:       valueSQL,
		dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
		kind:           fieldKindDynamic,
	}
}

func nativeMVElementSupportedSQL(valueSQL string) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	value := nativeMVElementSQL(valueSQL)
	finiteFloat := "(startsWith(" + typeSQL + ", 'Float') AND isFinite(toFloat64(" +
		valueSQL + ")))"
	validDecimalEnvelope := "(" + dynamicTaggedDecimalCondition(value) + " AND " +
		dynamicTaggedScalarEnvelopeCondition(value) + ")"
	return "(" + typeSQL + " = 'None' OR (" + typeSQL + " = 'String' AND " +
		"isValidUTF8(dynamicElement(" + valueSQL + ", 'String'))) OR " +
		typeSQL + " = 'Bool' OR " + dynamicIntegerTypePredicate(typeSQL) + " OR " +
		finiteFloat + " OR startsWith(" + typeSQL + ", 'Decimal') OR " +
		validDecimalEnvelope + ")"
}

// nativeMVCanonicalTextSQL is used by mvjoin, mvzip, mvfind, presentation,
// payload accounting, and typed dedup keys.  Callers validate support first.
func nativeMVCanonicalTextSQL(valueSQL string) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	value := nativeMVElementSQL(valueSQL)
	decimalCondition, decimalPayload := dynamicTaggedDecimalTextWithLimit(
		value,
		spl.MaximumNativeMVPayloadBytes,
	)
	physicalDecimal := "startsWith(" + typeSQL + ", 'Decimal')"
	canonicalDecimalPayload := canonicalDecimalPayloadTextSQLWithLimit(
		"multiIf("+decimalCondition+", "+decimalPayload+", "+physicalDecimal+
			", toString("+valueSQL+"), CAST('0' AS String))",
		spl.MaximumNativeMVPayloadBytes,
	)
	return "multiIf(" +
		typeSQL + " = 'None', CAST('null' AS String), " +
		typeSQL + " = 'String', ifNull(dynamicElement(" + valueSQL +
		", 'String'), CAST('' AS String)), " +
		typeSQL + " = 'Bool', if(ifNull(dynamicElement(" + valueSQL +
		", 'Bool'), false), CAST('true' AS String), CAST('false' AS String)), " +
		"(" + decimalCondition + " OR " + physicalDecimal + "), " +
		canonicalDecimalPayload + ", toString(" + valueSQL + "))"
}

func nativeMVArrayPayloadBytesSQL(valuesSQL string) string {
	return "arrayFold((bytes, member) -> bytes + toUInt128(length(" +
		nativeMVCanonicalTextSQL("member") + ")), " + valuesSQL + ", toUInt128(0))"
}

func nativeMVDedupTypeSQL(valueSQL string) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	value := nativeMVElementSQL(valueSQL)
	decimalCondition, _ := dynamicTaggedDecimalTextWithLimit(
		value,
		spl.MaximumNativeMVPayloadBytes,
	)
	wideInteger := typeSQL + " IN ('Int128', 'Int256', 'UInt128', 'UInt256')"
	signed := typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64')"
	unsigned := typeSQL + " IN ('UInt8', 'UInt16', 'UInt32', 'UInt64')"
	return "multiIf(startsWith(" + typeSQL + ", 'Decimal') OR " +
		decimalCondition + " OR " + wideInteger + ", CAST('Decimal' AS String), " +
		signed + ", CAST('Signed' AS String), " + unsigned +
		", CAST('Unsigned' AS String), startsWith(" + typeSQL +
		", 'Float'), CAST('Double' AS String), " + typeSQL + ")"
}

func nativeMVDedupKeySQL(valueSQL string) string {
	return "concat(" + nativeMVDedupTypeSQL(valueSQL) + ", char(0), " +
		nativeMVDedupKeyTextSQL(valueSQL, nativeMVCanonicalTextSQL(valueSQL)) + ")"
}

// nativeMVDedupKeyTextSQL normalizes the one display-level spelling that is
// not also native numeric equality: IEEE negative zero. Keep the original
// member in the result array so a first-occurring -0 still renders as -0, but
// use the same equality key for -0 and +0.
func nativeMVDedupKeyTextSQL(valueSQL, canonicalSQL string) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	return "if(startsWith(" + typeSQL + ", 'Float') AND toFloat64(" +
		valueSQL + ") = toFloat64(0), CAST('0' AS String), " + canonicalSQL + ")"
}

func maximumMVJoinOutputBytes(delimiter string) uint64 {
	separators := uint64(spl.MaximumNativeMVValues - 1)
	return uint64(spl.MaximumNativeMVPayloadBytes) + separators*uint64(len(delimiter))
}

func scalarExistsSQL(value compiledScalar) (string, []any) {
	exists := value.existsSQL
	if exists == "" {
		exists = "1"
	}
	return exists, append([]any(nil), value.existsArgs...)
}

func nativeMVPreservedStateSQL(
	input compiledScalar,
	normalized nativeMVState,
) (existsSQL, presentSQL string, args []any) {
	if isNativeMultivalueKind(input.kind) {
		existsSQL, args = scalarExistsSQL(input)
		presentSQL = input.optionalMultivaluePresentSQL
		if presentSQL == "" {
			presentSQL = existsSQL
		}
		return existsSQL, presentSQL, args
	}
	stateAlias := "__os_native_mv_preserved_state"
	existsSQL = bindSQLExpressions(
		[]string{stateAlias},
		[]string{normalized.sql},
		"toUInt8(tupleElement("+stateAlias+", 2) != 0)",
	)
	presentSQL = bindSQLExpressions(
		[]string{stateAlias},
		[]string{normalized.sql},
		"toUInt8(tupleElement("+stateAlias+", 3) != 0)",
	)
	return existsSQL, presentSQL, append([]any(nil), normalized.args...)
}

// compileNativeMVState accepts the two sealed physical native-list kinds and
// the open-schema Dynamic list forms.  When allowScalar is true (mvappend), a
// supported scalar becomes a singleton, explicit Dynamic/literal null becomes
// a retained None member, and only a genuinely missing argument is skipped.
func compileNativeMVState(input compiledScalar, allowScalar bool) (nativeMVState, error) {
	empty := emptyNativeMVSQL()
	existsSQL, existsArgs := scalarExistsSQL(input)
	listPresentSQL := existsSQL
	listPresentArgs := append([]any(nil), existsArgs...)
	if input.optionalMultivaluePresentSQL != "" {
		listPresentSQL = input.optionalMultivaluePresentSQL
	}
	descendantSQL := input.descendantSQL
	if descendantSQL == "" {
		descendantSQL = "0"
	}
	if !allowScalar && input.alwaysNull && existsSQL == "0" {
		return nativeMVState{
			sql: "tuple(" + empty + ", toUInt8(0), toUInt8(0), toUInt8(0))",
		}, nil
	}

	bind := func(body string, extraParameters, extraValues []string, extraArgs []any) nativeMVState {
		parameters := []string{"value", "field_exists", "list_present", "descendant_present"}
		values := []string{
			input.valueSQL,
			"toUInt8(" + existsSQL + ")",
			"toUInt8(" + listPresentSQL + ")",
			"toUInt8(" + descendantSQL + ")",
		}
		parameters = append(parameters, extraParameters...)
		values = append(values, extraValues...)
		args := make([]any, 0, len(input.valueArgs)+len(existsArgs)+len(listPresentArgs)+len(input.descendantArgs)+len(extraArgs))
		args = append(args, input.valueArgs...)
		args = append(args, existsArgs...)
		args = append(args, listPresentArgs...)
		args = append(args, input.descendantArgs...)
		args = append(args, extraArgs...)
		return nativeMVState{sql: bindSQLExpressions(parameters, values, body), args: args}
	}

	switch input.kind {
	case fieldKindDynamicArray:
		unsupported := "arrayExists(member -> NOT (" +
			nativeMVElementSupportedSQL("member") + "), value)"
		values := "if(list_present != 0, value, " + empty + ")"
		present := "list_present != 0"
		if allowScalar {
			values = "multiIf(list_present != 0, value, field_exists != 0, " +
				"[CAST(NULL AS Dynamic)], " + empty + ")"
			present = "field_exists != 0"
		}
		body := "tuple(" + values + ", " +
			"toUInt8(field_exists != 0), toUInt8(" + present + "), " +
			"toUInt8(descendant_present != 0 OR (list_present != 0 AND " + unsupported + ")))"
		return bind(body, nil, nil, nil), nil
	case fieldKindStringArray:
		values := "arrayMap(member -> CAST(member AS Dynamic), value)"
		unsupported := "arrayExists(member -> NOT isValidUTF8(member), value)"
		selected := "if(list_present != 0, " + values + ", " + empty + ")"
		present := "list_present != 0"
		if allowScalar {
			selected = "multiIf(list_present != 0, " + values +
				", field_exists != 0, [CAST(NULL AS Dynamic)], " + empty + ")"
			present = "field_exists != 0"
		}
		body := "tuple(" + selected + ", " +
			"toUInt8(field_exists != 0), toUInt8(" + present + "), " +
			"toUInt8(descendant_present != 0 OR (list_present != 0 AND " + unsupported + ")))"
		return bind(body, nil, nil, nil), nil
	case fieldKindDynamic:
		typeSQL := "dynamicType(value)"
		stringMembers := "dynamicElement(value, 'Array(String)')"
		dynamicMembers := "dynamicElement(value, 'Array(Dynamic)')"
		stringInvalid := "arrayExists(member -> NOT isValidUTF8(member), " + stringMembers + ")"
		dynamicInvalid := "arrayExists(member -> NOT (" +
			nativeMVElementSupportedSQL("member") + "), " + dynamicMembers + ")"
		scalarSupported := nativeMVElementSupportedSQL("value")
		values := "multiIf(" +
			"field_exists = 0, " + empty + ", " +
			typeSQL + " = 'Array(String)', arrayMap(member -> CAST(member AS Dynamic), " + stringMembers + "), " +
			typeSQL + " = 'Array(Dynamic)', " + dynamicMembers + ", "
		if allowScalar {
			values += "[value])"
		} else {
			values += empty + ")"
		}
		invalid := "descendant_present != 0 OR (field_exists != 0 AND multiIf(" +
			typeSQL + " = 'Array(String)', " + stringInvalid + ", " +
			typeSQL + " = 'Array(Dynamic)', " + dynamicInvalid + ", " +
			"startsWith(" + typeSQL + ", 'Array('), 1, "
		if allowScalar {
			invalid += "NOT (" + scalarSupported + ")))"
		} else {
			// None represents the retained null/missing state and is not an
			// unsupported container for a list-only consumer.
			invalid += typeSQL + " != 'None'))"
		}
		present := "field_exists != 0"
		if !allowScalar {
			present += " AND " + typeSQL + " IN ('Array(String)', 'Array(Dynamic)')"
		}
		body := "tuple(" + values + ", toUInt8(field_exists != 0), toUInt8(" +
			present + "), toUInt8(" + invalid + "))"
		return bind(body, nil, nil, nil), nil
	case fieldKindString, fieldKindNumber, fieldKindBool:
		if !allowScalar {
			return nativeMVState{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
				Message: "multivalue function requires a multivalue input",
			}
		}
		eligible := "isNotNull(value)"
		extraParameters := []string(nil)
		extraValues := []string(nil)
		extraArgs := []any(nil)
		if input.kind == fieldKindString {
			eligible += " AND isValidUTF8(assumeNotNull(value))"
			if input.textEligibleSQL != "" {
				extraParameters = append(extraParameters, "text_eligible")
				extraValues = append(extraValues, "toUInt8(ifNull("+input.textEligibleSQL+", 0))")
				extraArgs = append(extraArgs, input.semanticBytesArgs...)
				eligible += " AND text_eligible != 0"
			}
		} else if input.kind == fieldKindNumber {
			eligible += " AND isFinite(toFloat64(value))"
		}
		// A present SQL NULL is an explicit native null member.  Only the
		// independent field-presence proof may skip a scalar argument.
		values := "if(field_exists != 0, [CAST(value AS Dynamic)], " + empty + ")"
		invalid := "descendant_present != 0 OR (field_exists != 0 AND isNotNull(value) AND NOT (" + eligible + "))"
		body := "tuple(" + values + ", toUInt8(field_exists != 0), " +
			"toUInt8(field_exists != 0), toUInt8(" + invalid + "))"
		return bind(body, extraParameters, extraValues, extraArgs), nil
	case fieldKindInvalid:
		explicitNull := input.alwaysNull ||
			(input.literal != nil && input.literal.Kind == plan.ValueKindNull)
		if explicitNull {
			values := empty
			present := "toUInt8(0)"
			if allowScalar {
				values = "if(field_exists != 0, [CAST(NULL AS Dynamic)], " + empty + ")"
				present = "toUInt8(field_exists != 0)"
			}
			body := "tuple(" + values + ", toUInt8(field_exists != 0), " + present +
				", toUInt8(descendant_present != 0))"
			return nativeMVState{
				sql: bindSQLExpressions(
					[]string{"field_exists", "descendant_present"},
					[]string{"toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"},
					body,
				),
				args: append(append([]any(nil), existsArgs...), input.descendantArgs...),
			}, nil
		}
		return nativeMVState{sql: "tuple(" + empty + ", toUInt8(0), toUInt8(0), toUInt8(0))"}, nil
	default:
		return nativeMVState{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MULTIVALUE_VALUE_TYPE",
			Message: "multivalue functions accept only String, finite Number/Decimal, Boolean, null, and native multivalue values",
		}
	}
}

type canonicalScalarState struct {
	sql  string // tuple(String value, UInt8 exists, UInt8 present, UInt8 invalid)
	args []any
}

func compileCanonicalScalarState(input compiledScalar) (canonicalScalarState, error) {
	existsSQL, existsArgs := scalarExistsSQL(input)
	descendantSQL := input.descendantSQL
	if descendantSQL == "" {
		descendantSQL = "0"
	}
	bind := func(body string, extraParameters, extraValues []string, extraArgs []any) canonicalScalarState {
		parameters := []string{"value", "field_present", "descendant_present"}
		values := []string{input.valueSQL, "toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"}
		parameters = append(parameters, extraParameters...)
		values = append(values, extraValues...)
		args := make([]any, 0, len(input.valueArgs)+len(existsArgs)+len(input.descendantArgs)+len(extraArgs))
		args = append(args, input.valueArgs...)
		args = append(args, existsArgs...)
		args = append(args, input.descendantArgs...)
		args = append(args, extraArgs...)
		return canonicalScalarState{
			sql:  bindSQLExpressions(parameters, values, body),
			args: args,
		}
	}
	nullState := "tuple(CAST('' AS String), toUInt8(field_present != 0), " +
		"toUInt8(0), toUInt8(descendant_present != 0))"
	if input.alwaysNull || input.kind == fieldKindInvalid {
		return bind(nullState, nil, nil, nil), nil
	}
	switch input.kind {
	case fieldKindString:
		eligible := "isValidUTF8(assumeNotNull(value))"
		extraParameters := []string(nil)
		extraValues := []string(nil)
		extraArgs := []any(nil)
		if input.textEligibleSQL != "" {
			extraParameters = append(extraParameters, "text_eligible")
			extraValues = append(extraValues, "toUInt8(ifNull("+input.textEligibleSQL+", 0))")
			extraArgs = append(extraArgs, input.semanticBytesArgs...)
			eligible += " AND text_eligible != 0"
		}
		return bind("tuple(if(field_present != 0 AND isNotNull(value), toString(value), CAST('' AS String)), "+
			"toUInt8(field_present != 0), toUInt8(field_present != 0 AND isNotNull(value)), toUInt8(descendant_present != 0 OR "+
			"(field_present != 0 AND isNotNull(value) AND NOT ("+eligible+"))))",
			extraParameters, extraValues, extraArgs), nil
	case fieldKindNumber:
		return bind("tuple(if(field_present != 0 AND isNotNull(value), toString(value), CAST('' AS String)), "+
			"toUInt8(field_present != 0), toUInt8(field_present != 0 AND isNotNull(value)), toUInt8(descendant_present != 0 OR "+
			"(field_present != 0 AND isNotNull(value) AND NOT isFinite(toFloat64(value)))))",
			nil, nil, nil), nil
	case fieldKindBool:
		return bind("tuple(if(field_present != 0 AND isNotNull(value), if(value, CAST('true' AS String), CAST('false' AS String)), CAST('' AS String)), "+
			"toUInt8(field_present != 0), toUInt8(field_present != 0 AND isNotNull(value)), toUInt8(descendant_present != 0))",
			nil, nil, nil), nil
	case fieldKindDynamic:
		typeSQL := "dynamicType(value)"
		supported := nativeMVElementSupportedSQL("value")
		text := nativeMVCanonicalTextSQL("value")
		present := "field_present != 0 AND " + typeSQL + " != 'None'"
		invalid := "descendant_present != 0 OR (" + present + " AND NOT (" + supported + "))"
		return bind("tuple(if("+present+" AND ("+supported+"), "+text+
			", CAST('' AS String)), toUInt8(field_present != 0), toUInt8("+present+"), toUInt8("+invalid+"))",
			nil, nil, nil), nil
	default:
		return canonicalScalarState{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MULTIVALUE_VALUE_TYPE",
			Message: "text consumer requires String, finite Number/Decimal, Boolean, or null",
		}
	}
}

func compileSplitScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse split: expected two arguments")
	}
	delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok || !utf8.ValidString(delimiter) || len(delimiter) > spl.MaximumMVDelimiterBytes {
		return compiledScalar{}, errors.New("compile ClickHouse split: delimiter must be a bounded quoted string literal")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	canonical, err := compileCanonicalScalarState(input)
	if err != nil {
		return compiledScalar{}, err
	}
	stateAlias := "__os_split_scalar_state"
	values := ""
	parameters := []string{stateAlias}
	bindings := []string{canonical.sql}
	args := append([]any(nil), canonical.args...)
	if delimiter == "" {
		values = "arrayMap(position -> substringUTF8(tupleElement(" + stateAlias +
			", 1), toInt64(position), 1), range(toUInt64(1), lengthUTF8(tupleElement(" +
			stateAlias + ", 1)) + toUInt64(1)))"
	} else {
		parameters = append(parameters, "delimiter")
		bindings = append(bindings, "CAST(? AS String)")
		args = append(args, delimiter)
		values = "splitByString(delimiter, tupleElement(" + stateAlias + ", 1))"
	}
	present := "tupleElement(" + stateAlias + ", 3) != 0"
	memberCount := "toUInt64(0)"
	payloadBytes := "toUInt128(0)"
	if delimiter == "" {
		memberCount = "if(" + present + ", toUInt64(lengthUTF8(tupleElement(" +
			stateAlias + ", 1))), toUInt64(0))"
		payloadBytes = "if(" + present + ", toUInt128(length(tupleElement(" +
			stateAlias + ", 1))), toUInt128(0))"
	} else {
		separatorCount := "countSubstrings(tupleElement(" + stateAlias + ", 1), delimiter)"
		memberCount = "if(" + present + ", toUInt64(" + separatorCount + ") + 1, toUInt64(0))"
		payloadBytes = "if(" + present + ", toUInt128(length(tupleElement(" + stateAlias +
			", 1))) - toUInt128(" + separatorCount + ") * toUInt128(length(delimiter)), toUInt128(0))"
	}
	values = "if(" + present + ", " + values + ", CAST([], 'Array(String)'))"
	body := nativeMVPreflightSQL(
		values,
		"tupleElement("+stateAlias+", 4) != 0",
		memberCount,
		payloadBytes,
		"CAST([], 'Array(String)')",
	)
	valueSQL := bindSQLExpressions(parameters, bindings, body)
	markNativeMVRuntimeValidation(state)
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    args,
		existsSQL:                    "tupleElement(" + canonical.sql + ", 2) != 0",
		existsArgs:                   append([]any(nil), canonical.args...),
		optionalMultivaluePresentSQL: "tupleElement(" + canonical.sql + ", 3) != 0",
		kind:                         fieldKindStringArray,
		maxStringBytes:               input.maxStringBytes,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    true,
	}, nil
}

func compileMVAppendScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) == 0 || len(expression.Arguments) > spl.MaximumMVAppendArguments {
		return compiledScalar{}, errors.New("compile ClickHouse mvappend: expected one through 32 arguments")
	}
	parameters := make([]string, 0, len(expression.Arguments))
	bindings := make([]string, 0, len(expression.Arguments))
	args := make([]any, 0)
	values := make([]string, 0, len(expression.Arguments))
	invalid := make([]string, 0, len(expression.Arguments))
	memberCounts := make([]string, 0, len(expression.Arguments))
	materialize := false
	for index, argument := range expression.Arguments {
		input, err := compileScalarValue(argument, state)
		if err != nil {
			return compiledScalar{}, err
		}
		normalized, err := compileNativeMVState(input, true)
		if err != nil {
			return compiledScalar{}, err
		}
		name := fmt.Sprintf("__os_mvappend_state_%d", index)
		parameters = append(parameters, name)
		bindings = append(bindings, normalized.sql)
		args = append(args, normalized.args...)
		members := "tupleElement(" + name + ", 1)"
		values = append(values, members)
		invalid = append(invalid, "tupleElement("+name+", 4) != 0")
		memberCounts = append(memberCounts, "toUInt128(length("+members+"))")
		materialize = materialize || input.materializeForPredicate
	}
	combined := "arrayConcat(" + strings.Join(values, ", ") + ")"
	// Sum lengths independently so the member guard does not first materialize
	// [values...] as an Array(Array(Dynamic)). UInt128 also avoids wrapping the
	// total. The nested array used to share the large canonical-text payload
	// expression is evaluated only in the payload branch, after that guard;
	// arrayConcat remains in the final branch after both limit guards.
	memberCount := strings.Join(memberCounts, " + ")
	inputArrays := "[" + strings.Join(values, ", ") + "]"
	payload := "arrayFold((bytes, members) -> bytes + arrayFold((member_bytes, member) -> " +
		"member_bytes + toUInt128(length(" + nativeMVCanonicalTextSQL("member") + ")), " +
		"members, toUInt128(0)), " + inputArrays + ", toUInt128(0))"
	body := nativeMVPreflightSQL(
		combined,
		"("+strings.Join(invalid, " OR ")+")",
		memberCount,
		payload,
		emptyNativeMVSQL(),
	)
	markNativeMVRuntimeValidation(state)
	return compiledScalar{
		valueSQL:                     bindSQLExpressions(parameters, bindings, body),
		valueArgs:                    args,
		existsSQL:                    "1",
		optionalMultivaluePresentSQL: "1",
		kind:                         fieldKindDynamicArray,
		maxStringBytes:               spl.MaximumNativeMVPayloadBytes,
		materializeForPredicate:      materialize,
		requiresRuntimeValidation:    true,
	}, nil
}

func compileMVDedupScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	input, normalized, err := compileUnaryNativeMV(expression, state, "mvdedup")
	if err != nil {
		return compiledScalar{}, err
	}
	stateAlias := "__os_mvdedup_state"
	valuesAlias := "__os_mvdedup_values"
	canonicalAlias := "__os_mvdedup_canonical"
	keysAlias := "__os_mvdedup_keys"
	values := "tupleElement(" + stateAlias + ", 1)"
	canonical := "arrayMap(member -> " + nativeMVCanonicalTextSQL("member") +
		", " + valuesAlias + ")"
	keys := "arrayMap((member, canonical) -> concat(" +
		nativeMVDedupTypeSQL("member") + ", char(0), " +
		nativeMVDedupKeyTextSQL("member", "canonical") + "), " + valuesAlias +
		", " + canonicalAlias + ")"
	deduped := "arrayFilter((member, occurrence) -> occurrence = toUInt32(1), " +
		valuesAlias + ", arrayEnumerateUniq(" + keysAlias + "))"
	inner := bindSQLExpressions(
		[]string{canonicalAlias},
		[]string{canonical},
		nativeMVPreflightSQL(
			bindSQLExpressions([]string{keysAlias}, []string{keys}, deduped),
			"0",
			"toUInt64(0)",
			"arrayFold((bytes, canonical) -> bytes + toUInt128(length(canonical)), "+
				canonicalAlias+", toUInt128(0))",
			emptyNativeMVSQL(),
		),
	)
	body := bindSQLExpressions(
		[]string{valuesAlias},
		[]string{values},
		nativeMVPreflightSQL(
			inner,
			"tupleElement("+stateAlias+", 4) != 0",
			"length("+valuesAlias+")",
			"toUInt128(0)",
			emptyNativeMVSQL(),
		),
	)
	valueSQL := bindSQLExpressions([]string{stateAlias}, []string{normalized.sql}, body)
	exists, present, stateArgs := nativeMVPreservedStateSQL(input, normalized)
	markNativeMVRuntimeValidation(state)
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    append([]any(nil), normalized.args...),
		existsSQL:                    exists,
		existsArgs:                   stateArgs,
		optionalMultivaluePresentSQL: present,
		kind:                         fieldKindDynamicArray,
		maxStringBytes:               spl.MaximumNativeMVPayloadBytes,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    true,
	}, nil
}

func compileUnaryNativeMV(expression *plan.ScalarCallExpression, state compileState, name string) (compiledScalar, nativeMVState, error) {
	if expression == nil || len(expression.Arguments) != 1 {
		return compiledScalar{}, nativeMVState{}, fmt.Errorf("compile ClickHouse %s: expected one argument", name)
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, nativeMVState{}, err
	}
	normalized, err := compileNativeMVState(input, false)
	return input, normalized, err
}

func signedMVIndexLiteral(expression plan.ScalarExpression) (int64, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal == nil {
		return 0, false
	}
	switch literal.Value.Kind {
	case plan.ValueKindInt64:
		if literal.Value.Int64 < -1<<31 || literal.Value.Int64 > 1<<31-1 {
			return 0, false
		}
		return literal.Value.Int64, true
	case plan.ValueKindUint64:
		if literal.Value.Uint64 > 1<<31-1 {
			return 0, false
		}
		return int64(literal.Value.Uint64), true
	default:
		return 0, false
	}
}

func compileMVIndexScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
		return compiledScalar{}, errors.New("compile ClickHouse mvindex: expected two or three arguments")
	}
	start, ok := signedMVIndexLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse mvindex: start must be a signed 32-bit integer literal")
	}
	end := int64(0)
	if len(expression.Arguments) == 3 {
		end, ok = signedMVIndexLiteral(expression.Arguments[2])
		if !ok {
			return compiledScalar{}, errors.New("compile ClickHouse mvindex: end must be a signed 32-bit integer literal")
		}
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	normalized, err := compileNativeMVState(input, false)
	if err != nil {
		return compiledScalar{}, err
	}
	stateAlias := "__os_mvindex_state"
	values := "tupleElement(" + stateAlias + ", 1)"
	length := "toInt64(length(" + values + "))"
	position := func(index int64) string {
		literal := strconv.FormatInt(index, 10)
		if index >= 0 {
			return "toInt64(" + literal + ") + 1"
		}
		return length + " + toInt64(" + literal + ") + 1"
	}
	startPosition := position(start)
	exists := "tupleElement(" + stateAlias + ", 2) != 0"
	present := "tupleElement(" + stateAlias + ", 3) != 0"
	validStart := startPosition + " >= 1 AND " + startPosition + " <= " + length
	markNativeMVRuntimeValidation(state)
	if len(expression.Arguments) == 2 {
		body := "if(" + present + " AND " + validStart + ", arrayElement(" + values +
			", " + startPosition + "), CAST(NULL AS Dynamic))"
		body = nativeMVPreflightSQL(
			body,
			"tupleElement("+stateAlias+", 4) != 0",
			"length("+values+")",
			nativeMVArrayPayloadBytesSQL(values),
			"CAST(NULL AS Dynamic)",
		)
		existsSQL, existsArgs := scalarExistsSQL(input)
		return compiledScalar{
			valueSQL:                  bindSQLExpressions([]string{stateAlias}, []string{normalized.sql}, body),
			valueArgs:                 append([]any(nil), normalized.args...),
			existsSQL:                 existsSQL,
			existsArgs:                existsArgs,
			kind:                      fieldKindDynamic,
			maxStringBytes:            input.maxStringBytes,
			materializeForPredicate:   input.materializeForPredicate,
			requiresRuntimeValidation: true,
		}, nil
	}
	endPosition := position(end)
	valid := present + " AND " + validStart + " AND " + endPosition + " >= 1 AND " +
		endPosition + " <= " + length + " AND " + startPosition + " <= " + endPosition
	body := "if(" + valid + ", arraySlice(" + values + ", " + startPosition + ", " +
		endPosition + " - " + startPosition + " + 1), " + emptyNativeMVSQL() + ")"
	body = nativeMVPreflightSQL(
		body,
		"tupleElement("+stateAlias+", 4) != 0",
		"length("+values+")",
		nativeMVArrayPayloadBytesSQL(values),
		emptyNativeMVSQL(),
	)
	valueSQL := bindSQLExpressions([]string{stateAlias}, []string{normalized.sql}, body)
	presenceSQL := bindSQLExpressions([]string{stateAlias}, []string{normalized.sql}, "toUInt8("+valid+")")
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    append([]any(nil), normalized.args...),
		existsSQL:                    bindSQLExpressions([]string{stateAlias}, []string{normalized.sql}, "toUInt8("+exists+")"),
		existsArgs:                   append([]any(nil), normalized.args...),
		optionalMultivaluePresentSQL: presenceSQL,
		kind:                         fieldKindDynamicArray,
		maxStringBytes:               spl.MaximumNativeMVPayloadBytes,
		materializeForPredicate:      input.materializeForPredicate,
		requiresRuntimeValidation:    true,
	}, nil
}

func compileMVJoinScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse mvjoin: expected two arguments")
	}
	delimiter, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok || !utf8.ValidString(delimiter) || len(delimiter) > spl.MaximumMVDelimiterBytes {
		return compiledScalar{}, errors.New("compile ClickHouse mvjoin: delimiter must be a bounded quoted string literal")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	normalized, err := compileNativeMVState(input, false)
	if err != nil {
		return compiledScalar{}, err
	}
	stateAlias := "__os_mvjoin_state"
	canonicalAlias := "__os_mvjoin_canonical"
	values := "tupleElement(" + stateAlias + ", 1)"
	canonical := "arrayMap(member -> " + nativeMVCanonicalTextSQL("member") +
		", " + values + ")"
	result := "if(tupleElement(" + stateAlias + ", 3) != 0, arrayStringConcat(" + canonicalAlias +
		", delimiter), CAST(NULL AS Nullable(String)))"
	inner := bindSQLExpressions(
		[]string{canonicalAlias},
		[]string{canonical},
		nativeMVPreflightSQL(
			result,
			"0",
			"toUInt64(0)",
			"arrayFold((bytes, canonical) -> bytes + toUInt128(length(canonical)), "+
				canonicalAlias+", toUInt128(0))",
			"CAST(NULL AS Nullable(String))",
		),
	)
	body := nativeMVPreflightSQL(
		inner,
		"tupleElement("+stateAlias+", 4) != 0",
		"length("+values+")",
		"toUInt128(0)",
		"CAST(NULL AS Nullable(String))",
	)
	valueSQL := bindSQLExpressions(
		[]string{stateAlias, "delimiter"},
		[]string{normalized.sql, "CAST(? AS String)"},
		body,
	)
	markNativeMVRuntimeValidation(state)
	existsSQL, existsArgs := scalarExistsSQL(input)
	return compiledScalar{
		valueSQL:                  valueSQL,
		valueArgs:                 append(append([]any(nil), normalized.args...), delimiter),
		existsSQL:                 existsSQL,
		existsArgs:                existsArgs,
		kind:                      fieldKindString,
		maxStringBytes:            maximumMVJoinOutputBytes(delimiter),
		materializeForPredicate:   input.materializeForPredicate,
		requiresRuntimeValidation: true,
	}, nil
}

func compileMVZipScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
		return compiledScalar{}, errors.New("compile ClickHouse mvzip: expected two or three arguments")
	}
	delimiter := ","
	if len(expression.Arguments) == 3 {
		var ok bool
		delimiter, ok = scalarQuotedStringLiteral(expression.Arguments[2])
		if !ok || !utf8.ValidString(delimiter) || len(delimiter) > spl.MaximumMVDelimiterBytes {
			return compiledScalar{}, errors.New("compile ClickHouse mvzip: delimiter must be a bounded quoted string literal")
		}
	}
	left, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	right, err := compileScalarValue(expression.Arguments[1], state)
	if err != nil {
		return compiledScalar{}, err
	}
	leftState, err := compileNativeMVState(left, false)
	if err != nil {
		return compiledScalar{}, err
	}
	rightState, err := compileNativeMVState(right, false)
	if err != nil {
		return compiledScalar{}, err
	}
	leftAlias, rightAlias := "__os_mvzip_left", "__os_mvzip_right"
	canonicalArraysAlias := "__os_mvzip_canonical_arrays"
	leftValues := "tupleElement(" + leftAlias + ", 1)"
	rightValues := "tupleElement(" + rightAlias + ", 1)"
	length := "least(length(" + leftValues + "), length(" + rightValues + "))"
	leftPrefix := "arraySlice(" + leftValues + ", 1, " + length + ")"
	rightPrefix := "arraySlice(" + rightValues + ", 1, " + length + ")"
	canonicalArrays := "arrayMap(members -> arrayMap(member -> " +
		nativeMVCanonicalTextSQL("member") + ", members), [" + leftPrefix + ", " +
		rightPrefix + "])"
	leftTexts := "arrayElement(" + canonicalArraysAlias + ", 1)"
	rightTexts := "arrayElement(" + canonicalArraysAlias + ", 2)"
	values := "arrayMap((left_member, right_member) -> concat(" +
		"left_member, delimiter, right_member), " + leftTexts + ", " + rightTexts + ")"
	present := "tupleElement(" + leftAlias + ", 3) != 0 AND tupleElement(" + rightAlias + ", 3) != 0"
	memberCount := "if(" + present + ", toUInt64(" + length + "), toUInt64(0))"
	ordinalRange := "range(toUInt64(1), " + memberCount + " + toUInt64(1))"
	payloadBytes := "arrayFold((bytes, ordinal) -> bytes + toUInt128(length(" +
		"arrayElement(" + leftTexts + ", ordinal))) + " +
		"toUInt128(length(delimiter)) + toUInt128(length(" +
		"arrayElement(" + rightTexts + ", ordinal))), " + ordinalRange +
		", toUInt128(0))"
	outputBody := nativeMVPreflightSQL(
		"if("+present+", "+values+", CAST([], 'Array(String)'))",
		"0",
		memberCount,
		payloadBytes,
		"CAST([], 'Array(String)')",
	)
	outputBody = bindSQLExpressions(
		[]string{canonicalArraysAlias},
		[]string{canonicalArrays},
		outputBody,
	)
	inputArrays := "[" + leftValues + ", " + rightValues + "]"
	inputPayloadMaximum := "arrayMax(arrayMap(members -> arrayFold((bytes, member) -> " +
		"bytes + toUInt128(length(" + nativeMVCanonicalTextSQL("member") + ")), " +
		"members, toUInt128(0)), " + inputArrays + "))"
	body := nativeMVPreflightSQL(
		outputBody,
		"tupleElement("+leftAlias+", 4) != 0 OR tupleElement("+rightAlias+", 4) != 0",
		"greatest(length("+leftValues+"), length("+rightValues+"))",
		inputPayloadMaximum,
		"CAST([], 'Array(String)')",
	)
	valueSQL := bindSQLExpressions(
		[]string{leftAlias, rightAlias, "delimiter"},
		[]string{leftState.sql, rightState.sql, "CAST(? AS String)"},
		body,
	)
	presenceSQL := bindSQLExpressions(
		[]string{leftAlias, rightAlias},
		[]string{leftState.sql, rightState.sql},
		"toUInt8("+present+")",
	)
	existsSQL := bindSQLExpressions(
		[]string{leftAlias, rightAlias},
		[]string{leftState.sql, rightState.sql},
		"toUInt8(tupleElement("+leftAlias+", 2) != 0 AND tupleElement("+
			rightAlias+", 2) != 0)",
	)
	markNativeMVRuntimeValidation(state)
	valueArgs := append(append(append([]any(nil), leftState.args...), rightState.args...), delimiter)
	presenceArgs := append(append([]any(nil), leftState.args...), rightState.args...)
	return compiledScalar{
		valueSQL:                     valueSQL,
		valueArgs:                    valueArgs,
		existsSQL:                    existsSQL,
		existsArgs:                   presenceArgs,
		optionalMultivaluePresentSQL: presenceSQL,
		kind:                         fieldKindStringArray,
		maxStringBytes:               spl.MaximumNativeMVPayloadBytes,
		materializeForPredicate:      left.materializeForPredicate || right.materializeForPredicate,
		requiresRuntimeValidation:    true,
	}, nil
}

func compileMVFindScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if expression == nil || len(expression.Arguments) != 2 {
		return compiledScalar{}, errors.New("compile ClickHouse mvfind: expected two arguments")
	}
	pattern, ok := scalarQuotedStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse mvfind: pattern must be a quoted string literal")
	}
	compiledPattern, err := compileMatchPatternForBackend(pattern, expression.Range)
	if err != nil {
		return compiledScalar{}, err
	}
	if state.context != nil {
		if compiledPattern.ProgramWorkUnits > splregex.MaximumMatchQueryProgramWorkUnits-state.context.patternBudgets.match.programWorkUnits {
			return compiledScalar{}, &plan.Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("search match programs require more than %d work units", splregex.MaximumMatchQueryProgramWorkUnits),
				Range:   expression.Range,
			}
		}
		state.context.patternBudgets.match.programWorkUnits += compiledPattern.ProgramWorkUnits
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	normalized, err := compileNativeMVState(input, false)
	if err != nil {
		return compiledScalar{}, err
	}
	stateAlias := "__os_mvfind_state"
	canonicalAlias := "__os_mvfind_canonical"
	values := "tupleElement(" + stateAlias + ", 1)"
	canonical := "arrayMap(member -> " + nativeMVCanonicalTextSQL("member") +
		", " + values + ")"
	ordinal := "arrayFirstIndex(canonical -> match(canonical, pattern), " +
		canonicalAlias + ")"
	result := "if(tupleElement(" + stateAlias + ", 3) != 0 AND " + ordinal +
		" != 0, toInt64(" + ordinal + ") - 1, CAST(NULL AS Nullable(Int64)))"
	inner := bindSQLExpressions(
		[]string{canonicalAlias},
		[]string{canonical},
		nativeMVPreflightSQL(
			result,
			"0",
			"toUInt64(0)",
			"arrayFold((bytes, canonical) -> bytes + toUInt128(length(canonical)), "+
				canonicalAlias+", toUInt128(0))",
			"CAST(NULL AS Nullable(Int64))",
		),
	)
	body := nativeMVPreflightSQL(
		inner,
		"tupleElement("+stateAlias+", 4) != 0",
		"length("+values+")",
		"toUInt128(0)",
		"CAST(NULL AS Nullable(Int64))",
	)
	valueSQL := bindSQLExpressions(
		[]string{stateAlias, "pattern"},
		[]string{normalized.sql, "CAST(? AS String)"},
		body,
	)
	markNativeMVRuntimeValidation(state)
	existsSQL, existsArgs := scalarExistsSQL(input)
	return compiledScalar{
		valueSQL:                  valueSQL,
		valueArgs:                 append(append([]any(nil), normalized.args...), compiledPattern.Pattern),
		existsSQL:                 existsSQL,
		existsArgs:                existsArgs,
		kind:                      fieldKindNumber,
		numberType:                "Int64",
		numericIntegral:           true,
		maxStringBytes:            11,
		materializeForPredicate:   input.materializeForPredicate,
		requiresRuntimeValidation: true,
	}, nil
}
