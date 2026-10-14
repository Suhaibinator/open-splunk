package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// MaximumConvertInputBytes bounds each textual recognizer before parsing.
// An oversized value is ineligible, so preservation never truncates it.
const MaximumConvertInputBytes = 4 << 10

func compileConvert(
	relation compiledRelation,
	inputArgs []any,
	operator *plan.Convert,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || len(operator.Assignments) > spl.MaximumConvertWrites {
		return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse convert: assignments are invalid")
	}
	next := cloneCompileState(state)
	result := relation
	args := append([]any(nil), inputArgs...)
	for index, assignment := range operator.Assignments {
		if !assignment.Function.Valid() || assignment.Function == spl.ConvertFunctionNone {
			return compiledRelation{}, compileState{}, nil, errors.New("compile ClickHouse convert: function is invalid")
		}
		if err := validateCommandField("convert", assignment.Input); err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		if err := validateCommandField("convert", assignment.Output); err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		field, present, err := resolveCompiledField(assignment.Input, next)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d_convert_%d", stage, index))
		// Qualify a public physical source before publishing an identically named
		// destination. ClickHouse otherwise substitutes the new SELECT alias into
		// the source tuple and feeds the assignment back into itself.
		boundField := field
		if isCanonicalQuotedIdentifierSQL(field.valueSQL) {
			boundField.valueSQL = alias + "." + field.valueSQL
			boundField.existsSQL = strings.ReplaceAll(field.existsSQL, field.valueSQL, boundField.valueSQL)
			boundField.optionalMultivaluePresentSQL = strings.ReplaceAll(field.optionalMultivaluePresentSQL, field.valueSQL, boundField.valueSQL)
		}
		source, err := compileKnowledgeFieldSourceFromField(boundField, present)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		sourceAlias := quoteIdentifier(fmt.Sprintf("__os_convert_source_%d_%d", stage, index))
		candidateAlias := quoteIdentifier(fmt.Sprintf("__os_convert_candidate_%d_%d", stage, index))
		candidate, err := compileConvertCandidate(operator, assignment, boundField, source, sourceAlias, next)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		bindings := ""
		from := "(" + result.sql + ") AS " + alias +
			" ARRAY JOIN [" + source.sql + "] AS " + sourceAlias +
			" ARRAY JOIN [" + candidate.valueSQL + "] AS " + candidateAlias
		projectionBase := "* EXCEPT (" + sourceAlias + ", " + candidateAlias + ")"
		valid := "dynamicType(" + candidateAlias + ") != 'None'"
		neutral := "(" + source.producedSQL(sourceAlias) + " = 0 OR " +
			source.storedTypeSQL(sourceAlias) + " = " + storedValueTypeSQL(eventfields.StoredValueTypeNull) + ")"
		if assignment.Function == spl.ConvertFunctionAuto {
			// Materialize the complete incoming relation before eligibility and the
			// complete output afterward. Later WHERE/LIMIT/project stages cannot
			// change whether an earlier row made this field ineligible.
			inputName := quoteIdentifier(fmt.Sprintf("__os_convert_auto_input_%d_%d", stage, index))
			frozenSource := quoteIdentifier(fmt.Sprintf("__os_convert_frozen_%d_%d", stage, index))
			frozenCandidate := quoteIdentifier(fmt.Sprintf("__os_convert_recognized_%d_%d", stage, index))
			inputSQL := "SELECT " + projectionBase + ", " + sourceAlias + " AS " + frozenSource +
				", " + candidateAlias + " AS " + frozenCandidate + " FROM " + from
			sourceAlias, candidateAlias = frozenSource, frozenCandidate
			valid = "dynamicType(" + candidateAlias + ") != 'None'"
			neutral = "(" + source.producedSQL(sourceAlias) + " = 0 OR " + source.storedTypeSQL(sourceAlias) + " = " + storedValueTypeSQL(eventfields.StoredValueTypeNull) + ")"
			bindings = "WITH " + inputName + " AS MATERIALIZED (" + inputSQL + ") "
			from = inputName + " AS " + alias
			projectionBase = "* EXCEPT (" + sourceAlias + ", " + candidateAlias + ")"
			valid = "(" + valid + " AND min(toUInt8(" + neutral + " OR " + valid + ")) OVER () = 1)"
			next.context.requiresMaterializedValidationSettings = true
			result.depth++
		}
		valueSQL := "if(" + valid + ", " + candidateAlias + ", " + source.valueSQL(sourceAlias) + ")"
		presenceSQL := source.producedSQL(sourceAlias)
		preserved := "NOT (" + valid + ")"
		if assignment.Function == spl.ConvertFunctionNum {
			presenceSQL = "toUInt8(" + presenceSQL + " != 0 AND (" + valid + " OR " + neutral + "))"
			valueSQL = "if(" + valid + ", " + candidateAlias + ", CAST(NULL AS Dynamic))"
			preserved = neutral
		}
		candidateType := convertCandidateStoredTypeSQL(candidateAlias)
		if assignment.Function == spl.ConvertFunctionNum || assignment.Function == spl.ConvertFunctionAuto {
			originalType := source.storedTypeSQL(sourceAlias)
			candidateType = "if(" + originalType + " IN (toUInt8(3), toUInt8(4), toUInt8(5), toUInt8(12)), " + originalType + ", " + candidateType + ")"
		}
		typeSQL := "toUInt8(if(" + valid + ", " + candidateType +
			", if(" + preserved + ", " + source.storedTypeSQL(sourceAlias) + ", 0)))"
		namesSQL := "if(" + preserved + ", " + source.namesSQL(sourceAlias) + ", " + knowledgeEmptyRelativeFieldNamesSQL() + ")"
		typesSQL := "if(" + preserved + ", " + source.typesSQL(sourceAlias) + ", " + knowledgeEmptyRelativeFieldTypesSQL() + ")"
		metadataSQL := "toUInt8(if(" + preserved + ", " + source.metadataVersionSQL(sourceAlias) + ", 0))"
		private := func(role string) string {
			return quoteIdentifier(fmt.Sprintf("__os_convert_%s_%d_%d", role, stage, index))
		}
		existsAlias, typeAlias := private("exists"), private("type")
		namesAlias, typesAlias, metadataAlias := private("names"), private("types"), private("metadata")
		projection := upsertWildcardFieldProjection(projectionBase, next, assignment.Output.Name, valueSQL, alias,
			authoredFieldPhysicallyPublic(next, assignment.Output.Name))
		projection += ", " + presenceSQL + " AS " + existsAlias + ", " + typeSQL + " AS " + typeAlias +
			", " + namesSQL + " AS " + namesAlias + ", " + typesSQL + " AS " + typesAlias +
			", " + metadataSQL + " AS " + metadataAlias
		result = result.selectFrom(bindings+"SELECT "+projection+" FROM "+from, assignment.Range)
		if assignment.Function == spl.ConvertFunctionAuto {
			outputName := private("auto_output")
			result = result.selectFrom("WITH "+outputName+" AS MATERIALIZED ("+result.sql+") SELECT * FROM "+outputName, assignment.Range)
		}
		args = append(args, source.args...)
		args = append(args, candidate.valueArgs...)
		value := compiledScalar{
			valueSQL: quoteIdentifier(assignment.Output.Name), kind: fieldKindDynamic,
			existsSQL: existsAlias, storedTypeSQL: typeAlias,
			descendantSQL:         "notEmpty(" + namesAlias + ")",
			relativeFieldNamesSQL: namesAlias, relativeFieldTypesSQL: typesAlias,
			fieldMetadataVersionSQL: metadataAlias,
			maxStringBytes:          max(field.maxStringBytes, candidate.maxStringBytes),
			materializeForPredicate: true,
		}
		retainSparse := exposesRawFieldsPayload(next)
		next, err = extendCompileState(next, assignment.Output, value, true)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		if retainSparse && !slices.Contains(next.publicOrder, "fields") {
			next.publicOrder = append(next.publicOrder, "fields")
			next.sparseFieldsSubset = true
		}
		converted := next.visible[assignment.Output.Name]
		converted.convertedContainer = true
		next.visible[assignment.Output.Name] = converted
		next.privateColumns = append(next.privateColumns, existsAlias, typeAlias, namesAlias, typesAlias, metadataAlias)
		if err := validateRelationalDepth(result.depth, assignment.Range); err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
	}
	return result, next, args, nil
}

func convertCandidateStoredTypeSQL(value string) string {
	typeSQL := "dynamicType(" + value + ")"
	return "multiIf(" + typeSQL + " = 'String', " + storedValueTypeSQL(eventfields.StoredValueTypeString) +
		", startsWith(" + typeSQL + ", 'UInt'), " + storedValueTypeSQL(eventfields.StoredValueTypeUint64) +
		", startsWith(" + typeSQL + ", 'Int'), " + storedValueTypeSQL(eventfields.StoredValueTypeSint64) +
		", startsWith(" + typeSQL + ", 'Decimal') OR " + typeSQL + " = 'Map(String, String)', " + storedValueTypeSQL(eventfields.StoredValueTypeDecimal) + ", " + storedValueTypeSQL(eventfields.StoredValueTypeDouble) + ")"
}

func compileConvertCandidate(
	operator *plan.Convert,
	assignment plan.ConvertAssignment,
	field fieldState,
	source compiledKnowledgeFieldSource,
	sourceAlias string,
	state compileState,
) (compiledScalar, error) {
	valueSQL := source.valueSQL(sourceAlias)
	typeSQL := source.storedTypeSQL(sourceAlias)
	textSQL := "dynamicElement(" + valueSQL + ", 'String')"
	textSQL = "if(" + typeSQL + " = " + storedValueTypeSQL(eventfields.StoredValueTypeString) +
		" AND length(" + textSQL + ") <= " + strconv.Itoa(MaximumConvertInputBytes) +
		", trimBoth(" + textSQL + ", char(32, 9, 10, 13, 11, 12)), CAST('' AS String))"
	if assignment.Function == spl.ConvertFunctionCTime || assignment.Function == spl.ConvertFunctionMktime {
		input := fieldState{kind: fieldKindDynamic, valueSQL: valueSQL, existsSQL: "1", storedTypeSQL: typeSQL}
		function := plan.ScalarFunctionStrptime
		if assignment.Function == spl.ConvertFunctionCTime {
			function = plan.ScalarFunctionStrftime
			if field.kind == fieldKindTime || field.kind == fieldKindNumber {
				input = field
			} else {
				numeric, numericArgs := compileConvertNumericCandidate(valueSQL, typeSQL, textSQL, false)
				// Chronological aggregations publish native DateTime64 inside
				// Dynamic. Keep its nanoseconds instead of treating the timestamp
				// as an ineligible numeric value (or routing it through Float64).
				timestamp, err := unixTimestampScalarSQL(compiledScalar{kind: fieldKindDynamic}, "convert ctime")
				if err != nil {
					return compiledScalar{}, err
				}
				timestamp = bindSQLExpressions([]string{"value"}, []string{numeric}, timestamp)
				const epochText = "__os_convert_epoch_text"
				exactEpochPattern := `^[+-]?(?:[0-9]{1,10}(?:\.[0-9]{0,9})?|\.[0-9]{1,9})$`
				exactTextTimestamp := bindSQLExpressions([]string{epochText}, []string{canonicalNumericTextSQL(textSQL)},
					"fromUnixTimestamp64Nano(accurateCastOrNull(toDecimal128OrNull(if(match("+epochText+", "+quoteStringLiteral(exactEpochPattern)+"), "+epochText+", CAST('' AS String)), 9) * toDecimal128(1000000000, 0), 'Int64'), 'UTC')")
				timestamp = "coalesce(" + exactTextTimestamp + ", " + timestamp + ")"
				nativeType := "DateTime64(9, 'UTC')"
				nativeTimestamp := "accurateCastOrNull(" + valueSQL + ", " + quoteStringLiteral(nativeType) + ")"
				dynamic := compiledScalar{kind: fieldKindDynamic, valueSQL: valueSQL, dynamicTypeSQL: "dynamicType(" + valueSQL + ")"}
				payload := dynamicTaggedMapSQL(dynamic) + "[concat(char(0), 'open_splunk_value')]"
				payload = "if(length(" + payload + ") <= " + strconv.Itoa(MaximumConvertInputBytes) + ", " + payload + ", CAST('' AS String))"
				validity := newDynamicEnvelopePayloadValiditySQL(payload)
				typedTimestamp := "multiIf(" + typeSQL + " = " + storedValueTypeSQL(eventfields.StoredValueTypeTimestamp) +
					" AND startsWith(dynamicType(" + valueSQL + "), 'DateTime'), " + nativeTimestamp +
					", " + typeSQL + " = " + storedValueTypeSQL(eventfields.StoredValueTypeTimestamp) + " AND " + dynamicTaggedEnvelopeCondition(dynamic, "timestamp/v1") + " AND " + validity.timestampValid +
					", parseDateTime64BestEffortOrNull(" + payload + ", 9, 'UTC'), CAST(NULL AS Nullable(DateTime64(9, 'UTC'))))"
				input.kind = fieldKindTime
				input.numberType = nativeType
				input.valueSQL = bindSQLExpressions([]string{"__os_convert_native_timestamp"}, []string{typedTimestamp}, "if(isNotNull(__os_convert_native_timestamp), __os_convert_native_timestamp, "+timestamp+")")
				if err := chargeUnixTimestampDynamicDecimalBudget(compiledScalar{kind: fieldKindDynamic, maxStringBytes: MaximumConvertInputBytes}, state.context, "convert ctime", assignment.Range); err != nil {
					return compiledScalar{}, err
				}
				// Preserve bind order through the scalar compiler.
				state = cloneCompileState(state)
				state.visible["__os_convert_input"] = input
				call := convertTimeCall(function, operator, assignment.Range)
				formatted, err := compileStrftimeScalar(call, state)
				if err != nil {
					return compiledScalar{}, err
				}
				formatted.valueArgs = append(formatted.valueArgs, numericArgs...)
				formatted.valueSQL = "CAST(" + formatted.valueSQL + " AS Dynamic)"
				return formatted, nil
			}
		} else {
			rawText := "dynamicElement(" + valueSQL + ", 'String')"
			boundedText := "if(" + typeSQL + " = " + storedValueTypeSQL(eventfields.StoredValueTypeString) + " AND length(" + rawText + ") <= " + strconv.Itoa(MaximumConvertInputBytes) + ", " + rawText + ", CAST('' AS String))"
			input = fieldState{kind: fieldKindString, valueSQL: boundedText, existsSQL: "1", maxStringBytes: MaximumConvertInputBytes}
		}
		state = cloneCompileState(state)
		state.visible["__os_convert_input"] = input
		call := convertTimeCall(function, operator, assignment.Range)
		var candidate compiledScalar
		var err error
		if function == plan.ScalarFunctionStrftime {
			candidate, err = compileStrftimeScalar(call, state)
		} else {
			candidate, err = compileStrptimeScalar(call, state)
		}
		candidate.valueSQL = "CAST(" + candidate.valueSQL + " AS Dynamic)"
		return candidate, err
	}
	var sql string
	var args []any
	switch assignment.Function {
	case spl.ConvertFunctionAuto, spl.ConvertFunctionNum:
		sql, args = compileConvertNumericCandidate(valueSQL, typeSQL, textSQL, true)
	case spl.ConvertFunctionDur2Sec:
		sql, args = convertDurationTextSQL(textSQL)
		sql = "CAST(" + sql + " AS Dynamic)"
	case spl.ConvertFunctionRmUnit:
		sql = "CAST(" + finiteFloatOrNullSQL("extract("+textSQL+", ?)") + " AS Dynamic)"
		args = []any{convertNumericPrefixPattern}
	default:
		return compiledScalar{}, errors.New("compile ClickHouse convert: unknown function")
	}
	return compiledScalar{valueSQL: sql, valueArgs: args, kind: fieldKindDynamic, maxStringBytes: 64}, nil
}

func convertTimeCall(function plan.ScalarFunction, operator *plan.Convert, sourceRange spl.Range) *plan.ScalarCallExpression {
	return &plan.ScalarCallExpression{Function: function, Range: sourceRange, Arguments: []plan.ScalarExpression{
		&plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "__os_convert_input", Path: []string{"__os_convert_input"}}, Range: sourceRange},
		&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: operator.TimeFormat, Quoted: true}, Range: operator.TimeFormatRange},
	}}
}

const (
	convertDecimalPattern       = `^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`
	convertCommaPattern         = `^[+-]?[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?$`
	convertDurationPattern      = `^(?:([0-9]+)\+)?([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9](?:\.[0-9]+)?)$`
	convertMSTimePattern        = `^([0-9]+):([0-5][0-9]\.[0-9]{3})$`
	convertMemoryPattern        = `^[+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)[kKmMgG]$`
	convertNumericPrefixPattern = `^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?`
)

func convertDurationTextSQL(text string) (string, []any) {
	const groups = "__os_convert_duration_parts"
	component := func(index int) string {
		return finiteFloatOrNullSQL("arrayElement(" + groups + ", " + strconv.Itoa(index) + ")")
	}
	seconds := "if(length(" + groups + ") = 4, ifNotFinite(ifNull(" + component(1) + ", 0) * 86400 + " + component(2) + " * 3600 + " + component(3) + " * 60 + " + component(4) + ", CAST(NULL AS Nullable(Float64))), CAST(NULL AS Nullable(Float64)))"
	return bindSQLExpressions([]string{groups}, []string{"extractGroups(" + text + ", ?)"}, seconds), []any{convertDurationPattern}
}

func convertExactDecimalTextSQL(text string) string {
	const canonical = "__os_convert_decimal_canonical"
	const digits = "__os_convert_decimal_digits"
	negative := "startsWith(" + canonical + ", '-')"
	integer := "match(" + canonical + ", '^([+]|-|)[0-9]+$')"
	signedLimit := "if(" + negative + ", '9223372036854775808', '9223372036854775807')"
	signedFits := "(length(" + digits + ") < 19 OR (length(" + digits + ") = 19 AND " + digits + " <= " + signedLimit + "))"
	unsignedFits := "(length(" + digits + ") < 20 OR (length(" + digits + ") = 20 AND " + digits + " <= '18446744073709551615'))"
	decimal := "multiIf(" + integer + " AND " + signedFits + ", CAST(toInt64OrNull(" + canonical + ") AS Dynamic), " + integer + " AND NOT " + negative + " AND " + unsignedFits + ", CAST(toUInt64OrNull(" + canonical + ") AS Dynamic), CAST(" + finiteFloatOrNullSQL(canonical) + " AS Dynamic))"
	decimal = bindSQLExpressions([]string{canonical}, []string{canonicalNumericTextSQL(text)},
		bindSQLExpressions([]string{digits}, []string{"if(startsWith(" + canonical + ", '+') OR " + negative + ", substring(" + canonical + ", 2), " + canonical + ")"}, decimal))
	return decimal
}

func compileConvertNumericCandidate(value, semanticType, text string, allFormats bool) (string, []any) {
	const bound = "__os_convert_numeric_text"
	decimal := convertExactDecimalTextSQL(bound)
	textBody := "multiIf(match(" + bound + ", ?), " + decimal
	args := []any{convertDecimalPattern}
	if allFormats {
		duration, durationArgs := convertDurationTextSQL(bound)
		const groups = "__os_convert_mstime_parts"
		mstime := bindSQLExpressions([]string{groups}, []string{"extractGroups(" + bound + ", ?)"},
			"if(length("+groups+") = 2, ifNotFinite("+finiteFloatOrNullSQL("arrayElement("+groups+", 1)")+" * 60 + "+finiteFloatOrNullSQL("arrayElement("+groups+", 2)")+", CAST(NULL AS Nullable(Float64))), CAST(NULL AS Nullable(Float64)))")
		memory := "ifNotFinite(" + finiteFloatOrNullSQL("substring("+bound+", 1, length("+bound+") - 1)") + " * multiIf(lowerUTF8(right(" + bound + ", 1)) = 'g', 1048576, lowerUTF8(right(" + bound + ", 1)) = 'm', 1024, 1), CAST(NULL AS Nullable(Float64)))"
		textBody += ", match(" + bound + ", ?), CAST(" + convertExactDecimalTextSQL("replaceAll("+bound+", ',', '')") + " AS Dynamic)" +
			", isNotNull(" + duration + "), CAST(" + duration + " AS Dynamic)" +
			", isNotNull(" + mstime + "), CAST(" + mstime + " AS Dynamic)" +
			", match(" + bound + ", ?) AND " + memory + " > 0, CAST(" + memory + " AS Dynamic)"
		args = append(args, convertCommaPattern)
		args = append(args, durationArgs...)
		args = append(args, durationArgs...)
		args = append(args, convertMSTimePattern, convertMSTimePattern, convertMemoryPattern)
	}
	textBody += ", CAST(NULL AS Dynamic))"
	textBody = bindSQLExpressions([]string{bound}, []string{text}, textBody)
	numericType := "(" + semanticType + " IN (" + storedValueTypeSQL(eventfields.StoredValueTypeSint64) + ", " +
		storedValueTypeSQL(eventfields.StoredValueTypeUint64) + ", " + storedValueTypeSQL(eventfields.StoredValueTypeDouble) + "))"
	dynamic := compiledScalar{valueSQL: value, kind: fieldKindDynamic, dynamicTypeSQL: "dynamicType(" + value + ")"}
	decimalTag, decimalPayload := dynamicTaggedDecimalTextWithLimit(dynamic, MaximumConvertInputBytes)
	decimalValid := "(" + semanticType + " = " + storedValueTypeSQL(eventfields.StoredValueTypeDecimal) +
		" AND (isNotNull(" + finiteDynamicFloatOrNullSQL(value) + ") OR (" + decimalTag +
		" AND isNotNull(" + finiteFloatOrNullSQL(canonicalNumericTextSQL(decimalPayload)) + "))))"
	return "multiIf((" + numericType + " AND isNotNull(" + finiteDynamicFloatOrNullSQL(value) + ")) OR " + decimalValid + ", " + value +
		", " + textBody + ")", args
}
