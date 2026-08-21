package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// materializedValidationSettingsSQL keeps data-dependent multivalue
// validation ahead of every later relational consumer. ClickHouse 26.3's
// query-plan optimizer otherwise repeatedly rewrites a downstream WHERE across
// ARRAY JOIN and the nested validation graph until its own 10,000-pass ceiling
// preempts the stable code-395 resource marker. The settings are appended
// once to the complete outer query: materialization remains enabled, while
// plan and predicate rewrites cannot move a consumer ahead of the atomic guard.
const materializedValidationSettingsSQL = " SETTINGS enable_materialized_cte = 1, query_plan_enable_optimizations = 0, enable_optimize_predicate_expression = 0, splitby_max_substrings_includes_remaining_string = 0, enable_named_columns_in_function_tuple = 1, output_format_json_named_tuples_as_objects = 1, output_format_json_skip_null_value_in_named_tuples = 0, output_format_json_map_as_array_of_tuples = 0, output_format_json_escape_forward_slashes = 0, output_format_json_quote_64bit_integers = 0, output_format_json_quote_64bit_floats = 0, output_format_json_quote_decimals = 0, output_format_json_quote_denormals = 0"

func applyMaterializedValidationSettings(sql string) string {
	if strings.HasSuffix(sql, materializedValidationSettingsSQL) {
		return sql
	}
	sql = strings.TrimSuffix(sql, materializedCTESettingsSQL)
	return sql + materializedValidationSettingsSQL
}

// Stable runtime markers are deliberately short, public-secret-free strings.
// queryexec maps them to the documented public error categories and never
// returns ClickHouse's surrounding exception text.
const (
	UnsupportedMakeMVValueMarker     = "open-splunk: makemv input value is unsupported"
	MakeMVRowMembersLimitMarker      = "open-splunk: makemv row members exceed the limit"
	MakeMVRowBytesLimitMarker        = "open-splunk: makemv row bytes exceed the limit"
	MakeMVResultMembersLimitMarker   = "open-splunk: makemv result members exceed the limit"
	MakeMVResultBytesLimitMarker     = "open-splunk: makemv result bytes exceed the limit"
	MakeMVRetainedBytesLimitMarker   = "open-splunk: makemv retained bytes exceed the limit"
	UnsupportedMVExpandValueMarker   = "open-splunk: mvexpand input value is unsupported"
	MVExpandRowMembersLimitMarker    = "open-splunk: mvexpand row members exceed the limit"
	MVExpandStageRowsLimitMarker     = "open-splunk: mvexpand stage rows exceed the limit"
	MVExpandQueryRowsLimitMarker     = "open-splunk: mvexpand query rows exceed the limit"
	MVExpandRetainedBytesLimitMarker = "open-splunk: mvexpand retained bytes exceed the limit"
)

func isCompilerPrivateField(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), "__os_")
}

func validateCommandField(operation string, field plan.FieldRef) error {
	if field.Name == "" || isCompilerPrivateField(field.Name) {
		return errors.New("compile ClickHouse " + operation + ": input field is invalid")
	}
	return validateCanonicalFieldRef(operation, "input", field)
}

func compileFillNull(
	relation compiledRelation,
	operator *plan.FillNull,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || len(operator.Fields) < 1 ||
		!utf8.ValidString(operator.Value) {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse fillnull: operator is invalid",
		)
	}
	if len(operator.Fields) > spl.MaximumExplicitProjectionFields {
		return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX", Message: fmt.Sprintf(
				"fillnull contains more than %d fields", spl.MaximumExplicitProjectionFields,
			), Range: operator.Range,
		}
	}
	next := cloneCompileState(state)
	result := relation
	args := make([]any, 0, len(operator.Fields))
	seen := make(map[string]struct{}, len(operator.Fields))
	for index, ref := range operator.Fields {
		if err := validateCommandField("fillnull", ref); err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		if _, duplicate := seen[ref.Name]; duplicate {
			return compiledRelation{}, compileState{}, nil, errors.New(
				"compile ClickHouse fillnull: input field is repeated",
			)
		}
		seen[ref.Name] = struct{}{}
		field, exists, err := resolveCompiledField(ref, next)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		var (
			directSource       compiledKnowledgeFieldSource
			directSourceAlias  string
			directNamesAlias   string
			directTypesAlias   string
			directVersionAlias string
			materializeDirect  bool
			textEligibleAlias  string
			storedTypeAlias    string
		)
		if exists && !field.alwaysNull && field.kind == fieldKindDynamic &&
			!field.storedPath.isZero() {
			// A direct stored Dynamic path can be either an exact scalar or a
			// flattened object parent on each row. Bind the lossless source tuple
			// once so fillnull can preserve the object value and its aligned
			// relative type metadata without repeatedly expanding JSON extraction.
			directSource, err = compileKnowledgeFieldSourceFromField(field, true)
			if err != nil {
				return compiledRelation{}, compileState{}, nil, fmt.Errorf(
					"compile ClickHouse fillnull input %q: %w",
					ref.Name,
					err,
				)
			}
			directSourceAlias = quoteIdentifier(fmt.Sprintf(
				"__os_fillnull_source_%d_%d", stage, index+1,
			))
			directNamesAlias = quoteIdentifier(fmt.Sprintf(
				"__os_fillnull_names_%d_%d", stage, index+1,
			))
			directTypesAlias = quoteIdentifier(fmt.Sprintf(
				"__os_fillnull_types_%d_%d", stage, index+1,
			))
			directVersionAlias = quoteIdentifier(fmt.Sprintf(
				"__os_fillnull_metadata_version_%d_%d", stage, index+1,
			))
			field = fieldState{
				valueSQL:                directSource.valueSQL(directSourceAlias),
				dynamicTypeSQL:          "dynamicType(" + directSource.valueSQL(directSourceAlias) + ")",
				storedTypeSQL:           directSource.storedTypeSQL(directSourceAlias),
				existsSQL:               directSource.producedSQL(directSourceAlias),
				descendantSQL:           "notEmpty(" + directSource.namesSQL(directSourceAlias) + ")",
				relativeFieldNamesSQL:   directSource.namesSQL(directSourceAlias),
				relativeFieldTypesSQL:   directSource.typesSQL(directSourceAlias),
				fieldMetadataVersionSQL: directSource.metadataVersionSQL(directSourceAlias),
				kind:                    fieldKindDynamic,
			}
			materializeDirect = true
		}
		fill := "CAST(? AS String)"
		value := compiledScalar{
			valueSQL:        fill,
			valueArgs:       []any{operator.Value},
			maxStringBytes:  max(uint64(1), uint64(len(operator.Value))),
			textEligibleSQL: "1",
			existsSQL:       "1",
			kind:            fieldKindString,
		}
		if exists && !field.alwaysNull {
			existsSQL := field.existsSQL
			if existsSQL == "" {
				existsSQL = "1"
			}
			valueSQL := ""
			switch field.kind {
			case fieldKindString:
				preserve := "((" + existsSQL + ") AND isNotNull(" + field.valueSQL + "))"
				valueSQL = "if(" + preserve + ", " + field.valueSQL + ", " + fill + ")"
				value.kind = fieldKindString
				value.textEligibleSQL = "if(" + preserve + ", " +
					semanticStringEligibleSQL(field, field.valueSQL) + ", 1)"
			case fieldKindDynamic:
				typeSQL := dynamicTypeExpression(field)
				presentSQL := "((" + existsSQL + ") AND " + typeSQL + " != 'None')"
				valueArgs := append([]any(nil), field.existsArgs...)
				if field.descendantSQL != "" {
					// A flattened object is present even when the exact parent has no
					// scalar leaf. Preserve the parent and its sealed descendant
					// sidecar; filling it would perform a container rewrite that the
					// supported surface deliberately defers.
					presentSQL = "(" + presentSQL + " OR (" + field.descendantSQL + "))"
					valueArgs = append(valueArgs, field.descendantArgs...)
					value.descendantSQL = field.descendantSQL
					value.descendantArgs = append([]any(nil), field.descendantArgs...)
					value.storedPath = field.storedPath.clone()
					value.relativeFieldNamesSQL = field.relativeFieldNamesSQL
					value.relativeFieldTypesSQL = field.relativeFieldTypesSQL
					value.fieldMetadataVersionSQL = field.fieldMetadataVersionSQL
				}
				valueSQL = "if(" + presentSQL + ", " + field.valueSQL +
					", CAST(" + fill + " AS Dynamic))"
				value.kind = fieldKindDynamic
				value.dynamicDomain = dynamicScalarDomainAny
				value.valueArgs = append(valueArgs, operator.Value)
			case fieldKindNumber, fieldKindBool, fieldKindTime:
				valueSQL = "if((" + existsSQL + ") AND isNotNull(" +
					field.valueSQL + "), CAST(" + field.valueSQL + " AS Dynamic), " +
					"CAST(" + fill + " AS Dynamic))"
				value.kind = fieldKindDynamic
			case fieldKindStringArray, fieldKindDynamicArray:
				// Physical arrays cannot distinguish an explicit null from a
				// present-empty list. Preserve the latter and fill the former by
				// consulting the sealed logical-presence sidecar.
				presentSQL, presentArgs := logicalFieldPresenceSQL(field)
				valueSQL = "if(" + presentSQL + ", CAST(" + field.valueSQL +
					" AS Dynamic), CAST(" + fill + " AS Dynamic))"
				value.kind = fieldKindDynamic
				value.valueArgs = append(presentArgs, operator.Value)
			default:
				valueSQL = fill
			}
			value.valueSQL = valueSQL
			if field.kind != fieldKindDynamic && !isNativeMultivalueKind(field.kind) {
				value.valueArgs = append(append([]any(nil), field.existsArgs...), operator.Value)
			}
			value.maxStringBytes = max(value.maxStringBytes, field.maxStringBytes)
		}
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d_fillnull_%d", stage, index+1))
		prefixArgs := value.valueArgs
		if materializeDirect {
			// WITH keeps the singleton arrayJoin binding private while allowing
			// the public value and all three metadata columns to consume exactly
			// the same row-local tuple in one relational layer. Keeping fillnull at
			// one layer per authored field preserves the 64-field/depth contract.
			value.descendantSQL = "notEmpty(" + directNamesAlias + ")"
			value.descendantArgs = nil
			value.storedPath = storedPathAuthority{}
			value.relativeFieldNamesSQL = directNamesAlias
			value.relativeFieldTypesSQL = directTypesAlias
			value.fieldMetadataVersionSQL = directVersionAlias

			// A direct stored Dynamic path is not a physical column in the nested
			// relation. Publish the filled value under its authored name now that it
			// is calculated pipeline data. Downstream transforming commands such as
			// mvexpand replace that public column; retaining only a private value
			// alias would make their otherwise sealed projection reference a column
			// that never existed. The aligned descendant sidecars below preserve a
			// flattened object without reattaching immutable stored-path authority.
			projection := "SELECT " + upsertWildcardFieldProjection(
				"*",
				next,
				ref.Name,
				value.valueSQL,
				alias,
				false,
			)
			projection += ", " + directSource.namesSQL(directSourceAlias) +
				" AS " + directNamesAlias + ", " +
				directSource.typesSQL(directSourceAlias) + " AS " + directTypesAlias +
				", " + directSource.metadataVersionSQL(directSourceAlias) +
				" AS " + directVersionAlias + " FROM (" + result.sql + ") AS " + alias
			result = result.selectFrom(
				"WITH arrayJoin(["+directSource.sql+"]) AS "+directSourceAlias+" "+projection,
				operator.Range,
			)
			prefixArgs = make([]any, 0, len(directSource.args)+len(value.valueArgs))
			prefixArgs = append(prefixArgs, directSource.args...)
			prefixArgs = append(prefixArgs, value.valueArgs...)
		} else {
			if exists && field.kind == fieldKindString {
				// Bind the original value, presence, and semantic text proof before
				// publishing either sibling. ClickHouse resolves a bare reference in
				// the same SELECT to the REPLACE alias, so the source value is
				// explicitly qualified through the nested relation and captured in a
				// singleton tuple. This remains one relational layer per field.
				textEligibleAlias = quoteIdentifier(fmt.Sprintf(
					"__os_fillnull_text_eligible_%d_%d", stage, index+1,
				))
				mayCarryBytes := field.textEligibleSQL != "" || field.storedTypeSQL != ""
				if mayCarryBytes {
					storedTypeAlias = quoteIdentifier(fmt.Sprintf(
						"__os_fillnull_stored_type_%d_%d", stage, index+1,
					))
				}
				bindingAlias := quoteIdentifier(fmt.Sprintf(
					"__os_fillnull_string_source_%d_%d", stage, index+1,
				))
				sourceValueSQL := field.valueSQL
				// Calculated and aggregate fields may keep a compiler-private
				// physical identifier while publishing a different authored name.
				// Address that actual identifier through the nested relation. A
				// non-identifier expression is already scoped to the nested relation
				// and remains unchanged; it must not be rewritten to alias.<public>.
				if isCanonicalQuotedIdentifierSQL(field.valueSQL) {
					sourceValueSQL = alias + "." + field.valueSQL
				}
				rebind := func(expression string) string {
					if field.valueSQL == "" || field.valueSQL == sourceValueSQL {
						return expression
					}
					return strings.ReplaceAll(expression, field.valueSQL, sourceValueSQL)
				}
				sourceField := field
				sourceField.valueSQL = sourceValueSQL
				sourceField.existsSQL = field.existsSQL
				if sourceField.existsSQL == "" {
					sourceField.existsSQL = "1"
				}
				sourceField.existsSQL = rebind(sourceField.existsSQL)
				sourceField.textEligibleSQL = rebind(field.textEligibleSQL)
				sourceField.storedTypeSQL = rebind(field.storedTypeSQL)
				boundValueSQL := "tupleElement(" + bindingAlias + ", 1)"
				boundPresentSQL := "tupleElement(" + bindingAlias + ", 2) != 0"
				boundTextSQL := "tupleElement(" + bindingAlias + ", 3) != 0"
				preserveSQL := "(" + boundPresentSQL + " AND isNotNull(" +
					boundValueSQL + "))"
				bindingSQL := "tuple(" + sourceValueSQL + ", toUInt8(ifNull(" +
					sourceField.existsSQL + ", 0)), toUInt8(ifNull(" +
					semanticStringEligibleSQL(sourceField, sourceValueSQL) + ", 0)))"
				replacementSQL := "if(" + preserveSQL + ", " + boundValueSQL +
					", " + fill + ")"
				proofSQL := "toUInt8(if(" + preserveSQL + ", " + boundTextSQL + ", 1))"
				if mayCarryBytes {
					preservedValueSQL := "if(" + boundTextSQL + ", CAST(" +
						boundValueSQL + " AS Dynamic), " +
						bytesEnvelopePayloadDynamicSQL(rawStdBase64EncodeSQL(
							"assumeNotNull("+boundValueSQL+")",
						)) + ")"
					replacementSQL = "if(" + preserveSQL + ", " + preservedValueSQL +
						", CAST(" + fill + " AS Dynamic))"
				}
				replacePublic := authoredFieldPhysicallyPublic(next, ref.Name)
				publicProjection := upsertWildcardFieldProjection(
					"*",
					next,
					ref.Name,
					replacementSQL,
					alias,
					replacePublic,
				)
				// A logical field can be backed by a private aggregate or
				// chronological column. The shared projection appends its first
				// public materialization when REPLACE would be invalid, while also
				// preserving any physical ancestor or descendant columns.
				projection := "WITH arrayJoin([" + bindingSQL + "]) AS " + bindingAlias +
					" SELECT " + publicProjection + ", " +
					proofSQL + " AS " + textEligibleAlias + " FROM (" + result.sql +
					") AS " + alias
				if mayCarryBytes {
					storedTypeSQL := "toUInt8(if(" + preserveSQL + " AND NOT " +
						boundTextSQL + ", " + storedValueTypeSQL(eventfields.StoredValueTypeBytes) +
						", " + storedValueTypeSQL(eventfields.StoredValueTypeString) + "))"
					projection = "WITH arrayJoin([" + bindingSQL + "]) AS " + bindingAlias +
						" SELECT " + publicProjection + ", " +
						proofSQL + " AS " + textEligibleAlias + ", " + storedTypeSQL +
						" AS " + storedTypeAlias + " FROM (" + result.sql + ") AS " + alias
					value.kind = fieldKindDynamic
					value.dynamicDomain = dynamicScalarDomainAny
					value.storedTypeSQL = storedTypeAlias
				}
				result = result.selectFrom(projection, operator.Range)
				prefixArgs = append(append([]any(nil), field.existsArgs...), operator.Value)
				value.textEligibleSQL = textEligibleAlias
			} else {
				projection := "SELECT " + upsertWildcardFieldProjection(
					"*",
					next,
					ref.Name,
					value.valueSQL,
					alias,
					exists && authoredFieldPhysicallyPublic(next, ref.Name),
				) + " FROM (" + result.sql + ") AS " + alias
				result = result.selectFrom(projection, operator.Range)
			}
		}
		args = prependArguments(prefixArgs, args)
		next, err = extendCompileState(next, ref, value, value.descendantSQL != "")
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
		if exists && field.kind == fieldKindString {
			filled := next.visible[ref.Name]
			filled.caseSensitive = field.caseSensitive
			next.visible[ref.Name] = filled
			if textEligibleAlias != "" {
				next.privateColumns = append(next.privateColumns, textEligibleAlias)
			}
			if storedTypeAlias != "" {
				next.privateColumns = append(next.privateColumns, storedTypeAlias)
			}
		}
		if materializeDirect {
			next.privateColumns = append(
				next.privateColumns,
				directNamesAlias,
				directTypesAlias,
				directVersionAlias,
			)
		}
	}
	return result, next, args, nil
}

func compileRowTotal(
	relation compiledRelation,
	operator *plan.RowTotal,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil || len(operator.Inputs) < 1 ||
		operator.Output == "" || isCompilerPrivateField(operator.Output) {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse addtotals: operator is invalid",
		)
	}
	if len(operator.Inputs) > spl.MaximumExplicitProjectionFields {
		return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX", Message: fmt.Sprintf(
				"addtotals contains more than %d fields", spl.MaximumExplicitProjectionFields,
			), Range: operator.Range,
		}
	}
	if state.context == nil {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse addtotals: query context is required",
		)
	}
	// Dynamic numeric normalization can throw a sanitized malformed-value
	// marker even when a single input means there is no authored addition.
	state.context.atomicResult = true
	state.context.requiresMaterializedValidationSettings = true
	if _, err := plan.ResolveField(operator.Output, operator.Range); err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	seen := make(map[string]struct{}, len(operator.Inputs))
	terms := make([]string, 0, len(operator.Inputs))
	args := make([]any, 0)
	dynamicInputs := make([]string, 0, len(operator.Inputs))
	dynamicInputArgs := make([]any, 0)
	dynamicInputsAlias := quoteIdentifier(fmt.Sprintf(
		"__os_addtotals_dynamic_inputs_%d",
		stage,
	))
	dynamicCellsAlias := quoteIdentifier(fmt.Sprintf(
		"__os_addtotals_dynamic_cells_%d",
		stage,
	))
	for _, ref := range operator.Inputs {
		if err := validateCommandField("addtotals", ref); err != nil {
			return compiledRelation{}, compileState{}, nil, nil, err
		}
		if _, duplicate := seen[ref.Name]; duplicate {
			return compiledRelation{}, compileState{}, nil, nil, errors.New(
				"compile ClickHouse addtotals: input field is repeated",
			)
		}
		seen[ref.Name] = struct{}{}
		field, exists, err := resolveCompiledField(ref, state)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, nil, err
		}
		term := "CAST(NULL AS Nullable(Float64))"
		var termArgs []any
		if exists && field.kind == fieldKindDynamic {
			present := field.existsSQL
			if present == "" {
				present = "1"
			}
			dynamicInputs = append(
				dynamicInputs,
				"tuple("+field.valueSQL+", toUInt8(ifNull("+present+", 0)))",
			)
			dynamicInputArgs = append(dynamicInputArgs, compiledScalarFromField(field).valueArgs...)
			dynamicInputArgs = append(dynamicInputArgs, field.existsArgs...)
			dynamicOrdinal := len(dynamicInputs)
			cellValue := "tupleElement(arrayElement(" + dynamicCellsAlias + ", " +
				strconv.Itoa(dynamicOrdinal) + "), 1)"
			term = "if(isFinite(" + cellValue + "), " + cellValue +
				", CAST(NULL AS Nullable(Float64)))"
		} else if exists && field.kind == fieldKindNumber {
			normalized, normalizeErr := normalizeArithmeticOperand(
				compiledScalarFromField(field),
				ref.Range,
			)
			if normalizeErr != nil {
				return compiledRelation{}, compileState{}, nil, nil, normalizeErr
			}
			term = "if(isFinite(" + normalized.valueSQL + "), " +
				normalized.valueSQL + ", CAST(NULL AS Nullable(Float64)))"
			termArgs = append(termArgs, normalized.valueArgs...)
			termArgs = append(termArgs, normalized.valueArgs...)
		}
		terms = append(terms, "ifNull("+term+", toFloat64(0))")
		args = append(args, termArgs...)
	}
	for range len(operator.Inputs) - 1 {
		if err := chargeCompiledArithmeticOperator(state.context, operator.Range); err != nil {
			return compiledRelation{}, compileState{}, nil, nil, err
		}
	}
	// The contract publishes a nullable numeric column even though the
	// explicit all-ineligible rule yields the non-null value zero. Preserve that
	// schema promise instead of letting ClickHouse narrow this expression to a
	// non-null Float64 from the current data path.
	valueSQL := "CAST(plus(" + strings.Join(terms, " + ") +
		", toFloat64(0)) AS Nullable(Float64))"
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	prepared := relation
	if len(dynamicInputs) > 0 {
		inputAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_addtotals_inputs", stage))
		prepared = prepared.selectFrom(
			"SELECT *, ["+strings.Join(dynamicInputs, ", ")+"] AS "+
				dynamicInputsAlias+" FROM ("+prepared.sql+") AS "+inputAlias,
			operator.Range,
		)
		cellInput := compiledScalar{
			valueSQL:       "tupleElement(item, 1)",
			dynamicTypeSQL: "dynamicType(tupleElement(item, 1))",
			existsSQL:      "tupleElement(item, 2)",
			kind:           fieldKindDynamic,
		}
		normalized := normalizeDynamicArithmeticOperand(cellInput)
		malformed := dynamicMalformedSemanticScalarConditionSQL(cellInput)
		cellSQL := "tuple(" + normalized.valueSQL + ", toUInt8(" +
			"tupleElement(item, 2) != 0 AND " + malformed + "))"
		cellAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_addtotals_cells", stage))
		prepared = prepared.selectFrom(
			"SELECT *, arrayMap(item -> "+cellSQL+", "+dynamicInputsAlias+
				") AS "+dynamicCellsAlias+" FROM ("+prepared.sql+") AS "+cellAlias,
			operator.Range,
		)
	}
	validationColumn := quoteIdentifier(fmt.Sprintf(
		"__os_addtotals_validation_%d",
		stage,
	))
	validationSQL := "toUInt8(0)"
	if len(dynamicInputs) > 0 {
		validationSQL = "if(arrayExists(cell -> tupleElement(cell, 2) != 0, " +
			dynamicCellsAlias + ")" +
			", throwIf(toUInt8(1), '" + UnsupportedExpressionValueMarker +
			"'), toUInt8(0))"
	}
	// Evaluate the public total and the poison bit as sibling expressions over
	// the immutable pre-command row. In particular, fieldname=x with x among
	// the inputs must validate the original Dynamic x, not the Float64 value
	// that this same projection publishes under x.
	privateInputs := ""
	if len(dynamicInputs) > 0 {
		privateInputs = " EXCEPT (" + dynamicInputsAlias + ", " + dynamicCellsAlias + ")"
	}
	resultProjection := "SELECT " + upsertWildcardFieldProjection(
		"*"+privateInputs,
		state,
		operator.Output,
		valueSQL,
		alias,
		authoredFieldPhysicallyPublic(state, operator.Output),
	)
	resultSQL := resultProjection + ", " + validationSQL + " AS " +
		validationColumn + " FROM (" + prepared.sql + ") AS " + alias
	result := prepared.selectFrom(resultSQL, operator.Range)
	output, err := plan.ResolveField(operator.Output, operator.Range)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	next, err := extendCompileState(state, output, compiledScalar{
		valueSQL: valueSQL, valueArgs: args, existsSQL: "1", kind: fieldKindNumber,
		numberType: "Float64", ieeeComparison: true,
	}, false)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	barrierName := quoteIdentifier(fmt.Sprintf("__os_addtotals_result_%d", stage))
	barrier := &pendingChronologicalBarrier{
		name:              barrierName,
		sql:               result.sql,
		validationColumns: []string{validationColumn},
		fanout:            1,
		depth:             result.depth,
		ownerRange:        operator.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf("__os_addtotals_rows_%d", stage))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	prefixArgs := append(args, dynamicInputArgs...)
	return result.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func materializeEstablishedOrder(
	relation compiledRelation,
	state compileState,
	stage int,
	prefix string,
	sourceRange spl.Range,
) (compiledRelation, compileState, string, error) {
	keys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	if len(keys) == 0 {
		return compiledRelation{}, compileState{}, "", errors.New(
			"compile ClickHouse " + prefix + ": established input order is empty",
		)
	}
	projection := make([]string, 0, len(keys))
	durable := make([]compiledSortKey, len(keys))
	for index, key := range keys {
		name := quoteIdentifier(fmt.Sprintf("__os_%s_order_%d_%d", prefix, stage, index))
		projection = append(projection, key.valueSQL+" AS "+name)
		key.valueSQL = name
		durable[index] = key
	}
	order, err := compileMaterializedOrder(durable, false)
	if err != nil {
		return compiledRelation{}, compileState{}, "", err
	}
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d_%s_order", stage, prefix))
	result := relation.selectFrom(
		"SELECT *, "+strings.Join(projection, ", ")+" FROM ("+relation.sql+
			") AS "+alias+" ORDER BY "+order,
		sourceRange,
	)
	next := cloneCompileState(state)
	next.order = durable
	next.tieBreakers = append([]compiledSortKey(nil), state.tieBreakers...)
	for _, key := range durable {
		if !slices.Contains(next.privateColumns, key.valueSQL) {
			next.privateColumns = append(next.privateColumns, key.valueSQL)
		}
	}
	return result, next, order, nil
}

func compileOrderedDelta(
	relation compiledRelation,
	operator *plan.OrderedDelta,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil || operator.Previous < 1 ||
		operator.Output == "" || isCompilerPrivateField(operator.Output) {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse delta: operator is invalid",
		)
	}
	if operator.Previous > spl.MaximumStreamStatsWindow {
		return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX", Message: fmt.Sprintf(
				"delta p exceeds the supported row window of %d", spl.MaximumStreamStatsWindow,
			), Range: operator.Range,
		}
	}
	if err := validateCommandField("delta", operator.Input); err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	if _, explicitErr := plan.ResolveField(operator.Output, operator.Range); explicitErr != nil &&
		operator.Output != "delta("+operator.Input.Name+")" {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse delta: output field is invalid",
		)
	}
	ordered, next, orderSQL, err := materializeEstablishedOrder(
		relation, state, stage, "delta", operator.Range,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	field, exists, err := resolveCompiledField(operator.Input, next)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	input := compiledScalar{
		valueSQL: "CAST(NULL AS Nullable(String))", existsSQL: "0",
		kind: fieldKindString, alwaysNull: true,
	}
	if exists {
		input = compiledScalarFromField(field)
	}
	normalized := arithmeticCompiledScalar(
		"CAST(NULL AS Nullable(Float64))", nil, true, false,
	)
	if input.kind != fieldKindDynamic {
		normalized, err = normalizeArithmeticOperand(input, operator.Input.Range)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, nil, err
		}
	}
	if err := chargeCompiledArithmeticOperator(state.context, operator.Range); err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	// The row ceiling and malformed-value marker are relation-wide validation,
	// not output-row predicates. Keep later filters and projections behind this
	// guard on the pinned ClickHouse optimizer just like mvexpand validation.
	state.context.requiresMaterializedValidationSettings = true
	valueAlias := quoteIdentifier(fmt.Sprintf("__os_delta_value_%d", stage))
	countAlias := quoteIdentifier(fmt.Sprintf("__os_delta_input_count_%d", stage))
	validationColumn := quoteIdentifier(fmt.Sprintf("__os_delta_validation_%d", stage))
	preparedSource := ordered
	normalizedSQL := normalized.valueSQL
	validationSQL := "toUInt8(0)"
	prefixArgs := append([]any(nil), normalized.valueArgs...)
	privateColumns := make([]string, 0, 2)
	if input.kind == fieldKindDynamic {
		inputsAlias := quoteIdentifier(fmt.Sprintf(
			"__os_delta_dynamic_inputs_%d",
			stage,
		))
		cellsAlias := quoteIdentifier(fmt.Sprintf(
			"__os_delta_dynamic_cells_%d",
			stage,
		))
		presentSQL := input.existsSQL
		if presentSQL == "" {
			presentSQL = "1"
		}
		inputAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_delta_inputs", stage))
		preparedSource = preparedSource.selectFrom(
			"SELECT *, [tuple("+input.valueSQL+", toUInt8(ifNull("+
				presentSQL+", 0)))] AS "+inputsAlias+" FROM ("+
				preparedSource.sql+") AS "+inputAlias,
			operator.Range,
		)
		cellInput := compiledScalar{
			valueSQL:       "tupleElement(item, 1)",
			dynamicTypeSQL: "dynamicType(tupleElement(item, 1))",
			existsSQL:      "tupleElement(item, 2)",
			kind:           fieldKindDynamic,
		}
		cellValue := normalizeDynamicArithmeticOperand(cellInput)
		cellMalformed := dynamicMalformedSemanticScalarConditionSQL(cellInput)
		cellSQL := "tuple(" + cellValue.valueSQL + ", toUInt8(" +
			"tupleElement(item, 2) != 0 AND " + cellMalformed + "))"
		cellAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_delta_cells", stage))
		preparedSource = preparedSource.selectFrom(
			"SELECT *, arrayMap(item -> "+cellSQL+", "+inputsAlias+
				") AS "+cellsAlias+" FROM ("+preparedSource.sql+") AS "+cellAlias,
			operator.Range,
		)
		cell := "arrayElement(" + cellsAlias + ", 1)"
		normalizedSQL = "tupleElement(" + cell + ", 1)"
		validationSQL = "if(tupleElement(" + cell + ", 2) != 0, " +
			"throwIf(toUInt8(1), '" + UnsupportedExpressionValueMarker +
			"'), toUInt8(0))"
		prefixArgs = append(prefixArgs[:0], input.valueArgs...)
		prefixArgs = append(prefixArgs, input.existsArgs...)
		privateColumns = append(privateColumns, inputsAlias, cellsAlias)
	}
	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_delta_prepared", stage))
	preparedProjection := "SELECT *"
	if len(privateColumns) > 0 {
		preparedProjection += " EXCEPT (" + strings.Join(privateColumns, ", ") + ")"
	}
	prepared := preparedSource.selectFrom(
		// ClickHouse requires lagInFrame's value and default arguments to have
		// exactly the same static type. A fixed/non-null Float64 can otherwise be
		// inferred here even though delta's public result is nullable. Seal the
		// window input to Nullable(Float64) before binding the alias.
		preparedProjection+", CAST("+normalizedSQL+" AS Nullable(Float64)) AS "+valueAlias+
			", "+validationSQL+" AS "+validationColumn+
			" FROM ("+preparedSource.sql+") AS "+preparedAlias+
			" LIMIT "+strconv.FormatUint(MaximumStreamStatsInputRows+1, 10),
		operator.Range,
	)
	lag := "lagInFrame(" + valueAlias + ", " +
		strconv.FormatUint(operator.Previous, 10) + ", " +
		"CAST(NULL AS Nullable(Float64))) OVER (ORDER BY " + orderSQL +
		" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
	outputName := quoteIdentifier(operator.Output)
	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_delta_window", stage))
	wildcardProjection := "*"
	if authoredFieldPhysicallyPublic(next, operator.Output) {
		wildcardProjection += " EXCEPT (" + outputName + ")"
	}
	wildcardProjection = preserveWildcardFieldNamespace(
		wildcardProjection,
		next,
		operator.Output,
		windowAlias,
	)
	windowProjection := wildcardProjection + ", "
	windowSQL := "SELECT " + windowProjection + "count() OVER () AS " + countAlias + ", " +
		"if(isNull(" + valueAlias + ") OR isNull(" + lag + "), " +
		"CAST(NULL AS Nullable(Float64)), plus(" + valueAlias + " - " + lag +
		", toFloat64(0))) AS " + outputName + " FROM (" + prepared.sql +
		") AS " + windowAlias
	guardAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_delta_guard", stage))
	guard := "if(" + countAlias + " > toUInt64(" +
		strconv.FormatUint(MaximumStreamStatsInputRows, 10) +
		"), throwIf(toUInt8(1), '" + StreamStatsInputLimitMarker +
		"'), toUInt8(0)) = 0"
	result := prepared.selectFrom(
		"SELECT * EXCEPT ("+valueAlias+", "+countAlias+") FROM ("+
			windowSQL+") AS "+guardAlias+" WHERE "+guard,
		operator.Range,
	)
	output := plan.FieldRef{Name: operator.Output, Range: operator.Range}
	if resolved, resolveErr := plan.ResolveField(operator.Output, operator.Range); resolveErr == nil {
		output = resolved
	}
	next, err = extendCompileState(next, output, compiledScalar{
		valueSQL: outputName, existsSQL: "1", kind: fieldKindNumber,
		numberType: "Float64", ieeeComparison: true,
	}, false)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, nil, err
	}
	barrierName := quoteIdentifier(fmt.Sprintf("__os_delta_result_%d", stage))
	barrier := &pendingChronologicalBarrier{
		name:              barrierName,
		sql:               result.sql,
		validationColumns: []string{validationColumn},
		fanout:            1,
		depth:             result.depth,
		ownerRange:        operator.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf("__os_delta_rows_%d", stage))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	return result.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func publicRetainedTupleSQL(
	state compileState,
	replacementName, replacementSQL string,
	replacementPresentSQL ...string,
) string {
	if len(replacementPresentSQL) > 1 {
		panic("retained tuple: replacement presence has multiple expressions")
	}
	logicalReplacementSQL := replacementSQL
	if len(replacementPresentSQL) == 1 && replacementPresentSQL[0] != "" {
		logicalReplacementSQL = optionalMultivaluePublicJSONSQL(
			replacementSQL,
			replacementPresentSQL[0],
		)
	}
	type retainedValue struct {
		name string
		sql  string
	}
	values := make([]retainedValue, 0, len(state.publicOrder)+1)
	appendValue := func(name, valueSQL string) {
		values = append(values, retainedValue{name: name, sql: valueSQL})
	}
	seen := make(map[string]struct{}, len(state.publicOrder)+1)
	replacement, replacementErr := plan.ResolveField(replacementName, spl.Range{})
	keepsRawFieldsPayload := replacementErr == nil && replacement.Canonical
	for _, name := range state.publicOrder {
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		if name == replacementName {
			appendValue(name, logicalReplacementSQL)
			continue
		}
		if name == "fields" && exposesRawFieldsPayload(state) && !keepsRawFieldsPayload {
			// A calculated dynamic output shadows the raw convenience payload;
			// extendCompileState drops it from the public result.
			continue
		}
		if name == "fields" && exposesRawFieldsPayload(state) {
			// Replacing a promoted canonical field leaves the raw convenience
			// payload public. Charge that complete Dynamic object instead of
			// silently omitting the largest retained value from the stage limit.
			appendValue(name, quoteIdentifier(internalFieldsColumn))
			continue
		}
		if field, visible := state.visible[name]; visible {
			valueSQL := field.valueSQL
			if field.optionalMultivaluePresentSQL != "" {
				valueSQL = optionalMultivaluePublicJSONSQL(
					field.valueSQL,
					field.optionalMultivaluePresentSQL,
				)
			}
			appendValue(name, valueSQL)
		}
	}
	if _, included := seen[replacementName]; !included {
		appendValue(replacementName, logicalReplacementSQL)
	}
	if len(values) == 0 {
		return "tuple()"
	}
	// With enable_named_columns_in_function_tuple, ClickHouse resolves an AS
	// name throughout the tuple expression. A public name identical to its
	// source column (for example `_time AS _time`) can therefore self-shadow
	// that source inside the surrounding retained-member lambda. Bind every
	// source to a compiler-private parameter first, then assign only the bound
	// parameter its public JSON key.
	parameters := make([]string, len(values))
	expressions := make([]string, len(values))
	tupleValues := make([]string, len(values))
	for index, value := range values {
		parameters[index] = fmt.Sprintf("__os_retained_value_%d", index)
		expressions[index] = value.sql
		tupleValues[index] = parameters[index] + " AS " + quoteIdentifier(value.name)
	}
	return bindSQLExpressions(
		parameters,
		expressions,
		"tuple("+strings.Join(tupleValues, ", ")+")",
	)
}

// optionalMultivaluePublicJSONSQL reconstructs the logical public cell used
// by retained-byte accounting. Nullable(Array(String)) is not a legal
// ClickHouse type, so optional lists are physically an Array(String) plus a
// sealed presence bit. Dynamic can represent both branches without confusing
// public null with the physically empty array used for an absent value.
func optionalMultivaluePublicJSONSQL(valueSQL, presentSQL string) string {
	return "if(toUInt8(ifNull(" + presentSQL + ", 0)) = 0, " +
		"CAST(NULL AS Dynamic), CAST(" + valueSQL + " AS Dynamic))"
}

func storedValueTypeSQL(valueType eventfields.StoredValueType) string {
	return "toUInt8(" + strconv.Itoa(int(valueType)) + ")"
}

// semanticSourceTypeEligibleSQL keeps physical ClickHouse String values
// subordinate to their semantic provenance. In particular, _raw is physically
// String even when it carries bytes, and stored Dynamic leaves can likewise
// have String representation with Bytes metadata. Fixed String arrays use the
// same hook for producers that retain an aggregate/member text proof.
func semanticSourceTypeEligibleSQL(
	field fieldState,
	want eventfields.StoredValueType,
	includeTextEligibility bool,
) string {
	conditions := make([]string, 0, 2)
	if includeTextEligibility && field.textEligibleSQL != "" {
		conditions = append(conditions, "ifNull("+field.textEligibleSQL+", 0)")
	}
	if field.storedTypeSQL != "" {
		conditions = append(
			conditions,
			"toUInt8(ifNull("+field.storedTypeSQL+", 0)) = "+
				storedValueTypeSQL(want),
		)
	}
	if len(conditions) == 0 {
		return "1"
	}
	return "(" + strings.Join(conditions, " AND ") + ")"
}

func semanticStringEligibleSQL(field fieldState, valueSQL string) string {
	return "(" + semanticSourceTypeEligibleSQL(
		field,
		eventfields.StoredValueTypeString,
		true,
	) + " AND isValidUTF8(" + valueSQL + "))"
}

func compileMakeMultivalue(
	relation compiledRelation,
	operator *plan.MakeMultivalue,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || operator.Delimiter == "" ||
		!utf8.ValidString(operator.Delimiter) ||
		len(operator.Delimiter) > spl.MaximumMakeMVDelimiterBytes ||
		strings.HasPrefix(operator.Input.Name, "_") {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse makemv: operator is invalid",
		)
	}
	if err := validateCommandField("makemv", operator.Input); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if state.context == nil {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse makemv: query context is required",
		)
	}
	field, exists, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if exists && field.kind != fieldKindString && field.kind != fieldKindDynamic {
		return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_MAKEMV_VALUE_TYPE",
			Message: "makemv requires a scalar String input",
			Range:   operator.Input.Range,
		}
	}

	inputAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_input_%d", stage))
	sourcePresentAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_source_present_%d", stage))
	descendantAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_descendant_%d", stage))
	valuePresentAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_value_present_%d", stage))
	invalidAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_invalid_%d", stage))
	resultAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_result_%d", stage))
	rowMembersAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_row_members_%d", stage))
	rowBytesAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_row_bytes_%d", stage))
	totalMembersAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_total_members_%d", stage))
	totalBytesAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_total_bytes_%d", stage))
	retainedBytesAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_retained_bytes_%d", stage))
	anyInvalidAlias := quoteIdentifier(fmt.Sprintf("__os_makemv_any_invalid_%d", stage))

	inputSQL := "CAST(NULL AS Nullable(String))"
	existsSQL := "0"
	descendantSQL := "0"
	prefixArgs := make([]any, 0)
	inputKind := fieldKindString
	if exists {
		inputSQL = field.valueSQL
		existsSQL = field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		inputKind = field.kind
		prefixArgs = append(prefixArgs, field.existsArgs...)
		if field.descendantSQL != "" {
			descendantSQL = field.descendantSQL
			prefixArgs = append(prefixArgs, field.descendantArgs...)
		}
	}
	boundAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_makemv_bound", stage))
	boundSQL := "SELECT *, " + inputSQL + " AS " + inputAlias + ", " +
		"toUInt8(ifNull(" + existsSQL + ", 0)) AS " + sourcePresentAlias + ", " +
		"toUInt8(ifNull(" + descendantSQL + ", 0)) AS " + descendantAlias +
		" FROM (" + relation.sql + ") AS " + boundAlias
	bound := relation.selectFrom(boundSQL, operator.Range)

	stringSQL := inputAlias
	valuePresentSQL := "(" + sourcePresentAlias + " != 0 AND isNotNull(" + inputAlias + "))"
	invalidSQL := descendantAlias + " != 0"
	switch inputKind {
	case fieldKindDynamic:
		typeSQL := "dynamicType(" + inputAlias + ")"
		stringSQL = "dynamicElement(" + inputAlias + ", 'String')"
		valuePresentSQL = "(" + sourcePresentAlias + " != 0 AND " + typeSQL + " = 'String')"
		invalidSQL = "(" + descendantAlias + " != 0 OR (" + sourcePresentAlias +
			" != 0 AND " + typeSQL + " != 'None' AND (" + typeSQL +
			" != 'String' OR NOT " + semanticStringEligibleSQL(field, stringSQL) + ")))"
	case fieldKindString:
		invalidSQL = "(" + descendantAlias + " != 0 OR (" + valuePresentSQL +
			" AND NOT " + semanticStringEligibleSQL(field, stringSQL) + "))"
	}
	// Dynamic extraction and nullable stored strings can have a nullable static
	// type even though valuePresentSQL protects the split semantically.
	// ClickHouse rejects Nullable(Array(String)) during type inference, so make
	// the split input non-null before constructing the array and retain logical
	// null/missing exclusively in the sealed presence sidecar.
	// Never construct an attacker-sized intermediate array merely to reject it.
	// The extra sentinel member proves that the public 1,000-member ceiling was
	// crossed. With allowempty=false, split on a run of the literal delimiter so
	// a megabyte of adjacent separators remains one bounded regex match instead
	// of hundreds of thousands of empty array elements. Two sentinels preserve
	// room for the possible leading empty substring and still expose 1,001
	// nonempty members. splitby_max_substrings_includes_remaining_string=0 is
	// sealed onto every multivalue validation query, so an oversized unsplit remainder
	// is never copied into the last sentinel element.
	maximumSubstrings := plan.MaximumMakeMVMembersPerRow + 1
	splitFunction := "splitByString(?, ifNull(" + stringSQL +
		", CAST('' AS String)), toUInt64(" + strconv.FormatUint(maximumSubstrings, 10) + "))"
	if !operator.AllowEmpty {
		maximumSubstrings++
		splitFunction = "splitByRegexp(concat('(', regexpQuoteMeta(?), ')+'), ifNull(" +
			stringSQL + ", CAST('' AS String)), toUInt64(" +
			strconv.FormatUint(maximumSubstrings, 10) + "))"
	}
	split := splitFunction
	if !operator.AllowEmpty {
		split = "arrayFilter(member -> member != '', " + splitFunction + ")"
	}
	resultSQL := "if(" + valuePresentAlias + " != 0, " + split +
		", CAST([], 'Array(String)'))"
	computedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_makemv_computed", stage))
	computedSQL := "SELECT *, toUInt8(" + valuePresentSQL + ") AS " +
		valuePresentAlias + ", toUInt8(" + invalidSQL + ") AS " + invalidAlias +
		", " + resultSQL + " AS " + resultAlias + " FROM (" + bound.sql +
		") AS " + computedAlias
	computed := bound.selectFrom(computedSQL, operator.Range)
	prefixArgs = prependArguments([]any{operator.Delimiter}, prefixArgs)

	memberBytes := "arraySum(arrayMap(member -> toUInt64(length(member)), " + resultAlias + "))"
	retainedTuple := publicRetainedTupleSQL(
		state,
		operator.Input.Name,
		resultAlias,
		valuePresentAlias,
	)
	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_makemv_window", stage))
	windowSQL := "SELECT *, toUInt64(length(" + resultAlias + ")) AS " +
		rowMembersAlias + ", " + memberBytes + " AS " + rowBytesAlias +
		", max(" + invalidAlias + ") OVER () AS " + anyInvalidAlias +
		", sum(toUInt64(length(" + resultAlias + "))) OVER () AS " + totalMembersAlias +
		", sum(" + memberBytes + ") OVER () AS " + totalBytesAlias +
		", sum(toUInt64(length(toJSONString(" + retainedTuple + ")))) OVER () AS " +
		retainedBytesAlias + " FROM (" + computed.sql + ") AS " + windowAlias
	windowed := computed.selectFrom(windowSQL, operator.Range)

	guard := "multiIf(" +
		anyInvalidAlias + " != 0, throwIf(toUInt8(1), '" + UnsupportedMakeMVValueMarker + "'), " +
		rowMembersAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMakeMVMembersPerRow, 10) +
		"), throwIf(toUInt8(1), '" + MakeMVRowMembersLimitMarker + "'), " +
		rowBytesAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMakeMVMemberBytesPerRow, 10) +
		"), throwIf(toUInt8(1), '" + MakeMVRowBytesLimitMarker + "'), " +
		totalMembersAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMakeMVMembersPerResult, 10) +
		"), throwIf(toUInt8(1), '" + MakeMVResultMembersLimitMarker + "'), " +
		totalBytesAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMakeMVMemberBytesPerResult, 10) +
		"), throwIf(toUInt8(1), '" + MakeMVResultBytesLimitMarker + "'), " +
		retainedBytesAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMakeMVRetainedBytesPerResult, 10) +
		"), throwIf(toUInt8(1), '" + MakeMVRetainedBytesLimitMarker + "'), toUInt8(0)) = 0"
	helpers := []string{
		inputAlias, sourcePresentAlias, descendantAlias, invalidAlias, resultAlias,
		rowMembersAlias, rowBytesAlias, totalMembersAlias, totalBytesAlias,
		retainedBytesAlias, anyInvalidAlias,
	}
	outputName := quoteIdentifier(operator.Input.Name)
	cteName := quoteIdentifier(fmt.Sprintf("__os_makemv_stage_%d", stage))
	guardAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_makemv_guard", stage))
	publication := upsertWildcardFieldProjection(
		"* EXCEPT ("+strings.Join(helpers, ", ")+")",
		state,
		operator.Input.Name,
		resultAlias,
		guardAlias,
		authoredFieldPhysicallyPublic(state, operator.Input.Name),
	)
	guardSQL := "WITH " + cteName + " AS MATERIALIZED (" + windowed.sql + ") " +
		"SELECT " + publication + " FROM " + cteName + " AS " + guardAlias +
		" WHERE " + guard
	result := compiledRelation{
		sql: guardSQL, depth: relation.depth + 4, ownerRange: operator.Range,
	}

	transportState := cloneCompileState(state)
	transportState.privateColumns = append(
		transportState.privateColumns,
		valuePresentAlias,
	)
	next, err := extendCompileState(
		transportState,
		operator.Input,
		compiledScalar{
			valueSQL: outputName, existsSQL: valuePresentAlias,
			optionalMultivaluePresentSQL: valuePresentAlias,
			textEligibleSQL: "arrayAll(member -> isValidUTF8(member), " +
				outputName + ")",
			kind: fieldKindStringArray,
		},
		false,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	outputField := next.visible[operator.Input.Name]
	outputField.caseSensitive = field.caseSensitive
	next.visible[operator.Input.Name] = outputField
	state.context.atomicResult = true
	state.context.requiresMaterializedValidationSettings = true
	return result, next, prefixArgs, nil
}

func compileExpandMultivalue(
	relation compiledRelation,
	operator *plan.ExpandMultivalue,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || operator.QueryOrdinal < 1 ||
		int(operator.QueryOrdinal) > plan.MaximumMVExpandStages ||
		strings.HasPrefix(operator.Input.Name, "_") {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse mvexpand: operator is invalid",
		)
	}
	if operator.Limit > spl.MaximumMVExpandLimit {
		return compiledRelation{}, compileState{}, nil, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX", Message: fmt.Sprintf(
				"mvexpand limit exceeds %d", spl.MaximumMVExpandLimit,
			), Range: operator.Range,
		}
	}
	if err := validateCommandField("mvexpand", operator.Input); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if state.context == nil {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse mvexpand: query context is required",
		)
	}
	if operator.QueryOrdinal != state.context.mvExpandStages+1 {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse mvexpand: query ordinal is not contiguous",
		)
	}
	ordered, next, _, err := materializeEstablishedOrder(
		relation, state, stage, "mvexpand", operator.Range,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	field, exists, err := resolveCompiledField(operator.Input, next)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}

	inputAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_input_%d", stage))
	sourceExistsAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_source_exists_%d", stage))
	sourcePresentAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_source_present_%d", stage))
	descendantAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_descendant_%d", stage))
	valuesAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_values_%d", stage))
	selectedAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_selected_%d", stage))
	invalidAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_invalid_%d", stage))
	rowMembersAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_row_members_%d", stage))
	stageRowsAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_stage_rows_%d", stage))
	queryRowsAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_query_rows_%d", stage))
	retainedBytesAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_retained_bytes_%d", stage))
	anyInvalidAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_any_invalid_%d", stage))
	memberOrdinalAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_member_ordinal_%d", stage))
	expandedPairAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_pair_%d", stage))
	expandedExistsAlias := quoteIdentifier(fmt.Sprintf("__os_mvexpand_exists_%d", stage))

	inputSQL := "CAST(NULL AS Nullable(String))"
	existsSQL := "0"
	presentSQL := "0"
	descendantSQL := "0"
	prefixArgs := make([]any, 0)
	kind := fieldKindInvalid
	if exists {
		inputSQL = field.valueSQL
		existsSQL = field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		presentSQL = existsSQL
		if isNativeMultivalueKind(field.kind) && field.optionalMultivaluePresentSQL != "" {
			presentSQL = field.optionalMultivaluePresentSQL
		}
		descendantSQL = field.descendantSQL
		if descendantSQL == "" {
			descendantSQL = "0"
		}
		prefixArgs = append(prefixArgs, field.existsArgs...)
		if presentSQL == existsSQL {
			prefixArgs = append(prefixArgs, field.existsArgs...)
		}
		prefixArgs = append(prefixArgs, field.descendantArgs...)
		kind = field.kind
	}
	boundAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_bound", stage))
	boundSQL := "SELECT *, " + inputSQL + " AS " + inputAlias + ", " +
		"toUInt8(ifNull(" + existsSQL + ", 0)) AS " + sourceExistsAlias + ", " +
		"toUInt8(ifNull(" + presentSQL + ", 0)) AS " + sourcePresentAlias + ", " +
		"toUInt8(ifNull(" + descendantSQL + ", 0)) AS " + descendantAlias +
		" FROM (" + ordered.sql + ") AS " + boundAlias
	bound := ordered.selectFrom(boundSQL, operator.Range)

	outputName := quoteIdentifier(operator.Input.Name)
	var valuesSQL string
	invalidSQL := descendantAlias + " != 0"
	output := compiledScalar{
		valueSQL: outputName, existsSQL: expandedExistsAlias, kind: fieldKindDynamic,
		dynamicDomain: dynamicScalarDomainAny,
	}
	switch kind {
	case fieldKindStringArray:
		arrayValues := "arrayMap(value -> CAST(value AS Nullable(String)), " + inputAlias + ")"
		// Every fixed String-array producer carries logical presence. makemv
		// uses an explicit sidecar, while values()/list() use notEmpty(array)
		// because their empty physical result means the SPL field is absent.
		// Preserve one null row for either absent form; a sidecar-present empty
		// makemv array still expands to zero rows.
		valuesSQL = "if(" + sourcePresentAlias + " = 0, [CAST(NULL AS Nullable(String))], " +
			arrayValues + ")"
		arrayEligible := semanticSourceTypeEligibleSQL(
			field,
			eventfields.StoredValueTypeList,
			true,
		)
		invalidSQL = "(" + descendantAlias + " != 0 OR (" + sourcePresentAlias +
			" != 0 AND (NOT " + arrayEligible + " OR arrayExists(value -> " +
			"NOT isValidUTF8(value), " + inputAlias + "))))"
		output = compiledScalar{
			valueSQL: outputName, existsSQL: expandedExistsAlias, kind: fieldKindString,
			maxStringBytes: field.maxStringBytes,
			textEligibleSQL: "isValidUTF8(ifNull(" + outputName +
				", CAST('' AS String)))",
		}
	case fieldKindDynamicArray:
		// A sealed native mixed list is already Array(Dynamic). Preserve every
		// admitted scalar member (including explicit None) as the expanded
		// Dynamic value, while an absent list still contributes the command's
		// single null-preservation row and a present-empty list contributes none.
		valuesSQL = "if(" + sourcePresentAlias +
			" = 0, [CAST(NULL AS Dynamic)], " + inputAlias + ")"
		memberUnsupported := "arrayExists(member -> NOT (" +
			nativeMVElementSupportedSQL("member") + "), " + inputAlias + ")"
		invalidSQL = "(" + descendantAlias + " != 0 OR (" + sourcePresentAlias +
			" != 0 AND " + memberUnsupported + "))"
		output = compiledScalar{
			valueSQL: outputName, existsSQL: expandedExistsAlias, kind: fieldKindDynamic,
			dynamicDomain:  dynamicScalarDomainAny,
			maxStringBytes: field.maxStringBytes,
		}
	case fieldKindDynamic:
		typeSQL := "dynamicType(" + inputAlias + ")"
		dynamicInput := compiledScalar{
			valueSQL: inputAlias, dynamicTypeSQL: typeSQL, kind: fieldKindDynamic,
		}
		dynamicMembers := "dynamicElement(" + inputAlias + ", 'Array(Dynamic)')"
		stringMembers := "dynamicElement(" + inputAlias + ", 'Array(String)')"
		memberSupported := nativeMVElementSupportedSQL("member")
		arrayUnsupported := "arrayExists(member -> NOT (" + memberSupported + "), " +
			dynamicMembers + ")"
		listEligible := semanticSourceTypeEligibleSQL(
			field,
			eventfields.StoredValueTypeList,
			false,
		)
		stringEligible := semanticStringEligibleSQL(field, "dynamicElement("+inputAlias+", 'String')")
		semanticScalar := "(" + dynamicTaggedScalarEnvelopeCondition(dynamicInput) +
			" AND " + dynamicTaggedEnvelopeCondition(
			dynamicInput,
			"timestamp/v1",
			"decimal/v1",
		) + ")"
		valuesSQL = "multiIf(" + sourcePresentAlias + " = 0 OR " + typeSQL +
			" = 'None', [CAST(NULL AS Dynamic)], " + typeSQL +
			" = 'Array(String)', arrayMap(value -> CAST(value AS Dynamic), " +
			stringMembers + "), " + typeSQL + " = 'Array(Dynamic)', " +
			dynamicMembers + ", startsWith(" + typeSQL + ", 'Array('), " +
			"[CAST(NULL AS Dynamic)], [" + inputAlias + "])"
		invalidSQL = "(" + descendantAlias + " != 0 OR (" + sourcePresentAlias +
			" != 0 AND (startsWith(" + typeSQL + ", 'Array(') AND " + typeSQL +
			" NOT IN ('Array(String)', 'Array(Dynamic)') OR (" + typeSQL +
			" = 'Array(String)' AND (NOT " + listEligible + " OR arrayExists(member -> " +
			"NOT isValidUTF8(member), " + stringMembers + "))) OR (" + typeSQL +
			" = 'Array(Dynamic)' AND (NOT " + listEligible + " OR " + arrayUnsupported + ")) OR (" +
			typeSQL + " = 'String' AND NOT " + stringEligible + ") OR " +
			"(startsWith(" + typeSQL + ", 'Map(') AND NOT " + semanticScalar +
			") OR startsWith(" + typeSQL + ", 'Tuple(') OR " +
			typeSQL + " = 'Object')))"
	case fieldKindString, fieldKindNumber, fieldKindBool, fieldKindTime:
		// A fixed scalar is already a one-member relation. Keep its exact
		// ClickHouse type instead of widening it to Dynamic merely because the
		// shared expansion machinery consumes an array. This preserves public
		// String/Number/Bool/time schema as well as the value itself.
		valuesSQL = "[" + inputAlias + "]"
		if kind == fieldKindString {
			valuePresent := "(" + sourcePresentAlias + " != 0 AND isNotNull(" +
				inputAlias + "))"
			invalidSQL = "(" + descendantAlias + " != 0 OR (" + valuePresent +
				" AND NOT " + semanticStringEligibleSQL(field, inputAlias) + "))"
		}
		output = compiledScalar{
			valueSQL:        outputName,
			existsSQL:       expandedExistsAlias,
			kind:            kind,
			textEligibleSQL: field.textEligibleSQL,
			storedTypeSQL:   field.storedTypeSQL,
			numberType:      field.numberType,
			numericIntegral: field.numericIntegral,
			maxStringBytes:  field.maxStringBytes,
			alwaysNull:      field.alwaysNull,
			ieeeComparison:  field.ieeeComparison,
		}
	default:
		valuesSQL = "[CAST(NULL AS Dynamic)]"
	}
	limit := spl.MaximumMVExpandLimit
	if operator.Limit > 0 {
		limit = operator.Limit
	}
	selectedSQL := "arraySlice(" + valuesAlias + ", 1, " +
		strconv.FormatUint(limit, 10) + ")"
	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_prepared", stage))
	preparedSQL := "SELECT *, " + valuesSQL + " AS " + valuesAlias +
		", toUInt8(" + invalidSQL + ") AS " + invalidAlias + " FROM (" +
		bound.sql + ") AS " + preparedAlias
	prepared := bound.selectFrom(preparedSQL, operator.Range)
	selectedStageAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_selected", stage))
	selectedSQLLayer := "SELECT *, " + selectedSQL + " AS " + selectedAlias +
		", toUInt64(length(" + valuesAlias + ")) AS " + rowMembersAlias +
		" FROM (" + prepared.sql + ") AS " + selectedStageAlias
	selected := prepared.selectFrom(selectedSQLLayer, operator.Range)

	retainedMember := "__os_mvexpand_retained_member"
	retainedTuple := publicRetainedTupleSQL(next, operator.Input.Name, retainedMember)
	rowRetainedBytes := "arraySum(arrayMap(" + retainedMember +
		" -> toUInt64(length(toJSONString(" + retainedTuple + "))), " + selectedAlias + "))"
	previousQueryRows := "toUInt64(0)"
	if next.mvExpandQueryRowsSQL != "" {
		previousQueryRows = "max(" + next.mvExpandQueryRowsSQL + ") OVER ()"
	}
	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_window", stage))
	windowSQL := "SELECT *, max(" + invalidAlias + ") OVER () AS " + anyInvalidAlias +
		", sum(toUInt64(length(" + selectedAlias + "))) OVER () AS " + stageRowsAlias +
		", " + previousQueryRows + " + sum(toUInt64(length(" + selectedAlias +
		"))) OVER () AS " + queryRowsAlias +
		", sum(" + rowRetainedBytes + ") OVER () AS " +
		retainedBytesAlias + " FROM (" + selected.sql + ") AS " + windowAlias
	windowed := selected.selectFrom(windowSQL, operator.Range)

	guard := "multiIf(" + anyInvalidAlias + " != 0, throwIf(toUInt8(1), '" +
		UnsupportedMVExpandValueMarker + "'), " + rowMembersAlias + " > toUInt64(" +
		strconv.FormatUint(plan.MaximumMakeMVMembersPerRow, 10) +
		"), throwIf(toUInt8(1), '" + MVExpandRowMembersLimitMarker + "'), " +
		stageRowsAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMVExpandRowsPerStage, 10) +
		"), throwIf(toUInt8(1), '" + MVExpandStageRowsLimitMarker + "'), " +
		queryRowsAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMVExpandRowsPerQuery, 10) +
		"), throwIf(toUInt8(1), '" + MVExpandQueryRowsLimitMarker + "'), " +
		retainedBytesAlias + " > toUInt64(" + strconv.FormatUint(plan.MaximumMVExpandRetainedBytesPerStage, 10) +
		"), throwIf(toUInt8(1), '" + MVExpandRetainedBytesLimitMarker + "'), toUInt8(0)) = 0"
	guardedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_guard", stage))
	cteName := quoteIdentifier(fmt.Sprintf("__os_mvexpand_stage_%d", stage))
	guardedSQL := "WITH " + cteName + " AS MATERIALIZED (" + windowed.sql + ") " +
		"SELECT * FROM " + cteName + " AS " + guardedAlias + " WHERE " + guard
	guarded := windowed.selectFrom(guardedSQL, operator.Range)

	expandAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_mvexpand_rows", stage))
	helpers := []string{
		inputAlias, sourceExistsAlias, sourcePresentAlias, descendantAlias, valuesAlias, selectedAlias,
		invalidAlias, rowMembersAlias, stageRowsAlias, retainedBytesAlias,
		anyInvalidAlias,
	}
	memberSQL := "tupleElement(" + expandedPairAlias + ", 1)"
	memberOrdinalSQL := "tupleElement(" + expandedPairAlias + ", 2)"
	projection := upsertWildcardFieldProjection(
		"* EXCEPT ("+strings.Join(helpers, ", ")+")",
		next,
		operator.Input.Name,
		memberSQL,
		expandAlias,
		authoredFieldPhysicallyPublic(next, operator.Input.Name),
	)
	expandedSQL := "SELECT " + projection + ", " + memberOrdinalSQL + " AS " + memberOrdinalAlias +
		", " + sourceExistsAlias + " AS " + expandedExistsAlias +
		" FROM (" + guarded.sql + ") AS " + expandAlias + " ARRAY JOIN " +
		"arrayZip(" + selectedAlias + ", arrayEnumerate(" + selectedAlias +
		")) AS " + expandedPairAlias
	result := guarded.selectFrom(expandedSQL, operator.Range)

	next, err = extendCompileState(next, operator.Input, output, false)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	outputField := next.visible[operator.Input.Name]
	outputField.caseSensitive = field.caseSensitive
	next.visible[operator.Input.Name] = outputField
	next.mvExpandQueryRowsSQL = queryRowsAlias
	next.privateColumns = append(next.privateColumns, queryRowsAlias, memberOrdinalAlias, expandedExistsAlias)
	next.order = append(next.order, compiledSortKey{valueSQL: memberOrdinalAlias})
	next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: memberOrdinalAlias})
	state.context.mvExpandStages = operator.QueryOrdinal
	state.context.atomicResult = true
	state.context.requiresMaterializedValidationSettings = true
	return result, next, prefixArgs, nil
}
