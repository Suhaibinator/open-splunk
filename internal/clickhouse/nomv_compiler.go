package clickhouse

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func applyNoMultivaluePresentation(state compileState, name string) compileState {
	presented := state.visible[name]
	presented.flatMultivalueDelimiter = "\n"
	presented.hasFlatMultivalueDelimiter = true
	presented.statsSparkline = false
	state.visible[name] = presented
	return state
}

// nativeStringMVNeedsNoNomvValidation identifies String arrays constructed by
// the bounded native eval functions. Their eval projection has already forced
// the shared member, payload, and UTF-8 guards behind a materialized validation
// fence. The conjunction deliberately excludes makemv (which carries a text
// eligibility proof), copied makemv outputs, and legacy stats arrays (which do
// not carry the native eval presence sidecar).
func nativeStringMVNeedsNoNomvValidation(field fieldState) bool {
	return field.kind == fieldKindStringArray &&
		field.optionalMultivaluePresentSQL != "" &&
		strings.Contains(field.optionalMultivaluePresentSQL, "__os_eval_mv_state_") &&
		field.textEligibleSQL == "" &&
		!field.stringOrBytes
}

// compileNoMultivalue applies presentation metadata without changing a sealed
// native list.  An open-schema Dynamic input needs one normalization boundary
// first: only actual Array(String)/Array(Dynamic) values are admitted, and the
// physical Array(Dynamic) plus presence sidecar remain authoritative for every
// downstream SPL operator, API, export, and page.
func compileNoMultivalue(
	relation compiledRelation,
	operator *plan.NoMultivalue,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	if operator == nil || !spl.IsExactUnquotedFieldName(operator.Input.Name) ||
		strings.HasPrefix(operator.Input.Name, "_") {
		return compiledRelation{}, compileState{}, nil, errors.New(
			"compile ClickHouse nomv: operator is invalid",
		)
	}
	if err := validateCanonicalFieldRef("nomv", "input", operator.Input); err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	field, exists, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	if !exists {
		// A closed-schema missing field remains absent. There is no typed value
		// to annotate and nomv never creates a public column by itself.
		return relation, state, nil, nil
	}

	if field.kind == fieldKindStringArray {
		if nativeStringMVNeedsNoNomvValidation(field) {
			return relation, applyNoMultivaluePresentation(
				cloneCompileState(state),
				operator.Input.Name,
			), nil, nil
		}
		return compileNoMultivalueStringArray(
			relation,
			operator,
			field,
			state,
			stage,
		)
	}
	if field.kind == fieldKindDynamicArray {
		return relation, applyNoMultivaluePresentation(
			cloneCompileState(state),
			operator.Input.Name,
		), nil, nil
	}

	input := compiledScalarFromField(field)
	var normalized nativeMVState
	switch field.kind {
	case fieldKindDynamic, fieldKindInvalid:
		normalized, err = compileNativeMVState(input, false)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, err
		}
	default:
		// nomv is deliberately a runtime-validated presentation command. A
		// fixed scalar can still be nullable/missing on individual rows; retain
		// those rows and fail the complete search only when a scalar is present.
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		descendantSQL := field.descendantSQL
		if descendantSQL == "" {
			descendantSQL = "0"
		}
		invalid := "descendant_present != 0 OR (field_present != 0 AND isNotNull(value))"
		normalized.sql = bindSQLExpressions(
			[]string{"value", "field_present", "descendant_present"},
			[]string{field.valueSQL, "toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"},
			"tuple("+emptyNativeMVSQL()+", toUInt8(field_present != 0), "+
				"toUInt8(0), toUInt8("+invalid+"))",
		)
		normalized.args = append(normalized.args, input.valueArgs...)
		normalized.args = append(normalized.args, field.existsArgs...)
		normalized.args = append(normalized.args, field.descendantArgs...)
	}

	stateColumn := quoteIdentifier(fmt.Sprintf("__os_nomv_state_%d", stage))
	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv_prepared", stage))
	preparedSQL := "SELECT *, " + normalized.sql + " AS " + stateColumn +
		" FROM (" + relation.sql + ") AS " + preparedAlias
	prepared := relation.selectFrom(preparedSQL, operator.Range)
	stateAlias := "__os_nomv_state"
	values := nativeMVLimitsGuardSQL(
		"tupleElement("+stateAlias+", 1)",
		"tupleElement("+stateAlias+", 4) != 0",
	)
	valueSQL := bindSQLExpressions(
		[]string{stateAlias},
		[]string{stateColumn},
		values,
	)
	outputStateSQL := "tuple(toUInt8(tupleElement(" + stateColumn + ", 2) != 0), " +
		"toUInt8(tupleElement(" + stateColumn + ", 3) != 0))"
	outputStateAlias := quoteIdentifier(fmt.Sprintf("__os_nomv_output_state_%d", stage))
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv", stage))
	baseProjection := "* EXCEPT (" + stateColumn + ")"
	publication := upsertWildcardFieldProjection(
		baseProjection,
		state,
		operator.Input.Name,
		valueSQL,
		alias,
		authoredFieldPhysicallyPublic(state, operator.Input.Name),
	)
	projectedSQL := "SELECT " + publication + ", " + outputStateSQL + " AS " +
		outputStateAlias + " FROM (" + prepared.sql + ") AS " + alias
	validationInput := quoteIdentifier(fmt.Sprintf("__os_nomv_validation_%d", stage))
	validationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv_validation", stage))
	guardedSQL := "WITH " + validationInput + " AS MATERIALIZED (" + projectedSQL +
		") SELECT * FROM " + validationInput + " AS " + validationAlias +
		" WHERE ignore(" + quoteIdentifier(operator.Input.Name) + ") = 0"
	projected := prepared.selectFrom(guardedSQL, operator.Range)

	transportState := cloneCompileState(state)
	transportState.privateColumns = append(transportState.privateColumns, outputStateAlias)
	next, err := extendCompileState(
		transportState,
		operator.Input,
		compiledScalar{
			valueSQL:                     quoteIdentifier(operator.Input.Name),
			existsSQL:                    "tupleElement(" + outputStateAlias + ", 1) != 0",
			optionalMultivaluePresentSQL: "tupleElement(" + outputStateAlias + ", 2) != 0",
			kind:                         fieldKindDynamicArray,
			maxStringBytes:               fieldStateStringByteBound(field),
			materializeForPredicate:      field.materializeForPredicate,
		},
		false,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	output := next.visible[operator.Input.Name]
	output.caseSensitive = field.caseSensitive
	next.visible[operator.Input.Name] = output
	next = applyNoMultivaluePresentation(next, operator.Input.Name)
	markNativeMVRuntimeValidation(state)
	return projected, next, append([]any(nil), normalized.args...), nil
}

// compileNoMultivalueStringArray validates the legacy Array(String) domain
// without changing its physical kind. That preserves String-only downstream
// functions such as mvsort, while rejecting Bytes provenance and oversized
// stats list/values outputs before attaching native-MV presentation metadata.
func compileNoMultivalueStringArray(
	relation compiledRelation,
	operator *plan.NoMultivalue,
	field fieldState,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, error) {
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	descendantSQL := field.descendantSQL
	if descendantSQL == "" {
		descendantSQL = "0"
	}
	presentSQL := field.optionalMultivaluePresentSQL
	if presentSQL == "" {
		presentSQL = existsSQL
	}
	empty := "CAST([], 'Array(String)')"
	normalized := bindSQLExpressions(
		[]string{"value", "field_exists", "list_present", "descendant_present"},
		[]string{
			field.valueSQL,
			"toUInt8(" + existsSQL + ")",
			"toUInt8(" + presentSQL + ")",
			"toUInt8(" + descendantSQL + ")",
		},
		"tuple(if(list_present != 0, value, "+empty+"), "+
			"toUInt8(field_exists != 0), toUInt8(list_present != 0), "+
			"toUInt8(descendant_present != 0 OR "+
			"(list_present != 0 AND arrayExists(member -> NOT isValidUTF8(member), value))))",
	)
	prefixArgs := append([]any(nil), field.existsArgs...)
	if presentSQL == existsSQL {
		prefixArgs = append(prefixArgs, field.existsArgs...)
	}
	prefixArgs = append(prefixArgs, field.descendantArgs...)

	stateColumn := quoteIdentifier(fmt.Sprintf("__os_nomv_string_state_%d", stage))
	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv_string_prepared", stage))
	preparedSQL := "SELECT *, " + normalized + " AS " + stateColumn +
		" FROM (" + relation.sql + ") AS " + preparedAlias
	prepared := relation.selectFrom(preparedSQL, operator.Range)
	values := stringMVLimitsGuardSQL(
		"tupleElement("+stateColumn+", 1)",
		"tupleElement("+stateColumn+", 4) != 0",
	)
	outputStateAlias := quoteIdentifier(fmt.Sprintf("__os_nomv_output_state_%d", stage))
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv", stage))
	baseProjection := "* EXCEPT (" + stateColumn + ")"
	publication := upsertWildcardFieldProjection(
		baseProjection,
		state,
		operator.Input.Name,
		values,
		alias,
		authoredFieldPhysicallyPublic(state, operator.Input.Name),
	)
	projectedSQL := "SELECT " + publication + ", tuple(toUInt8(tupleElement(" +
		stateColumn + ", 2) != 0), toUInt8(tupleElement(" + stateColumn +
		", 3) != 0)) AS " + outputStateAlias + " FROM (" +
		prepared.sql + ") AS " + alias
	validationInput := quoteIdentifier(fmt.Sprintf("__os_nomv_validation_%d", stage))
	validationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_nomv_validation", stage))
	guardedSQL := "WITH " + validationInput + " AS MATERIALIZED (" + projectedSQL +
		") SELECT * FROM " + validationInput + " AS " + validationAlias +
		" WHERE ignore(" + quoteIdentifier(operator.Input.Name) + ") = 0"
	projected := prepared.selectFrom(guardedSQL, operator.Range)

	transportState := cloneCompileState(state)
	transportState.privateColumns = append(transportState.privateColumns, outputStateAlias)
	outputName := quoteIdentifier(operator.Input.Name)
	next, err := extendCompileState(
		transportState,
		operator.Input,
		compiledScalar{
			valueSQL:                     outputName,
			existsSQL:                    "tupleElement(" + outputStateAlias + ", 1) != 0",
			optionalMultivaluePresentSQL: "tupleElement(" + outputStateAlias + ", 2) != 0",
			textEligibleSQL:              "arrayAll(member -> isValidUTF8(member), " + outputName + ")",
			kind:                         fieldKindStringArray,
			maxStringBytes:               fieldStateStringByteBound(field),
			mvSortedLexicographic:        field.mvSortedLexicographic,
			materializeForPredicate:      field.materializeForPredicate,
		},
		false,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, err
	}
	next = applyNoMultivaluePresentation(next, operator.Input.Name)
	markNativeMVRuntimeValidation(state)
	return projected, next, prefixArgs, nil
}
