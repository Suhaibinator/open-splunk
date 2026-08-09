package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const maxCompiledKnowledgeRuntimeGuardSQLBytes = maxCompiledQueryBytes

const (
	knowledgeRuntimeGuardInputName        = `"__os_ko_guard_input"`
	knowledgeRuntimeGuardInputAlias       = `"__os_ko_guard_event"`
	knowledgeRuntimeGuardTotalsName       = `"__os_ko_guard_totals"`
	knowledgeRuntimeGuardTotalsAlias      = `"__os_ko_guard_total"`
	knowledgeRuntimeGuardViolationColumn  = `"__os_ko_guard_violation"`
	knowledgeRuntimeGuardValidationColumn = `"__os_ko_guard_validation"`
	knowledgeRuntimeGuardResultName       = `"__os_ko_guard_result"`
)

type compiledKnowledgeRuntimeGuardExpressions struct {
	violation  string
	validation string
}

// compiledKnowledgeRuntimeGuard is a pure relation wrapper. suffixArgs is
// intentionally always empty: selector and capture limits are compile-time
// constants, while every argument already owned by the input relation keeps
// its existing position.
type compiledKnowledgeRuntimeGuard struct {
	relation   compiledRelation
	state      compileState
	suffixArgs []any
}

// compileKnowledgeRuntimeGuard consumes the per-row accounting columns
// produced by a nonempty knowledge prelude. It materializes the completed
// event relation once, derives whole-event and whole-query maxima/totals, and
// republishes the event columns only after the ordered guards have passed.
//
// This helper is deliberately not wired into central compilation yet. The
// caller must keep nonempty knowledge finalization closed until the pinned
// ClickHouse compatibility matrix proves this relation can safely precede all
// authored suffix shapes. The later central composer must also construct
// relation directly from prelude.stages; accepting the relation separately
// here does not by itself prove that it contains those physical projections.
func compileKnowledgeRuntimeGuard(
	relation compiledRelation,
	prelude compiledKnowledgePrelude,
	preparation preparedKnowledgeCompilation,
) (compiledKnowledgeRuntimeGuard, error) {
	if err := validateKnowledgeRuntimeGuardRelation(relation); err != nil {
		return compiledKnowledgeRuntimeGuard{}, err
	}
	if err := validateKnowledgePreludePreparation(preparation); err != nil {
		return compiledKnowledgeRuntimeGuard{}, err
	}
	if !preparation.present || preparation.program.IsEmpty() {
		if err := validateKnowledgeRuntimeGuardIdentityPrelude(prelude, preparation); err != nil {
			return compiledKnowledgeRuntimeGuard{}, err
		}
		return compiledKnowledgeRuntimeGuard{
			relation: relation,
			state:    cloneCompileState(prelude.state),
		}, nil
	}
	if err := validateCompiledKnowledgePrelude(prelude, preparation); err != nil {
		return compiledKnowledgeRuntimeGuard{}, err
	}
	if err := validateKnowledgeRuntimeGuardInput(prelude); err != nil {
		return compiledKnowledgeRuntimeGuard{}, err
	}
	selectorCharges := prelude.selectorCharges
	expressions := compileKnowledgeRuntimeGuardExpressions(prelude)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 2048)
	sql.WriteString("WITH ")
	sql.WriteString(knowledgeRuntimeGuardInputName)
	sql.WriteString(" AS MATERIALIZED (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")
	sql.WriteString(knowledgeRuntimeGuardTotalsName)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(expressions.violation)
	sql.WriteString(" AS ")
	sql.WriteString(knowledgeRuntimeGuardViolationColumn)
	sql.WriteString(" FROM ")
	sql.WriteString(knowledgeRuntimeGuardInputName)
	sql.WriteString(") SELECT ")
	sql.WriteString("* EXCEPT (")
	sql.WriteString(selectorCharges.inputBytes)
	sql.WriteString(", ")
	sql.WriteString(selectorCharges.queryUnits)
	sql.WriteString(", ")
	sql.WriteString(knowledgeRuntimeGuardViolationColumn)
	sql.WriteString(")")
	sql.WriteString(" FROM ")
	sql.WriteString(knowledgeRuntimeGuardInputName)
	sql.WriteString(" AS ")
	sql.WriteString(knowledgeRuntimeGuardInputAlias)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(knowledgeRuntimeGuardTotalsName)
	sql.WriteString(" AS ")
	sql.WriteString(knowledgeRuntimeGuardTotalsAlias)
	sql.WriteString(" WHERE ")
	sql.WriteString(expressions.validation)
	sql.WriteString(" = 0")
	sql.WriteString(materializedCTESettingsSQL)
	guardedSQL := sql.String()
	if len(guardedSQL) > maxCompiledKnowledgeRuntimeGuardSQLBytes {
		return compiledKnowledgeRuntimeGuard{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"compiled knowledge runtime guard exceeds %d bytes",
				maxCompiledKnowledgeRuntimeGuardSQLBytes,
			),
			Range: relation.ownerRange,
		}
	}
	if strings.Count(guardedSQL, "?") != strings.Count(relation.sql, "?") {
		return compiledKnowledgeRuntimeGuard{}, errors.New(
			"compile ClickHouse knowledge runtime guard: wrapper introduced a placeholder",
		)
	}

	aggregateDepth := relationalNodeDepth(relation.depth)
	guardedDepth := relationalNodeDepth(relation.depth, aggregateDepth)
	if err := validateRelationalDepth(guardedDepth, relation.ownerRange); err != nil {
		return compiledKnowledgeRuntimeGuard{}, err
	}
	return compiledKnowledgeRuntimeGuard{
		relation: compiledRelation{
			sql:        guardedSQL,
			depth:      guardedDepth,
			ownerRange: relation.ownerRange,
		},
		state: cloneCompileState(prelude.state),
	}, nil
}

func compileKnowledgeRuntimeGuardExpressions(
	prelude compiledKnowledgePrelude,
) compiledKnowledgeRuntimeGuardExpressions {
	selectorCharges := prelude.selectorCharges
	selectorEventMaximum := "maxOrDefault(toUInt128(" + selectorCharges.inputBytes + "))"
	selectorQueryTotal := "sum(toUInt128(" + selectorCharges.queryUnits + "))"
	selectorEventOver := selectorEventMaximum + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeEventBytes) + ")"
	selectorQueryOver := selectorQueryTotal + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeQueryUnits) + ")"
	violation := "multiIf(" + selectorEventOver + ", toUInt8(1), "
	if prelude.capturedBytes != "" {
		rexMaximum := "maxOrDefault(toUInt128(" + prelude.capturedBytes + "))"
		rexOver := rexMaximum + " > toUInt128(" +
			strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + ")"
		violation += rexOver + ", toUInt8(2), "
	}
	violation += selectorQueryOver + ", toUInt8(3), toUInt8(0))"

	violationRef := knowledgeRuntimeGuardTotalsAlias + "." +
		knowledgeRuntimeGuardViolationColumn
	eventViolation := violationRef + " = toUInt8(1)"
	queryViolation := violationRef + " = toUInt8(3)"
	validation := "if(" + eventViolation + ", " +
		knowledgeRuntimeGuardThrow(eventViolation, KnowledgeSelectorEventLimitMarker) + ", "
	if prelude.capturedBytes != "" {
		rexViolation := violationRef + " = toUInt8(2)"
		validation += "if(" + rexViolation + ", " +
			knowledgeRuntimeGuardThrow(rexViolation, RexCaptureLimitMarker) + ", " +
			knowledgeRuntimeGuardThrow(queryViolation, KnowledgeSelectorQueryLimitMarker) + ")"
	} else {
		validation += knowledgeRuntimeGuardThrow(
			queryViolation,
			KnowledgeSelectorQueryLimitMarker,
		)
	}
	validation += ")"
	return compiledKnowledgeRuntimeGuardExpressions{
		violation:  violation,
		validation: validation,
	}
}

func validateKnowledgeRuntimeGuardInput(
	prelude compiledKnowledgePrelude,
) error {
	state := prelude.state
	if err := validateKnowledgeExtractionInputState(state); err != nil {
		return fmt.Errorf(
			"compile ClickHouse knowledge runtime guard input: %w",
			err,
		)
	}
	if knowledgeRuntimeGuardStateHasDeferredAnalysis(state) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: input contains deferred analysis state",
		)
	}
	lastStage := prelude.stages[len(prelude.stages)-1]
	if !validKnowledgeRuntimeGuardSelectorChargePair(
		prelude.selectorCharges,
		lastStage.operatorOffset,
	) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		lastStage.projection,
		prelude.selectorCharges.inputBytes,
	) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		lastStage.projection,
		prelude.selectorCharges.queryUnits,
	) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: selector charge columns are invalid",
		)
	}
	if prelude.capturedBytes != state.rexCapturedBytesSQL ||
		prelude.capturedBytes != "" && !validKnowledgeRuntimeGuardGeneratedColumn(
			prelude.capturedBytes,
			`"__os_ko_rex_captured_bytes_`,
		) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: capture accounting disagrees with state",
		)
	}
	if prelude.capturedBytes == prelude.selectorCharges.inputBytes ||
		prelude.capturedBytes == prelude.selectorCharges.queryUnits {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: accounting columns overlap",
		)
	}

	if knowledgeRuntimeGuardStateLeaksAccounting(
		state,
		prelude.selectorCharges.inputBytes,
		prelude.selectorCharges.queryUnits,
		prelude.capturedBytes,
		knowledgeRuntimeGuardViolationColumn,
		knowledgeRuntimeGuardValidationColumn,
	) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: accounting entered event state",
		)
	}
	return nil
}

func validateKnowledgeRuntimeGuardRelation(relation compiledRelation) error {
	if relation.sql == "" || relation.depth <= 0 {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: input relation is invalid",
		)
	}
	if len(relation.sql) > maxCompiledKnowledgeRuntimeGuardSQLBytes {
		return &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"compiled knowledge runtime guard input exceeds %d bytes",
				maxCompiledKnowledgeRuntimeGuardSQLBytes,
			),
			Range: relation.ownerRange,
		}
	}
	return validateRelationalDepth(relation.depth, relation.ownerRange)
}

func knowledgeRuntimeGuardStateLeaksAccounting(
	state compileState,
	columns ...string,
) bool {
	references := func(sql string) bool {
		if sql == "" {
			return false
		}
		for _, column := range columns {
			if column != "" && strings.Contains(sql, column) {
				return true
			}
		}
		return false
	}
	for name, field := range state.visible {
		if references(quoteIdentifier(name)) || references(field.valueSQL) ||
			references(field.exactNumericKeySQL) ||
			references(field.dynamicNumericEligibleSQL) ||
			references(field.textEligibleSQL) || references(field.dynamicTypeSQL) ||
			references(field.storedTypeSQL) || references(field.existsSQL) ||
			references(field.descendantSQL) ||
			references(field.relativeFieldNamesSQL) ||
			references(field.relativeFieldTypesSQL) ||
			references(field.fieldMetadataVersionSQL) {
			return true
		}
	}
	for _, name := range state.publicOrder {
		if references(quoteIdentifier(name)) {
			return true
		}
	}
	for _, column := range state.privateColumns {
		if references(column) {
			return true
		}
	}
	for _, key := range append(
		append([]compiledSortKey(nil), state.order...),
		state.tieBreakers...,
	) {
		if references(key.valueSQL) {
			return true
		}
	}
	return false
}

func validateKnowledgeRuntimeGuardIdentityPrelude(
	prelude compiledKnowledgePrelude,
	preparation preparedKnowledgeCompilation,
) error {
	if prelude.present != preparation.present || prelude.prefixLength != 0 ||
		len(prelude.stages) != 0 ||
		prelude.selectorCharges != (compiledKnowledgeSelectorChargeColumns{}) ||
		prelude.capturedBytes != "" || len(prelude.proof.operatorKinds) != 0 ||
		len(prelude.proof.extractions) != 0 || len(prelude.proof.aliases) != 0 ||
		len(prelude.proof.calculated) != 0 || prelude.proof.objectCount != 0 ||
		prelude.proof.charges != (knowledgeprogram.Charges{}) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: identity prelude contains authority",
		)
	}
	if !preparation.present {
		return nil
	}
	if !preparation.program.IsEmpty() {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: nonempty prelude cannot be an identity",
		)
	}
	if err := validateKnowledgeExtractionInputState(prelude.state); err != nil {
		return fmt.Errorf(
			"compile ClickHouse knowledge runtime guard empty input: %w",
			err,
		)
	}
	if prelude.state.rexCapturedBytesSQL != "" ||
		knowledgeRuntimeGuardStateHasDeferredAnalysis(prelude.state) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: empty prelude state is invalid",
		)
	}
	return nil
}

func knowledgeRuntimeGuardStateHasDeferredAnalysis(state compileState) bool {
	return len(state.preAggregateValidationColumns) != 0 ||
		len(state.preAggregateValidationArgs) != 0 ||
		len(state.preAggregateColumns) != 0 ||
		len(state.preAggregateArgs) != 0 ||
		len(state.preAggregateListWindowColumns) != 0 ||
		len(state.preAggregateListCandidateColumns) != 0 ||
		len(state.postAggregateChronological) != 0 ||
		len(state.postAggregateScalarExtrema) != 0 ||
		len(state.postAggregateExactStrings) != 0 ||
		len(state.postAggregateDistinctCounts) != 0 ||
		len(state.postAggregateOrderedStrings) != 0 ||
		len(state.chronologicalBarriers) != 0
}

func validKnowledgeRuntimeGuardSelectorChargePair(
	charges compiledKnowledgeSelectorChargeColumns,
	expectedStage int,
) bool {
	const inputPrefix = `"__os_ko_selector_input_bytes_`
	if !validKnowledgeRuntimeGuardGeneratedColumn(charges.inputBytes, inputPrefix) {
		return false
	}
	stage := strings.TrimSuffix(strings.TrimPrefix(charges.inputBytes, inputPrefix), `"`)
	return stage == strconv.Itoa(expectedStage) &&
		charges.queryUnits == `"__os_ko_selector_query_units_`+stage+`"`
}

func knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
	projection []string,
	alias string,
) bool {
	suffix := " AS " + alias
	count := 0
	for _, expression := range projection {
		if strings.HasSuffix(expression, suffix) {
			count++
		}
	}
	return count == 1
}

func validKnowledgeRuntimeGuardGeneratedColumn(column, prefix string) bool {
	if !strings.HasPrefix(column, prefix) || !strings.HasSuffix(column, `"`) {
		return false
	}
	ordinal := strings.TrimSuffix(strings.TrimPrefix(column, prefix), `"`)
	parsed, err := strconv.ParseUint(ordinal, 10, 31)
	return err == nil && strconv.FormatUint(parsed, 10) == ordinal
}

func knowledgeRuntimeGuardThrow(condition, marker string) string {
	return "throwIf(toUInt8(" + condition + "), '" + marker + "')"
}
