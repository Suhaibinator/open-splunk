package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

const maxCompiledKnowledgeRuntimeGuardSQLBytes = maxCompiledQueryBytes

const (
	KnowledgeAliasCopyEventLimitMarker = "open-splunk: knowledge alias copy exceeds the per-event limit"
	KnowledgeAliasCopyQueryLimitMarker = "open-splunk: knowledge alias copy work exceeds the per-query limit"
)

const (
	knowledgeRuntimeGuardInputName        = `"__os_ko_guard_input"`
	knowledgeRuntimeGuardInputAlias       = `"__os_ko_guard_event"`
	knowledgeRuntimeGuardViolationColumn  = `"__os_ko_guard_violation"`
	knowledgeRuntimeGuardValidationColumn = `"__os_ko_guard_validation"`
	knowledgeRuntimeGuardResultName       = `"__os_ko_guard_result"`
)

type compiledKnowledgeRuntimeGuardExpressions struct {
	violation  string
	validation string
}

type compiledKnowledgeRuntimeGuardViolation struct {
	over   string
	code   uint8
	marker string
}

// compileKnowledgeRuntimeWindowGuardExpressions derives the ordered per-event
// and per-query limits, and publishes the global aggregates beside every
// staged event. The deferred compiler can therefore keep one materialized
// staged input and one ordinary guarded result instead of exposing separate
// totals and CROSS JOIN nodes to every suffix consumer.
func compileKnowledgeRuntimeWindowGuardExpressions(
	prelude compiledKnowledgePrelude,
) compiledKnowledgeRuntimeGuardExpressions {
	// Bind one fixed-width evidence vector. Per-event limits contribute a local
	// zero/one witness; query limits contribute their UInt128 row charge. One
	// sumForEach window therefore preserves global category precedence without
	// making ClickHouse substitute the complete staged input once per maximum
	// and total aggregate.
	const evidence = "__os_ko_guard_evidence"
	vector := make([]string, 0, 5)
	violations := make([]compiledKnowledgeRuntimeGuardViolation, 0, 5)
	appendEventViolation := func(column string, limit uint64, code uint8, marker string) {
		limitSQL := strconv.FormatUint(limit, 10)
		vector = append(
			vector,
			"toUInt128(toUInt128("+column+") > toUInt128("+limitSQL+"))",
		)
		violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
			over: "toUInt128(arrayElement(" + evidence + ", " +
				strconv.Itoa(len(vector)) + ")) > toUInt128(0)",
			code:   code,
			marker: marker,
		})
	}
	appendQueryViolation := func(column string, limit uint64, code uint8, marker string) {
		vector = append(vector, "toUInt128("+column+")")
		violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
			over: "toUInt128(arrayElement(" + evidence + ", " +
				strconv.Itoa(len(vector)) + ")) > toUInt128(" +
				strconv.FormatUint(limit, 10) + ")",
			code:   code,
			marker: marker,
		})
	}

	appendEventViolation(
		prelude.selectorCharges.inputBytes,
		uint64(knowledge.MaximumSelectorRuntimeEventBytes),
		1,
		KnowledgeSelectorEventLimitMarker,
	)
	if prelude.capturedBytes != "" {
		appendEventViolation(
			prelude.capturedBytes,
			MaximumRexCapturedBytesPerRow,
			2,
			RexCaptureLimitMarker,
		)
	}
	if prelude.aliasCopyCharges.eventBytes != "" {
		appendEventViolation(
			prelude.aliasCopyCharges.eventBytes,
			knowledge.MaximumAliasCopyRuntimeEventBytes,
			4,
			KnowledgeAliasCopyEventLimitMarker,
		)
	}
	appendQueryViolation(
		prelude.selectorCharges.queryUnits,
		uint64(knowledge.MaximumSelectorRuntimeQueryUnits),
		3,
		KnowledgeSelectorQueryLimitMarker,
	)
	if prelude.aliasCopyCharges.queryUnits != "" {
		appendQueryViolation(
			prelude.aliasCopyCharges.queryUnits,
			knowledge.MaximumAliasCopyRuntimeQueryUnits,
			5,
			KnowledgeAliasCopyQueryLimitMarker,
		)
	}

	expressions := compileKnowledgeRuntimeGuardPrecedence(
		violations,
		knowledgeRuntimeGuardInputAlias+"."+
			knowledgeRuntimeGuardViolationColumn,
	)
	expressions.violation = bindSQLExpressions(
		[]string{evidence},
		[]string{"sumForEach([" + strings.Join(vector, ", ") + "]) OVER ()"},
		expressions.violation,
	)
	return expressions
}

func compileKnowledgeRuntimeGuardPrecedence(
	violations []compiledKnowledgeRuntimeGuardViolation,
	violationRef string,
) compiledKnowledgeRuntimeGuardExpressions {
	var violation strings.Builder
	violation.WriteString("multiIf(")
	for _, candidate := range violations {
		violation.WriteString(candidate.over)
		violation.WriteString(", toUInt8(")
		violation.WriteString(strconv.Itoa(int(candidate.code)))
		violation.WriteString("), ")
	}
	violation.WriteString("toUInt8(0))")

	last := violations[len(violations)-1]
	lastCondition := violationRef + " = toUInt8(" + strconv.Itoa(int(last.code)) + ")"
	validation := knowledgeRuntimeGuardThrow(lastCondition, last.marker)
	for index := len(violations) - 2; index >= 0; index-- {
		candidate := violations[index]
		condition := violationRef + " = toUInt8(" + strconv.Itoa(int(candidate.code)) + ")"
		validation = "if(" + condition + ", " +
			knowledgeRuntimeGuardThrow(condition, candidate.marker) + ", " + validation + ")"
	}
	return compiledKnowledgeRuntimeGuardExpressions{
		violation:  violation.String(),
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
	selectorOffset := -1
	var selectorProjection []string
	if prelude.automaticLookup != nil {
		selectorOffset = prelude.automaticLookup.operatorOffset
		selectorProjection = prelude.automaticLookup.finalProjection
	} else if len(prelude.stages) != 0 {
		lastStage := prelude.stages[len(prelude.stages)-1]
		selectorOffset = lastStage.operatorOffset
		selectorProjection = lastStage.projection
	}
	if !validKnowledgeRuntimeGuardSelectorChargePair(
		prelude.selectorCharges,
		selectorOffset,
	) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		selectorProjection,
		prelude.selectorCharges.inputBytes,
	) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		selectorProjection,
		prelude.selectorCharges.queryUnits,
	) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: selector charge columns are invalid",
		)
	}
	hasAliases := len(prelude.proof.aliases) != 0
	if hasAliases {
		lastStage := prelude.stages[len(prelude.stages)-1]
		if !validKnowledgeRuntimeGuardAliasCopyChargePair(
			prelude.aliasCopyCharges,
			lastStage.operatorOffset,
		) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
			lastStage.projection,
			prelude.aliasCopyCharges.eventBytes,
		) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
			lastStage.projection,
			prelude.aliasCopyCharges.queryUnits,
		) {
			return errors.New(
				"compile ClickHouse knowledge runtime guard: alias copy charge columns are invalid",
			)
		}
	} else if prelude.aliasCopyCharges != (compiledKnowledgeAliasCopyChargeColumns{}) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: alias copy charges exist without aliases",
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
		prelude.capturedBytes == prelude.selectorCharges.queryUnits ||
		prelude.capturedBytes != "" &&
			(prelude.capturedBytes == prelude.aliasCopyCharges.eventBytes ||
				prelude.capturedBytes == prelude.aliasCopyCharges.queryUnits) ||
		prelude.aliasCopyCharges.eventBytes != "" &&
			(prelude.aliasCopyCharges.eventBytes == prelude.selectorCharges.inputBytes ||
				prelude.aliasCopyCharges.eventBytes == prelude.selectorCharges.queryUnits ||
				prelude.aliasCopyCharges.eventBytes == prelude.aliasCopyCharges.queryUnits) ||
		prelude.aliasCopyCharges.queryUnits != "" &&
			(prelude.aliasCopyCharges.queryUnits == prelude.selectorCharges.inputBytes ||
				prelude.aliasCopyCharges.queryUnits == prelude.selectorCharges.queryUnits) {
		return errors.New(
			"compile ClickHouse knowledge runtime guard: accounting columns overlap",
		)
	}

	if knowledgeRuntimeGuardStateLeaksAccounting(
		state,
		prelude.selectorCharges.inputBytes,
		prelude.selectorCharges.queryUnits,
		prelude.aliasCopyCharges.eventBytes,
		prelude.aliasCopyCharges.queryUnits,
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
	if slices.ContainsFunc(state.privateColumns, references) {
		return true
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
		prelude.aliasCopyCharges != (compiledKnowledgeAliasCopyChargeColumns{}) ||
		prelude.capturedBytes != "" || len(prelude.proof.operatorKinds) != 0 ||
		len(prelude.proof.extractions) != 0 || len(prelude.proof.aliases) != 0 ||
		len(prelude.proof.calculated) != 0 || prelude.proof.objectCount != 0 ||
		prelude.proof.charges != (knowledgeprogram.Charges{}) ||
		prelude.automaticLookup != nil {
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

func knowledgeRuntimeGuardAccountingColumns(
	prelude compiledKnowledgePrelude,
) []string {
	columns := []string{
		prelude.selectorCharges.inputBytes,
		prelude.selectorCharges.queryUnits,
	}
	if prelude.aliasCopyCharges.eventBytes != "" {
		columns = append(
			columns,
			prelude.aliasCopyCharges.eventBytes,
			prelude.aliasCopyCharges.queryUnits,
		)
	}
	return columns
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

func validKnowledgeRuntimeGuardAliasCopyChargePair(
	charges compiledKnowledgeAliasCopyChargeColumns,
	expectedStage int,
) bool {
	const eventPrefix = `"__os_ko_alias_copy_bytes_`
	if !validKnowledgeRuntimeGuardGeneratedColumn(charges.eventBytes, eventPrefix) {
		return false
	}
	stage := strings.TrimSuffix(strings.TrimPrefix(charges.eventBytes, eventPrefix), `"`)
	return stage == strconv.Itoa(expectedStage) &&
		charges.queryUnits == `"__os_ko_alias_copy_units_`+stage+`"`
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
