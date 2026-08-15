package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// This file holds the test-only inline knowledge-relation compatibility
// oracle. Production compilation goes through compileKnowledgeStageRelation
// and the deferred top-level barrier in knowledge_deferred_relation.go; the
// inline whole-query CROSS JOIN form below exists only so the tests can assert
// the staged prelude, the runtime guard shape, and the production validators
// against an independently derived expectation.

const (
	knowledgeRuntimeGuardTotalsName  = `"__os_ko_guard_totals"`
	knowledgeRuntimeGuardTotalsAlias = `"__os_ko_guard_total"`
)

// compileKnowledgeRelation compiles the immutable prelude from the exact Scan
// state, applies every relation-neutral stage as two SELECT levels, and closes
// nonempty work behind the runtime guard. Production compiler.go uses the
// deferred top-level form instead; this independent inline form remains a
// compatibility and invariant oracle for the same compiler-minted authority.
func compileKnowledgeRelation(
	relation compiledRelation,
	scanState compileState,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) (compiledKnowledgeStageRelation, error) {
	staged, err := compileKnowledgeStageRelation(
		relation,
		scanState,
		existingArgs,
		preparation,
	)
	if err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	guarded, err := compileKnowledgeRuntimeGuard(
		staged.relation,
		staged.prelude,
		preparation,
	)
	if err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	if len(guarded.suffixArgs) != 0 {
		return compiledKnowledgeStageRelation{}, errors.New(
			"compile ClickHouse knowledge relation: runtime guard introduced arguments",
		)
	}
	if strings.Count(guarded.relation.sql, "?") != len(staged.args) {
		return compiledKnowledgeStageRelation{}, errors.New(
			"compile ClickHouse knowledge relation: final placeholder order is invalid",
		)
	}
	result := compiledKnowledgeStageRelation{
		relation: guarded.relation,
		state:    guarded.state,
		args:     staged.args,
		prelude:  staged.prelude,
	}
	if err := validateCompiledKnowledgeRelation(
		result,
		relation,
		existingArgs,
		preparation,
	); err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	return result, nil
}

// validateCompiledKnowledgeRelation is the guarded specialization of the
// shared staged-relation core: the runtime guard adds one aggregate level and
// one republishing level on top of the staged prefix.
func validateCompiledKnowledgeRelation(
	compiled compiledKnowledgeStageRelation,
	input compiledRelation,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) error {
	return validateCompiledKnowledgeStageRelationCore(
		compiled,
		input,
		existingArgs,
		preparation,
		2,
		"compile ClickHouse knowledge relation: identity relation changed",
		"compile ClickHouse knowledge relation: final relational depth disagrees",
	)
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
// This helper is the test-only inline compatibility oracle used by
// compileKnowledgeRelation below. Production central compilation uses the
// deferred top-level barrier built from the same staged prelude authority. A
// caller must still construct relation directly from prelude.stages; accepting
// a relation separately here does not prove that it contains those physical
// projections.
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
	accountingColumns := knowledgeRuntimeGuardAccountingColumns(prelude)
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
	sql.WriteString(strings.Join(accountingColumns, ", "))
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
	return compileKnowledgeRuntimeGuardExpressionsWithAggregates(
		prelude,
		func(column string) string {
			return "maxOrDefault(toUInt128(" + column + "))"
		},
		func(column string) string {
			return "sum(toUInt128(" + column + "))"
		},
		knowledgeRuntimeGuardTotalsAlias+"."+
			knowledgeRuntimeGuardViolationColumn,
	)
}

func compileKnowledgeRuntimeGuardExpressionsWithAggregates(
	prelude compiledKnowledgePrelude,
	maximum func(string) string,
	total func(string) string,
	violationRef string,
) compiledKnowledgeRuntimeGuardExpressions {
	selectorCharges := prelude.selectorCharges
	selectorEventMaximum := maximum(selectorCharges.inputBytes)
	selectorQueryTotal := total(selectorCharges.queryUnits)
	selectorEventOver := selectorEventMaximum + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeEventBytes) + ")"
	selectorQueryOver := selectorQueryTotal + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeQueryUnits) + ")"
	violations := []compiledKnowledgeRuntimeGuardViolation{{
		over: selectorEventOver, code: 1, marker: KnowledgeSelectorEventLimitMarker,
	}}
	if prelude.capturedBytes != "" {
		rexMaximum := maximum(prelude.capturedBytes)
		rexOver := rexMaximum + " > toUInt128(" +
			strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + ")"
		violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
			over: rexOver, code: 2, marker: RexCaptureLimitMarker,
		})
	}
	if prelude.aliasCopyCharges.eventBytes != "" {
		aliasEventMaximum := maximum(prelude.aliasCopyCharges.eventBytes)
		aliasEventOver := aliasEventMaximum + " > toUInt128(" +
			strconv.FormatUint(knowledge.MaximumAliasCopyRuntimeEventBytes, 10) + ")"
		violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
			over: aliasEventOver, code: 4, marker: KnowledgeAliasCopyEventLimitMarker,
		})
	}
	violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
		over: selectorQueryOver, code: 3, marker: KnowledgeSelectorQueryLimitMarker,
	})
	if prelude.aliasCopyCharges.queryUnits != "" {
		aliasQueryTotal := total(prelude.aliasCopyCharges.queryUnits)
		aliasQueryOver := aliasQueryTotal + " > toUInt128(" +
			strconv.FormatUint(knowledge.MaximumAliasCopyRuntimeQueryUnits, 10) + ")"
		violations = append(violations, compiledKnowledgeRuntimeGuardViolation{
			over: aliasQueryOver, code: 5, marker: KnowledgeAliasCopyQueryLimitMarker,
		})
	}
	return compileKnowledgeRuntimeGuardPrecedence(violations, violationRef)
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
