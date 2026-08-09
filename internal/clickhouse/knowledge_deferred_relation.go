package clickhouse

import (
	"errors"
	"reflect"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// compiledDeferredKnowledgeRelation moves a nonempty generated prefix into
// the compiler's existing flat, top-level validation graph. The barrier owns
// every argument used by the staged Scan relation; args therefore contains
// only still-active authored suffix arguments and is empty at this boundary.
// Identity preludes preserve their detached input arguments and add no graph.
type compiledDeferredKnowledgeRelation struct {
	relation compiledRelation
	state    compileState
	args     []any
	prelude  compiledKnowledgePrelude
}

// compileDeferredKnowledgeRelation lowers the generated prefix into the
// central compiler's flat validation graph so later authored CTEs never bury
// its MATERIALIZED input. Runtime finalization remains closed until the
// digest-pinned ClickHouse matrix proves this graph alongside every supported
// suffix shape.
func compileDeferredKnowledgeRelation(
	relation compiledRelation,
	scanState compileState,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) (compiledDeferredKnowledgeRelation, error) {
	staged, err := compileKnowledgeStageRelation(
		relation,
		scanState,
		existingArgs,
		preparation,
	)
	if err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	if !preparation.present || preparation.program.IsEmpty() {
		result := compiledDeferredKnowledgeRelation{
			relation: staged.relation,
			state:    cloneCompileState(staged.state),
			args:     staged.args,
			prelude:  staged.prelude,
		}
		if err := validateCompiledDeferredKnowledgeRelation(
			result,
			relation,
			existingArgs,
			preparation,
		); err != nil {
			return compiledDeferredKnowledgeRelation{}, err
		}
		return result, nil
	}
	if err := validateKnowledgeRuntimeGuardInput(staged.prelude); err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}

	expressions := compileKnowledgeRuntimeGuardExpressions(staged.prelude)
	accountingColumns := knowledgeRuntimeGuardAccountingColumns(staged.prelude)
	inputDefinition := knowledgeRuntimeGuardInputName + " AS MATERIALIZED (" +
		staged.relation.sql + ")"
	totalsDefinition := knowledgeRuntimeGuardTotalsName + " AS (SELECT " +
		expressions.violation + " AS " + knowledgeRuntimeGuardViolationColumn +
		" FROM " + knowledgeRuntimeGuardInputName + ")"
	barrierSQL := "SELECT * EXCEPT (" + strings.Join(accountingColumns, ", ") +
		", " + knowledgeRuntimeGuardViolationColumn +
		"), toUInt8(" + expressions.validation + ") AS " +
		knowledgeRuntimeGuardValidationColumn + " FROM " +
		knowledgeRuntimeGuardInputName + " AS " + knowledgeRuntimeGuardInputAlias +
		" CROSS JOIN " + knowledgeRuntimeGuardTotalsName + " AS " +
		knowledgeRuntimeGuardTotalsAlias

	barrierArgs, err := cloneKnowledgeRelationArguments(staged.args)
	if err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	totalsDepth := relationalNodeDepth(staged.relation.depth)
	barrierDepth := relationalNodeDepth(staged.relation.depth, totalsDepth)
	if err := validateRelationalDepth(barrierDepth, relation.ownerRange); err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	barrier := compiledChronologicalBarrier{
		name: knowledgeRuntimeGuardResultName,
		prerequisiteDefinitions: []string{
			inputDefinition,
			totalsDefinition,
		},
		sql:               barrierSQL,
		args:              barrierArgs,
		validationColumns: []string{knowledgeRuntimeGuardValidationColumn},
		fanout:            2,
		depth:             barrierDepth,
		ownerRange:        relation.ownerRange,
	}
	next := cloneCompileState(staged.state)
	next.chronologicalBarriers = append(next.chronologicalBarriers, barrier)

	publishedAlias := `"__os_ko_guard_published"`
	publishedSQL := "SELECT * EXCEPT (" +
		knowledgeRuntimeGuardValidationColumn + ") FROM " +
		knowledgeRuntimeGuardResultName + " AS " + publishedAlias
	published := compiledRelation{
		sql:        publishedSQL,
		depth:      relationalNodeDepth(barrierDepth),
		ownerRange: relation.ownerRange,
	}
	if err := validateDeferredKnowledgeDescriptorSize(barrier, published); err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	if err := validateKnowledgeRelationLayer(published); err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	result := compiledDeferredKnowledgeRelation{
		relation: published,
		state:    next,
		prelude:  staged.prelude,
	}
	if err := validateCompiledDeferredKnowledgeRelation(
		result,
		relation,
		existingArgs,
		preparation,
	); err != nil {
		return compiledDeferredKnowledgeRelation{}, err
	}
	return result, nil
}

func validateDeferredKnowledgeDescriptorSize(
	barrier compiledChronologicalBarrier,
	published compiledRelation,
) error {
	if _, ok := deferredKnowledgeDescriptorSQLBytes(barrier, published); ok {
		return nil
	}
	return &plan.Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: "compiled deferred knowledge relation exceeds the SQL byte limit",
		Range:   published.ownerRange,
	}
}

// deferredKnowledgeDescriptorSQLBytes charges the smallest complete top-level
// rendering of this descriptor. Later authored/finalizer SQL remains subject
// to the ordinary final-query ceiling, but an already-oversized knowledge
// graph must fail before any suffix lowering is attempted.
func deferredKnowledgeDescriptorSQLBytes(
	barrier compiledChronologicalBarrier,
	published compiledRelation,
) (int, bool) {
	total := 0
	add := func(bytes int) bool {
		if bytes < 0 || bytes > maxCompiledQueryBytes-total {
			return false
		}
		total += bytes
		return true
	}
	if !add(len("WITH ")) {
		return 0, false
	}
	for index, definition := range barrier.prerequisiteDefinitions {
		if index > 0 && !add(len(", ")) {
			return 0, false
		}
		if !add(len(definition)) {
			return 0, false
		}
	}
	for _, bytes := range []int{
		len(", "),
		len(barrier.name),
		len(" AS ("),
		len(barrier.sql),
		len(") "),
		len(published.sql),
		len(materializedCTESettingsSQL),
	} {
		if !add(bytes) {
			return 0, false
		}
	}
	return total, true
}

func validateCompiledDeferredKnowledgeRelation(
	compiled compiledDeferredKnowledgeRelation,
	input compiledRelation,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) error {
	if !preparation.present || preparation.program.IsEmpty() {
		if err := validateKnowledgeRuntimeGuardIdentityPrelude(
			compiled.prelude,
			preparation,
		); err != nil {
			return err
		}
		if compiled.relation != input ||
			!reflect.DeepEqual(compiled.state, compiled.prelude.state) ||
			!reflect.DeepEqual(compiled.args, existingArgs) {
			return errors.New(
				"compile ClickHouse deferred knowledge relation: identity changed",
			)
		}
		return nil
	}
	if err := validateCompiledKnowledgePrelude(compiled.prelude, preparation); err != nil {
		return err
	}
	if compiled.relation.ownerRange != input.ownerRange || compiled.args != nil ||
		len(compiled.state.chronologicalBarriers) != 1 {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: authority shape is invalid",
		)
	}
	barrier := compiled.state.chronologicalBarriers[0]
	if barrier.name != knowledgeRuntimeGuardResultName ||
		len(barrier.prerequisiteDefinitions) != 2 ||
		len(barrier.validationColumns) != 1 ||
		barrier.validationColumns[0] != knowledgeRuntimeGuardValidationColumn ||
		barrier.fanout != 2 || barrier.ownerRange != input.ownerRange {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: barrier is invalid",
		)
	}
	stagedDepth := input.depth + 2*len(compiled.prelude.stages)
	if barrier.depth != stagedDepth+2 || compiled.relation.depth != stagedDepth+3 {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: depth disagrees",
		)
	}
	if strings.Count(strings.Join(barrier.prerequisiteDefinitions, "\x00"), "?") !=
		len(barrier.args) || strings.Contains(barrier.sql, "?") ||
		!strings.HasPrefix(
			barrier.prerequisiteDefinitions[0],
			knowledgeRuntimeGuardInputName+" AS MATERIALIZED (",
		) || strings.Count(barrier.prerequisiteDefinitions[0], " AS MATERIALIZED (") != 1 ||
		!strings.HasPrefix(
			barrier.prerequisiteDefinitions[1],
			knowledgeRuntimeGuardTotalsName+" AS (SELECT ",
		) ||
		strings.Contains(barrier.prerequisiteDefinitions[1], " AS MATERIALIZED (") {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: barrier argument order is invalid",
		)
	}
	expectedArgs := len(existingArgs)
	if len(barrier.args) < expectedArgs || expectedArgs != 0 &&
		!reflect.DeepEqual(barrier.args[:expectedArgs], existingArgs) {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: input arguments disagree",
		)
	}
	for _, stage := range compiled.prelude.stages {
		end := expectedArgs + len(stage.suffixArgs)
		if end < expectedArgs || end > len(barrier.args) ||
			!reflect.DeepEqual(barrier.args[expectedArgs:end], stage.suffixArgs) {
			return errors.New(
				"compile ClickHouse deferred knowledge relation: stage arguments disagree",
			)
		}
		expectedArgs = end
	}
	if expectedArgs != len(barrier.args) {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: barrier arguments disagree",
		)
	}
	stateWithoutBarrier := cloneCompileState(compiled.state)
	stateWithoutBarrier.chronologicalBarriers = nil
	if !reflect.DeepEqual(stateWithoutBarrier, compiled.prelude.state) {
		return errors.New(
			"compile ClickHouse deferred knowledge relation: final state disagrees",
		)
	}
	if err := validateKnowledgeRelationLayer(compiled.relation); err != nil {
		return err
	}
	return nil
}
