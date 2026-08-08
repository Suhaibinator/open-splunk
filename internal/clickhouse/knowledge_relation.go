package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// compiledKnowledgeRelation is the complete, still-unreachable physical
// knowledge prefix. prelude is retained beside the relation so later sealing
// can prove that the SQL, final state, and runtime accounting came from one
// internally compiled authority rather than independently mutable inputs.
type compiledKnowledgeRelation struct {
	relation compiledRelation
	state    compileState
	args     []any
	prelude  compiledKnowledgePrelude
}

// compileKnowledgeRelation compiles the immutable prelude from the exact Scan
// state, applies every relation-neutral stage as two SELECT levels, and closes
// nonempty work behind the runtime guard. It is intentionally not wired into
// compiler.go; pinned ClickHouse suffix compatibility remains a prerequisite
// for opening nonempty finalization.
func compileKnowledgeRelation(
	relation compiledRelation,
	scanState compileState,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) (compiledKnowledgeRelation, error) {
	if err := validateKnowledgeRelationInput(relation, existingArgs); err != nil {
		return compiledKnowledgeRelation{}, err
	}
	args, err := cloneKnowledgeRelationArguments(existingArgs)
	if err != nil {
		return compiledKnowledgeRelation{}, err
	}
	prelude, err := compileKnowledgePrelude(scanState, preparation)
	if err != nil {
		return compiledKnowledgeRelation{}, err
	}
	if !preparation.present || preparation.program.IsEmpty() {
		identity, guardErr := compileKnowledgeRuntimeGuard(
			relation,
			prelude,
			preparation,
		)
		if guardErr != nil {
			return compiledKnowledgeRelation{}, guardErr
		}
		if len(identity.suffixArgs) != 0 {
			return compiledKnowledgeRelation{}, errors.New(
				"compile ClickHouse knowledge relation: identity guard introduced arguments",
			)
		}
		result := compiledKnowledgeRelation{
			relation: identity.relation,
			state:    identity.state,
			args:     args,
			prelude:  prelude,
		}
		if err := validateCompiledKnowledgeRelation(
			result,
			relation,
			existingArgs,
			preparation,
		); err != nil {
			return compiledKnowledgeRelation{}, err
		}
		return result, nil
	}

	current := relation
	nextOffset := 0
	for stageIndex, stage := range prelude.stages {
		spanEnd := nextOffset + len(stage.operatorKinds)
		if spanEnd < nextOffset || spanEnd > len(preparation.operatorKinds) {
			return compiledKnowledgeRelation{}, errors.New(
				"compile ClickHouse knowledge relation: physical stage span is invalid",
			)
		}
		if err := validateKnowledgeRelationStage(
			stage,
			nextOffset,
			preparation.operatorKinds[nextOffset:spanEnd],
		); err != nil {
			return compiledKnowledgeRelation{}, err
		}
		nextOffset += len(stage.operatorKinds)
		stageArgs, cloneErr := cloneKnowledgeRelationArguments(stage.suffixArgs)
		if cloneErr != nil {
			return compiledKnowledgeRelation{}, fmt.Errorf(
				"compile ClickHouse knowledge relation stage %d: %w",
				stageIndex,
				cloneErr,
			)
		}

		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_ko_stage_input_%d",
			stage.operatorOffset,
		))
		bindingAlias := quoteIdentifier(fmt.Sprintf(
			"__os_ko_stage_bound_%d",
			stage.operatorOffset,
		))
		bindingSQL := "SELECT " + strings.Join(stage.bindingProjection, ", ") +
			" FROM (" + current.sql + ") AS " + inputAlias + " ARRAY JOIN " +
			strings.Join(stage.arrayJoinBindings, ", ")
		current = current.selectFrom(bindingSQL, relation.ownerRange)
		if err := validateKnowledgeRelationLayer(current); err != nil {
			return compiledKnowledgeRelation{}, err
		}
		projectionSQL := "SELECT " + strings.Join(stage.projection, ", ") +
			" FROM (" + current.sql + ") AS " + bindingAlias
		current = current.selectFrom(projectionSQL, relation.ownerRange)
		if err := validateKnowledgeRelationLayer(current); err != nil {
			return compiledKnowledgeRelation{}, err
		}
		args = append(args, stageArgs...)
		if strings.Count(current.sql, "?") != len(args) {
			return compiledKnowledgeRelation{}, errors.New(
				"compile ClickHouse knowledge relation: stage placeholder order is invalid",
			)
		}
	}
	if nextOffset != prelude.prefixLength {
		return compiledKnowledgeRelation{}, errors.New(
			"compile ClickHouse knowledge relation: physical stages do not cover the prefix",
		)
	}

	guarded, err := compileKnowledgeRuntimeGuard(current, prelude, preparation)
	if err != nil {
		return compiledKnowledgeRelation{}, err
	}
	if len(guarded.suffixArgs) != 0 {
		return compiledKnowledgeRelation{}, errors.New(
			"compile ClickHouse knowledge relation: runtime guard introduced arguments",
		)
	}
	if strings.Count(guarded.relation.sql, "?") != len(args) {
		return compiledKnowledgeRelation{}, errors.New(
			"compile ClickHouse knowledge relation: final placeholder order is invalid",
		)
	}
	result := compiledKnowledgeRelation{
		relation: guarded.relation,
		state:    guarded.state,
		args:     args,
		prelude:  prelude,
	}
	if err := validateCompiledKnowledgeRelation(
		result,
		relation,
		existingArgs,
		preparation,
	); err != nil {
		return compiledKnowledgeRelation{}, err
	}
	return result, nil
}

func validateKnowledgeRelationInput(
	relation compiledRelation,
	args []any,
) error {
	if relation.sql == "" || relation.depth <= 0 {
		return errors.New(
			"compile ClickHouse knowledge relation: input relation is invalid",
		)
	}
	if strings.Count(relation.sql, "?") != len(args) {
		return errors.New(
			"compile ClickHouse knowledge relation: input placeholder order is invalid",
		)
	}
	return validateKnowledgeRelationLayer(relation)
}

func validateKnowledgeRelationStage(
	stage compiledKnowledgePreludeStage,
	expectedOffset int,
	expectedKinds []knowledgeprogram.OperatorKind,
) error {
	if stage.kind == compiledKnowledgePreludeStageInvalid ||
		expectedOffset < 0 || stage.operatorOffset != expectedOffset ||
		!slices.Equal(stage.operatorKinds, expectedKinds) ||
		!validCompiledKnowledgePreludeStageSpan(stage.kind, stage.operatorKinds) ||
		len(stage.bindingProjection) == 0 || len(stage.projection) == 0 ||
		len(stage.arrayJoinBindings) == 0 ||
		knowledgeRelationContainsEmptyExpression(stage.bindingProjection) ||
		knowledgeRelationContainsEmptyExpression(stage.projection) ||
		knowledgeRelationContainsEmptyExpression(stage.arrayJoinBindings) ||
		strings.Contains(strings.Join(stage.bindingProjection, "\x00"), "?") ||
		strings.Contains(strings.Join(stage.projection, "\x00"), "?") ||
		strings.Count(strings.Join(stage.arrayJoinBindings, "\x00"), "?") !=
			len(stage.suffixArgs) {
		return errors.New(
			"compile ClickHouse knowledge relation: physical stage is invalid",
		)
	}
	return nil
}

func knowledgeRelationContainsEmptyExpression(expressions []string) bool {
	for _, expression := range expressions {
		if expression == "" {
			return true
		}
	}
	return false
}

func validateKnowledgeRelationLayer(relation compiledRelation) error {
	if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
		return err
	}
	if len(relation.sql) <= maxCompiledQueryBytes {
		return nil
	}
	return &plan.Diagnostic{
		Code: "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf(
			"compiled knowledge relation exceeds %d bytes",
			maxCompiledQueryBytes,
		),
		Range: relation.ownerRange,
	}
}

func cloneKnowledgeRelationArguments(arguments []any) ([]any, error) {
	if arguments == nil {
		return nil, nil
	}
	cloned := make([]any, len(arguments))
	for index, argument := range arguments {
		if scope, ok := argument.(compiledReadScopeArgument); ok {
			cloned[index] = compiledReadScopeArgument{
				ordinal: scope.ordinal,
				value:   strings.Clone(scope.value),
			}
			continue
		}
		value, ok := cloneCompiledArgument(argument)
		if !ok {
			return nil, errors.New(
				"compile ClickHouse knowledge relation: argument type is unsupported",
			)
		}
		cloned[index] = value
	}
	return cloned, nil
}

func validateCompiledKnowledgeRelation(
	compiled compiledKnowledgeRelation,
	input compiledRelation,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) error {
	if compiled.relation.ownerRange != input.ownerRange {
		return errors.New(
			"compile ClickHouse knowledge relation: final owner range disagrees",
		)
	}
	if !reflect.DeepEqual(compiled.state, compiled.prelude.state) {
		return errors.New(
			"compile ClickHouse knowledge relation: final state disagrees",
		)
	}
	if strings.Count(compiled.relation.sql, "?") != len(compiled.args) {
		return errors.New(
			"compile ClickHouse knowledge relation: final placeholder count disagrees",
		)
	}
	if len(compiled.args) < len(existingArgs) || len(existingArgs) != 0 &&
		!reflect.DeepEqual(compiled.args[:len(existingArgs)], existingArgs) {
		return errors.New(
			"compile ClickHouse knowledge relation: existing argument prefix disagrees",
		)
	}
	if err := validateKnowledgeRelationLayer(compiled.relation); err != nil {
		return err
	}
	expectedArgs := len(existingArgs)
	for _, stage := range compiled.prelude.stages {
		if len(compiled.args) < expectedArgs+len(stage.suffixArgs) ||
			!reflect.DeepEqual(
				compiled.args[expectedArgs:expectedArgs+len(stage.suffixArgs)],
				stage.suffixArgs,
			) {
			return errors.New(
				"compile ClickHouse knowledge relation: final argument order disagrees",
			)
		}
		expectedArgs += len(stage.suffixArgs)
	}
	if expectedArgs != len(compiled.args) {
		return errors.New(
			"compile ClickHouse knowledge relation: final argument count disagrees",
		)
	}
	if !preparation.present || preparation.program.IsEmpty() {
		if err := validateKnowledgeRuntimeGuardIdentityPrelude(
			compiled.prelude,
			preparation,
		); err != nil {
			return err
		}
		if compiled.relation != input {
			return errors.New(
				"compile ClickHouse knowledge relation: identity relation changed",
			)
		}
		return nil
	}
	if err := validateCompiledKnowledgePrelude(compiled.prelude, preparation); err != nil {
		return err
	}
	expectedDepth := input.depth + 2*len(compiled.prelude.stages) + 2
	if compiled.relation.depth != expectedDepth {
		return errors.New(
			"compile ClickHouse knowledge relation: final relational depth disagrees",
		)
	}
	return nil
}
