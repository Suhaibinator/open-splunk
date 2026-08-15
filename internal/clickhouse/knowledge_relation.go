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

// compiledKnowledgeRelation is the complete inline compatibility form of the
// physical knowledge prefix. prelude is retained beside the relation so sealing
// can prove that the SQL, final state, and runtime accounting came from one
// internally compiled authority rather than independently mutable inputs.
type compiledKnowledgeRelation struct {
	relation compiledRelation
	state    compileState
	args     []any
	prelude  compiledKnowledgePrelude
}

// compiledKnowledgeStageRelation is the exact physical prefix before a
// whole-query runtime fence is chosen. Both the inline compatibility probe and
// the deferred top-level barrier consume this one compile-once authority.
type compiledKnowledgeStageRelation struct {
	relation compiledRelation
	state    compileState
	args     []any
	prelude  compiledKnowledgePrelude
}

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
) (compiledKnowledgeRelation, error) {
	staged, err := compileKnowledgeStageRelation(
		relation,
		scanState,
		existingArgs,
		preparation,
	)
	if err != nil {
		return compiledKnowledgeRelation{}, err
	}
	guarded, err := compileKnowledgeRuntimeGuard(
		staged.relation,
		staged.prelude,
		preparation,
	)
	if err != nil {
		return compiledKnowledgeRelation{}, err
	}
	if len(guarded.suffixArgs) != 0 {
		return compiledKnowledgeRelation{}, errors.New(
			"compile ClickHouse knowledge relation: runtime guard introduced arguments",
		)
	}
	if strings.Count(guarded.relation.sql, "?") != len(staged.args) {
		return compiledKnowledgeRelation{}, errors.New(
			"compile ClickHouse knowledge relation: final placeholder order is invalid",
		)
	}
	result := compiledKnowledgeRelation{
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
		return compiledKnowledgeRelation{}, err
	}
	return result, nil
}

func compileKnowledgeStageRelation(
	relation compiledRelation,
	scanState compileState,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) (compiledKnowledgeStageRelation, error) {
	if err := validateKnowledgeRelationInput(relation, existingArgs); err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	args, err := cloneKnowledgeRelationArguments(existingArgs)
	if err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	prelude, err := compileKnowledgePrelude(scanState, preparation)
	if err != nil {
		return compiledKnowledgeStageRelation{}, err
	}
	if !preparation.present || preparation.program.IsEmpty() {
		result := compiledKnowledgeStageRelation{
			relation: relation,
			state:    cloneCompileState(prelude.state),
			args:     args,
			prelude:  prelude,
		}
		if err := validateCompiledKnowledgeStageRelation(
			result,
			relation,
			existingArgs,
			preparation,
		); err != nil {
			return compiledKnowledgeStageRelation{}, err
		}
		return result, nil
	}

	current := relation
	nextOffset := 0
	for stageIndex, stage := range prelude.stages {
		spanEnd := nextOffset + len(stage.operatorKinds)
		if spanEnd < nextOffset || spanEnd > len(preparation.operatorKinds) {
			return compiledKnowledgeStageRelation{}, errors.New(
				"compile ClickHouse knowledge relation: physical stage span is invalid",
			)
		}
		if err := validateKnowledgeRelationStage(
			stage,
			nextOffset,
			preparation.operatorKinds[nextOffset:spanEnd],
		); err != nil {
			return compiledKnowledgeStageRelation{}, err
		}
		nextOffset += len(stage.operatorKinds)
		stageArgs, cloneErr := cloneKnowledgeRelationArguments(stage.suffixArgs)
		if cloneErr != nil {
			return compiledKnowledgeStageRelation{}, fmt.Errorf(
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
			return compiledKnowledgeStageRelation{}, err
		}
		projectionSQL := "SELECT " + strings.Join(stage.projection, ", ") +
			" FROM (" + current.sql + ") AS " + bindingAlias
		current = current.selectFrom(projectionSQL, relation.ownerRange)
		if err := validateKnowledgeRelationLayer(current); err != nil {
			return compiledKnowledgeStageRelation{}, err
		}
		args = append(args, stageArgs...)
		if strings.Count(current.sql, "?") != len(args) {
			return compiledKnowledgeStageRelation{}, errors.New(
				"compile ClickHouse knowledge relation: stage placeholder order is invalid",
			)
		}
	}
	if nextOffset != prelude.prefixLength {
		return compiledKnowledgeStageRelation{}, errors.New(
			"compile ClickHouse knowledge relation: physical stages do not cover the prefix",
		)
	}
	result := compiledKnowledgeStageRelation{
		relation: current,
		state:    cloneCompileState(prelude.state),
		args:     args,
		prelude:  prelude,
	}
	if err := validateCompiledKnowledgeStageRelation(
		result,
		relation,
		existingArgs,
		preparation,
	); err != nil {
		return compiledKnowledgeStageRelation{}, err
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
	return slices.Contains(expressions, "")
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
	if err := validateCompiledKnowledgeRelationAuthority(
		compiled.relation,
		compiled.state,
		compiled.args,
		compiled.prelude,
		input,
		existingArgs,
	); err != nil {
		return err
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

func validateCompiledKnowledgeStageRelation(
	compiled compiledKnowledgeStageRelation,
	input compiledRelation,
	existingArgs []any,
	preparation preparedKnowledgeCompilation,
) error {
	if err := validateCompiledKnowledgeRelationAuthority(
		compiled.relation,
		compiled.state,
		compiled.args,
		compiled.prelude,
		input,
		existingArgs,
	); err != nil {
		return err
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
				"compile ClickHouse knowledge relation: staged identity changed",
			)
		}
		return nil
	}
	if err := validateCompiledKnowledgePrelude(compiled.prelude, preparation); err != nil {
		return err
	}
	expectedDepth := input.depth + 2*len(compiled.prelude.stages)
	if compiled.relation.depth != expectedDepth {
		return errors.New(
			"compile ClickHouse knowledge relation: staged relational depth disagrees",
		)
	}
	return nil
}

func validateCompiledKnowledgeRelationAuthority(
	relation compiledRelation,
	state compileState,
	args []any,
	prelude compiledKnowledgePrelude,
	input compiledRelation,
	existingArgs []any,
) error {
	if relation.ownerRange != input.ownerRange {
		return errors.New(
			"compile ClickHouse knowledge relation: final owner range disagrees",
		)
	}
	if !reflect.DeepEqual(state, prelude.state) {
		return errors.New(
			"compile ClickHouse knowledge relation: final state disagrees",
		)
	}
	if strings.Count(relation.sql, "?") != len(args) {
		return errors.New(
			"compile ClickHouse knowledge relation: final placeholder count disagrees",
		)
	}
	if len(args) < len(existingArgs) || len(existingArgs) != 0 &&
		!reflect.DeepEqual(args[:len(existingArgs)], existingArgs) {
		return errors.New(
			"compile ClickHouse knowledge relation: existing argument prefix disagrees",
		)
	}
	if err := validateKnowledgeRelationLayer(relation); err != nil {
		return err
	}
	expectedArgs := len(existingArgs)
	for _, stage := range prelude.stages {
		if len(args) < expectedArgs+len(stage.suffixArgs) ||
			!reflect.DeepEqual(
				args[expectedArgs:expectedArgs+len(stage.suffixArgs)],
				stage.suffixArgs,
			) {
			return errors.New(
				"compile ClickHouse knowledge relation: final argument order disagrees",
			)
		}
		expectedArgs += len(stage.suffixArgs)
	}
	if expectedArgs != len(args) {
		return errors.New(
			"compile ClickHouse knowledge relation: final argument count disagrees",
		)
	}
	return nil
}
