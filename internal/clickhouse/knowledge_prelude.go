package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

type compiledKnowledgePreludeStageKind uint8

const (
	compiledKnowledgePreludeStageInvalid compiledKnowledgePreludeStageKind = iota
	compiledKnowledgePreludeStageExtraction
	compiledKnowledgePreludeStageAlias
	compiledKnowledgePreludeStageCalculated
)

// compiledKnowledgePreludeStage is one relation-neutral, two-SELECT physical
// stage. The central relation composer places the prior relation inside the
// binding projection and then publishes projection in an outer SELECT. Every
// suffix argument therefore follows all arguments owned by that prior relation.
type compiledKnowledgePreludeStage struct {
	kind              compiledKnowledgePreludeStageKind
	operatorOffset    int
	operatorKinds     []knowledgeprogram.OperatorKind
	bindingProjection []string
	projection        []string
	arrayJoinBindings []string
	suffixArgs        []any
}

// compiledKnowledgePreludeLoweringProof is derived only from operations that
// the physical lowerers returned. It is deliberately distinct from the
// immutable Program charges copied into preparedKnowledgeCompilation.
type compiledKnowledgePreludeLoweringProof struct {
	operatorKinds []knowledgeprogram.OperatorKind
	extractions   []compiledKnowledgeExtractionOperation
	aliases       []knowledgeprogram.Alias
	calculated    []knowledgeprogram.Calculated
	objectCount   uint32
	charges       knowledgeprogram.Charges
}

// compiledKnowledgePrelude is a pure physical plan. It opens no relation and
// installs no runtime guard, so it cannot affect legacy compilation until the
// central compiler explicitly consumes it. present keeps an admitted empty
// program distinct from a legacy query that never crossed knowledge admission.
type compiledKnowledgePrelude struct {
	present          bool
	prefixLength     int
	stages           []compiledKnowledgePreludeStage
	state            compileState
	selectorCharges  compiledKnowledgeSelectorChargeColumns
	aliasCopyCharges compiledKnowledgeAliasCopyChargeColumns
	capturedBytes    string
	proof            compiledKnowledgePreludeLoweringProof
}

// compileKnowledgePrelude composes the complete Tier-1 generated prefix in
// extraction -> alias -> calculated order. The input is the exact Scan state;
// prior capture accounting would be unproved work before the admitted prelude
// and is rejected.
func compileKnowledgePrelude(
	state compileState,
	preparation preparedKnowledgeCompilation,
) (compiledKnowledgePrelude, error) {
	if err := validateKnowledgePreludePreparation(preparation); err != nil {
		return compiledKnowledgePrelude{}, err
	}
	if !preparation.present {
		return compiledKnowledgePrelude{state: cloneCompileState(state)}, nil
	}
	if err := validateKnowledgeExtractionInputState(state); err != nil {
		return compiledKnowledgePrelude{}, fmt.Errorf(
			"compile ClickHouse knowledge prelude input: %w",
			err,
		)
	}
	if state.rexCapturedBytesSQL != "" {
		return compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse knowledge prelude: input contains prior capture accounting",
		)
	}

	result := compiledKnowledgePrelude{
		present:      true,
		prefixLength: preparation.prefixLength,
		state:        cloneCompileState(state),
	}
	if preparation.program.IsEmpty() {
		return result, nil
	}

	extractionCount, hasAliases, hasCalculated, err :=
		validateKnowledgePreludeGrammar(preparation.program)
	if err != nil {
		return compiledKnowledgePrelude{}, err
	}
	current := cloneCompileState(state)
	charges := compiledKnowledgeSelectorChargeColumns{}
	aliasCopyCharges := compiledKnowledgeAliasCopyChargeColumns{}
	offset := 0

	if extractionCount > 0 {
		compiled, compileErr := compileKnowledgeExtractionStage(
			current,
			preparation.program,
			offset,
			charges,
		)
		if compileErr != nil {
			return compiledKnowledgePrelude{}, compileErr
		}
		stage, stageErr := compiledKnowledgePreludeStageFromExtraction(
			compiled,
			offset,
		)
		if stageErr != nil {
			return compiledKnowledgePrelude{}, stageErr
		}
		result.stages = append(result.stages, stage)
		result.proof.extractions = append(
			result.proof.extractions,
			compiled.emittedOperations...,
		)
		result.proof.operatorKinds = append(
			result.proof.operatorKinds,
			stage.operatorKinds...,
		)
		result.proof.objectCount += compiled.emittedOperatorCount
		result.proof.charges.GeneratedOperators += compiled.emittedOperatorCount
		result.proof.charges.GeneratedFields += compiled.emittedOutputCount
		result.proof.charges.RegexPrograms += compiled.emittedRegexPrograms
		result.proof.charges.RegexWorkUnits += compiled.emittedRegexWorkUnits
		result.proof.charges.ExtractionOutputs += compiled.emittedOutputCount
		result.proof.charges.JSONEvaluationWork += compiled.emittedJSONEvaluationWork
		current = compiled.state
		charges = compiled.selectorCharges
		offset += extractionCount
	}

	if hasAliases {
		compiled, compileErr := compileKnowledgeAliasStage(
			current,
			preparation.program.Aliases(),
			offset,
			charges,
		)
		if compileErr != nil {
			return compiledKnowledgePrelude{}, compileErr
		}
		stage, stageErr := compiledKnowledgePreludeStageFromFields(
			compiledKnowledgePreludeStageAlias,
			compiled,
			offset,
			[]knowledgeprogram.OperatorKind{knowledgeprogram.OperatorCopyFieldAlias},
		)
		if stageErr != nil {
			return compiledKnowledgePrelude{}, stageErr
		}
		result.stages = append(result.stages, stage)
		result.proof.operatorKinds = append(result.proof.operatorKinds, stage.operatorKinds...)
		result.proof.aliases = slices.Clone(compiled.aliases)
		result.proof.objectCount += compiled.emittedAssignments
		result.proof.charges.GeneratedOperators++
		result.proof.charges.GeneratedFields += compiled.emittedAssignments
		current = compiled.state
		charges = compiled.selectorCharges
		aliasCopyCharges = compiled.aliasCopyCharges
		offset++
	}

	if hasCalculated {
		compiled, compileErr := compileKnowledgeCalculatedStage(
			current,
			preparation.program.CalculatedFields(),
			offset,
			charges,
			aliasCopyCharges,
		)
		if compileErr != nil {
			return compiledKnowledgePrelude{}, compileErr
		}
		stage, stageErr := compiledKnowledgePreludeStageFromFields(
			compiledKnowledgePreludeStageCalculated,
			compiled,
			offset,
			[]knowledgeprogram.OperatorKind{knowledgeprogram.OperatorParallelExtend},
		)
		if stageErr != nil {
			return compiledKnowledgePrelude{}, stageErr
		}
		result.stages = append(result.stages, stage)
		result.proof.operatorKinds = append(result.proof.operatorKinds, stage.operatorKinds...)
		result.proof.calculated = slices.Clone(compiled.calculated)
		result.proof.objectCount += compiled.emittedAssignments
		result.proof.charges.GeneratedOperators++
		result.proof.charges.GeneratedFields += compiled.emittedAssignments
		result.proof.charges.ScalarExpressions += compiled.emittedAssignments
		for _, operation := range compiled.calculated {
			result.proof.charges.ScalarExpressionNodes += operation.Nodes()
			result.proof.charges.ScalarPredicates += operation.Predicates()
		}
		current = compiled.state
		charges = compiled.selectorCharges
		aliasCopyCharges = compiled.aliasCopyCharges
	}

	result.state = current
	result.selectorCharges = charges
	result.aliasCopyCharges = aliasCopyCharges
	result.capturedBytes = current.rexCapturedBytesSQL
	if err := validateCompiledKnowledgePrelude(result, preparation); err != nil {
		return compiledKnowledgePrelude{}, err
	}
	return result, nil
}

func validateKnowledgePreludePreparation(preparation preparedKnowledgeCompilation) error {
	if !preparation.present {
		if !preparation.program.IsZero() || preparation.prefixLength != 0 ||
			len(preparation.operatorKinds) != 0 ||
			preparation.programCharges != (knowledgeprogram.Charges{}) ||
			preparation.programCommitment != ([32]byte{}) {
			return errors.New(
				"compile ClickHouse knowledge prelude: absent preparation contains authority",
			)
		}
		return nil
	}
	if preparation.program.IsZero() || preparation.prefixLength < 0 {
		return errors.New(
			"compile ClickHouse knowledge prelude: present preparation is invalid",
		)
	}
	commitment, ok := preparation.program.Commitment()
	programKinds := preparation.program.OperatorKinds()
	programCharges := preparation.program.Charges()
	if err := validateClickHouseKnowledgePhysicalCharges(programCharges); err != nil {
		return err
	}
	if !ok || commitment != preparation.programCommitment ||
		!slices.Equal(programKinds, preparation.operatorKinds) ||
		programCharges != preparation.programCharges ||
		preparation.prefixLength != len(programKinds) ||
		uint32(preparation.prefixLength) != programCharges.GeneratedOperators { // #nosec G115 -- physical charge validation bounds this by knowledgeprogram.MaximumObjects.
		return errors.New(
			"compile ClickHouse knowledge prelude: prepared authority disagrees with program",
		)
	}
	return nil
}

func validateKnowledgePreludeGrammar(
	program knowledgeprogram.Program,
) (int, bool, bool, error) {
	kinds := program.OperatorKinds()
	extractionCount := 0
	hasAliases, hasCalculated := false, false
	for _, kind := range kinds {
		switch kind {
		case knowledgeprogram.OperatorConditionalExtract,
			knowledgeprogram.OperatorConditionalExtractJSON:
			if hasAliases || hasCalculated {
				return 0, false, false, errors.New(
					"compile ClickHouse knowledge prelude: extraction follows a later stage",
				)
			}
			extractionCount++
		case knowledgeprogram.OperatorCopyFieldAlias:
			if hasAliases || hasCalculated {
				return 0, false, false, errors.New(
					"compile ClickHouse knowledge prelude: alias stage is duplicated or reordered",
				)
			}
			hasAliases = true
		case knowledgeprogram.OperatorParallelExtend:
			if hasCalculated {
				return 0, false, false, errors.New(
					"compile ClickHouse knowledge prelude: calculated stage is duplicated",
				)
			}
			hasCalculated = true
		default:
			return 0, false, false, errors.New(
				"compile ClickHouse knowledge prelude: operator kind is invalid",
			)
		}
	}
	if extractionCount != len(program.RegexExtractions())+len(program.JSONExtractions()) ||
		hasAliases != (len(program.Aliases()) != 0) ||
		hasCalculated != (len(program.CalculatedFields()) != 0) {
		return 0, false, false, errors.New(
			"compile ClickHouse knowledge prelude: operator inventories disagree",
		)
	}
	return extractionCount, hasAliases, hasCalculated, nil
}

func compiledKnowledgePreludeStageFromExtraction(
	compiled compiledKnowledgeFusedExtractionProjection,
	offset int,
) (compiledKnowledgePreludeStage, error) {
	kinds := make([]knowledgeprogram.OperatorKind, len(compiled.emittedOperations))
	for index, operation := range compiled.emittedOperations {
		kinds[index] = operation.kind
	}
	return newCompiledKnowledgePreludeStage(
		compiledKnowledgePreludeStageExtraction,
		offset,
		kinds,
		compiled.bindingProjection,
		compiled.projection,
		compiled.arrayJoinBindings,
		compiled.suffixArgs,
	)
}

func compiledKnowledgePreludeStageFromFields(
	kind compiledKnowledgePreludeStageKind,
	compiled compiledKnowledgeFusedFieldProjection,
	offset int,
	kinds []knowledgeprogram.OperatorKind,
) (compiledKnowledgePreludeStage, error) {
	return newCompiledKnowledgePreludeStage(
		kind,
		offset,
		kinds,
		compiled.bindingProjection,
		compiled.projection,
		compiled.arrayJoinBindings,
		compiled.suffixArgs,
	)
}

func newCompiledKnowledgePreludeStage(
	kind compiledKnowledgePreludeStageKind,
	offset int,
	kinds []knowledgeprogram.OperatorKind,
	bindingProjection []string,
	projection []string,
	arrayJoinBindings []string,
	suffixArgs []any,
) (compiledKnowledgePreludeStage, error) {
	if kind == compiledKnowledgePreludeStageInvalid || offset < 0 || len(kinds) == 0 ||
		len(bindingProjection) == 0 || len(projection) == 0 ||
		len(arrayJoinBindings) == 0 {
		return compiledKnowledgePreludeStage{}, errors.New(
			"compile ClickHouse knowledge prelude: physical stage is incomplete",
		)
	}
	args := make([]any, len(suffixArgs))
	for index, argument := range suffixArgs {
		cloned, ok := cloneCompiledArgument(argument)
		if !ok {
			return compiledKnowledgePreludeStage{}, errors.New(
				"compile ClickHouse knowledge prelude: stage argument is unsupported",
			)
		}
		args[index] = cloned
	}
	if strings.Contains(strings.Join(bindingProjection, "\x00"), "?") ||
		strings.Contains(strings.Join(projection, "\x00"), "?") ||
		strings.Count(strings.Join(arrayJoinBindings, "\x00"), "?") != len(args) {
		return compiledKnowledgePreludeStage{}, errors.New(
			"compile ClickHouse knowledge prelude: stage argument order is invalid",
		)
	}
	return compiledKnowledgePreludeStage{
		kind:              kind,
		operatorOffset:    offset,
		operatorKinds:     slices.Clone(kinds),
		bindingProjection: slices.Clone(bindingProjection),
		projection:        slices.Clone(projection),
		arrayJoinBindings: slices.Clone(arrayJoinBindings),
		suffixArgs:        args,
	}, nil
}

func validateCompiledKnowledgePrelude(
	compiled compiledKnowledgePrelude,
	preparation preparedKnowledgeCompilation,
) error {
	proof := compiled.proof
	if !compiled.present || compiled.prefixLength != preparation.prefixLength ||
		!slices.Equal(proof.operatorKinds, preparation.operatorKinds) ||
		proof.objectCount != preparation.program.ObjectCount() ||
		proof.charges != preparation.programCharges ||
		len(compiled.stages) == 0 ||
		compiled.selectorCharges.inputBytes == "" ||
		compiled.selectorCharges.queryUnits == "" ||
		(len(proof.aliases) != 0) !=
			(compiled.aliasCopyCharges != (compiledKnowledgeAliasCopyChargeColumns{})) ||
		compiled.capturedBytes != compiled.state.rexCapturedBytesSQL ||
		(proof.charges.RegexPrograms != 0) != (compiled.capturedBytes != "") {
		return errors.New(
			"compile ClickHouse knowledge prelude: physical lowering proof disagrees",
		)
	}
	if knowledgeRuntimeGuardStateLeaksAccounting(
		compiled.state,
		compiled.selectorCharges.inputBytes,
		compiled.selectorCharges.queryUnits,
		compiled.aliasCopyCharges.eventBytes,
		compiled.aliasCopyCharges.queryUnits,
		compiled.capturedBytes,
	) {
		return errors.New(
			"compile ClickHouse knowledge prelude: runtime accounting entered generic state",
		)
	}
	if len(proof.aliases) != 0 {
		lastStage := compiled.stages[len(compiled.stages)-1]
		if !validKnowledgeRuntimeGuardAliasCopyChargePair(
			compiled.aliasCopyCharges,
			lastStage.operatorOffset,
		) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
			lastStage.projection,
			compiled.aliasCopyCharges.eventBytes,
		) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
			lastStage.projection,
			compiled.aliasCopyCharges.queryUnits,
		) {
			return errors.New(
				"compile ClickHouse knowledge prelude: alias copy accounting is invalid",
			)
		}
	}
	nextOffset := 0
	for _, stage := range compiled.stages {
		if stage.operatorOffset != nextOffset ||
			!validCompiledKnowledgePreludeStageSpan(stage.kind, stage.operatorKinds) {
			return errors.New(
				"compile ClickHouse knowledge prelude: physical stage span is invalid",
			)
		}
		nextOffset += len(stage.operatorKinds)
	}
	if nextOffset != compiled.prefixLength {
		return errors.New(
			"compile ClickHouse knowledge prelude: physical stages do not cover the prefix",
		)
	}
	if !knowledgePreludeProofMatchesProgram(proof, preparation.program) {
		return errors.New(
			"compile ClickHouse knowledge prelude: emitted operations disagree with program",
		)
	}
	return nil
}

func validCompiledKnowledgePreludeStageSpan(
	kind compiledKnowledgePreludeStageKind,
	kinds []knowledgeprogram.OperatorKind,
) bool {
	if len(kinds) == 0 {
		return false
	}
	switch kind {
	case compiledKnowledgePreludeStageExtraction:
		for _, operatorKind := range kinds {
			if operatorKind != knowledgeprogram.OperatorConditionalExtract &&
				operatorKind != knowledgeprogram.OperatorConditionalExtractJSON {
				return false
			}
		}
		return true
	case compiledKnowledgePreludeStageAlias:
		return slices.Equal(kinds, []knowledgeprogram.OperatorKind{
			knowledgeprogram.OperatorCopyFieldAlias,
		})
	case compiledKnowledgePreludeStageCalculated:
		return slices.Equal(kinds, []knowledgeprogram.OperatorKind{
			knowledgeprogram.OperatorParallelExtend,
		})
	default:
		return false
	}
}

func knowledgePreludeProofMatchesProgram(
	proof compiledKnowledgePreludeLoweringProof,
	program knowledgeprogram.Program,
) bool {
	regex, jsonObjects := program.RegexExtractions(), program.JSONExtractions()
	regexIndex, jsonIndex := 0, 0
	for _, operation := range proof.extractions {
		switch operation.kind {
		case knowledgeprogram.OperatorConditionalExtract:
			if regexIndex >= len(regex) ||
				!knowledgeRegexExtractionOperationMatchesAuthority(
					operation.regex,
					knowledgeRegexExtractionAuthorityFromOperation(regex[regexIndex]),
				) {
				return false
			}
			regexIndex++
		case knowledgeprogram.OperatorConditionalExtractJSON:
			if jsonIndex >= len(jsonObjects) ||
				!knowledgeJSONExtractionOperationMatchesAuthority(
					operation.json,
					knowledgeJSONExtractionAuthorityFromOperation(jsonObjects[jsonIndex]),
				) {
				return false
			}
			jsonIndex++
		default:
			return false
		}
	}
	if regexIndex != len(regex) || jsonIndex != len(jsonObjects) ||
		!knowledgeAliasesEqual(proof.aliases, program.Aliases()) ||
		!knowledgeCalculatedEqual(proof.calculated, program.CalculatedFields()) {
		return false
	}
	return true
}

func knowledgeAliasesEqual(left, right []knowledgeprogram.Alias) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Origin() != right[index].Origin() ||
			left[index].Overwrite() != right[index].Overwrite() ||
			left[index].Source() != right[index].Source() ||
			left[index].Destination() != right[index].Destination() ||
			!bytes.Equal(
				left[index].Selector().CanonicalBytes(),
				right[index].Selector().CanonicalBytes(),
			) {
			return false
		}
	}
	return true
}

func knowledgeCalculatedEqual(left, right []knowledgeprogram.Calculated) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Origin() != right[index].Origin() ||
			left[index].Overwrite() != right[index].Overwrite() ||
			left[index].Destination() != right[index].Destination() ||
			left[index].Expression() != right[index].Expression() ||
			!slices.Equal(left[index].InputFields(), right[index].InputFields()) ||
			left[index].Nodes() != right[index].Nodes() ||
			left[index].Predicates() != right[index].Predicates() ||
			!bytes.Equal(
				left[index].Selector().CanonicalBytes(),
				right[index].Selector().CanonicalBytes(),
			) {
			return false
		}
	}
	return true
}
