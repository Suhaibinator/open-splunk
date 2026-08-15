package clickhouse

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

// Automatic lookup table/output aliases occupy a disjoint physical namespace
// from authored pipeline aliases. The logical stage count remains bounded by
// MaximumLookupStagesPerQuery; this high base is only an identifier domain.
const automaticLookupPhysicalStageBase = 1_000_000

type compiledAutomaticLookupProof struct {
	commitment        [sha256.Size]byte
	operatorOffset    int
	stageCount        int
	bindingProjection []string
	finalProjection   []string
	frozenColumns     []string
	args              []any
	selectorCharges   compiledKnowledgeSelectorChargeColumns
	additionalDepth   int
}

type compiledAutomaticLookupStageInput struct {
	selectorTuple string
	keys          []compiledFrozenLookupKey
}

// compileAutomaticLookupGroup freezes every selector and event-key tuple from
// the same completed Tier-1 relation. Ordered joins may then publish outputs
// deterministically, but no automatic stage can observe an earlier automatic
// stage's writes while deciding whether or where it matches.
func compileAutomaticLookupGroup(
	relation compiledRelation,
	state compileState,
	existingArgs []any,
	group *preparedAutomaticLookupGroup,
	prelude compiledKnowledgePrelude,
) (compiledRelation, compileState, []any, compiledKnowledgePrelude, error) {
	if group == nil || group.operator == nil || len(group.stages) == 0 ||
		len(group.stages) > MaximumLookupStagesPerQuery || !prelude.present {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse automatic lookups: authority is incomplete",
		)
	}
	digest, ok := group.operator.AuthorityDigest()
	if !ok || digest != group.commitment {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse automatic lookups: authority digest disagrees",
		)
	}
	if err := validateKnowledgeRelationInput(relation, existingArgs); err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}

	frozenState := cloneCompileState(state)
	tupleExpressions := make([]string, 0, len(group.stages)*3)
	bindingArgs := make([]any, 0)
	inputs := make([]compiledAutomaticLookupStageInput, len(group.stages))
	frozenColumns := make([]string, 0, len(group.stages)*3)
	tuplePosition := 1
	for stageIndex, stage := range group.stages {
		compiledSelector, err := compileKnowledgeSelector(stage.selector)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, fmt.Errorf(
				"compile ClickHouse automatic lookup selector %d: %w",
				stageIndex,
				err,
			)
		}
		selectorColumn := quoteIdentifier(fmt.Sprintf(
			"__os_auto_lookup_selector_%d",
			stageIndex,
		))
		tupleExpressions = append(tupleExpressions, compiledSelector.sql)
		bindingArgs = append(bindingArgs, compiledSelector.args...)
		inputs[stageIndex].selectorTuple = selectorColumn
		frozenColumns = append(frozenColumns, selectorColumn)
		tuplePosition++

		inputs[stageIndex].keys = make(
			[]compiledFrozenLookupKey,
			len(stage.lookup.operator.Keys),
		)
		for keyIndex, key := range stage.lookup.operator.Keys {
			valueSQL, eligibleSQL, keyArgs, err := compileLookupKey(key, frozenState)
			if err != nil {
				return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, fmt.Errorf(
					"compile ClickHouse automatic lookup key %d.%d: %w",
					stageIndex,
					keyIndex,
					err,
				)
			}
			keyColumn := quoteIdentifier(fmt.Sprintf(
				"__os_auto_lookup_key_%d_%d",
				stageIndex,
				keyIndex,
			))
			tupleExpressions = append(
				tupleExpressions,
				"tuple("+valueSQL+", toUInt8("+eligibleSQL+"))",
			)
			bindingArgs = append(bindingArgs, keyArgs...)
			inputs[stageIndex].keys[keyIndex] = compiledFrozenLookupKey{
				valueSQL:    "tupleElement(" + keyColumn + ", 1)",
				eligibleSQL: "tupleElement(" + keyColumn + ", 2) != toUInt8(0)",
			}
			frozenColumns = append(frozenColumns, keyColumn)
			tuplePosition++
		}
	}
	if len(tupleExpressions) == 0 || tuplePosition != len(tupleExpressions)+1 {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse automatic lookups: frozen tuple is empty",
		)
	}

	boundTuple := quoteIdentifier("__os_auto_lookup_frozen")
	bindingProjection := visibleEventProjection(state)
	position := 1
	for stageIndex := range group.stages {
		bindingProjection = append(
			bindingProjection,
			"tupleElement("+boundTuple+", "+fmt.Sprint(position)+") AS "+
				inputs[stageIndex].selectorTuple,
		)
		position++
		for keyIndex := range inputs[stageIndex].keys {
			column := quoteIdentifier(fmt.Sprintf(
				"__os_auto_lookup_key_%d_%d",
				stageIndex,
				keyIndex,
			))
			bindingProjection = append(
				bindingProjection,
				"tupleElement("+boundTuple+", "+fmt.Sprint(position)+") AS "+column,
			)
			position++
		}
	}
	baseAccounting := automaticLookupBaseAccountingColumns(prelude)
	for _, column := range baseAccounting {
		if !slices.Contains(bindingProjection, column) {
			bindingProjection = append(bindingProjection, column)
		}
	}
	inputAlias := quoteIdentifier("__os_auto_lookup_input")
	bindingSQL := "SELECT " + strings.Join(bindingProjection, ", ") +
		" FROM (" + relation.sql + ") AS " + inputAlias +
		" ARRAY JOIN [tuple(" + strings.Join(tupleExpressions, ", ") +
		")] AS " + boundTuple
	current := relation.selectFrom(bindingSQL, relation.ownerRange)
	if err := validateKnowledgeRelationLayer(current); err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}
	args, err := cloneKnowledgeRelationArguments(existingArgs)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}
	args = append(args, bindingArgs...)
	if strings.Count(current.sql, "?") != len(args) {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse automatic lookups: frozen argument order is invalid",
		)
	}

	passthrough := slices.Clone(frozenColumns)
	for _, column := range baseAccounting {
		if !slices.Contains(passthrough, column) {
			passthrough = append(passthrough, column)
		}
	}
	currentState := cloneCompileState(state)
	for stageIndex, stage := range group.stages {
		var additionalAliases int
		current, currentState, args, additionalAliases, err = compileLookupStageWithOptions(
			current,
			currentState,
			args,
			stage.lookup,
			automaticLookupPhysicalStageBase+stageIndex,
			compiledLookupStageOptions{
				keys:               inputs[stageIndex].keys,
				selectorTuple:      inputs[stageIndex].selectorTuple,
				passthroughColumns: passthrough,
			},
		)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
		}
		if additionalAliases != 1 {
			return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
				"compile ClickHouse automatic lookups: physical stage width disagrees",
			)
		}
	}

	finalState := cloneCompileState(currentState)
	finalState.privateColumns = slices.DeleteFunc(
		finalState.privateColumns,
		func(column string) bool { return slices.Contains(passthrough, column) },
	)
	charges := compiledKnowledgeSelectorChargeColumns{
		inputBytes: quoteIdentifier(fmt.Sprintf(
			"__os_ko_selector_input_bytes_%d",
			prelude.prefixLength,
		)),
		queryUnits: quoteIdentifier(fmt.Sprintf(
			"__os_ko_selector_query_units_%d",
			prelude.prefixLength,
		)),
	}
	inputCharges := make([]string, 0, len(group.stages)+1)
	queryCharges := make([]string, 0, len(group.stages)+1)
	if prelude.selectorCharges.inputBytes != "" {
		inputCharges = append(inputCharges, prelude.selectorCharges.inputBytes)
		queryCharges = append(queryCharges, prelude.selectorCharges.queryUnits)
	}
	for _, input := range inputs {
		inputCharges = append(
			inputCharges,
			knowledgeTupleElementUInt128(input.selectorTuple, 2),
		)
		queryCharges = append(
			queryCharges,
			knowledgeTupleElementUInt128(input.selectorTuple, 3),
		)
	}
	finalProjection := visibleEventProjection(finalState)
	for _, column := range []string{
		prelude.aliasCopyCharges.eventBytes,
		prelude.aliasCopyCharges.queryUnits,
	} {
		if column != "" && !slices.Contains(finalProjection, column) {
			finalProjection = append(finalProjection, column)
		}
	}
	finalProjection = append(
		finalProjection,
		knowledgeUInt128Sum(inputCharges)+" AS "+charges.inputBytes,
		knowledgeUInt128Sum(queryCharges)+" AS "+charges.queryUnits,
	)
	finalAlias := quoteIdentifier("__os_auto_lookup_accounting")
	current = current.selectFrom(
		"SELECT "+strings.Join(finalProjection, ", ")+" FROM ("+
			current.sql+") AS "+finalAlias,
		relation.ownerRange,
	)
	if err := validateKnowledgeRelationLayer(current); err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}
	if strings.Count(current.sql, "?") != len(args) {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, errors.New(
			"compile ClickHouse automatic lookups: final argument order is invalid",
		)
	}
	proofArgs, err := cloneKnowledgeRelationArguments(args)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}
	proof := &compiledAutomaticLookupProof{
		commitment:        group.commitment,
		operatorOffset:    prelude.prefixLength,
		stageCount:        len(group.stages),
		bindingProjection: slices.Clone(bindingProjection),
		finalProjection:   slices.Clone(finalProjection),
		frozenColumns:     slices.Clone(frozenColumns),
		args:              proofArgs,
		selectorCharges:   charges,
		additionalDepth:   current.depth - relation.depth,
	}
	prelude.automaticLookup = proof
	prelude.state = cloneCompileState(finalState)
	prelude.selectorCharges = charges
	if err := validateCompiledAutomaticLookupProof(prelude, group); err != nil {
		return compiledRelation{}, compileState{}, nil, compiledKnowledgePrelude{}, err
	}
	return current, finalState, args, prelude, nil
}

func visibleEventProjection(state compileState) []string {
	projection := make([]string, 0, len(state.visible)+len(state.privateColumns)+12)
	for _, name := range orderedVisibleNames(state) {
		field := state.visible[name]
		publicName := quoteIdentifier(name)
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}
	return appendPrivateEventProjection(projection, state)
}

func automaticLookupBaseAccountingColumns(prelude compiledKnowledgePrelude) []string {
	columns := make([]string, 0, 4)
	for _, column := range []string{
		prelude.selectorCharges.inputBytes,
		prelude.selectorCharges.queryUnits,
		prelude.aliasCopyCharges.eventBytes,
		prelude.aliasCopyCharges.queryUnits,
	} {
		if column != "" && !slices.Contains(columns, column) {
			columns = append(columns, column)
		}
	}
	return columns
}

func validateCompiledAutomaticLookupProof(
	prelude compiledKnowledgePrelude,
	group *preparedAutomaticLookupGroup,
) error {
	proof := prelude.automaticLookup
	if proof == nil || proof.commitment == ([sha256.Size]byte{}) ||
		proof.stageCount == 0 || proof.stageCount > MaximumLookupStagesPerQuery ||
		proof.operatorOffset != prelude.prefixLength || proof.additionalDepth <= 0 ||
		len(proof.bindingProjection) == 0 || len(proof.finalProjection) == 0 ||
		len(proof.frozenColumns) < proof.stageCount ||
		proof.selectorCharges != prelude.selectorCharges ||
		!validKnowledgeRuntimeGuardSelectorChargePair(
			proof.selectorCharges,
			proof.operatorOffset,
		) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		proof.finalProjection,
		proof.selectorCharges.inputBytes,
	) || !knowledgeRuntimeGuardProjectionDefinesExactlyOnce(
		proof.finalProjection,
		proof.selectorCharges.queryUnits,
	) {
		return errors.New(
			"compile ClickHouse automatic lookups: physical proof disagrees",
		)
	}
	if group != nil && (group.operator == nil ||
		proof.commitment != group.commitment || proof.stageCount != len(group.stages)) {
		return errors.New(
			"compile ClickHouse automatic lookups: logical and physical authority disagree",
		)
	}
	if knowledgeRuntimeGuardStateLeaksAccounting(
		prelude.state,
		proof.selectorCharges.inputBytes,
		proof.selectorCharges.queryUnits,
	) {
		return errors.New(
			"compile ClickHouse automatic lookups: final state contains accounting authority",
		)
	}
	seen := make(map[string]struct{}, len(proof.frozenColumns))
	for _, column := range proof.frozenColumns {
		if column == "" || strings.Contains(column, "?") {
			return errors.New("compile ClickHouse automatic lookups: frozen column is invalid")
		}
		if _, duplicate := seen[column]; duplicate {
			return errors.New("compile ClickHouse automatic lookups: frozen column is duplicated")
		}
		seen[column] = struct{}{}
		if slices.Contains(prelude.state.privateColumns, column) {
			return errors.New("compile ClickHouse automatic lookups: frozen column leaked")
		}
	}
	return nil
}

func validateAutomaticLookupPrelude(
	prelude compiledKnowledgePrelude,
	preparation preparedKnowledgeCompilation,
	group *preparedAutomaticLookupGroup,
) error {
	if group == nil {
		if prelude.automaticLookup != nil {
			return errors.New(
				"compile ClickHouse automatic lookups: proof exists without authority",
			)
		}
		return validateCompiledKnowledgePrelude(prelude, preparation)
	}
	if !preparation.present || prelude.automaticLookup == nil {
		return errors.New(
			"compile ClickHouse automatic lookups: admitted knowledge marker is missing",
		)
	}
	if err := validateDetachedAutomaticLookupPrelude(prelude, preparation); err != nil {
		return err
	}
	return validateCompiledAutomaticLookupProof(prelude, group)
}

func validateDetachedAutomaticLookupPrelude(
	prelude compiledKnowledgePrelude,
	preparation preparedKnowledgeCompilation,
) error {
	if prelude.automaticLookup == nil || !preparation.present {
		return errors.New(
			"compile ClickHouse automatic lookups: detached proof is incomplete",
		)
	}
	if preparation.program.IsEmpty() {
		if !prelude.present || prelude.prefixLength != 0 || len(prelude.stages) != 0 ||
			prelude.aliasCopyCharges != (compiledKnowledgeAliasCopyChargeColumns{}) ||
			prelude.capturedBytes != "" || len(prelude.proof.operatorKinds) != 0 ||
			len(prelude.proof.extractions) != 0 || len(prelude.proof.aliases) != 0 ||
			len(prelude.proof.calculated) != 0 || prelude.proof.objectCount != 0 ||
			prelude.proof.charges != (knowledgeprogram.Charges{}) {
			return errors.New(
				"compile ClickHouse automatic lookups: empty Tier-1 prelude contains authority",
			)
		}
		if err := validateKnowledgeExtractionInputState(prelude.state); err != nil {
			return fmt.Errorf("compile ClickHouse automatic lookup state: %w", err)
		}
		if prelude.state.rexCapturedBytesSQL != "" ||
			knowledgeRuntimeGuardStateHasDeferredAnalysis(prelude.state) {
			return errors.New(
				"compile ClickHouse automatic lookups: empty Tier-1 state is invalid",
			)
		}
	} else if err := validateCompiledKnowledgePrelude(prelude, preparation); err != nil {
		return err
	}
	return validateCompiledAutomaticLookupProof(prelude, nil)
}
