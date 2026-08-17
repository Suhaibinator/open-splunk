package clickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type preparedLookupCompilation struct {
	automatic   *preparedAutomaticLookupGroup
	stages      []preparedLookupStage
	resolutions []LookupResolution
}

type preparedAutomaticLookupGroup struct {
	operator   *plan.AutomaticLookupGroup
	commitment [sha256.Size]byte
	stages     []preparedAutomaticLookupStage
}

type preparedAutomaticLookupStage struct {
	stableID string
	selector knowledgeprogram.Selector
	lookup   preparedLookupStage
}

type preparedLookupStage struct {
	operator          *plan.Lookup
	sourceOperator    *plan.Lookup
	resolution        LookupResolution
	headerIndexes     map[string]int
	selectedColumns   []preparedLookupColumn
	selectedByHeader  map[string]int
	keyHeaderIndexes  []int
	outputHeaderIndex []int
}

type preparedLookupColumn struct {
	headerIndex int
	values      []string
}

func prepareLookupCompilation(
	query *plan.Query,
	scan *plan.Scan,
	resolutions []LookupResolution,
) (preparedLookupCompilation, error) {
	return prepareLookupCompilationContext(
		context.Background(),
		query,
		scan,
		resolutions,
	)
}

func prepareLookupCompilationContext(
	ctx context.Context,
	query *plan.Query,
	scan *plan.Scan,
	resolutions []LookupResolution,
) (preparedLookupCompilation, error) {
	return prepareLookupCompilationWithMaterializationContext(
		ctx,
		query,
		scan,
		resolutions,
		true,
	)
}

// prepareLookupCompilationForSeal revalidates the complete logical and
// immutable lookup authority without transposing selected cells a second
// time. The first preparation already owns the values used by the sealed
// external tables; the final seal comparison only consumes operators and
// resolutions.
func prepareLookupCompilationForSeal(
	query *plan.Query,
	scan *plan.Scan,
	resolutions []LookupResolution,
) (preparedLookupCompilation, error) {
	return prepareLookupCompilationForSealContext(
		context.Background(),
		query,
		scan,
		resolutions,
	)
}

func prepareLookupCompilationForSealContext(
	ctx context.Context,
	query *plan.Query,
	scan *plan.Scan,
	resolutions []LookupResolution,
) (preparedLookupCompilation, error) {
	return prepareLookupCompilationWithMaterializationContext(
		ctx,
		query,
		scan,
		resolutions,
		false,
	)
}

func prepareLookupCompilationWithMaterializationContext(
	ctx context.Context,
	query *plan.Query,
	scan *plan.Scan,
	resolutions []LookupResolution,
	materializeSelectedValues bool,
) (preparedLookupCompilation, error) {
	if ctx == nil {
		return preparedLookupCompilation{}, errors.New(
			"prepare ClickHouse lookup compilation: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return preparedLookupCompilation{}, err
	}
	if query == nil || scan == nil {
		return preparedLookupCompilation{}, errors.New(
			"prepare ClickHouse lookup compilation: query and scan are required",
		)
	}
	if err := plan.ValidateAutomaticLookupIntegrity(query); err != nil {
		return preparedLookupCompilation{}, fmt.Errorf(
			"prepare ClickHouse automatic lookups: %w",
			err,
		)
	}
	stageCount := 0
	for _, operator := range query.Operators[1:] {
		switch operator := operator.(type) {
		case *plan.Lookup:
			stageCount++
		case *plan.AutomaticLookupGroup:
			stageCount += len(operator.Lookups())
		}
	}
	if stageCount > MaximumLookupStagesPerQuery {
		return preparedLookupCompilation{}, &plan.Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"query contains more than %d lookup stages",
				MaximumLookupStagesPerQuery,
			),
		}
	}
	if stageCount != len(resolutions) {
		rangeValue := spl.Range{}
		for _, operator := range query.Operators[1:] {
			if lookup, ok := operator.(*plan.Lookup); ok && lookup != nil {
				rangeValue = lookup.Range
				break
			}
		}
		return preparedLookupCompilation{}, &plan.Diagnostic{
			Code:    "SPL_LOOKUP_UNRESOLVED",
			Message: "every lookup stage requires one exact pinned asset version",
			Range:   rangeValue,
		}
	}

	prepared := preparedLookupCompilation{
		stages:      make([]preparedLookupStage, 0, stageCount),
		resolutions: make([]LookupResolution, len(resolutions)),
	}
	var aggregatePayload uint64
	var aggregateMatchComponents uint64
	var aggregateSelectedCells uint64
	resolutionIndex := 0
	prepareOne := func(operator *plan.Lookup) (preparedLookupStage, error) {
		if resolutionIndex >= len(resolutions) {
			return preparedLookupStage{}, errors.New(
				"prepare ClickHouse lookup compilation: resolution order overflowed",
			)
		}
		matchComponents := uint64(len(operator.Keys))
		if matchComponents >
			uint64(MaximumLookupMatchKeyComponentsPerEvent)-aggregateMatchComponents {
			return preparedLookupStage{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"lookup match work exceeds %d key components per event",
					MaximumLookupMatchKeyComponentsPerEvent,
				),
				Range: operator.Range,
			}
		}
		aggregateMatchComponents += matchComponents
		selectedCells, preflightErr := lookupSelectedCellCountContext(
			ctx,
			operator,
			resolutions[resolutionIndex],
		)
		if preflightErr != nil {
			return preparedLookupStage{}, preflightErr
		}
		if selectedCells > MaximumLookupSelectedCellsPerQuery-aggregateSelectedCells {
			return preparedLookupStage{}, &plan.Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"lookup selected-cell work exceeds %d cells",
					MaximumLookupSelectedCellsPerQuery,
				),
				Range: operator.Range,
			}
		}
		aggregateSelectedCells += selectedCells
		stage, err := prepareLookupStageContext(
			ctx,
			operator,
			scan,
			resolutions[resolutionIndex],
			materializeSelectedValues,
		)
		if err != nil {
			return preparedLookupStage{}, err
		}
		prepared.resolutions[resolutionIndex] = resolutions[resolutionIndex].clone()
		resolutionIndex++
		payload, ok := lookupResolutionPayloadBytes(stage.resolution)
		if !ok {
			return preparedLookupStage{}, errors.New(
				"prepare ClickHouse lookup compilation: asset byte count overflows",
			)
		}
		aggregatePayload, ok = retainedAdd(aggregatePayload, payload)
		if !ok || aggregatePayload >
			uint64(MaximumLookupStagesPerQuery)*MaximumLookupAssetBytes {
			return preparedLookupStage{}, &plan.Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: "lookup assets exceed the per-query retained byte budget",
				Range:   operator.Range,
			}
		}
		return stage, nil
	}
	for _, operator := range query.Operators[1:] {
		switch operator := operator.(type) {
		case *plan.AutomaticLookupGroup:
			if prepared.automatic != nil {
				return preparedLookupCompilation{}, errors.New(
					"prepare ClickHouse automatic lookups: group is duplicated",
				)
			}
			commitment, ok := operator.AuthorityDigest()
			if !ok {
				return preparedLookupCompilation{}, errors.New(
					"prepare ClickHouse automatic lookups: group authority is invalid",
				)
			}
			entries := operator.Lookups()
			group := &preparedAutomaticLookupGroup{
				operator:   operator,
				commitment: commitment,
				stages:     make([]preparedAutomaticLookupStage, len(entries)),
			}
			for index, entry := range entries {
				logical := entry.LogicalLookup()
				stage, err := prepareOne(&logical)
				if err != nil {
					return preparedLookupCompilation{}, err
				}
				group.stages[index] = preparedAutomaticLookupStage{
					stableID: strings.Clone(entry.StableID()),
					selector: entry.Selector(),
					lookup:   stage,
				}
			}
			prepared.automatic = group
		case *plan.Lookup:
			stage, err := prepareOne(operator)
			if err != nil {
				return preparedLookupCompilation{}, err
			}
			stage.sourceOperator = operator
			prepared.stages = append(prepared.stages, stage)
		}
	}
	if resolutionIndex != len(resolutions) {
		return preparedLookupCompilation{}, errors.New(
			"prepare ClickHouse lookup compilation: not every resolution was consumed",
		)
	}
	return prepared, nil
}

func lookupSelectedCellCount(
	operator *plan.Lookup,
	resolution LookupResolution,
) (uint64, error) {
	return lookupSelectedCellCountContext(context.Background(), operator, resolution)
}

func lookupSelectedCellCountContext(
	ctx context.Context,
	operator *plan.Lookup,
	resolution LookupResolution,
) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("prepare ClickHouse lookup compilation: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validateCompiledLookupOperator(operator); err != nil {
		return 0, err
	}
	if err := validateLookupResolutionContext(ctx, resolution); err != nil {
		return 0, err
	}
	selected := make(map[string]struct{}, len(operator.Keys)+len(operator.Outputs))
	for _, key := range operator.Keys {
		selected[key.LookupField] = struct{}{}
	}
	for _, output := range operator.Outputs {
		selected[output.LookupField] = struct{}{}
	}
	cells, ok := lookupExternalTableCellCount(
		lookupResolutionRowCount(resolution),
		len(selected),
	)
	if !ok {
		return 0, errors.New(
			"prepare ClickHouse lookup compilation: selected-cell work overflows",
		)
	}
	return cells, nil
}

// lookupExternalTableCellCount is shared by preflight and sealed transport
// validation so the private match marker cannot appear only after admission.
func lookupExternalTableCellCount(rowCount, selectedColumns int) (uint64, bool) {
	if rowCount < 0 || selectedColumns < 0 {
		return 0, false
	}
	// Both inputs are nonnegative ints, so widening them to uint64 is lossless.
	// #nosec G115 -- guarded immediately above.
	columns := uint64(selectedColumns)
	// #nosec G115 -- guarded immediately above.
	rows := uint64(rowCount)
	if columns == ^uint64(0) {
		return 0, false
	}
	columns++ // one UInt8 match marker is materialized for every asset row
	if rows != 0 && columns > ^uint64(0)/rows {
		return 0, false
	}
	return rows * columns, true
}

func prepareLookupStageContext(
	ctx context.Context,
	operator *plan.Lookup,
	scan *plan.Scan,
	resolution LookupResolution,
	materializeSelectedValues bool,
) (preparedLookupStage, error) {
	if ctx == nil {
		return preparedLookupStage{}, errors.New(
			"prepare ClickHouse lookup compilation: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return preparedLookupStage{}, err
	}
	if err := validateCompiledLookupOperator(operator); err != nil {
		return preparedLookupStage{}, err
	}
	if err := validateLookupResolutionContext(ctx, resolution); err != nil {
		return preparedLookupStage{}, fmt.Errorf(
			"prepare ClickHouse lookup %q: %w",
			operator.DefinitionName,
			err,
		)
	}
	if resolution.tenantID != scan.TenantID ||
		resolution.definitionName != operator.DefinitionName {
		return preparedLookupStage{}, &plan.Diagnostic{
			Code:    "SPL_LOOKUP_UNAVAILABLE",
			Message: "lookup definition is unavailable in the admitted search scope",
			Range:   operator.DefinitionRange,
		}
	}
	if !resolution.contractSet ||
		!lookupResolutionContractsEqual(resolution.contract, *operator) {
		return preparedLookupStage{}, &plan.Diagnostic{
			Code:    "SPL_LOOKUP_DEFINITION_MISMATCH",
			Message: "lookup command does not match the resolved definition contract",
			Range:   operator.Range,
		}
	}

	headerIndexes := make(map[string]int, len(resolution.headers))
	for index, header := range resolution.headers {
		headerIndexes[header] = index
	}
	detachedOperator := *operator
	detachedOperator.Keys = slices.Clone(operator.Keys)
	detachedOperator.Outputs = slices.Clone(operator.Outputs)
	stage := preparedLookupStage{
		operator:          &detachedOperator,
		resolution:        resolution.clone(),
		headerIndexes:     headerIndexes,
		selectedByHeader:  make(map[string]int),
		keyHeaderIndexes:  make([]int, len(operator.Keys)),
		outputHeaderIndex: make([]int, len(operator.Outputs)),
	}
	selectColumn := func(name string, sourceRange spl.Range) (int, error) {
		headerIndex, ok := headerIndexes[name]
		if !ok {
			return 0, &plan.Diagnostic{
				Code:    "SPL_LOOKUP_UNKNOWN_FIELD",
				Message: "lookup field does not exist in the pinned asset schema",
				Range:   sourceRange,
			}
		}
		if selectedIndex, selected := stage.selectedByHeader[name]; selected {
			return selectedIndex, nil
		}
		var values []string
		if materializeSelectedValues {
			if resolution.columns != nil {
				// Retained replay columns are already private, immutable, and
				// authenticated by the source compiled-query seal.
				values = resolution.columns[headerIndex]
			} else {
				values = make([]string, lookupResolutionRowCount(resolution))
				for rowIndex := range values {
					if rowIndex%lookupContextCheckRows == 0 {
						if err := ctx.Err(); err != nil {
							return 0, err
						}
					}
					// encoding/csv cells may be substrings of one shared row string.
					// Detach selected bytes so a tiny charged value cannot retain large
					// unselected cells after the source Asset leaves admission scope.
					value, present := lookupResolutionCell(
						resolution,
						rowIndex,
						headerIndex,
					)
					if !present {
						return 0, errors.New(
							"prepare ClickHouse lookup compilation: asset row width changed",
						)
					}
					values[rowIndex] = strings.Clone(value)
				}
			}
		}
		selectedIndex := len(stage.selectedColumns)
		stage.selectedByHeader[name] = selectedIndex
		stage.selectedColumns = append(stage.selectedColumns, preparedLookupColumn{
			headerIndex: headerIndex,
			values:      values,
		})
		return selectedIndex, nil
	}
	for index, key := range operator.Keys {
		selectedIndex, err := selectColumn(key.LookupField, key.LookupFieldRange)
		if err != nil {
			return preparedLookupStage{}, err
		}
		stage.keyHeaderIndexes[index] = selectedIndex
	}
	for index, output := range operator.Outputs {
		selectedIndex, err := selectColumn(
			output.LookupField,
			output.LookupFieldRange,
		)
		if err != nil {
			return preparedLookupStage{}, err
		}
		stage.outputHeaderIndex[index] = selectedIndex
	}
	if materializeSelectedValues {
		// The first preparation owns the immutable selected backing and proves
		// key uniqueness. Seal-time preparation compares the same backing and
		// logical contract, so repeating this full-row proof cannot add authority.
		if err := validateExactLookupKeysContext(ctx, stage); err != nil {
			return preparedLookupStage{}, err
		}
	}
	return stage, nil
}

func validateCompiledLookupOperator(operator *plan.Lookup) error {
	if operator == nil || operator.DefinitionName == "" ||
		len(operator.Keys) == 0 || len(operator.Keys) > MaximumLookupKeys ||
		len(operator.Outputs) == 0 ||
		len(operator.Outputs) > spl.MaximumLookupOutputs ||
		(operator.WriteMode != plan.LookupWriteModeOverwrite &&
			operator.WriteMode != plan.LookupWriteModePreserveExisting) {
		return errors.New("compile ClickHouse lookup: logical operator is invalid")
	}
	seenLookupKeys := make(map[string]struct{}, len(operator.Keys))
	seenEventKeys := make(map[string]struct{}, len(operator.Keys))
	for _, key := range operator.Keys {
		if key.LookupField == "" || key.EventField.Name == "" {
			return errors.New("compile ClickHouse lookup: key mapping is incomplete")
		}
		if err := validateCanonicalFieldRef("lookup", "key", key.EventField); err != nil {
			return err
		}
		if _, duplicate := seenLookupKeys[key.LookupField]; duplicate {
			return errors.New("compile ClickHouse lookup: lookup key field is repeated")
		}
		if _, duplicate := seenEventKeys[key.EventField.Name]; duplicate {
			return errors.New("compile ClickHouse lookup: event key field is repeated")
		}
		seenLookupKeys[key.LookupField] = struct{}{}
		seenEventKeys[key.EventField.Name] = struct{}{}
	}
	seenLookupOutputs := make(map[string]struct{}, len(operator.Outputs))
	seenEventOutputs := make(map[string]struct{}, len(operator.Outputs))
	for _, output := range operator.Outputs {
		if output.LookupField == "" || output.EventField.Name == "" {
			return errors.New("compile ClickHouse lookup: output mapping is incomplete")
		}
		if err := validateCanonicalFieldRef("lookup", "output", output.EventField); err != nil {
			return err
		}
		if _, duplicate := seenLookupOutputs[output.LookupField]; duplicate {
			return errors.New("compile ClickHouse lookup: lookup output field is repeated")
		}
		if _, duplicate := seenEventOutputs[output.EventField.Name]; duplicate {
			return errors.New("compile ClickHouse lookup: event output field is repeated")
		}
		seenLookupOutputs[output.LookupField] = struct{}{}
		seenEventOutputs[output.EventField.Name] = struct{}{}
	}
	return nil
}

func validateExactLookupKeysContext(
	ctx context.Context,
	stage preparedLookupStage,
) error {
	if ctx == nil {
		return errors.New("validate ClickHouse lookup keys: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The digest selects a bounded bucket; canonical length-framed key bytes
	// remain the equality authority. A SHA-256 collision therefore cannot turn
	// distinct composite keys into duplicates or matches.
	buckets := make(
		map[[sha256.Size]byte][][]byte,
		lookupResolutionRowCount(stage.resolution),
	)
	values := make([]string, len(stage.keyHeaderIndexes))
	for rowIndex := 0; rowIndex < lookupResolutionRowCount(stage.resolution); rowIndex++ {
		if rowIndex%lookupContextCheckRows == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		key, err := canonicalLookupKey(rowIndex, stage, values)
		if err != nil {
			return fmt.Errorf(
				"validate ClickHouse lookup key at row %d: %w",
				rowIndex+1,
				err,
			)
		}
		canonical := key.Bytes()
		digest := key.SHA256()
		for _, existing := range buckets[digest] {
			if bytes.Equal(existing, canonical) {
				return &plan.Diagnostic{
					Code: "SPL_LOOKUP_DUPLICATE_KEY",
					Message: fmt.Sprintf(
						"lookup asset contains a duplicate exact key at data row %d",
						rowIndex+1,
					),
					Range: stage.operator.DefinitionRange,
				}
			}
		}
		// ExactKey.Bytes already returns a detached slice. Retain that one copy
		// directly instead of cloning the canonical key a second time.
		buckets[digest] = append(buckets[digest], canonical)
	}
	return nil
}

func canonicalLookupKey(
	rowIndex int,
	stage preparedLookupStage,
	values []string,
) (lookupasset.ExactKey, error) {
	if len(values) != len(stage.keyHeaderIndexes) {
		return lookupasset.ExactKey{}, errors.New(
			"lookup exact-key scratch space has invalid arity",
		)
	}
	for index, selectedIndex := range stage.keyHeaderIndexes {
		value, ok := lookupResolutionCell(
			stage.resolution,
			rowIndex,
			stage.selectedColumns[selectedIndex].headerIndex,
		)
		if !ok {
			return lookupasset.ExactKey{}, errors.New(
				"lookup asset row width changed",
			)
		}
		values[index] = value
	}
	return lookupasset.CanonicalizeExactKey(values)
}

func preparedLookupCompilationEqual(
	left,
	right preparedLookupCompilation,
) bool {
	if (left.automatic == nil) != (right.automatic == nil) ||
		len(left.stages) != len(right.stages) ||
		len(left.resolutions) != len(right.resolutions) {
		return false
	}
	for index := range left.resolutions {
		if !lookupResolutionEqual(left.resolutions[index], right.resolutions[index]) {
			return false
		}
	}
	for index := range left.stages {
		if left.stages[index].operator == nil || right.stages[index].operator == nil ||
			!lookupResolutionContractsEqual(
				*left.stages[index].operator,
				*right.stages[index].operator,
			) || !lookupResolutionEqual(
			left.stages[index].resolution,
			right.stages[index].resolution,
		) {
			return false
		}
	}
	if left.automatic == nil {
		return true
	}
	if left.automatic.commitment != right.automatic.commitment ||
		len(left.automatic.stages) != len(right.automatic.stages) {
		return false
	}
	for index := range left.automatic.stages {
		leftStage, rightStage := left.automatic.stages[index], right.automatic.stages[index]
		if leftStage.stableID != rightStage.stableID ||
			!bytes.Equal(
				leftStage.selector.CanonicalBytes(),
				rightStage.selector.CanonicalBytes(),
			) || leftStage.lookup.operator == nil || rightStage.lookup.operator == nil ||
			!lookupResolutionContractsEqual(
				*leftStage.lookup.operator,
				*rightStage.lookup.operator,
			) || !lookupResolutionEqual(
			leftStage.lookup.resolution,
			rightStage.lookup.resolution,
		) {
			return false
		}
	}
	return true
}

type compiledLookupOutput struct {
	name                    string
	valueSQL                string
	valueArgs               []any
	field                   fieldState
	existsProjection        string
	existsArgs              []any
	typeProjection          string
	typeArgs                []any
	textProjection          string
	textArgs                []any
	semanticBytesProjection string
	semanticBytesArgs       []any
	descendantProjection    string
	descendantArgs          []any
	namesProjection         string
	namesArgs               []any
	typesProjection         string
	typesArgs               []any
	metadataProjection      string
	metadataArgs            []any
	privateColumns          []string
}

// compiledLookupProjection pairs one private lookup projection expression with
// the bind values it carries.
type compiledLookupProjection struct {
	sql  string
	args []any
}

// projections returns the private projections of a lookup output in placeholder
// order. Both the projection list and the bind list must iterate this order.
func (output compiledLookupOutput) projections() []compiledLookupProjection {
	return []compiledLookupProjection{
		{output.existsProjection, output.existsArgs},
		{output.typeProjection, output.typeArgs},
		{output.textProjection, output.textArgs},
		{output.semanticBytesProjection, output.semanticBytesArgs},
		{output.descendantProjection, output.descendantArgs},
		{output.namesProjection, output.namesArgs},
		{output.typesProjection, output.typesArgs},
		{output.metadataProjection, output.metadataArgs},
	}
}

// compiledLookupStageOptions is compiler-private authority used only by the
// generated automatic-lookup group. Authored lookup stages use the zero value.
// Automatic keys and selector results are frozen against the one post-Tier-1
// relation before any automatic output is published, then carried through the
// ordered writes as private columns.
type compiledLookupStageOptions struct {
	keys               []compiledFrozenLookupKey
	selectorTuple      string
	passthroughColumns []string
}

type compiledFrozenLookupKey struct {
	valueSQL    string
	eligibleSQL string
}

func compileLookupStage(
	relation compiledRelation,
	state compileState,
	existingArgs []any,
	stage preparedLookupStage,
	stageNumber int,
) (compiledRelation, compileState, []any, int, error) {
	return compileLookupStageWithOptions(
		relation,
		state,
		existingArgs,
		stage,
		stageNumber,
		compiledLookupStageOptions{},
	)
}

func compileLookupStageWithOptions(
	relation compiledRelation,
	state compileState,
	existingArgs []any,
	stage preparedLookupStage,
	stageNumber int,
	options compiledLookupStageOptions,
) (compiledRelation, compileState, []any, int, error) {
	if stage.operator == nil ||
		!lookupResolutionEqual(stage.resolution, stage.resolution.clone()) {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse lookup: prepared authority is invalid",
		)
	}
	if state.context == nil {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse lookup: compile context is unavailable",
		)
	}
	tableName := fmt.Sprintf("__os_lookup_table_%d", stageNumber)
	rightAlias := quoteIdentifier(tableName)
	matchedColumnName := fmt.Sprintf("__os_lookup_matched_%d", stageNumber)
	matchedColumn := quoteIdentifier(matchedColumnName)
	selectedColumnNames := make([]string, len(stage.selectedColumns))
	selectedNames := make([]string, len(stage.selectedColumns))
	for index := range selectedNames {
		selectedColumnNames[index] = fmt.Sprintf(
			"__os_lookup_%d_column_%d",
			stageNumber,
			index,
		)
		selectedNames[index] = quoteIdentifier(selectedColumnNames[index])
	}
	externalTable, err := newCompiledLookupExternalTableContext(
		state.context.operationContext,
		tableName,
		matchedColumnName,
		stage,
		selectedColumnNames,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}

	if len(options.keys) != 0 && len(options.keys) != len(stage.operator.Keys) {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse lookup: frozen key authority is incomplete",
		)
	}
	joinPredicates := make([]string, 0, len(stage.operator.Keys)+1)
	if options.selectorTuple != "" {
		joinPredicates = append(
			joinPredicates,
			"tupleElement("+options.selectorTuple+", 1) != toUInt8(0)",
		)
	}
	keyArgs := make([]any, 0)
	for index, key := range stage.operator.Keys {
		var valueSQL, eligibleSQL string
		var valueArgs []any
		if len(options.keys) != 0 {
			valueSQL = options.keys[index].valueSQL
			eligibleSQL = options.keys[index].eligibleSQL
			if valueSQL == "" || eligibleSQL == "" ||
				strings.Contains(valueSQL, "?") || strings.Contains(eligibleSQL, "?") {
				return compiledRelation{}, compileState{}, nil, 0, errors.New(
					"compile ClickHouse lookup: frozen key expression is invalid",
				)
			}
		} else {
			var compileErr error
			valueSQL, eligibleSQL, valueArgs, compileErr = compileLookupKey(key, state)
			if compileErr != nil {
				return compiledRelation{}, compileState{}, nil, 0, compileErr
			}
		}
		selectedIndex := stage.keyHeaderIndexes[index]
		joinPredicates = append(joinPredicates,
			"("+eligibleSQL+") AND "+valueSQL+" = "+
				rightAlias+"."+selectedNames[selectedIndex],
		)
		keyArgs = append(keyArgs, valueArgs...)
	}
	leftAlias := quoteIdentifier(fmt.Sprintf("__os_lookup_input_%d", stageNumber))
	joinSQL := "SELECT * FROM (" + relation.sql + ") AS " + leftAlias +
		" LEFT ANY JOIN " + quoteIdentifier(tableName) + " AS " + rightAlias + " ON " +
		strings.Join(joinPredicates, " AND ")
	joined := compiledRelation{
		sql:        joinSQL,
		depth:      relationalNodeDepth(relation.depth, 1),
		ownerRange: stage.operator.Range,
	}
	if err := validateRelationalDepth(joined.depth, joined.ownerRange); err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	joinArgs := make([]any, 0, len(existingArgs)+len(keyArgs))
	joinArgs = append(joinArgs, existingArgs...)
	joinArgs = append(joinArgs, keyArgs...)
	if strings.Count(joined.sql, "?") != len(joinArgs) {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse lookup: join placeholder order is invalid",
		)
	}

	matchedSQL := "ifNull(" + matchedColumn + ", toUInt8(0)) != 0"
	next := cloneCompileState(state)
	outputs := make([]compiledLookupOutput, len(stage.operator.Outputs))
	for index, output := range stage.operator.Outputs {
		compiled, err := compileLookupOutput(
			output,
			stage.operator.WriteMode,
			state,
			matchedSQL,
			selectedNames[stage.outputHeaderIndex[index]],
			stageNumber,
			index,
		)
		if err != nil {
			return compiledRelation{}, compileState{}, nil, 0, err
		}
		outputs[index] = compiled
		delete(next.blocked, compiled.name)
		if !slices.Contains(next.publicOrder, compiled.name) {
			next.publicOrder = append(next.publicOrder, compiled.name)
		}
		next.visible[compiled.name] = compiled.field
	}
	// The sparse raw fields payload is immutable input-row authority. Keep it
	// for both matched and unmatched rows; the separately projected lookup
	// destinations shadow same-named dynamic paths for downstream SPL without
	// deleting unrelated open-schema members from the public event.

	liveOldPrivateColumns := livePrivateColumns(state.privateColumns, next.visible)
	for _, column := range options.passthroughColumns {
		if column == "" || strings.Contains(column, "?") {
			return compiledRelation{}, compileState{}, nil, 0, errors.New(
				"compile ClickHouse lookup: passthrough authority is invalid",
			)
		}
		if !slices.Contains(liveOldPrivateColumns, column) {
			liveOldPrivateColumns = append(liveOldPrivateColumns, column)
		}
	}
	next.privateColumns = append([]string(nil), liveOldPrivateColumns...)
	for _, output := range outputs {
		next.privateColumns = append(next.privateColumns, output.privateColumns...)
	}
	valuesByName := make(map[string]string, len(outputs))
	prefixArgs := make([]any, 0)
	privateProjections := make([]string, 0, 8*len(outputs))
	for _, output := range outputs {
		valuesByName[output.name] = output.valueSQL
		for _, expression := range output.projections() {
			if expression.sql != "" {
				privateProjections = append(privateProjections, expression.sql)
			}
		}
	}
	projection := make([]string, 0, len(next.visible)+len(privateProjections)+12)
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if valueSQL, replaced := valuesByName[name]; replaced {
			projection = append(projection, valueSQL+" AS "+publicName)
			for _, output := range outputs {
				if output.name == name {
					prefixArgs = append(prefixArgs, output.valueArgs...)
					break
				}
			}
			continue
		}
		field, ok := state.visible[name]
		if !ok {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
				"compile ClickHouse lookup: field %q has no input value",
				name,
			)
		}
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, privateProjections...)
	for _, output := range outputs {
		for _, expression := range output.projections() {
			if expression.sql != "" {
				prefixArgs = append(prefixArgs, expression.args...)
			}
		}
	}
	outputRelation := joined.selectFrom(
		"SELECT "+strings.Join(projection, ", ")+" FROM ("+joined.sql+") AS "+
			quoteIdentifier(fmt.Sprintf("__os_lookup_output_%d", stageNumber)),
		stage.operator.Range,
	)
	compiledArgs := make([]any, 0, len(prefixArgs)+len(joinArgs))
	compiledArgs = append(compiledArgs, prefixArgs...)
	compiledArgs = append(compiledArgs, joinArgs...)
	if strings.Count(outputRelation.sql, "?") != len(compiledArgs) {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse lookup: output placeholder order is invalid",
		)
	}
	state.context.lookupTables = append(state.context.lookupTables, externalTable)
	return outputRelation, next, compiledArgs, 1, nil
}

func compileLookupKey(
	key plan.LookupKey,
	state compileState,
) (valueSQL, eligibleSQL string, args []any, err error) {
	field, known, err := resolveCompiledField(key.EventField, state)
	if err != nil {
		return "", "", nil, err
	}
	if !known {
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	switch field.kind {
	case fieldKindString:
		eligible := "(" + existsSQL + ") AND isNotNull(" + field.valueSQL + ")"
		if field.textEligibleSQL != "" {
			eligible += " AND (" + field.textEligibleSQL + ")"
		}
		eligible += " AND isValidUTF8(" + field.valueSQL + ")"
		return field.valueSQL, eligible, append([]any(nil), field.existsArgs...), nil
	case fieldKindDynamic:
		typeSQL := dynamicTypeExpression(field)
		valueSQL := "dynamicElement(" + field.valueSQL + ", 'String')"
		eligible := "(" + existsSQL + ") AND " + typeSQL +
			" = 'String' AND isNotNull(" + valueSQL + ") AND isValidUTF8(" +
			valueSQL + ")"
		if field.textEligibleSQL != "" {
			eligible += " AND (" + field.textEligibleSQL + ")"
		}
		if field.storedTypeSQL != "" {
			eligible += " AND " + field.storedTypeSQL + " = toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")"
		}
		return valueSQL, eligible, append([]any(nil), field.existsArgs...), nil
	case fieldKindStringArray:
		// Multivalue keys have the documented no-match behavior. They neither
		// fan out nor raise a runtime error.
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	default:
		// Fixed non-String and null-only fields cannot match a String lookup key.
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
}

func compileLookupOutput(
	output plan.LookupOutput,
	mode plan.LookupWriteMode,
	state compileState,
	matchedSQL string,
	lookupValueSQL string,
	stageNumber int,
	outputIndex int,
) (compiledLookupOutput, error) {
	previous, previousKnown, err := resolveCompiledField(output.EventField, state)
	if err != nil {
		return compiledLookupOutput{}, err
	}
	if err := validateKnowledgeFieldSidecars(
		previous.relativeFieldNamesSQL,
		previous.relativeFieldTypesSQL,
		previous.fieldMetadataVersionSQL,
	); err != nil {
		return compiledLookupOutput{}, fmt.Errorf(
			"compile ClickHouse lookup prior output %q: %w",
			output.EventField.Name,
			err,
		)
	}

	previousPresence := "0"
	previousType := "toUInt8(0)"
	previousOccupied := "0"
	var presenceArgs, occupiedArgs, typeArgs, descendantArgs []any
	if previousKnown {
		previousPresence, presenceArgs = knownFieldPresenceSQL(previous)
		previousOccupied, occupiedArgs = lookupOutputOccupiedSQL(previous)
		previousType, typeArgs, err = knownFieldStoredTypeSQL(previous)
		if err != nil {
			return compiledLookupOutput{}, fmt.Errorf(
				"compile ClickHouse lookup prior output type %q: %w",
				output.EventField.Name,
				err,
			)
		}
	}
	writeSQL := matchedSQL
	writeArgs := []any(nil)
	if mode == plan.LookupWriteModePreserveExisting {
		// OUTPUTNEW treats an explicitly present null as writable. Presence
		// metadata alone cannot distinguish that case from an occupied scalar;
		// require both field presence and a non-null current value.
		writeSQL = "(" + matchedSQL + ") AND NOT ifNull(" + previousOccupied + ", 0)"
		writeArgs = append(writeArgs, occupiedArgs...)
	}

	prefix := fmt.Sprintf("__os_lookup_%d_output_%d", stageNumber, outputIndex)
	existsAlias := quoteIdentifier(prefix + "_exists")
	typeAlias := quoteIdentifier(prefix + "_type")
	existsProjection := "toUInt8(if(" + writeSQL + ", 1, ifNull(" +
		previousPresence + ", 0))) AS " + existsAlias
	typeProjection := "toUInt8(if(" + writeSQL + ", toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		previousType + ")) AS " + typeAlias

	previousValue := "CAST(NULL AS Nullable(String))"
	kind := fieldKindString
	if previousKnown {
		previousValue = previous.valueSQL
		if previous.kind != fieldKindString {
			kind = fieldKindDynamic
			previousValue = "CAST(" + previous.valueSQL + " AS Dynamic)"
		}
	}
	selectedValue := lookupValueSQL
	if kind == fieldKindDynamic {
		selectedValue = "CAST(" + lookupValueSQL + " AS Dynamic)"
	}
	valueSQL := "if(" + writeSQL + ", " + selectedValue + ", " + previousValue + ")"

	textAlias, textProjection := "", ""
	if previousKnown && previous.textEligibleSQL != "" {
		textAlias = quoteIdentifier(prefix + "_text")
		textProjection = "toUInt8(if(" + writeSQL + ", 1, ifNull(" +
			previous.textEligibleSQL + ", 0))) AS " + textAlias
	}
	semanticAlias, semanticProjection := "", ""
	if previousKnown && previous.kind == fieldKindString && previous.stringOrBytes {
		if previous.semanticBytesSQL == "" {
			return compiledLookupOutput{}, errors.New(
				"compile ClickHouse lookup: prior String-or-Bytes output lacks semantic provenance",
			)
		}
		semanticAlias = quoteIdentifier(prefix + "_semantic_bytes")
		semanticProjection = "toUInt8(if(" + writeSQL + ", 0, ifNull(" +
			previous.semanticBytesSQL + ", 0))) AS " + semanticAlias
	}
	descendantAlias, descendantProjection := "", ""
	if previousKnown && previous.descendantSQL != "" {
		descendantAlias = quoteIdentifier(prefix + "_descendant")
		descendantProjection = "toUInt8(NOT (" + writeSQL + ") AND (" +
			previous.descendantSQL + ")) AS " + descendantAlias
		descendantArgs = append(descendantArgs, previous.descendantArgs...)
	}
	namesAlias, namesProjection := "", ""
	typesAlias, typesProjection := "", ""
	metadataAlias, metadataProjection := "", ""
	if previousKnown && previous.relativeFieldNamesSQL != "" {
		namesAlias = quoteIdentifier(prefix + "_names")
		typesAlias = quoteIdentifier(prefix + "_types")
		metadataAlias = quoteIdentifier(prefix + "_metadata")
		namesProjection = "if(" + writeSQL + ", " +
			knowledgeEmptyRelativeFieldNamesSQL() + ", " +
			previous.relativeFieldNamesSQL + ") AS " + namesAlias
		typesProjection = "if(" + writeSQL + ", " +
			knowledgeEmptyRelativeFieldTypesSQL() + ", " +
			previous.relativeFieldTypesSQL + ") AS " + typesAlias
		metadataProjection = "toUInt8(if(" + writeSQL + ", 0, " +
			previous.fieldMetadataVersionSQL + ")) AS " + metadataAlias
	}

	publicName := quoteIdentifier(output.EventField.Name)
	maxStringBytes := uint64(MaximumLookupCellBytes)
	if previousKnown {
		maxStringBytes = max(maxStringBytes, fieldStateStringByteBound(previous))
	}
	field := fieldState{
		valueSQL:                publicName,
		maxStringBytes:          maxStringBytes,
		textEligibleSQL:         textAlias,
		storedTypeSQL:           typeAlias,
		existsSQL:               existsAlias,
		descendantSQL:           descendantAlias,
		relativeFieldNamesSQL:   namesAlias,
		relativeFieldTypesSQL:   typesAlias,
		fieldMetadataVersionSQL: metadataAlias,
		semanticBytesSQL:        semanticAlias,
		stringOrBytes:           semanticAlias != "",
		stringOrBytesNullable:   semanticAlias != "",
		kind:                    kind,
		caseSensitive:           false,
		materializeForPredicate: true,
	}
	if kind == fieldKindDynamic {
		field.dynamicTypeSQL = "dynamicType(" + publicName + ")"
	}
	privateColumns := []string{existsAlias, typeAlias}
	for _, column := range []string{
		textAlias,
		semanticAlias,
		descendantAlias,
		namesAlias,
		typesAlias,
		metadataAlias,
	} {
		if column != "" {
			privateColumns = append(privateColumns, column)
		}
	}
	valueArgs := append([]any(nil), writeArgs...)
	existsArgs := append(append([]any(nil), writeArgs...), presenceArgs...)
	projectedTypeArgs := append(append([]any(nil), writeArgs...), typeArgs...)
	textArgs := append([]any(nil), writeArgs...)
	semanticArgs := append([]any(nil), writeArgs...)
	projectedDescendantArgs := append(
		append([]any(nil), writeArgs...),
		descendantArgs...,
	)
	namesArgs := append([]any(nil), writeArgs...)
	typesArgs := append([]any(nil), writeArgs...)
	metadataArgs := append([]any(nil), writeArgs...)
	return compiledLookupOutput{
		name:                    output.EventField.Name,
		valueSQL:                valueSQL,
		valueArgs:               valueArgs,
		field:                   field,
		existsProjection:        existsProjection,
		existsArgs:              existsArgs,
		typeProjection:          typeProjection,
		typeArgs:                projectedTypeArgs,
		textProjection:          textProjection,
		textArgs:                textArgs,
		semanticBytesProjection: semanticProjection,
		semanticBytesArgs:       semanticArgs,
		descendantProjection:    descendantProjection,
		descendantArgs:          projectedDescendantArgs,
		namesProjection:         namesProjection,
		namesArgs:               namesArgs,
		typesProjection:         typesProjection,
		typesArgs:               typesArgs,
		metadataProjection:      metadataProjection,
		metadataArgs:            metadataArgs,
		privateColumns:          privateColumns,
	}, nil
}

func lookupOutputOccupiedSQL(field fieldState) (string, []any) {
	presence := field.existsSQL
	if presence == "" {
		presence = "1"
	}
	occupied := "((" + presence + ") AND isNotNull(" + field.valueSQL + "))"
	args := append([]any(nil), field.existsArgs...)
	if field.descendantSQL != "" {
		// A flattened container may have no materialized parent value while one
		// or more descendants are present. OUTPUTNEW must preserve that complete
		// container independently of direct-parent nullability.
		occupied = "(" + occupied + " OR (" + field.descendantSQL + "))"
		args = append(args, field.descendantArgs...)
	}
	return occupied, args
}
