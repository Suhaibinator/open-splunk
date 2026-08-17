package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// retainedAutomaticLookup is the minimum backend-neutral authority needed to
// place automatic lookups again when a completed search is compiled into a
// derived query. Physical identities and selected cells remain exclusively in
// the sealed lookup external tables.
type retainedAutomaticLookup struct {
	stableID string
	lookup   plan.Lookup
	selector knowledgeprogram.Selector
}

// retainedAutomaticLookupSelectorCharge matches the conservative per-object
// selector/NFA reservation used by knowledgeprogram.Program.
const retainedAutomaticLookupSelectorCharge uint64 = 32 << 10

func retainedAutomaticLookupsFromPreparation(
	preparation preparedLookupCompilation,
) []retainedAutomaticLookup {
	if preparation.automatic == nil {
		return nil
	}
	result := make([]retainedAutomaticLookup, len(preparation.automatic.stages))
	for index, stage := range preparation.automatic.stages {
		result[index] = retainedAutomaticLookup{
			stableID: strings.Clone(stage.stableID),
			lookup:   cloneLookupResolutionContract(*stage.lookup.operator),
			selector: stage.selector,
		}
	}
	return result
}

func cloneRetainedAutomaticLookups(
	lookups []retainedAutomaticLookup,
) []retainedAutomaticLookup {
	if lookups == nil {
		return nil
	}
	result := make([]retainedAutomaticLookup, len(lookups))
	for index, lookup := range lookups {
		result[index] = retainedAutomaticLookup{
			stableID: strings.Clone(lookup.stableID),
			lookup:   cloneLookupResolutionContract(lookup.lookup),
			selector: lookup.selector,
		}
	}
	return result
}

func validateRetainedAutomaticLookups(
	lookups []retainedAutomaticLookup,
	tables []compiledLookupExternalTable,
) bool {
	if len(lookups) > MaximumLookupStagesPerQuery || len(lookups) > len(tables) {
		return false
	}
	for index, lookup := range lookups {
		if lookup.stableID == "" || len(lookup.stableID) > 255 ||
			!utf8.ValidString(lookup.stableID) ||
			strings.IndexByte(lookup.stableID, 0) >= 0 {
			return false
		}
		normalized, err := normalizeLookupResolutionContract(lookup.lookup)
		if err != nil || !lookupResolutionContractsEqual(normalized, lookup.lookup) {
			return false
		}
		canonical := lookup.selector.CanonicalBytes()
		if len(canonical) == 0 ||
			len(canonical) > knowledge.MaximumSelectorNormalizedBytes {
			return false
		}
		constrained := false
		for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
			_, present := lookup.selector.RuntimeProgram(dimension)
			constrained = constrained || present
		}
		if lookup.selector.IsUnrestricted() == constrained {
			return false
		}
		if index > 0 {
			previous := lookups[index-1]
			if previous.lookup.DefinitionName > lookup.lookup.DefinitionName ||
				previous.lookup.DefinitionName == lookup.lookup.DefinitionName &&
					previous.stableID >= lookup.stableID {
				return false
			}
		}
		table := tables[index]
		if table.logicalID != lookup.stableID ||
			table.definitionName != lookup.lookup.DefinitionName {
			return false
		}
	}
	return true
}

func writeRetainedAutomaticLookups(
	writer hash.Hash,
	lookups []retainedAutomaticLookup,
	tables []compiledLookupExternalTable,
) bool {
	if writer == nil || !validateRetainedAutomaticLookups(lookups, tables) {
		return false
	}
	writeBool(writer, lookups == nil)
	writeUint64(writer, uint64(len(lookups)))
	for _, lookup := range lookups {
		writeTokenPart(writer, lookup.stableID)
		writeLookupReplayContract(writer, lookup.lookup)
		canonical := lookup.selector.CanonicalBytes()
		writeUint64(writer, uint64(len(canonical)))
		_, _ = writer.Write(canonical)
	}
	return true
}

func writeLookupReplayContract(writer hash.Hash, lookup plan.Lookup) {
	writeTokenPart(writer, lookup.DefinitionName)
	writeUint64(writer, uint64(lookup.WriteMode))
	writeUint64(writer, uint64(len(lookup.Keys)))
	for _, key := range lookup.Keys {
		writeTokenPart(writer, key.LookupField)
		writeTokenPart(writer, key.EventField.Name)
	}
	writeUint64(writer, uint64(len(lookup.Outputs)))
	for _, output := range lookup.Outputs {
		writeTokenPart(writer, output.LookupField)
		writeTokenPart(writer, output.EventField.Name)
	}
}

func retainedAutomaticLookupsBytes(
	total uint64,
	lookups []retainedAutomaticLookup,
	tables []compiledLookupExternalTable,
) (uint64, bool) {
	if !validateRetainedAutomaticLookups(lookups, tables) {
		return 0, false
	}
	var ok bool
	total, ok = retainedAdd(
		total,
		uint64(cap(lookups))*uint64(unsafe.Sizeof(retainedAutomaticLookup{})),
	)
	if !ok {
		return 0, false
	}
	for _, lookup := range lookups {
		for _, value := range []string{lookup.stableID, lookup.lookup.DefinitionName} {
			total, ok = retainedAdd(total, uint64(len(value)))
			if !ok {
				return 0, false
			}
		}
		total, ok = retainedAdd(
			total,
			uint64(cap(lookup.lookup.Keys))*uint64(unsafe.Sizeof(plan.LookupKey{})),
		)
		if !ok {
			return 0, false
		}
		for _, key := range lookup.lookup.Keys {
			total, ok = retainedStringSlice(total, []string{
				key.LookupField,
				key.EventField.Name,
			})
			if !ok {
				return 0, false
			}
			total, ok = retainedStringSlice(total, key.EventField.Path)
			if !ok {
				return 0, false
			}
		}
		total, ok = retainedAdd(
			total,
			uint64(cap(lookup.lookup.Outputs))*uint64(unsafe.Sizeof(plan.LookupOutput{})),
		)
		if !ok {
			return 0, false
		}
		for _, output := range lookup.lookup.Outputs {
			total, ok = retainedStringSlice(total, []string{
				output.LookupField,
				output.EventField.Name,
			})
			if !ok {
				return 0, false
			}
			total, ok = retainedStringSlice(total, output.EventField.Path)
			if !ok {
				return 0, false
			}
		}
		total, ok = retainedAdd(
			total,
			retainedAutomaticLookupSelectorCharge+
				uint64(len(lookup.selector.CanonicalBytes())),
		)
		if !ok {
			return 0, false
		}
	}
	return total, true
}

// HasLookupAuthorityContext reports whether this sealed executable retains
// exact lookup assets. Invalid authority fails closed.
func (compiled CompiledQuery) HasLookupAuthorityContext(
	ctx context.Context,
) (bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return false, err
	}
	if !valid {
		return false, errors.New("open retained ClickHouse lookup authority: execution seal is invalid")
	}
	return len(compiled.lookupTables) != 0, nil
}

func (compiled CompiledQuery) HasLookupAuthority() bool {
	present, err := compiled.HasLookupAuthorityContext(context.Background())
	return err == nil && present
}

// WithRetainedLookupAuthorityContext reattaches the exact lookup authority
// retained by source to a logical plan rebuilt from the same completed search.
// It never resolves names through a current catalog. Automatic placement is
// replayed from sealed descriptors; selected cells are shared from the sealed
// external-table backing.
func (compiler Compiler) WithRetainedLookupAuthorityContext(
	ctx context.Context,
	source CompiledQuery,
	query *plan.Query,
) (*plan.Query, Compiler, error) {
	if ctx == nil {
		return nil, Compiler{}, errors.New(
			"restore retained ClickHouse lookup authority: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, Compiler{}, err
	}
	valid, err := source.hasValidExecutionSealContext(ctx)
	if err != nil {
		return nil, Compiler{}, err
	}
	if !valid || query == nil || len(query.Operators) == 0 {
		return nil, Compiler{}, errors.New(
			"restore retained ClickHouse lookup authority: source or query is invalid",
		)
	}
	if err := plan.ValidateAutomaticLookupIntegrity(query); err != nil {
		return nil, Compiler{}, fmt.Errorf(
			"restore retained ClickHouse lookup authority: %w",
			err,
		)
	}
	restored := query
	if len(source.automaticLookupReplay) != 0 {
		specs := make([]plan.AutomaticLookupSpec, len(source.automaticLookupReplay))
		for index, replay := range source.automaticLookupReplay {
			specs[index] = plan.AutomaticLookupSpec{
				StableID: strings.Clone(replay.stableID),
				Lookup:   cloneLookupResolutionContract(replay.lookup),
				Selector: replay.selector,
			}
		}
		restored, err = plan.InjectAutomaticLookupGroup(query, specs)
		if err != nil {
			return nil, Compiler{}, fmt.Errorf(
				"restore retained ClickHouse automatic lookups: %w",
				err,
			)
		}
	}

	type logicalLookupStage struct {
		lookup    plan.Lookup
		logicalID string
	}
	stages := make([]logicalLookupStage, 0, len(source.lookupTables))
	for _, operator := range restored.Operators[1:] {
		switch operator := operator.(type) {
		case *plan.AutomaticLookupGroup:
			for _, automatic := range operator.Lookups() {
				stages = append(stages, logicalLookupStage{
					lookup:    automatic.LogicalLookup(),
					logicalID: automatic.StableID(),
				})
			}
		case *plan.Lookup:
			if operator != nil {
				stages = append(stages, logicalLookupStage{lookup: *operator})
			}
		}
	}
	if len(stages) != len(source.lookupTables) {
		return nil, Compiler{}, errors.New(
			"restore retained ClickHouse lookup authority: logical and physical stage counts disagree",
		)
	}
	resolutions := make([]LookupResolution, len(stages))
	for index, stage := range stages {
		logicalID := stage.logicalID
		if logicalID == "" {
			logicalID = source.lookupTables[index].logicalID
		}
		resolution, restoreErr := lookupResolutionFromRetainedTable(
			ctx,
			stage.lookup,
			logicalID,
			source.lookupTables[index],
		)
		if restoreErr != nil {
			return nil, Compiler{}, fmt.Errorf(
				"restore retained ClickHouse lookup authority stage %d: %w",
				index,
				restoreErr,
			)
		}
		resolutions[index] = resolution
	}
	configured, err := compiler.WithLookupResolutionsContext(ctx, resolutions)
	if err != nil {
		return nil, Compiler{}, err
	}
	return restored, configured, nil
}

func lookupResolutionFromRetainedTable(
	ctx context.Context,
	contract plan.Lookup,
	logicalID string,
	table compiledLookupExternalTable,
) (LookupResolution, error) {
	if err := validateCompiledLookupExternalTablesContext(
		ctx,
		[]compiledLookupExternalTable{table},
	); err != nil {
		return LookupResolution{}, err
	}
	normalized, err := normalizeLookupResolutionContract(contract)
	if err != nil {
		return LookupResolution{}, err
	}
	if table.definitionName != normalized.DefinitionName ||
		table.logicalID != logicalID {
		return LookupResolution{}, errors.New(
			"logical definition disagrees with retained physical authority",
		)
	}
	headers := make([]string, 0, len(normalized.Keys)+len(normalized.Outputs))
	appendHeader := func(value string) {
		if !slices.Contains(headers, value) {
			headers = append(headers, strings.Clone(value))
		}
	}
	for _, key := range normalized.Keys {
		appendHeader(key.LookupField)
	}
	for _, output := range normalized.Outputs {
		appendHeader(output.LookupField)
	}
	if len(headers) != len(table.columns) || len(headers) != len(table.backing.values) {
		return LookupResolution{}, errors.New(
			"logical lookup schema disagrees with retained selected columns",
		)
	}
	payloadBytes := table.backing.payloadBytes
	for _, header := range headers {
		var ok bool
		payloadBytes, ok = retainedAdd(payloadBytes, uint64(len(header)))
		if !ok || payloadBytes > MaximumLookupAssetBytes {
			return LookupResolution{}, errors.New(
				"retained lookup payload exceeds the asset byte limit",
			)
		}
	}
	resolution := LookupResolution{
		tenantID:       strings.Clone(table.tenantID),
		definitionName: strings.Clone(table.definitionName),
		logicalID:      strings.Clone(table.logicalID),
		logicalVersion: table.logicalVersion,
		objectID:       strings.Clone(table.objectID),
		version:        table.version,
		sizeBytes:      table.sizeBytes,
		contentSHA256:  table.contentSHA256,
		contract:       normalized,
		contractSet:    true,
		headers:        headers,
		columns:        table.backing.values,
	}
	measured, err := validateLookupResolutionAndMeasureContext(ctx, resolution)
	if err != nil {
		return LookupResolution{}, err
	}
	if measured != payloadBytes {
		return LookupResolution{}, errors.New(
			"retained lookup payload measurement disagrees",
		)
	}
	resolution.backing = newLookupResolutionBacking(resolution, measured)
	if err := validateLookupResolutionContext(ctx, resolution); err != nil {
		return LookupResolution{}, err
	}
	return resolution, nil
}
