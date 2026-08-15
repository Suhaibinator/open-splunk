package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// MaximumFieldCatalogFields is the hard cross-layer bound for one complete
	// field catalog. Callers may configure a lower limit, but the compiler,
	// executor, and analysis service must all reject larger contracts.
	MaximumFieldCatalogFields uint32 = 10_000
	// The prerequisite fixed known domain admits one overflow-sentinel profile
	// beyond the public hard maximum. Runtime dynamic names remain outside the
	// fixed vectors and are controlled by the independent grouped-result seal.
	maximumPrerequisiteFieldCatalogKnownFields = int(MaximumFieldCatalogFields) + 1

	FieldCatalogRowKindColumn       = "__os_field_catalog_row_kind"
	FieldCatalogNameColumn          = "__os_field_catalog_name"
	FieldCatalogObservedTypesColumn = "__os_field_catalog_observed_types"
	FieldCatalogEventCountColumn    = "__os_field_catalog_event_count"
	FieldCatalogNullCountColumn     = "__os_field_catalog_null_count"
	FieldCatalogMissingCountColumn  = "__os_field_catalog_missing_count"
	FieldCatalogTotalEventsColumn   = "__os_field_catalog_total_events"
	FieldCatalogInvalidColumn       = "__os_field_catalog_invalid"
)

// FieldCatalogSpec bounds the number of field profiles admitted from one
// completed event search. The compiler deliberately requests one extra row so
// the executor can reject overflow instead of returning a silent truncation.
type FieldCatalogSpec struct {
	MaximumFields uint32
}

// CompiledFieldCatalog is one immutable, parameterized field-catalog query.
type CompiledFieldCatalog struct {
	SQL  string
	Args []any
	Spec FieldCatalogSpec

	knowledgeGeneratedFields uint32
	readScope                compiledReadScope
	executionAuthority       *derivedExecutionAuthority
}

// KnowledgeGeneratedFields returns the exact number of fields admitted from
// the compiler-owned knowledge prelude. The value is available only while the
// complete derived executable remains sealed; hand-built or mutated catalogs
// cannot open the resource evidence.
func (compiled CompiledFieldCatalog) KnowledgeGeneratedFields() (uint32, bool) {
	count, ok, _ := compiled.KnowledgeGeneratedFieldsContext(context.Background())
	return count, ok
}

func (compiled CompiledFieldCatalog) KnowledgeGeneratedFieldsContext(
	ctx context.Context,
) (uint32, bool, error) {
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return 0, false, err
	}
	if compiled.knowledgeGeneratedFields > MaximumClickHouseKnowledgeGeneratedFields ||
		!valid {
		return 0, false, nil
	}
	return compiled.knowledgeGeneratedFields, true, nil
}

func fieldCatalogKnowledgeGeneratedFieldsFromSourceContext(
	ctx context.Context,
	source CompiledQuery,
) (uint32, bool, error) {
	valid, err := source.hasValidExecutionSealContext(ctx)
	if err != nil {
		return 0, false, err
	}
	if !valid {
		return 0, false, nil
	}
	if source.knowledgeEvidence == nil {
		return 0, true, nil
	}
	count := source.knowledgeEvidence.prelude.charges.GeneratedFields
	return count, count <= MaximumClickHouseKnowledgeGeneratedFields, nil
}

// CompileFieldCatalog compiles an exact catalog over the final event relation.
func (c Compiler) CompileFieldCatalog(query *plan.Query, spec FieldCatalogSpec) (CompiledFieldCatalog, error) {
	return c.CompileFieldCatalogContext(context.Background(), query, spec)
}

func (c Compiler) CompileFieldCatalogContext(
	ctx context.Context,
	query *plan.Query,
	spec FieldCatalogSpec,
) (CompiledFieldCatalog, error) {
	if ctx == nil {
		return CompiledFieldCatalog{}, errors.New(
			"compile ClickHouse field catalog: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return CompiledFieldCatalog{}, err
	}
	if spec.MaximumFields == 0 || spec.MaximumFields > MaximumFieldCatalogFields {
		return CompiledFieldCatalog{}, fmt.Errorf(
			"compile ClickHouse field catalog: MaximumFields must be between 1 and %d", MaximumFieldCatalogFields,
		)
	}
	compiled, err := c.compileEventAnalysisContext(ctx, query, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		aliasSequence int,
	) (CompiledQuery, error) {
		policy := eventAnalysisFinalizationPolicyFor(state.chronologicalBarriers)
		contract := fieldCatalogResultContractFor(policy)
		compiled, finalizeErr := finalizeFieldCatalog(
			relation,
			state,
			args,
			spec,
			scan.Range,
			policy,
		)
		if finalizeErr != nil {
			return CompiledQuery{}, finalizeErr
		}
		return wrapEventAnalysisValidation(
			compiled,
			state,
			contract,
			aliasSequence,
		)
	})
	if err != nil {
		return CompiledFieldCatalog{}, err
	}
	knowledgeGeneratedFields, ok, evidenceErr :=
		fieldCatalogKnowledgeGeneratedFieldsFromSourceContext(ctx, compiled)
	if evidenceErr != nil {
		return CompiledFieldCatalog{}, evidenceErr
	}
	if !ok {
		return CompiledFieldCatalog{}, errors.New(
			"compile ClickHouse field catalog: generated-field resource evidence is invalid",
		)
	}
	result := CompiledFieldCatalog{
		SQL:                      compiled.SQL,
		Args:                     compiled.Args,
		Spec:                     spec,
		knowledgeGeneratedFields: knowledgeGeneratedFields,
		readScope:                compiled.readScope,
	}
	result.executionAuthority, err = sealCompiledFieldCatalogExecutionContext(
		ctx,
		compiled,
		result,
	)
	if err != nil {
		return CompiledFieldCatalog{}, err
	}
	return result, nil
}

func fieldCatalogResultContract() eventAnalysisResultContract {
	return eventAnalysisResultContract{
		sourceFanout: eventStatsCatalogSourceFanout,
		columns: []string{
			FieldCatalogRowKindColumn,
			FieldCatalogNameColumn,
			FieldCatalogObservedTypesColumn,
			FieldCatalogEventCountColumn,
			FieldCatalogNullCountColumn,
			FieldCatalogMissingCountColumn,
			FieldCatalogTotalEventsColumn,
			FieldCatalogInvalidColumn,
		},
		order: quoteIdentifier(FieldCatalogRowKindColumn) + " ASC, " +
			quoteIdentifier(FieldCatalogNameColumn) + " ASC",
	}
}

func fieldCatalogResultContractFor(
	policy eventAnalysisFinalizationPolicy,
) eventAnalysisResultContract {
	contract := fieldCatalogResultContract()
	if !policy.materializeSharedCTEs {
		contract.sourceFanout = eventStatsOrdinarySourceFanout
	}
	return contract
}

const (
	fieldCatalogSourceCTE         = "__os_field_catalog_source"
	fieldCatalogTotalsCTE         = "__os_field_catalog_totals"
	fieldCatalogKnownRowsCTE      = "__os_field_catalog_known_rows"
	fieldCatalogProfilesCTE       = "__os_field_catalog_profiles"
	fieldCatalogLimitedCTE        = "__os_field_catalog_limited"
	fieldCatalogDynamicName       = "__os_field_catalog_dynamic_name"
	fieldCatalogStoredType        = "__os_field_catalog_stored_type"
	fieldCatalogPresent           = "__os_field_catalog_present"
	fieldCatalogType              = "__os_field_catalog_type"
	fieldCatalogProfileName       = "__os_field_catalog_profile_name"
	fieldCatalogProfileTypes      = "__os_field_catalog_profile_types"
	fieldCatalogProfileEvents     = "__os_field_catalog_profile_events"
	fieldCatalogProfileNulls      = "__os_field_catalog_profile_nulls"
	fieldCatalogProfileMissing    = "__os_field_catalog_profile_missing"
	fieldCatalogProfileTotal      = "__os_field_catalog_profile_total"
	fieldCatalogMetadataInvalid   = "__os_field_catalog_metadata_invalid"
	fieldCatalogKnownNames        = "__os_field_catalog_known_names"
	fieldCatalogKnownTypes        = "__os_field_catalog_known_types"
	fieldCatalogKnownEvents       = "__os_field_catalog_known_events"
	fieldCatalogKnownNulls        = "__os_field_catalog_known_nulls"
	fieldCatalogKnownMissing      = "__os_field_catalog_known_missing"
	fieldCatalogKnownTotals       = "__os_field_catalog_known_total_events"
	fieldCatalogRowsCTE           = "__os_field_catalog_rows"
	fieldCatalogObservationsCTE   = "__os_field_catalog_observations"
	fieldCatalogGroupsCTE         = "__os_field_catalog_groups"
	fieldCatalogControlledCTE     = "__os_field_catalog_controlled"
	fieldCatalogExpandedCTE       = "__os_field_catalog_expanded"
	fieldCatalogObservation       = "__os_field_catalog_observation"
	fieldCatalogObservedKind      = "__os_field_catalog_observed_kind"
	fieldCatalogObservedName      = "__os_field_catalog_observed_name"
	fieldCatalogObservedType      = "__os_field_catalog_observed_type"
	fieldCatalogEventWeight       = "__os_field_catalog_event_weight"
	fieldCatalogNullWeight        = "__os_field_catalog_null_weight"
	fieldCatalogTotalWeight       = "__os_field_catalog_total_weight"
	fieldCatalogInvalidEvidence   = "__os_field_catalog_invalid_evidence"
	fieldCatalogKnownCounts       = "__os_field_catalog_known_counts"
	fieldCatalogKnownTypeMasks    = "__os_field_catalog_known_type_masks"
	fieldCatalogGroupKind         = "__os_field_catalog_group_kind"
	fieldCatalogGroupName         = "__os_field_catalog_group_name"
	fieldCatalogGroupTypes        = "__os_field_catalog_group_types"
	fieldCatalogGroupEvents       = "__os_field_catalog_group_events"
	fieldCatalogGroupNulls        = "__os_field_catalog_group_nulls"
	fieldCatalogGroupTotal        = "__os_field_catalog_group_total"
	fieldCatalogGroupInvalid      = "__os_field_catalog_group_invalid"
	fieldCatalogGlobalTotal       = "__os_field_catalog_global_total"
	fieldCatalogGlobalInvalid     = "__os_field_catalog_global_invalid"
	fieldCatalogOutputTuple       = "__os_field_catalog_output_tuple"
	fieldCatalogCandidates        = "__os_field_catalog_candidates"
	fieldCatalogRelativeNames     = "__os_field_catalog_relative_names"
	fieldCatalogRelativeTypes     = "__os_field_catalog_relative_types"
	fieldCatalogRelativeVersion   = "__os_field_catalog_relative_version"
	fieldCatalogRawRowBinding     = "__os_field_catalog_raw_row_binding"
	fieldCatalogRowBinding        = "__os_field_catalog_row_binding"
	fieldCatalogPackedRows        = "__os_field_catalog_packed_rows"
	fieldCatalogPackedRow         = "__os_field_catalog_packed_row"
	fieldCatalogPackedKnownCounts = "__os_field_catalog_packed_known_counts"
	fieldCatalogPackedKnownMasks  = "__os_field_catalog_packed_known_masks"
	fieldCatalogSidecarInvalid    = "__os_field_catalog_sidecar_invalid"
	fieldCatalogSidecarCandidates = "__os_field_catalog_sidecar_candidates"
)

type compiledKnownField struct {
	name               string
	presenceSQL        string
	presenceArgs       []any
	typeSQL            string
	typeArgs           []any
	relativeNamesSQL   string
	relativeTypesSQL   string
	metadataVersionSQL string
}

func finalizeFieldCatalog(
	relation compiledRelation,
	state compileState,
	args []any,
	spec FieldCatalogSpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
) (CompiledQuery, error) {
	if !state.eventRows {
		return CompiledQuery{}, errors.New("compile ClickHouse field catalog: final relation is not an event relation")
	}

	knownNames := make([]string, 0, len(state.visible))
	for name := range state.visible {
		knownNames = append(knownNames, name)
	}
	sort.Strings(knownNames)

	shadows := make([]string, 0, len(state.visible)+len(state.blocked))
	shadowSet := make(map[string]struct{}, len(state.visible)+len(state.blocked))
	for name := range state.visible {
		shadowSet[name] = struct{}{}
	}
	for name := range state.blocked {
		shadowSet[name] = struct{}{}
	}
	for name := range shadowSet {
		shadows = append(shadows, name)
	}
	sort.Strings(shadows)
	prefixes := sortedSetValues(state.blockedPrefixes)
	knownFields, err := compileKnownFields(state, knownNames)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !policy.materializeSharedCTEs {
		return finalizePrerequisiteFieldCatalog(
			relation,
			args,
			knownFields,
			shadows,
			prefixes,
			state.allowDynamic,
			spec,
			ownerRange,
			policy,
		)
	}

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192 + len(knownNames)*768)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	writeCTEOpening(&sql, policy.materializeSharedCTEs)
	sql.WriteString(relation.sql)
	sql.WriteString("), ")
	if len(knownFields) > 0 {
		writeKnownFieldRows(&sql, knownFields)
		for _, known := range knownFields {
			args = append(args, known.presenceArgs...)
			args = append(args, known.typeArgs...)
		}
		sql.WriteString(", ")
	}

	// The header is the authority for metadata usability. Every row in the
	// final relation must use the current aligned metadata schema; a bad row
	// invalidates the whole catalog rather than allowing partial type guesses.
	sql.WriteString(q(fieldCatalogTotalsCTE))
	sql.WriteString(" AS (SELECT count() AS ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(", toUInt8(countIf(")
	writeAlignedFieldMetadataInvalidPredicate(&sql)
	sql.WriteString(")) > 0) AS ")
	sql.WriteString(q(fieldCatalogMetadataInvalid))
	if len(knownFields) > 0 {
		writeKnownAggregateColumns(&sql, knownFields)
	}
	sql.WriteString(" FROM ")
	if len(knownFields) > 0 {
		sql.WriteString(q(fieldCatalogKnownRowsCTE))
	} else {
		sql.WriteString(q(fieldCatalogSourceCTE))
	}
	sql.WriteString("), ")
	args = append(args,
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	)
	if len(knownFields) > 0 {
		for _, known := range knownFields {
			args = append(args, known.name)
		}
	}

	sql.WriteString(q(fieldCatalogProfilesCTE))
	sql.WriteString(" AS (")
	writeDynamicFieldProfiles(&sql)
	args = append(args, shadows, prefixes, state.allowDynamic)
	if len(knownFields) > 0 {
		sql.WriteString(" UNION ALL ")
		writeKnownFieldProfiles(&sql)
	}
	sql.WriteString("), ")

	// Limit the profile relation, not the final union, so the header cannot be
	// displaced. MaximumFields+1 is an overflow sentinel consumed atomically by
	// the executor.
	sql.WriteString(q(fieldCatalogLimitedCTE))
	sql.WriteString(" AS (SELECT * FROM ")
	sql.WriteString(q(fieldCatalogProfilesCTE))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(fieldCatalogProfileName))
	sql.WriteString(" ASC LIMIT ?)")
	args = append(args, uint64(spec.MaximumFields)+1)

	sql.WriteString(" SELECT * FROM (SELECT toUInt8(0) AS ")
	sql.WriteString(q(FieldCatalogRowKindColumn))
	sql.WriteString(", CAST('' AS String) AS ")
	sql.WriteString(q(FieldCatalogNameColumn))
	sql.WriteString(", CAST([], 'Array(UInt8)') AS ")
	sql.WriteString(q(FieldCatalogObservedTypesColumn))
	for _, column := range []string{
		FieldCatalogEventCountColumn,
		FieldCatalogNullCountColumn,
		FieldCatalogMissingCountColumn,
	} {
		sql.WriteString(", toUInt64(0) AS ")
		sql.WriteString(q(column))
	}
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogTotalEventsColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogMetadataInvalid))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogTotalsCTE))
	sql.WriteString(" UNION ALL SELECT toUInt8(1) AS ")
	sql.WriteString(q(FieldCatalogRowKindColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileName))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogNameColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileTypes))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogObservedTypesColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileEvents))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogEventCountColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileNulls))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogNullCountColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileMissing))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogMissingCountColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldCatalogTotalEventsColumn))
	sql.WriteString(", toUInt8(0) AS ")
	sql.WriteString(q(FieldCatalogInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogLimitedCTE))
	sql.WriteString(") AS ")
	sql.WriteString(q("__os_field_catalog_output"))
	if policy.includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldCatalogResultContract().order)
	}

	sourceDepth := relation.depth
	var totalsDepth, profilesDepth int
	if len(knownFields) > 0 {
		knownRowsDepth := relationalNodeDepth(sourceDepth)
		totalsDepth = relationalNodeDepth(knownRowsDepth)
		totalsScalarDepth := relationalNodeDepth(totalsDepth)
		dynamicLeavesDepth := relationalNodeDepth(sourceDepth, totalsScalarDepth)
		dynamicProfilesDepth := relationalNodeDepth(dynamicLeavesDepth, totalsDepth)
		knownProfilesDepth := relationalNodeDepth(totalsDepth)
		profilesDepth = relationalNodeDepth(dynamicProfilesDepth, knownProfilesDepth)
	} else {
		totalsDepth = relationalNodeDepth(sourceDepth)
		totalsScalarDepth := relationalNodeDepth(totalsDepth)
		dynamicLeavesDepth := relationalNodeDepth(sourceDepth, totalsScalarDepth)
		profilesDepth = relationalNodeDepth(dynamicLeavesDepth, totalsDepth)
	}
	limitedDepth := relationalNodeDepth(profilesDepth)
	headerDepth := relationalNodeDepth(totalsDepth)
	profileRowsDepth := relationalNodeDepth(limitedDepth)
	outputUnionDepth := relationalNodeDepth(headerDepth, profileRowsDepth)
	resultDepth := relationalNodeDepth(outputUnionDepth)

	compiled := CompiledQuery{SQL: sql.String(), Args: args}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

// finalizePrerequisiteFieldCatalog keeps a chronological event relation on one
// dependency chain. Every event contributes one header observation and zero or
// more eligible dynamic-name observations; a synthetic zero-weight header
// preserves the result contract for empty input. Known fields travel in one
// fixed-width UInt64 vector on header observations, so the sole GROUP BY is by
// the bounded public domain (row kind, field name). Global control runs only
// after that group boundary.
func finalizePrerequisiteFieldCatalog(
	relation compiledRelation,
	args []any,
	knownFields []compiledKnownField,
	shadows []string,
	prefixes []string,
	allowDynamic bool,
	spec FieldCatalogSpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
) (CompiledQuery, error) {
	if len(knownFields) > maximumPrerequisiteFieldCatalogKnownFields {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse prerequisite field catalog: known fields exceed the catalog overflow bound",
		)
	}
	sidecarCount := prerequisiteFieldCatalogSidecarCount(knownFields)
	if sidecarCount > knowledgeprogram.MaximumGeneratedFields {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse prerequisite field catalog: retained knowledge sidecars exceed the generated-field limit",
		)
	}

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 16_384 + len(knownFields)*2_048)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	sql.WriteString(" AS (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	rowsInput := fieldCatalogSourceCTE
	if len(knownFields) > 0 {
		writePrerequisiteKnownFieldRows(&sql, knownFields)
		for _, known := range knownFields {
			args = append(args, known.presenceArgs...)
			args = append(args, known.typeArgs...)
		}
		if sidecarCount > 0 {
			args = append(args,
				eventfields.CurrentFieldMetadataVersion,
				uint64(eventfields.MaximumStoredFieldsPerEvent),
				uint64(eventfields.MaximumStoredFieldsPerEvent),
				uint64(eventfields.MaximumNormalizedFieldNameBytes),
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeDecimal),
				prerequisiteFieldCatalogSidecarNames(knownFields),
			)
		}
		sql.WriteString(", ")
		rowsInput = fieldCatalogKnownRowsCTE
	}

	sql.WriteString(q(fieldCatalogRowsCTE))
	sql.WriteString(" AS (SELECT toUInt8(")
	writePrerequisiteFieldCatalogMetadataPredicate(&sql, len(knownFields) > 0)
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogInvalidEvidence))
	sql.WriteString(", ")
	if len(knownFields) > 0 {
		sql.WriteString(q(fieldCatalogPackedKnownCounts))
	} else {
		sql.WriteString("CAST([], 'Array(UInt64)')")
	}
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogKnownCounts))
	sql.WriteString(", ")
	if len(knownFields) > 0 {
		sql.WriteString(q(fieldCatalogPackedKnownMasks))
	} else {
		sql.WriteString("CAST([], 'Array(UInt16)')")
	}
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogKnownTypeMasks))
	sql.WriteString(", ")
	writePrerequisiteFieldCatalogCandidates(&sql, len(knownFields) > 0)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogCandidates))
	sql.WriteString(" FROM ")
	sql.WriteString(q(rowsInput))
	sql.WriteString("), ")
	args = append(args,
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	)

	writePrerequisiteFieldCatalogObservations(&sql, len(knownFields))
	sql.WriteString(", ")
	args = append(args, shadows, prefixes, allowDynamic)

	writePrerequisiteFieldCatalogGroups(&sql)
	sql.WriteString(", ")

	sql.WriteString(q(fieldCatalogControlledCTE))
	sql.WriteString(" AS (SELECT *, toUInt64(max(if(")
	sql.WriteString(q(fieldCatalogGroupKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldCatalogGroupTotal))
	sql.WriteString(", toUInt64(0))) OVER ()) AS ")
	sql.WriteString(q(fieldCatalogGlobalTotal))
	sql.WriteString(", toUInt8(max(if(")
	sql.WriteString(q(fieldCatalogGroupKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldCatalogGroupInvalid))
	sql.WriteString(", toUInt8(0))) OVER ()) AS ")
	sql.WriteString(q(fieldCatalogGlobalInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogGroupsCTE))
	sql.WriteString("), ")

	writePrerequisiteFieldCatalogExpansion(&sql, knownFields)
	if len(knownFields) > 0 {
		knownNames := make([]string, 0, len(knownFields))
		for _, known := range knownFields {
			knownNames = append(knownNames, known.name)
		}
		args = append(args, knownNames)
	}
	sql.WriteString(", ")

	// The header consumes one result row. MaximumFields+1 profile rows preserve
	// the executor's exact overflow sentinel, so the complete bounded result is
	// MaximumFields+2 rows.
	sql.WriteString(q(fieldCatalogLimitedCTE))
	sql.WriteString(" AS (SELECT * FROM ")
	sql.WriteString(q(fieldCatalogExpandedCTE))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(fieldCatalogResultContract().order)
	sql.WriteString(" LIMIT ?)")
	args = append(args, uint64(spec.MaximumFields)+2)

	sql.WriteString(" SELECT ")
	for index, column := range fieldCatalogResultContract().columns {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogLimitedCTE))
	if policy.includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldCatalogResultContract().order)
	}
	sql.WriteString(materializedCTESettingsSQL)

	inputDepth := relation.depth
	if len(knownFields) > 0 {
		inputDepth = relationalNodeDepth(inputDepth)
	}
	rowsDepth := relationalNodeDepth(inputDepth)
	observationRowsDepth := relationalNodeDepth(rowsDepth)
	syntheticObservationDepth := relationalNodeDepth()
	observationsDepth := relationalNodeDepth(observationRowsDepth, syntheticObservationDepth)
	groupsDepth := relationalNodeDepth(observationsDepth)
	controlledDepth := relationalNodeDepth(groupsDepth)
	expandedDepth := relationalNodeDepth(controlledDepth)
	limitedDepth := relationalNodeDepth(expandedDepth)
	resultDepth := relationalNodeDepth(limitedDepth)

	compiled := CompiledQuery{
		SQL:                       sql.String(),
		Args:                      args,
		validationDummyProjection: fieldCatalogValidationDummyProjection(),
	}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func fieldCatalogValidationDummyProjection() []string {
	q := quoteIdentifier
	return []string{
		"toUInt8(0) AS " + q(FieldCatalogRowKindColumn),
		"CAST('' AS String) AS " + q(FieldCatalogNameColumn),
		"CAST([], 'Array(UInt8)') AS " + q(FieldCatalogObservedTypesColumn),
		"toUInt64(0) AS " + q(FieldCatalogEventCountColumn),
		"toUInt64(0) AS " + q(FieldCatalogNullCountColumn),
		"toUInt64(0) AS " + q(FieldCatalogMissingCountColumn),
		"toUInt64(0) AS " + q(FieldCatalogTotalEventsColumn),
		"toUInt8(0) AS " + q(FieldCatalogInvalidColumn),
	}
}

// writeAlignedFieldMetadataInvalidPredicate emits the shared "aligned stored-field
// metadata is invalid" boolean. It binds six values in order:
// CurrentFieldMetadataVersion, MaximumStoredFieldsPerEvent, MaximumStoredFieldsPerEvent,
// MaximumNormalizedFieldNameBytes, StoredValueTypeNull, StoredValueTypeDecimal.
// Callers own the surrounding countIf(...)/toUInt8(...) wrapper and any suffix.
func writeAlignedFieldMetadataInvalidPredicate(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString(q(internalFieldMetadataVersionColumn))
	sql.WriteString(" != ? OR length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") > ? OR length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(") > ? OR length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") != length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(") OR arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, ")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") OR ")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(" != arraySort(arrayDistinct(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(")) OR arrayExists(stored_type -> stored_type < ? OR stored_type > ?, ")
	sql.WriteString(q(internalFieldTypesColumn))
}

func writePrerequisiteFieldCatalogMetadataPredicate(
	sql *strings.Builder,
	hasSidecars bool,
) {
	q := quoteIdentifier
	writeAlignedFieldMetadataInvalidPredicate(sql)
	sql.WriteString(")")
	if hasSidecars {
		sql.WriteString(" OR ")
		sql.WriteString(q(fieldCatalogSidecarInvalid))
		sql.WriteString(" != 0")
	}
}

func prerequisiteFieldCatalogSidecarCount(fields []compiledKnownField) int {
	count := 0
	for _, field := range fields {
		if field.relativeNamesSQL != "" {
			count++
		}
	}
	return count
}

func prerequisiteFieldCatalogSidecarNames(fields []compiledKnownField) []string {
	names := make([]string, 0, prerequisiteFieldCatalogSidecarCount(fields))
	for _, field := range fields {
		if field.relativeNamesSQL != "" {
			names = append(names, field.name)
		}
	}
	return names
}

func writePrerequisiteFieldCatalogBindingElement(sql *strings.Builder, element int) {
	sql.WriteString("tupleElement(")
	sql.WriteString(quoteIdentifier(fieldCatalogRowBinding))
	sql.WriteString(", ")
	fmt.Fprint(sql, element)
	sql.WriteByte(')')
}

func writePrerequisiteFieldCatalogSidecarBindingElements(
	sql *strings.Builder,
	fields []compiledKnownField,
	component int,
) {
	first := true
	sidecarOrdinal := 0
	for _, field := range fields {
		if field.relativeNamesSQL == "" {
			continue
		}
		if !first {
			sql.WriteString(", ")
		}
		first = false
		writePrerequisiteFieldCatalogBindingElement(
			sql,
			sidecarOrdinal*3+component+2,
		)
		sidecarOrdinal++
	}
}

func writePrerequisiteFieldCatalogSidecarInvalidPredicate(
	sql *strings.Builder,
	knownFields []compiledKnownField,
) {
	sql.WriteString("arrayExists((relative_names, relative_types, relative_version) -> ((notEmpty(relative_names) OR notEmpty(relative_types)) AND relative_version != ?) OR length(relative_names) > ? OR length(relative_types) > ? OR length(relative_names) != length(relative_types) OR arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, relative_names) OR relative_names != arraySort(arrayDistinct(relative_names)) OR arrayExists(stored_type -> stored_type < ? OR stored_type > ?, relative_types), [")
	writePrerequisiteFieldCatalogSidecarBindingElements(sql, knownFields, 0)
	sql.WriteString("], [")
	writePrerequisiteFieldCatalogSidecarBindingElements(sql, knownFields, 1)
	sql.WriteString("], [")
	writePrerequisiteFieldCatalogSidecarBindingElements(sql, knownFields, 2)
	sql.WriteString("])")
}

func writePrerequisiteFieldCatalogPackedSidecarCandidates(
	sql *strings.Builder,
	knownFields []compiledKnownField,
) {
	if prerequisiteFieldCatalogSidecarCount(knownFields) == 0 {
		sql.WriteString("CAST([], 'Array(Tuple(String, UInt8))')")
		return
	}
	perSidecarLimit := uint64(eventfields.MaximumStoredFieldsPerEvent) + 1
	// finalizePrerequisiteFieldCatalog rejects counts above the fixed generated-field limit.
	sidecarCount := uint64(prerequisiteFieldCatalogSidecarCount(knownFields)) // #nosec G115 -- bounded by knowledgeprogram.MaximumGeneratedFields.
	packedLimit := sidecarCount * perSidecarLimit
	sql.WriteString("arraySlice(arrayFlatten(arrayMap((field_name, relative_names, relative_types) -> arrayMap(field_metadata -> tuple(concat(field_name, '.', tupleElement(field_metadata, 1)), toUInt8(tupleElement(field_metadata, 2))), arrayZip(arraySlice(relative_names, 1, least(length(relative_names), length(relative_types), toUInt64(")
	fmt.Fprint(sql, perSidecarLimit)
	sql.WriteString("))), arraySlice(relative_types, 1, least(length(relative_names), length(relative_types), toUInt64(")
	fmt.Fprint(sql, perSidecarLimit)
	sql.WriteString("))))), CAST(? AS Array(String)), [")
	writePrerequisiteFieldCatalogSidecarBindingElements(sql, knownFields, 0)
	sql.WriteString("], [")
	writePrerequisiteFieldCatalogSidecarBindingElements(sql, knownFields, 1)
	sql.WriteString("])), 1, toUInt64(")
	fmt.Fprint(sql, packedLimit)
	sql.WriteString("))")
}

func writePrerequisiteFieldCatalogCandidates(
	sql *strings.Builder,
	hasSidecars bool,
) {
	q := quoteIdentifier
	perEventLimit := uint64(eventfields.MaximumStoredFieldsPerEvent) + 1
	sql.WriteString("arrayConcat(arrayZip(arraySlice(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString("), toUInt64(")
	fmt.Fprint(sql, perEventLimit)
	sql.WriteString("))), arraySlice(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString("), toUInt64(")
	fmt.Fprint(sql, perEventLimit)
	sql.WriteString("))))")
	if hasSidecars {
		sql.WriteString(", ")
		sql.WriteString(q(fieldCatalogSidecarCandidates))
	}
	sql.WriteByte(')')
}

// Each known field owns two UInt64 count cells: present and present-null. Type
// presence travels separately in one UInt16 mask whose dense bits 0..11 map
// durable stored type codes 1..12. Neither vector contains a runtime key.
func writePrerequisiteFieldCatalogKnownCounts(
	sql *strings.Builder,
	fields []compiledKnownField,
) {
	if len(fields) == 0 {
		sql.WriteString("CAST([], 'Array(UInt64)')")
		return
	}
	sql.WriteString("arrayFlatten(arrayMap(field_state -> [toUInt64(tupleElement(field_state, 1) != 0), toUInt64(tupleElement(field_state, 1) != 0 AND tupleElement(field_state, 2) = toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString("))], ")
	writePrerequisiteFieldCatalogBindingElement(sql, 1)
	sql.WriteString("))")
}

func writePrerequisiteFieldCatalogKnownTypeMasks(
	sql *strings.Builder,
	fields []compiledKnownField,
) {
	if len(fields) == 0 {
		sql.WriteString("CAST([], 'Array(UInt16)')")
		return
	}
	minimumType := uint8(eventfields.StoredValueTypeNull)
	maximumType := uint8(eventfields.StoredValueTypeDecimal)
	sql.WriteString("arrayMap(field_state -> toUInt16(toUInt16(bitShiftLeft(toUInt16(1), toUInt8(least(greatest(toInt16(tupleElement(field_state, 2)) - toInt16(")
	fmt.Fprint(sql, minimumType)
	sql.WriteString("), toInt16(0)), toInt16(")
	fmt.Fprint(sql, maximumType-minimumType)
	sql.WriteString("))))) * toUInt16(tupleElement(field_state, 1) != 0) * toUInt16(tupleElement(field_state, 2) >= toUInt8(")
	fmt.Fprint(sql, minimumType)
	sql.WriteString(")) * toUInt16(tupleElement(field_state, 2) <= toUInt8(")
	fmt.Fprint(sql, maximumType)
	sql.WriteString("))), ")
	writePrerequisiteFieldCatalogBindingElement(sql, 1)
	sql.WriteByte(')')
}

func writeStoredFieldTypeCodeArray(sql *strings.Builder) {
	sql.WriteByte('[')
	for code := uint8(eventfields.StoredValueTypeNull); code <= uint8(eventfields.StoredValueTypeDecimal); code++ {
		if code > uint8(eventfields.StoredValueTypeNull) {
			sql.WriteString(", ")
		}
		sql.WriteString("toUInt8(")
		fmt.Fprint(sql, code)
		sql.WriteByte(')')
	}
	sql.WriteByte(']')
}

func prerequisiteFieldCatalogKnownVectorLengths(knownCount int) (
	countLength uint64,
	maskLength uint64,
	ok bool,
) {
	if knownCount < 0 || knownCount > maximumPrerequisiteFieldCatalogKnownFields {
		return 0, 0, false
	}
	maskLength = uint64(knownCount)
	return maskLength * 2, maskLength, true
}

func writePrerequisiteFieldCatalogObservations(sql *strings.Builder, knownCount int) {
	q := quoteIdentifier
	knownCountLength, knownMaskLength, _ := prerequisiteFieldCatalogKnownVectorLengths(knownCount)
	fields := []string{
		fieldCatalogObservedKind,
		fieldCatalogObservedName,
		fieldCatalogObservedType,
		fieldCatalogEventWeight,
		fieldCatalogNullWeight,
		fieldCatalogTotalWeight,
		fieldCatalogInvalidEvidence,
		fieldCatalogKnownCounts,
		fieldCatalogKnownTypeMasks,
	}

	sql.WriteString(q(fieldCatalogObservationsCTE))
	sql.WriteString(" AS (SELECT ")
	for index, name := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("tupleElement(")
		sql.WriteString(q(fieldCatalogObservation))
		sql.WriteString(", ")
		fmt.Fprint(sql, index+1)
		sql.WriteString(") AS ")
		sql.WriteString(q(name))
	}
	sql.WriteString(" FROM (SELECT arrayJoin(arrayConcat([tuple(toUInt8(0), CAST('' AS String), toUInt8(0), toUInt64(0), toUInt64(0), toUInt64(1), ")
	sql.WriteString(q(fieldCatalogInvalidEvidence))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogKnownCounts))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogKnownTypeMasks))
	sql.WriteString(")], arrayMap(field_metadata -> tuple(toUInt8(1), tupleElement(field_metadata, 1), toUInt8(tupleElement(field_metadata, 2)), toUInt64(1), toUInt64(tupleElement(field_metadata, 2) = toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(")), toUInt64(0), toUInt8(0), CAST([], 'Array(UInt64)'), CAST([], 'Array(UInt16)')), arrayFilter(field_metadata -> ")
	sql.WriteString(q(fieldCatalogInvalidEvidence))
	sql.WriteString(" = 0 AND NOT has(CAST(? AS Array(String)), tupleElement(field_metadata, 1))")
	sql.WriteString(" AND NOT arrayExists(prefix -> tupleElement(field_metadata, 1) = prefix OR startsWith(tupleElement(field_metadata, 1), concat(prefix, '.')), CAST(? AS Array(String)))")
	sql.WriteString(" AND CAST(? AS Bool), ")
	sql.WriteString(q(fieldCatalogCandidates))
	sql.WriteString(")))) AS ")
	sql.WriteString(q(fieldCatalogObservation))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogRowsCTE))
	sql.WriteString(") AS ")
	sql.WriteString(q("__os_field_catalog_observation_rows"))
	sql.WriteString(" UNION ALL SELECT toUInt8(0), CAST('' AS String), toUInt8(0), toUInt64(0), toUInt64(0), toUInt64(0), toUInt8(0), arrayResize(CAST([], 'Array(UInt64)'), toUInt64(")
	fmt.Fprint(sql, knownCountLength)
	sql.WriteString("), toUInt64(0)), arrayResize(CAST([], 'Array(UInt16)'), toUInt64(")
	fmt.Fprint(sql, knownMaskLength)
	sql.WriteString("), toUInt16(0)))")
}

func writePrerequisiteFieldCatalogGroups(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString(q(fieldCatalogGroupsCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldCatalogObservedKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogGroupKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogObservedName))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogGroupName))
	sql.WriteString(", arraySort(groupUniqArrayIf(toUInt8(")
	sql.WriteString(q(fieldCatalogObservedType))
	sql.WriteString("), ")
	sql.WriteString(q(fieldCatalogEventWeight))
	sql.WriteString(" != 0)) AS ")
	sql.WriteString(q(fieldCatalogGroupTypes))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldCatalogEventWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogGroupEvents))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldCatalogNullWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogGroupNulls))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldCatalogTotalWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogGroupTotal))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldCatalogInvalidEvidence))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldCatalogGroupInvalid))
	sql.WriteString(", sumForEach(")
	sql.WriteString(q(fieldCatalogKnownCounts))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogKnownCounts))
	sql.WriteString(", groupBitOrForEach(")
	sql.WriteString(q(fieldCatalogKnownTypeMasks))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldCatalogKnownTypeMasks))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogObservationsCTE))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(fieldCatalogObservedKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogObservedName))
	sql.WriteString(")")
}

func writePrerequisiteFieldCatalogExpansion(
	sql *strings.Builder,
	knownFields []compiledKnownField,
) {
	q := quoteIdentifier
	sql.WriteString(q(fieldCatalogExpandedCTE))
	sql.WriteString(" AS (SELECT ")
	for index, column := range fieldCatalogResultContract().columns {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("tupleElement(")
		sql.WriteString(q(fieldCatalogOutputTuple))
		sql.WriteString(", ")
		fmt.Fprint(sql, index+1)
		sql.WriteString(") AS ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogControlledCTE))
	sql.WriteString(" ARRAY JOIN if(")
	sql.WriteString(q(fieldCatalogGroupKind))
	sql.WriteString(" = toUInt8(0), ")
	writePrerequisiteFieldCatalogHeaderArray(sql, knownFields)
	sql.WriteString(", arrayResize([tuple(toUInt8(1), ")
	sql.WriteString(q(fieldCatalogGroupName))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogGroupTypes))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogGroupEvents))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogGroupNulls))
	sql.WriteString(", toUInt64(if(")
	sql.WriteString(q(fieldCatalogGlobalTotal))
	sql.WriteString(" >= ")
	sql.WriteString(q(fieldCatalogGroupEvents))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogGlobalTotal))
	sql.WriteString(" - ")
	sql.WriteString(q(fieldCatalogGroupEvents))
	sql.WriteString(", 0)), ")
	sql.WriteString(q(fieldCatalogGlobalTotal))
	sql.WriteString(", toUInt8(0))], toUInt64(")
	sql.WriteString(q(fieldCatalogGlobalInvalid))
	sql.WriteString(" = 0))) AS ")
	sql.WriteString(q(fieldCatalogOutputTuple))
	sql.WriteString(")")
}

func writePrerequisiteFieldCatalogHeaderArray(
	sql *strings.Builder,
	knownFields []compiledKnownField,
) {
	q := quoteIdentifier
	sql.WriteString("arrayConcat([tuple(toUInt8(0), CAST('' AS String), CAST([], 'Array(UInt8)'), toUInt64(0), toUInt64(0), toUInt64(0), ")
	sql.WriteString(q(fieldCatalogGlobalTotal))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogGlobalInvalid))
	sql.WriteString(")]")
	if len(knownFields) > 0 {
		sql.WriteString(", arrayResize(arrayMap((field_name, field_index) -> tuple(toUInt8(1), field_name, arrayFilter(type_code -> bitAnd(toUInt16(arrayElement(")
		sql.WriteString(q(fieldCatalogKnownTypeMasks))
		sql.WriteString(", field_index + toUInt64(1))), toUInt16(bitShiftLeft(toUInt16(1), toUInt8(type_code - toUInt8(")
		fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
		sql.WriteString("))))) != toUInt16(0), ")
		writeStoredFieldTypeCodeArray(sql)
		sql.WriteString("), arrayElement(")
		sql.WriteString(q(fieldCatalogKnownCounts))
		sql.WriteString(", field_index * toUInt64(2) + toUInt64(1)), arrayElement(")
		sql.WriteString(q(fieldCatalogKnownCounts))
		sql.WriteString(", field_index * toUInt64(2) + toUInt64(2)), toUInt64(if(")
		sql.WriteString(q(fieldCatalogGlobalTotal))
		sql.WriteString(" >= arrayElement(")
		sql.WriteString(q(fieldCatalogKnownCounts))
		sql.WriteString(", field_index * toUInt64(2) + toUInt64(1)), ")
		sql.WriteString(q(fieldCatalogGlobalTotal))
		sql.WriteString(" - arrayElement(")
		sql.WriteString(q(fieldCatalogKnownCounts))
		sql.WriteString(", field_index * toUInt64(2) + toUInt64(1)), 0)), ")
		sql.WriteString(q(fieldCatalogGlobalTotal))
		sql.WriteString(", toUInt8(0)), CAST(? AS Array(String)), range(toUInt64(")
		fmt.Fprint(sql, len(knownFields))
		sql.WriteString("))), toUInt64(")
		fmt.Fprint(sql, len(knownFields))
		sql.WriteString(") * toUInt64(")
		sql.WriteString(q(fieldCatalogGlobalInvalid))
		sql.WriteString(" = 0))")
	}
	sql.WriteString(")")
}

func writeDynamicFieldProfiles(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString("SELECT ")
	sql.WriteString(q(fieldCatalogDynamicName))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogProfileName))
	sql.WriteString(", arraySort(groupUniqArray(toUInt8(")
	sql.WriteString(q(fieldCatalogStoredType))
	sql.WriteString("))) AS ")
	sql.WriteString(q(fieldCatalogProfileTypes))
	sql.WriteString(", count() AS ")
	sql.WriteString(q(fieldCatalogProfileEvents))
	sql.WriteString(", countIf(")
	sql.WriteString(q(fieldCatalogStoredType))
	sql.WriteString(" = toUInt8(1)) AS ")
	sql.WriteString(q(fieldCatalogProfileNulls))
	sql.WriteString(", toUInt64(if(")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" >= count(), ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" - count(), 0)) AS ")
	sql.WriteString(q(fieldCatalogProfileMissing))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" FROM (SELECT tupleElement(field_metadata, 1) AS ")
	sql.WriteString(q(fieldCatalogDynamicName))
	sql.WriteString(", tupleElement(field_metadata, 2) AS ")
	sql.WriteString(q(fieldCatalogStoredType))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	sql.WriteString(" ARRAY JOIN arrayZip(arraySlice(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString("))), arraySlice(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(")))) AS field_metadata WHERE NOT has(CAST(? AS Array(String)), tupleElement(field_metadata, 1))")
	sql.WriteString(" AND NOT arrayExists(prefix -> tupleElement(field_metadata, 1) = prefix OR startsWith(tupleElement(field_metadata, 1), concat(prefix, '.')), CAST(? AS Array(String)))")
	sql.WriteString(" AND CAST(? AS Bool) AND (SELECT ")
	sql.WriteString(q(fieldCatalogMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogTotalsCTE))
	sql.WriteString(") = 0) AS ")
	sql.WriteString(q("__os_field_catalog_dynamic_leaves"))
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(q(fieldCatalogTotalsCTE))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(fieldCatalogDynamicName))
	sql.WriteString(", ")
	sql.WriteString(q(fieldCatalogProfileTotal))
}

func compileKnownFields(state compileState, names []string) ([]compiledKnownField, error) {
	known := make([]compiledKnownField, 0, len(names))
	for _, name := range names {
		field := state.visible[name]
		presenceSQL, presenceArgs := knownFieldPresenceSQL(field)
		typeSQL, typeArgs, err := knownFieldStoredTypeSQL(field)
		if err != nil {
			return nil, fmt.Errorf("compile ClickHouse field catalog field %q: %w", name, err)
		}
		if err := validateKnowledgeFieldSidecars(
			field.relativeFieldNamesSQL,
			field.relativeFieldTypesSQL,
			field.fieldMetadataVersionSQL,
		); err != nil {
			return nil, fmt.Errorf("compile ClickHouse field catalog field %q: %w", name, err)
		}
		known = append(known, compiledKnownField{
			name:               name,
			presenceSQL:        presenceSQL,
			presenceArgs:       presenceArgs,
			typeSQL:            typeSQL,
			typeArgs:           typeArgs,
			relativeNamesSQL:   field.relativeFieldNamesSQL,
			relativeTypesSQL:   field.relativeFieldTypesSQL,
			metadataVersionSQL: field.fieldMetadataVersionSQL,
		})
	}
	return known, nil
}

func knownFieldPresenceSQL(field fieldState) (string, []any) {
	presence := field.existsSQL
	if presence == "" {
		presence = "1"
	}
	args := append([]any(nil), field.existsArgs...)
	if field.kind == fieldKindDynamic && field.descendantSQL != "" {
		presence = "((" + presence + ") OR (" + field.descendantSQL + "))"
		args = append(args, field.descendantArgs...)
	}
	return presence, args
}

func knownColumn(base string, index int) string {
	return quoteIdentifier(fmt.Sprintf("%s_%d", base, index))
}

func writePrerequisiteFieldCatalogRawRowBinding(
	sql *strings.Builder,
	fields []compiledKnownField,
) {
	sql.WriteString("tuple([")
	for index, known := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("tuple(toUInt8(ifNull(")
		sql.WriteString(known.presenceSQL)
		sql.WriteString(", 0)), toUInt8(ifNull(")
		sql.WriteString(known.typeSQL)
		sql.WriteString(", 0)))")
	}
	sql.WriteByte(']')
	for _, known := range fields {
		if known.relativeNamesSQL == "" {
			continue
		}
		sql.WriteString(", ")
		sql.WriteString(known.relativeNamesSQL)
		sql.WriteString(", ")
		sql.WriteString(known.relativeTypesSQL)
		sql.WriteString(", toUInt8(")
		sql.WriteString(known.metadataVersionSQL)
		sql.WriteByte(')')
	}
	sql.WriteByte(')')
}

// writePrerequisiteKnownFieldRows is the sole projection over the guarded
// chronological source. A singleton arrayMap derives the complete compact row
// inside its scoped raw binding; ARRAY JOIN then publishes exactly one packed
// row without changing cardinality or exposing any raw producer downstream.
func writePrerequisiteKnownFieldRows(sql *strings.Builder, fields []compiledKnownField) {
	q := quoteIdentifier
	sql.WriteString(q(fieldCatalogKnownRowsCTE))
	sql.WriteString(" AS (WITH ")
	writePrerequisiteFieldCatalogRawRowBinding(sql, fields)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogRawRowBinding))
	sql.WriteString(", arrayMap(")
	sql.WriteString(q(fieldCatalogRowBinding))
	sql.WriteString(" -> tuple(")
	writePrerequisiteFieldCatalogKnownCounts(sql, fields)
	sql.WriteString(", ")
	writePrerequisiteFieldCatalogKnownTypeMasks(sql, fields)
	sql.WriteString(", ")
	if prerequisiteFieldCatalogSidecarCount(fields) == 0 {
		sql.WriteString("toUInt8(0)")
	} else {
		sql.WriteString("toUInt8(")
		writePrerequisiteFieldCatalogSidecarInvalidPredicate(sql, fields)
		sql.WriteByte(')')
	}
	sql.WriteString(", ")
	writePrerequisiteFieldCatalogPackedSidecarCandidates(sql, fields)
	sql.WriteString("), [")
	sql.WriteString(q(fieldCatalogRawRowBinding))
	sql.WriteString("]) AS ")
	sql.WriteString(q(fieldCatalogPackedRows))
	sql.WriteString(" SELECT tupleElement(")
	sql.WriteString(q(fieldCatalogPackedRow))
	sql.WriteString(", 1) AS ")
	sql.WriteString(q(fieldCatalogPackedKnownCounts))
	sql.WriteString(", tupleElement(")
	sql.WriteString(q(fieldCatalogPackedRow))
	sql.WriteString(", 2) AS ")
	sql.WriteString(q(fieldCatalogPackedKnownMasks))
	sql.WriteString(", toUInt8(tupleElement(")
	sql.WriteString(q(fieldCatalogPackedRow))
	sql.WriteString(", 3)) AS ")
	sql.WriteString(q(fieldCatalogSidecarInvalid))
	sql.WriteString(", tupleElement(")
	sql.WriteString(q(fieldCatalogPackedRow))
	sql.WriteString(", 4) AS ")
	sql.WriteString(q(fieldCatalogSidecarCandidates))
	for _, column := range []string{
		internalFieldNamesColumn,
		internalFieldTypesColumn,
		internalFieldMetadataVersionColumn,
	} {
		sql.WriteString(", ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	sql.WriteString(" ARRAY JOIN ")
	sql.WriteString(q(fieldCatalogPackedRows))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldCatalogPackedRow))
	sql.WriteString(")")
}

// writeKnownFieldRows projects every known field's heterogeneous value and
// scalar analysis inputs in one pass over the shared final relation.
// Keeping Dynamic values in separate columns avoids grouping or array-building
// over Dynamic while eliminating one full CTE scan per known field.
func writeKnownFieldRows(sql *strings.Builder, fields []compiledKnownField) {
	q := quoteIdentifier
	sql.WriteString(q(fieldCatalogKnownRowsCTE))
	sql.WriteString(" AS (SELECT ")
	for index, known := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("toUInt8(ifNull(")
		sql.WriteString(known.presenceSQL)
		sql.WriteString(", 0)) AS ")
		sql.WriteString(knownColumn(fieldCatalogPresent, index))
		sql.WriteString(", ")
		sql.WriteString(known.typeSQL)
		sql.WriteString(" AS ")
		sql.WriteString(knownColumn(fieldCatalogType, index))
		if known.relativeNamesSQL != "" {
			sql.WriteString(", ")
			sql.WriteString(known.relativeNamesSQL)
			sql.WriteString(" AS ")
			sql.WriteString(knownColumn(fieldCatalogRelativeNames, index))
			sql.WriteString(", ")
			sql.WriteString(known.relativeTypesSQL)
			sql.WriteString(" AS ")
			sql.WriteString(knownColumn(fieldCatalogRelativeTypes, index))
			sql.WriteString(", toUInt8(")
			sql.WriteString(known.metadataVersionSQL)
			sql.WriteString(") AS ")
			sql.WriteString(knownColumn(fieldCatalogRelativeVersion, index))
		}
	}
	for _, column := range []string{
		internalFieldNamesColumn,
		internalFieldTypesColumn,
		internalFieldMetadataVersionColumn,
	} {
		sql.WriteString(", ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	sql.WriteString(")")
}

func writeKnownAggregateColumns(sql *strings.Builder, fields []compiledKnownField) {
	q := quoteIdentifier
	sql.WriteString(", [")
	for index := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("CAST(? AS String)")
	}
	sql.WriteString("] AS ")
	sql.WriteString(q(fieldCatalogKnownNames))

	writeKnownAggregateArray(sql, len(fields), fieldCatalogKnownTypes, func(index int) string {
		return "arraySort(groupUniqArrayIf(toUInt8(" + knownColumn(fieldCatalogType, index) + "), " +
			knownColumn(fieldCatalogPresent, index) + "))"
	})
	writeKnownAggregateArray(sql, len(fields), fieldCatalogKnownEvents, func(index int) string {
		return "countIf(" + knownColumn(fieldCatalogPresent, index) + ")"
	})
	writeKnownAggregateArray(sql, len(fields), fieldCatalogKnownNulls, func(index int) string {
		return "countIf(" + knownColumn(fieldCatalogPresent, index) + " AND " +
			knownColumn(fieldCatalogType, index) + " = toUInt8(1))"
	})
	writeKnownAggregateArray(sql, len(fields), fieldCatalogKnownMissing, func(index int) string {
		return "toUInt64(count() - countIf(" + knownColumn(fieldCatalogPresent, index) + "))"
	})
	writeKnownAggregateArray(sql, len(fields), fieldCatalogKnownTotals, func(int) string {
		return "count()"
	})
}

func writeKnownAggregateArray(sql *strings.Builder, count int, alias string, expression func(int) string) {
	sql.WriteString(", [")
	for index := range count {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(expression(index))
	}
	sql.WriteString("] AS ")
	sql.WriteString(quoteIdentifier(alias))
}

func writeKnownFieldProfiles(sql *strings.Builder) {
	q := quoteIdentifier
	const profileTuple = "known_profile"
	sql.WriteString("SELECT tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 1) AS ")
	sql.WriteString(q(fieldCatalogProfileName))
	sql.WriteString(", tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 2) AS ")
	sql.WriteString(q(fieldCatalogProfileTypes))
	sql.WriteString(", tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 3) AS ")
	sql.WriteString(q(fieldCatalogProfileEvents))
	sql.WriteString(", tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 4) AS ")
	sql.WriteString(q(fieldCatalogProfileNulls))
	sql.WriteString(", tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 5) AS ")
	sql.WriteString(q(fieldCatalogProfileMissing))
	sql.WriteString(", tupleElement(")
	sql.WriteString(profileTuple)
	sql.WriteString(", 6) AS ")
	sql.WriteString(q(fieldCatalogProfileTotal))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldCatalogTotalsCTE))
	sql.WriteString(" ARRAY JOIN arrayZip(")
	for index, column := range []string{
		fieldCatalogKnownNames,
		fieldCatalogKnownTypes,
		fieldCatalogKnownEvents,
		fieldCatalogKnownNulls,
		fieldCatalogKnownMissing,
		fieldCatalogKnownTotals,
	} {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(q(column))
	}
	sql.WriteString(") AS ")
	sql.WriteString(profileTuple)
}

func knownFieldStoredTypeSQL(field fieldState) (string, []any, error) {
	if field.storedTypeSQL != "" {
		return field.storedTypeSQL, nil, nil
	}
	if field.kind == fieldKindDynamic {
		path, ok := exactStoredMetadataPath(field)
		if !ok {
			return "", nil, errors.New("dynamic field has an invalid stored metadata path")
		}
		if field.descendantSQL == "" || len(field.descendantArgs) == 0 {
			return "", nil, errors.New("dynamic field has no exact descendant metadata proof")
		}
		firstIndex := "indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)"
		secondIndex := "indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)"
		result := "multiIf(" + firstIndex + " != 0, arrayElement(" +
			quoteIdentifier(internalFieldTypesColumn) + ", " + secondIndex + "), " +
			field.descendantSQL + ", CAST(? AS UInt8), isNull(" + field.valueSQL +
			"), CAST(? AS UInt8), CAST(? AS UInt8))"
		args := []any{path, path}
		args = append(args, field.descendantArgs...)
		args = append(args,
			uint8(eventfields.StoredValueTypeObject),
			uint8(eventfields.StoredValueTypeNull),
			uint8(0),
		)
		return result, args, nil
	}

	code, err := fixedFieldStoredType(field)
	if err != nil {
		return "", nil, err
	}
	if field.kind == fieldKindInvalid {
		return "CAST(? AS UInt8)", []any{uint8(eventfields.StoredValueTypeNull)}, nil
	}
	if field.kind == fieldKindString {
		stringEligible := "isValidUTF8(" + field.valueSQL + ")"
		if field.textEligibleSQL != "" {
			stringEligible = "(" + field.textEligibleSQL + ") AND isValidUTF8(" +
				field.valueSQL + ")"
		}
		return "multiIf(isNull(" + field.valueSQL + "), CAST(? AS UInt8), " + stringEligible +
				", CAST(? AS UInt8), CAST(? AS UInt8))", []any{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeString),
				uint8(eventfields.StoredValueTypeBytes),
			}, nil
	}
	return "if(isNull(" + field.valueSQL + "), CAST(? AS UInt8), CAST(? AS UInt8))", []any{
		uint8(eventfields.StoredValueTypeNull),
		uint8(code),
	}, nil
}

func exactStoredMetadataPath(field fieldState) (string, bool) {
	if len(field.existsArgs) == 1 {
		if path, ok := field.existsArgs[0].(string); ok && path != "" {
			return path, true
		}
	}
	// Eval materializes an output for every row and therefore intentionally
	// rewrites existence to 1. A direct Dynamic assignment still retains the
	// source's exact descendant probe, which carries the same normalized leaf
	// path plus one trailing dot. Reuse it for semantic type metadata rather
	// than guessing from Dynamic's physical ClickHouse representation.
	if len(field.descendantArgs) == 1 {
		if prefix, ok := field.descendantArgs[0].(string); ok && strings.HasSuffix(prefix, ".") && len(prefix) > 1 {
			return strings.TrimSuffix(prefix, "."), true
		}
	}
	return "", false
}

func fixedFieldStoredType(field fieldState) (eventfields.StoredValueType, error) {
	switch field.kind {
	case fieldKindInvalid:
		return eventfields.StoredValueTypeNull, nil
	case fieldKindString:
		return eventfields.StoredValueTypeString, nil
	case fieldKindBool:
		return eventfields.StoredValueTypeBool, nil
	case fieldKindTime:
		return eventfields.StoredValueTypeTimestamp, nil
	case fieldKindStringArray:
		return eventfields.StoredValueTypeList, nil
	case fieldKindNumber:
		if strings.HasPrefix(field.numberType, "UInt") {
			return eventfields.StoredValueTypeUint64, nil
		}
		if strings.HasPrefix(field.numberType, "Float") || field.numberType == "" {
			return eventfields.StoredValueTypeDouble, nil
		}
		return eventfields.StoredValueTypeSint64, nil
	default:
		return 0, fmt.Errorf("unsupported compiled field kind %d", field.kind)
	}
}

func sortedSetValues(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
