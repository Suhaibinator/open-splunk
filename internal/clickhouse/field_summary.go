package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// MaximumFieldSummaryValues bounds the caller-selected top-value result.
	// The compiler still emits every exact scalar group so the executor can
	// canonicalize semantically equal decimal spellings before selecting it.
	MaximumFieldSummaryValues uint32 = 100
	// MaximumFieldSummaryDistinctValues bounds raw (stored type, encoded value)
	// groups before executor canonicalization, and therefore also bounds the
	// semantic distinct domain. More raw groups fail even when equivalent
	// decimal spellings would later collapse.
	MaximumFieldSummaryDistinctValues uint32 = 10_000
	// MaximumFieldSummaryValueBytes is the largest encoded scalar admitted to a
	// summary. Larger values invalidate the whole result instead of truncating.
	MaximumFieldSummaryValueBytes uint32 = 256 << 10

	FieldSummaryRowKindColumn         = "__os_field_summary_row_kind"
	FieldSummaryFieldNameColumn       = "__os_field_summary_field_name"
	FieldSummaryObservedTypesColumn   = "__os_field_summary_observed_types"
	FieldSummaryEventCountColumn      = "__os_field_summary_event_count"
	FieldSummaryNullCountColumn       = "__os_field_summary_null_count"
	FieldSummaryMissingCountColumn    = "__os_field_summary_missing_count"
	FieldSummaryTotalEventCountColumn = "__os_field_summary_total_event_count"
	FieldSummaryValueTypeColumn       = "__os_field_summary_value_type"
	FieldSummaryEncodedValueColumn    = "__os_field_summary_encoded_value"
	FieldSummaryValueCountColumn      = "__os_field_summary_value_count"
	FieldSummaryMetadataInvalidColumn = "__os_field_summary_metadata_invalid"
	FieldSummaryUnsupportedColumn     = "__os_field_summary_unsupported"
	FieldSummaryOversizedColumn       = "__os_field_summary_oversized"
)

// ErrFieldSummaryNotFound means the requested exact spelling is not visible in
// the final relation. The service deliberately performs authorization before
// exposing this distinction to callers.
var ErrFieldSummaryNotFound = errors.New("field summary field is not in the final relation")

// FieldSummarySpec carries every effective resource bound. Zero values are
// invalid: callers must resolve their defaults before compilation.
type FieldSummarySpec struct {
	FieldName             string
	MaximumValues         uint32
	MaximumDistinctValues uint32
	MaximumValueBytes     uint32
}

// CompiledFieldSummary is one immutable, parameterized analysis query.
// FieldKnown distinguishes a field in the final closed schema from an
// open-schema dynamic reference whose presence can only be decided at runtime.
type CompiledFieldSummary struct {
	SQL        string
	Args       []any
	Spec       FieldSummarySpec
	FieldKnown bool

	readScope          compiledReadScope
	executionAuthority *derivedExecutionAuthority
}

const (
	fieldSummarySourceCTE       = "__os_field_summary_source"
	fieldSummaryTypedCTE        = "__os_field_summary_typed"
	fieldSummaryEncodedCTE      = "__os_field_summary_encoded"
	fieldSummaryRowsCTE         = "__os_field_summary_rows"
	fieldSummaryObservationsCTE = "__os_field_summary_observations"
	fieldSummaryTotalsCTE       = "__os_field_summary_totals"
	fieldSummaryGroupsCTE       = "__os_field_summary_groups"
	fieldSummaryControlledCTE   = "__os_field_summary_controlled"
	fieldSummaryPresent         = "__os_field_summary_present"
	fieldSummaryStoredType      = "__os_field_summary_stored_type"
	fieldSummaryRawValue        = "__os_field_summary_raw_value"
	fieldSummaryPhysicalType    = "__os_field_summary_physical_type"
	fieldSummaryAgreement       = "__os_field_summary_agreement"
	fieldSummaryEncoded         = "__os_field_summary_encoded"
	fieldSummaryRowInvalid      = "__os_field_summary_row_invalid"
	fieldSummaryRowUnsupported  = "__os_field_summary_row_unsupported"
	fieldSummaryRowOversized    = "__os_field_summary_row_oversized"
	fieldSummaryProfileTypes    = "__os_field_summary_profile_types"
	fieldSummaryProfileEvents   = "__os_field_summary_profile_events"
	fieldSummaryProfileNulls    = "__os_field_summary_profile_nulls"
	fieldSummaryProfileMissing  = "__os_field_summary_profile_missing"
	fieldSummaryProfileTotal    = "__os_field_summary_profile_total"
	fieldSummaryMetadataInvalid = "__os_field_summary_metadata_invalid_control"
	fieldSummaryUnsupported     = "__os_field_summary_unsupported_control"
	fieldSummaryOversized       = "__os_field_summary_oversized_control"
	fieldSummaryGroupType       = "__os_field_summary_group_type"
	fieldSummaryGroupEncoded    = "__os_field_summary_group_encoded"
	fieldSummaryGroupCount      = "__os_field_summary_group_count"
	fieldSummaryGroupRowKind    = "__os_field_summary_group_row_kind"
	fieldSummaryRowMetadataBad  = "__os_field_summary_row_metadata_bad"
	fieldSummaryObservation     = "__os_field_summary_observation"
	fieldSummaryObservedKind    = "__os_field_summary_observed_kind"
	fieldSummaryObservedType    = "__os_field_summary_observed_type"
	fieldSummaryObservedEncoded = "__os_field_summary_observed_encoded"
	fieldSummarySeenPresent     = "__os_field_summary_seen_present"
	fieldSummarySeenType        = "__os_field_summary_seen_type"
	fieldSummaryTotalWeight     = "__os_field_summary_total_weight"
	fieldSummaryEventWeight     = "__os_field_summary_event_weight"
	fieldSummaryNullWeight      = "__os_field_summary_null_weight"
	fieldSummaryInvalidEvidence = "__os_field_summary_invalid_evidence"
	fieldSummaryUnsupportedEv   = "__os_field_summary_unsupported_evidence"
	fieldSummaryOversizedEv     = "__os_field_summary_oversized_evidence"
	fieldSummaryValueWeight     = "__os_field_summary_value_weight"
	fieldSummaryGlobalInvalid   = "__os_field_summary_global_invalid"
	fieldSummaryGlobalUnsup     = "__os_field_summary_global_unsupported"
	fieldSummaryGlobalOversized = "__os_field_summary_global_oversized"
)

// CompileFieldSummary compiles an exact typed scalar summary over the final
// event relation. The requested name is resolved with the same exact,
// case-sensitive SPL field rules used by the planner.
func (c Compiler) CompileFieldSummary(query *plan.Query, spec FieldSummarySpec) (CompiledFieldSummary, error) {
	return c.CompileFieldSummaryContext(context.Background(), query, spec)
}

func (c Compiler) CompileFieldSummaryContext(
	ctx context.Context,
	query *plan.Query,
	spec FieldSummarySpec,
) (CompiledFieldSummary, error) {
	if ctx == nil {
		return CompiledFieldSummary{}, errors.New(
			"compile ClickHouse field summary: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return CompiledFieldSummary{}, err
	}
	if err := validateFieldSummarySpec(spec); err != nil {
		return CompiledFieldSummary{}, err
	}
	ref, err := plan.ResolveField(spec.FieldName, spl.Range{})
	if err != nil {
		return CompiledFieldSummary{}, err
	}

	fieldKnown := false
	compiled, err := c.compileEventAnalysisContext(ctx, query, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		aliasSequence int,
	) (CompiledQuery, error) {
		_, fieldKnown = state.visible[spec.FieldName]
		policy := eventAnalysisFinalizationPolicyFor(state.chronologicalBarriers)
		contract := fieldSummaryResultContractFor(policy)
		compiled, finalizeErr := finalizeFieldSummary(
			relation,
			state,
			args,
			ref,
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
		return CompiledFieldSummary{}, err
	}
	result := CompiledFieldSummary{
		SQL:        compiled.SQL,
		Args:       compiled.Args,
		Spec:       spec,
		FieldKnown: fieldKnown,
		readScope:  compiled.readScope,
	}
	result.executionAuthority, err = sealCompiledFieldSummaryExecutionContext(
		ctx,
		compiled,
		result,
	)
	if err != nil {
		return CompiledFieldSummary{}, err
	}
	return result, nil
}

func fieldSummaryResultContract() eventAnalysisResultContract {
	return eventAnalysisResultContract{
		sourceFanout: eventStatsSummarySourceFanout,
		columns: []string{
			FieldSummaryRowKindColumn,
			FieldSummaryFieldNameColumn,
			FieldSummaryObservedTypesColumn,
			FieldSummaryEventCountColumn,
			FieldSummaryNullCountColumn,
			FieldSummaryMissingCountColumn,
			FieldSummaryTotalEventCountColumn,
			FieldSummaryValueTypeColumn,
			FieldSummaryEncodedValueColumn,
			FieldSummaryValueCountColumn,
			FieldSummaryMetadataInvalidColumn,
			FieldSummaryUnsupportedColumn,
			FieldSummaryOversizedColumn,
		},
		order: quoteIdentifier(FieldSummaryRowKindColumn) + " ASC, " +
			quoteIdentifier(FieldSummaryValueTypeColumn) + " ASC, " +
			quoteIdentifier(FieldSummaryEncodedValueColumn) + " ASC",
	}
}

func fieldSummaryResultContractFor(
	policy eventAnalysisFinalizationPolicy,
) eventAnalysisResultContract {
	contract := fieldSummaryResultContract()
	if !policy.materializeSharedCTEs {
		contract.sourceFanout = eventStatsOrdinarySourceFanout
	}
	return contract
}

func fieldSummaryValidationDummyProjection() []string {
	q := quoteIdentifier
	return []string{
		"toUInt8(0) AS " + q(FieldSummaryRowKindColumn),
		"CAST('' AS String) AS " + q(FieldSummaryFieldNameColumn),
		"CAST([], 'Array(UInt8)') AS " + q(FieldSummaryObservedTypesColumn),
		"toUInt64(0) AS " + q(FieldSummaryEventCountColumn),
		"toUInt64(0) AS " + q(FieldSummaryNullCountColumn),
		"toUInt64(0) AS " + q(FieldSummaryMissingCountColumn),
		"toUInt64(0) AS " + q(FieldSummaryTotalEventCountColumn),
		"toUInt8(0) AS " + q(FieldSummaryValueTypeColumn),
		"CAST('' AS String) AS " + q(FieldSummaryEncodedValueColumn),
		"toUInt64(0) AS " + q(FieldSummaryValueCountColumn),
		"toUInt8(0) AS " + q(FieldSummaryMetadataInvalidColumn),
		"toUInt8(0) AS " + q(FieldSummaryUnsupportedColumn),
		"toUInt8(0) AS " + q(FieldSummaryOversizedColumn),
	}
}

func validateFieldSummarySpec(spec FieldSummarySpec) error {
	if spec.MaximumValues == 0 || spec.MaximumValues > MaximumFieldSummaryValues {
		return fmt.Errorf(
			"compile ClickHouse field summary: MaximumValues must be between 1 and %d",
			MaximumFieldSummaryValues,
		)
	}
	if spec.MaximumDistinctValues == 0 || spec.MaximumDistinctValues > MaximumFieldSummaryDistinctValues {
		return fmt.Errorf(
			"compile ClickHouse field summary: MaximumDistinctValues must be between 1 and %d",
			MaximumFieldSummaryDistinctValues,
		)
	}
	if spec.MaximumValues > spec.MaximumDistinctValues {
		return errors.New("compile ClickHouse field summary: MaximumValues cannot exceed MaximumDistinctValues")
	}
	if spec.MaximumValueBytes == 0 || spec.MaximumValueBytes > MaximumFieldSummaryValueBytes {
		return fmt.Errorf(
			"compile ClickHouse field summary: MaximumValueBytes must be between 1 and %d",
			MaximumFieldSummaryValueBytes,
		)
	}
	return nil
}

func finalizeFieldSummary(
	relation compiledRelation,
	state compileState,
	args []any,
	ref plan.FieldRef,
	spec FieldSummarySpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
) (CompiledQuery, error) {
	if !state.eventRows {
		return CompiledQuery{}, errors.New("compile ClickHouse field summary: final relation is not an event relation")
	}
	field, ok, err := resolveCompiledField(ref, state)
	if err != nil {
		return CompiledQuery{}, fmt.Errorf("compile ClickHouse field summary field %q: %w", spec.FieldName, err)
	}
	if !ok {
		return CompiledQuery{}, fmt.Errorf(
			"compile ClickHouse field summary field %q: %w",
			spec.FieldName,
			ErrFieldSummaryNotFound,
		)
	}
	presenceSQL, presenceArgs := knownFieldPresenceSQL(field)
	storedTypeSQL, storedTypeArgs, err := knownFieldStoredTypeSQL(field)
	if err != nil {
		return CompiledQuery{}, fmt.Errorf("compile ClickHouse field summary field %q: %w", spec.FieldName, err)
	}
	if !policy.materializeSharedCTEs {
		return finalizePrerequisiteFieldSummary(
			relation,
			args,
			field,
			presenceSQL,
			presenceArgs,
			storedTypeSQL,
			storedTypeArgs,
			spec,
			ownerRange,
			policy,
		)
	}

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 12_288)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSummarySourceCTE))
	sql.WriteString(" AS (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	// Keep the heterogeneous value out of GROUP BY: ClickHouse Dynamic has no
	// safe cross-type grouping contract. Share the already-narrow typed
	// projection before agreement and encoding: those expressions reuse the
	// Dynamic value and would otherwise make ClickHouse's analyzer clone a
	// complex final event pipeline. The ordinary analysis path materializes this
	// CTE; deferred eventstats graphs keep it ordinary so ClickHouse 26.3 can
	// schedule the flat dependency chain.
	sql.WriteString(q(fieldSummaryTypedCTE))
	writeCTEOpening(&sql, policy.materializeSharedCTEs)
	sql.WriteString("SELECT toUInt8(ifNull(")
	sql.WriteString(presenceSQL)
	sql.WriteString(", 0)) AS ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(storedTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", ")
	sql.WriteString(field.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryRawValue))
	if field.kind == fieldKindDynamic {
		sql.WriteString(", ")
		sql.WriteString(dynamicTypeExpression(field))
		sql.WriteString(" AS ")
		sql.WriteString(q(fieldSummaryPhysicalType))
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
	sql.WriteString(q(fieldSummarySourceCTE))
	sql.WriteString("), ")
	args = append(args, presenceArgs...)
	args = append(args, storedTypeArgs...)

	agreementSQL, encodedSQL := fieldSummaryScalarExpressions(field)
	sql.WriteString(q(fieldSummaryEncodedCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(")
	sql.WriteString(agreementSQL)
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(", ifNull(")
	sql.WriteString(encodedSQL)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(q(fieldSummaryEncoded))
	for _, column := range []string{
		internalFieldNamesColumn,
		internalFieldTypesColumn,
		internalFieldMetadataVersionColumn,
	} {
		sql.WriteString(", ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryTypedCTE))
	sql.WriteString("), ")

	sql.WriteString(q(fieldSummaryRowsCTE))
	writeCTEOpening(&sql, policy.materializeSharedCTEs)
	sql.WriteString("SELECT ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" = 0) AS ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(", toUInt8(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" != 0 AND ")
	writeFieldSummaryContainerTypePredicate(&sql)
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(", toUInt8(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" != toUInt8(")
	fmt.Fprint(&sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(") AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" != 0 AND NOT (")
	writeFieldSummaryContainerTypePredicate(&sql)
	sql.WriteString(") AND length(")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(") > CAST(? AS UInt64)) AS ")
	sql.WriteString(q(fieldSummaryRowOversized))
	for _, column := range []string{
		internalFieldNamesColumn,
		internalFieldTypesColumn,
		internalFieldMetadataVersionColumn,
	} {
		sql.WriteString(", ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryEncodedCTE))
	sql.WriteString("), ")
	args = append(args, uint64(spec.MaximumValueBytes))

	writeFieldSummaryTotals(&sql, policy.materializeSharedCTEs)
	args = append(args,
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	)
	sql.WriteString(", ")

	sql.WriteString(q(fieldSummaryGroupsCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryGroupType))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryGroupEncoded))
	sql.WriteString(", count() AS ")
	sql.WriteString(q(fieldSummaryGroupCount))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryRowsCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" != toUInt8(")
	fmt.Fprint(&sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(") AND ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryRowOversized))
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(q(fieldSummaryGroupType))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryGroupEncoded))
	sql.WriteString(") ")

	writeFieldSummaryResult(&sql, policy.includeResultOrder)
	sql.WriteString(materializedCTESettingsSQL)
	args = append(args, spec.FieldName)

	typedDepth := relationalNodeDepth(relation.depth)
	encodedDepth := relationalNodeDepth(typedDepth)
	rowsDepth := relationalNodeDepth(encodedDepth)
	totalsDepth := relationalNodeDepth(rowsDepth)
	groupsDepth := relationalNodeDepth(rowsDepth)
	headerDepth := relationalNodeDepth(totalsDepth)
	valueRowsDepth := relationalNodeDepth(groupsDepth, totalsDepth)
	outputUnionDepth := relationalNodeDepth(headerDepth, valueRowsDepth)
	resultDepth := relationalNodeDepth(outputUnionDepth)

	compiled := CompiledQuery{SQL: sql.String(), Args: args}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

// finalizePrerequisiteFieldSummary keeps the immutable chronological input on
// one dependency chain. Each event contributes one control observation and at
// most one exact-value observation; a zero-weight control observation preserves
// the header for an empty input. The only window runs after the distinct-value
// GROUP BY, so its state is bounded by MaximumDistinctValues plus the header.
func finalizePrerequisiteFieldSummary(
	relation compiledRelation,
	args []any,
	field fieldState,
	presenceSQL string,
	presenceArgs []any,
	storedTypeSQL string,
	storedTypeArgs []any,
	spec FieldSummarySpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
) (CompiledQuery, error) {
	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 16_384)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSummarySourceCTE))
	sql.WriteString(" AS (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	// Keep each expression in its own stage. Besides avoiding repeated Dynamic
	// evaluation, this prevents ClickHouse's global alias substitution from
	// replacing an input column with a same-SELECT output alias.
	sql.WriteString(q(fieldSummaryTypedCTE))
	sql.WriteString(" AS (SELECT toUInt8(ifNull(")
	sql.WriteString(presenceSQL)
	sql.WriteString(", 0)) AS ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(storedTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", ")
	sql.WriteString(field.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryRawValue))
	if field.kind == fieldKindDynamic {
		sql.WriteString(", ")
		sql.WriteString(dynamicTypeExpression(field))
		sql.WriteString(" AS ")
		sql.WriteString(q(fieldSummaryPhysicalType))
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
	sql.WriteString(q(fieldSummarySourceCTE))
	sql.WriteString("), ")
	args = append(args, presenceArgs...)
	args = append(args, storedTypeArgs...)

	agreementSQL, encodedSQL := fieldSummaryScalarExpressions(field)
	sql.WriteString(q(fieldSummaryEncodedCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(")
	sql.WriteString(agreementSQL)
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(", ifNull(")
	sql.WriteString(encodedSQL)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(q(fieldSummaryEncoded))
	for _, column := range []string{
		internalFieldNamesColumn,
		internalFieldTypesColumn,
		internalFieldMetadataVersionColumn,
	} {
		sql.WriteString(", ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryTypedCTE))
	sql.WriteString("), ")

	sql.WriteString(q(fieldSummaryRowsCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" = 0, 0)) AS ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(", toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" != 0 AND ")
	writeFieldSummaryContainerTypePredicate(&sql)
	sql.WriteString(", 0)) AS ")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(", toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" != toUInt8(")
	fmt.Fprint(&sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(") AND ")
	sql.WriteString(q(fieldSummaryAgreement))
	sql.WriteString(" != 0 AND NOT (")
	writeFieldSummaryContainerTypePredicate(&sql)
	sql.WriteString(") AND length(")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(") > CAST(? AS UInt64), 0)) AS ")
	sql.WriteString(q(fieldSummaryRowOversized))
	sql.WriteString(", toUInt8(")
	writePrerequisiteFieldSummaryMetadataPredicate(&sql)
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryRowMetadataBad))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryEncodedCTE))
	sql.WriteString("), ")
	args = append(args,
		uint64(spec.MaximumValueBytes),
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	)

	writePrerequisiteFieldSummaryObservations(&sql)
	sql.WriteString(", ")
	writePrerequisiteFieldSummaryGroups(&sql)
	sql.WriteString(", ")

	sql.WriteString(q(fieldSummaryControlledCTE))
	sql.WriteString(" AS (SELECT *, toUInt8(max(if(")
	sql.WriteString(q(fieldSummaryGroupRowKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldSummaryMetadataInvalid))
	sql.WriteString(", toUInt8(0))) OVER ()) AS ")
	sql.WriteString(q(fieldSummaryGlobalInvalid))
	sql.WriteString(", toUInt8(max(if(")
	sql.WriteString(q(fieldSummaryGroupRowKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldSummaryUnsupported))
	sql.WriteString(", toUInt8(0))) OVER ()) AS ")
	sql.WriteString(q(fieldSummaryGlobalUnsup))
	sql.WriteString(", toUInt8(max(if(")
	sql.WriteString(q(fieldSummaryGroupRowKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldSummaryOversized))
	sql.WriteString(", toUInt8(0))) OVER ()) AS ")
	sql.WriteString(q(fieldSummaryGlobalOversized))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryGroupsCTE))
	sql.WriteString(") ")

	writePrerequisiteFieldSummaryResult(&sql, policy.includeResultOrder)
	args = append(args, spec.FieldName)
	sql.WriteString(materializedCTESettingsSQL)

	typedDepth := relationalNodeDepth(relation.depth)
	encodedDepth := relationalNodeDepth(typedDepth)
	rowsDepth := relationalNodeDepth(encodedDepth)
	observationRowsDepth := relationalNodeDepth(rowsDepth)
	syntheticObservationDepth := relationalNodeDepth()
	observationsDepth := relationalNodeDepth(observationRowsDepth, syntheticObservationDepth)
	groupsDepth := relationalNodeDepth(observationsDepth)
	controlledDepth := relationalNodeDepth(groupsDepth)
	resultDepth := relationalNodeDepth(controlledDepth)

	compiled := CompiledQuery{
		SQL:                       sql.String(),
		Args:                      args,
		validationDummyProjection: fieldSummaryValidationDummyProjection(),
	}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func writePrerequisiteFieldSummaryMetadataPredicate(sql *strings.Builder) {
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
	sql.WriteString(")")
}

func writePrerequisiteFieldSummaryObservations(sql *strings.Builder) {
	q := quoteIdentifier
	fields := []string{
		fieldSummaryObservedKind,
		fieldSummaryObservedType,
		fieldSummaryObservedEncoded,
		fieldSummarySeenPresent,
		fieldSummarySeenType,
		fieldSummaryTotalWeight,
		fieldSummaryEventWeight,
		fieldSummaryNullWeight,
		fieldSummaryInvalidEvidence,
		fieldSummaryUnsupportedEv,
		fieldSummaryOversizedEv,
		fieldSummaryValueWeight,
	}

	sql.WriteString(q(fieldSummaryObservationsCTE))
	sql.WriteString(" AS (SELECT ")
	for index, name := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("tupleElement(")
		sql.WriteString(q(fieldSummaryObservation))
		sql.WriteString(", ")
		fmt.Fprint(sql, index+1)
		sql.WriteString(") AS ")
		sql.WriteString(q(name))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryRowsCTE))
	sql.WriteString(" ARRAY JOIN arrayConcat([tuple(toUInt8(0), toUInt8(0), CAST('' AS String), toUInt8(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0), toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(0))), toUInt64(1), toUInt64(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0), toUInt64(ifNull(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" = toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString("), 0)), toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryRowMetadataBad))
	sql.WriteString(" != 0 OR ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(" != 0, 0)), toUInt8(")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString("), toUInt8(")
	sql.WriteString(q(fieldSummaryRowOversized))
	sql.WriteString("), toUInt64(0))], arrayResize([tuple(toUInt8(1), toUInt8(ifNull(")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(", toUInt8(0))), ")
	sql.WriteString(q(fieldSummaryEncoded))
	sql.WriteString(", toUInt8(0), toUInt8(0), toUInt64(0), toUInt64(0), toUInt64(0), toUInt8(0), toUInt8(0), toUInt8(0), toUInt64(1))], toUInt64(ifNull(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" != toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(") AND ")
	sql.WriteString(q(fieldSummaryRowMetadataBad))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryRowOversized))
	sql.WriteString(" = 0, 0)))) AS ")
	sql.WriteString(q(fieldSummaryObservation))
	sql.WriteString(" UNION ALL SELECT toUInt8(0), toUInt8(0), CAST('' AS String), toUInt8(0), toUInt8(0), toUInt64(0), toUInt64(0), toUInt64(0), toUInt8(0), toUInt8(0), toUInt8(0), toUInt64(0))")
}

func writePrerequisiteFieldSummaryGroups(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSummaryGroupsCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSummaryObservedKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryGroupRowKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryObservedType))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryGroupType))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryObservedEncoded))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSummaryGroupEncoded))
	sql.WriteString(", arraySort(groupUniqArrayIf(toUInt8(")
	sql.WriteString(q(fieldSummarySeenType))
	sql.WriteString("), ")
	sql.WriteString(q(fieldSummarySeenPresent))
	sql.WriteString(" != 0)) AS ")
	sql.WriteString(q(fieldSummaryProfileTypes))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldSummaryEventWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryProfileEvents))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldSummaryNullWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryProfileNulls))
	sql.WriteString(", toUInt64(sum(")
	sql.WriteString(q(fieldSummaryTotalWeight))
	sql.WriteString(") - sum(")
	sql.WriteString(q(fieldSummaryEventWeight))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSummaryProfileMissing))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldSummaryTotalWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryProfileTotal))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldSummaryInvalidEvidence))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSummaryMetadataInvalid))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldSummaryUnsupportedEv))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSummaryUnsupported))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldSummaryOversizedEv))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSummaryOversized))
	sql.WriteString(", sum(")
	sql.WriteString(q(fieldSummaryValueWeight))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSummaryGroupCount))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryObservationsCTE))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(fieldSummaryObservedKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryObservedType))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryObservedEncoded))
	sql.WriteString(")")
}

func writePrerequisiteFieldSummaryResult(sql *strings.Builder, includeResultOrder bool) {
	q := quoteIdentifier
	header := q(fieldSummaryGroupRowKind) + " = toUInt8(0)"
	sql.WriteString("SELECT ")
	sql.WriteString(q(fieldSummaryGroupRowKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSummaryRowKindColumn))
	sql.WriteString(", if(")
	sql.WriteString(header)
	sql.WriteString(", CAST(? AS String), CAST('' AS String)) AS ")
	sql.WriteString(q(FieldSummaryFieldNameColumn))
	sql.WriteString(", if(")
	sql.WriteString(header)
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryProfileTypes))
	sql.WriteString(", CAST([], 'Array(UInt8)')) AS ")
	sql.WriteString(q(FieldSummaryObservedTypesColumn))
	for _, pair := range [][2]string{
		{fieldSummaryProfileEvents, FieldSummaryEventCountColumn},
		{fieldSummaryProfileNulls, FieldSummaryNullCountColumn},
		{fieldSummaryProfileMissing, FieldSummaryMissingCountColumn},
		{fieldSummaryProfileTotal, FieldSummaryTotalEventCountColumn},
	} {
		sql.WriteString(", if(")
		sql.WriteString(header)
		sql.WriteString(", ")
		sql.WriteString(q(pair[0]))
		sql.WriteString(", toUInt64(0)) AS ")
		sql.WriteString(q(pair[1]))
	}
	sql.WriteString(", if(")
	sql.WriteString(header)
	sql.WriteString(", toUInt8(0), ")
	sql.WriteString(q(fieldSummaryGroupType))
	sql.WriteString(") AS ")
	sql.WriteString(q(FieldSummaryValueTypeColumn))
	sql.WriteString(", if(")
	sql.WriteString(header)
	sql.WriteString(", CAST('' AS String), ")
	sql.WriteString(q(fieldSummaryGroupEncoded))
	sql.WriteString(") AS ")
	sql.WriteString(q(FieldSummaryEncodedValueColumn))
	sql.WriteString(", if(")
	sql.WriteString(header)
	sql.WriteString(", toUInt64(0), ")
	sql.WriteString(q(fieldSummaryGroupCount))
	sql.WriteString(") AS ")
	sql.WriteString(q(FieldSummaryValueCountColumn))
	for _, pair := range [][2]string{
		{fieldSummaryMetadataInvalid, FieldSummaryMetadataInvalidColumn},
		{fieldSummaryUnsupported, FieldSummaryUnsupportedColumn},
		{fieldSummaryOversized, FieldSummaryOversizedColumn},
	} {
		sql.WriteString(", if(")
		sql.WriteString(header)
		sql.WriteString(", ")
		sql.WriteString(q(pair[0]))
		sql.WriteString(", toUInt8(0)) AS ")
		sql.WriteString(q(pair[1]))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryControlledCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(header)
	sql.WriteString(" OR (")
	sql.WriteString(q(fieldSummaryGlobalInvalid))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryGlobalUnsup))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryGlobalOversized))
	sql.WriteString(" = 0)")
	if includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldSummaryResultContract().order)
	}
}

func writeFieldSummaryTotals(sql *strings.Builder, materialized bool) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSummaryTotalsCTE))
	writeCTEOpening(sql, materialized)
	sql.WriteString("SELECT arraySort(groupUniqArrayIf(toUInt8(")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString("), ")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0)) AS ")
	sql.WriteString(q(fieldSummaryProfileTypes))
	sql.WriteString(", countIf(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0) AS ")
	sql.WriteString(q(fieldSummaryProfileEvents))
	sql.WriteString(", countIf(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0 AND ")
	sql.WriteString(q(fieldSummaryStoredType))
	sql.WriteString(" = toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeNull))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSummaryProfileNulls))
	sql.WriteString(", toUInt64(count() - countIf(")
	sql.WriteString(q(fieldSummaryPresent))
	sql.WriteString(" != 0)) AS ")
	sql.WriteString(q(fieldSummaryProfileMissing))
	sql.WriteString(", count() AS ")
	sql.WriteString(q(fieldSummaryProfileTotal))

	// This is deliberately byte-for-byte equivalent in semantics to the field
	// catalog metadata guard. One corrupt event invalidates the whole result.
	sql.WriteString(", toUInt8(countIf(")
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
	sql.WriteString(") OR ")
	sql.WriteString(q(fieldSummaryRowInvalid))
	sql.WriteString(" != 0) > 0) AS ")
	sql.WriteString(q(fieldSummaryMetadataInvalid))
	sql.WriteString(", toUInt8(countIf(")
	sql.WriteString(q(fieldSummaryRowUnsupported))
	sql.WriteString(" != 0) > 0) AS ")
	sql.WriteString(q(fieldSummaryUnsupported))
	sql.WriteString(", toUInt8(countIf(")
	sql.WriteString(q(fieldSummaryRowOversized))
	sql.WriteString(" != 0) > 0) AS ")
	sql.WriteString(q(fieldSummaryOversized))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryRowsCTE))
	sql.WriteString(")")
}

func writeFieldSummaryResult(sql *strings.Builder, includeResultOrder bool) {
	q := quoteIdentifier
	sql.WriteString("SELECT * FROM (SELECT toUInt8(0) AS ")
	sql.WriteString(q(FieldSummaryRowKindColumn))
	sql.WriteString(", CAST(? AS String) AS ")
	sql.WriteString(q(FieldSummaryFieldNameColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryProfileTypes))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSummaryObservedTypesColumn))
	for _, pair := range [][2]string{
		{fieldSummaryProfileEvents, FieldSummaryEventCountColumn},
		{fieldSummaryProfileNulls, FieldSummaryNullCountColumn},
		{fieldSummaryProfileMissing, FieldSummaryMissingCountColumn},
		{fieldSummaryProfileTotal, FieldSummaryTotalEventCountColumn},
	} {
		sql.WriteString(", ")
		sql.WriteString(q(pair[0]))
		sql.WriteString(" AS ")
		sql.WriteString(q(pair[1]))
	}
	sql.WriteString(", toUInt8(0) AS ")
	sql.WriteString(q(FieldSummaryValueTypeColumn))
	sql.WriteString(", CAST('' AS String) AS ")
	sql.WriteString(q(FieldSummaryEncodedValueColumn))
	sql.WriteString(", toUInt64(0) AS ")
	sql.WriteString(q(FieldSummaryValueCountColumn))
	for _, pair := range [][2]string{
		{fieldSummaryMetadataInvalid, FieldSummaryMetadataInvalidColumn},
		{fieldSummaryUnsupported, FieldSummaryUnsupportedColumn},
		{fieldSummaryOversized, FieldSummaryOversizedColumn},
	} {
		sql.WriteString(", ")
		sql.WriteString(q(pair[0]))
		sql.WriteString(" AS ")
		sql.WriteString(q(pair[1]))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryTotalsCTE))

	sql.WriteString(" UNION ALL SELECT toUInt8(1) AS ")
	sql.WriteString(q(FieldSummaryRowKindColumn))
	sql.WriteString(", CAST('' AS String) AS ")
	sql.WriteString(q(FieldSummaryFieldNameColumn))
	sql.WriteString(", CAST([], 'Array(UInt8)') AS ")
	sql.WriteString(q(FieldSummaryObservedTypesColumn))
	for _, column := range []string{
		FieldSummaryEventCountColumn,
		FieldSummaryNullCountColumn,
		FieldSummaryMissingCountColumn,
		FieldSummaryTotalEventCountColumn,
	} {
		sql.WriteString(", toUInt64(0) AS ")
		sql.WriteString(q(column))
	}
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryGroupType))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSummaryValueTypeColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryGroupEncoded))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSummaryEncodedValueColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSummaryGroupCount))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSummaryValueCountColumn))
	for _, column := range []string{
		FieldSummaryMetadataInvalidColumn,
		FieldSummaryUnsupportedColumn,
		FieldSummaryOversizedColumn,
	} {
		sql.WriteString(", toUInt8(0) AS ")
		sql.WriteString(q(column))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSummaryGroupsCTE))
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(q(fieldSummaryTotalsCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(q(fieldSummaryMetadataInvalid))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryUnsupported))
	sql.WriteString(" = 0 AND ")
	sql.WriteString(q(fieldSummaryOversized))
	sql.WriteString(" = 0)")
	if includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldSummaryResultContract().order)
	}
}

type dynamicEnvelopeSQL struct {
	mapSQL         string
	typeKey        string
	envelope       string
	payload        string
	bytesValid     string
	timestampValid string
	durationValid  string
	decimalValid   string
}

type dynamicEnvelopePayloadValiditySQL struct {
	bytesValid     string
	timestampValid string
	durationValid  string
	decimalValid   string
}

const (
	dynamicBytesPayloadPattern = `'^([A-Za-z0-9+/][A-Za-z0-9+/][A-Za-z0-9+/][A-Za-z0-9+/])*` +
		`([A-Za-z0-9+/][AQgw]|[A-Za-z0-9+/][A-Za-z0-9+/][AEIMQUYcgkosw048]|)$'`
	dynamicDecimalPayloadPattern = `'^(-|)(0|[1-9][0-9]*)([.][0-9]+|)([eE]([+]|-|)(0|[1-9][0-9]*)|)$'`
)

func newDynamicEnvelopePayloadValiditySQL(payload string) dynamicEnvelopePayloadValiditySQL {
	return dynamicEnvelopePayloadValiditySQL{
		bytesValid: "match(" + payload + ", " + dynamicBytesPayloadPattern + ")",
		timestampValid: "match(" + payload +
			", '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]([.][0-9]+|)Z$')" +
			" AND (position(" + payload + ", '.') = 0 OR length(" + payload +
			") - position(" + payload + ", '.') - 1 BETWEEN 1 AND 9)",
		durationValid: "match(" + payload + ", '^(-|)(0|[1-9][0-9]*):(-|)(0|[1-9][0-9]*)$')",
		decimalValid:  "match(" + payload + ", " + dynamicDecimalPayloadPattern + ")",
	}
}

func newDynamicEnvelopeSQL(valueSQL, typeSQL string) dynamicEnvelopeSQL {
	mapSQL := "dynamicElement(" + valueSQL + ", 'Map(String, String)')"
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	envelope := "(" + typeSQL + " = 'Map(String, String)'" +
		" AND length(" + mapSQL + ") = 2" +
		" AND mapContains(" + mapSQL + ", " + typeKey + ")" +
		" AND mapContains(" + mapSQL + ", " + valueKey + "))"
	payload := mapSQL + "[" + valueKey + "]"
	validity := newDynamicEnvelopePayloadValiditySQL(payload)
	return dynamicEnvelopeSQL{
		mapSQL:         mapSQL,
		typeKey:        typeKey,
		envelope:       envelope,
		payload:        payload,
		bytesValid:     validity.bytesValid,
		timestampValid: validity.timestampValid,
		durationValid:  validity.durationValid,
		decimalValid:   validity.decimalValid,
	}
}

func fieldSummaryScalarExpressions(field fieldState) (agreement, encoded string) {
	storedType := quoteIdentifier(fieldSummaryStoredType)
	value := quoteIdentifier(fieldSummaryRawValue)
	nullCode := fmt.Sprint(uint8(eventfields.StoredValueTypeNull))

	if field.kind != fieldKindDynamic {
		validType := fieldSummaryFixedTypePredicate(field, storedType)
		agreement = storedType + " = toUInt8(" + nullCode + ") OR (" + validType + ")"
		return agreement, fieldSummaryFixedEncoding(field, storedType, value)
	}
	if field.storedTypeSQL != "" {
		return fieldSummaryRuntimeDynamicExpressions(storedType, value)
	}

	physicalType := quoteIdentifier(fieldSummaryPhysicalType)
	tagged := newDynamicEnvelopeSQL(value, physicalType)

	agreement = "multiIf(" +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeNull)) + "), " + physicalType + " = 'None', " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeString)) + "), " +
		physicalType + " = 'String' AND isValidUTF8(dynamicElement(" + value + ", 'String')), " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeSint64)) + "), " + physicalType + " = 'Int64', " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeUint64)) + "), " + physicalType + " = 'UInt64', " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeDouble)) + "), " + physicalType + " = 'Float64', " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBool)) + "), " + physicalType + " = 'Bool', " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBytes)) + "), " +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'bytes/v1' AND " +
		tagged.bytesValid + ", " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeTimestamp)) + "), " +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'timestamp/v1' AND " +
		tagged.timestampValid + ", " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeDuration)) + "), " +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'duration/v1' AND " +
		tagged.durationValid + ", " +
		storedType + " IN (toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeList)) + "), toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeObject)) + ")), 1, " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeDecimal)) + "), " +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'decimal/v1' AND " +
		tagged.decimalValid + ", " +
		"0)"
	encoded = "multiIf(" +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeString)) + "), dynamicElement(" + value + ", 'String'), " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeSint64)) + "), toString(dynamicElement(" + value + ", 'Int64')), " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeUint64)) + "), toString(dynamicElement(" + value + ", 'UInt64')), " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeDouble)) + "), toString(dynamicElement(" + value + ", 'Float64')), " +
		storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBool)) + "), if(dynamicElement(" + value + ", 'Bool'), 'true', 'false'), " +
		storedType + " IN (toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBytes)) + "), toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeTimestamp)) + "), toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeDuration)) + "), toUInt8(" +
		fmt.Sprint(uint8(eventfields.StoredValueTypeDecimal)) + ")), " + tagged.payload + ", " +
		"CAST('' AS String))"
	return agreement, encoded
}

func fieldSummaryRuntimeDynamicExpressions(storedType, value string) (agreement, encoded string) {
	physicalType := quoteIdentifier(fieldSummaryPhysicalType)
	stringSQL := "dynamicElement(" + value + ", 'String')"
	tagged := newDynamicEnvelopeSQL(value, physicalType)
	decimalPhysical := "(startsWith(" + physicalType + ", 'Decimal') OR " +
		physicalType + " = 'Int256')"
	code := func(value eventfields.StoredValueType) string {
		return "toUInt8(" + fmt.Sprint(uint8(value)) + ")"
	}

	agreement = "multiIf(" +
		storedType + " = " + code(eventfields.StoredValueTypeNull) + ", " + physicalType + " = 'None', " +
		storedType + " = " + code(eventfields.StoredValueTypeString) + ", " +
		physicalType + " = 'String' AND isValidUTF8(" + stringSQL + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeSint64) + ", startsWith(" + physicalType + ", 'Int'), " +
		storedType + " = " + code(eventfields.StoredValueTypeUint64) + ", startsWith(" + physicalType + ", 'UInt'), " +
		storedType + " = " + code(eventfields.StoredValueTypeDouble) + ", " +
		"startsWith(" + physicalType + ", 'Float') AND isFinite(toFloat64(" + value + ")), " +
		storedType + " = " + code(eventfields.StoredValueTypeBool) + ", " + physicalType + " = 'Bool', " +
		storedType + " = " + code(eventfields.StoredValueTypeBytes) + ", " +
		"(" + physicalType + " = 'String' AND NOT isValidUTF8(" + stringSQL + ")) OR (" +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'bytes/v1' AND " +
		tagged.bytesValid + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeTimestamp) + ", " +
		"startsWith(" + physicalType + ", 'Date') OR (" + tagged.envelope + " AND " +
		tagged.mapSQL + "[" + tagged.typeKey + "] = 'timestamp/v1' AND " + tagged.timestampValid + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeDuration) + ", " +
		tagged.envelope + " AND " + tagged.mapSQL + "[" + tagged.typeKey + "] = 'duration/v1' AND " +
		tagged.durationValid + ", " +
		storedType + " IN (" + code(eventfields.StoredValueTypeList) + ", " +
		code(eventfields.StoredValueTypeObject) + "), 1, " +
		storedType + " = " + code(eventfields.StoredValueTypeDecimal) + ", " +
		decimalPhysical + " OR (" + tagged.envelope + " AND " +
		tagged.mapSQL + "[" + tagged.typeKey + "] = 'decimal/v1' AND " + tagged.decimalValid + "), 0)"

	timestamp := "concat(replaceOne(toString(toDateTime64(" + value + ", 9, 'UTC')), ' ', 'T'), 'Z')"
	encoded = "multiIf(" +
		storedType + " = " + code(eventfields.StoredValueTypeString) + ", " + stringSQL + ", " +
		storedType + " = " + code(eventfields.StoredValueTypeSint64) + ", toString(" + value + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeUint64) + ", toString(" + value + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeDouble) + ", toString(" + value + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeBool) + ", " +
		"if(dynamicElement(" + value + ", 'Bool'), 'true', 'false'), " +
		storedType + " = " + code(eventfields.StoredValueTypeBytes) + ", " +
		"if(" + physicalType + " = 'String', " + rawStdBase64EncodeSQL(stringSQL) +
		", " + tagged.payload + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeTimestamp) + ", " +
		"if(startsWith(" + physicalType + ", 'Date'), " + timestamp + ", " + tagged.payload + "), " +
		storedType + " = " + code(eventfields.StoredValueTypeDuration) + ", " + tagged.payload + ", " +
		storedType + " = " + code(eventfields.StoredValueTypeDecimal) + ", " +
		"if(" + decimalPhysical + ", toString(" + value + "), " + tagged.payload + "), " +
		"CAST('' AS String))"
	return agreement, encoded
}

func writeFieldSummaryContainerTypePredicate(sql *strings.Builder) {
	sql.WriteString(quoteIdentifier(fieldSummaryStoredType))
	sql.WriteString(" IN (toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeList))
	sql.WriteString("), toUInt8(")
	fmt.Fprint(sql, uint8(eventfields.StoredValueTypeObject))
	sql.WriteString("))")
}

func fieldSummaryFixedTypePredicate(field fieldState, storedType string) string {
	null := "toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeNull)) + ")"
	switch field.kind {
	case fieldKindInvalid:
		return storedType + " = " + null
	case fieldKindString:
		return storedType + " IN (" +
			"toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeString)) + "), " +
			"toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBytes)) + "))"
	case fieldKindNumber:
		code, err := fixedFieldStoredType(field)
		if err != nil {
			return "0"
		}
		return storedType + " = toUInt8(" + fmt.Sprint(uint8(code)) + ")"
	case fieldKindBool:
		return storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeBool)) + ")"
	case fieldKindTime:
		return storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeTimestamp)) + ")"
	case fieldKindStringArray:
		return storedType + " = toUInt8(" + fmt.Sprint(uint8(eventfields.StoredValueTypeList)) + ")"
	default:
		return "0"
	}
}

func fieldSummaryFixedEncoding(field fieldState, storedType, value string) string {
	switch field.kind {
	case fieldKindString:
		return "if(" + storedType + " = toUInt8(" +
			fmt.Sprint(uint8(eventfields.StoredValueTypeBytes)) + "), " +
			rawStdBase64EncodeSQL("toString("+value+")") + ", toString(" + value + "))"
	case fieldKindNumber:
		return "toString(" + value + ")"
	case fieldKindBool:
		return "if(" + value + ", 'true', 'false')"
	case fieldKindTime:
		return "concat(replaceOne(toString(toDateTime64(" + value +
			", 9, 'UTC')), ' ', 'T'), 'Z')"
	case fieldKindStringArray:
		// Exact field summaries deliberately reject structured values after
		// profiling their semantic type. Keep the fixed String transport typed;
		// the container predicate marks every present array unsupported before
		// any value group can be emitted.
		return "CAST('' AS String)"
	default:
		return "CAST('' AS String)"
	}
}
