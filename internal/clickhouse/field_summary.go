package clickhouse

import (
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

	readScope compiledReadScope
}

const (
	fieldSummarySourceCTE       = "__os_field_summary_source"
	fieldSummaryTypedCTE        = "__os_field_summary_typed"
	fieldSummaryEncodedCTE      = "__os_field_summary_encoded"
	fieldSummaryRowsCTE         = "__os_field_summary_rows"
	fieldSummaryTotalsCTE       = "__os_field_summary_totals"
	fieldSummaryGroupsCTE       = "__os_field_summary_groups"
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
)

// CompileFieldSummary compiles an exact typed scalar summary over the final
// event relation. The requested name is resolved with the same exact,
// case-sensitive SPL field rules used by the planner.
func (c Compiler) CompileFieldSummary(query *plan.Query, spec FieldSummarySpec) (CompiledFieldSummary, error) {
	if err := validateFieldSummarySpec(spec); err != nil {
		return CompiledFieldSummary{}, err
	}
	ref, err := plan.ResolveField(spec.FieldName, spl.Range{})
	if err != nil {
		return CompiledFieldSummary{}, err
	}

	fieldKnown := false
	compiled, err := c.compileEventAnalysis(query, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		aliasSequence int,
	) (CompiledQuery, error) {
		_, fieldKnown = state.visible[spec.FieldName]
		contract := fieldSummaryResultContract()
		policy := eventAnalysisFinalizationPolicyFor(state.chronologicalBarriers)
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
	return CompiledFieldSummary{
		SQL:        compiled.SQL,
		Args:       compiled.Args,
		Spec:       spec,
		FieldKnown: fieldKnown,
		readScope:  compiled.readScope,
	}, nil
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

const dynamicDecimalPayloadPattern = `'^(-|)(0|[1-9][0-9]*)([.][0-9]+|)([eE]([+]|-|)(0|[1-9][0-9]*)|)$'`

func newDynamicEnvelopePayloadValiditySQL(payload string) dynamicEnvelopePayloadValiditySQL {
	return dynamicEnvelopePayloadValiditySQL{
		bytesValid: "match(" + payload +
			", '^[A-Za-z0-9+/]*$') AND modulo(length(" + payload + "), 4) != 1",
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
		"if(" + physicalType + " = 'String', replaceRegexpOne(base64Encode(" + stringSQL +
		"), '=+$', ''), " + tagged.payload + "), " +
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
	default:
		return "0"
	}
}

func fieldSummaryFixedEncoding(field fieldState, storedType, value string) string {
	switch field.kind {
	case fieldKindString:
		return "if(" + storedType + " = toUInt8(" +
			fmt.Sprint(uint8(eventfields.StoredValueTypeBytes)) + "), replaceRegexpOne(base64Encode(toString(" +
			value + ")), '=+$', ''), toString(" + value + "))"
	case fieldKindNumber:
		return "toString(" + value + ")"
	case fieldKindBool:
		return "if(" + value + ", 'true', 'false')"
	case fieldKindTime:
		return "concat(replaceOne(toString(toDateTime64(" + value +
			", 9, 'UTC')), ' ', 'T'), 'Z')"
	default:
		return "CAST('' AS String)"
	}
}
