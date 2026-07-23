package clickhouse

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const (
	// MaximumFieldCatalogFields is the hard cross-layer bound for one complete
	// field catalog. Callers may configure a lower limit, but the compiler,
	// executor, and analysis service must all reject larger contracts.
	MaximumFieldCatalogFields uint32 = 10_000

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
}

// CompileFieldCatalog compiles an exact catalog over the final event relation.
func (c Compiler) CompileFieldCatalog(query *plan.Query, spec FieldCatalogSpec) (CompiledFieldCatalog, error) {
	if spec.MaximumFields == 0 || spec.MaximumFields > MaximumFieldCatalogFields {
		return CompiledFieldCatalog{}, fmt.Errorf(
			"compile ClickHouse field catalog: MaximumFields must be between 1 and %d", MaximumFieldCatalogFields,
		)
	}
	compiled, err := c.compileEventAnalysis(query, func(
		fragment string,
		state compileState,
		args []any,
		_ *plan.Scan,
		_ int,
	) (CompiledQuery, error) {
		return finalizeFieldCatalog(fragment, state, args, spec)
	})
	if err != nil {
		return CompiledFieldCatalog{}, err
	}
	return CompiledFieldCatalog{
		SQL:  compiled.SQL,
		Args: compiled.Args,
		Spec: spec,
	}, nil
}

const (
	fieldCatalogSourceCTE       = "__os_field_catalog_source"
	fieldCatalogTotalsCTE       = "__os_field_catalog_totals"
	fieldCatalogKnownRowsCTE    = "__os_field_catalog_known_rows"
	fieldCatalogProfilesCTE     = "__os_field_catalog_profiles"
	fieldCatalogLimitedCTE      = "__os_field_catalog_limited"
	fieldCatalogDynamicName     = "__os_field_catalog_dynamic_name"
	fieldCatalogStoredType      = "__os_field_catalog_stored_type"
	fieldCatalogPresent         = "__os_field_catalog_present"
	fieldCatalogType            = "__os_field_catalog_type"
	fieldCatalogProfileName     = "__os_field_catalog_profile_name"
	fieldCatalogProfileTypes    = "__os_field_catalog_profile_types"
	fieldCatalogProfileEvents   = "__os_field_catalog_profile_events"
	fieldCatalogProfileNulls    = "__os_field_catalog_profile_nulls"
	fieldCatalogProfileMissing  = "__os_field_catalog_profile_missing"
	fieldCatalogProfileTotal    = "__os_field_catalog_profile_total"
	fieldCatalogMetadataInvalid = "__os_field_catalog_metadata_invalid"
	fieldCatalogKnownNames      = "__os_field_catalog_known_names"
	fieldCatalogKnownTypes      = "__os_field_catalog_known_types"
	fieldCatalogKnownEvents     = "__os_field_catalog_known_events"
	fieldCatalogKnownNulls      = "__os_field_catalog_known_nulls"
	fieldCatalogKnownMissing    = "__os_field_catalog_known_missing"
	fieldCatalogKnownTotals     = "__os_field_catalog_known_total_events"
)

type compiledKnownField struct {
	name         string
	presenceSQL  string
	presenceArgs []any
	typeSQL      string
	typeArgs     []any
}

func finalizeFieldCatalog(fragment string, state compileState, args []any, spec FieldCatalogSpec) (CompiledQuery, error) {
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

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(fragment) + 8_192 + len(knownNames)*768)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldCatalogSourceCTE))
	sql.WriteString(" AS MATERIALIZED (")
	sql.WriteString(fragment)
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
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(FieldCatalogRowKindColumn))
	sql.WriteString(" ASC, ")
	sql.WriteString(q(FieldCatalogNameColumn))
	sql.WriteString(" ASC")

	return CompiledQuery{SQL: sql.String(), Args: args}, nil
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
		known = append(known, compiledKnownField{
			name:        name,
			presenceSQL: presenceSQL, presenceArgs: presenceArgs,
			typeSQL: typeSQL, typeArgs: typeArgs,
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

// writeKnownFieldRows projects every known field's heterogeneous value and
// scalar analysis inputs in one pass over the materialized final relation.
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
	for index := 0; index < count; index++ {
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
		return "multiIf(isNull(" + field.valueSQL + "), CAST(? AS UInt8), isValidUTF8(" + field.valueSQL +
				"), CAST(? AS UInt8), CAST(? AS UInt8))", []any{
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
