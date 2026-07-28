package clickhouse

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// MaximumFieldSuggestions is the hard cross-layer result bound for one
	// field-name completion request. The compiler requests one additional
	// ranked name so the executor can report deterministic truncation.
	MaximumFieldSuggestions uint32 = 100

	FieldSuggestionRowKindColumn = "__os_field_suggestion_row_kind"
	FieldSuggestionNameColumn    = "__os_field_suggestion_name"
	FieldSuggestionInvalidColumn = "__os_field_suggestion_invalid"
)

// FieldSuggestionSpec describes one case-sensitive field-name prefix lookup.
// Prefix is the normalized SPL spelling and MaximumFields bounds the names
// returned to the caller, excluding the one-row overflow sentinel.
type FieldSuggestionSpec struct {
	Prefix        string
	MaximumFields uint32
}

// CompiledFieldSuggestions is one immutable, parameterized field-name lookup.
type CompiledFieldSuggestions struct {
	SQL  string
	Args []any
	Spec FieldSuggestionSpec
}

// CompileFieldSuggestions compiles a name-only lookup over the final event
// relation. It never computes or transports field values, counts, or types.
func (c Compiler) CompileFieldSuggestions(
	query *plan.Query,
	spec FieldSuggestionSpec,
) (CompiledFieldSuggestions, error) {
	if spec.MaximumFields == 0 || spec.MaximumFields > MaximumFieldSuggestions {
		return CompiledFieldSuggestions{}, fmt.Errorf(
			"compile ClickHouse field suggestions: MaximumFields must be between 1 and %d",
			MaximumFieldSuggestions,
		)
	}
	if len(spec.Prefix) > eventfields.MaximumNormalizedFieldNameBytes ||
		!utf8.ValidString(spec.Prefix) ||
		fieldSuggestionContainsControl(spec.Prefix) {
		return CompiledFieldSuggestions{}, errors.New(
			"compile ClickHouse field suggestions: Prefix has invalid encoding, length, or content",
		)
	}
	compiled, err := c.compileEventAnalysis(query, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		_ int,
	) (CompiledQuery, error) {
		return finalizeFieldSuggestions(relation, state, args, spec, scan.Range)
	})
	if err != nil {
		return CompiledFieldSuggestions{}, err
	}
	return CompiledFieldSuggestions{
		SQL:  compiled.SQL,
		Args: compiled.Args,
		Spec: spec,
	}, nil
}

const (
	fieldSuggestionSourceCTE       = "__os_field_suggestion_source"
	fieldSuggestionMetadataCTE     = "__os_field_suggestion_metadata"
	fieldSuggestionDynamicLeaves   = "__os_field_suggestion_dynamic_leaves"
	fieldSuggestionDynamicCTE      = "__os_field_suggestion_dynamic"
	fieldSuggestionKnownCTE        = "__os_field_suggestion_known"
	fieldSuggestionCandidatesCTE   = "__os_field_suggestion_candidates"
	fieldSuggestionLimitedCTE      = "__os_field_suggestion_limited"
	fieldSuggestionDynamicName     = "__os_field_suggestion_dynamic_name"
	fieldSuggestionCandidateName   = "__os_field_suggestion_candidate_name"
	fieldSuggestionMetadataInvalid = "__os_field_suggestion_metadata_invalid"
	fieldSuggestionBoundedNames    = "__os_field_suggestion_bounded_names"
	fieldSuggestionCheckedNames    = "__os_field_suggestion_checked_names"
	fieldSuggestionCheckedPaths    = "__os_field_suggestion_checked_paths"
	fieldSuggestionBoundedTypes    = "__os_field_suggestion_bounded_types"

	// The pattern is the byte-preserving grammar accepted by
	// eventfields.ParseNormalizedDynamicPath: nonempty segments, only canonical
	// dot/backslash escapes, no Unicode control characters, and at most sixteen
	// path segments. Decoded per-segment byte bounds are checked separately.
	fieldSuggestionNormalizedNamePattern    = `^(?:\\[\\.]|[^.\\\p{Cc}])+(?:\.(?:\\[\\.]|[^.\\\p{Cc}])+){0,15}$`
	fieldSuggestionNormalizedSegmentPattern = `(?:\\[\\.]|[^.\\\p{Cc}])+`
	// Suggestions must be insertable as unquoted SPL field tokens. Filtering
	// happens before LIMIT so malformed metadata cannot consume the bounded
	// candidate window and hide later usable names.
	fieldSuggestionInvalidEditorNamePattern = `[\p{Z}\p{C}|(),=!<>"*]`
	fieldSuggestionEscapedBackslash         = `\\`
	fieldSuggestionEscapedDot               = `\.`
)

func fieldSuggestionContainsControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func finalizeFieldSuggestions(
	relation compiledRelation,
	state compileState,
	args []any,
	spec FieldSuggestionSpec,
	ownerRange spl.Range,
) (CompiledQuery, error) {
	if !state.eventRows {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse field suggestions: final relation is not an event relation",
		)
	}

	knownNames := make([]string, 0, len(state.visible))
	for name := range state.visible {
		resolved, err := plan.ResolveField(name, spl.Range{})
		if err != nil || resolved.Name != name {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse field suggestions: visible field name is not a valid query field",
			)
		}
		if strings.HasPrefix(name, spec.Prefix) {
			knownNames = append(knownNames, name)
		}
	}
	sort.Strings(knownNames)

	shadowSet := make(map[string]struct{}, len(state.visible)+len(state.blocked))
	for name := range state.visible {
		shadowSet[name] = struct{}{}
	}
	for name := range state.blocked {
		shadowSet[name] = struct{}{}
	}
	shadows := sortedSetValues(shadowSet)
	blockedPrefixes := sortedSetValues(state.blockedPrefixes)
	reservedRoots := eventfields.ReservedDynamicRootNames()
	sort.Strings(reservedRoots)

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 6_144 + len(knownNames)*16)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString(" AS MATERIALIZED (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	// Validate the complete aligned metadata arrays independently of the
	// requested prefix. A poisoned source row invalidates the response even
	// when none of its names would otherwise become a candidate.
	sql.WriteString(q(fieldSuggestionMetadataCTE))
	sql.WriteString(" AS (WITH arraySlice(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(", 1, CAST(? AS UInt64)) AS ")
	sql.WriteString(q(fieldSuggestionBoundedNames))
	sql.WriteString(", arrayMap(field_name -> left(field_name, CAST(? AS UInt64)), ")
	sql.WriteString(q(fieldSuggestionBoundedNames))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(", arrayMap(field_name -> extractAll(field_name, CAST(? AS String)), ")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(") AS ")
	sql.WriteString(q(fieldSuggestionCheckedPaths))
	sql.WriteString(", arraySlice(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(", 1, CAST(? AS UInt64)) AS ")
	sql.WriteString(q(fieldSuggestionBoundedTypes))
	sql.WriteString(" SELECT toUInt8(countIf(")
	sql.WriteString(q(internalFieldMetadataVersionColumn))
	sql.WriteString(" != ? OR length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") > ? OR length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(") > ? OR length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") != length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(") OR arraySum(arrayMap(field_name -> length(field_name), ")
	sql.WriteString(q(fieldSuggestionBoundedNames))
	sql.WriteString(")) > ? OR arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, ")
	sql.WriteString(q(fieldSuggestionBoundedNames))
	sql.WriteString(") OR arrayExists(field_name -> NOT match(field_name, CAST(? AS String)) OR arrayExists(normalized_segment -> length(normalized_segment) > ?, splitByChar('.', replaceAll(replaceAll(field_name, CAST(? AS String), 'x'), CAST(? AS String), 'x'))), ")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(") OR arrayExists(field_name -> startsWith(lower(field_name), '__os_') OR arrayExists(reserved_root -> lower(field_name) = reserved_root OR startsWith(lower(field_name), concat(reserved_root, '.')), CAST(? AS Array(String))), ")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(") OR arrayExists(path -> arrayExists(depth -> depth < length(path) AND indexOfAssumeSorted(")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(", arrayStringConcat(arraySlice(path, 1, depth), '.')) != 0, arrayEnumerate(path)), ")
	sql.WriteString(q(fieldSuggestionCheckedPaths))
	sql.WriteString(") OR ")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(" != arraySort(arrayDistinct(")
	sql.WriteString(q(fieldSuggestionCheckedNames))
	sql.WriteString(")) OR arrayExists(stored_type -> stored_type < ? OR stored_type > ?, ")
	sql.WriteString(q(fieldSuggestionBoundedTypes))
	sql.WriteString(")) > 0) AS ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString("), ")
	args = append(args,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes+1),
		fieldSuggestionNormalizedSegmentPattern,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldNamesBytes),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		fieldSuggestionNormalizedNamePattern,
		uint64(eventfields.MaximumDynamicPathSegmentBytes),
		fieldSuggestionEscapedBackslash,
		fieldSuggestionEscapedDot,
		reservedRoots,
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	)

	// Prefix filtering happens inside the ARRAY JOIN leaf relation, before its
	// GROUP BY. This is the only runtime-cardinality branch and keeps common
	// editor prefixes materially cheaper than a complete field catalog.
	sql.WriteString(q(fieldSuggestionDynamicCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" FROM (SELECT left(tupleElement(field_metadata, 1), CAST(? AS UInt64)) AS ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString(" ARRAY JOIN arrayZip(arraySlice(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString("), CAST(? AS UInt64))), arraySlice(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString("), length(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString("), CAST(? AS UInt64)))) AS field_metadata WHERE startsWith(")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(", CAST(? AS String))")
	sql.WriteString(" AND NOT has(CAST(? AS Array(String)), ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(")")
	sql.WriteString(" AND NOT arrayExists(prefix -> ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(" = prefix OR startsWith(")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(", concat(prefix, '.')), CAST(? AS Array(String)))")
	sql.WriteString(" AND CAST(? AS Bool) AND (SELECT ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionMetadataCTE))
	sql.WriteString(") = 0) AS ")
	sql.WriteString(q(fieldSuggestionDynamicLeaves))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString("), ")
	args = append(
		args,
		uint64(eventfields.MaximumNormalizedFieldNameBytes+1),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		spec.Prefix,
		shadows,
		blockedPrefixes,
		state.allowDynamic,
	)

	sql.WriteString(q(fieldSuggestionKnownCTE))
	sql.WriteString(" AS (SELECT arrayJoin(CAST(? AS Array(String))) AS ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString("), ")
	args = append(args, knownNames)

	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionDynamicCTE))
	sql.WriteString(" UNION ALL SELECT ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionKnownCTE))
	sql.WriteString("), ")

	sql.WriteString(q(fieldSuggestionLimitedCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString(" WHERE NOT startsWith(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", '+') AND NOT startsWith(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", '-') AND NOT startsWith(lower(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString("), '__os_') AND NOT match(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", CAST(? AS String)) ORDER BY lower(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(") ASC, ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" ASC LIMIT ?)")
	args = append(
		args,
		fieldSuggestionInvalidEditorNamePattern,
		uint64(spec.MaximumFields)+1,
	)

	sql.WriteString(" SELECT * FROM (SELECT toUInt8(0) AS ")
	sql.WriteString(q(FieldSuggestionRowKindColumn))
	sql.WriteString(", CAST('' AS String) AS ")
	sql.WriteString(q(FieldSuggestionNameColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSuggestionInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionMetadataCTE))
	sql.WriteString(" UNION ALL SELECT toUInt8(1) AS ")
	sql.WriteString(q(FieldSuggestionRowKindColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSuggestionNameColumn))
	sql.WriteString(", toUInt8(0) AS ")
	sql.WriteString(q(FieldSuggestionInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionLimitedCTE))
	sql.WriteString(") AS ")
	sql.WriteString(q("__os_field_suggestion_output"))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(FieldSuggestionRowKindColumn))
	sql.WriteString(" ASC, ")
	sql.WriteString("lower(")
	sql.WriteString(q(FieldSuggestionNameColumn))
	sql.WriteString(") ASC, ")
	sql.WriteString(q(FieldSuggestionNameColumn))
	sql.WriteString(" ASC")

	sourceDepth := relation.depth
	metadataDepth := relationalNodeDepth(sourceDepth)
	metadataScalarDepth := relationalNodeDepth(metadataDepth)
	dynamicLeavesDepth := relationalNodeDepth(sourceDepth, metadataScalarDepth)
	dynamicDepth := relationalNodeDepth(dynamicLeavesDepth)
	knownDepth := relationalNodeDepth()
	candidatesDepth := relationalNodeDepth(dynamicDepth, knownDepth)
	limitedDepth := relationalNodeDepth(candidatesDepth)
	headerDepth := relationalNodeDepth(metadataDepth)
	nameRowsDepth := relationalNodeDepth(limitedDepth)
	outputDepth := relationalNodeDepth(headerDepth, nameRowsDepth)
	resultDepth := relationalNodeDepth(outputDepth)

	compiled := CompiledQuery{SQL: sql.String(), Args: args}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}
