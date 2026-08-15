package clickhouse

import (
	"context"
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

	readScope          compiledReadScope
	executionAuthority *derivedExecutionAuthority
}

// CompileFieldSuggestions compiles a name-only lookup over the final event
// relation. It never computes or transports field values, counts, or types.
func (c Compiler) CompileFieldSuggestions(
	query *plan.Query,
	spec FieldSuggestionSpec,
) (CompiledFieldSuggestions, error) {
	return c.CompileFieldSuggestionsContext(context.Background(), query, spec)
}

func (c Compiler) CompileFieldSuggestionsContext(
	ctx context.Context,
	query *plan.Query,
	spec FieldSuggestionSpec,
) (CompiledFieldSuggestions, error) {
	if ctx == nil {
		return CompiledFieldSuggestions{}, errors.New(
			"compile ClickHouse field suggestions: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return CompiledFieldSuggestions{}, err
	}
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
	compiled, err := c.compileEventAnalysisContext(ctx, query, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		aliasSequence int,
	) (CompiledQuery, error) {
		policy := eventAnalysisFinalizationPolicyFor(state.chronologicalBarriers)
		contract := fieldSuggestionResultContractFor(policy)
		compiled, finalizeErr := finalizeFieldSuggestions(
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
		return CompiledFieldSuggestions{}, err
	}
	result := CompiledFieldSuggestions{
		SQL:       compiled.SQL,
		Args:      compiled.Args,
		Spec:      spec,
		readScope: compiled.readScope,
	}
	result.executionAuthority, err = sealCompiledFieldSuggestionsExecutionContext(
		ctx,
		compiled,
		result,
	)
	if err != nil {
		return CompiledFieldSuggestions{}, err
	}
	return result, nil
}

func fieldSuggestionResultContract() eventAnalysisResultContract {
	return eventAnalysisResultContract{
		sourceFanout: eventStatsSummarySourceFanout,
		columns: []string{
			FieldSuggestionRowKindColumn,
			FieldSuggestionNameColumn,
			FieldSuggestionInvalidColumn,
		},
		order: quoteIdentifier(FieldSuggestionRowKindColumn) + " ASC, lower(" +
			quoteIdentifier(FieldSuggestionNameColumn) + ") ASC, " +
			quoteIdentifier(FieldSuggestionNameColumn) + " ASC",
	}
}

func fieldSuggestionResultContractFor(
	policy eventAnalysisFinalizationPolicy,
) eventAnalysisResultContract {
	contract := fieldSuggestionResultContract()
	if !policy.materializeSharedCTEs {
		contract.sourceFanout = eventStatsOrdinarySourceFanout
	}
	return contract
}

func fieldSuggestionValidationDummyProjection() []string {
	q := quoteIdentifier
	return []string{
		"toUInt8(0) AS " + q(FieldSuggestionRowKindColumn),
		"CAST('' AS String) AS " + q(FieldSuggestionNameColumn),
		"toUInt8(0) AS " + q(FieldSuggestionInvalidColumn),
	}
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
	fieldSuggestionSafeNames       = "__os_field_suggestion_safe_names"
	fieldSuggestionSafeTypes       = "__os_field_suggestion_safe_types"
	fieldSuggestionSidecarsCTE     = "__os_field_suggestion_sidecars"
	fieldSuggestionRowsCTE         = "__os_field_suggestion_rows"
	fieldSuggestionObservationsCTE = "__os_field_suggestion_observations"
	fieldSuggestionGroupsCTE       = "__os_field_suggestion_groups"
	fieldSuggestionControlledCTE   = "__os_field_suggestion_controlled"
	fieldSuggestionDynamicMetadata = "__os_field_suggestion_dynamic_metadata"
	fieldSuggestionRowInvalid      = "__os_field_suggestion_row_invalid"
	fieldSuggestionGlobalInvalid   = "__os_field_suggestion_global_invalid"
	fieldSuggestionObservation     = "__os_field_suggestion_observation"
	fieldSuggestionObservedKind    = "__os_field_suggestion_observed_kind"
	fieldSuggestionObservedName    = "__os_field_suggestion_observed_name"
	fieldSuggestionObservedInvalid = "__os_field_suggestion_observed_invalid"
	fieldSuggestionGroupKind       = "__os_field_suggestion_group_kind"
	fieldSuggestionCandidateKind   = "__os_field_suggestion_candidate_kind"

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

type fieldSuggestionSidecar struct {
	root               string
	relativeNamesSQL   string
	relativeTypesSQL   string
	metadataVersionSQL string
}

func retainedFieldSuggestionSidecars(
	state compileState,
) ([]fieldSuggestionSidecar, error) {
	names := make([]string, 0, len(state.visible))
	for name := range state.visible {
		names = append(names, name)
	}
	sort.Strings(names)

	sidecars := make([]fieldSuggestionSidecar, 0, len(names))
	for _, name := range names {
		field := state.visible[name]
		if err := validateKnowledgeFieldSidecars(
			field.relativeFieldNamesSQL,
			field.relativeFieldTypesSQL,
			field.fieldMetadataVersionSQL,
		); err != nil {
			return nil, fmt.Errorf(
				"compile ClickHouse field suggestions sidecars for %q: %w",
				name,
				err,
			)
		}
		if field.relativeFieldNamesSQL == "" {
			continue
		}
		sidecars = append(sidecars, fieldSuggestionSidecar{
			root:               name,
			relativeNamesSQL:   field.relativeFieldNamesSQL,
			relativeTypesSQL:   field.relativeFieldTypesSQL,
			metadataVersionSQL: field.fieldMetadataVersionSQL,
		})
	}
	return sidecars, nil
}

func finalizeFieldSuggestions(
	relation compiledRelation,
	state compileState,
	args []any,
	spec FieldSuggestionSpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
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
	if !policy.materializeSharedCTEs {
		sidecars, sidecarErr := retainedFieldSuggestionSidecars(state)
		if sidecarErr != nil {
			return CompiledQuery{}, sidecarErr
		}
		return finalizePrerequisiteFieldSuggestions(
			relation,
			args,
			spec,
			ownerRange,
			policy,
			knownNames,
			shadows,
			blockedPrefixes,
			reservedRoots,
			state.allowDynamic,
			sidecars,
		)
	}

	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 6_144 + len(knownNames)*16)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	writeCTEOpening(&sql, policy.materializeSharedCTEs)
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	// Validate the complete aligned metadata arrays independently of the
	// requested prefix. A poisoned source row invalidates the response even
	// when none of its names would otherwise become a candidate.
	args = writeFieldSuggestionMetadataCTE(&sql, args, reservedRoots)

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
	if policy.includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldSuggestionResultContract().order)
	}

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

// writeFieldSuggestionMetadataCTE emits the __os_field_suggestion_metadata
// validation CTE and appends its bind values. Every aligned metadata row is
// validated before any prefix/name filtering: a poisoned nonmatching field must
// invalidate the whole response atomically.
func writeFieldSuggestionMetadataCTE(sql *strings.Builder, args []any, reservedRoots []string) []any {
	q := quoteIdentifier
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
	return args
}

func finalizePrerequisiteFieldSuggestions(
	relation compiledRelation,
	args []any,
	spec FieldSuggestionSpec,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
	knownNames,
	shadows,
	blockedPrefixes,
	reservedRoots []string,
	allowDynamic bool,
	sidecars []fieldSuggestionSidecar,
) (CompiledQuery, error) {
	q := quoteIdentifier
	var sql strings.Builder
	args = writePrerequisiteFieldSuggestionPrelude(&sql, args, relation, knownNames, reservedRoots, sidecars)
	sql.WriteString(", ")

	writePrerequisiteFieldSuggestionObservations(&sql)
	args = append(args, spec.Prefix, shadows, blockedPrefixes, allowDynamic)
	sql.WriteString(", ")

	writePrerequisiteFieldSuggestionGroups(&sql)
	sql.WriteString(", ")

	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionGroupKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionGroupsCTE))
	sql.WriteString(" UNION ALL SELECT toUInt8(1), arrayJoin(CAST(? AS Array(String))), toUInt8(0)), ")
	args = append(args, knownNames)

	sql.WriteString(q(fieldSuggestionControlledCTE))
	sql.WriteString(" AS (SELECT *, toUInt8(max(if(")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(", toUInt8(0))) OVER ()) AS ")
	sql.WriteString(q(fieldSuggestionGlobalInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString("), ")

	writePrerequisiteFieldSuggestionLimit(&sql)
	args = append(args,
		fieldSuggestionInvalidEditorNamePattern,
		uint64(spec.MaximumFields)+2,
	)

	sql.WriteString(" SELECT ")
	for index, name := range fieldSuggestionResultContract().columns {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(q(name))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionLimitedCTE))
	if policy.includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(fieldSuggestionResultContract().order)
	}
	sql.WriteString(materializedCTESettingsSQL)

	sidecarsDepth := relationalNodeDepth(relation.depth)
	rowsDepth := relationalNodeDepth(sidecarsDepth)
	observationRowsDepth := relationalNodeDepth(rowsDepth)
	syntheticObservationDepth := relationalNodeDepth()
	observationsDepth := relationalNodeDepth(observationRowsDepth, syntheticObservationDepth)
	groupsDepth := relationalNodeDepth(observationsDepth)
	knownDepth := relationalNodeDepth()
	candidatesDepth := relationalNodeDepth(groupsDepth, knownDepth)
	controlledDepth := relationalNodeDepth(candidatesDepth)
	limitedDepth := relationalNodeDepth(controlledDepth)
	resultDepth := relationalNodeDepth(limitedDepth)

	compiled := CompiledQuery{
		SQL:                       sql.String(),
		Args:                      args,
		validationDummyProjection: fieldSuggestionValidationDummyProjection(),
	}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func fieldSuggestionSidecarRoots(sidecars []fieldSuggestionSidecar) []string {
	roots := make([]string, len(sidecars))
	for index, sidecar := range sidecars {
		roots[index] = sidecar.root
	}
	return roots
}

func fieldSuggestionSidecarNamesAlias(index int) string {
	return fmt.Sprintf("__os_field_suggestion_sidecar_names_%d", index)
}

func fieldSuggestionSidecarTypesAlias(index int) string {
	return fmt.Sprintf("__os_field_suggestion_sidecar_types_%d", index)
}

func fieldSuggestionSidecarVersionAlias(index int) string {
	return fmt.Sprintf("__os_field_suggestion_sidecar_version_%d", index)
}

// writePrerequisiteFieldSuggestionPrelude emits the shared prerequisite
// preamble (source CTE, sidecar projection, row validation) and appends its
// bind values. Callers append their own observation stage afterwards.
func writePrerequisiteFieldSuggestionPrelude(
	sql *strings.Builder,
	args []any,
	relation compiledRelation,
	knownNames, reservedRoots []string,
	sidecars []fieldSuggestionSidecar,
) []any {
	q := quoteIdentifier
	sql.Grow(len(relation.sql) + 16_384 + len(knownNames)*16)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString(" AS (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	writePrerequisiteFieldSuggestionSidecars(sql, sidecars)
	sql.WriteString(", ")
	writePrerequisiteFieldSuggestionRows(sql, sidecars)
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
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
		uint64(eventfields.MaximumNormalizedFieldNameBytes+1),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		fieldSuggestionSidecarRoots(sidecars),
	)
	return args
}

func writePrerequisiteFieldSuggestionSidecars(
	sql *strings.Builder,
	sidecars []fieldSuggestionSidecar,
) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSuggestionSidecarsCTE))
	sql.WriteString(" AS (SELECT *")
	for index, sidecar := range sidecars {
		sql.WriteString(", ifNull(")
		sql.WriteString(sidecar.relativeNamesSQL)
		sql.WriteString(", CAST([], 'Array(String)')) AS ")
		sql.WriteString(q(fieldSuggestionSidecarNamesAlias(index)))
		sql.WriteString(", ifNull(")
		sql.WriteString(sidecar.relativeTypesSQL)
		sql.WriteString(", CAST([], 'Array(UInt8)')) AS ")
		sql.WriteString(q(fieldSuggestionSidecarTypesAlias(index)))
		sql.WriteString(", toUInt8(ifNull(")
		sql.WriteString(sidecar.metadataVersionSQL)
		sql.WriteString(", toUInt8(0))) AS ")
		sql.WriteString(q(fieldSuggestionSidecarVersionAlias(index)))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString(")")
}

func writeFieldSuggestionSidecarNamesArray(
	sql *strings.Builder,
	sidecars []fieldSuggestionSidecar,
) {
	if len(sidecars) == 0 {
		sql.WriteString("CAST([], 'Array(Array(String))')")
		return
	}
	sql.WriteByte('[')
	for index := range sidecars {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(fieldSuggestionSidecarNamesAlias(index)))
	}
	sql.WriteByte(']')
}

func writeFieldSuggestionSidecarTypesArray(
	sql *strings.Builder,
	sidecars []fieldSuggestionSidecar,
) {
	if len(sidecars) == 0 {
		sql.WriteString("CAST([], 'Array(Array(UInt8))')")
		return
	}
	sql.WriteByte('[')
	for index := range sidecars {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(fieldSuggestionSidecarTypesAlias(index)))
	}
	sql.WriteByte(']')
}

func writeFieldSuggestionSidecarVersionsArray(
	sql *strings.Builder,
	sidecars []fieldSuggestionSidecar,
) {
	if len(sidecars) == 0 {
		sql.WriteString("CAST([], 'Array(UInt8)')")
		return
	}
	sql.WriteByte('[')
	for index := range sidecars {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteIdentifier(fieldSuggestionSidecarVersionAlias(index)))
	}
	sql.WriteByte(']')
}

func writePrerequisiteFieldSuggestionRows(
	sql *strings.Builder,
	sidecars []fieldSuggestionSidecar,
) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSuggestionRowsCTE))
	sql.WriteString(" AS (WITH ifNull(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(", CAST([], 'Array(String)')) AS ")
	sql.WriteString(q(fieldSuggestionSafeNames))
	sql.WriteString(", ifNull(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(", CAST([], 'Array(UInt8)')) AS ")
	sql.WriteString(q(fieldSuggestionSafeTypes))
	sql.WriteString(", arraySlice(")
	sql.WriteString(q(fieldSuggestionSafeNames))
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
	sql.WriteString(q(fieldSuggestionSafeTypes))
	sql.WriteString(", 1, CAST(? AS UInt64)) AS ")
	sql.WriteString(q(fieldSuggestionBoundedTypes))
	sql.WriteString(" SELECT toUInt8(ifNull(isNull(")
	sql.WriteString(q(internalFieldMetadataVersionColumn))
	sql.WriteString(") OR isNull(")
	sql.WriteString(q(internalFieldNamesColumn))
	sql.WriteString(") OR isNull(")
	sql.WriteString(q(internalFieldTypesColumn))
	sql.WriteString(") OR ")
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
	sql.WriteString(") OR arrayExists((sidecar_names, sidecar_types, sidecar_version) -> ((notEmpty(sidecar_names) OR notEmpty(sidecar_types)) AND sidecar_version != ?) OR length(sidecar_names) > ? OR length(sidecar_types) > ? OR length(sidecar_names) != length(sidecar_types) OR arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, sidecar_names) OR sidecar_names != arraySort(arrayDistinct(sidecar_names)) OR arrayExists(stored_type -> stored_type < ? OR stored_type > ?, sidecar_types), ")
	writeFieldSuggestionSidecarNamesArray(sql, sidecars)
	sql.WriteString(", ")
	writeFieldSuggestionSidecarTypesArray(sql, sidecars)
	sql.WriteString(", ")
	writeFieldSuggestionSidecarVersionsArray(sql, sidecars)
	sql.WriteString("), 1)) AS ")
	sql.WriteString(q(fieldSuggestionRowInvalid))
	sql.WriteString(", arrayConcat(arrayZip(arrayMap(field_name -> left(field_name, CAST(? AS UInt64)), arraySlice(")
	sql.WriteString(q(fieldSuggestionSafeNames))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(fieldSuggestionSafeNames))
	sql.WriteString("), length(")
	sql.WriteString(q(fieldSuggestionSafeTypes))
	sql.WriteString("), CAST(? AS UInt64)))), arraySlice(")
	sql.WriteString(q(fieldSuggestionSafeTypes))
	sql.WriteString(", 1, least(length(")
	sql.WriteString(q(fieldSuggestionSafeNames))
	sql.WriteString("), length(")
	sql.WriteString(q(fieldSuggestionSafeTypes))
	sql.WriteString("), CAST(? AS UInt64)))), arrayFlatten(arrayMap((sidecar_root, sidecar_names, sidecar_types) -> arrayMap(field_metadata -> tuple(concat(sidecar_root, '.', tupleElement(field_metadata, 1)), toUInt8(tupleElement(field_metadata, 2))), arrayZip(arraySlice(sidecar_names, 1, least(length(sidecar_names), length(sidecar_types))), arraySlice(sidecar_types, 1, least(length(sidecar_names), length(sidecar_types))))), CAST(? AS Array(String)), ")
	writeFieldSuggestionSidecarNamesArray(sql, sidecars)
	sql.WriteString(", ")
	writeFieldSuggestionSidecarTypesArray(sql, sidecars)
	sql.WriteString("))) AS ")
	sql.WriteString(q(fieldSuggestionDynamicMetadata))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionSidecarsCTE))
	sql.WriteString(")")
}

func writePrerequisiteFieldSuggestionObservations(sql *strings.Builder) {
	q := quoteIdentifier
	fields := []string{
		fieldSuggestionObservedKind,
		fieldSuggestionObservedName,
		fieldSuggestionObservedInvalid,
	}
	sql.WriteString(q(fieldSuggestionObservationsCTE))
	sql.WriteString(" AS (SELECT ")
	for index, name := range fields {
		if index > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("tupleElement(")
		sql.WriteString(q(fieldSuggestionObservation))
		sql.WriteString(", ")
		fmt.Fprint(sql, index+1)
		sql.WriteString(") AS ")
		sql.WriteString(q(name))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionRowsCTE))
	sql.WriteString(" ARRAY JOIN arrayConcat([tuple(toUInt8(0), CAST('' AS String), ")
	sql.WriteString(q(fieldSuggestionRowInvalid))
	sql.WriteString(")], arrayMap(field_metadata -> tuple(toUInt8(1), tupleElement(field_metadata, 1), toUInt8(0)), arrayFilter(field_metadata -> ")
	sql.WriteString(q(fieldSuggestionRowInvalid))
	sql.WriteString(" = 0 AND startsWith(tupleElement(field_metadata, 1), CAST(? AS String)) AND NOT has(CAST(? AS Array(String)), tupleElement(field_metadata, 1)) AND NOT arrayExists(prefix -> tupleElement(field_metadata, 1) = prefix OR startsWith(tupleElement(field_metadata, 1), concat(prefix, '.')), CAST(? AS Array(String))) AND CAST(? AS Bool), ")
	sql.WriteString(q(fieldSuggestionDynamicMetadata))
	sql.WriteString("))) AS ")
	sql.WriteString(q(fieldSuggestionObservation))
	sql.WriteString(" UNION ALL SELECT toUInt8(0), CAST('' AS String), toUInt8(0))")
}

func writePrerequisiteFieldSuggestionGroups(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSuggestionGroupsCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionObservedKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSuggestionGroupKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionObservedName))
	sql.WriteString(" AS ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldSuggestionObservedInvalid))
	sql.WriteString(")) AS ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionObservationsCTE))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(fieldSuggestionObservedKind))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionObservedName))
	sql.WriteString(")")
}

func writePrerequisiteFieldSuggestionLimit(sql *strings.Builder) {
	q := quoteIdentifier
	sql.WriteString(q(fieldSuggestionLimitedCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSuggestionRowKindColumn))
	sql.WriteString(", ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" AS ")
	sql.WriteString(q(FieldSuggestionNameColumn))
	sql.WriteString(", if(")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" = toUInt8(0), ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(", toUInt8(0)) AS ")
	sql.WriteString(q(FieldSuggestionInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionControlledCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" = toUInt8(0) OR (")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" = toUInt8(1) AND ")
	sql.WriteString(q(fieldSuggestionGlobalInvalid))
	sql.WriteString(" = 0 AND NOT startsWith(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", '+') AND NOT startsWith(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", '-') AND NOT startsWith(lower(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString("), '__os_') AND NOT match(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(", CAST(? AS String))) ORDER BY ")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" ASC, lower(")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(") ASC, ")
	sql.WriteString(q(fieldSuggestionCandidateName))
	sql.WriteString(" ASC LIMIT ?)")
}
