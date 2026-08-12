package clickhouse

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	StatsWildcardInventoryOrdinalColumn = "__os_stats_wildcard_ordinal"
	StatsWildcardInventoryFieldColumn   = "__os_stats_wildcard_field"
	StatsWildcardInventoryInvalidColumn = "__os_stats_wildcard_invalid"
	statsWildcardInventorySealDomain    = "open-splunk-stats-wildcard-inventory-v1"
)

type statsWildcardInventorySeal [sha256.Size]byte

// CompiledStatsWildcardInventory is a distinct compiler-sealed, name-only
// executable. It cannot be substituted for an ordinary search query.
type CompiledStatsWildcardInventory struct {
	SQL  string
	Args []any

	request            plan.StatsWildcardRequest
	readScope          compiledReadScope
	sourceDigest       [sha256.Size]byte
	executionAuthority *statsWildcardInventorySeal
}

// CompileStatsWildcardInventory lowers a parser-owned event-relation prefix
// to a bounded inventory of exact names for every requested aggregate ordinal.
func (c Compiler) CompileStatsWildcardInventory(
	prefix *plan.Query,
	request plan.StatsWildcardRequest,
) (CompiledStatsWildcardInventory, error) {
	if prefix == nil || !request.ValidForPrefix(prefix) {
		return CompiledStatsWildcardInventory{}, errors.New(
			"compile ClickHouse stats wildcard inventory: request does not belong to prefix",
		)
	}
	canonicalPrefix, ok := request.CanonicalPrefixFor(prefix)
	if !ok || canonicalPrefix == nil {
		return CompiledStatsWildcardInventory{}, errors.New(
			"compile ClickHouse stats wildcard inventory: canonical prefix authority is invalid",
		)
	}
	patterns := request.Patterns()
	maximumPairs := request.MaximumPairs()
	if len(patterns) == 0 || maximumPairs < 2 || maximumPairs > spl.MaximumStatsMeasures+1 {
		return CompiledStatsWildcardInventory{}, errors.New(
			"compile ClickHouse stats wildcard inventory: request is invalid",
		)
	}

	compiled, err := c.compileEventAnalysis(canonicalPrefix, func(
		relation compiledRelation,
		state compileState,
		args []any,
		scan *plan.Scan,
		aliasSequence int,
	) (CompiledQuery, error) {
		result, finalizeErr := finalizeStatsWildcardInventory(
			relation, state, args, request, scan.Range,
			eventAnalysisFinalizationPolicyFor(state.chronologicalBarriers),
		)
		if finalizeErr != nil {
			return CompiledQuery{}, finalizeErr
		}
		return wrapEventAnalysisValidation(
			result,
			state,
			statsWildcardInventoryResultContract(),
			aliasSequence,
		)
	})
	if err != nil {
		return CompiledStatsWildcardInventory{}, err
	}
	sourceDigest, ok := compiled.ExecutionAuthorityDigest()
	if !ok {
		return CompiledStatsWildcardInventory{}, errors.New(
			"compile ClickHouse stats wildcard inventory: source authority is invalid",
		)
	}
	result := CompiledStatsWildcardInventory{
		SQL:          compiled.SQL,
		Args:         compiled.Args,
		request:      request.Clone(),
		readScope:    compiled.readScope,
		sourceDigest: sourceDigest,
	}
	seal, ok := statsWildcardInventoryDigest(result)
	if !ok {
		return CompiledStatsWildcardInventory{}, errors.New(
			"compile ClickHouse stats wildcard inventory: executable contract is invalid",
		)
	}
	result.executionAuthority = &seal
	return result, nil
}

func statsWildcardInventoryResultContract() eventAnalysisResultContract {
	return eventAnalysisResultContract{
		sourceFanout: eventStatsOrdinarySourceFanout,
		columns: []string{
			StatsWildcardInventoryOrdinalColumn,
			StatsWildcardInventoryFieldColumn,
			StatsWildcardInventoryInvalidColumn,
		},
		order: quoteIdentifier(StatsWildcardInventoryInvalidColumn) + " DESC, " +
			quoteIdentifier(StatsWildcardInventoryOrdinalColumn) + " ASC, " +
			quoteIdentifier(StatsWildcardInventoryFieldColumn) + " ASC",
	}
}

func statsWildcardInventoryDummyProjection() []string {
	return []string{
		"toUInt8(0) AS " + quoteIdentifier(StatsWildcardInventoryOrdinalColumn),
		"CAST('' AS String) AS " + quoteIdentifier(StatsWildcardInventoryFieldColumn),
		"toUInt8(1) AS " + quoteIdentifier(StatsWildcardInventoryInvalidColumn),
	}
}

func (compiled CompiledStatsWildcardInventory) Request() plan.StatsWildcardRequest {
	if !compiled.hasValidExecutionSeal() {
		return plan.StatsWildcardRequest{}
	}
	return compiled.request.Clone()
}

func (compiled CompiledStatsWildcardInventory) ReadScope() (string, []string, bool) {
	if !compiled.hasValidExecutionSeal() {
		return "", nil, false
	}
	return compiled.readScope.openForSQL(compiled.SQL, compiled.Args)
}

func (compiled CompiledStatsWildcardInventory) SameReadScope(other CompiledQuery) bool {
	leftTenant, leftIndexes, leftOK := compiled.ReadScope()
	rightTenant, rightIndexes, rightOK := other.ReadScope()
	return leftOK && rightOK && leftTenant == rightTenant && slices.Equal(leftIndexes, rightIndexes)
}

func (compiled CompiledStatsWildcardInventory) hasValidExecutionSeal() bool {
	if compiled.executionAuthority == nil {
		return false
	}
	expected, ok := statsWildcardInventoryDigest(compiled)
	return ok && subtle.ConstantTimeCompare(expected[:], compiled.executionAuthority[:]) == 1
}

func (compiled CompiledStatsWildcardInventory) HasValidExecutionSeal() bool {
	return compiled.hasValidExecutionSeal()
}

func (compiled CompiledStatsWildcardInventory) ExecutionAuthorityDigest() ([sha256.Size]byte, bool) {
	if !compiled.hasValidExecutionSeal() {
		return [sha256.Size]byte{}, false
	}
	return [sha256.Size]byte(*compiled.executionAuthority), true
}

func (compiled CompiledStatsWildcardInventory) EqualForExecution(other CompiledStatsWildcardInventory) bool {
	if !compiled.hasValidExecutionSeal() || !other.hasValidExecutionSeal() {
		return false
	}
	return subtle.ConstantTimeCompare(compiled.executionAuthority[:], other.executionAuthority[:]) == 1
}

func (compiled CompiledStatsWildcardInventory) CloneForExecution() (CompiledStatsWildcardInventory, bool) {
	if !compiled.hasValidExecutionSeal() {
		return CompiledStatsWildcardInventory{}, false
	}
	cloned := compiled
	cloned.SQL = strings.Clone(compiled.SQL)
	cloned.request = compiled.request.Clone()
	cloned.Args = make([]any, len(compiled.Args))
	if compiled.Args == nil {
		cloned.Args = nil
	} else {
		for index, argument := range compiled.Args {
			value, ok := cloneCompiledArgument(argument)
			if !ok {
				return CompiledStatsWildcardInventory{}, false
			}
			cloned.Args[index] = value
		}
	}
	cloned.readScope = compiled.readScope
	cloned.readScope.tenantID = strings.Clone(compiled.readScope.tenantID)
	cloned.readScope.indexNames = cloneStrings(compiled.readScope.indexNames)
	cloned.readScope.argumentPositions = slices.Clone(compiled.readScope.argumentPositions)
	seal := *compiled.executionAuthority
	cloned.executionAuthority = &seal
	if !cloned.hasValidExecutionSeal() {
		return CompiledStatsWildcardInventory{}, false
	}
	return cloned, true
}

func (compiled CompiledStatsWildcardInventory) RetainedBytes() (uint64, bool) {
	if !compiled.hasValidExecutionSeal() {
		return 0, false
	}
	total := uint64(unsafe.Sizeof(compiled)) + uint64(len(compiled.SQL))
	var ok bool
	total, ok = retainedAdd(
		total,
		uint64(cap(compiled.Args))*uint64(unsafe.Sizeof(any(nil))),
	)
	if !ok {
		return 0, false
	}
	for _, argument := range compiled.Args {
		charge, supported := retainedCompiledArgument(argument)
		if !supported {
			return 0, false
		}
		total, ok = retainedAdd(total, charge)
		if !ok {
			return 0, false
		}
	}
	requestBytes, valid := compiled.request.RetainedBytes()
	if !valid {
		return 0, false
	}
	total, ok = retainedAdd(total, requestBytes)
	if !ok {
		return 0, false
	}
	total, ok = retainedAdd(total, uint64(len(compiled.readScope.tenantID)))
	if !ok {
		return 0, false
	}
	total, ok = retainedStringSlice(total, compiled.readScope.indexNames)
	if !ok {
		return 0, false
	}
	total, ok = retainedAdd(
		total,
		uint64(cap(compiled.readScope.argumentPositions))*uint64(unsafe.Sizeof(int(0))),
	)
	if !ok {
		return 0, false
	}
	return retainedAdd(total, uint64(unsafe.Sizeof(*compiled.executionAuthority)))
}

func statsWildcardInventoryDigest(
	compiled CompiledStatsWildcardInventory,
) (statsWildcardInventorySeal, bool) {
	if strings.TrimSpace(compiled.SQL) == "" || compiled.sourceDigest == ([sha256.Size]byte{}) ||
		compiled.request.IsZero() {
		return statsWildcardInventorySeal{}, false
	}
	if _, _, ok := compiled.readScope.openForSQL(compiled.SQL, compiled.Args); !ok {
		return statsWildcardInventorySeal{}, false
	}
	requestDigest, ok := compiled.request.AuthorityDigest()
	if !ok {
		return statsWildcardInventorySeal{}, false
	}
	digest := sha256.New()
	writeTokenPart(digest, statsWildcardInventorySealDomain)
	_, _ = digest.Write(compiled.sourceDigest[:])
	_, _ = digest.Write(requestDigest[:])
	writeTokenPart(digest, compiled.SQL)
	writeUint64(digest, uint64(len(compiled.Args)))
	for _, argument := range compiled.Args {
		if !writeCompiledArgument(digest, argument, 0) {
			return statsWildcardInventorySeal{}, false
		}
	}
	_, _ = digest.Write(compiled.readScope.seal[:])
	var result statsWildcardInventorySeal
	digest.Sum(result[:0])
	return result, true
}

func finalizeStatsWildcardInventory(
	relation compiledRelation,
	state compileState,
	args []any,
	request plan.StatsWildcardRequest,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
) (CompiledQuery, error) {
	if !state.eventRows || request.IsZero() {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse stats wildcard inventory: final relation or request is invalid",
		)
	}
	patterns := request.Patterns()
	maximumPairs := request.MaximumPairs()
	if len(patterns) == 0 || maximumPairs < 2 || maximumPairs > spl.MaximumStatsMeasures+1 {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse stats wildcard inventory: request is invalid",
		)
	}

	knownNames := make([]string, 0, len(state.visible))
	for name := range state.visible {
		if !validStatsWildcardInventoryFieldName(name) {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse stats wildcard inventory: visible field name is invalid",
			)
		}
		// The raw Dynamic payload is a transport container, not an aggregatable
		// SPL leaf. Exact stats compilation enforces the same exclusion.
		if name == "fields" || !statsWildcardMatchesAny(patterns, name) {
			continue
		}
		knownNames = append(knownNames, name)
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
	ordinals, regularExpressions := statsWildcardInventoryPatterns(patterns)

	if !policy.materializeSharedCTEs {
		sidecars, err := retainedFieldSuggestionSidecars(state)
		if err != nil {
			return CompiledQuery{}, err
		}
		return finalizePrerequisiteStatsWildcardInventory(
			relation, args, request, ownerRange, policy, knownNames, shadows,
			blockedPrefixes, reservedRoots, ordinals, regularExpressions,
			state.allowDynamic, sidecars,
		)
	}
	return finalizeMaterializedStatsWildcardInventory(
		relation, args, request, ownerRange, policy, knownNames, shadows,
		blockedPrefixes, reservedRoots, ordinals, regularExpressions,
		state.allowDynamic,
	)
}

func validStatsWildcardInventoryFieldName(name string) bool {
	if name == "" || name == "fields" ||
		strings.HasPrefix(strings.ToLower(name), "__os_") {
		return false
	}
	if eventfields.IsCanonicalSPLField(name) {
		return true
	}
	path, pathErr := eventfields.ParseNormalizedSearchFieldPath(name)
	if pathErr == nil && len(path) > 0 && eventfields.IsReservedDynamicRoot(path[0]) {
		return false
	}
	if spl.IsExactUnquotedFieldName(name) || spl.IsStatsLiteralFieldReference(name) {
		return true
	}
	resolved, err := plan.ResolveField(name, spl.Range{})
	return err == nil && resolved.Name == name
}

func statsWildcardMatchesAny(patterns []plan.StatsWildcardPattern, name string) bool {
	for _, pattern := range patterns {
		if spl.MatchStatsFieldGlob(pattern.Pattern, name) {
			return true
		}
	}
	return false
}

func statsWildcardInventoryPatterns(
	patterns []plan.StatsWildcardPattern,
) ([]uint8, []string) {
	ordinals := make([]uint8, len(patterns))
	regularExpressions := make([]string, len(patterns))
	for index, pattern := range patterns {
		ordinals[index] = pattern.Ordinal
		parts := strings.Split(pattern.Pattern, "*")
		for partIndex := range parts {
			parts[partIndex] = regexp.QuoteMeta(parts[partIndex])
		}
		regularExpressions[index] = `(?s:\A(?:` + strings.Join(parts, `.*`) + `)\z)`
	}
	return ordinals, regularExpressions
}

const (
	statsWildcardPatternsCTE      = "__os_stats_wildcard_patterns"
	statsWildcardDistinctCTE      = "__os_stats_wildcard_distinct"
	statsWildcardMatchesCTE       = "__os_stats_wildcard_matches"
	statsWildcardLimitedCTE       = "__os_stats_wildcard_limited"
	statsWildcardPatternTuple     = "__os_stats_wildcard_pattern"
	statsWildcardCandidateOrdinal = "__os_stats_wildcard_candidate_ordinal"
	statsWildcardCandidateField   = "__os_stats_wildcard_candidate_field"
	statsWildcardCandidateInvalid = "__os_stats_wildcard_candidate_invalid"
)

func writeStatsWildcardPatternCTE(
	sql *strings.Builder,
	args []any,
	ordinals []uint8,
	regularExpressions []string,
) []any {
	q := quoteIdentifier
	sql.WriteString(q(statsWildcardPatternsCTE))
	sql.WriteString(" AS (SELECT tupleElement(")
	sql.WriteString(q(statsWildcardPatternTuple))
	sql.WriteString(", 1) AS ")
	sql.WriteString(q(statsWildcardCandidateOrdinal))
	sql.WriteString(", tupleElement(")
	sql.WriteString(q(statsWildcardPatternTuple))
	sql.WriteString(", 2) AS __os_stats_wildcard_re2 FROM (SELECT arrayJoin(arrayZip(CAST(? AS Array(UInt8)), CAST(? AS Array(String)))) AS ")
	sql.WriteString(q(statsWildcardPatternTuple))
	sql.WriteString(")), ")
	return append(args, ordinals, regularExpressions)
}

func writeStatsWildcardMatchAndLimit(
	sql *strings.Builder,
	candidateRelation string,
	maximumPairs uint8,
) {
	q := quoteIdentifier
	sql.WriteString(q(statsWildcardMatchesCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(statsWildcardCandidateOrdinal))
	sql.WriteString(", ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(" FROM ")
	sql.WriteString(q(candidateRelation))
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(q(statsWildcardPatternsCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(q(statsWildcardCandidateInvalid))
	sql.WriteString(" = 0 AND match(")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(", __os_stats_wildcard_re2)), ")
	sql.WriteString(q(statsWildcardLimitedCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(statsWildcardCandidateOrdinal))
	sql.WriteString(", ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(" FROM ")
	sql.WriteString(q(statsWildcardMatchesCTE))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(statsWildcardCandidateOrdinal))
	sql.WriteString(" ASC, ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(" ASC LIMIT ")
	sql.WriteString(fmtUint(uint64(maximumPairs)))
	sql.WriteString(")")
}

func writeStatsWildcardFinalSelect(
	sql *strings.Builder,
	metadataInvalidSQL string,
	policy eventAnalysisFinalizationPolicy,
) {
	q := quoteIdentifier
	sql.WriteString(" SELECT * FROM (SELECT toUInt8(0) AS ")
	sql.WriteString(q(StatsWildcardInventoryOrdinalColumn))
	sql.WriteString(", CAST('' AS String) AS ")
	sql.WriteString(q(StatsWildcardInventoryFieldColumn))
	// A scalar subquery is Nullable in ClickHouse even when its aggregate
	// relation always emits one row. Collapse an impossible empty scalar to an
	// invalid sentinel so the physical transport remains non-nullable UInt8.
	sql.WriteString(", toUInt8(ifNull(")
	sql.WriteString(metadataInvalidSQL)
	sql.WriteString(", toUInt8(1))) AS ")
	sql.WriteString(q(StatsWildcardInventoryInvalidColumn))
	sql.WriteString(" UNION ALL SELECT ")
	sql.WriteString(q(statsWildcardCandidateOrdinal))
	sql.WriteString(", ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(", toUInt8(0) FROM ")
	sql.WriteString(q(statsWildcardLimitedCTE))
	sql.WriteString(") AS __os_stats_wildcard_output")
	if policy.includeResultOrder {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(statsWildcardInventoryResultContract().order)
	}
}

func fmtUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func finalizeMaterializedStatsWildcardInventory(
	relation compiledRelation,
	args []any,
	request plan.StatsWildcardRequest,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
	knownNames, shadows, blockedPrefixes, reservedRoots []string,
	ordinals []uint8,
	regularExpressions []string,
	allowDynamic bool,
) (CompiledQuery, error) {
	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192 + len(knownNames)*16)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	writeCTEOpening(&sql, policy.materializeSharedCTEs)
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	// Validate every aligned metadata row before filtering to requested names.
	// A poisoned nonmatching field must invalidate the inventory atomically.
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

	sql.WriteString(q(fieldSuggestionDynamicCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(" AS ")
	sql.WriteString(q(statsWildcardCandidateField))
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
	sql.WriteString("), CAST(? AS UInt64)))) AS field_metadata WHERE NOT has(CAST(? AS Array(String)), ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(") AND NOT arrayExists(prefix -> ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(" = prefix OR startsWith(")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(", concat(prefix, '.')), CAST(? AS Array(String))) AND CAST(? AS Bool) AND (SELECT ")
	sql.WriteString(q(fieldSuggestionMetadataInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionMetadataCTE))
	sql.WriteString(") = 0 AND arrayExists(wildcard_re -> match(")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString(", wildcard_re), CAST(? AS Array(String)))) GROUP BY ")
	sql.WriteString(q(fieldSuggestionDynamicName))
	sql.WriteString("), ")
	args = append(args,
		uint64(eventfields.MaximumNormalizedFieldNameBytes+1),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		shadows,
		blockedPrefixes,
		allowDynamic,
		regularExpressions,
	)

	sql.WriteString(q(fieldSuggestionKnownCTE))
	sql.WriteString(" AS (SELECT arrayJoin(CAST(? AS Array(String))) AS ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString("), ")
	args = append(args, knownNames)

	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionDynamicCTE))
	sql.WriteString(" UNION ALL SELECT ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionKnownCTE))
	sql.WriteString("), ")

	sql.WriteString(q(statsWildcardDistinctCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(", toUInt8(0) AS ")
	sql.WriteString(q(statsWildcardCandidateInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionCandidatesCTE))
	sql.WriteString(" GROUP BY ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString("), ")

	args = writeStatsWildcardPatternCTE(&sql, args, ordinals, regularExpressions)
	writeStatsWildcardMatchAndLimit(&sql, statsWildcardDistinctCTE, request.MaximumPairs())
	writeStatsWildcardFinalSelect(
		&sql,
		"(SELECT "+q(fieldSuggestionMetadataInvalid)+" FROM "+q(fieldSuggestionMetadataCTE)+")",
		policy,
	)

	sourceDepth := relation.depth
	metadataDepth := relationalNodeDepth(sourceDepth)
	metadataScalarDepth := relationalNodeDepth(metadataDepth)
	dynamicLeavesDepth := relationalNodeDepth(sourceDepth, metadataScalarDepth)
	dynamicDepth := relationalNodeDepth(dynamicLeavesDepth)
	knownDepth := relationalNodeDepth()
	candidatesDepth := relationalNodeDepth(dynamicDepth, knownDepth)
	distinctDepth := relationalNodeDepth(candidatesDepth)
	patternsDepth := relationalNodeDepth()
	matchesDepth := relationalNodeDepth(distinctDepth, patternsDepth)
	limitedDepth := relationalNodeDepth(matchesDepth)
	headerDepth := relationalNodeDepth(metadataDepth)
	matchRowsDepth := relationalNodeDepth(limitedDepth)
	outputDepth := relationalNodeDepth(headerDepth, matchRowsDepth)
	resultDepth := relationalNodeDepth(outputDepth)
	compiled := CompiledQuery{
		SQL:                       sql.String(),
		Args:                      args,
		validationDummyProjection: statsWildcardInventoryDummyProjection(),
	}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func finalizePrerequisiteStatsWildcardInventory(
	relation compiledRelation,
	args []any,
	request plan.StatsWildcardRequest,
	ownerRange spl.Range,
	policy eventAnalysisFinalizationPolicy,
	knownNames, shadows, blockedPrefixes, reservedRoots []string,
	ordinals []uint8,
	regularExpressions []string,
	allowDynamic bool,
	sidecars []fieldSuggestionSidecar,
) (CompiledQuery, error) {
	q := quoteIdentifier
	var sql strings.Builder
	sql.Grow(len(relation.sql) + 16_384 + len(knownNames)*16)
	sql.WriteString("WITH ")
	sql.WriteString(q(fieldSuggestionSourceCTE))
	sql.WriteString(" AS (")
	sql.WriteString(relation.sql)
	sql.WriteString("), ")

	writePrerequisiteFieldSuggestionSidecars(&sql, sidecars)
	sql.WriteString(", ")
	writePrerequisiteFieldSuggestionRows(&sql, sidecars)
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
	sql.WriteString(", ")

	writePrerequisiteStatsWildcardObservations(&sql)
	args = append(args, shadows, blockedPrefixes, allowDynamic, regularExpressions)
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
	sql.WriteString(" AS ")
	sql.WriteString(q(statsWildcardCandidateField))
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

	sql.WriteString(q(statsWildcardDistinctCTE))
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString(", toUInt8(max(")
	sql.WriteString(q(fieldSuggestionGlobalInvalid))
	sql.WriteString(")) AS ")
	sql.WriteString(q(statsWildcardCandidateInvalid))
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionControlledCTE))
	sql.WriteString(" WHERE ")
	sql.WriteString(q(fieldSuggestionCandidateKind))
	sql.WriteString(" = toUInt8(1) GROUP BY ")
	sql.WriteString(q(statsWildcardCandidateField))
	sql.WriteString("), ")

	args = writeStatsWildcardPatternCTE(&sql, args, ordinals, regularExpressions)
	writeStatsWildcardMatchAndLimit(&sql, statsWildcardDistinctCTE, request.MaximumPairs())
	writeStatsWildcardFinalSelect(
		&sql,
		"(SELECT max("+q(fieldSuggestionGlobalInvalid)+") FROM "+q(fieldSuggestionControlledCTE)+")",
		policy,
	)
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
	distinctDepth := relationalNodeDepth(controlledDepth)
	patternsDepth := relationalNodeDepth()
	matchesDepth := relationalNodeDepth(distinctDepth, patternsDepth)
	limitedDepth := relationalNodeDepth(matchesDepth)
	headerDepth := relationalNodeDepth(controlledDepth)
	matchRowsDepth := relationalNodeDepth(limitedDepth)
	outputDepth := relationalNodeDepth(headerDepth, matchRowsDepth)
	resultDepth := relationalNodeDepth(outputDepth)
	compiled := CompiledQuery{
		SQL:                       sql.String(),
		Args:                      args,
		validationDummyProjection: statsWildcardInventoryDummyProjection(),
	}
	return withCompiledRelationalDepth(compiled, resultDepth, ownerRange), nil
}

func writePrerequisiteStatsWildcardObservations(sql *strings.Builder) {
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
		sql.WriteString(strconv.Itoa(index + 1))
		sql.WriteString(") AS ")
		sql.WriteString(q(name))
	}
	sql.WriteString(" FROM ")
	sql.WriteString(q(fieldSuggestionRowsCTE))
	sql.WriteString(" ARRAY JOIN arrayConcat([tuple(toUInt8(0), CAST('' AS String), ")
	sql.WriteString(q(fieldSuggestionRowInvalid))
	sql.WriteString(")], arrayMap(field_metadata -> tuple(toUInt8(1), tupleElement(field_metadata, 1), toUInt8(0)), arrayFilter(field_metadata -> ")
	sql.WriteString(q(fieldSuggestionRowInvalid))
	sql.WriteString(" = 0 AND NOT has(CAST(? AS Array(String)), tupleElement(field_metadata, 1)) AND NOT arrayExists(prefix -> tupleElement(field_metadata, 1) = prefix OR startsWith(tupleElement(field_metadata, 1), concat(prefix, '.')), CAST(? AS Array(String))) AND CAST(? AS Bool) AND arrayExists(wildcard_re -> match(tupleElement(field_metadata, 1), wildcard_re), CAST(? AS Array(String))), ")
	sql.WriteString(q(fieldSuggestionDynamicMetadata))
	sql.WriteString("))) AS ")
	sql.WriteString(q(fieldSuggestionObservation))
	sql.WriteString(" UNION ALL SELECT toUInt8(0), CAST('' AS String), toUInt8(0))")
}
