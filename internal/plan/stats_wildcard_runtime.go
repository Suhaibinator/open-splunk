package plan

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	statsWildcardRequestDomain   = "open-splunk-stats-wildcard-request-v1"
	statsWildcardExpansionDomain = "open-splunk-stats-wildcard-expansion-v1"
	// Analyze has already proved the logical graph acyclic and bounded. The
	// reflection serializer traverses interface, pointer, and struct wrappers
	// separately, so its independent stack guard must leave enough room for an
	// expression accepted at maximumAnalysisDepth.
	maximumStatsWildcardReflectionDepth = maximumAnalysisDepth * 8
)

// StatsWildcardPattern identifies one authored wildcard aggregate by its
// zero-based ordinal in the stats command. Pattern uses SPL wc-field syntax.
type StatsWildcardPattern struct {
	Ordinal uint8
	Pattern string
}

// StatsWildcardInventoryMatch is one exact runtime field matched for an
// authored aggregate. Inventory executors must order rows by Ordinal and then
// exact bytewise Field spelling before this trust boundary accepts them.
type StatsWildcardInventoryMatch struct {
	Ordinal uint8
	Field   string
}

// StatsWildcardRequest is opaque planner authority for one bounded field-name
// inventory. Its seal binds exact authored source, immutable scan scope,
// command ordinal, wildcard patterns, and the overflow-sentinel row limit.
type StatsWildcardRequest struct {
	sourceDigest   [sha256.Size]byte
	scopeDigest    [sha256.Size]byte
	commandIndex   uint16
	maximumPairs   uint8
	patterns       []StatsWildcardPattern
	authoredSource string
	scope          Scope
	basePrefix     [sha256.Size]byte
	seal           [sha256.Size]byte
}

func (request StatsWildcardRequest) IsZero() bool {
	return request.seal == ([sha256.Size]byte{}) &&
		request.sourceDigest == ([sha256.Size]byte{}) &&
		request.scopeDigest == ([sha256.Size]byte{}) &&
		request.commandIndex == 0 && request.maximumPairs == 0 &&
		len(request.patterns) == 0 && request.authoredSource == "" &&
		request.basePrefix == ([sha256.Size]byte{}) && statsWildcardScopeIsZero(request.scope)
}

func (request StatsWildcardRequest) Clone() StatsWildcardRequest {
	if !request.valid() {
		return StatsWildcardRequest{}
	}
	request.patterns = cloneStatsWildcardPatterns(request.patterns)
	request.authoredSource = strings.Clone(request.authoredSource)
	request.scope = cloneStatsWildcardScope(request.scope)
	return request
}

func (request StatsWildcardRequest) Equal(other StatsWildcardRequest) bool {
	if request.IsZero() || other.IsZero() {
		return request.IsZero() && other.IsZero()
	}
	left, leftOK := request.AuthorityDigest()
	right, rightOK := other.AuthorityDigest()
	return leftOK && rightOK && subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

func (request StatsWildcardRequest) AuthorityDigest() ([sha256.Size]byte, bool) {
	if !request.valid() {
		return [sha256.Size]byte{}, false
	}
	return request.seal, true
}

func (request StatsWildcardRequest) MaximumPairs() uint8 {
	if !request.valid() {
		return 0
	}
	return request.maximumPairs
}

func (request StatsWildcardRequest) Patterns() []StatsWildcardPattern {
	if !request.valid() {
		return nil
	}
	return cloneStatsWildcardPatterns(request.patterns)
}

// RetainedBytes returns a conservative charge for the complete private
// request authority, including parser-retained source and immutable scope.
func (request StatsWildcardRequest) RetainedBytes() (uint64, bool) {
	if !request.valid() {
		return 0, false
	}
	total := uint64(unsafe.Sizeof(request))
	add := func(charge uint64) bool {
		if charge > ^uint64(0)-total {
			return false
		}
		total += charge
		return true
	}
	if !add(uint64(cap(request.patterns))*uint64(unsafe.Sizeof(StatsWildcardPattern{}))) ||
		!add(uint64(len(request.authoredSource))) ||
		!add(uint64(len(request.scope.TenantID))) ||
		!add(uint64(len(request.scope.SearchTimezone))) {
		return 0, false
	}
	for _, pattern := range request.patterns {
		if !add(uint64(len(pattern.Pattern))) {
			return 0, false
		}
	}
	for _, indexes := range [][]string{
		request.scope.AuthorizedIndexes,
		request.scope.RequestedIndexes,
	} {
		if !add(uint64(cap(indexes)) * uint64(unsafe.Sizeof(""))) {
			return 0, false
		}
		for _, index := range indexes {
			if !add(uint64(len(index))) {
				return 0, false
			}
		}
	}
	if request.scope.VisibilityCutoff != nil &&
		!add(uint64(unsafe.Sizeof(*request.scope.VisibilityCutoff))) {
		return 0, false
	}
	return total, true
}

// ValidForPrefix proves that prefix is the exact planner-produced relation and
// immutable read scope for this request. Knowledge injection preserves these
// private facts, so configured admission can discover post-knowledge fields.
func (request StatsWildcardRequest) ValidForPrefix(prefix *Query) bool {
	if !request.valid() || prefix == nil ||
		prefix.statsWildcardRequestDigest != request.seal {
		return false
	}
	canonical, ok := request.CanonicalPrefixFor(prefix)
	if !ok {
		return false
	}
	want, wantOK := statsWildcardPlanSemanticDigest(canonical)
	got, gotOK := statsWildcardPlanSemanticDigest(prefix)
	return wantOK && gotOK && subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

// CanonicalPrefixFor rebuilds the private authored source under the retained
// immutable scope and optionally applies only prefix's independently validated
// opaque knowledge program. Callers must compile this returned plan, never a
// caller-mutable prefix. The supplied prefix remains a semantic witness whose
// exact graph is checked by ValidForPrefix.
func (request StatsWildcardRequest) CanonicalPrefixFor(prefix *Query) (*Query, bool) {
	if !request.valid() || prefix == nil ||
		prefix.parsedSourceDigest != request.sourceDigest ||
		prefix.statsWildcardRequestDigest != request.seal {
		return nil, false
	}
	canonical := request.canonicalBasePrefix()
	if canonical == nil {
		return nil, false
	}
	program, knowledgePresent := prefix.KnowledgePrelude()
	if knowledgePresent {
		var err error
		canonical, err = InjectKnowledgePrelude(canonical, program)
		if err != nil {
			return nil, false
		}
	}
	return canonical, true
}

func (request StatsWildcardRequest) canonicalBasePrefix() *Query {
	if !request.valid() {
		return nil
	}
	parsed, err := spl.Parse(request.authoredSource)
	if err != nil {
		return nil
	}
	prefixAST, ok := parsed.ParsedPrefix(int(request.commandIndex))
	if !ok {
		return nil
	}
	canonical, err := Build(prefixAST, cloneStatsWildcardScope(request.scope))
	if err != nil {
		return nil
	}
	baseDigest, ok := statsWildcardPlanSemanticDigest(canonical)
	if !ok || subtle.ConstantTimeCompare(baseDigest[:], request.basePrefix[:]) != 1 {
		return nil
	}
	canonical.statsWildcardRequestDigest = request.seal
	return canonical
}

func (request StatsWildcardRequest) valid() bool {
	if request.sourceDigest == ([sha256.Size]byte{}) || request.authoredSource == "" ||
		sha256.Sum256([]byte(request.authoredSource)) != request.sourceDigest ||
		request.scopeDigest == ([sha256.Size]byte{}) ||
		request.basePrefix == ([sha256.Size]byte{}) ||
		request.maximumPairs < 2 ||
		request.maximumPairs > uint8(spl.MaximumStatsMeasures+1) ||
		len(request.patterns) == 0 || len(request.patterns) > spl.MaximumStatsMeasures {
		return false
	}
	previous := -1
	for _, pattern := range request.patterns {
		if int(pattern.Ordinal) <= previous || !spl.IsStatsFieldGlob(pattern.Pattern) {
			return false
		}
		previous = int(pattern.Ordinal)
	}
	parsed, err := spl.Parse(request.authoredSource)
	if err != nil {
		return false
	}
	prefixAST, ok := parsed.ParsedPrefix(int(request.commandIndex))
	if !ok {
		return false
	}
	prefix, err := Build(prefixAST, cloneStatsWildcardScope(request.scope))
	if err != nil {
		return false
	}
	scopeDigest, ok := statsWildcardPlanScopeDigest(prefix)
	if !ok || subtle.ConstantTimeCompare(scopeDigest[:], request.scopeDigest[:]) != 1 {
		return false
	}
	basePrefix, ok := statsWildcardPlanSemanticDigest(prefix)
	if !ok || subtle.ConstantTimeCompare(basePrefix[:], request.basePrefix[:]) != 1 {
		return false
	}
	expected := statsWildcardRequestDigest(request)
	return subtle.ConstantTimeCompare(expected[:], request.seal[:]) == 1
}

// StatsWildcardExpansion is replayable, backend-neutral evidence for the
// exact names used to expand one open-schema stats command.
type StatsWildcardExpansion struct {
	request StatsWildcardRequest
	matches []StatsWildcardInventoryMatch
	seal    [sha256.Size]byte
}

func (expansion StatsWildcardExpansion) IsZero() bool {
	return expansion.request.IsZero() && len(expansion.matches) == 0 &&
		expansion.seal == ([sha256.Size]byte{})
}

func (expansion StatsWildcardExpansion) Clone() StatsWildcardExpansion {
	if !expansion.valid() {
		return StatsWildcardExpansion{}
	}
	expansion.request = expansion.request.Clone()
	expansion.matches = cloneStatsWildcardMatches(expansion.matches)
	return expansion
}

func (expansion StatsWildcardExpansion) Equal(other StatsWildcardExpansion) bool {
	if expansion.IsZero() || other.IsZero() {
		return expansion.IsZero() && other.IsZero()
	}
	left, leftOK := expansion.AuthorityDigest()
	right, rightOK := other.AuthorityDigest()
	return leftOK && rightOK && subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

func (expansion StatsWildcardExpansion) AuthorityDigest() ([sha256.Size]byte, bool) {
	if !expansion.valid() {
		return [sha256.Size]byte{}, false
	}
	return expansion.seal, true
}

func (expansion StatsWildcardExpansion) RetainedBytes() (uint64, bool) {
	if !expansion.valid() {
		return 0, false
	}
	requestBytes, ok := expansion.request.RetainedBytes()
	if !ok {
		return 0, false
	}
	total := uint64(unsafe.Sizeof(expansion))
	if requestBytes > ^uint64(0)-total {
		return 0, false
	}
	total += requestBytes
	total += uint64(cap(expansion.matches)) * uint64(unsafe.Sizeof(StatsWildcardInventoryMatch{}))
	for _, match := range expansion.matches {
		if uint64(len(match.Field)) > ^uint64(0)-total {
			return 0, false
		}
		total += uint64(len(match.Field))
	}
	return total, true
}

func (expansion StatsWildcardExpansion) valid() bool {
	if !expansion.request.valid() || len(expansion.matches) == 0 ||
		len(expansion.matches) >= int(expansion.request.maximumPairs) ||
		!validStatsWildcardMatches(expansion.request, expansion.matches, true) {
		return false
	}
	expected := statsWildcardExpansionDigest(expansion.request, expansion.matches)
	return subtle.ConstantTimeCompare(expected[:], expansion.seal[:]) == 1
}

// StatsWildcardPreparation contains exactly one ordinary full plan or one
// prefix/request pair. Accessors detach all public slices.
type StatsWildcardPreparation struct {
	fullPlan   *Query
	fullSource string
	fullScope  Scope
	prefix     *Query
	request    StatsWildcardRequest
}

func (preparation *StatsWildcardPreparation) FullPlan() *Query {
	if preparation == nil || preparation.fullPlan == nil ||
		preparation.prefix != nil || !preparation.request.IsZero() {
		return nil
	}
	parsed, err := spl.Parse(preparation.fullSource)
	if err != nil {
		return nil
	}
	logical, err := Build(parsed, cloneStatsWildcardScope(preparation.fullScope))
	if err != nil {
		return nil
	}
	return logical
}

func (preparation *StatsWildcardPreparation) Prefix() *Query {
	if preparation == nil || preparation.prefix == nil ||
		preparation.fullPlan != nil || !preparation.request.ValidForPrefix(preparation.prefix) {
		return nil
	}
	return preparation.request.canonicalBasePrefix()
}

func (preparation *StatsWildcardPreparation) Request() StatsWildcardRequest {
	if preparation == nil || preparation.prefix == nil || preparation.fullPlan != nil ||
		!preparation.request.ValidForPrefix(preparation.prefix) {
		return StatsWildcardRequest{}
	}
	return preparation.request.Clone()
}

// PrepareStatsWildcard performs ordinary planning when the schema is already
// closed. Otherwise it returns the exact relation immediately before the first
// unresolved stats wildcard and an opaque bounded inventory request.
func PrepareStatsWildcard(query *spl.Query, scope Scope) (*StatsWildcardPreparation, error) {
	if query == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "query is nil"}
	}
	canonical, parsed := query.ParsedCanonicalClone()
	if !parsed {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "runtime stats wc-field expansion requires an unmodified parser-owned query",
			Range:   query.Range,
		}
	}
	logical, err := Build(canonical, scope)
	if err == nil {
		authoredSource, ok := canonical.ParsedSource()
		if !ok {
			return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "parser-owned source is unavailable"}
		}
		return &StatsWildcardPreparation{
			fullPlan: logical, fullSource: authoredSource, fullScope: cloneStatsWildcardScope(scope),
		}, nil
	}
	diagnostic, ok := err.(*Diagnostic)
	if !ok || diagnostic.Code != "SPL_UNSUPPORTED_STATS_WILDCARD" {
		return nil, err
	}
	sourceDigest, parsed := canonical.ParsedSourceDigest()
	if !parsed {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "runtime stats wc-field expansion requires parser-owned source provenance",
			Range:   diagnostic.Range,
		}
	}
	commandIndex, command, found := firstStatsWildcardCommand(canonical)
	if !found {
		return nil, err
	}
	prefixAST, ok := canonical.ParsedPrefix(commandIndex)
	if !ok {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "runtime stats wc-field prefix provenance is invalid",
			Range:   command.Range,
		}
	}
	prefix, prefixErr := Build(prefixAST, scope)
	if prefixErr != nil {
		return nil, prefixErr
	}
	request, requestErr := newStatsWildcardRequest(
		sourceDigest,
		canonical,
		commandIndex,
		command,
		prefix,
	)
	if requestErr != nil {
		return nil, requestErr
	}
	prefix.statsWildcardRequestDigest = request.seal
	return &StatsWildcardPreparation{prefix: prefix, request: request}, nil
}

// ValidateStatsWildcardInventory validates a complete, atomically buffered
// inventory and mints opaque replay evidence. The request's final permitted
// row is an overflow sentinel and is never admitted as a successful expansion.
func ValidateStatsWildcardInventory(
	request StatsWildcardRequest,
	matches []StatsWildcardInventoryMatch,
) (StatsWildcardExpansion, error) {
	if !request.valid() {
		return StatsWildcardExpansion{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "stats wc-field inventory request is invalid",
		}
	}
	if len(matches) == 0 {
		return StatsWildcardExpansion{}, &Diagnostic{
			Code:    "SPL_NO_MATCHING_STATS_FIELDS",
			Message: fmt.Sprintf("stats wc-field %q matched no fields", request.patterns[0].Pattern),
		}
	}
	if len(matches) >= int(request.maximumPairs) {
		return StatsWildcardExpansion{}, statsWildcardMeasureLimitDiagnostic(spl.Range{})
	}
	if !validStatsWildcardMatches(request, matches, false) {
		return StatsWildcardExpansion{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "stats wc-field inventory rows are invalid or out of order",
		}
	}
	seen := make(map[uint8]bool, len(request.patterns))
	for _, match := range matches {
		seen[match.Ordinal] = true
	}
	for _, pattern := range request.patterns {
		if !seen[pattern.Ordinal] {
			return StatsWildcardExpansion{}, &Diagnostic{
				Code:    "SPL_NO_MATCHING_STATS_FIELDS",
				Message: fmt.Sprintf("stats wc-field %q matched no fields", pattern.Pattern),
			}
		}
	}
	expansion := StatsWildcardExpansion{
		request: request.Clone(),
		matches: cloneStatsWildcardMatches(matches),
	}
	expansion.seal = statsWildcardExpansionDigest(expansion.request, expansion.matches)
	if !expansion.valid() {
		return StatsWildcardExpansion{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "stats wc-field expansion authority is invalid",
		}
	}
	return expansion, nil
}

// BuildWithStatsWildcardExpansion rebuilds the exact authored query with one
// validated runtime expansion. It never mutates the parser-owned AST.
func BuildWithStatsWildcardExpansion(
	query *spl.Query,
	scope Scope,
	expansion StatsWildcardExpansion,
) (*Query, error) {
	if query == nil || !expansion.valid() {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field expansion is invalid"}
	}
	preparation, err := PrepareStatsWildcard(query, scope)
	if err != nil {
		return nil, err
	}
	request := preparation.Request()
	if request.IsZero() || !request.Equal(expansion.request) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "stats wc-field expansion does not belong to this query and scope",
		}
	}
	commandIndex := int(request.commandIndex)
	if commandIndex < 0 || commandIndex >= len(query.Commands) {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field command ordinal is invalid"}
	}
	command, ok := query.Commands[commandIndex].(*spl.StatsCommand)
	if !ok || command == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field command changed"}
	}
	expandedAggregates, expandErr := expandStatsWildcardInventoryAggregates(
		command,
		expansion.matches,
	)
	if expandErr != nil {
		return nil, expandErr
	}
	expandedCommand := *command
	expandedCommand.Aggregates = expandedAggregates
	canonical, ok := query.ParsedCanonicalClone()
	if !ok {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field source AST changed"}
	}
	expandedQuery := *canonical
	expandedQuery.Commands = slices.Clone(canonical.Commands)
	expandedQuery.Commands[commandIndex] = &expandedCommand
	return Build(&expandedQuery, scope)
}

func firstStatsWildcardCommand(query *spl.Query) (int, *spl.StatsCommand, bool) {
	for index, candidate := range query.Commands {
		command, ok := candidate.(*spl.StatsCommand)
		if !ok || command == nil {
			continue
		}
		for _, aggregate := range command.Aggregates {
			if aggregate.InputGlob != nil ||
				(aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil) {
				return index, command, true
			}
		}
	}
	return 0, nil, false
}

func newStatsWildcardRequest(
	sourceDigest [sha256.Size]byte,
	query *spl.Query,
	commandIndex int,
	command *spl.StatsCommand,
	prefix *Query,
) (StatsWildcardRequest, error) {
	authoredSource, parsed := query.ParsedSource()
	if !parsed || sourceDigest == ([sha256.Size]byte{}) || commandIndex < 0 || commandIndex > int(^uint16(0)) ||
		command == nil || prefix == nil || len(command.Aggregates) > spl.MaximumStatsMeasures {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field request input is invalid"}
	}
	patterns := make([]StatsWildcardPattern, 0, len(command.Aggregates))
	nonWildcard := 0
	for ordinal, aggregate := range command.Aggregates {
		glob := aggregate.InputGlob
		valid := spl.ValidStatsFieldGlobAggregate(aggregate)
		if aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil {
			glob = aggregate.Sparkline.InputGlob
			valid = spl.ValidStatsSparklineFieldGlobAggregate(aggregate)
		}
		if glob == nil {
			nonWildcard++
			continue
		}
		if !valid {
			return StatsWildcardRequest{}, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats wc-field aggregate metadata is invalid",
				Range:   glob.Range,
			}
		}
		patterns = append(patterns, StatsWildcardPattern{
			Ordinal: uint8(ordinal), // #nosec G115 -- stats measures are bounded to 16.
			Pattern: strings.Clone(glob.Pattern),
		})
	}
	remaining := spl.MaximumStatsMeasures - nonWildcard
	if len(patterns) == 0 || remaining < len(patterns) {
		return StatsWildcardRequest{}, statsWildcardMeasureLimitDiagnostic(command.Range)
	}
	scopeDigest, ok := statsWildcardPlanScopeDigest(prefix)
	if !ok {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "stats wc-field prefix scope is invalid", Range: command.Range}
	}
	basePrefix, ok := statsWildcardPlanSemanticDigest(prefix)
	if !ok {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field prefix semantics are invalid", Range: command.Range}
	}
	request := StatsWildcardRequest{
		sourceDigest:   sourceDigest,
		scopeDigest:    scopeDigest,
		commandIndex:   uint16(commandIndex), // #nosec G115 -- parser commands are bounded well below uint16.
		maximumPairs:   uint8(remaining + 1), // #nosec G115 -- maximum is 17.
		patterns:       patterns,
		authoredSource: strings.Clone(authoredSource),
		scope:          statsWildcardScopeFromPlan(prefix),
		basePrefix:     basePrefix,
	}
	request.seal = statsWildcardRequestDigest(request)
	if !request.valid() {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field request authority is invalid", Range: command.Range}
	}
	return request, nil
}

func validStatsWildcardMatches(
	request StatsWildcardRequest,
	matches []StatsWildcardInventoryMatch,
	requireComplete bool,
) bool {
	if !request.valid() || len(matches) >= int(request.maximumPairs) ||
		(requireComplete && len(matches) == 0) {
		return false
	}
	patternByOrdinal := make(map[uint8]string, len(request.patterns))
	matchedOrdinals := make(map[uint8]bool, len(request.patterns))
	for _, pattern := range request.patterns {
		patternByOrdinal[pattern.Ordinal] = pattern.Pattern
	}
	previousOrdinal := -1
	previousField := ""
	for _, match := range matches {
		pattern, ok := patternByOrdinal[match.Ordinal]
		if !ok || !spl.IsSafeStatsInventoryFieldName(match.Field) ||
			!spl.MatchStatsFieldGlob(pattern, match.Field) {
			return false
		}
		if int(match.Ordinal) < previousOrdinal ||
			(int(match.Ordinal) == previousOrdinal && strings.Compare(match.Field, previousField) <= 0) {
			return false
		}
		previousOrdinal = int(match.Ordinal)
		previousField = match.Field
		matchedOrdinals[match.Ordinal] = true
	}
	if requireComplete {
		for _, pattern := range request.patterns {
			if !matchedOrdinals[pattern.Ordinal] {
				return false
			}
		}
	}
	return true
}

func expandStatsWildcardInventoryAggregates(
	command *spl.StatsCommand,
	matches []StatsWildcardInventoryMatch,
) ([]spl.StatsAggregate, *Diagnostic) {
	expanded := make([]spl.StatsAggregate, 0, spl.MaximumStatsMeasures)
	matchIndex := 0
	for ordinal, aggregate := range command.Aggregates {
		wildcard := aggregate.InputGlob != nil ||
			(aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil)
		if !wildcard {
			expanded = append(expanded, aggregate)
			continue
		}
		matchesForAggregate := 0
		for matchIndex < len(matches) && int(matches[matchIndex].Ordinal) == ordinal {
			match := matches[matchIndex]
			concrete, ok := spl.ExpandStatsFieldGlob(aggregate, match.Field)
			if aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil {
				concrete, ok = spl.ExpandStatsSparklineFieldGlob(aggregate, match.Field)
			}
			if !ok {
				return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "stats wc-field expansion row is inconsistent", Range: aggregate.Range}
			}
			expanded = append(expanded, concrete)
			matchesForAggregate++
			matchIndex++
		}
		if matchesForAggregate == 0 {
			glob := aggregate.InputGlob
			if aggregate.Sparkline != nil {
				glob = aggregate.Sparkline.InputGlob
			}
			return nil, &Diagnostic{Code: "SPL_NO_MATCHING_STATS_FIELDS", Message: fmt.Sprintf("stats wc-field %q matched no fields", glob.Pattern), Range: glob.Range}
		}
	}
	if matchIndex != len(matches) || len(expanded) > spl.MaximumStatsMeasures {
		return nil, statsWildcardMeasureLimitDiagnostic(command.Range)
	}
	return expanded, nil
}

func statsWildcardPlanScopeDigest(query *Query) ([sha256.Size]byte, bool) {
	if query == nil || len(query.Operators) == 0 || len(query.EffectiveIndexes) == 0 {
		return [sha256.Size]byte{}, false
	}
	scan, ok := query.Operators[0].(*Scan)
	if !ok || scan == nil || scan.TenantID == "" || !slices.Equal(scan.Indexes, query.EffectiveIndexes) {
		return [sha256.Size]byte{}, false
	}
	digest := sha256.New()
	writeStatsWildcardString(digest, "open-splunk-stats-wildcard-scope-v1")
	writeStatsWildcardString(digest, scan.TenantID)
	writeStatsWildcardStrings(digest, scan.Indexes)
	if !writeStatsWildcardTime(digest, scan.Earliest) || !writeStatsWildcardTime(digest, scan.Latest) ||
		!writeStatsWildcardTime(digest, scan.IndexTimeCutoff) ||
		!writeStatsWildcardTime(digest, query.SearchStart) {
		return [sha256.Size]byte{}, false
	}
	writeStatsWildcardUint64(digest, scan.VisibilityCutoff)
	writeStatsWildcardString(digest, query.SearchTimezone)
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, true
}

func statsWildcardRequestDigest(request StatsWildcardRequest) [sha256.Size]byte {
	digest := sha256.New()
	writeStatsWildcardString(digest, statsWildcardRequestDomain)
	_, _ = digest.Write(request.sourceDigest[:])
	_, _ = digest.Write(request.scopeDigest[:])
	_, _ = digest.Write(request.basePrefix[:])
	writeStatsWildcardString(digest, request.authoredSource)
	writeStatsWildcardUint64(digest, uint64(request.commandIndex))
	writeStatsWildcardUint64(digest, uint64(request.maximumPairs))
	writeStatsWildcardUint64(digest, uint64(len(request.patterns)))
	for _, pattern := range request.patterns {
		writeStatsWildcardUint64(digest, uint64(pattern.Ordinal))
		writeStatsWildcardString(digest, pattern.Pattern)
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func statsWildcardExpansionDigest(
	request StatsWildcardRequest,
	matches []StatsWildcardInventoryMatch,
) [sha256.Size]byte {
	digest := sha256.New()
	writeStatsWildcardString(digest, statsWildcardExpansionDomain)
	_, _ = digest.Write(request.seal[:])
	writeStatsWildcardUint64(digest, uint64(len(matches)))
	for _, match := range matches {
		writeStatsWildcardUint64(digest, uint64(match.Ordinal))
		writeStatsWildcardString(digest, match.Field)
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func cloneStatsWildcardPatterns(source []StatsWildcardPattern) []StatsWildcardPattern {
	result := make([]StatsWildcardPattern, len(source))
	for index, pattern := range source {
		result[index] = StatsWildcardPattern{Ordinal: pattern.Ordinal, Pattern: strings.Clone(pattern.Pattern)}
	}
	return result
}

func cloneStatsWildcardMatches(source []StatsWildcardInventoryMatch) []StatsWildcardInventoryMatch {
	result := make([]StatsWildcardInventoryMatch, len(source))
	for index, match := range source {
		result[index] = StatsWildcardInventoryMatch{Ordinal: match.Ordinal, Field: strings.Clone(match.Field)}
	}
	return result
}

func cloneStatsWildcardScope(source Scope) Scope {
	result := source
	result.TenantID = strings.Clone(source.TenantID)
	result.AuthorizedIndexes = slices.Clone(source.AuthorizedIndexes)
	result.RequestedIndexes = slices.Clone(source.RequestedIndexes)
	result.SearchTimezone = strings.Clone(source.SearchTimezone)
	if source.VisibilityCutoff != nil {
		visibility := *source.VisibilityCutoff
		result.VisibilityCutoff = &visibility
	}
	return result
}

func statsWildcardScopeIsZero(scope Scope) bool {
	return scope.TenantID == "" && len(scope.AuthorizedIndexes) == 0 &&
		len(scope.RequestedIndexes) == 0 && scope.Earliest.IsZero() &&
		scope.Latest.IsZero() && scope.SearchStart.IsZero() &&
		scope.SearchTimezone == "" && scope.IndexTimeCutoff.IsZero() &&
		scope.VisibilityCutoff == nil
}

func statsWildcardScopeFromPlan(query *Query) Scope {
	if query == nil || len(query.Operators) == 0 {
		return Scope{}
	}
	scan, ok := query.Operators[0].(*Scan)
	if !ok || scan == nil {
		return Scope{}
	}
	visibility := scan.VisibilityCutoff
	return Scope{
		TenantID:          strings.Clone(scan.TenantID),
		AuthorizedIndexes: slices.Clone(scan.Indexes),
		RequestedIndexes:  slices.Clone(scan.Indexes),
		Earliest:          scan.Earliest,
		Latest:            scan.Latest,
		SearchStart:       query.SearchStart,
		SearchTimezone:    strings.Clone(query.SearchTimezone),
		IndexTimeCutoff:   scan.IndexTimeCutoff,
		VisibilityCutoff:  &visibility,
	}
}

func statsWildcardPlanSemanticDigest(query *Query) ([sha256.Size]byte, bool) {
	if query == nil {
		return [sha256.Size]byte{}, false
	}
	if _, err := Analyze(query); err != nil || ValidateKnowledgePreludeIntegrity(query) != nil {
		return [sha256.Size]byte{}, false
	}
	program, knowledgePresent := query.KnowledgePrelude()
	generatedOperators := 0
	if knowledgePresent {
		generatedOperators = int(program.Charges().GeneratedOperators)
		if len(query.Operators) < generatedOperators+1 {
			return [sha256.Size]byte{}, false
		}
	}
	digest := sha256.New()
	writeStatsWildcardString(digest, "open-splunk-stats-wildcard-prefix-v1")
	writeStatsWildcardUint64(digest, uint64(len(query.Operators)-generatedOperators))
	for index, operator := range query.Operators {
		if knowledgePresent && index > 0 && index <= generatedOperators {
			continue
		}
		if !writeStatsWildcardSemanticValue(digest, reflect.ValueOf(operator), 0) {
			return [sha256.Size]byte{}, false
		}
	}
	writeStatsWildcardStrings(digest, query.EffectiveIndexes)
	writeStatsWildcardStrings(digest, query.OutputFields)
	if query.DynamicOutput == nil {
		writeStatsWildcardUint64(digest, 0)
	} else {
		writeStatsWildcardUint64(digest, 1)
		writeStatsWildcardStrings(digest, query.DynamicOutput.FixedFields)
		writeStatsWildcardUint64(digest, uint64(query.DynamicOutput.MaxSeries))
	}
	if !writeStatsWildcardTime(digest, query.SearchStart) {
		return [sha256.Size]byte{}, false
	}
	writeStatsWildcardString(digest, query.SearchTimezone)
	writeStatsWildcardUint64(digest, uint64(query.parsedEvalPredicates))
	if query.parsedSPL {
		writeStatsWildcardUint64(digest, 1)
	} else {
		writeStatsWildcardUint64(digest, 0)
	}
	_, _ = digest.Write(query.parsedSourceDigest[:])
	if knowledgePresent {
		commitment, ok := program.Commitment()
		if !ok {
			return [sha256.Size]byte{}, false
		}
		writeStatsWildcardUint64(digest, 1)
		_, _ = digest.Write(commitment[:])
		writeStatsWildcardUint64(digest, uint64(generatedOperators))
	} else {
		writeStatsWildcardUint64(digest, 0)
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, true
}

func writeStatsWildcardSemanticValue(writer hash.Hash, value reflect.Value, depth int) bool {
	if depth > maximumStatsWildcardReflectionDepth || !value.IsValid() {
		return false
	}
	writeStatsWildcardString(writer, value.Type().String())
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			writeStatsWildcardUint64(writer, 0)
			return true
		}
		writeStatsWildcardUint64(writer, 1)
		return writeStatsWildcardSemanticValue(writer, value.Elem(), depth+1)
	case reflect.Struct:
		if value.Type() == reflect.TypeFor[time.Time]() {
			return writeStatsWildcardTime(writer, value.Interface().(time.Time))
		}
		writeStatsWildcardUint64(writer, uint64(value.NumField()))
		for index := 0; index < value.NumField(); index++ {
			if !writeStatsWildcardSemanticValue(writer, value.Field(index), depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		writeStatsWildcardUint64(writer, uint64(value.Len()))
		for index := 0; index < value.Len(); index++ {
			if !writeStatsWildcardSemanticValue(writer, value.Index(index), depth+1) {
				return false
			}
		}
		return true
	case reflect.String:
		writeStatsWildcardString(writer, value.String())
		return true
	case reflect.Bool:
		if value.Bool() {
			writeStatsWildcardUint64(writer, 1)
		} else {
			writeStatsWildcardUint64(writer, 0)
		}
		return true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		writeStatsWildcardUint64(writer, uint64(value.Int()))
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		writeStatsWildcardUint64(writer, value.Uint())
		return true
	case reflect.Float32, reflect.Float64:
		writeStatsWildcardUint64(writer, math.Float64bits(value.Convert(reflect.TypeFor[float64]()).Float()))
		return true
	default:
		return false
	}
}

func cloneStatsWildcardPlan(query *Query) *Query {
	if query == nil {
		return nil
	}
	program, knowledgePresent := query.KnowledgePrelude()
	if knowledgePresent {
		generated := int(program.Charges().GeneratedOperators)
		if ValidateKnowledgePreludeIntegrity(query) != nil ||
			len(query.Operators) < generated+1 {
			return nil
		}
		base := cloneQueryHeader(query)
		base.Operators = make([]Operator, 0, len(query.Operators)-generated)
		base.Operators = append(base.Operators, query.Operators[0])
		base.Operators = append(base.Operators, query.Operators[generated+1:]...)
		base.knowledgePrelude = queryKnowledgePrelude{}
		clonedBase := cloneStatsWildcardPlan(base)
		if clonedBase == nil {
			return nil
		}
		injected, err := InjectKnowledgePrelude(clonedBase, program)
		if err != nil {
			return nil
		}
		return injected
	}
	value, ok := cloneStatsWildcardSemanticValue(reflect.ValueOf(query), 0)
	if !ok || !value.IsValid() || value.IsNil() {
		return nil
	}
	result, _ := value.Interface().(*Query)
	return result
}

func cloneStatsWildcardSemanticValue(value reflect.Value, depth int) (reflect.Value, bool) {
	if depth > maximumStatsWildcardReflectionDepth || !value.IsValid() {
		return reflect.Value{}, false
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned, ok := cloneStatsWildcardSemanticValue(value.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result, true
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned, ok := cloneStatsWildcardSemanticValue(value.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloned)
		return result, true
	case reflect.Struct:
		if value.Type() == reflect.TypeFor[queryKnowledgePrelude]() {
			marker := value.Interface().(queryKnowledgePrelude)
			return reflect.ValueOf(queryKnowledgePrelude{
				program: marker.program.Clone(), set: marker.set,
			}), true
		}
		result := reflect.New(value.Type()).Elem()
		for index := 0; index < value.NumField(); index++ {
			cloned, ok := cloneStatsWildcardSemanticValue(value.Field(index), depth+1)
			if !ok || !result.Field(index).CanSet() {
				return reflect.Value{}, false
			}
			result.Field(index).Set(cloned)
		}
		return result, true
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			cloned, ok := cloneStatsWildcardSemanticValue(value.Index(index), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			result.Index(index).Set(cloned)
		}
		return result, true
	default:
		return value, true
	}
}

func writeStatsWildcardString(writer hash.Hash, value string) {
	writeStatsWildcardUint64(writer, uint64(len(value)))
	_, _ = writer.Write([]byte(value))
}

func writeStatsWildcardStrings(writer hash.Hash, values []string) {
	writeStatsWildcardUint64(writer, uint64(len(values)))
	for _, value := range values {
		writeStatsWildcardString(writer, value)
	}
}

func writeStatsWildcardUint64(writer hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeStatsWildcardTime(writer hash.Hash, value time.Time) bool {
	encoded, err := value.MarshalBinary()
	if err != nil {
		return false
	}
	writeStatsWildcardUint64(writer, uint64(len(encoded)))
	_, _ = writer.Write(encoded)
	writeStatsWildcardString(writer, value.Location().String())
	return true
}
