package plan

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"slices"
	"strings"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// MaximumRuntimeWildcardStages bounds cumulative discovery and recursive replay
// work independently of per-command conversion and aggregate field limits.
const MaximumRuntimeWildcardStages = 8

// ValidForRequest verifies that the complete returned evidence belongs to the
// exact inventory round executed, including every preceding discovery stage.
func (expansion StatsWildcardExpansion) ValidForRequest(request StatsWildcardRequest) bool {
	return expansion.valid() && request.valid() && expansion.request.Equal(request)
}

// IsConvert distinguishes conversion discovery from aggregate discovery while
// retaining the existing sealed inventory transport and snapshot interfaces.
func (request StatsWildcardRequest) IsConvert() bool { return request.conversion && request.valid() }

// Exclusions returns detached selectors applied before discovery's overflow cap.
func (request StatsWildcardRequest) Exclusions() []string {
	if !request.valid() {
		return nil
	}
	return slices.Clone(request.exclusions)
}

// AdditionalMatches includes aliases created by earlier clauses in this command.
func (request StatsWildcardRequest) AdditionalMatches() []StatsWildcardInventoryMatch {
	if !request.valid() {
		return nil
	}
	return cloneStatsWildcardMatches(request.additions)
}

// IsSafeConvertInventoryFieldName admits legal literal aliases while retaining
// the reserved-root, control-character and canonical field-reference checks.
func IsSafeConvertInventoryFieldName(name string) bool {
	if name == "fields" || strings.HasPrefix(strings.ToLower(name), "__os_") || !spl.IsStatsLiteralOutputName(name) {
		return false
	}
	field, err := ResolveField(name, spl.Range{})
	if err != nil && spl.IsStatsLiteralFieldReference(name) {
		field, err = ResolveQuotedField(name, spl.Range{})
	}
	return err == nil && field.Name == name
}

func runtimeWildcardMatch(request StatsWildcardRequest, pattern, name string) bool {
	if !request.conversion {
		return spl.MatchStatsFieldGlob(pattern, name)
	}
	if !spl.MatchConvertFieldGlob(pattern, name) {
		return false
	}
	for _, excluded := range request.exclusions {
		if excluded == name || spl.MatchConvertFieldGlob(excluded, name) {
			return false
		}
	}
	return true
}

func runtimeWildcardLimitDiagnostic(request StatsWildcardRequest, owner spl.Range) *Diagnostic {
	if request.conversion {
		return convertWriteLimit(owner)
	}
	return statsWildcardMeasureLimitDiagnostic(owner)
}

func prepareRuntimeWildcard(query *spl.Query, scope Scope, previous *StatsWildcardExpansion) (*StatsWildcardPreparation, error) {
	if query == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "query is nil"}
	}
	canonical, ok := query.ParsedCanonicalClone()
	if !ok {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "runtime wildcard expansion requires an unmodified parser-owned query"}
	}
	if previous != nil {
		if !previous.valid() {
			return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "prior wildcard evidence is invalid"}
		}
		source, _ := canonical.ParsedSourceDigest()
		prefix, ok := canonical.ParsedPrefix(int(previous.request.commandIndex))
		if !ok || source != previous.request.sourceDigest {
			return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "prior wildcard evidence belongs to another source"}
		}
		logical, err := buildRuntimeWildcardPrefix(prefix, scope, previous.request.previous)
		if err != nil {
			return nil, err
		}
		digest, ok := statsWildcardPlanScopeDigest(logical)
		if !ok || subtle.ConstantTimeCompare(digest[:], previous.request.scopeDigest[:]) != 1 {
			return nil, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "prior wildcard evidence belongs to another scope"}
		}
	}
	logical, err := buildRuntimeWildcardPrefix(canonical, scope, previous)
	if err == nil {
		source, _ := canonical.ParsedSource()
		return &StatsWildcardPreparation{fullPlan: logical, fullSource: source, fullScope: cloneStatsWildcardScope(scope), fullExpansion: previous}, nil
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || (diagnostic.Code != "SPL_UNSUPPORTED_STATS_WILDCARD" && diagnostic.Code != "SPL_UNSUPPORTED_CONVERT_WILDCARD") {
		return nil, err
	}
	stages := 0
	for prior := previous; prior != nil; prior = prior.request.previous {
		stages++
	}
	if stages >= MaximumRuntimeWildcardStages {
		return nil, &Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "search exceeds the eight-stage runtime wildcard discovery limit", Range: diagnostic.Range}
	}
	// The unresolved selector range identifies its owning command without
	// reparsing and planning every preceding prefix again.
	for index, candidate := range canonical.Commands {
		owner := candidate.SourceRange()
		if diagnostic.Range.Start.Offset < owner.Start.Offset || diagnostic.Range.End.Offset > owner.End.Offset {
			continue
		}
		if previous != nil && index <= int(previous.request.commandIndex) {
			return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "wildcard discovery did not advance"}
		}
		prefixAST, _ := canonical.ParsedPrefix(index)
		prefix, prefixErr := buildRuntimeWildcardPrefix(prefixAST, scope, previous)
		if prefixErr != nil {
			return nil, prefixErr
		}
		sourceDigest, _ := canonical.ParsedSourceDigest()
		var request StatsWildcardRequest
		var requestErr error
		switch command := canonical.Commands[index].(type) {
		case *spl.StatsCommand:
			request, requestErr = newStatsWildcardRequest(sourceDigest, canonical, index, command, prefix, previous)
		case *spl.ConvertCommand:
			request, requestErr = newConvertWildcardRequest(sourceDigest, canonical, index, command, prefix, previous)
		default:
			return nil, err
		}
		if requestErr != nil {
			return nil, requestErr
		}
		prefix.statsWildcardRequestDigest = request.seal
		return &StatsWildcardPreparation{prefix: prefix, request: request}, nil
	}
	return nil, err
}

func buildRuntimeWildcardPrefix(query *spl.Query, scope Scope, expansion *StatsWildcardExpansion) (*Query, error) {
	expanded := *query
	expanded.Commands = slices.Clone(query.Commands)
	if expansion != nil {
		if err := applyRuntimeWildcardEvidence(&expanded, expansion); err != nil {
			return nil, err
		}
	}
	return Build(&expanded, scope)
}

func applyRuntimeWildcardEvidence(query *spl.Query, expansion *StatsWildcardExpansion) error {
	if expansion.request.previous != nil {
		if err := applyRuntimeWildcardEvidence(query, expansion.request.previous); err != nil {
			return err
		}
	}
	index := int(expansion.request.commandIndex)
	if index >= len(query.Commands) {
		return &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "wildcard evidence exceeds prefix"}
	}
	if expansion.request.conversion {
		command, ok := query.Commands[index].(*spl.ConvertCommand)
		if !ok || command == nil {
			return &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "conversion evidence command changed"}
		}
		changed := *command
		changed.Clauses = nil
		for ordinal, clause := range command.Clauses {
			if clause.Function == spl.ConvertFunctionNone {
				continue
			}
			if !spl.IsFieldsFieldGlob(clause.Input) {
				changed.Clauses = append(changed.Clauses, clause)
				continue
			}
			for _, match := range expansion.matches {
				if int(match.Ordinal) != ordinal {
					continue
				}
				exact := clause
				exact.Input = match.Field
				exact.Output = match.Field
				exact.ExplicitOutput = false
				changed.Clauses = append(changed.Clauses, exact)
			}
		}
		if len(changed.Clauses) == 0 {
			changed.Clauses = []spl.ConvertClause{{Function: spl.ConvertFunctionNone, Input: "_raw", Output: "_raw", InputRange: command.Range, OutputRange: command.Range, Range: command.Range}}
		}
		query.Commands[index] = &changed
		return nil
	}
	command, ok := query.Commands[index].(*spl.StatsCommand)
	if !ok || command == nil {
		return &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "aggregate evidence command changed"}
	}
	aggregates, err := expandStatsWildcardInventoryAggregates(command, expansion.matches)
	if err != nil {
		return err
	}
	changed := *command
	changed.Aggregates = aggregates
	query.Commands[index] = &changed
	return nil
}

func convertWildcardMetadata(command *spl.ConvertCommand) ([]StatsWildcardPattern, []string, []StatsWildcardInventoryMatch, uint8, error) {
	exclusions := []string{}
	for _, clause := range command.Clauses {
		if clause.Function == spl.ConvertFunctionNone {
			exclusions = append(exclusions, clause.Input)
		}
	}
	patterns := []StatsWildcardPattern{}
	additions := []StatsWildcardInventoryMatch{}
	aliases := []string{}
	exact := 0
	for ordinal, clause := range command.Clauses {
		if clause.Function == spl.ConvertFunctionNone {
			continue
		}
		if !spl.IsFieldsFieldGlob(clause.Input) {
			exact++
			if !slices.Contains(aliases, clause.Output) {
				aliases = append(aliases, clause.Output)
			}
			continue
		}
		pattern := StatsWildcardPattern{Ordinal: safecast.MustConv[uint8](ordinal), Pattern: clause.Input}
		patterns = append(patterns, pattern)
		for _, alias := range aliases {
			if spl.MatchConvertFieldGlob(clause.Input, alias) && !ConvertFieldExcluded(command, alias) {
				additions = append(additions, StatsWildcardInventoryMatch{Ordinal: pattern.Ordinal, Field: alias})
				if len(additions) > spl.MaximumConvertWrites {
					return nil, nil, nil, 0, convertWriteLimit(command.Range)
				}
			}
		}
	}
	remaining := spl.MaximumConvertWrites - exact
	if remaining < 0 || len(additions) > remaining {
		return nil, nil, nil, 0, convertWriteLimit(command.Range)
	}
	slices.SortFunc(additions, func(a, b StatsWildcardInventoryMatch) int {
		if a.Ordinal != b.Ordinal {
			return int(a.Ordinal) - int(b.Ordinal)
		}
		return strings.Compare(a.Field, b.Field)
	})
	return patterns, exclusions, additions, safecast.MustConv[uint8](remaining + 1), nil
}

func newConvertWildcardRequest(sourceDigest [sha256.Size]byte, query *spl.Query, index int, command *spl.ConvertCommand, prefix *Query, previous *StatsWildcardExpansion) (StatsWildcardRequest, error) {
	source, ok := query.ParsedSource()
	if !ok || index < 0 || index > int(^uint16(0)) {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "convert wildcard source is invalid"}
	}
	patterns, exclusions, additions, maximum, err := convertWildcardMetadata(command)
	if err != nil {
		return StatsWildcardRequest{}, err
	}
	scopeDigest, ok := statsWildcardPlanScopeDigest(prefix)
	if !ok {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "convert wildcard scope is invalid"}
	}
	base, ok := statsWildcardPlanSemanticDigest(prefix)
	if !ok {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "convert wildcard prefix is invalid"}
	}
	request := StatsWildcardRequest{sourceDigest: sourceDigest, scopeDigest: scopeDigest, commandIndex: safecast.MustConv[uint16](index), maximumPairs: maximum, patterns: patterns, authoredSource: source, scope: statsWildcardScopeFromPlan(prefix), basePrefix: base, conversion: true, exclusions: exclusions, additions: additions, previous: previous}
	request.seal = statsWildcardRequestDigest(request)
	if !request.valid() {
		return StatsWildcardRequest{}, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "convert wildcard authority is invalid"}
	}
	return request, nil
}

func validRuntimeWildcardCommand(request StatsWildcardRequest, parsed *spl.Query) bool {
	index := int(request.commandIndex)
	if index >= len(parsed.Commands) {
		return false
	}
	if request.conversion {
		command, ok := parsed.Commands[index].(*spl.ConvertCommand)
		if !ok {
			return false
		}
		patterns, exclusions, additions, maximum, err := convertWildcardMetadata(command)
		return err == nil && request.maximumPairs == maximum && slices.Equal(request.patterns, patterns) && slices.Equal(request.exclusions, exclusions) && slices.Equal(request.additions, additions)
	}
	command, ok := parsed.Commands[index].(*spl.StatsCommand)
	if !ok || len(request.exclusions) != 0 || len(request.additions) != 0 {
		return false
	}
	expected := []StatsWildcardPattern{}
	exact := 0
	for ordinal, aggregate := range command.Aggregates {
		glob := aggregate.InputGlob
		if aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil {
			glob = aggregate.Sparkline.InputGlob
		}
		if glob == nil {
			exact++
			continue
		}
		expected = append(expected, StatsWildcardPattern{Ordinal: safecast.MustConv[uint8](ordinal), Pattern: glob.Pattern})
	}
	return int(request.maximumPairs) == spl.MaximumStatsMeasures-exact+1 && slices.Equal(request.patterns, expected)
}
