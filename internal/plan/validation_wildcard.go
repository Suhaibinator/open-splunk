package plan

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// ValidateRuntimeWildcardSuffix checks deterministic pipeline semantics after
// unresolved names without reading storage or minting executable expansion
// evidence. Its temporary names never escape through plans or field analysis.
func ValidateRuntimeWildcardSuffix(query *spl.Query, scope Scope) error {
	canonical, ok := query.ParsedCanonicalClone()
	if !ok {
		return &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "validation requires an unmodified parser-owned query"}
	}
	commands := slices.Clone(canonical.Commands)
	stages := 0
	symbolicSchema := false
	for index, candidate := range commands {
		prefix, _ := canonical.ParsedPrefix(index + 1)
		prefix.Commands = slices.Clone(commands[:index+1])
		_, err := Build(prefix, scope)
		var diagnostic *Diagnostic
		unresolved := errors.As(err, &diagnostic) && (diagnostic.Code == "SPL_UNSUPPORTED_CONVERT_WILDCARD" || diagnostic.Code == "SPL_UNSUPPORTED_STATS_WILDCARD")
		deferredEmptyStats := symbolicSchema && errors.As(err, &diagnostic) && diagnostic.Code == "SPL_NO_MATCHING_STATS_FIELDS"
		if err != nil && !unresolved && !deferredEmptyStats {
			return err
		}
		if unresolved {
			stages++
			if stages > MaximumRuntimeWildcardStages {
				return &Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "search exceeds the eight-stage runtime wildcard discovery limit", Range: candidate.SourceRange()}
			}
		}
		switch command := candidate.(type) {
		case *spl.ConvertCommand:
			if unresolved {
				prior, _ := canonical.ParsedPrefix(index)
				prior.Commands = slices.Clone(commands[:index])
				known, knownErr := buildWithRelation(prior, scope, eventfields.CanonicalSPLFieldNames())
				if knownErr != nil {
					return knownErr
				}
				commands[index] = validationConvertCommand(command, known.OutputFields)
			}
		case *spl.StatsCommand:
			wildcard := statsCommandHasWildcard(command)
			if unresolved || deferredEmptyStats {
				changed, changeErr := validationStatsCommand(command)
				if changeErr != nil {
					return changeErr
				}
				commands[index] = changed
			}
			symbolicSchema = wildcard && (unresolved || deferredEmptyStats || symbolicSchema)
		case *spl.TableCommand:
			symbolicSchema = false
		}
		if unresolved || deferredEmptyStats {
			prefix.Commands = slices.Clone(commands[:index+1])
			if _, buildErr := Build(prefix, scope); buildErr != nil {
				return buildErr
			}
		}
	}
	return nil
}

func validationConvertCommand(command *spl.ConvertCommand, upstream []string) *spl.ConvertCommand {
	changed := *command
	changed.Clauses = nil
	// These names retain the original command's time and index rewrite barriers.
	// Other unknown fields can affect values and discovered width, but cannot
	// establish canonical time or physical index authority.
	known := []string{}
	for _, name := range []string{"_time", "index"} {
		if slices.Contains(upstream, name) {
			known = append(known, name)
		}
	}
	for _, clause := range command.Clauses {
		if clause.Function == spl.ConvertFunctionNone {
			continue
		}
		if !spl.IsFieldsFieldGlob(clause.Input) {
			changed.Clauses = append(changed.Clauses, clause)
			if !slices.Contains(known, clause.Output) {
				known = append(known, clause.Output)
			}
			continue
		}
		for _, name := range known {
			if !spl.MatchConvertFieldGlob(clause.Input, name) || ConvertFieldExcluded(command, name) {
				continue
			}
			exact := clause
			exact.Input, exact.Output, exact.ExplicitOutput = name, name, false
			changed.Clauses = append(changed.Clauses, exact)
		}
	}
	if len(changed.Clauses) == 0 {
		changed.Clauses = []spl.ConvertClause{{Function: spl.ConvertFunctionNone, Input: "_raw", Output: "_raw", InputRange: command.Range, OutputRange: command.Range, Range: command.Range}}
	}
	return &changed
}

func statsCommandHasWildcard(command *spl.StatsCommand) bool {
	for _, aggregate := range command.Aggregates {
		if aggregate.InputGlob != nil || (aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil) {
			return true
		}
	}
	return false
}

func validationStatsCommand(command *spl.StatsCommand) (*spl.StatsCommand, error) {
	changed := *command
	changed.Aggregates = nil
	occupied := make(map[string]bool)
	for _, group := range command.GroupBy {
		occupied[group.Name] = true
	}
	for _, aggregate := range command.Aggregates {
		if aggregate.InputGlob == nil && (aggregate.Sparkline == nil || aggregate.Sparkline.InputGlob == nil) {
			occupied[aggregate.Alias] = true
		}
	}
	for _, aggregate := range command.Aggregates {
		glob := aggregate.InputGlob
		if aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil {
			glob = aggregate.Sparkline.InputGlob
		}
		if glob == nil {
			changed.Aggregates = append(changed.Aggregates, aggregate)
			continue
		}
		var expanded spl.StatsAggregate
		found := false
		for attempt := range spl.MaximumStatsMeasures + spl.MaximumStatsGroupFields + 2 {
			replacement := ""
			if attempt == 1 {
				replacement = "x"
			} else if attempt > 1 {
				replacement = fmt.Sprintf("v%d", attempt)
			}
			field := strings.ReplaceAll(glob.Pattern, "*", replacement)
			var valid bool
			if aggregate.Sparkline != nil {
				expanded, valid = spl.ExpandStatsSparklineFieldGlob(aggregate, field)
			} else {
				expanded, valid = spl.ExpandStatsFieldGlob(aggregate, field)
			}
			if valid {
				input, inputRange, quoted := expanded.Input, expanded.InputRange, expanded.InputQuoted
				if expanded.Sparkline != nil {
					input, inputRange, quoted = expanded.Sparkline.Input, expanded.Sparkline.InputRange, expanded.Sparkline.InputQuoted
				}
				_, fieldErr := resolveStatsInputField(input, inputRange, quoted)
				valid = fieldErr == nil
			}
			if valid && !occupied[expanded.Alias] {
				found = true
				break
			}
		}
		if !found {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_STATS_WILDCARD", Message: "stats wildcard cannot be represented for static validation", Range: glob.Range}
		}
		occupied[expanded.Alias] = true
		changed.Aggregates = append(changed.Aggregates, expanded)
	}
	return &changed, nil
}
