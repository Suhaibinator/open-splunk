package plan

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func projectKnownOutputFields(current, requested []string, wildcards []bool, exclude bool) []string {
	matches := func(name string) bool {
		for index, selector := range requested {
			wildcard := spl.IsFieldsFieldGlob(selector)
			if len(wildcards) == len(requested) {
				wildcard = wildcards[index]
			}
			if (!wildcard && selector == name) ||
				(wildcard && spl.MatchFieldsFieldGlob(selector, name)) {
				return true
			}
		}
		return false
	}
	if exclude {
		result := make([]string, 0, len(current))
		for _, name := range current {
			if !matches(name) {
				result = append(result, name)
			}
		}
		return result
	}

	result := make([]string, 0, len(current)+2)
	for index, selector := range requested {
		wildcard := spl.IsFieldsFieldGlob(selector)
		if len(wildcards) == len(requested) {
			wildcard = wildcards[index]
		}
		for _, name := range current {
			selected := (!wildcard && selector == name) ||
				(wildcard && spl.MatchFieldsFieldGlob(selector, name))
			if selected && !slices.Contains(result, name) {
				result = append(result, name)
			}
		}
	}
	for _, implicit := range []string{"_time", "_raw"} {
		if slices.Contains(current, implicit) && !slices.Contains(result, implicit) {
			result = append(result, implicit)
		}
	}
	return result
}

func fieldsCommandSelectsName(command *spl.FieldsCommand, name string) bool {
	if command == nil {
		return false
	}
	for index, selector := range command.Fields {
		wildcard := spl.IsFieldsFieldGlob(selector)
		if len(command.WildcardFields) == len(command.Fields) {
			wildcard = command.WildcardFields[index]
		}
		if (!wildcard && selector == name) ||
			(wildcard && spl.MatchFieldsFieldGlob(selector, name)) {
			return true
		}
	}
	return false
}

func renameKnownOutputFields(current []string, assignments []spl.RenameAssignment) []string {
	result := append([]string(nil), current...)
	for _, assignment := range assignments {
		if !slices.Contains(result, assignment.Source) {
			// Splunk nulls an existing destination when the source is absent.
			// The column therefore remains part of a known result schema.
			continue
		}
		next := make([]string, 0, len(result))
		for _, name := range result {
			switch name {
			case assignment.Source:
				next = append(next, assignment.Destination)
			case assignment.Destination:
				// A present source replaces an existing destination.
			default:
				next = append(next, name)
			}
		}
		result = next
	}
	return result
}

func convertRenameAssignments(command *spl.RenameCommand) ([]RenameAssignment, error) {
	if command == nil || len(command.Assignments) == 0 {
		return nil, &Diagnostic{Code: "SPL_INVALID_RENAME", Message: "rename requires at least one assignment"}
	}
	result := make([]RenameAssignment, 0, len(command.Assignments))
	seenSources := make(map[string]struct{}, len(command.Assignments))
	seenDestinations := make(map[string]struct{}, len(command.Assignments))
	for _, assignment := range command.Assignments {
		if assignment.Source == assignment.Destination {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_RENAME",
				Message: fmt.Sprintf("rename source and destination are both %q", assignment.Source),
				Range:   assignment.Range,
			}
		}
		if _, duplicate := seenSources[assignment.Source]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_SOURCE",
				Message: fmt.Sprintf("rename source field %q is repeated", assignment.Source),
				Range:   assignment.SourceRange,
			}
		}
		if _, duplicate := seenDestinations[assignment.Destination]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_TARGET",
				Message: fmt.Sprintf("rename destination field %q is repeated", assignment.Destination),
				Range:   assignment.DestinationRange,
			}
		}
		source, err := ResolveField(assignment.Source, assignment.SourceRange)
		if err != nil {
			return nil, err
		}
		destination, err := ResolveField(assignment.Destination, assignment.DestinationRange)
		if err != nil {
			return nil, err
		}
		seenSources[assignment.Source] = struct{}{}
		seenDestinations[assignment.Destination] = struct{}{}
		result = append(result, RenameAssignment{
			Source:      source,
			Destination: destination,
			Range:       assignment.Range,
		})
	}
	return result, nil
}

func convertStatsGroupFields(
	commandName string,
	fields []spl.StatsGroupField,
) ([]FieldRef, error) {
	result := make([]FieldRef, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Quoted {
			if commandName != "stats" || !spl.IsStatsLiteralFieldReference(field.Name) {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_FIELD",
					Message: commandName + " grouping field has invalid quoted-field provenance",
					Range:   field.Range,
				}
			}
		} else if commandName == "stats" &&
			!spl.IsExactUnquotedFieldName(field.Name) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message: "stats BY requires exact quoted or unquoted fields; wildcard fields are not supported",
				Range:   field.Range,
			}
		}
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code: "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf(
					"%s grouping field %q is repeated",
					commandName,
					field.Name,
				),
				Range: field.Range,
			}
		}
		seen[field.Name] = struct{}{}
		resolved, err := resolveStatsInputField(field.Name, field.Range, field.Quoted)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

func resolveIndexes(
	scope Scope,
	query *spl.Query,
	rexPatterns map[*spl.RexCommand]splregex.ExtractionPattern,
) ([]string, error) {
	authorized := normalizedSet(scope.AuthorizedIndexes)
	if len(authorized) == 0 {
		return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: "search is not authorized for any index", Range: query.Range}
	}

	effective := authorized
	if len(scope.RequestedIndexes) > 0 {
		effective = make(map[string]struct{}, len(scope.RequestedIndexes))
		for _, requested := range scope.RequestedIndexes {
			name := strings.TrimSpace(requested)
			if _, ok := authorized[name]; !ok {
				return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", name), Range: query.Range}
			}
			effective[name] = struct{}{}
		}
	}

	references := positiveIndexReferences(query, rexPatterns)
	for _, reference := range references {
		if strings.Contains(reference.value, "*") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_INDEX_SELECTOR",
				Message: "wildcard index selectors are not supported",
				Range:   reference.sourceRange,
			}
		}
		if _, ok := authorized[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", reference.value), Range: reference.sourceRange}
		}
		if _, ok := effective[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the requested scope", reference.value), Range: reference.sourceRange}
		}
	}

	indexes := make([]string, 0, len(effective))
	for index := range effective {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return indexes, nil
}

type indexReference struct {
	value       string
	sourceRange spl.Range
}

func positiveIndexReferences(
	query *spl.Query,
	rexPatterns map[*spl.RexCommand]splregex.ExtractionPattern,
) []indexReference {
	var references []indexReference
	collect := func(expression spl.Expr) {
		collectPositiveIndexReferences(expression, false, &references)
	}
	if query.Search != nil {
		collect(query.Search)
	}
	for _, command := range query.Commands {
		switch command := command.(type) {
		case *spl.EvalCommand:
			for _, assignment := range command.Assignments {
				if assignment.Field == "index" {
					return references
				}
			}
		case *spl.RexCommand:
			compiled := rexPatterns[command]
			for _, capture := range compiled.Captures {
				if capture.Name == "index" {
					return references
				}
			}
		case *spl.StrcatCommand:
			if command != nil && command.Destination == "index" {
				return references
			}
		case *spl.AccumCommand:
			if command == nil || command.Output == "index" {
				return references
			}
		case *spl.FillNullCommand:
			if command == nil || slices.ContainsFunc(command.Fields, func(field spl.ExactCommandField) bool {
				return field.Name == "index"
			}) {
				return references
			}
		case *spl.AddTotalsCommand:
			if command == nil || command.Output == "index" {
				return references
			}
		case *spl.DeltaCommand:
			if command == nil || command.Output == "index" {
				return references
			}
		case *spl.MakeMVCommand:
			if command == nil || command.Field == "index" {
				return references
			}
		case *spl.AddInfoCommand:
			// addinfo has fixed outputs and never rewrites physical index scope.
		case *spl.SpathCommand:
			if command != nil && command.Output == "index" {
				return references
			}
		case *spl.LookupCommand:
			if command == nil ||
				(command.OutputMode == spl.LookupOutputModeOverwrite &&
					slices.ContainsFunc(command.Outputs, func(output spl.LookupOutputMapping) bool {
						return output.EventField == "index"
					})) {
				return references
			}
		case *spl.RenameCommand:
			for _, assignment := range command.Assignments {
				if assignment.Source == "index" || assignment.Destination == "index" {
					return references
				}
			}
		case *spl.EventStatsCommand:
			if command == nil || command.Aggregate.Alias == "index" {
				return references
			}
		case *spl.StreamStatsCommand:
			if command == nil || command.Aggregate.Alias == "index" {
				return references
			}
		case *spl.StatsCommand, *spl.TopCommand, *spl.RareCommand, *spl.TimechartCommand, *spl.ChartCommand:
			return references
		}
		if search, ok := command.(*spl.SearchCommand); ok {
			collect(search.Expression)
		}
	}
	return references
}

func compileRexPatterns(query *spl.Query) (map[*spl.RexCommand]splregex.ExtractionPattern, error) {
	patterns := make(map[*spl.RexCommand]splregex.ExtractionPattern)
	for _, command := range query.Commands {
		rex, ok := command.(*spl.RexCommand)
		if !ok {
			continue
		}
		compiled, err := compileRexPattern(rex)
		if err != nil {
			return nil, err
		}
		patterns[rex] = compiled
	}
	return patterns, nil
}

func compileRexPattern(command *spl.RexCommand) (splregex.ExtractionPattern, error) {
	if command == nil {
		return splregex.ExtractionPattern{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "rex command is nil",
		}
	}
	compiled, err := splregex.CompileExtractionPattern(command.Pattern)
	if err == nil {
		return compiled, nil
	}
	code := "SPL_UNSUPPORTED_REGEX"
	message := "rex regular expression is outside the supported named-capture RE2-compatible subset"
	if splregex.IsExtractionComplexityError(err) {
		code = "SPL_QUERY_TOO_COMPLEX"
		message = "rex regular expression exceeds the supported pattern or capture-group limit"
	}
	return splregex.ExtractionPattern{}, &Diagnostic{
		Code:    code,
		Message: message,
		Range:   command.PatternRange,
	}
}

func collectPositiveIndexReferences(expression spl.Expr, negated bool, destination *[]indexReference) {
	switch expression := expression.(type) {
	case *spl.BinaryExpr:
		collectPositiveIndexReferences(expression.Left, negated, destination)
		collectPositiveIndexReferences(expression.Right, negated, destination)
	case *spl.NotExpr:
		collectPositiveIndexReferences(expression.Operand, !negated, destination)
	case *spl.ComparisonExpr:
		if !negated && expression.Field == "index" && expression.Op == spl.CompareOpEqual {
			*destination = append(*destination, indexReference{value: expression.Value.Text, sourceRange: expression.Range})
		}
	}
}

func normalizedSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}
