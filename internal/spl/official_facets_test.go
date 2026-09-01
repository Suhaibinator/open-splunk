package spl_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/officialspl"
)

// assertOfficialFacets renders the final command's documented option surface
// from the AST and compares it with the corpus expectation. Every facet the
// registry grants the command must be rendered (so a documented option the
// AST cannot express is a failure here, not a silent gap), and every expected
// facet must match its rendering exactly.
func assertOfficialFacets(t *testing.T, testCase officialspl.Case, query *spl.Query) {
	t.Helper()
	command := query.Commands[len(query.Commands)-1]
	got := officialFacets(t, testCase.Query, command)
	gotNames := make([]string, 0, len(got))
	for name := range got {
		gotNames = append(gotNames, name)
	}
	slices.Sort(gotNames)
	if allowed := officialspl.AllowedFacets[testCase.Command]; !slices.Equal(gotNames, allowed) {
		t.Fatalf("rendered facets %v for %s, want the registry's %v", gotNames, testCase.Command, allowed)
	}
	for name, want := range testCase.Expect.Facets {
		if value, ok := got[name]; !ok || value != want {
			t.Errorf("facet %s = %q, want %q", name, value, want)
		}
	}
}

// officialFacets renders one command's option surface as canonical text.
// Field lists join with ", ", aggregates and expressions reproduce their
// authored source text, and absent optional facets render as "".
func officialFacets(t *testing.T, source string, command spl.Command) map[string]string {
	t.Helper()
	text := func(sourceRange spl.Range) string {
		if sourceRange.Start.Offset < 0 || sourceRange.End.Offset < sourceRange.Start.Offset ||
			sourceRange.End.Offset > len(source) {
			t.Fatalf("invalid source range %#v for %d-byte query", sourceRange, len(source))
		}
		return source[sourceRange.Start.Offset:sourceRange.End.Offset]
	}
	number := strconv.FormatUint
	boolean := strconv.FormatBool
	groupBy := func(fields []spl.StatsGroupField) string {
		names := make([]string, len(fields))
		for index, field := range fields {
			names[index] = field.Name
		}
		return strings.Join(names, ", ")
	}
	sortKeys := func(fields []spl.SortField) string {
		keys := make([]string, len(fields))
		for index, field := range fields {
			direction := "+"
			if field.Descending {
				direction = "-"
			}
			key := field.Field
			switch field.Mode {
			case spl.SortValueModeString:
				key = "str(" + key + ")"
			case spl.SortValueModeNumber:
				key = "num(" + key + ")"
			case spl.SortValueModeIP:
				key = "ip(" + key + ")"
			case spl.SortValueModeAuto:
			default:
				t.Fatalf("unexpected sort mode %v", field.Mode)
			}
			keys[index] = direction + key
		}
		return strings.Join(keys, ", ")
	}
	exactFields := func(fields []spl.ExactCommandField) string {
		names := make([]string, len(fields))
		for index, field := range fields {
			names[index] = field.Name
		}
		return strings.Join(names, ", ")
	}

	switch command := command.(type) {
	case *spl.SearchCommand:
		return map[string]string{"filter": text(command.Expression.SourceRange())}
	case *spl.WhereCommand:
		return map[string]string{"predicate": text(command.Expression.SourceRange())}
	case *spl.EvalCommand:
		fields := make([]string, len(command.Assignments))
		expressions := make([]string, len(command.Assignments))
		for index, assignment := range command.Assignments {
			fields[index] = assignment.Field
			expressions[index] = text(assignment.Expression.SourceRange())
		}
		return map[string]string{
			"expressions": strings.Join(expressions, ", "),
			"fields":      strings.Join(fields, ", "),
		}
	case *spl.RexCommand:
		return map[string]string{
			"field":     command.Field,
			"max_match": number(command.MaxMatch, 10),
			"pattern":   command.Pattern,
		}
	case *spl.RegexCommand:
		return map[string]string{
			"field":   command.Field,
			"negated": boolean(command.Negated),
			"pattern": command.Pattern,
		}
	case *spl.SpathCommand:
		return map[string]string{
			"input":  command.Input,
			"output": command.Output,
			"path":   command.Path,
		}
	case *spl.FieldsCommand:
		mode := "include"
		if command.Exclude {
			mode = "exclude"
		}
		return map[string]string{"mode": mode, "names": strings.Join(command.Fields, ", ")}
	case *spl.TableCommand:
		return map[string]string{"fields": strings.Join(command.Fields, ", ")}
	case *spl.RenameCommand:
		assignments := make([]string, len(command.Assignments))
		for index, assignment := range command.Assignments {
			assignments[index] = assignment.Source + " AS " + assignment.Destination
		}
		return map[string]string{"assignments": strings.Join(assignments, ", ")}
	case *spl.SortCommand:
		limit := ""
		if command.LimitSpecified {
			limit = number(command.Limit, 10)
		}
		return map[string]string{"keys": sortKeys(command.Fields), "limit": limit}
	case *spl.DedupCommand:
		names := make([]string, len(command.Fields))
		for index, field := range command.Fields {
			names[index] = field.Name
		}
		return map[string]string{
			"consecutive": boolean(command.Consecutive),
			"count":       number(command.Count, 10),
			"fields":      strings.Join(names, ", "),
			"sortby":      sortKeys(command.SortBy),
		}
	case *spl.LimitCommand:
		return map[string]string{"count": number(command.Count, 10)}
	case *spl.StatsCommand:
		aggregates := make([]string, len(command.Aggregates))
		for index, aggregate := range command.Aggregates {
			aggregates[index] = text(aggregate.Range)
		}
		return map[string]string{
			"aggregates": strings.Join(aggregates, ", "),
			"group_by":   groupBy(command.GroupBy),
		}
	case *spl.EventStatsCommand:
		return map[string]string{
			"aggregate": text(command.Aggregate.Range),
			"group_by":  groupBy(command.GroupBy),
		}
	case *spl.StreamStatsCommand:
		facets := map[string]string{
			"aggregate": text(command.Aggregate.Range),
			"current":   "",
			"global":    "",
			"group_by":  groupBy(command.GroupBy),
			"window":    "",
		}
		if command.CurrentSpecified {
			facets["current"] = boolean(command.Current)
		}
		if command.GlobalSpecified {
			facets["global"] = boolean(command.Global)
		}
		if command.WindowSpecified {
			facets["window"] = number(command.Window, 10)
		}
		return facets
	case *spl.TopCommand:
		return map[string]string{"fields": groupBy(command.Fields), "limit": number(command.Limit, 10)}
	case *spl.RareCommand:
		return map[string]string{"fields": groupBy(command.Fields), "limit": number(command.Limit, 10)}
	case *spl.BinCommand:
		span := number(command.Span.Magnitude, 10)
		switch command.Span.Kind {
		case spl.BinSpanKindTime:
			span += command.Span.Unit.String()
		case spl.BinSpanKindNumeric:
		default:
			t.Fatalf("unexpected bin span kind %v", command.Span.Kind)
		}
		output := ""
		if command.OutputRange != command.FieldRange {
			output = command.Output
		}
		return map[string]string{"field": command.Field, "output": output, "span": span}
	case *spl.TimechartCommand:
		splitBy := ""
		if command.SplitBy != nil {
			splitBy = command.SplitBy.Name
		}
		return map[string]string{
			"aggregate": text(command.Aggregate.Range),
			"span":      number(command.Span.Magnitude, 10) + command.Span.Unit.String(),
			"split_by":  splitBy,
		}
	case *spl.ChartCommand:
		return map[string]string{
			"aggregate": text(command.Aggregate.Range),
			"over":      command.Over.Name,
			"split_by":  command.SplitBy.Name,
		}
	case *spl.ReverseCommand, *spl.AddInfoCommand:
		return map[string]string{}
	case *spl.AccumCommand:
		output := ""
		if command.ExplicitOutput {
			output = command.Output
		}
		return map[string]string{"field": command.Field, "output": output}
	case *spl.StrcatCommand:
		operands := make([]string, len(command.Operands))
		for index, operand := range command.Operands {
			if operand.Literal != nil {
				operands[index] = strconv.Quote(*operand.Literal)
				continue
			}
			operands[index] = operand.Field
		}
		allRequired := ""
		if command.AllRequiredSpecified {
			allRequired = boolean(command.AllRequired)
		}
		return map[string]string{
			"all_required": allRequired,
			"destination":  command.Destination,
			"operands":     strings.Join(operands, ", "),
		}
	case *spl.FillNullCommand:
		return map[string]string{"fields": exactFields(command.Fields), "value": command.Value}
	case *spl.AddTotalsCommand:
		return map[string]string{"fields": exactFields(command.Fields), "output": command.Output}
	case *spl.DeltaCommand:
		output := ""
		if !command.OutputDefault {
			output = command.Output
		}
		return map[string]string{
			"field":    command.Field,
			"output":   output,
			"previous": number(command.Previous, 10),
		}
	case *spl.MakeMVCommand:
		return map[string]string{
			"allow_empty": boolean(command.AllowEmpty),
			"delimiter":   command.Delimiter,
			"field":       command.Field,
		}
	case *spl.MVExpandCommand:
		limit := ""
		if command.LimitSpecified {
			limit = number(command.Limit, 10)
		}
		return map[string]string{"field": command.Field, "limit": limit}
	case *spl.NoMVCommand:
		return map[string]string{"field": command.Field}
	case *spl.LookupCommand:
		keys := make([]string, len(command.Keys))
		for index, key := range command.Keys {
			keys[index] = key.LookupField + " AS " + key.EventField
		}
		outputs := make([]string, len(command.Outputs))
		for index, output := range command.Outputs {
			outputs[index] = output.LookupField + " AS " + output.EventField
		}
		var mode string
		switch command.OutputMode {
		case spl.LookupOutputModeOverwrite:
			mode = "overwrite"
		case spl.LookupOutputModePreserveExisting:
			mode = "preserve"
		default:
			t.Fatalf("unexpected lookup output mode %v", command.OutputMode)
		}
		return map[string]string{
			"definition":  command.DefinitionName,
			"keys":        strings.Join(keys, ", "),
			"output_mode": mode,
			"outputs":     strings.Join(outputs, ", "),
		}
	default:
		t.Fatalf("no facet renderer for %T (%s)", command, command.Name())
		return nil
	}
}

// TestOfficialFacetRenderingCoversEveryRegisteredCommand proves the renderer
// and the registry agree on the whole documented command surface without
// depending on which cases the corpus happens to contain.
func TestOfficialFacetRenderingCoversEveryRegisteredCommand(t *testing.T) {
	t.Parallel()
	corpus := loadOfficialSPLCorpus(t)
	seen := make(map[string]struct{})
	for _, testCase := range corpus.Cases {
		seen[testCase.Command] = struct{}{}
	}
	for command := range officialspl.AllowedFacets {
		if _, ok := seen[command]; !ok {
			t.Errorf("AllowedFacets registers %q but the official corpus has no case for it", command)
		}
	}
}
