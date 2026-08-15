package spl

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

// TestChartBreakPipelineAggOptionKeepsAggregateCodeInEveryPosition pins the
// reclassification the contract calls out explicitly: `agg=` names the
// aggregate rather than a rendering option, so it must keep
// SPL_UNSUPPORTED_CHART_AGGREGATE wherever it appears — never the generic
// option code that every other `name=value` token receives.
func TestChartBreakPipelineAggOptionKeepsAggregateCodeInEveryPosition(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{"before the aggregate", `index=main | chart agg=count over path by level`},
		{"between the aggregate and OVER", `index=main | chart count agg=count over path by level`},
		{"in place of the row field", `index=main | chart count over agg=count by level`},
		{"in place of the column field", `index=main | chart count over path by agg=count`},
		{"after a complete OVER form", `index=main | chart count over path by level agg=avg`},
		{"after a complete BY list", `index=main | chart count by path, level agg=avg`},
		{"in place of the first BY field", `index=main | chart count by agg=count, level`},
		{"in place of the second BY field", `index=main | chart count by path, agg=count`},
		{"uppercase spelling", `index=main | chart count over path by level AGG=avg`},
		{"mixed case spelling", `index=main | chart count over path by level Agg=avg`},
		{"empty value", `index=main | chart count over path by level agg=`},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			diagnostic := &Diagnostic{}
			ok := errors.As(err, &diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_AGGREGATE" {
				t.Fatalf("Parse(%q) diagnostic = %#v, want SPL_UNSUPPORTED_CHART_AGGREGATE", test.source, err)
			}
			located := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
			if !strings.EqualFold(located, "agg") {
				t.Fatalf("Parse(%q) located the rejection at %q, want the agg token", test.source, located)
			}
		})
	}

	// An option that is not agg keeps the option code in the same positions,
	// so the two classifications never bleed into each other.
	for _, source := range []string{
		`index=main | chart bins=10 count over path by level`,
		`index=main | chart count over path by level bins=10`,
		`index=main | chart count by path, level bins=10`,
	} {
		diagnostic := &Diagnostic{}
		ok := errors.As(chartBreakPipelineParseError(t, source), &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_OPTION" {
			t.Fatalf("Parse(%q) diagnostic = %#v, want SPL_UNSUPPORTED_CHART_OPTION", source, diagnostic)
		}
	}
}

// TestChartBreakPipelineKeywordShapedFieldNamesAgreeAcrossSpellings walks the
// grammar's own vocabulary as field names. Chart's separator rule is the only
// documented asymmetry between the two spellings, so every other keyword-shaped
// name must produce the same axes through all three separator forms.
func TestChartBreakPipelineKeywordShapedFieldNamesAgreeAcrossSpellings(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		row    string
		column string
	}{
		{"count", "level"},
		{"path", "count"},
		{"by", "level"},
		{"path", "by"},
		{"chart", "stats"},
		{"as", "where"},
		{"limit", "span"},
		{"useother", "usenull"},
		{"null", "other"},
		{"NULL", "OTHER"},
		{"fields", "_raw"},
		{"_time", "_indextime"},
		{"in", "top"},
		{"and", "or"},
		{"not", "like"},
	} {
		test := test
		t.Run(test.row+"/"+test.column, func(t *testing.T) {
			t.Parallel()
			for _, source := range []string{
				`index=main | chart count over ` + test.row + ` by ` + test.column,
				`index=main | chart count by ` + test.row + `, ` + test.column,
				`index=main | chart count by ` + test.row + ` ` + test.column,
			} {
				parsed, err := Parse(source)
				if err != nil {
					t.Fatalf("Parse(%q): %v", source, err)
				}
				command, ok := parsed.Commands[len(parsed.Commands)-1].(*ChartCommand)
				if !ok || command.Over.Name != test.row || command.SplitBy.Name != test.column {
					t.Fatalf("Parse(%q) axes = %#v, want %q/%q", source, parsed.Commands[0], test.row, test.column)
				}
			}
		})
	}
}

// TestChartBreakPipelineOverIsTheOnlySeparatorAsymmetry pins the exact boundary
// the contract documents: a field named `over` is charted by both spellings,
// but only the comma-separated BY list can carry it in the column position,
// because the whitespace form is indistinguishable from BY-before-OVER.
func TestChartBreakPipelineOverIsTheOnlySeparatorAsymmetry(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | chart count over over by level`,
		`index=main | chart count by over, level`,
		`index=main | chart count by over level`,
	} {
		parsed, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command := parsed.Commands[0].(*ChartCommand)
		if command.Over.Name != "over" || command.SplitBy.Name != "level" {
			t.Fatalf("Parse(%q) axes = %#v", source, command)
		}
	}

	for _, source := range []string{
		`index=main | chart count over level by over`,
		`index=main | chart count by level, over`,
	} {
		parsed, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command := parsed.Commands[0].(*ChartCommand)
		if command.Over.Name != "level" || command.SplitBy.Name != "over" {
			t.Fatalf("Parse(%q) axes = %#v", source, command)
		}
	}

	// The one spelling the contract reserves for the rejected BY-before-OVER
	// form, and the doubled keyword that cannot be disambiguated either.
	for _, source := range []string{
		`index=main | chart count by level over`,
		`index=main | chart count by over over`,
	} {
		diagnostic := &Diagnostic{}
		ok := errors.As(chartBreakPipelineParseError(t, source), &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_SYNTAX" {
			t.Fatalf("Parse(%q) diagnostic = %#v, want SPL_UNSUPPORTED_CHART_SYNTAX", source, diagnostic)
		}
		if located := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; located != "over" {
			t.Fatalf("Parse(%q) located the rejection at %q, want the misplaced OVER", source, located)
		}
	}
}

// TestChartBreakPipelineParsesAfterEverySupportedCommandFamily proves the pivot
// is an ordinary terminal stage of the grammar: no upstream command changes how
// the chart clause itself is tokenized, and chart is always exactly one command.
func TestChartBreakPipelineParsesAfterEverySupportedCommandFamily(t *testing.T) {
	t.Parallel()

	upstreams := []string{
		``,
		` | search status>=500`,
		` | where severity>3`,
		` | eval area="x"`,
		` | eval n=null`,
		` | rex field=path "^/api/(?<area>[^/]+)"`,
		` | rename level AS lvl`,
		` | fields path level`,
		` | table path level`,
		` | sort 0 +path`,
		` | dedup 2 path, level`,
		` | head 100`,
		` | tail 100`,
		` | stats count AS hits BY path, level`,
		` | top limit=5 message`,
		` | rare limit=5 message`,
		` | bin severity span=10 AS band`,
		` | bucket _time span=5m AS bucket_time`,
	}
	for _, upstream := range upstreams {
		source := `index=main` + upstream + ` | chart count OVER path BY level`
		parsed, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command, ok := parsed.Commands[len(parsed.Commands)-1].(*ChartCommand)
		if !ok {
			t.Fatalf("Parse(%q) last command = %T, want *ChartCommand", source, parsed.Commands[len(parsed.Commands)-1])
		}
		if command.Over.Name != "path" || command.SplitBy.Name != "level" || !command.OverSpelledOver {
			t.Fatalf("Parse(%q) chart = %#v", source, command)
		}
		if text := source[command.Range.Start.Offset:command.Range.End.Offset]; text != "chart count OVER path BY level" {
			t.Fatalf("Parse(%q) chart source range covers %q", source, text)
		}
	}
}

// TestChartBreakPipelineSurvivesHostileLayout pins that the pivot's grammar is
// decided by tokens rather than by spacing, and that a leading pipe with no
// base search is still a complete query.
func TestChartBreakPipelineSurvivesHostileLayout(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`| chart count OVER path BY level`,
		`|chart count OVER path BY level`,
		"index=main\n| chart\n  count\n  OVER path\n  BY level",
		"index=main |    chart     count     OVER     path     BY     level   ",
		"index=main\t|\tchart\tcount\tOVER\tpath\tBY\tlevel",
		`index=main | chart count BY path,level`,
		`index=main | chart count BY path ,level`,
		`index=main | chart count BY path , level`,
	} {
		parsed, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command, ok := parsed.Commands[len(parsed.Commands)-1].(*ChartCommand)
		if !ok || command.Over.Name != "path" || command.SplitBy.Name != "level" {
			t.Fatalf("Parse(%q) chart = %#v", source, parsed.Commands[len(parsed.Commands)-1])
		}
	}

	// A trailing pipe after a complete chart is a missing command, not a chart
	// diagnostic: chart's own clause already closed.
	diagnostic := &Diagnostic{}
	ok := errors.As(chartBreakPipelineParseError(t, `index=main | chart count OVER path BY level |`), &diagnostic)
	if !ok || diagnostic.Code != "SPL_EXPECTED_COMMAND" {
		t.Fatalf("trailing pipe diagnostic = %#v, want SPL_EXPECTED_COMMAND", diagnostic)
	}
}

// TestChartBreakPipelineQuotedAndNonWordAggregates keeps the aggregate slot
// closed to quoted, non-word, and unknown function tokens.
func TestChartBreakPipelineQuotedAndNonWordAggregates(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | chart "count" over path by level`,
		`index=main | chart 5 over path by level`,
		`index=main | chart , count over path by level`,
		`index=main | chart counter over path by level`,
		`index=main | chart countx over path by level`,
	} {
		diagnostic := &Diagnostic{}
		ok := errors.As(chartBreakPipelineParseError(t, source), &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_AGGREGATE" {
			t.Fatalf("Parse(%q) diagnostic = %#v, want SPL_UNSUPPORTED_CHART_AGGREGATE", source, diagnostic)
		}
	}
}

// TestChartBreakPipelineBudgetsCountChartAsOneOrdinaryStage pins that the
// terminal pivot neither gets a free pipeline stage nor blocks a query that
// already fits inside the parser's command and source-byte budgets.
func TestChartBreakPipelineBudgetsCountChartAsOneOrdinaryStage(t *testing.T) {
	t.Parallel()

	build := func(stages int) string {
		var source strings.Builder
		source.WriteString("index=main")
		for index := range stages {
			source.WriteString(" | eval f")
			source.WriteString(strconv.Itoa(index))
			source.WriteString("=1")
		}
		source.WriteString(" | chart count OVER path BY level")
		return source.String()
	}

	parsed, err := Parse(build(maxPipelineCommands - 1))
	if err != nil {
		t.Fatalf("Parse(%d commands ending in chart): %v", maxPipelineCommands, err)
	}
	if len(parsed.Commands) != maxPipelineCommands {
		t.Fatalf("command count = %d, want %d", len(parsed.Commands), maxPipelineCommands)
	}
	if _, ok := parsed.Commands[len(parsed.Commands)-1].(*ChartCommand); !ok {
		t.Fatal("the budget-filling pipeline lost its terminal chart")
	}

	diagnostic := &Diagnostic{}
	ok := errors.As(chartBreakPipelineParseError(t, build(maxPipelineCommands)), &diagnostic)
	if !ok || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("over-budget diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", diagnostic)
	}

	tail := " | chart count OVER path BY level"
	fits := "index=main" + strings.Repeat(" ", maxSPLSourceBytes-len("index=main")-len(tail)) + tail
	if len(fits) != maxSPLSourceBytes {
		t.Fatalf("padded source is %d bytes, want exactly %d", len(fits), maxSPLSourceBytes)
	}
	if _, err := Parse(fits); err != nil {
		t.Fatalf("Parse(exactly %d source bytes ending in chart): %v", maxSPLSourceBytes, err)
	}
	diagnostic = &Diagnostic{}
	ok = errors.As(chartBreakPipelineParseError(t, fits+" "), &diagnostic)
	if !ok || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("oversized-source diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", diagnostic)
	}
}

func chartBreakPipelineParseError(t *testing.T, source string) error {
	t.Helper()
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded", source)
	}
	return err
}
