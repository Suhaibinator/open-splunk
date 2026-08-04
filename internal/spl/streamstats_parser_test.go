package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseStreamStatsCountPreservesOptionsAliasGroupsAndRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                                   string
		source                                 string
		alias                                  string
		explicitAlias                          bool
		current                                bool
		currentSpecified                       bool
		window                                 uint64
		windowSpecified                        bool
		global                                 bool
		globalSpecified                        bool
		groups                                 []string
		currentRange, windowRange, globalRange string
	}{
		{
			name:    "defaults",
			source:  `index=main | streamstats count`,
			alias:   "count",
			current: true,
			global:  true,
		},
		{
			name:             "options before aggregate",
			source:           `index=main | StReAmStAtS current=f window=7 global=false CoUnT AS prior BY host, source`,
			alias:            "prior",
			explicitAlias:    true,
			current:          false,
			currentSpecified: true,
			window:           7,
			windowSpecified:  true,
			global:           false,
			globalSpecified:  true,
			groups:           []string{"host", "source"},
			currentRange:     "current=f",
			windowRange:      "window=7",
			globalRange:      "global=false",
		},
		{
			name:             "options after BY",
			source:           `index=main | streamstats count AS running BY service current=true window=0 global=t`,
			alias:            "running",
			explicitAlias:    true,
			current:          true,
			currentSpecified: true,
			window:           0,
			windowSpecified:  true,
			global:           true,
			globalSpecified:  true,
			groups:           []string{"service"},
			currentRange:     "current=true",
			windowRange:      "window=0",
			globalRange:      "global=t",
		},
		{
			name:             "options between aggregate and alias",
			source:           `index=main | streamstats count current=false window=10000 global=f AS preceding`,
			alias:            "preceding",
			explicitAlias:    true,
			current:          false,
			currentSpecified: true,
			window:           MaximumStreamStatsWindow,
			windowSpecified:  true,
			global:           false,
			globalSpecified:  true,
			currentRange:     "current=false",
			windowRange:      "window=10000",
			globalRange:      "global=f",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(query.Commands) != 1 {
				t.Fatalf("command count = %d, want 1", len(query.Commands))
			}
			command, ok := query.Commands[0].(*StreamStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
			}
			if command.Name() != "streamstats" || command.SourceRange() != command.Range {
				t.Fatalf("command identity/range = %q/%#v/%#v", command.Name(), command.SourceRange(), command.Range)
			}
			if command.Aggregate.Function != AggregateFunctionCount ||
				command.Aggregate.Input != "" ||
				command.Aggregate.InputRange != (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias {
				t.Fatalf("aggregate = %#v", command.Aggregate)
			}
			if command.Current != test.current ||
				command.CurrentSpecified != test.currentSpecified ||
				command.Window != test.window ||
				command.WindowSpecified != test.windowSpecified ||
				command.Global != test.global ||
				command.GlobalSpecified != test.globalSpecified {
				t.Fatalf("options = current %t/%t, window %d/%t, global %t/%t",
					command.Current, command.CurrentSpecified,
					command.Window, command.WindowSpecified,
					command.Global, command.GlobalSpecified)
			}
			if len(command.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", command.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if command.GroupBy[index].Name != want || command.GroupBy[index].Range == (Range{}) {
					t.Fatalf("group %d = %#v, want %q with range", index, command.GroupBy[index], want)
				}
			}
			commandStart := strings.Index(strings.ToLower(test.source), "streamstats")
			if commandStart < 0 {
				t.Fatalf("test source has no streamstats command: %q", test.source)
			}
			assertSourceRangeText(t, test.source, command.Range, test.source[commandStart:])
			if test.explicitAlias {
				assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.alias)
			}
			for _, option := range []struct {
				sourceText  string
				sourceRange Range
			}{
				{test.currentRange, command.CurrentRange},
				{test.windowRange, command.WindowRange},
				{test.globalRange, command.GlobalRange},
			} {
				sourceText, sourceRange := option.sourceText, option.sourceRange
				if sourceText == "" {
					if sourceRange != (Range{}) {
						t.Fatalf("unspecified option range = %#v, want zero", sourceRange)
					}
					continue
				}
				assertSourceRangeText(t, test.source, sourceRange, sourceText)
			}
		})
	}
}

func TestParseStreamStatsCountField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, source, input, alias string
		explicitAlias              bool
		current                    bool
		window                     uint64
		global                     bool
		groups                     []string
	}{
		{
			name:    "default output",
			source:  `index=main | streamstats count(status)`,
			input:   "status",
			alias:   "count(status)",
			current: true,
			global:  true,
		},
		{
			name:          "explicit output and bounded group window",
			source:        `index=main | streamstats current=f window=3 global=f count(payload.items) AS populated BY service`,
			input:         "payload.items",
			alias:         "populated",
			explicitAlias: true,
			window:        3,
			groups:        []string{"service"},
		},
		{
			name:   "trailing options",
			source: `index=main | streamstats count(tags) BY service current=false window=0 global=t`,
			input:  "tags",
			alias:  "count(tags)",
			global: true,
			groups: []string{"service"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*StreamStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionCountValues ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias ||
				command.Current != test.current ||
				command.Window != test.window ||
				command.Global != test.global {
				t.Fatalf("streamstats count(field) command = %#v", command)
			}
			if len(command.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", command.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group %d = %#v, want %q", index, command.GroupBy[index], want)
				}
			}
			assertSourceRangeText(t, test.source, command.Aggregate.InputRange, test.input)
			if test.explicitAlias {
				assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.alias)
			} else {
				assertSourceRangeText(
					t,
					test.source,
					command.Aggregate.AliasRange,
					"count("+test.input+")",
				)
			}
		})
	}
}

func TestParseStreamStatsSumField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, source, input, alias string
		explicitAlias              bool
		current                    bool
		window                     uint64
		global                     bool
		groups                     []string
	}{
		{
			name:    "default output preserves input spelling",
			source:  `index=main | streamstats SuM(Payload.Bytes)`,
			input:   "Payload.Bytes",
			alias:   "sum(Payload.Bytes)",
			current: true,
			global:  true,
		},
		{
			name:          "explicit output and bounded group window",
			source:        `index=main | streamstats current=f window=3 global=f sum(payload.bytes) AS prior_bytes BY service`,
			input:         "payload.bytes",
			alias:         "prior_bytes",
			explicitAlias: true,
			window:        3,
			groups:        []string{"service"},
		},
		{
			name:   "trailing options",
			source: `index=main | streamstats sum(bytes) BY service current=false window=0 global=t`,
			input:  "bytes",
			alias:  "sum(bytes)",
			global: true,
			groups: []string{"service"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*StreamStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionSum ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias ||
				command.Current != test.current ||
				command.Window != test.window ||
				command.Global != test.global {
				t.Fatalf("streamstats sum(field) command = %#v", command)
			}
			if len(command.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", command.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group %d = %#v, want %q", index, command.GroupBy[index], want)
				}
			}
			assertSourceRangeText(t, test.source, command.Aggregate.InputRange, test.input)
			if test.explicitAlias {
				assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.alias)
			} else {
				functionStart := strings.Index(strings.ToLower(test.source), "sum(")
				assertSourceRangeText(
					t,
					test.source,
					command.Aggregate.AliasRange,
					test.source[functionStart:functionStart+len("sum(")+len(test.input)+1],
				)
			}
		})
	}
}

func TestParseStreamStatsAverageField(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, input, alias string
		explicitAlias              bool
		current                    bool
		window                     uint64
		global                     bool
		groups                     []string
	}{
		{
			name:    "canonical default preserves input spelling",
			source:  `index=main | streamstats AvG(Payload.Bytes)`,
			input:   "Payload.Bytes",
			alias:   "avg(Payload.Bytes)",
			current: true,
			global:  true,
		},
		{
			name:          "prior bounded group window",
			source:        `index=main | streamstats current=f window=3 global=f avg(payload.bytes) AS prior_mean BY service`,
			input:         "payload.bytes",
			alias:         "prior_mean",
			explicitAlias: true,
			window:        3,
			groups:        []string{"service"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*StreamStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionAverage ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias ||
				command.Current != test.current ||
				command.Window != test.window ||
				command.Global != test.global {
				t.Fatalf("streamstats avg(field) command = %#v", command)
			}
			if len(command.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", command.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group %d = %#v, want %q", index, command.GroupBy[index], want)
				}
			}
			assertSourceRangeText(t, test.source, command.Aggregate.InputRange, test.input)
			if test.explicitAlias {
				assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.alias)
			} else {
				functionStart := strings.Index(strings.ToLower(test.source), "avg(")
				assertSourceRangeText(
					t,
					test.source,
					command.Aggregate.AliasRange,
					test.source[functionStart:functionStart+len("avg(")+len(test.input)+1],
				)
			}
		})
	}
}

func TestExactUnquotedFieldNameMatchesParserTokenBoundary(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"status",
		"Payload.Items",
		`literal\.dot`,
		"field-name",
	} {
		if !IsExactUnquotedFieldName(name) {
			t.Fatalf("IsExactUnquotedFieldName(%q) = false", name)
		}
	}
	for _, name := range []string{
		"",
		"status host",
		"status\thost",
		"status\nhost",
		"status\u00a0host",
		"status\u2003host",
		"status\u3000host",
		"status,host",
		"status(host)",
		"status|host",
		"status=host",
		`"status"`,
		"'status'",
		"`status`",
		"status*",
	} {
		if IsExactUnquotedFieldName(name) {
			t.Fatalf("IsExactUnquotedFieldName(%q) = true", name)
		}
	}
}

func TestParseStreamStatsAcceptsEveryBooleanSpelling(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		value string
		want  bool
	}{
		{value: "t", want: true},
		{value: "TrUe", want: true},
		{value: "f", want: false},
		{value: "FaLsE", want: false},
	} {
		test := test
		t.Run(test.value, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(`index=main | streamstats current=` + test.value + ` count`)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command := query.Commands[0].(*StreamStatsCommand)
			if command.Current != test.want || !command.CurrentSpecified {
				t.Fatalf("current = %t/%t, want %t/specified", command.Current, command.CurrentSpecified, test.want)
			}
		})
	}
}

func TestParseStreamStatsRejectsUnsupportedSurfaceAtSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, source, code, rangeText string
	}{
		{"empty call", `index=main | streamstats count()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "count"},
		{"eval count", `index=main | streamstats count(eval(status=500))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "count"},
		{"abbreviated field count", `index=main | streamstats c(status)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "c"},
		{"wildcard count input", `index=main | streamstats count(status*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "status*"},
		{"quoted count input", `index=main | streamstats count("status")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"status"`},
		{"single quoted count input", `index=main | streamstats count('status')`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "'status'"},
		{"backtick quoted count input", "index=main | streamstats count(`status`)", "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "`status`"},
		{"multiple count inputs", `index=main | streamstats count(status, host)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing count close", `index=main | streamstats count(status`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"bare sum", `index=main | streamstats sum`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "sum"},
		{"empty sum", `index=main | streamstats sum()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "sum"},
		{"eval sum", `index=main | streamstats sum(eval(bytes>0))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "sum"},
		{"abbreviated sum", `index=main | streamstats s(bytes)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "s"},
		{"wildcard sum input", `index=main | streamstats sum(bytes*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "bytes*"},
		{"quoted sum input", `index=main | streamstats sum("bytes")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"bytes"`},
		{"single quoted sum input", `index=main | streamstats sum('bytes')`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "'bytes'"},
		{"backtick quoted sum input", "index=main | streamstats sum(`bytes`)", "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "`bytes`"},
		{"multiple sum inputs", `index=main | streamstats sum(bytes, duration)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing sum close", `index=main | streamstats sum(bytes`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"bare average", `index=main | streamstats avg`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "avg"},
		{"empty average", `index=main | streamstats avg()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "avg"},
		{"eval average", `index=main | streamstats avg(eval(bytes>0))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "avg"},
		{"wildcard average input", `index=main | streamstats avg(bytes*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "bytes*"},
		{"quoted average input", `index=main | streamstats avg("bytes")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"bytes"`},
		{"multiple average inputs", `index=main | streamstats avg(bytes, duration)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing average close", `index=main | streamstats avg(bytes`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"average long name", `index=main | streamstats average(bytes)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "average"},
		{"mean synonym", `index=main | streamstats mean(bytes)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "mean"},
		{"other aggregate", `index=main | streamstats max(bytes)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "max"},
		{"comma second aggregate", `index=main | streamstats count, count`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"space second aggregate", `index=main | streamstats count count`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "count"},
		{"allnum", `index=main | streamstats allnum=false count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "allnum"},
		{"time window", `index=main | streamstats count time_window=5m`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "time_window"},
		{"reset before", `index=main | streamstats reset_before=x count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "reset_before"},
		{"reset after", `index=main | streamstats count reset_after=x`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "reset_after"},
		{"reset on change", `index=main | streamstats reset_on_change=false count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "reset_on_change"},
		{"unknown option", `index=main | streamstats partitions=2 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "partitions"},
		{"duplicate current", `index=main | streamstats current=t count current=f`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "current"},
		{"duplicate window", `index=main | streamstats window=1 count window=2`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "window"},
		{"duplicate global", `index=main | streamstats global=f count global=t`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "global"},
		{"case folded duplicate current after BY", `index=main | streamstats current=t count BY host CURRENT=f`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "CURRENT"},
		{"case folded duplicate window after BY", `index=main | streamstats count BY host window=1 WINDOW=2`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "WINDOW"},
		{"bad current", `index=main | streamstats current=yes count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "yes"},
		{"bad global", `index=main | streamstats global=1 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "1"},
		{"negative window", `index=main | streamstats window=-1 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "-1"},
		{"negative zero window", `index=main | streamstats window=-0 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "-0"},
		{"positive sign window", `index=main | streamstats window=+1 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "+1"},
		{"fractional window", `index=main | streamstats window=1.5 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "1.5"},
		{"over window", `index=main | streamstats window=10001 count`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "10001"},
		{"wildcard alias", `index=main | streamstats count AS rank*`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "rank*"},
		{"quoted alias", `index=main | streamstats count AS "rank"`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"rank"`},
		{"single quoted alias", `index=main | streamstats count AS 'rank'`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `'rank'`},
		{"backtick quoted alias", "index=main | streamstats count AS `rank`", "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "`rank`"},
		{"wildcard group", `index=main | streamstats count BY host*`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "host*"},
		{"quoted group", `index=main | streamstats count BY "host"`, "SPL_EXPECTED_FIELD", `"host"`},
		{"single quoted group", `index=main | streamstats count BY 'host'`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `'host'`},
		{"backtick quoted group", "index=main | streamstats count BY `host`", "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "`host`"},
		{"duplicate group", `index=main | streamstats count BY host, host`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "host"},
		{"alias after BY", `index=main | streamstats count BY host AS rank`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "AS"},
		{"missing explicit global false", `index=main | streamstats window=2 count BY host`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "window=2"},
		{"positive grouped global true", `index=main | streamstats count BY host window=2 global=true`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "global=true"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("code = %q, want %q (%v)", diagnostic.Code, test.code, diagnostic)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
			if test.code == "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE" ||
				test.code == "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX" {
				wantSuggestion := streamStatsSyntaxSuggestion
				if test.name == "missing explicit global false" ||
					test.name == "positive grouped global true" {
					wantSuggestion = "streamstats window=5 global=false count BY group"
				}
				if strings.Join(diagnostic.Suggestions, "\x00") != wantSuggestion {
					t.Fatalf("suggestions = %q, want [%q]", diagnostic.Suggestions, wantSuggestion)
				}
			}
		})
	}
}

func TestParseStreamStatsBoundsGroupingFields(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=main | streamstats count BY ")
	for index := 0; index <= MaximumStatsGroupFields; index++ {
		if index > 0 {
			source.WriteString(", ")
		}
		fmt.Fprintf(&source, "field%d", index)
	}
	_, err := Parse(source.String())
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	assertSourceRangeText(t, source.String(), diagnostic.Range, fmt.Sprintf("field%d", MaximumStatsGroupFields))
}

func TestClassifyResultShapeTreatsStreamStatsAsRowPreserving(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   ResultShape
	}{
		{source: `index=main | streamstats count`, want: ResultShape{Kind: ResultKindEvents}},
		{source: `index=main | streamstats count(status)`, want: ResultShape{Kind: ResultKindEvents}},
		{source: `index=main | streamstats sum(bytes)`, want: ResultShape{Kind: ResultKindEvents}},
		{source: `index=main | streamstats min(bytes)`, want: ResultShape{Kind: ResultKindEvents}},
		{source: `index=main | stats count BY host | streamstats count AS groups`, want: ResultShape{Kind: ResultKindStatistics}},
		{source: `index=main | stats count BY host | streamstats count(count) AS populated`, want: ResultShape{Kind: ResultKindStatistics}},
		{source: `index=main | stats sum(bytes) AS bytes BY host | streamstats sum(bytes) AS running`, want: ResultShape{Kind: ResultKindStatistics}},
		{
			source: `index=main | timechart span=5m count BY level | streamstats count AS buckets`,
			want:   ResultShape{Kind: ResultKindTimeSeries, RuntimeNamedColumns: true},
		},
	} {
		query, err := Parse(test.source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.source, err)
		}
		if got := ClassifyResultShape(query); got != test.want {
			t.Fatalf("ClassifyResultShape(%q) = %#v, want %#v", test.source, got, test.want)
		}
	}

	var typedNil *StreamStatsCommand
	if got := ClassifyResultShape(&Query{Commands: []Command{typedNil}}); got != (ResultShape{}) {
		t.Fatalf("typed-nil streamstats shape = %#v, want invalid", got)
	}
}
