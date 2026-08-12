package spl

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestParseStatsOptionsPreservesAuthoredValuesSpecifiedBitsAndRanges(t *testing.T) {
	t.Parallel()

	const source = `| stats ALLNUM=T delim="\t," PARTITIONS=0 count BY host, service dedup_splitvals=F`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	options := command.Options
	if options.Partitions != 0 || !options.PartitionsSpecified ||
		!options.AllNumeric || !options.AllNumericSpecified ||
		options.Delimiter != "\t," || !options.DelimiterSpecified ||
		options.DeduplicateSplitValues || !options.DeduplicateSplitValuesSpecified {
		t.Fatalf("options = %#v", options)
	}
	for _, test := range []struct {
		sourceRange Range
		want        string
	}{
		{options.AllNumericRange, "ALLNUM=T"},
		{options.DelimiterRange, `delim="\t,"`},
		{options.PartitionsRange, "PARTITIONS=0"},
		{options.DeduplicateSplitValuesRange, "dedup_splitvals=F"},
	} {
		if test.sourceRange == (Range{}) {
			t.Fatalf("range for %q is empty", test.want)
		}
		got := source[test.sourceRange.Start.Offset:test.sourceRange.End.Offset]
		if got != test.want {
			t.Errorf("option range = %q, want %q", got, test.want)
		}
	}
	if !slices.Equal(
		[]string{command.GroupBy[0].Name, command.GroupBy[1].Name},
		[]string{"host", "service"},
	) {
		t.Fatalf("group by = %#v", command.GroupBy)
	}
	if got := source[command.Range.Start.Offset:command.Range.End.Offset]; got != strings.TrimPrefix(source, "| ") {
		t.Fatalf("command range = %q, want stats command without pipe", got)
	}
}

func TestParseStatsOptionsDefaultsBareAndEmptyDelimitersAndTerminalNoop(t *testing.T) {
	t.Parallel()

	defaults, err := Parse(`| stats count`)
	if err != nil {
		t.Fatalf("Parse defaults: %v", err)
	}
	if options := defaults.Commands[0].(*StatsCommand).Options; options != (StatsOptions{}) {
		t.Fatalf("default AST options = %#v, want zero authored metadata", options)
	}

	for _, test := range []struct {
		source    string
		delimiter string
		dedup     bool
	}{
		{source: `| stats delim=comma count`, delimiter: "comma"},
		{source: `| stats delim="" count`, delimiter: ""},
		{source: `| stats count dedup_splitvals=true`, dedup: true},
		{source: `| stats count BY host dedup_splitvals=t`, dedup: true},
	} {
		query, parseErr := Parse(test.source)
		if parseErr != nil {
			t.Errorf("Parse(%q): %v", test.source, parseErr)
			continue
		}
		options := query.Commands[0].(*StatsCommand).Options
		if strings.Contains(test.source, "delim=") &&
			(!options.DelimiterSpecified || options.Delimiter != test.delimiter) {
			t.Errorf("Parse(%q) delimiter options = %#v", test.source, options)
		}
		if strings.Contains(test.source, "dedup_splitvals=") &&
			(!options.DeduplicateSplitValuesSpecified || options.DeduplicateSplitValues != test.dedup) {
			t.Errorf("Parse(%q) dedup options = %#v", test.source, options)
		}
	}
}

func TestParseStatsPartitionsPreservesAuthoredUint64BeforePlanClamping(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"0", "1", "100", "101", "999999", "18446744073709551615",
	} {
		query, err := Parse(`| stats partitions=` + value + ` count`)
		if err != nil {
			t.Errorf("Parse(partitions=%s): %v", value, err)
			continue
		}
		options := query.Commands[0].(*StatsCommand).Options
		if !options.PartitionsSpecified ||
			strconv.FormatUint(options.Partitions, 10) != value {
			t.Errorf("partitions=%s lost specified bit", value)
		}
	}
}

func TestParseStatsOptionsRejectsInvalidValuesDuplicatesAndPlacement(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		source  string
		code    string
		message string
	}{
		{name: "negative partitions", source: `| stats partitions=-1 count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "unsigned"},
		{name: "overflow partitions", source: `| stats partitions=18446744073709551616 count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "64-bit"},
		{name: "quoted partitions", source: `| stats partitions="2" count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "invalid allnum", source: `| stats allnum=yes count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "true"},
		{name: "quoted allnum", source: `| stats allnum="true" count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "invalid dedup", source: `| stats count dedup_splitvals=yes`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "true"},
		{name: "quoted dedup", source: `| stats count dedup_splitvals="true"`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "delimiter missing value", source: `| stats delim=`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "bare comma is syntax", source: `| stats delim=, count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "duplicate partitions", source: `| stats partitions=1 partitions=2 count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "only once"},
		{name: "duplicate allnum", source: `| stats allnum=true allnum=false count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "only once"},
		{name: "duplicate delimiter", source: `| stats delim=one delim=two count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "only once"},
		{name: "leading option after aggregate", source: `| stats count allnum=true`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "precede"},
		{name: "dedup before aggregate", source: `| stats dedup_splitvals=true count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "dedup not terminal", source: `| stats count dedup_splitvals=true BY host`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "final"},
		{name: "BY still requires a field", source: `| stats count BY dedup_splitvals=true`, code: "SPL_EXPECTED_FIELD"},
		{name: "unknown leading option", source: `| stats future=true count`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "not supported"},
		{name: "bare partitions needs equal", source: `| stats partitions count`, code: "SPL_EXPECTED_EQUAL"},
		{name: "options need aggregate", source: `| stats partitions=1`, code: "SPL_EXPECTED_AGGREGATE"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
			if test.message != "" && !strings.Contains(diagnostic.Message, test.message) {
				t.Fatalf("message = %q, want substring %q", diagnostic.Message, test.message)
			}
		})
	}
}

func TestStatsOptionSuggestionsRespectLeadingAndTerminalPositions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   []string
	}{
		{source: "| stats par", want: []string{"partitions="}},
		{source: "| stats count ded", want: []string{"dedup_splitvals="}},
		{source: "| stats count BY host ded", want: []string{"dedup_splitvals="}},
	} {
		result := Suggest(test.source, len(test.source), 20)
		if result.Diagnostic != nil {
			t.Fatalf("Suggest(%q): %v", test.source, result.Diagnostic)
		}
		if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, test.want) {
			t.Errorf("Suggest(%q) labels = %v, want %v", test.source, labels, test.want)
		}
	}

	const afterLeading = "| stats partitions=1 "
	leading := Suggest(afterLeading, len(afterLeading), 20)
	if leading.Diagnostic != nil {
		t.Fatalf("Suggest after leading option: %v", leading.Diagnostic)
	}
	if !leading.Context.Allows(SuggestionKindFunction) ||
		!leading.Context.Allows(SuggestionKindKeyword) ||
		!slices.Equal(leading.Context.Keywords, []string{"allnum=", "delim="}) {
		t.Fatalf("leading option context = %#v", leading.Context)
	}

	const emptyBy = "| stats count BY "
	by := Suggest(emptyBy, len(emptyBy), 20)
	if by.Diagnostic != nil {
		t.Fatalf("Suggest empty BY: %v", by.Diagnostic)
	}
	if !by.Context.Allows(SuggestionKindField) ||
		by.Context.Allows(SuggestionKindKeyword) {
		t.Fatalf("empty BY context = %#v", by.Context)
	}
}
