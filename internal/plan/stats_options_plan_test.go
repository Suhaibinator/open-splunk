package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsOptionsResolvesEffectiveDefaultsAndAuthoredValues(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		partitions uint8
		allNumeric bool
		delimiter  string
		dedup      bool
		groups     int
	}{
		{
			name:       "defaults",
			source:     `index=gradethis | stats count`,
			partitions: spl.DefaultStatsPartitions,
			delimiter:  spl.DefaultStatsDelimiter,
		},
		{
			name:       "authored maximum and empty delimiter",
			source:     `index=gradethis | stats partitions=100 allnum=true delim="" count BY host dedup_splitvals=true`,
			partitions: spl.MaximumStatsPartitions,
			allNumeric: true,
			delimiter:  "",
			dedup:      true,
			groups:     1,
		},
		{
			name:       "authored over-limit value clamps",
			source:     `index=gradethis | stats partitions=18446744073709551615 count`,
			partitions: spl.MaximumStatsPartitions,
			delimiter:  spl.DefaultStatsDelimiter,
		},
		{
			name:       "authored zero resolves default",
			source:     `index=gradethis | stats partitions=0 delim=comma count dedup_splitvals=t`,
			partitions: spl.DefaultStatsPartitions,
			delimiter:  "comma",
			dedup:      true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
			if aggregate.StatsOptions == nil ||
				aggregate.StatsOptions.Partitions != test.partitions ||
				aggregate.StatsOptions.AllNumeric != test.allNumeric ||
				aggregate.StatsOptions.Delimiter != test.delimiter ||
				aggregate.StatsOptions.DeduplicateSplitValues != test.dedup ||
				len(aggregate.GroupBy) != test.groups {
				t.Fatalf("aggregate options = %#v, groups = %#v", aggregate.StatsOptions, aggregate.GroupBy)
			}
			if _, analysisErr := Analyze(logical); analysisErr != nil {
				t.Fatalf("Analyze: %v", analysisErr)
			}
		})
	}
}

func TestBuildStatsOptionsRejectsForgedASTMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	validCommand := func() *spl.StatsCommand {
		return &spl.StatsCommand{
			Aggregates: []spl.StatsAggregate{{
				Function:   spl.AggregateFunctionCount,
				Alias:      "count",
				Range:      sourceRange,
				AliasRange: sourceRange,
			}},
			Range: sourceRange,
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*spl.StatsOptions)
	}{
		{name: "partitions value without specified bit", mutate: func(options *spl.StatsOptions) { options.Partitions = 2 }},
		{name: "partitions range without specified bit", mutate: func(options *spl.StatsOptions) { options.PartitionsRange = sourceRange }},
		{name: "partitions specified without range", mutate: func(options *spl.StatsOptions) { options.PartitionsSpecified = true }},
		{name: "allnum value without specified bit", mutate: func(options *spl.StatsOptions) { options.AllNumeric = true }},
		{name: "allnum specified without range", mutate: func(options *spl.StatsOptions) { options.AllNumericSpecified = true }},
		{name: "delimiter without specified bit", mutate: func(options *spl.StatsOptions) { options.Delimiter = "," }},
		{name: "delimiter specified without range", mutate: func(options *spl.StatsOptions) { options.DelimiterSpecified = true }},
		{name: "invalid UTF-8 delimiter", mutate: func(options *spl.StatsOptions) {
			options.Delimiter = string([]byte{0xff})
			options.DelimiterSpecified = true
			options.DelimiterRange = sourceRange
		}},
		{name: "dedup value without specified bit", mutate: func(options *spl.StatsOptions) { options.DeduplicateSplitValues = true }},
		{name: "dedup specified without range", mutate: func(options *spl.StatsOptions) { options.DeduplicateSplitValuesSpecified = true }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			command := validCommand()
			test.mutate(&command.Options)
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_SYNTAX")
		})
	}
}

func TestBuildStatsOptionsClampsForgedAuthoredPartitions(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	command := &spl.StatsCommand{
		Aggregates: []spl.StatsAggregate{{
			Function:   spl.AggregateFunctionCount,
			Alias:      "count",
			Range:      sourceRange,
			AliasRange: sourceRange,
		}},
		Options: spl.StatsOptions{
			Partitions:          ^uint64(0),
			PartitionsSpecified: true,
			PartitionsRange:     sourceRange,
		},
		Range: sourceRange,
	}
	logical, err := Build(
		&spl.Query{Search: base.Search, Commands: []spl.Command{command}, Range: base.Range},
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	options := logical.Operators[len(logical.Operators)-1].(*Aggregate).StatsOptions
	if options == nil || options.Partitions != spl.MaximumStatsPartitions {
		t.Fatalf("effective options = %#v", options)
	}
}

func TestBuildStatsOptionsAcceptsExplicitFalseZeroAndEmptyMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	command := &spl.StatsCommand{
		Aggregates: []spl.StatsAggregate{{
			Function:   spl.AggregateFunctionCount,
			Alias:      "count",
			Range:      sourceRange,
			AliasRange: sourceRange,
		}},
		Options: spl.StatsOptions{
			PartitionsSpecified:             true,
			PartitionsRange:                 sourceRange,
			AllNumericSpecified:             true,
			AllNumericRange:                 sourceRange,
			DelimiterSpecified:              true,
			DelimiterRange:                  sourceRange,
			DeduplicateSplitValuesSpecified: true,
			DeduplicateSplitValuesRange:     sourceRange,
		},
		Range: sourceRange,
	}
	logical, err := Build(
		&spl.Query{Search: base.Search, Commands: []spl.Command{command}, Range: base.Range},
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	options := logical.Operators[len(logical.Operators)-1].(*Aggregate).StatsOptions
	if options == nil || options.Partitions != spl.DefaultStatsPartitions ||
		options.AllNumeric || options.Delimiter != "" || options.DeduplicateSplitValues {
		t.Fatalf("effective options = %#v", options)
	}
}

func TestAnalyzeStatsOptionsRejectsForgedEffectiveMetadata(t *testing.T) {
	t.Parallel()

	valid := func() *Aggregate {
		return &Aggregate{
			Measures: []AggregateMeasure{{
				Function: AggregateFunctionCountRows,
				Output:   "count",
			}},
			StatsOptions: &StatsOptions{
				Partitions: spl.DefaultStatsPartitions,
				Delimiter:  spl.DefaultStatsDelimiter,
			},
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*StatsOptions)
	}{
		{name: "zero partitions", mutate: func(options *StatsOptions) { options.Partitions = 0 }},
		{name: "over partitions maximum", mutate: func(options *StatsOptions) { options.Partitions = spl.MaximumStatsPartitions + 1 }},
		{name: "invalid UTF-8 delimiter", mutate: func(options *StatsOptions) { options.Delimiter = string([]byte{0xff}) }},
	} {
		operator := valid()
		test.mutate(operator.StatsOptions)
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
			t.Errorf("Analyze accepted %s options %#v", test.name, operator.StatsOptions)
		}
	}

	for _, options := range []*StatsOptions{
		nil,
		{Partitions: 1, Delimiter: ""},
		{Partitions: 100, AllNumeric: true, Delimiter: "::", DeduplicateSplitValues: true},
	} {
		operator := valid()
		operator.StatsOptions = options
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err != nil {
			t.Errorf("Analyze valid options %#v: %v", options, err)
		}
	}
}

func TestStatsOptionsPreserveMeasureAndGroupBudgets(t *testing.T) {
	t.Parallel()

	measures := make([]string, spl.MaximumStatsMeasures)
	for index := range measures {
		measures[index] = fmt.Sprintf("count(value_%d) AS count_%d", index, index)
	}
	source := `index=gradethis | stats partitions=100 allnum=false delim="," ` +
		strings.Join(measures, " ") + ` dedup_splitvals=false`
	if _, err := Build(
		mustParse(t, source),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build at measure limit: %v", err)
	}
}
