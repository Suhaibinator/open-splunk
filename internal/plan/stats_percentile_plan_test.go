package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsPercentileFamilyPreservesRequestedLevels(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | stats p1(metric) p50(metric) p90(metric) `+
				`p95(metric) p99(metric) perc42(metric) AS answer BY service`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(
		logical.OutputFields,
		[]string{
			"service",
			"perc1(metric)",
			"perc50(metric)",
			"perc90(metric)",
			"perc95(metric)",
			"perc99(metric)",
			"answer",
		},
	) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok || len(aggregate.Measures) != 6 {
		t.Fatalf("aggregate operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	want := []uint8{1, 50, 90, 95, 99, 42}
	for index, measure := range aggregate.Measures {
		if measure.Function != AggregateFunctionPercentile ||
			measure.Input.Name != "metric" ||
			measure.Percentile != want[index] {
			t.Errorf("measure[%d] = %#v, want percentile %d", index, measure, want[index])
		}
	}
}

func TestBuildRejectsForgedStatsPercentileRange(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 7},
	}
	for _, percentile := range []uint8{0, 100, 255} {
		command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{{
			Function:   spl.AggregateFunctionPercentile,
			Input:      "latency",
			InputRange: fieldRange,
			Percentile: percentile,
			Alias:      "result",
			Range:      fieldRange,
			AliasRange: fieldRange,
		}}}
		query := &spl.Query{
			Search:   base.Search,
			Commands: []spl.Command{command},
			Range:    base.Range,
		}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
	}
}

func TestBuildRejectsForgedPercentileMetadataOnOtherAggregate(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 7},
	}
	command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{{
		Function:   spl.AggregateFunctionSum,
		Input:      "latency",
		InputRange: fieldRange,
		Percentile: 95,
		Alias:      "result",
		Range:      fieldRange,
		AliasRange: fieldRange,
	}}}
	query := &spl.Query{
		Search:   base.Search,
		Commands: []spl.Command{command},
		Range:    base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
}
