package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileRelatedAggregateCommandsRejectStatsOnlyMeasureMetadata(t *testing.T) {
	t.Parallel()

	for _, command := range []struct {
		name    string
		source  string
		measure func(*plan.Query) *plan.AggregateMeasure
	}{
		{
			name:   "eventstats",
			source: `index=gradethis | eventstats count AS total`,
			measure: func(query *plan.Query) *plan.AggregateMeasure {
				for _, operator := range query.Operators {
					if aggregate, ok := operator.(*plan.EventAggregate); ok {
						return &aggregate.Measure
					}
				}
				return nil
			},
		},
		{
			name:   "streamstats",
			source: `index=gradethis | streamstats count AS running`,
			measure: func(query *plan.Query) *plan.AggregateMeasure {
				for _, operator := range query.Operators {
					if aggregate, ok := operator.(*plan.StreamAggregate); ok {
						return &aggregate.Measure
					}
				}
				return nil
			},
		},
		{
			name:   "timechart",
			source: `index=gradethis | timechart span=5m count`,
			measure: func(query *plan.Query) *plan.AggregateMeasure {
				for _, operator := range query.Operators {
					if aggregate, ok := operator.(*plan.Timechart); ok {
						return &aggregate.Measure
					}
				}
				return nil
			},
		},
		{
			name:   "chart",
			source: `index=gradethis | chart count OVER host BY service`,
			measure: func(query *plan.Query) *plan.AggregateMeasure {
				for _, operator := range query.Operators {
					if aggregate, ok := operator.(*plan.Chart); ok {
						return &aggregate.Measure
					}
				}
				return nil
			},
		},
	} {
		for _, mutation := range []struct {
			name   string
			mutate func(*plan.AggregateMeasure)
		}{
			{
				name: "sparkline",
				mutate: func(measure *plan.AggregateMeasure) {
					measure.Sparkline = &plan.SparklineMeasure{}
				},
			},
			{
				name: "scalar input",
				mutate: func(measure *plan.AggregateMeasure) {
					measure.InputExpression = &plan.ScalarLiteralExpression{
						Value: plan.Value{Kind: plan.ValueKindInt64, Int64: 1},
					}
				},
			},
			{
				name: "literal output",
				mutate: func(measure *plan.AggregateMeasure) {
					measure.OutputLiteral = true
				},
			},
		} {
			t.Run(command.name+"/"+mutation.name, func(t *testing.T) {
				t.Parallel()
				logical := buildPlan(t, command.source)
				measure := command.measure(logical)
				if measure == nil {
					t.Fatal("aggregate measure is missing")
				}
				mutation.mutate(measure)
				_, err := (Compiler{}).Compile(logical)
				if err == nil || !strings.Contains(err.Error(), "stats-only") {
					t.Fatalf("Compile error = %v, want stats-only metadata rejection", err)
				}
			})
		}
	}
}

func TestCompileChartAndTimechartRejectForgedFieldReferences(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		mutate func(*plan.Query)
	}{
		{
			name:   "chart row axis path",
			source: `index=gradethis | table host,service | chart count OVER host BY service`,
			mutate: func(query *plan.Query) {
				chart := query.Operators[len(query.Operators)-1].(*plan.Chart)
				chart.Over.Path = []string{"forged"}
			},
		},
		{
			name:   "chart split axis canonical bit",
			source: `index=gradethis | chart count OVER host BY _time`,
			mutate: func(query *plan.Query) {
				chart := query.Operators[len(query.Operators)-1].(*plan.Chart)
				chart.SplitBy.Canonical = false
			},
		},
		{
			name:   "timechart time path",
			source: `index=gradethis | timechart span=5m count`,
			mutate: func(query *plan.Query) {
				timechart := query.Operators[len(query.Operators)-1].(*plan.Timechart)
				timechart.Time.Path = []string{"forged"}
			},
		},
		{
			name:   "timechart canonical bit",
			source: `index=gradethis | timechart span=5m count`,
			mutate: func(query *plan.Query) {
				timechart := query.Operators[len(query.Operators)-1].(*plan.Timechart)
				timechart.Time.Canonical = false
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, test.source)
			test.mutate(logical)
			if _, err := (Compiler{}).Compile(logical); err == nil ||
				!strings.Contains(err.Error(), "metadata is not canonical") {
				t.Fatalf("Compile error = %v, want canonical metadata rejection", err)
			}
		})
	}
}

func TestCompileStatsRejectsForgedDuplicateSourceRenames(t *testing.T) {
	t.Parallel()

	t.Run("exact field", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(
			t,
			`index=gradethis | stats avg(latency) AS average sum(latency) AS total`,
		)
		aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
		aggregate.Measures[1].Function = plan.AggregateFunctionAverage
		if _, err := (Compiler{}).Compile(logical); err == nil ||
			!strings.Contains(err.Error(), "source is renamed more than once") {
			t.Fatalf("Compile error = %v, want duplicate source rejection", err)
		}
	})

	t.Run("sparkline", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(
			t,
			`index=gradethis | stats sparkline(avg(latency),30m) AS half_hour `+
				`sparkline(avg(latency),1h) AS hourly`,
		)
		aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
		aggregate.Measures[1].Sparkline.Span = aggregate.Measures[0].Sparkline.Span
		if _, err := (Compiler{}).Compile(logical); err == nil ||
			!strings.Contains(err.Error(), "source is renamed more than once") {
			t.Fatalf("Compile error = %v, want duplicate sparkline source rejection", err)
		}
	})

	// Different functions over one exact field are explicitly valid and must
	// not be conflated by the conservative identity rule.
	if _, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | stats avg(latency) AS average sum(latency) AS total`,
	)); err != nil {
		t.Fatalf("Compile distinct aggregate sources: %v", err)
	}
}
