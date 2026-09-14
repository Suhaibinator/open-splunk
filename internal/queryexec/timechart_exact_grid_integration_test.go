package queryexec

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func queryIntegrationTestExactTimechartGrid(t *testing.T, ctx context.Context, executor *Executor, base, indexTime time.Time) {
	anchor := base.Add(5 * time.Minute)
	t.Run("exact boundaries", func(t *testing.T) { queryIntegrationTestExactTimechartBoundaries(t, ctx, executor, indexTime) })
	for _, aggregate := range []string{"count", "count(metric)", "sum(metric)", "avg(metric)", "perc50(metric)"} {
		for _, split := range []string{"", " BY level"} {
			for _, controls := range exactTimechartControlCases() {
				t.Run(aggregate+split+" "+controls.source, func(t *testing.T) {
					source := fmt.Sprintf(`index=main source="timechart-level" | eval metric=1 | timechart span=250ms %s %s%s`, controls.source, aggregate, split)
					job, page := queryIntegrationRunSearchRange(t, ctx, executor, indexTime, "exact-grid", source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
					if job.State != searchjobs.StateCompleted {
						queryIntegrationExplainExactGridFailure(t, ctx, executor, indexTime, source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
						t.Fatalf("state=%v failure=%+v", job.State, job.Failure)
					}
					if len(page.Rows) != controls.rows {
						t.Fatalf("rows=%d want=%d page=%+v", len(page.Rows), controls.rows, page)
					}
				})
			}
		}
	}

	for _, aggregate := range []string{"count(missing_metric)", "sum(missing_metric)", "avg(missing_metric)", "perc50(missing_metric)"} {
		for _, split := range []string{"", " BY level"} {
			for _, controls := range exactTimechartControlCases() {
				for _, empty := range []bool{false, true} {
					t.Run(fmt.Sprintf("missing_%s%s_%s_empty=%t", aggregate, split, controls.source, empty), func(t *testing.T) {
						filter := "timechart-level"
						want := controls.rows
						if empty {
							filter = "timechart-exact-no-events"
							want = 0
						}
						source := fmt.Sprintf(`index=main source="%s" | timechart span=250ms %s %s%s`, filter, controls.source, aggregate, split)
						job, page := queryIntegrationRunSearchRange(t, ctx, executor, indexTime, "exact-grid-missing", source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
						if job.State != searchjobs.StateCompleted || len(page.Rows) != want {
							if job.State != searchjobs.StateCompleted {
								queryIntegrationExplainExactGridFailure(t, ctx, executor, indexTime, source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
							}
							t.Fatalf("state=%v failure=%+v rows=%d want=%d", job.State, job.Failure, len(page.Rows), want)
						}
					})
				}
			}
		}
	}
	for _, span := range []string{"49h", "2d", "2w", "2month", "1q", "2y"} {
		t.Run(span, func(t *testing.T) {
			source := fmt.Sprintf(`index=main source="timechart-level" | timechart span=%s count`, span)
			job, page := queryIntegrationRunSearchRange(t, ctx, executor, indexTime, "exact-grid-calendar", source, anchor.Add(-time.Second), anchor.Add(time.Second))
			if job.State != searchjobs.StateCompleted || len(page.Rows) == 0 {
				t.Fatalf("state=%v failure=%+v rows=%d", job.State, job.Failure, len(page.Rows))
			}
		})
	}
}

func exactTimechartControlCases() []struct {
	source string
	rows   int
} {
	return []struct {
		source string
		rows   int
	}{
		{"cont=true partial=true fixedrange=true", 4},
		{"cont=true partial=false fixedrange=true", 2},
		{"cont=false partial=true fixedrange=true", 2},
		{"cont=false partial=false fixedrange=true", 1},
		{"cont=true partial=true fixedrange=false", 2},
		{"cont=true partial=false fixedrange=false", 1},
		{"cont=false partial=true fixedrange=false", 2},
		{"cont=false partial=false fixedrange=false", 1},
	}
}

func queryIntegrationTestExactTimechartBoundaries(t *testing.T, ctx context.Context, executor *Executor, indexTime time.Time) {
	parse := func(source string) time.Time {
		value, err := time.Parse(time.RFC3339Nano, source)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, test := range []struct {
		name, source, axis, timezone, earliest, latest string
		bounds                                         []string
		counts                                         []uint64
	}{
		{"pre-epoch", "timechart-exact-pre-epoch", "span=250ms cont=false", "UTC", "1969-12-31T23:59:59.9Z", "1970-01-01T00:00:00.6Z", []string{"1969-12-31T23:59:59.75Z", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.25Z"}, []uint64{1, 1}},
		{"nanosecond-exclusive-ceiling", "timechart-exact-ceiling", "span=250ms", "UTC", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.250000001Z", []string{"1970-01-01T00:00:00Z", "1970-01-01T00:00:00.25Z", "1970-01-01T00:00:00.5Z"}, []uint64{0, 1}},
		{"spring-multi-day", "timechart-exact-spring", "span=2d", "America/Los_Angeles", "2026-03-07T08:00:00Z", "2026-03-11T07:00:00Z", []string{"2026-03-06T08:00:00Z", "2026-03-08T08:00:00Z", "2026-03-10T07:00:00Z", "2026-03-12T07:00:00Z"}, []uint64{1, 1, 0}},
		{"fall-multi-day", "timechart-exact-fall", "span=2d", "America/Los_Angeles", "2026-10-31T07:00:00Z", "2026-11-04T08:00:00Z", []string{"2026-10-30T07:00:00Z", "2026-11-01T07:00:00Z", "2026-11-03T08:00:00Z", "2026-11-05T08:00:00Z"}, []uint64{1, 1, 0}},
		{"leap-multi-month", "timechart-exact-leap", "span=2month", "UTC", "2024-02-28T00:00:00Z", "2024-03-02T00:00:00Z", []string{"2024-01-01T00:00:00Z", "2024-03-01T00:00:00Z", "2024-05-01T00:00:00Z"}, []uint64{1, 0}},
		{"aligned-multi-week", "timechart-exact-spring", "span=2w aligntime=earliest", "America/Los_Angeles", "2026-03-07T08:17:00Z", "2026-03-25T07:17:00Z", []string{"2026-03-07T08:17:00Z", "2026-03-21T07:17:00Z", "2026-04-04T07:17:00Z"}, []uint64{2, 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := fmt.Sprintf(`index=main source="%s" | timechart %s count`, test.source, test.axis)
			query, err := spl.Parse(source)
			if err != nil {
				t.Fatal(err)
			}
			visibility := uint64(1)
			logical, err := plan.Build(query, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, Earliest: parse(test.earliest), Latest: parse(test.latest), SearchStart: indexTime, SearchTimezone: test.timezone, IndexTimeCutoff: indexTime.Add(500 * time.Microsecond), VisibilityCutoff: &visibility})
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := (clickhouse.Compiler{}).Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			sink := &exactGridTestSink{}
			if err := executor.Execute(ctx, compiled, sink); err != nil {
				t.Fatal(err)
			}
			if len(sink.rows) != len(test.counts) || len(sink.bounds) != len(test.counts) {
				t.Fatalf("rows=%d bounds=%d expected=%d", len(sink.rows), len(sink.bounds), len(test.counts))
			}
			for index, count := range test.counts {
				actual, ok := sink.rows[index][1].Unsigned()
				if !ok || actual != count || sink.bounds[index].Earliest != test.bounds[index] || sink.bounds[index].Latest != test.bounds[index+1] {
					t.Fatalf("row%d count=%d bounds=%+v", index, actual, sink.bounds[index])
				}
			}
		})
	}
}

func queryIntegrationExplainExactGridFailure(t *testing.T, ctx context.Context, executor *Executor, indexTime time.Time, source string, earliest, latest time.Time) {
	t.Helper()
	compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
	diagnosticSink := &exactGridTestSink{}
	t.Logf("direct native execution error: %v", executor.Execute(ctx, compiled, diagnosticSink))
}
