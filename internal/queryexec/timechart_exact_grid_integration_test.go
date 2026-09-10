package queryexec

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func queryIntegrationTestExactTimechartGrid(t *testing.T, ctx context.Context, executor *Executor, base, indexTime time.Time) {
	anchor := base.Add(5 * time.Minute)
	for _, aggregate := range []string{"count", "count(metric)", "sum(metric)", "avg(metric)", "perc50(metric)"} {
		for _, split := range []string{"", " BY level"} {
			for _, controls := range []struct {
				source string
				rows   int
			}{{"cont=false", 2}, {"cont=false partial=false", 1}, {"partial=false", 2}} {
				t.Run(aggregate+split+" "+controls.source, func(t *testing.T) {
					source := fmt.Sprintf(`index=main source="timechart-level" | eval metric=1 | timechart span=250ms %s %s%s`, controls.source, aggregate, split)
					job, page := queryIntegrationRunSearchRange(t, ctx, executor, indexTime, "exact-grid", source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
					if job.State != searchjobs.StateCompleted {
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
		t.Run("present input missing measure "+aggregate, func(t *testing.T) {
			source := fmt.Sprintf(`index=main source="timechart-level" | timechart span=250ms cont=false %s`, aggregate)
			job, page := queryIntegrationRunSearchRange(t, ctx, executor, indexTime, "exact-grid-missing", source, anchor.Add(-100*time.Millisecond), anchor.Add(600*time.Millisecond))
			if job.State != searchjobs.StateCompleted || len(page.Rows) != 2 {
				t.Fatalf("state=%v failure=%+v rows=%d", job.State, job.Failure, len(page.Rows))
			}
		})
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
