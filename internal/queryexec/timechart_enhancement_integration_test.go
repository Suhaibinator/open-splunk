package queryexec

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestTimechartEnhancementSeams runs inside the shared pinned
// ClickHouse harness after its timechart fixture is inserted. The harness owner
// calls this once with the same executor and snapshot used by the focused
// timechart integration cases.
func queryIntegrationTestTimechartEnhancementSeams(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	explainer *Explainer,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()

	t.Run("unlimited split suffix scans events once and emits no other", func(t *testing.T) {
		const source = `index=main source="timechart-top"` +
			` | timechart span=5m cont=false count BY path limit=0 useother=true` +
			` | head 1`
		compiled := queryIntegrationCompileSearchRange(
			t,
			source,
			indexTime,
			base,
			base.Add(5*time.Minute),
		)
		if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
			t.Fatalf("initial stage does not contain exactly one events source:\n%s", compiled.SQL)
		}
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("EXPLAIN enhanced split timechart: %v", err)
		}
		physical := queryIntegrationAssertStructuredExplain(t, explained)
		if len(physical.Reads) != 1 {
			t.Fatalf("initial timechart physical reads = %#v, want one", physical.Reads)
		}

		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-enhanced-limit-zero",
			source,
			base,
			base.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 1 {
			t.Fatalf("unlimited split suffix job=%#v page=%#v", job, page)
		}
		wantColumns := []string{
			"_time", "a", "b", "c", "d", "e", "f", "g", "h", "hot", "i", "j", "k", "NULL",
		}
		gotColumns := make([]string, len(page.Schema.Columns))
		for index, column := range page.Schema.Columns {
			gotColumns[index] = column.Name
		}
		if !slices.Equal(gotColumns, wantColumns) {
			t.Fatalf("unlimited split columns = %v, want %v", gotColumns, wantColumns)
		}
		if slices.Contains(gotColumns, "OTHER") {
			t.Fatalf("limit=0 emitted OTHER without an exclusion: %v", gotColumns)
		}
	})

	t.Run("actual extent sparse static suffix carries exact bounds", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-enhanced-actual-extent",
			`index=main source="timechart-level"`+
				` | timechart fixedrange=false cont=false count`+
				` | where count > 0 | sort 0 +_time`,
			base,
			base.Add(20*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) == 0 {
			t.Fatalf("actual-extent suffix job=%#v page=%#v", job, page)
		}
		for index, row := range page.Rows {
			count, ok := row.Values[1].Unsigned()
			if !ok || count == 0 {
				t.Fatalf("sparse row %d count = %#v", index, row.Values[1])
			}
			if row.TimeBucket == nil || row.TimeBucket.Earliest >= row.TimeBucket.Latest {
				t.Fatalf("sparse row %d bounds = %#v", index, row.TimeBucket)
			}
		}
	})

	t.Run("dynamic projection feeds a repeated timechart", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-enhanced-repeated",
			`index=main source="timechart-underscore"`+
				` | timechart span=5m count BY path limit=0`+
				` | fields _time Z`+
				` | timechart span=10m sum(Z) AS total | head 1`,
			base,
			base.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 1 ||
			len(page.Schema.Columns) != 2 || page.Schema.Columns[0].Name != "_time" ||
			page.Schema.Columns[1].Name != "total" {
			t.Fatalf("repeated timechart job=%#v page=%#v", job, page)
		}
		if total, ok := page.Rows[0].Values[1].Double(); !ok || total != 1 {
			t.Fatalf("repeated timechart total = %#v, want 1", page.Rows[0].Values[1])
		}
		if page.Rows[0].TimeBucket == nil {
			t.Fatal("repeated timechart did not replace bucket bounds")
		}
	})

	t.Run("head cannot hide invalid split input or publish a prefix", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-enhanced-hidden-invalid",
			`index=main source="timechart-invalid"`+
				` | timechart span=5m count BY path limit=0 | head 1`,
			base,
			base.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL || job.Schema != nil ||
			job.RowCount != 0 || job.ResultBytes != 0 || len(page.Rows) != 0 {
			t.Fatalf("hidden invalid split published partial state: job=%#v page=%#v", job, page)
		}
	})
}
