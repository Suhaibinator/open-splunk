package queryexec

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestChartCountField reuses the adversarial occurrence
// fixture shared with timechart. Source-row frequency intentionally disagrees
// with immediate-member occurrence totals, so bare-count ranking or cells
// cannot satisfy these assertions.
func queryIntegrationTestChartCountField(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
	explainer *Explainer,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()

	earliest := base
	latest := base.Add(15 * time.Minute)
	run := func(t *testing.T, id, source string) (searchjobs.Job, searchjobs.ResultPage) {
		t.Helper()
		return queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			id,
			source,
			earliest,
			latest,
		)
	}

	t.Run("immediate occurrences isolation and physical shape", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-fixed" | chart count(occurrence) OVER host BY segment`
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		queryIntegrationAssertChartCountFieldPhysicalShape(
			t,
			ctx,
			connection,
			explainer,
			compiled,
		)
		if compiled.Chart == nil ||
			compiled.Chart.RowField != "host" ||
			compiled.Chart.ValueKind != clickhouse.ChartValueKindCount ||
			!slices.Equal(compiled.OutputFields, []string{"host"}) {
			t.Fatalf("chart count(field) contract = %#v / %v", compiled.Chart, compiled.OutputFields)
		}

		job, page := run(t, "queryexec-chart-count-field-fixed", source)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{"host", "NULL"},
			[]string{"host"},
			[][]uint64{{9}},
		)
	})

	t.Run("occurrence totals select top ten tie null and other", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-split",
			`index=main source="timechart-count-field-split" | chart count(occurrence) OVER host BY segment`,
		)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{
				"host", "a", "b", "c", "d", "e", "f", "g", "h", "i",
				"spike", "NULL", "OTHER",
			},
			[]string{"host"},
			[][]uint64{{19, 18, 17, 16, 15, 14, 13, 12, 11, 20, 0, 11}},
		)
		for _, forbidden := range []string{"j", "volume", "poison"} {
			for _, column := range page.Schema.Columns {
				if column.Name == forbidden {
					t.Fatalf("chart count(field) leaked or misranked %q: %#v", forbidden, page.Schema)
				}
			}
		}
	})

	t.Run("zero contribution ordinary and null domains remain visible", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-ineligible",
			`index=main source="timechart-count-field-split-ineligible" | chart count(occurrence) OVER host BY segment`,
		)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{"host", "zero", "NULL"},
			[]string{"host"},
			[][]uint64{{0, 0}},
		)
	})

	t.Run("missing and null dynamic row values do not invalidate valid rows", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-dynamic-row",
			`index=main source="timechart-count-field-split-ineligible" | chart count(occurrence) OVER segment BY source`,
		)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{"segment", "timechart-count-field-split-ineligible"},
			[]string{"zero"},
			[][]uint64{{0}},
		)
	})

	t.Run("measure may equal the column axis", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-column-measure",
			`index=main source="timechart-count-field-split-ineligible" | chart count(segment) OVER host BY segment`,
		)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{"host", "zero", "NULL"},
			[]string{"host"},
			[][]uint64{{3, 0}},
		)
	})

	t.Run("projected measure remains missing", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-projected",
			`index=main source="timechart-count-field-fixed" | fields host segment | chart count(occurrence) OVER host BY segment`,
		)
		queryIntegrationAssertChartCountFieldPage(
			t,
			job,
			page,
			[]string{"host", "NULL"},
			[]string{"host"},
			[][]uint64{{0}},
		)
	})

	t.Run("invalid column domain fails despite zero contribution", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-invalid-split" | chart count(occurrence) OVER host BY segment`
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) ||
			sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"zero-occurrence invalid chart: err=%v schema calls=%d schema=%#v rows=%d",
				err,
				sink.setCalls,
				sink.schema,
				len(sink.rows),
			)
		}
		job, _ := run(t, "queryexec-chart-count-field-invalid", source)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL ||
			job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("zero-occurrence invalid chart job = %#v", job)
		}
	})

	t.Run("empty input publishes only the row schema", func(t *testing.T) {
		job, page := run(
			t,
			"queryexec-chart-count-field-empty",
			`index=main source="timechart-count-field-empty" | chart count(occurrence) OVER host BY segment`,
		)
		if job.State != searchjobs.StateCompleted || job.RowCount != 0 || len(page.Rows) != 0 {
			t.Fatalf("empty chart count(field) job=%#v page=%#v", job, page)
		}
		queryIntegrationAssertColumns(t, page, []string{"host"})
		if page.Schema.Columns[0] != (searchjobs.Column{Name: "host", Kind: searchjobs.ValueKindString}) {
			t.Fatalf("empty chart count(field) schema = %#v", page.Schema)
		}
	})
}

func queryIntegrationAssertChartCountFieldPhysicalShape(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	explainer *Explainer,
	compiled clickhouse.CompiledQuery,
) {
	t.Helper()
	queryIntegrationAssertOneScanNoArrayJoin(
		t,
		ctx,
		connection,
		explainer,
		compiled,
		"chart count(field)",
	)
}

func queryIntegrationAssertChartCountFieldPage(
	t *testing.T,
	job searchjobs.Job,
	page searchjobs.ResultPage,
	wantNames []string,
	wantRows []string,
	wantCounts [][]uint64,
) {
	t.Helper()
	if job.State != searchjobs.StateCompleted {
		t.Fatalf("chart count(field) state = %v, failure=%#v", job.State, job.Failure)
	}
	queryIntegrationAssertColumns(t, page, wantNames)
	for index, column := range page.Schema.Columns {
		wantKind := searchjobs.ValueKindUnsigned
		if index == 0 {
			wantKind = searchjobs.ValueKindString
		}
		if column.Kind != wantKind || column.Nullable || column.Multivalue {
			t.Fatalf("chart count(field) column %d = %#v, want %v", index, column, wantKind)
		}
	}
	if len(page.Rows) != len(wantCounts) || len(wantRows) != len(wantCounts) {
		t.Fatalf(
			"chart count(field) rows = %d, labels = %d, counts = %d",
			len(page.Rows),
			len(wantRows),
			len(wantCounts),
		)
	}
	for rowIndex, counts := range wantCounts {
		if len(page.Rows[rowIndex].Values) != len(counts)+1 {
			t.Fatalf("chart count(field) row %d = %#v", rowIndex, page.Rows[rowIndex])
		}
		rowLabel, ok := page.Rows[rowIndex].Values[0].String()
		if !ok || rowLabel != wantRows[rowIndex] {
			t.Fatalf(
				"chart count(field) row %d label = %q, %v, want %q",
				rowIndex,
				rowLabel,
				ok,
				wantRows[rowIndex],
			)
		}
		for columnIndex, want := range counts {
			got, ok := page.Rows[rowIndex].Values[columnIndex+1].Unsigned()
			if !ok || got != want {
				t.Fatalf(
					"chart count(field) row %d column %q = %d, %v, want %d",
					rowIndex,
					wantNames[columnIndex+1],
					got,
					ok,
					want,
				)
			}
		}
	}
}
