package queryexec

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestStreamStatsTransport runs both ungrouped and grouped
// field-occurrence results through Executor and Manager. The store integration
// owns the broader ordering and resource-bound matrix; this pins the public
// UInt64/Nullable(UInt64) schemas and current=false zero at the transport
// boundary.
func queryIntegrationTestStreamStatsTransport(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
) {
	t.Helper()

	job, page := queryIntegrationRunSearch(
		t,
		ctx,
		executor,
		indexTime,
		"queryexec-streamstats-transport",
		`index=main source="source"`+
			` | streamstats count(path) AS populated`+
			` | streamstats current=false count(status) AS preceding BY path`+
			` | table event_id populated preceding`,
	)
	if job.State != searchjobs.StateCompleted {
		t.Fatalf(
			"streamstats transport state = %v, failure=%#v",
			job.State,
			job.Failure,
		)
	}
	wantColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "populated", Kind: searchjobs.ValueKindUnsigned},
		{Name: "preceding", Kind: searchjobs.ValueKindUnsigned, Nullable: true},
	}
	if len(page.Schema.Columns) != len(wantColumns) {
		t.Fatalf(
			"streamstats transport schema = %#v, want %#v",
			page.Schema.Columns,
			wantColumns,
		)
	}
	for index := range wantColumns {
		if page.Schema.Columns[index] != wantColumns[index] {
			t.Fatalf(
				"streamstats transport column %d = %#v, want %#v",
				index,
				page.Schema.Columns[index],
				wantColumns[index],
			)
		}
	}
	if len(page.Rows) != 1 {
		t.Fatalf("streamstats transport rows = %#v, want one row", page.Rows)
	}
	if eventID, ok := page.Rows[0].Values[0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("streamstats transport event_id = %q, string=%v", eventID, ok)
	}
	if populated, ok := page.Rows[0].Values[1].Unsigned(); !ok || populated != 1 {
		t.Fatalf("streamstats transport populated = %d, unsigned=%v", populated, ok)
	}
	if preceding, ok := page.Rows[0].Values[2].Unsigned(); !ok || preceding != 0 {
		t.Fatalf("streamstats transport preceding = %d, unsigned=%v", preceding, ok)
	}

	const analysis = `index=main source="source" | streamstats count(path) AS populated`
	logical := queryIntegrationFieldPlan(t, "main", indexTime, analysis)
	compiler := clickhouse.Compiler{}

	catalogQuery, err := compiler.CompileFieldCatalog(
		logical,
		clickhouse.FieldCatalogSpec{MaximumFields: clickhouse.MaximumFieldCatalogFields},
	)
	if err != nil {
		t.Fatalf("compile streamstats field catalog: %v", err)
	}
	catalog, err := executor.ExecuteFieldCatalog(ctx, catalogQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats field catalog: %v\nSQL: %s\nargs: %#v",
			err,
			catalogQuery.SQL,
			catalogQuery.Args,
		)
	}
	if catalog.TotalEvents != 1 {
		t.Fatalf("streamstats field catalog = %#v, want one event", catalog)
	}
	assertFieldCatalogProfile(t, catalog, FieldProfileRow{
		FieldName:     "populated",
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeUint64},
		EventCount:    1,
	})

	summaryQuery, err := compiler.CompileFieldSummary(
		logical,
		clickhouse.FieldSummarySpec{
			FieldName:             "populated",
			MaximumValues:         10,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
			MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
		},
	)
	if err != nil {
		t.Fatalf("compile streamstats field summary: %v", err)
	}
	summary, err := executor.ExecuteFieldSummary(ctx, summaryQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats field summary: %v\nSQL: %s\nargs: %#v",
			err,
			summaryQuery.SQL,
			summaryQuery.Args,
		)
	}
	if summary.FieldName != "populated" ||
		len(summary.ObservedTypes) != 1 ||
		summary.ObservedTypes[0] != eventfields.StoredValueTypeUint64 ||
		summary.EventCount != 1 || summary.NullCount != 0 ||
		summary.MissingCount != 0 || summary.DistinctCount != 1 ||
		len(summary.TopValues) != 1 || summary.TopValues[0].Count != 1 {
		t.Fatalf("streamstats field summary = %#v", summary)
	}
	if populated, ok := summary.TopValues[0].Value.Unsigned(); !ok || populated != 1 {
		t.Fatalf(
			"streamstats summary populated = %d, unsigned=%v",
			populated,
			ok,
		)
	}

	firstBucket := indexTime.Truncate(2 * time.Second)
	timelineQuery := queryIntegrationCompileTimeline(
		t,
		analysis,
		"main",
		indexTime,
		clickhouse.TimelineSpec{
			FirstBucket: firstBucket,
			SpanSeconds: 2,
			BucketCount: 1,
			Earliest:    firstBucket,
			Latest:      firstBucket.Add(2 * time.Second),
		},
	)
	buckets, err := executor.ExecuteTimeline(ctx, timelineQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats timeline: %v\nSQL: %s\nargs: %#v",
			err,
			timelineQuery.SQL,
			timelineQuery.Args,
		)
	}
	if len(buckets) != 1 || buckets[0].Count != 1 ||
		!buckets[0].AlignedStart.Equal(firstBucket) {
		t.Fatalf(
			"streamstats timeline = %#v, want one event at %v",
			buckets,
			firstBucket,
		)
	}
}
