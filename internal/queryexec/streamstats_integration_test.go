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

	sumJob, sumPage := queryIntegrationRunSearch(
		t,
		ctx,
		executor,
		indexTime,
		"queryexec-streamstats-sum-transport",
		`index=main source="source"`+
			` | streamstats sum(status) AS running_total`+
			` | streamstats current=false sum(status) AS preceding_total BY path`+
			` | table event_id running_total preceding_total`,
	)
	if sumJob.State != searchjobs.StateCompleted {
		t.Fatalf(
			"streamstats sum transport state = %v, failure=%#v",
			sumJob.State,
			sumJob.Failure,
		)
	}
	wantSumColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "running_total", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "preceding_total", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}
	if len(sumPage.Schema.Columns) != len(wantSumColumns) {
		t.Fatalf(
			"streamstats sum transport schema = %#v, want %#v",
			sumPage.Schema.Columns,
			wantSumColumns,
		)
	}
	for index := range wantSumColumns {
		if sumPage.Schema.Columns[index] != wantSumColumns[index] {
			t.Fatalf(
				"streamstats sum transport column %d = %#v, want %#v",
				index,
				sumPage.Schema.Columns[index],
				wantSumColumns[index],
			)
		}
	}
	if len(sumPage.Rows) != 1 {
		t.Fatalf("streamstats sum transport rows = %#v, want one row", sumPage.Rows)
	}
	if eventID, ok := sumPage.Rows[0].Values[0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("streamstats sum transport event_id = %q, string=%v", eventID, ok)
	}
	if total, ok := sumPage.Rows[0].Values[1].Double(); !ok || total != 200 {
		t.Fatalf("streamstats sum running_total = %v, double=%v", total, ok)
	}
	if !sumPage.Rows[0].Values[2].IsNull() {
		t.Fatalf(
			"streamstats sum preceding_total = %#v, want null empty prior frame",
			sumPage.Rows[0].Values[2],
		)
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

	const sumAnalysis = `index=main source="source" | streamstats sum(status) AS running_total`
	sumLogical := queryIntegrationFieldPlan(t, "main", indexTime, sumAnalysis)
	sumCatalogQuery, err := compiler.CompileFieldCatalog(
		sumLogical,
		clickhouse.FieldCatalogSpec{MaximumFields: clickhouse.MaximumFieldCatalogFields},
	)
	if err != nil {
		t.Fatalf("compile streamstats sum field catalog: %v", err)
	}
	sumCatalog, err := executor.ExecuteFieldCatalog(ctx, sumCatalogQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats sum field catalog: %v\nSQL: %s\nargs: %#v",
			err,
			sumCatalogQuery.SQL,
			sumCatalogQuery.Args,
		)
	}
	if sumCatalog.TotalEvents != 1 {
		t.Fatalf("streamstats sum field catalog = %#v, want one event", sumCatalog)
	}
	assertFieldCatalogProfile(t, sumCatalog, FieldProfileRow{
		FieldName:     "running_total",
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeDouble},
		EventCount:    1,
	})

	sumSummaryQuery, err := compiler.CompileFieldSummary(
		sumLogical,
		clickhouse.FieldSummarySpec{
			FieldName:             "running_total",
			MaximumValues:         10,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
			MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
		},
	)
	if err != nil {
		t.Fatalf("compile streamstats sum field summary: %v", err)
	}
	sumSummary, err := executor.ExecuteFieldSummary(ctx, sumSummaryQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats sum field summary: %v\nSQL: %s\nargs: %#v",
			err,
			sumSummaryQuery.SQL,
			sumSummaryQuery.Args,
		)
	}
	if sumSummary.FieldName != "running_total" ||
		len(sumSummary.ObservedTypes) != 1 ||
		sumSummary.ObservedTypes[0] != eventfields.StoredValueTypeDouble ||
		sumSummary.EventCount != 1 || sumSummary.NullCount != 0 ||
		sumSummary.MissingCount != 0 || sumSummary.DistinctCount != 1 ||
		len(sumSummary.TopValues) != 1 || sumSummary.TopValues[0].Count != 1 {
		t.Fatalf("streamstats sum field summary = %#v", sumSummary)
	}
	if total, ok := sumSummary.TopValues[0].Value.Double(); !ok || total != 200 {
		t.Fatalf(
			"streamstats sum summary running_total = %v, double=%v",
			total,
			ok,
		)
	}

	sumTimelineQuery := queryIntegrationCompileTimeline(
		t,
		sumAnalysis,
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
	sumBuckets, err := executor.ExecuteTimeline(ctx, sumTimelineQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats sum timeline: %v\nSQL: %s\nargs: %#v",
			err,
			sumTimelineQuery.SQL,
			sumTimelineQuery.Args,
		)
	}
	if len(sumBuckets) != 1 || sumBuckets[0].Count != 1 ||
		!sumBuckets[0].AlignedStart.Equal(firstBucket) {
		t.Fatalf(
			"streamstats sum timeline = %#v, want one event at %v",
			sumBuckets,
			firstBucket,
		)
	}
}
