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
// count, numeric, and Dynamic-extrema schemas plus current=false empty-frame
// behavior at the transport boundary.
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

	averageJob, averagePage := queryIntegrationRunSearch(
		t,
		ctx,
		executor,
		indexTime,
		"queryexec-streamstats-average-transport",
		`index=main source="source"`+
			` | streamstats avg(status) AS running_mean`+
			` | streamstats current=false avg(status) AS preceding_mean BY path`+
			` | table event_id running_mean preceding_mean`,
	)
	if averageJob.State != searchjobs.StateCompleted {
		t.Fatalf(
			"streamstats average transport state = %v, failure=%#v",
			averageJob.State,
			averageJob.Failure,
		)
	}
	wantAverageColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "running_mean", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "preceding_mean", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}
	if len(averagePage.Schema.Columns) != len(wantAverageColumns) {
		t.Fatalf(
			"streamstats average transport schema = %#v, want %#v",
			averagePage.Schema.Columns,
			wantAverageColumns,
		)
	}
	for index := range wantAverageColumns {
		if averagePage.Schema.Columns[index] != wantAverageColumns[index] {
			t.Fatalf(
				"streamstats average transport column %d = %#v, want %#v",
				index,
				averagePage.Schema.Columns[index],
				wantAverageColumns[index],
			)
		}
	}
	if len(averagePage.Rows) != 1 {
		t.Fatalf("streamstats average transport rows = %#v, want one row", averagePage.Rows)
	}
	if eventID, ok := averagePage.Rows[0].Values[0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("streamstats average transport event_id = %q, string=%v", eventID, ok)
	}
	if mean, ok := averagePage.Rows[0].Values[1].Double(); !ok || mean != 200 {
		t.Fatalf("streamstats average running_mean = %v, double=%v", mean, ok)
	}
	if !averagePage.Rows[0].Values[2].IsNull() {
		t.Fatalf(
			"streamstats average preceding_mean = %#v, want null empty prior frame",
			averagePage.Rows[0].Values[2],
		)
	}

	minimumJob, minimumPage := queryIntegrationRunSearch(
		t,
		ctx,
		executor,
		indexTime,
		"queryexec-streamstats-minimum-transport",
		`index=main source="source"`+
			` | streamstats min(status) AS running_min`+
			` | streamstats current=false min(status) AS preceding_min BY path`+
			` | table event_id running_min preceding_min`,
	)
	if minimumJob.State != searchjobs.StateCompleted {
		t.Fatalf(
			"streamstats minimum transport state = %v, failure=%#v",
			minimumJob.State,
			minimumJob.Failure,
		)
	}
	wantMinimumColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "running_min", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "preceding_min", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}
	if len(minimumPage.Schema.Columns) != len(wantMinimumColumns) {
		t.Fatalf(
			"streamstats minimum transport schema = %#v, want %#v",
			minimumPage.Schema.Columns,
			wantMinimumColumns,
		)
	}
	for index := range wantMinimumColumns {
		if minimumPage.Schema.Columns[index] != wantMinimumColumns[index] {
			t.Fatalf(
				"streamstats minimum transport column %d = %#v, want %#v",
				index,
				minimumPage.Schema.Columns[index],
				wantMinimumColumns[index],
			)
		}
	}
	if len(minimumPage.Rows) != 1 {
		t.Fatalf("streamstats minimum transport rows = %#v, want one row", minimumPage.Rows)
	}
	if eventID, ok := minimumPage.Rows[0].Values[0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("streamstats minimum transport event_id = %q, string=%v", eventID, ok)
	}
	if minimum, ok := minimumPage.Rows[0].Values[1].Double(); !ok || minimum != 200 {
		t.Fatalf("streamstats minimum running_min = %v, double=%v", minimum, ok)
	}
	if !minimumPage.Rows[0].Values[2].IsNull() {
		t.Fatalf(
			"streamstats minimum preceding_min = %#v, want null empty prior frame",
			minimumPage.Rows[0].Values[2],
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

	const averageAnalysis = `index=main source="source" | streamstats avg(status) AS running_mean`
	averageLogical := queryIntegrationFieldPlan(t, "main", indexTime, averageAnalysis)
	averageCatalogQuery, err := compiler.CompileFieldCatalog(
		averageLogical,
		clickhouse.FieldCatalogSpec{MaximumFields: clickhouse.MaximumFieldCatalogFields},
	)
	if err != nil {
		t.Fatalf("compile streamstats average field catalog: %v", err)
	}
	averageCatalog, err := executor.ExecuteFieldCatalog(ctx, averageCatalogQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats average field catalog: %v\nSQL: %s\nargs: %#v",
			err,
			averageCatalogQuery.SQL,
			averageCatalogQuery.Args,
		)
	}
	if averageCatalog.TotalEvents != 1 {
		t.Fatalf("streamstats average field catalog = %#v, want one event", averageCatalog)
	}
	assertFieldCatalogProfile(t, averageCatalog, FieldProfileRow{
		FieldName:     "running_mean",
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeDouble},
		EventCount:    1,
	})

	averageSummaryQuery, err := compiler.CompileFieldSummary(
		averageLogical,
		clickhouse.FieldSummarySpec{
			FieldName:             "running_mean",
			MaximumValues:         10,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
			MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
		},
	)
	if err != nil {
		t.Fatalf("compile streamstats average field summary: %v", err)
	}
	averageSummary, err := executor.ExecuteFieldSummary(ctx, averageSummaryQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats average field summary: %v\nSQL: %s\nargs: %#v",
			err,
			averageSummaryQuery.SQL,
			averageSummaryQuery.Args,
		)
	}
	if averageSummary.FieldName != "running_mean" ||
		len(averageSummary.ObservedTypes) != 1 ||
		averageSummary.ObservedTypes[0] != eventfields.StoredValueTypeDouble ||
		averageSummary.EventCount != 1 || averageSummary.NullCount != 0 ||
		averageSummary.MissingCount != 0 || averageSummary.DistinctCount != 1 ||
		len(averageSummary.TopValues) != 1 || averageSummary.TopValues[0].Count != 1 {
		t.Fatalf("streamstats average field summary = %#v", averageSummary)
	}
	if mean, ok := averageSummary.TopValues[0].Value.Double(); !ok || mean != 200 {
		t.Fatalf(
			"streamstats average summary running_mean = %v, double=%v",
			mean,
			ok,
		)
	}

	averageTimelineQuery := queryIntegrationCompileTimeline(
		t,
		averageAnalysis,
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
	averageBuckets, err := executor.ExecuteTimeline(ctx, averageTimelineQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats average timeline: %v\nSQL: %s\nargs: %#v",
			err,
			averageTimelineQuery.SQL,
			averageTimelineQuery.Args,
		)
	}
	if len(averageBuckets) != 1 || averageBuckets[0].Count != 1 ||
		!averageBuckets[0].AlignedStart.Equal(firstBucket) {
		t.Fatalf(
			"streamstats average timeline = %#v, want one event at %v",
			averageBuckets,
			firstBucket,
		)
	}

	const minimumAnalysis = `index=main source="source" | streamstats min(status) AS running_min`
	minimumLogical := queryIntegrationFieldPlan(t, "main", indexTime, minimumAnalysis)
	minimumCatalogQuery, err := compiler.CompileFieldCatalog(
		minimumLogical,
		clickhouse.FieldCatalogSpec{MaximumFields: clickhouse.MaximumFieldCatalogFields},
	)
	if err != nil {
		t.Fatalf("compile streamstats minimum field catalog: %v", err)
	}
	minimumCatalog, err := executor.ExecuteFieldCatalog(ctx, minimumCatalogQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats minimum field catalog: %v\nSQL: %s\nargs: %#v",
			err,
			minimumCatalogQuery.SQL,
			minimumCatalogQuery.Args,
		)
	}
	if minimumCatalog.TotalEvents != 1 {
		t.Fatalf("streamstats minimum field catalog = %#v, want one event", minimumCatalog)
	}
	assertFieldCatalogProfile(t, minimumCatalog, FieldProfileRow{
		FieldName:     "running_min",
		ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeDouble},
		EventCount:    1,
	})

	minimumSummaryQuery, err := compiler.CompileFieldSummary(
		minimumLogical,
		clickhouse.FieldSummarySpec{
			FieldName:             "running_min",
			MaximumValues:         10,
			MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
			MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
		},
	)
	if err != nil {
		t.Fatalf("compile streamstats minimum field summary: %v", err)
	}
	minimumSummary, err := executor.ExecuteFieldSummary(ctx, minimumSummaryQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats minimum field summary: %v\nSQL: %s\nargs: %#v",
			err,
			minimumSummaryQuery.SQL,
			minimumSummaryQuery.Args,
		)
	}
	if minimumSummary.FieldName != "running_min" ||
		len(minimumSummary.ObservedTypes) != 1 ||
		minimumSummary.ObservedTypes[0] != eventfields.StoredValueTypeDouble ||
		minimumSummary.EventCount != 1 || minimumSummary.NullCount != 0 ||
		minimumSummary.MissingCount != 0 || minimumSummary.DistinctCount != 1 ||
		len(minimumSummary.TopValues) != 1 || minimumSummary.TopValues[0].Count != 1 {
		t.Fatalf("streamstats minimum field summary = %#v", minimumSummary)
	}
	if minimum, ok := minimumSummary.TopValues[0].Value.Double(); !ok || minimum != 200 {
		t.Fatalf(
			"streamstats minimum summary running_min = %v, double=%v",
			minimum,
			ok,
		)
	}

	minimumTimelineQuery := queryIntegrationCompileTimeline(
		t,
		minimumAnalysis,
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
	minimumBuckets, err := executor.ExecuteTimeline(ctx, minimumTimelineQuery)
	if err != nil {
		t.Fatalf(
			"execute streamstats minimum timeline: %v\nSQL: %s\nargs: %#v",
			err,
			minimumTimelineQuery.SQL,
			minimumTimelineQuery.Args,
		)
	}
	if len(minimumBuckets) != 1 || minimumBuckets[0].Count != 1 ||
		!minimumBuckets[0].AlignedStart.Equal(firstBucket) {
		t.Fatalf(
			"streamstats minimum timeline = %#v, want one event at %v",
			minimumBuckets,
			firstBucket,
		)
	}
}
