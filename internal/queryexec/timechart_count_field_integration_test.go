package queryexec

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestTimechartCountField pins exact occurrence counting to
// the production ClickHouse transport. In particular, the split fixture makes
// source-row frequency disagree with occurrence totals so the top-ten choice
// cannot accidentally reuse bare-count ranking.
func queryIntegrationTestTimechartCountField(
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

	t.Run("fixed exact occurrences isolation and physical shape", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-fixed" | timechart span=5m count(occurrence) AS occurrences`
		compiled := queryIntegrationCompileSearchRange(
			t,
			source,
			indexTime,
			earliest,
			latest,
		)
		queryIntegrationAssertCountFieldPhysicalShape(
			t,
			ctx,
			connection,
			explainer,
			compiled,
			clickhouse.TimechartModeFixedFieldCount,
		)
		if !slices.Equal(compiled.OutputFields, []string{"_time", "occurrences"}) ||
			compiled.Timechart.ValueField != "occurrences" ||
			compiled.Timechart.ValueKind != clickhouse.TimechartValueKindInvalid {
			t.Fatalf("fixed count(field) contract = %#v / %v", compiled.Timechart, compiled.OutputFields)
		}

		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-fixed",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("fixed count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "occurrences"})
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{"occurrences": {8, 1, 0}},
		)
	})

	t.Run("fixed canonical output name preserves parentheses", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-fixed" | timechart span=5m count(occurrence)`
		compiled := queryIntegrationCompileSearchRange(
			t,
			source,
			indexTime,
			earliest,
			latest,
		)
		if compiled.Timechart == nil ||
			compiled.Timechart.Mode != clickhouse.TimechartModeFixedFieldCount ||
			compiled.Timechart.ValueField != "count(occurrence)" ||
			!slices.Equal(compiled.OutputFields, []string{"_time", "count(occurrence)"}) {
			t.Fatalf("canonical count(field) contract = %#v / %v", compiled.Timechart, compiled.OutputFields)
		}

		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-canonical",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("canonical count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "count(occurrence)"})
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{"count(occurrence)": {8, 1, 0}},
		)
	})

	t.Run("nonempty all-ineligible fixed input publishes a zero grid", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-ineligible",
			`index=main source="timechart-count-field-ineligible" | timechart span=5m count(occurrence) AS occurrences`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("all-ineligible fixed count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "occurrences"})
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{"occurrences": {0, 0, 0}},
		)
	})

	t.Run("projected-away fixed input remains zero without rebinding storage", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-projected",
			`index=main source="timechart-count-field-fixed" | fields - occurrence | timechart span=5m count(occurrence) AS occurrences`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("projected count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "occurrences"})
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{"occurrences": {0, 0, 0}},
		)
	})

	t.Run("empty fixed input publishes its static schema without rows", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-empty-fixed",
			`index=main source="timechart-count-field-empty" | timechart span=5m count(occurrence) AS occurrences`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 0 {
			t.Fatalf("empty fixed count(field) job=%#v page=%#v", job, page)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "occurrences"})
	})

	t.Run("split ranking uses occurrence totals and retains zero NULL", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-split" | timechart span=5m count(occurrence) BY segment`
		compiled := queryIntegrationCompileSearchRange(
			t,
			source,
			indexTime,
			earliest,
			latest,
		)
		queryIntegrationAssertCountFieldPhysicalShape(
			t,
			ctx,
			connection,
			explainer,
			compiled,
			clickhouse.TimechartModeRuntimeWide,
		)
		if !slices.Equal(compiled.OutputFields, []string{"_time"}) ||
			compiled.Timechart.MaxSeries != 12 ||
			compiled.Timechart.MaxLabelBytes == 0 ||
			compiled.Timechart.ValueField != "" ||
			compiled.Timechart.ValueKind != clickhouse.TimechartValueKindInvalid {
			t.Fatalf("split count(field) contract = %#v / %v", compiled.Timechart, compiled.OutputFields)
		}

		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-split",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("split count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		wantNames := []string{
			"_time", "a", "b", "c", "d", "e", "f", "g", "h", "i",
			"spike", "NULL", "OTHER",
		}
		queryIntegrationAssertCountFieldSchema(t, page, wantNames)
		for _, forbidden := range []string{"volume", "j", "poison"} {
			for _, column := range page.Schema.Columns {
				if column.Name == forbidden {
					t.Fatalf("split count(field) leaked or misranked %q: schema=%#v", forbidden, page.Schema)
				}
			}
		}
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{
				"a":     {19, 0, 0},
				"b":     {18, 0, 0},
				"c":     {17, 0, 0},
				"d":     {16, 0, 0},
				"e":     {15, 0, 0},
				"f":     {14, 0, 0},
				"g":     {13, 0, 0},
				"h":     {12, 0, 0},
				"i":     {11, 0, 0},
				"spike": {20, 0, 0},
				"NULL":  {0, 0, 0},
				"OTHER": {11, 0, 0},
			},
		)
	})

	t.Run("split zero-contribution ordinary and NULL domains remain visible", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-split-ineligible",
			`index=main source="timechart-count-field-split-ineligible" | timechart span=5m count(occurrence) BY segment`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("split all-ineligible count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time", "zero", "NULL"})
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			base,
			5*time.Minute,
			map[string][]uint64{
				"zero": {0, 0, 0},
				"NULL": {0, 0, 0},
			},
		)
	})

	t.Run("invalid split domain fails atomically despite zero occurrences", func(t *testing.T) {
		const source = `index=main source="timechart-count-field-invalid-split" | timechart span=5m count(occurrence) BY segment`
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) ||
			sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"zero-occurrence invalid split direct execution: err=%v schema calls=%d schema=%#v rows=%d",
				err,
				sink.setCalls,
				sink.schema,
				len(sink.rows),
			)
		}
		job, _ := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-invalid-split",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL ||
			job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("zero-occurrence invalid split manager job = %#v", job)
		}
	})

	t.Run("empty split input publishes only the fixed time schema", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-count-field-empty-split",
			`index=main source="timechart-count-field-empty" | timechart span=5m count(occurrence) BY segment`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 0 {
			t.Fatalf("empty split count(field) job=%#v page=%#v", job, page)
		}
		queryIntegrationAssertCountFieldSchema(t, page, []string{"_time"})
	})
}

func queryIntegrationAssertCountFieldPhysicalShape(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	explainer *Explainer,
	compiled clickhouse.CompiledQuery,
	wantMode clickhouse.TimechartMode,
) {
	t.Helper()
	if compiled.Timechart == nil ||
		compiled.Timechart.Mode != wantMode ||
		compiled.Timechart.BucketCount != 3 ||
		compiled.Timechart.Span != 5*time.Minute {
		t.Fatalf("count(field) compiled shape = %#v", compiled.Timechart)
	}
	upperSQL := strings.ToUpper(compiled.SQL)
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 ||
		strings.Contains(upperSQL, "ARRAY JOIN") ||
		strings.Contains(compiled.SQL, "arrayJoin(") {
		t.Fatalf("count(field) timechart rescans or expands source rows:\n%s", compiled.SQL)
	}
	explained, err := explainer.Explain(ctx, compiled)
	if err != nil {
		t.Fatalf("EXPLAIN count(field) timechart: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	physical := queryIntegrationAssertStructuredExplain(t, explained)
	if len(physical.Reads) != 1 || queryIntegrationPlanContainsArrayJoin(physical) {
		t.Fatalf("count(field) timechart physical plan rescans or expands rows: %#v", physical)
	}
	actions := queryIntegrationExplainActions(t, ctx, connection, compiled)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("count(field) timechart EXPLAIN actions expand rows:\n%s", actions)
	}
}

func queryIntegrationAssertCountFieldSchema(
	t *testing.T,
	page searchjobs.ResultPage,
	wantNames []string,
) {
	t.Helper()
	if len(page.Schema.Columns) != len(wantNames) {
		t.Fatalf("count(field) timechart schema = %#v, want %v", page.Schema, wantNames)
	}
	for index, name := range wantNames {
		column := page.Schema.Columns[index]
		wantKind := searchjobs.ValueKindUnsigned
		if index == 0 {
			wantKind = searchjobs.ValueKindTime
		}
		if column.Name != name || column.Kind != wantKind ||
			column.Nullable || column.Multivalue {
			t.Fatalf("count(field) timechart column %d = %#v, want %q/%v", index, column, name, wantKind)
		}
	}
}

type queryIntegrationTimechartCountFieldEvent struct {
	id            string
	tenant        string
	indexName     string
	source        string
	at            time.Time
	segmentSet    bool
	segment       any
	occurrenceSet bool
	occurrence    clickhousedriver.Dynamic
	objectParent  bool
	visibility    uint64
}

func queryIntegrationInsertTimechartCountFieldEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) (time.Time, time.Time) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	// Keep this fixture outside the ranges of the older shared timechart fixtures.
	base := time.Now().UTC().Add(-4 * time.Hour).Truncate(5 * time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	events := []queryIntegrationTimechartCountFieldEvent{
		// The first fixed bucket has eight occurrences: false, zero, empty String,
		// one flattened object parent, and four non-null immediate MV members.
		{id: "fixed-missing", source: "timechart-count-field-fixed", at: base.Add(10 * time.Second)},
		{id: "fixed-null", source: "timechart-count-field-fixed", at: base.Add(20 * time.Second), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(nil)},
		{id: "fixed-empty-mv", source: "timechart-count-field-fixed", at: base.Add(30 * time.Second), occurrenceSet: true, occurrence: queryIntegrationCountFieldArray()},
		{id: "fixed-false", source: "timechart-count-field-fixed", at: base.Add(40 * time.Second), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(false)},
		{id: "fixed-zero", source: "timechart-count-field-fixed", at: base.Add(50 * time.Second), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(int64(0))},
		{id: "fixed-empty-string", source: "timechart-count-field-fixed", at: base.Add(time.Minute), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic("")},
		{id: "fixed-object", source: "timechart-count-field-fixed", at: base.Add(70 * time.Second), objectParent: true},
		{id: "fixed-mv", source: "timechart-count-field-fixed", at: base.Add(80 * time.Second), occurrenceSet: true, occurrence: queryIntegrationCountFieldArray(int64(7), nil, false, "", uint64(0))},
		{id: "fixed-second", source: "timechart-count-field-fixed", at: base.Add(6 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic("present")},
		{id: "fixed-second-null", source: "timechart-count-field-fixed", at: base.Add(7 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(nil)},
		// Scope poison must be discarded before occurrence aggregation.
		{id: "fixed-other-tenant", tenant: "other", source: "timechart-count-field-fixed", at: base.Add(2 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		{id: "fixed-other-index", indexName: "other", source: "timechart-count-field-fixed", at: base.Add(2 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		{id: "fixed-future-visibility", source: "timechart-count-field-fixed", at: base.Add(2 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50), visibility: 2},
		{id: "fixed-before", source: "timechart-count-field-fixed", at: base.Add(-time.Nanosecond), occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		{id: "fixed-latest", source: "timechart-count-field-fixed", at: base.Add(15 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		// A nonempty input with no eligible occurrences must still own the full grid.
		{id: "ineligible-missing", source: "timechart-count-field-ineligible", at: base.Add(time.Minute)},
		{id: "ineligible-null", source: "timechart-count-field-ineligible", at: base.Add(6 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(nil)},
		{id: "ineligible-empty", source: "timechart-count-field-ineligible", at: base.Add(11 * time.Minute), occurrenceSet: true, occurrence: queryIntegrationCountFieldArray()},
	}

	// One-row multivalues determine the selected top ten by occurrence totals.
	// The 25-row volume series has the largest source-row count but contributes
	// zero, so row-frequency ranking would displace a selected series.
	for _, ranked := range []struct {
		label string
		score int
	}{
		{label: "spike", score: 20},
		{label: "a", score: 19},
		{label: "b", score: 18},
		{label: "c", score: 17},
		{label: "d", score: 16},
		{label: "e", score: 15},
		{label: "f", score: 14},
		{label: "g", score: 13},
		{label: "h", score: 12},
		{label: "i", score: 11},
		// Tie the cutoff score so deterministic normalized-label ordering selects
		// i and collapses j into OTHER.
		{label: "j", score: 11},
	} {
		events = append(events, queryIntegrationTimechartCountFieldEvent{
			id:            "split-" + ranked.label,
			source:        "timechart-count-field-split",
			at:            base.Add(time.Minute),
			segmentSet:    true,
			segment:       ranked.label,
			occurrenceSet: true,
			occurrence:    queryIntegrationRepeatedCountFieldOccurrences(ranked.score),
		})
	}
	for index := range 25 {
		events = append(events, queryIntegrationTimechartCountFieldEvent{
			id:         fmt.Sprintf("split-volume-%02d", index),
			source:     "timechart-count-field-split",
			at:         base.Add(2 * time.Minute),
			segmentSet: true,
			segment:    "volume",
		})
	}
	events = append(events,
		// Missing and explicit-null splits retain NULL even though both measures
		// contribute zero.
		queryIntegrationTimechartCountFieldEvent{id: "split-null-missing", source: "timechart-count-field-split", at: base.Add(3 * time.Minute)},
		queryIntegrationTimechartCountFieldEvent{id: "split-null-explicit", source: "timechart-count-field-split", at: base.Add(4 * time.Minute), segmentSet: true, segment: nil, occurrenceSet: true, occurrence: queryIntegrationCountFieldArray()},
		queryIntegrationTimechartCountFieldEvent{id: "split-other-tenant", tenant: "other", source: "timechart-count-field-split", at: base.Add(time.Minute), segmentSet: true, segment: "poison", occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		queryIntegrationTimechartCountFieldEvent{id: "split-other-index", indexName: "other", source: "timechart-count-field-split", at: base.Add(time.Minute), segmentSet: true, segment: "poison", occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50)},
		queryIntegrationTimechartCountFieldEvent{id: "split-future-visibility", source: "timechart-count-field-split", at: base.Add(time.Minute), segmentSet: true, segment: "poison", occurrenceSet: true, occurrence: queryIntegrationRepeatedCountFieldOccurrences(50), visibility: 2},
		// An ordinary and NULL domain with only missing/null/empty measures must not
		// disappear merely because every aggregate contribution is zero.
		queryIntegrationTimechartCountFieldEvent{id: "split-zero-missing", source: "timechart-count-field-split-ineligible", at: base.Add(time.Minute), segmentSet: true, segment: "zero"},
		queryIntegrationTimechartCountFieldEvent{id: "split-zero-null", source: "timechart-count-field-split-ineligible", at: base.Add(6 * time.Minute), segmentSet: true, segment: "zero", occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(nil)},
		queryIntegrationTimechartCountFieldEvent{id: "split-zero-empty", source: "timechart-count-field-split-ineligible", at: base.Add(11 * time.Minute), segmentSet: true, segment: "zero", occurrenceSet: true, occurrence: queryIntegrationCountFieldArray()},
		queryIntegrationTimechartCountFieldEvent{id: "split-zero-null-domain-missing", source: "timechart-count-field-split-ineligible", at: base.Add(2 * time.Minute)},
		queryIntegrationTimechartCountFieldEvent{id: "split-zero-null-domain-explicit", source: "timechart-count-field-split-ineligible", at: base.Add(7 * time.Minute), segmentSet: true, segment: nil, occurrenceSet: true, occurrence: queryIntegrationCountFieldDynamic(nil)},
		// Split-domain validation is independent of aggregate contribution. This
		// numeric label is unsupported even though occurrence is absent and the
		// row would otherwise contribute zero.
		queryIntegrationTimechartCountFieldEvent{id: "split-invalid-zero-occurrence", source: "timechart-count-field-invalid-split", at: base.Add(time.Minute), segmentSet: true, segment: int64(7)},
	)

	for index, event := range events {
		document := clickhousedriver.NewJSON()
		fieldNames := make([]string, 0, 2)
		if event.segmentSet {
			document.SetValueAtPath("segment", queryIntegrationCountFieldDynamic(event.segment))
			fieldNames = append(fieldNames, "segment")
		}
		if event.objectParent {
			document.SetValueAtPath("occurrence.child", queryIntegrationCountFieldDynamic("leaf"))
			fieldNames = append(fieldNames, "occurrence.child")
		} else if event.occurrenceSet {
			document.SetValueAtPath("occurrence", event.occurrence)
			fieldNames = append(fieldNames, "occurrence")
		}
		slices.Sort(fieldNames)
		tenant := event.tenant
		if tenant == "" {
			tenant = "tenant"
		}
		indexName := event.indexName
		if indexName == "" {
			indexName = "main"
		}
		visibility := event.visibility
		if visibility == 0 {
			visibility = 1
		}
		message := "timechart count(field) " + event.id
		if err := batch.Append(
			"queryexec-timechart-count-field-"+event.id,
			tenant,
			indexName,
			event.at,
			indexTime,
			nil,
			uint8(1),
			"host",
			event.source,
			"test",
			nil,
			uint8(1),
			nil,
			&message,
			[]byte(message),
			uint8(1),
			nil,
			nil,
			document,
			fieldNames,
			"collector",
			"timechart-count-field-batch",
			uint64(index+1),
			indexTime.Add(24*time.Hour),
			visibility,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime
}

func queryIntegrationCountFieldDynamic(value any) clickhousedriver.Dynamic {
	if text, ok := value.(string); ok {
		return clickhousedriver.NewDynamicWithType(text, "String")
	}
	return clickhousedriver.NewDynamic(value)
}

func queryIntegrationCountFieldArray(values ...any) clickhousedriver.Dynamic {
	items := make([]clickhousedriver.Dynamic, len(values))
	for index, value := range values {
		items[index] = queryIntegrationCountFieldDynamic(value)
	}
	return clickhousedriver.NewDynamicWithType(items, "Array(Dynamic)")
}

func queryIntegrationRepeatedCountFieldOccurrences(count int) clickhousedriver.Dynamic {
	values := make([]any, count)
	for index := range values {
		values[index] = int64(index)
	}
	return queryIntegrationCountFieldArray(values...)
}
