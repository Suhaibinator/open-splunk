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

// queryIntegrationTestSplitNumericTimechart pins the runtime-wide numeric
// transport against real ClickHouse behavior. The fixture deliberately makes
// event frequency disagree with numeric score: the twenty-row volume series is
// omitted while the one-row spike series is selected.
func queryIntegrationTestSplitNumericTimechart(
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
	type measureCase struct {
		name      string
		function  string
		valueKind clickhouse.TimechartValueKind
		want      map[string][]*float64
	}
	measures := []measureCase{
		{
			name: "sum", function: "sum", valueKind: clickhouse.TimechartValueKindSum,
			want: map[string][]*float64{
				"a":     splitNumericValues(15, 100, nil),
				"b":     splitNumericValues(90, 0, nil),
				"c":     splitNumericValues(80, nil, nil),
				"d":     splitNumericValues(70, nil, nil),
				"e":     splitNumericValues(60, nil, nil),
				"f":     splitNumericValues(50, nil, nil),
				"g":     splitNumericValues(40, nil, nil),
				"h":     splitNumericValues(35, nil, nil),
				"i":     splitNumericValues(30, nil, nil),
				"spike": splitNumericValues(1_000, nil, nil),
				"NULL":  splitNumericValues(12, nil, nil),
				"OTHER": splitNumericValues(50, nil, nil),
			},
		},
		{
			name: "average", function: "avg", valueKind: clickhouse.TimechartValueKindAverage,
			want: map[string][]*float64{
				"a":     splitNumericValues(7.5, 100, nil),
				"b":     splitNumericValues(90, 0, nil),
				"c":     splitNumericValues(80, nil, nil),
				"d":     splitNumericValues(70, nil, nil),
				"e":     splitNumericValues(60, nil, nil),
				"f":     splitNumericValues(50, nil, nil),
				"g":     splitNumericValues(40, nil, nil),
				"h":     splitNumericValues(35, nil, nil),
				"i":     splitNumericValues(30, nil, nil),
				"spike": splitNumericValues(1_000, nil, nil),
				"NULL":  splitNumericValues(4, nil, nil),
				// OTHER combines one j value of 30 with twenty volume
				// values of 1. This must be the member-weighted 50/21,
				// not the average-of-series-averages value 15.5.
				"OTHER": splitNumericValues(50.0/21.0, nil, nil),
			},
		},
	}

	wantNames := []string{
		"_time", "a", "b", "c", "d", "e", "f", "g", "h", "i",
		"spike", "NULL", "OTHER",
	}
	for _, test := range measures {
		t.Run(test.name+" ranking normalization nullability and physical shape", func(t *testing.T) {
			source := `index=main source="timechart-numeric-split" | timechart span=5m ` +
				test.function + `(metric) AS ignored_alias BY path`
			compiled := queryIntegrationCompileSearchRange(
				t,
				source,
				indexTime,
				earliest,
				latest,
			)
			if compiled.Timechart == nil ||
				compiled.Timechart.Mode != clickhouse.TimechartModeRuntimeWideValue ||
				compiled.Timechart.ValueKind != test.valueKind ||
				compiled.Timechart.ValueField != "" ||
				compiled.Timechart.BucketCount != 3 ||
				compiled.Timechart.MaxSeries != 12 ||
				compiled.Timechart.BucketCount > 10_000 {
				t.Fatalf("compiled split %s timechart contract = %#v", test.name, compiled.Timechart)
			}
			upperSQL := strings.ToUpper(compiled.SQL)
			if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 ||
				strings.Contains(upperSQL, "ARRAY JOIN") {
				t.Fatalf("split %s timechart rescans or expands source rows:\n%s", test.name, compiled.SQL)
			}
			explained, err := explainer.Explain(ctx, compiled)
			if err != nil {
				t.Fatalf("EXPLAIN split %s timechart: %v\nSQL: %s\nargs: %#v", test.name, err, compiled.SQL, compiled.Args)
			}
			physical := queryIntegrationAssertStructuredExplain(t, explained)
			if len(physical.Reads) != 1 || queryIntegrationPlanContainsArrayJoin(physical) {
				t.Fatalf("split %s timechart physical plan rescans or expands rows: %#v", test.name, physical)
			}
			actions := queryIntegrationExplainActions(t, ctx, connection, compiled)
			if strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("split %s timechart EXPLAIN actions expand rows:\n%s", test.name, actions)
			}

			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-numeric-split-"+test.name,
				source,
				earliest,
				latest,
			)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("split %s timechart state = %v, failure=%#v", test.name, job.State, job.Failure)
			}
			queryIntegrationAssertSplitNumericSchema(t, page, wantNames)
			queryIntegrationAssertSplitNumericMatrix(t, page, base, test.want)
		})
	}

	for _, test := range measures {
		t.Run(test.name+" all-ineligible input retains nullable grid", func(t *testing.T) {
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-numeric-split-ineligible-"+test.name,
				`index=main source="timechart-numeric-split-ineligible" | timechart span=5m `+
					test.function+`(metric) BY path`,
				earliest,
				latest,
			)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("split %s all-ineligible state = %v, failure=%#v", test.name, job.State, job.Failure)
			}
			queryIntegrationAssertSplitNumericSchema(t, page, []string{"_time", "bad"})
			queryIntegrationAssertSplitNumericMatrix(
				t,
				page,
				base,
				map[string][]*float64{"bad": splitNumericValues(nil, nil, nil)},
			)
		})

		t.Run(test.name+" empty input publishes runtime schema only", func(t *testing.T) {
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-numeric-split-empty-"+test.name,
				`index=main source="timechart-numeric-split-empty" | timechart span=5m `+
					test.function+`(metric) BY path`,
				earliest,
				latest,
			)
			if job.State != searchjobs.StateCompleted || len(page.Rows) != 0 ||
				len(page.Schema.Columns) != 1 ||
				page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) {
				t.Fatalf("split %s empty job=%#v page=%#v", test.name, job, page)
			}
		})
	}

	t.Run("raw label budget precedes top-series collapse", func(t *testing.T) {
		// This one-bucket relation has fourteen raw split groups but only twelve
		// public series. Sizing max_rows_to_group_by from output width would reject
		// the exact ranking before the top-ten/NULL/OTHER collapse can run.
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-numeric-split-raw-groups",
			`index=main source="timechart-numeric-split" | timechart span=5m sum(metric) BY path`,
			earliest,
			earliest.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 1 ||
			len(page.Schema.Columns) != 13 {
			t.Fatalf("raw-group split timechart job=%#v page=%#v", job, page)
		}
		for _, column := range page.Schema.Columns {
			if column.Name == "unused" {
				t.Fatalf("zero-score overflow label escaped OTHER: schema=%#v", page.Schema)
			}
		}
	})

	t.Run("invalid split domain fails atomically", func(t *testing.T) {
		const source = `index=main source="timechart-numeric-split-invalid" | timechart span=5m sum(metric) BY path`
		compiled := queryIntegrationCompileSearchRange(t, source, indexTime, earliest, latest)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) ||
			sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"invalid split direct execution: err=%v schema calls=%d schema=%#v rows=%d",
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
			"queryexec-timechart-numeric-split-invalid",
			source,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL ||
			job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("invalid split manager job = %#v", job)
		}
	})
}

func queryIntegrationAssertSplitNumericSchema(
	t *testing.T,
	page searchjobs.ResultPage,
	wantNames []string,
) {
	t.Helper()
	if len(page.Schema.Columns) != len(wantNames) {
		t.Fatalf("split numeric timechart schema = %#v, want names %v", page.Schema, wantNames)
	}
	for index, wantName := range wantNames {
		column := page.Schema.Columns[index]
		if index == 0 {
			if column != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) {
				t.Fatalf("split numeric timechart time column = %#v", column)
			}
			continue
		}
		want := searchjobs.Column{
			Name:     wantName,
			Kind:     searchjobs.ValueKindDouble,
			Nullable: true,
		}
		if column != want {
			t.Fatalf("split numeric timechart column %d = %#v, want %#v (schema %#v)", index, column, want, page.Schema)
		}
	}
}

func queryIntegrationAssertSplitNumericMatrix(
	t *testing.T,
	page searchjobs.ResultPage,
	base time.Time,
	want map[string][]*float64,
) {
	t.Helper()
	const bucketCount = 3
	if len(page.Rows) != bucketCount {
		t.Fatalf("split numeric timechart rows = %d, want %d", len(page.Rows), bucketCount)
	}
	for rowIndex, row := range page.Rows {
		bucket, ok := row.Values[0].Time()
		wantBucket := base.Add(time.Duration(rowIndex) * 5 * time.Minute)
		if !ok || !bucket.Equal(wantBucket) {
			t.Fatalf("split numeric row %d bucket = %v, %v, want %v", rowIndex, bucket, ok, wantBucket)
		}
	}
	for series, values := range want {
		if len(values) != bucketCount {
			t.Fatalf("invalid split numeric fixture for %q: %d buckets", series, len(values))
		}
		column := queryIntegrationColumnIndex(t, page, series)
		for rowIndex, expected := range values {
			actual := page.Rows[rowIndex].Values[column]
			if expected == nil {
				if !actual.IsNull() {
					t.Fatalf("split numeric row %d series %q = %#v, want null (row %#v)", rowIndex, series, actual, page.Rows[rowIndex])
				}
				continue
			}
			queryIntegrationAssertDouble(
				t,
				actual,
				*expected,
				fmt.Sprintf("split numeric row %d series %q", rowIndex, series),
			)
		}
	}
}

func splitNumericValues(values ...any) []*float64 {
	result := make([]*float64, len(values))
	for index, value := range values {
		if value == nil {
			continue
		}
		var numeric float64
		switch typed := value.(type) {
		case int:
			numeric = float64(typed)
		case float64:
			numeric = typed
		default:
			panic(fmt.Sprintf("split numeric test value %T is not numeric", value))
		}
		result[index] = new(float64)
		*result[index] = numeric
	}
	return result
}

func queryIntegrationInsertSplitNumericTimechartEvents(
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
	// Keep these rows outside the default one-hour event ranges exercised by
	// unrelated main-index cases in the shared integration container.
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(5 * time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	type fixtureEvent struct {
		id         string
		tenant     string
		indexName  string
		source     string
		at         time.Time
		pathSet    bool
		path       any
		metricSet  bool
		metric     clickhousedriver.Dynamic
		visibility uint64
	}
	numericList := func(values ...any) clickhousedriver.Dynamic {
		items := make([]clickhousedriver.Dynamic, len(values))
		for index, value := range values {
			items[index] = clickhousedriver.NewDynamic(value)
		}
		return clickhousedriver.NewDynamicWithType(items, "Array(Dynamic)")
	}
	number := func(value any) clickhousedriver.Dynamic {
		if _, ok := value.(string); ok {
			// Pin scalar Strings explicitly. When the first value seen for a JSON
			// path is Array(Dynamic), clickhouse-go otherwise infers later Go
			// strings as byte arrays, which would be genuine numeric multivalues.
			return clickhousedriver.NewDynamicWithType(value, "String")
		}
		return clickhousedriver.NewDynamic(value)
	}
	events := []fixtureEvent{
		// a proves immediate multivalue normalization. The extra missing and
		// nonnumeric rows must not contribute to either sum or average.
		{id: "a-list", at: base.Add(time.Minute), pathSet: true, path: "a", metricSet: true, metric: numericList(int64(10), "5", "not-numeric", nil)},
		{id: "a-missing", at: base.Add(2 * time.Minute), pathSet: true, path: "a"},
		{id: "a-text", at: base.Add(3 * time.Minute), pathSet: true, path: "a", metricSet: true, metric: number("not-numeric")},
		{id: "a-second-bucket", at: base.Add(6 * time.Minute), pathSet: true, path: "a", metricSet: true, metric: number(int64(100))},
		{id: "b", at: base.Add(time.Minute), pathSet: true, path: "b", metricSet: true, metric: number(int64(90))},
		// The second b bucket is a real zero with two eligible members. It must
		// remain distinguishable from every nullable gap in the result.
		{id: "b-zero-positive", at: base.Add(6 * time.Minute), pathSet: true, path: "b", metricSet: true, metric: number(int64(7))},
		{id: "b-zero-negative", at: base.Add(7 * time.Minute), pathSet: true, path: "b", metricSet: true, metric: number(int64(-7))},
		{id: "c", at: base.Add(time.Minute), pathSet: true, path: "c", metricSet: true, metric: number(int64(80))},
		{id: "d", at: base.Add(time.Minute), pathSet: true, path: "d", metricSet: true, metric: number(int64(70))},
		{id: "e", at: base.Add(time.Minute), pathSet: true, path: "e", metricSet: true, metric: number(int64(60))},
		{id: "f", at: base.Add(time.Minute), pathSet: true, path: "f", metricSet: true, metric: number(int64(50))},
		{id: "g", at: base.Add(time.Minute), pathSet: true, path: "g", metricSet: true, metric: number(int64(40))},
		{id: "h", at: base.Add(time.Minute), pathSet: true, path: "h", metricSet: true, metric: number(int64(35))},
		{id: "i", at: base.Add(time.Minute), pathSet: true, path: "i", metricSet: true, metric: number(int64(30))},
		{id: "spike", at: base.Add(time.Minute), pathSet: true, path: "spike", metricSet: true, metric: number(int64(1_000))},
		// i and j tie at the selection boundary; lexical ordering retains i.
		// The two omitted labels make OTHER's weighted average 50/21.
		{id: "j", at: base.Add(time.Minute), pathSet: true, path: "j", metricSet: true, metric: number(int64(30))},
		// A thirteenth ordinary label with no eligible measure member proves that
		// pre-ranking group work is budgeted independently from the output width.
		{id: "unused-ineligible", at: base.Add(time.Minute), pathSet: true, path: "unused"},
		{id: "j-ineligible", at: base.Add(6 * time.Minute), pathSet: true, path: "j"},
		{id: "volume-ineligible", at: base.Add(6 * time.Minute), pathSet: true, path: "volume", metricSet: true, metric: number("not-numeric")},
		// Missing and explicit-null split values both belong to NULL. The list
		// contains two eligible immediate members and two ignored members.
		{id: "split-missing", at: base.Add(time.Minute), metricSet: true, metric: number(int64(7))},
		{id: "split-null", at: base.Add(time.Minute), pathSet: true, path: nil, metricSet: true, metric: numericList(int64(3), "2", "not-numeric", nil)},
		// Poison values pin the tenant, index, half-open time, and visibility
		// scope before ranking. Any leaked row displaces the expected domain.
		{id: "before", at: base.Add(-time.Nanosecond), pathSet: true, path: "poison", metricSet: true, metric: number(int64(999_999))},
		{id: "latest", at: base.Add(15 * time.Minute), pathSet: true, path: "poison", metricSet: true, metric: number(int64(999_999))},
		{id: "other-tenant", tenant: "other", at: base.Add(time.Minute), pathSet: true, path: "poison", metricSet: true, metric: number(int64(999_999))},
		{id: "other-index", indexName: "other", at: base.Add(time.Minute), pathSet: true, path: "poison", metricSet: true, metric: number(int64(999_999))},
		{id: "future-visibility", at: base.Add(time.Minute), pathSet: true, path: "poison", metricSet: true, metric: number(int64(999_999)), visibility: 2},
		// A nonempty relation whose measure never yields an eligible immediate
		// member still owns a runtime series and a complete nullable grid.
		{id: "ineligible-missing", source: "timechart-numeric-split-ineligible", at: base.Add(time.Minute), pathSet: true, path: "bad"},
		{id: "ineligible-text", source: "timechart-numeric-split-ineligible", at: base.Add(6 * time.Minute), pathSet: true, path: "bad", metricSet: true, metric: number("not-numeric")},
		{id: "ineligible-null", source: "timechart-numeric-split-ineligible", at: base.Add(11 * time.Minute), pathSet: true, path: "bad", metricSet: true, metric: number(nil)},
		{id: "invalid-split", source: "timechart-numeric-split-invalid", at: base.Add(time.Minute), pathSet: true, path: int64(7), metricSet: true, metric: number(int64(10))},
	}
	for index := range 20 {
		events = append(events, fixtureEvent{
			id:        fmt.Sprintf("volume-%02d", index),
			at:        base.Add(time.Minute),
			pathSet:   true,
			path:      "volume",
			metricSet: true,
			metric:    number(int64(1)),
		})
	}
	for index, event := range events {
		message := "split numeric timechart " + event.id
		document := clickhousedriver.NewJSON()
		var fieldNames []string
		if event.pathSet {
			document.SetValueAtPath("path", clickhousedriver.NewDynamic(event.path))
			fieldNames = append(fieldNames, "path")
		}
		if event.metricSet {
			document.SetValueAtPath("metric", event.metric)
			fieldNames = append(fieldNames, "metric")
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
		source := event.source
		if source == "" {
			source = "timechart-numeric-split"
		}
		visibility := event.visibility
		if visibility == 0 {
			visibility = 1
		}
		if err := batch.Append(
			"queryexec-timechart-numeric-split-"+event.id,
			tenant,
			indexName,
			event.at,
			indexTime,
			nil,
			uint8(1),
			"host",
			source,
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
			"timechart-numeric-split-batch",
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
