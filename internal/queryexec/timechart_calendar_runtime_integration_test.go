package queryexec

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type queryIntegrationCalendarRuntimeCase struct {
	name     string
	source   string
	timezone string
	span     string
	controls string
	earliest time.Time
	latest   time.Time
	events   []time.Time
}

// queryIntegrationTestCalendarBoundaryRuntime verifies the complete planner,
// compiler, ClickHouse, decoder, manager, and exact-bound publication path.
// Direct grid tests pin each boundary; these cases independently prove that
// SQL assignment cannot lose or duplicate events at civil gaps and folds.
func queryIntegrationTestCalendarBoundaryRuntime(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
) {
	t.Helper()

	indexTime, cases := queryIntegrationInsertCalendarRuntimeEvents(t, ctx, connection)
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := fmt.Sprintf(
				`index=main source="%s" | search host="calendar-runtime" | timechart span=%s %s count | sort 0 +_time`,
				test.source,
				test.span,
				test.controls,
			)
			if test.name == "sao-paulo-midnight-gap" {
				compiled := queryIntegrationCompileCalendarSearch(t, source, indexTime, test.earliest, test.latest, test.timezone)
				assertTimechartWorkOneRead(t, ctx, executor, compiled)
			}
			job, page := queryIntegrationRunCalendarSearch(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-calendar-runtime-"+test.name,
				source,
				test.earliest,
				test.latest,
				test.timezone,
			)
			queryIntegrationAssertCalendarEventConservation(t, job, page, "count", test.events)
		})
	}

	sao := cases[0]
	t.Run("sao-paulo-observed-filtered", func(t *testing.T) {
		job, page := queryIntegrationRunCalendarSearch(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-calendar-runtime-sao-observed",
			`index=main source="calendar-sao" | search host="calendar-runtime"`+
				` | timechart span=1d fixedrange=false cont=false partial=false count`+
				` | where count > 0 | sort 0 +_time`,
			sao.earliest,
			sao.latest,
			sao.timezone,
		)
		queryIntegrationAssertCalendarEventConservation(t, job, page, "count", sao.events)
	})

	t.Run("sao-paulo-repeated-timechart", func(t *testing.T) {
		job, page := queryIntegrationRunCalendarSearch(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-calendar-runtime-sao-repeated",
			`index=main source="calendar-sao" | search host="calendar-runtime"`+
				` | timechart span=1d fixedrange=true cont=false partial=true count`+
				` | where count > 0`+
				` | timechart span=2d fixedrange=true cont=false partial=true sum(count) AS total`+
				` | sort 0 +_time`,
			sao.earliest,
			sao.latest,
			sao.timezone,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("repeated calendar timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		if got := queryIntegrationCalendarDoubleTotal(t, page, "total"); got != 5 {
			t.Fatalf("repeated calendar total = %g, want %d", got, len(sao.events))
		}
		queryIntegrationAssertCalendarBounds(t, page)
	})
}

func queryIntegrationCompileCalendarSearch(
	t *testing.T,
	source string,
	indexTime, earliest, latest time.Time,
	timezone string,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       indexTime,
		SearchTimezone:    timezone,
		IndexTimeCutoff:   indexTime.Add(500 * time.Microsecond),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func queryIntegrationAssertCalendarEventConservation(
	t *testing.T,
	job searchjobs.Job,
	page searchjobs.ResultPage,
	valueColumn string,
	events []time.Time,
) {
	t.Helper()
	if job.State != searchjobs.StateCompleted {
		t.Fatalf("calendar timechart state = %v, failure=%#v", job.State, job.Failure)
	}
	queryIntegrationAssertCalendarBounds(t, page)
	valueIndex := queryIntegrationColumnIndex(t, page, valueColumn)
	total := uint64(0)
	membership := make([]int, len(events))
	for rowIndex, row := range page.Rows {
		earliest := queryIntegrationParseCalendarBound(t, row.TimeBucket.Earliest)
		latest := queryIntegrationParseCalendarBound(t, row.TimeBucket.Latest)
		wantCount := 0
		for eventIndex, eventTime := range events {
			if !eventTime.Before(earliest) && eventTime.Before(latest) {
				wantCount++
				membership[eventIndex]++
			}
		}
		gotCount := queryIntegrationCalendarCount(t, row.Values[valueIndex])
		if gotCount != uint64(wantCount) {
			t.Fatalf(
				"calendar row %d bounds [%s,%s) count = %d, want %d",
				rowIndex,
				earliest.Format(time.RFC3339Nano),
				latest.Format(time.RFC3339Nano),
				gotCount,
				wantCount,
			)
		}
		total += gotCount
	}
	if total != uint64(len(events)) {
		t.Fatalf("calendar result total = %d, want %d events", total, len(events))
	}
	for index, count := range membership {
		if count != 1 {
			t.Fatalf(
				"event %s belongs to %d published intervals, want exactly one",
				events[index].Format(time.RFC3339Nano),
				count,
			)
		}
	}
}

func queryIntegrationAssertCalendarBounds(t *testing.T, page searchjobs.ResultPage) {
	t.Helper()
	timeIndex := queryIntegrationColumnIndex(t, page, "_time")
	var previous time.Time
	for rowIndex, row := range page.Rows {
		if row.TimeBucket == nil {
			t.Fatalf("calendar row %d has no exact bounds", rowIndex)
		}
		earliest := queryIntegrationParseCalendarBound(t, row.TimeBucket.Earliest)
		latest := queryIntegrationParseCalendarBound(t, row.TimeBucket.Latest)
		if !earliest.Before(latest) {
			t.Fatalf("calendar row %d interval [%s,%s) is not positive", rowIndex, earliest, latest)
		}
		bucket, ok := row.Values[timeIndex].Time()
		if !ok || !bucket.Equal(earliest) {
			t.Fatalf("calendar row %d _time = %v (%v), want %v", rowIndex, bucket, ok, earliest)
		}
		if rowIndex > 0 && !previous.Before(earliest) {
			t.Fatalf("calendar row %d starts %v after prior start %v", rowIndex, earliest, previous)
		}
		previous = earliest
	}
}

func queryIntegrationCalendarDoubleTotal(t *testing.T, page searchjobs.ResultPage, name string) float64 {
	t.Helper()
	column := queryIntegrationColumnIndex(t, page, name)
	total := float64(0)
	for rowIndex, row := range page.Rows {
		value, ok := row.Values[column].Double()
		if !ok {
			t.Fatalf("calendar row %d total = %#v, want number", rowIndex, row.Values[column])
		}
		total += value
	}
	return total
}

func queryIntegrationCalendarCount(t *testing.T, value searchjobs.Value) uint64 {
	t.Helper()
	if unsigned, ok := value.Unsigned(); ok {
		return unsigned
	}
	t.Fatalf("calendar count = %#v, want unsigned integer", value)
	return 0
}

func queryIntegrationParseCalendarBound(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse calendar bound %q: %v", value, err)
	}
	return parsed
}

func queryIntegrationRunCalendarSearch(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
	id, source string,
	earliest, latest time.Time,
	timezone string,
) (searchjobs.Job, searchjobs.ResultPage) {
	t.Helper()
	resolved, err := searchtime.Resolve(
		earliest.Format(time.RFC3339Nano),
		latest.Format(time.RFC3339Nano),
		&timezone,
		indexTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(1),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(500 * time.Microsecond) },
		NewID:           func() string { return id },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	job, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           "owner",
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolved,
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := queryIntegrationWaitForTerminal(t, manager, job.ID)
	if terminal.State != searchjobs.StateCompleted {
		return terminal, searchjobs.ResultPage{}
	}
	page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return terminal, page
}

func queryIntegrationInsertCalendarRuntimeEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) (time.Time, []queryIntegrationCalendarRuntimeCase) {
	t.Helper()
	parse := func(value string) time.Time {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	cases := []queryIntegrationCalendarRuntimeCase{
		{
			name: "sao-paulo-midnight-gap", source: "calendar-sao", timezone: "America/Sao_Paulo", span: "1d", controls: "fixedrange=true cont=true partial=false",
			earliest: parse("2018-11-01T03:00:00Z"), latest: parse("2018-11-09T02:00:00Z"),
			events: []time.Time{parse("2018-11-02T03:30:00Z"), parse("2018-11-03T03:30:00Z"), parse("2018-11-04T03:30:00Z"), parse("2018-11-05T02:30:00Z"), parse("2018-11-06T02:30:00Z")},
		},
		{
			name: "cairo-month-gap", source: "calendar-cairo", timezone: "Africa/Cairo", span: "1mon", controls: "fixedrange=true cont=false partial=true",
			earliest: parse("2014-06-30T22:00:00Z"), latest: parse("2014-10-01T22:00:00Z"),
			events: []time.Time{parse("2014-07-15T10:00:00Z"), parse("2014-07-31T22:30:00Z"), parse("2014-08-31T21:30:00Z"), parse("2014-09-30T22:30:00Z")},
		},
		{
			name: "apia-skipped-date", source: "calendar-apia", timezone: "Pacific/Apia", span: "1d", controls: "fixedrange=true cont=false partial=true",
			earliest: parse("2011-12-28T10:00:00Z"), latest: parse("2012-01-02T10:00:00Z"),
			events: []time.Time{parse("2011-12-28T10:30:00Z"), parse("2011-12-29T10:30:00Z"), parse("2011-12-30T10:30:00Z"), parse("2011-12-31T10:30:00Z"), parse("2012-01-01T10:30:00Z")},
		},
		{
			name: "asuncion-quarter-gap", source: "calendar-asuncion", timezone: "America/Asuncion", span: "1q", controls: "fixedrange=true cont=false partial=true",
			earliest: parse("2017-07-01T04:00:00Z"), latest: parse("2018-01-01T03:00:00Z"),
			events: []time.Time{parse("2017-07-01T04:30:00Z"), parse("2017-10-01T04:30:00Z"), parse("2018-01-01T02:59:59Z")},
		},
		{
			name: "lima-year-gap", source: "calendar-lima", timezone: "America/Lima", span: "1y", controls: "fixedrange=true cont=false partial=true",
			earliest: parse("1985-01-01T05:00:00Z"), latest: parse("1988-01-01T05:00:00Z"),
			events: []time.Time{parse("1985-01-01T05:30:00Z"), parse("1986-01-01T05:30:00Z"), parse("1987-01-01T05:30:00Z")},
		},
		{
			name: "havana-midnight-fold", source: "calendar-havana", timezone: "America/Havana", span: "1d", controls: "fixedrange=true cont=false partial=true",
			earliest: parse("2018-11-03T04:00:00Z"), latest: parse("2018-11-06T05:00:00Z"),
			events: []time.Time{parse("2018-11-03T04:30:00Z"), parse("2018-11-04T04:30:00Z"), parse("2018-11-04T05:30:00Z"), parse("2018-11-05T05:30:00Z")},
		},
	}

	const insert = "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, " +
		"collector_id, ingest_source_kind, ingest_source_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	sequence := uint64(0)
	for _, test := range cases {
		for eventIndex, eventTime := range test.events {
			sequence++
			id := fmt.Sprintf("queryexec-%s-%d", test.source, eventIndex)
			message := fmt.Sprintf("calendar runtime %s event %d", test.name, eventIndex)
			if err := batch.Append(
				id, "tenant", "main", eventTime, indexTime,
				nil, uint8(1), "calendar-runtime", test.source, "test", nil, uint8(1), nil, &message, []byte(message),
				uint8(1), nil, nil, clickhousedriver.NewJSON(), []string{}, []uint8{},
				eventfields.CurrentFieldMetadataVersion,
				"collector", uint8(1), "collector", "timechart-calendar-runtime", sequence,
				indexTime.Add(24*time.Hour), uint64(1),
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return indexTime, cases
}
