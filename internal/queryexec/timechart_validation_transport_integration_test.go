package queryexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhousedriverlib "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartChronologicalValidationTransportAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1")
	}
	first := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	indexTime := first.Add(time.Hour)
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, []semanticBytesLineageEvent{
		{id: "one", at: first.Add(time.Second), host: "api", raw: []byte("x")},
	})
	compile := func(t *testing.T, source string) clickhouse.CompiledQuery {
		t.Helper()
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		visibility := uint64(1)
		logical, err := plan.Build(parsed, plan.Scope{
			TenantID: "tenant", AuthorizedIndexes: []string{semanticBytesLineageIndex},
			Earliest: first, Latest: first.Add(time.Minute), SearchStart: indexTime,
			IndexTimeCutoff: indexTime, VisibilityCutoff: &visibility,
			SearchTimezone: "America/Los_Angeles",
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
	for _, span := range []string{"1m", "250ms", "1d"} {
		for _, measure := range []string{"count", "count(value)", "sum(value)", "avg(value)", "p95(value)", "count BY host", "count(value) BY host", "sum(value) BY host", "avg(value) BY host", "p95(value) BY host"} {
			t.Run(span+"/"+measure, func(t *testing.T) {
				base := `index=` + semanticBytesLineageIndex + ` | eval value=9`
				terminal := ` | timechart span=` + span + ` ` + measure
				ordinary := compile(t, base+terminal)
				guarded := compile(t, base+` | eventstats min(missing) AS ignored`+terminal)
				want := timechartNativeTransportHeader(t, ctx, executor, ordinary)
				got := timechartNativeTransportHeader(t, ctx, executor, guarded)
				if !slices.Equal(got, want) {
					t.Fatalf("validation changed native transport: got %v, want %v", got, want)
				}
				if strings.Contains(guarded.SQL, " LIMIT 0") {
					t.Fatal("validation re-analyzes the timechart to infer its transport")
				}
				assertTimechartWorkOneRead(t, ctx, executor, guarded)
			})
		}
	}
	t.Run("work receipt", func(t *testing.T) {
		compiled := compile(t, `index=`+semanticBytesLineageIndex+` | eval payload="a,b" | makemv delim="," payload | mvexpand payload | timechart span=1m count BY host`)
		if !compiled.HasTimechartWorkReceipt() {
			t.Fatal("work receipt is missing")
		}
		header := timechartNativeTransportHeader(t, ctx, executor, compiled)
		if header[len(header)-1] != clickhouse.TimechartWorkRowsColumn+" UInt64" {
			t.Fatalf("work receipt transport = %v", header)
		}
		assertTimechartWorkOneRead(t, ctx, executor, compiled)
	})
	connection, ok := executor.connection.(clickhousedriverlib.Conn)
	if !ok {
		t.Fatal("native fixture connection is unavailable")
	}
	// A stored object is an unsupported extrema input. Keep that poison in a
	// source row whose values and even entire timechart result may be hidden.
	if err := connection.Exec(ctx, fmt.Sprintf(`INSERT INTO open_splunk.events
		SELECT * REPLACE (
			'invalid' AS event_id,
			CAST('{"bad":{"child":1}}' AS JSON) AS fields,
			['bad', 'bad.child'] AS field_names,
			[toUInt8(%d), toUInt8(%d)] AS field_types
		) FROM open_splunk.events AS original WHERE original.event_id = 'one'`, eventfields.StoredValueTypeObject, eventfields.StoredValueTypeSint64)); err != nil {
		t.Fatal(err)
	}
	var poisonRows uint64
	if err := connection.QueryRow(ctx, `SELECT count() FROM open_splunk.events WHERE event_id = 'invalid' AND has(field_names, 'bad')`).Scan(&poisonRows); err != nil || poisonRows != 1 {
		t.Fatalf("stored poison fixture rows=%d err=%v", poisonRows, err)
	}
	for _, suffix := range []string{
		` | where host="absent" | timechart span=1m count`,
		` | where host="absent" | timechart span=1m count BY host`,
		` | timechart span=1d cont=false partial=false count`,
		` | timechart span=1d cont=false partial=false sum(missing) BY host`,
		` | timechart span=1m count | head 1`,
		` | timechart span=1m count BY host | head 1`,
		` | timechart span=1m count | timechart span=1m sum(count)`,
		` | timechart span=1m count BY host | timechart span=1m sum(api)`,
	} {
		t.Run("invalid upstream"+suffix, func(t *testing.T) {
			compiled := compile(t, `index=`+semanticBytesLineageIndex+` | eventstats min(bad) AS ignored`+suffix)
			sink := &compositionSink{}
			err := executor.Execute(ctx, compiled, sink)
			if !errors.Is(err, searchjobs.ErrUnsupportedValue) || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("invalid upstream was hidden: err=%v schemas=%d rows=%d", err, sink.setCalls, len(sink.rows))
			}
		})
	}
	for _, terminal := range []string{
		`timechart span=1m cont=false count`,
		`timechart span=1m cont=false sum(missing)`,
		`timechart span=1m cont=false count BY host`,
		`timechart span=1m cont=false sum(missing) BY host`,
		`timechart span=1d cont=false partial=false count`,
		`timechart span=1d cont=false partial=false sum(missing) BY host`,
	} {
		t.Run("valid empty "+terminal, func(t *testing.T) {
			compiled := compile(t, `index=`+semanticBytesLineageIndex+
				` | eventstats min(missing) AS ignored | where host="absent" | `+terminal)
			sink := &compositionSink{}
			if err := executor.Execute(ctx, compiled, sink); err != nil || len(sink.rows) != 0 {
				t.Fatalf("valid empty source: err=%v rows=%d", err, len(sink.rows))
			}
		})
	}
}

func timechartNativeTransportHeader(t *testing.T, ctx context.Context, executor *Executor, compiled clickhouse.CompiledQuery) []string {
	t.Helper()
	settings, err := executor.settingsForContext(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := executor.connection.Query(clickhousedriver.Context(ctx, clickhousedriver.WithSettings(settings)), compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	header := make([]string, 0, len(rows.ColumnTypes()))
	for _, column := range rows.ColumnTypes() {
		header = append(header, column.Name()+" "+column.DatabaseTypeName())
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	return header
}
