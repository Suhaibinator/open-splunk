package clickhouse

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	driverproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestStaticTimechartFiniteValidationMatchesAggregate(t *testing.T) {
	for _, measure := range []string{"sum", "avg", "p95"} {
		compiled := compileSPL(t, `index=gradethis | timechart span=1h `+measure+`(metric) AS value | head 1`)
		guard := `NOT isFinite(ifNull("__os_timechart_value", 0))`
		if strings.Contains(compiled.SQL, guard) != (measure == "p95") {
			t.Fatalf("%s finite guard disagrees with terminal aggregate: %s", measure, compiled.SQL)
		}
		if !compiled.RequiresAtomicResult() || !strings.Contains(compiled.SQL, "__os_chronological_validation") {
			t.Fatal("removed complete-result validation barrier")
		}
	}
}

func TestTimechartTypedFloat64ContinuationPreservesIEEEBits(t *testing.T) {
	values := []float64{math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000001234), math.Float64frombits(0xfff8000000005678), math.Copysign(0, -1)}
	for _, kind := range []string{"Float64", "Nullable(Float64)"} {
		t.Run(kind, func(t *testing.T) {
			compiled := compileSPL(t, `index=gradethis | timechart span=1h sum(metric) BY host | table _time api`)
			columns := []RelationColumn{{Name: "_time", Type: "DateTime64(9, 'UTC')"}, {Name: "api", Type: kind}}
			first := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
			rows := make([][]any, len(values))
			for i, value := range values {
				rows[i] = []any{first.Add(time.Duration(i) * time.Hour), value}
			}
			next, err := compiled.ContinueContext(context.Background(), columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if !next.HasValidExecutionSeal() || !next.IsContinuationOf(compiled) {
				t.Fatal("continuation is not sealed")
			}
			clone, ok := next.CloneForExecution()
			if !ok || !clone.EqualForExecution(next) {
				t.Fatal("IEEE clone changed authority")
			}
			if _, ok := next.RetainedBytes(); !ok {
				t.Fatal("IEEE relation cannot be accounted")
			}
			rows[0][1] = float64(42)
			if !next.HasValidExecutionSeal() {
				t.Fatal("caller mutation changed relation backing")
			}
			tables, err := clone.ExternalTablesForExecution(context.Background())
			if err != nil || len(tables) != 1 {
				t.Fatalf("tables=%d err=%v", len(tables), err)
			}
			buffer := &chproto.Buffer{}
			if err := tables[0].Block().Encode(buffer, 0); err != nil {
				t.Fatal(err)
			}
			decoded := driverproto.NewBlock()
			if err := decoded.Decode(chproto.NewReader(bytes.NewReader(buffer.Buf)), 0); err != nil {
				t.Fatal(err)
			}
			if got := string(decoded.Columns[1].Type()); got != kind {
				t.Fatalf("native column type=%s want=%s", got, kind)
			}
			for i, want := range values {
				var got float64
				if err := decoded.Columns[1].ScanRow(&got, i); err != nil {
					t.Fatal(err)
				}
				if math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("row%d bits=%x want=%x", i, math.Float64bits(got), math.Float64bits(want))
				}
			}
			// Payload changes affect the commitment even though both are NaN.
			rows[0][1] = values[0]
			rows[2][1] = math.Float64frombits(0x7ff8000000001235)
			other, err := compiled.ContinueContext(context.Background(), columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if next.EqualForExecution(other) {
				t.Fatal("distinct NaN payloads share a seal")
			}
		})
	}
}
