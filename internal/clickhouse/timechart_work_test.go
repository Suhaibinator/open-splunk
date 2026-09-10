package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTimechartWorkReceiptAuthority(t *testing.T) {
	source := compileSPL(t, `index=gradethis | eval payload="x,y" | makemv delim="," payload | mvexpand payload | timechart span=1h count BY host | head 1`)
	if !source.HasTimechartWorkReceipt() || !source.HasValidExecutionSeal() {
		t.Fatal("missing sealed receipt declaration")
	}
	columns := []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {"api", "UInt64"}}
	rows := [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(1)}}
	if _, err := source.ContinueContext(context.Background(), columns, rows); err == nil {
		t.Fatal("continuation omitted required native receipt")
	}
	if _, err := source.ContinueWithTimeBucketsAndWorkContext(context.Background(), columns, rows, nil, 15001); !errors.Is(err, ErrTimechartResourceLimit) {
		t.Fatalf("over-limit receipt error=%v", err)
	}
	next, err := source.ContinueWithTimeBucketsAndWorkContext(context.Background(), columns, rows, nil, 8000)
	if err != nil {
		t.Fatal(err)
	}
	if next.TimechartWorkFloor() != 8000 || !next.HasValidExecutionSeal() {
		t.Fatal("continuation did not seal cumulative receipt")
	}
	clone, valid, err := next.CloneForExecutionContext(context.Background())
	if err != nil || !valid || clone.TimechartWorkFloor() != 8000 {
		t.Fatalf("receipt clone valid=%v error=%v", valid, err)
	}
	next.timechartWorkFloor = 0
	if next.HasValidExecutionSeal() {
		t.Fatal("mutated inherited receipt retained authority")
	}
	source.timechartWorkReceipt = false
	if source.HasValidExecutionSeal() {
		t.Fatal("mutated native receipt declaration retained authority")
	}
}
