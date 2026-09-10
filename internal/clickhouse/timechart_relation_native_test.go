package clickhouse

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	driverproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestRelationNativeScratchDoesNotAliasDriverRows(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	rows := [][]any{
		{uint64(1), "first", []any{uint64(11)}},
		{uint64(2), "second", []any{uint64(22)}},
		{uint64(3), "third", []any{uint64(33)}},
	}
	input, err := newRelationInput(context.Background(), []RelationColumn{{Name: "ordinal", Type: "UInt64"}, {Name: "text", Type: "String"}, {Name: "nested", Type: "Dynamic"}}, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	input.bucketEnds = []time.Time{first.Add(time.Second), first.Add(2 * time.Second), first.Add(3 * time.Second)}
	table, err := materializeRelationInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	// Dynamic offsets are populated on native decoding, so round-trip the real
	// driver block before reading values instead of inspecting append internals.
	buffer := &chproto.Buffer{}
	if err := table.Block().Encode(buffer, 0); err != nil {
		t.Fatal(err)
	}
	decoded := driverproto.NewBlock()
	if err := decoded.Decode(chproto.NewReader(bytes.NewReader(buffer.Buf)), 0); err != nil {
		t.Fatal(err)
	}
	for rowIndex, source := range rows {
		for columnIndex := range 2 {
			if got := decoded.Columns[columnIndex].Row(rowIndex, false); !reflect.DeepEqual(got, source[columnIndex]) {
				t.Fatalf("driver retained scratch alias at row%d column%d: %v", rowIndex, columnIndex, got)
			}
		}
		want := chcol.NewDynamicWithType([]chcol.Dynamic{chcol.NewDynamicWithType(source[2].([]any)[0], "UInt64")}, "Array(Dynamic)")
		if got := decoded.Columns[2].Row(rowIndex, false); !reflect.DeepEqual(got, want) {
			t.Fatalf("dynamic row%d changed after later appends: got=%#v want=%#v", rowIndex, got, want)
		}
		if got := decoded.Columns[3].Row(rowIndex, false); !reflect.DeepEqual(got, input.bucketEnds[rowIndex]) {
			t.Fatalf("bucket end row%d aliases scratch: %v", rowIndex, got)
		}
		if _, ok := input.rows[rowIndex][2].([]any); !ok {
			t.Fatal("native conversion mutated immutable input")
		}
	}
}
