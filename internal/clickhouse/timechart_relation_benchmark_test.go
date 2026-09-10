package clickhouse

import (
	"context"
	"testing"
)

func BenchmarkTimechartRelationNativeRows10000(b *testing.B) {
	const rowCount = 10000
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{uint64(i), "row payload"}
	}
	input, err := newRelationInput(context.Background(), []RelationColumn{{Name: "ordinal", Type: "UInt64"}, {Name: "text", Type: "String"}}, rows, false)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		table, err := materializeValidatedRelationInput(context.Background(), input)
		if err != nil || table.Block().Rows() != rowCount {
			b.Fatalf("native rows: err=%v", err)
		}
	}
	b.ReportMetric(rowCount, "rows/op")
}
