package clickhouse

import (
	"context"
	"strconv"
	"testing"
	"time"
)

var timechartPreflightBenchmarkSink relationInputPreflight

func BenchmarkTimechartRelationPreflightScalar(b *testing.B) {
	columns := []RelationColumn{
		{Name: "count", Type: "UInt64"},
		{Name: "delta", Type: "Int64"},
		{Name: "ratio", Type: "Float64"},
		{Name: "label", Type: "String"},
		{Name: "active", Type: "Bool"},
		{Name: "stamp", Type: "DateTime64(9, 'UTC')"},
	}
	stamp := time.Unix(1_789_000_000, 123).UTC()
	rows := make([][]any, 4_096)
	for index := range rows {
		rows[index] = []any{uint64(index), -int64(index), float64(index) / 7, "series", index%2 == 0, stamp}
	}
	benchmarkTimechartRelationPreflight(b, columns, rows)
}

func BenchmarkTimechartRelationPreflightWideMixed(b *testing.B) {
	const columnCount = 96
	const rowCount = 256
	columns := make([]RelationColumn, columnCount)
	for index := range columns {
		typeName := "UInt64"
		switch index % 4 {
		case 0:
			typeName = "Dynamic"
		case 1:
			typeName = "String"
		case 2:
			typeName = "Bool"
		}
		columns[index] = RelationColumn{Name: "field_" + strconv.Itoa(index), Type: typeName}
	}
	rows := make([][]any, rowCount)
	for rowIndex := range rows {
		row := make([]any, columnCount)
		for columnIndex, descriptor := range columns {
			switch descriptor.Type {
			case "Dynamic":
				row[columnIndex] = []any{uint64(rowIndex), "series", []any{rowIndex%2 == 0}}
			case "String":
				row[columnIndex] = "series"
			case "Bool":
				row[columnIndex] = rowIndex%2 == 0
			default:
				row[columnIndex] = uint64(rowIndex)
			}
		}
		rows[rowIndex] = row
	}
	benchmarkTimechartRelationPreflight(b, columns, rows)
}

func BenchmarkTimechartRelationPreflightWideScalar(b *testing.B) {
	const columnCount = 96
	const rowCount = 256
	columns := make([]RelationColumn, columnCount)
	for index := range columns {
		columns[index] = RelationColumn{Name: "field_" + strconv.Itoa(index), Type: "UInt64"}
	}
	rows := make([][]any, rowCount)
	for rowIndex := range rows {
		row := make([]any, columnCount)
		for columnIndex := range row {
			row[columnIndex] = uint64(rowIndex + columnIndex)
		}
		rows[rowIndex] = row
	}
	benchmarkTimechartRelationPreflight(b, columns, rows)
}

func benchmarkTimechartRelationPreflight(b *testing.B, columns []RelationColumn, rows [][]any) {
	const unlimited = ^uint64(0)
	var err error
	timechartPreflightBenchmarkSink, err = preflightRelationInput(context.Background(), columns, rows, nil, unlimited, unlimited)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		timechartPreflightBenchmarkSink, err = preflightRelationInput(context.Background(), columns, rows, nil, unlimited, unlimited)
	}
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
}
