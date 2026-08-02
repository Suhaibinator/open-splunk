package clickhouse

import (
	"context"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

type eventStatsNullableFloatRow struct {
	id    string
	value *float64
}

type eventStatsFieldPresence struct {
	present uint64
	nulls   uint64
	missing uint64
	total   uint64
}

type eventStatsDynamicPairRow struct {
	id     string
	first  any
	second any
}

func collectEventStatsDynamicPairRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	name string,
	query CompiledQuery,
) []eventStatsDynamicPairRow {
	t.Helper()

	rows, err := connection.Query(ctx, query.SQL, query.Args...)
	if err != nil {
		t.Fatalf(
			"execute %s: %v\nSQL: %s\nargs: %#v",
			name,
			err,
			query.SQL,
			query.Args,
		)
	}
	var got []eventStatsDynamicPairRow
	for rows.Next() {
		var id string
		var first, second chcol.Dynamic
		if err := rows.Scan(&id, &first, &second); err != nil {
			_ = rows.Close()
			t.Fatalf("scan %s: %v", name, err)
		}
		got = append(got, eventStatsDynamicPairRow{
			id: id, first: first.Any(), second: second.Any(),
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate %s: %v", name, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s rows: %v", name, err)
	}
	return got
}

func collectEventStatsFieldPresence(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	logical *plan.Query,
	field string,
) eventStatsFieldPresence {
	t.Helper()

	summary, err := (Compiler{}).CompileFieldSummary(
		logical,
		FieldSummarySpec{
			FieldName:             field,
			MaximumValues:         10,
			MaximumDistinctValues: 10,
			MaximumValueBytes:     64,
		},
	)
	if err != nil {
		t.Fatalf("compile %s field summary: %v", field, err)
	}
	control := `SELECT ` +
		quoteIdentifier(FieldSummaryEventCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryNullCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryMissingCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryTotalEventCountColumn) +
		` FROM (` + summary.SQL + `) WHERE ` +
		quoteIdentifier(FieldSummaryRowKindColumn) + ` = 0`
	presence := eventStatsFieldPresence{}
	if err := connection.QueryRow(
		ctx,
		control,
		summary.Args...,
	).Scan(
		&presence.present,
		&presence.nulls,
		&presence.missing,
		&presence.total,
	); err != nil {
		t.Fatalf(
			"execute %s field summary: %v\nSQL: %s\nargs: %#v",
			field,
			err,
			summary.SQL,
			summary.Args,
		)
	}
	return presence
}

func collectEventStatsNullableFloatRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	name string,
	query CompiledQuery,
) []eventStatsNullableFloatRow {
	t.Helper()

	rows, err := connection.Query(ctx, query.SQL, query.Args...)
	if err != nil {
		t.Fatalf(
			"execute %s: %v\nSQL: %s\nargs: %#v",
			name,
			err,
			query.SQL,
			query.Args,
		)
	}
	types := rows.ColumnTypes()
	if len(types) != 2 ||
		types[0].DatabaseTypeName() != "String" ||
		types[1].DatabaseTypeName() != "Nullable(Float64)" {
		typeNames := make([]string, len(types))
		for index, columnType := range types {
			typeNames[index] = columnType.DatabaseTypeName()
		}
		_ = rows.Close()
		t.Fatalf(
			"%s column types = %#v, want String/Nullable(Float64)",
			name,
			typeNames,
		)
	}

	var got []eventStatsNullableFloatRow
	for rows.Next() {
		var row eventStatsNullableFloatRow
		if err := rows.Scan(&row.id, &row.value); err != nil {
			_ = rows.Close()
			t.Fatalf("scan %s: %v", name, err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate %s: %v", name, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s rows: %v", name, err)
	}
	return got
}

func collectEventStatsNullableFloat(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	name string,
	query CompiledQuery,
) *float64 {
	t.Helper()

	rows, err := connection.Query(ctx, query.SQL, query.Args...)
	if err != nil {
		t.Fatalf(
			"execute %s: %v\nSQL: %s\nargs: %#v",
			name,
			err,
			query.SQL,
			query.Args,
		)
	}
	types := rows.ColumnTypes()
	if len(types) != 1 || types[0].DatabaseTypeName() != "Nullable(Float64)" {
		typeNames := make([]string, len(types))
		for index, columnType := range types {
			typeNames[index] = columnType.DatabaseTypeName()
		}
		_ = rows.Close()
		t.Fatalf(
			"%s column types = %#v, want Nullable(Float64)",
			name,
			typeNames,
		)
	}
	if !rows.Next() {
		err := rows.Err()
		_ = rows.Close()
		t.Fatalf("%s returned no row: %v", name, err)
	}
	var value *float64
	if err := rows.Scan(&value); err != nil {
		_ = rows.Close()
		t.Fatalf("scan %s: %v", name, err)
	}
	if rows.Next() {
		_ = rows.Close()
		t.Fatalf("%s returned multiple rows", name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate %s: %v", name, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s rows: %v", name, err)
	}
	return value
}
