package export

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSchemaMatchesCompiledChartPercentileMetadata(t *testing.T) {
	t.Parallel()

	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"path"},
		Chart: &clickhouse.ChartOutput{
			RowField:        "path",
			RowKind:         clickhouse.ChartRowKindString,
			RowDatabaseType: "String",
			RowLimit:        10_000,
			MaxSeries:       12,
			MaxLabelBytes:   256,
			ValueKind:       clickhouse.ChartValueKindPercentile,
		},
	}
	valid := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "NULL", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "OTHER", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if !schemaMatchesCompiledQuery(valid, compiled) {
		t.Fatal("valid percentile chart schema was rejected")
	}

	countSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindUnsigned},
	}}
	if schemaMatchesCompiledQuery(countSchema, compiled) {
		t.Fatal("count schema was admitted for percentile chart metadata")
	}
	nonnullable := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindDouble},
	}}
	if schemaMatchesCompiledQuery(nonnullable, compiled) {
		t.Fatal("nonnullable numeric schema was admitted for percentile chart metadata")
	}

	forged := compiled
	forgedChart := *compiled.Chart
	forgedChart.ValueKind = clickhouse.ChartValueKind(5)
	forged.Chart = &forgedChart
	if schemaMatchesCompiledQuery(valid, forged) {
		t.Fatal("forged percentile chart value kind was admitted")
	}
}
