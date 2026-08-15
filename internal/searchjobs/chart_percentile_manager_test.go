package searchjobs

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestValidateChartSchemaEnforcesPercentileCellPolicy(t *testing.T) {
	t.Parallel()

	output := clickhouse.ChartOutput{
		RowField:        "path",
		RowKind:         clickhouse.ChartRowKindString,
		RowDatabaseType: "String",
		RowLimit:        10_000,
		MaxSeries:       12,
		MaxLabelBytes:   256,
		ValueKind:       clickhouse.ChartValueKindPercentile,
	}
	valid := Schema{Columns: []Column{
		{Name: "path", Kind: ValueKindString},
		{Name: "api", Kind: ValueKindDouble, Nullable: true},
		{Name: "NULL", Kind: ValueKindDouble, Nullable: true},
		{Name: "OTHER", Kind: ValueKindDouble, Nullable: true},
	}}
	if err := validateChartSchema(valid, []string{"path"}, output); err != nil {
		t.Fatalf("valid percentile chart schema: %v", err)
	}

	for _, test := range []struct {
		name   string
		schema Schema
		kind   clickhouse.ChartValueKind
	}{
		{
			name: "count cells",
			schema: Schema{Columns: []Column{
				{Name: "path", Kind: ValueKindString},
				{Name: "api", Kind: ValueKindUnsigned},
			}},
			kind: clickhouse.ChartValueKindPercentile,
		},
		{
			name: "nonnullable double",
			schema: Schema{Columns: []Column{
				{Name: "path", Kind: ValueKindString},
				{Name: "api", Kind: ValueKindDouble},
			}},
			kind: clickhouse.ChartValueKindPercentile,
		},
		{
			name:   "forged value kind",
			schema: valid,
			kind:   clickhouse.ChartValueKind(5),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := output
			candidate.ValueKind = test.kind
			if err := validateChartSchema(test.schema, []string{"path"}, candidate); !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("validateChartSchema() = %v, want ErrInvalidResult", err)
			}
		})
	}
}
