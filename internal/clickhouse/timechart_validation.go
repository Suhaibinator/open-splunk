package clickhouse

import "fmt"

// The terminal timechart transport has closed types independent of its public
// pivot schema. Give the validation-only UNION branch those types directly so
// ClickHouse need not analyze the complete aggregate again to infer an empty
// row. The branch still consumes the complete chronological validation graph.
func timechartChronologicalDummyProjection(columns []string) ([]string, error) {
	projection := make([]string, 0, len(columns))
	for _, name := range columns {
		var value string
		switch name {
		case TimechartOrdinalColumn, TimechartCountColumn, TimechartWorkRowsColumn:
			value = "toUInt64(0)"
		case TimechartBucketColumn:
			value = "toDateTime64(0, 9, 'UTC')"
		case TimechartBucketPresentColumn, TimechartInputPresentColumn, TimechartInvalidColumn:
			value = "toUInt8(0)"
		case TimechartNamesColumn:
			value = "CAST([], 'Array(String)')"
		case TimechartCountsColumn:
			value = "CAST([], 'Array(UInt64)')"
		case TimechartValueColumn:
			value = "CAST(NULL AS Nullable(Float64))"
		case TimechartValuesColumn:
			value = "CAST([], 'Array(Float64)')"
		case TimechartValuePresentColumn:
			value = "CAST([], 'Array(UInt8)')"
		default:
			return nil, fmt.Errorf("compile ClickHouse query: unknown timechart transport column %q", name)
		}
		projection = append(projection, value+" AS "+quoteIdentifier(name))
	}
	return projection, nil
}
