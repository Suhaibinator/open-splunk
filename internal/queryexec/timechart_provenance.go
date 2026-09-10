package queryexec

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func ordinaryTimeBucketColumns(query clickhouse.CompiledQuery, columns []string, types []driver.ColumnType) ([]string, []driver.ColumnType, error) {
	if query.TimeBucket == nil {
		return columns, types, nil
	}
	index := query.TimeBucket.TimeIndex
	if index < 0 || index >= len(query.OutputFields) || query.OutputFields[index] != "_time" || len(columns) != len(types) || len(columns) == 0 || columns[len(columns)-1] != clickhouse.ResultTimeBucketEndColumn || types[len(types)-1].DatabaseTypeName() != "DateTime64(9, 'UTC')" {
		return nil, nil, searchjobs.ErrInvalidResult
	}
	return columns[:len(columns)-1], types[:len(types)-1], nil
}
func publishOrdinaryTimeBucketRow(sink searchjobs.ResultSink, query clickhouse.CompiledQuery, values []searchjobs.Value) error {
	if query.TimeBucket == nil {
		return sink.AddRow(values)
	}
	if len(values) != len(query.OutputFields)+1 {
		return searchjobs.ErrInvalidResult
	}
	start, ok := values[query.TimeBucket.TimeIndex].Time()
	end, endOK := values[len(values)-1].Time()
	if !ok || !endOK || !start.Before(end) {
		return fmt.Errorf("%w: invalid result bucket interval", searchjobs.ErrInvalidResult)
	}
	return publishWithTimeBucket(sink, values[:len(values)-1], searchjobs.TimeBucketBounds{Earliest: start.UTC().Format(time.RFC3339Nano), Latest: end.UTC().Format(time.RFC3339Nano)})
}
func publishWithTimeBucket(sink searchjobs.ResultSink, values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	if recipient, ok := sink.(searchjobs.TimeBucketResultSink); ok {
		return recipient.AddRowWithTimeBucket(values, bounds)
	}
	return sink.AddRow(values)
}
