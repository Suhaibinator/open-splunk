package queryexec

import (
	"context"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

type timechartWorkReceiver interface{ SetTimechartWork(uint64) error }

// Work is a cumulative receipt, repeated identically on every dense native
// row. It is validated before any public schema, rows or bounds are emitted.
type timechartWorkRows struct {
	driver.Rows
	destinations          [8]any
	current, total, floor uint64
	seen                  bool
}

func (rows *timechartWorkRows) Scan(dest ...any) error {
	if len(dest)+1 > len(rows.destinations) {
		return fmt.Errorf("%w: timechart work transport width is invalid", searchjobs.ErrInvalidResult)
	}
	destinations := rows.destinations[:len(dest)+1]
	copy(destinations, dest)
	rows.current = 0
	destinations[len(dest)] = &rows.current
	err := rows.Rows.Scan(destinations...)
	clear(destinations)
	if err != nil {
		return err
	}
	if rows.current > plan.MaximumMVExpandRowsPerQuery {
		return searchjobs.ErrExecutionLimit
	}
	if rows.current < rows.floor || (rows.seen && rows.current != rows.total) {
		return fmt.Errorf("%w: timechart cumulative work receipt is inconsistent", searchjobs.ErrInvalidResult)
	}
	rows.total, rows.seen = rows.current, true
	return nil
}

func prepareTimechartWorkTransport(ctx context.Context, rows driver.Rows, columns []string, types []driver.ColumnType, compiled clickhouse.CompiledQuery) (driver.Rows, []string, []driver.ColumnType, *timechartWorkRows, error) {
	if !compiled.HasTimechartWorkReceipt() {
		return rows, columns, types, nil, nil
	}
	last := len(columns) - 1
	if last < 0 || len(types) != len(columns) || columns[last] != clickhouse.TimechartWorkRowsColumn || types[last].DatabaseTypeName() != "UInt64" || types[last].ScanType() != reflect.TypeFor[uint64]() {
		if last >= 0 && len(types) == len(columns) {
			return rows, columns, types, nil, fmt.Errorf("%w: timechart work receipt column is invalid (%s %s %v)", searchjobs.ErrInvalidResult, columns[last], types[last].DatabaseTypeName(), types[last].ScanType())
		}
		return rows, columns, types, nil, fmt.Errorf("%w: timechart work receipt column is invalid", searchjobs.ErrInvalidResult)
	}
	if remaining, ok := searchlimits.RemainingExecutionBytes(ctx); ok && remaining < uint64(unsafe.Sizeof(timechartWorkRows{})) {
		return rows, columns, types, nil, searchjobs.ErrExecutionLimit
	}
	wrapped := &timechartWorkRows{Rows: rows, floor: compiled.TimechartWorkFloor(), total: compiled.TimechartWorkFloor()}
	return wrapped, columns[:last], types[:last], wrapped, nil
}

func publishTimechartWork(sink searchjobs.ResultSink, work uint64) error {
	if work > plan.MaximumMVExpandRowsPerQuery {
		return searchjobs.ErrExecutionLimit
	}
	if receiver, ok := sink.(timechartWorkReceiver); ok {
		return receiver.SetTimechartWork(work)
	}
	return nil
}

type timechartWorkSink struct {
	searchjobs.ResultSink
	receipt *timechartWorkRows
}

func (sink timechartWorkSink) SetSchema(schema searchjobs.Schema) error {
	if err := publishTimechartWork(sink.ResultSink, sink.receipt.total); err != nil {
		return err
	}
	return sink.ResultSink.SetSchema(schema)
}
func (sink timechartWorkSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	return publishWithTimeBucket(sink.ResultSink, values, bounds)
}
