package queryexec

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"fortio.org/safecast"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// timechartGridRows removes the orthogonal occupancy column while retaining
// every dense row for the existing aggregate-specific validation barriers.
type timechartGridRows struct {
	driver.Rows
	present []uint8
	// Seven columns is the widest exact grid transport: ordinal, boundary,
	// occupancy, names, values, value-presence, and invalid-value marker.
	destinations [7]any
	rowPresent   uint8
}

func (rows *timechartGridRows) Scan(dest ...any) error {
	if len(dest) < 2 || len(dest)+1 > len(rows.destinations) {
		return fmt.Errorf("%w: timechart grid scan width is invalid", searchjobs.ErrInvalidResult)
	}
	destinations := rows.destinations[:len(dest)+1]
	copy(destinations, dest[:2])
	destinations[2] = &rows.rowPresent
	copy(destinations[3:], dest[2:])
	rows.rowPresent = 0
	err := rows.Rows.Scan(destinations...)
	// Release caller pointers after each scan while retaining bounded storage.
	clear(destinations)
	if err != nil {
		return err
	}
	if rows.rowPresent > 1 {
		return fmt.Errorf("%w: timechart bucket presence is invalid", searchjobs.ErrInvalidResult)
	}
	rows.present = append(rows.present, rows.rowPresent)
	return nil
}

func prepareTimechartGridTransport(rows driver.Rows, columns []string, types []driver.ColumnType, output clickhouse.TimechartOutput) (driver.Rows, []string, []driver.ColumnType, *timechartGridRows, error) {
	if !output.ExactGrid {
		return rows, columns, types, nil, nil
	}
	if len(columns) < 3 || len(types) < 3 || columns[2] != clickhouse.TimechartBucketPresentColumn || types[2].DatabaseTypeName() != "UInt8" || types[2].ScanType() != reflect.TypeFor[uint8]() {
		return rows, columns, types, nil, fmt.Errorf("%w: timechart bucket presence column is invalid", searchjobs.ErrInvalidResult)
	}
	wrapped := &timechartGridRows{Rows: rows, present: make([]uint8, 0, safecast.MustConv[int](output.BucketCount))}
	return wrapped, slices.Delete(slices.Clone(columns), 2, 3), slices.Delete(slices.Clone(types), 2, 3), wrapped, nil
}

// validateTimechartGridAggregate runs inside the aggregate readers' complete
// validation barrier, before any presentation filter or sink can observe rows.
func validateTimechartGridAggregate(rows driver.Rows, aggregatePresent, inputPresent bool) error {
	grid, ok := rows.(*timechartGridRows)
	if !ok {
		return nil
	}
	if (grid.rowPresent == 0 && aggregatePresent) || (grid.rowPresent != 0 && !inputPresent) {
		return fmt.Errorf("%w: timechart occupancy disagrees with aggregate or input presence", searchjobs.ErrInvalidResult)
	}
	return nil
}

// timechartGridSink applies presentation controls only after the complete
// upstream transport has been decoded, and attaches exact interval metadata.
type timechartGridSink struct {
	searchjobs.ResultSink
	output    clickhouse.TimechartOutput
	occupancy *timechartGridRows
}

func (sink *timechartGridSink) AddRow(values []searchjobs.Value) error {
	if len(values) == 0 {
		return fmt.Errorf("%w: timechart row is empty", searchjobs.ErrInvalidResult)
	}
	bucket, ok := values[0].Time()
	if !ok {
		return fmt.Errorf("%w: timechart timestamp is invalid", searchjobs.ErrInvalidResult)
	}
	boundaries := sink.output.Boundaries
	if len(boundaries) == 0 {
		return sink.ResultSink.AddRow(values)
	}
	ordinal := sort.Search(len(boundaries), func(index int) bool { return !boundaries[index].Before(bucket) })
	if ordinal >= len(boundaries)-1 || !boundaries[ordinal].Equal(bucket) {
		return fmt.Errorf("%w: timechart row is outside its grid", searchjobs.ErrInvalidResult)
	}
	if sink.output.ExactGrid {
		included, err := sink.includesOrdinal(ordinal)
		if err != nil {
			return err
		}
		if !included {
			return nil
		}
	}
	if bounded, ok := sink.ResultSink.(searchjobs.TimeBucketResultSink); ok {
		return bounded.AddRowWithTimeBucket(values, searchjobs.TimeBucketBounds{Earliest: bucket.UTC().Format(time.RFC3339Nano), Latest: boundaries[ordinal+1].UTC().Format(time.RFC3339Nano)})
	}
	return sink.ResultSink.AddRow(values)
}

// includesOrdinal is shared by prepublication admission and presentation, so
// sparse and partial buckets count identically at both boundaries.
func (sink *timechartGridSink) includesOrdinal(ordinal int) (bool, error) {
	if sink.occupancy == nil || uint64(len(sink.occupancy.present)) != sink.output.BucketCount {
		return false, fmt.Errorf("%w: timechart presence sequence is incomplete", searchjobs.ErrInvalidResult)
	}
	if ordinal < 0 || ordinal >= len(sink.occupancy.present) || ordinal >= len(sink.output.Boundaries)-1 {
		return false, fmt.Errorf("%w: timechart row is outside its grid", searchjobs.ErrInvalidResult)
	}
	if !sink.output.Continuous && sink.occupancy.present[ordinal] == 0 {
		return false, nil
	}
	if !sink.output.IncludePartial && (sink.output.Boundaries[ordinal].Before(sink.output.SearchEarliest) || sink.output.Boundaries[ordinal+1].After(sink.output.SearchLatest)) {
		return false, nil
	}
	return true, nil
}

// All native rows have already passed the complete validation barrier. Count
// only rows the publisher will expose, without constructing another buffer.
func (sink *timechartGridSink) validateRowLimit(ctx context.Context, rows int, maximum uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if maximum == 0 || safecast.MustConv[uint64](rows) <= maximum {
		return nil
	}
	if !sink.output.ExactGrid {
		return searchjobs.ErrExecutionLimit
	}
	var visible uint64
	for ordinal := range rows {
		if ordinal%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		included, err := sink.includesOrdinal(ordinal)
		if err != nil {
			return err
		}
		if included {
			visible++
			if visible > maximum {
				return searchjobs.ErrExecutionLimit
			}
		}
	}
	return ctx.Err()
}
