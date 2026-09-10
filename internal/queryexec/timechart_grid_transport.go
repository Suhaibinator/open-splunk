package queryexec

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// timechartGridRows removes the orthogonal occupancy column while retaining
// every dense row for the existing aggregate-specific validation barriers.
type timechartGridRows struct {
	driver.Rows
	present []uint8
}

func (rows *timechartGridRows) Scan(dest ...any) error {
	var present uint8
	destinations := make([]any, 0, len(dest)+1)
	destinations = append(destinations, dest[:2]...)
	destinations = append(destinations, &present)
	destinations = append(destinations, dest[2:]...)
	if err := rows.Rows.Scan(destinations...); err != nil {
		return err
	}
	if present > 1 {
		return fmt.Errorf("%w: timechart bucket presence is invalid", searchjobs.ErrInvalidResult)
	}
	rows.present = append(rows.present, present)
	return nil
}

func prepareTimechartGridTransport(rows driver.Rows, columns []string, types []driver.ColumnType, output clickhouse.TimechartOutput) (driver.Rows, []string, []driver.ColumnType, *timechartGridRows, error) {
	if !output.ExactGrid {
		return rows, columns, types, nil, nil
	}
	if len(columns) < 3 || len(types) < 3 || columns[2] != clickhouse.TimechartBucketPresentColumn || types[2].DatabaseTypeName() != "UInt8" || types[2].ScanType() != reflect.TypeFor[uint8]() {
		return rows, columns, types, nil, fmt.Errorf("%w: timechart bucket presence column is invalid", searchjobs.ErrInvalidResult)
	}
	wrapped := &timechartGridRows{Rows: rows, present: make([]uint8, 0, int(output.BucketCount))}
	return wrapped, slices.Delete(slices.Clone(columns), 2, 3), slices.Delete(slices.Clone(types), 2, 3), wrapped, nil
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
		if sink.occupancy == nil || len(sink.occupancy.present) != int(sink.output.BucketCount) {
			return fmt.Errorf("%w: timechart presence sequence is incomplete", searchjobs.ErrInvalidResult)
		}
		if !sink.output.Continuous && sink.occupancy.present[ordinal] == 0 {
			return nil
		}
		if !sink.output.IncludePartial && (bucket.Before(sink.output.SearchEarliest) || boundaries[ordinal+1].After(sink.output.SearchLatest)) {
			return nil
		}
	}
	if bounded, ok := sink.ResultSink.(searchjobs.TimeBucketResultSink); ok {
		return bounded.AddRowWithTimeBucket(values, searchjobs.TimeBucketBounds{Earliest: bucket.UTC().Format(time.RFC3339Nano), Latest: boundaries[ordinal+1].UTC().Format(time.RFC3339Nano)})
	}
	return sink.ResultSink.AddRow(values)
}
