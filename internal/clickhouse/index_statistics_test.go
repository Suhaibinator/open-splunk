package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

const (
	indexStatisticsAggregateSQL = `
SELECT
    count(),
    minOrNull("event_time"),
    maxOrNull("event_time")
FROM "open_splunk"."events"
PREWHERE "tenant_id" = ? AND "index_name" = ?
WHERE "expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')
  AND "index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')
  AND "visibility_seq" <= ?`

	indexStatisticsCustomAggregateSQL = `
SELECT
    count(),
    minOrNull("event_time"),
    maxOrNull("event_time")
FROM "analytics"."logs"
PREWHERE "tenant_id" = ? AND "index_name" = ?
WHERE "expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')
  AND "index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')
  AND "visibility_seq" <= ?`

	indexStatisticsPartsSQL = `
SELECT
    coalesce(sum("rows"), toUInt64(0)),
    coalesce(sum("bytes_on_disk"), toUInt64(0))
FROM system.parts
WHERE "database" = ? AND "table" = ? AND "active" = 1`
)

func TestIndexStatisticsEmptyResultUsesOneExactParameterizedQuery(t *testing.T) {
	t.Parallel()

	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{{
			row: &indexStatisticsScriptRow{
				values: []any{uint64(0), nil, nil},
			},
		}},
	}
	reader := mustIndexStatisticsReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	request := indexStatisticsValidRequest()
	request.VisibilityCutoff = 0
	probe := &indexStatisticsContextProbe{Context: context.Background()}

	result, err := reader.GetIndexStatistics(probe, request)
	if err != nil {
		t.Fatalf("GetIndexStatistics(): %v", err)
	}
	if result.TenantID != request.TenantID ||
		result.IndexID != request.IndexID ||
		result.IndexName != request.IndexName ||
		result.VisibilityCutoff != request.VisibilityCutoff ||
		result.EventCount != 0 ||
		result.StorageBytes != 0 ||
		result.EarliestEventTime != nil ||
		result.LatestEventTime != nil ||
		!result.MeasuredAt.Equal(request.MeasuredAt) ||
		result.MeasuredAt.Location() != time.UTC ||
		!result.Estimates {
		t.Fatalf("empty statistics = %#v, want exact scoped empty estimate", result)
	}

	calls := connection.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("queries = %d, want exactly one aggregate query", len(calls))
	}
	assertIndexStatisticsQuery(
		t,
		calls[0],
		indexStatisticsAggregateSQL,
		[]any{
			request.TenantID,
			request.IndexName,
			indexStatisticsTimeArgument(request.MeasuredAt),
			indexStatisticsTimeArgument(request.MeasuredAt),
			uint64(0),
		},
	)
	assertIndexStatisticsBoundedSettings(t, probe, calls[0])
}

func TestIndexStatisticsNonemptyResultUsesPartsEstimateAndCustomTable(t *testing.T) {
	t.Parallel()

	request := indexStatisticsValidRequest()
	earliest := request.MeasuredAt.Add(-3*time.Hour - 123*time.Nanosecond).
		In(time.FixedZone("earliest-offset", -7*60*60))
	latest := request.MeasuredAt.Add(-time.Hour + 456*time.Nanosecond).
		In(time.FixedZone("latest-offset", 2*60*60))
	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{
			{
				row: &indexStatisticsScriptRow{
					values: []any{uint64(3), &earliest, &latest},
				},
			},
			{
				row: &indexStatisticsScriptRow{
					values: []any{uint64(10), uint64(101)},
				},
			},
		},
	}
	reader := mustIndexStatisticsReader(
		t,
		connection,
		IndexStatisticsConfig{Database: "analytics", Table: "logs"},
	)
	probe := &indexStatisticsContextProbe{Context: context.Background()}

	result, err := reader.GetIndexStatistics(probe, request)
	if err != nil {
		t.Fatalf("GetIndexStatistics(): %v", err)
	}
	if result.TenantID != request.TenantID ||
		result.IndexID != request.IndexID ||
		result.IndexName != request.IndexName ||
		result.VisibilityCutoff != request.VisibilityCutoff ||
		result.EventCount != 3 ||
		result.StorageBytes != 31 ||
		result.EarliestEventTime == nil ||
		result.LatestEventTime == nil ||
		!result.EarliestEventTime.Equal(earliest) ||
		!result.LatestEventTime.Equal(latest) ||
		result.EarliestEventTime.Location() != time.UTC ||
		result.LatestEventTime.Location() != time.UTC ||
		!result.MeasuredAt.Equal(request.MeasuredAt) ||
		result.MeasuredAt.Location() != time.UTC ||
		!result.Estimates {
		t.Fatalf("nonempty statistics = %#v", result)
	}

	calls := connection.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("queries = %d, want aggregate plus parts metadata", len(calls))
	}
	assertIndexStatisticsQuery(
		t,
		calls[0],
		indexStatisticsCustomAggregateSQL,
		[]any{
			request.TenantID,
			request.IndexName,
			indexStatisticsTimeArgument(request.MeasuredAt),
			indexStatisticsTimeArgument(request.MeasuredAt),
			request.VisibilityCutoff,
		},
	)
	assertIndexStatisticsQuery(
		t,
		calls[1],
		indexStatisticsPartsSQL,
		[]any{"analytics", "logs"},
	)
	assertIndexStatisticsBoundedSettings(t, probe, calls[0])
	assertIndexStatisticsBoundedSettings(t, probe, calls[1])
}

func TestIndexStatisticsStorageEstimateUsesOverflowSafeCeilingArithmetic(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		eventCount uint64
		partRows   uint64
		partBytes  uint64
	}{
		{
			name:       "rounds fractional byte upward",
			eventCount: 3,
			partRows:   10,
			partBytes:  101,
		},
		{
			name:       "near maximum without intermediate overflow",
			eventCount: math.MaxUint64 - 1,
			partRows:   math.MaxUint64,
			partBytes:  math.MaxUint64,
		},
		{
			name:       "maximum exact ratio",
			eventCount: math.MaxUint64,
			partRows:   math.MaxUint64,
			partBytes:  math.MaxUint64,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := indexStatisticsValidRequest()
			earliest := request.MeasuredAt.Add(-time.Hour)
			latest := request.MeasuredAt.Add(-time.Minute)
			connection := &indexStatisticsScriptConnection{
				steps: []indexStatisticsScriptStep{
					{
						row: &indexStatisticsScriptRow{
							values: []any{
								test.eventCount,
								&earliest,
								&latest,
							},
						},
					},
					{
						row: &indexStatisticsScriptRow{
							values: []any{test.partRows, test.partBytes},
						},
					},
				},
			}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			result, err := reader.GetIndexStatistics(
				context.Background(),
				request,
			)
			if err != nil {
				t.Fatalf("GetIndexStatistics(): %v", err)
			}
			want := indexStatisticsCeilingEstimate(
				test.eventCount,
				test.partRows,
				test.partBytes,
			)
			if result.EventCount != test.eventCount ||
				result.StorageBytes != want ||
				!result.Estimates {
				t.Fatalf(
					"result count/bytes/estimate = %d/%d/%v, want %d/%d/true",
					result.EventCount,
					result.StorageBytes,
					result.Estimates,
					test.eventCount,
					want,
				)
			}
			if got := len(connection.snapshotCalls()); got != 2 {
				t.Fatalf("queries = %d, want 2", got)
			}
		})
	}
}

func TestIndexStatisticsRejectsInconsistentPartsMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		eventCount uint64
		partRows   uint64
		partBytes  uint64
	}{
		{
			name:       "positive events but no active part rows",
			eventCount: 1,
			partRows:   0,
			partBytes:  0,
		},
		{
			name:       "scoped event count exceeds table rows",
			eventCount: 11,
			partRows:   10,
			partBytes:  100,
		},
		{
			name:       "positive events but no active part bytes",
			eventCount: 7,
			partRows:   8,
			partBytes:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := indexStatisticsValidRequest()
			earliest := request.MeasuredAt.Add(-time.Hour)
			latest := request.MeasuredAt.Add(-time.Minute)
			connection := &indexStatisticsScriptConnection{
				steps: []indexStatisticsScriptStep{
					{
						row: &indexStatisticsScriptRow{
							values: []any{
								test.eventCount,
								&earliest,
								&latest,
							},
						},
					},
					{
						row: &indexStatisticsScriptRow{
							values: []any{test.partRows, test.partBytes},
						},
					},
				},
			}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			if result, err := reader.GetIndexStatistics(
				context.Background(),
				request,
			); err == nil {
				t.Fatalf(
					"GetIndexStatistics() = %#v, nil; want fail-closed metadata error",
					result,
				)
			}
			if got := len(connection.snapshotCalls()); got != 2 {
				t.Fatalf("queries = %d, want 2", got)
			}
		})
	}
}

func TestIndexStatisticsRejectsInvalidAggregateBoundsBeforePartsQuery(
	t *testing.T,
) {
	t.Parallel()

	request := indexStatisticsValidRequest()
	earliest := request.MeasuredAt.Add(-2 * time.Hour)
	latest := request.MeasuredAt.Add(-time.Hour)
	reversedEarliest, reversedLatest := latest, earliest
	outOfRange := MinimumSearchTime().Add(-time.Nanosecond)
	tests := []struct {
		name  string
		count uint64
		first *time.Time
		last  *time.Time
	}{
		{
			name:  "empty count has earliest",
			count: 0,
			first: &earliest,
		},
		{
			name:  "empty count has latest",
			count: 0,
			last:  &latest,
		},
		{
			name:  "positive count misses earliest",
			count: 1,
			last:  &latest,
		},
		{
			name:  "positive count misses latest",
			count: 1,
			first: &earliest,
		},
		{
			name:  "bounds are reversed",
			count: 1,
			first: &reversedEarliest,
			last:  &reversedLatest,
		},
		{
			name:  "bound is outside supported ClickHouse range",
			count: 1,
			first: &outOfRange,
			last:  &latest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsScriptConnection{
				steps: []indexStatisticsScriptStep{{
					row: &indexStatisticsScriptRow{
						values: []any{test.count, test.first, test.last},
					},
				}},
			}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			if result, err := reader.GetIndexStatistics(
				context.Background(),
				request,
			); err == nil {
				t.Fatalf(
					"GetIndexStatistics() = %#v, nil; want invalid aggregate error",
					result,
				)
			}
			if got := len(connection.snapshotCalls()); got != 1 {
				t.Fatalf(
					"queries = %d, want no parts query after invalid aggregate",
					got,
				)
			}
		})
	}
}

func TestIndexStatisticsRejectsInvalidConstruction(t *testing.T) {
	t.Parallel()

	var typedNil *indexStatisticsScriptConnection
	tests := []struct {
		name       string
		connection driver.Conn
		config     IndexStatisticsConfig
	}{
		{
			name:       "nil connection",
			connection: nil,
		},
		{
			name:       "typed nil connection",
			connection: typedNil,
		},
		{
			name:       "database expression",
			connection: &indexStatisticsScriptConnection{},
			config: IndexStatisticsConfig{
				Database:      "open_splunk; DROP TABLE events",
				Table:         "events",
				ReadAdmission: indexread.UnfencedAdmission{},
			},
		},
		{
			name:       "quoted table",
			connection: &indexStatisticsScriptConnection{},
			config: IndexStatisticsConfig{
				Database:      "open_splunk",
				Table:         "`events`",
				ReadAdmission: indexread.UnfencedAdmission{},
			},
		},
		{
			name:       "qualified table",
			connection: &indexStatisticsScriptConnection{},
			config: IndexStatisticsConfig{
				Database:      "open_splunk",
				Table:         "other.events",
				ReadAdmission: indexread.UnfencedAdmission{},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if reader, err := NewIndexStatisticsReader(
				test.connection,
				test.config,
			); err == nil {
				t.Fatalf(
					"NewIndexStatisticsReader() = %#v, nil; want validation error",
					reader,
				)
			}
		})
	}
}

func TestIndexStatisticsRejectsInvalidRequestsBeforeQuery(t *testing.T) {
	t.Parallel()

	base := indexStatisticsValidRequest()
	tests := []struct {
		name   string
		mutate func(*IndexStatisticsRequest)
	}{
		{
			name: "empty tenant ID",
			mutate: func(request *IndexStatisticsRequest) {
				request.TenantID = ""
			},
		},
		{
			name: "tenant ID has surrounding whitespace",
			mutate: func(request *IndexStatisticsRequest) {
				request.TenantID = " " + request.TenantID
			},
		},
		{
			name: "tenant ID is invalid UTF-8",
			mutate: func(request *IndexStatisticsRequest) {
				request.TenantID = string([]byte{0xff})
			},
		},
		{
			name: "tenant ID has a control character",
			mutate: func(request *IndexStatisticsRequest) {
				request.TenantID += "\n"
			},
		},
		{
			name: "tenant ID exceeds control-plane bound",
			mutate: func(request *IndexStatisticsRequest) {
				request.TenantID = strings.Repeat("t", 256)
			},
		},
		{
			name: "index ID is not a protocol identifier",
			mutate: func(request *IndexStatisticsRequest) {
				request.IndexID = " invalid"
			},
		},
		{
			name: "index name is not canonical",
			mutate: func(request *IndexStatisticsRequest) {
				request.IndexName = "GradeThis"
			},
		},
		{
			name: "measured at is zero",
			mutate: func(request *IndexStatisticsRequest) {
				request.MeasuredAt = time.Time{}
			},
		},
		{
			name: "measured at is not UTC",
			mutate: func(request *IndexStatisticsRequest) {
				request.MeasuredAt = request.MeasuredAt.In(
					time.FixedZone("offset", -7*60*60),
				)
			},
		},
		{
			name: "measured at is not millisecond aligned",
			mutate: func(request *IndexStatisticsRequest) {
				request.MeasuredAt = request.MeasuredAt.Add(time.Nanosecond)
			},
		},
		{
			name: "measured at predates ClickHouse range",
			mutate: func(request *IndexStatisticsRequest) {
				request.MeasuredAt = MinimumSearchTime().
					Add(-time.Millisecond).
					UTC()
			},
		},
		{
			name: "measured at exceeds ClickHouse range",
			mutate: func(request *IndexStatisticsRequest) {
				request.MeasuredAt = MaximumSearchTime().
					Add(time.Millisecond).
					UTC()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsScriptConnection{}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)
			request := base
			test.mutate(&request)

			if result, err := reader.GetIndexStatistics(
				context.Background(),
				request,
			); err == nil {
				t.Fatalf(
					"GetIndexStatistics() = %#v, nil; want validation error",
					result,
				)
			}
			if got := len(connection.snapshotCalls()); got != 0 {
				t.Fatalf("queries = %d, want validation before query", got)
			}
		})
	}

	connection := &indexStatisticsScriptConnection{}
	reader := mustIndexStatisticsReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if result, err := reader.GetIndexStatistics(nil, base); err == nil {
		t.Fatalf(
			"GetIndexStatistics(nil) = %#v, nil; want nil-context error",
			result,
		)
	}
	if got := len(connection.snapshotCalls()); got != 0 {
		t.Fatalf("nil-context queries = %d, want 0", got)
	}
}

func TestIndexStatisticsRejectsNilAndTypedNilQueryRows(t *testing.T) {
	t.Parallel()

	var typedNil *indexStatisticsScriptRow
	tests := []struct {
		name string
		row  driver.Row
	}{
		{name: "nil row", row: nil},
		{name: "typed nil row", row: typedNil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsScriptConnection{
				steps: []indexStatisticsScriptStep{{row: test.row}},
			}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			if result, err := reader.GetIndexStatistics(
				context.Background(),
				indexStatisticsValidRequest(),
			); err == nil {
				t.Fatalf(
					"GetIndexStatistics() = %#v, nil; want nil-row error",
					result,
				)
			}
			if got := len(connection.snapshotCalls()); got != 1 {
				t.Fatalf("queries = %d, want 1", got)
			}
		})
	}
}

func TestIndexStatisticsPropagatesQueryErrorsAndStops(t *testing.T) {
	t.Parallel()

	aggregateErr := errors.New("aggregate unavailable")
	metadataErr := errors.New("parts unavailable")
	request := indexStatisticsValidRequest()
	earliest := request.MeasuredAt.Add(-time.Hour)
	latest := request.MeasuredAt.Add(-time.Minute)
	tests := []struct {
		name      string
		steps     []indexStatisticsScriptStep
		wantErr   error
		wantCalls int
	}{
		{
			name: "aggregate error",
			steps: []indexStatisticsScriptStep{{
				row: &indexStatisticsScriptRow{scanErr: aggregateErr},
			}},
			wantErr:   aggregateErr,
			wantCalls: 1,
		},
		{
			name: "parts metadata error",
			steps: []indexStatisticsScriptStep{
				{
					row: &indexStatisticsScriptRow{
						values: []any{uint64(1), &earliest, &latest},
					},
				},
				{
					row: &indexStatisticsScriptRow{scanErr: metadataErr},
				},
			},
			wantErr:   metadataErr,
			wantCalls: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsScriptConnection{steps: test.steps}
			reader := mustIndexStatisticsReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			if result, err := reader.GetIndexStatistics(
				context.Background(),
				request,
			); !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"GetIndexStatistics() = %#v, %v; want errors.Is(%v)",
					result,
					err,
					test.wantErr,
				)
			}
			if got := len(connection.snapshotCalls()); got != test.wantCalls {
				t.Fatalf("queries = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestIndexStatisticsCapsLongCallerDeadlineBeforeDriverOptions(t *testing.T) {
	t.Parallel()

	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{{
			row: &indexStatisticsScriptRow{
				values: []any{uint64(0), nil, nil},
			},
		}},
	}
	reader := mustIndexStatisticsReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	caller, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	probe := &indexStatisticsContextProbe{Context: caller}

	if _, err := reader.GetIndexStatistics(
		probe,
		indexStatisticsValidRequest(),
	); err != nil {
		t.Fatalf("GetIndexStatistics(): %v", err)
	}
	calls := connection.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("queries = %d, want 1", len(calls))
	}
	deadline, ok := calls[0].ctx.Deadline()
	if !ok {
		t.Fatal("query context has no reader-owned deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > indexStatisticsOperationTimeout {
		t.Fatalf(
			"query deadline = %s (%s remaining), want 0 < remaining <= %s",
			deadline,
			remaining,
			indexStatisticsOperationTimeout,
		)
	}
	settings := indexStatisticsCapturedSettings(t, probe, calls[0].ctx)
	assertIndexStatisticsSettingEqual(
		t,
		settings,
		"max_execution_time",
		indexStatisticsMaxExecutionSeconds,
	)
}

func TestIndexStatisticsRejectsConcurrentOperationWithoutUsingSharedPool(
	t *testing.T,
) {
	t.Parallel()

	entered := make(chan struct{})
	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{
			{
				rowForContext: func(ctx context.Context) driver.Row {
					return &indexStatisticsBlockingRow{
						ctx:     ctx,
						entered: entered,
					}
				},
			},
			{
				row: &indexStatisticsScriptRow{
					values: []any{uint64(0), nil, nil},
				},
			},
		},
	}
	reader := mustIndexStatisticsReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := reader.GetIndexStatistics(
			firstContext,
			indexStatisticsValidRequest(),
		)
		firstResult <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first index-statistics query did not enter its scan")
	}
	if result, err := reader.GetIndexStatistics(
		context.Background(),
		indexStatisticsValidRequest(),
	); err == nil {
		t.Fatalf(
			"concurrent GetIndexStatistics() = %#v, nil; want capacity error",
			result,
		)
	}
	if got := len(connection.snapshotCalls()); got != 1 {
		t.Fatalf(
			"queries after concurrent rejection = %d, want shared pool untouched",
			got,
		)
	}

	cancelFirst()
	select {
	case err := <-firstResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first GetIndexStatistics() error = %v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first GetIndexStatistics did not return after cancellation")
	}
	if _, err := reader.GetIndexStatistics(
		context.Background(),
		indexStatisticsValidRequest(),
	); err != nil {
		t.Fatalf("GetIndexStatistics() after release: %v", err)
	}
	if got := len(connection.snapshotCalls()); got != 2 {
		t.Fatalf("queries after gate release = %d, want 2", got)
	}
}

func TestIndexStatisticsPropagatesCancellation(t *testing.T) {
	t.Parallel()

	t.Run("already canceled", func(t *testing.T) {
		t.Parallel()

		connection := &indexStatisticsScriptConnection{}
		reader := mustIndexStatisticsReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if result, err := reader.GetIndexStatistics(
			ctx,
			indexStatisticsValidRequest(),
		); !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"GetIndexStatistics() = %#v, %v; want context.Canceled",
				result,
				err,
			)
		}
		if got := len(connection.snapshotCalls()); got != 0 {
			t.Fatalf("queries = %d, want 0 for pre-canceled context", got)
		}
	})

	t.Run("during aggregate scan", func(t *testing.T) {
		t.Parallel()

		entered := make(chan struct{})
		connection := &indexStatisticsScriptConnection{
			steps: []indexStatisticsScriptStep{{
				rowForContext: func(ctx context.Context) driver.Row {
					return &indexStatisticsBlockingRow{
						ctx:     ctx,
						entered: entered,
					}
				},
			}},
		}
		reader := mustIndexStatisticsReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := reader.GetIndexStatistics(
				ctx,
				indexStatisticsValidRequest(),
			)
			result <- err
		}()

		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("aggregate scan did not start")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("GetIndexStatistics() error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("GetIndexStatistics did not return after cancellation")
		}
		if got := len(connection.snapshotCalls()); got != 1 {
			t.Fatalf("queries = %d, want aggregate only", got)
		}
	})
}

type indexStatisticsGetter interface {
	GetIndexStatistics(
		context.Context,
		IndexStatisticsRequest,
	) (IndexStatisticsResult, error)
}

func mustIndexStatisticsReader(
	t *testing.T,
	connection driver.Conn,
	config IndexStatisticsConfig,
) indexStatisticsGetter {
	t.Helper()
	if config.ReadAdmission == nil {
		config.ReadAdmission = indexread.UnfencedAdmission{}
	}
	reader, err := NewIndexStatisticsReader(connection, config)
	if err != nil {
		t.Fatalf("NewIndexStatisticsReader(): %v", err)
	}
	if reader == nil {
		t.Fatal("NewIndexStatisticsReader() returned a nil reader")
	}
	return reader
}

func indexStatisticsValidRequest() IndexStatisticsRequest {
	return IndexStatisticsRequest{
		TenantID:         "tenant-a",
		IndexID:          "idx_0123456789abcdef012345",
		IndexName:        "gradethis",
		MeasuredAt:       time.Date(2026, 7, 29, 12, 34, 56, 789000000, time.UTC),
		VisibilityCutoff: 73,
	}
}

func indexStatisticsTimeArgument(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}

func assertIndexStatisticsQuery(
	t *testing.T,
	call indexStatisticsQueryCall,
	wantSQL string,
	wantArgs []any,
) {
	t.Helper()
	if got, want := strings.Join(strings.Fields(call.query), " "),
		strings.Join(strings.Fields(wantSQL), " "); got != want {
		t.Fatalf("query =\n%s\nwant:\n%s", call.query, wantSQL)
	}
	if !reflect.DeepEqual(call.args, wantArgs) {
		t.Fatalf("query args = %#v, want %#v", call.args, wantArgs)
	}
	if placeholders := strings.Count(call.query, "?"); placeholders != len(call.args) {
		t.Fatalf(
			"query placeholders/args = %d/%d, want exact parameterization",
			placeholders,
			len(call.args),
		)
	}
	for _, argument := range wantArgs {
		text, ok := argument.(string)
		if ok && text != "" && strings.Contains(call.query, text) {
			t.Fatalf("query interpolates argument %q: %s", text, call.query)
		}
	}
}

func assertIndexStatisticsBoundedSettings(
	t *testing.T,
	probe *indexStatisticsContextProbe,
	call indexStatisticsQueryCall,
) {
	t.Helper()
	settings := indexStatisticsCapturedSettings(t, probe, call.ctx)
	assertIndexStatisticsSettingEqual(t, settings, "readonly", 2)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_execution_time",
		30,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_memory_usage",
		128<<20,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_rows_to_read",
		250_000_000,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_bytes_to_read",
		64<<30,
	)
	assertIndexStatisticsSettingEqual(t, settings, "max_result_rows", 1)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_result_bytes",
		64<<10,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_threads",
		4,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_query_size",
		64<<10,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_subquery_depth",
		16,
	)
	assertIndexStatisticsSettingEqual(t, settings, "use_query_cache", 0)
	assertIndexStatisticsSettingEqual(t, settings, "async_insert", 0)
	for _, name := range []string{
		"timeout_overflow_mode",
		"read_overflow_mode",
		"result_overflow_mode",
	} {
		if got := indexStatisticsStringSetting(t, settings, name); got != "throw" {
			t.Errorf("ClickHouse setting %q = %q, want %q", name, got, "throw")
		}
	}
}

type indexStatisticsContextProbe struct {
	context.Context
	key any
}

func (probe *indexStatisticsContextProbe) Value(key any) any {
	if reflect.TypeOf(key) ==
		reflect.TypeFor[*clickhousedriver.QueryOptions]() {
		probe.key = key
	}
	return probe.Context.Value(key)
}

type indexStatisticsSettingValue struct {
	kind        reflect.Kind
	unsigned    uint64
	signed      int64
	text        string
	unsupported string
}

func indexStatisticsCapturedSettings(
	t *testing.T,
	probe *indexStatisticsContextProbe,
	ctx context.Context,
) map[string]indexStatisticsSettingValue {
	t.Helper()
	if probe.key == nil {
		t.Fatal("query context did not use clickhouse.Context with driver settings")
	}
	raw := ctx.Value(probe.key)
	if raw == nil {
		t.Fatal("query context does not carry ClickHouse QueryOptions")
	}
	options := reflect.ValueOf(raw)
	for options.Kind() == reflect.Pointer {
		if options.IsNil() {
			t.Fatal("query context carries nil ClickHouse QueryOptions")
		}
		options = options.Elem()
	}
	settings := options.FieldByName("settings")
	if !settings.IsValid() || settings.Kind() != reflect.Map {
		t.Fatalf("ClickHouse QueryOptions settings field = %#v", raw)
	}
	result := make(map[string]indexStatisticsSettingValue, settings.Len())
	iterator := settings.MapRange()
	for iterator.Next() {
		name := iterator.Key().String()
		result[name] = indexStatisticsReflectSetting(iterator.Value())
	}
	return result
}

func indexStatisticsReflectSetting(value reflect.Value) indexStatisticsSettingValue {
	for value.IsValid() &&
		(value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) {
		if value.IsNil() {
			return indexStatisticsSettingValue{unsupported: "nil"}
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return indexStatisticsSettingValue{unsupported: "invalid"}
	}
	result := indexStatisticsSettingValue{kind: value.Kind()}
	switch value.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		result.unsigned = value.Uint()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result.signed = value.Int()
	case reflect.String:
		result.text = value.String()
	default:
		result.unsupported = value.Type().String()
	}
	return result
}

func assertIndexStatisticsSettingEqual(
	t *testing.T,
	settings map[string]indexStatisticsSettingValue,
	name string,
	want uint64,
) {
	t.Helper()
	got := indexStatisticsUnsignedSetting(t, settings, name)
	if got != want {
		t.Errorf("ClickHouse setting %q = %d, want %d", name, got, want)
	}
}

func assertIndexStatisticsPositiveSettingAtMost(
	t *testing.T,
	settings map[string]indexStatisticsSettingValue,
	name string,
	maximum uint64,
) {
	t.Helper()
	got := indexStatisticsUnsignedSetting(t, settings, name)
	if got == 0 || got > maximum {
		t.Errorf(
			"ClickHouse setting %q = %d, want 0 < value <= %d",
			name,
			got,
			maximum,
		)
	}
}

func indexStatisticsUnsignedSetting(
	t *testing.T,
	settings map[string]indexStatisticsSettingValue,
	name string,
) uint64 {
	t.Helper()
	value, ok := settings[name]
	if !ok {
		t.Fatalf("ClickHouse query is missing bounded setting %q", name)
	}
	switch value.kind {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.unsigned
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.signed < 0 {
			t.Fatalf("ClickHouse setting %q = %d, want nonnegative", name, value.signed)
		}
		return uint64(value.signed)
	default:
		t.Fatalf(
			"ClickHouse setting %q has unsupported type %q",
			name,
			value.unsupported,
		)
		return 0
	}
}

func indexStatisticsStringSetting(
	t *testing.T,
	settings map[string]indexStatisticsSettingValue,
	name string,
) string {
	t.Helper()
	value, ok := settings[name]
	if !ok {
		t.Fatalf("ClickHouse query is missing bounded setting %q", name)
	}
	if value.kind != reflect.String {
		t.Fatalf(
			"ClickHouse setting %q has unsupported type %q",
			name,
			value.unsupported,
		)
	}
	return value.text
}

func indexStatisticsCeilingEstimate(
	eventCount uint64,
	partRows uint64,
	partBytes uint64,
) uint64 {
	numerator := new(big.Int).SetUint64(partBytes)
	numerator.Mul(numerator, new(big.Int).SetUint64(eventCount))
	divisor := new(big.Int).SetUint64(partRows)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, divisor, remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsUint64() {
		panic("test estimate does not fit uint64")
	}
	return quotient.Uint64()
}

type indexStatisticsScriptStep struct {
	row           driver.Row
	rowForContext func(context.Context) driver.Row
}

type indexStatisticsQueryCall struct {
	ctx   context.Context
	query string
	args  []any
}

type indexStatisticsScriptConnection struct {
	driver.Conn
	steps []indexStatisticsScriptStep
	calls []indexStatisticsQueryCall
}

func (connection *indexStatisticsScriptConnection) QueryRow(
	ctx context.Context,
	query string,
	args ...any,
) driver.Row {
	if connection == nil {
		panic("typed-nil index statistics connection was used")
	}
	callIndex := len(connection.calls)
	connection.calls = append(connection.calls, indexStatisticsQueryCall{
		ctx:   ctx,
		query: query,
		args:  append([]any(nil), args...),
	})
	if callIndex >= len(connection.steps) {
		return &indexStatisticsScriptRow{
			scanErr: fmt.Errorf("unexpected index statistics query %d", callIndex+1),
		}
	}
	step := connection.steps[callIndex]
	if step.rowForContext != nil {
		return step.rowForContext(ctx)
	}
	return step.row
}

func (connection *indexStatisticsScriptConnection) snapshotCalls() []indexStatisticsQueryCall {
	if connection == nil {
		return nil
	}
	result := make([]indexStatisticsQueryCall, len(connection.calls))
	copy(result, connection.calls)
	for index := range result {
		result[index].args = append([]any(nil), result[index].args...)
	}
	return result
}

type indexStatisticsScriptRow struct {
	values  []any
	scanErr error
	rowErr  error
}

func (row *indexStatisticsScriptRow) Err() error {
	if row == nil {
		panic("typed-nil index statistics row Err called")
	}
	return row.rowErr
}

func (row *indexStatisticsScriptRow) Scan(destinations ...any) error {
	if row == nil {
		panic("typed-nil index statistics row Scan called")
	}
	if row.scanErr != nil {
		return row.scanErr
	}
	if len(destinations) != len(row.values) {
		return fmt.Errorf(
			"scan destinations = %d, values = %d",
			len(destinations),
			len(row.values),
		)
	}
	for index, destination := range destinations {
		pointer := reflect.ValueOf(destination)
		if !pointer.IsValid() ||
			pointer.Kind() != reflect.Pointer ||
			pointer.IsNil() {
			return fmt.Errorf("scan destination %d is not a nonnil pointer", index)
		}
		target := pointer.Elem()
		value := row.values[index]
		if value == nil {
			target.SetZero()
			continue
		}
		source := reflect.ValueOf(value)
		if source.Type().AssignableTo(target.Type()) {
			target.Set(source)
			continue
		}
		if source.Type().ConvertibleTo(target.Type()) {
			target.Set(source.Convert(target.Type()))
			continue
		}
		return fmt.Errorf(
			"scan value %d type %s is not assignable to %s",
			index,
			source.Type(),
			target.Type(),
		)
	}
	return nil
}

func (row *indexStatisticsScriptRow) ScanStruct(any) error {
	return errors.New("ScanStruct is not supported by index statistics test row")
}

type indexStatisticsBlockingRow struct {
	ctx     context.Context
	entered chan struct{}
}

func (row *indexStatisticsBlockingRow) Err() error {
	return nil
}

func (row *indexStatisticsBlockingRow) Scan(...any) error {
	close(row.entered)
	<-row.ctx.Done()
	return row.ctx.Err()
}

func (row *indexStatisticsBlockingRow) ScanStruct(any) error {
	return errors.New("ScanStruct is not supported by blocking row")
}
