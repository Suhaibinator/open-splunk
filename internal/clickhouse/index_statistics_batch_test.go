package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

const indexStatisticsBatchAggregateSQL = `
SELECT
    "index_name",
    count(),
    minOrNull("event_time"),
    maxOrNull("event_time")
FROM "analytics"."logs"
PREWHERE "tenant_id" = ? AND "index_name" IN (?, ?, ?)
WHERE "expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')
  AND "index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')
  AND "visibility_seq" <= ?
GROUP BY "index_name"
ORDER BY "index_name"`

func TestIndexStatisticsBatchUsesOneGroupedQueryAndOnePartsSample(
	t *testing.T,
) {
	t.Parallel()

	request := indexStatisticsValidBatchRequest()
	alphaEarliest := request.MeasuredAt.Add(-4 * time.Hour).
		In(time.FixedZone("alpha-earliest", -7*60*60))
	alphaLatest := request.MeasuredAt.Add(-2 * time.Hour).
		In(time.FixedZone("alpha-latest", 3*60*60))
	bravoEarliest := request.MeasuredAt.Add(-3 * time.Hour)
	bravoLatest := request.MeasuredAt.Add(-time.Hour)
	logicalRows := &indexStatisticsBatchRows{values: [][]any{
		{"alpha", uint64(3), &alphaEarliest, &alphaLatest},
		{"bravo", uint64(2), &bravoEarliest, &bravoLatest},
	}}
	connection := &indexStatisticsBatchConnection{
		querySteps: []indexStatisticsBatchQueryStep{{rows: logicalRows}},
		rowSteps: []indexStatisticsBatchRowStep{{
			row: &indexStatisticsScriptRow{
				values: []any{uint64(100), uint64(1001)},
			},
		}},
	}
	reader := mustIndexStatisticsBatchReader(
		t,
		connection,
		IndexStatisticsConfig{Database: "analytics", Table: "logs"},
	)
	probe := &indexStatisticsContextProbe{Context: context.Background()}

	results, err := reader.GetIndexStatisticsBatch(probe, request)
	if err != nil {
		t.Fatalf("GetIndexStatisticsBatch(): %v", err)
	}
	if results == nil || len(results) != len(request.Indexes) {
		t.Fatalf(
			"batch results = %#v, want nonnil result for each requested index",
			results,
		)
	}
	assertIndexStatisticsBatchResult(
		t,
		results[0],
		request,
		request.Indexes[0],
		2,
		21,
		&bravoEarliest,
		&bravoLatest,
	)
	assertIndexStatisticsBatchResult(
		t,
		results[1],
		request,
		request.Indexes[1],
		0,
		0,
		nil,
		nil,
	)
	assertIndexStatisticsBatchResult(
		t,
		results[2],
		request,
		request.Indexes[2],
		3,
		31,
		&alphaEarliest,
		&alphaLatest,
	)

	queryCalls, rowCalls := connection.snapshotCalls()
	if len(queryCalls) != 1 || len(rowCalls) != 1 {
		t.Fatalf(
			"Query/QueryRow calls = %d/%d, want one grouped query and one parts sample",
			len(queryCalls),
			len(rowCalls),
		)
	}
	assertIndexStatisticsQuery(
		t,
		queryCalls[0],
		indexStatisticsBatchAggregateSQL,
		[]any{
			request.TenantID,
			"bravo",
			"empty",
			"alpha",
			indexStatisticsTimeArgument(request.MeasuredAt),
			indexStatisticsTimeArgument(request.MeasuredAt),
			request.VisibilityCutoff,
		},
	)
	assertIndexStatisticsQuery(
		t,
		rowCalls[0],
		indexStatisticsPartsSQL,
		[]any{"analytics", "logs"},
	)
	assertIndexStatisticsBatchBoundedSettings(t, probe, queryCalls[0])
	assertIndexStatisticsBoundedSettings(t, probe, rowCalls[0])
	if logicalRows.closeCalls != 1 {
		t.Fatalf("logical rows Close calls = %d, want 1", logicalRows.closeCalls)
	}
}

func TestIndexStatisticsBatchReturnsExplicitEmptyResultsWithoutPartsQuery(
	t *testing.T,
) {
	t.Parallel()

	request := indexStatisticsValidBatchRequest()
	rows := &indexStatisticsBatchRows{}
	connection := &indexStatisticsBatchConnection{
		querySteps: []indexStatisticsBatchQueryStep{{rows: rows}},
	}
	reader := mustIndexStatisticsBatchReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)

	results, err := reader.GetIndexStatisticsBatch(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("GetIndexStatisticsBatch(): %v", err)
	}
	if results == nil || len(results) != len(request.Indexes) {
		t.Fatalf("empty batch results = %#v", results)
	}
	for index, result := range results {
		assertIndexStatisticsBatchResult(
			t,
			result,
			request,
			request.Indexes[index],
			0,
			0,
			nil,
			nil,
		)
	}
	queryCalls, rowCalls := connection.snapshotCalls()
	if len(queryCalls) != 1 || len(rowCalls) != 0 {
		t.Fatalf(
			"Query/QueryRow calls = %d/%d, want 1/0 for all-empty batch",
			len(queryCalls),
			len(rowCalls),
		)
	}
	if rows.closeCalls != 1 {
		t.Fatalf("logical rows Close calls = %d, want 1", rows.closeCalls)
	}
}

func TestIndexStatisticsBatchAcceptsValidEmptyAndMaximumRequests(
	t *testing.T,
) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		request := indexStatisticsValidBatchRequest()
		request.Indexes = nil
		connection := &indexStatisticsBatchConnection{}
		reader := mustIndexStatisticsBatchReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)

		results, err := reader.GetIndexStatisticsBatch(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatalf("GetIndexStatisticsBatch(): %v", err)
		}
		if results == nil || len(results) != 0 {
			t.Fatalf("empty batch results = %#v, want nonnil empty slice", results)
		}
		queryCalls, rowCalls := connection.snapshotCalls()
		if len(queryCalls) != 0 || len(rowCalls) != 0 {
			t.Fatalf(
				"empty batch Query/QueryRow calls = %d/%d, want 0/0",
				len(queryCalls),
				len(rowCalls),
			)
		}
	})

	t.Run("maximum", func(t *testing.T) {
		t.Parallel()

		request := indexStatisticsValidBatchRequest()
		request.Indexes = make([]IndexStatisticsScope, 64)
		for index := range request.Indexes {
			namePrefix := fmt.Sprintf("%02d-", index+1)
			request.Indexes[index] = IndexStatisticsScope{
				IndexID: fmt.Sprintf("idx_%024x", index+1),
				IndexName: namePrefix + strings.Repeat(
					"a",
					indexname.MaximumBytes-len(namePrefix),
				),
			}
		}
		connection := &indexStatisticsBatchConnection{
			querySteps: []indexStatisticsBatchQueryStep{{
				rows: &indexStatisticsBatchRows{},
			}},
		}
		reader := mustIndexStatisticsBatchReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)

		results, err := reader.GetIndexStatisticsBatch(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatalf("GetIndexStatisticsBatch(): %v", err)
		}
		if len(results) != 64 {
			t.Fatalf("maximum batch results = %d, want 64", len(results))
		}
		queryCalls, rowCalls := connection.snapshotCalls()
		if len(queryCalls) != 1 || len(rowCalls) != 0 {
			t.Fatalf(
				"maximum batch Query/QueryRow calls = %d/%d, want 1/0",
				len(queryCalls),
				len(rowCalls),
			)
		}
		if placeholders := strings.Count(queryCalls[0].query, "?"); placeholders != len(queryCalls[0].args) ||
			placeholders != 64+4 {
			t.Fatalf(
				"maximum batch placeholders/args = %d/%d, want 68/68",
				placeholders,
				len(queryCalls[0].args),
			)
		}
		expandedBytes := indexStatisticsExpandedQueryBytes(
			t,
			queryCalls[0].query,
			queryCalls[0].args,
		)
		if expandedBytes <= int(indexStatisticsMaxQueryBytes) ||
			expandedBytes > int(indexStatisticsBatchMaxQueryBytes) {
			t.Fatalf(
				"maximum expanded batch query bytes = %d, want %d < bytes <= %d",
				expandedBytes,
				indexStatisticsMaxQueryBytes,
				indexStatisticsBatchMaxQueryBytes,
			)
		}
	})
}

func TestIndexStatisticsBatchRejectsInvalidRequestsBeforeQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*IndexStatisticsBatchRequest)
	}{
		{
			name: "empty tenant",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.TenantID = ""
			},
		},
		{
			name: "invalid measurement",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.MeasuredAt = request.MeasuredAt.Add(time.Nanosecond)
			},
		},
		{
			name: "too many indexes",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.Indexes = make([]IndexStatisticsScope, 65)
				for index := range request.Indexes {
					request.Indexes[index] = IndexStatisticsScope{
						IndexID:   fmt.Sprintf("idx_%024x", index+1),
						IndexName: fmt.Sprintf("index-%02d", index+1),
					}
				}
			},
		},
		{
			name: "invalid index ID",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.Indexes[0].IndexID = " invalid"
			},
		},
		{
			name: "invalid index name",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.Indexes[0].IndexName = "Alpha"
			},
		},
		{
			name: "duplicate index ID",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.Indexes[1].IndexID = request.Indexes[0].IndexID
			},
		},
		{
			name: "duplicate index name",
			mutate: func(request *IndexStatisticsBatchRequest) {
				request.Indexes[1].IndexName = request.Indexes[0].IndexName
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := indexStatisticsValidBatchRequest()
			test.mutate(&request)
			connection := &indexStatisticsBatchConnection{}
			reader := mustIndexStatisticsBatchReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			if results, err := reader.GetIndexStatisticsBatch(
				context.Background(),
				request,
			); err == nil || results != nil {
				t.Fatalf(
					"GetIndexStatisticsBatch() = (%#v, %v), want nil/error",
					results,
					err,
				)
			}
			queryCalls, rowCalls := connection.snapshotCalls()
			if len(queryCalls) != 0 || len(rowCalls) != 0 {
				t.Fatalf(
					"invalid batch Query/QueryRow calls = %d/%d, want 0/0",
					len(queryCalls),
					len(rowCalls),
				)
			}
		})
	}

	request := indexStatisticsValidBatchRequest()
	request.Indexes = nil
	request.TenantID = ""
	connection := &indexStatisticsBatchConnection{}
	reader := mustIndexStatisticsBatchReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	if results, err := reader.GetIndexStatisticsBatch(
		context.Background(),
		request,
	); err == nil || results != nil {
		t.Fatalf(
			"invalid common empty batch = (%#v, %v), want nil/error",
			results,
			err,
		)
	}

	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if results, err := reader.GetIndexStatisticsBatch(
		nil,
		indexStatisticsValidBatchRequest(),
	); err == nil || results != nil {
		t.Fatalf(
			"GetIndexStatisticsBatch(nil) = (%#v, %v), want nil/error",
			results,
			err,
		)
	}
	var nilReader *IndexStatisticsReader
	if results, err := nilReader.GetIndexStatisticsBatch(
		context.Background(),
		indexStatisticsValidBatchRequest(),
	); err == nil || results != nil {
		t.Fatalf(
			"nil reader GetIndexStatisticsBatch() = (%#v, %v), want nil/error",
			results,
			err,
		)
	}
}

func TestIndexStatisticsBatchRejectsMalformedLogicalRowsAndCloses(
	t *testing.T,
) {
	t.Parallel()

	request := indexStatisticsValidBatchRequest()
	earliest := request.MeasuredAt.Add(-time.Hour)
	latest := request.MeasuredAt.Add(-time.Minute)
	scanErr := io.ErrUnexpectedEOF
	rowsErr := errors.New("row stream failed")
	closeErr := errors.New("close failed")
	queryErr := errors.New("query failed")
	var typedNilRows *indexStatisticsBatchRows
	tests := []struct {
		name      string
		queryStep indexStatisticsBatchQueryStep
		wantClose int
	}{
		{
			name:      "query error",
			queryStep: indexStatisticsBatchQueryStep{err: queryErr},
		},
		{
			name:      "nil rows",
			queryStep: indexStatisticsBatchQueryStep{},
		},
		{
			name: "typed nil rows",
			queryStep: indexStatisticsBatchQueryStep{
				rows: typedNilRows,
			},
		},
		{
			name: "unknown index",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{values: [][]any{{
					"unknown", uint64(1), &earliest, &latest,
				}}},
			},
			wantClose: 1,
		},
		{
			name: "duplicate index",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{values: [][]any{
					{"alpha", uint64(1), &earliest, &latest},
					{"alpha", uint64(1), &earliest, &latest},
				}},
			},
			wantClose: 1,
		},
		{
			name: "returned empty group",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{values: [][]any{{
					"alpha", uint64(0), nil, nil,
				}}},
			},
			wantClose: 1,
		},
		{
			name: "nonempty group omits bound",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{values: [][]any{{
					"alpha", uint64(1), nil, &latest,
				}}},
			},
			wantClose: 1,
		},
		{
			name: "scan error",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{
					values:    [][]any{{"alpha", uint64(1), &earliest, &latest}},
					scanErrAt: 0,
					scanErr:   scanErr,
				},
			},
			wantClose: 1,
		},
		{
			name: "rows error",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{rowErr: rowsErr},
			},
			wantClose: 1,
		},
		{
			name: "close error",
			queryStep: indexStatisticsBatchQueryStep{
				rows: &indexStatisticsBatchRows{closeErr: closeErr},
			},
			wantClose: 1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsBatchConnection{
				querySteps: []indexStatisticsBatchQueryStep{test.queryStep},
			}
			reader := mustIndexStatisticsBatchReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			results, err := reader.GetIndexStatisticsBatch(
				context.Background(),
				request,
			)
			if err == nil || results != nil {
				t.Fatalf(
					"GetIndexStatisticsBatch() = (%#v, %v), want nil/error",
					results,
					err,
				)
			}
			if test.queryStep.rows != nil &&
				!isNilIndexStatisticsDependency(test.queryStep.rows) {
				rows := test.queryStep.rows.(*indexStatisticsBatchRows)
				if rows.closeCalls != test.wantClose {
					t.Fatalf(
						"rows Close calls = %d, want %d",
						rows.closeCalls,
						test.wantClose,
					)
				}
			}
			queryCalls, rowCalls := connection.snapshotCalls()
			if len(queryCalls) != 1 || len(rowCalls) != 0 {
				t.Fatalf(
					"malformed batch Query/QueryRow calls = %d/%d, want 1/0",
					len(queryCalls),
					len(rowCalls),
				)
			}
		})
	}
}

func TestIndexStatisticsBatchRejectsOverflowAndInconsistentPartsTotals(
	t *testing.T,
) {
	t.Parallel()

	request := indexStatisticsValidBatchRequest()
	earliest := request.MeasuredAt.Add(-time.Hour)
	latest := request.MeasuredAt.Add(-time.Minute)
	tests := []struct {
		name          string
		logicalValues [][]any
		partsValues   []any
		wantRowCalls  int
	}{
		{
			name: "logical total overflows",
			logicalValues: [][]any{
				{"alpha", ^uint64(0), &earliest, &latest},
				{"bravo", uint64(1), &earliest, &latest},
			},
			wantRowCalls: 0,
		},
		{
			name: "logical total exceeds physical rows",
			logicalValues: [][]any{
				{"alpha", uint64(3), &earliest, &latest},
				{"bravo", uint64(2), &earliest, &latest},
			},
			partsValues:  []any{uint64(4), uint64(100)},
			wantRowCalls: 1,
		},
		{
			name: "positive logical total has zero physical bytes",
			logicalValues: [][]any{
				{"alpha", uint64(1), &earliest, &latest},
			},
			partsValues:  []any{uint64(10), uint64(0)},
			wantRowCalls: 1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &indexStatisticsBatchConnection{
				querySteps: []indexStatisticsBatchQueryStep{{
					rows: &indexStatisticsBatchRows{values: test.logicalValues},
				}},
			}
			if test.partsValues != nil {
				connection.rowSteps = []indexStatisticsBatchRowStep{{
					row: &indexStatisticsScriptRow{values: test.partsValues},
				}}
			}
			reader := mustIndexStatisticsBatchReader(
				t,
				connection,
				IndexStatisticsConfig{},
			)

			results, err := reader.GetIndexStatisticsBatch(
				context.Background(),
				request,
			)
			if err == nil || results != nil {
				t.Fatalf(
					"GetIndexStatisticsBatch() = (%#v, %v), want nil/error",
					results,
					err,
				)
			}
			queryCalls, rowCalls := connection.snapshotCalls()
			if len(queryCalls) != 1 || len(rowCalls) != test.wantRowCalls {
				t.Fatalf(
					"Query/QueryRow calls = %d/%d, want 1/%d",
					len(queryCalls),
					len(rowCalls),
					test.wantRowCalls,
				)
			}
		})
	}
}

func TestIndexStatisticsBatchPropagatesCancellationAndCapsDeadline(
	t *testing.T,
) {
	t.Parallel()

	t.Run("pre-canceled", func(t *testing.T) {
		t.Parallel()

		connection := &indexStatisticsBatchConnection{}
		reader := mustIndexStatisticsBatchReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		results, err := reader.GetIndexStatisticsBatch(
			ctx,
			indexStatisticsValidBatchRequest(),
		)
		if !errors.Is(err, context.Canceled) || results != nil {
			t.Fatalf(
				"GetIndexStatisticsBatch() = (%#v, %v), want nil/context.Canceled",
				results,
				err,
			)
		}
		queryCalls, _ := connection.snapshotCalls()
		if len(queryCalls) != 0 {
			t.Fatalf("pre-canceled queries = %d, want 0", len(queryCalls))
		}
	})

	t.Run("during row iteration", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		rows := &indexStatisticsCancelingBatchRows{
			indexStatisticsBatchRows: indexStatisticsBatchRows{},
			cancel:                   cancel,
		}
		connection := &indexStatisticsBatchConnection{
			querySteps: []indexStatisticsBatchQueryStep{{rows: rows}},
		}
		reader := mustIndexStatisticsBatchReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)

		results, err := reader.GetIndexStatisticsBatch(
			ctx,
			indexStatisticsValidBatchRequest(),
		)
		if !errors.Is(err, context.Canceled) || results != nil {
			t.Fatalf(
				"GetIndexStatisticsBatch() = (%#v, %v), want nil/context.Canceled",
				results,
				err,
			)
		}
		if rows.closeCalls != 1 {
			t.Fatalf("rows Close calls = %d, want 1", rows.closeCalls)
		}
	})

	t.Run("caps caller deadline", func(t *testing.T) {
		t.Parallel()

		connection := &indexStatisticsBatchConnection{
			querySteps: []indexStatisticsBatchQueryStep{{
				rows: &indexStatisticsBatchRows{},
			}},
		}
		reader := mustIndexStatisticsBatchReader(
			t,
			connection,
			IndexStatisticsConfig{},
		)
		caller, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		probe := &indexStatisticsContextProbe{Context: caller}

		if _, err := reader.GetIndexStatisticsBatch(
			probe,
			indexStatisticsValidBatchRequest(),
		); err != nil {
			t.Fatalf("GetIndexStatisticsBatch(): %v", err)
		}
		queryCalls, _ := connection.snapshotCalls()
		if len(queryCalls) != 1 {
			t.Fatalf("queries = %d, want 1", len(queryCalls))
		}
		deadline, ok := queryCalls[0].ctx.Deadline()
		if !ok {
			t.Fatal("batch query context has no reader-owned deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > indexStatisticsOperationTimeout {
			t.Fatalf(
				"batch query deadline remaining = %s, want 0 < remaining <= %s",
				remaining,
				indexStatisticsOperationTimeout,
			)
		}
	})
}

func TestIndexStatisticsBatchSharesSingleOperationGate(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	connection := &indexStatisticsBatchConnection{
		querySteps: []indexStatisticsBatchQueryStep{
			{
				rowsForContext: func(ctx context.Context) driver.Rows {
					return &indexStatisticsBlockingBatchRows{
						ctx:     ctx,
						entered: entered,
					}
				},
			},
			{rows: &indexStatisticsBatchRows{}},
		},
	}
	reader := mustIndexStatisticsBatchReader(
		t,
		connection,
		IndexStatisticsConfig{},
	)
	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := reader.GetIndexStatisticsBatch(
			firstContext,
			indexStatisticsValidBatchRequest(),
		)
		firstResult <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch query did not begin row iteration")
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
	queryCalls, rowCalls := connection.snapshotCalls()
	if len(queryCalls) != 1 || len(rowCalls) != 0 {
		t.Fatalf(
			"calls after concurrent rejection = %d/%d, want 1/0",
			len(queryCalls),
			len(rowCalls),
		)
	}

	cancelFirst()
	select {
	case err := <-firstResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first batch error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first batch did not return after cancellation")
	}
	if _, err := reader.GetIndexStatisticsBatch(
		context.Background(),
		indexStatisticsValidBatchRequest(),
	); err != nil {
		t.Fatalf("GetIndexStatisticsBatch() after gate release: %v", err)
	}
}

func indexStatisticsValidBatchRequest() IndexStatisticsBatchRequest {
	request := indexStatisticsValidRequest()
	return IndexStatisticsBatchRequest{
		TenantID: request.TenantID,
		Indexes: []IndexStatisticsScope{
			{
				IndexID:   "idx_1123456789abcdef012345",
				IndexName: "bravo",
			},
			{
				IndexID:   "idx_2123456789abcdef012345",
				IndexName: "empty",
			},
			{
				IndexID:   "idx_3123456789abcdef012345",
				IndexName: "alpha",
			},
		},
		MeasuredAt:       request.MeasuredAt,
		VisibilityCutoff: request.VisibilityCutoff,
	}
}

func mustIndexStatisticsBatchReader(
	t *testing.T,
	connection *indexStatisticsBatchConnection,
	config IndexStatisticsConfig,
) *IndexStatisticsReader {
	t.Helper()
	if config.ReadAdmission == nil {
		config.ReadAdmission = indexread.UnfencedAdmission{}
	}
	reader, err := NewIndexStatisticsReader(connection, config)
	if err != nil {
		t.Fatalf("NewIndexStatisticsReader(): %v", err)
	}
	if reader == nil {
		t.Fatal("NewIndexStatisticsReader() returned nil")
	}
	return reader
}

func assertIndexStatisticsBatchResult(
	t *testing.T,
	result IndexStatisticsResult,
	request IndexStatisticsBatchRequest,
	scope IndexStatisticsScope,
	eventCount uint64,
	storageBytes uint64,
	earliest *time.Time,
	latest *time.Time,
) {
	t.Helper()
	if result.TenantID != request.TenantID ||
		result.IndexID != scope.IndexID ||
		result.IndexName != scope.IndexName ||
		result.VisibilityCutoff != request.VisibilityCutoff ||
		result.EventCount != eventCount ||
		result.StorageBytes != storageBytes ||
		!result.MeasuredAt.Equal(request.MeasuredAt) ||
		result.MeasuredAt.Location() != time.UTC ||
		!result.Estimates {
		t.Fatalf("batch result = %#v, want exact echoed scope/count/estimate", result)
	}
	assertIndexStatisticsBatchTime(t, result.EarliestEventTime, earliest, "earliest")
	assertIndexStatisticsBatchTime(t, result.LatestEventTime, latest, "latest")
}

func assertIndexStatisticsBatchTime(
	t *testing.T,
	got *time.Time,
	want *time.Time,
	label string,
) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatalf("%s event time = %s, want nil", label, got)
		}
		return
	}
	if got == nil || !got.Equal(*want) || got.Location() != time.UTC {
		t.Fatalf("%s event time = %v, want UTC %s", label, got, want.UTC())
	}
}

func assertIndexStatisticsBatchBoundedSettings(
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
	assertIndexStatisticsSettingEqual(t, settings, "max_result_rows", 64)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_result_bytes",
		64<<10,
	)
	assertIndexStatisticsPositiveSettingAtMost(t, settings, "max_threads", 4)
	assertIndexStatisticsSettingEqual(
		t,
		settings,
		"max_query_size",
		indexStatisticsBatchMaxQueryBytes,
	)
	assertIndexStatisticsPositiveSettingAtMost(
		t,
		settings,
		"max_subquery_depth",
		16,
	)
	assertIndexStatisticsSettingEqual(t, settings, "max_rows_to_group_by", 64)
	assertIndexStatisticsSettingEqual(t, settings, "use_query_cache", 0)
	assertIndexStatisticsSettingEqual(t, settings, "async_insert", 0)
	for _, name := range []string{
		"timeout_overflow_mode",
		"read_overflow_mode",
		"result_overflow_mode",
		"group_by_overflow_mode",
	} {
		if got := indexStatisticsStringSetting(t, settings, name); got != "throw" {
			t.Errorf("ClickHouse setting %q = %q, want %q", name, got, "throw")
		}
	}
}

func indexStatisticsExpandedQueryBytes(
	t *testing.T,
	query string,
	args []any,
) int {
	t.Helper()
	expandedBytes := len(query) - len(args)
	for _, argument := range args {
		switch value := argument.(type) {
		case string:
			// All strings accepted by this reader exclude quotes, so the
			// driver's SQL literal adds exactly the surrounding quotes.
			if strings.ContainsAny(value, "'\\") {
				t.Fatalf("test argument requires escaping: %q", value)
			}
			expandedBytes += len(value) + 2
		case uint64:
			expandedBytes += len(strconv.FormatUint(value, 10))
		default:
			t.Fatalf("unsupported batch query argument type %T", argument)
		}
	}
	return expandedBytes
}

type indexStatisticsBatchQueryStep struct {
	rows           driver.Rows
	rowsForContext func(context.Context) driver.Rows
	err            error
}

type indexStatisticsBatchRowStep struct {
	row driver.Row
}

type indexStatisticsBatchConnection struct {
	driver.Conn

	mutex      sync.Mutex
	querySteps []indexStatisticsBatchQueryStep
	rowSteps   []indexStatisticsBatchRowStep
	queryCalls []indexStatisticsQueryCall
	rowCalls   []indexStatisticsQueryCall
}

func (connection *indexStatisticsBatchConnection) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	callIndex := len(connection.queryCalls)
	connection.queryCalls = append(connection.queryCalls, indexStatisticsQueryCall{
		ctx:   ctx,
		query: query,
		args:  append([]any(nil), args...),
	})
	if callIndex >= len(connection.querySteps) {
		return nil, fmt.Errorf("unexpected batch Query call %d", callIndex+1)
	}
	step := connection.querySteps[callIndex]
	if step.rowsForContext != nil {
		return step.rowsForContext(ctx), step.err
	}
	return step.rows, step.err
}

func (connection *indexStatisticsBatchConnection) QueryRow(
	ctx context.Context,
	query string,
	args ...any,
) driver.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	callIndex := len(connection.rowCalls)
	connection.rowCalls = append(connection.rowCalls, indexStatisticsQueryCall{
		ctx:   ctx,
		query: query,
		args:  append([]any(nil), args...),
	})
	if callIndex >= len(connection.rowSteps) {
		return &indexStatisticsScriptRow{
			scanErr: fmt.Errorf("unexpected batch QueryRow call %d", callIndex+1),
		}
	}
	return connection.rowSteps[callIndex].row
}

func (connection *indexStatisticsBatchConnection) snapshotCalls() (
	[]indexStatisticsQueryCall,
	[]indexStatisticsQueryCall,
) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	queryCalls := append([]indexStatisticsQueryCall(nil), connection.queryCalls...)
	rowCalls := append([]indexStatisticsQueryCall(nil), connection.rowCalls...)
	for index := range queryCalls {
		queryCalls[index].args = append([]any(nil), queryCalls[index].args...)
	}
	for index := range rowCalls {
		rowCalls[index].args = append([]any(nil), rowCalls[index].args...)
	}
	return queryCalls, rowCalls
}

type indexStatisticsBatchRows struct {
	values     [][]any
	nextIndex  int
	scanErrAt  int
	scanErr    error
	rowErr     error
	closeErr   error
	closeCalls int
}

func (rows *indexStatisticsBatchRows) Next() bool {
	if rows == nil {
		panic("typed-nil index statistics batch rows Next called")
	}
	if rows.nextIndex >= len(rows.values) {
		return false
	}
	rows.nextIndex++
	return true
}

func (rows *indexStatisticsBatchRows) Scan(destinations ...any) error {
	if rows == nil {
		panic("typed-nil index statistics batch rows Scan called")
	}
	index := rows.nextIndex - 1
	if rows.scanErr != nil && index == rows.scanErrAt {
		return rows.scanErr
	}
	if index < 0 || index >= len(rows.values) {
		return errors.New("Scan called without a current row")
	}
	return indexStatisticsAssignBatchValues(rows.values[index], destinations)
}

func (rows *indexStatisticsBatchRows) ScanStruct(any) error {
	return errors.New("ScanStruct is not supported by index statistics batch rows")
}

func (rows *indexStatisticsBatchRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (rows *indexStatisticsBatchRows) Totals(...any) error {
	return errors.New("Totals is not supported by index statistics batch rows")
}

func (rows *indexStatisticsBatchRows) Columns() []string {
	return []string{"index_name", "count()", "minOrNull(event_time)", "maxOrNull(event_time)"}
}

func (rows *indexStatisticsBatchRows) Close() error {
	if rows == nil {
		panic("typed-nil index statistics batch rows Close called")
	}
	rows.closeCalls++
	return rows.closeErr
}

func (rows *indexStatisticsBatchRows) Err() error {
	if rows == nil {
		panic("typed-nil index statistics batch rows Err called")
	}
	return rows.rowErr
}

func (rows *indexStatisticsBatchRows) HasData() bool {
	return len(rows.values) > 0
}

func indexStatisticsAssignBatchValues(values []any, destinations []any) error {
	if len(values) != len(destinations) {
		return fmt.Errorf(
			"batch scan values/destinations = %d/%d",
			len(values),
			len(destinations),
		)
	}
	for index, destination := range destinations {
		pointer := reflect.ValueOf(destination)
		if !pointer.IsValid() ||
			pointer.Kind() != reflect.Pointer ||
			pointer.IsNil() {
			return fmt.Errorf("batch scan destination %d is not a pointer", index)
		}
		target := pointer.Elem()
		if values[index] == nil {
			target.SetZero()
			continue
		}
		source := reflect.ValueOf(values[index])
		if source.Type().AssignableTo(target.Type()) {
			target.Set(source)
			continue
		}
		if source.Type().ConvertibleTo(target.Type()) {
			target.Set(source.Convert(target.Type()))
			continue
		}
		return fmt.Errorf(
			"batch scan value %d type %s is not assignable to %s",
			index,
			source.Type(),
			target.Type(),
		)
	}
	return nil
}

type indexStatisticsCancelingBatchRows struct {
	indexStatisticsBatchRows
	cancel context.CancelFunc
}

func (rows *indexStatisticsCancelingBatchRows) Next() bool {
	rows.cancel()
	return false
}

type indexStatisticsBlockingBatchRows struct {
	ctx        context.Context
	entered    chan struct{}
	enteredOne sync.Once
}

func (rows *indexStatisticsBlockingBatchRows) Next() bool {
	rows.enteredOne.Do(func() { close(rows.entered) })
	<-rows.ctx.Done()
	return false
}

func (rows *indexStatisticsBlockingBatchRows) Scan(...any) error {
	return errors.New("blocking batch rows have no current row")
}

func (rows *indexStatisticsBlockingBatchRows) ScanStruct(any) error {
	return errors.New("ScanStruct is not supported by blocking batch rows")
}

func (rows *indexStatisticsBlockingBatchRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (rows *indexStatisticsBlockingBatchRows) Totals(...any) error {
	return errors.New("Totals is not supported by blocking batch rows")
}

func (rows *indexStatisticsBlockingBatchRows) Columns() []string {
	return nil
}

func (rows *indexStatisticsBlockingBatchRows) Close() error {
	return nil
}

func (rows *indexStatisticsBlockingBatchRows) Err() error {
	return rows.ctx.Err()
}

func (rows *indexStatisticsBlockingBatchRows) HasData() bool {
	return false
}
