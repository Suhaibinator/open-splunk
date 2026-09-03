//go:build !windows

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	backendInsertTargetRows           = uint64(10_000)
	backendInsertMaxRows              = uint64(50_000)
	backendInsertBurstRequests        = 30
	backendInsertBurstConcurrency     = 8
	backendInsertBurstEvents          = backendInsertBurstRequests * backendHECLoadFullEvents
	backendInsertCoalescingChannel    = "123e4567-e89b-42d3-a456-426614174013"
	backendInsertSteadyStateRequests  = 40
	backendInsertSteadyStateWorkers   = 40
	backendInsertSteadyStateEvents    = backendInsertSteadyStateRequests * backendHECLoadFullEvents
	backendInsertSteadyStateChannel   = "123e4567-e89b-42d3-a456-426614174014"
	backendInsertSteadyStateChannel2  = "123e4567-e89b-42d3-a456-426614174015"
	backendInsertSteadyStateChannel3  = "123e4567-e89b-42d3-a456-426614174016"
	backendInsertSteadyStateChannel4  = "123e4567-e89b-42d3-a456-426614174017"
	backendInsertBurstRetryLimit      = 20
	backendInsertBurstRetryDelay      = 25 * time.Millisecond
	backendInsertQuiescenceSamples    = 5
	backendInsertQuiescenceInterval   = 200 * time.Millisecond
	backendInsertPartGrowthSlack      = uint64(2)
	backendInsertMaximumMergePasses   = uint64(4)
	backendInsertMaximumMergeTime     = 60 * time.Second
	backendInsertEventsDatabase       = "open_splunk"
	backendInsertEventsTable          = "events"
	backendInsertEventsQualifiedTable = backendInsertEventsDatabase + "." + backendInsertEventsTable
)

type backendPhysicalInsertShape struct {
	Rows            []uint64
	ZeroRowFinishes uint64
	Diagnostics     []string
}

func (shape backendPhysicalInsertShape) count() uint64 {
	return uint64(len(shape.Rows))
}

func (shape backendPhysicalInsertShape) totalRows() uint64 {
	var total uint64
	for _, rows := range shape.Rows {
		total += rows
	}
	return total
}

func (shape backendPhysicalInsertShape) maximumRows() uint64 {
	return slices.Max(append([]uint64{0}, shape.Rows...))
}

func (shape backendPhysicalInsertShape) validate(
	minimumObservedRows uint64,
	minimumTargetInserts uint64,
) error {
	if minimumObservedRows == 0 || minimumTargetInserts == 0 {
		return errors.New("physical insert shape expectation must be positive")
	}
	if len(shape.Rows) == 0 {
		return fmt.Errorf(
			"ClickHouse query log contains no positive-row physical event inserts (%d zero-row finishes)",
			shape.ZeroRowFinishes,
		)
	}
	var targetInserts uint64
	for ordinal, rows := range shape.Rows {
		switch {
		case rows == 0:
			return fmt.Errorf("physical insert %d reports zero rows", ordinal)
		case rows > backendInsertMaxRows:
			return fmt.Errorf(
				"physical insert %d has %d rows, above hard maximum %d",
				ordinal,
				rows,
				backendInsertMaxRows,
			)
		}
		if rows >= backendInsertTargetRows {
			targetInserts++
		}
	}
	if shape.totalRows() < minimumObservedRows {
		return fmt.Errorf(
			"physical inserts account for %d rows, want at least %d (shape %v, zero-row finishes %d, diagnostics %v)",
			shape.totalRows(),
			minimumObservedRows,
			shape.Rows,
			shape.ZeroRowFinishes,
			shape.Diagnostics,
		)
	}
	if targetInserts < minimumTargetInserts {
		return fmt.Errorf(
			"physical inserts at or above %d rows = %d, want at least %d (shape %v)",
			backendInsertTargetRows,
			targetInserts,
			minimumTargetInserts,
			shape.Rows,
		)
	}
	return nil
}

// validateSteadyState applies the published performance envelope only to a
// window that deliberately kept at least the target rows eligible ahead of
// maximum linger. Startup, recovery, sparse traffic, and final drain must use
// validate instead; they are allowed to create smaller inserts.
func (shape backendPhysicalInsertShape) validateSteadyState(
	logicalAcceptedBatches uint64,
	logicalAcceptedRows uint64,
) error {
	if logicalAcceptedBatches == 0 || logicalAcceptedRows == 0 {
		return errors.New("steady-state logical accepted batch and row counts must be positive")
	}
	if err := shape.validate(logicalAcceptedRows, 1); err != nil {
		return err
	}
	if shape.totalRows() != logicalAcceptedRows {
		return fmt.Errorf(
			"steady-state physical inserts account for %d rows, want exactly %d (shape %v, diagnostics %v)",
			shape.totalRows(),
			logicalAcceptedRows,
			shape.Rows,
			shape.Diagnostics,
		)
	}
	sortedRows := slices.Clone(shape.Rows)
	slices.Sort(sortedRows)
	lowerMedian := sortedRows[(len(sortedRows)-1)/2]
	if lowerMedian < backendInsertTargetRows {
		return fmt.Errorf(
			"steady-state lower median physical insert is %d rows, want at least %d (shape %v, diagnostics %v)",
			lowerMedian,
			backendInsertTargetRows,
			shape.Rows,
			shape.Diagnostics,
		)
	}
	var insertsAtLeastFiveThousand uint64
	for _, rows := range sortedRows {
		if rows >= 5_000 {
			insertsAtLeastFiveThousand++
		}
	}
	if insertsAtLeastFiveThousand*10 < uint64(len(sortedRows))*9 {
		return fmt.Errorf(
			"steady-state physical inserts at or above 5000 rows = %d/%d, want at least 90%% (shape %v, diagnostics %v)",
			insertsAtLeastFiveThousand,
			len(sortedRows),
			shape.Rows,
			shape.Diagnostics,
		)
	}
	if uint64(len(sortedRows)) > logicalAcceptedBatches/10 {
		return fmt.Errorf(
			"steady-state physical/logical insert ratio = %d/%d, want at most 1/10 (shape %v, diagnostics %v)",
			len(sortedRows),
			logicalAcceptedBatches,
			shape.Rows,
			shape.Diagnostics,
		)
	}
	return nil
}

func backendPhysicalInsertShapeQuery() string {
	return `SELECT greatest(
			toUInt64(written_rows),
			toUInt64(ProfileEvents['InsertedRows'])
		 ),
		 toUInt64(written_rows),
		 toUInt64(ProfileEvents['InsertedRows']),
		 query,
		 toString(tables),
		 toString(event_time_microseconds)
		 FROM system.query_log
		 WHERE type = 'QueryFinish'
		   AND query_kind = 'Insert'
		   AND has(databases, ?)
		   AND has(tables, ?)
		   AND event_time_microseconds >= ?
		 ORDER BY event_time_microseconds, query_id`
}

// prepareBackendPhysicalInsertInspection grants the least-privilege runtime
// test observer access to ClickHouse's own query log. Production credentials
// remain unchanged; the disposable fixture is destroyed after the test.
func prepareBackendPhysicalInsertInspection(
	ctx context.Context,
	clickHouse *testsupport.ClickHouseContainer,
) error {
	for _, statement := range []string{
		"GRANT SELECT ON system.query_log TO open_splunk_runtime",
		"GRANT SELECT ON system.events TO open_splunk_runtime",
		"GRANT SELECT ON system.merges TO open_splunk_runtime",
		"GRANT SELECT ON system.parts TO open_splunk_runtime",
	} {
		if err := clickHouse.ExecuteBootstrapSQLForTest(ctx, statement); err != nil {
			return fmt.Errorf("prepare ClickHouse insert inspection with %q: %w", statement, err)
		}
	}
	return nil
}

func beginBackendPhysicalInsertWindow(
	ctx context.Context,
	clickHouse *testsupport.ClickHouseContainer,
	connection clickhousedriver.Conn,
) (time.Time, error) {
	if clickHouse == nil {
		return time.Time{}, errors.New("ClickHouse fixture is required")
	}
	if err := clickHouse.ExecuteBootstrapSQLForTest(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return time.Time{}, fmt.Errorf("flush ClickHouse query log before insert window: %w", err)
	}
	var startedAt time.Time
	if err := connection.QueryRow(ctx, "SELECT now64(6)").Scan(&startedAt); err != nil {
		return time.Time{}, fmt.Errorf("read ClickHouse insert-window clock: %w", err)
	}
	if startedAt.IsZero() {
		return time.Time{}, errors.New("ClickHouse insert-window clock returned zero")
	}
	return startedAt.UTC(), nil
}

// waitForBackendPhysicalInsertQuiescence closes the small interval between a
// durable terminal commit and ClickHouse publishing QueryFinish. Without this
// barrier, a just-completed recovery insert can legitimately appear after the
// administrator queue first reports empty and contaminate the live window.
func waitForBackendPhysicalInsertQuiescence(
	ctx context.Context,
	clickHouse *testsupport.ClickHouseContainer,
	connection clickhousedriver.Conn,
) error {
	if clickHouse == nil {
		return errors.New("ClickHouse fixture is required")
	}
	var (
		previousCount uint64
		havePrevious  bool
		stableSamples int
	)
	for stableSamples < backendInsertQuiescenceSamples {
		if err := clickHouse.ExecuteBootstrapSQLForTest(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			return fmt.Errorf("flush ClickHouse query log while waiting for quiescence: %w", err)
		}
		var count uint64
		if err := connection.QueryRow(
			ctx,
			`SELECT count()
			 FROM system.query_log
			 WHERE type = 'QueryFinish'
			   AND query_kind = 'Insert'
			   AND has(databases, ?)
			   AND has(tables, ?)`,
			backendInsertEventsDatabase,
			backendInsertEventsQualifiedTable,
		).Scan(&count); err != nil {
			return fmt.Errorf("read ClickHouse insert count while waiting for quiescence: %w", err)
		}
		if havePrevious && count == previousCount {
			stableSamples++
		} else {
			stableSamples = 0
		}
		previousCount = count
		havePrevious = true
		if stableSamples == backendInsertQuiescenceSamples {
			return nil
		}
		timer := time.NewTimer(backendInsertQuiescenceInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func readBackendPhysicalInsertShape(
	ctx context.Context,
	clickHouse *testsupport.ClickHouseContainer,
	connection clickhousedriver.Conn,
	startedAt time.Time,
) (backendPhysicalInsertShape, error) {
	if clickHouse == nil {
		return backendPhysicalInsertShape{}, errors.New("ClickHouse fixture is required")
	}
	if startedAt.IsZero() {
		return backendPhysicalInsertShape{}, errors.New("physical insert window start is required")
	}
	if err := clickHouse.ExecuteBootstrapSQLForTest(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return backendPhysicalInsertShape{}, fmt.Errorf("flush ClickHouse query log: %w", err)
	}
	rows, err := connection.Query(
		ctx,
		backendPhysicalInsertShapeQuery(),
		backendInsertEventsDatabase,
		backendInsertEventsQualifiedTable,
		startedAt.UTC(),
	)
	if err != nil {
		return backendPhysicalInsertShape{}, fmt.Errorf("read ClickHouse physical insert shape: %w", err)
	}
	shape := backendPhysicalInsertShape{}
	for rows.Next() {
		var (
			inserted            uint64
			writtenRows         uint64
			profileInsertedRows uint64
			query               string
			tables              string
			eventTime           string
		)
		if err := rows.Scan(
			&inserted,
			&writtenRows,
			&profileInsertedRows,
			&query,
			&tables,
			&eventTime,
		); err != nil {
			return backendPhysicalInsertShape{}, errors.Join(err, rows.Close())
		}
		shape.Diagnostics = append(shape.Diagnostics, fmt.Sprintf(
			"written=%d inserted=%d tables=%s event_time=%s query=%q",
			writtenRows,
			profileInsertedRows,
			tables,
			eventTime,
			query,
		))
		if inserted == 0 {
			shape.ZeroRowFinishes++
			continue
		}
		shape.Rows = append(shape.Rows, inserted)
	}
	return shape, errors.Join(rows.Err(), rows.Close())
}

type backendClickHousePressureSnapshot struct {
	ActiveParts                    uint64
	ActiveRows                     uint64
	DelayedInserts                 uint64
	DelayedInsertsSupported        bool
	RejectedInserts                uint64
	RejectedInsertsSupported       bool
	MergedRows                     uint64
	MergedRowsSupported            bool
	MergeTimeMilliseconds          uint64
	MergeTimeMillisecondsSupported bool
	ActiveMerges                   uint64
	ActiveMergeSourceParts         uint64
}

func readBackendClickHousePressure(
	ctx context.Context,
	connection clickhousedriver.Conn,
) (backendClickHousePressureSnapshot, error) {
	var snapshot backendClickHousePressureSnapshot
	if err := connection.QueryRow(
		ctx,
		`SELECT count(), coalesce(sum(rows), 0)
		 FROM system.parts
		 WHERE database = ? AND table = ? AND active = 1`,
		backendInsertEventsDatabase,
		backendInsertEventsTable,
	).Scan(&snapshot.ActiveParts, &snapshot.ActiveRows); err != nil {
		return backendClickHousePressureSnapshot{}, fmt.Errorf("read active event parts: %w", err)
	}
	var (
		delayedSupported   uint8
		rejectedSupported  uint8
		mergedSupported    uint8
		mergeTimeSupported uint8
	)
	if err := connection.QueryRow(
		ctx,
		`SELECT
			countIf(event = 'DelayedInserts') > 0,
			coalesce(sumIf(value, event = 'DelayedInserts'), 0),
			countIf(event = 'RejectedInserts') > 0,
			coalesce(sumIf(value, event = 'RejectedInserts'), 0),
			countIf(event = 'MergedRows') > 0,
			coalesce(sumIf(value, event = 'MergedRows'), 0),
			countIf(event = 'MergesTimeMilliseconds') > 0,
			coalesce(sumIf(value, event = 'MergesTimeMilliseconds'), 0)
		 FROM system.events
		 WHERE event IN (
			'DelayedInserts',
			'RejectedInserts',
			'MergedRows',
			'MergesTimeMilliseconds'
		 )`,
	).Scan(
		&delayedSupported,
		&snapshot.DelayedInserts,
		&rejectedSupported,
		&snapshot.RejectedInserts,
		&mergedSupported,
		&snapshot.MergedRows,
		&mergeTimeSupported,
		&snapshot.MergeTimeMilliseconds,
	); err != nil {
		return backendClickHousePressureSnapshot{}, fmt.Errorf("read ClickHouse insert and merge counters: %w", err)
	}
	snapshot.DelayedInsertsSupported = delayedSupported != 0
	snapshot.RejectedInsertsSupported = rejectedSupported != 0
	snapshot.MergedRowsSupported = mergedSupported != 0
	snapshot.MergeTimeMillisecondsSupported = mergeTimeSupported != 0
	if err := connection.QueryRow(
		ctx,
		`SELECT count(), coalesce(sum(num_parts), 0)
		 FROM system.merges
		 WHERE database = ? AND table = ?`,
		backendInsertEventsDatabase,
		backendInsertEventsTable,
	).Scan(&snapshot.ActiveMerges, &snapshot.ActiveMergeSourceParts); err != nil {
		return backendClickHousePressureSnapshot{}, fmt.Errorf("read active event merges: %w", err)
	}
	return snapshot, nil
}

func backendMonotonicCounterDelta(
	name string,
	before uint64,
	after uint64,
	supportedBefore bool,
	supportedAfter bool,
) (uint64, error) {
	if supportedBefore != supportedAfter {
		return 0, fmt.Errorf("ClickHouse %s counter support changed during measurement", name)
	}
	if !supportedBefore {
		return 0, nil
	}
	if after < before {
		return 0, fmt.Errorf("ClickHouse %s counter decreased from %d to %d", name, before, after)
	}
	return after - before, nil
}

func (after backendClickHousePressureSnapshot) validateWindow(
	before backendClickHousePressureSnapshot,
	shape backendPhysicalInsertShape,
	acceptedRows uint64,
) error {
	if acceptedRows == 0 {
		return errors.New("ClickHouse pressure window accepted row count must be positive")
	}
	expectedActiveRows := backendSaturatingAdd(before.ActiveRows, acceptedRows)
	if expectedActiveRows == ^uint64(0) && before.ActiveRows > ^uint64(0)-acceptedRows {
		return errors.New("active event-part row expectation overflows uint64")
	}
	if after.ActiveRows != expectedActiveRows {
		return fmt.Errorf(
			"active event-part rows grew from %d to %d, want exactly %d admitted rows",
			before.ActiveRows,
			after.ActiveRows,
			acceptedRows,
		)
	}
	maximumActiveParts := backendSaturatingAdd(
		backendSaturatingAdd(before.ActiveParts, shape.count()),
		backendInsertPartGrowthSlack,
	)
	if after.ActiveParts > maximumActiveParts {
		return fmt.Errorf(
			"active event parts grew from %d to %d for %d inserts, want at most %d",
			before.ActiveParts,
			after.ActiveParts,
			shape.count(),
			maximumActiveParts,
		)
	}
	delayed, err := backendMonotonicCounterDelta(
		"DelayedInserts",
		before.DelayedInserts,
		after.DelayedInserts,
		before.DelayedInsertsSupported,
		after.DelayedInsertsSupported,
	)
	if err != nil {
		return err
	}
	if delayed != 0 {
		return fmt.Errorf("ClickHouse delayed %d inserts during live coalescing window", delayed)
	}
	rejected, err := backendMonotonicCounterDelta(
		"RejectedInserts",
		before.RejectedInserts,
		after.RejectedInserts,
		before.RejectedInsertsSupported,
		after.RejectedInsertsSupported,
	)
	if err != nil {
		return err
	}
	if rejected != 0 {
		return fmt.Errorf("ClickHouse rejected %d inserts during live coalescing window", rejected)
	}
	mergedRows, err := backendMonotonicCounterDelta(
		"MergedRows",
		before.MergedRows,
		after.MergedRows,
		before.MergedRowsSupported,
		after.MergedRowsSupported,
	)
	if err != nil {
		return err
	}
	maximumMergedRows := backendSaturatingMultiply(
		expectedActiveRows,
		backendInsertMaximumMergePasses,
	)
	if mergedRows > maximumMergedRows {
		return fmt.Errorf(
			"ClickHouse merged %d rows during live coalescing window, want at most %d",
			mergedRows,
			maximumMergedRows,
		)
	}
	mergeTime, err := backendMonotonicCounterDelta(
		"MergesTimeMilliseconds",
		before.MergeTimeMilliseconds,
		after.MergeTimeMilliseconds,
		before.MergeTimeMillisecondsSupported,
		after.MergeTimeMillisecondsSupported,
	)
	if err != nil {
		return err
	}
	if mergeTime > uint64(backendInsertMaximumMergeTime/time.Millisecond) {
		return fmt.Errorf(
			"ClickHouse spent %dms merging during live coalescing window, want at most %s",
			mergeTime,
			backendInsertMaximumMergeTime,
		)
	}
	return nil
}

func backendSaturatingAdd(left uint64, right uint64) uint64 {
	if left > ^uint64(0)-right {
		return ^uint64(0)
	}
	return left + right
}

func backendSaturatingMultiply(left uint64, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}

type backendInsertBurstResult struct {
	Acknowledgments         []int64
	AcknowledgmentsByTarget [][]int64
	AcceptedRequests        uint64
	AcceptedEvents          uint64
}

type backendInsertBurstTarget struct {
	Credential string
	Channel    string
}

type backendInsertBurstResponse struct {
	TargetIndex int
	Response    backendHECLoadHTTPResponse
}

func validateBackendInsertCoalescingWindow(
	before *opensplunk.GetHECOperationalSnapshotResponse,
	after *opensplunk.GetHECOperationalSnapshotResponse,
	load backendInsertBurstResult,
	shape backendPhysicalInsertShape,
) error {
	if before == nil || after == nil || before.GetInsertCoalescing() == nil ||
		after.GetInsertCoalescing() == nil {
		return errors.New("administrator insert-coalescing telemetry is unavailable")
	}
	beforeMetrics := before.GetInsertCoalescing()
	afterMetrics := after.GetInsertCoalescing()
	wantDeltas := []struct {
		name   string
		before uint64
		after  uint64
		want   uint64
	}{
		{
			name:   "staged logical batches",
			before: beforeMetrics.GetStagedLogicalBatches(),
			after:  afterMetrics.GetStagedLogicalBatches(),
			want:   load.AcceptedRequests,
		},
		{
			name:   "staged logical rows",
			before: beforeMetrics.GetStagedLogicalRows(),
			after:  afterMetrics.GetStagedLogicalRows(),
			want:   load.AcceptedEvents,
		},
		{
			name:   "formed groups",
			before: beforeMetrics.GetFormedGroups(),
			after:  afterMetrics.GetFormedGroups(),
			want:   shape.count(),
		},
		{
			name:   "physical sends",
			before: beforeMetrics.GetPhysicalSends(),
			after:  afterMetrics.GetPhysicalSends(),
			want:   shape.count(),
		},
		{
			name:   "successful groups",
			before: beforeMetrics.GetSuccessfulGroups(),
			after:  afterMetrics.GetSuccessfulGroups(),
			want:   shape.count(),
		},
		{
			name:   "retries",
			before: beforeMetrics.GetRetries(),
			after:  afterMetrics.GetRetries(),
		},
		{
			name:   "ambiguities",
			before: beforeMetrics.GetAmbiguities(),
			after:  afterMetrics.GetAmbiguities(),
		},
	}
	for _, expectation := range wantDeltas {
		if expectation.after < expectation.before {
			return fmt.Errorf(
				"administrator %s counter decreased from %d to %d",
				expectation.name,
				expectation.before,
				expectation.after,
			)
		}
		if delta := expectation.after - expectation.before; delta != expectation.want {
			return fmt.Errorf(
				"administrator %s delta = %d, want %d",
				expectation.name,
				delta,
				expectation.want,
			)
		}
	}
	beforeRows := beforeMetrics.GetRowsPerPhysicalInsert()
	afterRows := afterMetrics.GetRowsPerPhysicalInsert()
	if beforeRows == nil || afterRows == nil {
		return errors.New("administrator rows-per-physical-insert histogram is unavailable")
	}
	for _, expectation := range []struct {
		name   string
		before uint64
		after  uint64
		want   uint64
	}{
		{
			name:   "rows-per-physical-insert count",
			before: beforeRows.GetCount(),
			after:  afterRows.GetCount(),
			want:   shape.count(),
		},
		{
			name:   "rows-per-physical-insert sum",
			before: beforeRows.GetSum(),
			after:  afterRows.GetSum(),
			want:   load.AcceptedEvents,
		},
	} {
		if expectation.after < expectation.before ||
			expectation.after-expectation.before != expectation.want {
			return fmt.Errorf(
				"administrator %s before/after = %d/%d, want delta %d",
				expectation.name,
				expectation.before,
				expectation.after,
				expectation.want,
			)
		}
	}
	queue := afterMetrics.GetQueue()
	if queue == nil || queue.GetPendingReservations() != 0 ||
		queue.GetUngroupedReservations() != 0 || queue.GetReadyGroups() != 0 ||
		queue.GetAmbiguousGroups() != 0 || queue.GetLeasedGroups() != 0 ||
		queue.GetPendingOutboxBytes() != 0 || queue.GetPendingMetadataBytes() != 0 {
		return fmt.Errorf("administrator insert-coalescing queue did not drain: %+v", queue)
	}
	return nil
}

// runBackendInsertBurst submits already-bounded HEC batches concurrently. The
// outage phase uses it to prove durable staging; the live phase uses a larger
// continuously eligible workload to measure ordinary physical insert shape.
func runBackendInsertBurst(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	requestCount int,
	concurrencyLimit int,
	targets []backendInsertBurstTarget,
	endpointPath string,
	contentType string,
	body []byte,
) (backendInsertBurstResult, error) {
	if requestCount <= 0 || concurrencyLimit <= 0 || concurrencyLimit > requestCount ||
		len(targets) == 0 || endpointPath == "" || contentType == "" || len(body) == 0 {
		return backendInsertBurstResult{}, errors.New("coalescing burst configuration is invalid")
	}
	for _, target := range targets {
		if target.Credential == "" || target.Channel == "" {
			return backendInsertBurstResult{}, errors.New("coalescing burst target is invalid")
		}
	}
	start := make(chan struct{})
	responses := make(chan backendInsertBurstResponse, requestCount)
	errorsByRequest := make(chan error, requestCount)
	concurrency := make(chan struct{}, concurrencyLimit)
	var workers sync.WaitGroup
	for ordinal := range requestCount {
		targetIndex := ordinal % len(targets)
		target := targets[targetIndex]
		workers.Go(func() {
			<-start
			select {
			case concurrency <- struct{}{}:
			case <-ctx.Done():
				errorsByRequest <- ctx.Err()
				return
			}
			response, err := backendInsertBurstPost(
				ctx,
				client,
				baseURL,
				target.Credential,
				target.Channel,
				endpointPath,
				contentType,
				body,
			)
			<-concurrency
			if err != nil {
				errorsByRequest <- err
				return
			}
			responses <- backendInsertBurstResponse{TargetIndex: targetIndex, Response: response}
		})
	}
	close(start)
	workers.Wait()
	close(responses)
	close(errorsByRequest)
	var resultErrors []error
	for err := range errorsByRequest {
		resultErrors = append(resultErrors, err)
	}
	result := backendInsertBurstResult{
		Acknowledgments:         make([]int64, 0, requestCount),
		AcknowledgmentsByTarget: make([][]int64, len(targets)),
	}
	seenAcknowledgments := make(map[int64]struct{}, requestCount)
	for observed := range responses {
		response := observed.Response
		if response.status != http.StatusOK || response.code != 0 || response.acknowledgment <= 0 {
			resultErrors = append(resultErrors, fmt.Errorf(
				"coalescing burst response status/code/ack = %d/%d/%d",
				response.status,
				response.code,
				response.acknowledgment,
			))
			continue
		}
		if _, duplicate := seenAcknowledgments[response.acknowledgment]; duplicate {
			resultErrors = append(resultErrors, fmt.Errorf(
				"coalescing burst repeated acknowledgment %d",
				response.acknowledgment,
			))
			continue
		}
		seenAcknowledgments[response.acknowledgment] = struct{}{}
		result.Acknowledgments = append(result.Acknowledgments, response.acknowledgment)
		result.AcknowledgmentsByTarget[observed.TargetIndex] = append(
			result.AcknowledgmentsByTarget[observed.TargetIndex],
			response.acknowledgment,
		)
		result.AcceptedRequests++
		result.AcceptedEvents += backendHECLoadFullEvents
	}
	slices.Sort(result.Acknowledgments)
	for index := range result.AcknowledgmentsByTarget {
		slices.Sort(result.AcknowledgmentsByTarget[index])
	}
	expectedEvents := uint64(requestCount * backendHECLoadFullEvents)
	if result.AcceptedRequests != uint64(requestCount) ||
		result.AcceptedEvents != expectedEvents ||
		len(result.Acknowledgments) != requestCount {
		resultErrors = append(resultErrors, fmt.Errorf(
			"coalescing burst accepted requests/events/acks = %d/%d/%d, want %d/%d/%d",
			result.AcceptedRequests,
			result.AcceptedEvents,
			len(result.Acknowledgments),
			requestCount,
			expectedEvents,
			requestCount,
		))
	}
	return result, errors.Join(resultErrors...)
}

func backendInsertBurstPost(
	ctx context.Context,
	client *http.Client,
	baseURL, credential string,
	channel string,
	endpointPath string,
	contentType string,
	body []byte,
) (backendHECLoadHTTPResponse, error) {
	for attempt := range backendInsertBurstRetryLimit {
		response, err := backendHECLoadPost(
			ctx,
			client,
			baseURL+endpointPath,
			credential,
			channel,
			contentType,
			body,
		)
		if err != nil {
			return backendHECLoadHTTPResponse{}, err
		}
		if !backendInsertBurstRetryable(response) {
			return response, nil
		}
		if attempt+1 == backendInsertBurstRetryLimit {
			return response, nil
		}
		timer := time.NewTimer(backendInsertBurstRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return backendHECLoadHTTPResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
	return backendHECLoadHTTPResponse{}, errors.New("coalescing burst retry loop exhausted")
}

func backendInsertSteadyStateBody() []byte {
	const rawEvent = "open-splunk-live-insert-coalescing"
	body := make([]byte, 0, (len(rawEvent)+1)*backendHECLoadFullEvents)
	for ordinal := range backendHECLoadFullEvents {
		if ordinal != 0 {
			body = append(body, '\n')
		}
		body = append(body, rawEvent...)
	}
	return body
}

func backendInsertBurstRetryable(response backendHECLoadHTTPResponse) bool {
	return (response.status == http.StatusServiceUnavailable ||
		response.status == http.StatusTooManyRequests) && response.code == 9
}

func TestBackendPhysicalInsertShapeValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		shape       backendPhysicalInsertShape
		minimumRows uint64
		minimumBig  uint64
		wantError   string
	}{
		{
			name:        "coalesced",
			shape:       backendPhysicalInsertShape{Rows: []uint64{10_000, 12_500, 750}},
			minimumRows: 20_000,
			minimumBig:  2,
		},
		{
			name:        "sparse linger is not enough for target proof",
			shape:       backendPhysicalInsertShape{Rows: []uint64{1, 250, 999}},
			minimumRows: 1_000,
			minimumBig:  1,
			wantError:   "at or above 10000 rows",
		},
		{
			name:        "hard row maximum",
			shape:       backendPhysicalInsertShape{Rows: []uint64{50_001}},
			minimumRows: 1,
			minimumBig:  1,
			wantError:   "above hard maximum 50000",
		},
		{
			name:        "zero row query log record",
			shape:       backendPhysicalInsertShape{Rows: []uint64{0, 10_000}},
			minimumRows: 1,
			minimumBig:  1,
			wantError:   "reports zero rows",
		},
		{
			name:        "missing query log records",
			shape:       backendPhysicalInsertShape{},
			minimumRows: 1,
			minimumBig:  1,
			wantError:   "contains no positive-row physical event inserts",
		},
		{
			name:        "missing expectation",
			shape:       backendPhysicalInsertShape{Rows: []uint64{10_000}},
			minimumRows: 0,
			minimumBig:  1,
			wantError:   "expectation must be positive",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.shape.validate(testCase.minimumRows, testCase.minimumBig)
			if testCase.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("physical insert validation error = %v, want %q", err, testCase.wantError)
			}
		})
	}
}

func TestBackendPhysicalInsertSteadyStateValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rows        []uint64
		logical     uint64
		logicalRows uint64
		wantError   string
	}{
		{
			name:        "published envelope",
			rows:        []uint64{5_000, 10_000, 10_000, 12_000, 15_000, 20_000, 25_000, 30_000, 40_000, 50_000},
			logical:     100,
			logicalRows: 217_000,
		},
		{
			name:        "median regression",
			rows:        []uint64{5_000, 5_000, 9_999, 10_000, 10_000},
			logical:     50,
			logicalRows: 39_999,
			wantError:   "lower median physical insert",
		},
		{
			name:        "lower tail regression",
			rows:        []uint64{4_998, 4_999, 10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 10_000},
			logical:     100,
			logicalRows: 89_997,
			wantError:   "want at least 90%",
		},
		{
			name:        "insert reduction regression",
			rows:        []uint64{10_000, 10_000},
			logical:     19,
			logicalRows: 20_000,
			wantError:   "want at most 1/10",
		},
		{
			name:        "exact row accounting regression",
			rows:        []uint64{10_000, 10_000},
			logical:     20,
			logicalRows: 20_001,
			wantError:   "want at least 20001",
		},
		{
			name:        "unexpected extra physical rows",
			rows:        []uint64{10_000, 10_000},
			logical:     20,
			logicalRows: 19_999,
			wantError:   "want exactly 19999",
		},
		{
			name:        "missing accepted batches",
			rows:        []uint64{10_000},
			logical:     0,
			logicalRows: 10_000,
			wantError:   "must be positive",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := (backendPhysicalInsertShape{Rows: testCase.rows}).validateSteadyState(
				testCase.logical,
				testCase.logicalRows,
			)
			if testCase.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("steady-state shape validation error = %v, want %q", err, testCase.wantError)
			}
		})
	}
}

func TestBackendPhysicalInsertShapeQueryTargetsEvents(t *testing.T) {
	t.Parallel()
	query := backendPhysicalInsertShapeQuery()
	for _, required := range []string{
		"FROM system.query_log",
		"type = 'QueryFinish'",
		"query_kind = 'Insert'",
		"has(databases, ?)",
		"has(tables, ?)",
		"event_time_microseconds >= ?",
		"ProfileEvents['InsertedRows']",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("physical insert query %q does not contain %q", query, required)
		}
	}
	if strings.Contains(query, "query LIKE") || backendInsertEventsQualifiedTable != "open_splunk.events" {
		t.Fatalf("physical insert query is not structurally scoped to open_splunk.events: %q", query)
	}
}

func TestBackendClickHousePressureValidation(t *testing.T) {
	t.Parallel()
	base := backendClickHousePressureSnapshot{
		ActiveParts:                    4,
		ActiveRows:                     100_000,
		DelayedInserts:                 2,
		DelayedInsertsSupported:        true,
		RejectedInserts:                3,
		RejectedInsertsSupported:       true,
		MergedRows:                     10_000,
		MergedRowsSupported:            true,
		MergeTimeMilliseconds:          500,
		MergeTimeMillisecondsSupported: true,
		ActiveMerges:                   1,
		ActiveMergeSourceParts:         2,
	}
	healthy := base
	healthy.ActiveParts = 7
	healthy.ActiveRows += 40_000
	healthy.MergedRows += 100_000
	healthy.MergeTimeMilliseconds += 2_000
	shape := backendPhysicalInsertShape{Rows: []uint64{10_000, 10_000, 10_000, 10_000}}
	if err := healthy.validateWindow(base, shape, 40_000); err != nil {
		t.Fatalf("healthy pressure window: %v", err)
	}
	for _, testCase := range []struct {
		name      string
		mutate    func(*backendClickHousePressureSnapshot)
		wantError string
	}{
		{
			name: "active row mismatch",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.ActiveRows--
			},
			wantError: "active event-part rows grew",
		},
		{
			name: "active part growth",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.ActiveParts = base.ActiveParts + shape.count() + backendInsertPartGrowthSlack + 1
			},
			wantError: "active event parts grew",
		},
		{
			name: "delayed insert",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.DelayedInserts++
			},
			wantError: "delayed 1 inserts",
		},
		{
			name: "rejected insert",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.RejectedInserts++
			},
			wantError: "rejected 1 inserts",
		},
		{
			name: "counter regression",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.MergedRows = base.MergedRows - 1
			},
			wantError: "counter decreased",
		},
		{
			name: "merge row pressure",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.MergedRows = base.MergedRows +
					(base.ActiveRows+40_000)*backendInsertMaximumMergePasses + 1
			},
			wantError: "rows during live coalescing window",
		},
		{
			name: "merge time pressure",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.MergeTimeMilliseconds = base.MergeTimeMilliseconds +
					uint64(backendInsertMaximumMergeTime/time.Millisecond) + 1
			},
			wantError: "spent 60001ms merging",
		},
		{
			name: "support changes",
			mutate: func(snapshot *backendClickHousePressureSnapshot) {
				snapshot.DelayedInsertsSupported = false
			},
			wantError: "counter support changed",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			after := healthy
			testCase.mutate(&after)
			err := after.validateWindow(base, shape, 40_000)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("pressure validation error = %v, want %q", err, testCase.wantError)
			}
		})
	}
}

func TestBackendInsertBurstRetriesOnlyTransientBusy(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		response backendHECLoadHTTPResponse
		want     bool
	}{
		{name: "service unavailable busy", response: backendHECLoadHTTPResponse{status: 503, code: 9}, want: true},
		{name: "too many requests busy", response: backendHECLoadHTTPResponse{status: 429, code: 9}, want: true},
		{name: "durable capacity", response: backendHECLoadHTTPResponse{status: 429, code: 26}},
		{name: "success", response: backendHECLoadHTTPResponse{status: 200, code: 0}},
		{name: "other service failure", response: backendHECLoadHTTPResponse{status: 503, code: 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := backendInsertBurstRetryable(testCase.response); got != testCase.want {
				t.Fatalf("retryable = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestBackendInsertSteadyStateBodyPinsOneThousandRawEvents(t *testing.T) {
	t.Parallel()
	body := backendInsertSteadyStateBody()
	if len(body) == 0 || strings.Count(string(body), "\n") != backendHECLoadFullEvents-1 ||
		strings.ContainsAny(string(body), "{}") {
		t.Fatalf("steady-state raw body shape is invalid: bytes=%d", len(body))
	}
}
