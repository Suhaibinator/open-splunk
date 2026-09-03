//go:build !windows

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

type backendLoadStorageState struct {
	Rows             uint64
	DistinctEventIDs uint64
}

func (state backendLoadStorageState) classify(expected uint64) (bool, error) {
	if state.DistinctEventIDs > state.Rows {
		return false, fmt.Errorf(
			"backend load storage has %d distinct event IDs in %d rows",
			state.DistinctEventIDs,
			state.Rows,
		)
	}
	if state.Rows > expected || state.DistinctEventIDs > expected || state.Rows > state.DistinctEventIDs {
		return false, fmt.Errorf(
			"backend load storage rows/distinct = %d/%d, want no duplicates and at most %d",
			state.Rows,
			state.DistinctEventIDs,
			expected,
		)
	}
	return state.Rows == expected && state.DistinctEventIDs == expected, nil
}

func readBackendLoadStorageState(
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID, indexName string,
) (backendLoadStorageState, error) {
	var state backendLoadStorageState
	err := connection.QueryRow(
		ctx,
		`SELECT count(), uniqExact(event_id)
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		tenantID,
		indexName,
	).Scan(&state.Rows, &state.DistinctEventIDs)
	return state, err
}

func readBackendLoadRowCount(
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID, indexName string,
) (uint64, error) {
	var rows uint64
	err := connection.QueryRow(
		ctx,
		`SELECT count()
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		tenantID,
		indexName,
	).Scan(&rows)
	return rows, err
}

func waitForBackendLoadStorage(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	process *managedProcess,
	tenantID, indexName string,
	expected uint64,
	timeout time.Duration,
	secrets ...string,
) {
	t.Helper()
	waitContext, waitCancel := context.WithTimeout(ctx, timeout)
	defer waitCancel()
	expiresAt, _ := waitContext.Deadline()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var (
		lastRows  uint64
		lastExact backendLoadStorageState
		exactRead bool
		lastErr   error
	)
	for {
		queryTimeout := min(5*time.Second, time.Until(expiresAt))
		queryContext, queryCancel := context.WithTimeout(waitContext, queryTimeout)
		exactRead = false
		lastRows, lastErr = readBackendLoadRowCount(
			queryContext,
			connection,
			tenantID,
			indexName,
		)
		if lastErr == nil {
			if lastRows > expected {
				queryCancel()
				t.Fatalf("backend load storage rows = %d, want at most %d", lastRows, expected)
			}
			if lastRows == expected {
				lastExact, lastErr = readBackendLoadStorageState(
					queryContext,
					connection,
					tenantID,
					indexName,
				)
				exactRead = lastErr == nil
				if lastErr == nil {
					done, classifyErr := lastExact.classify(expected)
					if classifyErr != nil {
						queryCancel()
						t.Fatal(classifyErr)
					}
					if done {
						queryCancel()
						return
					}
				}
			}
		}
		queryCancel()
		if process.Exited() {
			t.Fatalf(
				"collector exited before backend load storage reached %d: %v rows=%d exact_state=%+v exact_read=%t query_error=%v\nlogs:\n%s",
				expected,
				process.Err(),
				lastRows,
				lastExact,
				exactRead,
				lastErr,
				redactForFailure(process.Logs(), secrets...),
			)
		}
		select {
		case <-waitContext.Done():
			t.Fatalf(
				"wait for backend load storage after %s: %v rows=%d exact_state=%+v exact_read=%t query_error=%v\nlogs:\n%s",
				timeout,
				waitContext.Err(),
				lastRows,
				lastExact,
				exactRead,
				lastErr,
				redactForFailure(process.Logs(), secrets...),
			)
		case <-ticker.C:
		}
	}
}

type backendLoadStorageMetrics struct {
	Rows              uint64
	DistinctEventIDs  uint64
	EmptyEventIDs     uint64
	TypedUserRows     uint64
	DistinctUserIDs   uint64
	RawBytes          uint64
	GlobalRows        uint64
	ActiveParts       uint64
	PartRows          uint64
	CompressedBytes   uint64
	UncompressedBytes uint64
	BytesOnDisk       uint64
}

type backendLoadPhysicalInsertShape struct {
	PhysicalInserts         uint64
	WrittenRows             uint64
	WrittenBytes            uint64
	RowsAtLeastFiveThousand uint64
	MinimumRows             uint64
	MedianRows              uint64
	MaximumRows             uint64
	FailedInserts           uint64
	NewParts                uint64
	NewPartRows             uint64
	MergeParts              uint64
	MergedRows              uint64
	DelayedInserts          uint64
	RejectedInserts         uint64
}

type backendLoadStorageActivityMarker struct {
	StartedAt   time.Time
	EndedAt     time.Time
	Events      map[string]uint64
	EndedEvents map[string]uint64
}

var backendLoadStorageEventNames = []string{
	"DelayedInserts",
	"RejectedInserts",
}

func readBackendLoadStorageActivityMarker(
	ctx context.Context,
	connection clickhousedriver.Conn,
) (backendLoadStorageActivityMarker, error) {
	var startedAt time.Time
	if err := connection.QueryRow(ctx, `SELECT now64(6)`).Scan(&startedAt); err != nil {
		return backendLoadStorageActivityMarker{}, fmt.Errorf("read ClickHouse activity clock: %w", err)
	}
	events, err := readBackendLoadStorageEvents(ctx, connection)
	if err != nil {
		return backendLoadStorageActivityMarker{}, err
	}
	return backendLoadStorageActivityMarker{
		StartedAt: startedAt.UTC(),
		Events:    events,
	}, nil
}

func readBackendLoadPhysicalInsertShape(
	ctx context.Context,
	connection clickhousedriver.Conn,
	marker backendLoadStorageActivityMarker,
) (backendLoadPhysicalInsertShape, error) {
	if marker.StartedAt.IsZero() {
		return backendLoadPhysicalInsertShape{}, errors.New("ClickHouse activity marker time is required")
	}
	queryEnd := ""
	queryArguments := []any{marker.StartedAt}
	partArguments := []any{marker.StartedAt}
	if !marker.EndedAt.IsZero() {
		if !marker.EndedAt.After(marker.StartedAt) || marker.EndedEvents == nil {
			return backendLoadPhysicalInsertShape{}, errors.New("ClickHouse activity marker end must follow its start with event counters")
		}
		queryEnd = " AND query_start_time_microseconds < ?"
		queryArguments = append(queryArguments, marker.EndedAt)
		partArguments = append(partArguments, marker.EndedAt)
	}
	var shape backendLoadPhysicalInsertShape
	if err := connection.QueryRow(
		ctx,
		`SELECT
			countIf(type = 'QueryFinish' AND written_rows > 0),
			coalesce(sumIf(written_rows, type = 'QueryFinish'), 0),
			coalesce(sumIf(written_bytes, type = 'QueryFinish'), 0),
			countIf(type = 'QueryFinish' AND written_rows >= 5000),
			minIf(written_rows, type = 'QueryFinish' AND written_rows > 0),
			quantileExactIf(0.5)(written_rows, type = 'QueryFinish' AND written_rows > 0),
			maxIf(written_rows, type = 'QueryFinish'),
			countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing'))
		 FROM system.query_log
		 WHERE query_start_time_microseconds >= ?
			AND query_kind = 'Insert'
			AND has(tables, 'open_splunk.events')`+queryEnd,
		queryArguments...,
	).Scan(
		&shape.PhysicalInserts,
		&shape.WrittenRows,
		&shape.WrittenBytes,
		&shape.RowsAtLeastFiveThousand,
		&shape.MinimumRows,
		&shape.MedianRows,
		&shape.MaximumRows,
		&shape.FailedInserts,
	); err != nil {
		return backendLoadPhysicalInsertShape{}, fmt.Errorf("read backend load insert query shape: %w", err)
	}
	if err := connection.QueryRow(
		ctx,
		`SELECT
			countIf(event_type = 'NewPart'),
			coalesce(sumIf(rows, event_type = 'NewPart'), 0),
			countIf(event_type = 'MergeParts'),
			coalesce(sumIf(rows, event_type = 'MergeParts'), 0)
		 FROM system.part_log
		 WHERE event_time_microseconds >= ?
			AND database = 'open_splunk'
			AND table = 'events'`+strings.Replace(queryEnd, "query_start_time_microseconds", "event_time_microseconds", 1),
		partArguments...,
	).Scan(
		&shape.NewParts,
		&shape.NewPartRows,
		&shape.MergeParts,
		&shape.MergedRows,
	); err != nil {
		return backendLoadPhysicalInsertShape{}, fmt.Errorf("read backend load part activity: %w", err)
	}
	events := marker.EndedEvents
	if events == nil {
		var err error
		events, err = readBackendLoadStorageEvents(ctx, connection)
		if err != nil {
			return backendLoadPhysicalInsertShape{}, err
		}
	}
	shape.DelayedInserts = monotonicCounterDelta(marker.Events["DelayedInserts"], events["DelayedInserts"])
	shape.RejectedInserts = monotonicCounterDelta(marker.Events["RejectedInserts"], events["RejectedInserts"])
	return shape, nil
}

func readBackendLoadStorageEvents(
	ctx context.Context,
	connection clickhousedriver.Conn,
) (map[string]uint64, error) {
	rows, err := connection.Query(
		ctx,
		`SELECT event, value
		 FROM system.events
		 WHERE event IN ('DelayedInserts', 'RejectedInserts')`,
	)
	if err != nil {
		return nil, fmt.Errorf("read backend load ClickHouse events: %w", err)
	}
	defer rows.Close()
	values := make(map[string]uint64, len(backendLoadStorageEventNames))
	for _, name := range backendLoadStorageEventNames {
		values[name] = 0
	}
	for rows.Next() {
		var name string
		var value uint64
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("scan backend load ClickHouse event: %w", err)
		}
		if _, known := values[name]; known {
			values[name] = value
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate backend load ClickHouse events: %w", err)
	}
	return values, nil
}

func monotonicCounterDelta(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func waitForBackendLoadPhysicalInsertShape(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	marker backendLoadStorageActivityMarker,
	expectedRows uint64,
) backendLoadPhysicalInsertShape {
	t.Helper()
	waitContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last backendLoadPhysicalInsertShape
	var lastErr error
	for {
		queryContext, queryCancel := context.WithTimeout(waitContext, 5*time.Second)
		last, lastErr = readBackendLoadPhysicalInsertShape(queryContext, connection, marker)
		queryCancel()
		if lastErr == nil && last.WrittenRows >= expectedRows && last.NewParts > 0 {
			return last
		}
		select {
		case <-waitContext.Done():
			t.Fatalf(
				"wait for backend load physical insert telemetry: %v shape=%+v error=%v",
				waitContext.Err(),
				last,
				lastErr,
			)
		case <-ticker.C:
		}
	}
}

func validateBackendLoadPhysicalInsertShape(
	shape backendLoadPhysicalInsertShape,
	logicalBatches uint64,
	qualifiedSteadyState bool,
) error {
	if shape.PhysicalInserts == 0 || shape.WrittenRows == 0 || shape.MinimumRows == 0 {
		return errors.New("physical insert telemetry contains no successful nonempty insert")
	}
	if shape.MaximumRows > 50_000 {
		return fmt.Errorf("physical insert maximum rows %d exceeds hard limit 50000", shape.MaximumRows)
	}
	if !qualifiedSteadyState {
		return nil
	}
	if shape.MedianRows < 10_000 {
		return fmt.Errorf("physical insert median rows %d is below 10000", shape.MedianRows)
	}
	minimumLargeInserts := shape.PhysicalInserts/10*9 + (shape.PhysicalInserts%10*9+9)/10
	if shape.RowsAtLeastFiveThousand < minimumLargeInserts {
		return fmt.Errorf(
			"physical inserts with at least 5000 rows = %d, want at least %d of %d",
			shape.RowsAtLeastFiveThousand,
			minimumLargeInserts,
			shape.PhysicalInserts,
		)
	}
	if logicalBatches == 0 || shape.PhysicalInserts > logicalBatches/10 {
		return fmt.Errorf(
			"physical inserts %d exceed one tenth of %d logical batches",
			shape.PhysicalInserts,
			logicalBatches,
		)
	}
	return nil
}

func TestValidateQualifiedInsertShape(t *testing.T) {
	t.Parallel()
	sparse := backendLoadPhysicalInsertShape{
		PhysicalInserts: 2,
		WrittenRows:     200,
		MinimumRows:     100,
		MedianRows:      100,
		MaximumRows:     100,
	}
	if err := validateBackendLoadPhysicalInsertShape(sparse, 2, false); err != nil {
		t.Fatalf("sparse insert shape: %v", err)
	}
	qualified := backendLoadPhysicalInsertShape{
		PhysicalInserts:         10,
		WrittenRows:             100_000,
		RowsAtLeastFiveThousand: 9,
		MinimumRows:             1_000,
		MedianRows:              10_000,
		MaximumRows:             50_000,
	}
	if err := validateBackendLoadPhysicalInsertShape(qualified, 100, true); err != nil {
		t.Fatalf("qualified insert shape: %v", err)
	}
	for name, shape := range map[string]backendLoadPhysicalInsertShape{
		"empty": {},
		"hard maximum": {
			PhysicalInserts: 1,
			WrittenRows:     50_001,
			MinimumRows:     50_001,
			MedianRows:      50_001,
			MaximumRows:     50_001,
		},
		"median": {
			PhysicalInserts:         10,
			WrittenRows:             99_990,
			RowsAtLeastFiveThousand: 9,
			MinimumRows:             4_999,
			MedianRows:              9_999,
			MaximumRows:             50_000,
		},
		"large insert ratio": {
			PhysicalInserts:         10,
			WrittenRows:             100_000,
			RowsAtLeastFiveThousand: 8,
			MinimumRows:             1,
			MedianRows:              10_000,
			MaximumRows:             50_000,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateBackendLoadPhysicalInsertShape(shape, 100, true); err == nil {
				t.Fatal("invalid physical insert shape unexpectedly validated")
			}
		})
	}
	tooMany := qualified
	tooMany.PhysicalInserts = 11
	tooMany.RowsAtLeastFiveThousand = 11
	if err := validateBackendLoadPhysicalInsertShape(tooMany, 100, true); err == nil {
		t.Fatal("physical/logical insert ratio above one tenth unexpectedly validated")
	}
}

func TestQueryPhysicalInsertShapeAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker integration was requested but the CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouseWithServicePrincipals(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close ClickHouse fixture: %v", closeErr)
		}
	})
	migrator, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: "default",
			Username: container.MigrationUsername,
			Password: container.MigrationPassword,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.ApplyClickHouseMigrations(ctx, migrator, migrations.ClickHouse()); err != nil {
		_ = migrator.Close()
		t.Fatal(err)
	}
	if err := migrator.Close(); err != nil {
		t.Fatalf("close ClickHouse migration connection: %v", err)
	}
	// Load evidence needs test-only system telemetry that is deliberately
	// excluded from the server's least-privilege runtime principal.
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	marker, err := readBackendLoadStorageActivityMarker(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var shape backendLoadPhysicalInsertShape
	for {
		shape, err = readBackendLoadPhysicalInsertShape(ctx, connection, marker)
		if err == nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("wait for ClickHouse system telemetry tables: %v", err)
		case <-ticker.C:
		}
	}
	if shape.PhysicalInserts != 0 || shape.WrittenRows != 0 || shape.NewParts != 0 {
		t.Fatalf("empty post-marker ClickHouse activity = %+v", shape)
	}
}

func readBackendLoadStorageMetrics(
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID, indexName string,
) (backendLoadStorageMetrics, error) {
	var metrics backendLoadStorageMetrics
	if err := connection.QueryRow(
		ctx,
		`SELECT
			count(),
			uniqExact(event_id),
			countIf(event_id = ''),
			countIf(has(field_names, 'user_id') AND dynamicType(fields.user_id) = 'String'),
			uniqExactIf(
				dynamicElement(fields.user_id, 'String'),
				has(field_names, 'user_id') AND dynamicType(fields.user_id) = 'String'
			),
			coalesce(sum(toUInt64(length(raw))), 0)
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		tenantID,
		indexName,
	).Scan(
		&metrics.Rows,
		&metrics.DistinctEventIDs,
		&metrics.EmptyEventIDs,
		&metrics.TypedUserRows,
		&metrics.DistinctUserIDs,
		&metrics.RawBytes,
	); err != nil {
		return backendLoadStorageMetrics{}, fmt.Errorf("read backend load event metrics: %w", err)
	}
	if err := connection.QueryRow(
		ctx,
		`SELECT count() FROM open_splunk.events`,
	).Scan(&metrics.GlobalRows); err != nil {
		return backendLoadStorageMetrics{}, fmt.Errorf("read backend load global event count: %w", err)
	}
	if err := connection.QueryRow(
		ctx,
		`SELECT
			count(),
			coalesce(sum(rows), 0),
			coalesce(sum(data_compressed_bytes), 0),
			coalesce(sum(data_uncompressed_bytes), 0),
			coalesce(sum(bytes_on_disk), 0)
		 FROM system.parts
		 WHERE database = 'open_splunk' AND table = 'events' AND active = 1`,
	).Scan(
		&metrics.ActiveParts,
		&metrics.PartRows,
		&metrics.CompressedBytes,
		&metrics.UncompressedBytes,
		&metrics.BytesOnDisk,
	); err != nil {
		return backendLoadStorageMetrics{}, fmt.Errorf("read backend load part metrics: %w", err)
	}
	return metrics, nil
}

func assertBackendLoadStorageMetrics(
	t *testing.T,
	plan backendLoadPlan,
	source backendLoadSourceCorpus,
	metrics backendLoadStorageMetrics,
) {
	t.Helper()
	if metrics.Rows != plan.eventCount() ||
		metrics.DistinctEventIDs != plan.eventCount() ||
		metrics.EmptyEventIDs != 0 ||
		metrics.TypedUserRows != plan.eventCount() ||
		metrics.DistinctUserIDs != uint64(len(source.UserIDs)) ||
		metrics.RawBytes != source.RawBytes ||
		metrics.GlobalRows != plan.eventCount() {
		t.Fatalf(
			"backend load event metrics = %+v, want rows/event IDs/typed users/global rows=%d empty event IDs=0 distinct users=%d raw bytes=%d",
			metrics,
			plan.eventCount(),
			len(source.UserIDs),
			source.RawBytes,
		)
	}
	if metrics.ActiveParts == 0 ||
		metrics.PartRows != plan.eventCount() ||
		metrics.CompressedBytes == 0 ||
		metrics.UncompressedBytes == 0 ||
		metrics.BytesOnDisk == 0 {
		t.Fatalf(
			"backend load part metrics = %+v, want nonempty parts covering exactly %d rows",
			metrics,
			plan.eventCount(),
		)
	}
}

func verifyBackendLoadRawRows(
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID, indexName string,
	expected *backendLoadRawMultiset,
) error {
	remaining := expected.Clone()
	rows, err := connection.Query(
		ctx,
		`SELECT
			raw,
			dynamicType(fields.request_id),
			dynamicElement(fields.request_id, 'String'),
			dynamicType(fields.user_id),
			dynamicElement(fields.user_id, 'String'),
			dynamicType(fields.ordinal),
			dynamicElement(fields.ordinal, 'Int64'),
			event_time
		 FROM open_splunk.events
		 WHERE tenant_id = ? AND index_name = ?`,
		tenantID,
		indexName,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			raw                    string
			requestType, requestID string
			userType, userID       string
			ordinalType            string
			ordinal                int64
			eventTime              time.Time
		)
		if err := rows.Scan(
			&raw,
			&requestType,
			&requestID,
			&userType,
			&userID,
			&ordinalType,
			&ordinal,
			&eventTime,
		); err != nil {
			return errors.Join(err, rows.Close())
		}
		if err := remaining.Consume(raw); err != nil {
			return errors.Join(err, rows.Close())
		}
		var decoded struct {
			Timestamp string  `json:"timestamp"`
			RequestID string  `json:"request_id"`
			UserID    string  `json:"user_id"`
			Ordinal   *uint64 `json:"ordinal"`
		}
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			return errors.Join(fmt.Errorf("decode stored backend load raw record: %w", err), rows.Close())
		}
		timestamp, err := time.Parse(time.RFC3339Nano, decoded.Timestamp)
		if err != nil {
			return errors.Join(fmt.Errorf("decode stored backend load timestamp: %w", err), rows.Close())
		}
		if requestType != "String" ||
			requestID != decoded.RequestID ||
			userType != "String" ||
			userID != decoded.UserID ||
			ordinalType != "Int64" ||
			decoded.Ordinal == nil ||
			ordinal < 0 ||
			uint64(ordinal) != *decoded.Ordinal ||
			!eventTime.Equal(timestamp) {
			return errors.Join(
				fmt.Errorf(
					"stored backend load extraction request=%s/%q user=%s/%q ordinal=%s/%d event_time=%s does not match raw request=%q user=%q ordinal=%v timestamp=%q",
					requestType,
					requestID,
					userType,
					userID,
					ordinalType,
					ordinal,
					eventTime.Format(time.RFC3339Nano),
					decoded.RequestID,
					decoded.UserID,
					decoded.Ordinal,
					decoded.Timestamp,
				),
				rows.Close(),
			)
		}
	}
	return errors.Join(rows.Err(), rows.Close(), remaining.Finish())
}

func TestBackendLoadStorageStateClassifier(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		state    backendLoadStorageState
		expected uint64
		done     bool
		wantErr  bool
	}{
		{name: "incomplete", state: backendLoadStorageState{Rows: 7, DistinctEventIDs: 7}, expected: 10},
		{name: "exact", state: backendLoadStorageState{Rows: 10, DistinctEventIDs: 10}, expected: 10, done: true},
		{name: "duplicate", state: backendLoadStorageState{Rows: 8, DistinctEventIDs: 7}, expected: 10, wantErr: true},
		{name: "row overshoot", state: backendLoadStorageState{Rows: 11, DistinctEventIDs: 11}, expected: 10, wantErr: true},
		{name: "distinct overshoot", state: backendLoadStorageState{Rows: 10, DistinctEventIDs: 11}, expected: 10, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			done, err := test.state.classify(test.expected)
			if (err != nil) != test.wantErr || done != test.done {
				t.Fatalf("classify(%+v, %d) = %t, %v", test.state, test.expected, done, err)
			}
		})
	}
}
