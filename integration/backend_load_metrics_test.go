//go:build !windows

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
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
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			done, err := test.state.classify(test.expected)
			if (err != nil) != test.wantErr || done != test.done {
				t.Fatalf("classify(%+v, %d) = %t, %v", test.state, test.expected, done, err)
			}
		})
	}
}
