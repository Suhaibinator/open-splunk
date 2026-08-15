package queryexec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// recordingConnection is the concurrency-safe counterpart of
// fakeQueryConnection: issueRead is reached from several buffered readers, so
// the shared tail must be exercised from more than one goroutine at a time.
type recordingConnection struct {
	mutex   sync.Mutex
	calls   int
	rows    driver.Rows
	err     error
	queries []string
}

func (connection *recordingConnection) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.calls++
	connection.queries = append(connection.queries, query)
	return connection.rows, connection.err
}

// TestIssueReadRejectsEveryDegenerateStreamHandoff walks the failure ladder of
// the tail every buffered reader shares. A typed nil driver.Rows is the
// dangerous one: the interface is non-nil, so only the reflective nil check
// stops the caller from dereferencing it.
func TestIssueReadRejectsEveryDegenerateStreamHandoff(t *testing.T) {
	t.Parallel()

	minted := errors.New("id source exhausted")
	refused := errors.New("connection refused")
	for _, testCase := range []struct {
		name       string
		queryID    func() (string, error)
		cancel     bool
		rows       driver.Rows
		queryErr   error
		wantCalls  int
		wantErrIs  error
		wantSubstr string
	}{
		{
			name:       "query ID source fails",
			queryID:    func() (string, error) { return "", minted },
			wantErrIs:  minted,
			wantSubstr: "execute ClickHouse field catalog: create query ID",
		},
		{
			name:       "query ID source returns an empty string",
			queryID:    func() (string, error) { return "", nil },
			wantSubstr: "execute ClickHouse field catalog: query ID is empty",
		},
		{
			name:       "context is canceled after the ID is minted",
			cancel:     true,
			wantErrIs:  context.Canceled,
			wantSubstr: "context canceled",
		},
		{
			name:       "driver returns an untyped nil stream",
			wantErrIs:  searchjobs.ErrInvalidResult,
			wantCalls:  1,
			wantSubstr: "ClickHouse field catalog returned no result stream",
		},
		{
			name:       "driver returns a typed nil stream",
			rows:       (*fakeRows)(nil),
			wantErrIs:  searchjobs.ErrInvalidResult,
			wantCalls:  1,
			wantSubstr: "returned no result stream",
		},
		{
			name:       "driver refuses the query",
			queryErr:   refused,
			wantErrIs:  refused,
			wantCalls:  1,
			wantSubstr: "query ClickHouse field catalog",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			connection := &recordingConnection{rows: testCase.rows, err: testCase.queryErr}
			executor := mustExecutor(t, connection)
			if testCase.queryID != nil {
				executor.newQueryID = testCase.queryID
			}
			ctx := context.Background()
			if testCase.cancel {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			rows, err := executor.issueRead(ctx, "field catalog", "SELECT 1", nil, nil, nil)
			if err == nil || rows != nil {
				t.Fatalf("issueRead = %v, %v; want a failure and no stream", rows, err)
			}
			if testCase.wantErrIs != nil && !errors.Is(err, testCase.wantErrIs) {
				t.Fatalf("err = %v, want %v", err, testCase.wantErrIs)
			}
			if !strings.Contains(err.Error(), testCase.wantSubstr) {
				t.Fatalf("err = %q, want it to mention %q", err, testCase.wantSubstr)
			}
			if connection.calls != testCase.wantCalls {
				t.Fatalf("connection calls = %d, want %d", connection.calls, testCase.wantCalls)
			}
		})
	}
}

// TestIssueReadKeepsEachOperationWordingDistinct guards the one thing the
// shared tail must not flatten: every caller's error still names its own
// operation.
func TestIssueReadKeepsEachOperationWordingDistinct(t *testing.T) {
	t.Parallel()

	executor := mustExecutor(t, &recordingConnection{})
	for _, operation := range []string{
		"field catalog", "field summary", "timeline", "field suggestions", "stats wildcard inventory",
	} {
		_, err := executor.issueRead(context.Background(), operation, "SELECT 1", nil, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "ClickHouse "+operation+" returned no result stream") {
			t.Fatalf("operation %q error = %v", operation, err)
		}
	}
}

// TestIssueReadIsRaceFreeAcrossConcurrentReaders runs the shared tail from many
// goroutines against one executor, which is how the buffered readers reach it
// in a server process. Run with -race.
func TestIssueReadIsRaceFreeAcrossConcurrentReaders(t *testing.T) {
	t.Parallel()

	const callers = 32
	var counter int64
	var counterMutex sync.Mutex
	seen := make(map[string]struct{}, callers)
	connection := &recordingConnection{rows: &fakeRows{}}
	executor := mustExecutor(t, connection)
	executor.newQueryID = func() (string, error) {
		counterMutex.Lock()
		defer counterMutex.Unlock()
		counter++
		return fmt.Sprintf("open-splunk-search-%d", counter), nil
	}
	settings := clickhousedriver.Settings{"max_result_rows": uint64(1)}
	var group sync.WaitGroup
	failures := make([]error, callers)
	for index := range callers {
		group.Go(func() {
			rows, err := executor.issueRead(
				context.Background(),
				"timeline",
				fmt.Sprintf("SELECT %d", index),
				[]any{index},
				settings,
				nil,
			)
			if err != nil || rows == nil {
				failures[index] = err
			}
		})
	}
	group.Wait()
	for index, err := range failures {
		if err != nil {
			t.Fatalf("caller %d: %v", index, err)
		}
	}
	if connection.calls != callers || len(connection.queries) != callers {
		t.Fatalf("calls=%d queries=%d, want %d", connection.calls, len(connection.queries), callers)
	}
	counterMutex.Lock()
	defer counterMutex.Unlock()
	if counter != callers {
		t.Fatalf("minted %d query IDs, want %d", counter, callers)
	}
	for _, query := range connection.queries {
		seen[query] = struct{}{}
	}
	if len(seen) != callers {
		t.Fatalf("distinct queries = %d, want %d", len(seen), callers)
	}
}

// TestCloseReadStreamDiscardsTheBufferOnlyWhenCloseIsTheFirstFailure covers the
// deferred fallback that pairs with issueRead.
func TestCloseReadStreamDiscardsTheBufferOnlyWhenCloseIsTheFirstFailure(t *testing.T) {
	t.Parallel()

	closeFailure := errors.New("close failed")
	earlier := errors.New("earlier failure")
	for _, testCase := range []struct {
		name        string
		alreadyDone bool
		closeErr    error
		priorErr    error
		wantResult  int
		wantErrIs   error
		wantClosed  bool
	}{
		{name: "already closed is a no-op", alreadyDone: true, closeErr: closeFailure, wantResult: 7},
		{name: "clean close preserves the buffer", wantResult: 7, wantClosed: true},
		{
			name: "close failure discards the buffer", closeErr: closeFailure,
			wantErrIs: closeFailure, wantClosed: true,
		},
		{
			name: "an earlier failure wins and keeps the buffer", closeErr: closeFailure,
			priorErr: earlier, wantResult: 7, wantErrIs: earlier, wantClosed: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			rows := &fakeRows{closeErr: testCase.closeErr}
			result := 7
			resultErr := testCase.priorErr
			closed := testCase.alreadyDone
			closeReadStream(context.Background(), rows, "timeline", &closed, &result, &resultErr)
			if result != testCase.wantResult || rows.closed != testCase.wantClosed {
				t.Fatalf("result=%d closed=%v", result, rows.closed)
			}
			if testCase.wantErrIs == nil {
				if resultErr != nil {
					t.Fatalf("resultErr = %v, want nil", resultErr)
				}
				return
			}
			if !errors.Is(resultErr, testCase.wantErrIs) {
				t.Fatalf("resultErr = %v, want %v", resultErr, testCase.wantErrIs)
			}
		})
	}
}
