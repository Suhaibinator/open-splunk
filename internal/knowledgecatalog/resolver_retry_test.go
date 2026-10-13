package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/jackc/pgx/v5/pgconn"
)

type resolverRetryCodeImpostor struct {
	code int
}

func (err *resolverRetryCodeImpostor) Error() string { return "not a PostgreSQL error" }
func (err *resolverRetryCodeImpostor) Code() int     { return err.code }

func TestResolverPostgresRetryClassification(t *testing.T) {
	for _, test := range []struct {
		name, state string
		want        bool
	}{
		{"serialization", "40001", true}, {"deadlock", "40P01", true}, {"lock unavailable", "55P03", true},
		{"canceled", "57014", false}, {"duplicate", "23505", false}, {"missing table", "42P01", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := &pgconn.PgError{Code: test.state}
			if got := resolverPostgresRetryable(fmt.Errorf("wrapped: %w", err)); got != test.want {
				t.Fatalf("retry state %s = %t", test.state, got)
			}
		})
	}
	if resolverPostgresRetryable(&resolverRetryCodeImpostor{code: 5}) {
		t.Fatal("non-PostgreSQL Code method was retryable")
	}
	if resolverPostgresRetryable(errors.New("database is locked")) {
		t.Fatal("error text was retryable")
	}
	database, _ := newCatalogTestStore(t)
	busyErr := resolverTestPostgresLockError(t, database.SQLDB())
	if !resolverPostgresRetryable(fmt.Errorf("wrapped: %w", busyErr)) {
		t.Fatal("wrapped PostgreSQL lock error was not retryable")
	}
	_, nonbusyErr := database.SQLDB().ExecContext(t.Context(), `SELECT * FROM resolver_retry_missing_table`)
	if nonbusyErr == nil || resolverPostgresRetryable(nonbusyErr) {
		t.Fatalf("missing-table error was retryable: %v", nonbusyErr)
	}
}

func TestResolverRetriesBusyExactlyThreeAttemptsWithPureBackoff(t *testing.T) {
	database, store := newCatalogTestStore(t)
	busyErr := resolverTestPostgresLockError(t, database.SQLDB())
	resolver := mustTestResolver(t, store)

	var attempts atomic.Int32
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		attempts.Add(1)
		return Resolution{}, busyErr
	}
	var delays []time.Duration
	resolver.backoff = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}

	if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, busyErr) {
		t.Fatalf("Resolve(always BUSY) error = %v, want original PostgreSQL BUSY", err)
	}
	if got := attempts.Load(); got != MaximumResolutionAttempts {
		t.Fatalf("Resolve(always BUSY) attempts = %d, want %d", got, MaximumResolutionAttempts)
	}
	wantDelays := []time.Duration{2 * time.Millisecond, 4 * time.Millisecond}
	if !slices.Equal(delays, wantDelays) {
		t.Fatalf("Resolve(always BUSY) backoffs = %v, want %v", delays, wantDelays)
	}
}

func TestResolverDoesNotRetryNonbusyPostgreSQLError(t *testing.T) {
	database, store := newCatalogTestStore(t)
	_, nonbusyErr := database.SQLDB().ExecContext(t.Context(), `SELECT * FROM resolver_retry_missing_table`)
	if nonbusyErr == nil || resolverPostgresRetryable(nonbusyErr) {
		t.Fatalf("nonbusy PostgreSQL probe error = %v", nonbusyErr)
	}
	resolver := mustTestResolver(t, store)

	var attempts atomic.Int32
	var backoffs atomic.Int32
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		attempts.Add(1)
		return Resolution{}, nonbusyErr
	}
	resolver.backoff = func(context.Context, time.Duration) error {
		backoffs.Add(1)
		return nil
	}

	if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, nonbusyErr) {
		t.Fatalf("Resolve(nonbusy PostgreSQL error) = %v, want original error", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("Resolve(nonbusy PostgreSQL error) attempts = %d, want 1", got)
	}
	if got := backoffs.Load(); got != 0 {
		t.Fatalf("Resolve(nonbusy PostgreSQL error) backoffs = %d, want 0", got)
	}
}

func TestResolverCancellationInterruptsRetryBackoff(t *testing.T) {
	database, store := newCatalogTestStore(t)
	busyErr := resolverTestPostgresLockError(t, database.SQLDB())
	resolver := mustTestResolver(t, store)

	var attempts atomic.Int32
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		attempts.Add(1)
		return Resolution{}, busyErr
	}
	backoffStarted := make(chan time.Duration, 1)
	resolver.backoff = func(ctx context.Context, delay time.Duration) error {
		backoffStarted <- delay
		return waitResolutionRetry(ctx, time.Hour)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := resolver.Resolve(ctx, testResolutionScope("main"))
		result <- err
	}()

	select {
	case delay := <-backoffStarted:
		if delay != 2*time.Millisecond {
			cancel()
			t.Fatalf("first retry backoff = %v, want 2ms", delay)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("resolver did not enter retry backoff")
	}
	canceledAt := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Resolve(canceled in backoff) error = %v, want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("resolver backoff ignored cancellation")
	}
	if elapsed := time.Since(canceledAt); elapsed > 100*time.Millisecond {
		t.Fatalf("resolver backoff cancellation took %v", elapsed)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("Resolve(canceled in backoff) attempts = %d, want 1", got)
	}
}

func TestResolverSoleConnectionExhaustionHonorsBoundAndReleasesGate(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	raw := database.SQLDB()
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	held, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatalf("hold sole PostgreSQL connection: %v", err)
	}

	started := time.Now()
	_, err = resolver.Resolve(context.Background(), testResolutionScope("main"))
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		_ = held.Close()
		t.Fatalf("Resolve(sole connection held) error = %v, want DeadlineExceeded", err)
	}
	if elapsed > MaximumResolutionDuration+150*time.Millisecond {
		_ = held.Close()
		t.Fatalf("Resolve(sole connection held) took %v, fixed bound is %v", elapsed, MaximumResolutionDuration)
	}
	if err := held.Close(); err != nil {
		t.Fatalf("release sole PostgreSQL connection: %v", err)
	}

	acquired := 0
	for acquired < MaximumConcurrentResolutions && resolver.gate.TryAcquire() {
		acquired++
	}
	if acquired != MaximumConcurrentResolutions {
		for range acquired {
			resolver.gate.Release()
		}
		t.Fatalf("resolver gate retained a timed-out permit: acquired %d of %d", acquired, MaximumConcurrentResolutions)
	}
	if resolver.gate.TryAcquire() {
		resolver.gate.Release()
		for range acquired {
			resolver.gate.Release()
		}
		t.Fatal("resolver gate admitted work beyond its fixed capacity")
	}
	for range acquired {
		resolver.gate.Release()
	}

	resolved, err := resolver.Resolve(context.Background(), testResolutionScope("main"))
	if err != nil || resolved.IsZero() {
		t.Fatalf("Resolve(after pool and gate release) = (%#v, %v), want prepared authority", resolved.Summary(), err)
	}
}

func resolverTestPostgresLockError(t *testing.T, database *sql.DB) error {
	t.Helper()
	ctx := t.Context()
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE resolver_retry_probe (
			id INTEGER PRIMARY KEY,
			value INTEGER NOT NULL
		)
	`); err != nil {
		t.Fatalf("create resolver retry probe: %v", err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO resolver_retry_probe (id, value) VALUES (1, 0)`); err != nil {
		t.Fatalf("seed resolver retry probe: %v", err)
	}

	writer, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("open resolver retry writer: %v", err)
	}
	defer func() { _ = writer.Close() }()
	contender, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("open resolver retry contender: %v", err)
	}
	defer func() { _ = contender.Close() }()

	tx, err := pgmeta.BeginSQL(ctx, writer, nil)
	if err != nil {
		t.Fatalf("begin resolver retry writer: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE resolver_retry_probe SET value = value + 1 WHERE id = 1`); err != nil {
		t.Fatalf("lock resolver retry probe: %v", err)
	}
	_, busyErr := contender.ExecContext(ctx, `SELECT id FROM resolver_retry_probe WHERE id = 1 FOR UPDATE NOWAIT`)
	if busyErr == nil {
		t.Fatal("contending resolver retry write unexpectedly succeeded")
	}
	if !resolverPostgresRetryable(busyErr) {
		t.Fatalf("contending resolver retry write error = %v, want concrete PostgreSQL BUSY/LOCKED", busyErr)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback resolver retry writer: %v", err)
	}
	return busyErr
}
