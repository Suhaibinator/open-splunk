package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	"modernc.org/sqlite"
)

const (
	defaultBusyTimeout  = 5 * time.Second
	defaultMaxOpenConns = 8
)

// DB is the SQLite-backed single-node control-plane database.
type DB struct {
	sql *sql.DB
}

// Open opens a persistent SQLite control-plane database, configures its
// connection invariants, and applies all embedded migrations. path must name a
// file; an in-memory database cannot provide the required WAL durability.
func Open(ctx context.Context, path string) (*DB, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return nil, fmt.Errorf("%w: SQLite path must name a persistent file", ErrInvalidArgument)
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite path: %w", err)
	}
	if err := secureSQLiteFiles(absPath, true); err != nil {
		return nil, err
	}
	dsn := sqliteDSN(absPath)
	raw, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite control plane: %w", err)
	}
	raw.SetMaxOpenConns(defaultMaxOpenConns)
	raw.SetMaxIdleConns(defaultMaxOpenConns)

	closeOnError := func(openErr error) (*DB, error) {
		if closeErr := raw.Close(); closeErr != nil {
			return nil, errors.Join(openErr, fmt.Errorf("close SQLite control plane: %w", closeErr))
		}
		return nil, openErr
	}
	if err := raw.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("connect to SQLite control plane: %w", err))
	}

	if err := enableWAL(ctx, raw); err != nil {
		return closeOnError(err)
	}
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		return closeOnError(err)
	}
	if err := secureSQLiteFiles(absPath, false); err != nil {
		return closeOnError(err)
	}

	return &DB{sql: raw}, nil
}

// secureSQLiteFiles ensures the control database and every SQLite sidecar are
// accessible only to their owner. The ingestion visibility outbox can contain
// normalized log payloads, so relying on the process umask is insufficient.
func secureSQLiteFiles(path string, create bool) error {
	if create {
		for {
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err == nil {
				if closeErr := file.Close(); closeErr != nil {
					return fmt.Errorf("secure SQLite control plane: close new database: %w", closeErr)
				}
				break
			}
			if !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("secure SQLite control plane: create database: %w", err)
			}
			break
		}
	}

	for _, candidate := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) && candidate != path {
			continue
		}
		if err != nil {
			return fmt.Errorf("secure SQLite control plane file %q: %w", filepath.Base(candidate), err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("secure SQLite control plane file %q: must be a regular file", filepath.Base(candidate))
		}
		if err := os.Chmod(candidate, 0o600); err != nil {
			if candidate != path && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("secure SQLite control plane file %q: %w", filepath.Base(candidate), err)
		}
	}
	return nil
}

func enableWAL(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(defaultBusyTimeout)
	delay := 2 * time.Millisecond
	for {
		var journalMode string
		err := db.QueryRowContext(ctx, `PRAGMA journal_mode = WAL`).Scan(&journalMode)
		if err == nil {
			if !strings.EqualFold(journalMode, "wal") {
				return fmt.Errorf("enable SQLite WAL: database selected %q journal mode", journalMode)
			}
			return nil
		}
		if !sqliteBusyOrLocked(err) || !time.Now().Before(deadline) {
			return fmt.Errorf("enable SQLite WAL: %w", err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("enable SQLite WAL: %w", ctx.Err())
		case <-timer.C:
		}
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
}

func sqliteBusyOrLocked(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	// SQLite's primary result codes are stable API values. Mask extended
	// result codes before checking SQLITE_BUSY (5) and SQLITE_LOCKED (6).
	switch sqliteErr.Code() & 0xff {
	case 5, 6:
		return true
	default:
		return false
	}
}

func sqliteDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	query.Add("_pragma", "synchronous(FULL)")
	query.Set("_txlock", "immediate")
	query.Set("_dqs", "0")
	u.RawQuery = query.Encode()
	return u.String()
}

// SQLDB exposes the pooled database handle to other internal persistence
// packages. Callers must preserve the connection invariants established by
// Open and must not close the returned handle directly.
func (db *DB) SQLDB() *sql.DB {
	if db == nil {
		return nil
	}
	return db.sql
}

// Close releases all SQLite connections.
func (db *DB) Close() error {
	if db == nil || db.sql == nil {
		return nil
	}
	return db.sql.Close()
}
