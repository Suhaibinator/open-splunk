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
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"modernc.org/sqlite"
)

const (
	defaultBusyTimeout               = 5 * time.Second
	defaultMaxOpenConns              = 8
	maximumControlTimestampUnixMicro = int64(253_402_300_799_999_999)
)

// DB is the SQLite-backed single-node control-plane database.
type DB struct {
	sql                *sql.DB
	orm                *gorm.DB
	fileInfo           os.FileInfo
	sharedAdmissionMu  sync.Mutex
	sharedAdmissionMap map[string]*AdmissionGate
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
	orm, err := configureGORM(raw)
	if err != nil {
		return closeOnError(fmt.Errorf("configure GORM control plane: %w", err))
	}
	if _, err := readIndexCatalogIntegrity(orm.WithContext(ctx)); err != nil {
		return closeOnError(fmt.Errorf("audit index catalog: %w", err))
	}
	if err := secureSQLiteFiles(absPath, false); err != nil {
		return closeOnError(err)
	}
	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return closeOnError(fmt.Errorf("identify SQLite control plane: %w", err))
	}

	return &DB{sql: raw, orm: orm, fileInfo: fileInfo}, nil
}

// OpenReadOnly opens an existing control-plane database without creating it,
// changing its permissions, selecting a journal mode, or applying migrations.
// It is intended for offline verification and backup tooling. The returned
// GORM handle shares the same single-connection, query-only pool.
func OpenReadOnly(ctx context.Context, path string) (*DB, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return nil, fmt.Errorf("%w: SQLite path must name an existing persistent file", ErrInvalidArgument)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve read-only SQLite path: %w", err)
	}
	before, err := os.Lstat(absPath)
	if err != nil {
		return nil, fmt.Errorf("inspect read-only SQLite control plane: %w", err)
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("open read-only SQLite control plane: source must be a regular file")
	}

	raw, err := sql.Open("sqlite", sqliteReadOnlyDSN(absPath))
	if err != nil {
		return nil, fmt.Errorf("open read-only SQLite control plane: %w", err)
	}
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	closeOnError := func(openErr error) (*DB, error) {
		if closeErr := raw.Close(); closeErr != nil {
			return nil, errors.Join(openErr, fmt.Errorf("close read-only SQLite control plane: %w", closeErr))
		}
		return nil, openErr
	}
	if err := raw.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("connect to read-only SQLite control plane: %w", err))
	}
	after, err := os.Lstat(absPath)
	if err != nil {
		return closeOnError(fmt.Errorf("reinspect read-only SQLite control plane: %w", err))
	}
	if !sameReadOnlySQLiteFileState(before, after) {
		return closeOnError(errors.New("open read-only SQLite control plane: source changed while opening"))
	}
	orm, err := configureGORM(raw)
	if err != nil {
		return closeOnError(fmt.Errorf("configure read-only GORM control plane: %w", err))
	}
	return &DB{sql: raw, orm: orm, fileInfo: after}, nil
}

func configureGORM(raw *sql.DB) (*gorm.DB, error) {
	return gorm.Open(gormsqlite.New(gormsqlite.Config{
		DriverName: "sqlite",
		Conn:       raw,
	}), &gorm.Config{
		DisableAutomaticPing:   true,
		Logger:                 logger.Discard,
		SkipDefaultTransaction: true,
	})
}

func sameReadOnlySQLiteFileState(left, right os.FileInfo) bool {
	return left != nil && right != nil &&
		left.Mode().IsRegular() && right.Mode().IsRegular() &&
		os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}

// secureSQLiteFiles ensures the control database and every SQLite sidecar are
// accessible only to their owner. The ingestion visibility outbox can contain
// normalized log payloads, so relying on the process umask is insufficient.
func secureSQLiteFiles(path string, create bool) error {
	if create {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			if closeErr := file.Close(); closeErr != nil {
				return fmt.Errorf("secure SQLite control plane: close new database: %w", closeErr)
			}
		} else if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("secure SQLite control plane: create database: %w", err)
		}
	}

	for _, candidate := range sqliteFileCandidates(path) {
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
	var journalMode string
	err := retrySQLiteBusy(ctx, defaultBusyTimeout, func() error {
		journalMode = ""
		return db.QueryRowContext(ctx, `PRAGMA journal_mode = WAL`).Scan(&journalMode)
	})
	if err != nil {
		return fmt.Errorf("enable SQLite WAL: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("enable SQLite WAL: database selected %q journal mode", journalMode)
	}
	return nil
}

func retrySQLiteBusy(
	ctx context.Context,
	retryWindow time.Duration,
	operation func() error,
) error {
	// The window bounds when another attempt may begin. The operation itself
	// remains bounded by its context and any driver-level busy timeout.
	deadline := time.Now().Add(retryWindow)
	delay := 2 * time.Millisecond
	for {
		err := operation()
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !sqliteBusyOrLocked(err) || !time.Now().Before(deadline) {
			return err
		}

		wait := min(delay, time.Until(deadline))
		if wait <= 0 {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 100*time.Millisecond)
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

func sqliteReadOnlyDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	query.Add("_pragma", "query_only(1)")
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

// GORMDB exposes the GORM handle configured over the same SQLite connection
// pool as SQLDB. Versioned SQL migrations remain authoritative; callers must
// not use AutoMigrate or close the underlying pool.
func (db *DB) GORMDB() *gorm.DB {
	if db == nil {
		return nil
	}
	return db.orm
}

// SameSQLiteFile reports whether two control handles were opened over the same
// physical database file. It is used only for process-local ownership fencing;
// SQLite transactions remain authoritative for persisted state.
func (db *DB) SameSQLiteFile(other *DB) bool {
	if db == nil || other == nil {
		return false
	}
	if db.fileInfo == nil || other.fileInfo == nil {
		return db == other || (db.sql != nil && db.sql == other.sql)
	}
	return os.SameFile(db.fileInfo, other.fileInfo)
}

// Close releases all SQLite connections.
func (db *DB) Close() error {
	if db == nil || db.sql == nil {
		return nil
	}
	return db.sql.Close()
}
