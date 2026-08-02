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

	"modernc.org/sqlite"
)

const sqliteBackupStepPages int32 = 256

type backupHooks struct {
	afterStep func(more bool)
}

type nativeSQLiteBackuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// BackupTo creates a transactionally consistent SQLite backup at destination.
// The destination and its SQLite sidecar paths must not already exist. Backup
// pages are copied in bounded batches so cancellation is observed promptly.
func (db *DB) BackupTo(ctx context.Context, destination string) error {
	return db.backupTo(ctx, destination, backupHooks{})
}

func (db *DB) backupTo(
	ctx context.Context,
	destination string,
	hooks backupHooks,
) (err error) {
	if ctx == nil || db == nil || db.sql == nil {
		return fmt.Errorf("%w: backup context and database are required", ErrInvalidArgument)
	}
	if strings.TrimSpace(destination) == "" || destination == ":memory:" {
		return fmt.Errorf("%w: backup destination must name a persistent file", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	absDestination, err := filepath.Abs(destination)
	if err != nil {
		return fmt.Errorf("resolve SQLite backup destination: %w", err)
	}
	if err := requireAbsentSQLiteDestination(absDestination); err != nil {
		return err
	}
	parent := filepath.Dir(absDestination)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("inspect SQLite backup directory: %w", err)
	}
	if !parentInfo.IsDir() {
		return errors.New("create SQLite backup: destination parent is not a directory")
	}

	temporary, err := os.CreateTemp(parent, "."+filepath.Base(absDestination)+"-backup-*")
	if err != nil {
		return fmt.Errorf("create temporary SQLite backup: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	temporaryExists := true
	destinationCreated := false
	var completedIdentity os.FileInfo
	completed := false
	defer func() {
		if completed {
			return
		}
		if temporaryOpen {
			closeErr := temporary.Close()
			temporaryOpen = false
			if closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close temporary SQLite backup: %w", closeErr))
			}
		}
		if temporaryExists {
			err = errors.Join(err, removeBackupOwnedSQLiteFiles(temporaryPath))
		}
		if destinationCreated {
			err = errors.Join(err, removeCreatedBackup(absDestination, completedIdentity))
		}
		if temporaryExists || destinationCreated {
			err = errors.Join(err, syncDirectory(parent))
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary SQLite backup: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary SQLite backup: %w", err)
	}
	temporaryOpen = false

	if err := backupSQLiteToPath(ctx, db.sql, temporaryPath, hooks); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := normalizeSQLiteBackup(ctx, temporaryPath); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ensureNoSQLiteSidecars(temporaryPath); err != nil {
		return err
	}
	completedIdentity, err = syncRegularFile(temporaryPath)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireAbsentSQLiteDestination(absDestination); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, absDestination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: SQLite backup destination appeared while publishing", ErrAlreadyExists)
		}
		return fmt.Errorf("publish SQLite backup: %w", err)
	}
	destinationCreated = true
	destinationInfo, err := os.Lstat(absDestination)
	if err != nil {
		return fmt.Errorf("inspect published SQLite backup: %w", err)
	}
	if !destinationInfo.Mode().IsRegular() || !os.SameFile(completedIdentity, destinationInfo) {
		return errors.New("publish SQLite backup: destination identity changed")
	}
	if err := ensureNoSQLiteSidecars(absDestination); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove temporary SQLite backup link: %w", err)
	}
	temporaryExists = false
	if err := syncDirectory(parent); err != nil {
		return err
	}
	completed = true
	return nil
}

func backupSQLiteToPath(
	ctx context.Context,
	database *sql.DB,
	destination string,
	hooks backupHooks,
) (err error) {
	conn, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire SQLite backup connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close SQLite backup connection: %w", closeErr))
		}
	}()

	if _, err := conn.ExecContext(ctx, `BEGIN`); err != nil {
		return fmt.Errorf("begin SQLite backup snapshot: %w", err)
	}
	transactionOpen := true
	defer func() {
		if !transactionOpen {
			return
		}
		if _, rollbackErr := conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("close SQLite backup snapshot: %w", rollbackErr))
		}
	}()
	var schemaVersion int64
	if err := conn.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		return fmt.Errorf("establish SQLite backup snapshot: %w", err)
	}

	if err := conn.Raw(func(driverConnection any) error {
		backuper, ok := driverConnection.(nativeSQLiteBackuper)
		if !ok {
			return errors.New("SQLite driver does not support native online backup")
		}
		return runNativeSQLiteBackup(ctx, backuper, sqliteBackupDSN(destination), hooks)
	}); err != nil {
		return fmt.Errorf("copy SQLite backup: %w", err)
	}
	if _, err := conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`); err != nil {
		return fmt.Errorf("close SQLite backup snapshot: %w", err)
	}
	transactionOpen = false
	return nil
}

func runNativeSQLiteBackup(
	ctx context.Context,
	backuper nativeSQLiteBackuper,
	destinationDSN string,
	hooks backupHooks,
) (err error) {
	backup, err := backuper.NewBackup(destinationDSN)
	if err != nil {
		return fmt.Errorf("start native SQLite backup: %w", err)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		if finishErr := backup.Finish(); finishErr != nil {
			err = errors.Join(err, fmt.Errorf("finish native SQLite backup: %w", finishErr))
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		more, err := backup.Step(sqliteBackupStepPages)
		if err != nil {
			return fmt.Errorf("step native SQLite backup: %w", err)
		}
		if hooks.afterStep != nil {
			hooks.afterStep(more)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !more {
			break
		}
	}
	if err := backup.Finish(); err != nil {
		finished = true
		return fmt.Errorf("finish native SQLite backup: %w", err)
	}
	finished = true
	return nil
}

func requireAbsentSQLiteDestination(path string) error {
	for _, candidate := range sqliteFileCandidates(path) {
		_, err := os.Lstat(candidate)
		switch {
		case err == nil:
			return fmt.Errorf("%w: SQLite backup path %q exists", ErrAlreadyExists, filepath.Base(candidate))
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return fmt.Errorf("inspect SQLite backup path %q: %w", filepath.Base(candidate), err)
		}
	}
	return nil
}

func ensureNoSQLiteSidecars(path string) error {
	for _, candidate := range sqliteFileCandidates(path)[1:] {
		_, err := os.Lstat(candidate)
		switch {
		case err == nil:
			return fmt.Errorf("finalize SQLite backup: sidecar %q remains", filepath.Base(candidate))
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return fmt.Errorf("inspect SQLite backup sidecar %q: %w", filepath.Base(candidate), err)
		}
	}
	return nil
}

func removeBackupOwnedSQLiteFiles(path string) error {
	var cleanupErr error
	for _, candidate := range sqliteFileCandidates(path) {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove temporary SQLite backup %q: %w", filepath.Base(candidate), err))
		}
	}
	return cleanupErr
}

func removeCreatedBackup(path string, expected os.FileInfo) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect incomplete SQLite backup: %w", err)
	}
	if expected == nil || !info.Mode().IsRegular() || !os.SameFile(expected, info) {
		return errors.New("incomplete SQLite backup identity changed; refusing cleanup")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove incomplete SQLite backup: %w", err)
	}
	return nil
}

func sqliteFileCandidates(path string) []string {
	return []string{path, path + "-wal", path + "-shm", path + "-journal"}
}

func sqliteBackupDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Set("mode", "rw")
	query.Set("_dqs", "0")
	u.RawQuery = query.Encode()
	return u.String()
}

func normalizeSQLiteBackup(ctx context.Context, path string) (err error) {
	database, err := sql.Open("sqlite", sqliteBackupDSN(path))
	if err != nil {
		return fmt.Errorf("open completed SQLite backup for normalization: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	defer func() {
		if closeErr := database.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close normalized SQLite backup: %w", closeErr))
		}
	}()

	var journalMode string
	if err := database.QueryRowContext(ctx, `PRAGMA journal_mode = DELETE`).Scan(&journalMode); err != nil {
		return fmt.Errorf("normalize SQLite backup journal mode: %w", err)
	}
	if !strings.EqualFold(journalMode, "delete") {
		return fmt.Errorf("normalize SQLite backup journal mode: selected %q", journalMode)
	}
	return nil
}

func syncRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect completed SQLite backup: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("finalize SQLite backup: output is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open completed SQLite backup: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("sync completed SQLite backup: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close completed SQLite backup: %w", err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect completed SQLite backup: %w", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(info, after) {
		return nil, errors.New("finalize SQLite backup: output identity changed")
	}
	return after, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open SQLite backup directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync SQLite backup directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close SQLite backup directory: %w", err)
	}
	return nil
}

// VerifyIntegrity runs SQLite's structural and foreign-key integrity checks.
func (db *DB) VerifyIntegrity(ctx context.Context) error {
	if ctx == nil || db == nil || db.sql == nil {
		return fmt.Errorf("%w: integrity context and database are required", ErrInvalidArgument)
	}
	var result string
	if err := db.sql.QueryRowContext(ctx, `PRAGMA integrity_check(1)`).Scan(&result); err != nil {
		return fmt.Errorf("run SQLite integrity check: %w", err)
	}
	if result != "ok" {
		return errors.New("SQLite integrity check failed")
	}
	rows, err := db.sql.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("run SQLite foreign-key check: %w", err)
	}
	violation := rows.Next()
	iterationErr := rows.Err()
	closeErr := rows.Close()
	if iterationErr != nil {
		return fmt.Errorf("iterate SQLite foreign-key check: %w", iterationErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close SQLite foreign-key check: %w", closeErr)
	}
	if violation {
		return errors.New("SQLite foreign-key integrity check failed")
	}
	return nil
}
