package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	pgx "github.com/jackc/pgx/v5"
)

// ConnectionString is the configured PostgreSQL connection string. It may
// contain credentials and must never be logged or included in a manifest.
func (db *DB) ConnectionString() string {
	if db == nil || db.connectionConfig == nil {
		return ""
	}
	return db.connectionConfig.ConnString()
}

// BackupTo creates a private, transactionally consistent pg_dump custom archive
// of the shared control/identity schema. Callers coordinate its generation with
// MinIO and ClickHouse through the deployment recovery write freeze.
func (db *DB) BackupTo(ctx context.Context, destination string) (err error) {
	if ctx == nil || db == nil || db.connectionConfig == nil || strings.TrimSpace(destination) == "" {
		return ErrInvalidArgument
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return ErrAlreadyExists
		}
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".open-splunk-pg-backup-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	var schema string
	if err := db.sql.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return err
	}
	command, err := postgresCommand(ctx, false, "--format=custom", "--no-owner", "--no-acl", "--schema="+schema, "--exclude-table-data="+schema+".control_recovery_publication", "--file="+name)
	if err != nil {
		return err
	}
	command.Env = postgresToolEnv(db.connectionConfig)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("PostgreSQL backup failed: %w: %s", err, redactToolOutput(output, db.connectionConfig))
	}
	archive, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	info, err := archive.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Size() == 0) {
		err = errors.New("PostgreSQL backup is empty or not a regular file")
	}
	if err == nil {
		err = archive.Sync()
	}
	err = errors.Join(err, archive.Close())
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A hard link publishes without overwriting a destination created concurrently.
	if err := os.Link(name, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrAlreadyExists
		}
		return err
	}
	directory, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// RestoreFrom restores a custom archive into an empty PostgreSQL schema. It
// never cleans, overwrites or adopts a populated target. Archive schema identity
// must match the target; production uses one stable control schema.
func RestoreFrom(ctx context.Context, dsn, source string) error {
	lease, err := AcquireMaintenanceLease(ctx, dsn)
	if err != nil {
		return err
	}
	defer lease.Close()
	return RestoreFromWithLease(ctx, dsn, source, lease)
}

// RestoreFromWithLease composes PostgreSQL restore with the caller's existing
// deployment recovery lease; it never reacquires an advisory lock on another session.
func RestoreFromWithLease(ctx context.Context, dsn, source string, lease *MaintenanceLease) error {
	archive, err := os.Open(source)
	if err != nil {
		return err
	}
	defer archive.Close()
	return RestorePinnedFromWithLease(ctx, dsn, archive, lease)
}

// RestorePinnedFromWithLease reads the exact descriptor validated by recovery.
// Both archive inspection and restore inherit that descriptor, preventing a
// pathname replacement between manifest verification and pg_restore.
func RestorePinnedFromWithLease(ctx context.Context, dsn string, archive *os.File, lease *MaintenanceLease) error {
	if err := lease.Validate(dsn); err != nil {
		return err
	}
	if ctx == nil || archive == nil || strings.TrimSpace(dsn) == "" {
		return ErrInvalidArgument
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("%w: invalid PostgreSQL target", ErrInvalidArgument)
	}
	info, err := archive.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return errors.New("PostgreSQL archive must be a nonempty regular file")
	}
	source := "/dev/fd/3"
	conn := lease.conn

	var schema string
	var populated bool
	if err := conn.QueryRowContext(ctx, "SELECT current_schema(), EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','S','f'))").Scan(&schema, &populated); err != nil {
		return err
	}
	if populated {
		return errors.New("PostgreSQL restore target schema must be empty")
	}
	list, err := PostgresRestoreListCommand(ctx)
	if err != nil {
		return err
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	list.ExtraFiles = []*os.File{archive}
	list.Env = postgresToolEnv(cfg)
	contents, err := list.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL archive: %w: %s", err, redactToolOutput(contents, cfg))
	}
	lines := strings.Split(string(contents), "\n")
	for index, line := range lines {
		if strings.HasPrefix(line, ";") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 6 && fields[3] == "SCHEMA" && fields[4] == "-" {
			if fields[5] != schema {
				return errors.New("PostgreSQL archive schema differs from restore target")
			}
			// The empty target schema already exists. Keep its namespace identity and
			// restore only its contents rather than issuing a conflicting CREATE SCHEMA.
			lines[index] = ";" + line
		}
	}
	toc, err := os.CreateTemp("", "open-splunk-restore-toc-*")
	if err != nil {
		return err
	}
	defer os.Remove(toc.Name())
	if _, err := toc.WriteString(strings.Join(lines, "\n")); err != nil {
		return errors.Join(err, toc.Close())
	}
	if err := toc.Close(); err != nil {
		return err
	}
	command, err := postgresCommand(ctx, true, "--single-transaction", "--exit-on-error", "--no-owner", "--no-acl", "--dbname="+cfg.Database, "--use-list="+toc.Name(), source)
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	command.ExtraFiles = []*os.File{archive}
	command.Env = postgresToolEnv(cfg)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("PostgreSQL restore failed: %w: %s", err, redactToolOutput(output, cfg))
	}
	return nil
}

func postgresToolEnv(cfg *pgx.ConnConfig) []string {
	env := make([]string, 0, len(os.Environ())+10)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, "PG") {
			env = append(env, entry)
		}
	}
	settings := postgresTLSSettings(cfg.ConnString())
	mode := settings["sslmode"]
	if mode == "" {
		mode = "prefer"
	}
	env = append(env, "PGHOST="+cfg.Host, "PGPORT="+strconv.Itoa(int(cfg.Port)), "PGUSER="+cfg.User, "PGDATABASE="+cfg.Database, "PGPASSWORD="+cfg.Password, "PGSSLMODE="+mode)
	for key, value := range map[string]string{"sslrootcert": "PGSSLROOTCERT", "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY"} {
		if setting := settings[key]; setting != "" {
			env = append(env, value+"="+setting)
		}
	}
	return env
}

// postgresTLSSettings preserves libpq TLS policy for both URI and keyword DSNs.
// Passwords are neither returned nor passed as subprocess arguments.
func postgresTLSSettings(source string) map[string]string {
	result := make(map[string]string)
	wanted := func(key string) bool {
		return key == "sslmode" || key == "sslrootcert" || key == "sslcert" || key == "sslkey"
	}
	if parsed, err := url.Parse(source); err == nil && (parsed.Scheme == "postgres" || parsed.Scheme == "postgresql") {
		for key, values := range parsed.Query() {
			if wanted(key) && len(values) > 0 {
				result[key] = values[len(values)-1]
			}
		}
		return result
	}
	for index := 0; index < len(source); {
		for index < len(source) && (source[index] == ' ' || source[index] == '\t' || source[index] == '\n') {
			index++
		}
		start := index
		for index < len(source) && source[index] != '=' && source[index] != ' ' && source[index] != '\t' {
			index++
		}
		key := source[start:index]
		for index < len(source) && (source[index] == ' ' || source[index] == '\t') {
			index++
		}
		if index >= len(source) || source[index] != '=' {
			break
		}
		index++
		for index < len(source) && (source[index] == ' ' || source[index] == '\t') {
			index++
		}
		quoted := index < len(source) && source[index] == '\''
		if quoted {
			index++
		}
		var value strings.Builder
		for index < len(source) {
			character := source[index]
			index++
			if character == '\\' && index < len(source) {
				value.WriteByte(source[index])
				index++
				continue
			}
			if (quoted && character == '\'') || (!quoted && (character == ' ' || character == '\t' || character == '\n')) {
				break
			}
			value.WriteByte(character)
		}
		if wanted(key) {
			result[key] = value.String()
		}
	}
	return result
}

func redactToolOutput(output []byte, cfg *pgx.ConnConfig) string {
	result := string(output)
	if cfg.Password != "" {
		result = strings.ReplaceAll(result, cfg.Password, "[redacted]")
	}
	result = strings.ReplaceAll(result, cfg.ConnString(), "[redacted connection]")
	return strings.TrimSpace(result)
}

// VerifyIntegrity checks PostgreSQL schema validation and the bounded index
// catalog. PostgreSQL enforces foreign keys and checks on every write; domain
// repositories additionally validate their immutable journals during bootstrap.
func (db *DB) VerifyIntegrity(ctx context.Context) error {
	if ctx == nil || db == nil || db.sql == nil {
		return ErrInvalidArgument
	}
	tx, err := pgmeta.BeginSQL(ctx, db.sql, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var invalid bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=current_schema() AND NOT c.convalidated)
 OR EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND (NOT i.indisvalid OR NOT i.indisready))`).Scan(&invalid); err != nil {
		return err
	}
	if invalid {
		return errors.New("PostgreSQL control schema has unvalidated constraints or invalid indexes")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = readIndexCatalogIntegrity(db.orm.WithContext(ctx))
	return err
}
