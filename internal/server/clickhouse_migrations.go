package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
)

var (
	// ErrClickHouseMigrationDrift means the migration ledger is not an exact
	// prefix of the migrations embedded in this server binary.
	ErrClickHouseMigrationDrift = errors.New("server: ClickHouse migration history differs from embedded migrations")
	// ErrClickHouseDatabaseTooNew means ClickHouse was migrated by a newer
	// server binary. Starting an older binary could otherwise corrupt data.
	ErrClickHouseDatabaseTooNew = errors.New("server: ClickHouse schema is newer than this binary")
)

var clickHouseMigrationFilename = regexp.MustCompile(`^([0-9]{4})_([a-z0-9][a-z0-9_]*)\.sql$`)

const (
	maximumClickHouseMigrationCount             = 9_999
	clickHouseMigrationTableResultLimit         = clickHouseReleaseOwnedTableSentinel
	clickHouseMigrationTableReadByteLimit       = 1 << 20
	clickHouseMigrationLedgerReadByteLimit      = 16 << 20
	clickHouseMigrationLedgerMaximumMemoryBytes = 64 << 20
	clickHouseMigrationLedgerMaximumResultBytes = 8 << 20
	// One additional raw row and ordered group form overflow sentinels. The inner
	// limit bounds duplicate-heavy input before aggregation, and callers reject
	// any duplicate or full output sentinel before retaining migration history.
	clickHouseMigrationLedgerResultLimit = maximumClickHouseMigrationCount + 1
)

var (
	clickHouseMigrationLedgerInsertPrefix = regexp.MustCompile("(?is)^\\s*INSERT\\s+INTO\\s+(?:open_splunk|`open_splunk`)\\s*\\.\\s*(?:schema_migrations\\b|`schema_migrations`)")
	clickHouseMigrationLedgerInsert       = regexp.MustCompile("(?is)^\\s*INSERT\\s+INTO\\s+(?:open_splunk|`open_splunk`)\\s*\\.\\s*(?:schema_migrations\\b|`schema_migrations`)\\s*(?:\\([^)]*\\))?\\s*SELECT\\s+([0-9]+)\\s*,\\s*'([a-z0-9_]+)'(?:\\s*,|\\s*(?:WHERE\\b|$))")
	clickHouseMigrationLedgerQuery        = clickHouseMigrationLedgerQueryForDatabase(recoverycontract.CanonicalDatabase)
	clickHouseMigrationTablesQuery        = fmt.Sprintf(`
		SELECT name
		FROM
		(
			SELECT name
			FROM system.tables
			WHERE database = 'open_splunk'
			LIMIT %d
		)
		ORDER BY name
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = %d,
			result_overflow_mode = 'throw'`,
		clickHouseMigrationTableResultLimit,
		clickHouseMigrationTableResultLimit,
		clickHouseMigrationTableReadByteLimit,
		clickHouseMigrationTableResultLimit,
	)
	clickHouseMigrationTablesByDatabaseQuery = fmt.Sprintf(`
		SELECT name
		FROM
		(
			SELECT name
			FROM system.tables
			WHERE database = ?
			LIMIT %d
		)
		ORDER BY name
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = %d,
			result_overflow_mode = 'throw'`,
		clickHouseMigrationTableResultLimit,
		clickHouseMigrationTableResultLimit,
		clickHouseMigrationTableReadByteLimit,
		clickHouseMigrationTableResultLimit,
	)
)

// One server process has only one canonical open_splunk schema. Serializing
// migration runs prevents callers in the same process from racing the
// non-transactional MergeTree ledger. A deployment must likewise start only
// one server process at a time; ClickHouse MergeTree does not provide a unique
// constraint that can serve as a cross-process migration lock.
var clickHouseMigrationRunGate = make(chan struct{}, 1)

// ClickHouseMigrationConnection is the subset of clickhouse-go's Conn used by
// the startup migration runner.
type ClickHouseMigrationConnection interface {
	Select(ctx context.Context, dest any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) error
}

type clickHouseMigration struct {
	version    uint32
	name       string
	filename   string
	statements []string
}

type clickHouseMigrationLedgerRow struct {
	Version  uint32 `ch:"version"`
	Name     string `ch:"name"`
	RowCount uint64 `ch:"row_count"`
}

type clickHouseMigrationTable struct {
	Name string `ch:"name"`
}

// ApplyClickHouseMigrations validates and applies a contiguous set of
// restart-safe ClickHouse migrations. ClickHouse DDL is not transactional, so
// every migration must be independently retryable and record its ledger row
// only after its schema changes. The shipped migrations follow that contract.
//
// Before executing any DDL, the runner verifies that the existing ledger is an
// exact prefix of the embedded migrations. After applying the pending suffix,
// it verifies the complete ledger again. This rejects reordered, renamed,
// duplicate, gapped, and newer histories. It also refuses to claim an existing
// open_splunk table set that has no migration ledger.
func ApplyClickHouseMigrations(ctx context.Context, connection ClickHouseMigrationConnection, migrationFiles fs.FS) error {
	if ctx == nil || connection == nil || migrationFiles == nil {
		return errors.New("apply ClickHouse migrations: context, connection, and filesystem are required")
	}
	select {
	case clickHouseMigrationRunGate <- struct{}{}:
		defer func() { <-clickHouseMigrationRunGate }()
	case <-ctx.Done():
		return fmt.Errorf("wait to apply ClickHouse migrations: %w", ctx.Err())
	}

	migrations, err := loadClickHouseMigrations(migrationFiles)
	if err != nil {
		return err
	}
	history, err := readClickHouseMigrationHistory(ctx, connection)
	if err != nil {
		return err
	}
	if err := verifyClickHouseMigrationHistory(history, migrations, false); err != nil {
		return err
	}

	for _, migration := range migrations[len(history):] {
		for statementIndex, statement := range migration.statements {
			if err := connection.Exec(ctx, statement); err != nil {
				return fmt.Errorf(
					"apply ClickHouse migration %s statement %d of %d: %w",
					migration.filename,
					statementIndex+1,
					len(migration.statements),
					err,
				)
			}
		}
	}

	history, err = readClickHouseMigrationHistory(ctx, connection)
	if err != nil {
		return fmt.Errorf("verify applied ClickHouse migrations: %w", err)
	}
	if err := verifyClickHouseMigrationHistory(history, migrations, true); err != nil {
		return fmt.Errorf("verify applied ClickHouse migrations: %w", err)
	}
	return nil
}

func loadClickHouseMigrations(migrationFiles fs.FS) ([]clickHouseMigration, error) {
	entries, err := fs.ReadDir(migrationFiles, ".")
	if err != nil {
		return nil, fmt.Errorf("read ClickHouse migrations: %w", err)
	}

	loaded := make([]clickHouseMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		matches := clickHouseMigrationFilename.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil, fmt.Errorf("invalid ClickHouse migration filename %q", entry.Name())
		}
		parsedVersion, err := strconv.ParseUint(matches[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse ClickHouse migration version in %q: %w", entry.Name(), err)
		}
		contents, err := fs.ReadFile(migrationFiles, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read ClickHouse migration %s: %w", entry.Name(), err)
		}
		statements, err := splitClickHouseStatements(string(contents))
		if err != nil {
			return nil, fmt.Errorf("parse ClickHouse migration %s: %w", entry.Name(), err)
		}
		if len(statements) == 0 {
			return nil, fmt.Errorf("ClickHouse migration %q contains no statements", entry.Name())
		}
		migration := clickHouseMigration{
			version:    uint32(parsedVersion),
			name:       matches[2],
			filename:   entry.Name(),
			statements: statements,
		}
		if err := validateClickHouseMigrationLedgerInsert(migration); err != nil {
			return nil, err
		}
		loaded = append(loaded, migration)
	}
	if len(loaded) == 0 {
		return nil, errors.New("no ClickHouse migrations found")
	}

	sort.Slice(loaded, func(i, j int) bool {
		if loaded[i].version != loaded[j].version {
			return loaded[i].version < loaded[j].version
		}
		return loaded[i].filename < loaded[j].filename
	})
	if len(loaded) > maximumClickHouseMigrationCount {
		return nil, errors.New("too many ClickHouse migrations")
	}
	for index, migration := range loaded {
		// #nosec G115 -- the migration count is capped at the four-digit filename version space.
		wantVersion := uint32(index + 1)
		if migration.version != wantVersion {
			return nil, fmt.Errorf(
				"ClickHouse migration %q has version %04d, want %04d",
				migration.filename,
				migration.version,
				wantVersion,
			)
		}
	}
	return loaded, nil
}

func validateClickHouseMigrationLedgerInsert(migration clickHouseMigration) error {
	ledgerStatement := -1
	var matches []string
	for index, statement := range migration.statements {
		if !clickHouseMigrationLedgerInsertPrefix.MatchString(statement) {
			continue
		}
		if ledgerStatement >= 0 {
			return fmt.Errorf("ClickHouse migration %q contains more than one migration-ledger insert", migration.filename)
		}
		ledgerStatement = index
		matches = clickHouseMigrationLedgerInsert.FindStringSubmatch(statement)
	}
	if ledgerStatement < 0 {
		return fmt.Errorf("ClickHouse migration %q does not record its migration-ledger row", migration.filename)
	}
	if ledgerStatement != len(migration.statements)-1 {
		return fmt.Errorf("ClickHouse migration %q must record its migration-ledger row in the final statement", migration.filename)
	}
	if matches == nil {
		return fmt.Errorf("ClickHouse migration %q has an invalid migration-ledger insert", migration.filename)
	}
	parsedVersion, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return fmt.Errorf("parse migration-ledger version in %q: %w", migration.filename, err)
	}
	if uint32(parsedVersion) != migration.version || matches[2] != migration.name {
		return fmt.Errorf(
			"ClickHouse migration %q records version %04d name %q, want version %04d name %q",
			migration.filename,
			parsedVersion,
			matches[2],
			migration.version,
			migration.name,
		)
	}
	return nil
}

func readClickHouseMigrationHistory(ctx context.Context, connection ClickHouseMigrationConnection) ([]clickHouseMigrationLedgerRow, error) {
	return readClickHouseMigrationHistoryForDatabase(
		ctx,
		connection,
		recoverycontract.CanonicalDatabase,
		false,
	)
}

type clickHouseMigrationHistoryConnection interface {
	Select(ctx context.Context, dest any, query string, args ...any) error
}

// readClickHouseMigrationHistoryForDatabase is the single migration-history
// reader for both the canonical migrator and recovery aliases. A caller may
// skip the preliminary table enumeration only after the exact physical-schema
// inspector has already proved that the database contains precisely the
// release-owned table set, including schema_migrations.
func readClickHouseMigrationHistoryForDatabase(
	ctx context.Context,
	connection clickHouseMigrationHistoryConnection,
	databaseName string,
	tableSetValidated bool,
) ([]clickHouseMigrationLedgerRow, error) {
	if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
		return nil, fmt.Errorf("read ClickHouse migration history: %w", err)
	}
	tablesQuery := clickHouseMigrationTablesByDatabaseQuery
	ledgerQuery := clickHouseMigrationLedgerQueryForDatabase(databaseName)
	var tableArguments []any
	if databaseName == recoverycontract.CanonicalDatabase {
		tablesQuery = clickHouseMigrationTablesQuery
		ledgerQuery = clickHouseMigrationLedgerQuery
	} else {
		tableArguments = []any{databaseName}
	}

	if !tableSetValidated {
		return readClickHouseMigrationHistoryWithTableProbe(
			ctx,
			connection,
			databaseName,
			tablesQuery,
			tableArguments,
			ledgerQuery,
		)
	}
	return readClickHouseMigrationLedgerRows(ctx, connection, ledgerQuery)
}

func readClickHouseMigrationHistoryWithTableProbe(
	ctx context.Context,
	connection clickHouseMigrationHistoryConnection,
	databaseName string,
	tablesQuery string,
	tableArguments []any,
	ledgerQuery string,
) ([]clickHouseMigrationLedgerRow, error) {
	// clickhouse-go's Select maps each row into a struct, even for a one-column
	// query; using a scalar slice here would fail at runtime.
	var tables []clickHouseMigrationTable
	if err := connection.Select(ctx, &tables, tablesQuery, tableArguments...); err != nil {
		return nil, fmt.Errorf("inspect ClickHouse migration tables: %w", err)
	}
	if len(tables) >= clickHouseMigrationTableResultLimit {
		return nil, fmt.Errorf(
			"%w: %s contains more than the supported maximum of %d release-owned tables",
			ErrClickHouseMigrationDrift,
			databaseName,
			clickHouseReleaseOwnedTableCount,
		)
	}
	ledgerExists := false
	for _, table := range tables {
		if table.Name == "schema_migrations" {
			ledgerExists = true
			break
		}
	}
	if !ledgerExists && len(tables) != 0 {
		names := make([]string, len(tables))
		for index, table := range tables {
			names[index] = table.Name
		}
		return nil, fmt.Errorf(
			"%w: %s contains tables %q but no migration ledger",
			ErrClickHouseMigrationDrift,
			databaseName,
			strings.Join(names, ", "),
		)
	}
	if !ledgerExists {
		return nil, nil
	}

	return readClickHouseMigrationLedgerRows(ctx, connection, ledgerQuery)
}

func readClickHouseMigrationLedgerRows(
	ctx context.Context,
	connection clickHouseMigrationHistoryConnection,
	ledgerQuery string,
) ([]clickHouseMigrationLedgerRow, error) {
	var history []clickHouseMigrationLedgerRow
	if err := connection.Select(ctx, &history, ledgerQuery); err != nil {
		return nil, fmt.Errorf("read ClickHouse migration ledger: %w", err)
	}
	if len(history) >= clickHouseMigrationLedgerResultLimit {
		return nil, fmt.Errorf(
			"%w: migration ledger contains more than the supported maximum of %d distinct version/name identities",
			ErrClickHouseMigrationDrift,
			maximumClickHouseMigrationCount,
		)
	}
	return history, nil
}

func clickHouseMigrationLedgerQueryForDatabase(databaseName string) string {
	return fmt.Sprintf(`
		SELECT version, name, count() AS row_count
		FROM
		(
			SELECT version, name
			FROM %s.schema_migrations
			LIMIT %d
		)
		GROUP BY version, name
		ORDER BY version, name
		LIMIT %d
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_rows_to_group_by = %d,
			group_by_overflow_mode = 'throw',
			max_memory_usage = %d,
			max_result_rows = %d,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		databaseName,
		clickHouseMigrationLedgerResultLimit,
		clickHouseMigrationLedgerResultLimit,
		clickHouseMigrationLedgerResultLimit,
		clickHouseMigrationLedgerReadByteLimit,
		clickHouseMigrationLedgerResultLimit,
		clickHouseMigrationLedgerMaximumMemoryBytes,
		clickHouseMigrationLedgerResultLimit,
		clickHouseMigrationLedgerMaximumResultBytes,
	)
}

func verifyClickHouseMigrationHistory(history []clickHouseMigrationLedgerRow, migrations []clickHouseMigration, requireComplete bool) error {
	if len(migrations) > maximumClickHouseMigrationCount {
		return fmt.Errorf("%w: embedded migration count exceeds the supported version space", ErrClickHouseMigrationDrift)
	}
	// #nosec G115 -- the migration count is capped at the four-digit filename version space.
	latestVersion := uint32(len(migrations))
	for _, row := range history {
		if row.Version > latestVersion {
			return fmt.Errorf(
				"%w: database version %04d, latest embedded version %04d",
				ErrClickHouseDatabaseTooNew,
				row.Version,
				latestVersion,
			)
		}
	}

	for index, row := range history {
		// #nosec G115 -- a matching history cannot exceed the bounded embedded migration prefix.
		wantVersion := uint32(index + 1)
		if row.Version != wantVersion {
			return fmt.Errorf(
				"%w: row %d has version %04d, want %04d",
				ErrClickHouseMigrationDrift,
				index+1,
				row.Version,
				wantVersion,
			)
		}
		embedded := migrations[index]
		if row.Name != embedded.name || row.RowCount != 1 {
			return fmt.Errorf(
				"%w: version %04d has name %q and row count %d, want name %q and row count 1",
				ErrClickHouseMigrationDrift,
				row.Version,
				row.Name,
				row.RowCount,
				embedded.name,
			)
		}
	}

	if requireComplete && len(history) != len(migrations) {
		return fmt.Errorf(
			"%w: migration ledger contains %d versions, want %d",
			ErrClickHouseMigrationDrift,
			len(history),
			len(migrations),
		)
	}
	return nil
}

// splitClickHouseStatements separates a migration into executable statements
// without treating semicolons inside quoted strings, quoted identifiers, or
// comments as delimiters. Comments are removed while preserving whitespace so
// that tokens on either side cannot be joined accidentally.
func splitClickHouseStatements(source string) ([]string, error) {
	const (
		sqlStatePlain = iota
		sqlStateSingleQuote
		sqlStateDoubleQuote
		sqlStateBacktick
		sqlStateDollarQuote
		sqlStateLineComment
		sqlStateBlockComment
	)

	state := sqlStatePlain
	blockDepth := 0
	dollarQuoteDelimiter := ""
	var statement strings.Builder
	statements := make([]string, 0, strings.Count(source, ";")+1)
	flush := func() {
		trimmed := strings.TrimSpace(statement.String())
		if trimmed != "" {
			statements = append(statements, trimmed)
		}
		statement.Reset()
	}

	for index := 0; index < len(source); index++ {
		current := source[index]
		var next byte
		if index+1 < len(source) {
			next = source[index+1]
		}

		switch state {
		case sqlStatePlain:
			switch {
			case current == '-' && next == '-':
				statement.WriteByte(' ')
				state = sqlStateLineComment
				index++
			case current == '#':
				statement.WriteByte(' ')
				state = sqlStateLineComment
			case current == '/' && next == '*':
				statement.WriteByte(' ')
				state = sqlStateBlockComment
				blockDepth = 1
				index++
			case current == '\'':
				statement.WriteByte(current)
				state = sqlStateSingleQuote
			case current == '"':
				statement.WriteByte(current)
				state = sqlStateDoubleQuote
			case current == '`':
				statement.WriteByte(current)
				state = sqlStateBacktick
			case current == '$':
				if delimiter, ok := clickHouseDollarQuoteDelimiter(source[index:]); ok {
					statement.WriteString(delimiter)
					dollarQuoteDelimiter = delimiter
					state = sqlStateDollarQuote
					index += len(delimiter) - 1
				} else {
					statement.WriteByte(current)
				}
			case current == ';':
				flush()
			default:
				statement.WriteByte(current)
			}

		case sqlStateSingleQuote, sqlStateDoubleQuote, sqlStateBacktick:
			statement.WriteByte(current)
			var delimiter byte
			switch state {
			case sqlStateSingleQuote:
				delimiter = '\''
			case sqlStateDoubleQuote:
				delimiter = '"'
			case sqlStateBacktick:
				delimiter = '`'
			}
			if current == '\\' && index+1 < len(source) {
				index++
				statement.WriteByte(source[index])
				continue
			}
			if current != delimiter {
				continue
			}
			if next == delimiter {
				index++
				statement.WriteByte(next)
				continue
			}
			state = sqlStatePlain

		case sqlStateDollarQuote:
			if strings.HasPrefix(source[index:], dollarQuoteDelimiter) {
				statement.WriteString(dollarQuoteDelimiter)
				index += len(dollarQuoteDelimiter) - 1
				dollarQuoteDelimiter = ""
				state = sqlStatePlain
			} else {
				statement.WriteByte(current)
			}

		case sqlStateLineComment:
			if current == '\n' || current == '\r' {
				statement.WriteByte(current)
				state = sqlStatePlain
			}

		case sqlStateBlockComment:
			switch {
			case current == '/' && next == '*':
				blockDepth++
				index++
			case current == '*' && next == '/':
				blockDepth--
				index++
				if blockDepth == 0 {
					state = sqlStatePlain
				}
			}
		}
	}

	switch state {
	case sqlStatePlain, sqlStateLineComment:
		flush()
	case sqlStateSingleQuote:
		return nil, errors.New("unterminated single-quoted string")
	case sqlStateDoubleQuote:
		return nil, errors.New("unterminated double-quoted identifier")
	case sqlStateBacktick:
		return nil, errors.New("unterminated backtick-quoted identifier")
	case sqlStateDollarQuote:
		return nil, fmt.Errorf("unterminated dollar-quoted string %q", dollarQuoteDelimiter)
	case sqlStateBlockComment:
		return nil, errors.New("unterminated block comment")
	}
	return statements, nil
}

func clickHouseDollarQuoteDelimiter(source string) (string, bool) {
	if len(source) < 2 || source[0] != '$' {
		return "", false
	}
	closing := strings.IndexByte(source[1:], '$')
	if closing < 0 {
		return "", false
	}
	closing++
	for _, character := range source[1:closing] {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '_' {
			return "", false
		}
	}
	return source[:closing+1], true
}
