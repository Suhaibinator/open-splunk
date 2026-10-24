package control_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	_ "modernc.org/sqlite"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// These tests try to break the text semantics of the PostgreSQL control
// plane: byte versus character bounds at multibyte UTF-8 boundaries,
// COLLATE "C" versus SQLite BINARY ordering, NUL and invalid UTF-8, case and
// whitespace normalization, empty strings versus NULL, btree key limits, and
// database or role defaults that change how text and string literals are
// read. The corpus in testdata/postgres_break_semantics_text holds the
// boundary strings evaluated in both engines.

const semTextBtreeMaximumTupleBytes = 2704

type semTextExpression struct {
	name     string
	sqlite   string
	postgres string
	boolean  bool
}

// semTextExpressions pairs each SQLite expression of the final SQLite schema
// with its PostgreSQL translation (migrations/postgres/README.md). Both sides
// read a column x: SQLite TEXT COLLATE BINARY, PostgreSQL text COLLATE "C".
var semTextExpressions = []semTextExpression{
	{name: "character length", sqlite: `length(x)`, postgres: `char_length(x)`},
	{name: "byte length", sqlite: `length(CAST(x AS BLOB))`, postgres: `octet_length(x)`},
	{name: "lower", sqlite: `lower(x)`, postgres: `lower(x)`},
	{name: "upper", sqlite: `upper(x)`, postgres: `upper(x)`},
	{name: "trim", sqlite: `trim(x)`, postgres: `trim(x)`},
	{name: "first character", sqlite: `substr(x, 1, 1)`, postgres: `substr(x, 1, 1)`},
	{name: "from the fifth character", sqlite: `substr(x, 5)`, postgres: `substr(x, 5)`},
	{name: "four characters from the second", sqlite: `substr(x, 2, 4)`, postgres: `substr(x, 2, 4)`},
	{name: "hyphens removed", sqlite: `replace(x, '-', '')`, postgres: `replace(x, '-', '')`},
	{name: "kvstore position", sqlite: `instr(x, 'kvstore')`, postgres: `strpos(x, 'kvstore')`},
	{name: "is trimmed", sqlite: `x = trim(x)`, postgres: `x = trim(x)`, boolean: true},
	{name: "is lower case", sqlite: `x = lower(x)`, postgres: `x = lower(x)`, boolean: true},
	{
		name:     "slug first character",
		sqlite:   `substr(x, 1, 1) GLOB '[a-z0-9]'`,
		postgres: `substr(x, 1, 1) ~ '^[a-z0-9]$'`,
		boolean:  true,
	},
	{
		name:     "slug characters",
		sqlite:   `x NOT GLOB '*[^a-z0-9_-]*'`,
		postgres: `x !~ '[^a-z0-9_-]'`,
		boolean:  true,
	},
	{
		name:     "app id characters",
		sqlite:   `x NOT GLOB '*[^A-Za-z0-9_-]*'`,
		postgres: `x !~ '[^A-Za-z0-9_-]'`,
		boolean:  true,
	},
	{
		name:     "app id final character",
		sqlite:   `substr(x, 26, 1) GLOB '[AQgw]'`,
		postgres: `substr(x, 26, 1) ~ '^[AQgw]$'`,
		boolean:  true,
	},
	{
		name:     "hex characters",
		sqlite:   `x NOT GLOB '*[^0-9a-f]*'`,
		postgres: `x !~ '[^0-9a-f]'`,
		boolean:  true,
	},
	{
		name: "no C0 or C1 control",
		sqlite: `x NOT GLOB ('*[' || char(1) || '-' || char(31)
			|| char(127) || '-' || char(159) || ']*')`,
		postgres: `x !~ '[\u0001-\u001f\u007f-\u009f]'`,
		boolean:  true,
	},
	{
		name: "no control except tab, line feed and carriage return",
		sqlite: `x NOT GLOB ('*[' || char(1) || '-' || char(8)
			|| char(11) || '-' || char(12) || char(14) || '-' || char(31)
			|| char(127) || '-' || char(159) || ']*')`,
		postgres: `x !~ '[\u0001-\u0008\u000b-\u000c\u000e-\u001f\u007f-\u009f]'`,
		boolean:  true,
	},
	{name: "no backslash", sqlite: `x NOT LIKE '%\%'`, postgres: `strpos(x, '\') = 0`, boolean: true},
	{name: "no double backslash", sqlite: `x NOT LIKE '%\\%'`, postgres: `strpos(x, '\\') = 0`, boolean: true},
	{name: "no slash", sqlite: `x NOT LIKE '%/%'`, postgres: `x NOT LIKE '%/%'`, boolean: true},
	{name: "before e acute", sqlite: "x < '\u00e9'", postgres: "x < '\u00e9'", boolean: true},
	{name: "after Z", sqlite: `x > 'Z'`, postgres: `x > 'Z'`, boolean: true},
	{name: "at most B", sqlite: `x <= 'B'`, postgres: `x <= 'B'`, boolean: true},
}

// semTextProbe is everything one engine reports about the corpus.
type semTextProbe struct {
	expressions map[string][]string
	ascending   []int64
	descending  []int64
	greater     []int64
	minimum     string
	maximum     string
}

func loadSemTextCorpus(t *testing.T) []string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "postgres_break_semantics_text", "corpus.json"))
	if err != nil {
		t.Fatalf("read text corpus: %v", err)
	}
	var corpus struct {
		Values []struct {
			Why   string `json:"why"`
			Value string `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(contents, &corpus); err != nil {
		t.Fatalf("decode text corpus: %v", err)
	}
	seen := make(map[string]struct{}, len(corpus.Values))
	values := make([]string, 0, len(corpus.Values))
	for _, entry := range corpus.Values {
		if entry.Why == "" || !utf8.ValidString(entry.Value) || strings.IndexByte(entry.Value, 0) >= 0 {
			t.Fatalf("corpus entry %q must explain itself and be valid UTF-8 without NUL", entry.Value)
		}
		if _, duplicate := seen[entry.Value]; duplicate {
			t.Fatalf("corpus value %q appears twice", entry.Value)
		}
		seen[entry.Value] = struct{}{}
		values = append(values, entry.Value)
	}
	if len(values) < 60 {
		t.Fatalf("corpus holds %d values; it must cover every boundary", len(values))
	}
	return values
}

// semTextGoOrder is the byte-order answer Go itself gives, which is what
// cursor and sort code in the stores assumes.
func semTextGoOrder(corpus []string, descending bool) []int64 {
	order := make([]int64, len(corpus))
	for index := range corpus {
		order[index] = int64(index + 1)
	}
	sort.SliceStable(order, func(left, right int) bool {
		a, b := corpus[order[left]-1], corpus[order[right]-1]
		if descending {
			return a > b
		}
		return a < b
	})
	return order
}

func semTextGoGreater(corpus []string) []int64 {
	counts := make([]int64, len(corpus))
	for index, value := range corpus {
		for _, other := range corpus {
			if other > value {
				counts[index]++
			}
		}
	}
	return counts
}

func semTextExpressionSQL(expression string, boolean bool) string {
	if boolean {
		return `CASE WHEN (` + expression + `) THEN '1' ELSE '0' END`
	}
	return `CAST((` + expression + `) AS TEXT)`
}

func semTextQueryStrings(ctx context.Context, t *testing.T, query func(context.Context, string, ...any) (*sql.Rows, error), statement string, args ...any) []string {
	t.Helper()
	rows, err := query(ctx, statement, args...)
	if err != nil {
		t.Fatalf("query %q: %v", statement, err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value sql.NullString
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan %q: %v", statement, err)
		}
		if !value.Valid {
			t.Fatalf("query %q returned NULL for a non-NULL corpus value", statement)
		}
		values = append(values, value.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %q: %v", statement, err)
	}
	return values
}

func semTextQueryInts(ctx context.Context, t *testing.T, query func(context.Context, string, ...any) (*sql.Rows, error), statement string) []int64 {
	t.Helper()
	rows, err := query(ctx, statement)
	if err != nil {
		t.Fatalf("query %q: %v", statement, err)
	}
	defer rows.Close()
	var values []int64
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan %q: %v", statement, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %q: %v", statement, err)
	}
	return values
}

func semTextProbeSQLite(t *testing.T, corpus []string) semTextProbe {
	t.Helper()
	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite: %v", err)
		}
	})
	// Every connection to :memory: is its own database.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE semtext_probe (id INTEGER PRIMARY KEY, x TEXT COLLATE BINARY NOT NULL) STRICT`,
	); err != nil {
		t.Fatalf("create SQLite probe table: %v", err)
	}
	for index, value := range corpus {
		if _, err := db.ExecContext(ctx, `INSERT INTO semtext_probe (id, x) VALUES (?, ?)`, index+1, value); err != nil {
			t.Fatalf("insert SQLite corpus value %q: %v", value, err)
		}
	}
	probe := semTextProbe{expressions: make(map[string][]string, len(semTextExpressions))}
	for _, expression := range semTextExpressions {
		probe.expressions[expression.name] = semTextQueryStrings(ctx, t, db.QueryContext,
			`SELECT `+semTextExpressionSQL(expression.sqlite, expression.boolean)+` FROM semtext_probe ORDER BY id`)
	}
	probe.ascending = semTextQueryInts(ctx, t, db.QueryContext, `SELECT id FROM semtext_probe ORDER BY x, id`)
	probe.descending = semTextQueryInts(ctx, t, db.QueryContext, `SELECT id FROM semtext_probe ORDER BY x DESC, id`)
	probe.greater = make([]int64, len(corpus))
	for index, value := range corpus {
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM semtext_probe WHERE x > ?`, value).
			Scan(&probe.greater[index]); err != nil {
			t.Fatalf("SQLite keyset count after %q: %v", value, err)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT min(x), max(x) FROM semtext_probe`).
		Scan(&probe.minimum, &probe.maximum); err != nil {
		t.Fatalf("SQLite min/max: %v", err)
	}
	return probe
}

// semTextProbePostgres evaluates the corpus in a temporary table whose column
// is declared exactly like every control-plane text column.
func semTextProbePostgres(t *testing.T, db *sql.DB, corpus []string) semTextProbe {
	t.Helper()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin PostgreSQL probe: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back PostgreSQL probe: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx,
		`CREATE TEMP TABLE semtext_probe (id bigint PRIMARY KEY, x text COLLATE "C" NOT NULL) ON COMMIT DROP`,
	); err != nil {
		t.Fatalf("create PostgreSQL probe table: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO semtext_probe (id, x) SELECT ord, v FROM unnest($1::text[]) WITH ORDINALITY AS u(v, ord)`,
		corpus,
	); err != nil {
		t.Fatalf("insert PostgreSQL corpus: %v", err)
	}
	probe := semTextProbe{expressions: make(map[string][]string, len(semTextExpressions))}
	for _, expression := range semTextExpressions {
		probe.expressions[expression.name] = semTextQueryStrings(ctx, t, tx.QueryContext,
			`SELECT `+semTextExpressionSQL(expression.postgres, expression.boolean)+` FROM semtext_probe ORDER BY id`)
	}
	probe.ascending = semTextQueryInts(ctx, t, tx.QueryContext, `SELECT id FROM semtext_probe ORDER BY x, id`)
	probe.descending = semTextQueryInts(ctx, t, tx.QueryContext, `SELECT id FROM semtext_probe ORDER BY x DESC, id`)
	// A keyset cursor compares a C column with an untyped bind parameter,
	// whose own collation is the database default. The column must win.
	probe.greater = make([]int64, len(corpus))
	for index, value := range corpus {
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM semtext_probe WHERE x > $1`, value).
			Scan(&probe.greater[index]); err != nil {
			t.Fatalf("PostgreSQL keyset count after %q: %v", value, err)
		}
	}
	if err := tx.QueryRowContext(ctx, `SELECT min(x), max(x) FROM semtext_probe`).
		Scan(&probe.minimum, &probe.maximum); err != nil {
		t.Fatalf("PostgreSQL min/max: %v", err)
	}
	return probe
}

func semTextCompareIDs(t *testing.T, label string, corpus []string, want, got []int64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: %d rows, want %d", label, len(got), len(want))
	}
	for index := range want {
		if want[index] != got[index] {
			t.Fatalf("%s: position %d holds %q, want %q", label, index, corpus[got[index]-1], corpus[want[index]-1])
		}
	}
}

func semTextCompareProbes(t *testing.T, label string, corpus []string, want, got semTextProbe) {
	t.Helper()
	for _, expression := range semTextExpressions {
		wantValues, gotValues := want.expressions[expression.name], got.expressions[expression.name]
		if len(wantValues) != len(corpus) || len(gotValues) != len(corpus) {
			t.Fatalf("%s %s: %d and %d results for %d values", label, expression.name, len(wantValues), len(gotValues), len(corpus))
		}
		for index := range corpus {
			if wantValues[index] != gotValues[index] {
				t.Errorf("%s %s(%q) = %q, want %q", label, expression.name, corpus[index], gotValues[index], wantValues[index])
			}
		}
	}
	semTextCompareIDs(t, label+" ascending order", corpus, want.ascending, got.ascending)
	semTextCompareIDs(t, label+" descending order", corpus, want.descending, got.descending)
	for index := range corpus {
		if want.greater[index] != got.greater[index] {
			t.Errorf("%s: %d values sort after %q, want %d", label, got.greater[index], corpus[index], want.greater[index])
		}
	}
	if want.minimum != got.minimum || want.maximum != got.maximum {
		t.Errorf("%s min/max = %q/%q, want %q/%q", label, got.minimum, got.maximum, want.minimum, want.maximum)
	}
}

// semTextCheckSanity pins a few answers outright, so the differential cannot
// pass by both engines agreeing on something unexpected.
func semTextCheckSanity(t *testing.T, corpus []string, probe semTextProbe) {
	t.Helper()
	position := make(map[string]int, len(corpus))
	for index, value := range corpus {
		position[value] = index
	}
	expect := func(expression, value, want string) {
		t.Helper()
		index, ok := position[value]
		if !ok {
			t.Fatalf("corpus lacks %q", value)
		}
		if got := probe.expressions[expression][index]; got != want {
			t.Errorf("%s(%q) = %q, want %q", expression, value, got, want)
		}
	}
	expect("lower", "\u00c9cole", "\u00c9cole")
	expect("lower", "I", "i")
	expect("upper", "i", "I")
	expect("upper", "\u00df", "\u00df")
	expect("lower", "\u0130", "\u0130")
	expect("character length", "e\u0301", "2")
	expect("byte length", "\U0001F600", "4")
	expect("character length", "\U0001F468\u200d\U0001F469\u200d\U0001F467", "5")
	expect("trim", "\u00a0a", "\u00a0a")
	expect("is trimmed", "\ta", "1")
	expect("no C0 or C1 control", "\u009f", "0")
	expect("no C0 or C1 control", "\u00a0", "1")
	expect("no C0 or C1 control", "a\u0085", "0")
	expect("no control except tab, line feed and carriage return", "a\tb", "1")
	expect("no control except tab, line feed and carriage return", "a\u000bb", "0")
	expect("kvstore position", "\u00e9\u00e9kvstore", "3")
	expect("no backslash", "a\\b", "0")
	expect("no double backslash", "a\\b", "1")
	expect("no double backslash", "a\\\\b", "0")
	expect("slug characters", "\u212a", "0")
	expect("at most B", "a", "0")

	first := func(value string) int {
		for rank, id := range probe.ascending {
			if corpus[id-1] == value {
				return rank
			}
		}
		t.Fatalf("ordering lacks %q", value)
		return -1
	}
	for _, pair := range [][2]string{
		{"B", "a"},
		{"Z", "_"},
		{"_", "a"},
		{"ab", "abc"},
		{"abd", "ab\u00e9"},
		{"e\u0301", "\u00e9"},
		{"\uffff", "\U0001F600"},
		{"\U0001F600", "\U0010FFFF"},
	} {
		if first(pair[0]) >= first(pair[1]) {
			t.Errorf("byte order puts %q after %q", pair[0], pair[1])
		}
	}
	if probe.minimum != "" || probe.maximum != "\U0010FFFF" {
		t.Errorf("min/max = %q/%q, want the empty string and U+10FFFF", probe.minimum, probe.maximum)
	}
}

func TestPostgresBreakTextTranslationsMatchSQLiteBinaryAndGoByteOrder(t *testing.T) {
	t.Parallel()
	corpus := loadSemTextCorpus(t)
	db := testsupport.NewPostgresControlDB(t)

	sqlite := semTextProbeSQLite(t, corpus)
	postgres := semTextProbePostgres(t, db.SQLDB(), corpus)
	semTextCompareProbes(t, "PostgreSQL versus SQLite", corpus, sqlite, postgres)
	semTextCheckSanity(t, corpus, postgres)

	semTextCompareIDs(t, "Go byte order", corpus, semTextGoOrder(corpus, false), postgres.ascending)
	semTextCompareIDs(t, "Go reverse byte order", corpus, semTextGoOrder(corpus, true), postgres.descending)
	goGreater := semTextGoGreater(corpus)
	for index := range corpus {
		if goGreater[index] != postgres.greater[index] {
			t.Errorf("PostgreSQL keyset after %q counts %d, Go counts %d", corpus[index], postgres.greater[index], goGreater[index])
		}
	}
}

// TestPostgresBreakTextSchemaIgnoresTheDatabaseDefaultCollation builds the
// control schema in a database whose default collation is ICU Turkish, where
// lower('I') is a dotless i and 'a' sorts before 'B'. Every comparison,
// case mapping and index order must still be byte-wise, because every text
// column, index key and expression is COLLATE "C".
func TestPostgresBreakTextSchemaIgnoresTheDatabaseDefaultCollation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	corpus := loadSemTextCorpus(t)
	adminDSN := testsupport.PostgresAdminDSN(t)
	admin := openRawPostgres(t, adminDSN)

	var icuCollations int64
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_catalog.pg_collation WHERE collprovider = 'i'`,
	).Scan(&icuCollations); err != nil {
		t.Fatalf("inspect ICU support: %v", err)
	}
	if icuCollations == 0 {
		t.Skip("the PostgreSQL server was built without ICU")
	}
	name := semTextDatabaseName(t)
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(),
			`DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`,
		); err != nil {
			t.Errorf("drop Turkish-collation database: %v", err)
		}
	})
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()+
		` TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'tr-TR' LOCALE 'tr_TR.utf8'`,
	); err != nil {
		// Not every libc ships tr_TR; the ICU locale is what matters.
		if _, retryErr := admin.ExecContext(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()+
			` TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'tr-TR' LOCALE 'C.UTF-8'`,
		); retryErr != nil {
			t.Fatalf("create Turkish-collation database: %v; %v", err, retryErr)
		}
	}
	db, err := control.OpenPostgres(ctx, semTextDSNForDatabase(t, adminDSN, name))
	if err != nil {
		t.Fatalf("OpenPostgres() on a Turkish-collation database = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close Turkish-collation database: %v", err)
		}
	})

	// The hostile default is real: untyped literals use it.
	var defaultLower string
	var defaultOrder bool
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT lower('I'), 'a' < 'B'`).Scan(&defaultLower, &defaultOrder); err != nil {
		t.Fatalf("read default-collation behavior: %v", err)
	}
	if defaultLower != "\u0131" || !defaultOrder {
		t.Fatalf("default collation lower('I') = %q and 'a' < 'B' = %v; the database is not Turkish ICU", defaultLower, defaultOrder)
	}

	sqlite := semTextProbeSQLite(t, corpus)
	turkish := semTextProbePostgres(t, db.SQLDB(), corpus)
	semTextCompareProbes(t, "Turkish-collation PostgreSQL versus SQLite", corpus, sqlite, turkish)
	semTextCheckSanity(t, corpus, turkish)

	// The real tables too: tenant_id orders by bytes and lowers by ASCII
	// rules, whatever the database default says.
	if err := semTextInsertTenant(ctx, db.SQLDB(), "tenant-tr"); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	var tenantIDs []string
	for _, tenantID := range []string{"I", "i", "\u0130", "\u0131", "a", "B"} {
		if err := semTextInsertTenant(ctx, db.SQLDB(), tenantID); err != nil {
			t.Fatalf("insert tenant %q: %v", tenantID, err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	ordered := semTextQueryStrings(ctx, t, db.SQLDB().QueryContext,
		`SELECT tenant_id FROM tenants WHERE tenant_id <> 'tenant-tr' ORDER BY tenant_id`)
	sort.Strings(tenantIDs)
	if strings.Join(ordered, "|") != strings.Join(tenantIDs, "|") {
		t.Fatalf("tenants ORDER BY tenant_id = %q, want byte order %q", ordered, tenantIDs)
	}
	var lowered string
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT lower(tenant_id) FROM tenants WHERE tenant_id = 'I'`).Scan(&lowered); err != nil || lowered != "i" {
		t.Fatalf("lower() of a C column = %q, %v; want the ASCII mapping", lowered, err)
	}
}

func semTextDatabaseName(t *testing.T) string {
	t.Helper()
	suffix := make([]byte, 8)
	for index := range suffix {
		suffix[index] = byte(rand.IntN(256))
	}
	return "ostest_semtext_" + hex.EncodeToString(suffix)
}

func semTextDSNForDatabase(t *testing.T, adminDSN, database string) string {
	t.Helper()
	if strings.HasPrefix(adminDSN, "postgres://") || strings.HasPrefix(adminDSN, "postgresql://") {
		parsed, err := url.Parse(adminDSN)
		if err != nil {
			t.Fatalf("administrator connection string is not a valid URL")
		}
		parsed.Path = "/" + database
		parsed.RawPath = ""
		return parsed.String()
	}
	return adminDSN + " dbname=" + database
}

func semTextWithParameters(dsn string, parameters map[string]string) string {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return dsn
		}
		query := parsed.Query()
		for _, key := range keys {
			query.Set(key, parameters[key])
		}
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	for _, key := range keys {
		dsn += " " + key + "=" + parameters[key]
	}
	return dsn
}

func semTextInsertTenant(ctx context.Context, db *sql.DB, tenantID string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ($1, 1, 1)`,
		tenantID,
	)
	return err
}

func semTextRequireCheck(t *testing.T, label string, err error, constraint string) {
	t.Helper()
	if !control.IsCheckViolation(err) {
		code, _ := control.PostgresErrorCode(err)
		t.Fatalf("%s: error = %v (SQLSTATE %q), want check violation %s", label, err, code, constraint)
	}
	if name, _ := control.PostgresConstraintName(err); name != constraint {
		t.Fatalf("%s: violated %q, want %q", label, name, constraint)
	}
}

// TestPostgresBreakTextByteBoundsCountUTF8Bytes drives tenants_tenant_id_bounded
// (1..255 bytes) with 1-, 2-, 3- and 4-byte characters at and across the
// boundary, including a multibyte character that straddles byte 255.
func TestPostgresBreakTextByteBoundsCountUTF8Bytes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	cases := []struct {
		name   string
		value  string
		accept bool
	}{
		{"255 ASCII bytes", strings.Repeat("a", 255), true},
		{"256 ASCII bytes", strings.Repeat("b", 256), false},
		{"127 two-byte characters and one ASCII (255 bytes)", strings.Repeat("\u00e9", 127) + "a", true},
		{"128 two-byte characters (256 bytes, 128 characters)", strings.Repeat("\u00e9", 128), false},
		{"85 three-byte characters (255 bytes)", strings.Repeat("\u20ac", 85), true},
		{"85 three-byte characters and one ASCII (256 bytes)", strings.Repeat("\u20ac", 85) + "a", false},
		{"63 four-byte characters and three ASCII (255 bytes)", strings.Repeat("\U0001F600", 63) + "abc", true},
		{"64 four-byte characters (256 bytes, 64 characters)", strings.Repeat("\U0001F600", 64), false},
		{"three-byte character ending at byte 255", strings.Repeat("c", 252) + "\u20ac", true},
		{"three-byte character straddling byte 255", strings.Repeat("d", 253) + "\u20ac", false},
		{"four-byte character straddling byte 255", strings.Repeat("e", 254) + "\U00020000", false},
		{"combining sequence ending at byte 255", strings.Repeat("f", 252) + "e\u0301", true},
		{"one byte", "g", true},
		{"empty", "", false},
	}
	for _, tc := range cases {
		err := semTextInsertTenant(ctx, db, tc.value)
		if !tc.accept {
			semTextRequireCheck(t, tc.name, err, "tenants_tenant_id_bounded")
			continue
		}
		if err != nil {
			t.Fatalf("%s: insert = %v", tc.name, err)
		}
		var octets, characters int
		var stored string
		if err := db.QueryRowContext(ctx,
			`SELECT octet_length(tenant_id), char_length(tenant_id), tenant_id FROM tenants WHERE tenant_id = $1`,
			tc.value,
		).Scan(&octets, &characters, &stored); err != nil {
			t.Fatalf("%s: read back = %v", tc.name, err)
		}
		if octets != len(tc.value) || characters != utf8.RuneCountInString(tc.value) || stored != tc.value {
			t.Fatalf("%s: stored %d bytes and %d characters (equal=%v), want %d and %d",
				tc.name, octets, characters, stored == tc.value, len(tc.value), utf8.RuneCountInString(tc.value))
		}
	}
}

// semTextShadow copies a control table's columns, defaults, generated
// columns, NOT NULL and CHECK constraints and indexes, but none of its
// triggers or foreign keys, so a test reaches exactly the column rules.
func semTextShadow(ctx context.Context, t *testing.T, tx *sql.Tx, table string) string {
	t.Helper()
	shadow := "semtext_" + table
	if _, err := tx.ExecContext(ctx,
		`CREATE TEMP TABLE `+shadow+` (LIKE `+table+` INCLUDING ALL) ON COMMIT DROP`,
	); err != nil {
		t.Fatalf("shadow %s: %v", table, err)
	}
	return shadow
}

// semTextTry runs one statement under a savepoint, so a refused row leaves the
// surrounding transaction usable.
func semTextTry(ctx context.Context, t *testing.T, tx *sql.Tx, statement string, args ...any) error {
	t.Helper()
	if _, err := tx.ExecContext(ctx, `SAVEPOINT semtext_try`); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	_, execErr := tx.ExecContext(ctx, statement, args...)
	release := `RELEASE SAVEPOINT semtext_try`
	if execErr != nil {
		release = `ROLLBACK TO SAVEPOINT semtext_try`
	}
	if _, err := tx.ExecContext(ctx, release); err != nil {
		t.Fatalf("end savepoint after %v: %v", execErr, err)
	}
	return execErr
}

func semTextBegin(ctx context.Context, t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	// The transaction outlives t.Context(), which ends before cleanups run.
	tx, err := db.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back: %v", err)
		}
	})
	return tx
}

// TestPostgresBreakTextCharacterBoundsCountCodePoints drives the
// char_length() bounds SQLite length() became: code points, not bytes and
// not graphemes. dashboards.name admits 1..255 code points.
func TestPostgresBreakTextCharacterBoundsCountCodePoints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	tx := semTextBegin(ctx, t, db)
	shadow := semTextShadow(ctx, t, tx, "dashboards")
	insert := `INSERT INTO ` + shadow + ` (dashboard_id, version, name, app_id, tenant_id, owner_id,
		sharing_scope, definition_proto, created_at_unix_micro, updated_at_unix_micro)
		VALUES ($1, 1, $2, 'search', 'tenant', 'owner', 1, '\x0a'::bytea, 1, 1)`
	cases := []struct {
		name   string
		value  string
		accept bool
	}{
		{"255 ASCII", strings.Repeat("a", 255), true},
		{"256 ASCII", strings.Repeat("a", 256), false},
		{"255 two-byte characters (510 bytes)", strings.Repeat("\u00e9", 255), true},
		{"255 four-byte characters (1020 bytes)", strings.Repeat("\U0001F600", 255), true},
		{"256 four-byte characters", strings.Repeat("\U0001F600", 256), false},
		{"127 decomposed accents and one ASCII (255 code points)", strings.Repeat("e\u0301", 127) + "a", true},
		{"128 decomposed accents (256 code points, 128 graphemes)", strings.Repeat("e\u0301", 128), false},
		{"51 five-code-point emoji (255 code points, 51 graphemes)", strings.Repeat("\U0001F468\u200d\U0001F469\u200d\U0001F467", 51), true},
		{"empty", "", false},
	}
	for index, tc := range cases {
		err := semTextTry(ctx, t, tx, insert, fmt.Sprintf("dashboard-%d", index), tc.value)
		if !tc.accept {
			semTextRequireCheck(t, tc.name, err, "dashboards_name_check")
			continue
		}
		if err != nil {
			t.Fatalf("%s: insert = %v", tc.name, err)
		}
	}
}

// TestPostgresBreakTextWhitespaceAndControlBoundaries drives the trimmed,
// control-free identity rule (tenants_tenant_id_bounded and its siblings).
// SQL trim() strips only U+0020, exactly like SQLite; other whitespace is
// either a refused control or an accepted ordinary character.
func TestPostgresBreakTextWhitespaceAndControlBoundaries(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	cases := []struct {
		name   string
		value  string
		accept bool
	}{
		{"inner space", "a b", true},
		{"leading space", " a", false},
		{"trailing space", "a ", false},
		{"only spaces", "   ", false},
		{"leading tab", "\ta", false},
		{"trailing line feed", "a\n", false},
		{"U+0001", "a\u0001", false},
		{"U+001F", "a\u001f", false},
		{"U+007E tilde", "a~", true},
		{"U+007F DEL", "a\u007f", false},
		{"U+0080", "a\u0080", false},
		{"U+0085 next line", "a\u0085", false},
		{"U+009F", "a\u009f", false},
		{"U+00A0 alone", "\u00a0", true},
		{"leading U+00A0", "\u00a0a", true},
		{"trailing U+00A0", "a\u00a0", true},
		{"U+1680 ogham space", "\u1680a", true},
		{"U+2028 line separator", "a\u2028", true},
		{"U+3000 ideographic space", "\u3000a", true},
		{"U+FEFF byte order mark", "\ufeffa", true},
		{"U+200B zero width space", "a\u200b", true},
		{"U+FFFD", "\ufffd", true},
		{"upper case", "Tenant", true},
		{"lower case twin", "tenant", true},
	}
	for _, tc := range cases {
		err := semTextInsertTenant(ctx, db, tc.value)
		if !tc.accept {
			semTextRequireCheck(t, tc.name, err, "tenants_tenant_id_bounded")
			continue
		}
		if err != nil {
			t.Fatalf("%s: insert = %v", tc.name, err)
		}
	}
	// Case is significant: 'Tenant' and 'tenant' are two tenants, and lower()
	// of a C column leaves non-ASCII letters alone, as SQLite does.
	var count int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE lower(tenant_id) = 'tenant'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("case-folded tenant count = %d, %v; want both spellings", count, err)
	}
}

// TestPostgresBreakTextGoIdentityValidationNeverOutrunsTheSchema evaluates the
// schema's own identity CHECKs against every corpus value and the Go
// validator the stores run first. A value Go admits but PostgreSQL refuses
// would surface as an unmapped database error instead of invalid input.
func TestPostgresBreakTextGoIdentityValidationNeverOutrunsTheSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	corpus := loadSemTextCorpus(t)
	corpus = append(corpus,
		strings.Repeat("\u20ac", 42)+"ab", // 128 bytes
		strings.Repeat("\u20ac", 43),      // 129 bytes
		strings.Repeat("\U0001F600", 32),  // 128 bytes
		"\u000b", "\u000c", "a\u000c",
	)
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	for _, check := range []struct {
		table, constraint, column string
		maximumBytes              int
	}{
		{"knowledge_objects", "knowledge_objects_id_bounded", "knowledge_object_id", 128},
		{"tenants", "tenants_tenant_id_bounded", "tenant_id", 255},
	} {
		var expression string
		if err := db.QueryRowContext(ctx, `
			SELECT pg_get_expr(con.conbin, con.conrelid)
			FROM pg_constraint AS con
			JOIN pg_class AS c ON c.oid = con.conrelid
			WHERE c.relnamespace = 'open_splunk'::regnamespace AND c.relname = $1 AND con.conname = $2`,
			check.table, check.constraint,
		).Scan(&expression); err != nil {
			t.Fatalf("read %s: %v", check.constraint, err)
		}
		results := semTextQueryStrings(ctx, t, db.QueryContext, `
			SELECT CASE WHEN (`+expression+`) THEN '1' ELSE '0' END
			FROM (SELECT v COLLATE "C" AS `+check.column+`, ord
			      FROM unnest($1::text[]) WITH ORDINALITY AS u(v, ord)) AS probe
			ORDER BY ord`, corpus)
		for index, value := range corpus {
			if knowledge.ValidIdentity(value, check.maximumBytes) && results[index] != "1" {
				t.Errorf("Go admits %q but %s refuses it", value, check.constraint)
			}
		}
	}
}

// TestPostgresBreakTextNULAndInvalidUTF8AreRefusedBeforeAnyConstraint pins
// SQLSTATE 22021 for every text value SQLite stored but PostgreSQL cannot
// hold, on writes and on reads, and its classifier.
func TestPostgresBreakTextNULAndInvalidUTF8AreRefusedBeforeAnyConstraint(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	if err := semTextInsertTenant(ctx, db, "tenant"); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	for _, tc := range []struct{ name, value string }{
		{"NUL", "a\x00b"},
		{"leading NUL", "\x00"},
		{"lone continuation byte", "a\x80"},
		{"invalid byte", "\xff"},
		{"overlong slash", "\xc0\xaf"},
		{"UTF-16 surrogate", "\xed\xa0\x80"},
		{"beyond U+10FFFF", "\xf4\x90\x80\x80"},
		{"truncated sequence", "\xe2\x82"},
	} {
		insertErr := semTextInsertTenant(ctx, db, tc.value)
		var count int64
		readErr := db.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE tenant_id = $1`, tc.value).Scan(&count)
		for label, err := range map[string]error{"insert": insertErr, "lookup": readErr} {
			code, _ := control.PostgresErrorCode(err)
			if code != "22021" || !control.IsInvalidTextEncoding(err) {
				t.Fatalf("%s %s: error = %v (SQLSTATE %q), want 22021", tc.name, label, err, code)
			}
			if control.IsCheckViolation(err) || control.IsUniqueViolation(err) || control.IsRaise(err) || control.IsRetryable(err) {
				t.Fatalf("%s %s: 22021 classified as another failure", tc.name, label)
			}
		}
	}
	// A duplicate key is a different failure.
	for _, err := range []error{nil, errors.New("plain"), semTextInsertTenant(ctx, db, "tenant")} {
		if control.IsInvalidTextEncoding(err) {
			t.Fatalf("IsInvalidTextEncoding(%v) = true", err)
		}
	}

	// SQLite stored the same NUL-bearing text, which is why its schema
	// carried instr(..., X'00') = 0 guards that the port drops.
	lite, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	defer func() {
		if err := lite.Close(); err != nil {
			t.Errorf("close SQLite: %v", err)
		}
	}()
	lite.SetMaxOpenConns(1)
	var storedBytes int64
	if err := lite.QueryRowContext(ctx, `SELECT length(CAST(? AS BLOB))`, "a\x00b").Scan(&storedBytes); err != nil || storedBytes != 3 {
		t.Fatalf("SQLite NUL-bearing text = %d bytes, %v; want 3", storedBytes, err)
	}

	// Server-side, chr(0) is refused outright, so a translation of
	// SQLite's char(0) cannot use it.
	_, chrErr := db.ExecContext(ctx, `SELECT chr(0)`)
	if code, _ := control.PostgresErrorCode(chrErr); code != "54000" {
		t.Fatalf("chr(0) = %v (SQLSTATE %q), want 54000", chrErr, code)
	}
}

// TestPostgresBreakTextBytesAndEmptyValuesKeepTheirIdentity covers the other
// side of NUL: bytea holds it exactly, and an empty value is never NULL.
func TestPostgresBreakTextBytesAndEmptyValuesKeepTheirIdentity(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	tx := semTextBegin(ctx, t, db)

	dashboards := semTextShadow(ctx, t, tx, "dashboards")
	insertDashboard := `INSERT INTO ` + dashboards + ` (dashboard_id, version, name, app_id, tenant_id, owner_id,
		sharing_scope, definition_proto, created_at_unix_micro, updated_at_unix_micro)
		VALUES ($1, 1, $1, 'search', 'tenant', 'owner', 1, $2, 1, 1)`
	payloads := [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0xff, 0x00}, []byte("a\x00b")}
	for index, payload := range payloads {
		if err := semTextTry(ctx, t, tx, insertDashboard, fmt.Sprintf("bytes-%d", index), payload); err != nil {
			t.Fatalf("store %x: %v", payload, err)
		}
		var stored []byte
		var length int
		if err := tx.QueryRowContext(ctx,
			`SELECT definition_proto, octet_length(definition_proto) FROM `+dashboards+` WHERE dashboard_id = $1`,
			fmt.Sprintf("bytes-%d", index),
		).Scan(&stored, &length); err != nil {
			t.Fatalf("read %x: %v", payload, err)
		}
		if !bytes.Equal(stored, payload) || length != len(payload) {
			t.Fatalf("bytea %x read back as %x (%d bytes)", payload, stored, length)
		}
	}
	// NUL bytes are significant: every payload above is a distinct value.
	var distinct int64
	if err := tx.QueryRowContext(ctx, `SELECT count(DISTINCT definition_proto) FROM `+dashboards).Scan(&distinct); err != nil || distinct != int64(len(payloads)) {
		t.Fatalf("distinct bytea payloads = %d, %v; want %d", distinct, err, len(payloads))
	}
	// An empty Go slice is an empty bytea that the 1..98304 CHECK refuses; a
	// nil slice is NULL that NOT NULL refuses.
	semTextRequireCheck(t, "empty bytea", semTextTry(ctx, t, tx, insertDashboard, "bytes-empty", []byte{}), "dashboards_definition_proto_check")
	nullErr := semTextTry(ctx, t, tx, insertDashboard, "bytes-nil", []byte(nil))
	if code, _ := control.PostgresErrorCode(nullErr); code != "23502" {
		t.Fatalf("nil bytea = %v (SQLSTATE %q), want not_null_violation", nullErr, code)
	}

	apps := semTextShadow(ctx, t, tx, "app_workspaces")
	insertApp := `INSERT INTO ` + apps + ` (app_id, tenant_id, version, slug, display_name,
		default_time_range_present, default_earliest, state, created_at_unix_micro, updated_at_unix_micro)
		VALUES ($1, 'tenant', 1, $2, 'App', 1, $3, 'active', 1, 1)`
	// NULL means "no default range"; the empty string is a malformed range.
	if err := semTextTry(ctx, t, tx, insertApp, "app_0123456789abcdefABCDEw", "first", sql.NullString{}); err != nil {
		t.Fatalf("NULL default_earliest: %v", err)
	}
	semTextRequireCheck(t, "empty default_earliest",
		semTextTry(ctx, t, tx, insertApp, "app_0123456789abcdefABCDEA", "second", ""),
		"app_workspaces_default_earliest_check")
	var description string
	var earliestIsNull bool
	if err := tx.QueryRowContext(ctx,
		`SELECT description, default_earliest IS NULL FROM `+apps+` WHERE app_id = 'app_0123456789abcdefABCDEw'`,
	).Scan(&description, &earliestIsNull); err != nil {
		t.Fatalf("read app: %v", err)
	}
	if description != "" || !earliestIsNull {
		t.Fatalf("omitted description = %q and NULL default_earliest kept = %v", description, earliestIsNull)
	}
	explicitNullErr := semTextTry(ctx, t, tx, `INSERT INTO `+apps+` (app_id, tenant_id, version, slug, display_name,
		description, default_time_range_present, state, created_at_unix_micro, updated_at_unix_micro)
		VALUES ('app_0123456789abcdefABCDEQ', 'tenant', 1, 'third', 'App', NULL, 0, 'active', 1, 1)`)
	if code, _ := control.PostgresErrorCode(explicitNullErr); code != "23502" {
		t.Fatalf("explicit NULL description = %v (SQLSTATE %q), want not_null_violation, not the '' default", explicitNullErr, code)
	}
	// Two NULLs never collide in a UNIQUE constraint, as in SQLite.
	var nullsNotDistinct int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_index AS x JOIN pg_class AS c ON c.oid = x.indrelid
		WHERE c.relnamespace = 'open_splunk'::regnamespace AND x.indnullsnotdistinct`,
	).Scan(&nullsNotDistinct); err != nil || nullsNotDistinct != 0 {
		t.Fatalf("NULLS NOT DISTINCT indexes = %d, %v; SQLite treats NULLs as distinct", nullsNotDistinct, err)
	}
}

func semTextRandomText(rng *rand.Rand, low, high rune, totalBytes int) string {
	var builder strings.Builder
	for builder.Len() < totalBytes {
		character := low + rune(rng.IntN(int(high-low)+1))
		if builder.Len()+utf8.RuneLen(character) > totalBytes {
			builder.WriteString(strings.Repeat("z", totalBytes-builder.Len()))
			break
		}
		builder.WriteRune(character)
	}
	return builder.String()
}

// TestPostgresBreakTextWideDescriptionsStayIndexable is the regression test
// for knowledge_list_projection_description_idx, which SQLite keyed on the
// description itself. Incompressible descriptions that satisfy every CHECK
// (up to 16384 bytes) must be storable; under the SQLite key shape PostgreSQL
// refused them with SQLSTATE 54000.
func TestPostgresBreakTextWideDescriptionsStayIndexable(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	tx := semTextBegin(ctx, t, db)
	shadow := semTextShadow(ctx, t, tx, "knowledge_object_list_projections")
	insert := `INSERT INTO ` + shadow + ` (tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, description_present, description, index_selector_count,
		host_selector_count, source_selector_count, sourcetype_selector_count, selector_value_bytes,
		canonical_selector_bytes)
		VALUES ('tenant', $1, 1, 'search', 'owner', 'field_alias', $1, 'private', 'active', 1, $2, 0, 0, 0, 0, 0, 43)`

	rng := rand.New(rand.NewPCG(20261024, 1))
	hexBytes := make([]byte, 8192)
	for index := range hexBytes {
		hexBytes[index] = byte(rng.IntN(256))
	}
	descriptions := []struct{ name, value string }{
		{"16384 hex bytes", hex.EncodeToString(hexBytes)},
		{"16384 bytes of random CJK", semTextRandomText(rng, 0x4e00, 0x9fff, 16384)},
		{"16384 bytes of random four-byte CJK", semTextRandomText(rng, 0x20000, 0x2a6df, 16384)},
		{"3000 bytes of random two-byte letters", semTextRandomText(rng, 0x0100, 0x024f, 3000)},
	}
	for _, description := range descriptions {
		if len(description.value) > 16384 || !utf8.ValidString(description.value) {
			t.Fatalf("%s: generated %d bytes", description.name, len(description.value))
		}
		if err := semTextTry(ctx, t, tx, insert, description.name, description.value); err != nil {
			code, _ := control.PostgresErrorCode(err)
			t.Fatalf("%s: insert = %v (SQLSTATE %q)", description.name, err, code)
		}
	}
	semTextRequireCheck(t, "16385 bytes",
		semTextTry(ctx, t, tx, insert, "too wide", strings.Repeat("y", 16385)),
		"knowledge_list_projection_description_canonical")

	// The data really is beyond the btree limit: the SQLite key shape cannot
	// be built over it.
	shapeErr := semTextTry(ctx, t, tx, `CREATE INDEX semtext_sqlite_description_shape ON `+shadow+
		` (tenant_id, description, knowledge_object_id, object_version) WHERE description_present = 1`)
	if code, _ := control.PostgresErrorCode(shapeErr); code != "54000" {
		t.Fatalf("SQLite-shaped description index = %v (SQLSTATE %q), want 54000", shapeErr, code)
	}
}

var semTextLengthBound = regexp.MustCompile(`\b(octet|char)_length\(([a-z_][a-z0-9_]*)\) (<=|<|=) ([0-9]+)\b`)

// TestPostgresBreakTextNoIndexKeysAColumnWiderThanABtreeTuple scans every
// btree index for a text or bytea key or INCLUDE column whose tightest CHECK
// byte bound (a character is up to four bytes) still exceeds what one btree
// tuple holds, which PostgreSQL refuses with 54000 and SQLite never did.
// Columns without a recognizable bound are not judged.
func TestPostgresBreakTextNoIndexKeysAColumnWiderThanABtreeTuple(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()

	bounds := make(map[string]int)
	definitions := semTextQueryStrings(ctx, t, db.QueryContext, `
		SELECT c.relname || ' ' || pg_get_constraintdef(con.oid)
		FROM pg_constraint AS con
		JOIN pg_class AS c ON c.oid = con.conrelid
		WHERE c.relnamespace = 'open_splunk'::regnamespace AND con.contype = 'c'`)
	for _, definition := range definitions {
		table, body, _ := strings.Cut(definition, " ")
		for _, match := range semTextLengthBound.FindAllStringSubmatch(body, -1) {
			limit, err := strconv.Atoi(match[4])
			if err != nil {
				t.Fatalf("parse bound in %q: %v", definition, err)
			}
			if match[3] == "<" {
				limit--
			}
			if match[1] == "char" {
				limit *= utf8.UTFMax
			}
			key := table + "." + match[2]
			if current, ok := bounds[key]; !ok || limit < current {
				bounds[key] = limit
			}
		}
	}
	// A foreign key column holds only values its parent column holds, so it
	// inherits the parent's bound (tenant_id columns that admit 1024
	// characters are still 255 bytes through tenants).
	references := semTextQueryStrings(ctx, t, db.QueryContext, `
		SELECT c.relname || '.' || a.attname || ' ' || pc.relname || '.' || pa.attname
		FROM pg_constraint AS con
		JOIN pg_class AS c ON c.oid = con.conrelid
		JOIN pg_class AS pc ON pc.oid = con.confrelid
		CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS k(child, parent)
		JOIN pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = k.child
		JOIN pg_attribute AS pa ON pa.attrelid = con.confrelid AND pa.attnum = k.parent
		WHERE con.contype = 'f' AND c.relnamespace = 'open_splunk'::regnamespace`)
	for range 4 {
		for _, reference := range references {
			child, parent, _ := strings.Cut(reference, " ")
			if parentLimit, ok := bounds[parent]; ok {
				if childLimit, bounded := bounds[child]; !bounded || parentLimit < childLimit {
					bounds[child] = parentLimit
				}
			}
		}
	}
	if bounds["knowledge_object_list_projections.description"] != 16384 ||
		bounds["tenants.tenant_id"] != 255 || bounds["search_history.tenant_id"] != 255 {
		t.Fatalf("bound parser read description=%d tenants.tenant_id=%d search_history.tenant_id=%d; "+
			"it no longer understands the catalog",
			bounds["knowledge_object_list_projections.description"], bounds["tenants.tenant_id"],
			bounds["search_history.tenant_id"])
	}

	keys := semTextQueryStrings(ctx, t, db.QueryContext, `
		SELECT i.relname || ' ' || c.relname || '.' || a.attname
		FROM pg_index AS x
		JOIN pg_class AS i ON i.oid = x.indexrelid
		JOIN pg_am AS am ON am.oid = i.relam AND am.amname = 'btree'
		JOIN pg_class AS c ON c.oid = x.indrelid
		CROSS JOIN LATERAL unnest(x.indkey::int2[]) AS k(attnum)
		JOIN pg_attribute AS a ON a.attrelid = c.oid AND a.attnum = k.attnum
		WHERE c.relnamespace = 'open_splunk'::regnamespace
		  AND a.atttypid IN ('text'::regtype, 'bytea'::regtype)
		ORDER BY 1`)
	if len(keys) < 100 {
		t.Fatalf("found %d text or bytea index columns; the catalog query is broken", len(keys))
	}
	for _, key := range keys {
		index, column, _ := strings.Cut(key, " ")
		if limit, ok := bounds[column]; ok && limit > semTextBtreeMaximumTupleBytes {
			t.Errorf("btree index %s keys %s, which admits %d bytes; one btree tuple holds %d",
				index, column, limit, semTextBtreeMaximumTupleBytes)
		}
	}
}

var semTextForbiddenSQL = []struct {
	why     string
	pattern *regexp.Regexp
}{
	{"ILIKE and ~~* fold case by the collation", regexp.MustCompile(`(?i)\bILIKE\b|~~\*`)},
	{"case-insensitive regex folds case by the collation", regexp.MustCompile(`!?~\*`)},
	{"SIMILAR TO has its own escape rules", regexp.MustCompile(`(?i)\bSIMILAR\s+TO\b`)},
	{"a collation other than \"C\" changes byte order", regexp.MustCompile(`(?i)\bCOLLATE\s+(?:"(?:[^C"]|C[^"])[^"]*"|[a-z_][a-z0-9_.]*)`)},
	{"chr(0) raises 54000", regexp.MustCompile(`(?i)\bchr\s*\(\s*0\s*\)`)},
	{"a bare length() hides whether bytes or characters are meant", regexp.MustCompile(`(?i)(?:^|[^a-z_])length\s*\(`)},
	{"SQLite LIKE folded ASCII case; PostgreSQL LIKE does not", regexp.MustCompile(`(?i)(?:\bLIKE|~~)\s+'[^']*[a-z]`)},
	{"citext, varchar and char(n) compare differently from text", regexp.MustCompile(`(?i)\b(?:citext|character varying|varchar|bpchar)\b`)},
}

// TestPostgresBreakTextCatalogIsByteOrderedEverywhere is the catalog-wide
// guard behind the differential tests: every collatable column and index key
// is COLLATE "C", only the five translated column types exist, no routine
// returns a collatable value (which would take the database default), and no
// constraint, index, default or routine body uses a construct whose meaning
// depends on the collation or differs from SQLite.
func TestPostgresBreakTextCatalogIsByteOrderedEverywhere(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	requireNone := func(label, query string) {
		t.Helper()
		if found := semTextQueryStrings(ctx, t, db.QueryContext, query); len(found) != 0 {
			t.Errorf("%s: %q", label, found)
		}
	}
	requireNone("collatable columns not COLLATE \"C\"", `
		SELECT c.relname || '.' || a.attname || ' ' || coalesce(co.collname, '?')
		FROM pg_attribute AS a
		JOIN pg_class AS c ON c.oid = a.attrelid
		LEFT JOIN pg_collation AS co ON co.oid = a.attcollation
		WHERE c.relnamespace = 'open_splunk'::regnamespace AND a.attnum > 0 AND NOT a.attisdropped
		  AND a.attcollation <> 0 AND co.collname IS DISTINCT FROM 'C'`)
	requireNone("column types outside the translation table", `
		SELECT c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute AS a
		JOIN pg_class AS c ON c.oid = a.attrelid AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		WHERE c.relnamespace = 'open_splunk'::regnamespace AND a.attnum > 0 AND NOT a.attisdropped
		  AND format_type(a.atttypid, a.atttypmod) NOT IN ('text', 'bytea', 'bigint', 'boolean', 'double precision')`)
	requireNone("index keys not COLLATE \"C\"", `
		SELECT i.relname || ' key ' || k.ord || ' ' || coalesce(co.collname, '?')
		FROM pg_index AS x
		JOIN pg_class AS i ON i.oid = x.indexrelid
		CROSS JOIN LATERAL unnest(x.indcollation::oid[]) WITH ORDINALITY AS k(coll, ord)
		LEFT JOIN pg_collation AS co ON co.oid = k.coll
		WHERE i.relnamespace = 'open_splunk'::regnamespace AND k.coll <> 0 AND co.collname IS DISTINCT FROM 'C'`)
	requireNone("domains, enums and composite types", `
		SELECT t.typname FROM pg_type AS t
		WHERE t.typnamespace = 'open_splunk'::regnamespace AND t.typtype IN ('d', 'e', 'c', 'r', 'm') AND t.typrelid = 0`)
	requireNone("routines returning a collatable type", `
		SELECT p.proname || ' returns ' || format_type(p.prorettype, NULL)
		FROM pg_proc AS p JOIN pg_type AS t ON t.oid = p.prorettype
		WHERE p.pronamespace = 'open_splunk'::regnamespace AND t.typcollation <> 0`)

	sources := semTextQueryStrings(ctx, t, db.QueryContext, `
		SELECT 'constraint ' || c.relname || '.' || con.conname || ': ' || pg_get_constraintdef(con.oid)
		FROM pg_constraint AS con JOIN pg_class AS c ON c.oid = con.conrelid
		WHERE c.relnamespace = 'open_splunk'::regnamespace
		UNION ALL
		SELECT 'index ' || i.relname || ': ' || pg_get_indexdef(x.indexrelid)
		FROM pg_index AS x JOIN pg_class AS i ON i.oid = x.indexrelid
		WHERE i.relnamespace = 'open_splunk'::regnamespace
		UNION ALL
		SELECT 'default ' || c.relname || '.' || a.attname || ': ' || pg_get_expr(d.adbin, d.adrelid)
		FROM pg_attrdef AS d
		JOIN pg_class AS c ON c.oid = d.adrelid
		JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE c.relnamespace = 'open_splunk'::regnamespace
		UNION ALL
		SELECT 'routine ' || p.proname || ': ' || p.prosrc
		FROM pg_proc AS p WHERE p.pronamespace = 'open_splunk'::regnamespace`)
	if len(sources) < 500 {
		t.Fatalf("catalog scan saw %d definitions; the query is broken", len(sources))
	}
	for _, source := range sources {
		// Comments in routine bodies may discuss the SQLite originals.
		code := semTextStripSQLComments(source)
		for _, forbidden := range semTextForbiddenSQL {
			if location := forbidden.pattern.FindStringIndex(code); location != nil {
				label, _, _ := strings.Cut(source, ":")
				t.Errorf("%s uses %q: %s", label, code[location[0]:location[1]], forbidden.why)
			}
		}
	}

	// The migration files themselves are ASCII without NUL, so neither the
	// client encoding nor a text-only transport can change what they say.
	err := fs.WalkDir(migrations.Postgres(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		contents, err := fs.ReadFile(migrations.Postgres(), path)
		if err != nil {
			return err
		}
		for offset, value := range contents {
			if value == 0 || value > 0x7f {
				return fmt.Errorf("%s byte %d is %#x", path, offset, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("PostgreSQL migrations must be ASCII without NUL: %v", err)
	}
}

var (
	semTextLineComment  = regexp.MustCompile(`--[^\n]*`)
	semTextBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
)

func semTextStripSQLComments(source string) string {
	return semTextLineComment.ReplaceAllString(semTextBlockComment.ReplaceAllString(source, " "), " ")
}

// TestPostgresBreakTextSessionDefaultsCannotChangeTextSemantics sets the
// database defaults an operator sharing the database might choose:
// standard_conforming_strings off, a LATIN1 client encoding and escaped
// bytea output. Under the first, '\x'::bytea in 0001 parses as the byte 'x'
// and 0003 fails to parse; under the second, every UTF-8 byte pgx sends is
// reread as a LATIN1 character. The runner and every pooled session must pin
// both, whatever the database, role or connection string says.
func TestPostgresBreakTextSessionDefaultsCannotChangeTextSemantics(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	corpus := loadSemTextCorpus(t)

	hostile := func(t *testing.T, conformingOff bool) string {
		t.Helper()
		dsn := newEmptyPostgresDatabase(t)
		config, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("parse test database connection string: %v", err)
		}
		owner := openRawPostgres(t, dsn)
		wantConforming := "on"
		if conformingOff {
			wantConforming = "off"
			if _, err := owner.ExecContext(ctx, `ALTER DATABASE `+pgx.Identifier{config.Database}.Sanitize()+
				` SET standard_conforming_strings = off`); err != nil {
				t.Fatalf("set standard_conforming_strings default: %v", err)
			}
		}
		if _, err := owner.ExecContext(ctx, `ALTER DATABASE `+pgx.Identifier{config.Database}.Sanitize()+
			` SET client_encoding = 'LATIN1'`); err != nil {
			t.Fatalf("set client_encoding default: %v", err)
		}
		if _, err := owner.ExecContext(ctx, `ALTER DATABASE `+pgx.Identifier{config.Database}.Sanitize()+
			` SET bytea_output = 'escape'`); err != nil {
			t.Fatalf("set bytea_output default: %v", err)
		}
		// The defaults are live for a session that does not pin them, and
		// they really do reinterpret UTF-8.
		raw := openRawPostgres(t, dsn)
		var encoding, conforming string
		var octets int
		var hasC1 bool
		if err := raw.QueryRowContext(ctx, `
			SELECT current_setting('client_encoding'), current_setting('standard_conforming_strings'),
			       octet_length($1::text), strpos($2::text, chr(159)) > 0`,
			"\u00e9", "\U0001F600",
		).Scan(&encoding, &conforming, &octets, &hasC1); err != nil {
			t.Fatalf("read hostile session: %v", err)
		}
		if encoding != "LATIN1" || conforming != wantConforming || octets != 4 || !hasC1 {
			t.Fatalf("hostile session encoding=%q conforming=%q \u00e9=%d bytes emoji-has-C1=%v",
				encoding, conforming, octets, hasC1)
		}
		return dsn
	}

	verify := func(t *testing.T, db *sql.DB) {
		t.Helper()
		var outboxDefault, principalDefault string
		if err := db.QueryRowContext(ctx, `
			SELECT pg_get_expr(d.adbin, d.adrelid)
			FROM pg_attrdef AS d
			JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
			WHERE d.adrelid = 'open_splunk.ingest_visibility_reservations'::regclass AND a.attname = 'outbox_sha256'`,
		).Scan(&outboxDefault); err != nil {
			t.Fatalf("read outbox_sha256 default: %v", err)
		}
		if err := db.QueryRowContext(ctx, `
			SELECT pg_get_expr(d.adbin, d.adrelid)
			FROM pg_attrdef AS d
			JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
			WHERE d.adrelid = 'open_splunk.ingest_visibility_reservations'::regclass AND a.attname = 'principal_sha256'`,
		).Scan(&principalDefault); err != nil {
			t.Fatalf("read principal_sha256 default: %v", err)
		}
		// PostgreSQL prints the empty bytea as ''::bytea; the broken parse
		// printed '\x78'::bytea.
		if outboxDefault != `''::bytea` || principalDefault != `''::bytea` {
			t.Fatalf("bytea defaults = %s and %s, want the empty bytea", outboxDefault, principalDefault)
		}
		var checksum []byte
		if err := db.QueryRowContext(ctx,
			`SELECT checksum FROM open_splunk.schema_migrations WHERE version = 1`,
		).Scan(&checksum); err != nil {
			t.Fatalf("read ledger checksum: %v", err)
		}
		first, err := fs.ReadFile(migrations.Postgres(), "0001_tenants_core_ingest.sql")
		if err != nil {
			t.Fatalf("read 0001: %v", err)
		}
		if want := sha256.Sum256(first); !bytes.Equal(checksum, want[:]) {
			t.Fatalf("ledger checksum = %x, want %x", checksum, want)
		}
	}

	t.Run("runner pins string semantics on an unpinned pool", func(t *testing.T) {
		t.Parallel()
		raw := openRawPostgres(t, hostile(t, true))
		if err := control.ApplyPostgresMigrations(ctx, raw, migrations.Postgres()); err != nil {
			t.Fatalf("ApplyPostgresMigrations() under hostile defaults = %v", err)
		}
		verify(t, raw)
	})

	// LATIN1 alone leaves the ASCII migrations intact, so only the session
	// pin keeps multibyte text byte-exact.
	t.Run("control pool pins the client encoding", func(t *testing.T) {
		t.Parallel()
		db, err := control.OpenPostgres(ctx, hostile(t, false))
		if err != nil {
			t.Fatalf("OpenPostgres() under a LATIN1 default = %v", err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		semTextRequireUTF8Session(t, db.SQLDB(), corpus)
	})

	t.Run("control pool pins the session against database and DSN defaults", func(t *testing.T) {
		t.Parallel()
		dsn := hostile(t, true)
		db, err := control.OpenPostgres(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenPostgres() under hostile defaults = %v", err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		verify(t, db.SQLDB())
		semTextRequireUTF8Session(t, db.SQLDB(), corpus)

		// A connection string asking for the hostile values is overridden too.
		overridden, err := control.OpenPostgres(ctx, semTextWithParameters(dsn, map[string]string{
			"client_encoding":             "LATIN1",
			"standard_conforming_strings": "off",
		}))
		if err != nil {
			t.Fatalf("OpenPostgres() with hostile DSN parameters = %v", err)
		}
		t.Cleanup(func() {
			if err := overridden.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		semTextRequireUTF8Session(t, overridden.SQLDB(), corpus)
	})
}

// semTextRequireUTF8Session checks pinned settings on several pooled
// sessions and that the corpus is stored and measured byte for byte.
func semTextRequireUTF8Session(t *testing.T, db *sql.DB, corpus []string) {
	t.Helper()
	ctx := t.Context()
	octets := semTextQueryStrings(ctx, t, db.QueryContext,
		`SELECT CAST(octet_length(v) AS TEXT) FROM unnest($1::text[]) WITH ORDINALITY AS u(v, ord) ORDER BY ord`, corpus)
	for index, value := range corpus {
		if octets[index] != strconv.Itoa(len(value)) {
			t.Fatalf("%q measured %s bytes, want %d", value, octets[index], len(value))
		}
	}
	tenantID := "\U0001F600\u00e9-" + strconv.Itoa(rand.IntN(1<<30))
	if err := semTextInsertTenant(ctx, db, tenantID); err != nil {
		t.Fatalf("insert multibyte tenant: %v", err)
	}
	var stored string
	var storedOctets, storedCharacters int
	if err := db.QueryRowContext(ctx,
		`SELECT tenant_id, octet_length(tenant_id), char_length(tenant_id) FROM tenants WHERE tenant_id = $1`, tenantID,
	).Scan(&stored, &storedOctets, &storedCharacters); err != nil {
		t.Fatalf("read multibyte tenant: %v", err)
	}
	if stored != tenantID || storedOctets != len(tenantID) || storedCharacters != utf8.RuneCountInString(tenantID) {
		t.Fatalf("multibyte tenant stored as %d bytes and %d characters (equal=%v)", storedOctets, storedCharacters, stored == tenantID)
	}
	conns := make([]*sql.Conn, 3)
	for index := range conns {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire connection: %v", err)
		}
		conns[index] = conn
	}
	for _, conn := range conns {
		var encoding, conforming string
		if err := conn.QueryRowContext(ctx,
			`SELECT current_setting('client_encoding'), current_setting('standard_conforming_strings')`,
		).Scan(&encoding, &conforming); err != nil {
			t.Fatalf("read session settings: %v", err)
		}
		if encoding != "UTF8" || conforming != "on" {
			t.Fatalf("pooled session client_encoding=%q standard_conforming_strings=%q", encoding, conforming)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("release connection: %v", err)
		}
	}
}
