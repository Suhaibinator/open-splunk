package pgtest

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Per-test clones must copy the template's files rather than write every
// block of it to WAL, which cost about 7 MB of WAL per clone.
func TestClonesCopyTheTemplateFiles(t *testing.T) {
	t.Parallel()
	statement := clonePostgresStatement("ostpl_f2_x", "ostest_y")
	if statement != `CREATE DATABASE "ostest_y" TEMPLATE "ostpl_f2_x" STRATEGY FILE_COPY` {
		t.Fatalf("clone statement = %q", statement)
	}
	dsn := NewDatabase(t, Template{
		Key: "file_copy_probe",
		Build: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, `CREATE TABLE copied AS SELECT 1 AS one`)
			return err
		},
	})
	db := openTestPool(t, dsn)
	var one int
	if err := db.QueryRowContext(t.Context(), `SELECT one FROM copied`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("FILE_COPY clone lost the template's contents: %d, %v", one, err)
	}
}

func openTestPool(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	return db
}

func templateComment(t *testing.T, admin *sql.DB, name string) (string, bool) {
	t.Helper()
	var comment sql.NullString
	err := admin.QueryRowContext(t.Context(),
		`SELECT pg_catalog.shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = $1`, name,
	).Scan(&comment)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read comment of %s: %v", name, err)
	}
	return comment.String, true
}

// plantTemplate creates name as a template database carrying comment, or no
// comment when it is empty, and drops it when the test ends.
func plantTemplate(t *testing.T, admin *sql.DB, name, comment string, template bool) {
	t.Helper()
	dropTemplateOnCleanup(t, name)
	ctx := t.Context()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(name)); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if comment != "" {
		if _, err := admin.ExecContext(ctx,
			`COMMENT ON DATABASE `+quotePostgresIdentifier(name)+` IS '`+comment+`'`,
		); err != nil {
			t.Fatalf("comment %s: %v", name, err)
		}
	}
	if template {
		if _, err := admin.ExecContext(ctx,
			`ALTER DATABASE `+quotePostgresIdentifier(name)+` WITH IS_TEMPLATE true ALLOW_CONNECTIONS false`,
		); err != nil {
			t.Fatalf("flag %s as a template: %v", name, err)
		}
	}
}

// Templates nobody has used for the retention period, and staging databases
// of builds that died, are dropped; fresh, unstamped and locked ones stay.
func TestReclaimStaleTemplatesDropsOnlyUnusedTemplates(t *testing.T) {
	t.Parallel()
	_, admin := postgresAdminHandle(t)
	ctx := t.Context()
	nonce, err := randomHex(6)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	const retention = time.Hour
	name := func(kind string) string { return TemplateDatabaseName("reclaim_" + kind + "_" + nonce) }
	stale := name("stale")
	fresh := name("fresh")
	unstamped := name("unstamped")
	obsolete := postgresTemplatePrefix + "f0_reclaim_" + nonce
	staging := name("crashed") + "_build"
	locked := name("locked")
	plantTemplate(t, admin, stale, stampText(now.Add(-2*retention)), true)
	plantTemplate(t, admin, fresh, stampText(now.Add(-retention/2)), true)
	plantTemplate(t, admin, unstamped, "", true)
	plantTemplate(t, admin, obsolete, stampText(now.Add(-2*retention)), true)
	plantTemplate(t, admin, staging, stampText(now.Add(-2*retention)), false)
	plantTemplate(t, admin, locked, stampText(now.Add(-2*retention)), true)

	// A builder of locked holds its construction lock throughout the sweep.
	builder, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = builder.Close() }()
	if _, err := builder.ExecContext(ctx, postgresTemplateLockSQL, locked); err != nil {
		t.Fatalf("hold construction lock: %v", err)
	}
	defer func() { _, _ = builder.ExecContext(context.Background(), postgresTemplateUnlockSQL, locked) }()

	ours := func(database string) bool { return strings.Contains(database, nonce) }
	for range 2 {
		// A second sweep, as a concurrently starting process would run, is a
		// no-op rather than a failure.
		if err := reclaimStaleTemplates(ctx, admin, now, retention, ours); err != nil {
			t.Fatalf("reclaimStaleTemplates() = %v", err)
		}
	}
	for database, wantKept := range map[string]bool{
		stale:     false,
		obsolete:  false,
		staging:   false,
		fresh:     true,
		unstamped: true,
		locked:    true,
	} {
		if _, exists := templateComment(t, admin, database); exists != wantKept {
			t.Errorf("%s exists=%v after the sweep, want %v", database, exists, wantKept)
		}
	}
	// A template first seen without a stamp is stamped, so it is reclaimed
	// once it has gone unused for the retention period from now.
	// Another test process starting meanwhile may have stamped it first, a
	// moment later.
	comment, _ := templateComment(t, admin, unstamped)
	match := postgresTemplateStampPattern.FindStringSubmatch(comment)
	if match == nil {
		t.Fatalf("unstamped template comment = %q, want a last-use stamp", comment)
	}
	if stamped, err := strconv.ParseInt(match[1], 10, 64); err != nil ||
		stamped < now.Unix() || stamped > now.Add(time.Minute).Unix() {
		t.Fatalf("unstamped template stamp = %s, want about %d: %v", match[1], now.Unix(), err)
	}
	if err := reclaimStaleTemplates(ctx, admin, now.Add(2*retention), retention, func(database string) bool {
		return database == unstamped
	}); err != nil {
		t.Fatalf("later reclaimStaleTemplates() = %v", err)
	}
	if _, exists := templateComment(t, admin, unstamped); exists {
		t.Fatal("a template stamped on first sight survived the retention period unused")
	}
}

func stampText(at time.Time) string {
	return "open-splunk test template; last used at unix " + strconv.FormatInt(at.Unix(), 10)
}

// Every process that uses a template stamps it, so a template in use is
// never reclaimed as stale.
func TestNewDatabaseStampsTheTemplateItUses(t *testing.T) {
	t.Parallel()
	_, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	key = "stamp_" + key
	templateName := TemplateDatabaseName(key)
	dropTemplateOnCleanup(t, templateName)
	before := time.Now().Add(-time.Second)
	NewDatabase(t, Template{Key: key, Build: func(context.Context, *sql.DB) error { return nil }})
	comment, exists := templateComment(t, admin, templateName)
	match := postgresTemplateStampPattern.FindStringSubmatch(comment)
	if !exists || match == nil {
		t.Fatalf("template %s comment = %q, want a last-use stamp", templateName, comment)
	}
	if stamped, err := strconv.ParseInt(match[1], 10, 64); err != nil || stamped < before.Unix() {
		t.Fatalf("stamp %s predates the test (%d): %v", match[1], before.Unix(), err)
	}
}

// A template reclaimed while a process still clones it is rebuilt instead
// of failing that process's later tests.
func TestNewDatabaseRebuildsAReclaimedTemplate(t *testing.T) {
	t.Parallel()
	_, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	key = "rebuild_" + key
	templateName := TemplateDatabaseName(key)
	dropTemplateOnCleanup(t, templateName)
	var builds atomic.Int32
	template := Template{Key: key, Build: func(ctx context.Context, db *sql.DB) error {
		builds.Add(1)
		_, err := db.ExecContext(ctx, `CREATE TABLE rebuilt (id int)`)
		return err
	}}
	NewDatabase(t, template)
	conn, err := admin.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := discardPostgresDatabaseOn(t.Context(), conn, templateName); err != nil {
		t.Fatalf("reclaim template behind the process: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	dsn := NewDatabase(t, template)
	if got := builds.Load(); got != 2 {
		t.Fatalf("template built %d times, want a rebuild after it was reclaimed", got)
	}
	var tables int
	if err := openTestPool(t, dsn).QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_tables WHERE tablename = 'rebuilt'`).Scan(&tables); err != nil || tables != 1 {
		t.Fatalf("clone of the rebuilt template has %d rebuilt tables, %v", tables, err)
	}
}
