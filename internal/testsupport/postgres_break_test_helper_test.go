package testsupport

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// White-box regression tests from the PostgreSQL test-helper break lane. The
// black-box half lives in internal/control/postgres_break_test_helper_test.go.

func TestPostgresDSNForDatabaseOverridesQueryDatabase(t *testing.T) {
	t.Parallel()
	// pgx applies URL query parameters after the path, so a dbname or
	// database parameter left in place wins over the rewritten path.
	hazard, err := pgx.ParseConfig("postgres://admin@127.0.0.1/ostest_x?dbname=postgres")
	if err != nil || hazard.Database != "postgres" {
		t.Fatalf("pgx no longer prefers the dbname parameter (%v); revisit this test", err)
	}
	for _, input := range []string{
		"postgres://admin:pw@127.0.0.1:5432/postgres?dbname=postgres&sslmode=disable",
		"postgres://admin:pw@127.0.0.1:5432/?database=postgres&sslmode=disable",
		"postgresql://admin:pw@127.0.0.1/postgres?sslmode=disable&dbname=postgres&database=postgres",
	} {
		rewritten, err := postgresDSNForDatabase(input, "ostest_x")
		if err != nil {
			t.Fatalf("postgresDSNForDatabase(%q) = %v", input, err)
		}
		config, err := pgx.ParseConfig(rewritten)
		if err != nil {
			t.Fatalf("parse rewritten %q: %v", rewritten, err)
		}
		if config.Database != "ostest_x" {
			t.Fatalf("postgresDSNForDatabase(%q) connects to %q, want ostest_x", input, config.Database)
		}
		if config.TLSConfig != nil {
			t.Fatalf("postgresDSNForDatabase(%q) dropped sslmode=disable", input)
		}
	}
}

func breakHelperTemplateFlags(t *testing.T, admin *sql.DB, name string) (exists, isTemplate bool) {
	t.Helper()
	err := admin.QueryRowContext(context.Background(),
		`SELECT datistemplate FROM pg_database WHERE datname = $1`, name,
	).Scan(&isTemplate)
	if err == sql.ErrNoRows {
		return false, false
	}
	if err != nil {
		t.Fatalf("inspect database %s: %v", name, err)
	}
	return true, isTemplate
}

// A builder of an older helper version that died between marking its
// staging database a template and renaming it left a database DROP DATABASE
// refuses. The next build must discard it rather than fail forever.
func TestPostgresTemplateBuildDiscardsTemplateFlaggedStaging(t *testing.T) {
	t.Parallel()
	adminDSN, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	templateName := postgresTemplateName("stage_" + key)
	staging := templateName + "_build"
	dropTemplateOnCleanup(t, templateName)
	dropTemplateOnCleanup(t, staging)
	ctx := t.Context()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(staging)); err != nil {
		t.Fatalf("plant staging database: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`ALTER DATABASE `+quotePostgresIdentifier(staging)+` WITH IS_TEMPLATE true ALLOW_CONNECTIONS false`,
	); err != nil {
		t.Fatalf("flag staging database as a template: %v", err)
	}
	if err := buildPostgresTemplate(ctx, adminDSN, admin, templateName, func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, `CREATE TABLE rebuilt (id int)`)
		return err
	}); err != nil {
		t.Fatalf("buildPostgresTemplate() over a template-flagged staging database = %v", err)
	}
	if exists, isTemplate := breakHelperTemplateFlags(t, admin, templateName); !exists || !isTemplate {
		t.Fatalf("template exists=%v datistemplate=%v after rebuild", exists, isTemplate)
	}
	if exists, _ := breakHelperTemplateFlags(t, admin, staging); exists {
		t.Fatal("staging database survived the rebuild")
	}
}

// Publication is one transaction: when the rename fails, the staging
// database must not be left flagged as a template.
func TestPostgresTemplatePublicationIsAtomic(t *testing.T) {
	t.Parallel()
	_, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	templateName := postgresTemplateName("atomic_" + key)
	staging := templateName + "_build"
	dropTemplateOnCleanup(t, templateName)
	dropTemplateOnCleanup(t, staging)
	ctx := t.Context()
	for _, name := range []string{staging, templateName} {
		if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	conn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// The final name is taken, so the rename fails after the flag is set.
	if err := publishPostgresTemplate(ctx, conn, staging, templateName); err == nil ||
		!strings.Contains(err.Error(), "publish template") {
		t.Fatalf("publishPostgresTemplate() onto a taken name = %v", err)
	}
	if exists, isTemplate := breakHelperTemplateFlags(t, admin, staging); !exists || isTemplate {
		t.Fatalf("staging exists=%v datistemplate=%v after a failed publication", exists, isTemplate)
	}
}

// The reclaimer drops only databases whose owner has no session: never a
// live owner's, never this process's own, and never a name in another format.
func TestReclaimDeadOwnerDatabasesSparesLiveOwnersAndForeignNames(t *testing.T) {
	t.Parallel()
	adminDSN, admin := postgresAdminHandle(t)
	ctx := t.Context()
	token := func() string {
		value, err := randomHex(postgresOwnerTokenBytes)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	name := func(owner string) string { return postgresTestDatabasePrefix + owner + "_" + token() }

	live, err := claimPostgresOwner(ctx, adminDSN)
	if err != nil {
		t.Fatalf("claimPostgresOwner() = %v", err)
	}
	t.Cleanup(func() { _ = live.conn.Close(context.Background()) })
	// This process's real token: a made-up one would have no session, and a
	// concurrently starting test process would rightly reclaim it.
	own := postgresAdmin.owner.token
	dead := name(token())
	deadSibling := dead[:len(postgresTestDatabasePrefix)+16] + "_" + token()
	liveDB := name(live.token)
	ownDB := name(own)
	legacy := postgresTestDatabasePrefix + token()
	upper := postgresTestDatabasePrefix + strings.ToUpper(token()) + "_" + token()
	all := []string{dead, deadSibling, liveDB, ownDB, legacy, upper}
	for _, database := range all {
		dropTemplateOnCleanup(t, database)
		if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(database)); err != nil {
			t.Fatalf("create %s: %v", database, err)
		}
	}
	// A reclaimer started concurrently by another test process makes these
	// drops no-ops rather than failures.
	for range 2 {
		if err := reclaimDeadOwnerDatabases(ctx, admin, own); err != nil {
			t.Fatalf("reclaimDeadOwnerDatabases() = %v", err)
		}
	}
	for _, database := range all {
		wantGone := database == dead || database == deadSibling
		if exists, _ := breakHelperTemplateFlags(t, admin, database); exists == wantGone {
			t.Errorf("database %s exists=%v after reclaiming, want exists=%v", database, exists, !wantGone)
		}
	}
}

// Advisory locks are scoped to the database a session is connected to, so a
// liveness signal built on them misses an owner whose administrator DSN
// names another database. Its databases must survive too.
func TestReclaimDeadOwnerDatabasesSeesOwnersConnectedElsewhere(t *testing.T) {
	t.Parallel()
	_, admin := postgresAdminHandle(t)
	ctx := t.Context()
	elsewhere := NewPostgresDatabase(t, PostgresTemplate{
		Key:   "reclaim_elsewhere",
		Build: func(context.Context, *sql.DB) error { return nil },
	})
	owner, err := claimPostgresOwner(ctx, elsewhere)
	if err != nil {
		t.Fatalf("claimPostgresOwner() on another database = %v", err)
	}
	suffix, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	owned := postgresTestDatabasePrefix + owner.token + "_" + suffix
	dropTemplateOnCleanup(t, owned)
	// Registered last, so it runs before the cleanup that drops elsewhere.
	t.Cleanup(func() { _ = owner.conn.Close(context.Background()) })
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(owned)); err != nil {
		t.Fatalf("create %s: %v", owned, err)
	}
	if err := reclaimDeadOwnerDatabases(ctx, admin, postgresAdmin.owner.token); err != nil {
		t.Fatalf("reclaimDeadOwnerDatabases() = %v", err)
	}
	if exists, _ := breakHelperTemplateFlags(t, admin, owned); !exists {
		t.Fatal("reclaimed the database of a live owner connected to another database")
	}
	// Once that owner's session ends, its database is reclaimable.
	if err := owner.conn.Close(ctx); err != nil {
		t.Fatalf("close owner session: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := reclaimDeadOwnerDatabases(ctx, admin, postgresAdmin.owner.token); err != nil {
			t.Fatalf("reclaimDeadOwnerDatabases() = %v", err)
		}
		exists, _ := breakHelperTemplateFlags(t, admin, owned)
		if !exists {
			break
		}
		// The backend leaves pg_stat_activity shortly after the client
		// disconnects, not synchronously with Close.
		if time.Now().After(deadline) {
			t.Fatal("database of a closed owner session was never reclaimed")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
