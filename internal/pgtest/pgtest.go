// Package pgtest provides isolated PostgreSQL schemas for real database tests.
// It never deletes an existing schema or silently skips an unavailable database.
package pgtest

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// DSN creates a disposable schema and returns a PostgreSQL connection string
// pinned to it. OPEN_SPLUNK_TEST_POSTGRES_DSN must select an isolated test server.
func DSN(t testing.TB) string {
	t.Helper()
	source := os.Getenv("OPEN_SPLUNK_TEST_POSTGRES_DSN")
	if source == "" {
		t.Fatal("OPEN_SPLUNK_TEST_POSTGRES_DSN is required for PostgreSQL tests")
	}
	cfg, err := pgx.ParseConfig(source)
	if err != nil {
		t.Fatal("parse test PostgreSQL connection string:", err)
	}
	admin := stdlib.OpenDB(*cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatal("connect to required test PostgreSQL:", err)
	}
	schema := "os_test_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = admin.Close()
		t.Fatal("create isolated PostgreSQL test schema:", err)
	}
	t.Cleanup(func() {
		forgetCorruptedConstraints(schema)
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop isolated PostgreSQL test schema: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close PostgreSQL fixture: %v", err)
		}
	})
	// PGX's generated connection string quotes runtime parameters safely.
	cfg.RuntimeParams["search_path"] = quoted
	if strings.HasPrefix(source, "postgres://") || strings.HasPrefix(source, "postgresql://") {
		parsed, err := url.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	return source + " search_path=" + schema
}

// Open returns an un-migrated pool in a fresh isolated schema.
func Open(t testing.TB) *sql.DB {
	t.Helper()
	cfg, err := pgx.ParseConfig(DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL fixture pool: %v", err)
		}
	})
	return db
}

// EmptyCopy creates an isolated database with the source fixture's schema name.
// Native pg_dump archives preserve schema identities, so a restore test uses a
// separate database rather than rewriting names or resetting its source.
func EmptyCopy(t testing.TB, sourceDSN string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	schema := strings.Trim(cfg.RuntimeParams["search_path"], "\"")
	if !regexp.MustCompile(`^os_test_[a-f0-9_]+$`).MatchString(schema) {
		t.Fatal("restore source must be an isolated pgtest schema")
	}
	adminConfig, err := pgx.ParseConfig(os.Getenv("OPEN_SPLUNK_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*adminConfig)
	name := "os_restore_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	quotedName := pgx.Identifier{name}.Sanitize()
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedName+" TEMPLATE template0"); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+quotedName+" WITH (FORCE)"); err != nil {
			t.Errorf("drop isolated restore database: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close restore fixture admin pool: %v", err)
		}
	})
	cfg.Database = name
	destination := stdlib.OpenDB(*cfg)
	if _, err := destination.ExecContext(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		_ = destination.Close()
		t.Fatal(err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(sourceDSN, "postgres://") || strings.HasPrefix(sourceDSN, "postgresql://") {
		parsed, err := url.Parse(sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		parsed.Path = "/" + name
		parsed.RawPath = ""
		return parsed.String()
	}
	// PGX ConnString retains the original parsed text; mutating cfg.Database
	// changes OpenDB but does not serialize a new connection string.
	return sourceDSN + " dbname=" + name
}
