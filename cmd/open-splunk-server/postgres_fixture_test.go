package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

var runtimeDatabaseFixtures sync.Map

// Each named restart fixture uses one disposable PostgreSQL schema. Reopening
// that fixture tests persisted state without touching a configured deployment.
func runtimeDatabaseDSN(t testing.TB, fixture string) string {
	t.Helper()
	if strings.HasPrefix(fixture, "postgres://") || strings.HasPrefix(fixture, "postgresql://") {
		return fixture
	}
	key := t.Name() + "\x00" + fixture
	if dsn, ok := runtimeDatabaseFixtures.Load(key); ok {
		return dsn.(string)
	}
	dsn := pgtest.DSN(t)
	runtimeDatabaseFixtures.Store(key, dsn)
	t.Cleanup(func() { runtimeDatabaseFixtures.Delete(key) })
	return dsn
}

func openTestControl(t testing.TB, ctx context.Context, fixture string) (*control.DB, error) {
	t.Helper()
	return control.Open(ctx, runtimeDatabaseDSN(t, fixture))
}
