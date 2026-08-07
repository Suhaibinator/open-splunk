package knowledgecatalog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestReadCatalogStateUsesOneBoundedQuery(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	head := ensureIntegrationRevisionZeroTenant(t, database)

	state, readErr, queryCount := countedCatalogStateRead(t, database)
	if readErr != nil {
		t.Fatalf("read valid catalog state: %v", readErr)
	}
	if !state.found || state.revision != head.revision || state.token == "" || queryCount != 1 {
		t.Fatalf(
			"valid catalog state = %#v with %d queries, want found revision %d in one query",
			state,
			queryCount,
			head.revision,
		)
	}

	dropIntegrationTableTriggers(t, database, "knowledge_catalog_revision_heads")
	connection := integrationCorruptionConnection(t, database)
	if _, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_catalog_revision_heads
		SET state_token = zeroblob(?)
		WHERE tenant_id = ?
	`, maximumDefinitionBytes*2, testTenant); err != nil {
		closeIntegrationCorruptionConnection(t, connection)
		t.Fatalf("write oversized revision-head token: %v", err)
	}
	closeIntegrationCorruptionConnection(t, connection)

	state, readErr, queryCount = countedCatalogStateRead(t, database)
	if !errors.Is(readErr, ErrCorrupt) {
		t.Fatalf("oversized catalog-state error = %v, want ErrCorrupt", readErr)
	}
	if state != (catalogState{}) || queryCount != 1 {
		t.Fatalf("oversized catalog state = %#v with %d queries, want zero in one bounded query", state, queryCount)
	}
}

func countedCatalogStateRead(t *testing.T, database *control.DB) (catalogState, error, int64) {
	t.Helper()
	var queryCount atomic.Int64
	callbackName := "test:count-catalog-state-query"
	if err := database.GORMDB().Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(*gorm.DB) { queryCount.Add(1) },
	); err != nil {
		t.Fatalf("register catalog-state query counter: %v", err)
	}
	state, err := readCatalogState(database.GORMDB(), testTenant)
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove catalog-state query counter: %v", removeErr)
	}
	return state, err, queryCount.Load()
}
