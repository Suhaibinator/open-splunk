package knowledgecatalog

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProjectionPortalCorruptionKeepsCallerContextAndReleasesPool(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	pool := database.SQLDB()
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := readProjectionRecordsBounded(database.GORMDB().WithContext(ctx).Raw("SELECT 0::bigint AS width_knowledge_object_id_bytes"), 1, maximumProjectionBytes)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt projection error=%v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("reader canceled caller context: %v", ctx.Err())
	}
	probe, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := pool.PingContext(probe); err != nil {
		t.Fatalf("reader retained the only pool connection: %v", err)
	}
}

func TestProjectionPortalBorrowsAndClosesWithinAmbientTransaction(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	tx := database.GORMDB().WithContext(t.Context()).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	portal, err := newProjectionPortal(tx.Raw("SELECT 7 AS value"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if portal.ownTransaction {
		t.Fatal("reader replaced the caller's metadata transaction")
	}
	rows, err := portal.Fetch(1)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("portal did not return its row")
	}
	var value int
	if err := rows.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 7 || rows.Next() {
		t.Fatalf("unexpected portal result value%d", value)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	portal.Close()
	var open int64
	if err := tx.Raw("SELECT count(*) FROM pg_cursors WHERE name LIKE 'os_projection_%'").Scan(&open).Error; err != nil {
		t.Fatalf("reader invalidated caller transaction: %v", err)
	}
	if open != 0 {
		t.Fatalf("reader retained %d metadata portals", open)
	}
}
