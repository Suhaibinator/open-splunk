package auth

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const testCollectorID = "123e4567-e89b-12d3-a456-426614174000"

func TestCreateCollectorTokenRequiresCanonicalCollectorBinding(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	index, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	valid := []string{
		"a",
		testCollectorID,
		"Collector_01:west.node",
		strings.Repeat("a", maximumCollectorIDBytes),
	}
	for position, collectorID := range valid {
		issued, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("valid-%d", position),
			AllowedIndexNames: []string{index.Definition.Name},
			BoundCollectorID:  collectorID,
		})
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%q): %v", collectorID, createErr)
		}
		if issued.Token.BoundCollectorID != collectorID {
			t.Fatalf("created binding = %q, want %q", issued.Token.BoundCollectorID, collectorID)
		}
		authentication, authenticateErr := store.Authenticate(ctx, issued.Secret.Plaintext())
		if authenticateErr != nil {
			t.Fatalf("Authenticate(%q): %v", collectorID, authenticateErr)
		}
		if authentication.BoundCollectorID != collectorID {
			t.Fatalf("authenticated binding = %q, want %q", authentication.BoundCollectorID, collectorID)
		}
	}

	invalid := []string{
		"",
		" leading",
		"trailing ",
		"-leading",
		".leading",
		"_leading",
		":leading",
		"contains/slash",
		"contains\ncontrol",
		"a\x00hidden",
		"collectör",
		strings.Repeat("a", maximumCollectorIDBytes+1),
	}
	for position, collectorID := range invalid {
		_, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("invalid-%d", position),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  collectorID,
		})
		if !errors.Is(createErr, control.ErrInvalidArgument) {
			t.Fatalf(
				"CreateCollectorToken(bound_collector_id=%q) error = %v, want ErrInvalidArgument",
				collectorID,
				createErr,
			)
		}
	}

	// Binding is deliberately not unique: overlapping credentials are needed
	// for safe collector-token rotation.
	duplicate, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "rotation token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(rotation): %v", err)
	}
	if duplicate.Token.BoundCollectorID != testCollectorID {
		t.Fatalf("rotation binding = %q, want %q", duplicate.Token.BoundCollectorID, testCollectorID)
	}
}

func TestLegacyUnboundCollectorTokenFailsClosedUntilOneWayBinding(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	index, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const plaintext = "ost_v1_legacy-unbound-secret"
	const tokenID = "tok_legacy_unbound"
	seedUnboundCollectorToken(t, db, store, index.ID, tokenID, plaintext, now)

	got, err := store.GetCollectorToken(ctx, tokenID)
	if err != nil {
		t.Fatalf("GetCollectorToken(legacy): %v", err)
	}
	if got.BoundCollectorID != "" {
		t.Fatalf("legacy binding = %q, want empty safe projection", got.BoundCollectorID)
	}
	listed, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(legacy): %v", err)
	}
	if len(listed) != 1 || listed[0].BoundCollectorID != "" {
		t.Fatalf("listed legacy token = %#v", listed)
	}
	if _, err := store.Authenticate(ctx, plaintext); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(unbound) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authorize(ctx, plaintext, "main"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(unbound) error = %v, want ErrUnauthorized", err)
	}
	if err := store.RecordCollectorTokenUse(ctx, tokenID, now.Add(time.Minute)); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(unbound) error = %v, want ErrInactiveToken", err)
	}

	bound, err := store.UpdateCollectorToken(ctx, tokenID, got.Version, UpdateCollectorTokenRequest{
		Name:              got.Name,
		Description:       got.Description,
		AllowedIndexNames: got.AllowedIndexNames,
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("UpdateCollectorToken(bind legacy): %v", err)
	}
	if bound.Version != got.Version+1 || bound.BoundCollectorID != testCollectorID {
		t.Fatalf("bound legacy token = %#v", bound)
	}
	authentication, err := store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(bound legacy): %v", err)
	}
	if authentication.BoundCollectorID != testCollectorID {
		t.Fatalf("bound authentication = %#v", authentication)
	}
	if err := store.RecordCollectorTokenUse(ctx, tokenID, now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordCollectorTokenUse(bound legacy): %v", err)
	}

	if _, err := store.UpdateCollectorToken(ctx, tokenID, bound.Version, UpdateCollectorTokenRequest{
		Name:              bound.Name,
		Description:       bound.Description,
		AllowedIndexNames: bound.AllowedIndexNames,
		BoundCollectorID:  "another-collector",
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("UpdateCollectorToken(change binding) error = %v, want ErrInvalidArgument", err)
	}

	// An omitted binding means "leave unchanged", not "clear". This lets
	// callers update unrelated safe metadata without weakening identity.
	preserved, err := store.UpdateCollectorToken(ctx, tokenID, bound.Version, UpdateCollectorTokenRequest{
		Name:              "renamed",
		Description:       bound.Description,
		AllowedIndexNames: bound.AllowedIndexNames,
	})
	if err != nil {
		t.Fatalf("UpdateCollectorToken(omitted binding): %v", err)
	}
	if preserved.BoundCollectorID != testCollectorID {
		t.Fatalf("omitted binding cleared collector identity: %#v", preserved)
	}

	for label, statement := range map[string]string{
		"clear":  `UPDATE ingestion_tokens SET bound_collector_id = NULL WHERE ingestion_token_id = ?`,
		"change": `UPDATE ingestion_tokens SET bound_collector_id = 'another-collector' WHERE ingestion_token_id = ?`,
	} {
		if _, err := db.SQLDB().ExecContext(ctx, statement, tokenID); err == nil {
			t.Fatalf("direct %s of immutable collector binding unexpectedly succeeded", label)
		}
	}
}

func TestConcurrentLegacyCollectorBindingHasOneCASWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	index, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 28, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const tokenID = "tok_legacy_concurrent"
	seedUnboundCollectorToken(t, db, store, index.ID, tokenID, "ost_v1_concurrent-secret", now)

	const contenders = 8
	start := make(chan struct{})
	results := make(chan CollectorToken, contenders)
	errs := make(chan error, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			result, updateErr := store.UpdateCollectorToken(
				ctx,
				tokenID,
				1,
				UpdateCollectorTokenRequest{
					Name:              "legacy",
					AllowedIndexNames: []string{"main"},
					BoundCollectorID:  fmt.Sprintf("collector-%d", contender),
				},
			)
			if updateErr != nil {
				errs <- updateErr
				return
			}
			results <- result
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errs)

	var winners []CollectorToken
	for result := range results {
		winners = append(winners, result)
	}
	if len(winners) != 1 {
		t.Fatalf("successful collector bindings = %d, want 1: %#v", len(winners), winners)
	}
	conflicts := 0
	for updateErr := range errs {
		if !errors.Is(updateErr, control.ErrVersionConflict) {
			t.Fatalf("losing binding error = %v, want ErrVersionConflict", updateErr)
		}
		conflicts++
	}
	if conflicts != contenders-1 {
		t.Fatalf("binding conflicts = %d, want %d", conflicts, contenders-1)
	}
	got, err := store.GetCollectorToken(ctx, tokenID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if got.Version != 2 || got.BoundCollectorID != winners[0].BoundCollectorID {
		t.Fatalf("persisted binding = %#v, winner = %#v", got, winners[0])
	}
}

func TestCollectorBindingSurvivesDatabaseReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	key := []byte("0123456789abcdef0123456789abcdef")
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(first): %v", err)
	}
	index, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, key)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStore(first): %v", err)
	}
	now := time.Date(2026, 7, 28, 16, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const tokenID = "tok_legacy_reopen"
	const plaintext = "ost_v1_reopen-secret"
	seedUnboundCollectorToken(t, db, store, index.ID, tokenID, plaintext, now)
	bound, err := store.UpdateCollectorToken(ctx, tokenID, 1, UpdateCollectorTokenRequest{
		Name:              "legacy",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("UpdateCollectorToken(bind): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(second): %v", err)
	}
	defer reopened.Close()
	reopenedStore, err := NewStore(reopened, key)
	if err != nil {
		t.Fatalf("NewStore(second): %v", err)
	}
	got, err := reopenedStore.GetCollectorToken(ctx, tokenID)
	if err != nil {
		t.Fatalf("GetCollectorToken(reopened): %v", err)
	}
	if got.BoundCollectorID != bound.BoundCollectorID {
		t.Fatalf("reopened binding = %q, want %q", got.BoundCollectorID, bound.BoundCollectorID)
	}
	authentication, err := reopenedStore.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(reopened): %v", err)
	}
	if authentication.BoundCollectorID != testCollectorID {
		t.Fatalf("reopened authentication = %#v", authentication)
	}
}

func TestCollectorBindingCorruptionFailsSafeDomainProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	index, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	const tokenID = "tok_corrupt_binding"
	const plaintext = "ost_v1_corrupt-secret"

	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable checks for corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro,
			bound_collector_id
		) VALUES (?, 1, 'corrupt', '', 'ost_v1_corrupt', ?, 'active', ?, ?, ?)`,
		tokenID,
		store.digest(plaintext),
		now.UnixMicro(),
		now.UnixMicro(),
		"invalid/collector",
	); err != nil {
		t.Fatalf("insert corrupt token: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES (?, ?)`,
		tokenID,
		index.ID,
	); err != nil {
		t.Fatalf("insert corrupt token scope: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore checks after corruption fixture: %v", err)
	}

	if _, err := store.GetCollectorToken(ctx, tokenID); err == nil ||
		!strings.Contains(err.Error(), "bound collector ID") {
		t.Fatalf("GetCollectorToken(corrupt binding) error = %v", err)
	}
	if _, err := store.ListCollectorTokens(ctx); err == nil ||
		!strings.Contains(err.Error(), "bound collector ID") {
		t.Fatalf("ListCollectorTokens(corrupt binding) error = %v", err)
	}
	if _, err := store.Authenticate(ctx, plaintext); err == nil ||
		errors.Is(err, ErrUnauthorized) ||
		!strings.Contains(err.Error(), "bound collector ID") {
		t.Fatalf("Authenticate(corrupt binding) error = %v, want internal corruption error", err)
	}
}

func seedUnboundCollectorToken(
	t *testing.T,
	db *control.DB,
	store *Store,
	indexID string,
	tokenID string,
	plaintext string,
	createdAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	var requiredTriggerSQL string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT sql
		FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'ingestion_token_collector_binding_is_required'`).
		Scan(&requiredTriggerSQL); err != nil {
		t.Fatalf("read collector-binding insert trigger: %v", err)
	}
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin legacy token fixture: %v", err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `
		DROP TRIGGER ingestion_token_collector_binding_is_required`); err != nil {
		t.Fatalf("temporarily remove collector-binding insert trigger: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, 1, 'legacy', '', 'ost_v1_legacy', ?, 'active', ?, ?)`,
		tokenID,
		store.digest(plaintext),
		createdAt.UnixMicro(),
		createdAt.UnixMicro(),
	); err != nil {
		t.Fatalf("seed unbound token: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES (?, ?)`,
		tokenID,
		indexID,
	); err != nil {
		t.Fatalf("seed unbound token scope: %v", err)
	}
	if _, err := tx.ExecContext(ctx, requiredTriggerSQL); err != nil {
		t.Fatalf("restore collector-binding insert trigger: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit legacy token fixture: %v", err)
	}
	finished = true
}
