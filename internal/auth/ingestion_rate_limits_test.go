package auth

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func TestCollectorTokenRateLimitsRoundTripAndAuthenticateFreshIndexPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	definition := activeIndex("main")
	definition.IngestionRateLimits = ingestquota.Limits{
		MaxEventsPerSecond:            200,
		MaxUncompressedBytesPerSecond: 2 << 20,
	}
	createdIndex, err := database.CreateIndex(ctx, definition)
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	tokenLimits := ingestquota.Limits{
		MaxEventsPerSecond:            100,
		MaxUncompressedBytesPerSecond: 1 << 20,
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                "rate-limited collector",
		AllowedIndexNames:   []string{"main"},
		BoundCollectorID:    testCollectorID,
		IngestionRateLimits: tokenLimits,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if issued.Token.IngestionRateLimits != tokenLimits {
		t.Fatalf("issued limits = %+v, want %+v", issued.Token.IngestionRateLimits, tokenLimits)
	}
	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if got.IngestionRateLimits != tokenLimits {
		t.Fatalf("stored limits = %+v, want %+v", got.IngestionRateLimits, tokenLimits)
	}

	authentication, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(): %v", err)
	}
	if authentication.TokenRateLimits != tokenLimits ||
		len(authentication.AuthorizedIndexes) != 1 ||
		authentication.AuthorizedIndexes[0].IngestionRateLimits !=
			definition.IngestionRateLimits {
		t.Fatalf("authentication rate policy = %+v", authentication)
	}

	updatedTokenLimits := ingestquota.Limits{MaxEventsPerSecond: 50}
	updated, err := store.UpdateCollectorToken(
		ctx,
		got.ID,
		got.Version,
		UpdateCollectorTokenRequest{
			Name:                got.Name,
			Description:         got.Description,
			AllowedIndexNames:   got.AllowedIndexNames,
			BoundCollectorID:    got.BoundCollectorID,
			ExpiresAt:           got.ExpiresAt,
			IngestionRateLimits: updatedTokenLimits,
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken(): %v", err)
	}
	if updated.IngestionRateLimits != updatedTokenLimits {
		t.Fatalf("updated limits = %+v, want %+v", updated.IngestionRateLimits, updatedTokenLimits)
	}

	updatedDefinition := createdIndex.Definition
	updatedDefinition.IngestionRateLimits = ingestquota.Limits{
		MaxUncompressedBytesPerSecond: 512 << 10,
	}
	updatedIndex, err := database.UpdateIndex(
		ctx,
		createdIndex.ID,
		createdIndex.Version,
		updatedDefinition,
	)
	if err != nil {
		t.Fatalf("UpdateIndex(): %v", err)
	}
	fresh, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(updated): %v", err)
	}
	if fresh.TokenRateLimits != updatedTokenLimits ||
		len(fresh.AuthorizedIndexes) != 1 ||
		!reflect.DeepEqual(
			fresh.AuthorizedIndexes[0].IngestionRateLimits,
			updatedIndex.Definition.IngestionRateLimits,
		) {
		t.Fatalf("fresh rate policy = %+v", fresh)
	}
}

func TestCollectorTokenRateLimitsRejectInvalidAndCorruptPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "invalid",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		IngestionRateLimits: ingestquota.Limits{
			MaxEventsPerSecond: ingestquota.HardMaxEventsPerSecond + 1,
		},
	})
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("invalid create error = %v, want ErrInvalidArgument", err)
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "valid",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(valid): %v", err)
	}
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("Conn(): %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("ignore checks: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE ingestion_tokens
		SET max_ingest_events_per_second = ?
		WHERE ingestion_token_id = ?`,
		int64(ingestquota.HardMaxEventsPerSecond+1),
		issued.Token.ID,
	); err != nil {
		t.Fatalf("corrupt token rate: %v", err)
	}
	if _, err := store.Authenticate(ctx, issued.Secret.Plaintext()); err == nil || !strings.Contains(err.Error(), "invalid rate limits") {
		t.Fatalf("Authenticate(corrupt) error = %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil || !strings.Contains(err.Error(), "invalid collector token rate limits") {
		t.Fatalf("GetCollectorToken(corrupt) error = %v", err)
	}
}

func TestCollectorTokenPruningReclaimsDurableQuotaBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	store, err := NewStoreWithOptions(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 1},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	create := func(name string) IssuedCollectorToken {
		t.Helper()
		issued, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              name,
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%q): %v", name, createErr)
		}
		if _, insertErr := database.SQLDB().ExecContext(ctx, `
			INSERT INTO ingest_quota_buckets (
				tenant_id, scope_kind, scope_id,
				max_ingest_events_per_second,
				max_ingest_uncompressed_bytes_per_second,
				next_event_admission_unix_nano,
				next_byte_admission_unix_nano,
				updated_at_unix_micro,
				token_owner_id
			) VALUES ('tenant-a', 'token', ?, 1, 0, 1, 0, 1, ?)`,
			issued.Token.ID,
			issued.Token.ID,
		); insertErr != nil {
			t.Fatalf("seed quota bucket for %q: %v", name, insertErr)
		}
		return issued
	}

	first := create("first rotating token")
	if _, err := store.RevokeCollectorToken(
		ctx,
		first.Token.ID,
		first.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}
	second := create("second rotating token")
	if _, err := store.RevokeCollectorToken(
		ctx,
		second.Token.ID,
		second.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(second): %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, first.Token.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetCollectorToken(pruned first) error = %v, want ErrNotFound", err)
	}

	for tokenID, want := range map[string]int{
		first.Token.ID:  0,
		second.Token.ID: 1,
	} {
		var count int
		if err := database.SQLDB().QueryRowContext(ctx, `
			SELECT count(*)
			FROM ingest_quota_buckets
			WHERE scope_kind = 'token' AND scope_id = ?`, tokenID,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("quota bucket count for token %q = %d, want %d", tokenID, count, want)
		}
	}
}
