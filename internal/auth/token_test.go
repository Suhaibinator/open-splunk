package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestCollectorTokenLifecycleStoresOnlyKeyedDigest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	for _, name := range []string{"main", "audit", "unrelated"} {
		if _, err := db.CreateIndex(ctx, activeIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}

	key := []byte("0123456789abcdef0123456789abcdef")
	store, err := NewStore(db, key)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "production collector",
		Description:       "writes two indexes",
		AllowedIndexNames: []string{" AUDIT ", "main", "main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken() error = %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	if !strings.HasPrefix(plaintext, collectorTokenPrefix) || len(plaintext) < 40 {
		t.Fatalf("issued plaintext has unexpected format (length=%d)", len(plaintext))
	}
	if issued.Token.Version != 1 ||
		issued.Token.State != CollectorTokenStateActive ||
		issued.Token.BoundCollectorID != testCollectorID {
		t.Fatalf("issued metadata = %#v", issued.Token)
	}
	if got, want := issued.Token.AllowedIndexNames, []string{"audit", "main"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("allowed indexes = %v, want %v", got, want)
	}

	for label, rendered := range map[string]string{
		"secret String":   fmt.Sprint(issued.Secret),
		"secret GoString": fmt.Sprintf("%#v", issued.Secret),
		"issued String":   fmt.Sprint(issued),
		"issued GoString": fmt.Sprintf("%#v", issued),
	} {
		if strings.Contains(rendered, plaintext) {
			t.Fatalf("%s leaked plaintext: %s", label, rendered)
		}
	}
	encoded, err := json.Marshal(issued)
	if err != nil {
		t.Fatalf("json.Marshal(issued): %v", err)
	}
	if strings.Contains(string(encoded), plaintext) {
		t.Fatalf("JSON leaked plaintext: %s", encoded)
	}

	var digest []byte
	var safePrefix string
	var boundCollectorID string
	if queryErr := db.SQLDB().QueryRowContext(ctx, `
		SELECT token_digest, token_prefix, bound_collector_id
		FROM ingestion_tokens
		WHERE ingestion_token_id = ?`, issued.Token.ID).
		Scan(&digest, &safePrefix, &boundCollectorID); queryErr != nil {
		t.Fatalf("read stored token: %v", queryErr)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(plaintext))
	wantDigest := mac.Sum(nil)
	if !hmac.Equal(digest, wantDigest) {
		t.Fatalf("stored digest = %s, want HMAC-SHA-256 %s", hex.EncodeToString(digest), hex.EncodeToString(wantDigest))
	}
	if string(digest) == plaintext || safePrefix == plaintext || !strings.HasPrefix(safePrefix, collectorTokenPrefix) {
		t.Fatal("stored token representation is not a safe digest/prefix")
	}
	if boundCollectorID != testCollectorID {
		t.Fatalf("stored collector binding = %q, want %q", boundCollectorID, testCollectorID)
	}
	rows, err := db.SQLDB().QueryContext(ctx, `SELECT name FROM pragma_table_info('ingestion_tokens')`)
	if err != nil {
		t.Fatalf("inspect ingestion_tokens columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if scanErr := rows.Scan(&column); scanErr != nil {
			t.Fatalf("scan ingestion_tokens column: %v", scanErr)
		}
		if column == "token" || column == "secret" || column == "plaintext" {
			t.Fatalf("ingestion_tokens contains plaintext-capable column %q", column)
		}
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		t.Fatalf("iterate ingestion_tokens columns: %v", iterationErr)
	}

	principal, err := store.Authorize(ctx, plaintext, " MAIN ")
	if err != nil {
		t.Fatalf("Authorize(main) error = %v", err)
	}
	if principal.TokenID != issued.Token.ID ||
		principal.IndexName != "main" ||
		principal.BoundCollectorID != testCollectorID {
		t.Fatalf("Authorize(main) = %#v", principal)
	}
	authentication, err := store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if authentication.TokenID != issued.Token.ID ||
		authentication.BoundCollectorID != testCollectorID ||
		fmt.Sprint(authentication.AuthorizedIndexNames()) != fmt.Sprint([]string{"audit", "main"}) {
		t.Fatalf("Authenticate() = %#v", authentication)
	}
	if _, authorizeErr := store.Authorize(ctx, plaintext, "unrelated"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(unrelated) error = %v, want ErrUnauthorized", authorizeErr)
	}
	if _, authorizeErr := store.Authorize(ctx, "attacker-controlled-secret", "main"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(bad token) error = %v, want ErrUnauthorized", authorizeErr)
	} else if strings.Contains(authorizeErr.Error(), "attacker-controlled-secret") {
		t.Fatalf("Authorize error leaked supplied token: %v", authorizeErr)
	}
	if _, updateErr := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET state = 'revoked'
		WHERE ingestion_token_id = ?`, issued.Token.ID); updateErr == nil {
		t.Fatal("revoked state without revoked_at unexpectedly succeeded")
	}
	if _, updateErr := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET revoked_at_unix_micro = created_at_unix_micro
		WHERE ingestion_token_id = ?`, issued.Token.ID); updateErr == nil {
		t.Fatal("active state with revoked_at unexpectedly succeeded")
	}
	if _, updateErr := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET expires_at_unix_micro = created_at_unix_micro
		WHERE ingestion_token_id = ?`, issued.Token.ID); updateErr == nil {
		t.Fatal("expiration at creation time unexpectedly succeeded")
	}

	revoked, err := store.RevokeCollectorToken(ctx, issued.Token.ID, issued.Token.Version)
	if err != nil {
		t.Fatalf("RevokeCollectorToken() error = %v", err)
	}
	if revoked.State != CollectorTokenStateRevoked || revoked.Version != 2 || revoked.RevokedAt.IsZero() {
		t.Fatalf("revoked token = %#v", revoked)
	}
	if _, authorizeErr := store.Authorize(ctx, plaintext, "main"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(revoked) error = %v, want ErrUnauthorized", authorizeErr)
	}
	if _, authenticateErr := store.Authenticate(ctx, plaintext); !errors.Is(authenticateErr, ErrUnauthorized) {
		t.Fatalf("Authenticate(revoked) error = %v, want ErrUnauthorized", authenticateErr)
	}
	if _, updateErr := db.SQLDB().ExecContext(ctx, `UPDATE ingestion_tokens SET state = 'active' WHERE ingestion_token_id = ?`, issued.Token.ID); updateErr == nil {
		t.Fatal("direct reactivation of revoked token unexpectedly succeeded")
	}
	if _, updateErr := db.SQLDB().ExecContext(ctx, `UPDATE ingestion_tokens SET token_digest = zeroblob(32) WHERE ingestion_token_id = ?`, issued.Token.ID); updateErr == nil {
		t.Fatal("direct mutation of token digest unexpectedly succeeded")
	}
	if _, revokeErr := store.RevokeCollectorToken(ctx, issued.Token.ID, issued.Token.Version); !errors.Is(revokeErr, control.ErrVersionConflict) {
		t.Fatalf("stale RevokeCollectorToken() error = %v, want ErrVersionConflict", revokeErr)
	}
}

func TestStoreCopiesDigestKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	store, err := NewStore(db, key)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "key-copy",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	for i := range key {
		key[i] = 0
	}
	if _, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "main"); authorizeErr != nil {
		t.Fatalf("Authorize() after caller key mutation = %v", authorizeErr)
	}
}

func TestAuthenticateRefreshesCurrentActiveScopes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	first, err := db.CreateIndex(ctx, activeIndex("first"))
	if err != nil {
		t.Fatalf("CreateIndex(first): %v", err)
	}
	second, err := db.CreateIndex(ctx, activeIndex("second"))
	if err != nil {
		t.Fatalf("CreateIndex(second): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "scope refresh",
		AllowedIndexNames: []string{"second", "first"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	authentication, err := store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(initial): %v", err)
	}
	if fmt.Sprint(authentication.AuthorizedIndexNames()) != fmt.Sprint([]string{"first", "second"}) {
		t.Fatalf("initial scopes = %v", authentication.AuthorizedIndexNames())
	}

	secondDefinition := second.Definition
	secondDefinition.IngestionEnabled = false
	if _, updateErr := db.UpdateIndex(ctx, second.ID, second.Version, secondDefinition); updateErr != nil {
		t.Fatalf("UpdateIndex(disable second): %v", updateErr)
	}
	authentication, err = store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(partial scope): %v", err)
	}
	if fmt.Sprint(authentication.AuthorizedIndexNames()) != fmt.Sprint([]string{"first"}) {
		t.Fatalf("refreshed scopes = %v, want [first]", authentication.AuthorizedIndexNames())
	}

	if _, stateErr := db.SetIndexState(ctx, first.ID, first.Version, control.IndexStateArchived); stateErr != nil {
		t.Fatalf("SetIndexState(archive first): %v", stateErr)
	}
	if _, authenticateErr := store.Authenticate(ctx, plaintext); !errors.Is(authenticateErr, ErrUnauthorized) {
		t.Fatalf("Authenticate(no active scopes) error = %v, want ErrUnauthorized", authenticateErr)
	}
}

func TestGetAndListCollectorTokensReturnSafeMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	first, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "first",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		ExpiresAt:         now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(first): %v", err)
	}
	store.now = func() time.Time { return now }
	second, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "second",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(second): %v", err)
	}

	got, err := store.GetCollectorToken(ctx, first.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if got.ID != first.Token.ID || got.Prefix != first.Token.Prefix || strings.Contains(fmt.Sprintf("%#v", got), first.Secret.Plaintext()) {
		t.Fatalf("GetCollectorToken() = %#v", got)
	}
	if _, getErr := store.GetCollectorToken(ctx, "missing"); !errors.Is(getErr, control.ErrNotFound) {
		t.Fatalf("GetCollectorToken(missing) error = %v, want ErrNotFound", getErr)
	}

	tokens, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(): %v", err)
	}
	wantIDs := []string{first.Token.ID, second.Token.ID}
	slices.Sort(wantIDs)
	if len(tokens) != 2 || tokens[0].ID != wantIDs[0] || tokens[1].ID != wantIDs[1] {
		t.Fatalf("ListCollectorTokens() = %#v", tokens)
	}
	store.now = func() time.Time { return now.Add(time.Hour) }
	expired, err := store.GetCollectorToken(ctx, first.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(expired): %v", err)
	}
	if expired.State != CollectorTokenStateExpired {
		t.Fatalf("expired state = %q, want %q", expired.State, CollectorTokenStateExpired)
	}
}

func TestUpdateCollectorTokenReplacesDefinitionAndScopesAtomically(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	for _, name := range []string{"first", "second"} {
		if _, err := db.CreateIndex(ctx, activeIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "before",
		Description:       "old",
		AllowedIndexNames: []string{"first"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	updated, err := store.UpdateCollectorToken(ctx, issued.Token.ID, issued.Token.Version, UpdateCollectorTokenRequest{
		Name: " after ", Description: "new", AllowedIndexNames: []string{"second"}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("UpdateCollectorToken(): %v", err)
	}
	if updated.Version != 2 || updated.Name != "after" || updated.Description != "new" ||
		fmt.Sprint(updated.AllowedIndexNames) != fmt.Sprint([]string{"second"}) || !updated.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("updated token = %#v", updated)
	}
	if _, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "first"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(old scope) error = %v, want ErrUnauthorized", authorizeErr)
	}
	if principal, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "second"); authorizeErr != nil || principal.TokenID != issued.Token.ID {
		t.Fatalf("Authorize(new scope) = %#v, %v", principal, authorizeErr)
	}

	if _, updateErr := store.UpdateCollectorToken(ctx, issued.Token.ID, 1, UpdateCollectorTokenRequest{
		Name: "stale", AllowedIndexNames: []string{"first"}, ExpiresAt: now.Add(time.Hour),
	}); !errors.Is(updateErr, control.ErrVersionConflict) {
		t.Fatalf("stale UpdateCollectorToken() error = %v, want ErrVersionConflict", updateErr)
	}
	current, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if current.Name != "after" || fmt.Sprint(current.AllowedIndexNames) != fmt.Sprint([]string{"second"}) {
		t.Fatalf("stale update mutated token: %#v", current)
	}
	cleared, err := store.UpdateCollectorToken(ctx, current.ID, current.Version, UpdateCollectorTokenRequest{
		Name: "without expiration", AllowedIndexNames: []string{"second"},
	})
	if err != nil {
		t.Fatalf("UpdateCollectorToken(clear expiration): %v", err)
	}
	if cleared.Version != 3 || !cleared.ExpiresAt.IsZero() {
		t.Fatalf("expiration was not cleared: %#v", cleared)
	}
}

func TestUpdateCollectorTokenRejectsInactiveOrInvalidReplacement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "token",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if _, updateErr := store.UpdateCollectorToken(ctx, issued.Token.ID, issued.Token.Version, UpdateCollectorTokenRequest{
		Name: "invalid", AllowedIndexNames: []string{"missing"},
	}); !errors.Is(updateErr, control.ErrInvalidArgument) {
		t.Fatalf("invalid scope error = %v, want ErrInvalidArgument", updateErr)
	}
	unchanged, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil || unchanged.Version != 1 || unchanged.Name != "token" {
		t.Fatalf("invalid update changed token = %#v, %v", unchanged, err)
	}

	revoked, err := store.RevokeCollectorToken(ctx, issued.Token.ID, issued.Token.Version)
	if err != nil {
		t.Fatalf("RevokeCollectorToken(): %v", err)
	}
	if _, updateErr := store.UpdateCollectorToken(ctx, issued.Token.ID, revoked.Version, UpdateCollectorTokenRequest{
		Name: "cannot-reactivate", AllowedIndexNames: []string{"main"},
	}); !errors.Is(updateErr, ErrInactiveToken) {
		t.Fatalf("revoked update error = %v, want ErrInactiveToken", updateErr)
	}

	shortLived, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "short",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		ExpiresAt:         now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(short): %v", err)
	}
	store.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, updateErr := store.UpdateCollectorToken(ctx, shortLived.Token.ID, shortLived.Token.Version, UpdateCollectorTokenRequest{
		Name: "cannot-extend", AllowedIndexNames: []string{"main"}, ExpiresAt: now.Add(time.Hour),
	}); !errors.Is(updateErr, ErrInactiveToken) {
		t.Fatalf("expired update error = %v, want ErrInactiveToken", updateErr)
	}
}

func TestCollectorTokenRandomnessFailuresAreSafe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	store.random = errorReader{err: errors.New("sensitive random source detail")}
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "failure",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err == nil {
		t.Fatal("CreateCollectorToken() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "sensitive random source detail") {
		t.Fatalf("CreateCollectorToken() exposed random-source detail: %v", err)
	}

	knownRandom := byte('x')
	store.random = &tokenThenErrorReader{tokenByte: knownRandom}
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "id failure",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err == nil {
		t.Fatal("CreateCollectorToken(ID randomness failure) unexpectedly succeeded")
	}
	knownPlaintext := collectorTokenPrefix + base64.RawURLEncoding.EncodeToString(bytesOf(knownRandom, tokenRandomBytes))
	if strings.Contains(err.Error(), knownPlaintext) {
		t.Fatalf("CreateCollectorToken() error leaked generated plaintext: %v", err)
	}
}

func TestCollectorTokensRequireExplicitActiveIngestionIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	active, err := db.CreateIndex(ctx, activeIndex("active"))
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	archived, err := db.CreateIndex(ctx, activeIndex("archived"))
	if err != nil {
		t.Fatalf("CreateIndex(archived): %v", err)
	}
	if _, stateErr := db.SetIndexState(ctx, archived.ID, archived.Version, control.IndexStateArchived); stateErr != nil {
		t.Fatalf("SetIndexState(archived): %v", stateErr)
	}
	disabledDef := activeIndex("disabled")
	disabledDef.IngestionEnabled = false
	if _, createErr := db.CreateIndex(ctx, disabledDef); createErr != nil {
		t.Fatalf("CreateIndex(disabled): %v", createErr)
	}

	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	for name, indexes := range map[string][]string{
		"empty":    nil,
		"unknown":  {"missing"},
		"archived": {"archived"},
		"disabled": {"disabled"},
	} {
		t.Run(name, func(t *testing.T) {
			_, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name:              name,
				AllowedIndexNames: indexes,
				BoundCollectorID:  testCollectorID,
			})
			if !errors.Is(createErr, control.ErrInvalidArgument) {
				t.Fatalf("CreateCollectorToken() error = %v, want ErrInvalidArgument", createErr)
			}
		})
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "valid",
		AllowedIndexNames: []string{"active"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(valid): %v", err)
	}
	if _, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "active"); authorizeErr != nil {
		t.Fatalf("Authorize(active): %v", authorizeErr)
	}
	definition := active.Definition
	definition.IngestionEnabled = false
	if _, updateErr := db.UpdateIndex(ctx, active.ID, active.Version, definition); updateErr != nil {
		t.Fatalf("UpdateIndex(disable ingestion): %v", updateErr)
	}
	if _, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "active"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(disabled active) error = %v, want ErrUnauthorized", authorizeErr)
	}
	if _, authenticateErr := store.Authenticate(ctx, issued.Secret.Plaintext()); !errors.Is(authenticateErr, ErrUnauthorized) {
		t.Fatalf("Authenticate(without active scope) error = %v, want ErrUnauthorized", authenticateErr)
	}
}

func TestCollectorTokenValidationExpirationAndRandomness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	if _, err := NewStore(db, []byte("too short")); !errors.Is(err, ErrInvalidDigestKey) {
		t.Fatalf("NewStore(short key) error = %v, want ErrInvalidDigestKey", err)
	}

	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "already expired",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		ExpiresAt:         now,
	}); !errors.Is(createErr, control.ErrInvalidArgument) {
		t.Fatalf("CreateCollectorToken(expired) error = %v, want ErrInvalidArgument", createErr)
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "short lived",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
		ExpiresAt:         now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(short lived): %v", err)
	}
	store.now = func() time.Time { return now.Add(time.Hour) }
	if _, authorizeErr := store.Authorize(ctx, issued.Secret.Plaintext(), "main"); !errors.Is(authorizeErr, ErrUnauthorized) {
		t.Fatalf("Authorize(expired) error = %v, want ErrUnauthorized", authorizeErr)
	}
	if _, authenticateErr := store.Authenticate(ctx, issued.Secret.Plaintext()); !errors.Is(authenticateErr, ErrUnauthorized) {
		t.Fatalf("Authenticate(expired) error = %v, want ErrUnauthorized", authenticateErr)
	}

	store.now = func() time.Time { return now }
	seen := make(map[string]struct{}, 128)
	for i := range 128 {
		issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("random-%03d", i),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", i, err)
		}
		plaintext := issued.Secret.Plaintext()
		if _, duplicate := seen[plaintext]; duplicate {
			t.Fatalf("duplicate randomly generated token at iteration %d", i)
		}
		seen[plaintext] = struct{}{}
	}
}

func TestCollectorTokenGORMModelsMatchMigratedSQLiteSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	tests := []struct {
		table string
		model any
	}{
		{table: "ingestion_tokens", model: &collectorTokenRecord{}},
		{table: "ingestion_token_indexes", model: &collectorTokenIndexRecord{}},
		{table: "ingestion_token_constraints", model: &collectorTokenConstraintRecord{}},
		{table: "ingestion_token_hec_profiles", model: &collectorTokenHECProfileRecord{}},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			statement := &gorm.Statement{DB: db.GORMDB()}
			if err := statement.Parse(test.model); err != nil {
				t.Fatalf("parse GORM model: %v", err)
			}

			rows, err := db.SQLDB().QueryContext(
				ctx,
				fmt.Sprintf("SELECT name FROM pragma_table_info('%s') ORDER BY cid", test.table),
			)
			if err != nil {
				t.Fatalf("read migrated columns: %v", err)
			}
			var migratedColumns []string
			for rows.Next() {
				var column string
				if err := rows.Scan(&column); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				migratedColumns = append(migratedColumns, column)
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("close migrated-column rows: %v", err)
			}
			if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
				t.Fatalf(
					"GORM %s columns = %v, migrated columns = %v",
					test.table,
					statement.Schema.DBNames,
					migratedColumns,
				)
			}

			switch test.table {
			case "ingestion_tokens":
				id := statement.Schema.LookUpField("IngestionTokenID")
				digest := statement.Schema.LookUpField("TokenDigest")
				if id == nil || !id.PrimaryKey || digest == nil || !digest.Unique {
					t.Fatalf("GORM token keys are not explicit: ID=%#v digest=%#v", id, digest)
				}
				binding := statement.Schema.LookUpField("BoundCollectorID")
				if binding == nil || binding.NotNull || binding.Unique {
					t.Fatalf("GORM collector binding must be nullable and non-unique: %#v", binding)
				}
				wantChecks := []string{
					"ingestion_tokens_bound_collector_id_canonical",
					"ingestion_tokens_digest_length",
					"ingestion_tokens_expiration_after_create",
					"ingestion_tokens_last_use_not_before_create",
					"ingestion_tokens_max_ingest_events_per_second_bounded",
					"ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded",
					"ingestion_tokens_name_length",
					"ingestion_tokens_prefix_length",
					"ingestion_tokens_purpose_supported",
					"ingestion_tokens_revocation_consistency",
					"ingestion_tokens_state",
					"ingestion_tokens_update_not_before_create",
					"ingestion_tokens_version_positive",
				}
				checks := statement.Schema.ParseCheckConstraints()
				gotChecks := make([]string, 0, len(checks))
				for name := range checks {
					gotChecks = append(gotChecks, name)
				}
				slices.Sort(gotChecks)
				if !slices.Equal(gotChecks, wantChecks) {
					t.Fatalf("GORM token checks = %v, want %v", gotChecks, wantChecks)
				}
			case "ingestion_token_indexes":
				primaryColumns := make([]string, len(statement.Schema.PrimaryFields))
				for index, field := range statement.Schema.PrimaryFields {
					primaryColumns[index] = field.DBName
				}
				if want := []string{"ingestion_token_id", "index_id"}; !slices.Equal(primaryColumns, want) {
					t.Fatalf("GORM token-scope primary key = %v, want %v", primaryColumns, want)
				}
				assertCollectorTokenScopeIndexParity(t, db, statement)
			case "ingestion_token_constraints":
				primaryColumns := make([]string, len(statement.Schema.PrimaryFields))
				for index, field := range statement.Schema.PrimaryFields {
					primaryColumns[index] = field.DBName
				}
				if want := []string{"ingestion_token_id", "constraint_kind", "ordinal"}; !slices.Equal(primaryColumns, want) {
					t.Fatalf("GORM token-constraint primary key = %v, want %v", primaryColumns, want)
				}
				checks := statement.Schema.ParseCheckConstraints()
				for _, name := range []string{
					"ingestion_token_constraints_kind",
					"ingestion_token_constraints_ordinal",
					"ingestion_token_constraints_pattern_valid",
				} {
					if _, present := checks[name]; !present {
						t.Fatalf("GORM token-constraint check %q is missing: %v", name, checks)
					}
				}
			}
		})
	}
}

func TestCollectorTokenStoreSurvivesDatabaseReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	key := []byte("0123456789abcdef0123456789abcdef")
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(first): %v", err)
	}
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		_ = db.Close()
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, key)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStore(first): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                 "persistent",
		Description:          "before reopen",
		AllowedIndexNames:    []string{"main"},
		BoundCollectorID:     testCollectorID,
		AllowedHostRegexes:   []string{"^before-host$"},
		AllowedSourceRegexes: []string{"^before-source$"},
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("CreateCollectorToken(): %v", err)
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
	got, err := reopenedStore.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(reopened): %v", err)
	}
	if got.ID != issued.Token.ID || got.Name != "persistent" ||
		got.BoundCollectorID != testCollectorID ||
		!slices.Equal(got.AllowedIndexNames, []string{"main"}) ||
		!slices.Equal(got.AllowedHostRegexes, []string{"^before-host$"}) ||
		!slices.Equal(got.AllowedSourceRegexes, []string{"^before-source$"}) {
		t.Fatalf("reopened token = %#v", got)
	}
	if _, err := reopenedStore.Authorize(ctx, issued.Secret.Plaintext(), "main"); err != nil {
		t.Fatalf("Authorize(reopened): %v", err)
	}
	authentication, err := reopenedStore.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(reopened): %v", err)
	}
	if !slices.Equal(authentication.AllowedHostRegexes, []string{"^before-host$"}) ||
		!slices.Equal(authentication.AllowedSourceRegexes, []string{"^before-source$"}) {
		t.Fatalf("reopened authentication constraints = %#v", authentication)
	}
	updated, err := reopenedStore.UpdateCollectorToken(
		ctx,
		got.ID,
		got.Version,
		UpdateCollectorTokenRequest{
			Name:                 "after reopen",
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^after-host$"},
			AllowedSourceRegexes: []string{"^after-source$"},
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken(reopened): %v", err)
	}
	if updated.Version != 2 ||
		updated.Name != "after reopen" ||
		updated.BoundCollectorID != testCollectorID ||
		!slices.Equal(updated.AllowedHostRegexes, []string{"^after-host$"}) ||
		!slices.Equal(updated.AllowedSourceRegexes, []string{"^after-source$"}) {
		t.Fatalf("updated reopened token = %#v", updated)
	}
}

func TestConcurrentCollectorTokenUpdatesHaveOneAtomicCASWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	for _, name := range []string{"alpha", "beta"} {
		if _, err := db.CreateIndex(ctx, activeIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	fixedNow := time.Date(2026, 7, 28, 9, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return fixedNow }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "before",
		AllowedIndexNames: []string{"alpha"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	const contenders = 8
	start := make(chan struct{})
	results := make(chan CollectorToken, contenders)
	errs := make(chan error, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		workers.Go(func() {
			<-start
			scope := "alpha"
			if contender%2 == 1 {
				scope = "beta"
			}
			updated, updateErr := store.UpdateCollectorToken(
				ctx,
				issued.Token.ID,
				issued.Token.Version,
				UpdateCollectorTokenRequest{
					Name:              fmt.Sprintf("winner-%d-%s", contender, scope),
					Description:       fmt.Sprintf("definition-%d", contender),
					AllowedIndexNames: []string{scope},
				},
			)
			if updateErr != nil {
				errs <- updateErr
				return
			}
			results <- updated
		})
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
		t.Fatalf("successful updates = %d, want 1: %#v", len(winners), winners)
	}
	conflicts := 0
	for updateErr := range errs {
		if !errors.Is(updateErr, control.ErrVersionConflict) {
			t.Fatalf("losing update error = %v, want ErrVersionConflict", updateErr)
		}
		conflicts++
	}
	if conflicts != contenders-1 {
		t.Fatalf("version conflicts = %d, want %d", conflicts, contenders-1)
	}

	current, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	winner := winners[0]
	if current.Version != 2 || current.Name != winner.Name ||
		current.Description != winner.Description ||
		!slices.Equal(current.AllowedIndexNames, winner.AllowedIndexNames) {
		t.Fatalf("persisted token = %#v, winning transaction = %#v", current, winner)
	}
}

func TestCollectorTokenScopeReplacementRollsBackOnPersistenceFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	for _, name := range []string{"before", "after"} {
		if _, err := db.CreateIndex(ctx, activeIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "original",
		Description:       "original definition",
		AllowedIndexNames: []string{"before"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER reject_replacement_scope
		BEFORE INSERT ON ingestion_token_indexes
		BEGIN
			SELECT RAISE(ABORT, 'forced token-scope failure');
		END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}

	if _, err := store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		UpdateCollectorTokenRequest{
			Name:              "mutated",
			Description:       "mutated definition",
			AllowedIndexNames: []string{"after"},
		},
	); err == nil {
		t.Fatal("UpdateCollectorToken() unexpectedly succeeded")
	}
	current, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if current.Version != issued.Token.Version ||
		current.Name != issued.Token.Name ||
		current.Description != issued.Token.Description ||
		!slices.Equal(current.AllowedIndexNames, issued.Token.AllowedIndexNames) {
		t.Fatalf("failed scope replacement was not atomic: %#v", current)
	}
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "before"); err != nil {
		t.Fatalf("Authorize(original scope): %v", err)
	}
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "after"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(rejected scope) error = %v, want ErrUnauthorized", err)
	}
}

func TestCollectorTokenStoreHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "context",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	operations := map[string]func() error{
		"authenticate": func() error {
			_, operationErr := store.Authenticate(canceled, issued.Secret.Plaintext())
			return operationErr
		},
		"authorize": func() error {
			_, operationErr := store.Authorize(canceled, issued.Secret.Plaintext(), "main")
			return operationErr
		},
		"create": func() error {
			_, operationErr := store.CreateCollectorToken(canceled, CreateCollectorTokenRequest{
				Name:              "canceled",
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			})
			return operationErr
		},
		"get": func() error {
			_, operationErr := store.GetCollectorToken(canceled, issued.Token.ID)
			return operationErr
		},
		"list": func() error {
			_, operationErr := store.ListCollectorTokens(canceled)
			return operationErr
		},
		"revoke": func() error {
			_, operationErr := store.RevokeCollectorToken(
				canceled,
				issued.Token.ID,
				issued.Token.Version,
			)
			return operationErr
		},
		"update": func() error {
			_, operationErr := store.UpdateCollectorToken(
				canceled,
				issued.Token.ID,
				issued.Token.Version,
				UpdateCollectorTokenRequest{
					Name:              "canceled",
					AllowedIndexNames: []string{"main"},
				},
			)
			return operationErr
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if operationErr := operation(); !errors.Is(operationErr, context.Canceled) {
				t.Fatalf("operation error = %v, want context.Canceled", operationErr)
			}
		})
	}
}

func assertCollectorTokenScopeIndexParity(
	t *testing.T,
	db *control.DB,
	statement *gorm.Statement,
) {
	t.Helper()
	const indexName = "ingestion_token_indexes_index_idx"
	var modelColumns []string
	for _, index := range statement.Schema.ParseIndexes() {
		if index.Name != indexName {
			continue
		}
		modelColumns = make([]string, len(index.Fields))
		for fieldIndex, option := range index.Fields {
			modelColumns[fieldIndex] = option.DBName
		}
	}
	want := []string{"index_id", "ingestion_token_id"}
	if !slices.Equal(modelColumns, want) {
		t.Fatalf("GORM %s columns = %v, want %v", indexName, modelColumns, want)
	}
	rows, err := db.SQLDB().QueryContext(
		context.Background(),
		fmt.Sprintf("SELECT name FROM pragma_index_info('%s') ORDER BY seqno", indexName),
	)
	if err != nil {
		t.Fatalf("read migrated index: %v", err)
	}
	var migratedColumns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		migratedColumns = append(migratedColumns, column)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated-index rows: %v", err)
	}
	if !slices.Equal(migratedColumns, want) {
		t.Fatalf("migrated %s columns = %v, want %v", indexName, migratedColumns, want)
	}
}

func BenchmarkAuthorizeCollectorToken(b *testing.B) {
	ctx := context.Background()
	db, err := control.Open(ctx, b.TempDir()+"/control.sqlite")
	if err != nil {
		b.Fatalf("control.Open(): %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, createErr := db.CreateIndex(ctx, activeIndex("main")); createErr != nil {
		b.Fatalf("CreateIndex(main): %v", createErr)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		b.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "benchmark",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		b.Fatalf("CreateCollectorToken(): %v", err)
	}
	plaintext := issued.Secret.Plaintext()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, authorizeErr := store.Authorize(ctx, plaintext, "main"); authorizeErr != nil {
			b.Fatalf("Authorize(): %v", authorizeErr)
		}
	}
}

func openControlDB(t *testing.T) *control.DB {
	t.Helper()
	return controlTestOpen(t)
}

func controlTestOpen(t *testing.T) *control.DB {
	t.Helper()
	db, err := control.Open(context.Background(), t.TempDir()+"/control.sqlite")
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("control DB Close(): %v", closeErr)
		}
	})
	return db
}

func activeIndex(name string) control.IndexDefinition {
	return control.IndexDefinition{
		Name:             name,
		DisplayName:      name,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}
}

type errorReader struct{ err error }

func (reader errorReader) Read([]byte) (int, error) { return 0, reader.err }

type tokenThenErrorReader struct {
	tokenByte byte
	served    bool
}

func (reader *tokenThenErrorReader) Read(buffer []byte) (int, error) {
	if reader.served {
		return 0, errors.New("ID random source failed")
	}
	reader.served = true
	for i := range buffer {
		buffer[i] = reader.tokenByte
	}
	return len(buffer), nil
}

func bytesOf(value byte, count int) []byte {
	buffer := make([]byte, count)
	for i := range buffer {
		buffer[i] = value
	}
	return buffer
}
