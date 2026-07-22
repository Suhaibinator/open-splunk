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
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
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
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken() error = %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	if !strings.HasPrefix(plaintext, collectorTokenPrefix) || len(plaintext) < 40 {
		t.Fatalf("issued plaintext has unexpected format (length=%d)", len(plaintext))
	}
	if issued.Token.Version != 1 || issued.Token.State != CollectorTokenStateActive {
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
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT token_digest, token_prefix
		FROM ingestion_tokens
		WHERE ingestion_token_id = ?`, issued.Token.ID).Scan(&digest, &safePrefix); err != nil {
		t.Fatalf("read stored token: %v", err)
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
	rows, err := db.SQLDB().QueryContext(ctx, `SELECT name FROM pragma_table_info('ingestion_tokens')`)
	if err != nil {
		t.Fatalf("inspect ingestion_tokens columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan ingestion_tokens column: %v", err)
		}
		if column == "token" || column == "secret" || column == "plaintext" {
			t.Fatalf("ingestion_tokens contains plaintext-capable column %q", column)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ingestion_tokens columns: %v", err)
	}

	principal, err := store.Authorize(ctx, plaintext, " MAIN ")
	if err != nil {
		t.Fatalf("Authorize(main) error = %v", err)
	}
	if principal.TokenID != issued.Token.ID || principal.IndexName != "main" {
		t.Fatalf("Authorize(main) = %#v", principal)
	}
	authentication, err := store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if authentication.TokenID != issued.Token.ID || fmt.Sprint(authentication.AllowedIndexNames) != fmt.Sprint([]string{"audit", "main"}) {
		t.Fatalf("Authenticate() = %#v", authentication)
	}
	if _, err := store.Authorize(ctx, plaintext, "unrelated"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(unrelated) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authorize(ctx, "attacker-controlled-secret", "main"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(bad token) error = %v, want ErrUnauthorized", err)
	} else if strings.Contains(err.Error(), "attacker-controlled-secret") {
		t.Fatalf("Authorize error leaked supplied token: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET state = 'revoked'
		WHERE ingestion_token_id = ?`, issued.Token.ID); err == nil {
		t.Fatal("revoked state without revoked_at unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET revoked_at_unix_micro = created_at_unix_micro
		WHERE ingestion_token_id = ?`, issued.Token.ID); err == nil {
		t.Fatal("active state with revoked_at unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens SET expires_at_unix_micro = created_at_unix_micro
		WHERE ingestion_token_id = ?`, issued.Token.ID); err == nil {
		t.Fatal("expiration at creation time unexpectedly succeeded")
	}

	revoked, err := store.RevokeCollectorToken(ctx, issued.Token.ID, issued.Token.Version)
	if err != nil {
		t.Fatalf("RevokeCollectorToken() error = %v", err)
	}
	if revoked.State != CollectorTokenStateRevoked || revoked.Version != 2 || revoked.RevokedAt.IsZero() {
		t.Fatalf("revoked token = %#v", revoked)
	}
	if _, err := store.Authorize(ctx, plaintext, "main"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(revoked) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authenticate(ctx, plaintext); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(revoked) error = %v, want ErrUnauthorized", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `UPDATE ingestion_tokens SET state = 'active' WHERE ingestion_token_id = ?`, issued.Token.ID); err == nil {
		t.Fatal("direct reactivation of revoked token unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `UPDATE ingestion_tokens SET token_digest = zeroblob(32) WHERE ingestion_token_id = ?`, issued.Token.ID); err == nil {
		t.Fatal("direct mutation of token digest unexpectedly succeeded")
	}
	if _, err := store.RevokeCollectorToken(ctx, issued.Token.ID, issued.Token.Version); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale RevokeCollectorToken() error = %v, want ErrVersionConflict", err)
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
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: "key-copy", AllowedIndexNames: []string{"main"}})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	for i := range key {
		key[i] = 0
	}
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "main"); err != nil {
		t.Fatalf("Authorize() after caller key mutation = %v", err)
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
		Name: "scope refresh", AllowedIndexNames: []string{"second", "first"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	plaintext := issued.Secret.Plaintext()
	authentication, err := store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(initial): %v", err)
	}
	if fmt.Sprint(authentication.AllowedIndexNames) != fmt.Sprint([]string{"first", "second"}) {
		t.Fatalf("initial scopes = %v", authentication.AllowedIndexNames)
	}

	secondDefinition := second.Definition
	secondDefinition.IngestionEnabled = false
	if _, err := db.UpdateIndex(ctx, second.ID, second.Version, secondDefinition); err != nil {
		t.Fatalf("UpdateIndex(disable second): %v", err)
	}
	authentication, err = store.Authenticate(ctx, plaintext)
	if err != nil {
		t.Fatalf("Authenticate(partial scope): %v", err)
	}
	if fmt.Sprint(authentication.AllowedIndexNames) != fmt.Sprint([]string{"first"}) {
		t.Fatalf("refreshed scopes = %v, want [first]", authentication.AllowedIndexNames)
	}

	if _, err := db.SetIndexState(ctx, first.ID, first.Version, control.IndexStateArchived); err != nil {
		t.Fatalf("SetIndexState(archive first): %v", err)
	}
	if _, err := store.Authenticate(ctx, plaintext); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(no active scopes) error = %v, want ErrUnauthorized", err)
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
		Name: "first", AllowedIndexNames: []string{"main"}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(first): %v", err)
	}
	store.now = func() time.Time { return now.Add(time.Microsecond) }
	second, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name: "second", AllowedIndexNames: []string{"main"},
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
	if _, err := store.GetCollectorToken(ctx, "missing"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetCollectorToken(missing) error = %v, want ErrNotFound", err)
	}

	tokens, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(): %v", err)
	}
	if len(tokens) != 2 || tokens[0].ID != first.Token.ID || tokens[1].ID != second.Token.ID {
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
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: "failure", AllowedIndexNames: []string{"main"}})
	if err == nil {
		t.Fatal("CreateCollectorToken() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "sensitive random source detail") {
		t.Fatalf("CreateCollectorToken() exposed random-source detail: %v", err)
	}

	knownRandom := byte('x')
	store.random = &tokenThenErrorReader{tokenByte: knownRandom}
	_, err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: "id failure", AllowedIndexNames: []string{"main"}})
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
	if _, err := db.SetIndexState(ctx, archived.ID, archived.Version, control.IndexStateArchived); err != nil {
		t.Fatalf("SetIndexState(archived): %v", err)
	}
	disabledDef := activeIndex("disabled")
	disabledDef.IngestionEnabled = false
	if _, err := db.CreateIndex(ctx, disabledDef); err != nil {
		t.Fatalf("CreateIndex(disabled): %v", err)
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
			_, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: name, AllowedIndexNames: indexes})
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("CreateCollectorToken() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: "valid", AllowedIndexNames: []string{"active"}})
	if err != nil {
		t.Fatalf("CreateCollectorToken(valid): %v", err)
	}
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "active"); err != nil {
		t.Fatalf("Authorize(active): %v", err)
	}
	definition := active.Definition
	definition.IngestionEnabled = false
	if _, err := db.UpdateIndex(ctx, active.ID, active.Version, definition); err != nil {
		t.Fatalf("UpdateIndex(disable ingestion): %v", err)
	}
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "active"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(disabled active) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authenticate(ctx, issued.Secret.Plaintext()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(without active scope) error = %v, want ErrUnauthorized", err)
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
	if _, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name: "already expired", AllowedIndexNames: []string{"main"}, ExpiresAt: now,
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("CreateCollectorToken(expired) error = %v, want ErrInvalidArgument", err)
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name: "short lived", AllowedIndexNames: []string{"main"}, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(short lived): %v", err)
	}
	store.now = func() time.Time { return now.Add(time.Hour) }
	if _, err := store.Authorize(ctx, issued.Secret.Plaintext(), "main"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(expired) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authenticate(ctx, issued.Secret.Plaintext()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(expired) error = %v, want ErrUnauthorized", err)
	}

	store.now = func() time.Time { return now }
	seen := make(map[string]struct{}, 128)
	for i := 0; i < 128; i++ {
		issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name: fmt.Sprintf("random-%03d", i), AllowedIndexNames: []string{"main"},
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

func BenchmarkAuthorizeCollectorToken(b *testing.B) {
	ctx := context.Background()
	db, err := control.Open(ctx, b.TempDir()+"/control.sqlite")
	if err != nil {
		b.Fatalf("control.Open(): %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		b.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		b.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{Name: "benchmark", AllowedIndexNames: []string{"main"}})
	if err != nil {
		b.Fatalf("CreateCollectorToken(): %v", err)
	}
	plaintext := issued.Secret.Plaintext()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Authorize(ctx, plaintext, "main"); err != nil {
			b.Fatalf("Authorize(): %v", err)
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
		if err := db.Close(); err != nil {
			t.Errorf("control DB Close(): %v", err)
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
