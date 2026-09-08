package auth

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestAuthenticateHECRejectsCredentialsWithoutSQLiteWriterAdmission(t *testing.T) {
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
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	create := func(name string, purpose IngestionTokenPurpose) IssuedCollectorToken {
		t.Helper()
		request := CreateCollectorTokenRequest{
			Name:              name,
			Purpose:           purpose,
			AllowedIndexNames: []string{"main"},
		}
		if purpose == IngestionTokenPurposeNativeCollector {
			request.BoundCollectorID = testCollectorID
		}
		issued, createErr := store.CreateCollectorToken(ctx, request)
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, createErr)
		}
		return issued
	}
	disabled := create("disabled", IngestionTokenPurposeHEC)
	expired := create("expired", IngestionTokenPurposeHEC)
	revoked := create("revoked", IngestionTokenPurposeHEC)
	scopeLess := create("scope-less", IngestionTokenPurposeHEC)
	native := create("native", IngestionTokenPurposeNativeCollector)
	valid := create("valid", IngestionTokenPurposeHEC)
	if _, err := store.RevokeCollectorToken(ctx, revoked.Token.ID, revoked.Token.Version); err != nil {
		t.Fatalf("revoke HEC credential: %v", err)
	}
	for _, mutation := range []struct {
		statement string
		arguments []any
	}{
		{`UPDATE ingestion_tokens SET state = 'disabled' WHERE ingestion_token_id = ?`, []any{disabled.Token.ID}},
		{`UPDATE ingestion_tokens SET expires_at_unix_micro = ? WHERE ingestion_token_id = ?`, []any{now.Add(time.Minute).UnixMicro(), expired.Token.ID}},
		{`DELETE FROM ingestion_token_indexes WHERE ingestion_token_id = ?`, []any{scopeLess.Token.ID}},
	} {
		if _, err := db.SQLDB().ExecContext(ctx, mutation.statement, mutation.arguments...); err != nil {
			t.Fatalf("prepare rejected credential: %v", err)
		}
	}
	store.now = func() time.Time { return now.Add(2 * time.Minute) }

	// Keep the only writer reserved. A credential rejection must still finish
	// through a read snapshot, even when its scope fails after token lookup.
	db.SQLDB().SetMaxOpenConns(2)
	writer, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire writer connection: %v", err)
	}
	t.Cleanup(func() {
		_, _ = writer.ExecContext(context.Background(), `ROLLBACK`)
		_ = writer.Close()
	})
	if _, err := writer.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("reserve SQLite writer: %v", err)
	}
	reader, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire credential reader: %v", err)
	}
	if _, err := reader.ExecContext(ctx, `PRAGMA busy_timeout = 1`); err != nil {
		_ = reader.Close()
		t.Fatalf("bound unexpected writer contention: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("release credential reader: %v", err)
	}
	for _, test := range []struct {
		name       string
		credential string
		want       error
	}{
		{"empty", "", ErrUnauthorized},
		{"unknown", "ost_unknown-credential", ErrUnauthorized},
		{"disabled", disabled.Secret.Plaintext(), ErrInactiveToken},
		{"expired", expired.Secret.Plaintext(), ErrUnauthorized},
		{"revoked", revoked.Secret.Plaintext(), ErrUnauthorized},
		{"wrong purpose", native.Secret.Plaintext(), ErrUnauthorized},
		{"unusable scope", scopeLess.Secret.Plaintext(), ErrUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			authentication, authErr := store.AuthenticateHEC(ctx, test.credential)
			if !errors.Is(authErr, test.want) || control.IsDatabaseContention(authErr) || authentication.TokenID != "" {
				t.Fatalf("rejected authentication = %q, %v; want empty identity and %v without writer contention", authentication.TokenID, authErr, test.want)
			}
		})
	}
	if _, err := store.AuthenticateHEC(ctx, valid.Secret.Plaintext()); !errors.Is(err, ErrHECAuthenticationTemporarilyUnavailable) || !control.IsDatabaseContention(err) {
		t.Fatalf("valid contended authentication = %v, want temporary writer contention", err)
	}
	var observations int64
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM ingestion_tokens WHERE last_used_at_unix_micro IS NOT NULL`).Scan(&observations); err != nil {
		t.Fatalf("read token use observations: %v", err)
	}
	if observations != 0 {
		t.Fatalf("rejected authentication wrote %d last-use observations", observations)
	}
	admissionRejected := errors.New("request capacity unavailable")
	if _, err := store.AuthenticateHECWithAdmission(ctx, valid.Secret.Plaintext(), func(identity Authentication) (context.Context, error) {
		if identity.TokenID != valid.Token.ID {
			t.Fatalf("admission received unexpected identity %q", identity.TokenID)
		}
		return nil, admissionRejected
	}); !errors.Is(err, admissionRejected) || control.IsDatabaseContention(err) {
		t.Fatalf("admission rejection reached the SQLite writer: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.AuthenticateHECWithAdmission(ctx, valid.Secret.Plaintext(), func(Authentication) (context.Context, error) {
		return canceled, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("final authentication ignored its admitted context: %v", err)
	}
}

func TestAuthenticateHECRechecksAuthorityAfterReadOnlyPreflight(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		statement string
		value     any
		want      error
		integrity bool
	}{
		{"disabled", `UPDATE ingestion_tokens SET state = ? WHERE ingestion_token_id = ?`, "disabled", ErrInactiveToken, false},
		{"revoked", `UPDATE ingestion_tokens SET state = 'revoked', revoked_at_unix_micro = ? WHERE ingestion_token_id = ?`, createdAt.Add(time.Minute).UnixMicro(), ErrUnauthorized, false},
		{"expired", `UPDATE ingestion_tokens SET expires_at_unix_micro = ? WHERE ingestion_token_id = ?`, createdAt.Add(time.Minute).UnixMicro(), ErrUnauthorized, false},
		{"scope removed", `DELETE FROM ingestion_token_indexes WHERE ingestion_token_id = ?`, nil, ErrUnauthorized, false},
		{"profile widened", `UPDATE ingestion_token_hec_profiles SET default_host = ? WHERE ingestion_token_id = ?`, strings.Repeat("h", maximumHECMetadataBytes+1), nil, true},
		{"new generation", `UPDATE ingestion_tokens SET name = ?, version = version + 1 WHERE ingestion_token_id = ?`, "updated", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := openControlDB(t)
			if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
				t.Fatalf("CreateIndex(main): %v", err)
			}
			store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
			if err != nil {
				t.Fatalf("NewStore(): %v", err)
			}
			store.now = func() time.Time { return createdAt }
			issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name:              "original",
				Purpose:           IngestionTokenPurposeHEC,
				AllowedIndexNames: []string{"main"},
			})
			if err != nil {
				t.Fatalf("CreateCollectorToken(): %v", err)
			}
			store.now = func() time.Time { return createdAt.Add(2 * time.Minute) }
			var changed atomic.Bool
			const callbackName = "test:change-hec-preflight-authority"
			if err := store.orm.Callback().Query().After("gorm:query").Register(callbackName, func(query *gorm.DB) {
				if _, ok := query.Statement.Dest.(*collectorTokenHECAuthenticationWidths); !ok || !changed.CompareAndSwap(false, true) {
					return
				}
				// A concurrent administrator write commits while the preliminary
				// snapshot stays open; only the final authentication can see it.
				arguments := []any{issued.Token.ID}
				if test.value != nil {
					arguments = append([]any{test.value}, arguments...)
				}
				if _, err := db.SQLDB().ExecContext(ctx, test.statement, arguments...); err != nil {
					_ = query.AddError(err)
				}
			}); err != nil {
				t.Fatalf("register authority mutation: %v", err)
			}
			t.Cleanup(func() {
				if err := store.orm.Callback().Query().Remove(callbackName); err != nil {
					t.Errorf("remove authority mutation: %v", err)
				}
			})
			authentication, authErr := store.AuthenticateHEC(ctx, issued.Secret.Plaintext())
			if !changed.Load() {
				t.Fatal("authentication did not observe the concurrent mutation")
			}
			if test.want != nil || test.integrity {
				if authErr == nil || test.want != nil && !errors.Is(authErr, test.want) || control.IsDatabaseContention(authErr) || authentication.TokenID != "" {
					t.Fatalf("changed authority = %q, %v; want rejection %v", authentication.TokenID, authErr, test.want)
				}
				var lastUsedAt *int64
				if err := db.SQLDB().QueryRowContext(ctx, `SELECT last_used_at_unix_micro FROM ingestion_tokens WHERE ingestion_token_id = ?`, issued.Token.ID).Scan(&lastUsedAt); err != nil {
					t.Fatalf("read rejected token use: %v", err)
				}
				if lastUsedAt != nil {
					t.Fatal("changed authority recorded successful use")
				}
				return
			}
			if authErr != nil || authentication.TokenName != "updated" || authentication.TokenVersion != issued.Token.Version+1 {
				t.Fatalf("authentication did not return the final generation: %#v, %v", authentication, authErr)
			}
		})
	}
}
