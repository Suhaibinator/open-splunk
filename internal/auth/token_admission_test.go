package auth

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestRevalidateCollectorInTransactionDoesNotRecordTokenUse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	createdAt := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	store.now = func() time.Time { return createdAt }
	issued, err := store.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "collector",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID, OrganizationID: 1, IssuerUserID: 42,
		},
	)
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	tx := pgmeta.Begin(database.GORMDB().WithContext(ctx),
		&sql.TxOptions{ReadOnly: true},
	)
	if tx.Error != nil {
		t.Fatalf("begin read transaction: %v", tx.Error)
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback().Error
		}
	}()
	got, err := store.RevalidateCollectorInTransaction(
		ctx,
		tx,
		issued.Secret.Plaintext(),
		createdAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("RevalidateCollectorInTransaction(): %v", err)
	}
	if got.TokenID != issued.Token.ID ||
		got.BoundCollectorID != testCollectorID ||
		!slices.Equal(got.AuthorizedIndexNames(), []string{"main"}) {
		t.Fatalf("authentication = %#v", got)
	}
	got.AuthorizedIndexes[0].Name = "mutated"
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit read transaction: %v", err)
	}
	finished = true

	persisted, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if !persisted.LastUsedAt.IsZero() {
		t.Fatalf(
			"RevalidateCollectorInTransaction() recorded LastUsedAt = %v",
			persisted.LastUsedAt,
		)
	}
}

func TestRevalidateCollectorInTransactionRequiresActiveTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var nilContext context.Context
	database := openControlDB(t)
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	if _, err := store.RevalidateCollectorInTransaction(
		ctx,
		database.GORMDB(),
		"opaque",
		time.Now().UTC(),
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("root-handle error = %v, want ErrInvalidArgument", err)
	}

	if _, err := store.RevalidateCollectorInTransaction(
		nilContext,
		nil,
		"opaque",
		time.Now().UTC(),
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil-input error = %v, want ErrInvalidArgument", err)
	}
}

func TestAmbientCollectorAdmissionReadsScopeAfterCredentialMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	database := openControlDB(t)
	for _, name := range []string{"main", "replacement"} {
		if _, err := database.CreateIndex(ctx, activeIndex(name)); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewStore(database, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name: "scope-fence", AllowedIndexNames: []string{"main"},
		BoundCollectorID: testCollectorID, OrganizationID: 1, IssuerUserID: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := pgmeta.BeginCatalogReader(database.GORMDB().WithContext(ctx), "15:ingestion-credentials")
	if writer.Error != nil {
		t.Fatal(writer.Error)
	}
	defer func() { _ = writer.Rollback().Error }()
	reader := pgmeta.Begin(database.GORMDB().WithContext(ctx))
	if reader.Error != nil {
		t.Fatal(reader.Error)
	}
	defer func() { _ = reader.Rollback().Error }()
	var pid int
	if err := reader.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
		t.Fatal(err)
	}
	type admissionResult struct {
		authentication Authentication
		err            error
	}
	result := make(chan admissionResult, 1)
	go func() {
		authentication, err := store.RevalidateAndRecordCollectorUseInTransaction(ctx, reader, issued.Secret.Plaintext(), time.Now().UTC())
		result <- admissionResult{authentication: authentication, err: err}
	}()
	// The writer is still uncommitted. Wait until admission reaches its actual
	// database fence, then change the scope before allowing admission to proceed.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := database.SQLDB().QueryRowContext(ctx, "SELECT COALESCE(wait_event='advisory',false) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case got := <-result:
			t.Fatalf("admission did not wait for credential mutation: %+v", got)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if err := writer.Exec("DELETE FROM ingestion_token_indexes WHERE ingestion_token_id=?", issued.Token.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := writer.Exec("INSERT INTO ingestion_token_indexes(ingestion_token_id,index_id) SELECT ?,index_id FROM indexes WHERE name='replacement'", issued.Token.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || !slices.Equal(got.authentication.AuthorizedIndexNames(), []string{"replacement"}) {
			t.Fatalf("admission retained pre-mutation scope: %+v %v", got.authentication, got.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := reader.Commit().Error; err != nil {
		t.Fatal(err)
	}
}

func TestLeaseRevalidationReturnsIdentityWithDeferredIndexAuthorityOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*testing.T, context.Context, *control.DB, control.Index)
		want       error
		strictWant error
	}{
		{
			name: "no active index",
			mutate: func(t *testing.T, ctx context.Context, database *control.DB, index control.Index) {
				definition := index.Definition
				definition.IngestionEnabled = false
				if _, err := database.UpdateIndex(ctx, index.ID, index.Version, definition); err != nil {
					t.Fatalf("UpdateIndex(disable): %v", err)
				}
			},
			want:       ErrNoActiveIndexAuthority,
			strictWant: ErrUnauthorized,
		},
		{
			name: "invalid index policy",
			mutate: func(t *testing.T, ctx context.Context, database *control.DB, index control.Index) {
				if _, err := database.SQLDB().ExecContext(ctx, `
					UPDATE indexes
					SET retention_nanoseconds = $1
					WHERE index_id = $2`, int64(8_000_000_000*time.Second), index.ID); err != nil {
					t.Fatalf("corrupt retention: %v", err)
				}
			},
			want: ErrInvalidIndexAuthority,
		},
		{
			name: "orphaned index scope",
			mutate: func(t *testing.T, ctx context.Context, database *control.DB, index control.Index) {
				connection, err := database.SQLDB().Conn(ctx)
				if err != nil {
					t.Fatalf("acquire corruption connection: %v", err)
				}
				defer connection.Close()
				if _, err := pgtest.DropForeignKeys(ctx, connection); err != nil {
					t.Fatalf("disable test-only foreign keys: %v", err)
				}
				if _, err := connection.ExecContext(ctx, `DROP TRIGGER index_catalog_index_delete_is_forbidden ON indexes`); err != nil {
					t.Fatalf("disable test-only deletion guard: %v", err)
				}
				if _, err := connection.ExecContext(ctx, `DELETE FROM indexes WHERE index_id = $1`, index.ID); err != nil {
					t.Fatalf("orphan token scope: %v", err)
				}
				if _, err := pgtest.RestoreForeignKeys(ctx, connection); err != nil {
					t.Fatalf("restore foreign keys: %v", err)
				}
			},
			want: ErrInvalidIndexAuthority,
		},
		{
			name: "invalid event constraint",
			mutate: func(t *testing.T, ctx context.Context, database *control.DB, _ control.Index) {
				if _, err := database.SQLDB().ExecContext(ctx, `
					UPDATE ingestion_token_constraints
					SET pattern = '['
					WHERE constraint_kind = 'host'`); err != nil {
					t.Fatalf("corrupt event constraint: %v", err)
				}
			},
			want: ErrInvalidEventAuthority,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			database := openControlDB(t)
			index, err := database.CreateIndex(ctx, activeIndex("main"))
			if err != nil {
				t.Fatalf("CreateIndex(main): %v", err)
			}
			createdAt := time.Date(2026, 7, 31, 20, 0, 0, 0, time.UTC)
			store, err := NewStore(database, []byte("0123456789abcdef0123456789abcdef"))
			if err != nil {
				t.Fatalf("NewStore(): %v", err)
			}
			store.now = func() time.Time { return createdAt }
			issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name: "collector", AllowedIndexNames: []string{"main"}, BoundCollectorID: testCollectorID,
				AllowedHostRegexes: []string{"^valid$"}, OrganizationID: 1, IssuerUserID: 42,
			})
			if err != nil {
				t.Fatalf("CreateCollectorToken(): %v", err)
			}
			test.mutate(t, ctx, database, index)

			tx := pgmeta.Begin(database.GORMDB().WithContext(ctx), &sql.TxOptions{ReadOnly: true})
			if tx.Error != nil {
				t.Fatalf("begin read transaction: %v", tx.Error)
			}
			got, err := store.RevalidateCollectorInTransaction(
				ctx, tx, issued.Secret.Plaintext(), createdAt.Add(time.Minute),
			)
			if !errors.Is(err, test.want) || got.TokenID != issued.Token.ID ||
				got.BoundCollectorID != testCollectorID || len(got.AuthorizedIndexes) != 0 {
				t.Fatalf("lease revalidation = (%#v, %v), want verified identity/%v", got, err, test.want)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatalf("commit read transaction: %v", err)
			}

			strict, err := store.Authenticate(ctx, issued.Secret.Plaintext())
			if err == nil || strict.TokenID != "" ||
				(test.strictWant != nil && !errors.Is(err, test.strictWant)) {
				t.Fatalf("strict Authenticate = (%#v, %v), want zero authentication failure", strict, err)
			}

			write := pgmeta.Begin(database.GORMDB().WithContext(ctx))
			if write.Error != nil {
				t.Fatalf("begin write transaction: %v", write.Error)
			}
			strict, err = store.RevalidateAndRecordCollectorUseInTransaction(
				ctx, write, issued.Secret.Plaintext(), createdAt.Add(time.Minute),
			)
			if rollbackErr := write.Rollback().Error; rollbackErr != nil {
				t.Fatalf("rollback write transaction: %v", rollbackErr)
			}
			if err == nil || strict.TokenID != "" ||
				(test.strictWant != nil && !errors.Is(err, test.strictWant)) {
				t.Fatalf("strict use revalidation = (%#v, %v), want zero authentication failure", strict, err)
			}
		})
	}
}
