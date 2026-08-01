package auth

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
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
			BoundCollectorID:  testCollectorID,
		},
	)
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	tx := database.GORMDB().WithContext(ctx).Begin(
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
	//nolint:staticcheck // The transaction boundary must reject a nil context.
	if _, err := store.RevalidateCollectorInTransaction(
		nil,
		nil,
		"opaque",
		time.Now().UTC(),
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil-input error = %v, want ErrInvalidArgument", err)
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
					SET retention_nanoseconds = ?
					WHERE index_id = ?`, int64(8_000_000_000*time.Second), index.ID); err != nil {
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
				if _, err := connection.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
					t.Fatalf("disable test-only foreign keys: %v", err)
				}
				if _, err := connection.ExecContext(ctx, `DROP TRIGGER index_catalog_index_delete_is_forbidden`); err != nil {
					t.Fatalf("disable test-only deletion guard: %v", err)
				}
				if _, err := connection.ExecContext(ctx, `DELETE FROM indexes WHERE index_id = ?`, index.ID); err != nil {
					t.Fatalf("orphan token scope: %v", err)
				}
				if _, err := connection.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
					t.Fatalf("restore foreign keys: %v", err)
				}
			},
			want: ErrInvalidIndexAuthority,
		},
	}

	for _, test := range tests {
		test := test
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
			})
			if err != nil {
				t.Fatalf("CreateCollectorToken(): %v", err)
			}
			test.mutate(t, ctx, database, index)

			tx := database.GORMDB().WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
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

			write := database.GORMDB().WithContext(ctx).Begin()
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
