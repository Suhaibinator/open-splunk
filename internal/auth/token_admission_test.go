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
		!slices.Equal(got.AllowedIndexNames, []string{"main"}) {
		t.Fatalf("authentication = %#v", got)
	}
	got.AllowedIndexNames[0] = "mutated"
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
