package collectorfleet

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIsCurrentLeaseInTransactionFencesExactEnabledLease(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatalf("Claim(): %v", err)
	}
	assertCurrent := func(t *testing.T, candidate Lease, want bool) {
		t.Helper()
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
		got, err := store.IsCurrentLeaseInTransaction(ctx, tx, candidate)
		if err != nil {
			t.Fatalf("IsCurrentLeaseInTransaction(): %v", err)
		}
		if got != want {
			t.Fatalf(
				"IsCurrentLeaseInTransaction(%#v) = %t, want %t",
				candidate,
				got,
				want,
			)
		}
		if err := tx.Commit().Error; err != nil {
			t.Fatalf("commit read transaction: %v", err)
		}
		finished = true
	}

	assertCurrent(t, lease, true)
	for name, mutate := range map[string]func(*Lease){
		"tenant": func(candidate *Lease) {
			candidate.TenantID = "tenant-b"
		},
		"collector": func(candidate *Lease) {
			candidate.CollectorID = "123e4567-e89b-12d3-a456-426614174999"
		},
		"boot": func(candidate *Lease) {
			candidate.BootEpoch = "server-boot-2"
		},
		"stream": func(candidate *Lease) {
			candidate.StreamID = "stream-2"
		},
		"generation": func(candidate *Lease) {
			candidate.Generation++
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := lease
			mutate(&candidate)
			assertCurrent(t, candidate, false)
		})
	}

	if _, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		1,
		Administration{State: AdministrativeStateDisabled},
		connectedAt.Add(time.Minute),
	); err != nil {
		t.Fatalf("UpdateAdministration(disable): %v", err)
	}
	assertCurrent(t, lease, false)
}

func TestIsCurrentLeaseInTransactionRejectsInvalidBoundary(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	lease := Lease{
		Scope:       Scope{TenantID: "tenant-a"},
		CollectorID: "123e4567-e89b-12d3-a456-426614174000",
		BootEpoch:   "server-boot-1",
		StreamID:    "stream-1",
		Generation:  1,
	}
	if _, err := store.IsCurrentLeaseInTransaction(
		ctx,
		database.GORMDB(),
		lease,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("root-handle error = %v, want ErrInvalidArgument", err)
	}
	lease.Generation = 0
	tx := database.GORMDB().WithContext(ctx).Begin(
		&sql.TxOptions{ReadOnly: true},
	)
	if tx.Error != nil {
		t.Fatalf("begin read transaction: %v", tx.Error)
	}
	defer tx.Rollback()
	if _, err := store.IsCurrentLeaseInTransaction(
		ctx,
		tx,
		lease,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("invalid-lease error = %v, want ErrInvalidArgument", err)
	}
}
