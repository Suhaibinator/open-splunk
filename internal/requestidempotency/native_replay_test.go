package requestidempotency_test

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
)

func TestNativeReceiptReplayLeavesCallerTransactionUsable(t *testing.T) {
	for _, port := range []string{"gorm", "sql"} {
		t.Run(port, func(t *testing.T) {
			database, err := control.Open(t.Context(), pgtest.DSN(t))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			intent, err := requestidempotency.NewIntent("tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch, "native-replay-request", &opensplunk.CreateSavedSearchRequest{})
			if err != nil {
				t.Fatal(err)
			}
			target := requestidempotency.Target{Kind: requestidempotency.TargetSavedSearch, ID: "source", Version: 1}
			now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
			var appendReceipt func(requestidempotency.Intent, requestidempotency.Target, time.Time) (requestidempotency.Receipt, error)
			var commit func() error
			if port == "gorm" {
				tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
				if tx.Error != nil {
					t.Fatal(tx.Error)
				}
				defer func() {
					if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
						t.Error(err)
					}
				}()
				appendReceipt = func(i requestidempotency.Intent, target requestidempotency.Target, at time.Time) (requestidempotency.Receipt, error) {
					return requestidempotency.AppendInTransaction(t.Context(), tx, i, target, nil, at)
				}
				commit = func() error { return tx.Commit().Error }
			} else {
				tx, err := pgmeta.BeginSQL(t.Context(), database.SQLDB(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
						t.Error(err)
					}
				}()
				appendReceipt = func(i requestidempotency.Intent, target requestidempotency.Target, at time.Time) (requestidempotency.Receipt, error) {
					return requestidempotency.AppendInSQLTransaction(t.Context(), tx, i, target, nil, at)
				}
				commit = tx.Commit
			}
			original, err := appendReceipt(intent, target, now)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := appendReceipt(intent, target, now.Add(time.Minute))
			if err != nil || !reflect.DeepEqual(original, replay) {
				t.Fatalf("exact replay changed committed outcome: %+v %v", replay, err)
			}
			next := intent
			next.ClientRequestID = "next-native-request"
			nextTarget := target
			nextTarget.ID = "second-source"
			if _, err := appendReceipt(next, nextTarget, now.Add(2*time.Minute)); err != nil {
				t.Fatalf("transaction unusable after replay: %v", err)
			}
			if err := commit(); err != nil {
				t.Fatal(err)
			}
			var count int64
			if err := database.SQLDB().QueryRowContext(t.Context(), "SELECT receipt_count FROM api_mutation_receipt_tenant_state WHERE tenant_id=$1", "tenant").Scan(&count); err != nil || count != 2 {
				t.Fatalf("replay altered atomic accounting: count=%d error=%v", count, err)
			}
		})
	}
}

func TestNativeReceiptCannotReplayAcrossOrganizations(t *testing.T) {
	database, err := control.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	canonical := &opensplunk.CreateIndexRequest{}
	one, err := requestidempotency.NewScopedIntent("tenant", "browser", "101", requestidempotency.RouteCreateIndex, "organization-replay", 1, canonical)
	if err != nil {
		t.Fatal(err)
	}
	two, err := requestidempotency.NewScopedIntent("tenant", "browser", "101", requestidempotency.RouteCreateIndex, "organization-replay", 2, canonical)
	if err != nil {
		t.Fatal(err)
	}
	tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer func() {
		if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	target := requestidempotency.Target{Kind: requestidempotency.TargetIndex, ID: "index-owned-by-one", Version: 1}
	original, err := requestidempotency.AppendInTransaction(t.Context(), tx, one, target, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	replay, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), one)
	if err != nil || !found || !reflect.DeepEqual(original, replay) {
		t.Fatalf("same organization replay: %+v %v %v", replay, found, err)
	}
	if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), two); !found || !errors.Is(err, requestidempotency.ErrConflict) {
		t.Fatalf("cross organization replay accepted: found=%v error=%v", found, err)
	}
}
