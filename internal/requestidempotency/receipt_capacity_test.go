package requestidempotency_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"gorm.io/gorm"
)

// Seed the exact production ledger boundary without quadratic fixture setup.
// The accounting trigger verifies COUNT/SUM after every production insertion.
// This fixture transaction suspends only its AFTER INSERT bookkeeping, inserts
// fully constrained rows, restores exact COUNT/SUM, and reinstalls the original
// trigger before commit. Every operation under test runs with the complete
// unchanged production schema and accounting guards.
func seedReceiptCapacity(t *testing.T, database *gorm.DB, intent requestidempotency.Intent, count int, keyWidth int) {
	t.Helper()
	err := database.Transaction(func(tx *gorm.DB) error {
		var trigger string
		if err := tx.Raw("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'api_mutation_receipt_after_insert'").Scan(&trigger).Error; err != nil {
			return err
		}
		if trigger == "" {
			return errors.New("production accounting trigger missing")
		}
		if err := tx.Exec("DROP TRIGGER api_mutation_receipt_after_insert").Error; err != nil {
			return err
		}
		if err := tx.Exec(`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n < ?)
   INSERT INTO api_mutation_receipts (
    tenant_id, actor_kind, actor_id, route, client_request_id, canonical_version,
    request_sha256, target_kind, target_id, target_version, audit_sequence,
    created_at_unix_micro, retain_until_unix_micro, target_terminal_at_unix_micro, encoded_bytes)
   SELECT tenant_id, actor_kind, actor_id, route, printf('%0*d', ?, n), canonical_version,
    request_sha256, target_kind, target_id, target_version, audit_sequence,
    created_at_unix_micro, retain_until_unix_micro, target_terminal_at_unix_micro,
    encoded_bytes - length(CAST(client_request_id AS BLOB)) + ?
   FROM api_mutation_receipts CROSS JOIN fixture
   WHERE tenant_id = ? AND client_request_id = ?`, count, keyWidth, keyWidth, intent.TenantID, intent.ClientRequestID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE api_mutation_receipt_tenant_state SET
   receipt_count = (SELECT count(*) FROM api_mutation_receipts WHERE tenant_id = ?),
   encoded_bytes = (SELECT sum(encoded_bytes) FROM api_mutation_receipts WHERE tenant_id = ?)
   WHERE tenant_id = ?`, intent.TenantID, intent.TenantID, intent.TenantID).Error; err != nil {
			return err
		}
		return tx.Exec(trigger).Error
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReceiptCapacityPreservesReplayAndRollsBackNewMutation(t *testing.T) {
	for _, boundary := range []string{"count", "encoded-bytes"} {
		t.Run(boundary, func(t *testing.T) {
			database, err := control.Open(t.Context(), t.TempDir()+"/control.db")
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			tenant, actor, key, targetID := "tenant", "owner", "accepted-request-key", "accepted-target"
			keyWidth := 16
			if boundary == "encoded-bytes" {
				tenant, actor, key, targetID = strings.Repeat("t", 255), strings.Repeat("o", 255), strings.Repeat("a", 128), strings.Repeat("r", 256)
				keyWidth = 128
			}
			intent, err := requestidempotency.NewIntent(tenant, "browser", actor, requestidempotency.RouteCreateSavedSearch, key, &opensplunk.CreateSavedSearchRequest{})
			if err != nil {
				t.Fatal(err)
			}
			target := requestidempotency.Target{Kind: requestidempotency.TargetSavedSearch, ID: targetID, Version: 1}
			acceptedAt := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
			var first requestidempotency.Receipt
			if err := database.GORMDB().Transaction(func(tx *gorm.DB) error {
				first, err = requestidempotency.AppendInTransaction(t.Context(), tx, intent, target, nil, acceptedAt)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			count := int(requestidempotency.MaximumReceipts) - 1
			if boundary == "encoded-bytes" {
				count = int(requestidempotency.MaximumBytes/first.EncodedBytes) - 1
			}
			seedReceiptCapacity(t, database.GORMDB(), intent, count, keyWidth)
			var before struct {
				Count uint64
				Bytes uint64
			}
			if err := database.GORMDB().Raw("SELECT receipt_count AS count, encoded_bytes AS bytes FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?", tenant).Scan(&before).Error; err != nil {
				t.Fatal(err)
			}
			if boundary == "count" && before.Count != requestidempotency.MaximumReceipts {
				t.Fatalf("count boundary=%d", before.Count)
			}
			if boundary == "encoded-bytes" && (before.Bytes > requestidempotency.MaximumBytes || requestidempotency.MaximumBytes-before.Bytes >= first.EncodedBytes) {
				t.Fatalf("byte boundary=%d, receipt=%d", before.Bytes, first.EncodedBytes)
			}
			if replay, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent); err != nil || !found || replay.Target != target {
				t.Fatalf("replay at capacity=%#v %v %v", replay, found, err)
			}
			if err := database.GORMDB().Exec("CREATE TABLE test_mutations (id TEXT PRIMARY KEY)").Error; err != nil {
				t.Fatal(err)
			}
			next := intent
			next.ClientRequestID = strings.Repeat("b", len(key))
			err = database.GORMDB().Transaction(func(tx *gorm.DB) error {
				if err := tx.Exec("INSERT INTO test_mutations VALUES ('must-roll-back')").Error; err != nil {
					return err
				}
				_, err := requestidempotency.AppendInTransaction(t.Context(), tx, next, target, nil, acceptedAt.Add(time.Hour))
				return err
			})
			if !errors.Is(err, requestidempotency.ErrCapacity) {
				t.Fatalf("new receipt at capacity=%v", err)
			}
			var mutations int
			if err := database.GORMDB().Raw("SELECT count(*) FROM test_mutations").Scan(&mutations).Error; err != nil || mutations != 0 {
				t.Fatalf("mutation survived capacity failure=%d %v", mutations, err)
			}
			if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), next); err != nil || found {
				t.Fatalf("failed receipt survived=%v %v", found, err)
			}
			var after struct {
				Count uint64
				Bytes uint64
			}
			if err := database.GORMDB().Raw("SELECT receipt_count AS count, encoded_bytes AS bytes FROM api_mutation_receipt_tenant_state WHERE tenant_id = ?", tenant).Scan(&after).Error; err != nil {
				t.Fatal(err)
			}
			if after != before {
				t.Fatalf("capacity failure changed accounting: %#v -> %#v", before, after)
			}
			if err := database.GORMDB().Exec("DELETE FROM api_mutation_receipts WHERE tenant_id = ? AND client_request_id = ?", tenant, key).Error; err == nil {
				t.Fatal("unexpired receipt was evicted")
			}
			otherTenant := intent
			otherTenant.TenantID = "independent-tenant"
			if err := database.GORMDB().Transaction(func(tx *gorm.DB) error {
				_, err := requestidempotency.AppendInTransaction(t.Context(), tx, otherTenant, target, nil, acceptedAt)
				return err
			}); err != nil {
				t.Fatalf("one tenant's capacity blocked another: %v", err)
			}
			changed := intent
			changed.RequestSHA256[0] ^= 1
			if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), changed); !found || !errors.Is(err, requestidempotency.ErrConflict) {
				t.Fatalf("changed intent at capacity=%v %v", found, err)
			}
		})
	}
}

func TestActiveReceiptSurvivesLongWorkAndRollbackUntilTerminalFenceEnds(t *testing.T) {
	database, err := control.Open(t.Context(), t.TempDir()+"/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	intent, err := requestidempotency.NewIntent("tenant", "browser", "owner", requestidempotency.RouteCreateSearchJob, "long-running-request", &opensplunk.CreateSearchJobRequest{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	appendReceipt := func(value requestidempotency.Intent, target requestidempotency.Target, at time.Time) {
		t.Helper()
		if err := database.GORMDB().Transaction(func(tx *gorm.DB) error {
			_, err := requestidempotency.AppendInTransaction(t.Context(), tx, value, target, nil, at)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	appendReceipt(intent, requestidempotency.Target{Kind: requestidempotency.TargetSearchJob, ID: "search-1", Version: 1}, start)
	later := intent
	later.Route = requestidempotency.RouteCreateSavedSearch
	later.ClientRequestID = "later-saved-request"
	highWater := start.Add(20 * 24 * time.Hour)
	appendReceipt(later, requestidempotency.Target{Kind: requestidempotency.TargetSavedSearch, ID: "saved-1", Version: 1}, highWater)
	active, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent)
	if err != nil || !found || active.TerminalAt != nil {
		t.Fatalf("long work lost its receipt: %#v %v %v", active, found, err)
	}
	if err := database.GORMDB().Transaction(func(tx *gorm.DB) error {
		return requestidempotency.MarkTargetTerminalInTransaction(t.Context(), tx, intent.TenantID, requestidempotency.TargetSearchJob, "search-1", highWater.Add(-24*time.Hour))
	}); err != nil {
		t.Fatal(err)
	}
	terminal, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent)
	if err != nil || !found || terminal.TerminalAt == nil || terminal.TerminalAt.Before(highWater) || terminal.RetainUntil.Before(highWater.Add(7*24*time.Hour)) {
		t.Fatalf("rollback shortened terminal fence: %#v %v %v", terminal, found, err)
	}
	later.ClientRequestID = "fence-ended-request"
	appendReceipt(later, requestidempotency.Target{Kind: requestidempotency.TargetSavedSearch, ID: "saved-2", Version: 1}, terminal.RetainUntil.Add(time.Microsecond))
	if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent); err != nil || found {
		t.Fatalf("terminal receipt never reclaimed: %v %v", found, err)
	}
}

func TestReceiptKeysAreCaseSensitiveAndUnknownFingerprintVersionsStayFenced(t *testing.T) {
	database, err := control.Open(t.Context(), t.TempDir()+"/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	intent, err := requestidempotency.NewIntent("tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch, "case-sensitive-key", &opensplunk.CreateSavedSearchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	intent.CanonicalVersion++
	if err := database.GORMDB().Transaction(func(tx *gorm.DB) error {
		_, err := requestidempotency.AppendInTransaction(t.Context(), tx, intent, requestidempotency.Target{Kind: requestidempotency.TargetSavedSearch, ID: "saved-1", Version: 1}, nil, time.Now())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent); !found || !errors.Is(err, requestidempotency.ErrUnavailable) {
		t.Fatalf("unknown version permitted new execution: %v %v", found, err)
	}
	changedCase := intent
	changedCase.ClientRequestID = strings.ToUpper(intent.ClientRequestID)
	if _, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), changedCase); found || err != nil {
		t.Fatalf("key case was folded: %v %v", found, err)
	}
}
