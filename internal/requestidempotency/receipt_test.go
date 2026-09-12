package requestidempotency_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"google.golang.org/protobuf/proto"
)

func TestIntentDigestIsCanonicalAndExcludesRequestKey(t *testing.T) {
	t.Parallel()

	canonical := &opensplunk.CreateSavedSearchRequest{
		Definition: &opensplunk.SavedSearchDefinition{Name: "Errors"},
	}
	first, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"logical request 1", canonical,
	)
	if err != nil {
		t.Fatalf("NewIntent(first): %v", err)
	}
	second, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"logical request 2", proto.Clone(canonical),
	)
	if err != nil {
		t.Fatalf("NewIntent(second): %v", err)
	}
	if !bytes.Equal(first.RequestSHA256[:], second.RequestSHA256[:]) {
		t.Fatal("request key changed canonical client intent digest")
	}
	changed := proto.Clone(canonical).(*opensplunk.CreateSavedSearchRequest)
	changed.Definition.Name = "Warnings"
	third, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"logical request 1", changed,
	)
	if err != nil {
		t.Fatalf("NewIntent(changed): %v", err)
	}
	if bytes.Equal(first.RequestSHA256[:], third.RequestSHA256[:]) {
		t.Fatal("changed client intent retained the same digest")
	}
}

func TestClientRequestIDContractIncludesPrintableSpace(t *testing.T) {
	t.Parallel()

	for _, valid := range []string{
		"1234567890123456",
		"request key with spaces",
		strings.Repeat("~", requestidempotency.MaximumRequestID),
	} {
		if err := requestidempotency.ValidateClientRequestID(valid); err != nil {
			t.Errorf("ValidateClientRequestID(%q): %v", valid, err)
		}
	}
	for _, invalid := range []string{
		"short",
		"123456789012345\n",
		strings.Repeat("a", requestidempotency.MaximumRequestID+1),
		"12345678901234é",
	} {
		if err := requestidempotency.ValidateClientRequestID(invalid); err == nil {
			t.Errorf("ValidateClientRequestID(%q) succeeded", invalid)
		}
	}
}

func TestReceiptCoCommitReplayConflictAndClockRollback(t *testing.T) {
	database, err := control.Open(t.Context(), t.TempDir()+"/control.db")
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	canonical := &opensplunk.CreateSavedSearchRequest{
		Definition: &opensplunk.SavedSearchDefinition{Name: "Errors"},
	}
	intent, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"saved create 0001", canonical,
	)
	if err != nil {
		t.Fatalf("NewIntent(): %v", err)
	}
	target := requestidempotency.Target{
		Kind: requestidempotency.TargetSavedSearch, ID: "saved-1", Version: 1,
	}
	createdAt := time.Date(2026, time.September, 12, 12, 0, 0, 123_456_000, time.UTC)
	auditSequence := uint64(7)
	tx := database.GORMDB().WithContext(t.Context()).Begin()
	receipt, err := requestidempotency.AppendInTransaction(
		t.Context(), tx, intent, target, &auditSequence, createdAt,
	)
	if err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendInTransaction(): %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit receipt: %v", err)
	}
	if receipt.Target != target || !receipt.CreatedAt.Equal(createdAt) ||
		receipt.RetainUntil.Sub(receipt.CreatedAt) != 7*24*time.Hour ||
		receipt.TerminalAt == nil || !receipt.TerminalAt.Equal(createdAt) {
		t.Fatalf("receipt = %#v", receipt)
	}

	replayed, found, err := requestidempotency.Read(
		t.Context(), database.GORMDB(), intent,
	)
	if err != nil || !found || replayed.Target != target {
		t.Fatalf("Read(exact) = (%#v, %t, %v)", replayed, found, err)
	}

	changed := proto.Clone(canonical).(*opensplunk.CreateSavedSearchRequest)
	changed.Definition.Name = "Warnings"
	conflict, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		intent.ClientRequestID, changed,
	)
	if err != nil {
		t.Fatalf("NewIntent(conflict): %v", err)
	}
	if _, found, err := requestidempotency.Read(
		t.Context(), database.GORMDB(), conflict,
	); !found || !errors.Is(err, requestidempotency.ErrConflict) {
		t.Fatalf("Read(conflict) = found %t error %v", found, err)
	}

	rollbackKey, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"saved create 0002", canonical,
	)
	if err != nil {
		t.Fatalf("NewIntent(rollback): %v", err)
	}
	tx = database.GORMDB().WithContext(t.Context()).Begin()
	rollbackReceipt, err := requestidempotency.AppendInTransaction(
		t.Context(), tx, rollbackKey,
		requestidempotency.Target{
			Kind: requestidempotency.TargetSavedSearch, ID: "saved-2", Version: 1,
		},
		nil,
		createdAt.Add(-time.Hour),
	)
	if err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendInTransaction(clock rollback): %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit rollback-clock receipt: %v", err)
	}
	if rollbackReceipt.CreatedAt.Before(receipt.CreatedAt) {
		t.Fatalf("rollback receipt time = %s before high water %s", rollbackReceipt.CreatedAt, receipt.CreatedAt)
	}

	reclaimKey, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSavedSearch,
		"saved create 0003", canonical,
	)
	if err != nil {
		t.Fatalf("NewIntent(reclaim): %v", err)
	}
	tx = database.GORMDB().WithContext(t.Context()).Begin()
	if _, err := requestidempotency.AppendInTransaction(
		t.Context(), tx, reclaimKey,
		requestidempotency.Target{
			Kind: requestidempotency.TargetSavedSearch, ID: "saved-3", Version: 1,
		},
		nil,
		createdAt.Add(8*24*time.Hour),
	); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("AppendInTransaction(reclaim): %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit reclaim receipt: %v", err)
	}
	if _, found, err := requestidempotency.Read(
		t.Context(), database.GORMDB(), intent,
	); err != nil || found {
		t.Fatalf("Read(reclaimed) = found %t error %v", found, err)
	}
}

func TestAsynchronousReceiptStaysFencedUntilTerminal(t *testing.T) {
	database, err := control.Open(t.Context(), t.TempDir()+"/control.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	request := &opensplunk.CreateSearchJobRequest{
		Definition: &opensplunk.SearchDefinition{Spl: "search index=main"},
	}
	intent, err := requestidempotency.NewIntent(
		"tenant", "browser", "owner", requestidempotency.RouteCreateSearchJob,
		"search create 01", request,
	)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	tx := database.GORMDB().WithContext(t.Context()).Begin()
	receipt, err := requestidempotency.AppendInTransaction(
		t.Context(), tx, intent,
		requestidempotency.Target{
			Kind: requestidempotency.TargetSearchJob, ID: "job-1", Version: 1,
		},
		nil, createdAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalAt != nil {
		t.Fatalf("active receipt terminal = %s", receipt.TerminalAt)
	}
	terminalAt := createdAt.Add(10 * 24 * time.Hour)
	if err := requestidempotency.MarkTargetTerminalInTransaction(
		t.Context(), tx, "tenant", requestidempotency.TargetSearchJob, "job-1", terminalAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	closed, found, err := requestidempotency.Read(t.Context(), database.GORMDB(), intent)
	if err != nil || !found || closed.TerminalAt == nil || !closed.TerminalAt.Equal(terminalAt) ||
		closed.RetainUntil.Before(terminalAt.Add(7*24*time.Hour)) {
		t.Fatalf("closed receipt = (%#v, %t, %v)", closed, found, err)
	}
}
