package exportjournal

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func openTestJournal(t *testing.T) (*Store, *control.DB) {
	t.Helper()
	db, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	events, err := audit.NewStore(db, audit.StoreOptions{CursorKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db.GORMDB(), events)
	if err != nil {
		t.Fatal(err)
	}
	return store, db
}
func journalFixture(t *testing.T) (context.Context, searchjobs.AccessScope, exportjobs.Job, requestidempotency.Intent) {
	t.Helper()
	ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "owner", Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	now := time.Now().UTC().Truncate(time.Microsecond)
	job := exportjobs.Job{ID: "export-accepted", Version: 1, SearchJobID: "search-1", Format: exportjobs.FormatCSV, Columns: []string{"_raw"}, RowLimit: 100, ByteLimit: 1024, State: exportjobs.StateQueued, CreatedAt: now, Progress: exportjobs.Progress{UpdatedAt: now}}
	intent, err := requestidempotency.NewIntent("tenant", "browser", "owner", requestidempotency.RouteCreateExportJob, "logical-export-request", &opensplunk.CreateExportJobRequest{Definition: &opensplunk.ExportDefinition{SearchJobId: "search-1"}})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, access, job, intent
}
func TestJournalAdmissionCommitsTargetReceiptAndOneAudit(t *testing.T) {
	store, db := openTestJournal(t)
	ctx, access, job, intent := journalFixture(t)
	if err := store.AdmitIdempotent(ctx, access, job, intent); err != nil {
		t.Fatal(err)
	}
	updated := job
	updated.Version = 3
	updated.State = exportjobs.StateCanceled
	updated.FinishedAt = job.CreatedAt
	updated.ExpiresAt = job.CreatedAt.Add(time.Hour)
	if err := store.Update(ctx, exportjobs.DurableJob{Access: access, Job: updated}); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Lookup(ctx, access, intent)
	if err != nil || !found || got.ID != job.ID || got.Version != 3 {
		t.Fatalf("current replay = %#v,%v,%v", got, found, err)
	}
	var receipts, events int64
	if err := db.GORMDB().Table("api_mutation_receipts").Count(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GORMDB().Table("audit_events").Where("action = ?", "export.create").Count(&events).Error; err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || events != 1 {
		t.Fatalf("receipt/audit counts=%d/%d", receipts, events)
	}
	changed := intent
	changed.RequestSHA256[0] ^= 1
	if _, _, err := store.Lookup(ctx, access, changed); !errors.Is(err, requestidempotency.ErrConflict) {
		t.Fatalf("changed replay=%v", err)
	}
	wrong := access
	wrong.OwnerID = "another"
	if _, _, err := store.Lookup(ctx, wrong, intent); !errors.Is(err, exportjobs.ErrNotFound) {
		t.Fatalf("crossowner replay=%v", err)
	}
}
func TestJournalReceiptFailureRollsBackTargetAndAudit(t *testing.T) {
	store, db := openTestJournal(t)
	ctx, access, job, intent := journalFixture(t)
	if err := db.GORMDB().Exec(`CREATE TRIGGER test_reject_receipt BEFORE INSERT ON api_mutation_receipts BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.AdmitIdempotent(ctx, access, job, intent); err == nil {
		t.Fatal("receipt failure accepted")
	}
	for _, table := range []string{"durable_export_jobs", "audit_events", "api_mutation_receipts"} {
		var count int64
		if err := db.GORMDB().Table(table).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s survived rollback", table)
		}
	}
}
func TestJournalDeletedTargetNeverRecreatedFromReceipt(t *testing.T) {
	store, db := openTestJournal(t)
	ctx, access, job, intent := journalFixture(t)
	if err := store.AdmitIdempotent(ctx, access, job, intent); err != nil {
		t.Fatal(err)
	}
	if err := db.GORMDB().Exec("DELETE FROM durable_export_jobs WHERE export_id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Lookup(ctx, access, intent); !found || !errors.Is(err, exportjobs.ErrNotFound) {
		t.Fatalf("deleted replay=%v,%v", found, err)
	}
}
