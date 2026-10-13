package exportjournal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestJournalSourceIdentityCannotBeRetargetedAndCorruptProjectionFailsClosed(t *testing.T) {
	journal, db := openTestJournal(t)
	ctx, access, job, _ := journalFixture(t)
	if err := journal.Admit(ctx, access, job); err != nil {
		t.Fatal(err)
	}
	retained, err := journal.Get(ctx, access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	retained.Job.Version++
	retained.Job.SearchJobID = "foreign-source"
	if err := journal.Update(ctx, retained); err == nil {
		t.Fatal("accepted export source retargeting")
	}
	current, err := journal.Get(ctx, access, job.ID)
	if err != nil || current.Job.SearchJobID != job.SearchJobID {
		t.Fatalf("source retargeting changed journal: %#v %v", current, err)
	}
	if _, err := pgtest.CheckTables(context.Background(), db.SQLDB(), false, "durable_export_jobs"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pgtest.CheckTables(context.Background(), db.SQLDB(), true, "durable_export_jobs"); err != nil {
			t.Error(err)
		}
	})
	payload, err := json.Marshal(retained.Job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `UPDATE durable_export_jobs SET job_json=$1,version=$2 WHERE export_id=$3`, payload, retained.Job.Version, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Get(ctx, access, job.ID); err == nil {
		t.Fatal("corrupt source projection became authorization authority")
	}
}
