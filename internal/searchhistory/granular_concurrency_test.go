package searchhistory

import (
	"context"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"gorm.io/gorm"
)

func TestHistoryDeletionWaitsForOwnerBeforeLockingRows(t *testing.T) {
	for _, operation := range []string{"expiry", "delete", "clear"} {
		t.Run(operation, func(t *testing.T) {
			now := testStoreClock()
			database, store := openTestStore(t, Options{Clock: func() time.Time { return now }, MaximumAge: time.Hour})
			scope := AccessScope{TenantID: "tenant", OwnerID: "held-owner", OrganizationID: "1"}
			for _, owner := range []string{"held-owner", "other-owner"} {
				entryScope := scope
				entryScope.OwnerID = owner
				if _, err := store.Record(t.Context(), entryScope, historyEntry(owner, "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, now)); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(2 * time.Hour)
			key := "search-history:tenant:held-owner"
			held := pgmeta.BeginLocked(database.GORMDB().WithContext(t.Context()), key)
			if held.Error != nil {
				t.Fatal(held.Error)
			}
			defer held.Rollback()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				admin := scope
				admin.OwnerID, admin.SiteAdministrator = "site-admin", true
				var err error
				switch operation {
				case "expiry":
					_, _, err = store.pruneExpiredTransaction(ctx, now, 2)
				case "delete":
					err = store.Delete(ctx, admin, "held-owner")
				case "clear":
					_, err = store.Clear(ctx, admin, Filter{})
				}
				done <- err
			}()
			waitHistoryLock(t, database.GORMDB(), key)
			// The deleting transaction must be waiting without owning this row.
			// A BEFORE ROW advisory trigger alone would already hold the row and
			// this query would time out, exposing the row/owner lock inversion.
			rowCtx, cancelRow := context.WithTimeout(t.Context(), time.Second)
			var row string
			err := held.WithContext(rowCtx).Raw("SELECT search_job_id FROM search_history WHERE search_job_id=? FOR UPDATE", "held-owner").Scan(&row).Error
			cancelRow()
			if err != nil || row != "held-owner" {
				t.Fatalf("owner wait already locked the history row: row=%q err=%v", row, err)
			}
			if err := held.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			var rows, counted int64
			if err := database.GORMDB().Model(&historyRecord{}).Count(&rows).Error; err != nil {
				t.Fatal(err)
			}
			if err := database.GORMDB().Raw("SELECT COALESCE(sum(terminal_count),0) FROM search_history_owner_counts").Scan(&counted).Error; err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if operation == "delete" {
				want = 1
			}
			if rows != want || counted != want {
				t.Fatalf("history/count state = %d/%d, want %d", rows, counted, want)
			}
		})
	}
}

func TestHistoryCrossOwnerJobIdentityFencesBeforeCheckingPending(t *testing.T) {
	database, store := openTestStore(t, Options{})
	held := pgmeta.BeginLocked(database.GORMDB().WithContext(t.Context()), "search-history:tenant:pending-owner")
	if held.Error != nil {
		t.Fatal(held.Error)
	}
	defer held.Rollback()
	key := "search-history-job:shared-job"
	if err := pgmeta.Lock(held, key); err != nil {
		t.Fatal(err)
	}
	_, indexed, err := normalizePendingEntry(pendingHistoryEntry("shared-job", "index=main", testStoreClock()))
	if err != nil {
		t.Fatal(err)
	}
	if err := held.Create(&pendingHistoryRecord{
		SearchJobID: indexed.jobID, TenantID: "tenant", OwnerID: "pending-owner", OrganizationID: 1,
		State: indexed.state, CreatedAtUnixMicro: indexed.createdAt, SourceIndexNames: indexed.sourceIndexNames,
		EntryProto: indexed.encoded, EntrySHA256: indexed.checksum[:],
	}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.CompleteAttempt(ctx, AccessScope{TenantID: "tenant", OwnerID: "terminal-owner", OrganizationID: "1"}, historyEntry("shared-job", "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, testStoreClock()))
		done <- err
	}()
	waitHistoryLock(t, database.GORMDB(), key)
	if err := held.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("contradictory job owner was admitted: %v", err)
	}
	var terminal, pending int64
	if err := database.GORMDB().Model(&historyRecord{}).Count(&terminal).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.GORMDB().Model(&pendingHistoryRecord{}).Count(&pending).Error; err != nil {
		t.Fatal(err)
	}
	if terminal != 0 || pending != 1 {
		t.Fatalf("job authority exists in conflicting journals: terminal=%d pending=%d", terminal, pending)
	}
}

func waitHistoryLock(t *testing.T, database *gorm.DB, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		err := database.WithContext(ctx).Raw(`WITH expected AS (SELECT hashtextextended(current_database() || ':' || current_schema() || ':' || ?,0) AS key)
SELECT EXISTS(SELECT 1 FROM pg_locks,expected WHERE locktype='advisory' AND NOT granted
AND classid=((expected.key >> 32) & 4294967295)::oid AND objid=(expected.key & 4294967295)::oid AND objsubid=1)`, key).Scan(&waiting).Error
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("history mutation did not contend on its invariant")
		case <-tick.C:
		}
	}
}
