package scheduledreports

import (
	"context"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

func TestScheduledSearchDeletionTakesExclusiveCatalogBeforeCreator(t *testing.T) {
	database, err := control.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	saved, err := savedobjects.New(database, savedobjects.Options{CursorKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := NewRepository(database.GORMDB(), RepositoryOptions{CursorKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	owners := []string{"owner-a", "owner-b"}
	ids := make([]string, len(owners))
	for i, owner := range owners {
		source, err := saved.Create(t.Context(), savedobjects.AccessScope{OwnerID: owner, OrganizationID: "1"}, &opensplunk.SavedSearchDefinition{Name: owner, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, Search: &opensplunk.SearchDefinition{Spl: "index=main"}})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = source.SavedSearchId
		if _, err = schedules.Configure(t.Context(), AccessScope{OwnerID: owner, OrganizationID: "1"}, "tenant", ids[i], 0, Configuration{Cron: "*/5 * * * *", Timezone: "UTC", DispatchTTL: "2p", Enabled: false}, nil); err != nil {
			t.Fatal(err)
		}
	}
	blocker := pgmeta.BeginCatalogReader(database.GORMDB().WithContext(t.Context()), "independent-catalog-reader")
	if blocker.Error != nil {
		t.Fatal(blocker.Error)
	}
	defer blocker.Rollback()
	var blockerPID int
	if err = blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	outcomes := make(chan error, len(owners))
	for i, owner := range owners {
		go func() {
			outcomes <- saved.Delete(ctx, savedobjects.AccessScope{OwnerID: owner, OrganizationID: "1"}, ids[i], 1)
		}()
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiters int
		if err = database.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))", blockerPID).Scan(&waiters); err != nil {
			t.Fatal(err)
		}
		if waiters == len(owners) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	probe, stopProbe := context.WithTimeout(ctx, 2*time.Second)
	defer stopProbe()
	observer := pgmeta.Begin(database.GORMDB().WithContext(probe))
	if observer.Error != nil {
		t.Fatal(observer.Error)
	}
	defer observer.Rollback()
	if err = pgmeta.Lock(observer, "saved-content:owner-a", "saved-content:owner-b"); err != nil {
		t.Fatalf("delete took creator key before exclusive catalog: %v", err)
	}
	if err = observer.Exec("SELECT saved_search_id FROM saved_searches FOR UPDATE").Error; err != nil {
		t.Fatalf("delete took row before exclusive catalog: %v", err)
	}
	if err = observer.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err = blocker.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	for range owners {
		if err = <-outcomes; err != nil {
			t.Fatalf("concurrent scheduled deletion: %v", err)
		}
	}
	var remaining int
	if err = database.SQLDB().QueryRowContext(ctx, "SELECT (SELECT count(*) FROM saved_searches)+(SELECT count(*) FROM saved_search_schedules)").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("incomplete cascade: %d %v", remaining, err)
	}
}
