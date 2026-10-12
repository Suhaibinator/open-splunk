package savedobjects_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func scopeDefinition(name, appID string) *opensplunk.SavedSearchDefinition {
	return &opensplunk.SavedSearchDefinition{Name: name, Search: &opensplunk.SearchDefinition{Spl: "index=main | stats count by host", AppId: &appID, IndexScope: []string{"main", "audit"}}, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE}
}

func TestSharingScopeUpdatePreservesDefinitionScheduleAndOwnerAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := control.Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := savedobjects.New(database, savedobjects.Options{CursorKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	owner := savedobjects.AccessScope{OwnerID: "scope-owner", OrganizationID: "1"}
	created, err := store.Create(ctx, owner, scopeDefinition("scoped", "search"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	schedules, err := scheduledreports.NewRepository(database.GORMDB(), scheduledreports.RepositoryOptions{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	nextRun := now.Add(time.Hour)
	beforeSchedule, err := schedules.Configure(ctx, owner, "tenant", created.SavedSearchId, 0, scheduledreports.Configuration{
		Cron: "0 * * * *", Timezone: "UTC", DispatchTTL: "2p", Enabled: true,
	}, &nextRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, sharing := range []opensplunk.SharingScope{
		opensplunk.SharingScope_SHARING_SCOPE_APP,
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	} {
		// Deliberately stale unrelated fields prove that the scope mask is the
		// boundary, even when callers carry a whole definition in their request.
		incoming := scopeDefinition("stale name", "stale-app")
		incoming.Search.Spl = "index=other"
		incoming.Search.TimeRange = &opensplunk.TimeRangeSpec{Earliest: new("-1m"), Latest: new("now")}
		incoming.SharingScope = sharing
		incoming.Schedule = &opensplunk.SavedSearchSchedule{Cron: "* * * * *", Timezone: "UTC"}
		updated, updateErr := store.Update(ctx, owner, created.SavedSearchId, created.Version, incoming, &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}})
		if updateErr != nil {
			t.Fatal(updateErr)
		}
		want := proto.Clone(created.Definition).(*opensplunk.SavedSearchDefinition)
		want.SharingScope = sharing
		if !proto.Equal(updated.Definition, want) {
			t.Fatalf("scope update changed unrelated definition fields: got %v, want %v", updated.Definition, want)
		}
		reopened, getErr := store.Get(ctx, owner, created.SavedSearchId)
		if getErr != nil || !proto.Equal(updated, reopened) {
			t.Fatalf("scope did not persist: %v, %v", reopened, getErr)
		}
		afterSchedule, scheduleErr := schedules.Get(ctx, owner, created.SavedSearchId)
		if scheduleErr != nil || !reflect.DeepEqual(beforeSchedule, afterSchedule) {
			t.Fatalf("scope changed schedule: %v, %v", afterSchedule, scheduleErr)
		}
		other := savedobjects.AccessScope{OwnerID: "other-owner", OrganizationID: "1"}
		_, getErr = store.Get(ctx, other, created.SavedSearchId)
		if sharing == opensplunk.SharingScope_SHARING_SCOPE_PRIVATE {
			if !errors.Is(getErr, control.ErrNotFound) {
				t.Fatalf("private source readable by another owner: %v", getErr)
			}
		} else if getErr != nil {
			t.Fatalf("shared source unavailable in same organization: %v", getErr)
		}
		foreign := savedobjects.AccessScope{OwnerID: other.OwnerID, OrganizationID: "2"}
		if _, err := store.Get(ctx, foreign, created.SavedSearchId); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("sharing crossed organization: %v", err)
		}
		if _, updateErr := store.Update(ctx, other, created.SavedSearchId, updated.Version, incoming, &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}}); !errors.Is(updateErr, control.ErrNotFound) {
			t.Fatalf("scope %v granted another owner update authority: %v", sharing, updateErr)
		}
		if _, scheduleErr := schedules.Get(ctx, other, created.SavedSearchId); !errors.Is(scheduleErr, scheduledreports.ErrNotFound) {
			t.Fatalf("scope %v granted another owner schedule authority: %v", sharing, scheduleErr)
		}
		sharedRun, sharedClaimed, claimErr := schedules.ClaimOneOff(ctx, other, "tenant", created.SavedSearchId, now, time.Hour, 2*time.Hour)
		if sharing == opensplunk.SharingScope_SHARING_SCOPE_PRIVATE {
			if sharedClaimed || !errors.Is(claimErr, scheduledreports.ErrNotFound) {
				t.Fatalf("private source granted run authority: claimed %v, %v", sharedClaimed, claimErr)
			}
		} else {
			if claimErr != nil || !sharedClaimed || sharedRun.OwnerID != other.OwnerID || sharedRun.OrganizationID != "1" {
				t.Fatalf("shared source run did not preserve run-as organization: %+v, %v", sharedRun, claimErr)
			}
			if err := schedules.Finish(ctx, sharedRun.RunID, scheduledreports.RunOutcomeSucceeded, "", now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		if _, claimed, err := schedules.ClaimOneOff(ctx, foreign, "tenant", created.SavedSearchId, now, time.Hour, 2*time.Hour); claimed || !errors.Is(err, scheduledreports.ErrNotFound) {
			t.Fatalf("source run crossed organization: %v, %v", claimed, err)
		}
		created = updated
	}
	run, claimed, err := schedules.ClaimRunNow(ctx, beforeSchedule, now, time.Hour, 2*time.Hour)
	if err != nil || !claimed || run.OwnerID != owner.OwnerID || run.TenantID != "tenant" || run.DefinitionVersion != created.Version || !proto.Equal(run.Definition.Search, created.Definition.Search) {
		t.Fatalf("scope updates changed scheduled-run authority/definition: %+v, claimed %v, %v", run, claimed, err)
	}
}
