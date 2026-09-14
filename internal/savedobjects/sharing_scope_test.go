package savedobjects

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestSharingScopeUpdatePreservesDefinitionScheduleAndOwnerAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, store := openTestStore(t)
	owner := AccessScope{OwnerID: "scope-owner"}
	created, err := store.Create(ctx, owner, savedSearchDefinition("scoped", "search"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	schedules, err := scheduledreports.NewRepository(database.GORMDB(), scheduledreports.RepositoryOptions{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	nextRun := now.Add(time.Hour)
	beforeSchedule, err := schedules.Configure(ctx, owner.OwnerID, "tenant", created.SavedSearchId, 0, scheduledreports.Configuration{
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
		incoming := savedSearchDefinition("stale name", "stale-app")
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
		afterSchedule, scheduleErr := schedules.Get(ctx, owner.OwnerID, created.SavedSearchId)
		if scheduleErr != nil || !reflect.DeepEqual(beforeSchedule, afterSchedule) {
			t.Fatalf("scope changed schedule: %v, %v", afterSchedule, scheduleErr)
		}
		other := AccessScope{OwnerID: "other-owner"}
		if _, getErr := store.Get(ctx, other, created.SavedSearchId); !errors.Is(getErr, control.ErrNotFound) {
			t.Fatalf("scope %v granted another owner read authority: %v", sharing, getErr)
		}
		if _, updateErr := store.Update(ctx, other, created.SavedSearchId, updated.Version, incoming, &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}}); !errors.Is(updateErr, control.ErrNotFound) {
			t.Fatalf("scope %v granted another owner update authority: %v", sharing, updateErr)
		}
		if _, scheduleErr := schedules.Get(ctx, other.OwnerID, created.SavedSearchId); !errors.Is(scheduleErr, scheduledreports.ErrNotFound) {
			t.Fatalf("scope %v granted another owner schedule authority: %v", sharing, scheduleErr)
		}
		if _, claimed, claimErr := schedules.ClaimOneOff(ctx, other.OwnerID, "tenant", created.SavedSearchId, now, time.Hour, 2*time.Hour); claimed || !errors.Is(claimErr, scheduledreports.ErrNotFound) {
			t.Fatalf("scope %v granted another owner run authority: claimed %v, %v", sharing, claimed, claimErr)
		}
		created = updated
	}
	run, claimed, err := schedules.ClaimRunNow(ctx, beforeSchedule, now, time.Hour, 2*time.Hour)
	if err != nil || !claimed || run.OwnerID != owner.OwnerID || run.TenantID != "tenant" || run.DefinitionVersion != created.Version || !proto.Equal(run.Definition.Search, created.Definition.Search) {
		t.Fatalf("scope updates changed scheduled-run authority/definition: %+v, claimed %v, %v", run, claimed, err)
	}
}

func TestSharingScopeAuditFailureAndInvalidScopeLeaveStoredStateUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, store := openTestStore(t)
	owner := AccessScope{OwnerID: "scope-owner"}
	created, err := store.Create(ctx, owner, savedSearchDefinition("scoped", "search"))
	if err != nil {
		t.Fatal(err)
	}
	before := readSavedSearchAuditPersistence(t, database)
	appender := &recordingSavedSearchAuditAppender{failAction: SavedSearchMutationAuditActionUpdate}
	audited := newSavedSearchAuditStore(t, store, appender)
	mask := &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}}
	for _, test := range []struct {
		scope opensplunk.SharingScope
		want  error
	}{
		{scope: opensplunk.SharingScope(99), want: control.ErrInvalidArgument},
		{scope: opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, want: errTestSavedSearchAuditAppend},
	} {
		incoming := proto.Clone(created.Definition).(*opensplunk.SavedSearchDefinition)
		incoming.SharingScope = test.scope
		if _, err := audited.Update(ctx, owner, created.SavedSearchId, created.Version, incoming, mask); !errors.Is(err, test.want) {
			t.Fatalf("scope %v update error = %v, want %v", test.scope, err, test.want)
		}
		if after := readSavedSearchAuditPersistence(t, database); !reflect.DeepEqual(before, after) {
			t.Fatalf("rejected scope %v changed persisted definition/version", test.scope)
		}
	}
	calls := appender.snapshot()
	if len(calls) != 1 || !calls[0].insideSQL || calls[0].row.SharingScope != int64(opensplunk.SharingScope_SHARING_SCOPE_GLOBAL) {
		t.Fatalf("scope audit must observe pending mutation inside rollback transaction: %+v", calls)
	}
}

func TestSharingScopeConcurrentEditorsRequireNewVersionForSecondSubmission(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store := openTestStore(t)
	owner := AccessScope{OwnerID: "scope-owner"}
	created, err := store.Create(ctx, owner, savedSearchDefinition("scoped", "search"))
	if err != nil {
		t.Fatal(err)
	}
	appender := &recordingSavedSearchAuditAppender{}
	audited := newSavedSearchAuditStore(t, store, appender)
	mask := &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}}
	start := make(chan struct{})
	type outcome struct {
		scope opensplunk.SharingScope
		err   error
	}
	results := make(chan outcome, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, sharing := range []opensplunk.SharingScope{opensplunk.SharingScope_SHARING_SCOPE_APP, opensplunk.SharingScope_SHARING_SCOPE_GLOBAL} {
		go func() {
			incoming := proto.Clone(created.Definition).(*opensplunk.SavedSearchDefinition)
			incoming.SharingScope = sharing
			ready.Done()
			<-start
			_, err := audited.Update(ctx, owner, created.SavedSearchId, created.Version, incoming, mask)
			results <- outcome{scope: sharing, err: err}
		}()
	}
	ready.Wait()
	close(start)
	var loser opensplunk.SharingScope
	successes := 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil:
			successes++
		case errors.Is(result.err, control.ErrVersionConflict):
			loser = result.scope
		default:
			t.Fatalf("concurrent scope update: %v", result.err)
		}
	}
	if successes != 1 || loser == opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED || len(appender.snapshot()) != 1 {
		t.Fatalf("expected one committed editor and one conflict, got %d successes and loser %v", successes, loser)
	}
	baseline, err := audited.Get(ctx, owner, created.SavedSearchId)
	if err != nil || baseline.Version != created.Version+1 {
		t.Fatalf("fresh conflict baseline = %v, %v", baseline, err)
	}
	proposed := proto.Clone(baseline.Definition).(*opensplunk.SavedSearchDefinition)
	proposed.SharingScope = loser
	updated, err := audited.Update(ctx, owner, baseline.SavedSearchId, baseline.Version, proposed, mask)
	if err != nil || updated.Version != baseline.Version+1 || updated.Definition.SharingScope != loser || !proto.Equal(updated.Definition.Search, created.Definition.Search) {
		t.Fatalf("explicit second submission = %v, %v", updated, err)
	}
}
