package savedobjects

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestSharingScopeAuditFailureAndInvalidScopeLeaveStoredStateUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, store := openTestStore(t)
	owner := AccessScope{OwnerID: "scope-owner", OrganizationID: "1"}
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
	owner := AccessScope{OwnerID: "scope-owner", OrganizationID: "1"}
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
