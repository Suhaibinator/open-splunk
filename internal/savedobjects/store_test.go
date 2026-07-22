package savedobjects

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var testCursorKey = []byte("saved-search-test-cursor-key-32-bytes-minimum")

type testDependencies struct {
	clockCalls atomic.Int64
	idCalls    atomic.Int64
}

func (dependencies *testDependencies) options() Options {
	base := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	return Options{
		CursorKey: testCursorKey,
		Clock: func() time.Time {
			return base.Add(time.Duration(dependencies.clockCalls.Add(1)) * time.Microsecond)
		},
		IDGenerator: func() (string, error) {
			return fmt.Sprintf("ss_test_%04d", dependencies.idCalls.Add(1)), nil
		},
	}
}

func openTestStore(t *testing.T) (*control.DB, *Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	dependencies := new(testDependencies)
	store, err := New(database, dependencies.options())
	if err != nil {
		database.Close()
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database, store, path
}

func savedSearchDefinition(name, appID string) *opensplunkv1.SavedSearchDefinition {
	description := " description for " + name + " "
	app := " " + appID + " "
	return &opensplunkv1.SavedSearchDefinition{
		Name:        " " + name + " ",
		Description: &description,
		Search: &opensplunkv1.SearchDefinition{
			Spl:                "index=main | stats count by host",
			AppId:              &app,
			IndexScope:         []string{" main ", "audit", "main"},
			SelectedFields:     []string{" host ", "count", "host"},
			PreferredResultTab: opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED,
		},
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
	}
}

func TestNewValidatesDependenciesAndClonesCursorKey(t *testing.T) {
	database, _, _ := openTestStore(t)
	for _, options := range []Options{{}, {CursorKey: make([]byte, 31)}} {
		if _, err := New(database, options); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("New(%d byte key) error = %v, want ErrInvalidArgument", len(options.CursorKey), err)
		}
	}
	if _, err := New(nil, Options{CursorKey: testCursorKey}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(nil) error = %v, want ErrInvalidArgument", err)
	}

	key := slices.Clone(testCursorKey)
	store, err := New(database, Options{CursorKey: key})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	key[0] ^= 0xff
	if store.cursorKey[0] == key[0] {
		t.Fatal("New() retained caller cursor-key storage")
	}
}

func TestCreateGetUpdateDeleteNormalizeAndDoNotAlias(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: " user-1 "}
	input := savedSearchDefinition("Errors", "search")
	created, err := store.Create(ctx, scope, input)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.SavedSearchId != "ss_test_0001" || created.Version != 1 {
		t.Fatalf("Create() identity = (%q,%d)", created.SavedSearchId, created.Version)
	}
	if created.Definition.Name != "Errors" || created.Definition.GetDescription() != "description for Errors" || created.Definition.GetOwnerId() != "user-1" || created.Definition.Search.GetAppId() != "search" {
		t.Fatalf("Create() did not normalize definition: %+v", created.Definition)
	}
	if created.Definition.Search.PreferredResultTab != opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_EVENTS || !slices.Equal(created.Definition.Search.IndexScope, []string{"main", "audit"}) {
		t.Fatalf("Create() did not normalize search: %+v", created.Definition.Search)
	}

	input.Name = "mutated input"
	input.Search.Spl = "mutated input SPL"
	created.Definition.Name = "mutated result"
	created.Definition.Search.Spl = "mutated result SPL"
	got, err := store.Get(ctx, AccessScope{OwnerID: "user-1"}, created.SavedSearchId)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Definition.Name != "Errors" || got.Definition.Search.Spl != "index=main | stats count by host" {
		t.Fatalf("persistent definition aliased caller: %+v", got.Definition)
	}

	patch := &opensplunkv1.SavedSearchDefinition{Name: " Renamed ", Search: &opensplunkv1.SearchDefinition{Spl: "ignored"}}
	updated, err := store.Update(ctx, scope, got.SavedSearchId, 1, patch, &fieldmaskpb.FieldMask{Paths: []string{"definition.name"}})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Version != 2 || updated.Definition.Name != "Renamed" || updated.Definition.Search.Spl != got.Definition.Search.Spl {
		t.Fatalf("Update() = %+v", updated)
	}
	if !updated.UpdatedAt.AsTime().After(updated.CreatedAt.AsTime()) {
		t.Fatalf("Update timestamps did not advance: created=%v updated=%v", updated.CreatedAt, updated.UpdatedAt)
	}
	patch.Name = "mutated"
	updated.Definition.Name = "mutated result"
	got, err = store.Get(ctx, scope, got.SavedSearchId)
	if err != nil || got.Definition.Name != "Renamed" {
		t.Fatalf("Get(after update) = (%+v,%v)", got, err)
	}

	if _, err := store.Update(ctx, scope, got.SavedSearchId, 1, got.Definition, nil); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale Update() error = %v, want ErrVersionConflict", err)
	}
	if err := store.Delete(ctx, scope, got.SavedSearchId, 1); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale Delete() error = %v, want ErrVersionConflict", err)
	}
	if err := store.Delete(ctx, scope, got.SavedSearchId, 2); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Get(ctx, scope, got.SavedSearchId); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(after delete) error = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, scope, got.SavedSearchId, 2); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Delete(after delete) error = %v, want ErrNotFound", err)
	}
}

func TestDefinitionPersistenceIsDeterministicAndDefinitionOnly(t *testing.T) {
	database, store, _ := openTestStore(t)
	created, err := store.Create(context.Background(), AccessScope{OwnerID: "owner"}, savedSearchDefinition("Canonical", "app"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	want, err := (proto.MarshalOptions{Deterministic: true}).Marshal(created.Definition)
	if err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	var name, appID, ownerID string
	if err := database.SQLDB().QueryRow(`SELECT definition_proto, name, app_id, owner_id FROM saved_searches WHERE saved_search_id = ?`, created.SavedSearchId).Scan(&encoded, &name, &appID, &ownerID); err != nil {
		t.Fatalf("read raw definition: %v", err)
	}
	if !slices.Equal(encoded, want) || name != "Canonical" || appID != "app" || ownerID != "owner" {
		t.Fatalf("stored record mismatch: protoEqual=%v name=%q app=%q owner=%q", slices.Equal(encoded, want), name, appID, ownerID)
	}
	if strings.Contains(string(encoded), "SELECT ") || strings.Contains(string(encoded), "clickhouse") {
		t.Fatal("stored definition unexpectedly contains generated SQL/storage state")
	}
}

func TestValidationOwnershipAndSharing(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	tests := []struct {
		name   string
		mutate func(*opensplunkv1.SavedSearchDefinition)
	}{
		{name: "nil search", mutate: func(definition *opensplunkv1.SavedSearchDefinition) { definition.Search = nil }},
		{name: "empty name", mutate: func(definition *opensplunkv1.SavedSearchDefinition) { definition.Name = "  " }},
		{name: "empty SPL", mutate: func(definition *opensplunkv1.SavedSearchDefinition) { definition.Search.Spl = "\n" }},
		{name: "invalid sharing", mutate: func(definition *opensplunkv1.SavedSearchDefinition) { definition.SharingScope = 99 }},
		{name: "app sharing no app", mutate: func(definition *opensplunkv1.SavedSearchDefinition) {
			definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_APP
			definition.Search.AppId = nil
		}},
		{name: "owner mismatch", mutate: func(definition *opensplunkv1.SavedSearchDefinition) {
			other := "other"
			definition.OwnerId = &other
		}},
		{name: "too many fields", mutate: func(definition *opensplunkv1.SavedSearchDefinition) {
			definition.Search.SelectedFields = make([]string, maximumRepeatedFields+1)
		}},
		{name: "bad result tab", mutate: func(definition *opensplunkv1.SavedSearchDefinition) { definition.Search.PreferredResultTab = 99 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := savedSearchDefinition("test", "app")
			test.mutate(definition)
			if _, err := store.Create(ctx, scope, definition); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Create() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if _, err := store.Create(ctx, AccessScope{}, savedSearchDefinition("test", "app")); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Create(empty scope) error = %v", err)
	}
	definition := savedSearchDefinition("default sharing", "")
	definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED
	created, err := store.Create(ctx, scope, definition)
	if err != nil || created.Definition.SharingScope != opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE {
		t.Fatalf("default sharing Create() = (%+v,%v)", created, err)
	}
}

func TestUniquenessClassificationAndOwnerIsolation(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	first, err := store.Create(ctx, AccessScope{OwnerID: "owner-a"}, savedSearchDefinition("Same", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, AccessScope{OwnerID: "owner-a"}, savedSearchDefinition("Same", "app")); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("duplicate Create() error = %v, want ErrAlreadyExists", err)
	}
	if _, err := store.Create(ctx, AccessScope{OwnerID: "owner-a"}, savedSearchDefinition("Same", "other-app")); err != nil {
		t.Fatalf("same name in another app error = %v", err)
	}
	second, err := store.Create(ctx, AccessScope{OwnerID: "owner-a"}, savedSearchDefinition("Second", "app"))
	if err != nil {
		t.Fatal(err)
	}
	rename := proto.Clone(second.Definition).(*opensplunkv1.SavedSearchDefinition)
	rename.Name = "Same"
	if _, err := store.Update(ctx, AccessScope{OwnerID: "owner-a"}, second.SavedSearchId, 1, rename, nil); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("conflicting rename error = %v, want ErrAlreadyExists", err)
	}
	if _, err := store.Create(ctx, AccessScope{OwnerID: "owner-b"}, savedSearchDefinition("Same", "app")); err != nil {
		t.Fatalf("same name for another owner error = %v", err)
	}
	if _, err := store.Get(ctx, AccessScope{OwnerID: "owner-b"}, first.SavedSearchId); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Get() error = %v, want ErrNotFound", err)
	}
	if _, err := store.Update(ctx, AccessScope{OwnerID: "owner-b"}, first.SavedSearchId, 1, first.Definition, nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Update() error = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, AccessScope{OwnerID: "owner-b"}, first.SavedSearchId, 1); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Delete() error = %v, want ErrNotFound", err)
	}
}

func TestConcurrentOptimisticUpdateAllowsOneWriter(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	created, err := store.Create(ctx, scope, savedSearchDefinition("Original", "app"))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsByWriter := make(chan error, 2)
	var wait sync.WaitGroup
	for _, name := range []string{"Writer A", "Writer B"} {
		wait.Add(1)
		go func(name string) {
			defer wait.Done()
			definition := proto.Clone(created.Definition).(*opensplunkv1.SavedSearchDefinition)
			definition.Name = name
			<-start
			_, err := store.Update(ctx, scope, created.SavedSearchId, 1, definition, nil)
			errorsByWriter <- err
		}(name)
	}
	close(start)
	wait.Wait()
	close(errorsByWriter)
	var successes, conflicts int
	for err := range errorsByWriter {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, control.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent Update() unexpected error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent updates: successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestConcurrentCreateClassifiesUniqueName(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Create(ctx, AccessScope{OwnerID: "owner"}, savedSearchDefinition("same", "app"))
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var success, exists int
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, control.ErrAlreadyExists) {
			exists++
		} else {
			t.Fatalf("Create() unexpected error = %v", err)
		}
	}
	if success != 1 || exists != 1 {
		t.Fatalf("concurrent creates: success=%d exists=%d", success, exists)
	}
}

func TestDuplicateClonesAndClassifiesConflicts(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	source, err := store.Create(ctx, scope, savedSearchDefinition("source", "app-a"))
	if err != nil {
		t.Fatal(err)
	}
	destination := " app-b "
	duplicate, err := store.Duplicate(ctx, scope, source.SavedSearchId, " copy ", &destination)
	if err != nil {
		t.Fatalf("Duplicate() error = %v", err)
	}
	if duplicate.SavedSearchId == source.SavedSearchId || duplicate.Version != 1 || duplicate.Definition.Name != "copy" || duplicate.Definition.Search.GetAppId() != "app-b" {
		t.Fatalf("Duplicate() = %+v", duplicate)
	}
	duplicate.Definition.Search.Spl = "mutated"
	gotSource, err := store.Get(ctx, scope, source.SavedSearchId)
	if err != nil || gotSource.Definition.Search.Spl == "mutated" {
		t.Fatalf("duplicate aliased source: (%+v,%v)", gotSource, err)
	}
	if _, err := store.Duplicate(ctx, scope, source.SavedSearchId, "copy", &destination); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("duplicate name error = %v, want ErrAlreadyExists", err)
	}
	if _, err := store.Duplicate(ctx, AccessScope{OwnerID: "other"}, source.SavedSearchId, "copy", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Duplicate() error = %v, want ErrNotFound", err)
	}
}

func TestListPaginationFiltersSortingCursorBindingAndNoAliasing(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	definitions := []*opensplunkv1.SavedSearchDefinition{
		savedSearchDefinition("delta", "app-a"),
		savedSearchDefinition("Alpha", "app-a"),
		savedSearchDefinition("charlie", "app-b"),
		savedSearchDefinition("bravo", "app-a"),
		savedSearchDefinition("Echo", "app-a"),
	}
	definitions[2].SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
	definitions[4].SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_APP
	for _, definition := range definitions {
		if _, err := store.Create(ctx, scope, definition); err != nil {
			t.Fatalf("Create(%q) error = %v", definition.Name, err)
		}
	}
	oldest, err := store.Get(ctx, scope, "ss_test_0001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(ctx, scope, oldest.SavedSearchId, oldest.Version, oldest.Definition, nil); err != nil {
		t.Fatalf("Update(delta timestamp) error = %v", err)
	}

	request := ListRequest{PageSize: 2, IncludeTotal: true}
	var names []string
	for {
		page, err := store.List(ctx, scope, request)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if page.TotalSize == nil || *page.TotalSize != 5 || !page.TotalSizeExact {
			t.Fatalf("List total = (%v,%v), want exact 5", page.TotalSize, page.TotalSizeExact)
		}
		for _, savedSearch := range page.SavedSearches {
			names = append(names, savedSearch.Definition.Name)
		}
		if page.NextPageToken == nil {
			if len(page.SavedSearches) != 1 {
				t.Fatalf("last page size = %d, want 1", len(page.SavedSearches))
			}
			break
		}
		request.PageToken = *page.NextPageToken
	}
	if !slices.Equal(names, []string{"Alpha", "Echo", "bravo", "charlie", "delta"}) {
		t.Fatalf("name pages = %v", names)
	}

	app := "app-a"
	text := "A"
	private := []opensplunkv1.SharingScope{opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE}
	filtered, err := store.List(ctx, scope, ListRequest{AppIDFilter: &app, TextFilter: &text, SharingScopeFilters: private})
	if err != nil {
		t.Fatalf("filtered List() error = %v", err)
	}
	var filteredNames []string
	for _, savedSearch := range filtered.SavedSearches {
		filteredNames = append(filteredNames, savedSearch.Definition.Name)
	}
	if !slices.Equal(filteredNames, []string{"Alpha", "bravo", "delta"}) {
		t.Fatalf("filtered names = %v", filteredNames)
	}

	sortTests := []struct {
		name      string
		sortBy    opensplunkv1.SavedSearchSortBy
		direction opensplunkv1.SortDirection
		want      []string
	}{
		{name: "name ascending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME, direction: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"Alpha", "Echo", "bravo", "charlie", "delta"}},
		{name: "name descending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME, direction: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"delta", "charlie", "bravo", "Echo", "Alpha"}},
		{name: "created ascending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT, direction: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"delta", "Alpha", "charlie", "bravo", "Echo"}},
		{name: "created descending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT, direction: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"Echo", "bravo", "charlie", "Alpha", "delta"}},
		{name: "updated ascending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT, direction: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"Alpha", "charlie", "bravo", "Echo", "delta"}},
		{name: "updated descending", sortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT, direction: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"delta", "Echo", "bravo", "charlie", "Alpha"}},
	}
	for _, test := range sortTests {
		t.Run(test.name, func(t *testing.T) {
			request := ListRequest{PageSize: 2, SortBy: test.sortBy, SortDirection: test.direction}
			var got []string
			for {
				page, err := store.List(ctx, scope, request)
				if err != nil {
					t.Fatalf("List() error = %v", err)
				}
				for _, record := range page.SavedSearches {
					got = append(got, record.Definition.Name)
				}
				if page.NextPageToken == nil {
					break
				}
				request.PageToken = *page.NextPageToken
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("sorted names = %v, want %v", got, test.want)
			}
		})
	}

	descending, err := store.List(ctx, scope, ListRequest{
		PageSize: 5, SortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstID := descending.SavedSearches[0].SavedSearchId
	descending.SavedSearches[0].Definition.Name = "mutated"
	got, err := store.Get(ctx, scope, firstID)
	if err != nil || got.Definition.Name == "mutated" {
		t.Fatalf("List result aliased persistence: (%+v,%v)", got, err)
	}

	firstPage, err := store.List(ctx, scope, ListRequest{PageSize: 1})
	if err != nil || firstPage.NextPageToken == nil {
		t.Fatalf("first cursor page = (%+v,%v)", firstPage, err)
	}
	token := *firstPage.NextPageToken
	tampered := token[:len(token)-1] + differentCursorByte(token[len(token)-1])
	if _, err := store.List(ctx, scope, ListRequest{PageSize: 1, PageToken: tampered}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("tampered cursor error = %v, want ErrInvalidArgument", err)
	}
	changedApp := "app-a"
	for name, changed := range map[string]ListRequest{
		"app":       {PageSize: 1, PageToken: token, AppIDFilter: &changedApp},
		"text":      {PageSize: 1, PageToken: token, TextFilter: &text},
		"sharing":   {PageSize: 1, PageToken: token, SharingScopeFilters: private},
		"sort":      {PageSize: 1, PageToken: token, SortBy: opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT},
		"direction": {PageSize: 1, PageToken: token, SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING},
	} {
		t.Run("cursor binding "+name, func(t *testing.T) {
			if _, err := store.List(ctx, scope, changed); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("List() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if _, err := store.List(ctx, AccessScope{OwnerID: "other"}, ListRequest{PageSize: 1, PageToken: token}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("cross-owner cursor error = %v, want ErrInvalidArgument", err)
	}
}

func differentCursorByte(value byte) string {
	if value == 'A' {
		return "B"
	}
	return "A"
}

func TestCursorAndRecordsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sqlite")
	ctx := context.Background()
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := new(testDependencies)
	store, err := New(database, dependencies.options())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, err := store.Create(ctx, AccessScope{OwnerID: "owner"}, savedSearchDefinition(name, "app")); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.List(ctx, AccessScope{OwnerID: "owner"}, ListRequest{PageSize: 1})
	if err != nil || first.NextPageToken == nil {
		t.Fatalf("List() = (%+v,%v)", first, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	reopened, err := New(database, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	second, err := reopened.List(ctx, AccessScope{OwnerID: "owner"}, ListRequest{PageSize: 1, PageToken: *first.NextPageToken})
	if err != nil || len(second.SavedSearches) != 1 || second.SavedSearches[0].Definition.Name != "b" {
		t.Fatalf("List(after reopen) = (%+v,%v)", second, err)
	}
	differentKeyStore, err := New(database, Options{CursorKey: []byte("a-different-stable-cursor-key-32-bytes")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := differentKeyStore.List(ctx, AccessScope{OwnerID: "owner"}, ListRequest{PageSize: 1, PageToken: *first.NextPageToken}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("List(different key) error = %v, want ErrInvalidArgument", err)
	}
}

func TestMalformedStoredProtoAndMetadataAreRejected(t *testing.T) {
	database, store, _ := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	created, err := store.Create(ctx, scope, savedSearchDefinition("valid", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().Exec(`UPDATE saved_searches SET definition_proto = x'ff' WHERE saved_search_id = ?`, created.SavedSearchId); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, created.SavedSearchId); err == nil || errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(malformed proto) error = %v", err)
	}

	other, err := store.Create(ctx, scope, savedSearchDefinition("valid two", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().Exec(`UPDATE saved_searches SET name = 'mismatch' WHERE saved_search_id = ?`, other.SavedSearchId); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, other.SavedSearchId); err == nil {
		t.Fatal("Get(mismatched metadata) unexpectedly succeeded")
	}
}

func TestCancellationAndBoundedInputs(t *testing.T) {
	_, store, _ := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Create(ctx, AccessScope{OwnerID: "owner"}, savedSearchDefinition("test", "app")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.Get(nil, AccessScope{OwnerID: "owner"}, "id"); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(nil context) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.List(context.Background(), AccessScope{OwnerID: "owner"}, ListRequest{PageSize: maximumListPageSize + 1}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("List(large page) error = %v", err)
	}
	if _, err := store.List(context.Background(), AccessScope{OwnerID: "owner"}, ListRequest{PageToken: string(make([]byte, maximumCursorBytes+1))}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("List(large token) error = %v", err)
	}
	definition := savedSearchDefinition("test", "app")
	created, err := store.Create(context.Background(), AccessScope{OwnerID: "owner"}, definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(context.Background(), AccessScope{OwnerID: "owner"}, created.SavedSearchId, 1, definition, &fieldmaskpb.FieldMask{Paths: []string{"search.spl"}}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Update(unsupported mask) error = %v", err)
	}
	if err := store.Delete(context.Background(), AccessScope{OwnerID: "owner"}, created.SavedSearchId, 0); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Delete(version 0) error = %v", err)
	}
}
