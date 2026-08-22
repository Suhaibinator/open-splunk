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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
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

func openTestStore(t *testing.T) (*control.DB, *Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dependencies := new(testDependencies)
	store, err := New(database, dependencies.options())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return database, store
}

func savedSearchDefinition(name, appID string) *opensplunk.SavedSearchDefinition {
	description := " description for " + name + " "
	app := " " + appID + " "
	return &opensplunk.SavedSearchDefinition{
		Name:        " " + name + " ",
		Description: &description,
		Search: &opensplunk.SearchDefinition{
			Spl:                "index=main | stats count by host",
			AppId:              &app,
			IndexScope:         []string{" main ", "audit", "main"},
			SelectedFields:     []string{" host ", "count", "host"},
			PreferredResultTab: opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED,
		},
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	}
}

func TestNewValidatesDependenciesAndClonesCursorKey(t *testing.T) {
	database, _ := openTestStore(t)
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
	if store.orm != database.GORMDB() {
		t.Fatal("New() did not retain the configured shared GORM handle")
	}
	key[0] ^= 0xff
	if store.cursorKey[0] == key[0] {
		t.Fatal("New() retained caller cursor-key storage")
	}
}

func TestSavedSearchGORMModelMatchesMigratedSQLiteSchema(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	statement := &gorm.Statement{DB: database.GORMDB()}
	if err := statement.Parse(&savedSearchRecord{}); err != nil {
		t.Fatalf("parse GORM saved-search model: %v", err)
	}

	rows, err := database.SQLDB().QueryContext(
		context.Background(),
		`SELECT name FROM pragma_table_info('saved_searches') ORDER BY cid`,
	)
	if err != nil {
		t.Fatalf("read migrated saved-search columns: %v", err)
	}
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan migrated saved-search column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated saved-search columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf(
			"GORM saved-search columns = %v, migrated columns = %v",
			statement.Schema.DBNames,
			migratedColumns,
		)
	}
	idField := statement.Schema.LookUpField("SavedSearchID")
	if idField == nil || !idField.PrimaryKey {
		t.Fatalf("GORM saved-search primary key is not explicit: %#v", idField)
	}

	expectedChecks := map[string]string{
		"saved_searches_app_id_length":            "length(app_id) <= 255",
		"saved_searches_definition_length":        "length(definition_proto) BETWEEN 1 AND 262144",
		"saved_searches_id_length":                "length(saved_search_id) BETWEEN 1 AND 128",
		"saved_searches_name_length":              "length(name) BETWEEN 1 AND 255",
		"saved_searches_owner_id_length":          "length(owner_id) <= 255",
		"saved_searches_sharing_scope_range":      "sharing_scope BETWEEN 1 AND 3",
		"saved_searches_update_not_before_create": "updated_at_unix_micro >= created_at_unix_micro",
		"saved_searches_version_positive":         "version >= 1",
	}
	modelChecks := statement.Schema.ParseCheckConstraints()
	if len(modelChecks) != len(expectedChecks) {
		t.Fatalf("GORM saved-search checks = %v, want %v", modelChecks, expectedChecks)
	}
	var migratedDDL string
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'saved_searches'`,
	).Scan(&migratedDDL); err != nil {
		t.Fatalf("read migrated saved-search DDL: %v", err)
	}
	normalizedDDL := strings.Join(strings.Fields(migratedDDL), " ")
	for name, constraint := range expectedChecks {
		check, exists := modelChecks[name]
		if !exists || check.Constraint != constraint {
			t.Errorf("GORM check %s = %#v, want %q", name, check, constraint)
		}
		if !strings.Contains(normalizedDDL, "CHECK ("+constraint+")") {
			t.Errorf("migrated saved-search DDL does not contain check %q", constraint)
		}
	}

	type indexShape struct {
		class   string
		columns []string
		sorts   []string
	}
	expectedIndexes := map[string]indexShape{
		"saved_searches_app_name_id_idx": {
			columns: []string{"app_id", "name", "saved_search_id"},
			sorts:   []string{"", "", ""},
		},
		"saved_searches_owner_app_name_key": {
			class:   "UNIQUE",
			columns: []string{"owner_id", "app_id", "name"},
			sorts:   []string{"", "", ""},
		},
		"saved_searches_owner_created_id_idx": {
			columns: []string{"owner_id", "created_at_unix_micro", "saved_search_id"},
			sorts:   []string{"", "", ""},
		},
		"saved_searches_owner_name_id_idx": {
			columns: []string{"owner_id", "name", "saved_search_id"},
			sorts:   []string{"", "", ""},
		},
		"saved_searches_owner_updated_id_idx": {
			columns: []string{"owner_id", "updated_at_unix_micro", "saved_search_id"},
			sorts:   []string{"", "", ""},
		},
		"saved_searches_updated_idx": {
			columns: []string{"updated_at_unix_micro", "saved_search_id"},
			sorts:   []string{"desc", ""},
		},
	}
	modelIndexes := make(map[string]indexShape)
	for _, index := range statement.Schema.ParseIndexes() {
		shape := indexShape{class: index.Class}
		for _, option := range index.Fields {
			shape.columns = append(shape.columns, option.DBName)
			shape.sorts = append(shape.sorts, option.Sort)
		}
		modelIndexes[index.Name] = shape
	}
	if len(modelIndexes) != len(expectedIndexes) {
		t.Fatalf("GORM saved-search indexes = %v, want %v", modelIndexes, expectedIndexes)
	}
	for name, want := range expectedIndexes {
		got, exists := modelIndexes[name]
		if !exists ||
			got.class != want.class ||
			!slices.Equal(got.columns, want.columns) ||
			!slices.Equal(got.sorts, want.sorts) {
			t.Errorf("GORM index %s = %#v, want %#v", name, got, want)
		}
		if name == "saved_searches_owner_app_name_key" {
			continue
		}
		migratedColumns, migratedDescending := readSavedSearchIndexShape(t, database, name)
		if !slices.Equal(migratedColumns, want.columns) ||
			!slices.Equal(migratedDescending, descendingFlags(want.sorts)) {
			t.Errorf(
				"migrated index %s = columns %v descending %v, want columns %v descending %v",
				name,
				migratedColumns,
				migratedDescending,
				want.columns,
				descendingFlags(want.sorts),
			)
		}
	}
	assertMigratedSavedSearchUniqueKey(t, database, expectedIndexes["saved_searches_owner_app_name_key"].columns)
}

func TestSavedSearchGORMListUsesOwnerKeysetIndexes(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	integerCursor := int64(1)
	tests := []struct {
		name          string
		sortBy        opensplunk.SavedSearchSortBy
		sortDirection opensplunk.SortDirection
		cursor        listCursor
		wantIndex     string
	}{
		{
			name:          "name ascending",
			sortBy:        opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME,
			sortDirection: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			cursor:        listCursor{StringKey: "middle", SavedSearch: "ss_middle"},
			wantIndex:     "saved_searches_owner_name_id_idx",
		},
		{
			name:          "created descending",
			sortBy:        opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
			sortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			cursor:        listCursor{IntegerKey: &integerCursor, SavedSearch: "ss_middle"},
			wantIndex:     "saved_searches_owner_created_id_idx",
		},
		{
			name:          "updated ascending",
			sortBy:        opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT,
			sortDirection: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			cursor:        listCursor{IntegerKey: &integerCursor, SavedSearch: "ss_middle"},
			wantIndex:     "saved_searches_owner_updated_id_idx",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := normalizedListRequest{
				ownerID:       "owner",
				pageSize:      50,
				sortBy:        test.sortBy,
				sortDirection: test.sortDirection,
			}
			query := applySavedSearchListFilters(
				database.GORMDB().Session(&gorm.Session{DryRun: true}).Model(&savedSearchRecord{}),
				request,
			)
			query = applySavedSearchListCursor(query, request, test.cursor)
			query = applySavedSearchListOrder(query, request)
			var records []savedSearchRecord
			generated := query.Limit(int(request.pageSize) + 1).Find(&records)
			if generated.Error != nil {
				t.Fatalf("build GORM list query: %v", generated.Error)
			}
			planRows, err := database.SQLDB().QueryContext(
				context.Background(),
				"EXPLAIN QUERY PLAN "+generated.Statement.SQL.String(),
				generated.Statement.Vars...,
			)
			if err != nil {
				t.Fatalf("explain GORM list query: %v", err)
			}
			var details []string
			for planRows.Next() {
				var id, parent, unused int64
				var detail string
				if err := planRows.Scan(&id, &parent, &unused, &detail); err != nil {
					_ = planRows.Close()
					t.Fatalf("scan GORM list query plan: %v", err)
				}
				details = append(details, detail)
			}
			if err := planRows.Close(); err != nil {
				t.Fatalf("close GORM list query plan: %v", err)
			}
			plan := strings.Join(details, "\n")
			if !strings.Contains(plan, test.wantIndex) {
				t.Errorf("GORM list query plan = %q, want index %q", plan, test.wantIndex)
			}
			if strings.Contains(plan, "USE TEMP B-TREE") {
				t.Errorf("GORM list query plan performs a temporary sort: %q", plan)
			}
		})
	}
}

func TestCreateGetUpdateDeleteNormalizeAndDoNotAlias(t *testing.T) {
	_, store := openTestStore(t)
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
	if created.Definition.Search.PreferredResultTab != opensplunk.SearchResultTab_SEARCH_RESULT_TAB_EVENTS || !slices.Equal(created.Definition.Search.IndexScope, []string{"main", "audit"}) {
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

	patch := &opensplunk.SavedSearchDefinition{Name: " Renamed ", Search: &opensplunk.SearchDefinition{Spl: "ignored"}}
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
	database, store := openTestStore(t)
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
	if err := database.SQLDB().QueryRowContext(context.Background(), `SELECT definition_proto, name, app_id, owner_id FROM saved_searches WHERE saved_search_id = ?`, created.SavedSearchId).Scan(&encoded, &name, &appID, &ownerID); err != nil {
		t.Fatalf("read raw definition: %v", err)
	}
	if !slices.Equal(encoded, want) || name != "Canonical" || appID != "app" || ownerID != "owner" {
		t.Fatalf("stored record mismatch: protoEqual=%v name=%q app=%q owner=%q", slices.Equal(encoded, want), name, appID, ownerID)
	}
	if strings.Contains(string(encoded), "SELECT ") || strings.Contains(string(encoded), "clickhouse") {
		t.Fatal("stored definition unexpectedly contains generated SQL/storage state")
	}
}

func TestGORMPersistenceHonorsMigratedAppWorkspaceTriggers(t *testing.T) {
	database, store := openTestStore(t)
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: []byte("saved-search-app-catalog-test-key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	app, err := catalog.CreateApp(
		context.Background(),
		control.AppAccessScope{TenantID: "tenant"},
		control.AppDefinition{Slug: "search-app", DisplayName: "Search App"},
	)
	if err != nil {
		t.Fatalf("CreateApp() error = %v", err)
	}
	created, err := store.Create(
		context.Background(),
		AccessScope{OwnerID: "owner"},
		savedSearchDefinition("existing canonical app", app.ID),
	)
	if err != nil {
		t.Fatalf("Create(existing canonical app) error = %v", err)
	}
	if created.Definition.Search.GetAppId() != app.ID {
		t.Fatalf("Create(existing canonical app) app = %q, want %q", created.Definition.Search.GetAppId(), app.ID)
	}

	missingCanonicalApp := "app_AAAAAAAAAAAAAAAAAAAAAA"
	if _, err := store.Create(
		context.Background(),
		AccessScope{OwnerID: "owner"},
		savedSearchDefinition("missing canonical app", missingCanonicalApp),
	); err == nil || errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("Create(missing canonical app) error = %v, want migrated dependency rejection", err)
	}
	page, err := store.List(
		context.Background(),
		AccessScope{OwnerID: "owner"},
		ListRequest{IncludeTotal: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalSize == nil || *page.TotalSize != 1 {
		t.Fatalf("saved-search total after rejected app reference = %v, want 1", page.TotalSize)
	}
}

func TestValidationOwnershipAndSharing(t *testing.T) {
	_, store := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	tests := []struct {
		name   string
		mutate func(*opensplunk.SavedSearchDefinition)
	}{
		{name: "nil search", mutate: func(definition *opensplunk.SavedSearchDefinition) { definition.Search = nil }},
		{name: "empty name", mutate: func(definition *opensplunk.SavedSearchDefinition) { definition.Name = "  " }},
		{name: "empty SPL", mutate: func(definition *opensplunk.SavedSearchDefinition) { definition.Search.Spl = "\n" }},
		{name: "invalid sharing", mutate: func(definition *opensplunk.SavedSearchDefinition) { definition.SharingScope = 99 }},
		{name: "app sharing no app", mutate: func(definition *opensplunk.SavedSearchDefinition) {
			definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_APP
			definition.Search.AppId = nil
		}},
		{name: "owner mismatch", mutate: func(definition *opensplunk.SavedSearchDefinition) {
			other := "other"
			definition.OwnerId = &other
		}},
		{name: "too many fields", mutate: func(definition *opensplunk.SavedSearchDefinition) {
			definition.Search.SelectedFields = make([]string, maximumRepeatedFields+1)
		}},
		{name: "bad result tab", mutate: func(definition *opensplunk.SavedSearchDefinition) { definition.Search.PreferredResultTab = 99 }},
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
	definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED
	created, err := store.Create(ctx, scope, definition)
	if err != nil || created.Definition.SharingScope != opensplunk.SharingScope_SHARING_SCOPE_PRIVATE {
		t.Fatalf("default sharing Create() = (%+v,%v)", created, err)
	}
}

func TestUniquenessClassificationAndOwnerIsolation(t *testing.T) {
	_, store := openTestStore(t)
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
	rename := proto.Clone(second.Definition).(*opensplunk.SavedSearchDefinition)
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
	_, store := openTestStore(t)
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
			definition := proto.Clone(created.Definition).(*opensplunk.SavedSearchDefinition)
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
	_, store := openTestStore(t)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			<-start
			_, err := store.Create(ctx, AccessScope{OwnerID: "owner"}, savedSearchDefinition("same", "app"))
			results <- err
		})
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

func TestGORMCreateAndDuplicateRetryIDCollisions(t *testing.T) {
	database, _ := openTestStore(t)
	ids := []string{
		"ss_source",
		"ss_existing",
		"ss_existing",
		"ss_created_after_collision",
		"ss_source",
		"ss_duplicate_after_collision",
	}
	var idCalls atomic.Int64
	store, err := New(database, Options{
		CursorKey: testCursorKey,
		Clock: func() time.Time {
			return time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
		},
		IDGenerator: func() (string, error) {
			index := idCalls.Add(1) - 1
			return ids[index], nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{OwnerID: "owner"}
	source, err := store.Create(
		context.Background(),
		scope,
		savedSearchDefinition("source", "app"),
	)
	if err != nil {
		t.Fatalf("Create(source) error = %v", err)
	}
	if _, err := store.Create(
		context.Background(),
		scope,
		savedSearchDefinition("existing", "app"),
	); err != nil {
		t.Fatalf("Create(existing) error = %v", err)
	}
	created, err := store.Create(
		context.Background(),
		scope,
		savedSearchDefinition("created after collision", "app"),
	)
	if err != nil {
		t.Fatalf("Create(after collision) error = %v", err)
	}
	if created.SavedSearchId != "ss_created_after_collision" {
		t.Fatalf("Create(after collision) ID = %q", created.SavedSearchId)
	}
	duplicate, err := store.Duplicate(
		context.Background(),
		scope,
		source.SavedSearchId,
		"duplicate after collision",
		nil,
	)
	if err != nil {
		t.Fatalf("Duplicate(after collision) error = %v", err)
	}
	if duplicate.SavedSearchId != "ss_duplicate_after_collision" {
		t.Fatalf("Duplicate(after collision) ID = %q", duplicate.SavedSearchId)
	}
	if got := idCalls.Load(); got != int64(len(ids)) {
		t.Fatalf("ID generator calls = %d, want %d", got, len(ids))
	}
}

func TestDuplicateClonesAndClassifiesConflicts(t *testing.T) {
	_, store := openTestStore(t)
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
	_, store := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	definitions := []*opensplunk.SavedSearchDefinition{
		savedSearchDefinition("delta", "app-a"),
		savedSearchDefinition("Alpha", "app-a"),
		savedSearchDefinition("charlie", "app-b"),
		savedSearchDefinition("bravo", "app-a"),
		savedSearchDefinition("Echo", "app-a"),
	}
	definitions[2].SharingScope = opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	definitions[4].SharingScope = opensplunk.SharingScope_SHARING_SCOPE_APP
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
	private := []opensplunk.SharingScope{opensplunk.SharingScope_SHARING_SCOPE_PRIVATE}
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
		sortBy    opensplunk.SavedSearchSortBy
		direction opensplunk.SortDirection
		want      []string
	}{
		{name: "name ascending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME, direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"Alpha", "Echo", "bravo", "charlie", "delta"}},
		{name: "name descending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME, direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"delta", "charlie", "bravo", "Echo", "Alpha"}},
		{name: "created ascending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT, direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"delta", "Alpha", "charlie", "bravo", "Echo"}},
		{name: "created descending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT, direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"Echo", "bravo", "charlie", "Alpha", "delta"}},
		{name: "updated ascending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT, direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING, want: []string{"Alpha", "charlie", "bravo", "Echo", "delta"}},
		{name: "updated descending", sortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT, direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING, want: []string{"delta", "Echo", "bravo", "charlie", "Alpha"}},
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
		PageSize: 5, SortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
		SortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
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
		"sort":      {PageSize: 1, PageToken: token, SortBy: opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT},
		"direction": {PageSize: 1, PageToken: token, SortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING},
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

func readSavedSearchIndexShape(
	t *testing.T,
	database *control.DB,
	indexName string,
) ([]string, []int64) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(
		context.Background(),
		`SELECT name, "desc" FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno`,
		indexName,
	)
	if err != nil {
		t.Fatalf("read migrated saved-search index %s: %v", indexName, err)
	}
	var columns []string
	var descending []int64
	for rows.Next() {
		var column string
		var isDescending int64
		if err := rows.Scan(&column, &isDescending); err != nil {
			_ = rows.Close()
			t.Fatalf("scan migrated saved-search index %s: %v", indexName, err)
		}
		columns = append(columns, column)
		descending = append(descending, isDescending)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated saved-search index %s: %v", indexName, err)
	}
	return columns, descending
}

func descendingFlags(sorts []string) []int64 {
	result := make([]int64, len(sorts))
	for index, sort := range sorts {
		if strings.EqualFold(sort, "DESC") {
			result[index] = 1
		}
	}
	return result
}

func assertMigratedSavedSearchUniqueKey(
	t *testing.T,
	database *control.DB,
	wantColumns []string,
) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(
		context.Background(),
		`SELECT name FROM pragma_index_list('saved_searches') WHERE "unique" = 1`,
	)
	if err != nil {
		t.Fatalf("read migrated saved-search unique indexes: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan migrated saved-search unique index: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated saved-search unique indexes: %v", err)
	}
	for _, name := range names {
		columns, _ := readSavedSearchIndexShape(t, database, name)
		if slices.Equal(columns, wantColumns) {
			return
		}
	}
	t.Fatalf("migrated saved-search unique key %v was not found", wantColumns)
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
	database, store := openTestStore(t)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}
	created, err := store.Create(ctx, scope, savedSearchDefinition("valid", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `UPDATE saved_searches SET definition_proto = x'ff' WHERE saved_search_id = ?`, created.SavedSearchId); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, created.SavedSearchId); err == nil ||
		errors.Is(err, control.ErrNotFound) ||
		errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(malformed proto) error = %v, want internal persistence error", err)
	}

	other, err := store.Create(ctx, scope, savedSearchDefinition("valid two", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `UPDATE saved_searches SET name = 'mismatch' WHERE saved_search_id = ?`, other.SavedSearchId); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, other.SavedSearchId); err == nil ||
		errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(mismatched metadata) error = %v, want internal persistence error", err)
	}

	invalidDefinitionProto, err := proto.Marshal(&opensplunk.SavedSearchDefinition{Name: "missing search"})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := store.Create(ctx, scope, savedSearchDefinition("valid three", "app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(
		ctx,
		`UPDATE saved_searches SET definition_proto = ? WHERE saved_search_id = ?`,
		invalidDefinitionProto,
		invalid.SavedSearchId,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, invalid.SavedSearchId); err == nil ||
		errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(invalid definition) error = %v, want internal persistence error", err)
	}
}

func TestCancellationAndBoundedInputs(t *testing.T) {
	_, store := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Create(ctx, AccessScope{OwnerID: "owner"}, savedSearchDefinition("test", "app")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create(canceled) error = %v, want context.Canceled", err)
	}
	var nilContext context.Context

	if _, err := store.Get(nilContext, AccessScope{OwnerID: "owner"}, "id"); !errors.Is(err, control.ErrInvalidArgument) {
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
