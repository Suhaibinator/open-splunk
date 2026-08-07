package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

func TestBodyDerivedFiltersCannotHideSameByteProjectionCorruption(t *testing.T) {
	for _, test := range []struct {
		name    string
		corrupt func(*testing.T, *control.DB)
		request func() ListRequest
	}{
		{
			name: "description",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_list_projections
					SET description = 'xxxxxx'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-filter-corrupt'`, testTenant)
			},
			request: func() ListRequest {
				value := "needle"
				return ListRequest{TextFilter: &value}
			},
		},
		{
			name: "selector",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_selector_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_list_selector_patterns
					SET value = 'xxxxxx'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-filter-corrupt'`, testTenant)
			},
			request: func() ListRequest {
				value := "prod-"
				return ListRequest{SelectorTextFilter: &value}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			description := "needle"
			insertFixtureObject(t, database, fixtureObject{id: "ko-filter-corrupt", owner: testOwner, versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, "filter-corrupt", SharingScopePrivate, &description, "prod-*"),
				state:      StateActive, mutation: "create", timestamp: 10,
			}}})
			test.corrupt(t, database)
			if _, err := store.List(context.Background(), testReadScope(), test.request()); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("List() error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestBodyFilterValidatesFalseInclusionBeyondReturnedPage(t *testing.T) {
	database, store := newCatalogTestStore(t)
	matching := "needle"
	nonmatching := "xxxxxx"
	for _, object := range []fixtureObject{
		{id: "ko-filter-alpha", owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "alpha", SharingScopePrivate, &matching, "alpha-*"),
			state:      StateActive, mutation: "create", timestamp: 10,
		}}},
		{id: "ko-filter-zulu", owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "zulu", SharingScopePrivate, &nonmatching, "zulu-*"),
			state:      StateActive, mutation: "create", timestamp: 20,
		}}},
	} {
		insertFixtureObject(t, database, object)
	}
	dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_object_list_projections
		SET description = 'needle'
		WHERE tenant_id = ? AND knowledge_object_id = 'ko-filter-zulu'`, testTenant)
	filter := "needle"
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{
		PageSize: 1, IncludeTotal: true, TextFilter: &filter,
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("List(false inclusion beyond page) error = %v, want ErrCorrupt", err)
	}
}

func TestDependencyIntegrityForCurrentAndHistoricalVersions(t *testing.T) {
	seed := func(t *testing.T, versions int) (*control.DB, *Store) {
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{id: "ko-target", owner: testOwner, versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "target", SharingScopePrivate, nil, "target-*", dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}}})
		sourceVersions := make([]fixtureVersion, versions)
		for index := range sourceVersions {
			mutation := "update"
			if index == 0 {
				mutation = "create"
			}
			description := fmt.Sprintf("source-v%d", index+1)
			sourceVersions[index] = fixtureVersion{
				definition: dependencyAliasDefinition(
					testApp, "source", SharingScopePrivate, &description, "source-*",
					dependencyFixtureInputField, "dependency_alias",
				),
				state: StateActive, mutation: mutation, timestamp: int64(20 + index),
				dependencies: []fixtureDependency{{targetObjectID: "ko-target", targetVersion: 1}},
			}
		}
		insertFixtureObject(t, database, fixtureObject{id: "ko-source", owner: testOwner, versions: sourceVersions})
		return database, store
	}

	t.Run("valid current and historical", func(t *testing.T) {
		_, store := seed(t, 2)
		if _, err := store.Get(context.Background(), testReadScope(), "ko-source", nil); err != nil {
			t.Fatalf("Get(current): %v", err)
		}
		version := uint64(1)
		if _, err := store.Get(context.Background(), testReadScope(), "ko-source", &version); err != nil {
			t.Fatalf("Get(historical): %v", err)
		}
	})

	t.Run("seal count", func(t *testing.T) {
		database, store := seed(t, 1)
		dropTrigger(t, database, "knowledge_dependency_seal_update_is_forbidden")
		execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_dependency_seals
			SET dependency_count = 0
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-source' AND object_version = 1`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-source")
	})

	t.Run("ordinal", func(t *testing.T) {
		database, store := seed(t, 1)
		dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_object_dependencies SET ordinal = 1
			WHERE tenant_id = ? AND source_object_id = 'ko-source' AND source_object_version = 1`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-source")
	})

	t.Run("target", func(t *testing.T) {
		database, store := seed(t, 1)
		dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
		execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_dependencies
			SET target_object_id = 'ko-missing'
			WHERE tenant_id = ? AND source_object_id = 'ko-source' AND source_object_version = 1`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-source")
	})

	t.Run("target beyond current boundary", func(t *testing.T) {
		database, store := seed(t, 1)
		insertOrphanVersion(t, database, "ko-target", 2, 30)
		dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_object_dependencies
			SET target_object_version = 2
			WHERE tenant_id = ? AND source_object_id = 'ko-source' AND source_object_version = 1`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-source")
	})

	t.Run("historical ordinal", func(t *testing.T) {
		database, store := seed(t, 2)
		dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_object_dependencies SET ordinal = 1
			WHERE tenant_id = ? AND source_object_id = 'ko-source' AND source_object_version = 1`, testTenant)
		version := uint64(1)
		if _, err := store.Get(context.Background(), testReadScope(), "ko-source", &version); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Get(corrupt historical dependencies) error = %v, want ErrCorrupt", err)
		}
	})
}

func TestCurrentTimestampLifecycleAndRevisionIntegrity(t *testing.T) {
	t.Run("current version timestamp", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{id: "ko-time", owner: testOwner, versions: []fixtureVersion{
			{definition: aliasDefinition(testApp, "time", SharingScopePrivate, nil, "time-a"), state: StateActive, mutation: "create", timestamp: 10},
			{definition: aliasDefinition(testApp, "time", SharingScopePrivate, nil, "time-b"), state: StateActive, mutation: "update", timestamp: 20},
		}})
		dropTrigger(t, database, "knowledge_object_version_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_object_versions SET created_at_unix_micro = 19
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-time' AND object_version = 2`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-time")
	})

	t.Run("lifecycle marker", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{id: "ko-disabled", owner: testOwner, versions: []fixtureVersion{
			{definition: aliasDefinition(testApp, "disabled", SharingScopePrivate, nil, "disabled-a"), state: StateActive, mutation: "create", timestamp: 10},
			{definition: aliasDefinition(testApp, "disabled", SharingScopePrivate, nil, "disabled-b"), state: StateDisabled, mutation: "disable", timestamp: 20},
		}})
		dropTrigger(t, database, "knowledge_object_registry_transition_is_valid")
		mustExec(t, database, `UPDATE knowledge_objects SET disabled_at_unix_micro = 10
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-disabled'`, testTenant)
		assertGetAndListCorrupt(t, store, "ko-disabled")
	})

	t.Run("nonempty revision zero", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{id: "ko-revision", owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "revision", SharingScopePrivate, nil, "revision-*"),
			state:      StateActive, mutation: "create", timestamp: 10,
		}}})
		overwriteCatalogRevisionAuthority(t, database, 0, false)
		assertGetAndListCorrupt(t, store, "ko-revision")
	})
}

func TestListBudgetExactSliceAndUnhydratedSentinel(t *testing.T) {
	t.Run("budget", func(t *testing.T) {
		oneMaximum := []projectionRecord{{State: StateActive, DefinitionBytes: maximumDefinitionBytes}}
		if err := validateListDefinitionBudget(MaximumListResponseCanonicalDefinitionBytes, oneMaximum); err != nil {
			t.Fatalf("one maximum definition rejected: %v", err)
		}
		exact := make(
			[]projectionRecord,
			MaximumListFilterIntegrityDefinitionBytes/maximumDefinitionBytes,
		)
		for index := range exact {
			exact[index] = oneMaximum[0]
		}
		if err := validateListDefinitionBudget(MaximumListFilterIntegrityDefinitionBytes, exact); err != nil {
			t.Fatalf("exact aggregate budget rejected: %v", err)
		}
		exceeded := append(append([]projectionRecord(nil), exact...), oneMaximum[0])
		if err := validateListDefinitionBudget(
			MaximumListFilterIntegrityDefinitionBytes,
			exceeded,
		); !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("over-budget error = %v, want ErrCapacityExceeded", err)
		}
		prefix, hasMore, err := boundedListResponseRecords(exact, len(exact))
		if err != nil || len(prefix) != 1 || !hasMore || cap(prefix) != 1 {
			t.Fatalf("bounded response = len/cap %d/%d, more=%t, error=%v", len(prefix), cap(prefix), hasMore, err)
		}
	})

	t.Run("sentinel", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		for index, name := range []string{"alpha", "bravo"} {
			insertFixtureObject(t, database, fixtureObject{id: "ko-" + name, owner: testOwner, versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, name, SharingScopePrivate, nil, name+"-*"),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}}})
		}
		dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_definition_blobs SET definition_proto = X'00', definition_bytes = 1
			WHERE tenant_id = ? AND definition_digest = (
				SELECT definition_digest FROM knowledge_objects
				WHERE tenant_id = ? AND knowledge_object_id = 'ko-bravo'
			)`, testTenant, testTenant)
		page, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 1})
		if err != nil {
			t.Fatalf("List(first page): %v", err)
		}
		if len(page.Objects) != 1 || cap(page.Objects) != 1 || page.Objects[0].Name != "alpha" || page.NextPageToken == "" {
			t.Fatalf("List(first page) = %#v, len/cap = %d/%d", page, len(page.Objects), cap(page.Objects))
		}
		if _, err := store.List(context.Background(), testReadScope(), ListRequest{
			PageSize: 1, PageToken: page.NextPageToken,
		}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(corrupt sentinel as next page) error = %v, want ErrCorrupt", err)
		}
	})
}

func TestNilAndEmptyFilterSlicesShareCursorFingerprint(t *testing.T) {
	nilRequest, err := normalizeListRequest(testReadScope(), ListRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("normalize nil slices: %v", err)
	}
	emptyRequest, err := normalizeListRequest(testReadScope(), ListRequest{
		PageSize:          1,
		ObjectTypeFilters: []ObjectType{}, StateFilters: []State{}, SharingScopeFilters: []SharingScope{},
	})
	if err != nil {
		t.Fatalf("normalize empty slices: %v", err)
	}
	nilFingerprint, err := requestFingerprint(nilRequest)
	if err != nil {
		t.Fatal(err)
	}
	emptyFingerprint, err := requestFingerprint(emptyRequest)
	if err != nil {
		t.Fatal(err)
	}
	if nilFingerprint != emptyFingerprint {
		t.Fatalf("nil fingerprint %q != empty fingerprint %q", nilFingerprint, emptyFingerprint)
	}

	database, store := newCatalogTestStore(t)
	for index, name := range []string{"alpha", "bravo"} {
		insertFixtureObject(t, database, fixtureObject{id: "ko-empty-" + name, owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "empty-"+name, SharingScopePrivate, nil, name+"-*"),
			state:      StateActive, mutation: "create", timestamp: int64(10 + index),
		}}})
	}
	first, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("List(nil slices): %v", err)
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{
		PageSize: 1, PageToken: first.NextPageToken,
		ObjectTypeFilters: []ObjectType{}, StateFilters: []State{}, SharingScopeFilters: []SharingScope{},
	}); err != nil {
		t.Fatalf("List(empty slices continuation): %v", err)
	}
}

func TestGetRejectsPersistedVersionBeyondCurrentBoundary(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-future", owner: testOwner, versions: []fixtureVersion{
		{definition: aliasDefinition(testApp, "future", SharingScopePrivate, nil, "future-a"), state: StateActive, mutation: "create", timestamp: 10},
		{definition: aliasDefinition(testApp, "future", SharingScopePrivate, nil, "future-b"), state: StateActive, mutation: "update", timestamp: 20},
	}})
	insertOrphanVersion(t, database, "ko-future", 3, 30)
	version := uint64(3)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-future", &version); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(future persisted version) error = %v, want ErrNotFound", err)
	}
}

func assertGetAndListCorrupt(t *testing.T, store *Store, objectID string) {
	t.Helper()
	if _, err := store.Get(context.Background(), testReadScope(), objectID, nil); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Get(corrupt %s) error = %v, want ErrCorrupt", objectID, err)
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
		t.Errorf("List(corrupt %s) error = %v, want ErrCorrupt", objectID, err)
	}
}

func execWithForeignKeysDisabled(t *testing.T, database *control.DB, query string, args ...any) {
	t.Helper()
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable fixture foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore fixture foreign keys: %v", err)
		}
	}()
	if _, err := connection.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("execute foreign-key corruption: %v", err)
	}
}

func insertOrphanVersion(t *testing.T, database *control.DB, objectID string, version, timestamp int64) {
	t.Helper()
	description := "orphan future version"
	normalized, err := knowledgedefinition.Normalize(aliasDefinition(
		testApp, "future", SharingScopePrivate, &description, "future-c",
	))
	if err != nil {
		t.Fatalf("normalize orphan version: %v", err)
	}
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin orphan version: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?)`, testTenant, normalized.Digest[:], normalized.Bytes, len(normalized.Bytes), timestamp); err != nil {
		t.Fatalf("insert orphan body: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, dependency_count, mutation_kind, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?, 'field_alias', 'future', 'private', 'active', ?, 0, 'update', ?)`,
		testTenant, objectID, version, testApp, testOwner, normalized.Digest[:], timestamp); err != nil {
		t.Fatalf("insert orphan version: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	) VALUES (?, ?, ?, 0)`, testTenant, objectID, version); err != nil {
		t.Fatalf("seal orphan version: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit orphan version: %v", err)
	}
}
