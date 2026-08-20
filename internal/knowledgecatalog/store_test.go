package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
)

const (
	testTenant = "tenant-a"
	testOwner  = "owner-a"
	testApp    = "app_000000000100000000001A"
	testAppTwo = "app_000000000100000000002A"
)

var testCursorKey = []byte("knowledge-catalog-test-cursor-key-at-least-32-bytes")

type fixtureVersion struct {
	definition   *opensplunk.KnowledgeObjectDefinition
	state        State
	mutation     string
	reason       *string
	timestamp    int64
	dependencies []fixtureDependency
}

type fixtureDependency struct {
	targetObjectID string
	targetVersion  int64
}

type fixtureObject struct {
	id       string
	owner    string
	versions []fixtureVersion
}

func TestStoreReadsFreshMigratedDatabase(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	if _, err := store.Get(context.Background(), testReadScope(), "missing", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrNotFound", err)
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil {
		t.Fatalf("List(empty): %v", err)
	}
	if len(page.Objects) != 0 || page.NextPageToken != "" || page.CatalogRevision != 0 {
		t.Fatalf("List(empty) = %#v", page)
	}
	assertTableExists(t, database, "knowledge_objects")
	assertTableExists(t, database, "knowledge_object_list_projections")
}

func TestGetCurrentHistoricalAuthorizationAndDetachedResults(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	oldDescription := "old body"
	newDescription := "current body"
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-history",
		owner: testOwner,
		versions: []fixtureVersion{
			{definition: aliasDefinition(testApp, "history", SharingScopeGlobal, &oldDescription, "old-*"), state: StateActive, mutation: "create", timestamp: 10},
			{definition: aliasDefinition(testApp, "history", SharingScopePrivate, &newDescription, "new-*"), state: StateActive, mutation: "scope_change", timestamp: 20},
		},
	})

	current, err := store.Get(context.Background(), testReadScope(), "ko-history", nil)
	if err != nil {
		t.Fatalf("Get(current): %v", err)
	}
	if current.Version != 2 || current.Definition.GetDescription() != newDescription || len(current.DefinitionSHA256) != 32 {
		t.Fatalf("Get(current) = %#v", current)
	}
	version := uint64(1)
	historical, err := store.Get(context.Background(), testReadScope(), "ko-history", &version)
	if err != nil {
		t.Fatalf("Get(version 1): %v", err)
	}
	if historical.Version != 1 || historical.SharingScope != SharingScopeGlobal || historical.Definition.GetDescription() != oldDescription {
		t.Fatalf("Get(version 1) = %#v", historical)
	}

	for _, scope := range []ReadScope{
		{TenantID: testTenant, OwnerID: "owner-b", ReadableAppIDs: []string{testApp}},
		{TenantID: testTenant, OwnerID: testOwner, ReadableAppIDs: []string{testAppTwo}},
		{TenantID: "tenant-b", OwnerID: testOwner, ReadableAppIDs: []string{testApp}},
	} {
		if _, getErr := store.Get(context.Background(), scope, "ko-history", &version); !errors.Is(getErr, control.ErrNotFound) {
			t.Errorf("Get(hidden historical, %#v) error = %v, want ErrNotFound", scope, getErr)
		}
	}

	current.Name = "mutated"
	current.Definition.Name = "mutated"
	current.DefinitionSHA256[0] ^= 0xff
	again, err := store.Get(context.Background(), testReadScope(), "ko-history", nil)
	if err != nil {
		t.Fatalf("Get(after caller mutation): %v", err)
	}
	if again.Name != "history" || again.Definition.GetName() != "history" || again.DefinitionSHA256[0] == current.DefinitionSHA256[0] {
		t.Fatalf("Get retained caller-owned storage: %#v", again)
	}
}

func TestQuarantineNeverReadsHistoricalDefinition(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	reason := "root_corruption"
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-quarantine",
		owner: testOwner,
		versions: []fixtureVersion{
			{definition: aliasDefinition(testApp, "quarantine", SharingScopePrivate, nil, "secret-*"), state: StateActive, mutation: "create", timestamp: 10},
			{state: StateQuarantined, mutation: "quarantine", reason: &reason, timestamp: 20},
		},
	})

	// Make the retained v1 bytes invalid. A historical request must still return
	// the bodyless current quarantine scalar without opening this blob.
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ?`, testTenant)
	version := uint64(1)
	got, err := store.Get(context.Background(), testReadScope(), "ko-quarantine", &version)
	if err != nil {
		t.Fatalf("Get(quarantined historical): %v", err)
	}
	if got.Version != 2 || got.State != StateQuarantined || got.Definition != nil || got.DefinitionSHA256 != nil ||
		got.QuarantineReason == nil || *got.QuarantineReason != reason {
		t.Fatalf("Get(quarantined historical) = %#v", got)
	}
}

func TestListFiltersBeforeLimitPaginatesAndBindsCursor(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	fixtures := []struct {
		id, name, description, selector string
		scope                           SharingScope
		owner                           string
	}{
		{"ko-a", "aaa", "irrelevant", "host-a", SharingScopePrivate, testOwner},
		{"ko-b", "bravo", "needle one", "prod-*", SharingScopePrivate, testOwner},
		{"ko-c", "charlie", "needle two", "prod-east", SharingScopeApp, "owner-b"},
		{"ko-d", "delta", "needle hidden", "prod-west", SharingScopePrivate, "owner-b"},
		{"ko-e", "echo", "needle global", "global-*", SharingScopeGlobal, "owner-b"},
	}
	for index, item := range fixtures {
		description := item.description
		insertFixtureObject(t, database, fixtureObject{
			id: item.id, owner: item.owner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, item.name, item.scope, &description, item.selector),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}},
		})
	}
	needle := " needle "
	request := ListRequest{PageSize: 1, IncludeTotal: true, TextFilter: &needle}
	first, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if !slices.Equal(names(first.Objects), []string{"bravo"}) || first.TotalSize == nil || *first.TotalSize != 3 || first.NextPageToken == "" {
		t.Fatalf("List(first) = %#v", first)
	}
	request.PageToken = first.NextPageToken
	second, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(second): %v", err)
	}
	if !slices.Equal(names(second.Objects), []string{"charlie"}) || second.NextPageToken == "" {
		t.Fatalf("List(second) = %#v", second)
	}
	request.PageToken = second.NextPageToken
	third, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(third): %v", err)
	}
	if !slices.Equal(names(third.Objects), []string{"echo"}) || third.NextPageToken != "" {
		t.Fatalf("List(third) = %#v", third)
	}
	ownerB := "owner-b"
	appTwo := testAppTwo
	for _, test := range []struct {
		name    string
		request ListRequest
		want    []string
	}{
		{"descending", ListRequest{SortDirection: SortDescending}, []string{"echo", "charlie", "bravo", "aaa"}},
		{"owner", ListRequest{OwnerIDFilter: &ownerB}, []string{"charlie", "echo"}},
		{"unmatched app", ListRequest{AppIDFilter: &appTwo}, []string{}},
		{"type", ListRequest{ObjectTypeFilters: []ObjectType{ObjectTypeFieldAlias}}, []string{"aaa", "bravo", "charlie", "echo"}},
		{"state", ListRequest{StateFilters: []State{StateActive}}, []string{"aaa", "bravo", "charlie", "echo"}},
		{"scope", ListRequest{SharingScopeFilters: []SharingScope{SharingScopeApp}}, []string{"charlie"}},
	} {
		page, err := store.List(context.Background(), testReadScope(), test.request)
		if err != nil || !slices.Equal(names(page.Objects), test.want) {
			t.Errorf("List(%s) = %v, %v, want %v", test.name, names(page.Objects), err, test.want)
		}
	}

	selector := "prod-"
	filtered, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 1, SelectorTextFilter: &selector})
	if err != nil || !slices.Equal(names(filtered.Objects), []string{"bravo"}) {
		t.Fatalf("List(selector filter) = %#v, %v", filtered, err)
	}

	changed := request
	changed.PageToken = first.NextPageToken
	changed.TextFilter = &selector
	if _, err := store.List(context.Background(), testReadScope(), changed); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(rebound filter) error = %v, want ErrInvalidCursor", err)
	}
	if _, err := store.List(context.Background(), ReadScope{TenantID: testTenant, OwnerID: "owner-b", ReadableAppIDs: []string{testApp}}, ListRequest{PageSize: 1, IncludeTotal: true, TextFilter: &needle, PageToken: first.NextPageToken}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(rebound scope) error = %v, want ErrInvalidCursor", err)
	}
	tampered := request
	tampered.PageToken = first.NextPageToken[:len(first.NextPageToken)-1] + "!"
	if _, err := store.List(context.Background(), testReadScope(), tampered); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(tampered cursor) error = %v, want ErrInvalidCursor", err)
	}
	mustExec(t, database, `UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant)
	request.PageToken = first.NextPageToken
	if _, err := store.List(context.Background(), testReadScope(), request); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("List(stale cursor) error = %v, want ErrPageInvalidated", err)
	}
}

func TestVisibleCorruptionFailsClosedWithoutDisclosingHiddenObject(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	visibleDescription := "visible description"
	hiddenDescription := "hidden description"
	insertFixtureObject(t, database, fixtureObject{id: "ko-visible", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "visible", SharingScopePrivate, &visibleDescription, "visible-*"), state: StateActive, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-hidden", owner: "owner-b", versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "hidden", SharingScopePrivate, &hiddenDescription, "hidden-*"), state: StateActive, mutation: "create", timestamp: 20,
	}}})
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ? AND definition_digest = (
			SELECT definition_digest FROM knowledge_objects WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden'
		)`, testTenant, testTenant)

	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil || !slices.Equal(names(page.Objects), []string{"visible"}) {
		t.Fatalf("List(with hidden corrupt object) = %#v, %v", page, err)
	}
	if _, err := store.Get(context.Background(), testReadScope(), "ko-hidden", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(hidden corrupt object) error = %v, want ErrNotFound", err)
	}

	dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_object_list_projections SET description = 'tampered'
		WHERE tenant_id = ? AND knowledge_object_id = 'ko-visible'`, testTenant)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-visible", nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(visible corrupt projection) error = %v, want ErrCorrupt", err)
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("List(visible corrupt projection) error = %v, want ErrCorrupt", err)
	}
}

func TestBoundsCancellationAndCursorKeyOwnership(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)
	key := append([]byte(nil), testCursorKey...)
	owned, err := New(database, Options{CursorKey: key})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	key[0] ^= 0xff
	if reflect.DeepEqual(owned.cursorKey, key) {
		t.Fatal("New retained caller cursor-key storage")
	}
	for _, options := range []Options{{}, {CursorKey: make([]byte, 31)}, {CursorKey: make([]byte, maximumCursorKeyBytes+1)}} {
		if _, err := New(database, options); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("New(%d-byte key) error = %v, want ErrInvalidArgument", len(options.CursorKey), err)
		}
	}
	if _, err := New(nil, Options{CursorKey: testCursorKey}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Errorf("New(nil database) error = %v, want ErrInvalidArgument", err)
	}
	var nilContext context.Context
	if _, err := store.List(nilContext, testReadScope(), ListRequest{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Errorf("List(nil context) error = %v", err)
	}
	if _, err := store.Get(nilContext, testReadScope(), "ko", nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Errorf("Get(nil context) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.List(canceled, testReadScope(), ListRequest{}); !errors.Is(err, context.Canceled) {
		t.Errorf("List(canceled) error = %v", err)
	}
	if _, err := store.Get(canceled, testReadScope(), "ko", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Get(canceled) error = %v", err)
	}
	tooLong := string(make([]byte, maximumFilterBytes+1))
	badRequests := []ListRequest{
		{PageSize: MaximumPageSize + 1},
		{PageToken: string(make([]byte, maximumCursorBytes+1))},
		{TextFilter: &tooLong},
		{ObjectTypeFilters: []ObjectType{"future"}},
		{StateFilters: []State{"future"}},
		{SharingScopeFilters: []SharingScope{"future"}},
		{SortBy: "future"},
		{SortDirection: "future"},
	}
	for _, request := range badRequests {
		if _, err := store.List(context.Background(), testReadScope(), request); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("List(%#v) error = %v, want ErrInvalidArgument", request, err)
		}
	}
	badScope := testReadScope()
	badScope.ReadableAppIDs = nil
	if _, err := store.List(context.Background(), badScope, ListRequest{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Errorf("List(empty readable apps) error = %v", err)
	}
}

func TestConcurrentReadsReturnDetachedDefinitions(t *testing.T) {
	database, store := newCatalogTestStore(t)
	description := "race-safe"
	insertFixtureObject(t, database, fixtureObject{id: "ko-race", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "race", SharingScopePrivate, &description, "race-*"), state: StateActive, mutation: "create", timestamp: 10,
	}}})
	const workers = 12
	var ready sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	errorsSeen := make(chan error, workers)
	for index := range workers {
		go func(index int) {
			ready.Done()
			<-start
			if index%2 == 0 {
				object, err := store.Get(context.Background(), testReadScope(), "ko-race", nil)
				if err == nil {
					object.Definition.Name = fmt.Sprintf("mutated-%d", index)
					object.DefinitionSHA256[0] ^= byte(index + 1)
				}
				errorsSeen <- err
				return
			}
			page, err := store.List(context.Background(), testReadScope(), ListRequest{})
			if err == nil {
				page.Objects[0].Definition.Name = fmt.Sprintf("mutated-%d", index)
			}
			errorsSeen <- err
		}(index)
	}
	ready.Wait()
	close(start)
	for range workers {
		if err := <-errorsSeen; err != nil {
			t.Errorf("concurrent read: %v", err)
		}
	}
	got, err := store.Get(context.Background(), testReadScope(), "ko-race", nil)
	if err != nil || got.Definition.GetName() != "race" {
		t.Fatalf("Get(after concurrent mutation) = %#v, %v", got, err)
	}
}

func newCatalogTestStore(t *testing.T) (*control.DB, *Store) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	ids := []string{testApp, testAppTwo}
	var clockSequence atomic.Int64
	var idSequence atomic.Int64
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: testCursorKey,
		Clock:     func() time.Time { return time.UnixMicro(1_000 + clockSequence.Add(1)).UTC() },
		IDGenerator: func() (string, error) {
			index := int(idSequence.Add(1)) - 1
			if index < 0 || index >= len(ids) {
				return "", errors.New("test app ID sequence exhausted")
			}
			return ids[index], nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	for _, slug := range []string{"catalog-one", "catalog-two"} {
		if _, err := apps.CreateApp(context.Background(), control.AppAccessScope{TenantID: testTenant}, control.AppDefinition{
			Slug: slug, DisplayName: slug,
			DefaultTimeRange: &control.AppTimeRange{Earliest: new("-24h"), Latest: new("now")},
		}); err != nil {
			t.Fatalf("CreateApp(%s): %v", slug, err)
		}
	}
	store, err := New(database, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return database, store
}

func insertFixtureObject(t *testing.T, database *control.DB, object fixtureObject) {
	t.Helper()
	if object.owner == "" {
		object.owner = testOwner
	}
	ctx := context.Background()
	tx, err := database.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fixture transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_catalog_tenants (tenant_id)
		SELECT ? WHERE NOT EXISTS (SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = ?)`, testTenant, testTenant); err != nil {
		t.Fatalf("insert tenant ledger: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
		SELECT ? WHERE NOT EXISTS (SELECT 1 FROM knowledge_projection_tenant_ledgers WHERE tenant_id = ?)`, testTenant, testTenant); err != nil {
		t.Fatalf("insert projection ledger: %v", err)
	}

	var current knowledgedefinition.Normalized
	var priorKnown knowledgedefinition.Normalized
	var havePriorKnown bool
	insertedDigests := make(map[[32]byte]struct{})
	for index, version := range object.versions {
		versionNumber := int64(index + 1)
		var normalized knowledgedefinition.Normalized
		var digest any
		if version.state != StateQuarantined {
			normalized, err = knowledgedefinition.Normalize(version.definition)
			if err != nil {
				t.Fatalf("Normalize(%s v%d): %v", object.id, versionNumber, err)
			}
			// State-only immutable versions are exact body/metadata copies. Older
			// read fixtures used distinct description markers merely to make rows
			// visually distinguishable; migration 0030 correctly rejects that
			// impossible writer history, so canonicalize those fixture versions to
			// the prior immutable definition.
			if index > 0 && (version.mutation == "enable" || version.mutation == "disable" || version.mutation == "delete") {
				if !havePriorKnown {
					t.Fatalf("state-only fixture %s v%d has no prior definition", object.id, versionNumber)
				}
				normalized = priorKnown
			}
			digest = normalized.Digest[:]
			if _, exists := insertedDigests[normalized.Digest]; !exists {
				if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_definition_blobs (
					tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
				) VALUES (?, ?, ?, ?, ?)`, testTenant, normalized.Digest[:], normalized.Bytes, len(normalized.Bytes), version.timestamp); err != nil {
					t.Fatalf("insert definition blob %s v%d: %v", object.id, versionNumber, err)
				}
				insertedDigests[normalized.Digest] = struct{}{}
			}
		} else {
			digest = nil
		}
		identity := normalized
		if version.state == StateQuarantined {
			if !havePriorKnown {
				t.Fatalf("quarantine fixture %s v%d has no prior definition", object.id, versionNumber)
			}
			identity = priorKnown
		}
		appID, name, scope, objectType := fixtureIdentity(
			t,
			object.id,
			fmt.Sprintf("v%d", versionNumber),
			identity,
		)
		if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
			sharing_scope, state, definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			testTenant, object.id, versionNumber, appID, object.owner, objectType, name,
			scope, version.state, digest, len(version.dependencies), version.mutation, version.reason, version.timestamp); err != nil {
			t.Fatalf("insert immutable version %s v%d: %v", object.id, versionNumber, err)
		}
		for ordinal, dependency := range version.dependencies {
			if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_dependencies (
				tenant_id, source_object_id, source_object_version, ordinal, target_kind,
				target_object_id, target_object_version, dependency_role
			) VALUES (?, ?, ?, ?, 'object', ?, ?, 'field_input')`, testTenant, object.id,
				versionNumber, ordinal, dependency.targetObjectID, dependency.targetVersion); err != nil {
				t.Fatalf("insert dependency %s v%d[%d]: %v", object.id, versionNumber, ordinal, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES (?, ?, ?, ?)`, testTenant, object.id, versionNumber, len(version.dependencies)); err != nil {
			t.Fatalf("seal dependencies %s v%d: %v", object.id, versionNumber, err)
		}
		if version.state != StateQuarantined {
			priorKnown = normalized
			havePriorKnown = true
		}
		if index == len(object.versions)-1 {
			current = normalized
		}
	}

	last := object.versions[len(object.versions)-1]
	currentVersion := int64(len(object.versions))
	identity := current
	if last.state == StateQuarantined {
		identity = priorKnown
	}
	appID, name, scope, objectType := fixtureIdentity(t, object.id, "current", identity)
	description, descriptionPresent := "", 0
	counts := [4]int{}
	selectorValueBytes, canonicalSelectorBytes := 0, 0
	if last.state != StateQuarantined {
		if current.Description != nil {
			description, descriptionPresent = *current.Description, 1
		}
		dimensions := []knowledge.Dimension{knowledge.DimensionIndex, knowledge.DimensionHost, knowledge.DimensionSource, knowledge.DimensionSourcetype}
		for index, dimension := range dimensions {
			values := current.Selector.Patterns(dimension)
			counts[index] = len(values)
			for _, value := range values {
				selectorValueBytes += len(value)
			}
		}
		canonicalSelectorBytes = len(current.Selector.CanonicalBytes())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, description_present, description, index_selector_count,
		host_selector_count, source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		testTenant, object.id, currentVersion, appID, object.owner, objectType, name, scope, last.state,
		descriptionPresent, description, counts[0], counts[1], counts[2], counts[3], selectorValueBytes, canonicalSelectorBytes); err != nil {
		t.Fatalf("insert projection %s: %v", object.id, err)
	}
	if last.state != StateQuarantined {
		insertSelectorRows(t, tx, object.id, currentVersion, current)
	}
	projectionBytes := len(description) + selectorValueBytes
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version, projection_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?)`, testTenant, object.id, currentVersion, projectionBytes, canonicalSelectorBytes); err != nil {
		t.Fatalf("seal projection %s: %v", object.id, err)
	}
	var digest any
	if last.state != StateQuarantined {
		digest = current.Digest[:]
	}
	var disabledAt, quarantinedAt, deletedAt any
	if last.state == StateDisabled {
		for _, v := range slices.Backward(object.versions) {
			if v.mutation == "disable" {
				disabledAt = v.timestamp
				break
			}
		}
		if disabledAt == nil {
			t.Fatalf("disabled fixture %s has no disable transition", object.id)
		}
	}
	if last.state == StateQuarantined {
		quarantinedAt = last.timestamp
	}
	if last.state == StateDeleted {
		deletedAt = last.timestamp
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, testTenant, object.id, currentVersion,
		appID, object.owner, objectType, name, scope, last.state, digest,
		object.versions[0].timestamp, last.timestamp, disabledAt, quarantinedAt, deletedAt, last.reason); err != nil {
		t.Fatalf("insert registry %s: %v", object.id, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant); err != nil {
		t.Fatalf("advance catalog revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit fixture %s: %v", object.id, err)
	}
}

func fixtureIdentity(
	t *testing.T,
	objectID string,
	versionLabel string,
	identity knowledgedefinition.Normalized,
) (string, string, string, string) {
	t.Helper()
	objectType, typeOK := objectTypeFromProto(identity.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(identity.SharingScope)
	if !typeOK || !scopeOK {
		t.Fatalf("fixture %s %s has unsupported identity", objectID, versionLabel)
	}
	return identity.AppID, identity.Name, string(sharingScope), string(objectType)
}

func insertSelectorRows(t *testing.T, tx *sql.Tx, objectID string, version int64, normalized knowledgedefinition.Normalized) {
	t.Helper()
	dimensions := []struct {
		name      string
		dimension knowledge.Dimension
	}{
		{"index", knowledge.DimensionIndex},
		{"host", knowledge.DimensionHost},
		{"source", knowledge.DimensionSource},
		{"sourcetype", knowledge.DimensionSourcetype},
	}
	for _, dimension := range dimensions {
		for ordinal, value := range normalized.Selector.Patterns(dimension.dimension) {
			pattern, err := knowledge.NormalizePattern(value)
			if err != nil {
				t.Fatalf("normalize projected selector: %v", err)
			}
			matchKind := "wildcard"
			if pattern.IsLiteral() {
				matchKind = "exact"
			}
			if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_selector_patterns (
				tenant_id, knowledge_object_id, object_version, dimension, ordinal, match_kind, value
			) VALUES (?, ?, ?, ?, ?, ?, ?)`, testTenant, objectID, version, dimension.name, ordinal, matchKind, value); err != nil {
				t.Fatalf("insert selector %s[%d]: %v", dimension.name, ordinal, err)
			}
		}
	}
}

func aliasDefinition(appID, name string, scope SharingScope, description *string, hostPattern string) *opensplunk.KnowledgeObjectDefinition {
	protoScope := opensplunk.SharingScope_SHARING_SCOPE_PRIVATE
	switch scope {
	case SharingScopeApp:
		protoScope = opensplunk.SharingScope_SHARING_SCOPE_APP
	case SharingScopeGlobal:
		protoScope = opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	}
	definition := &opensplunk.KnowledgeObjectDefinition{
		AppId: appID, Name: name, Description: description, SharingScope: protoScope,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunk.FieldAliasDefinition{
			SourceField: "source", DestinationField: "source_alias",
		}},
	}
	if hostPattern != "" {
		definition.Selector = &opensplunk.KnowledgeSelector{HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: hostPattern}}}
	}
	return definition
}

func testReadScope() ReadScope {
	return ReadScope{TenantID: testTenant, OwnerID: testOwner, ReadableAppIDs: []string{testApp}}
}

func names(objects []Object) []string {
	result := make([]string, len(objects))
	for index := range objects {
		result[index] = objects[index].Name
	}
	return result
}

func dropTrigger(t *testing.T, database *control.DB, name string) {
	t.Helper()
	// #nosec G202 -- name is supplied only by fixed test fixture identifiers.
	if _, err := database.SQLDB().ExecContext(t.Context(), `DROP TRIGGER `+name); err != nil {
		t.Fatalf("drop trigger %s: %v", name, err)
	}
}

func overwriteCatalogRevisionAuthority(
	t *testing.T,
	database *control.DB,
	revision int64,
	bypassChecks bool,
) {
	t.Helper()
	dropTrigger(t, database, "knowledge_catalog_revision_transition_is_valid")
	dropTrigger(t, database, "knowledge_catalog_revision_rotates_state_token")
	dropTrigger(t, database, "knowledge_catalog_revision_head_transition_is_exact")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire revision-authority corruption connection: %v", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close revision-authority corruption connection: %v", err)
		}
	}()
	if bypassChecks {
		if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatalf("ignore revision-authority checks: %v", err)
		}
		defer func() {
			if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
				t.Errorf("restore revision-authority checks: %v", err)
			}
		}()
	}
	transaction, err := connection.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin revision-authority corruption: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.ExecContext(context.Background(), `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = ?
		WHERE tenant_id = ?
	`, revision, testTenant); err != nil {
		t.Fatalf("overwrite tenant revision authority: %v", err)
	}
	if _, err := transaction.ExecContext(context.Background(), `
		UPDATE knowledge_catalog_revision_heads
		SET catalog_revision = ?
		WHERE tenant_id = ?
	`, revision, testTenant); err != nil {
		t.Fatalf("overwrite revision-head authority: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit revision-authority corruption: %v", err)
	}
}

func mustExec(t *testing.T, database *control.DB, query string, args ...any) {
	t.Helper()
	if _, err := database.SQLDB().ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("execute fixture corruption: %v", err)
	}
}

func assertTableExists(t *testing.T, database *control.DB, table string) {
	t.Helper()
	var count int
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&count); err != nil {
		t.Fatalf("inspect migrated table %s: %v", table, err)
	}
	if count != 1 {
		t.Fatalf("migrated table %s count = %d", table, count)
	}
}
