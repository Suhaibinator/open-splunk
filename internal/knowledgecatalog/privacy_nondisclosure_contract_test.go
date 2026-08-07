package knowledgecatalog

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type privacyContractListSnapshot struct {
	first  ListPage
	second ListPage
}

func TestIntegrationCatalogVisibleMalformedAuthoritiesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		corrupt func(*testing.T, *control.DB)
	}{
		{
			name: "current version identity",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_object_version_update_is_forbidden")
				execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_versions
					SET owner_id = 'owner-b'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-visible-authority'
					  AND object_version = 1`, testTenant)
			},
		},
		{
			name: "current projection identity",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
				execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_list_projections
					SET owner_id = 'owner-b'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-visible-authority'
					  AND object_version = 1`, testTenant)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			description := "visible authority"
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-visible-authority", owner: testOwner,
				versions: []fixtureVersion{{
					definition: aliasDefinition(testApp, "visible-authority", SharingScopePrivate, &description, "authority-*"),
					state:      StateActive, mutation: "create", timestamp: 10,
				}},
			})
			test.corrupt(t, database)

			if _, err := store.Get(context.Background(), testReadScope(), "ko-visible-authority", nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Get(visible malformed authority) error = %v, want ErrCorrupt", err)
			}
			if _, err := store.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("List(visible malformed authority) error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestIntegrationCatalogAuthorizedPhysicalIdentityBoundFailsClosed(t *testing.T) {
	database, store := newCatalogTestStore(t)
	description := "visible capacity body"
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-visible-capacity-seed", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "visible-capacity", SharingScopePrivate, &description, "visible-capacity-*"),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	if page, err := store.List(context.Background(), testReadScope(), ListRequest{}); err != nil || len(page.Objects) != 1 {
		t.Fatalf("List(before visible over-cap fixture) = %#v, %v", page, err)
	}

	privacyContractCloneVisibleAuthorities(t, database, maximumObjectsPerTenant)
	privacyContractAssertPhysicalCountDiagnostic(t, database, maximumObjectsPerTenant+1)
	for _, request := range []ListRequest{
		{},
		{TextFilter: privacyContractStringPointer("visible capacity")},
		{SelectorTextFilter: privacyContractStringPointer("visible-capacity-")},
	} {
		if _, err := store.List(context.Background(), testReadScope(), request); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(8,193 authorized identities, request=%#v) error = %v, want ErrCorrupt", request, err)
		}
	}
}

func privacyContractSeedReadablePair(t *testing.T, database *control.DB, prefix string) []string {
	t.Helper()
	ids := make([]string, 0, 2)
	for index, suffix := range []string{"alpha", "zulu"} {
		name := prefix + "-visible-" + suffix
		description := prefix + "-visible-body"
		objectID := "ko-" + name
		insertFixtureObject(t, database, fixtureObject{
			id: objectID, owner: testOwner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, name, SharingScopePrivate, &description, name+"-*"),
				state:      StateActive, mutation: "create", timestamp: int64(100 + index),
			}},
		})
		ids = append(ids, objectID)
	}
	return ids
}

func privacyContractListRequests(needle string) []ListRequest {
	text := needle
	selector := needle
	return []ListRequest{
		{PageSize: 1, IncludeTotal: true},
		{PageSize: 1, IncludeTotal: true, TextFilter: &text},
		{PageSize: 1, IncludeTotal: true, SelectorTextFilter: &selector},
	}
}

func privacyContractCaptureGets(t *testing.T, store *Store, objectIDs []string) []Object {
	t.Helper()
	objects := make([]Object, len(objectIDs))
	for index, objectID := range objectIDs {
		object, err := store.Get(context.Background(), testReadScope(), objectID, nil)
		if err != nil {
			t.Fatalf("Get(%s) baseline: %v", objectID, err)
		}
		objects[index] = object
	}
	return objects
}

func privacyContractAssertGets(t *testing.T, store *Store, objectIDs []string, baseline []Object) {
	t.Helper()
	if len(objectIDs) != len(baseline) {
		t.Fatalf("Get baseline shape = %d ids/%d objects", len(objectIDs), len(baseline))
	}
	for index, objectID := range objectIDs {
		object, err := store.Get(context.Background(), testReadScope(), objectID, nil)
		if err != nil {
			t.Fatalf("Get(%s) after hidden tenant corruption: %v", objectID, err)
		}
		integrationAssertPagesEqual(
			t,
			ListPage{Objects: []Object{object}},
			ListPage{Objects: []Object{baseline[index]}},
		)
	}
}

func privacyContractCaptureLists(
	t *testing.T,
	store *Store,
	requests []ListRequest,
) []privacyContractListSnapshot {
	t.Helper()
	snapshots := make([]privacyContractListSnapshot, len(requests))
	for index, request := range requests {
		first, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(baseline request %d first page): %v", index, err)
		}
		if len(first.Objects) != 1 || first.NextPageToken == "" || first.TotalSize == nil ||
			*first.TotalSize != 2 || !first.TotalSizeExact {
			t.Fatalf("List(baseline request %d first page) = %#v", index, first)
		}
		continuation := request
		continuation.PageToken = first.NextPageToken
		second, err := store.List(context.Background(), testReadScope(), continuation)
		if err != nil {
			t.Fatalf("List(baseline request %d second page): %v", index, err)
		}
		if len(second.Objects) != 1 || second.NextPageToken != "" || second.TotalSize == nil ||
			*second.TotalSize != 2 || !second.TotalSizeExact {
			t.Fatalf("List(baseline request %d second page) = %#v", index, second)
		}
		snapshots[index] = privacyContractListSnapshot{first: first, second: second}
	}
	return snapshots
}

func privacyContractAssertLists(
	t *testing.T,
	store *Store,
	requests []ListRequest,
	baseline []privacyContractListSnapshot,
) {
	t.Helper()
	if len(requests) != len(baseline) {
		t.Fatalf("List baseline shape = %d requests/%d snapshots", len(requests), len(baseline))
	}
	for index, request := range requests {
		first, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(after hidden tenant corruption request %d first page): %v", index, err)
		}
		integrationAssertPagesEqual(t, first, baseline[index].first)
		continuation := request
		continuation.PageToken = baseline[index].first.NextPageToken
		second, err := store.List(context.Background(), testReadScope(), continuation)
		if err != nil {
			t.Fatalf("List(after hidden tenant corruption request %d second page): %v", index, err)
		}
		integrationAssertPagesEqual(t, second, baseline[index].second)
	}
}

func privacyContractAssertPhysicalCountDiagnostic(
	t *testing.T,
	database *control.DB,
	wantBoundedCount int64,
) {
	t.Helper()
	record, found, err := readCatalogTenantRecord(database.GORMDB(), testTenant)
	if err != nil || !found {
		t.Fatalf("read tenant health ledger = %#v, found=%t, error=%v", record, found, err)
	}
	boundedCount, err := readBoundedKnowledgeObjectCount(database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read bounded physical identity count: %v", err)
	}
	if boundedCount != wantBoundedCount {
		t.Fatalf("bounded physical identity count = %d, want %d", boundedCount, wantBoundedCount)
	}
	if boundedCount == record.IdentityCount {
		t.Fatalf("health fixture did not produce a diagnostic mismatch: count=%d ledger=%d", boundedCount, record.IdentityCount)
	}
	t.Logf("administrator health diagnostic: bounded physical identities=%d ledger identities=%d", boundedCount, record.IdentityCount)
}

func privacyContractCloneVisibleAuthorities(t *testing.T, database *control.DB, count int) {
	t.Helper()
	if count < 1 {
		t.Fatalf("visible authority clone count = %d", count)
	}
	triggerRows, err := database.SQLDB().Query(`
		SELECT name
		FROM sqlite_schema
		WHERE type = 'trigger' AND tbl_name = 'knowledge_objects'
		ORDER BY name
	`)
	if err != nil {
		t.Fatalf("enumerate visible over-cap registry triggers: %v", err)
	}
	var triggers []string
	for triggerRows.Next() {
		var name string
		if err := triggerRows.Scan(&name); err != nil {
			_ = triggerRows.Close()
			t.Fatalf("scan visible over-cap registry trigger: %v", err)
		}
		triggers = append(triggers, name)
	}
	if err := triggerRows.Err(); err != nil {
		_ = triggerRows.Close()
		t.Fatalf("read visible over-cap registry triggers: %v", err)
	}
	if err := triggerRows.Close(); err != nil {
		t.Fatalf("close visible over-cap registry triggers: %v", err)
	}
	for _, trigger := range triggers {
		dropTrigger(t, database, trigger)
	}
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire visible over-cap fixture connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable visible over-cap fixture foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `
		WITH RECURSIVE staged(sequence) AS (
			SELECT 0
			UNION ALL
			SELECT sequence + 1 FROM staged WHERE sequence + 1 < ?
		)
		INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro,
		quarantine_reason
	)
	SELECT source.tenant_id, printf('ko-visible-capacity-%05d', staged.sequence), source.current_version,
		source.app_id, source.owner_id, source.object_type, source.name,
		source.sharing_scope, source.state, source.definition_digest,
		source.created_at_unix_micro, source.updated_at_unix_micro,
		source.disabled_at_unix_micro, source.quarantined_at_unix_micro,
		source.deleted_at_unix_micro, source.quarantine_reason
	FROM staged
	JOIN knowledge_objects AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	`, count, testTenant, "ko-visible-capacity-seed"); err != nil {
		t.Fatalf("insert visible over-cap registry identities: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore visible over-cap fixture foreign keys: %v", err)
	}
}

func privacyContractStringPointer(value string) *string { return &value }
