package knowledgecatalog

import (
	"context"
	"slices"
	"testing"
)

func TestIntegrationAuthorizedRogueProjectionsCannotConsumeListDriverBound(t *testing.T) {
	database, store := newCatalogTestStore(t)
	description := "legitimate visible projection"
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-projection-coverage-valid", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(
				testApp,
				"zzzz-projection-coverage-valid",
				SharingScopePrivate,
				&description,
				"coverage-*",
			),
			state: StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	dropTableTriggers(t, database, "knowledge_object_list_projections")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire rogue-projection connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable rogue-projection foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `
		WITH RECURSIVE sequence(value) AS (
			SELECT 1
			UNION ALL
			SELECT value + 1 FROM sequence WHERE value < ?
		)
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		)
		SELECT ?, printf('aaa-rogue-projection-%05d', value), 1,
		       ?, ?, 'field_alias', 'aaa-rogue-projection', 'private', 'draft',
		       0, '', 0, 0, 0, 0, 0, 46
		FROM sequence
	`, maximumObjectsPerTenant+1, testTenant, testApp, testOwner); err != nil {
		t.Fatalf("insert rogue visible projections: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore rogue-projection foreign keys: %v", err)
	}

	if _, err := store.Get(context.Background(), testReadScope(), "ko-projection-coverage-valid", nil); err != nil {
		t.Fatalf("Get(valid object amid rogue projections): %v", err)
	}

	textFilter := "legitimate"
	selectorFilter := "coverage-"
	for _, request := range []ListRequest{
		{},
		{IncludeTotal: true},
		{TextFilter: &textFilter, IncludeTotal: true},
		{SelectorTextFilter: &selectorFilter, IncludeTotal: true},
	} {
		page, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(rogue projections, request=%#v): %v", request, err)
		}
		if got := objectIDs(page.Objects); !slices.Equal(got, []string{"ko-projection-coverage-valid"}) ||
			page.NextPageToken != "" {
			t.Fatalf("List(rogue projections, request=%#v) = objects:%v token:%q",
				request, got, page.NextPageToken)
		}
		if request.IncludeTotal && (page.TotalSize == nil || *page.TotalSize != 1 || !page.TotalSizeExact) {
			t.Fatalf("List(rogue projections, request=%#v) total = %v/%t",
				request, page.TotalSize, page.TotalSizeExact)
		}
	}
}
