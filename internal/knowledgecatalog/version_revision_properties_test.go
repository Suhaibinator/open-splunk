package knowledgecatalog

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCatalogRevisionExactSQLiteDomainBoundariesForGetAndList(t *testing.T) {
	for _, test := range []struct {
		name        string
		revision    int64
		bypassCheck bool
		wantCorrupt bool
	}{
		{name: "maximum admitted", revision: math.MaxInt64 - 1},
		{name: "signed maximum excluded", revision: math.MaxInt64, bypassCheck: true, wantCorrupt: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertFixtureObject(t, database, fixtureObject{id: "ko-revision-boundary", owner: testOwner, versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, "revision_boundary", SharingScopePrivate, nil, "revision-*"),
				state:      StateActive, mutation: "create", timestamp: 10,
			}}})
			overwriteCatalogRevisionAuthority(t, database, test.revision, test.bypassCheck)

			object, getErr := store.Get(context.Background(), testReadScope(), "ko-revision-boundary", nil)
			page, listErr := store.List(context.Background(), testReadScope(), ListRequest{})
			if test.wantCorrupt {
				if !errors.Is(getErr, ErrCorrupt) || !errors.Is(listErr, ErrCorrupt) {
					t.Fatalf("boundary errors = Get:%v List:%v, want ErrCorrupt", getErr, listErr)
				}
				return
			}
			if getErr != nil || object.KnowledgeObjectID != "ko-revision-boundary" {
				t.Fatalf("Get at admitted boundary = %#v, %v", object, getErr)
			}
			if listErr != nil || page.CatalogRevision != uint64(test.revision) || len(page.Objects) != 1 {
				t.Fatalf("List at admitted boundary = %#v, %v", page, listErr)
			}
		})
	}
}

func TestExplicitMissingHistoryMakesEveryVisibleVersionRequestCorrupt(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-missing-history", owner: testOwner, versions: []fixtureVersion{
		{
			definition: aliasDefinition(testApp, "missing_history", SharingScopePrivate, nil, "history-v1-*"),
			state:      StateActive, mutation: "create", timestamp: 10,
		},
		{
			definition: aliasDefinition(testApp, "missing_history", SharingScopePrivate, nil, "history-v2-*"),
			state:      StateActive, mutation: "update", timestamp: 20,
		},
	}})
	dropTrigger(t, database, "knowledge_dependency_seal_delete_is_forbidden")
	dropTrigger(t, database, "knowledge_object_version_delete_is_forbidden")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire history-corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `DELETE FROM knowledge_object_dependency_seals
		WHERE tenant_id = ? AND knowledge_object_id = 'ko-missing-history' AND object_version = 1`, testTenant); err != nil {
		_ = connection.Close()
		t.Fatalf("delete historical dependency seal: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `DELETE FROM knowledge_object_versions
		WHERE tenant_id = ? AND knowledge_object_id = 'ko-missing-history' AND object_version = 1`, testTenant); err != nil {
		_ = connection.Close()
		t.Fatalf("delete historical immutable version: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close history-corruption connection: %v", err)
	}

	missing := uint64(1)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-missing-history", &missing); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(missing retained v1) error = %v, want ErrCorrupt", err)
	}
	future := uint64(3)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-missing-history", &future); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(v3 beyond corrupt current history) error = %v, want ErrCorrupt", err)
	}
}

func TestFutureVersionOfCoherentObjectIsNotFound(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-coherent-future", owner: testOwner, versions: []fixtureVersion{
		{
			definition: aliasDefinition(testApp, "coherent_future", SharingScopePrivate, nil, "future-v1-*"),
			state:      StateActive, mutation: "create", timestamp: 10,
		},
		{
			definition: aliasDefinition(testApp, "coherent_future", SharingScopePrivate, nil, "future-v2-*"),
			state:      StateActive, mutation: "update", timestamp: 20,
		},
	}})
	future := uint64(3)
	if object, err := store.Get(context.Background(), testReadScope(), "ko-coherent-future", &future); !errors.Is(err, control.ErrNotFound) || !reflect.DeepEqual(object, Object{}) {
		t.Fatalf("Get(v3 beyond coherent current) = %#v, %v, want zero/ErrNotFound", object, err)
	}
}
