package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestGraphOutgoingExactVersionPrivacyPaginationAndDetachedAuthority(t *testing.T) {
	database, store := newCatalogTestStore(t)
	targetAV1Description := "target a v1"
	targetAV2Description := "target a v2"
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-target-a", owner: testOwner, versions: []fixtureVersion{
		{
			definition: dependencyExtractionDefinition(
				testApp, "graph-target-a", SharingScopePrivate, &targetAV1Description, "graph-*", "raw_value",
			),
			state: StateDraft, mutation: "create", timestamp: 10,
		},
		{
			definition: dependencyExtractionDefinition(
				testApp, "graph-target-a", SharingScopePrivate, &targetAV2Description, "graph-*", "raw_value",
			),
			state: StateDraft, mutation: "update", timestamp: 11,
		},
	}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-target-hidden", owner: "owner-b", versions: []fixtureVersion{{
		definition: dependencyExtractionDefinition(
			testApp, "graph-target-hidden", SharingScopePrivate, nil, "graph-*", "hidden_value",
		),
		state: StateDraft, mutation: "create", timestamp: 12,
	}}})
	reason := "root_corruption"
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-target-quarantined", owner: testOwner, versions: []fixtureVersion{
		{
			definition: dependencyExtractionDefinition(
				testApp, "graph-target-quarantined", SharingScopePrivate, nil, "graph-*", "quarantined_value",
			),
			state: StateDraft, mutation: "create", timestamp: 13,
		},
		{state: StateQuarantined, mutation: "quarantine", reason: &reason, timestamp: 14},
	}})
	rootV1Description := "root v1"
	rootV2Description := "root v2"
	rootV1Definition := dependencyAliasDefinition(
		testApp,
		"graph-root",
		SharingScopePrivate,
		&rootV1Description,
		"graph-*",
		"raw_value",
		"alias_value",
	)
	rootV2Definition := dependencyAliasDefinition(
		testApp,
		"graph-root",
		SharingScopePrivate,
		&rootV2Description,
		"graph-*",
		"raw_value",
		"alias_value",
	)
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-root", owner: testOwner, versions: []fixtureVersion{
		{
			definition: rootV1Definition, state: StateDraft, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{
				{targetObjectID: "ko-graph-target-a", targetVersion: 1},
				{targetObjectID: "ko-graph-target-a", targetVersion: 2},
				{targetObjectID: "ko-graph-target-hidden", targetVersion: 1},
				{targetObjectID: "ko-graph-target-quarantined", targetVersion: 1},
			},
		},
		{
			definition: rootV2Definition, state: StateDraft, mutation: "update", timestamp: 21,
			dependencies: []fixtureDependency{{targetObjectID: "ko-graph-target-a", targetVersion: 2}},
		},
	}})
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ? AND definition_digest = (
			SELECT definition_digest FROM knowledge_objects
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-graph-target-hidden'
		)`, testTenant, testTenant)

	current, err := store.ListDependencies(context.Background(), testReadScope(), DependencyListRequest{
		KnowledgeObjectID: "ko-graph-root",
		IncludeTotal:      true,
	})
	if err != nil {
		t.Fatalf("ListDependencies(current): %v", err)
	}
	if current.ResolvedObject.Version != 2 || len(current.Edges) != 1 ||
		current.Edges[0].Target.Version != 2 || current.TotalSize == nil || *current.TotalSize != 1 {
		t.Fatalf("ListDependencies(current) = %#v", current)
	}
	assertGraphAuthority(t, current, graphDirectionDependencies)

	version := uint64(1)
	request := DependencyListRequest{
		KnowledgeObjectID: "ko-graph-root",
		Version:           &version,
		PageSize:          1,
		IncludeTotal:      true,
	}
	first, err := store.ListDependencies(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("ListDependencies(v1 first): %v", err)
	}
	if first.ResolvedObject.Version != 1 || len(first.Edges) != 1 ||
		first.Edges[0].Target.KnowledgeObjectID != "ko-graph-target-a" ||
		first.Edges[0].Target.Version != 1 || first.TotalSize == nil || *first.TotalSize != 2 ||
		first.NextPageToken == "" {
		t.Fatalf("ListDependencies(v1 first) = %#v", first)
	}
	assertGraphAuthority(t, first, graphDirectionDependencies)
	request.PageToken = first.NextPageToken
	second, err := store.ListDependencies(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("ListDependencies(v1 second): %v", err)
	}
	if len(second.Edges) != 1 || second.Edges[0].Target.Version != 2 ||
		second.TotalSize == nil || *second.TotalSize != 2 || second.NextPageToken != "" {
		t.Fatalf("ListDependencies(v1 second) = %#v", second)
	}
	assertGraphAuthority(t, second, graphDirectionDependencies)

	rebound := request
	rebound.PageSize = 2
	if _, err := store.ListDependencies(context.Background(), testReadScope(), rebound); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ListDependencies(rebound cursor) error = %v, want ErrInvalidCursor", err)
	}
	if _, err := store.ListDependents(context.Background(), testReadScope(), request); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ListDependents(outgoing cursor) error = %v, want ErrInvalidCursor", err)
	}

	first.ResolvedCurrent.AppID = "mutated"
	first.Edges[0].Target.KnowledgeObjectID = "mutated"
	againRequest := request
	againRequest.PageToken = ""
	again, err := store.ListDependencies(context.Background(), testReadScope(), againRequest)
	if err != nil || again.ResolvedCurrent.AppID != testApp || again.Edges[0].Target.KnowledgeObjectID != "ko-graph-target-a" {
		t.Fatalf("ListDependencies(after caller mutation) = %#v, %v", again, err)
	}
}

func TestGraphIncomingCurrentSourcesAllLifecycleStatesAndExactTargetVersion(t *testing.T) {
	database, store := newCatalogTestStore(t)
	target := func(description string) *opensplunkv1.KnowledgeObjectDefinition {
		return dependencyExtractionDefinition(
			testApp, "incoming-target", SharingScopePrivate, &description, "incoming-*", "raw_value",
		)
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-incoming-target", owner: testOwner, versions: []fixtureVersion{
		{definition: target("v1"), state: StateDraft, mutation: "create", timestamp: 10},
		{definition: target("v2"), state: StateDraft, mutation: "update", timestamp: 11},
	}})
	dependency := []fixtureDependency{{targetObjectID: "ko-incoming-target", targetVersion: 1}}
	alias := func(name string) *opensplunkv1.KnowledgeObjectDefinition {
		return dependencyAliasDefinition(
			testApp, name, SharingScopePrivate, nil, "incoming-*", "raw_value", name+"_value",
		)
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-incoming-a-draft", owner: testOwner, versions: []fixtureVersion{{
		definition: alias("incoming-a-draft"), state: StateDraft, mutation: "create", timestamp: 20,
		dependencies: dependency,
	}}})
	for _, fixture := range []struct {
		id    string
		state State
		time  int64
	}{
		{id: "ko-incoming-b-disabled", state: StateDisabled, time: 30},
		{id: "ko-incoming-c-deleted", state: StateDeleted, time: 40},
	} {
		definition := alias(fixture.id)
		versions := []fixtureVersion{
			{definition: definition, state: StateDraft, mutation: "create", timestamp: fixture.time, dependencies: dependency},
			{definition: definition, state: StateDisabled, mutation: "disable", timestamp: fixture.time + 1, dependencies: dependency},
		}
		if fixture.state == StateDeleted {
			versions = append(versions, fixtureVersion{
				definition: definition, state: StateDeleted, mutation: "delete", timestamp: fixture.time + 2,
				dependencies: dependency,
			})
		}
		insertFixtureObject(t, database, fixtureObject{id: fixture.id, owner: testOwner, versions: versions})
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-incoming-hidden", owner: "owner-b", versions: []fixtureVersion{{
		definition: alias("incoming-hidden"), state: StateDraft, mutation: "create", timestamp: 50,
		dependencies: dependency,
	}}})
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ? AND definition_digest = (
			SELECT definition_digest FROM knowledge_objects
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-incoming-hidden'
		)`, testTenant, testTenant)

	current, err := store.ListDependents(context.Background(), testReadScope(), DependencyListRequest{
		KnowledgeObjectID: "ko-incoming-target",
		IncludeTotal:      true,
	})
	if err != nil {
		t.Fatalf("ListDependents(current target): %v", err)
	}
	if current.ResolvedObject.Version != 2 || len(current.Edges) != 0 ||
		current.TotalSize == nil || *current.TotalSize != 0 {
		t.Fatalf("ListDependents(current target) = %#v", current)
	}

	version := uint64(1)
	request := DependencyListRequest{
		KnowledgeObjectID: "ko-incoming-target",
		Version:           &version,
		PageSize:          2,
		IncludeTotal:      true,
	}
	first, err := store.ListDependents(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("ListDependents(v1 first): %v", err)
	}
	if got := graphSourceIDs(first.Edges); !slices.Equal(got, []string{
		"ko-incoming-a-draft", "ko-incoming-b-disabled",
	}) || first.TotalSize == nil || *first.TotalSize != 3 || first.NextPageToken == "" {
		t.Fatalf("ListDependents(v1 first) = %#v", first)
	}
	assertGraphAuthority(t, first, graphDirectionDependents)
	request.PageToken = first.NextPageToken
	second, err := store.ListDependents(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("ListDependents(v1 second): %v", err)
	}
	if got := graphSourceIDs(second.Edges); !slices.Equal(got, []string{"ko-incoming-c-deleted"}) ||
		second.TotalSize == nil || *second.TotalSize != 3 || second.NextPageToken != "" {
		t.Fatalf("ListDependents(v1 second) = %#v", second)
	}
	assertGraphAuthority(t, second, graphDirectionDependents)
}

func TestGraphRootAuthorizationQuarantineCursorInvalidationAndCancellation(t *testing.T) {
	database, store := newCatalogTestStore(t)
	definition := aliasDefinition(testApp, "graph-guard", SharingScopePrivate, nil, "guard-*")
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-guard", owner: testOwner, versions: []fixtureVersion{{
		definition: definition, state: StateDraft, mutation: "create", timestamp: 10,
	}}})
	reason := "root_corruption"
	quarantineDefinition := aliasDefinition(
		testApp, "graph-quarantine", SharingScopePrivate, nil, "quarantine-*",
	)
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-quarantine", owner: testOwner, versions: []fixtureVersion{
		{definition: quarantineDefinition, state: StateDraft, mutation: "create", timestamp: 20},
		{state: StateQuarantined, mutation: "quarantine", reason: &reason, timestamp: 21},
	}})
	for _, method := range []func(context.Context, ReadScope, DependencyListRequest) (DependencyPage, error){
		store.ListDependencies,
		store.ListDependents,
	} {
		if _, err := method(context.Background(), testReadScope(), DependencyListRequest{
			KnowledgeObjectID: "ko-graph-quarantine",
		}); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("quarantined root error = %v, want ErrNotFound", err)
		} else if _, found := AuthorizedContextFromError(err); found {
			t.Errorf("quarantined root carried authorized context: %v", err)
		}
		hiddenScope := ReadScope{TenantID: testTenant, OwnerID: "owner-b", ReadableAppIDs: []string{testApp}}
		if _, err := method(context.Background(), hiddenScope, DependencyListRequest{
			KnowledgeObjectID: "ko-graph-guard",
		}); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("hidden root error = %v, want ErrNotFound", err)
		}
		if _, err := method(nil, testReadScope(), DependencyListRequest{KnowledgeObjectID: "ko-graph-guard"}); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("nil context error = %v, want ErrInvalidArgument", err)
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := method(canceled, testReadScope(), DependencyListRequest{KnowledgeObjectID: "ko-graph-guard"}); !errors.Is(err, context.Canceled) {
			t.Errorf("canceled context error = %v, want context.Canceled", err)
		}
		future := uint64(2)
		if _, err := method(context.Background(), testReadScope(), DependencyListRequest{
			KnowledgeObjectID: "ko-graph-guard", Version: &future,
		}); !errors.Is(err, control.ErrNotFound) {
			t.Errorf("future graph version error = %v, want ErrNotFound", err)
		} else if authorized, found := AuthorizedContextFromError(err); !found ||
			authorized.Object == nil || authorized.Object.KnowledgeObjectID != "ko-graph-guard" {
			t.Errorf("future graph version authorization = %#v, found %t", authorized, found)
		}
	}

	// Build a two-edge source solely to obtain a continuation, then advance the
	// tenant state with an unrelated object. Continuations are restore-fork-safe
	// catalog snapshots, not best-effort offsets.
	for _, id := range []string{"ko-graph-page-a", "ko-graph-page-b"} {
		insertFixtureObject(t, database, fixtureObject{id: id, owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, id, SharingScopePrivate, nil, id+"-*"),
			state:      StateDraft, mutation: "create", timestamp: 30,
		}}})
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-page-root", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-page-root", SharingScopePrivate, nil, "page-*"),
		state:      StateDraft, mutation: "create", timestamp: 31,
		dependencies: []fixtureDependency{
			{targetObjectID: "ko-graph-page-a", targetVersion: 1},
			{targetObjectID: "ko-graph-page-b", targetVersion: 1},
		},
	}}})
	request := DependencyListRequest{KnowledgeObjectID: "ko-graph-page-root", PageSize: 1}
	first, err := store.ListDependencies(context.Background(), testReadScope(), request)
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("ListDependencies(first continuation) = %#v, %v", first, err)
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-unrelated", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-unrelated", SharingScopePrivate, nil, "unrelated-*"),
		state:      StateDraft, mutation: "create", timestamp: 40,
	}}})
	request.PageToken = first.NextPageToken
	if _, err := store.ListDependencies(context.Background(), testReadScope(), request); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("ListDependencies(stale cursor) error = %v, want ErrPageInvalidated", err)
	}
}

func TestGraphCursorInvalidatesAtSameRevisionWithDifferentStateToken(t *testing.T) {
	database, store := newCatalogTestStore(t)
	for _, id := range []string{"ko-graph-fork-a", "ko-graph-fork-b"} {
		insertFixtureObject(t, database, fixtureObject{id: id, owner: testOwner, versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, id, SharingScopePrivate, nil, id+"-*"),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}}})
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-fork-root", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-fork-root", SharingScopePrivate, nil, "fork-*"),
		state:      StateDraft, mutation: "create", timestamp: 20,
		dependencies: []fixtureDependency{
			{targetObjectID: "ko-graph-fork-a", targetVersion: 1},
			{targetObjectID: "ko-graph-fork-b", targetVersion: 1},
		},
	}}})
	request := DependencyListRequest{KnowledgeObjectID: "ko-graph-fork-root", PageSize: 1}
	first, err := store.ListDependencies(context.Background(), testReadScope(), request)
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("ListDependencies(fork first) = %#v, %v", first, err)
	}
	var oldToken []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT state_token FROM knowledge_catalog_revision_heads
		WHERE tenant_id = ?`, testTenant).Scan(&oldToken); err != nil {
		t.Fatalf("read old catalog state token: %v", err)
	}
	newToken := bytes.Clone(oldToken)
	newToken[0] ^= 0xff
	dropTrigger(t, database, "knowledge_catalog_revision_head_transition_is_exact")
	mustExec(t, database, `UPDATE knowledge_catalog_revision_heads SET state_token = ?
		WHERE tenant_id = ?`, newToken, testTenant)
	request.PageToken = first.NextPageToken
	if _, err := store.ListDependencies(context.Background(), testReadScope(), request); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("ListDependencies(equal revision divergent state) error = %v, want ErrPageInvalidated", err)
	}
}

func TestIncomingGraphRejectsOversizedPersistedRoleBeforePayloadDisclosure(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-wide-target", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-wide-target", SharingScopePrivate, nil, "wide-target-*"),
		state:      StateDraft, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-wide-source", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-wide-source", SharingScopePrivate, nil, "wide-source-*"),
		state:      StateDraft, mutation: "create", timestamp: 20,
		dependencies: []fixtureDependency{{
			targetObjectID: "ko-graph-wide-target", targetVersion: 1,
		}},
	}}})
	dropTableTriggers(t, database, "knowledge_object_dependencies")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire oversized graph connection: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable graph dependency checks: %v", err)
	}
	const marker = "SECRET-GRAPH-ROLE-"
	if _, err := connection.ExecContext(context.Background(), `UPDATE knowledge_object_dependencies
		SET dependency_role = ? || CAST(zeroblob(?) AS TEXT)
		WHERE tenant_id = ? AND source_object_id = ?`, marker, 1<<20, testTenant, "ko-graph-wide-source"); err != nil {
		t.Fatalf("inject oversized graph role: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore graph dependency checks: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close oversized graph connection: %v", err)
	}
	_, err = store.ListDependents(context.Background(), testReadScope(), DependencyListRequest{
		KnowledgeObjectID: "ko-graph-wide-target",
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ListDependents(oversized role) error = %v, want ErrCorrupt", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("oversized persisted role leaked through error: %v", err)
	}
}

func TestGraphIgnoresHiddenTenantLedgerAndPhysicalIdentityCorruption(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-private-target", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-private-target", SharingScopePrivate, nil, "private-target-*"),
		state:      StateDraft, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-graph-private-source", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "graph-private-source", SharingScopePrivate, nil, "private-source-*"),
		state:      StateDraft, mutation: "create", timestamp: 20,
		dependencies: []fixtureDependency{{
			targetObjectID: "ko-graph-private-target", targetVersion: 1,
		}},
	}}})
	outgoingRequest := DependencyListRequest{
		KnowledgeObjectID: "ko-graph-private-source", IncludeTotal: true,
	}
	incomingRequest := DependencyListRequest{
		KnowledgeObjectID: "ko-graph-private-target", IncludeTotal: true,
	}
	baselineOutgoing, err := store.ListDependencies(context.Background(), testReadScope(), outgoingRequest)
	if err != nil {
		t.Fatalf("ListDependencies(hidden-corruption baseline): %v", err)
	}
	baselineIncoming, err := store.ListDependents(context.Background(), testReadScope(), incomingRequest)
	if err != nil {
		t.Fatalf("ListDependents(hidden-corruption baseline): %v", err)
	}

	mustExec(t, database, `UPDATE knowledge_catalog_tenants
		SET identity_count = identity_count - 1 WHERE tenant_id = ?`, testTenant)
	dropTableTriggers(t, database, "knowledge_objects")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire hidden graph corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable hidden graph foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	)
	SELECT tenant_id, 'ko-graph-hidden-orphan', current_version, app_id, 'owner-hidden',
		object_type, name, 'private', state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro, disabled_at_unix_micro,
		quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	FROM knowledge_objects WHERE tenant_id = ? AND knowledge_object_id = ?`,
		testTenant, "ko-graph-private-source"); err != nil {
		t.Fatalf("insert hidden graph physical corruption: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore hidden graph foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close hidden graph corruption connection: %v", err)
	}

	afterOutgoing, err := store.ListDependencies(context.Background(), testReadScope(), outgoingRequest)
	if err != nil || !reflect.DeepEqual(afterOutgoing, baselineOutgoing) {
		t.Fatalf("ListDependencies(after hidden corruption) = %#v, %v, want %#v", afterOutgoing, err, baselineOutgoing)
	}
	afterIncoming, err := store.ListDependents(context.Background(), testReadScope(), incomingRequest)
	if err != nil || !reflect.DeepEqual(afterIncoming, baselineIncoming) {
		t.Fatalf("ListDependents(after hidden corruption) = %#v, %v, want %#v", afterIncoming, err, baselineIncoming)
	}
}

func graphSourceIDs(edges []DependencyEdge) []string {
	result := make([]string, len(edges))
	for index, edge := range edges {
		result[index] = edge.Source.KnowledgeObjectID
	}
	return result
}

func assertGraphAuthority(t *testing.T, page DependencyPage, direction graphDirection) {
	t.Helper()
	if err := validateDependencyPageAuthorities(page, direction); err != nil {
		t.Fatalf("graph authority: %v", err)
	}
	if page.ResolvedCurrent.TenantID != testTenant {
		t.Fatalf("resolved tenant = %q, want %q", page.ResolvedCurrent.TenantID, testTenant)
	}
}
