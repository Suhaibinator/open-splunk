package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIntegrationDependencySemanticsValidTierOneChainAcrossGetAndBatchList(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-semantic-extraction", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyExtractionDefinition(
			testApp, "semantic-extraction", SharingScopePrivate, nil, "semantic-*", "raw_value",
		),
		state: StateActive, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-semantic-alias", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyAliasDefinition(
			testApp, "semantic-alias", SharingScopePrivate, nil, "semantic-*", "raw_value", "alias_value",
		),
		state: StateActive, mutation: "create", timestamp: 11,
		dependencies: []fixtureDependency{{
			targetObjectID: "ko-semantic-extraction", targetVersion: 1,
		}},
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-semantic-calculated", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyCalculatedDefinition(
			testApp, "semantic-calculated", SharingScopePrivate, nil, "semantic-*",
			"lower(alias_value)", "calculated_value",
		),
		state: StateActive, mutation: "create", timestamp: 12,
		dependencies: []fixtureDependency{{
			targetObjectID: "ko-semantic-alias", targetVersion: 1,
		}},
	}}})

	for _, objectID := range []string{"ko-semantic-alias", "ko-semantic-calculated"} {
		if _, err := store.Get(context.Background(), testReadScope(), objectID, nil); err != nil {
			t.Fatalf("Get(%s valid dependency chain): %v", objectID, err)
		}
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 10})
	if err != nil {
		t.Fatalf("List(valid dependency chain): %v", err)
	}
	if !integrationPageContainsObject(page, "ko-semantic-extraction") ||
		!integrationPageContainsObject(page, "ko-semantic-alias") ||
		!integrationPageContainsObject(page, "ko-semantic-calculated") {
		t.Fatalf("List(valid dependency chain) omitted an object: %#v", page)
	}
}

func TestIntegrationBatchDependencySemanticSharedDAGQueryCountIsBounded(t *testing.T) {
	seed := func(t *testing.T, dependents int) (*control.DB, *Store) {
		t.Helper()
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{id: "ko-shared-target", owner: testOwner, versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "aaa-shared-target", SharingScopePrivate, nil, "shared-*", "shared_input",
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}}})
		for index := 0; index < dependents; index++ {
			insertFixtureObject(t, database, fixtureObject{
				id: fmt.Sprintf("ko-shared-source-%03d", index), owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyAliasDefinition(
						testApp, fmt.Sprintf("shared-source-%03d", index), SharingScopePrivate, nil,
						"shared-*", "shared_input", fmt.Sprintf("shared_alias_%03d", index),
					),
					state: StateActive, mutation: "create", timestamp: int64(20 + index),
					dependencies: []fixtureDependency{{
						targetObjectID: "ko-shared-target", targetVersion: 1,
					}},
				}},
			})
		}
		return database, store
	}
	smallDatabase, smallStore := seed(t, 1)
	largeDatabase, largeStore := seed(t, MaximumPageSize-1)
	request := ListRequest{PageSize: MaximumPageSize}
	smallPage, smallQueries := integrationCountCatalogQueries(t, smallDatabase, func() (ListPage, error) {
		return smallStore.List(context.Background(), testReadScope(), request)
	})
	largePage, largeQueries := integrationCountCatalogQueries(t, largeDatabase, func() (ListPage, error) {
		return largeStore.List(context.Background(), testReadScope(), request)
	})
	if len(smallPage.Objects) != 2 || len(largePage.Objects) != MaximumPageSize {
		t.Fatalf("shared-DAG page sizes = small:%d large:%d", len(smallPage.Objects), len(largePage.Objects))
	}
	if largeQueries > smallQueries+3 || largeQueries > 24 {
		t.Fatalf("shared-DAG query count grew per dependent: small=%d large=%d", smallQueries, largeQueries)
	}
	t.Logf("shared-DAG catalog query counts: 1 dependent=%d; %d dependents=%d", smallQueries, MaximumPageSize-1, largeQueries)
}

func TestIntegrationRecognizedInactiveDefinitionsSkipExecutableDependencySemantics(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-inactive-semantic-target", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyAliasDefinition(
			testApp, "inactive-semantic-target", SharingScopePrivate, nil, "inactive-*",
			"event_input", "inactive_input",
		),
		state: StateActive, mutation: "create", timestamp: 10,
	}}})
	fixtures := []struct {
		id       string
		versions []fixtureVersion
		state    State
	}{
		{
			id: "ko-semantic-draft", state: StateDraft,
			versions: []fixtureVersion{{
				definition: dependencyAliasDefinition(
					testApp, "semantic-draft", SharingScopePrivate, nil, "inactive-*",
					"inactive_input", "draft_alias",
				),
				state: StateDraft, mutation: "create", timestamp: 20,
				dependencies: []fixtureDependency{{
					targetObjectID: "ko-inactive-semantic-target", targetVersion: 1,
				}},
			}},
		},
		{
			id: "ko-semantic-disabled", state: StateDisabled,
			versions: recognizedInactiveSemanticVersions("semantic-disabled", "disabled_alias", StateDisabled, 30),
		},
		{
			id: "ko-semantic-deleted", state: StateDeleted,
			versions: recognizedInactiveSemanticVersions("semantic-deleted", "deleted_alias", StateDeleted, 40),
		},
	}
	for _, fixture := range fixtures {
		insertFixtureObject(t, database, fixtureObject{id: fixture.id, owner: testOwner, versions: fixture.versions})
		object, err := store.Get(context.Background(), testReadScope(), fixture.id, nil)
		if err != nil || object.State != fixture.state {
			t.Fatalf("Get(%s inactive same-stage dependency) = %#v, %v", fixture.state, object, err)
		}
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 10}); err != nil {
		t.Fatalf("List(inactive same-stage dependencies): %v", err)
	}
}

func recognizedInactiveSemanticVersions(name, destination string, state State, timestamp int64) []fixtureVersion {
	dependencies := []fixtureDependency{{
		targetObjectID: "ko-inactive-semantic-target", targetVersion: 1,
	}}
	definition := dependencyAliasDefinition(
		testApp, name, SharingScopePrivate,
		stringPointer(fmt.Sprintf("%s inactive", name)),
		"inactive-*", "inactive_input", destination,
	)
	versions := []fixtureVersion{{
		definition: definition,
		state:      StateDraft, mutation: "create", timestamp: timestamp,
		dependencies: dependencies,
	}}
	inactive := fixtureVersion{
		definition: definition,
		state:      StateDisabled, mutation: "disable", timestamp: timestamp + 1,
		dependencies: dependencies,
	}
	versions = append(versions, inactive)
	switch state {
	case StateDisabled:
		return versions
	case StateDeleted:
		deleted := fixtureVersion{
			definition: definition,
			state:      StateDeleted, mutation: "delete", timestamp: timestamp + 2,
			dependencies: dependencies,
		}
		return append(versions, deleted)
	default:
		panic("recognizedInactiveSemanticVersions requires disabled or deleted state")
	}
}

func TestIntegrationInactiveRecognizedExpressionsRemainInspectableUntilPublication(t *testing.T) {
	database, store := newCatalogTestStore(t)
	definition := func(name, expression, revision string) *opensplunkv1.KnowledgeObjectDefinition {
		return dependencyCalculatedDefinition(
			testApp, name, SharingScopePrivate, stringPointer(revision), "editable-*", expression, "result_value",
		)
	}
	valid := func(name, revision string) *opensplunkv1.KnowledgeObjectDefinition {
		return definition(name, "lower(source_value)", revision)
	}
	invalid := func(name, revision string) *opensplunkv1.KnowledgeObjectDefinition {
		return definition(name, "lower(", revision)
	}
	fixtures := []fixtureObject{
		{
			id: "ko-invalid-draft-expression", owner: testOwner,
			versions: []fixtureVersion{{
				definition: invalid("invalid-draft-expression", "draft v1"),
				state:      StateDraft, mutation: "create", timestamp: 20,
			}},
		},
		{
			id: "ko-invalid-disabled-expression", owner: testOwner,
			versions: []fixtureVersion{
				{definition: valid("invalid-disabled-expression", "active v1"), state: StateActive, mutation: "create", timestamp: 30},
				{definition: valid("invalid-disabled-expression", "disabled v2"), state: StateDisabled, mutation: "disable", timestamp: 31},
				{definition: invalid("invalid-disabled-expression", "disabled v3"), state: StateDisabled, mutation: "update", timestamp: 32},
			},
		},
		{
			id: "ko-invalid-deleted-expression", owner: testOwner,
			versions: []fixtureVersion{
				{definition: valid("invalid-deleted-expression", "active v1"), state: StateActive, mutation: "create", timestamp: 40},
				{definition: valid("invalid-deleted-expression", "disabled v2"), state: StateDisabled, mutation: "disable", timestamp: 41},
				{definition: invalid("invalid-deleted-expression", "disabled v3"), state: StateDisabled, mutation: "update", timestamp: 42},
				{definition: invalid("invalid-deleted-expression", "deleted v4"), state: StateDeleted, mutation: "delete", timestamp: 43},
			},
		},
	}
	for _, fixture := range fixtures {
		insertFixtureObject(t, database, fixture)
		object, err := store.Get(context.Background(), testReadScope(), fixture.id, nil)
		if err != nil || object.Definition.GetCalculatedField().GetExpression() != "lower(" {
			t.Fatalf("Get(%s editable expression) = %#v, %v", fixture.id, object, err)
		}
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 10}); err != nil {
		t.Fatalf("List(editable inactive expressions): %v", err)
	}

	corruptDatabase, corruptStore := newCatalogTestStore(t)
	insertFixtureObject(t, corruptDatabase, fixtureObject{
		id: "ko-invalid-active-expression", owner: testOwner,
		versions: []fixtureVersion{{
			definition: invalid("invalid-active-expression", "active v1"),
			state:      StateActive, mutation: "create", timestamp: 50,
		}},
	})
	if _, err := corruptStore.Get(context.Background(), testReadScope(), "ko-invalid-active-expression", nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(invalid active expression) error = %v, want ErrCorrupt", err)
	}
	if _, err := corruptStore.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("List(invalid active expression) error = %v, want ErrCorrupt", err)
	}
}

func TestIntegrationDependencySemanticsRejectInvalidEdgesAcrossGetAndBatchList(t *testing.T) {
	tests := []struct {
		name             string
		targetDefinition func() *fixtureVersion
		sourceDefinition func() *fixtureVersion
	}{
		{
			name: "extraction edge",
			targetDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyExtractionDefinition(
					testApp, "invalid-target", SharingScopePrivate, nil, "invalid-*", "earlier_output",
				))
			},
			sourceDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyExtractionDefinition(
					testApp, "invalid-source", SharingScopePrivate, nil, "invalid-*", "source_output",
				))
			},
		},
		{
			name: "same-stage alias edge",
			targetDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyAliasDefinition(
					testApp, "invalid-target", SharingScopePrivate, nil, "invalid-*", "event_input", "alias_output",
				))
			},
			sourceDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyAliasDefinition(
					testApp, "invalid-source", SharingScopePrivate, nil, "invalid-*", "alias_output", "source_output",
				))
			},
		},
		{
			name: "later-stage alias to calculated edge",
			targetDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyCalculatedDefinition(
					testApp, "invalid-target", SharingScopePrivate, nil, "invalid-*", `"literal"`, "calculated_output",
				))
			},
			sourceDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyAliasDefinition(
					testApp, "invalid-source", SharingScopePrivate, nil, "invalid-*", "calculated_output", "source_output",
				))
			},
		},
		{
			name: "mismatched binary field identity",
			targetDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyExtractionDefinition(
					testApp, "invalid-target", SharingScopePrivate, nil, "invalid-*", "OtherField",
				))
			},
			sourceDefinition: func() *fixtureVersion {
				return semanticFixtureVersion(dependencyAliasDefinition(
					testApp, "invalid-source", SharingScopePrivate, nil, "invalid-*", "otherfield", "source_output",
				))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			target := test.targetDefinition()
			target.timestamp = 10
			insertFixtureObject(t, database, fixtureObject{id: "ko-invalid-target", owner: testOwner, versions: []fixtureVersion{*target}})
			source := test.sourceDefinition()
			source.timestamp = 11
			source.dependencies = []fixtureDependency{{targetObjectID: "ko-invalid-target", targetVersion: 1}}
			insertFixtureObject(t, database, fixtureObject{id: "ko-invalid-source", owner: testOwner, versions: []fixtureVersion{*source}})
			integrationAssertDependencyReadPolicy(t, store, "ko-invalid-source", true)
		})
	}
}

func semanticFixtureVersion(definition *opensplunkv1.KnowledgeObjectDefinition) *fixtureVersion {
	return &fixtureVersion{definition: definition, state: StateActive, mutation: "create"}
}

func TestIntegrationDependencyGraphRejectsTwoNodeCycleBeforeDefinitionDecode(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-cycle-a", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyAliasDefinition(testApp, "cycle-a", SharingScopePrivate, nil, "cycle-*", "field_b", "field_a"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-cycle-b", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyAliasDefinition(testApp, "cycle-b", SharingScopePrivate, nil, "cycle-*", "field_a", "field_b"),
		state:      StateActive, mutation: "create", timestamp: 11,
	}}})
	rewriteSemanticDependencySet(t, database, "ko-cycle-a", "ko-cycle-b")
	rewriteSemanticDependencySet(t, database, "ko-cycle-b", "ko-cycle-a")
	cycleVersion, found, readErr := readVersionRecord(database.GORMDB(), testTenant, "ko-cycle-a", 1)
	if readErr != nil || !found {
		t.Fatalf("read cycle fixture version: found=%t err=%v", found, readErr)
	}
	cycleRecords, readErr := readDependencyRecords(database.GORMDB(), cycleVersion)
	if readErr != nil {
		t.Fatalf("read cycle fixture dependencies: %v", readErr)
	}
	if err := validateDependencyRecords(cycleVersion, cycleRecords); err != nil {
		t.Fatalf("cycle fixture structural rows = %#v: %v", cycleRecords, err)
	}
	_, err := store.Get(context.Background(), testReadScope(), "ko-cycle-a", nil)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Get(two-node dependency cycle) error = %v, want cycle ErrCorrupt", err)
	}
}

func TestIntegrationDependencyGraphRejectsDepthSeventeen(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-depth-00", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyExtractionDefinition(testApp, "depth-00", SharingScopePrivate, nil, "depth-*", "depth_00"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	for index := 1; index <= 17; index++ {
		objectID := fmt.Sprintf("ko-depth-%02d", index)
		targetID := fmt.Sprintf("ko-depth-%02d", index-1)
		definition := dependencyCalculatedDefinition(
			testApp,
			fmt.Sprintf("depth-%02d", index),
			SharingScopePrivate,
			nil,
			"depth-*",
			fmt.Sprintf("lower(depth_%02d)", index-1),
			fmt.Sprintf("depth_%02d", index),
		)
		if index == 1 {
			definition = dependencyAliasDefinition(
				testApp, "depth-01", SharingScopePrivate, nil, "depth-*", "depth_00", "depth_01",
			)
		}
		insertFixtureObject(t, database, fixtureObject{id: objectID, owner: testOwner, versions: []fixtureVersion{{
			definition: definition, state: StateActive, mutation: "create", timestamp: int64(10 + index),
			dependencies: []fixtureDependency{{targetObjectID: targetID, targetVersion: 1}},
		}}})
	}
	_, err := store.Get(context.Background(), testReadScope(), "ko-depth-17", nil)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("Get(depth 17 dependency graph) error = %v, want depth ErrCorrupt", err)
	}
}

func TestIntegrationDependencyGraphRejectsNodeTwoHundredFiftySeven(t *testing.T) {
	database, store := newCatalogTestStore(t)
	dependencies := make([]fixtureDependency, maximumDependencyGraphNodes)
	fields := make([]string, maximumDependencyGraphNodes)
	for index := 0; index < maximumDependencyGraphNodes; index++ {
		objectID := fmt.Sprintf("ko-node-target-%03d", index)
		field := fmt.Sprintf("node_%03d", index)
		insertFixtureObject(t, database, fixtureObject{id: objectID, owner: testOwner, versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, fmt.Sprintf("node-target-%03d", index), SharingScopePrivate, nil, "nodes-*", field,
			),
			state: StateActive, mutation: "create", timestamp: int64(1000 + index),
		}}})
		dependencies[index] = fixtureDependency{targetObjectID: objectID, targetVersion: 1}
		fields[index] = field
	}
	groups := make([]string, 0, 8)
	for start := 0; start < len(fields); start += 32 {
		groups = append(groups, "coalesce("+strings.Join(fields[start:start+32], ",")+")")
	}
	expression := "coalesce(" + strings.Join(groups, ",") + ")"
	insertFixtureObject(t, database, fixtureObject{id: "ko-node-root", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyCalculatedDefinition(
			testApp, "node-root", SharingScopePrivate, nil, "nodes-*", expression, "node_result",
		),
		state: StateActive, mutation: "create", timestamp: 2000, dependencies: dependencies,
	}}})
	_, err := store.Get(context.Background(), testReadScope(), "ko-node-root", nil)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "nodes") {
		t.Fatalf("Get(257-node dependency graph) error = %v, want node-bound ErrCorrupt", err)
	}
}

func TestIntegrationDependencyGraphRejectsEdgeOneThousandTwentyFive(t *testing.T) {
	database, store := newCatalogTestStore(t)
	const leafCount = 16
	const middleCount = 64
	leaves := make([]fixtureDependency, leafCount)
	for index := 0; index < leafCount; index++ {
		objectID := fmt.Sprintf("ko-edge-leaf-%02d", index)
		insertFixtureObject(t, database, fixtureObject{id: objectID, owner: testOwner, versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, fmt.Sprintf("edge-leaf-%02d", index), SharingScopePrivate, nil,
				"edges-*", fmt.Sprintf("edge_leaf_%02d", index),
			),
			state: StateActive, mutation: "create", timestamp: int64(3000 + index),
		}}})
		leaves[index] = fixtureDependency{targetObjectID: objectID, targetVersion: 1}
	}
	middles := make([]fixtureDependency, middleCount)
	middleFields := make([]string, middleCount)
	for index := 0; index < middleCount; index++ {
		objectID := fmt.Sprintf("ko-edge-middle-%02d", index)
		output := fmt.Sprintf("edge_middle_%02d", index)
		insertFixtureObject(t, database, fixtureObject{id: objectID, owner: testOwner, versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp, fmt.Sprintf("edge-middle-%02d", index), SharingScopePrivate, nil,
				"edges-*", "edge_leaf_00", output,
			),
			state: StateActive, mutation: "create", timestamp: int64(3100 + index), dependencies: leaves,
		}}})
		middles[index] = fixtureDependency{targetObjectID: objectID, targetVersion: 1}
		middleFields[index] = output
	}
	groups := []string{
		"coalesce(" + strings.Join(middleFields[:32], ",") + ")",
		"coalesce(" + strings.Join(middleFields[32:], ",") + ")",
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-edge-root", owner: testOwner, versions: []fixtureVersion{{
		definition: dependencyCalculatedDefinition(
			testApp, "edge-root", SharingScopePrivate, nil, "edges-*",
			"coalesce("+strings.Join(groups, ",")+")", "edge_result",
		),
		state: StateActive, mutation: "create", timestamp: 4000, dependencies: middles,
	}}})
	_, err := store.Get(context.Background(), testReadScope(), "ko-edge-root", nil)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "edges") {
		t.Fatalf("Get(1,088-edge dependency graph) error = %v, want edge-bound ErrCorrupt", err)
	}
}

func TestIntegrationHistoricalDependencyFieldIdentityIsVersionPinned(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-history-semantic-target", owner: testOwner, versions: []fixtureVersion{
		{
			definition: dependencyExtractionDefinition(testApp, "history-semantic-target", SharingScopePrivate, nil, "history-*", "wrong_v1"),
			state:      StateActive, mutation: "create", timestamp: 10,
		},
		{
			definition: dependencyExtractionDefinition(testApp, "history-semantic-target", SharingScopePrivate, nil, "history-*", "wanted"),
			state:      StateActive, mutation: "update", timestamp: 11,
		},
	}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-history-semantic-source", owner: testOwner, versions: []fixtureVersion{
		{
			definition: dependencyAliasDefinition(
				testApp, "history-semantic-source", SharingScopePrivate, stringPointer("history source v1"),
				"history-*", "wanted", "history_alias",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{targetObjectID: "ko-history-semantic-target", targetVersion: 1}},
		},
		{
			definition: dependencyAliasDefinition(
				testApp, "history-semantic-source", SharingScopePrivate, stringPointer("history source v2"),
				"history-*", "wanted", "history_alias",
			),
			state: StateActive, mutation: "update", timestamp: 21,
			dependencies: []fixtureDependency{{targetObjectID: "ko-history-semantic-target", targetVersion: 2}},
		},
	}})
	if _, err := store.Get(context.Background(), testReadScope(), "ko-history-semantic-source", nil); err != nil {
		t.Fatalf("Get(current version-pinned dependency): %v", err)
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{}); err != nil {
		t.Fatalf("List(current version-pinned dependency): %v", err)
	}
	versionOne := uint64(1)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-history-semantic-source", &versionOne); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(historical mismatched dependency field) error = %v, want ErrCorrupt", err)
	}
}

func rewriteSemanticDependencySet(t *testing.T, database *control.DB, sourceID, targetID string) {
	t.Helper()
	for _, trigger := range []string{
		"knowledge_object_version_update_is_forbidden",
		"knowledge_dependency_seal_delete_is_forbidden",
	} {
		dropTriggerIfPresent(t, database, trigger)
	}
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire dependency rewrite connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable dependency rewrite foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore dependency rewrite foreign keys: %v", err)
		}
	}()
	statements := []struct {
		query string
		args  []any
	}{
		{
			query: `UPDATE knowledge_object_versions SET dependency_count = 1
				WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
			args: []any{testTenant, sourceID},
		},
		{
			query: `DELETE FROM knowledge_object_dependency_seals
				WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
			args: []any{testTenant, sourceID},
		},
		{
			query: `INSERT INTO knowledge_object_dependencies (
				tenant_id, source_object_id, source_object_version, ordinal, target_kind,
				target_object_id, target_object_version, dependency_role
			) VALUES (?, ?, 1, 0, 'object', ?, 1, 'field_input')`,
			args: []any{testTenant, sourceID, targetID},
		},
		{
			query: `INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES (?, ?, 1, 1)`,
			args: []any{testTenant, sourceID},
		},
	}
	for _, statement := range statements {
		if _, err := connection.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("rewrite semantic dependency set: %v", err)
		}
	}
}

func dropTriggerIfPresent(t *testing.T, database *control.DB, name string) {
	t.Helper()
	// #nosec G202 -- name is supplied only by fixed test fixture identifiers.
	if _, err := database.SQLDB().ExecContext(t.Context(), `DROP TRIGGER IF EXISTS `+name); err != nil {
		t.Fatalf("drop trigger %s: %v", name, err)
	}
}
