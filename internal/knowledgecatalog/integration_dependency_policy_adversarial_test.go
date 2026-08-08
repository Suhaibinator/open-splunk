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

type integrationDependencyIdentity struct {
	appID string
	owner string
	scope SharingScope
}

func TestIntegrationActiveDependencySharingMatrixAcrossGetAndList(t *testing.T) {
	privateA := integrationDependencyIdentity{appID: testApp, owner: testOwner, scope: SharingScopePrivate}
	privateOtherOwner := integrationDependencyIdentity{appID: testApp, owner: "owner-b", scope: SharingScopePrivate}
	privateOtherApp := integrationDependencyIdentity{appID: testAppTwo, owner: testOwner, scope: SharingScopePrivate}
	appA := integrationDependencyIdentity{appID: testApp, owner: "owner-b", scope: SharingScopeApp}
	appOther := integrationDependencyIdentity{appID: testAppTwo, owner: "owner-b", scope: SharingScopeApp}
	globalA := integrationDependencyIdentity{appID: testApp, owner: "owner-b", scope: SharingScopeGlobal}
	globalOther := integrationDependencyIdentity{appID: testAppTwo, owner: "owner-b", scope: SharingScopeGlobal}

	tests := []struct {
		name        string
		source      integrationDependencyIdentity
		target      integrationDependencyIdentity
		wantCorrupt bool
	}{
		{name: "private to same-owner private", source: privateA, target: privateA},
		{name: "private to same-app app", source: privateA, target: appA},
		{name: "private to global in another app", source: privateA, target: globalOther},
		{name: "app to same-app app", source: appA, target: appA},
		{name: "app to global", source: appA, target: globalA},
		{name: "global to global in another app", source: globalA, target: globalOther},
		{name: "private to other-owner private", source: privateA, target: privateOtherOwner, wantCorrupt: true},
		{name: "private to other-app private", source: privateA, target: privateOtherApp, wantCorrupt: true},
		{name: "private to other-app app", source: privateA, target: appOther, wantCorrupt: true},
		{name: "app to private", source: appA, target: privateA, wantCorrupt: true},
		{name: "app to other-app app", source: appA, target: appOther, wantCorrupt: true},
		{name: "global to app", source: globalA, target: appA, wantCorrupt: true},
		{name: "global to private", source: globalA, target: privateA, wantCorrupt: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertIntegrationDependencyObject(
				t,
				database,
				"ko-policy-target",
				"policy-target",
				test.target,
				[]fixtureVersion{{
					definition: integrationDependencyDefinition(test.target, "policy-target", "target-v1"),
					state:      StateActive, mutation: "create", timestamp: 10,
				}},
			)
			insertIntegrationDependencyObject(
				t,
				database,
				"ko-policy-source",
				"policy-source",
				test.source,
				[]fixtureVersion{{
					definition: integrationDependencyDefinition(test.source, "policy-source", "source-v1"),
					state:      StateActive, mutation: "create", timestamp: 11,
					dependencies: []fixtureDependency{{
						targetObjectID: "ko-policy-target", targetVersion: 1,
					}},
				}},
			)
			integrationAssertDependencyReadPolicy(t, store, "ko-policy-source", test.wantCorrupt)
		})
	}
}

func TestIntegrationActiveDependencyPinnedAndCurrentTargetStateAcrossGetAndList(t *testing.T) {
	identity := integrationDependencyIdentity{appID: testApp, owner: testOwner, scope: SharingScopePrivate}
	tests := []struct {
		name           string
		targetVersions []fixtureVersion
		pinnedVersion  int64
		wantCorrupt    bool
	}{
		{
			name: "current active vNext retains active pin",
			targetVersions: []fixtureVersion{
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v1"), state: StateActive, mutation: "create", timestamp: 10},
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v2"), state: StateActive, mutation: "update", timestamp: 11},
			},
			pinnedVersion: 1,
		},
		{
			name: "pinned disabled while current active",
			targetVersions: []fixtureVersion{
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v1"), state: StateActive, mutation: "create", timestamp: 10},
				{definition: integrationDependencyDefinition(identity, "state-target", "disabled-v2"), state: StateDisabled, mutation: "disable", timestamp: 11},
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v3"), state: StateActive, mutation: "enable", timestamp: 12},
			},
			pinnedVersion: 2,
			wantCorrupt:   true,
		},
		{
			name: "pinned draft while current active",
			targetVersions: []fixtureVersion{
				{definition: integrationDependencyDefinition(identity, "state-target", "draft-v1"), state: StateDraft, mutation: "create", timestamp: 10},
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v2"), state: StateActive, mutation: "enable", timestamp: 11},
			},
			pinnedVersion: 1,
			wantCorrupt:   true,
		},
		{
			name: "current disabled invalidates active source",
			targetVersions: []fixtureVersion{
				{definition: integrationDependencyDefinition(identity, "state-target", "active-v1"), state: StateActive, mutation: "create", timestamp: 10},
				{definition: integrationDependencyDefinition(identity, "state-target", "disabled-v2"), state: StateDisabled, mutation: "disable", timestamp: 11},
			},
			pinnedVersion: 1,
			wantCorrupt:   true,
		},
		{
			name: "current draft invalidates active source",
			targetVersions: []fixtureVersion{
				{definition: integrationDependencyDefinition(identity, "state-target", "draft-v1"), state: StateDraft, mutation: "create", timestamp: 10},
			},
			pinnedVersion: 1,
			wantCorrupt:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertIntegrationDependencyObject(
				t,
				database,
				"ko-state-target",
				"state-target",
				identity,
				test.targetVersions,
			)
			insertIntegrationDependencyObject(
				t,
				database,
				"ko-state-source",
				"state-source",
				identity,
				[]fixtureVersion{{
					definition: integrationDependencyDefinition(identity, "state-source", "source-v1"),
					state:      StateActive, mutation: "create", timestamp: 20,
					dependencies: []fixtureDependency{{
						targetObjectID: "ko-state-target", targetVersion: test.pinnedVersion,
					}},
				}},
			)
			integrationAssertDependencyReadPolicy(t, store, "ko-state-source", test.wantCorrupt)
		})
	}
}

func TestIntegrationRecognizedInactiveSourcesSkipExecutableScopeAndStateSemantics(t *testing.T) {
	database, store := newCatalogTestStore(t)
	targetIdentity := integrationDependencyIdentity{appID: testAppTwo, owner: "owner-b", scope: SharingScopePrivate}
	insertIntegrationDependencyObject(
		t,
		database,
		"ko-inactive-target",
		"inactive-target",
		targetIdentity,
		[]fixtureVersion{
			{definition: integrationDependencyDefinition(targetIdentity, "inactive-target", "active-v1"), state: StateActive, mutation: "create", timestamp: 10},
			{definition: integrationDependencyDefinition(targetIdentity, "inactive-target", "disabled-v2"), state: StateDisabled, mutation: "disable", timestamp: 11},
		},
	)
	sourceIdentity := integrationDependencyIdentity{appID: testApp, owner: testOwner, scope: SharingScopeGlobal}
	fixtures := []struct {
		id       string
		name     string
		versions []fixtureVersion
		state    State
	}{
		{
			id: "ko-draft-source", name: "draft-source", state: StateDraft,
			versions: []fixtureVersion{{
				definition: integrationDependencyDefinition(sourceIdentity, "draft-source", "draft-v1"),
				state:      StateDraft, mutation: "create", timestamp: 20,
				dependencies: []fixtureDependency{{targetObjectID: "ko-inactive-target", targetVersion: 2}},
			}},
		},
		{
			id: "ko-disabled-source", name: "disabled-source", state: StateDisabled,
			versions: []fixtureVersion{
				{definition: integrationDependencyDefinition(sourceIdentity, "disabled-source", "active-v1"), state: StateActive, mutation: "create", timestamp: 21},
				{definition: integrationDependencyDefinition(sourceIdentity, "disabled-source", "active-v1"), state: StateDisabled, mutation: "disable", timestamp: 22},
				{
					definition: integrationDependencyDefinition(sourceIdentity, "disabled-source", "disabled-update-v3"),
					state:      StateDisabled, mutation: "update", timestamp: 23,
					dependencies: []fixtureDependency{{targetObjectID: "ko-inactive-target", targetVersion: 2}},
				},
			},
		},
		{
			id: "ko-deleted-source", name: "deleted-source", state: StateDeleted,
			versions: []fixtureVersion{
				{
					definition: integrationDependencyDefinition(sourceIdentity, "deleted-source", "draft-v1"),
					state:      StateDraft, mutation: "create", timestamp: 24,
					dependencies: []fixtureDependency{{targetObjectID: "ko-inactive-target", targetVersion: 2}},
				},
				{
					definition: integrationDependencyDefinition(sourceIdentity, "deleted-source", "draft-v1"),
					state:      StateDeleted, mutation: "delete", timestamp: 25,
					dependencies: []fixtureDependency{{targetObjectID: "ko-inactive-target", targetVersion: 2}},
				},
			},
		},
	}
	for _, fixture := range fixtures {
		insertIntegrationDependencyObject(t, database, fixture.id, fixture.name, sourceIdentity, fixture.versions)
	}
	// The forbidden inactive target body is corrupt on purpose. An administrative
	// read of an inactive source validates only the sealed structural graph, so it
	// must neither apply executable scope/state rules nor open the target body.
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ? AND definition_digest = (
			SELECT definition_digest FROM knowledge_objects
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-inactive-target'
		)`, testTenant, testTenant)
	for _, fixture := range fixtures {
		object, err := store.Get(context.Background(), testReadScope(), fixture.id, nil)
		if err != nil || object.State != fixture.state {
			t.Fatalf("Get(%s inactive forbidden dependency) = %#v, %v", fixture.state, object, err)
		}
	}
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 10}); err != nil {
		t.Fatalf("List(inactive forbidden dependencies): %v", err)
	}
}

func TestIntegrationHistoricalActiveSourceRemainsReadableAfterOrderlyDisableCascade(t *testing.T) {
	database, store := newCatalogTestStore(t)
	identity := integrationDependencyIdentity{appID: testApp, owner: testOwner, scope: SharingScopePrivate}
	targetActive := integrationDependencyDefinition(identity, "history-target", "active-v1")
	insertIntegrationDependencyObject(
		t,
		database,
		"ko-history-target",
		"history-target",
		identity,
		[]fixtureVersion{{
			definition: targetActive,
			state:      StateActive, mutation: "create", timestamp: 10,
		}},
	)
	sourceActive := integrationDependencyDefinition(identity, "history-source", "active-v1")
	insertIntegrationDependencyObject(
		t,
		database,
		"ko-history-source",
		"history-source",
		identity,
		[]fixtureVersion{{
			definition: sourceActive,
			state:      StateActive, mutation: "create", timestamp: 11,
			dependencies: []fixtureDependency{{targetObjectID: "ko-history-target", targetVersion: 1}},
		}},
	)

	sourceTransaction, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-history-source",
		sourceActive,
		StateDisabled,
		"disable",
		20,
	)
	if err := sourceTransaction.Commit(); err != nil {
		t.Fatalf("commit source disable: %v", err)
	}
	targetTransaction, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-history-target",
		targetActive,
		StateDisabled,
		"disable",
		21,
	)
	if err := targetTransaction.Commit(); err != nil {
		t.Fatalf("commit target disable after dependent: %v", err)
	}

	current, err := store.Get(context.Background(), testReadScope(), "ko-history-source", nil)
	if err != nil || current.Version != 2 || current.State != StateDisabled {
		t.Fatalf("Get(current disabled source) = %#v, %v", current, err)
	}
	versionOne := uint64(1)
	historical, err := store.Get(context.Background(), testReadScope(), "ko-history-source", &versionOne)
	if err != nil || historical.Version != 1 || historical.State != StateActive {
		t.Fatalf("Get(historical active source after target disable) = %#v, %v", historical, err)
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil || !integrationPageContainsObject(page, "ko-history-source") ||
		!integrationPageContainsObject(page, "ko-history-target") {
		t.Fatalf("List(orderly disabled catalog) = %#v, %v", page, err)
	}
}

func insertIntegrationDependencyObject(
	t *testing.T,
	database *control.DB,
	objectID string,
	name string,
	identity integrationDependencyIdentity,
	versions []fixtureVersion,
) {
	t.Helper()
	if len(versions) == 0 {
		t.Fatal("dependency fixture requires a version")
	}
	for index := range versions {
		if versions[index].definition.GetName() != name || versions[index].definition.GetAppId() != identity.appID {
			t.Fatalf("dependency fixture identity disagrees at version %d", index+1)
		}
	}
	insertFixtureObject(t, database, fixtureObject{id: objectID, owner: identity.owner, versions: versions})
}

func integrationDependencyDefinition(
	identity integrationDependencyIdentity,
	name string,
	marker string,
) *opensplunkv1.KnowledgeObjectDefinition {
	description := fmt.Sprintf("%s dependency policy definition", marker)
	if strings.Contains(name, "target") {
		return dependencyExtractionDefinition(
			identity.appID,
			name,
			identity.scope,
			&description,
			marker+"-*",
			dependencyFixtureInputField,
		)
	}
	return dependencyAliasDefinition(
		identity.appID,
		name,
		identity.scope,
		&description,
		marker+"-*",
		dependencyFixtureInputField,
		"dependency_alias",
	)
}

func integrationAssertDependencyReadPolicy(t *testing.T, store *Store, sourceID string, wantCorrupt bool) {
	t.Helper()
	object, getErr := store.Get(context.Background(), testReadScope(), sourceID, nil)
	page, listErr := store.List(context.Background(), testReadScope(), ListRequest{})
	if wantCorrupt {
		if !errors.Is(getErr, ErrCorrupt) {
			t.Errorf("Get(invalid dependency policy) = %#v, %v, want ErrCorrupt", object, getErr)
		}
		if !errors.Is(listErr, ErrCorrupt) {
			t.Errorf("List(invalid dependency policy) = %#v, %v, want ErrCorrupt", page, listErr)
		}
		return
	}
	if getErr != nil || object.KnowledgeObjectID != sourceID {
		t.Errorf("Get(valid dependency policy) = %#v, %v", object, getErr)
	}
	if listErr != nil || !integrationPageContainsObject(page, sourceID) {
		t.Errorf("List(valid dependency policy) = %#v, %v", page, listErr)
	}
}

func integrationPageContainsObject(page ListPage, objectID string) bool {
	for _, object := range page.Objects {
		if object.KnowledgeObjectID == objectID {
			return true
		}
	}
	return false
}
