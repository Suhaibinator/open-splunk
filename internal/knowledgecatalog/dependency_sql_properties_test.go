package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestActiveDependencySQLScopeStateAndHistoricalMatrix(t *testing.T) {
	type matrixCase struct {
		name             string
		sourceScope      SharingScope
		sourceApp        string
		sourceOwner      string
		sourceStates     []State
		sourceTargetRefs []int64
		targetScope      SharingScope
		targetApp        string
		targetOwner      string
		targetStates     []State
		requestedVersion uint64
		wantCorrupt      bool
		currentCorrupt   bool
	}
	active := []State{StateActive}
	cases := []matrixCase{
		{
			name: "private to same-owner same-app private", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner, targetStates: active,
		},
		{
			name: "private to other-owner private is forbidden", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: "owner-b", targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "private to other-app private is forbidden", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testAppTwo, targetOwner: testOwner, targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "private to same-app app", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeApp, targetApp: testApp, targetOwner: "owner-b", targetStates: active,
		},
		{
			name: "private to other-app app is forbidden", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeApp, targetApp: testAppTwo, targetOwner: "owner-b", targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "private to global from any app", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeGlobal, targetApp: testAppTwo, targetOwner: "owner-b", targetStates: active,
		},
		{
			name: "app to private is forbidden", sourceScope: SharingScopeApp,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner, targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "app to same-app app", sourceScope: SharingScopeApp,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeApp, targetApp: testApp, targetOwner: "owner-b", targetStates: active,
		},
		{
			name: "app to other-app app is forbidden", sourceScope: SharingScopeApp,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeApp, targetApp: testAppTwo, targetOwner: "owner-b", targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "app to global", sourceScope: SharingScopeApp,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeGlobal, targetApp: testAppTwo, targetOwner: "owner-b", targetStates: active,
		},
		{
			name: "global to private is forbidden", sourceScope: SharingScopeGlobal,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner, targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "global to app is forbidden", sourceScope: SharingScopeGlobal,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeApp, targetApp: testApp, targetOwner: testOwner, targetStates: active,
			wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "global to global", sourceScope: SharingScopeGlobal,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopeGlobal, targetApp: testAppTwo, targetOwner: "owner-b", targetStates: active,
		},
		{
			name: "active source to disabled pinned version is forbidden", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{2},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner,
			targetStates: []State{StateActive, StateDisabled}, wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "current active source requires current active target", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: active, sourceTargetRefs: []int64{1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner,
			targetStates: []State{StateActive, StateDisabled}, wantCorrupt: true, currentCorrupt: true,
		},
		{
			name: "draft source may retain forbidden inactive target", sourceScope: SharingScopeGlobal,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateDraft}, sourceTargetRefs: []int64{2},
			targetScope: SharingScopePrivate, targetApp: testAppTwo, targetOwner: "owner-b",
			targetStates: []State{StateActive, StateDisabled},
		},
		{
			name: "disabled source may retain forbidden inactive target", sourceScope: SharingScopeGlobal,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateActive, StateDisabled, StateDisabled}, sourceTargetRefs: []int64{0, 0, 2},
			targetScope: SharingScopePrivate, targetApp: testAppTwo, targetOwner: "owner-b",
			targetStates: []State{StateActive, StateDisabled},
		},
		{
			name: "deleted source may retain forbidden deleted target", sourceScope: SharingScopeApp,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateDraft, StateDeleted}, sourceTargetRefs: []int64{2, 2},
			targetScope: SharingScopePrivate, targetApp: testAppTwo, targetOwner: "owner-b",
			targetStates: []State{StateActive, StateDeleted},
		},
		{
			name: "historical active source ignores later target disable", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateActive, StateDisabled}, sourceTargetRefs: []int64{1, 1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner,
			targetStates: []State{StateActive, StateDisabled}, requestedVersion: 1,
		},
		{
			name: "historical active source still requires pinned active target", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateActive, StateDisabled}, sourceTargetRefs: []int64{1, 1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: testOwner,
			targetStates: []State{StateDraft, StateActive}, requestedVersion: 1, wantCorrupt: true,
		},
		{
			name: "historical active source still requires pinned identity matrix", sourceScope: SharingScopePrivate,
			sourceApp: testApp, sourceOwner: testOwner, sourceStates: []State{StateActive, StateDisabled}, sourceTargetRefs: []int64{1, 1},
			targetScope: SharingScopePrivate, targetApp: testApp, targetOwner: "owner-b",
			targetStates: active, requestedVersion: 1, wantCorrupt: true, currentCorrupt: true,
		},
	}

	cleanDatabase, cleanStore := newCatalogTestStore(t)
	corruptDatabase, corruptStore := newCatalogTestStore(t)
	cleanCount, corruptCount := 0, 0
	for index := range cases {
		test := &cases[index]
		if test.sourceApp == "" {
			test.sourceApp = testApp
		}
		if test.sourceOwner == "" {
			test.sourceOwner = testOwner
		}
		if test.targetApp == "" {
			test.targetApp = testApp
		}
		if test.targetOwner == "" {
			test.targetOwner = testOwner
		}
		var database *control.DB
		var store *Store
		if test.currentCorrupt {
			database, store = corruptDatabase, corruptStore
			corruptCount++
		} else {
			database, store = cleanDatabase, cleanStore
			cleanCount++
		}
		targetID := fmt.Sprintf("ko-dependency-target-%02d", index)
		sourceID := fmt.Sprintf("ko-dependency-source-%02d", index)
		insertFixtureObject(t, database, fixtureObject{
			id: targetID, owner: test.targetOwner,
			versions: dependencyPropertyVersions(
				fmt.Sprintf("dependency_target_%02d", index), test.targetApp, test.targetScope,
				test.targetStates, nil, int64(1000+index*20),
			),
		})
		refs := make([]fixtureDependency, len(test.sourceTargetRefs))
		for versionIndex, targetVersion := range test.sourceTargetRefs {
			if targetVersion != 0 {
				refs[versionIndex] = fixtureDependency{targetObjectID: targetID, targetVersion: targetVersion}
			}
		}
		insertFixtureObject(t, database, fixtureObject{
			id: sourceID, owner: test.sourceOwner,
			versions: dependencyPropertyVersions(
				fmt.Sprintf("dependency_source_%02d", index), test.sourceApp, test.sourceScope,
				test.sourceStates, refs, int64(2000+index*20),
			),
		})

		var requested *uint64
		if test.requestedVersion != 0 {
			value := test.requestedVersion
			requested = &value
		}
		_, err := store.Get(context.Background(), testReadScope(), sourceID, requested)
		if test.wantCorrupt {
			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("%s: Get error = %v, want ErrCorrupt", test.name, err)
			}
		} else if err != nil {
			t.Errorf("%s: Get error = %v, want success", test.name, err)
		}
	}
	if cleanCount == 0 || corruptCount == 0 {
		t.Fatalf("matrix partition = clean:%d corrupt:%d", cleanCount, corruptCount)
	}
	if _, err := cleanStore.List(context.Background(), testReadScope(), ListRequest{}); err != nil {
		t.Fatalf("List(clean dependency matrix): %v", err)
	}
	if _, err := corruptStore.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("List(corrupt dependency matrix) error = %v, want ErrCorrupt", err)
	}
}

func dependencyPropertyVersions(
	name, appID string,
	scope SharingScope,
	states []State,
	targets []fixtureDependency,
	baseTimestamp int64,
) []fixtureVersion {
	versions := make([]fixtureVersion, len(states))
	for index, state := range states {
		mutation := "create"
		if index > 0 {
			switch state {
			case states[index-1]:
				mutation = "update"
			case StateActive:
				mutation = "enable"
			case StateDisabled:
				mutation = "disable"
			case StateDeleted:
				mutation = "delete"
			case StateQuarantined:
				mutation = "quarantine"
			default:
				mutation = "update"
			}
		}
		var dependencies []fixtureDependency
		if index < len(targets) && targets[index].targetVersion != 0 {
			dependencies = []fixtureDependency{targets[index]}
		}
		version := fixtureVersion{
			state: state, mutation: mutation, timestamp: baseTimestamp + int64(index), dependencies: dependencies,
		}
		if state != StateQuarantined {
			hostPattern := fmt.Sprintf("%s-v%d-*", name, index+1)
			if strings.Contains(name, "target") {
				// Keep the dependency target universal so each matrix case
				// isolates sharing/state/version validity. Sources retain their
				// case-specific selectors and therefore imply this target.
				hostPattern = ""
				version.definition = dependencyExtractionDefinition(
					appID, name, scope, nil, hostPattern, dependencyFixtureInputField,
				)
			} else {
				version.definition = dependencyAliasDefinition(
					appID, name, scope, nil, hostPattern,
					dependencyFixtureInputField, "dependency_alias",
				)
			}
		} else {
			reason := "root_corruption"
			version.reason = &reason
		}
		versions[index] = version
	}
	return versions
}
