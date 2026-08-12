package knowledgecatalog

import (
	"context"
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

type writerRecognizedActiveRemovalHarness struct {
	database     *control.DB
	writer       *Writer
	actorContext context.Context
	scope        WriteScope
	targetID     string
	candidateID  string
}

func newWriterRecognizedActiveRemovalHarness(
	t *testing.T,
	suffix string,
) writerRecognizedActiveRemovalHarness {
	t.Helper()
	database, _ := newCatalogTestStore(t)
	targetID := "ko-recognized-active-target-" + suffix
	candidateID := "ko-recognized-active-removal-" + suffix
	insertFixtureObject(t, database, fixtureObject{
		id: targetID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"recognized-active-target-"+suffix,
				SharingScopePrivate,
				nil,
				"recognized-active-*",
				dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: candidateID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp,
				"recognized-active-removal-"+suffix,
				SharingScopePrivate,
				nil,
				"recognized-active-*",
				dependencyFixtureInputField,
				"recognized_active_alias",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{
				targetObjectID: targetID,
				targetVersion:  1,
			}},
		}},
	})
	createPublicationTransitionTestIndex(t, database, "main")
	writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
	return writerRecognizedActiveRemovalHarness{
		database:     database,
		writer:       writer,
		actorContext: actorContext,
		scope:        scope,
		targetID:     targetID,
		candidateID:  candidateID,
	}
}

func TestWriterRecognizedActiveRemovalRoutesUseTransitionAuthority(t *testing.T) {
	tests := []struct {
		name   string
		after  State
		mutate func(*Writer, context.Context, WriteScope, string) (proto.Message, error)
	}{
		{
			name:  "disable",
			after: StateDisabled,
			mutate: func(
				writer *Writer,
				ctx context.Context,
				scope WriteScope,
				objectID string,
			) (proto.Message, error) {
				return writer.SetState(ctx, scope, &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: objectID,
					ExpectedVersion:   1,
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "recognized-active-disable-0001",
				})
			},
		},
		{
			name:  "delete",
			after: StateDeleted,
			mutate: func(
				writer *Writer,
				ctx context.Context,
				scope WriteScope,
				objectID string,
			) (proto.Message, error) {
				return writer.Delete(ctx, scope, &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: objectID,
					ExpectedVersion:   1,
					ClientRequestId:   "recognized-active-delete-00001",
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterRecognizedActiveRemovalHarness(t, test.name)
			committed, err := test.mutate(
				harness.writer,
				harness.actorContext,
				harness.scope,
				harness.candidateID,
			)
			if err != nil {
				t.Fatalf("%s recognized ACTIVE object: %v", test.name, err)
			}
			replayed, err := test.mutate(
				harness.writer,
				harness.actorContext,
				harness.scope,
				harness.candidateID,
			)
			if err != nil || !proto.Equal(replayed, committed) {
				t.Fatalf("%s recognized ACTIVE replay = (%v, %v), want %v", test.name, replayed, err, committed)
			}

			version, found, err := readVersionRecord(
				harness.database.GORMDB(),
				testTenant,
				harness.candidateID,
				2,
			)
			if err != nil || !found || version.State != test.after ||
				version.DependencyCount != 1 {
				t.Fatalf("%s recognized ACTIVE version = (%#v, %t, %v)", test.name, version, found, err)
			}
			dependencies, err := readValidatedVersionDependencies(harness.database.GORMDB(), version)
			if err != nil || len(dependencies) != 1 ||
				dependencies[0].TargetObjectID != harness.targetID ||
				dependencies[0].TargetObjectVersion != 1 {
				t.Fatalf("%s retained dependencies = (%#v, %v)", test.name, dependencies, err)
			}
		})
	}
}

func TestPublishMutationRejectsZeroAuthorityForRecognizedActiveRemoval(t *testing.T) {
	fixture := newWriterTransitionPersistenceFixture(t)
	fixture.plan.activeTransition = publicationTransitionPersistenceAuthority{}
	fixture.trace.reset()
	var hooks int
	fixture.writer.hook = func(context.Context, writerHookEvent) error {
		hooks++
		return nil
	}
	if _, _, _, err := fixture.writer.publishMutation(
		fixture.ctx,
		fixture.tx,
		fixture.prepared,
		fixture.plan,
		true,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("zero-authority recognized ACTIVE removal error = %v, want invalid argument", err)
	}
	assertWriterTransitionRejectedBeforePersistence(t, fixture, hooks)
}

func TestWriterRecognizedActiveRemovalRequiresCompleteAppInventory(t *testing.T) {
	harness := newWriterRecognizedActiveRemovalHarness(t, "missing-app-inventory")
	dropTrigger(t, harness.database, "app_catalog_revision_delete_is_forbidden")
	mustExec(
		t,
		harness.database,
		"DELETE FROM app_catalog_revisions WHERE tenant_id = ?",
		testTenant,
	)
	stable := readWriterForwardCompatAuthoritySnapshot(t, harness.database)
	_, err := harness.writer.SetState(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: harness.candidateID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   "recognized-missing-app-inventory-0001",
		},
	)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("recognized ACTIVE removal with missing app inventory error = %v, want corrupt", err)
	}
	assertWriterForwardCompatAuthorityUnchanged(
		t,
		harness.database,
		stable,
		"rejected recognized ACTIVE removal with missing app inventory",
	)
}

func TestWriterActiveDependentConflictPrecedesMalformedTransitionInventory(t *testing.T) {
	harness := newWriterRecognizedActiveRemovalHarness(t, "dependent-precedence")
	dropTrigger(t, harness.database, "app_catalog_revision_delete_is_forbidden")
	mustExec(
		t,
		harness.database,
		"DELETE FROM app_catalog_revisions WHERE tenant_id = ?",
		testTenant,
	)
	stable := readWriterForwardCompatAuthoritySnapshot(t, harness.database)
	_, err := harness.writer.SetState(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: harness.targetID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   "recognized-dependent-precedence-0001",
		},
	)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("active-dependent precedence error = %v, want dependency conflict", err)
	}
	assertWriterForwardCompatAuthorityUnchanged(
		t,
		harness.database,
		stable,
		"active-dependent conflict before malformed transition inventory",
	)
}

func TestPublishMutationLiveOpaqueRemovalValidationRejectsForgedPlans(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*publicationPlan)
		want   error
	}{
		{
			name: "forged opaque flag",
			mutate: func(plan *publicationPlan) {
				plan.definition.opaque = true
			},
			want: control.ErrInvalidArgument,
		},
		{
			name: "swapped retained dependencies",
			mutate: func(plan *publicationPlan) {
				plan.definition.opaque = true
				plan.dependencies[0] = publicationDependency{
					targetObjectID: "ko-writer-transition-swapped-target",
					targetVersion:  1,
				}
			},
			want: control.ErrDependencyConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWriterTransitionPersistenceFixture(t)
			fixture.plan.activeTransition = publicationTransitionPersistenceAuthority{}
			test.mutate(&fixture.plan)
			fixture.trace.reset()
			var hooks int
			fixture.writer.hook = func(context.Context, writerHookEvent) error {
				hooks++
				return nil
			}
			if _, _, _, err := fixture.writer.publishMutation(
				fixture.ctx,
				fixture.tx,
				fixture.prepared,
				fixture.plan,
				true,
			); !errors.Is(err, test.want) {
				t.Fatalf("forged zero-proof removal error = %v, want %v", err, test.want)
			}
			assertWriterTransitionRejectedBeforePersistence(t, fixture, hooks)
		})
	}
}
