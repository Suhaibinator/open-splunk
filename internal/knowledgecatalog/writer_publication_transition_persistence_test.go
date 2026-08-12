package knowledgecatalog

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var errWriterTransitionPersistenceStop = errors.New("stop writer transition persistence test")

func TestPublishMutationZeroTransitionPreservesPersistenceHookOrder(t *testing.T) {
	harness := newWriterFaultHarness(t)
	tx := harness.database.GORMDB().Begin()
	if tx.Error != nil {
		t.Fatalf("begin zero-transition hook-order transaction: %v", tx.Error)
	}
	defer func() { _ = tx.Rollback().Error }()
	prepared, plan := prepareWriterPublicationBatchPlan(t, harness, tx, 0, 0)

	var got []writerHookBoundary
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		got = append(got, event.Boundary)
		return nil
	}
	if _, _, _, err := harness.writer.publishMutation(
		harness.actorContext,
		tx,
		prepared,
		plan,
		true,
	); err != nil {
		t.Fatalf("publish zero transition hook order: %v", err)
	}
	want := []writerHookBoundary{
		writerHookDefinitionBlobReady,
		writerHookVersionInserted,
		writerHookDependencyRowsInserted,
		writerHookDependencySealed,
		writerHookProjectionInserted,
		writerHookSelectorRowsInserted,
		writerHookProjectionSealed,
		writerHookRegistryPublished,
		writerHookSuccessAuditAppended,
		writerHookCatalogRevisionAdvanced,
		writerHookCommitAuthorityRecorded,
		writerHookIdempotencyOutcomeRecorded,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("zero-transition persistence hooks = %v, want %v", got, want)
	}
}

func TestPublishMutationDetachesPlanDependenciesBeforeFirstHook(t *testing.T) {
	harness := newWriterFaultHarness(t)
	tx := harness.database.GORMDB().Begin()
	if tx.Error != nil {
		t.Fatalf("begin aliased-dependency transaction: %v", tx.Error)
	}
	defer func() { _ = tx.Rollback().Error }()
	prepared, plan := prepareWriterPublicationBatchPlan(t, harness, tx, 2, 0)
	callerDependencies := plan.dependencies

	var got []writerHookBoundary
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		got = append(got, event.Boundary)
		if event.Boundary == writerHookDefinitionBlobReady {
			callerDependencies[0] = publicationDependency{
				targetObjectID: "caller-mutated-invalid-target",
				targetVersion:  0,
			}
			callerDependencies[1] = callerDependencies[0]
		}
		if event.Boundary == writerHookDependencySealed {
			return errWriterTransitionPersistenceStop
		}
		return nil
	}
	if _, _, _, err := harness.writer.publishMutation(
		harness.actorContext,
		tx,
		prepared,
		plan,
		true,
	); !errors.Is(err, errWriterTransitionPersistenceStop) {
		t.Fatalf("publish aliased dependencies error = %v, want stop", err)
	}
	wantHooks := []writerHookBoundary{
		writerHookDefinitionBlobReady,
		writerHookVersionInserted,
		writerHookDependencyRowsInserted,
		writerHookDependencySealed,
	}
	if !slices.Equal(got, wantHooks) {
		t.Fatalf("aliased-dependency hooks = %v, want %v", got, wantHooks)
	}

	var version versionRecord
	if err := tx.Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = 1",
		writerFaultTenant,
		writerPublicationBatchObjectID,
	).Take(&version).Error; err != nil {
		t.Fatalf("read aliased-dependency version: %v", err)
	}
	if version.DependencyCount != 2 {
		t.Fatalf("aliased-dependency version count = %d, want 2", version.DependencyCount)
	}
	var rows []persistedPublicationDependency
	if err := tx.Where(
		"tenant_id = ? AND source_object_id = ? AND source_object_version = 1",
		writerFaultTenant,
		writerPublicationBatchObjectID,
	).Order("ordinal ASC").Find(&rows).Error; err != nil {
		t.Fatalf("read aliased dependency rows: %v", err)
	}
	assertWriterPublicationDependencyRows(t, rows, 2)
	seal, err := readDependencySeal(
		tx,
		writerFaultTenant,
		writerPublicationBatchObjectID,
		1,
	)
	if err != nil || seal.DependencyCount != 2 {
		t.Fatalf("aliased-dependency seal = (%#v, %v), want count 2", seal, err)
	}
}

func TestPublishMutationTransitionAuthorityUsesLiveBeforeAndPrivateProjection(t *testing.T) {
	fixture := newWriterTransitionPersistenceFixture(t)
	fixture.trace.reset()
	if _, _, _, err := fixture.writer.publishMutation(
		fixture.ctx,
		fixture.tx,
		fixture.prepared,
		fixture.plan,
		true,
	); err != nil {
		t.Fatalf("publish transition-authorized removal: %v", err)
	}

	version, found, err := readVersionRecord(
		fixture.tx,
		testTenant,
		writerTransitionPersistenceCandidateID,
		2,
	)
	if err != nil || !found || version.State != StateDisabled || version.DependencyCount != 1 {
		t.Fatalf("transition-authorized version = (%#v, %t, %v)", version, found, err)
	}
	records, err := readValidatedVersionDependencies(fixture.tx, version)
	if err != nil || len(records) != 1 ||
		records[0].TargetObjectID != writerTransitionPersistenceTargetID ||
		records[0].TargetObjectVersion != 1 {
		t.Fatalf("transition-authorized dependencies = (%#v, %v)", records, err)
	}
}

func TestPublishMutationACTIVEUsesOnlyWrapperPrivateProjection(t *testing.T) {
	fixture := newWriterACTIVETransitionPersistenceFixture(t)
	if len(fixture.plan.dependencies) != 0 || fixture.plan.state != StateActive {
		t.Fatalf("ACTIVE transition plan unexpectedly carries dependencies: %#v", fixture.plan)
	}
	fixture.trace.reset()
	if _, _, _, err := fixture.writer.publishMutation(
		fixture.ctx,
		fixture.tx,
		fixture.prepared,
		fixture.plan,
		true,
	); err != nil {
		t.Fatalf("publish wrapper-derived ACTIVE transition: %v", err)
	}

	version, found, err := readVersionRecord(
		fixture.tx,
		testTenant,
		writerTransitionPersistenceCandidateID,
		3,
	)
	if err != nil || !found || version.State != StateActive || version.DependencyCount != 1 {
		t.Fatalf("wrapper-derived ACTIVE version = (%#v, %t, %v)", version, found, err)
	}
	records, err := readValidatedVersionDependencies(fixture.tx, version)
	if err != nil || len(records) != 1 ||
		records[0].TargetObjectID != writerTransitionPersistenceTargetID ||
		records[0].TargetObjectVersion != 1 {
		t.Fatalf("wrapper-derived ACTIVE dependencies = (%#v, %v)", records, err)
	}
}

func TestPublishMutationTransitionRejectionsPrecedePersistence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*writerTransitionPersistenceFixture) context.Context
		want   error
	}{
		{
			name: "current endpoint tamper",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				current := *fixture.plan.current
				current.Name += "-tampered"
				fixture.plan.current = &current
				return fixture.ctx
			},
			want: control.ErrDependencyConflict,
		},
		{
			name: "dependency tamper",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				fixture.plan.dependencies[0].targetVersion++
				return fixture.ctx
			},
			want: control.ErrDependencyConflict,
		},
		{
			name: "missing live current",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				const missingID = "ko-writer-transition-missing"
				current := *fixture.plan.current
				current.KnowledgeObjectID = missingID
				fixture.plan.current = &current
				fixture.plan.objectID = missingID
				return fixture.ctx
			},
			want: ErrCorrupt,
		},
		{
			name: "stale plan catalog",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				fixture.plan.oldCatalogState.revision++
				return fixture.ctx
			},
			want: control.ErrVersionConflict,
		},
		{
			name: "ACTIVE plan dependencies",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				fixture.plan.state = StateActive
				return fixture.ctx
			},
			want: control.ErrInvalidArgument,
		},
		{
			name: "canceled context",
			mutate: func(fixture *writerTransitionPersistenceFixture) context.Context {
				ctx, cancel := context.WithCancel(fixture.ctx)
				cancel()
				return ctx
			},
			want: context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWriterTransitionPersistenceFixture(t)
			ctx := test.mutate(fixture)
			fixture.trace.reset()
			var hooks int
			fixture.writer.hook = func(context.Context, writerHookEvent) error {
				hooks++
				return nil
			}
			if _, _, _, err := fixture.writer.publishMutation(
				ctx,
				fixture.tx,
				fixture.prepared,
				fixture.plan,
				true,
			); !errors.Is(err, test.want) {
				t.Fatalf("publish rejected transition error = %v, want %v", err, test.want)
			}
			assertWriterTransitionRejectedBeforePersistence(t, fixture, hooks)
		})
	}
}

func TestPublishMutationRejectsZeroAuthorityForACTIVEPlan(t *testing.T) {
	harness := newWriterFaultHarness(t)
	trace := &writerTransitionDMLTrace{}
	tx := harness.database.GORMDB().Session(&gorm.Session{Logger: trace}).Begin()
	if tx.Error != nil {
		t.Fatalf("begin zero-ACTIVE transaction: %v", tx.Error)
	}
	defer func() { _ = tx.Rollback().Error }()
	prepared, plan := prepareWriterPublicationBatchPlan(t, harness, tx, 0, 0)
	plan.state = StateActive
	trace.reset()
	var hooks int
	harness.writer.hook = func(context.Context, writerHookEvent) error {
		hooks++
		return nil
	}
	if _, _, _, err := harness.writer.publishMutation(
		harness.actorContext,
		tx,
		prepared,
		plan,
		true,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("zero-authority ACTIVE error = %v, want invalid argument", err)
	}
	if hooks != 0 || len(trace.snapshot()) != 0 {
		t.Fatalf("zero-authority ACTIVE reached persistence: hooks=%d dml=%v", hooks, trace.snapshot())
	}
}

func TestWriterActiveEnableRequiresCurrentIndexWinningWitness(t *testing.T) {
	harness := newWriterFaultHarness(t)
	created, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		writerFaultCreateRequest("transition-gate", "transition-gate-create-0001"),
	)
	if err != nil {
		t.Fatalf("create transition-gate draft: %v", err)
	}
	_, err = harness.writer.SetState(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
			ExpectedVersion:   created.GetKnowledgeObject().GetVersion(),
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId:   "transition-gate-enable-0001",
		},
	)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("ACTIVE enable witness error = %v, want dependency conflict", err)
	}
}

func TestPublishMutationWrongTransactionWinsBeforeMissingBindingRead(t *testing.T) {
	fixture := newWriterTransitionPersistenceFixture(t)
	const missingID = "ko-writer-transition-wrong-tx-missing"
	current := *fixture.plan.current
	current.KnowledgeObjectID = missingID
	fixture.plan.current = &current
	fixture.plan.objectID = missingID

	trace := &writerTransitionDMLTrace{}
	otherDatabase, _ := newCatalogTestStore(t)
	other := otherDatabase.GORMDB().Session(&gorm.Session{Logger: trace}).Begin()
	if other.Error != nil {
		t.Fatalf("begin wrong transition transaction: %v", other.Error)
	}
	defer func() { _ = other.Rollback().Error }()
	trace.reset()
	var hooks int
	fixture.writer.hook = func(context.Context, writerHookEvent) error {
		hooks++
		return nil
	}
	if _, _, _, err := fixture.writer.publishMutation(
		fixture.ctx,
		other,
		fixture.prepared,
		fixture.plan,
		true,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("wrong transaction error = %v, want version conflict", err)
	}
	if hooks != 0 || len(trace.snapshot()) != 0 {
		t.Fatalf("wrong transaction performed binding/persistence work: hooks=%d sql=%v", hooks, trace.snapshot())
	}
}

const (
	writerTransitionPersistenceTargetID    = "ko-writer-transition-target"
	writerTransitionPersistenceCandidateID = "ko-writer-transition-candidate"
)

type writerTransitionPersistenceFixture struct {
	database *control.DB
	writer   *Writer
	ctx      context.Context
	tx       *gorm.DB
	trace    *writerTransitionDMLTrace
	prepared preparedMutation
	plan     publicationPlan
}

func newWriterTransitionPersistenceFixture(t *testing.T) *writerTransitionPersistenceFixture {
	t.Helper()
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: writerTransitionPersistenceTargetID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"writer-transition-target",
				SharingScopePrivate,
				nil,
				"writer-transition-*",
				dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: writerTransitionPersistenceCandidateID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp,
				"writer-transition-candidate",
				SharingScopePrivate,
				nil,
				"writer-transition-*",
				dependencyFixtureInputField,
				"writer_transition_alias",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{
				targetObjectID: writerTransitionPersistenceTargetID,
				targetVersion:  1,
			}},
		}},
	})
	writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		t.Fatalf("normalize transition Writer scope: %v", err)
	}
	actor, found := audit.ActorFromContext(actorContext)
	if !found {
		t.Fatal("transition Writer actor is absent")
	}
	prepared, err := prepareSetStateMutation(
		normalizedScope,
		actor,
		&opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: writerTransitionPersistenceCandidateID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   "writer-transition-disable-0001",
		},
	)
	if err != nil {
		t.Fatalf("prepare transition Writer request: %v", err)
	}
	trace := &writerTransitionDMLTrace{}
	tx := database.GORMDB().Session(&gorm.Session{Logger: trace}).Begin()
	if tx.Error != nil {
		t.Fatalf("begin transition Writer transaction: %v", tx.Error)
	}
	t.Cleanup(func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back transition Writer transaction: %v", err)
		}
	})
	current, err := readAuthorizedMutationRegistry(
		tx,
		prepared.scope,
		writerTransitionPersistenceCandidateID,
	)
	if err != nil {
		t.Fatalf("read transition Writer current registry: %v", err)
	}
	_, currentVersion, definition, err := writer.hydrateAuthorizedCurrentStateOnly(tx, current)
	if err != nil {
		t.Fatalf("hydrate transition Writer current: %v", err)
	}
	dependencies, err := dependenciesFromCurrent(tx, currentVersion)
	if err != nil {
		t.Fatalf("read transition Writer current dependencies: %v", err)
	}

	baseRead, err := writer.reader.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{},
	)
	if err != nil {
		t.Fatalf("read unbound transition Writer inventory: %v", err)
	}
	var beforeWinner publicationWinner
	for _, winner := range baseRead.inventory.currentActive {
		if winner.object.KnowledgeObjectID == writerTransitionPersistenceCandidateID {
			beforeWinner = publicationCloneWinner(winner)
			break
		}
	}
	if beforeWinner.object.KnowledgeObjectID == "" {
		t.Fatal("transition Writer candidate is absent from ACTIVE inventory")
	}
	afterWinner := publicationCloneWinner(beforeWinner)
	afterWinner.object.Version++
	boundRead, err := writer.reader.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		publicationTransitionEndpoint{present: true, state: StateActive, winner: beforeWinner},
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: afterWinner},
	)
	if err != nil {
		t.Fatalf("read bound transition Writer inventory: %v", err)
	}
	transition, err := mintPublicationTransitionPersistenceAuthority(
		actorContext,
		tx,
		boundRead,
	)
	if err != nil || transition.IsZero() {
		t.Fatalf("mint transition Writer persistence authority = (%#v, %v)", transition, err)
	}
	now := time.UnixMicro(current.UpdatedAtUnixMicro + 10).UTC()
	plan := publicationPlan{
		route:            mutationRouteSetState,
		mutationKind:     "disable",
		auditAction:      audit.ActionKnowledgeObjectDisable,
		objectID:         current.KnowledgeObjectID,
		version:          current.CurrentVersion + 1,
		state:            StateDisabled,
		definition:       definition,
		dependencies:     dependencies,
		activeTransition: transition,
		ownerID:          current.OwnerID,
		createdAt:        time.UnixMicro(current.CreatedAtUnixMicro).UTC(),
		updatedAt:        now,
		disabledAt:       &now,
		current:          &current,
		oldCatalogState:  boundRead.catalog,
	}
	trace.reset()
	return &writerTransitionPersistenceFixture{
		database: database,
		writer:   writer,
		ctx:      actorContext,
		tx:       tx,
		trace:    trace,
		prepared: prepared,
		plan:     plan,
	}
}

func newWriterACTIVETransitionPersistenceFixture(t *testing.T) *writerTransitionPersistenceFixture {
	t.Helper()
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: writerTransitionPersistenceTargetID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"writer-transition-active-target",
				SharingScopePrivate,
				nil,
				"writer-transition-active-*",
				dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	candidateDefinition := dependencyAliasDefinition(
		testApp,
		"writer-transition-active-candidate",
		SharingScopePrivate,
		nil,
		"writer-transition-active-*",
		dependencyFixtureInputField,
		"writer_transition_active_alias",
	)
	candidateDependencies := []fixtureDependency{{
		targetObjectID: writerTransitionPersistenceTargetID,
		targetVersion:  1,
	}}
	insertFixtureObject(t, database, fixtureObject{
		id: writerTransitionPersistenceCandidateID, owner: testOwner,
		versions: []fixtureVersion{
			{
				definition:   candidateDefinition,
				state:        StateActive,
				mutation:     "create",
				timestamp:    20,
				dependencies: candidateDependencies,
			},
			{
				definition:   candidateDefinition,
				state:        StateDisabled,
				mutation:     "disable",
				timestamp:    30,
				dependencies: candidateDependencies,
			},
		},
	})
	createPublicationTransitionTestIndex(t, database, "writer-transition-active-001")
	writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		t.Fatalf("normalize ACTIVE transition Writer scope: %v", err)
	}
	actor, found := audit.ActorFromContext(actorContext)
	if !found {
		t.Fatal("ACTIVE transition Writer actor is absent")
	}
	prepared, err := prepareSetStateMutation(
		normalizedScope,
		actor,
		&opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: writerTransitionPersistenceCandidateID,
			ExpectedVersion:   2,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId:   "writer-transition-enable-0001",
		},
	)
	if err != nil {
		t.Fatalf("prepare ACTIVE transition Writer request: %v", err)
	}
	trace := &writerTransitionDMLTrace{}
	tx := database.GORMDB().Session(&gorm.Session{Logger: trace}).Begin()
	if tx.Error != nil {
		t.Fatalf("begin ACTIVE transition Writer transaction: %v", tx.Error)
	}
	t.Cleanup(func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back ACTIVE transition Writer transaction: %v", err)
		}
	})
	current, err := readAuthorizedMutationRegistry(
		tx,
		prepared.scope,
		writerTransitionPersistenceCandidateID,
	)
	if err != nil {
		t.Fatalf("read ACTIVE transition Writer current registry: %v", err)
	}
	object, currentVersion, definition, err := writer.hydrateAuthorizedCurrentStateOnly(tx, current)
	if err != nil {
		t.Fatalf("hydrate ACTIVE transition Writer current: %v", err)
	}
	dependencyRows, err := readValidatedVersionDependencies(tx, currentVersion)
	if err != nil {
		t.Fatalf("read ACTIVE transition Writer current dependencies: %v", err)
	}
	snapshot, err := resolutionSnapshotObject(object)
	if err != nil {
		t.Fatalf("build ACTIVE transition Writer snapshot: %v", err)
	}
	beforeWinner := publicationWinner{
		object:                      snapshot,
		existingDependenciesPresent: true,
		existingDependencies: make(
			[]publicationPersistedDependency,
			len(dependencyRows),
		),
	}
	for index, dependency := range dependencyRows {
		beforeWinner.existingDependencies[index] = publicationPersistedDependency{
			ordinal:        dependency.Ordinal,
			targetObjectID: dependency.TargetObjectID,
			targetVersion:  dependency.TargetObjectVersion,
			role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}
	}
	afterWinner := publicationCloneWinner(beforeWinner)
	afterWinner.object.Version++
	afterWinner.existingDependenciesPresent = false
	afterWinner.existingDependencies = nil
	boundRead, err := writer.reader.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: beforeWinner},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: afterWinner},
	)
	if err != nil {
		t.Fatalf("read bound ACTIVE transition Writer inventory: %v", err)
	}
	transition, err := mintPublicationTransitionPersistenceAuthority(
		actorContext,
		tx,
		boundRead,
	)
	if err != nil || transition.IsZero() {
		t.Fatalf("mint ACTIVE transition Writer persistence authority = (%#v, %v)", transition, err)
	}
	now := time.UnixMicro(current.UpdatedAtUnixMicro + 10).UTC()
	plan := publicationPlan{
		route:            mutationRouteSetState,
		mutationKind:     "enable",
		auditAction:      audit.ActionKnowledgeObjectEnable,
		objectID:         current.KnowledgeObjectID,
		version:          current.CurrentVersion + 1,
		state:            StateActive,
		definition:       definition,
		dependencies:     nil,
		activeTransition: transition,
		ownerID:          current.OwnerID,
		createdAt:        time.UnixMicro(current.CreatedAtUnixMicro).UTC(),
		updatedAt:        now,
		current:          &current,
		oldCatalogState:  boundRead.catalog,
	}
	trace.reset()
	return &writerTransitionPersistenceFixture{
		database: database,
		writer:   writer,
		ctx:      actorContext,
		tx:       tx,
		trace:    trace,
		prepared: prepared,
		plan:     plan,
	}
}

func assertWriterTransitionRejectedBeforePersistence(
	t *testing.T,
	fixture *writerTransitionPersistenceFixture,
	hooks int,
) {
	t.Helper()
	if hooks != 0 || len(fixture.trace.snapshot()) != 0 {
		t.Fatalf(
			"rejected transition reached persistence: hooks=%d dml=%v",
			hooks,
			fixture.trace.snapshot(),
		)
	}
	var count int64
	if err := fixture.tx.Model(&versionRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = 2",
		testTenant,
		writerTransitionPersistenceCandidateID,
	).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rejected transition version count = %d, %v", count, err)
	}
	if err := fixture.tx.Table("knowledge_object_dependency_seals").Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = 2",
		testTenant,
		writerTransitionPersistenceCandidateID,
	).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rejected transition dependency seal count = %d, %v", count, err)
	}
}

type writerTransitionDMLTrace struct {
	statements []string
}

func (trace *writerTransitionDMLTrace) LogMode(logger.LogLevel) logger.Interface {
	return trace
}

func (*writerTransitionDMLTrace) Info(context.Context, string, ...any)  {}
func (*writerTransitionDMLTrace) Warn(context.Context, string, ...any)  {}
func (*writerTransitionDMLTrace) Error(context.Context, string, ...any) {}

func (trace *writerTransitionDMLTrace) Trace(
	_ context.Context,
	_ time.Time,
	statement func() (string, int64),
	_ error,
) {
	sql, _ := statement()
	normalized := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(normalized, "INSERT ") ||
		strings.HasPrefix(normalized, "UPDATE ") ||
		strings.HasPrefix(normalized, "DELETE ") ||
		strings.HasPrefix(normalized, "REPLACE ") {
		trace.statements = append(trace.statements, sql)
	}
}

func (trace *writerTransitionDMLTrace) reset() {
	trace.statements = nil
}

func (trace *writerTransitionDMLTrace) snapshot() []string {
	return append([]string(nil), trace.statements...)
}
