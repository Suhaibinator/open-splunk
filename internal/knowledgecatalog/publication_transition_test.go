package knowledgecatalog

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

func TestValidatePublicationActiveTransitionCreatesDetachedAuthority(t *testing.T) {
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-create",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "transition-create", SharingScopeApp, nil, "", "created_field",
		), "main"),
	)}
	input := publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-b", "app-a"},
		[]string{"other", "main"},
	)
	authority, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("validatePublicationActiveTransition(create): %v", err)
	}
	pre, prePresent, preState, post, postState, present := authority.candidateBindings()
	if authority.IsZero() || !present || prePresent || preState != "" ||
		post != publicationTestCandidate(candidate.object) || postState != StateActive ||
		authority.candidateDependencies().IsZero() {
		t.Fatalf(
			"create authority = pre:%#v/%v/%q post:%#v/%q present:%v deps-zero:%v",
			pre, prePresent, preState, post, postState, present,
			authority.candidateDependencies().IsZero(),
		)
	}

	reordered := input
	reordered.activeAppIDs = slices.Clone(input.activeAppIDs)
	reordered.potentiallySearchableIndexNames = slices.Clone(input.potentiallySearchableIndexNames)
	slices.Reverse(reordered.activeAppIDs)
	slices.Reverse(reordered.potentiallySearchableIndexNames)
	replayed, err := validatePublicationActiveTransition(t.Context(), reordered)
	if err != nil || !authority.Equal(replayed) {
		t.Fatalf("deterministic replay = (%v, %v)", replayed, err)
	}
	if authority.Equal(publicationActiveTransitionAuthority{}) {
		t.Fatal("successful transition authority collapsed into zero")
	}
}

func TestValidatePublicationActiveTransitionRemovalNeedsNoIndexWitness(t *testing.T) {
	candidate := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-unmatched-removal",
			4,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "unmatched-removal", SharingScopeApp, nil, "", "old_field",
			), "old*"),
		),
		existingDependenciesPresent: true,
	}
	after := publicationCloneWinner(candidate)
	after.object.Version++
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{candidate},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		publicationTransitionEndpoint{present: true, state: StateDeleted, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); err != nil {
		t.Fatalf("unmatched removal unexpectedly required a witness: %v", err)
	}
}

func TestValidatePublicationActiveTransitionActivationRequiresIndexWitness(t *testing.T) {
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-unmatched-activation",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "unmatched-activation", SharingScopeApp, nil, "", "old_field",
		), "old*"),
	)}
	input := publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("unmatched activation error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationActiveTransitionGlobalCandidateUsesGenericFutureApp(t *testing.T) {
	appShadow := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-app-shadow",
			1,
			dependencyExtractionDefinition(
				"app-a", "future-app-slot", SharingScopeApp, nil, "main", "app_field",
			),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-global-candidate",
		1,
		dependencyExtractionDefinition(
			"app-a", "future-app-slot", SharingScopeGlobal, nil, "main", "global_field",
		),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{appShadow},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); err != nil {
		t.Fatalf("global candidate lacked generic future-app witness: %v", err)
	}
}

func TestValidatePublicationActiveTransitionScopeMoveTraversesPreAndPostClasses(t *testing.T) {
	before := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-scope-move",
			5,
			dependencyExtractionDefinition(
				"app-a", "scope-move", SharingScopeApp, nil, "main", "scope_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-scope-move",
		6,
		dependencyExtractionDefinition(
			"app-a", "scope-move", SharingScopePrivate, nil, "main", "scope_field",
		),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{before},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: before},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); err != nil {
		t.Fatalf("ACTIVE app-to-private scope move failed: %v", err)
	}
}

func TestValidatePublicationActiveTransitionRemovalRejectsCorruptUnshadowedRows(t *testing.T) {
	role := publicationPersistedDependency{
		ordinal:        0,
		targetObjectID: "ko-transition-missing-target",
		targetVersion:  1,
		role:           1,
	}
	lower := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-corrupt-lower",
			1,
			dependencyExtractionDefinition(
				"app-a", "corrupt-slot", SharingScopeGlobal, nil, "main", "lower_field",
			),
		),
		existingDependenciesPresent: true,
		existingDependencies:        []publicationPersistedDependency{role},
	}
	candidate := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-removing-shadow",
			2,
			dependencyExtractionDefinition(
				"app-a", "corrupt-slot", SharingScopeApp, nil, "main", "upper_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationCloneWinner(candidate)
	after.object.Version++
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{candidate, lower},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt unshadowed dependency error = %v, want ErrCorrupt", err)
	}
}

func TestValidatePublicationActiveTransitionNameMoveUnshadowsOldSlot(t *testing.T) {
	lower := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-old-global",
			1,
			dependencyExtractionDefinition(
				"app-a", "old-slot", SharingScopeGlobal, nil, "main", "global_old_field",
			),
		),
		existingDependenciesPresent: true,
	}
	before := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-moving",
			5,
			dependencyExtractionDefinition(
				"app-a", "old-slot", SharingScopeApp, nil, "main", "app_old_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-moving",
		6,
		dependencyExtractionDefinition(
			"app-a", "new-slot", SharingScopeApp, nil, "main", "app_new_field",
		),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{before, lower},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: before},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); err != nil {
		t.Fatalf("ACTIVE name move failed old-slot unshadow validation: %v", err)
	}
}

func TestValidatePublicationActiveTransitionRemovalRevalidatesUnshadowedWinner(t *testing.T) {
	lower := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-global",
			1,
			dependencyExtractionDefinition(
				"app-a", "shared-slot", SharingScopeGlobal, nil, "main", "global_field",
			),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-app",
			3,
			dependencyExtractionDefinition(
				"app-a", "shared-slot", SharingScopeApp, nil, "main", "app_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationCloneWinner(candidate)
	after.object.Version++
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{candidate, lower},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	authority, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("validatePublicationActiveTransition(removal): %v", err)
	}
	pre, prePresent, preState, post, postState, present := authority.candidateBindings()
	if !present || !prePresent || preState != StateActive || postState != StateDisabled ||
		pre != publicationTestCandidate(candidate.object) || post != publicationTestCandidate(after.object) ||
		!authority.candidateDependencies().IsZero() {
		t.Fatalf(
			"removal authority = pre:%#v/%v/%q post:%#v/%q deps:%#v",
			pre, prePresent, preState, post, postState, authority.candidateDependencies(),
		)
	}
	deletedInput := input
	deletedInput.candidateAfter.state = StateDeleted
	deletedAuthority, err := validatePublicationActiveTransition(t.Context(), deletedInput)
	if err != nil {
		t.Fatalf("validatePublicationActiveTransition(delete replay): %v", err)
	}
	if authority.Equal(deletedAuthority) {
		t.Fatal("DISABLED and DELETED endpoint authorities compare equal")
	}
}

func TestValidatePublicationActiveTransitionEnableBindsInactivePredecessor(t *testing.T) {
	before := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-enable",
			7,
			dependencyExtractionDefinition(
				"app-a", "transition-enable", SharingScopeApp, nil, "main", "enabled_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationCloneWinner(before)
	after.object.Version++
	after.existingDependenciesPresent = false
	input := publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: before},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	authority, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("validatePublicationActiveTransition(enable): %v", err)
	}
	pre, prePresent, preState, post, postState, present := authority.candidateBindings()
	if !present || !prePresent || preState != StateDisabled || postState != StateActive ||
		pre != publicationTestCandidate(before.object) || post != publicationTestCandidate(after.object) {
		t.Fatalf(
			"enable bindings = pre:%#v/%v/%q post:%#v/%q present:%v",
			pre, prePresent, preState, post, postState, present,
		)
	}
}

func TestValidatePublicationActiveTransitionInactiveBeforeFaultsAreCorrupt(t *testing.T) {
	before := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-corrupt-inactive-before",
			7,
			dependencyExtractionDefinition(
				"app-a", "corrupt-inactive-before", SharingScopeApp, nil, "main", "before_field",
			),
		),
		existingDependenciesPresent: true,
	}
	after := publicationCloneWinner(before)
	after.object.Version++
	after.existingDependenciesPresent = false
	after.existingDependencies = nil

	tests := []struct {
		name   string
		mutate func(*publicationWinner)
	}{
		{
			name: "malformed_definition",
			mutate: func(winner *publicationWinner) {
				winner.object.Definition = nil
			},
		},
		{
			name: "digest_disagreement",
			mutate: func(winner *publicationWinner) {
				winner.object.DefinitionSHA256[0] ^= 0xff
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := publicationTransitionTestInventory(
				t,
				nil,
				publicationTransitionEndpoint{present: true, state: StateDisabled, winner: before},
				publicationTransitionEndpoint{present: true, state: StateActive, winner: after},
				[]string{"app-a"},
				[]string{"main"},
			)
			test.mutate(&input.candidateBefore.winner)
			if _, err := validatePublicationActiveTransition(t.Context(), input); !errors.Is(
				err,
				ErrCorrupt,
			) || errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("inactive persisted endpoint error = %v, want only ErrCorrupt", err)
			}
		})
	}
}

func TestPublicationActiveTransitionAuthorityMatchesActivePersistenceProjection(t *testing.T) {
	target := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-persistence-target",
			3,
			dependencyExtractionDefinition(
				"app-a", "persistence-target", SharingScopeApp, nil, "main", "persisted_input",
			),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-persistence-active",
		1,
		dependencyAliasDefinition(
			"app-a", "persistence-active", SharingScopeApp, nil, "main", "persisted_input", "persisted_output",
		),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{target},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	authority, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("validate active persistence projection: %v", err)
	}
	binding := publicationTransitionTestPersistenceBinding(
		input,
		[]publicationDependency{{
			targetObjectID: target.object.KnowledgeObjectID,
			targetVersion:  int64(target.object.Version),
		}},
	)
	if !authority.matchesPersistence(binding) {
		t.Fatal("active transition authority rejected its exact derived persistence projection")
	}
	otherTenant := binding
	otherTenant.tenantID = "tenant-b"
	if authority.matchesPersistence(otherTenant) {
		t.Fatal("active transition authority matched a cross-tenant persistence plan")
	}
}

func TestPublicationActiveTransitionAuthorityRejectsRemovalDependencySwap(t *testing.T) {
	targets := []publicationWinner{
		{
			object: publicationTestObject(
				t,
				"ko-transition-persistence-target-a",
				1,
				dependencyExtractionDefinition(
					"app-a", "persistence-target-a", SharingScopeApp, nil, "main", "persisted_a",
				),
			),
			existingDependenciesPresent: true,
		},
		{
			object: publicationTestObject(
				t,
				"ko-transition-persistence-target-b",
				1,
				dependencyExtractionDefinition(
					"app-a", "persistence-target-b", SharingScopeApp, nil, "main", "persisted_b",
				),
			),
			existingDependenciesPresent: true,
		},
	}
	rows := []publicationPersistedDependency{
		{
			ordinal:        0,
			targetObjectID: targets[0].object.KnowledgeObjectID,
			targetVersion:  int64(targets[0].object.Version),
			role:           opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		},
		{
			ordinal:        1,
			targetObjectID: targets[1].object.KnowledgeObjectID,
			targetVersion:  int64(targets[1].object.Version),
			role:           opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		},
	}
	candidate := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-persistence-removal",
			8,
			dependencyAliasDefinition(
				"app-a", "persistence-removal", SharingScopeApp, nil, "main", "persisted_a", "removed_output",
			),
		),
		existingDependenciesPresent: true,
		existingDependencies:        rows,
	}
	after := publicationCloneWinner(candidate)
	after.object.Version++
	current := append(slices.Clone(targets), candidate)
	input := publicationTransitionTestInventory(
		t,
		current,
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: after},
		[]string{"app-a"},
		[]string{"main"},
	)
	authority, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("validate removal persistence projection: %v", err)
	}
	binding := publicationTransitionTestPersistenceBinding(
		input,
		[]publicationDependency{
			{targetObjectID: rows[0].targetObjectID, targetVersion: rows[0].targetVersion},
			{targetObjectID: rows[1].targetObjectID, targetVersion: rows[1].targetVersion},
		},
	)
	if !authority.matchesPersistence(binding) {
		t.Fatal("removal transition authority rejected its exact retained persistence projection")
	}

	rowSwap := binding
	rowSwap.after.existingDependencies = slices.Clone(binding.after.existingDependencies)
	rowSwap.after.existingDependencies[0], rowSwap.after.existingDependencies[1] =
		rowSwap.after.existingDependencies[1], rowSwap.after.existingDependencies[0]
	if authority.matchesPersistence(rowSwap) {
		t.Fatal("removal transition authority matched swapped retained endpoint rows")
	}
	projectionSwap := binding
	projectionSwap.dependencies = slices.Clone(binding.dependencies)
	projectionSwap.dependencies[0], projectionSwap.dependencies[1] =
		projectionSwap.dependencies[1], projectionSwap.dependencies[0]
	if authority.matchesPersistence(projectionSwap) {
		t.Fatal("removal transition authority matched a swapped database projection")
	}
}

func TestValidatePublicationActiveTransitionFindsMultiIndexOnlyConflict(t *testing.T) {
	existing := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-existing-output",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "existing-output", SharingScopeApp, nil, "", "shared_output",
			), "*prod"),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-candidate-output",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "candidate-output", SharingScopeApp, nil, "", "shared_output",
		), "main*"),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{existing},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main-dev", "stage-prod"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("multi-index-only conflict error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationActiveTransitionRejectsCrossCohortCandidateAuthority(t *testing.T) {
	appTarget := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-app-target",
			1,
			dependencyExtractionDefinition(
				"app-a", "target-slot", SharingScopeApp, nil, "", "derived_input",
			),
		),
		existingDependenciesPresent: true,
	}
	privateTarget := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-private-target",
			1,
			dependencyExtractionDefinition(
				"app-a", "target-slot", SharingScopePrivate, nil, "", "derived_input",
			),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-source",
		1,
		dependencyAliasDefinition(
			"app-a", "source", SharingScopeApp, nil, "", "derived_input", "derived_output",
		),
	)}
	input := publicationTransitionTestInventory(
		t,
		[]publicationWinner{appTarget, privateTarget},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	if _, err := validatePublicationActiveTransition(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("cross-cohort authority error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationActiveTransitionDetachesBeforeContextCallbacks(t *testing.T) {
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-detach",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "transition-detach", SharingScopeApp, nil, "", "detached_field",
		), "main"),
	)}
	input := publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	expected, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("baseline transition: %v", err)
	}
	mutation := &publicationMutationContext{
		Context:  t.Context(),
		mutateAt: 1,
		mutate: func() {
			input.candidateAfter.winner.object.DefinitionSHA256[0] ^= 0xff
			input.candidateAfter.winner.object.Definition.Name = "mutated"
			input.activeAppIDs[0] = "mutated-app"
			input.potentiallySearchableIndexNames[0] = "mutated-index"
		},
	}
	actual, err := validatePublicationActiveTransition(mutation, input)
	if err != nil || !mutation.mutated || !expected.Equal(actual) {
		t.Fatalf("detached transition = (%v, %v), mutated=%v", actual, err, mutation.mutated)
	}
}

func TestValidatePublicationActiveTransitionCancellationAndConcurrency(t *testing.T) {
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-transition-cancel",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "transition-cancel", SharingScopeApp, nil, "", "cancel_field",
		), "main*"),
	)}
	input := publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := validatePublicationActiveTransition(canceled, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transition error = %v, want context.Canceled", err)
	}

	want, err := validatePublicationActiveTransition(t.Context(), input)
	if err != nil {
		t.Fatalf("baseline concurrent transition: %v", err)
	}
	const workers = 8
	errorsByWorker := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			got, workerErr := validatePublicationActiveTransition(t.Context(), input)
			if workerErr != nil {
				errorsByWorker <- workerErr
				return
			}
			if !want.Equal(got) {
				errorsByWorker <- errors.New("concurrent transition authority differs")
			}
		})
	}
	group.Wait()
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		t.Error(workerErr)
	}
}

func TestValidatePublicationTransitionPostAggregateFailsClosed(t *testing.T) {
	input := publicationActiveTransitionInventory{
		expectedDefinitionBytes: maximumPublicationTransitionDefinitionBytes,
	}
	after := &publicationTransitionCanonicalObject{
		canonical: canonicalPublicationWinner{definitionBytes: 1},
	}
	if _, err := validatePublicationTransitionPostAggregate(
		input,
		false,
		nil,
		true,
		after,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("post aggregate +1 error = %v, want capacity exceeded", err)
	}
}

func TestPublicationTransitionWorkSemanticChargeLimits(t *testing.T) {
	tests := []struct {
		name string
		full ResolutionStaticCharges
		unit ResolutionStaticCharges
	}{
		{
			name: "generated_fields",
			full: ResolutionStaticCharges{GeneratedFields: knowledgesnapshot.MaximumGeneratedFields},
			unit: ResolutionStaticCharges{GeneratedFields: 1},
		},
		{
			name: "regex_programs",
			full: ResolutionStaticCharges{ExtractionRegexPrograms: knowledgesnapshot.MaximumRegexPrograms},
			unit: ResolutionStaticCharges{ExtractionRegexPrograms: 1},
		},
		{
			name: "regex_work",
			full: ResolutionStaticCharges{ExtractionRegexWorkUnits: knowledgesnapshot.MaximumRegexWorkUnits},
			unit: ResolutionStaticCharges{ExtractionRegexWorkUnits: 1},
		},
		{
			name: "extraction_outputs",
			full: ResolutionStaticCharges{ExtractionOutputs: knowledgesnapshot.MaximumExtractionOutputs},
			unit: ResolutionStaticCharges{ExtractionOutputs: 1},
		},
		{
			name: "json_work",
			full: ResolutionStaticCharges{JSONEvaluationWorkUnits: knowledgesnapshot.MaximumJSONEvaluationWorkUnits},
			unit: ResolutionStaticCharges{JSONEvaluationWorkUnits: 1},
		},
		{
			name: "scalar_expressions",
			full: ResolutionStaticCharges{ScalarExpressions: knowledgesnapshot.MaximumScalarExpressions},
			unit: ResolutionStaticCharges{ScalarExpressions: 1},
		},
		{
			name: "scalar_nodes",
			full: ResolutionStaticCharges{ScalarExpressionNodes: knowledgesnapshot.MaximumScalarExpressionNodes},
			unit: ResolutionStaticCharges{ScalarExpressionNodes: 1},
		},
		{
			name: "scalar_predicates",
			full: ResolutionStaticCharges{ScalarPredicates: knowledgesnapshot.MaximumScalarPredicates},
			unit: ResolutionStaticCharges{ScalarPredicates: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var work publicationTransitionWork
			for range maximumPublicationTransitionSemanticPrograms {
				if err := work.chargeSemanticCharges(test.full); err != nil {
					t.Fatalf("exact semantic charge failed: %v", err)
				}
			}
			if err := work.chargeSemanticCharges(test.unit); !errors.Is(
				err,
				control.ErrCapacityExceeded,
			) {
				t.Fatalf("semantic charge +1 error = %v, want capacity exceeded", err)
			}
		})
	}
}

func TestPublicationTransitionChangedCohortChargesRealSemantics(t *testing.T) {
	winner := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-semantic-charge",
			1,
			dependencyExtractionDefinition(
				"app-a", "semantic-charge", SharingScopeApp, nil, "main", "charged_field",
			),
		),
		existingDependenciesPresent: true,
	}
	canonical, err := canonicalizePublicationTransitionObject(winner, false)
	if err != nil {
		t.Fatalf("canonicalize semantic charge fixture: %v", err)
	}
	charges := canonical.canonical.semantics.charges
	if charges.GeneratedFields == 0 {
		t.Fatal("semantic charge fixture has no generated-field contribution")
	}
	work := publicationTransitionWork{
		generatedFields:         maximumPublicationTransitionGeneratedFields - uint64(charges.GeneratedFields),
		regexPrograms:           maximumPublicationTransitionRegexPrograms - uint64(charges.ExtractionRegexPrograms),
		regexWorkUnits:          maximumPublicationTransitionRegexWorkUnits - charges.ExtractionRegexWorkUnits,
		extractionOutputs:       maximumPublicationTransitionExtractionOutputs - uint64(charges.ExtractionOutputs),
		jsonEvaluationWorkUnits: maximumPublicationTransitionJSONEvaluationWorkUnits - uint64(charges.JSONEvaluationWorkUnits),
		scalarExpressions:       maximumPublicationTransitionScalarExpressions - uint64(charges.ScalarExpressions),
		scalarExpressionNodes:   maximumPublicationTransitionScalarExpressionNodes - uint64(charges.ScalarExpressionNodes),
		scalarPredicates:        maximumPublicationTransitionScalarPredicates - uint64(charges.ScalarPredicates),
	}
	if err := work.chargeChangedCohort([]*publicationTransitionCanonicalObject{canonical}); err != nil {
		t.Fatalf("exact changed-cohort semantic charge failed: %v", err)
	}
	if err := work.chargeChangedCohort(
		[]*publicationTransitionCanonicalObject{canonical},
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("changed-cohort semantic charge +1 error = %v, want capacity exceeded", err)
	}
}

func TestPublicationTransitionChangedCohortSelectorWorkLimit(t *testing.T) {
	canonical := &publicationTransitionCanonicalObject{
		canonical: canonicalPublicationWinner{
			selectorWork: knowledge.MaximumSelectorWildcardWorkUnits,
		},
	}
	var work publicationTransitionWork
	for range maximumPublicationTransitionSemanticPrograms {
		if err := work.chargeChangedCohort(
			[]*publicationTransitionCanonicalObject{canonical},
		); err != nil {
			t.Fatalf("exact changed-cohort selector charge failed: %v", err)
		}
	}
	if work.selectorWorkRevisits != maximumPublicationTransitionSelectorWorkRevisits {
		t.Fatalf(
			"exact changed-cohort selector charge = %d, want %d",
			work.selectorWorkRevisits,
			maximumPublicationTransitionSelectorWorkRevisits,
		)
	}
	unit := &publicationTransitionCanonicalObject{
		canonical: canonicalPublicationWinner{selectorWork: 1},
	}
	if err := work.chargeChangedCohort(
		[]*publicationTransitionCanonicalObject{unit},
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("changed-cohort selector charge +1 error = %v, want capacity exceeded", err)
	}
}

func TestPublicationTransitionChangedCohortChargesRealSelectorWork(t *testing.T) {
	winner := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-transition-selector-charge",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "selector-charge", SharingScopeApp, nil, "", "charged_field",
			), "main*"),
		),
		existingDependenciesPresent: true,
	}
	canonical, err := canonicalizePublicationTransitionObject(winner, false)
	if err != nil {
		t.Fatalf("canonicalize selector charge fixture: %v", err)
	}
	if canonical.canonical.selectorWork == 0 {
		t.Fatal("selector charge fixture has no wildcard-work contribution")
	}
	var work publicationTransitionWork
	if err := work.chargeChangedCohort(
		[]*publicationTransitionCanonicalObject{canonical},
	); err != nil {
		t.Fatalf("charge real changed-cohort selector work: %v", err)
	}
	want := publicationTransitionCohortSelectorNormalizationPasses * canonical.canonical.selectorWork
	if work.selectorWorkRevisits != want {
		t.Fatalf("real changed-cohort selector charge = %d, want %d", work.selectorWorkRevisits, want)
	}
}

func TestSelectPublicationTransitionWinnersAmbiguityTaxonomy(t *testing.T) {
	objects := make([]*publicationTransitionCanonicalObject, 2)
	for index, objectID := range []string{"ko-transition-ambiguous-a", "ko-transition-ambiguous-b"} {
		winner := publicationWinner{
			object: publicationTestObject(
				t,
				objectID,
				1,
				dependencyExtractionDefinition(
					"app-a", "ambiguous-slot", SharingScopeGlobal, nil, "main", "ambiguous_field",
				),
			),
			existingDependenciesPresent: true,
		}
		canonical, err := canonicalizePublicationTransitionObject(winner, false)
		if err != nil {
			t.Fatalf("canonicalize ambiguity fixture %d: %v", index, err)
		}
		objects[index] = canonical
	}
	var membership publicationIndexMembership
	publicationTransitionSetMembership(&membership, 0)
	publicationTransitionSetMembership(&membership, 1)
	class := publicationTransitionPrincipalClass{
		kind:  publicationTransitionGenericPrincipal,
		appID: "app-a",
	}
	tests := []struct {
		name      string
		candidate *publicationTransitionCanonicalObject
		exposed   bool
		want      error
	}{
		{
			name:    "preexisting_ambiguity",
			exposed: false,
			want:    ErrCorrupt,
		},
		{
			name:    "removal_exposes_ambiguity",
			exposed: true,
			want:    control.ErrDependencyConflict,
		},
		{
			name:      "candidate_creates_ambiguity",
			candidate: objects[1],
			exposed:   true,
			want:      control.ErrAlreadyExists,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var work publicationTransitionWork
			_, err := selectPublicationTransitionWinners(
				t.Context(),
				class,
				&membership,
				objects,
				test.candidate,
				test.exposed,
				&work,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("ambiguity error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSelectPublicationTransitionWinnersMatchesResolverPrecedencePermutations(t *testing.T) {
	canonical := func(
		t *testing.T,
		objectID string,
		scope SharingScope,
	) *publicationTransitionCanonicalObject {
		t.Helper()
		winner := publicationWinner{
			object: publicationTestObject(
				t,
				objectID,
				1,
				dependencyExtractionDefinition(
					"app-a", "precedence-parity", scope, nil, "main", "parity_field",
				),
			),
			existingDependenciesPresent: true,
		}
		result, err := canonicalizePublicationTransitionObject(winner, false)
		if err != nil {
			t.Fatalf("canonicalize precedence fixture %q: %v", objectID, err)
		}
		return result
	}
	class := publicationTransitionPrincipalClass{
		kind:    publicationTransitionPrivatePrincipal,
		appID:   "app-a",
		ownerID: "owner-a",
	}
	permutations := [][3]int{
		{0, 1, 2}, {0, 2, 1}, {1, 0, 2},
		{1, 2, 0}, {2, 0, 1}, {2, 1, 0},
	}
	tests := []struct {
		name      string
		objects   []*publicationTransitionCanonicalObject
		wantError bool
	}{
		{
			name: "duplicate_lower_rank",
			objects: []*publicationTransitionCanonicalObject{
				canonical(t, "ko-transition-parity-global-a", SharingScopeGlobal),
				canonical(t, "ko-transition-parity-global-b", SharingScopeGlobal),
				canonical(t, "ko-transition-parity-private", SharingScopePrivate),
			},
		},
		{
			name: "duplicate_highest_rank",
			objects: []*publicationTransitionCanonicalObject{
				canonical(t, "ko-transition-parity-global", SharingScopeGlobal),
				canonical(t, "ko-transition-parity-private-a", SharingScopePrivate),
				canonical(t, "ko-transition-parity-private-b", SharingScopePrivate),
			},
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, permutation := range permutations {
				objects := []*publicationTransitionCanonicalObject{
					test.objects[permutation[0]],
					test.objects[permutation[1]],
					test.objects[permutation[2]],
				}
				var membership publicationIndexMembership
				resolverGroup := make([]resolutionCandidate, len(objects))
				for index, object := range objects {
					publicationTransitionSetMembership(&membership, index)
					rank, visible := publicationTransitionVisibilityRank(class, object.canonical.object)
					if !visible {
						t.Fatalf("fixture %q is not visible", object.canonical.key.objectID)
					}
					resolverGroup[index] = resolutionCandidate{
						object: Object{KnowledgeObjectID: object.canonical.key.objectID},
						rank:   rank,
					}
				}
				var work publicationTransitionWork
				winners, transitionErr := selectPublicationTransitionWinners(
					t.Context(),
					class,
					&membership,
					objects,
					nil,
					false,
					&work,
				)
				resolverWinner, resolverErr := uniqueHighestResolutionCandidate(resolverGroup)
				if test.wantError {
					if !errors.Is(transitionErr, ErrCorrupt) || !errors.Is(resolverErr, ErrCorrupt) {
						t.Fatalf(
							"permutation %v errors = transition:%v resolver:%v, want ErrCorrupt",
							permutation,
							transitionErr,
							resolverErr,
						)
					}
					continue
				}
				if transitionErr != nil || resolverErr != nil || len(winners) != 1 ||
					winners[0].canonical.key.objectID != resolverWinner.object.KnowledgeObjectID {
					t.Fatalf(
						"permutation %v = transition:%v/%v resolver:%q/%v",
						permutation,
						winners,
						transitionErr,
						resolverWinner.object.KnowledgeObjectID,
						resolverErr,
					)
				}
			}
		})
	}
}

func publicationTransitionTestInventory(
	t *testing.T,
	current []publicationWinner,
	before publicationTransitionEndpoint,
	after publicationTransitionEndpoint,
	activeApps []string,
	indexes []string,
) publicationActiveTransitionInventory {
	t.Helper()
	input := publicationActiveTransitionInventory{
		tenantID:                                "tenant-a",
		expectedActiveAppCount:                  uint16(len(activeApps)),
		activeAppIDs:                            slices.Clone(activeApps),
		expectedCurrentActiveCount:              uint32(len(current)),
		currentActive:                           make([]publicationWinner, len(current)),
		candidateBefore:                         before,
		candidateAfter:                          after,
		expectedPotentiallySearchableIndexCount: uint16(len(indexes)),
		potentiallySearchableIndexNames:         slices.Clone(indexes),
	}
	for index, winner := range current {
		input.currentActive[index] = publicationCloneWinner(winner)
		canonical, err := canonicalizePublicationTransitionObject(winner, false)
		if err != nil {
			t.Fatalf("canonicalize current transition fixture %d: %v", index, err)
		}
		input.expectedDefinitionBytes += canonical.canonical.definitionBytes
		input.expectedProjectionBytes += canonical.projectionBytes
		input.expectedSelectorPatterns += canonical.selectorPatterns
		input.expectedSelectorValueBytes += canonical.selectorValueBytes
		input.expectedCanonicalSelectorBytes += canonical.canonicalSelectorBytes
		input.expectedSelectorWork += canonical.canonical.selectorWork
		input.expectedDependencyCount += uint64(len(winner.existingDependencies))
	}
	input.candidateBefore = publicationTransitionEndpoint{
		present: before.present,
		state:   before.state,
	}
	if before.present {
		input.candidateBefore.winner = publicationCloneWinner(before.winner)
	}
	input.candidateAfter.winner = publicationCloneWinner(after.winner)
	return input
}

func publicationTransitionTestIndexDefinition(
	definition *opensplunk.KnowledgeObjectDefinition,
	pattern string,
) *opensplunk.KnowledgeObjectDefinition {
	definition.Selector = &opensplunk.KnowledgeSelector{
		IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: pattern}},
	}
	return definition
}

func publicationTransitionTestPersistenceBinding(
	input publicationActiveTransitionInventory,
	dependencies []publicationDependency,
) publicationTransitionPersistenceBinding {
	return publicationTransitionPersistenceBinding{
		tenantID:     input.tenantID,
		before:       publicationTransitionPersistenceEndpointFrom(input.candidateBefore),
		after:        publicationTransitionPersistenceEndpointFrom(input.candidateAfter),
		dependencies: slices.Clone(dependencies),
	}
}

func (authority publicationActiveTransitionAuthority) candidateBindings() (
	pre publicationCandidateAuthority,
	prePresent bool,
	preState State,
	post publicationCandidateAuthority,
	postState State,
	present bool,
) {
	if authority.state == nil {
		return publicationCandidateAuthority{}, false, "", publicationCandidateAuthority{}, "", false
	}
	pre = publicationTransitionCandidateFromPersistence(authority.state.beforePersistence)
	post = publicationTransitionCandidateFromPersistence(authority.state.afterPersistence)
	return pre, authority.state.beforePersistence.present, authority.state.beforePersistence.state,
		post, authority.state.afterPersistence.state, true
}

func publicationTransitionCandidateFromPersistence(
	endpoint publicationTransitionPersistenceEndpoint,
) publicationCandidateAuthority {
	if !endpoint.present {
		return publicationCandidateAuthority{}
	}
	return publicationCandidateAuthority{
		objectID:         strings.Clone(endpoint.objectID),
		version:          endpoint.version,
		definitionDigest: endpoint.definitionDigest,
		ownerID:          strings.Clone(endpoint.ownerID),
	}
}
