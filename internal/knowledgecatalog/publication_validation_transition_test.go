package knowledgecatalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestValidatePublicationActiveCandidateReturnsDetachedDependencyAuthority(t *testing.T) {
	tests := []struct {
		name       string
		input      publicationActiveTransitionInventory
		wantTarget string
	}{
		{
			name: "present empty",
			input: publicationActiveValidationZeroDependencyInventory(
				t,
				"ko-validation-zero",
			),
		},
		{
			name: "nonempty",
			input: publicationActiveValidationDependencyInventory(
				t,
				"ko-validation-source",
			),
			wantTarget: "ko-validation-target",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := validatePublicationActiveCandidate(t.Context(), test.input)
			if err != nil {
				t.Fatalf("validatePublicationActiveCandidate(): %v", err)
			}
			if decision.IsZero() {
				t.Fatal("valid decision collapsed into zero")
			}
			if conflict, present := decision.conflict(); present {
				t.Fatalf("valid decision conflict = %d", conflict)
			}
			dependencies, present := decision.candidateDependencies()
			if !present || dependencies.IsZero() {
				t.Fatal("valid decision omitted present candidate dependency authority")
			}
			projection := dependencies.databaseProjection()
			if test.wantTarget == "" {
				if len(projection) != 0 {
					t.Fatalf("zero dependency projection = %#v", projection)
				}
				return
			}
			if len(projection) != 1 || projection[0].targetObjectID != test.wantTarget ||
				projection[0].targetVersion != 1 {
				t.Fatalf("dependency projection = %#v", projection)
			}

			dependencies.state.projection[0].targetObjectID = "caller-mutated"
			dependencies.state.targets[0].objectID = "caller-mutated"
			replayed, replayPresent := decision.candidateDependencies()
			if !replayPresent || replayed.databaseProjection()[0].targetObjectID != test.wantTarget ||
				replayed.derivedTargets()[0].objectID != test.wantTarget {
				t.Fatal("decision exposed mutable dependency authority")
			}
		})
	}
}

func TestPublicationActiveValidationPreservesMutationCommitmentGolden(t *testing.T) {
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
		t.Fatalf("mutation compatibility transition: %v", err)
	}
	if authority.state == nil {
		t.Fatal("mutation compatibility authority is absent")
	}
	const want = "11079f4fa8ab70f7e80e29b4ce34a717725b15b4ff0d84c3c87f5fa4b7e1e547"
	if got := fmt.Sprintf("%x", authority.state.transitionCommitment); got != want {
		t.Fatalf("mutation transition commitment = %s, want %s", got, want)
	}
}

func TestPublicationActiveValidationDecisionRejectsMalformedState(t *testing.T) {
	presentEmpty := candidateDependencyAuthority{state: &candidateDependencyAuthorityState{}}
	tests := []struct {
		name     string
		decision publicationActiveValidationDecision
	}{
		{name: "zero"},
		{
			name:     "neither branch",
			decision: publicationActiveValidationDecision{state: &publicationActiveValidationDecisionState{}},
		},
		{
			name: "unknown conflict",
			decision: publicationActiveValidationDecision{state: &publicationActiveValidationDecisionState{
				conflict: publicationActiveValidationConflict(255),
			}},
		},
		{
			name: "both branches",
			decision: publicationActiveValidationDecision{state: &publicationActiveValidationDecisionState{
				conflict:              publicationActiveValidationCandidatePostConflict,
				candidateDependencies: presentEmpty,
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.decision.valid() {
				t.Fatal("malformed decision passed closed-state validation")
			}
			if conflict, present := test.decision.conflict(); present || conflict != 0 {
				t.Fatalf("malformed decision exposed conflict %d/%t", conflict, present)
			}
			if dependencies, present := test.decision.candidateDependencies(); present || !dependencies.IsZero() {
				t.Fatal("malformed decision exposed dependencies")
			}
		})
	}
	if !publicationActiveValidationConflictDecision(
		publicationActiveValidationNoWinningWitness,
	).valid() || !publicationActiveValidationValidDecision(presentEmpty).valid() {
		t.Fatal("closed decision constructors produced invalid state")
	}
}

func TestValidatePublicationActiveCandidateClassifiesConflicts(t *testing.T) {
	tests := []struct {
		name  string
		input publicationActiveTransitionInventory
		want  publicationActiveValidationConflict
	}{
		{
			name:  "candidate post semantic conflict",
			input: publicationActiveValidationSemanticConflictInventory(t),
			want:  publicationActiveValidationCandidatePostConflict,
		},
		{
			name:  "cohort local target absence",
			input: publicationActiveValidationTargetAbsentInventory(t),
			want:  publicationActiveValidationCohortDependencyTargetAbsent,
		},
		{
			name:  "no winning witness",
			input: publicationActiveValidationNoWitnessInventory(t),
			want:  publicationActiveValidationNoWinningWitness,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := validatePublicationActiveCandidate(t.Context(), test.input)
			if err != nil {
				t.Fatalf("validatePublicationActiveCandidate(): %v", err)
			}
			conflict, present := decision.conflict()
			if decision.IsZero() || !present || conflict != test.want {
				t.Fatalf("conflict decision = zero:%t kind:%d/%t, want %d", decision.IsZero(), conflict, present, test.want)
			}
			if dependencies, present := decision.candidateDependencies(); present || !dependencies.IsZero() {
				t.Fatal("conflict decision exposed candidate dependency authority")
			}
		})
	}
}

func TestPublicationActiveValidationClassifiesDefensiveCrossCohortMismatch(t *testing.T) {
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-merge-candidate",
		1,
		publicationTransitionTestIndexDefinition(dependencyAliasDefinition(
			"app-a", "validation-merge-candidate", SharingScopeApp, nil, "", "merge_input", "merge_output",
		), "main"),
	)}
	candidateAuthority := publicationTestCandidate(candidate.object)
	derived := make([]candidateDependencyAuthority, 2)
	for index, targetID := range []string{
		"ko-validation-merge-target-a",
		"ko-validation-merge-target-b",
	} {
		target := publicationWinner{
			object: publicationTestObject(
				t,
				targetID,
				1,
				publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
					"app-a", "validation-merge-target", SharingScopeApp, nil, "", "merge_input",
				), "main"),
			),
			existingDependenciesPresent: true,
		}
		cohort, err := validatePublicationWinnerCohort(
			t.Context(),
			publicationWinnerCohort{
				expectedWinnerCount: 2,
				winners:             []publicationWinner{target, candidate},
			},
			candidateAuthority,
			true,
		)
		if err != nil {
			t.Fatalf("independent candidate cohort %d: %v", index, err)
		}
		derived[index] = cohort.candidateDependencies()
	}

	validation := publicationTransitionEvaluator{validation: true}
	var merged candidateDependencyAuthority
	if conflict, err := validation.mergeCandidateDependencies(&merged, derived[0]); err != nil || conflict != 0 {
		t.Fatalf("first candidate cohort merge = %d, %v", conflict, err)
	}
	if conflict, err := validation.mergeCandidateDependencies(&merged, derived[1]); err != nil || conflict != publicationActiveValidationCrossCohortDependencyMismatch {
		t.Fatalf("cross-cohort mismatch merge = %d, %v", conflict, err)
	}

	mutation := publicationTransitionEvaluator{}
	merged = candidateDependencyAuthority{}
	if _, err := mutation.mergeCandidateDependencies(&merged, derived[0]); err != nil {
		t.Fatalf("mutation first cohort merge: %v", err)
	}
	_, err := mutation.mergeCandidateDependencies(&merged, derived[1])
	const want = "control: dependent object conflict: publication candidate dependency authority differs across winner cohorts"
	if !errors.Is(err, control.ErrDependencyConflict) || err.Error() != want {
		t.Fatalf("mutation mismatch error = %q, want %q", err, want)
	}
}

func TestValidatePublicationActiveCandidateDoesNotDowngradeMixedPostConflicts(t *testing.T) {
	for _, test := range []struct {
		name          string
		absentIndex   string
		conflictIndex string
	}{
		{name: "target absence first", absentIndex: "one", conflictIndex: "two"},
		{name: "semantic conflict first", absentIndex: "two", conflictIndex: "one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision, err := validatePublicationActiveCandidate(
				t.Context(),
				publicationActiveValidationMixedConflictInventory(
					t,
					test.absentIndex,
					test.conflictIndex,
				),
			)
			if err != nil {
				t.Fatalf("mixed post conflict validation: %v", err)
			}
			conflict, present := decision.conflict()
			if !present || conflict != publicationActiveValidationCandidatePostConflict {
				t.Fatalf("mixed post conflict = %d/%t, want candidate post conflict", conflict, present)
			}
		})
	}
}

func TestValidatePublicationActiveCandidateChecksBaselineBeforeCandidateConflict(t *testing.T) {
	input := publicationActiveValidationStaleBaselineInventory(t)
	decision, err := validatePublicationActiveCandidate(t.Context(), input)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("stale baseline error = %v, want dependency conflict", err)
	}
	if !decision.IsZero() {
		t.Fatal("stale baseline was converted into a candidate conflict decision")
	}

	input.currentActive[1].existingDependencies = []publicationPersistedDependency{{
		ordinal:        0,
		targetObjectID: "ko-validation-baseline-target",
		targetVersion:  1,
		role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
	}}
	input.expectedDependencyCount = 1
	decision, err = validatePublicationActiveCandidate(t.Context(), input)
	if err != nil {
		t.Fatalf("repaired baseline validation: %v", err)
	}
	if conflict, present := decision.conflict(); !present || conflict != publicationActiveValidationCandidatePostConflict {
		t.Fatalf("repaired baseline conflict = %d/%t", conflict, present)
	}
}

func TestValidatePublicationActiveCandidateRejectsGloballyMissingPersistedTarget(t *testing.T) {
	input := publicationActiveValidationTargetAbsentInventory(t)
	input.currentActive = input.currentActive[1:]
	input.expectedCurrentActiveCount--
	canonical, err := canonicalizePublicationTransitionObject(input.currentActive[0], false)
	if err != nil {
		t.Fatalf("canonicalize retained source: %v", err)
	}
	input.expectedDefinitionBytes = canonical.canonical.definitionBytes
	input.expectedProjectionBytes = canonical.projectionBytes
	input.expectedSelectorPatterns = canonical.selectorPatterns
	input.expectedSelectorValueBytes = canonical.selectorValueBytes
	input.expectedCanonicalSelectorBytes = canonical.canonicalSelectorBytes
	input.expectedSelectorWork = canonical.canonical.selectorWork
	input.expectedDependencyCount = 1

	decision, err := validatePublicationActiveCandidate(t.Context(), input)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("globally missing target error = %v, want ErrCorrupt", err)
	}
	if !decision.IsZero() {
		t.Fatal("globally missing target became a candidate conflict")
	}
}

func TestValidatePublicationActiveCandidateAlphaRenameLowMidHigh(t *testing.T) {
	identities := []string{"a-validation-candidate", "ko-validation-candidate", "z-validation-candidate"}
	var wantProjection []publicationDependency
	var wantTargets []publicationDerivedDependencyTarget
	for index, identity := range identities {
		decision, err := validatePublicationActiveCandidate(
			t.Context(),
			publicationActiveValidationDependencyInventory(t, identity),
		)
		if err != nil {
			t.Fatalf("alpha identity %q: %v", identity, err)
		}
		dependencies, present := decision.candidateDependencies()
		if !present {
			t.Fatalf("alpha identity %q omitted dependency authority", identity)
		}
		if index == 0 {
			wantProjection = dependencies.databaseProjection()
			wantTargets = dependencies.derivedTargets()
			continue
		}
		if !slices.Equal(wantProjection, dependencies.databaseProjection()) ||
			!slices.Equal(wantTargets, dependencies.derivedTargets()) {
			t.Fatalf("alpha identity %q changed target authority", identity)
		}
	}
}

func TestValidatePublicationActiveCandidateAlphaRenameProperty(t *testing.T) {
	var want []publicationDependency
	for index := 0; index < 48; index++ {
		identity := fmt.Sprintf("%c-validation-%02d", 'a'+rune(index%26), index)
		decision, err := validatePublicationActiveCandidate(
			t.Context(),
			publicationActiveValidationDependencyInventory(t, identity),
		)
		if err != nil {
			t.Fatalf("alpha property identity %q: %v", identity, err)
		}
		dependencies, present := decision.candidateDependencies()
		if !present {
			t.Fatalf("alpha property identity %q omitted authority", identity)
		}
		projection := dependencies.databaseProjection()
		if index == 0 {
			want = projection
			continue
		}
		if !slices.Equal(want, projection) {
			t.Fatalf("alpha property identity %q changed projection", identity)
		}
	}
}

func FuzzValidatePublicationActiveCandidateAlphaRename(f *testing.F) {
	f.Add([]byte("alpha"))
	f.Add([]byte{0, 1, 2, 0xff})
	f.Fuzz(func(t *testing.T, seed []byte) {
		digest := sha256.Sum256(seed)
		low := fmt.Sprintf("a-%x", digest[:12])
		high := fmt.Sprintf("z-%x", digest[:12])
		left, err := validatePublicationActiveCandidate(
			t.Context(),
			publicationActiveValidationDependencyInventory(t, low),
		)
		if err != nil {
			t.Fatalf("low alpha validation: %v", err)
		}
		right, err := validatePublicationActiveCandidate(
			t.Context(),
			publicationActiveValidationDependencyInventory(t, high),
		)
		if err != nil {
			t.Fatalf("high alpha validation: %v", err)
		}
		leftDependencies, leftPresent := left.candidateDependencies()
		rightDependencies, rightPresent := right.candidateDependencies()
		if !leftPresent || !rightPresent ||
			!slices.Equal(leftDependencies.databaseProjection(), rightDependencies.databaseProjection()) ||
			!slices.Equal(leftDependencies.derivedTargets(), rightDependencies.derivedTargets()) {
			t.Fatal("alpha rename changed validation target authority")
		}
	})
}

func TestPublicationActiveValidationBaselineAndPostShareSemanticBudget(t *testing.T) {
	evaluator := publicationTransitionEvaluator{
		ctx:                  t.Context(),
		seenBaselineCohorts:  make(map[[sha256.Size]byte][]publicationTransitionValidatedCohort),
		winnerKeyCommitments: make(map[*publicationTransitionCanonicalObject][sha256.Size]byte),
	}
	for index := uint64(0); index < maximumPublicationTransitionSemanticPrograms-1; index++ {
		winner := publicationActiveValidationBudgetWinner(t, index)
		if err := evaluator.validateBaselineCohort([]*publicationTransitionCanonicalObject{winner}); err != nil {
			t.Fatalf("baseline cohort %d: %v", index, err)
		}
	}
	post := publicationActiveValidationBudgetWinner(
		t,
		maximumPublicationTransitionSemanticPrograms-1,
	)
	if err := evaluator.work.chargeChangedCohort([]*publicationTransitionCanonicalObject{post}); err != nil {
		t.Fatalf("last shared semantic slot: %v", err)
	}
	over := publicationActiveValidationBudgetWinner(
		t,
		maximumPublicationTransitionSemanticPrograms,
	)
	if err := evaluator.validateBaselineCohort(
		[]*publicationTransitionCanonicalObject{over},
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("shared semantic overflow = %v, want capacity", err)
	}
}

func TestPublicationActiveValidationDeduplicatesRepeatedTargetAbsentCohort(t *testing.T) {
	input := publicationActiveValidationTargetAbsentInventory(t)
	target, err := canonicalizePublicationTransitionObject(input.currentActive[0], false)
	if err != nil {
		t.Fatalf("canonicalize target: %v", err)
	}
	source, err := canonicalizePublicationTransitionObject(input.currentActive[1], false)
	if err != nil {
		t.Fatalf("canonicalize source: %v", err)
	}
	candidate, err := canonicalizePublicationTransitionObject(
		input.candidateAfter.winner,
		true,
	)
	if err != nil {
		t.Fatalf("canonicalize candidate: %v", err)
	}

	var signature publicationIndexORSignature
	publicationTransitionSetMembership(&signature.before, 0)
	publicationTransitionSetMembership(&signature.before, 1)
	publicationTransitionSetMembership(&signature.after, 0)
	publicationTransitionSetMembership(&signature.after, 1)
	publicationTransitionSetMembership(&signature.after, 2)
	signatures := make([]publicationIndexORSignature, 96)
	for index := range signatures {
		signatures[index] = signature
	}
	evaluator := publicationTransitionEvaluator{
		ctx:        t.Context(),
		validation: true,
		classes: []publicationTransitionPrincipalClass{{
			kind:  publicationTransitionGenericPrincipal,
			appID: "app-a",
		}},
		signatures:           signatures,
		preSlots:             []*publicationTransitionCanonicalObject{target, source, nil},
		postSlots:            []*publicationTransitionCanonicalObject{target, source, candidate},
		candidateAfter:       candidate,
		postCandidate:        publicationCandidateAuthorityFromCanonical(candidate.canonical),
		afterActive:          true,
		classHydration:       []publicationTransitionClassHydration{{}},
		semanticHasher:       sha256.New(),
		seenChangedCohorts:   make(map[[sha256.Size]byte][]publicationTransitionValidatedCohort),
		seenBaselineCohorts:  make(map[[sha256.Size]byte][]publicationTransitionValidatedCohort),
		winnerKeyCommitments: make(map[*publicationTransitionCanonicalObject][sha256.Size]byte),
	}
	dependencies, witnesses, conflict, err := evaluator.evaluate()
	if err != nil {
		t.Fatalf("repeated target-absent evaluation: %v", err)
	}
	if conflict != publicationActiveValidationCohortDependencyTargetAbsent ||
		witnesses != uint64(len(signatures)) || !dependencies.IsZero() {
		t.Fatalf("repeated target-absent result = deps-zero:%t witnesses:%d conflict:%d", dependencies.IsZero(), witnesses, conflict)
	}
	if evaluator.work.semanticPrograms != 2 {
		t.Fatalf("repeated target-absent semantic programs = %d, want baseline+post", evaluator.work.semanticPrograms)
	}
}

func TestValidatePublicationActiveCandidateHonorsCancellation(t *testing.T) {
	preCanceled, cancel := context.WithCancel(t.Context())
	cancel()
	decision, err := validatePublicationActiveCandidate(
		preCanceled,
		publicationActiveValidationDependencyInventory(t, "ko-validation-canceled"),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled validation error = %v, want context canceled", err)
	}
	if !decision.IsZero() {
		t.Fatal("canceled validation returned a decision")
	}

	base, cancelMidway := context.WithCancel(t.Context())
	midway := &publicationIndexCancelContext{
		Context:  base,
		cancel:   cancelMidway,
		cancelAt: 4,
	}
	decision, err = validatePublicationActiveCandidate(
		midway,
		publicationActiveValidationDependencyInventory(t, "ko-validation-canceled-midway"),
	)
	if !errors.Is(err, context.Canceled) || midway.calls < midway.cancelAt {
		t.Fatalf("mid-validation cancellation = %v after %d calls", err, midway.calls)
	}
	if !decision.IsZero() {
		t.Fatal("mid-validation cancellation returned a decision")
	}
}

func publicationActiveValidationZeroDependencyInventory(
	t *testing.T,
	candidateID string,
) publicationActiveTransitionInventory {
	t.Helper()
	candidate := publicationWinner{object: publicationTestObject(
		t,
		candidateID,
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "validation-zero", SharingScopeApp, nil, "", "validation_zero",
		), "main"),
	)}
	return publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationDependencyInventory(
	t *testing.T,
	candidateID string,
) publicationActiveTransitionInventory {
	t.Helper()
	target := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-target",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "validation-target", SharingScopeApp, nil, "", "validation_input",
			), "main"),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		candidateID,
		1,
		publicationTransitionTestIndexDefinition(dependencyAliasDefinition(
			"app-a", "validation-source", SharingScopeApp, nil, "", "validation_input", "validation_output",
		), "main"),
	)}
	return publicationTransitionTestInventory(
		t,
		[]publicationWinner{target},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationSemanticConflictInventory(
	t *testing.T,
) publicationActiveTransitionInventory {
	t.Helper()
	existing := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-existing-output",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "existing-output", SharingScopeApp, nil, "", "duplicate_output",
			), "main"),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-conflicting-output",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "conflicting-output", SharingScopeApp, nil, "", "duplicate_output",
		), "main"),
	)}
	return publicationTransitionTestInventory(
		t,
		[]publicationWinner{existing},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationTargetAbsentInventory(
	t *testing.T,
) publicationActiveTransitionInventory {
	t.Helper()
	target := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-topology-target",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "validation-topology-slot", SharingScopeGlobal, nil, "", "topology_input",
			), "main"),
		),
		existingDependenciesPresent: true,
	}
	source := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-topology-source",
			1,
			publicationTransitionTestIndexDefinition(dependencyAliasDefinition(
				"app-a", "validation-topology-source", SharingScopeApp, nil, "", "topology_input", "topology_output",
			), "main"),
		),
		existingDependenciesPresent: true,
		existingDependencies: []publicationPersistedDependency{{
			ordinal:        0,
			targetObjectID: target.object.KnowledgeObjectID,
			targetVersion:  int64(target.object.Version),
			role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}},
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-topology-shadow",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "validation-topology-slot", SharingScopeApp, nil, "", "replacement_input",
		), "main"),
	)}
	return publicationTransitionTestInventory(
		t,
		[]publicationWinner{target, source},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationNoWitnessInventory(
	t *testing.T,
) publicationActiveTransitionInventory {
	t.Helper()
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-no-witness",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "validation-no-witness", SharingScopeApp, nil, "", "no_witness_output",
		), "archive-*"),
	)}
	return publicationTransitionTestInventory(
		t,
		nil,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationStaleBaselineInventory(
	t *testing.T,
) publicationActiveTransitionInventory {
	t.Helper()
	target := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-baseline-target",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "validation-baseline-target", SharingScopeApp, nil, "", "baseline_input",
			), "main"),
		),
		existingDependenciesPresent: true,
	}
	source := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-baseline-source",
			1,
			publicationTransitionTestIndexDefinition(dependencyAliasDefinition(
				"app-a", "validation-baseline-source", SharingScopeApp, nil, "", "baseline_input", "baseline_output",
			), "main"),
		),
		existingDependenciesPresent: true,
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-baseline-conflict",
		1,
		publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
			"app-a", "validation-baseline-conflict", SharingScopeApp, nil, "", "baseline_input",
		), "main"),
	)}
	return publicationTransitionTestInventory(
		t,
		[]publicationWinner{target, source},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"main"},
	)
}

func publicationActiveValidationMixedConflictInventory(
	t *testing.T,
	absentIndex string,
	conflictIndex string,
) publicationActiveTransitionInventory {
	t.Helper()
	target := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-mixed-target",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "validation-mixed-slot", SharingScopeGlobal, nil, "", "mixed_input",
			), "*"),
		),
		existingDependenciesPresent: true,
	}
	source := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-mixed-source",
			1,
			publicationTransitionTestIndexDefinition(dependencyAliasDefinition(
				"app-a", "validation-mixed-source", SharingScopeApp, nil, "", "mixed_input", "mixed_output",
			), absentIndex),
		),
		existingDependenciesPresent: true,
		existingDependencies: []publicationPersistedDependency{{
			ordinal:        0,
			targetObjectID: target.object.KnowledgeObjectID,
			targetVersion:  int64(target.object.Version),
			role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}},
	}
	conflicting := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-validation-mixed-conflicting",
			1,
			publicationTransitionTestIndexDefinition(dependencyExtractionDefinition(
				"app-a", "validation-mixed-conflicting", SharingScopeApp, nil, "", "replacement_input",
			), conflictIndex),
		),
		existingDependenciesPresent: true,
	}
	candidateDefinition := dependencyExtractionDefinition(
		"app-a", "validation-mixed-slot", SharingScopeApp, nil, "", "replacement_input",
	)
	candidateDefinition.Selector = &opensplunkv1.KnowledgeSelector{
		IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{
			{Value: absentIndex},
			{Value: conflictIndex},
		},
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-validation-mixed-candidate",
		1,
		candidateDefinition,
	)}
	return publicationTransitionTestInventory(
		t,
		[]publicationWinner{target, source, conflicting},
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
		[]string{"app-a"},
		[]string{"one", "two"},
	)
}

func publicationActiveValidationBudgetWinner(
	t *testing.T,
	ordinal uint64,
) *publicationTransitionCanonicalObject {
	t.Helper()
	winner := publicationWinner{
		object: publicationTestObject(
			t,
			fmt.Sprintf("ko-validation-budget-%03d", ordinal),
			1,
			dependencyExtractionDefinition(
				"app-a",
				fmt.Sprintf("validation-budget-%03d", ordinal),
				SharingScopeApp,
				nil,
				"",
				fmt.Sprintf("validation_budget_%03d", ordinal),
			),
		),
		existingDependenciesPresent: true,
	}
	canonical, err := canonicalizePublicationTransitionObject(winner, false)
	if err != nil {
		t.Fatalf("canonicalize semantic budget winner %d: %v", ordinal, err)
	}
	return canonical
}
