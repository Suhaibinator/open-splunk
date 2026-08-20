package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestValidatePublicationExistingWinnerCohortCandidateAbsentMode(t *testing.T) {
	t.Run("parity with a real nonwinning candidate", func(t *testing.T) {
		cohort := publicationTestExistingChain(t)
		commitment, err := validatePublicationExistingWinnerCohort(t.Context(), cohort)
		if err != nil {
			t.Fatalf("validate existing-only cohort: %v", err)
		}
		if commitment == ([32]byte{}) {
			t.Fatal("existing-only cohort returned an absent program commitment")
		}

		present, err := validatePublicationWinnerCohort(
			t.Context(),
			cohort,
			publicationTestAbsentCandidate(t),
			false,
		)
		if err != nil {
			t.Fatalf("validate candidate-present parity cohort: %v", err)
		}
		presentCommitment, ok := present.programCommitment()
		if !ok || presentCommitment != commitment {
			t.Fatalf(
				"candidate-free commitment = %x, candidate-present = %x/%t",
				commitment,
				presentCommitment,
				ok,
			)
		}
	})

	t.Run("no sentinel identity collision", func(t *testing.T) {
		winner := publicationWinner{
			object: publicationTestObject(
				t,
				"ko-transition-absent",
				13,
				dependencyExtractionDefinition(
					"app-a",
					"transition-absent",
					SharingScopeApp,
					nil,
					"",
					"transition_absent_output",
				),
			),
			existingDependenciesPresent: true,
		}
		commitment, err := validatePublicationExistingWinnerCohort(
			t.Context(),
			publicationWinnerCohort{
				expectedWinnerCount: 1,
				winners:             []publicationWinner{winner},
			},
		)
		if err != nil || commitment == ([32]byte{}) {
			t.Fatalf("sentinel-shaped existing winner = %x, %v", commitment, err)
		}
	})

	t.Run("candidate-present winner path is unchanged", func(t *testing.T) {
		cohort, candidate := publicationTestChain(t)
		compiled, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
		if err != nil {
			t.Fatalf("compile candidate-present cohort: %v", err)
		}
		validated, err := validatePublicationWinnerCohort(
			t.Context(),
			cohort,
			candidate,
			true,
		)
		if err != nil {
			t.Fatalf("validate candidate-present cohort: %v", err)
		}
		binding, wins, present := validated.candidateBinding()
		if !present || !wins || binding != candidate {
			t.Fatalf("candidate binding = %#v/%t/%t, want %#v/true/true", binding, wins, present, candidate)
		}
		if !validated.candidateDependencies().Equal(compiled) {
			t.Fatal("candidate-present compile and validation authority differ")
		}
	})
}

func TestPublicationTransitionSemanticProgramLimit(t *testing.T) {
	var work publicationTransitionWork
	for program := range maximumPublicationTransitionSemanticPrograms {
		if err := work.chargeChangedCohort(nil); err != nil {
			t.Fatalf("charge semantic program %d at exact limit: %v", program, err)
		}
	}
	if work.semanticPrograms != maximumPublicationTransitionSemanticPrograms {
		t.Fatalf(
			"semantic programs = %d, want %d",
			work.semanticPrograms,
			maximumPublicationTransitionSemanticPrograms,
		)
	}
	if err := work.chargeChangedCohort(nil); !errors.Is(
		err,
		control.ErrCapacityExceeded,
	) || !strings.Contains(err.Error(), "semantic-program limit") {
		t.Fatalf("semantic program +1 error = %v, want semantic-program capacity exceeded", err)
	}
}

func TestPublicationTransitionSemanticProgramLimitPropagates(t *testing.T) {
	t.Run("candidate-free index admission", func(t *testing.T) {
		exact := publicationIndexAdmissionSemanticProgramInventory(t, 6)
		if authority, err := validatePublicationIndexNameAdmission(t.Context(), exact); err != nil || authority.IsZero() {
			t.Fatalf("exact 64-program admission = %#v, %v", authority, err)
		}

		over := publicationIndexAdmissionSemanticProgramInventory(t, 7)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), over); !errors.Is(
			err,
			control.ErrCapacityExceeded,
		) || !strings.Contains(err.Error(), "semantic-program limit") {
			t.Fatalf("65th admission program error = %v, want semantic-program capacity exceeded", err)
		}
	})

	t.Run("candidate-present publication transition", func(t *testing.T) {
		exact := publicationIndexAdmissionSemanticProgramTransition(t, 6)
		if authority, err := validatePublicationActiveTransition(t.Context(), exact); err != nil || authority.IsZero() {
			t.Fatalf("exact 64-program transition = %#v, %v", authority, err)
		}

		over := publicationIndexAdmissionSemanticProgramTransition(t, 7)
		if _, err := validatePublicationActiveTransition(t.Context(), over); !errors.Is(
			err,
			control.ErrCapacityExceeded,
		) || !strings.Contains(err.Error(), "semantic-program limit") {
			t.Fatalf("65th transition program error = %v, want semantic-program capacity exceeded", err)
		}
	})
}

func TestValidatePublicationIndexNameAdmissionSharesBatchBudget(t *testing.T) {
	exactInputs := publicationIndexAdmissionBatchInputs(t, "audit", "audit")
	wildcardInputs := publicationIndexAdmissionBatchInputs(t, "audit-*", "audit-prod")

	t.Run("fresh budget preserves semantic commitment", func(t *testing.T) {
		want, err := validatePublicationIndexNameAdmission(t.Context(), exactInputs[0])
		if err != nil || want.IsZero() {
			t.Fatalf("fresh wrapper authority = %#v, %v", want, err)
		}
		var budget publicationIndexNameAdmissionBatchBudget
		got, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(),
			exactInputs[0],
			&budget,
		)
		if err != nil || got.IsZero() {
			t.Fatalf("explicit fresh-budget authority = %#v, %v", got, err)
		}
		if !got.Equal(want) {
			t.Fatalf("fresh-budget authority drifted: got %#v, want %#v", got, want)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(),
			exactInputs[0],
			nil,
		); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("nil batch budget error = %v, want InvalidArgument", err)
		}
	})

	exactCharge := publicationIndexAdmissionMeasureBatchCharge(t, exactInputs[0])
	wildcardCharge := publicationIndexAdmissionMeasureBatchCharge(t, wildcardInputs[0])
	if exactCharge.classStates == 0 ||
		exactCharge.work.semanticPrograms == 0 ||
		exactCharge.work.changedCohorts == 0 ||
		exactCharge.work.membershipVisits == 0 ||
		exactCharge.closure.probes == 0 ||
		exactCharge.closure.wordOperations == 0 {
		t.Fatalf("exact fixture omitted a required batch charge: %#v", exactCharge)
	}
	if wildcardCharge.matcher.selectorProbes == 0 ||
		wildcardCharge.matcher.matcherWork == 0 {
		t.Fatalf("wildcard fixture omitted matcher charges: %#v", wildcardCharge.matcher)
	}

	t.Run("wildcard probe exact across two tenants and plus one", func(t *testing.T) {
		charge := wildcardCharge.matcher.selectorProbes
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.matcher.selectorProbes = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionIndexSelectorProbes,
			charge,
		)
		publicationIndexAdmissionValidateBatchTenants(t, wildcardInputs[:2], &budget)
		if budget.matcher.selectorProbes != maximumPublicationTransitionIndexSelectorProbes {
			t.Fatalf("exact shared wildcard probes = %d, want %d", budget.matcher.selectorProbes, maximumPublicationTransitionIndexSelectorProbes)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), wildcardInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "wildcard index probe limit") {
			t.Fatalf("third-tenant wildcard probe error = %v, want probe CapacityExceeded", err)
		}
	})

	t.Run("wildcard matcher work exact across two tenants and plus one", func(t *testing.T) {
		charge := wildcardCharge.matcher.matcherWork
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.matcher.matcherWork = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionIndexMatcherWork,
			charge,
		)
		publicationIndexAdmissionValidateBatchTenants(t, wildcardInputs[:2], &budget)
		if budget.matcher.matcherWork != maximumPublicationTransitionIndexMatcherWork {
			t.Fatalf("exact shared wildcard work = %d, want %d", budget.matcher.matcherWork, maximumPublicationTransitionIndexMatcherWork)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), wildcardInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "wildcard matcher work limit") {
			t.Fatalf("third-tenant wildcard work error = %v, want matcher-work CapacityExceeded", err)
		}
	})

	t.Run("closure work exact across two tenants and plus one", func(t *testing.T) {
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.closure.probes = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationIndexClosureProbes,
			exactCharge.closure.probes,
		)
		budget.closure.wordOperations = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationIndexClosureWordOperations,
			exactCharge.closure.wordOperations,
		)
		publicationIndexAdmissionValidateBatchTenants(t, exactInputs[:2], &budget)
		if budget.closure.probes != maximumPublicationIndexClosureProbes ||
			budget.closure.wordOperations != maximumPublicationIndexClosureWordOperations {
			t.Fatalf("exact shared closure work = %#v", budget.closure)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), exactInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "publication index closure exceeds its work limit") {
			t.Fatalf("third-tenant closure error = %v, want work CapacityExceeded", err)
		}
	})

	t.Run("class states exact across two tenants and plus one", func(t *testing.T) {
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.classStates = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionClassStates,
			exactCharge.classStates,
		)
		publicationIndexAdmissionValidateBatchTenants(t, exactInputs[:2], &budget)
		if budget.classStates != maximumPublicationTransitionClassStates {
			t.Fatalf("exact shared class states = %d, want %d", budget.classStates, maximumPublicationTransitionClassStates)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), exactInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "batch exceeds its visibility-state limit") {
			t.Fatalf("third-tenant class-state error = %v, want batch CapacityExceeded", err)
		}
	})

	t.Run("semantic programs exact across two tenants and plus one", func(t *testing.T) {
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.work.semanticPrograms = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionSemanticPrograms,
			exactCharge.work.semanticPrograms,
		)
		publicationIndexAdmissionValidateBatchTenants(t, exactInputs[:2], &budget)
		if budget.work.semanticPrograms != maximumPublicationTransitionSemanticPrograms {
			t.Fatalf("exact shared semantic programs = %d, want %d", budget.work.semanticPrograms, maximumPublicationTransitionSemanticPrograms)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), exactInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "semantic-program limit") {
			t.Fatalf("third-tenant semantic-program error = %v, want CapacityExceeded", err)
		}
	})

	t.Run("changed cohorts exact across two tenants and plus one", func(t *testing.T) {
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.work.changedCohorts = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionChangedCohorts,
			exactCharge.work.changedCohorts,
		)
		publicationIndexAdmissionValidateBatchTenants(t, exactInputs[:2], &budget)
		if budget.work.changedCohorts != maximumPublicationTransitionChangedCohorts {
			t.Fatalf("exact shared changed cohorts = %d, want %d", budget.work.changedCohorts, maximumPublicationTransitionChangedCohorts)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), exactInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "changed-cohort limit") {
			t.Fatalf("third-tenant changed-cohort error = %v, want CapacityExceeded", err)
		}
	})

	t.Run("membership work exact across two tenants and plus one", func(t *testing.T) {
		budget := publicationIndexNameAdmissionBatchBudget{}
		budget.work.membershipVisits = publicationIndexAdmissionBatchStart(
			t,
			maximumPublicationTransitionMembershipVisits,
			exactCharge.work.membershipVisits,
		)
		publicationIndexAdmissionValidateBatchTenants(t, exactInputs[:2], &budget)
		if budget.work.membershipVisits != maximumPublicationTransitionMembershipVisits {
			t.Fatalf("exact shared membership visits = %d, want %d", budget.work.membershipVisits, maximumPublicationTransitionMembershipVisits)
		}
		if _, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(), exactInputs[2], &budget,
		); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "candidate-membership work limit") {
			t.Fatalf("third-tenant membership-work error = %v, want CapacityExceeded", err)
		}
	})
}

func TestValidatePublicationIndexNameAdmissionMatchingModes(t *testing.T) {
	tests := []struct {
		name       string
		pattern    string
		newName    string
		objectName string
	}{
		{name: "nonmatch", pattern: "main", newName: "audit", objectName: "main-only"},
		{name: "exact", pattern: "audit", newName: "audit", objectName: "exact-audit"},
		{name: "wildcard", pattern: "audit-*", newName: "audit-prod", objectName: "wildcard-audit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			winner := publicationIndexAdmissionTestExtraction(
				t,
				"ko-"+test.name,
				1,
				"app-a",
				"owner-a",
				SharingScopeApp,
				test.objectName,
				test.pattern,
				test.name+"_output",
			)
			input := publicationIndexAdmissionTestInventory(
				t,
				[]publicationWinner{winner},
				[]string{"app-a"},
				[]string{"main"},
				test.newName,
			)
			authority, err := validatePublicationIndexNameAdmission(t.Context(), input)
			if err != nil {
				t.Fatalf("validate admission: %v", err)
			}
			if authority.IsZero() {
				t.Fatal("successful admission returned zero authority")
			}
		})
	}
}

func TestPublicationIndexNameAdmissionCandidateAtomIsAbsentBefore(t *testing.T) {
	winners := []publicationWinner{
		publicationIndexAdmissionTestExtraction(
			t, "ko-future-exact", 1, "app-a", "owner-a", SharingScopeApp,
			"future-exact", "future", "future_exact_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-future-wildcard", 1, "app-a", "owner-a", SharingScopeApp,
			"future-wildcard", "future*", "future_wildcard_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-main", 1, "app-a", "owner-a", SharingScopeApp,
			"main", "main", "main_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-other", 1, "app-a", "owner-a", SharingScopeApp,
			"other", "other", "other_output",
		),
		{
			object: publicationTestObject(
				t,
				"ko-all-indexes",
				1,
				dependencyExtractionDefinition(
					"app-a",
					"all-indexes",
					SharingScopeApp,
					nil,
					"",
					"all_indexes_output",
				),
			),
			existingDependenciesPresent: true,
		},
	}
	slots := make([]*publicationTransitionCanonicalObject, len(winners))
	for index, winner := range winners {
		canonical, err := canonicalizePublicationTransitionObject(winner, false)
		if err != nil {
			t.Fatalf("canonicalize atom fixture %d: %v", index, err)
		}
		slots[index] = canonical
	}
	atoms, err := publicationIndexNameAdmissionAtoms(
		t.Context(),
		[]string{"other", "main"},
		"future",
		slots,
	)
	if err != nil {
		t.Fatalf("build index-name admission atoms: %v", err)
	}
	if len(atoms) != 3 {
		t.Fatalf("atom count = %d, want 3", len(atoms))
	}
	if atoms[0].before != (publicationIndexMembership{}) ||
		atoms[0].after != publicationIndexTestBits(0, 1, 4) {
		t.Fatalf("future atom = %#v -> %#v, want 0 -> {0,1,4}", atoms[0].before, atoms[0].after)
	}
	if atoms[1].before != publicationIndexTestBits(2, 4) ||
		atoms[1].after != publicationIndexTestBits(2, 4) {
		t.Fatalf("main atom = %#v -> %#v, want {2,4} -> {2,4}", atoms[1].before, atoms[1].after)
	}
	if atoms[2].before != publicationIndexTestBits(3, 4) ||
		atoms[2].after != publicationIndexTestBits(3, 4) {
		t.Fatalf("other atom = %#v -> %#v, want {3,4} -> {3,4}", atoms[2].before, atoms[2].after)
	}
}

func TestValidatePublicationIndexNameAdmissionRejectsNewOnlyPrecedenceConflict(t *testing.T) {
	winners := []publicationWinner{
		publicationIndexAdmissionTestExtraction(
			t, "ko-new-only-a", 1, "app-a", "owner-a", SharingScopeApp,
			"dormant-slot", "audit", "new_only_a",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-new-only-b", 1, "app-a", "owner-b", SharingScopeApp,
			"dormant-slot", "audit", "new_only_b",
		),
	}
	input := publicationIndexAdmissionTestInventory(
		t,
		winners,
		[]string{"app-a"},
		[]string{"main"},
		"audit",
	)
	if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("new-only precedence error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationIndexNameAdmissionRejectsMultiIndexOnlyConflict(t *testing.T) {
	winners := []publicationWinner{
		publicationIndexAdmissionTestExtraction(
			t,
			"ko-existing-output",
			1,
			"app-a",
			"owner-a",
			SharingScopeApp,
			"existing-output",
			"*prod",
			"shared_output",
		),
		publicationIndexAdmissionTestExtraction(
			t,
			"ko-new-output",
			1,
			"app-a",
			"owner-a",
			SharingScopeApp,
			"new-output",
			"main*",
			"shared_output",
		),
	}
	input := publicationIndexAdmissionTestInventory(
		t,
		winners,
		[]string{"app-a"},
		[]string{"stage-prod"},
		"main-dev",
	)
	if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("old+new OR-only conflict error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationIndexNameAdmissionRejectsDependencyDrift(t *testing.T) {
	target := publicationIndexAdmissionTestExtraction(
		t,
		"ko-admission-target",
		3,
		"app-a",
		"owner-a",
		SharingScopeApp,
		"target",
		"*",
		"derived_input",
	)
	source := publicationWinner{
		object: publicationTestObject(
			t,
			"ko-admission-source",
			7,
			publicationTransitionTestIndexDefinition(
				dependencyAliasDefinition(
					"app-a",
					"source",
					SharingScopeApp,
					nil,
					"",
					"derived_input",
					"derived_output",
				),
				"audit",
			),
		),
		existingDependenciesPresent: true,
	}
	exactSource := publicationCloneWinner(source)
	exactSource.existingDependencies = []publicationPersistedDependency{{
		ordinal:        0,
		targetObjectID: "ko-admission-target",
		targetVersion:  3,
		role: opensplunk.
			KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
	}}
	exactInput := publicationIndexAdmissionTestInventory(
		t,
		[]publicationWinner{exactSource, target},
		[]string{"app-a"},
		[]string{"main"},
		"audit",
	)
	if authority, err := validatePublicationIndexNameAdmission(t.Context(), exactInput); err != nil || authority.IsZero() {
		t.Fatalf("exact persisted dependency admission = %#v, %v", authority, err)
	}

	input := publicationIndexAdmissionTestInventory(
		t,
		[]publicationWinner{source, target},
		[]string{"app-a"},
		[]string{"main"},
		"audit",
	)
	if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
		err,
		control.ErrDependencyConflict,
	) {
		t.Fatalf("dependency drift error = %v, want dependency conflict", err)
	}
}

func TestValidatePublicationIndexNameAdmissionDependencyClosureTaxonomy(t *testing.T) {
	t.Run("new topology excludes a durable target", func(t *testing.T) {
		globalTarget := publicationIndexAdmissionTestExtraction(
			t,
			"ko-topology-global-target",
			3,
			"app-a",
			"owner-a",
			SharingScopeGlobal,
			"target-slot",
			"*",
			"topology_input",
		)
		appShadow := publicationIndexAdmissionTestExtraction(
			t,
			"ko-topology-app-shadow",
			5,
			"app-a",
			"owner-a",
			SharingScopeApp,
			"target-slot",
			"audit",
			"topology_input",
		)
		source := publicationWinner{
			object: publicationTestObject(
				t,
				"ko-topology-source",
				7,
				publicationTransitionTestIndexDefinition(
					dependencyAliasDefinition(
						"app-a",
						"topology-source",
						SharingScopeApp,
						nil,
						"",
						"topology_input",
						"topology_output",
					),
					"*",
				),
			),
			existingDependenciesPresent: true,
			existingDependencies: []publicationPersistedDependency{{
				ordinal:        0,
				targetObjectID: "ko-topology-global-target",
				targetVersion:  3,
				role: opensplunk.
					KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}},
		}
		input := publicationIndexAdmissionTestInventory(
			t,
			[]publicationWinner{source, appShadow, globalTarget},
			[]string{"app-a"},
			[]string{"main"},
			"audit",
		)
		_, err := validatePublicationIndexNameAdmission(t.Context(), input)
		if !errors.Is(err, control.ErrDependencyConflict) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("cohort-local missing target error = %v, want dependency conflict only", err)
		}
	})

	t.Run("globally missing durable target remains corrupt", func(t *testing.T) {
		source := publicationWinner{
			object: publicationTestObject(
				t,
				"ko-globally-invalid-source",
				1,
				publicationTransitionTestIndexDefinition(
					dependencyAliasDefinition(
						"app-a",
						"globally-invalid-source",
						SharingScopeApp,
						nil,
						"",
						"missing_input",
						"missing_output",
					),
					"audit",
				),
			),
			existingDependenciesPresent: true,
			existingDependencies: []publicationPersistedDependency{{
				ordinal:        0,
				targetObjectID: "ko-globally-missing-target",
				targetVersion:  1,
				role: opensplunk.
					KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}},
		}
		input := publicationIndexAdmissionTestInventory(
			t,
			[]publicationWinner{source},
			[]string{"app-a"},
			[]string{"main"},
			"audit",
		)
		_, err := validatePublicationIndexNameAdmission(t.Context(), input)
		if !errors.Is(err, ErrCorrupt) || errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("globally missing target error = %v, want corrupt only", err)
		}
	})

	t.Run("malformed durable row remains corrupt", func(t *testing.T) {
		target := publicationIndexAdmissionTestExtraction(
			t,
			"ko-malformed-target",
			1,
			"app-a",
			"owner-a",
			SharingScopeApp,
			"malformed-target",
			"audit",
			"malformed_input",
		)
		source := publicationWinner{
			object: publicationTestObject(
				t,
				"ko-malformed-source",
				1,
				publicationTransitionTestIndexDefinition(
					dependencyAliasDefinition(
						"app-a",
						"malformed-source",
						SharingScopeApp,
						nil,
						"",
						"malformed_input",
						"malformed_output",
					),
					"audit",
				),
			),
			existingDependenciesPresent: true,
			existingDependencies: []publicationPersistedDependency{{
				ordinal:        0,
				targetObjectID: "ko-malformed-target",
				targetVersion:  1,
				role: opensplunk.
					KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}},
		}
		input := publicationIndexAdmissionTestInventory(
			t,
			[]publicationWinner{source, target},
			[]string{"app-a"},
			[]string{"main"},
			"audit",
		)
		input.currentActive[0].existingDependencies[0].ordinal = 1
		_, err := validatePublicationIndexNameAdmission(t.Context(), input)
		if !errors.Is(err, ErrCorrupt) || errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("malformed dependency row error = %v, want corrupt only", err)
		}
	})
}

func TestValidatePublicationIndexNameAdmissionPrincipalClasses(t *testing.T) {
	t.Run("private owners remain isolated", func(t *testing.T) {
		winners := []publicationWinner{
			publicationIndexAdmissionTestExtraction(
				t, "ko-private-a", 1, "app-a", "owner-a", SharingScopePrivate,
				"private-slot", "audit", "private_a",
			),
			publicationIndexAdmissionTestExtraction(
				t, "ko-private-b", 1, "app-a", "owner-b", SharingScopePrivate,
				"private-slot", "audit", "private_b",
			),
		}
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-a"}, []string{"main"}, "audit",
		)
		if authority, err := validatePublicationIndexNameAdmission(t.Context(), input); err != nil || authority.IsZero() {
			t.Fatalf("private-principal admission = %#v, %v", authority, err)
		}
	})

	t.Run("same private principal rejects a tie", func(t *testing.T) {
		winners := []publicationWinner{
			publicationIndexAdmissionTestExtraction(
				t, "ko-private-tie-a", 1, "app-a", "owner-a", SharingScopePrivate,
				"private-slot", "audit", "private_a",
			),
			publicationIndexAdmissionTestExtraction(
				t, "ko-private-tie-b", 1, "app-a", "owner-a", SharingScopePrivate,
				"private-slot", "audit", "private_b",
			),
		}
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-a"}, []string{"main"}, "audit",
		)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
			err,
			control.ErrDependencyConflict,
		) {
			t.Fatalf("same-private-principal tie error = %v, want dependency conflict", err)
		}
	})

	t.Run("app principals remain isolated", func(t *testing.T) {
		winners := []publicationWinner{
			publicationIndexAdmissionTestExtraction(
				t, "ko-app-a", 1, "app-a", "owner-a", SharingScopeApp,
				"app-slot", "audit", "app_a",
			),
			publicationIndexAdmissionTestExtraction(
				t, "ko-app-b", 1, "app-b", "owner-b", SharingScopeApp,
				"app-slot", "audit", "app_b",
			),
		}
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-b", "app-a"}, []string{"main"}, "audit",
		)
		if authority, err := validatePublicationIndexNameAdmission(t.Context(), input); err != nil || authority.IsZero() {
			t.Fatalf("app-principal admission = %#v, %v", authority, err)
		}
	})

	t.Run("generic future app is validated", func(t *testing.T) {
		winners := []publicationWinner{
			publicationIndexAdmissionTestExtraction(
				t, "ko-global-a", 1, "app-a", "owner-a", SharingScopeGlobal,
				"future-slot", "audit", "global_a",
			),
			publicationIndexAdmissionTestExtraction(
				t, "ko-global-b", 1, "app-a", "owner-b", SharingScopeGlobal,
				"future-slot", "audit", "global_b",
			),
			publicationIndexAdmissionTestExtraction(
				t, "ko-app-shadow", 1, "app-a", "owner-a", SharingScopeApp,
				"future-slot", "audit", "app_shadow",
			),
		}
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-a"}, []string{"main"}, "audit",
		)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
			err,
			control.ErrDependencyConflict,
		) {
			t.Fatalf("future-app precedence error = %v, want dependency conflict", err)
		}
	})
}

func TestValidatePublicationIndexNameAdmissionActiveAppInventoryTaxonomy(t *testing.T) {
	winner := publicationIndexAdmissionTestExtraction(
		t,
		"ko-app-inventory",
		1,
		"app-a",
		"owner-a",
		SharingScopeApp,
		"app-inventory",
		"audit",
		"app_inventory_output",
	)
	missing := publicationIndexAdmissionTestInventory(
		t,
		[]publicationWinner{winner},
		nil,
		[]string{"main"},
		"audit",
	)
	if _, err := validatePublicationIndexNameAdmission(t.Context(), missing); !errors.Is(
		err,
		ErrCorrupt,
	) || errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("zero active-app inventory error = %v, want corrupt only", err)
	}

	overApps := make([]string, maximumReadableApps+1)
	overApps[0] = "app-a"
	for index := 1; index < len(overApps); index++ {
		overApps[index] = fmt.Sprintf("app-extra-%03d", index)
	}
	over := publicationIndexAdmissionTestInventory(
		t,
		[]publicationWinner{winner},
		overApps,
		[]string{"main"},
		"audit",
	)
	if _, err := validatePublicationIndexNameAdmission(t.Context(), over); !errors.Is(
		err,
		control.ErrCapacityExceeded,
	) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("over-limit active-app inventory error = %v, want capacity exceeded only", err)
	}
}

func TestValidatePublicationIndexNameAdmissionDeterminism(t *testing.T) {
	winners := []publicationWinner{
		publicationIndexAdmissionTestExtraction(
			t, "ko-global", 1, "app-core", "owner-core", SharingScopeGlobal,
			"shared-slot", "audit", "global_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-app-a", 1, "app-a", "owner-a", SharingScopeApp,
			"shared-slot", "audit", "app_a_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-app-b", 1, "app-b", "owner-b", SharingScopeApp,
			"shared-slot", "audit", "app_b_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-private-a", 1, "app-a", "owner-a", SharingScopePrivate,
			"shared-slot", "audit", "private_a_output",
		),
		publicationIndexAdmissionTestExtraction(
			t, "ko-private-b", 1, "app-a", "owner-b", SharingScopePrivate,
			"shared-slot", "audit", "private_b_output",
		),
	}
	base := publicationIndexAdmissionTestInventory(
		t,
		winners,
		[]string{"app-core", "app-a", "app-b"},
		[]string{"main", "archive"},
		"audit",
	)
	expected, err := validatePublicationIndexNameAdmission(t.Context(), base)
	if err != nil {
		t.Fatalf("validate base admission: %v", err)
	}

	reversedWinners := slices.Clone(winners)
	slices.Reverse(reversedWinners)
	permuted := publicationIndexAdmissionTestInventory(
		t,
		reversedWinners,
		[]string{"app-b", "app-a", "app-core"},
		[]string{"archive", "main"},
		"audit",
	)
	actual, err := validatePublicationIndexNameAdmission(t.Context(), permuted)
	if err != nil {
		t.Fatalf("validate permuted admission: %v", err)
	}
	if !actual.Equal(expected) {
		t.Fatal("permuted inventory changed admission authority")
	}

	const workers = 8
	errorsByWorker := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			authority, workerErr := validatePublicationIndexNameAdmission(t.Context(), base)
			if workerErr != nil {
				errorsByWorker <- workerErr
				return
			}
			if !authority.Equal(expected) {
				errorsByWorker <- errors.New("concurrent admission authority differs")
			}
		})
	}
	wait.Wait()
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		t.Error(workerErr)
	}

	nonmatch := publicationIndexAdmissionTestInventory(
		t,
		winners,
		[]string{"app-core", "app-a", "app-b"},
		[]string{"audit", "main"},
		"nomatch",
	)
	nonmatchExpected, err := validatePublicationIndexNameAdmission(t.Context(), nonmatch)
	if err != nil {
		t.Fatalf("validate nonmatching admission: %v", err)
	}
	nonmatchPermuted := publicationIndexAdmissionTestInventory(
		t,
		reversedWinners,
		[]string{"app-b", "app-core", "app-a"},
		[]string{"main", "audit"},
		"nomatch",
	)
	nonmatchActual, err := validatePublicationIndexNameAdmission(t.Context(), nonmatchPermuted)
	if err != nil {
		t.Fatalf("validate permuted nonmatching admission: %v", err)
	}
	if nonmatchExpected.IsZero() || !nonmatchActual.Equal(nonmatchExpected) {
		t.Fatal("nonmatching inventory order changed its nonzero admission authority")
	}
}

func TestValidatePublicationIndexNameAdmissionDetachesBeforeContextCallbacks(t *testing.T) {
	cohort := publicationTestExistingChain(t)
	input := publicationIndexAdmissionTestInventory(
		t,
		cohort.winners,
		[]string{"app-a"},
		[]string{"main"},
		"audit",
	)
	expected, err := validatePublicationIndexNameAdmission(t.Context(), input)
	if err != nil {
		t.Fatalf("baseline index-name admission: %v", err)
	}
	mutation := &publicationMutationContext{
		Context:  t.Context(),
		mutateAt: 1,
		mutate: func() {
			input.currentActive[0].object.DefinitionSHA256[0] ^= 0xff
			input.currentActive[0].object.Definition.Name = "mutated-name"
			input.currentActive[0].existingDependencies[0].targetObjectID = "mutated-target"
			input.activeAppIDs[0] = "mutated-app"
			input.potentiallySearchableIndexNames[0] = "mutated-index"
			input.newlyPotentiallySearchableIndexName = "mutated-new-index"
		},
	}
	actual, err := validatePublicationIndexNameAdmission(mutation, input)
	if err != nil || !mutation.mutated || !actual.Equal(expected) {
		t.Fatalf("detached index-name admission = (%#v, %v), mutated=%t", actual, err, mutation.mutated)
	}
}

func TestValidatePublicationIndexNameAdmissionBoundsAndCancellation(t *testing.T) {
	winner := publicationIndexAdmissionTestExtraction(
		t,
		"ko-boundary",
		1,
		"app-a",
		"owner-a",
		SharingScopeApp,
		"boundary",
		"audit",
		"boundary_output",
	)

	t.Run("exact post-index atom boundary", func(t *testing.T) {
		existing := make([]string, maximumPublicationIndexAtoms-1)
		for index := range existing {
			existing[index] = fmt.Sprintf("idx-%04d", index)
		}
		input := publicationIndexAdmissionTestInventory(
			t, []publicationWinner{winner}, []string{"app-a"}, existing, "audit",
		)
		if authority, err := validatePublicationIndexNameAdmission(t.Context(), input); err != nil || authority.IsZero() {
			t.Fatalf("exact atom-boundary admission = %#v, %v", authority, err)
		}

		over := append(slices.Clone(existing), "idx-overflow")
		overInput := publicationIndexAdmissionTestInventory(
			t, []publicationWinner{winner}, []string{"app-a"}, over, "audit",
		)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), overInput); !errors.Is(
			err,
			control.ErrCapacityExceeded,
		) {
			t.Fatalf("over atom-boundary error = %v, want capacity exceeded", err)
		}
	})

	t.Run("OR closure state bound", func(t *testing.T) {
		const oldIndexes = 10
		winners := make([]publicationWinner, 0, oldIndexes+1)
		existing := make([]string, oldIndexes)
		for index := range existing {
			name := fmt.Sprintf("old-%02d", index)
			existing[index] = name
			winners = append(winners, publicationIndexAdmissionTestExtraction(
				t,
				fmt.Sprintf("ko-old-%02d", index),
				1,
				"app-a",
				"owner-a",
				SharingScopeApp,
				fmt.Sprintf("old-slot-%02d", index),
				name,
				fmt.Sprintf("old_output_%02d", index),
			))
		}
		winners = append(winners, publicationIndexAdmissionTestExtraction(
			t, "ko-new-independent", 1, "app-a", "owner-a", SharingScopeApp,
			"new-independent", "audit", "new_independent_output",
		))
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-a"}, existing, "audit",
		)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
			err,
			control.ErrCapacityExceeded,
		) {
			t.Fatalf("closure state-bound error = %v, want capacity exceeded", err)
		}
	})

	t.Run("class by signature state bound", func(t *testing.T) {
		const privatePrincipals = 64
		const oldIndexes = 9
		winners := make([]publicationWinner, 0, privatePrincipals+oldIndexes)
		for owner := range privatePrincipals {
			winners = append(winners, publicationIndexAdmissionTestExtraction(
				t,
				fmt.Sprintf("ko-private-bound-%02d", owner),
				1,
				"app-a",
				fmt.Sprintf("owner-%02d", owner),
				SharingScopePrivate,
				fmt.Sprintf("private-bound-%02d", owner),
				"old-00",
				fmt.Sprintf("private_bound_output_%02d", owner),
			))
		}
		existing := make([]string, oldIndexes)
		for index := range existing {
			name := fmt.Sprintf("old-%02d", index)
			existing[index] = name
			if index == 0 {
				continue
			}
			winners = append(winners, publicationIndexAdmissionTestExtraction(
				t,
				fmt.Sprintf("ko-class-bound-%02d", index),
				1,
				"app-a",
				"owner-app",
				SharingScopeApp,
				fmt.Sprintf("class-bound-%02d", index),
				name,
				fmt.Sprintf("class_bound_output_%02d", index),
			))
		}
		winners = append(winners, publicationIndexAdmissionTestExtraction(
			t,
			"ko-class-bound-new",
			1,
			"app-a",
			"owner-app",
			SharingScopeApp,
			"class-bound-new",
			"audit",
			"class_bound_new_output",
		))
		input := publicationIndexAdmissionTestInventory(
			t, winners, []string{"app-a"}, existing, "audit",
		)
		if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
			err,
			control.ErrCapacityExceeded,
		) || !strings.Contains(err.Error(), "visibility-state limit") {
			t.Fatalf("class-state-bound error = %v, want visibility-state capacity exceeded", err)
		}
	})

	t.Run("scalar authority mismatch", func(t *testing.T) {
		input := publicationIndexAdmissionTestInventory(
			t, []publicationWinner{winner}, []string{"app-a"}, []string{"main"}, "audit",
		)
		input.expectedDefinitionBytes++
		if _, err := validatePublicationIndexNameAdmission(t.Context(), input); !errors.Is(
			err,
			ErrCorrupt,
		) {
			t.Fatalf("scalar mismatch error = %v, want corrupt authority", err)
		}
	})

	t.Run("pre-canceled", func(t *testing.T) {
		input := publicationIndexAdmissionTestInventory(
			t, []publicationWinner{winner}, []string{"app-a"}, []string{"main"}, "audit",
		)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := validatePublicationIndexNameAdmission(ctx, input); !errors.Is(
			err,
			context.Canceled,
		) {
			t.Fatalf("pre-canceled error = %v, want context canceled", err)
		}
	})

	t.Run("mid-work cancellation", func(t *testing.T) {
		wildcard := publicationIndexAdmissionTestExtraction(
			t,
			"ko-cancel-wildcard",
			1,
			"app-a",
			"owner-a",
			SharingScopeApp,
			"cancel-wildcard",
			"idx-*",
			"cancel_output",
		)
		existing := make([]string, 32)
		for index := range existing {
			existing[index] = fmt.Sprintf("idx-%02d", index)
		}
		input := publicationIndexAdmissionTestInventory(
			t, []publicationWinner{wildcard}, []string{"app-a"}, existing, "idx-new",
		)
		base, cancel := context.WithCancel(t.Context())
		ctx := &publicationIndexCancelContext{
			Context:  base,
			cancel:   cancel,
			cancelAt: 12,
		}
		if _, err := validatePublicationIndexNameAdmission(ctx, input); !errors.Is(
			err,
			context.Canceled,
		) {
			t.Fatalf("mid-work cancellation error = %v, want context canceled", err)
		}
	})
}

func publicationIndexAdmissionTestExtraction(
	t *testing.T,
	objectID string,
	version uint64,
	appID string,
	ownerID string,
	scope SharingScope,
	name string,
	pattern string,
	output string,
) publicationWinner {
	t.Helper()
	object := publicationTestObject(
		t,
		objectID,
		version,
		publicationTransitionTestIndexDefinition(
			dependencyExtractionDefinition(
				appID,
				name,
				scope,
				nil,
				"",
				output,
			),
			pattern,
		),
	)
	object.OwnerID = ownerID
	return publicationWinner{
		object:                      object,
		existingDependenciesPresent: true,
	}
}

func publicationIndexAdmissionBatchInputs(
	t *testing.T,
	pattern string,
	newName string,
) []publicationIndexNameAdmissionInventory {
	t.Helper()
	winner := publicationIndexAdmissionTestExtraction(
		t,
		"ko-batch-budget",
		1,
		"app-a",
		"owner-a",
		SharingScopeApp,
		"batch-budget",
		pattern,
		"batch_budget_output",
	)
	result := make([]publicationIndexNameAdmissionInventory, 3)
	for index, tenantID := range []string{"tenant-a", "tenant-b", "tenant-c"} {
		result[index] = publicationIndexAdmissionTestInventory(
			t,
			[]publicationWinner{winner},
			[]string{"app-a"},
			[]string{"main"},
			newName,
		)
		result[index].tenantID = tenantID
	}
	return result
}

func publicationIndexAdmissionMeasureBatchCharge(
	t *testing.T,
	input publicationIndexNameAdmissionInventory,
) publicationIndexNameAdmissionBatchBudget {
	t.Helper()
	var budget publicationIndexNameAdmissionBatchBudget
	authority, err := validatePublicationIndexNameAdmissionWithBudget(
		t.Context(),
		input,
		&budget,
	)
	if err != nil || authority.IsZero() {
		t.Fatalf("measure batch charge authority = %#v, %v", authority, err)
	}
	return budget
}

func publicationIndexAdmissionBatchStart(
	t *testing.T,
	maximum uint64,
	perTenant uint64,
) uint64 {
	t.Helper()
	if perTenant == 0 || perTenant > maximum/2 {
		t.Fatalf("invalid two-tenant batch charge %d for maximum %d", perTenant, maximum)
	}
	return maximum - 2*perTenant
}

func publicationIndexAdmissionValidateBatchTenants(
	t *testing.T,
	inputs []publicationIndexNameAdmissionInventory,
	budget *publicationIndexNameAdmissionBatchBudget,
) {
	t.Helper()
	for index, input := range inputs {
		authority, err := validatePublicationIndexNameAdmissionWithBudget(
			t.Context(),
			input,
			budget,
		)
		if err != nil || authority.IsZero() {
			t.Fatalf("batch tenant %d authority = %#v, %v", index, authority, err)
		}
	}
}

func publicationIndexAdmissionSemanticProgramInventory(
	t *testing.T,
	oldCount int,
) publicationIndexNameAdmissionInventory {
	t.Helper()
	winners, existing := publicationIndexAdmissionSemanticProgramBase(t, oldCount)
	winners = append(winners, publicationIndexAdmissionTestExtraction(
		t,
		"ko-semantic-program-new",
		1,
		"app-a",
		"owner-a",
		SharingScopeApp,
		"semantic-program-new",
		"audit",
		"semantic_program_new_output",
	))
	return publicationIndexAdmissionTestInventory(
		t,
		winners,
		[]string{"app-a"},
		existing,
		"audit",
	)
}

func publicationIndexAdmissionSemanticProgramTransition(
	t *testing.T,
	oldCount int,
) publicationActiveTransitionInventory {
	t.Helper()
	winners, indexes := publicationIndexAdmissionSemanticProgramBase(t, oldCount)
	indexes = append(indexes, "audit")
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-semantic-program-candidate",
		1,
		publicationTransitionTestIndexDefinition(
			dependencyExtractionDefinition(
				"app-a",
				"semantic-program-candidate",
				SharingScopeApp,
				nil,
				"",
				"semantic_program_candidate_output",
			),
			"audit",
		),
	)}
	return publicationTransitionTestInventory(
		t,
		winners,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{
			present: true,
			state:   StateActive,
			winner:  candidate,
		},
		[]string{"app-a"},
		indexes,
	)
}

func publicationIndexAdmissionSemanticProgramBase(
	t *testing.T,
	oldCount int,
) ([]publicationWinner, []string) {
	t.Helper()
	winners := make([]publicationWinner, oldCount)
	indexes := make([]string, oldCount)
	for index := range oldCount {
		name := fmt.Sprintf("old-%02d", index)
		indexes[index] = name
		winners[index] = publicationIndexAdmissionTestExtraction(
			t,
			fmt.Sprintf("ko-semantic-program-old-%02d", index),
			1,
			"app-a",
			"owner-a",
			SharingScopeApp,
			fmt.Sprintf("semantic-program-old-%02d", index),
			name,
			fmt.Sprintf("semantic_program_old_output_%02d", index),
		)
	}
	return winners, indexes
}

func publicationIndexAdmissionTestInventory(
	t *testing.T,
	winners []publicationWinner,
	activeApps []string,
	existingNames []string,
	newName string,
) publicationIndexNameAdmissionInventory {
	t.Helper()
	input := publicationIndexNameAdmissionInventory{
		tenantID:                                "tenant-a",
		expectedActiveAppCount:                  uint16(len(activeApps)),
		activeAppIDs:                            slices.Clone(activeApps),
		expectedCurrentActiveCount:              uint32(len(winners)),
		currentActive:                           make([]publicationWinner, len(winners)),
		expectedPotentiallySearchableIndexCount: uint16(len(existingNames)),
		potentiallySearchableIndexNames:         slices.Clone(existingNames),
		newlyPotentiallySearchableIndexName:     newName,
	}
	for index, winner := range winners {
		input.currentActive[index] = publicationCloneWinner(winner)
		canonical, err := canonicalizePublicationTransitionObject(winner, false)
		if err != nil {
			t.Fatalf("canonicalize index-admission fixture %d: %v", index, err)
		}
		input.expectedDefinitionBytes += canonical.canonical.definitionBytes
		input.expectedProjectionBytes += canonical.projectionBytes
		input.expectedSelectorPatterns += canonical.selectorPatterns
		input.expectedSelectorValueBytes += canonical.selectorValueBytes
		input.expectedCanonicalSelectorBytes += canonical.canonicalSelectorBytes
		input.expectedSelectorWork += canonical.canonical.selectorWork
		input.expectedDependencyCount += uint64(len(winner.existingDependencies))
	}
	return input
}
