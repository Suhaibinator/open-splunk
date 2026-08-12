package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"google.golang.org/protobuf/proto"
)

func TestCompilePublicationWinnerCohortDerivesPinnedDetachedAuthority(t *testing.T) {
	cohort, candidate := publicationTestChain(t)
	authority, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil {
		t.Fatalf("compilePublicationWinnerCohort(chain): %v", err)
	}
	if authority.IsZero() || authority.candidateAuthority() != candidate ||
		authority.sourceStage() != opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS {
		t.Fatalf("candidate authority = %#v, stage %v", authority.candidateAuthority(), authority.sourceStage())
	}
	targets := authority.derivedTargets()
	projection := authority.databaseProjection()
	extraction := publicationTestWinnerByID(cohort, "ko-extraction").object
	if len(targets) != 1 || len(projection) != 1 ||
		targets[0].objectID != "ko-extraction" || targets[0].version != 3 ||
		targets[0].definitionDigest != publicationTestDigest(extraction.DefinitionSHA256) ||
		targets[0].ownerID != extraction.OwnerID ||
		targets[0].role != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
		targets[0].targetStage != opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION ||
		projection[0] != (publicationDependency{targetObjectID: "ko-extraction", targetVersion: 3}) {
		t.Fatalf("derived authority = targets:%#v projection:%#v", targets, projection)
	}

	reordered := publicationCloneCohort(cohort)
	reordered.winners[0], reordered.winners[2] = reordered.winners[2], reordered.winners[0]
	recompiled, err := compilePublicationWinnerCohort(t.Context(), reordered, candidate)
	if err != nil {
		t.Fatalf("compilePublicationWinnerCohort(reordered): %v", err)
	}
	if !authority.Equal(recompiled) {
		t.Fatal("winner input order changed candidate dependency authority")
	}

	targets[0].objectID = "mutated-target"
	projection[0].targetObjectID = "mutated-projection"
	for index := range cohort.winners {
		winner := &cohort.winners[index]
		winner.object.DefinitionSHA256[0] ^= 0xff
		winner.object.Definition.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
		if len(winner.existingDependencies) != 0 {
			winner.existingDependencies[0].targetObjectID = "mutated-existing"
		}
	}
	if got := authority.derivedTargets(); len(got) != 1 || got[0].objectID != "ko-extraction" {
		t.Fatalf("derived target accessor aliases caller storage: %#v", got)
	}
	if got := authority.databaseProjection(); len(got) != 1 || got[0].targetObjectID != "ko-extraction" {
		t.Fatalf("database projection accessor aliases caller storage: %#v", got)
	}
}

func TestCompilePublicationWinnerCohortDistinguishesAbsentAndPresentEmpty(t *testing.T) {
	candidateObject := publicationTestObject(t, "ko-extraction", 1, dependencyExtractionDefinition(
		"app-a", "extract-a", SharingScopeApp, nil, "", "extracted_value",
	))
	candidate := publicationTestCandidate(candidateObject)
	cohort := publicationWinnerCohort{
		expectedWinnerCount: 1,
		winners: []publicationWinner{{
			object: candidateObject,
			// Candidate dependency rows must be structurally absent.
			existingDependenciesPresent: false,
			existingDependencies:        nil,
		}},
	}
	authority, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil {
		t.Fatalf("compilePublicationWinnerCohort(empty candidate): %v", err)
	}
	if authority.IsZero() || len(authority.derivedTargets()) != 0 ||
		len(authority.databaseProjection()) != 0 || authority.Equal(candidateDependencyAuthority{}) {
		t.Fatal("successfully compiled empty authority collapsed into absence")
	}

	tests := []struct {
		name   string
		mutate func(*publicationWinner)
	}{
		{
			name: "present empty marker",
			mutate: func(winner *publicationWinner) {
				winner.existingDependenciesPresent = true
			},
		},
		{
			name: "non-nil empty rows",
			mutate: func(winner *publicationWinner) {
				winner.existingDependencies = make([]publicationPersistedDependency, 0)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := publicationCloneCohort(cohort)
			test.mutate(&invalid.winners[0])
			if _, err := compilePublicationWinnerCohort(t.Context(), invalid, candidate); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("candidate submitted rows error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestValidatePublicationWinnerCohortRetainsCandidateAbsentProof(t *testing.T) {
	empty := publicationWinnerCohort{}
	absentCandidate := publicationTestAbsentCandidate(t)
	emptyAuthority, err := validatePublicationWinnerCohort(
		t.Context(), empty, absentCandidate, false,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(empty): %v", err)
	}
	emptyCommitment, present := emptyAuthority.programCommitment()
	emptyProgram, compileErr := knowledgeprogram.Compile(nil)
	if compileErr != nil {
		t.Fatalf("knowledgeprogram.Compile(empty): %v", compileErr)
	}
	wantEmptyCommitment, wantPresent := emptyProgram.Commitment()
	boundCandidate, candidateWins, candidatePresent := emptyAuthority.candidateBinding()
	if emptyAuthority.IsZero() || !present || emptyCommitment == ([32]byte{}) ||
		!wantPresent || emptyCommitment != wantEmptyCommitment ||
		!emptyAuthority.candidateDependencies().IsZero() || !candidatePresent || candidateWins ||
		boundCandidate != absentCandidate {
		t.Fatalf(
			"empty authority = zero:%t commitment:%x/%t candidate-zero:%t binding:%#v/%t/%t",
			emptyAuthority.IsZero(),
			emptyCommitment,
			present,
			emptyAuthority.candidateDependencies().IsZero(),
			boundCandidate,
			candidateWins,
			candidatePresent,
		)
	}
	emptyAgain, err := validatePublicationWinnerCohort(
		t.Context(), empty, absentCandidate, false,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(empty again): %v", err)
	}
	if !emptyAuthority.Equal(emptyAgain) || emptyAuthority.Equal(publicationWinnerCohortAuthority{}) {
		t.Fatal("validated empty cohort equality collapsed present and absent authority")
	}
	if _, present := (publicationWinnerCohortAuthority{}).programCommitment(); present {
		t.Fatal("absent cohort authority exposed a program commitment")
	}
	if _, _, present := (publicationWinnerCohortAuthority{}).candidateBinding(); present {
		t.Fatal("absent cohort authority exposed a candidate binding")
	}

	replayMutations := []struct {
		name   string
		mutate func(*publicationCandidateAuthority)
	}{
		{name: "object ID", mutate: func(candidate *publicationCandidateAuthority) {
			candidate.objectID = "ko-transition-replay"
		}},
		{name: "version", mutate: func(candidate *publicationCandidateAuthority) {
			candidate.version++
		}},
		{name: "definition digest", mutate: func(candidate *publicationCandidateAuthority) {
			candidate.definitionDigest[0] ^= 0xff
		}},
		{name: "owner ID", mutate: func(candidate *publicationCandidateAuthority) {
			candidate.ownerID = "owner-replay"
		}},
	}
	for _, replayMutation := range replayMutations {
		t.Run("binds "+replayMutation.name, func(t *testing.T) {
			replayCandidate := absentCandidate
			replayMutation.mutate(&replayCandidate)
			replay, replayErr := validatePublicationWinnerCohort(
				t.Context(), empty, replayCandidate, false,
			)
			if replayErr != nil {
				t.Fatalf("validatePublicationWinnerCohort(replay candidate): %v", replayErr)
			}
			if emptyAuthority.Equal(replay) {
				t.Fatal("candidate-absent authority can be replayed for another transition identity")
			}
		})
	}
	oppositeMode := emptyAuthority
	oppositeModeState := *emptyAuthority.state
	oppositeModeState.candidateWins = true
	oppositeMode.state = &oppositeModeState
	if emptyAuthority.Equal(oppositeMode) {
		t.Fatal("cohort authority equality does not bind candidate winner mode")
	}

	cohort := publicationTestExistingChain(t)
	fullAuthority, err := validatePublicationWinnerCohort(
		t.Context(), cohort, absentCandidate, false,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(existing chain): %v", err)
	}
	reordered := publicationCloneCohort(cohort)
	reordered.winners[0], reordered.winners[2] = reordered.winners[2], reordered.winners[0]
	reorderedAuthority, err := validatePublicationWinnerCohort(
		t.Context(), reordered, absentCandidate, false,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(reordered existing chain): %v", err)
	}
	if fullAuthority.IsZero() || !fullAuthority.candidateDependencies().IsZero() ||
		!fullAuthority.Equal(reorderedAuthority) || fullAuthority.Equal(emptyAuthority) {
		t.Fatal("candidate-absent cohort proof does not bind its canonical program")
	}
	_, exactWinner := publicationTestChain(t)
	if _, err := validatePublicationWinnerCohort(
		t.Context(), cohort, exactWinner, false,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("false candidate-absent assertion error = %v, want DependencyConflict", err)
	}
	if _, err := validatePublicationWinnerCohort(
		t.Context(), cohort, absentCandidate, true,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("false candidate-winner assertion error = %v, want DependencyConflict", err)
	}
	duplicateCandidate := publicationCloneCohort(cohort)
	duplicateCandidate.winners = append(
		duplicateCandidate.winners,
		publicationCloneWinner(publicationTestWinnerByID(cohort, "ko-alias")),
	)
	duplicateCandidate.expectedWinnerCount++
	if _, err := validatePublicationWinnerCohort(
		t.Context(), duplicateCandidate, exactWinner, false,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate candidate error = %v, want ErrCorrupt", err)
	}
}

func TestValidatePublicationWinnerCohortCandidateProofIsDetached(t *testing.T) {
	cohort, candidate := publicationTestChain(t)
	authority, err := validatePublicationWinnerCohort(t.Context(), cohort, candidate, true)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(candidate): %v", err)
	}
	strict, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil {
		t.Fatalf("compilePublicationWinnerCohort(candidate): %v", err)
	}
	boundCandidate, candidateWins, candidatePresent := authority.candidateBinding()
	if authority.IsZero() || !authority.candidateDependencies().Equal(strict) ||
		!candidatePresent || !candidateWins || boundCandidate != candidate {
		t.Fatal("generalized validator changed strict candidate dependency authority")
	}

	reordered := publicationCloneCohort(cohort)
	reordered.winners[0], reordered.winners[2] = reordered.winners[2], reordered.winners[0]
	reorderedAuthority, err := validatePublicationWinnerCohort(
		t.Context(), reordered, candidate, true,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(reordered candidate): %v", err)
	}
	if !authority.Equal(reorderedAuthority) {
		t.Fatal("winner input order changed cohort authority")
	}

	detached := authority.candidateDependencies()
	detached.state.candidate.objectID = "mutated-candidate"
	detached.state.targets[0].objectID = "mutated-target"
	detached.state.projection[0].targetObjectID = "mutated-projection"
	got := authority.candidateDependencies()
	if got.candidateAuthority() != candidate || got.derivedTargets()[0].objectID != "ko-extraction" ||
		got.databaseProjection()[0].targetObjectID != "ko-extraction" {
		t.Fatal("cohort candidate accessor aliases retained authority")
	}

	mutableCandidate := candidate
	mutation := &publicationMutationContext{
		Context:  t.Context(),
		mutateAt: 2,
		mutate: func() {
			mutableCandidate.objectID = "caller-mutated"
			mutableCandidate.ownerID = "caller-mutated"
		},
	}
	detachedIngress, err := validatePublicationWinnerCohort(
		mutation, cohort, mutableCandidate, true,
	)
	if err != nil {
		t.Fatalf("validatePublicationWinnerCohort(candidate mutation): %v", err)
	}
	detachedCandidate, detachedWins, detachedPresent := detachedIngress.candidateBinding()
	if !mutation.mutated || !authority.Equal(detachedIngress) ||
		!detachedPresent || !detachedWins || detachedCandidate != candidate {
		t.Fatalf(
			"candidate ingress detachment = mutated:%t equal:%t binding:%#v/%t/%t",
			mutation.mutated,
			authority.Equal(detachedIngress),
			detachedCandidate,
			detachedWins,
			detachedPresent,
		)
	}
}

func TestValidatePublicationWinnerCohortRequiresExactExistingAuthority(t *testing.T) {
	base := publicationTestExistingChain(t)
	absentCandidate := publicationTestAbsentCandidate(t)
	tests := []struct {
		name    string
		mutate  func(*publicationWinnerCohort)
		wantErr error
	}{
		{
			name: "missing present marker",
			mutate: func(cohort *publicationWinnerCohort) {
				alias := publicationTestWinnerIndex(cohort, "ko-alias")
				cohort.winners[alias].existingDependenciesPresent = false
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "stale exact-shape rows",
			mutate: func(cohort *publicationWinnerCohort) {
				alias := publicationTestWinnerIndex(cohort, "ko-alias")
				cohort.winners[alias].existingDependencies = nil
			},
			wantErr: control.ErrDependencyConflict,
		},
		{
			name: "malformed existing definition",
			mutate: func(cohort *publicationWinnerCohort) {
				alias := publicationTestWinnerIndex(cohort, "ko-alias")
				cohort.winners[alias].object.Definition = nil
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "incomplete count",
			mutate: func(cohort *publicationWinnerCohort) {
				cohort.expectedWinnerCount--
			},
			wantErr: ErrCorrupt,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cohort := publicationCloneCohort(base)
			test.mutate(&cohort)
			if _, err := validatePublicationWinnerCohort(
				t.Context(), cohort, absentCandidate, false,
			); !errors.Is(err, test.wantErr) {
				t.Fatalf("validatePublicationWinnerCohort() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	presentNonNilEmpty := publicationCloneCohort(base)
	extraction := publicationTestWinnerIndex(&presentNonNilEmpty, "ko-extraction")
	presentNonNilEmpty.winners[extraction].existingDependencies = make(
		[]publicationPersistedDependency,
		0,
	)
	if _, err := validatePublicationWinnerCohort(
		t.Context(), presentNonNilEmpty, absentCandidate, false,
	); err != nil {
		t.Fatalf("present non-nil empty persisted authority: %v", err)
	}

	_, candidate := publicationTestChain(t)
	if _, err := validatePublicationWinnerCohort(
		t.Context(), publicationWinnerCohort{}, candidate, true,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("candidate with zero winners error = %v, want ErrCorrupt", err)
	}

	colliding := publicationWinnerCohort{
		expectedWinnerCount: 2,
		winners: []publicationWinner{
			{
				object: publicationTestObject(t, "ko-collision-a", 1, dependencyExtractionDefinition(
					"app-a", "collision-a", SharingScopeApp, nil, "", "same_output",
				)),
				existingDependenciesPresent: true,
			},
			{
				object: publicationTestObject(t, "ko-collision-b", 1, dependencyExtractionDefinition(
					"app-a", "collision-b", SharingScopeApp, nil, "", "same_output",
				)),
				existingDependenciesPresent: true,
			},
		},
	}
	if _, err := validatePublicationWinnerCohort(
		t.Context(), colliding, absentCandidate, false,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("candidate-absent semantic conflict error = %v, want DependencyConflict", err)
	}

	over := publicationWinnerCohort{
		expectedWinnerCount: knowledgeprogram.MaximumObjects + 1,
		winners:             make([]publicationWinner, knowledgeprogram.MaximumObjects+1),
	}
	if _, err := validatePublicationWinnerCohort(
		t.Context(), over, absentCandidate, false,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("candidate-absent object capacity error = %v, want CapacityExceeded", err)
	}
	invalidCandidate := absentCandidate
	invalidCandidate.objectID = ""
	if _, err := validatePublicationWinnerCohort(
		t.Context(), publicationWinnerCohort{}, invalidCandidate, false,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("invalid non-winning candidate error = %v, want InvalidArgument", err)
	}
	var nilContext context.Context
	if _, err := validatePublicationWinnerCohort(
		nilContext, base, absentCandidate, false,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil context error = %v, want InvalidArgument", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := validatePublicationWinnerCohort(
		canceled, base, absentCandidate, false,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
}

func TestCompilePublicationWinnerCohortRejectsMalformedAuthority(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*publicationWinnerCohort, *publicationCandidateAuthority)
		wantErr error
	}{
		{
			name: "incomplete count",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				cohort.expectedWinnerCount--
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "missing existing row authority",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				winner := publicationTestWinnerIndex(cohort, "ko-extraction")
				cohort.winners[winner].existingDependenciesPresent = false
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "duplicate identity",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				duplicate := publicationCloneWinner(cohort.winners[0])
				cohort.winners = append(cohort.winners, duplicate)
				cohort.expectedWinnerCount++
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "ambiguous winner slot",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				shadow := publicationCloneWinner(publicationTestWinnerByID(*cohort, "ko-extraction"))
				shadow.object.KnowledgeObjectID = "ko-shadow"
				cohort.winners = append(cohort.winners, shadow)
				cohort.expectedWinnerCount++
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "candidate absent",
			mutate: func(_ *publicationWinnerCohort, candidate *publicationCandidateAuthority) {
				candidate.objectID = "ko-absent"
			},
			wantErr: control.ErrDependencyConflict,
		},
		{
			name: "candidate version mismatch",
			mutate: func(_ *publicationWinnerCohort, candidate *publicationCandidateAuthority) {
				candidate.version++
			},
			wantErr: control.ErrDependencyConflict,
		},
		{
			name: "candidate digest mismatch",
			mutate: func(_ *publicationWinnerCohort, candidate *publicationCandidateAuthority) {
				candidate.definitionDigest[0] ^= 0xff
			},
			wantErr: control.ErrDependencyConflict,
		},
		{
			name: "candidate owner mismatch",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				candidate := publicationTestWinnerIndex(cohort, "ko-alias")
				cohort.winners[candidate].object.OwnerID = "owner-substituted"
			},
			wantErr: control.ErrDependencyConflict,
		},
		{
			name: "candidate malformed definition",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				candidate := publicationTestWinnerIndex(cohort, "ko-alias")
				cohort.winners[candidate].object.Definition = nil
			},
			wantErr: control.ErrInvalidArgument,
		},
		{
			name: "candidate oversized definition",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				candidate := publicationTestWinnerIndex(cohort, "ko-alias")
				description := strings.Repeat("x", knowledgeprogram.MaximumDefinitionBytes+1)
				cohort.winners[candidate].object.Definition.Description = &description
			},
			wantErr: control.ErrCapacityExceeded,
		},
		{
			name: "existing malformed definition",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				existing := publicationTestWinnerIndex(cohort, "ko-extraction")
				cohort.winners[existing].object.Definition = nil
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "old version sharing candidate ID is existing authority",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				old := publicationCloneWinner(publicationTestWinnerByID(*cohort, "ko-alias"))
				old.object.Version--
				old.object.Name = "alias-old-version"
				old.object.Definition = nil
				old.existingDependenciesPresent = true
				cohort.winners = append(cohort.winners, old)
				cohort.expectedWinnerCount++
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "persisted ordinal mismatch",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				calculated := publicationTestWinnerIndex(cohort, "ko-calculated")
				cohort.winners[calculated].existingDependencies[0].ordinal = 1
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "persisted rows reordered",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				calculated := publicationTestWinnerIndex(cohort, "ko-calculated")
				rows := cohort.winners[calculated].existingDependencies
				rows[0], rows[1] = rows[1], rows[0]
				rows[0].ordinal, rows[1].ordinal = 0, 1
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "persisted role mismatch",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				calculated := publicationTestWinnerIndex(cohort, "ko-calculated")
				cohort.winners[calculated].existingDependencies[0].role =
					opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_UNSPECIFIED
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "persisted target outside closure",
			mutate: func(cohort *publicationWinnerCohort, _ *publicationCandidateAuthority) {
				calculated := publicationTestWinnerIndex(cohort, "ko-calculated")
				cohort.winners[calculated].existingDependencies[0].targetObjectID = "ko-missing"
			},
			wantErr: ErrCorrupt,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cohort, candidate := publicationTestChain(t)
			test.mutate(&cohort, &candidate)
			if _, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate); !errors.Is(err, test.wantErr) {
				t.Fatalf("compilePublicationWinnerCohort() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestCompilePublicationWinnerCohortRejectsNonExecutableBooleanCalculatedFields(t *testing.T) {
	booleanDefinition := func(name, destination string) *opensplunkv1.KnowledgeObjectDefinition {
		return dependencyCalculatedDefinition(
			"app-a", name, SharingScopeApp, nil, "", "isnull(stored_field)", destination,
		)
	}

	candidateObject := publicationTestObject(
		t,
		"ko-boolean-candidate",
		1,
		booleanDefinition("boolean-candidate", "candidate_output"),
	)
	if _, err := compilePublicationWinnerCohort(t.Context(), publicationWinnerCohort{
		expectedWinnerCount: 1,
		winners:             []publicationWinner{{object: candidateObject}},
	}, publicationTestCandidate(candidateObject)); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("boolean candidate error = %v, want InvalidArgument", err)
	}

	cohort, candidate := publicationTestChain(t)
	existing := publicationTestWinnerIndex(&cohort, "ko-extraction")
	cohort.winners[existing].object = publicationTestObject(
		t,
		"ko-extraction",
		3,
		booleanDefinition("boolean-existing", "existing_output"),
	)
	if _, err := compilePublicationWinnerCohort(
		t.Context(), cohort, candidate,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("boolean existing error = %v, want ErrCorrupt", err)
	}
}

func TestCompilePublicationWinnerCohortRejectsOversizedRepeatedShapeBeforeTraversal(t *testing.T) {
	object := publicationTestObject(t, "ko-shape-candidate", 1, dependencyExtractionDefinition(
		"app-a", "shape-candidate", SharingScopeApp, nil, "", "output",
	))
	object.Definition.GetFieldExtraction().Extraction = &opensplunkv1.FieldExtractionDefinition_Regex{
		Regex: &opensplunkv1.RegexFieldExtractionDefinition{
			Pattern:      "(?<output>.*)",
			OutputFields: make([]string, 1<<18),
		},
	}
	if _, err := compilePublicationWinnerCohort(t.Context(), publicationWinnerCohort{
		expectedWinnerCount: 1,
		winners:             []publicationWinner{{object: object}},
	}, publicationTestCandidate(object)); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("oversized repeated shape error = %v, want CapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortEnforcesSelectorAndScope(t *testing.T) {
	tests := []struct {
		name                         string
		sourceScope, targetScope     SharingScope
		sourcePattern, targetPattern string
		wantDependencies             int
		wantErr                      error
	}{
		{
			name: "literal selector implies wildcard", sourceScope: SharingScopeApp, targetScope: SharingScopeApp,
			sourcePattern: "api-01", targetPattern: "api-??", wantDependencies: 1,
		},
		{
			name: "disjoint selectors omit edge", sourceScope: SharingScopeApp, targetScope: SharingScopeApp,
			sourcePattern: "worker-01", targetPattern: "api-*",
		},
		{
			name: "overlap without implication", sourceScope: SharingScopeApp, targetScope: SharingScopeApp,
			sourcePattern: "api-?", targetPattern: "api-*", wantErr: control.ErrDependencyConflict,
		},
		{
			name: "app source cannot read private target", sourceScope: SharingScopeApp, targetScope: SharingScopePrivate,
			wantErr: control.ErrDependencyConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cohort, candidate := publicationTestPair(
				t, test.sourceScope, test.targetScope, test.sourcePattern, test.targetPattern,
			)
			authority, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("compilePublicationWinnerCohort() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("compilePublicationWinnerCohort(): %v", err)
			}
			if got := len(authority.databaseProjection()); got != test.wantDependencies {
				t.Fatalf("dependency count = %d, want %d", got, test.wantDependencies)
			}
		})
	}
}

func TestCompilePublicationWinnerCohortRejectsChangedExistingWinner(t *testing.T) {
	candidateObject := publicationTestObject(t, "ko-new-extraction", 1, dependencyExtractionDefinition(
		"app-a", "extract-new", SharingScopeApp, nil, "", "existing_input",
	))
	existingAlias := publicationTestObject(t, "ko-existing-alias", 4, dependencyAliasDefinition(
		"app-a", "alias-existing", SharingScopeApp, nil, "", "existing_input", "existing_output",
	))
	cohort := publicationWinnerCohort{
		expectedWinnerCount: 2,
		winners: []publicationWinner{
			{object: existingAlias, existingDependenciesPresent: true},
			{object: candidateObject},
		},
	}
	if _, err := compilePublicationWinnerCohort(
		t.Context(), cohort, publicationTestCandidate(candidateObject),
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("changed existing winner error = %v, want ErrDependencyConflict", err)
	}
}

func TestCompilePublicationWinnerCohortContextAndProgramLimit(t *testing.T) {
	cohort, candidate := publicationTestChain(t)
	var nilContext context.Context
	if _, err := compilePublicationWinnerCohort(nilContext, cohort, candidate); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil context error = %v, want ErrInvalidArgument", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := compilePublicationWinnerCohort(canceled, cohort, candidate); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}

	winners := make([]publicationWinner, 33)
	for index := range winners {
		object := publicationTestObject(t, fmt.Sprintf("ko-calculated-%02d", index), 1,
			dependencyCalculatedDefinition(
				"app-a", fmt.Sprintf("calculated-%02d", index), SharingScopeApp, nil, "",
				"stored_field", fmt.Sprintf("calculated_%02d", index),
			),
		)
		winners[index] = publicationWinner{
			object:                      object,
			existingDependenciesPresent: index != len(winners)-1,
		}
	}
	limitCandidate := publicationTestCandidate(winners[len(winners)-1].object)
	if _, err := compilePublicationWinnerCohort(t.Context(), publicationWinnerCohort{
		expectedWinnerCount: uint32(len(winners)),
		winners:             winners,
	}, limitCandidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("program limit error = %v, want ErrCapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortResourcePreflightBoundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		maximum uint64
	}{
		{name: "definition bytes", maximum: knowledgeprogram.MaximumDefinitionBytes},
		{name: "selector work", maximum: knowledge.MaximumSelectorWildcardWorkUnits},
		{name: "persisted dependencies", maximum: knowledgeprogram.MaximumDependencies},
	} {
		t.Run(test.name, func(t *testing.T) {
			var total uint64
			if !addPublicationResource(&total, test.maximum, test.maximum) || total != test.maximum {
				t.Fatalf("exact resource addition = (%d, false), want (%d, true)", total, test.maximum)
			}
			if addPublicationResource(&total, 1, test.maximum) || total != test.maximum {
				t.Fatalf("over-limit resource addition = (%d, true), want (%d, false)", total, test.maximum)
			}
		})
	}

	cohort, candidate := publicationTestChain(t)
	existing := publicationTestWinnerIndex(&cohort, "ko-calculated")
	cohort.winners[existing].existingDependencies = make(
		[]publicationPersistedDependency,
		knowledgeprogram.MaximumDependencies+1,
	)
	if err := preflightPublicationWinnerCohort(cohort, candidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("dependency preflight error = %v, want CapacityExceeded", err)
	}

	cohort, candidate = publicationTestChain(t)
	existing = publicationTestWinnerIndex(&cohort, "ko-calculated")
	cohort.winners[existing].existingDependencies[0].targetObjectID = strings.Repeat("x", 1<<20)
	if err := preflightPublicationWinnerCohort(cohort, candidate); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized dependency identity error = %v, want ErrCorrupt", err)
	}
}

func TestCompilePublicationWinnerCohortEnforcesAggregateDefinitionBytes(t *testing.T) {
	description := strings.Repeat("d", knowledgedefinition.MaximumDescriptionBytes)
	winners := make([]publicationWinner, knowledgeprogram.MaximumObjects)
	for index := range winners {
		object := publicationTestObject(
			t,
			fmt.Sprintf("ko-definition-boundary-%03d", index),
			1,
			dependencyAliasDefinition(
				"app-a",
				fmt.Sprintf("definition-boundary-%03d", index),
				SharingScopeApp,
				&description,
				"",
				fmt.Sprintf("source_%03d", index),
				fmt.Sprintf("destination_%03d", index),
			),
		)
		winners[index] = publicationWinner{
			object:                      object,
			existingDependenciesPresent: index != len(winners)-1,
		}
	}
	candidate := publicationTestCandidate(winners[len(winners)-1].object)
	if _, err := compilePublicationWinnerCohort(t.Context(), publicationWinnerCohort{
		expectedWinnerCount: uint32(len(winners)),
		winners:             winners,
	}, candidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("aggregate definition bytes error = %v, want CapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortSelectorWorkBoundary(t *testing.T) {
	patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, knowledge.MaximumSelectorPatternsPerDimension)
	for index := range patterns {
		prefix := fmt.Sprintf("%02d", index)
		patterns[index] = &opensplunkv1.KnowledgeSelectorPattern{
			Value: prefix + strings.Repeat("x", 64-len(prefix)),
		}
	}
	target := publicationTestObject(t, "ko-selector-target", 1, dependencyExtractionDefinition(
		"app-a", "selector-target", SharingScopeApp, nil, "", "selector_input",
	))
	candidateDefinition := dependencyAliasDefinition(
		"app-a", "selector-candidate", SharingScopeApp, nil, "", "selector_input", "selector_output",
	)
	candidateDefinition.Selector = &opensplunkv1.KnowledgeSelector{HostPatterns: patterns}
	candidateObject := publicationTestObject(t, "ko-selector-candidate", 1, candidateDefinition)
	cohort := publicationWinnerCohort{
		expectedWinnerCount: 2,
		winners: []publicationWinner{
			{object: target, existingDependenciesPresent: true},
			{object: candidateObject},
		},
	}
	candidate := publicationTestCandidate(candidateObject)
	if authority, err := compilePublicationWinnerCohort(
		t.Context(), cohort, candidate,
	); err != nil || authority.IsZero() {
		t.Fatalf("exact selector work boundary = (%#v, %v), want present", authority, err)
	}

	over := publicationCloneCohort(cohort)
	overTarget := publicationTestWinnerIndex(&over, "ko-selector-target")
	over.winners[overTarget].object.Definition.Selector = &opensplunkv1.KnowledgeSelector{
		HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "extra"}},
	}
	normalized, err := knowledgedefinition.Normalize(over.winners[overTarget].object.Definition)
	if err != nil {
		t.Fatalf("normalize selector overflow target: %v", err)
	}
	over.winners[overTarget].object.Definition = normalized.Definition
	over.winners[overTarget].object.DefinitionSHA256 = bytes.Clone(normalized.Digest[:])
	if _, err := compilePublicationWinnerCohort(
		t.Context(), over, candidate,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("selector work +1 error = %v, want CapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortObjectBoundary(t *testing.T) {
	winners := make([]publicationWinner, knowledgeprogram.MaximumObjects)
	for index := range winners {
		object := publicationTestObject(
			t,
			fmt.Sprintf("ko-alias-boundary-%03d", index),
			1,
			dependencyAliasDefinition(
				"app-a",
				fmt.Sprintf("alias-boundary-%03d", index),
				SharingScopeApp,
				nil,
				"",
				fmt.Sprintf("raw_%03d", index),
				fmt.Sprintf("derived_%03d", index),
			),
		)
		winners[index] = publicationWinner{
			object:                      object,
			existingDependenciesPresent: index != len(winners)-1,
		}
	}
	candidate := publicationTestCandidate(winners[len(winners)-1].object)
	cohort := publicationWinnerCohort{
		expectedWinnerCount: uint32(len(winners)),
		winners:             winners,
	}
	authority, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil || authority.IsZero() || len(authority.databaseProjection()) != 0 {
		t.Fatalf("exact object boundary = (%#v, %v), want present empty", authority, err)
	}

	over := publicationCloneCohort(cohort)
	extra := publicationCloneWinner(over.winners[0])
	extra.object.KnowledgeObjectID = "ko-alias-boundary-over"
	extra.object.Name = "alias-boundary-over"
	extra.object.Definition.Name = "alias-boundary-over"
	normalized, normalizeErr := knowledgedefinition.Normalize(extra.object.Definition)
	if normalizeErr != nil {
		t.Fatalf("normalize over-limit object: %v", normalizeErr)
	}
	extra.object.Definition = normalized.Definition
	extra.object.DefinitionSHA256 = bytes.Clone(normalized.Digest[:])
	over.winners = append(over.winners, extra)
	over.expectedWinnerCount++
	if _, err := compilePublicationWinnerCohort(t.Context(), over, candidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over object boundary error = %v, want CapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortDependencyBoundary(t *testing.T) {
	cohort, candidate := publicationDependencyBoundaryCohort(t, "input_00")
	authority, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil || authority.IsZero() || len(authority.databaseProjection()) != 1 {
		t.Fatalf("exact dependency boundary = (%#v, %v), want one candidate edge", authority, err)
	}

	over, _ := publicationDependencyBoundaryCohort(
		t,
		"coalesce(input_00, input_01)",
	)
	overCandidate := publicationTestCandidate(
		publicationTestWinnerByID(over, "ko-candidate-calculated").object,
	)
	if _, err := compilePublicationWinnerCohort(t.Context(), over, overCandidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("over dependency boundary error = %v, want CapacityExceeded", err)
	}
}

func TestCompilePublicationWinnerCohortDetachesIngressBeforeCompilation(t *testing.T) {
	cohort, candidate := publicationTestChain(t)
	expectedInput := publicationCloneCohort(cohort)
	expected, err := compilePublicationWinnerCohort(t.Context(), expectedInput, candidate)
	if err != nil {
		t.Fatalf("compile expected cohort: %v", err)
	}
	calculated := publicationTestWinnerIndex(&cohort, "ko-calculated")
	mutation := &publicationMutationContext{
		Context:  t.Context(),
		mutateAt: 2,
		mutate: func() {
			cohort.winners[calculated].existingDependencies[0].targetObjectID = "caller-mutated"
		},
	}
	actual, err := compilePublicationWinnerCohort(mutation, cohort, candidate)
	if err != nil {
		t.Fatalf("compile cohort across caller mutation: %v", err)
	}
	if !mutation.mutated || !actual.Equal(expected) {
		t.Fatalf("ingress detachment = mutated %t authorityEqual %t", mutation.mutated, actual.Equal(expected))
	}
}

func TestCompilePublicationWinnerCohortConcurrentDeterminism(t *testing.T) {
	cohort, candidate := publicationTestChain(t)
	expected, err := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
	if err != nil {
		t.Fatalf("compile expected cohort: %v", err)
	}
	const workers = 8
	errorsByWorker := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			compiled, compileErr := compilePublicationWinnerCohort(t.Context(), cohort, candidate)
			if compileErr != nil {
				errorsByWorker <- compileErr
				return
			}
			if !compiled.Equal(expected) {
				errorsByWorker <- errors.New("concurrent authority differs")
				return
			}
			targets := compiled.derivedTargets()
			if len(targets) != 1 {
				errorsByWorker <- fmt.Errorf("concurrent targets = %d, want 1", len(targets))
				return
			}
			targets[0].objectID = "worker-mutated"
			if compiled.derivedTargets()[0].objectID != "ko-extraction" {
				errorsByWorker <- errors.New("concurrent accessor aliases authority")
			}
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		t.Error(workerErr)
	}
}

type publicationMutationContext struct {
	context.Context
	mutateAt int
	calls    int
	mutated  bool
	mutate   func()
}

func (ctx *publicationMutationContext) Err() error {
	ctx.calls++
	if !ctx.mutated && ctx.calls == ctx.mutateAt {
		ctx.mutated = true
		ctx.mutate()
	}
	return ctx.Context.Err()
}

func publicationTestChain(t *testing.T) (publicationWinnerCohort, publicationCandidateAuthority) {
	t.Helper()
	extraction := publicationTestObject(t, "ko-extraction", 3, dependencyExtractionDefinition(
		"app-a", "extract-a", SharingScopeApp, nil, "", "extracted_value",
	))
	alias := publicationTestObject(t, "ko-alias", 7, dependencyAliasDefinition(
		"app-a", "alias-a", SharingScopeApp, nil, "", "extracted_value", "alias_value",
	))
	calculated := publicationTestObject(t, "ko-calculated", 11, dependencyCalculatedDefinition(
		"app-a", "calculated-a", SharingScopeApp, nil, "",
		"coalesce(alias_value, extracted_value)", "calculated_value",
	))
	role := opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT
	return publicationWinnerCohort{
		expectedWinnerCount: 3,
		winners: []publicationWinner{
			{
				object:                      calculated,
				existingDependenciesPresent: true,
				existingDependencies: []publicationPersistedDependency{
					{ordinal: 0, targetObjectID: "ko-alias", targetVersion: 7, role: role},
					{ordinal: 1, targetObjectID: "ko-extraction", targetVersion: 3, role: role},
				},
			},
			{object: extraction, existingDependenciesPresent: true},
			{object: alias},
		},
	}, publicationTestCandidate(alias)
}

func publicationTestExistingChain(t *testing.T) publicationWinnerCohort {
	t.Helper()
	cohort, _ := publicationTestChain(t)
	alias := publicationTestWinnerIndex(&cohort, "ko-alias")
	cohort.winners[alias].existingDependenciesPresent = true
	cohort.winners[alias].existingDependencies = []publicationPersistedDependency{{
		ordinal:        0,
		targetObjectID: "ko-extraction",
		targetVersion:  3,
		role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
	}}
	return cohort
}

func publicationTestAbsentCandidate(t *testing.T) publicationCandidateAuthority {
	t.Helper()
	return publicationTestCandidate(publicationTestObject(
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
	))
}

func publicationDependencyBoundaryCohort(
	t *testing.T,
	candidateExpression string,
) (publicationWinnerCohort, publicationCandidateAuthority) {
	t.Helper()
	const targets = 33
	role := opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT
	winners := make([]publicationWinner, 0, targets+knowledgeprogram.MaximumScalarExpressions)
	fields := make([]string, targets)
	rows := make([]publicationPersistedDependency, targets)
	for index := 0; index < targets; index++ {
		fields[index] = fmt.Sprintf("input_%02d", index)
		objectID := fmt.Sprintf("ko-target-%02d", index)
		definition := aliasDefinition(
			"app-a", fmt.Sprintf("target-%02d", index), SharingScopeApp, nil, "",
		)
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      fmt.Sprintf("(?<%s>.*)", fields[index]),
						OutputFields: []string{fields[index]},
					},
				},
			},
		}
		object := publicationTestObject(t, objectID, 1, definition)
		winners = append(winners, publicationWinner{
			object:                      object,
			existingDependenciesPresent: true,
		})
		rows[index] = publicationPersistedDependency{
			ordinal:        int64(index),
			targetObjectID: objectID,
			targetVersion:  1,
			role:           role,
		}
	}
	expression := publicationDenseDependencyExpression(fields)
	for index := 0; index < knowledgeprogram.MaximumScalarExpressions-1; index++ {
		object := publicationTestObject(
			t,
			fmt.Sprintf("ko-existing-calculated-%02d", index),
			1,
			dependencyCalculatedDefinition(
				"app-a",
				fmt.Sprintf("existing-calculated-%02d", index),
				SharingScopeApp,
				nil,
				"",
				expression,
				fmt.Sprintf("existing_output_%02d", index),
			),
		)
		winnerRows := append([]publicationPersistedDependency(nil), rows...)
		winners = append(winners, publicationWinner{
			object:                      object,
			existingDependenciesPresent: true,
			existingDependencies:        winnerRows,
		})
	}
	candidateObject := publicationTestObject(
		t,
		"ko-candidate-calculated",
		1,
		dependencyCalculatedDefinition(
			"app-a",
			"candidate-calculated",
			SharingScopeApp,
			nil,
			"",
			candidateExpression,
			"candidate_output",
		),
	)
	winners = append(winners, publicationWinner{object: candidateObject})
	return publicationWinnerCohort{
		expectedWinnerCount: uint32(len(winners)),
		winners:             winners,
	}, publicationTestCandidate(candidateObject)
}

func publicationDenseDependencyExpression(fields []string) string {
	parts := append([]string(nil), fields...)
	for len(parts) > 32 {
		parts = append(
			[]string{"coalesce(" + strings.Join(parts[:32], ",") + ")"},
			parts[32:]...,
		)
	}
	return "coalesce(" + strings.Join(parts, ",") + ")"
}

func publicationTestPair(
	t *testing.T,
	sourceScope, targetScope SharingScope,
	sourcePattern, targetPattern string,
) (publicationWinnerCohort, publicationCandidateAuthority) {
	t.Helper()
	target := publicationTestObject(t, "ko-target", 2, dependencyExtractionDefinition(
		"app-a", "extract-target", targetScope, nil, targetPattern, "derived_input",
	))
	source := publicationTestObject(t, "ko-source", 5, dependencyAliasDefinition(
		"app-a", "alias-source", sourceScope, nil, sourcePattern, "derived_input", "derived_output",
	))
	return publicationWinnerCohort{
		expectedWinnerCount: 2,
		winners: []publicationWinner{
			{object: source},
			{object: target, existingDependenciesPresent: true},
		},
	}, publicationTestCandidate(source)
}

func publicationTestObject(
	t *testing.T,
	objectID string,
	version uint64,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) knowledgesnapshot.Object {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("Normalize(%s): %v", objectID, err)
	}
	return knowledgesnapshot.Object{
		KnowledgeObjectID: objectID,
		Version:           version,
		ObjectType:        normalized.ObjectType,
		Name:              normalized.Name,
		AppID:             normalized.AppID,
		OwnerID:           "owner-a",
		SharingScope:      normalized.SharingScope,
		Definition:        normalized.Definition,
		DefinitionSHA256:  bytes.Clone(normalized.Digest[:]),
	}
}

func publicationTestCandidate(object knowledgesnapshot.Object) publicationCandidateAuthority {
	return publicationCandidateAuthority{
		objectID:         object.KnowledgeObjectID,
		version:          int64(object.Version),
		definitionDigest: publicationTestDigest(object.DefinitionSHA256),
		ownerID:          object.OwnerID,
	}
}

func publicationTestDigest(input []byte) [32]byte {
	var result [32]byte
	copy(result[:], input)
	return result
}

func publicationCloneCohort(input publicationWinnerCohort) publicationWinnerCohort {
	result := publicationWinnerCohort{
		expectedWinnerCount: input.expectedWinnerCount,
		winners:             make([]publicationWinner, len(input.winners)),
	}
	for index := range input.winners {
		result.winners[index] = publicationCloneWinner(input.winners[index])
	}
	return result
}

func publicationCloneWinner(input publicationWinner) publicationWinner {
	result := input
	result.object.Definition, _ = proto.Clone(input.object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	result.object.DefinitionSHA256 = bytes.Clone(input.object.DefinitionSHA256)
	if input.existingDependencies != nil {
		result.existingDependencies = append([]publicationPersistedDependency(nil), input.existingDependencies...)
	}
	return result
}

func publicationTestWinnerIndex(cohort *publicationWinnerCohort, objectID string) int {
	for index := range cohort.winners {
		if cohort.winners[index].object.KnowledgeObjectID == objectID {
			return index
		}
	}
	return -1
}

func publicationTestWinnerByID(cohort publicationWinnerCohort, objectID string) publicationWinner {
	index := publicationTestWinnerIndex(&cohort, objectID)
	if index < 0 {
		return publicationWinner{}
	}
	return cohort.winners[index]
}
