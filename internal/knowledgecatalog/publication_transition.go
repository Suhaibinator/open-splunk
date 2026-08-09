package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"math/bits"
	"slices"
	"sort"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"google.golang.org/protobuf/proto"
)

const (
	maximumPublicationTransitionDefinitionBytes       = uint64(64 << 20)
	maximumPublicationTransitionProjectionBytes       = uint64(64 << 20)
	maximumPublicationTransitionSelectorPatterns      = uint64(64 << 10)
	maximumPublicationTransitionSelectorBytes         = uint64(64 << 20)
	maximumPublicationTransitionSelectorWork          = uint64(64 << 10)
	maximumPublicationTransitionDependencies          = uint64(64 << 10)
	maximumPublicationTransitionClassStates           = uint64(64 << 10)
	maximumPublicationTransitionMembershipVisits      = uint64(1 << 20)
	maximumPublicationTransitionIndexSelectorProbes   = uint64(64 << 10)
	maximumPublicationTransitionIndexMatcherWork      = uint64(64 << 20)
	maximumPublicationTransitionChangedCohorts        = uint64(1024)
	maximumPublicationTransitionWinnerRevisits        = uint64(64 << 10)
	maximumPublicationTransitionDependencyRevisits    = uint64(64 << 10)
	maximumPublicationTransitionWinnerPairComparisons = uint64(1 << 20)

	maximumPublicationTransitionSemanticPrograms = uint64(64)
	// Cohort validation normalizes each winner before compiling, and the program
	// compiler independently normalizes it again.
	publicationTransitionCohortSelectorNormalizationPasses = uint64(2)
	maximumPublicationTransitionSelectorWorkRevisits       = publicationTransitionCohortSelectorNormalizationPasses *
		maximumPublicationTransitionSemanticPrograms * uint64(knowledge.MaximumSelectorWildcardWorkUnits)
	maximumPublicationTransitionGeneratedFields         = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumGeneratedFields)
	maximumPublicationTransitionRegexPrograms           = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumRegexPrograms)
	maximumPublicationTransitionRegexWorkUnits          = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumRegexWorkUnits)
	maximumPublicationTransitionExtractionOutputs       = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumExtractionOutputs)
	maximumPublicationTransitionJSONEvaluationWorkUnits = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumJSONEvaluationWorkUnits)
	maximumPublicationTransitionScalarExpressions       = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumScalarExpressions)
	maximumPublicationTransitionScalarExpressionNodes   = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumScalarExpressionNodes)
	maximumPublicationTransitionScalarPredicates        = maximumPublicationTransitionSemanticPrograms * uint64(knowledgesnapshot.MaximumScalarPredicates)
)

// publicationActiveTransitionInventory is one exact transactional ACTIVE
// inventory plus the exact object version produced by the proposed mutation.
// A caller must separately prove that currentActive completely enumerates the
// tenant's current ACTIVE objects. potentiallySearchableIndexNames completely
// enumerates live index rows in an ACTIVE or ARCHIVED state, regardless of
// search_enabled. DELETING rows and terminal tombstones are excluded because
// they can never authorize a future search again. Future index-name creation
// remains separately gated.
//
// Explicit endpoints prevent an ACTIVE->DISABLED proof from being replayed as
// ACTIVE->DELETED, and bind the inactive predecessor of an enable operation.
// The pure validator does not open any Writer or index-lifecycle gate.
type publicationActiveTransitionInventory struct {
	tenantID                                string
	expectedDefinitionBytes                 uint64
	expectedProjectionBytes                 uint64
	expectedSelectorPatterns                uint64
	expectedSelectorValueBytes              uint64
	expectedCanonicalSelectorBytes          uint64
	expectedSelectorWork                    uint64
	expectedDependencyCount                 uint64
	expectedActiveAppCount                  uint16
	activeAppIDs                            []string
	expectedCurrentActiveCount              uint32
	currentActive                           []publicationWinner
	candidateBefore                         publicationTransitionEndpoint
	candidateAfter                          publicationTransitionEndpoint
	expectedPotentiallySearchableIndexCount uint16
	potentiallySearchableIndexNames         []string
}

type publicationTransitionEndpoint struct {
	present bool
	state   State
	winner  publicationWinner
}

// publicationTransitionPersistenceEndpoint is the body-free scalar authority
// a future same-transaction persistence adapter must match. Persisted rows are
// retained in canonical order so disable/delete cannot swap dependency
// authority while reusing a semantic transition proof.
type publicationTransitionPersistenceEndpoint struct {
	present                     bool
	state                       State
	objectID                    string
	version                     int64
	objectType                  opensplunkv1.KnowledgeObjectType
	name                        string
	appID                       string
	ownerID                     string
	sharingScope                opensplunkv1.SharingScope
	definitionDigest            [sha256.Size]byte
	existingDependenciesPresent bool
	existingDependencies        []publicationPersistedDependency
}

type publicationTransitionPersistenceBinding struct {
	tenantID     string
	before       publicationTransitionPersistenceEndpoint
	after        publicationTransitionPersistenceEndpoint
	dependencies []publicationDependency
}

// publicationActiveTransitionAuthority is pointer-backed so a validated
// removal with no candidate dependency projection cannot collapse into an
// uninitialized value.
type publicationActiveTransitionAuthority struct {
	state *publicationActiveTransitionAuthorityState
}

type publicationActiveTransitionAuthorityState struct {
	transitionCommitment  [sha256.Size]byte
	tenantID              string
	beforePersistence     publicationTransitionPersistenceEndpoint
	afterPersistence      publicationTransitionPersistenceEndpoint
	candidateDependencies candidateDependencyAuthority
}

func (authority publicationActiveTransitionAuthority) IsZero() bool {
	return authority.state == nil
}

func (authority publicationActiveTransitionAuthority) Equal(
	other publicationActiveTransitionAuthority,
) bool {
	if authority.state == nil || other.state == nil {
		return authority.state == nil && other.state == nil
	}
	return authority.state.transitionCommitment == other.state.transitionCommitment &&
		authority.state.tenantID == other.state.tenantID &&
		publicationTransitionPersistenceEndpointEqual(
			authority.state.beforePersistence,
			other.state.beforePersistence,
		) &&
		publicationTransitionPersistenceEndpointEqual(
			authority.state.afterPersistence,
			other.state.afterPersistence,
		) &&
		authority.state.candidateDependencies.Equal(other.state.candidateDependencies)
}

// matchesPersistence binds a future same-transaction write plan to the exact
// tenant, before/after scalar endpoints, retained ordered rows, and dependency
// projection proved by this transition. Comparisons are bounded by the
// detached authority: unequal caller-sized slices fail on length before any
// element traversal, and equal-length slices are within admitted limits.
func (authority publicationActiveTransitionAuthority) matchesPersistence(
	binding publicationTransitionPersistenceBinding,
) bool {
	if authority.state == nil {
		return false
	}
	return authority.state.tenantID == binding.tenantID &&
		publicationTransitionPersistenceEndpointEqual(
			authority.state.beforePersistence,
			binding.before,
		) &&
		publicationTransitionPersistenceEndpointEqual(
			authority.state.afterPersistence,
			binding.after,
		) &&
		publicationTransitionPersistenceDependenciesEqual(
			authority.state.afterPersistence,
			authority.state.candidateDependencies,
			binding.dependencies,
		)
}

func (authority publicationActiveTransitionAuthority) candidateDependencies() candidateDependencyAuthority {
	if authority.state == nil {
		return candidateDependencyAuthority{}
	}
	return authority.state.candidateDependencies.detached()
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

type publicationTransitionCanonicalObject struct {
	winner                 publicationWinner
	canonical              canonicalPublicationWinner
	indexSelector          *knowledge.Selector
	indexProgram           knowledge.DimensionRuntimeProgram
	indexProgramKey        string
	allIndexes             bool
	projectionBytes        uint64
	selectorPatterns       uint64
	selectorValueBytes     uint64
	canonicalSelectorBytes uint64
}

type publicationTransitionPrincipalClassKind uint8

const (
	publicationTransitionGenericApp publicationTransitionPrincipalClassKind = iota
	publicationTransitionGenericPrincipal
	publicationTransitionPrivatePrincipal
)

type publicationTransitionPrincipalClass struct {
	kind    publicationTransitionPrincipalClassKind
	appID   string
	ownerID string
}

type publicationTransitionPrivatePrincipalKey struct {
	appID   string
	ownerID string
}

type publicationTransitionWinnerKey struct {
	objectID         string
	version          int64
	definitionDigest [sha256.Size]byte
	ownerID          string
	appID            string
	sharingScope     opensplunkv1.SharingScope
	objectType       opensplunkv1.KnowledgeObjectType
	name             string
}

type publicationTransitionWork struct {
	membershipVisits        uint64
	changedCohorts          uint64
	winnerRevisits          uint64
	dependencyRevisits      uint64
	definitionRevisits      uint64
	selectorWorkRevisits    uint64
	winnerPairComparisons   uint64
	generatedFields         uint64
	regexPrograms           uint64
	regexWorkUnits          uint64
	extractionOutputs       uint64
	jsonEvaluationWorkUnits uint64
	scalarExpressions       uint64
	scalarExpressionNodes   uint64
	scalarPredicates        uint64
}

type publicationTransitionHydrationCharge struct {
	definitionBytes    uint64
	projectionBytes    uint64
	selectorPatterns   uint64
	selectorValueBytes uint64
	dependencies       uint64
}

type publicationTransitionAggregateCharge struct {
	definitionBytes        uint64
	projectionBytes        uint64
	selectorPatterns       uint64
	selectorValueBytes     uint64
	canonicalSelectorBytes uint64
	selectorWork           uint64
	dependencies           uint64
}

type publicationTransitionClassHydration struct {
	preCharge        publicationTransitionHydrationCharge
	postCharge       publicationTransitionHydrationCharge
	postDependencies uint64
	candidateVisible bool
}

type publicationTransitionValidatedCohort struct {
	winners           []*publicationTransitionCanonicalObject
	candidateWins     bool
	programCommitment [sha256.Size]byte
}

// publicationTransitionEvaluator owns the bounded class-by-signature
// traversal. Its caller establishes and hashes the inventory prefix; evaluate
// appends the exact cohort transcript in traversal order and returns the one
// cross-cohort candidate dependency authority it proves.
type publicationTransitionEvaluator struct {
	ctx                  context.Context
	classes              []publicationTransitionPrincipalClass
	signatures           []publicationIndexORSignature
	preSlots             []*publicationTransitionCanonicalObject
	postSlots            []*publicationTransitionCanonicalObject
	candidateAfter       *publicationTransitionCanonicalObject
	postCandidate        publicationCandidateAuthority
	beforeActive         bool
	afterActive          bool
	classHydration       []publicationTransitionClassHydration
	semanticHasher       hash.Hash
	seenChangedCohorts   map[[sha256.Size]byte][]publicationTransitionValidatedCohort
	winnerKeyCommitments map[*publicationTransitionCanonicalObject][sha256.Size]byte
	work                 publicationTransitionWork
}

func (evaluator *publicationTransitionEvaluator) evaluate() (
	candidateDependencyAuthority,
	uint64,
	error,
) {
	var candidateDependencies candidateDependencyAuthority
	candidateWinningWitnesses := uint64(0)
	for classIndex, class := range evaluator.classes {
		if err := evaluator.ctx.Err(); err != nil {
			return candidateDependencyAuthority{}, 0, err
		}
		publicationTransitionHashPrincipalClass(evaluator.semanticHasher, class)
		publicationTransitionHashHydration(
			evaluator.semanticHasher,
			evaluator.classHydration[classIndex],
		)
		for signatureIndex := range evaluator.signatures {
			signature := &evaluator.signatures[signatureIndex]
			if err := evaluator.ctx.Err(); err != nil {
				return candidateDependencyAuthority{}, 0, err
			}
			beforeWinners, err := selectPublicationTransitionWinners(
				evaluator.ctx,
				class,
				&signature.before,
				evaluator.preSlots,
				nil,
				false,
				&evaluator.work,
			)
			if err != nil {
				return candidateDependencyAuthority{}, 0, err
			}
			var proposedCandidate *publicationTransitionCanonicalObject
			if evaluator.afterActive {
				proposedCandidate = evaluator.candidateAfter
			}
			afterWinners, err := selectPublicationTransitionWinners(
				evaluator.ctx,
				class,
				&signature.after,
				evaluator.postSlots,
				proposedCandidate,
				evaluator.beforeActive,
				&evaluator.work,
			)
			if err != nil {
				return candidateDependencyAuthority{}, 0, err
			}

			candidateWins := publicationTransitionWinnersContain(
				afterWinners,
				evaluator.postCandidate,
			)
			if candidateWins {
				candidateWinningWitnesses++
			}
			publicationTransitionHashIndexSignature(evaluator.semanticHasher, signature)
			publicationTransitionHashWinnerPointers(
				evaluator.semanticHasher,
				beforeWinners,
				evaluator.winnerKeyCommitments,
			)
			publicationTransitionHashWinnerPointers(
				evaluator.semanticHasher,
				afterWinners,
				evaluator.winnerKeyCommitments,
			)
			publicationTransitionHashBool(evaluator.semanticHasher, candidateWins)
			changed := !slices.Equal(beforeWinners, afterWinners)
			publicationTransitionHashBool(evaluator.semanticHasher, changed)
			if !changed {
				continue
			}

			cohortCommitment := publicationTransitionCohortCommitment(
				afterWinners,
				candidateWins,
				evaluator.winnerKeyCommitments,
			)
			var prior *publicationTransitionValidatedCohort
			bucket := evaluator.seenChangedCohorts[cohortCommitment]
			for index := range bucket {
				record := &bucket[index]
				if record.candidateWins == candidateWins && slices.Equal(record.winners, afterWinners) {
					prior = record
					break
				}
			}
			if prior != nil {
				_, _ = evaluator.semanticHasher.Write(prior.programCommitment[:])
				continue
			}
			if err := evaluator.work.chargeChangedCohort(afterWinners); err != nil {
				return candidateDependencyAuthority{}, 0, err
			}
			cohort := publicationWinnerCohort{
				expectedWinnerCount: uint32(len(afterWinners)),
				winners:             make([]publicationWinner, len(afterWinners)),
			}
			for index, winner := range afterWinners {
				cohort.winners[index] = winner.winner
			}
			// Keep knowledgeprogram compilation as an independent semantic
			// authority check. The repeated normalization/compilation work is
			// charged above across every unique changed cohort.
			cohortAuthority, err := validatePublicationWinnerCohort(
				evaluator.ctx,
				cohort,
				evaluator.postCandidate,
				candidateWins,
			)
			if err != nil {
				return candidateDependencyAuthority{}, 0, err
			}
			programCommitment, present := cohortAuthority.programCommitment()
			if !present || programCommitment == ([sha256.Size]byte{}) {
				return candidateDependencyAuthority{}, 0, invalidPublicationTransition(
					"changed cohort program commitment is absent",
				)
			}
			evaluator.seenChangedCohorts[cohortCommitment] = append(
				evaluator.seenChangedCohorts[cohortCommitment],
				publicationTransitionValidatedCohort{
					winners:           slices.Clone(afterWinners),
					candidateWins:     candidateWins,
					programCommitment: programCommitment,
				},
			)
			_, _ = evaluator.semanticHasher.Write(programCommitment[:])
			if !candidateWins {
				continue
			}
			if cohortAuthority.state == nil {
				return candidateDependencyAuthority{}, 0, invalidPublicationTransition(
					"changed cohort candidate authority is absent",
				)
			}
			derived := cohortAuthority.state.candidateDependencies
			if err := evaluator.work.chargeDerivedDependencies(
				publicationTransitionDerivedTargetCount(derived),
			); err != nil {
				return candidateDependencyAuthority{}, 0, err
			}
			if candidateDependencies.IsZero() {
				candidateDependencies = derived
				continue
			}
			if !candidateDependencies.Equal(derived) {
				return candidateDependencyAuthority{}, 0, fmt.Errorf(
					"%w: publication candidate dependency authority differs across winner cohorts",
					control.ErrDependencyConflict,
				)
			}
		}
	}
	return candidateDependencies, candidateWinningWitnesses, nil
}

// validatePublicationActiveTransition proves every reachable post-mutation
// winner cohort derived from one complete bounded inventory. It validates and
// detaches all definitions before index pruning, pairs before/after physical
// index applicability, and compiles every distinct changed post cohort.
//
// This boundary intentionally accepts only definition bodies recognized by
// this binary. It must not be wired over the existing opaque-future ACTIVE
// disable/delete emergency path; a later projection-only transactional proof
// must cover that path without reinterpreting its body.
func validatePublicationActiveTransition(
	ctx context.Context,
	input publicationActiveTransitionInventory,
) (publicationActiveTransitionAuthority, error) {
	if ctx == nil {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication transition context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := preflightPublicationActiveTransition(input); err != nil {
		return publicationActiveTransitionAuthority{}, err
	}

	// Admission normalizes and detaches every bounded definition, selector,
	// dependency row, and scalar before the first caller-controlled context
	// callback. Normalization performs its own shape preflight before cloning.
	detached, current, activeCandidateAfter, err := admitPublicationActiveTransitionInventory(input)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	if err := ctx.Err(); err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	candidateID := detached.candidateAfter.winner.object.KnowledgeObjectID
	beforeActive := detached.candidateBefore.present && detached.candidateBefore.state == StateActive
	afterActive := detached.candidateAfter.state == StateActive

	currentByID := make(map[string]*publicationTransitionCanonicalObject, len(current))
	var preCandidate *publicationTransitionCanonicalObject
	for index := range current {
		if err := ctx.Err(); err != nil {
			return publicationActiveTransitionAuthority{}, err
		}
		object := current[index]
		objectID := object.canonical.key.objectID
		if _, duplicate := currentByID[objectID]; duplicate {
			return publicationActiveTransitionAuthority{}, invalidPublicationTransition(
				"duplicates a current ACTIVE object identity",
			)
		}
		currentByID[objectID] = object
		if objectID == candidateID {
			preCandidate = object
		}
	}
	if beforeActive != (preCandidate != nil) {
		return publicationActiveTransitionAuthority{}, invalidPublicationTransition(
			"candidate pre-ACTIVE presence disagrees",
		)
	}
	candidateBefore, candidateAfter, err := validatePublicationTransitionDisposition(
		preCandidate,
		activeCandidateAfter,
		detached.candidateBefore,
		detached.candidateAfter,
	)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	postCandidate := publicationCandidateAuthorityFromCanonical(candidateAfter.canonical)
	if afterActive && !slices.Contains(detached.activeAppIDs, candidateAfter.canonical.object.GetAppId()) {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication candidate defining app is not active",
			control.ErrInvalidArgument,
		)
	}
	postDependencyCount, err := validatePublicationTransitionPostAggregate(
		detached,
		beforeActive,
		candidateBefore,
		afterActive,
		candidateAfter,
	)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}

	postByID := make(map[string]*publicationTransitionCanonicalObject, len(current)+1)
	for objectID, object := range currentByID {
		postByID[objectID] = object
	}
	delete(postByID, candidateID)
	if afterActive {
		postByID[candidateID] = candidateAfter
	}
	if len(postByID) > MaximumResolutionCandidates {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication transition post-ACTIVE inventory exceeds its object limit",
			control.ErrCapacityExceeded,
		)
	}

	objectIDs := make([]string, 0, len(currentByID)+1)
	for objectID := range currentByID {
		objectIDs = append(objectIDs, objectID)
	}
	if _, exists := currentByID[candidateID]; !exists && afterActive {
		objectIDs = append(objectIDs, candidateID)
	}
	slices.Sort(objectIDs)
	preSlots := make([]*publicationTransitionCanonicalObject, len(objectIDs))
	postSlots := make([]*publicationTransitionCanonicalObject, len(objectIDs))
	for index, objectID := range objectIDs {
		preSlots[index] = currentByID[objectID]
		postSlots[index] = postByID[objectID]
	}
	if err := ctx.Err(); err != nil {
		return publicationActiveTransitionAuthority{}, err
	}

	classes, err := publicationTransitionPrincipalClasses(
		ctx,
		detached.activeAppIDs,
		preSlots,
		postSlots,
	)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	classHydration, err := validatePublicationTransitionClassHydration(
		ctx,
		classes,
		preSlots,
		postSlots,
		candidateAfter,
	)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	atoms, err := publicationTransitionIndexAtoms(
		ctx,
		detached.potentiallySearchableIndexNames,
		preSlots,
		postSlots,
	)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	signatures, err := enumeratePublicationIndexORSignatures(ctx, atoms)
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	classStates, ok := publicationTransitionProductWithin(
		uint64(len(classes)),
		uint64(len(signatures)),
		maximumPublicationTransitionClassStates,
	)
	if !ok {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication transition exceeds its visibility-state limit",
			control.ErrCapacityExceeded,
		)
	}
	inventoryCommitment := publicationTransitionInventoryCommitment(
		detached,
		current,
		postSlots,
		candidateBefore,
		candidateAfter,
	)
	semanticCommitment := sha256.New()
	publicationTransitionHashString(
		semanticCommitment,
		"open-splunk/publication-active-transition-semantic/v1",
	)
	_, _ = semanticCommitment.Write(inventoryCommitment[:])
	publicationTransitionHashUint64(semanticCommitment, classStates)
	evaluator := publicationTransitionEvaluator{
		ctx:                  ctx,
		classes:              classes,
		signatures:           signatures,
		preSlots:             preSlots,
		postSlots:            postSlots,
		candidateAfter:       candidateAfter,
		postCandidate:        postCandidate,
		beforeActive:         beforeActive,
		afterActive:          afterActive,
		classHydration:       classHydration,
		semanticHasher:       semanticCommitment,
		seenChangedCohorts:   make(map[[sha256.Size]byte][]publicationTransitionValidatedCohort),
		winnerKeyCommitments: make(map[*publicationTransitionCanonicalObject][sha256.Size]byte),
	}
	candidateDependencies, candidateWinningWitnesses, err := evaluator.evaluate()
	if err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	if afterActive && candidateWinningWitnesses == 0 {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication candidate has no current-index winning witness",
			control.ErrDependencyConflict,
		)
	}
	if afterActive && candidateDependencies.IsZero() {
		return publicationActiveTransitionAuthority{}, fmt.Errorf(
			"%w: publication candidate winner authority is absent",
			control.ErrDependencyConflict,
		)
	}
	if afterActive {
		derivedCount := uint64(publicationTransitionDerivedTargetCount(candidateDependencies))
		if postDependencyCount > maximumPublicationTransitionDependencies ||
			derivedCount > maximumPublicationTransitionDependencies-postDependencyCount {
			return publicationActiveTransitionAuthority{}, fmt.Errorf(
				"%w: publication transition post-ACTIVE dependencies exceed their aggregate limit",
				control.ErrCapacityExceeded,
			)
		}
		for _, hydration := range classHydration {
			if !hydration.candidateVisible {
				continue
			}
			if hydration.postDependencies > uint64(resolutionHydrationBudget.dependencies) ||
				derivedCount > uint64(resolutionHydrationBudget.dependencies)-hydration.postDependencies {
				return publicationActiveTransitionAuthority{}, fmt.Errorf(
					"%w: publication transition post cohort exceeds dependency hydration limits",
					control.ErrCapacityExceeded,
				)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return publicationActiveTransitionAuthority{}, err
	}
	publicationTransitionHashCandidateDependencies(semanticCommitment, candidateDependencies)
	var commitment [sha256.Size]byte
	copy(commitment[:], semanticCommitment.Sum(nil))

	return publicationActiveTransitionAuthority{state: &publicationActiveTransitionAuthorityState{
		transitionCommitment:  commitment,
		tenantID:              strings.Clone(detached.tenantID),
		beforePersistence:     publicationTransitionPersistenceEndpointFrom(detached.candidateBefore),
		afterPersistence:      publicationTransitionPersistenceEndpointFrom(detached.candidateAfter),
		candidateDependencies: candidateDependencies,
	}}, nil
}

func preflightPublicationActiveTransition(input publicationActiveTransitionInventory) error {
	if !validIdentity(input.tenantID, maximumTenantIDBytes) {
		return fmt.Errorf(
			"%w: publication transition tenant authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	beforeActive := input.candidateBefore.present && input.candidateBefore.state == StateActive
	afterActive := input.candidateAfter.present && input.candidateAfter.state == StateActive
	if !input.candidateAfter.present ||
		(!input.candidateBefore.present && !afterActive) ||
		(input.candidateBefore.present && input.candidateBefore.state == StateActive &&
			input.candidateAfter.state != StateActive &&
			input.candidateAfter.state != StateDisabled &&
			input.candidateAfter.state != StateDeleted) ||
		(input.candidateBefore.present &&
			(input.candidateBefore.state == StateDraft || input.candidateBefore.state == StateDisabled) &&
			!afterActive) ||
		(input.candidateBefore.present && input.candidateBefore.state != StateActive &&
			input.candidateBefore.state != StateDraft && input.candidateBefore.state != StateDisabled) {
		return fmt.Errorf(
			"%w: publication transition endpoint state matrix is invalid",
			control.ErrInvalidArgument,
		)
	}
	if !input.candidateBefore.present &&
		(input.candidateBefore.state != "" || !publicationTransitionWinnerIsZero(input.candidateBefore.winner)) {
		return fmt.Errorf(
			"%w: absent publication transition endpoint carries authority",
			control.ErrInvalidArgument,
		)
	}
	if input.expectedDefinitionBytes > maximumPublicationTransitionDefinitionBytes ||
		input.expectedProjectionBytes > maximumPublicationTransitionProjectionBytes ||
		input.expectedSelectorPatterns > maximumPublicationTransitionSelectorPatterns ||
		input.expectedSelectorValueBytes > maximumPublicationTransitionSelectorBytes ||
		input.expectedCanonicalSelectorBytes > maximumPublicationTransitionSelectorBytes ||
		input.expectedSelectorWork > maximumPublicationTransitionSelectorWork ||
		input.expectedDependencyCount > maximumPublicationTransitionDependencies {
		return fmt.Errorf(
			"%w: publication transition aggregate scalar authority exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	if int(input.expectedActiveAppCount) != len(input.activeAppIDs) {
		return invalidPublicationTransition("active-app inventory is incomplete")
	}
	if len(input.activeAppIDs) == 0 || len(input.activeAppIDs) > maximumReadableApps {
		return fmt.Errorf(
			"%w: publication transition active-app inventory exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	for _, appID := range input.activeAppIDs {
		if !validIdentity(appID, maximumAppIDBytes) {
			return invalidPublicationTransition("contains an invalid active-app identity")
		}
	}
	if int(input.expectedCurrentActiveCount) != len(input.currentActive) {
		return invalidPublicationTransition("current ACTIVE inventory is incomplete")
	}
	if len(input.currentActive) > MaximumResolutionCandidates {
		return fmt.Errorf(
			"%w: publication transition exceeds its current ACTIVE object limit",
			control.ErrCapacityExceeded,
		)
	}
	postCount := len(input.currentActive)
	if beforeActive {
		postCount--
	}
	if afterActive {
		postCount++
	}
	if postCount < 0 || postCount > MaximumResolutionCandidates {
		return fmt.Errorf(
			"%w: publication transition exceeds its post-ACTIVE object limit",
			control.ErrCapacityExceeded,
		)
	}
	if int(input.expectedPotentiallySearchableIndexCount) != len(input.potentiallySearchableIndexNames) {
		return invalidPublicationTransition("potentially-searchable index inventory is incomplete")
	}
	if len(input.potentiallySearchableIndexNames) > maximumPublicationIndexAtoms {
		return fmt.Errorf(
			"%w: publication transition exceeds its potentially-searchable index limit",
			control.ErrCapacityExceeded,
		)
	}
	for _, name := range input.potentiallySearchableIndexNames {
		if len(name) == 0 || len(name) > maximumFilterBytes {
			return invalidPublicationTransition(
				"contains an invalid potentially-searchable index identity",
			)
		}
		canonical, err := control.NormalizeIndexName(name)
		if err != nil || canonical != name {
			return invalidPublicationTransition(
				"contains a noncanonical potentially-searchable index identity",
			)
		}
	}

	var dependencies uint64
	for index := range input.currentActive {
		winner := input.currentActive[index]
		if !winner.existingDependenciesPresent {
			return invalidPublicationTransition("current ACTIVE object omits dependency authority")
		}
		if err := validatePersistedPublicationDependencyScalars(winner.existingDependencies); err != nil {
			return err
		}
		if !addPublicationResource(
			&dependencies,
			uint64(len(winner.existingDependencies)),
			maximumPublicationTransitionDependencies,
		) {
			return fmt.Errorf(
				"%w: publication transition exceeds its aggregate dependency limit",
				control.ErrCapacityExceeded,
			)
		}
	}
	for endpointIndex, endpoint := range []publicationTransitionEndpoint{
		input.candidateBefore,
		input.candidateAfter,
	} {
		if !endpoint.present {
			continue
		}
		if endpointIndex == 1 && endpoint.state == StateActive {
			if endpoint.winner.existingDependenciesPresent ||
				endpoint.winner.existingDependencies != nil {
				return fmt.Errorf(
					"%w: ACTIVE publication candidate submits dependency rows",
					control.ErrInvalidArgument,
				)
			}
			continue
		}
		if !endpoint.winner.existingDependenciesPresent {
			if endpointIndex == 0 {
				return invalidPublicationTransition(
					"persisted candidate endpoint omits dependency authority",
				)
			}
			return fmt.Errorf(
				"%w: publication transition endpoint omits dependency authority",
				control.ErrInvalidArgument,
			)
		}
		var err error
		if endpointIndex == 1 {
			err = validatePublicationTransitionCandidateDependencyScalars(
				endpoint.winner.existingDependencies,
			)
		} else {
			err = validatePersistedPublicationDependencyScalars(
				endpoint.winner.existingDependencies,
			)
		}
		if err != nil {
			return err
		}
	}
	if dependencies != input.expectedDependencyCount {
		return invalidPublicationTransition("aggregate dependency scalar authority disagrees")
	}
	return nil
}

func publicationTransitionWinnerIsZero(winner publicationWinner) bool {
	object := winner.object
	return object.KnowledgeObjectID == "" && object.Version == 0 &&
		object.ObjectType == 0 && object.Name == "" && object.AppID == "" &&
		object.OwnerID == "" && object.SharingScope == 0 &&
		object.Definition == nil && object.DefinitionSHA256 == nil &&
		!winner.existingDependenciesPresent && winner.existingDependencies == nil
}

func validatePublicationTransitionCandidateDependencyScalars(
	dependencies []publicationPersistedDependency,
) error {
	if len(dependencies) > maximumDependenciesPerVersion {
		return fmt.Errorf(
			"%w: publication transition candidate dependency rows exceed their limit",
			control.ErrCapacityExceeded,
		)
	}
	for index, dependency := range dependencies {
		if dependency.ordinal != int64(index) ||
			!validIdentity(dependency.targetObjectID, maximumObjectIDBytes) ||
			dependency.targetVersion < 1 ||
			dependency.role != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
			(index > 0 && !publicationPersistedDependencyAfter(dependencies[index-1], dependency)) {
			return fmt.Errorf(
				"%w: publication transition candidate dependency rows are malformed",
				control.ErrInvalidArgument,
			)
		}
	}
	return nil
}

func admitPublicationActiveTransitionInventory(
	input publicationActiveTransitionInventory,
) (
	publicationActiveTransitionInventory,
	[]*publicationTransitionCanonicalObject,
	*publicationTransitionCanonicalObject,
	error,
) {
	result := publicationActiveTransitionInventory{
		tenantID:                       strings.Clone(input.tenantID),
		expectedDefinitionBytes:        input.expectedDefinitionBytes,
		expectedProjectionBytes:        input.expectedProjectionBytes,
		expectedSelectorPatterns:       input.expectedSelectorPatterns,
		expectedSelectorValueBytes:     input.expectedSelectorValueBytes,
		expectedCanonicalSelectorBytes: input.expectedCanonicalSelectorBytes,
		expectedSelectorWork:           input.expectedSelectorWork,
		expectedDependencyCount:        input.expectedDependencyCount,
		expectedActiveAppCount:         input.expectedActiveAppCount,
		activeAppIDs:                   make([]string, len(input.activeAppIDs)),
		expectedCurrentActiveCount:     input.expectedCurrentActiveCount,
		candidateBefore: publicationTransitionEndpoint{
			present: input.candidateBefore.present,
			state:   input.candidateBefore.state,
		},
		candidateAfter: publicationTransitionEndpoint{
			present: input.candidateAfter.present,
			state:   input.candidateAfter.state,
		},
		expectedPotentiallySearchableIndexCount: input.expectedPotentiallySearchableIndexCount,
		potentiallySearchableIndexNames: make(
			[]string,
			len(input.potentiallySearchableIndexNames),
		),
	}
	for index, appID := range input.activeAppIDs {
		result.activeAppIDs[index] = strings.Clone(appID)
	}
	for index, name := range input.potentiallySearchableIndexNames {
		result.potentiallySearchableIndexNames[index] = strings.Clone(name)
	}

	current := make([]*publicationTransitionCanonicalObject, len(input.currentActive))
	var definitionBytes uint64
	var projectionBytes uint64
	var selectorPatterns uint64
	var selectorValueBytes uint64
	var canonicalSelectorBytes uint64
	var selectorWork uint64
	for index := range input.currentActive {
		object, err := canonicalizePublicationTransitionObject(input.currentActive[index], false)
		if err != nil {
			return publicationActiveTransitionInventory{}, nil, nil, err
		}
		if !addPublicationResource(&definitionBytes, object.canonical.definitionBytes, maximumPublicationTransitionDefinitionBytes) ||
			!addPublicationResource(&projectionBytes, object.projectionBytes, maximumPublicationTransitionProjectionBytes) ||
			!addPublicationResource(&selectorPatterns, object.selectorPatterns, maximumPublicationTransitionSelectorPatterns) ||
			!addPublicationResource(&selectorValueBytes, object.selectorValueBytes, maximumPublicationTransitionSelectorBytes) ||
			!addPublicationResource(&canonicalSelectorBytes, object.canonicalSelectorBytes, maximumPublicationTransitionSelectorBytes) ||
			!addPublicationResource(&selectorWork, object.canonical.selectorWork, maximumPublicationTransitionSelectorWork) {
			return publicationActiveTransitionInventory{}, nil, nil, fmt.Errorf(
				"%w: publication transition exceeds canonical aggregate resource limits",
				control.ErrCapacityExceeded,
			)
		}
		current[index] = object
	}
	if definitionBytes != input.expectedDefinitionBytes ||
		projectionBytes != input.expectedProjectionBytes ||
		selectorPatterns != input.expectedSelectorPatterns ||
		selectorValueBytes != input.expectedSelectorValueBytes ||
		canonicalSelectorBytes != input.expectedCanonicalSelectorBytes ||
		selectorWork != input.expectedSelectorWork {
		return publicationActiveTransitionInventory{}, nil, nil, invalidPublicationTransition(
			"aggregate projection and definition scalar authority disagrees",
		)
	}
	if input.candidateBefore.present {
		before, err := normalizePublicationTransitionEndpointWinner(
			input.candidateBefore.winner,
			true,
		)
		if err != nil {
			return publicationActiveTransitionInventory{}, nil, nil, err
		}
		result.candidateBefore.winner = before
	}
	if input.candidateAfter.state == StateActive {
		activeAfter, err := canonicalizePublicationTransitionObject(
			input.candidateAfter.winner,
			true,
		)
		if err != nil {
			return publicationActiveTransitionInventory{}, nil, nil, err
		}
		result.candidateAfter.winner = activeAfter.winner
		return result, current, activeAfter, nil
	}
	after, err := normalizePublicationTransitionEndpointWinner(input.candidateAfter.winner, false)
	if err != nil {
		return publicationActiveTransitionInventory{}, nil, nil, err
	}
	result.candidateAfter.winner = after
	return result, current, nil, nil
}

func normalizePublicationTransitionEndpointWinner(
	input publicationWinner,
	persisted bool,
) (publicationWinner, error) {
	object := input.object
	if !validIdentity(object.KnowledgeObjectID, maximumObjectIDBytes) ||
		object.Version == 0 || object.Version > math.MaxInt64 ||
		!validIdentity(object.AppID, maximumAppIDBytes) ||
		!validIdentity(object.OwnerID, maximumOwnerIDBytes) ||
		object.Definition == nil || len(object.DefinitionSHA256) != sha256.Size {
		return publicationWinner{}, invalidPublicationTransitionEndpointAuthority(
			persisted,
			false,
			"endpoint object authority is invalid",
		)
	}
	normalized, err := knowledgedefinition.Normalize(object.Definition)
	if err != nil {
		if errors.Is(err, knowledgedefinition.ErrDefinitionTooLarge) {
			return publicationWinner{}, invalidPublicationTransitionEndpointAuthority(
				persisted,
				true,
				"endpoint definition exceeds its limit",
			)
		}
		return publicationWinner{}, invalidPublicationTransitionEndpointAuthority(
			persisted,
			false,
			"endpoint definition is invalid",
		)
	}
	if !proto.Equal(normalized.Definition, object.Definition) ||
		!bytes.Equal(normalized.Digest[:], object.DefinitionSHA256) ||
		normalized.ObjectType != object.ObjectType || normalized.Name != object.Name ||
		normalized.AppID != object.AppID || normalized.SharingScope != object.SharingScope {
		return publicationWinner{}, invalidPublicationTransitionEndpointAuthority(
			persisted,
			false,
			"endpoint definition authority disagrees",
		)
	}
	result := publicationWinner{
		object: knowledgesnapshot.Object{
			KnowledgeObjectID: strings.Clone(object.KnowledgeObjectID),
			Version:           object.Version,
			ObjectType:        object.ObjectType,
			Name:              strings.Clone(object.Name),
			AppID:             strings.Clone(object.AppID),
			OwnerID:           strings.Clone(object.OwnerID),
			SharingScope:      object.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSHA256:  bytes.Clone(normalized.Digest[:]),
		},
		existingDependenciesPresent: input.existingDependenciesPresent,
	}
	if input.existingDependencies != nil {
		result.existingDependencies = make(
			[]publicationPersistedDependency,
			len(input.existingDependencies),
		)
		for index, dependency := range input.existingDependencies {
			result.existingDependencies[index] = dependency
			result.existingDependencies[index].targetObjectID = strings.Clone(dependency.targetObjectID)
		}
	}
	return result, nil
}

func invalidPublicationTransitionEndpointAuthority(
	persisted bool,
	resource bool,
	reason string,
) error {
	if persisted {
		return invalidPublicationTransition("contains " + reason)
	}
	if resource {
		return fmt.Errorf("%w: publication transition %s", control.ErrCapacityExceeded, reason)
	}
	return fmt.Errorf("%w: publication transition %s", control.ErrInvalidArgument, reason)
}

func publicationTransitionPersistenceEndpointFrom(
	input publicationTransitionEndpoint,
) publicationTransitionPersistenceEndpoint {
	result := publicationTransitionPersistenceEndpoint{
		present: input.present,
		state:   input.state,
	}
	if !input.present {
		return result
	}
	object := input.winner.object
	result.objectID = strings.Clone(object.KnowledgeObjectID)
	result.version = int64(object.Version)
	result.objectType = object.ObjectType
	result.name = strings.Clone(object.Name)
	result.appID = strings.Clone(object.AppID)
	result.ownerID = strings.Clone(object.OwnerID)
	result.sharingScope = object.SharingScope
	copy(result.definitionDigest[:], object.DefinitionSHA256)
	result.existingDependenciesPresent = input.winner.existingDependenciesPresent
	if input.winner.existingDependencies != nil {
		result.existingDependencies = make(
			[]publicationPersistedDependency,
			len(input.winner.existingDependencies),
		)
		for index, dependency := range input.winner.existingDependencies {
			result.existingDependencies[index] = dependency
			result.existingDependencies[index].targetObjectID = strings.Clone(
				dependency.targetObjectID,
			)
		}
	}
	return result
}

func publicationTransitionPersistenceEndpointEqual(
	left publicationTransitionPersistenceEndpoint,
	right publicationTransitionPersistenceEndpoint,
) bool {
	return left.present == right.present &&
		left.state == right.state &&
		left.objectID == right.objectID &&
		left.version == right.version &&
		left.objectType == right.objectType &&
		left.name == right.name &&
		left.appID == right.appID &&
		left.ownerID == right.ownerID &&
		left.sharingScope == right.sharingScope &&
		left.definitionDigest == right.definitionDigest &&
		left.existingDependenciesPresent == right.existingDependenciesPresent &&
		slices.Equal(left.existingDependencies, right.existingDependencies)
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

func publicationTransitionPersistenceDependenciesEqual(
	after publicationTransitionPersistenceEndpoint,
	derived candidateDependencyAuthority,
	actual []publicationDependency,
) bool {
	if after.state == StateActive {
		if derived.state == nil || len(derived.state.projection) != len(actual) {
			return false
		}
		return slices.Equal(derived.state.projection, actual)
	}
	if len(after.existingDependencies) != len(actual) {
		return false
	}
	for index, dependency := range after.existingDependencies {
		if actual[index].targetObjectID != dependency.targetObjectID ||
			actual[index].targetVersion != dependency.targetVersion {
			return false
		}
	}
	return true
}

func canonicalizePublicationTransitionObject(
	winner publicationWinner,
	candidate bool,
) (*publicationTransitionCanonicalObject, error) {
	canonical, err := canonicalizePublicationWinner(winner, candidate)
	if err != nil {
		return nil, err
	}
	selector := canonical.semantics.selector
	stats := selector.Stats()
	selectorValueBytes := uint64(0)
	for _, dimension := range []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	} {
		for _, pattern := range selector.Patterns(dimension) {
			selectorValueBytes += uint64(len(pattern))
		}
	}
	descriptionBytes := uint64(len(canonical.object.GetDefinition().GetDescription()))
	program, constrained := selector.RuntimeProgram(knowledge.DimensionIndex)
	programKey := ""
	if program.WildcardRE2 != "" {
		programKey = publicationTransitionIndexProgramKey(program)
	}
	result := &publicationTransitionCanonicalObject{
		winner:                 publicationTransitionWinnerFromCanonical(canonical),
		canonical:              canonical,
		indexSelector:          selector,
		indexProgram:           program,
		indexProgramKey:        programKey,
		allIndexes:             !constrained,
		projectionBytes:        descriptionBytes + selectorValueBytes,
		selectorPatterns:       stats.Patterns,
		selectorValueBytes:     selectorValueBytes,
		canonicalSelectorBytes: stats.NormalizedBytes,
	}
	return result, nil
}

func publicationTransitionWinnerFromCanonical(
	canonical canonicalPublicationWinner,
) publicationWinner {
	object := canonical.object
	return publicationWinner{
		object: knowledgesnapshot.Object{
			KnowledgeObjectID: object.GetKnowledgeObjectId(),
			Version:           object.GetVersion(),
			ObjectType:        object.GetObjectType(),
			Name:              object.GetName(),
			AppID:             object.GetAppId(),
			OwnerID:           object.GetOwnerId(),
			SharingScope:      object.GetSharingScope(),
			Definition:        object.GetDefinition(),
			DefinitionSHA256:  object.GetDefinitionSha256(),
		},
		existingDependenciesPresent: canonical.existingDependenciesPresent,
		existingDependencies:        canonical.existingDependencies,
	}
}

func validatePublicationTransitionDisposition(
	currentCandidate *publicationTransitionCanonicalObject,
	activeAfter *publicationTransitionCanonicalObject,
	beforeEndpoint publicationTransitionEndpoint,
	afterEndpoint publicationTransitionEndpoint,
) (
	*publicationTransitionCanonicalObject,
	*publicationTransitionCanonicalObject,
	error,
) {
	beforeActive := beforeEndpoint.present && beforeEndpoint.state == StateActive
	afterActive := afterEndpoint.state == StateActive
	if beforeActive != (currentCandidate != nil) || afterActive != (activeAfter != nil) {
		return nil, nil, invalidPublicationTransition("candidate disposition authority is incomplete")
	}
	if beforeActive && !publicationTransitionWinnerAuthorityEqual(
		beforeEndpoint.winner,
		currentCandidate.winner,
	) {
		return nil, nil, invalidPublicationTransition(
			"pre-ACTIVE endpoint disagrees with the exact ACTIVE inventory",
		)
	}
	if !beforeEndpoint.present {
		return nil, activeAfter, nil
	}

	beforeObject := beforeEndpoint.winner.object
	afterObject := afterEndpoint.winner.object
	if beforeObject.KnowledgeObjectID != afterObject.KnowledgeObjectID ||
		beforeObject.OwnerID != afterObject.OwnerID ||
		beforeObject.ObjectType != afterObject.ObjectType ||
		beforeObject.Version >= math.MaxInt64 ||
		afterObject.Version != beforeObject.Version+1 {
		return nil, nil, fmt.Errorf(
			"%w: publication candidate replacement identity or version is invalid",
			control.ErrInvalidArgument,
		)
	}
	stateOnly := !beforeActive || !afterActive
	if stateOnly && !publicationTransitionWinnerDefinitionEqual(
		beforeEndpoint.winner,
		afterEndpoint.winner,
	) {
		return nil, nil, fmt.Errorf(
			"%w: state-only publication transition changes definition authority",
			control.ErrInvalidArgument,
		)
	}
	if !afterActive && (!beforeEndpoint.winner.existingDependenciesPresent ||
		!afterEndpoint.winner.existingDependenciesPresent ||
		!slices.Equal(
			beforeEndpoint.winner.existingDependencies,
			afterEndpoint.winner.existingDependencies,
		)) {
		return nil, nil, fmt.Errorf(
			"%w: removal candidate changes immutable or retained dependency authority",
			control.ErrInvalidArgument,
		)
	}
	before := currentCandidate
	if !beforeActive {
		before = publicationTransitionLinkedCanonicalObject(beforeEndpoint.winner, activeAfter)
	}
	after := activeAfter
	if !afterActive {
		after = publicationTransitionLinkedCanonicalObject(afterEndpoint.winner, currentCandidate)
	}
	if before == nil || after == nil {
		return nil, nil, invalidPublicationTransition("candidate endpoint linkage is incomplete")
	}
	return before, after, nil
}

func publicationTransitionWinnerAuthorityEqual(left, right publicationWinner) bool {
	return left.existingDependenciesPresent == right.existingDependenciesPresent &&
		slices.Equal(left.existingDependencies, right.existingDependencies) &&
		left.object.Version == right.object.Version &&
		publicationTransitionWinnerDefinitionEqual(left, right)
}

func publicationTransitionWinnerDefinitionEqual(left, right publicationWinner) bool {
	return left.object.KnowledgeObjectID == right.object.KnowledgeObjectID &&
		left.object.ObjectType == right.object.ObjectType &&
		left.object.Name == right.object.Name &&
		left.object.AppID == right.object.AppID &&
		left.object.OwnerID == right.object.OwnerID &&
		left.object.SharingScope == right.object.SharingScope &&
		bytes.Equal(left.object.DefinitionSHA256, right.object.DefinitionSHA256) &&
		proto.Equal(left.object.Definition, right.object.Definition)
}

func publicationTransitionLinkedCanonicalObject(
	winner publicationWinner,
	semantic *publicationTransitionCanonicalObject,
) *publicationTransitionCanonicalObject {
	if semantic == nil || semantic.canonical.object == nil {
		return nil
	}
	result := *semantic
	result.canonical = semantic.canonical
	semanticObject := semantic.canonical.object
	result.canonical.object = &opensplunkv1.KnowledgeSnapshotObject{
		ResolutionOrdinal: semanticObject.GetResolutionOrdinal(),
		Stage:             semanticObject.GetStage(),
		StageOrdinal:      semanticObject.GetStageOrdinal(),
		KnowledgeObjectId: strings.Clone(winner.object.KnowledgeObjectID),
		Version:           winner.object.Version,
		ObjectType:        semanticObject.GetObjectType(),
		Name:              semanticObject.GetName(),
		AppId:             semanticObject.GetAppId(),
		OwnerId:           semanticObject.GetOwnerId(),
		SharingScope:      semanticObject.GetSharingScope(),
		Definition:        semanticObject.GetDefinition(),
		DefinitionSha256:  semanticObject.GetDefinitionSha256(),
	}
	result.canonical.key = dependencyVersionKey{
		objectID: strings.Clone(winner.object.KnowledgeObjectID),
		version:  int64(winner.object.Version),
	}
	result.canonical.existingDependenciesPresent = winner.existingDependenciesPresent
	result.canonical.existingDependencies = winner.existingDependencies
	result.winner = publicationTransitionWinnerFromCanonical(result.canonical)
	return &result
}

func validatePublicationTransitionPostAggregate(
	input publicationActiveTransitionInventory,
	beforeActive bool,
	before *publicationTransitionCanonicalObject,
	afterActive bool,
	after *publicationTransitionCanonicalObject,
) (uint64, error) {
	charge := publicationTransitionAggregateCharge{
		definitionBytes:        input.expectedDefinitionBytes,
		projectionBytes:        input.expectedProjectionBytes,
		selectorPatterns:       input.expectedSelectorPatterns,
		selectorValueBytes:     input.expectedSelectorValueBytes,
		canonicalSelectorBytes: input.expectedCanonicalSelectorBytes,
		selectorWork:           input.expectedSelectorWork,
		dependencies:           input.expectedDependencyCount,
	}
	if beforeActive {
		if !publicationTransitionSubtractAggregateCharge(
			&charge,
			publicationTransitionObjectAggregateCharge(before),
		) {
			return 0, invalidPublicationTransition(
				"current aggregate authority is smaller than the pre-ACTIVE candidate",
			)
		}
	}
	if afterActive {
		if !publicationTransitionAddAggregateCharge(
			&charge,
			publicationTransitionObjectAggregateCharge(after),
		) {
			return 0, fmt.Errorf(
				"%w: publication transition post-ACTIVE aggregate exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
	}
	return charge.dependencies, nil
}

func publicationTransitionObjectAggregateCharge(
	object *publicationTransitionCanonicalObject,
) publicationTransitionAggregateCharge {
	if object == nil {
		return publicationTransitionAggregateCharge{}
	}
	return publicationTransitionAggregateCharge{
		definitionBytes:        object.canonical.definitionBytes,
		projectionBytes:        object.projectionBytes,
		selectorPatterns:       object.selectorPatterns,
		selectorValueBytes:     object.selectorValueBytes,
		canonicalSelectorBytes: object.canonicalSelectorBytes,
		selectorWork:           object.canonical.selectorWork,
		dependencies:           uint64(len(object.winner.existingDependencies)),
	}
}

func publicationTransitionSubtractAggregateCharge(
	total *publicationTransitionAggregateCharge,
	value publicationTransitionAggregateCharge,
) bool {
	if total == nil || total.definitionBytes < value.definitionBytes ||
		total.projectionBytes < value.projectionBytes ||
		total.selectorPatterns < value.selectorPatterns ||
		total.selectorValueBytes < value.selectorValueBytes ||
		total.canonicalSelectorBytes < value.canonicalSelectorBytes ||
		total.selectorWork < value.selectorWork ||
		total.dependencies < value.dependencies {
		return false
	}
	total.definitionBytes -= value.definitionBytes
	total.projectionBytes -= value.projectionBytes
	total.selectorPatterns -= value.selectorPatterns
	total.selectorValueBytes -= value.selectorValueBytes
	total.canonicalSelectorBytes -= value.canonicalSelectorBytes
	total.selectorWork -= value.selectorWork
	total.dependencies -= value.dependencies
	return true
}

func publicationTransitionAddAggregateCharge(
	total *publicationTransitionAggregateCharge,
	value publicationTransitionAggregateCharge,
) bool {
	return total != nil &&
		addPublicationResource(&total.definitionBytes, value.definitionBytes, maximumPublicationTransitionDefinitionBytes) &&
		addPublicationResource(&total.projectionBytes, value.projectionBytes, maximumPublicationTransitionProjectionBytes) &&
		addPublicationResource(&total.selectorPatterns, value.selectorPatterns, maximumPublicationTransitionSelectorPatterns) &&
		addPublicationResource(&total.selectorValueBytes, value.selectorValueBytes, maximumPublicationTransitionSelectorBytes) &&
		addPublicationResource(&total.canonicalSelectorBytes, value.canonicalSelectorBytes, maximumPublicationTransitionSelectorBytes) &&
		addPublicationResource(&total.selectorWork, value.selectorWork, maximumPublicationTransitionSelectorWork) &&
		addPublicationResource(&total.dependencies, value.dependencies, maximumPublicationTransitionDependencies)
}

func publicationTransitionPrincipalClasses(
	ctx context.Context,
	activeAppIDs []string,
	preSlots, postSlots []*publicationTransitionCanonicalObject,
) ([]publicationTransitionPrincipalClass, error) {
	apps := make(map[string]struct{}, len(activeAppIDs))
	for _, appID := range activeAppIDs {
		if !validIdentity(appID, maximumAppIDBytes) {
			return nil, invalidPublicationTransition("contains an invalid active-app identity")
		}
		if _, duplicate := apps[appID]; duplicate {
			return nil, invalidPublicationTransition("duplicates an active-app identity")
		}
		apps[appID] = struct{}{}
	}
	representedApps := make(map[string]struct{})
	privatePrincipals := make(map[publicationTransitionPrivatePrincipalKey]struct{})
	visit := func(object *publicationTransitionCanonicalObject) error {
		if object == nil {
			return nil
		}
		appID := object.canonical.object.GetAppId()
		if _, active := apps[appID]; !active {
			return invalidPublicationTransition(
				"ACTIVE object defining app is absent from the active-app inventory",
			)
		}
		if object.canonical.object.GetSharingScope() !=
			opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL {
			representedApps[appID] = struct{}{}
		}
		if object.canonical.object.GetSharingScope() ==
			opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE {
			privatePrincipals[publicationTransitionPrivatePrincipalKey{
				appID:   strings.Clone(appID),
				ownerID: strings.Clone(object.canonical.object.GetOwnerId()),
			}] = struct{}{}
			if len(privatePrincipals) > MaximumResolutionCandidates {
				return fmt.Errorf(
					"%w: publication transition exceeds its private-principal visibility limit",
					control.ErrCapacityExceeded,
				)
			}
		}
		return nil
	}
	for index := range preSlots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := visit(preSlots[index]); err != nil {
			return nil, err
		}
		if err := visit(postSlots[index]); err != nil {
			return nil, err
		}
	}

	privateKeys := make([]publicationTransitionPrivatePrincipalKey, 0, len(privatePrincipals))
	for key := range privatePrincipals {
		privateKeys = append(privateKeys, key)
	}
	slices.SortFunc(privateKeys, func(left, right publicationTransitionPrivatePrincipalKey) int {
		if left.appID != right.appID {
			return strings.Compare(left.appID, right.appID)
		}
		return strings.Compare(left.ownerID, right.ownerID)
	})
	appIDs := make([]string, 0, len(representedApps))
	for appID := range representedApps {
		appIDs = append(appIDs, strings.Clone(appID))
	}
	slices.Sort(appIDs)
	classes := make(
		[]publicationTransitionPrincipalClass,
		0,
		1+len(appIDs)+len(privateKeys),
	)
	classes = append(classes, publicationTransitionPrincipalClass{kind: publicationTransitionGenericApp})
	for _, appID := range appIDs {
		classes = append(classes, publicationTransitionPrincipalClass{
			kind:  publicationTransitionGenericPrincipal,
			appID: strings.Clone(appID),
		})
	}
	for _, key := range privateKeys {
		classes = append(classes, publicationTransitionPrincipalClass{
			kind:    publicationTransitionPrivatePrincipal,
			appID:   strings.Clone(key.appID),
			ownerID: strings.Clone(key.ownerID),
		})
	}
	return classes, nil
}

func validatePublicationTransitionClassHydration(
	ctx context.Context,
	classes []publicationTransitionPrincipalClass,
	preSlots, postSlots []*publicationTransitionCanonicalObject,
	candidateAfter *publicationTransitionCanonicalObject,
) ([]publicationTransitionClassHydration, error) {
	type hydrationInventory struct {
		global  publicationTransitionHydrationCharge
		apps    map[string]publicationTransitionHydrationCharge
		private map[publicationTransitionPrivatePrincipalKey]publicationTransitionHydrationCharge
	}
	aggregate := func(objects []*publicationTransitionCanonicalObject) (hydrationInventory, error) {
		result := hydrationInventory{
			apps:    make(map[string]publicationTransitionHydrationCharge),
			private: make(map[publicationTransitionPrivatePrincipalKey]publicationTransitionHydrationCharge),
		}
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				return hydrationInventory{}, err
			}
			if object == nil {
				continue
			}
			charge := publicationTransitionObjectHydrationCharge(object)
			switch object.canonical.object.GetSharingScope() {
			case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
				result.global = publicationTransitionAddHydrationCharge(result.global, charge)
			case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
				appID := object.canonical.object.GetAppId()
				result.apps[appID] = publicationTransitionAddHydrationCharge(result.apps[appID], charge)
			case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
				key := publicationTransitionPrivatePrincipalKey{
					appID:   object.canonical.object.GetAppId(),
					ownerID: object.canonical.object.GetOwnerId(),
				}
				result.private[key] = publicationTransitionAddHydrationCharge(result.private[key], charge)
			default:
				return hydrationInventory{}, invalidPublicationTransition(
					"contains an invalid ACTIVE sharing scope",
				)
			}
		}
		return result, nil
	}
	pre, err := aggregate(preSlots)
	if err != nil {
		return nil, err
	}
	post, err := aggregate(postSlots)
	if err != nil {
		return nil, err
	}
	chargeForClass := func(
		inventory hydrationInventory,
		class publicationTransitionPrincipalClass,
	) publicationTransitionHydrationCharge {
		result := inventory.global
		if class.kind != publicationTransitionGenericApp {
			result = publicationTransitionAddHydrationCharge(result, inventory.apps[class.appID])
		}
		if class.kind == publicationTransitionPrivatePrincipal {
			result = publicationTransitionAddHydrationCharge(
				result,
				inventory.private[publicationTransitionPrivatePrincipalKey{
					appID: class.appID, ownerID: class.ownerID,
				}],
			)
		}
		return result
	}
	result := make([]publicationTransitionClassHydration, len(classes))
	for index, class := range classes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		preCharge := chargeForClass(pre, class)
		postCharge := chargeForClass(post, class)
		if !publicationTransitionHydrationWithinResolutionBudget(postCharge) {
			return nil, fmt.Errorf(
				"%w: publication transition visibility class exceeds hydration limits",
				control.ErrCapacityExceeded,
			)
		}
		_, candidateVisible := publicationTransitionVisibilityRank(class, candidateAfter.canonical.object)
		result[index] = publicationTransitionClassHydration{
			preCharge:        preCharge,
			postCharge:       postCharge,
			postDependencies: postCharge.dependencies,
			candidateVisible: candidateVisible,
		}
	}
	return result, nil
}

func publicationTransitionObjectHydrationCharge(
	object *publicationTransitionCanonicalObject,
) publicationTransitionHydrationCharge {
	if object == nil {
		return publicationTransitionHydrationCharge{}
	}
	return publicationTransitionHydrationCharge{
		definitionBytes:    object.canonical.definitionBytes,
		projectionBytes:    object.projectionBytes,
		selectorPatterns:   object.selectorPatterns,
		selectorValueBytes: object.selectorValueBytes,
		dependencies:       uint64(len(object.winner.existingDependencies)),
	}
}

func publicationTransitionAddHydrationCharge(
	left, right publicationTransitionHydrationCharge,
) publicationTransitionHydrationCharge {
	return publicationTransitionHydrationCharge{
		definitionBytes:    left.definitionBytes + right.definitionBytes,
		projectionBytes:    left.projectionBytes + right.projectionBytes,
		selectorPatterns:   left.selectorPatterns + right.selectorPatterns,
		selectorValueBytes: left.selectorValueBytes + right.selectorValueBytes,
		dependencies:       left.dependencies + right.dependencies,
	}
}

func publicationTransitionHydrationWithinResolutionBudget(
	charge publicationTransitionHydrationCharge,
) bool {
	return charge.definitionBytes <= uint64(resolutionHydrationBudget.definitionBytes) &&
		charge.projectionBytes <= uint64(resolutionHydrationBudget.projectionBytes) &&
		charge.selectorPatterns <= uint64(resolutionHydrationBudget.selectorPatterns) &&
		charge.selectorValueBytes <= uint64(resolutionHydrationBudget.selectorValueBytes) &&
		charge.dependencies <= uint64(resolutionHydrationBudget.dependencies)
}

func publicationTransitionIndexAtoms(
	ctx context.Context,
	indexNames []string,
	preSlots, postSlots []*publicationTransitionCanonicalObject,
) ([]publicationIndexAtom, error) {
	names := slices.Clone(indexNames)
	for index, name := range names {
		canonical, err := control.NormalizeIndexName(name)
		if err != nil || canonical != name {
			return nil, invalidPublicationTransition("contains a noncanonical physical index name")
		}
		names[index] = canonical
	}
	slices.Sort(names)
	for index := 1; index < len(names); index++ {
		if names[index-1] == names[index] {
			return nil, invalidPublicationTransition("duplicates a physical index identity")
		}
	}
	atoms := make([]publicationIndexAtom, len(names))
	nameOrdinal := make(map[string]int, len(names))
	for index, name := range names {
		nameOrdinal[name] = index
	}
	var universalBefore publicationIndexMembership
	var universalAfter publicationIndexMembership
	type wildcardResult struct {
		indexes []int
		before  publicationIndexMembership
		after   publicationIndexMembership
	}
	wildcardCache := make(map[string]*wildcardResult)
	var wildcardProbes uint64
	var wildcardWork uint64
	apply := func(
		object *publicationTransitionCanonicalObject,
		ordinal int,
		before bool,
	) error {
		if object == nil {
			return nil
		}
		if ordinal < 0 || ordinal >= MaximumResolutionCandidates {
			return fmt.Errorf(
				"%w: publication transition membership universe is exhausted",
				control.ErrCapacityExceeded,
			)
		}
		if object.allIndexes {
			if before {
				publicationTransitionSetMembership(&universalBefore, ordinal)
			} else {
				publicationTransitionSetMembership(&universalAfter, ordinal)
			}
			return nil
		}
		if object.indexSelector == nil {
			return invalidPublicationTransition("contains incomplete index-selector authority")
		}
		matchedIndexes := make([]int, 0, len(object.indexProgram.ExactLiterals))
		if object.indexProgram.WildcardRE2 == "" {
			for _, literal := range object.indexProgram.ExactLiterals {
				if index, found := nameOrdinal[literal]; found {
					matchedIndexes = append(matchedIndexes, index)
				}
			}
		} else {
			key := object.indexProgramKey
			if key == "" {
				return invalidPublicationTransition("contains incomplete wildcard index-program authority")
			}
			cached, found := wildcardCache[key]
			if found {
				matchedIndexes = cached.indexes
			} else {
				if uint64(len(names)) > maximumPublicationTransitionIndexSelectorProbes-wildcardProbes {
					return fmt.Errorf(
						"%w: publication transition exceeds its wildcard index probe limit",
						control.ErrCapacityExceeded,
					)
				}
				indexOnly, err := knowledge.CompileSelector(knowledge.SelectorSpec{Dimensions: []knowledge.DimensionSpec{{
					Dimension: knowledge.DimensionIndex,
					Patterns:  object.indexSelector.Patterns(knowledge.DimensionIndex),
				}}})
				if err != nil {
					return invalidPublicationTransition("contains invalid index-only selector authority")
				}
				matchedIndexes = make([]int, 0, len(names))
				for index, name := range names {
					if err := ctx.Err(); err != nil {
						return err
					}
					charge, assessmentErr := object.indexProgram.Assessment.UpperBound(
						uint64(len(name)),
					)
					if assessmentErr != nil ||
						charge > maximumPublicationTransitionIndexMatcherWork-wildcardWork {
						return fmt.Errorf(
							"%w: publication transition exceeds its wildcard matcher work limit",
							control.ErrCapacityExceeded,
						)
					}
					wildcardWork += charge
					wildcardProbes++
					matched, _, matchErr := indexOnly.Match(ctx, knowledge.EventMetadata{
						Index: knowledge.StringMetadata(name),
					}, knowledge.DefaultRuntimeBudget())
					if matchErr != nil {
						if ctxErr := ctx.Err(); ctxErr != nil {
							return ctxErr
						}
						return invalidPublicationTransition("index-selector matching failed")
					}
					if matched {
						matchedIndexes = append(matchedIndexes, index)
					}
				}
				cached = &wildcardResult{indexes: slices.Clone(matchedIndexes)}
				wildcardCache[key] = cached
			}
			if before {
				publicationTransitionSetMembership(&cached.before, ordinal)
			} else {
				publicationTransitionSetMembership(&cached.after, ordinal)
			}
			return nil
		}
		for _, index := range matchedIndexes {
			if before {
				publicationTransitionSetMembership(&atoms[index].before, ordinal)
			} else {
				publicationTransitionSetMembership(&atoms[index].after, ordinal)
			}
		}
		return nil
	}
	for ordinal := range preSlots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := apply(preSlots[ordinal], ordinal, true); err != nil {
			return nil, err
		}
		if err := apply(postSlots[ordinal], ordinal, false); err != nil {
			return nil, err
		}
	}
	for _, cached := range wildcardCache {
		for _, index := range cached.indexes {
			for word := range atoms[index].before {
				atoms[index].before[word] |= cached.before[word]
				atoms[index].after[word] |= cached.after[word]
			}
		}
	}
	for index := range atoms {
		for word := range atoms[index].before {
			atoms[index].before[word] |= universalBefore[word]
			atoms[index].after[word] |= universalAfter[word]
		}
	}
	return atoms, nil
}

func publicationTransitionIndexProgramKey(program knowledge.DimensionRuntimeProgram) string {
	var builder strings.Builder
	publicationTransitionWriteKeyString(&builder, program.WildcardRE2)
	publicationTransitionWriteKeyUint64(&builder, uint64(len(program.ExactLiterals)))
	for _, literal := range program.ExactLiterals {
		publicationTransitionWriteKeyString(&builder, literal)
	}
	return builder.String()
}

func publicationTransitionWriteKeyString(builder *strings.Builder, value string) {
	publicationTransitionWriteKeyUint64(builder, uint64(len(value)))
	_, _ = builder.WriteString(value)
}

func publicationTransitionWriteKeyUint64(builder *strings.Builder, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = builder.Write(encoded[:])
}

func publicationTransitionSetMembership(
	membership *publicationIndexMembership,
	ordinal int,
) {
	membership[ordinal/64] |= uint64(1) << (ordinal % 64)
}

func selectPublicationTransitionWinners(
	ctx context.Context,
	class publicationTransitionPrincipalClass,
	membership *publicationIndexMembership,
	objects []*publicationTransitionCanonicalObject,
	proposedCandidate *publicationTransitionCanonicalObject,
	exposedByTransition bool,
	work *publicationTransitionWork,
) ([]*publicationTransitionCanonicalObject, error) {
	type precedenceGroup struct {
		winner             *publicationTransitionCanonicalObject
		rank               uint8
		highestCount       uint32
		candidateAtHighest bool
	}
	type nameKey struct {
		objectType opensplunkv1.KnowledgeObjectType
		name       string
	}
	groups := make(map[nameKey]precedenceGroup)
	eligible := uint64(0)
	if membership == nil {
		return nil, invalidPublicationTransition("contains absent index membership authority")
	}
	for wordIndex, word := range *membership {
		for word != 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			bit := bits.TrailingZeros64(word)
			word &^= uint64(1) << bit
			ordinal := wordIndex*64 + bit
			if ordinal >= len(objects) || objects[ordinal] == nil {
				return nil, invalidPublicationTransition("contains an invalid index membership ordinal")
			}
			if work == nil || work.membershipVisits >= maximumPublicationTransitionMembershipVisits {
				return nil, fmt.Errorf(
					"%w: publication transition exceeds its candidate-membership work limit",
					control.ErrCapacityExceeded,
				)
			}
			work.membershipVisits++
			object := objects[ordinal]
			rank, visible := publicationTransitionVisibilityRank(class, object.canonical.object)
			if !visible {
				continue
			}
			eligible++
			key := nameKey{
				objectType: object.canonical.object.GetObjectType(),
				name:       object.canonical.object.GetName(),
			}
			group, exists := groups[key]
			switch {
			case !exists || rank > group.rank:
				group.winner = object
				group.rank = rank
				group.highestCount = 1
				group.candidateAtHighest = object == proposedCandidate
			case rank == group.rank:
				group.highestCount++
				group.candidateAtHighest = group.candidateAtHighest || object == proposedCandidate
			}
			groups[key] = group
		}
	}
	if len(groups) > knowledgesnapshot.MaximumExecutableObjects ||
		eligible-uint64(len(groups)) > knowledgesnapshot.MaximumShadows {
		return nil, fmt.Errorf(
			"%w: publication transition resolved cohort exceeds winner or shadow limits",
			control.ErrCapacityExceeded,
		)
	}
	winners := make([]*publicationTransitionCanonicalObject, 0, len(groups))
	for _, group := range groups {
		if group.highestCount != 1 {
			if group.candidateAtHighest {
				return nil, fmt.Errorf(
					"%w: publication candidate creates a non-unique highest-rank precedence slot",
					control.ErrAlreadyExists,
				)
			}
			if exposedByTransition {
				return nil, fmt.Errorf(
					"%w: publication transition exposes a non-unique highest-rank precedence slot",
					control.ErrDependencyConflict,
				)
			}
			return nil, invalidPublicationTransition(
				"contains a non-unique highest-rank precedence slot",
			)
		}
		winners = append(winners, group.winner)
	}
	sort.Slice(winners, func(left, right int) bool {
		if winners[left].canonical.stageRank != winners[right].canonical.stageRank {
			return winners[left].canonical.stageRank < winners[right].canonical.stageRank
		}
		if winners[left].canonical.object.GetName() != winners[right].canonical.object.GetName() {
			return winners[left].canonical.object.GetName() < winners[right].canonical.object.GetName()
		}
		return winners[left].canonical.object.GetKnowledgeObjectId() <
			winners[right].canonical.object.GetKnowledgeObjectId()
	})
	return winners, nil
}

func publicationTransitionVisibilityRank(
	class publicationTransitionPrincipalClass,
	object *opensplunkv1.KnowledgeSnapshotObject,
) (uint8, bool) {
	if object == nil {
		return 0, false
	}
	switch object.GetSharingScope() {
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return 1, true
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		if class.kind != publicationTransitionGenericApp && class.appID == object.GetAppId() {
			return 2, true
		}
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		if class.kind == publicationTransitionPrivatePrincipal &&
			class.appID == object.GetAppId() && class.ownerID == object.GetOwnerId() {
			return 3, true
		}
	}
	return 0, false
}

func publicationTransitionWinnersContain(
	winners []*publicationTransitionCanonicalObject,
	candidate publicationCandidateAuthority,
) bool {
	for _, winner := range winners {
		if publicationWinnerMatchesCandidate(winner.winner.object, candidate) {
			return true
		}
	}
	return false
}

func publicationTransitionWinnerKeyFromObject(
	winner *publicationTransitionCanonicalObject,
) publicationTransitionWinnerKey {
	object := winner.canonical.object
	var result publicationTransitionWinnerKey
	copy(result.definitionDigest[:], object.GetDefinitionSha256())
	// These strings belong to the already-detached immutable invocation state.
	// Borrowing them avoids multiplying wide identity allocations per
	// class/signature state.
	result.objectID = object.GetKnowledgeObjectId()
	result.version = int64(object.GetVersion())
	result.ownerID = object.GetOwnerId()
	result.appID = object.GetAppId()
	result.sharingScope = object.GetSharingScope()
	result.objectType = object.GetObjectType()
	result.name = object.GetName()
	return result
}

func (work *publicationTransitionWork) chargeChangedCohort(
	winners []*publicationTransitionCanonicalObject,
) error {
	if work == nil || work.changedCohorts >= maximumPublicationTransitionChangedCohorts {
		return fmt.Errorf(
			"%w: publication transition exceeds its changed-cohort limit",
			control.ErrCapacityExceeded,
		)
	}
	work.changedCohorts++
	if !addPublicationResource(
		&work.winnerRevisits,
		uint64(len(winners)),
		maximumPublicationTransitionWinnerRevisits,
	) {
		return fmt.Errorf(
			"%w: publication transition exceeds its winner-revisit limit",
			control.ErrCapacityExceeded,
		)
	}
	pairs := uint64(len(winners)) * uint64(len(winners)-1) / 2
	if !addPublicationResource(
		&work.winnerPairComparisons,
		pairs,
		maximumPublicationTransitionWinnerPairComparisons,
	) {
		return fmt.Errorf(
			"%w: publication transition exceeds its winner-comparison limit",
			control.ErrCapacityExceeded,
		)
	}
	for _, winner := range winners {
		if err := work.chargeSemanticCharges(winner.canonical.semantics.charges); err != nil {
			return err
		}
		if !addPublicationResource(
			&work.definitionRevisits,
			winner.canonical.definitionBytes,
			maximumPublicationTransitionDefinitionBytes,
		) || !addPublicationResource(
			&work.dependencyRevisits,
			uint64(len(winner.winner.existingDependencies)),
			maximumPublicationTransitionDependencyRevisits,
		) {
			return fmt.Errorf(
				"%w: publication transition exceeds definition or dependency revisit limits",
				control.ErrCapacityExceeded,
			)
		}
		selectorWork, withinLimit := publicationTransitionProductWithin(
			winner.canonical.selectorWork,
			publicationTransitionCohortSelectorNormalizationPasses,
			maximumPublicationTransitionSelectorWorkRevisits,
		)
		if !withinLimit || !addPublicationResource(
			&work.selectorWorkRevisits,
			selectorWork,
			maximumPublicationTransitionSelectorWorkRevisits,
		) {
			return fmt.Errorf(
				"%w: publication transition exceeds its repeated selector-work limit",
				control.ErrCapacityExceeded,
			)
		}
	}
	return nil
}

func (work *publicationTransitionWork) chargeSemanticCharges(
	charges ResolutionStaticCharges,
) error {
	if work == nil ||
		!addPublicationResource(&work.generatedFields, uint64(charges.GeneratedFields), maximumPublicationTransitionGeneratedFields) ||
		!addPublicationResource(&work.regexPrograms, uint64(charges.ExtractionRegexPrograms), maximumPublicationTransitionRegexPrograms) ||
		!addPublicationResource(&work.regexWorkUnits, charges.ExtractionRegexWorkUnits, maximumPublicationTransitionRegexWorkUnits) ||
		!addPublicationResource(&work.extractionOutputs, uint64(charges.ExtractionOutputs), maximumPublicationTransitionExtractionOutputs) ||
		!addPublicationResource(&work.jsonEvaluationWorkUnits, uint64(charges.JSONEvaluationWorkUnits), maximumPublicationTransitionJSONEvaluationWorkUnits) ||
		!addPublicationResource(&work.scalarExpressions, uint64(charges.ScalarExpressions), maximumPublicationTransitionScalarExpressions) ||
		!addPublicationResource(&work.scalarExpressionNodes, uint64(charges.ScalarExpressionNodes), maximumPublicationTransitionScalarExpressionNodes) ||
		!addPublicationResource(&work.scalarPredicates, uint64(charges.ScalarPredicates), maximumPublicationTransitionScalarPredicates) {
		return fmt.Errorf(
			"%w: publication transition exceeds its repeated semantic-work limit",
			control.ErrCapacityExceeded,
		)
	}
	return nil
}

func (work *publicationTransitionWork) chargeDerivedDependencies(count int) error {
	if count < 0 || work == nil || !addPublicationResource(
		&work.dependencyRevisits,
		uint64(count),
		maximumPublicationTransitionDependencyRevisits,
	) {
		return fmt.Errorf(
			"%w: publication transition exceeds its derived-dependency revisit limit",
			control.ErrCapacityExceeded,
		)
	}
	return nil
}

func publicationTransitionProductWithin(left, right, maximum uint64) (uint64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if left > maximum/right {
		return 0, false
	}
	return left * right, true
}

func publicationCandidateAuthorityFromCanonical(
	object canonicalPublicationWinner,
) publicationCandidateAuthority {
	var digest [sha256.Size]byte
	copy(digest[:], object.object.GetDefinitionSha256())
	return publicationCandidateAuthority{
		objectID:         strings.Clone(object.object.GetKnowledgeObjectId()),
		version:          int64(object.object.GetVersion()),
		definitionDigest: digest,
		ownerID:          strings.Clone(object.object.GetOwnerId()),
	}
}

func publicationTransitionCohortCommitment(
	winners []*publicationTransitionCanonicalObject,
	candidateWins bool,
	cache map[*publicationTransitionCanonicalObject][sha256.Size]byte,
) [sha256.Size]byte {
	digest := sha256.New()
	publicationTransitionHashString(
		digest,
		"open-splunk/publication-transition-cohort/v1",
	)
	publicationTransitionHashBool(digest, candidateWins)
	publicationTransitionHashWinnerPointers(digest, winners, cache)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func publicationTransitionInventoryCommitment(
	input publicationActiveTransitionInventory,
	current []*publicationTransitionCanonicalObject,
	post []*publicationTransitionCanonicalObject,
	candidateBefore *publicationTransitionCanonicalObject,
	candidateAfter *publicationTransitionCanonicalObject,
) [sha256.Size]byte {
	ordered := slices.Clone(current)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].canonical.key.objectID < ordered[right].canonical.key.objectID
	})
	orderedPost := make([]*publicationTransitionCanonicalObject, 0, len(post))
	for _, object := range post {
		if object != nil {
			orderedPost = append(orderedPost, object)
		}
	}
	sort.Slice(orderedPost, func(left, right int) bool {
		return orderedPost[left].canonical.key.objectID < orderedPost[right].canonical.key.objectID
	})
	apps := slices.Clone(input.activeAppIDs)
	slices.Sort(apps)
	names := slices.Clone(input.potentiallySearchableIndexNames)
	slices.Sort(names)
	digest := sha256.New()
	publicationTransitionHashString(digest, "open-splunk/publication-active-transition/v1")
	publicationTransitionHashString(digest, input.tenantID)
	publicationTransitionHashUint64(digest, input.expectedDefinitionBytes)
	publicationTransitionHashUint64(digest, input.expectedProjectionBytes)
	publicationTransitionHashUint64(digest, input.expectedSelectorPatterns)
	publicationTransitionHashUint64(digest, input.expectedSelectorValueBytes)
	publicationTransitionHashUint64(digest, input.expectedCanonicalSelectorBytes)
	publicationTransitionHashUint64(digest, input.expectedSelectorWork)
	publicationTransitionHashUint64(digest, input.expectedDependencyCount)
	publicationTransitionHashUint64(digest, uint64(len(ordered)))
	for _, object := range ordered {
		publicationTransitionHashObject(digest, object)
	}
	publicationTransitionHashUint64(digest, uint64(len(orderedPost)))
	for _, object := range orderedPost {
		publicationTransitionHashObject(digest, object)
	}
	publicationTransitionHashEndpoint(digest, input.candidateBefore, candidateBefore)
	publicationTransitionHashEndpoint(digest, input.candidateAfter, candidateAfter)
	publicationTransitionHashUint64(digest, uint64(len(apps)))
	for _, appID := range apps {
		publicationTransitionHashString(digest, appID)
	}
	publicationTransitionHashUint64(digest, uint64(len(names)))
	for _, name := range names {
		publicationTransitionHashString(digest, name)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func publicationTransitionHashEndpoint(
	digest hash.Hash,
	endpoint publicationTransitionEndpoint,
	object *publicationTransitionCanonicalObject,
) {
	publicationTransitionHashBool(digest, endpoint.present)
	publicationTransitionHashString(digest, string(endpoint.state))
	publicationTransitionHashObject(digest, object)
}

func publicationTransitionHashObject(
	digest hash.Hash,
	object *publicationTransitionCanonicalObject,
) {
	if object == nil {
		publicationTransitionHashBool(digest, false)
		return
	}
	publicationTransitionHashBool(digest, true)
	key := publicationTransitionWinnerKeyFromObject(object)
	publicationTransitionHashWinnerKey(digest, key)
	publicationTransitionHashBool(digest, object.winner.existingDependenciesPresent)
	publicationTransitionHashUint64(digest, uint64(len(object.winner.existingDependencies)))
	for _, dependency := range object.winner.existingDependencies {
		publicationTransitionHashUint64(digest, uint64(dependency.ordinal))
		publicationTransitionHashString(digest, dependency.targetObjectID)
		publicationTransitionHashUint64(digest, uint64(dependency.targetVersion))
		publicationTransitionHashUint64(digest, uint64(dependency.role))
	}
}

func publicationTransitionHashWinnerKey(digest hash.Hash, key publicationTransitionWinnerKey) {
	publicationTransitionHashString(digest, key.objectID)
	publicationTransitionHashUint64(digest, uint64(key.version))
	_, _ = digest.Write(key.definitionDigest[:])
	publicationTransitionHashString(digest, key.ownerID)
	publicationTransitionHashString(digest, key.appID)
	publicationTransitionHashUint64(digest, uint64(key.sharingScope))
	publicationTransitionHashUint64(digest, uint64(key.objectType))
	publicationTransitionHashString(digest, key.name)
}

func publicationTransitionHashWinnerPointers(
	digest hash.Hash,
	winners []*publicationTransitionCanonicalObject,
	cache map[*publicationTransitionCanonicalObject][sha256.Size]byte,
) {
	publicationTransitionHashUint64(digest, uint64(len(winners)))
	for _, winner := range winners {
		commitment := publicationTransitionCachedWinnerKeyCommitment(cache, winner)
		_, _ = digest.Write(commitment[:])
	}
}

func publicationTransitionCachedWinnerKeyCommitment(
	cache map[*publicationTransitionCanonicalObject][sha256.Size]byte,
	winner *publicationTransitionCanonicalObject,
) [sha256.Size]byte {
	if commitment, exists := cache[winner]; exists {
		return commitment
	}
	digest := sha256.New()
	publicationTransitionHashString(
		digest,
		"open-splunk/publication-transition-winner-key/v1",
	)
	publicationTransitionHashWinnerKey(
		digest,
		publicationTransitionWinnerKeyFromObject(winner),
	)
	var commitment [sha256.Size]byte
	copy(commitment[:], digest.Sum(nil))
	cache[winner] = commitment
	return commitment
}

func publicationTransitionHashPrincipalClass(
	digest hash.Hash,
	class publicationTransitionPrincipalClass,
) {
	publicationTransitionHashUint64(digest, uint64(class.kind))
	publicationTransitionHashString(digest, class.appID)
	publicationTransitionHashString(digest, class.ownerID)
}

func publicationTransitionHashHydration(
	digest hash.Hash,
	hydration publicationTransitionClassHydration,
) {
	publicationTransitionHashHydrationCharge(digest, hydration.preCharge)
	publicationTransitionHashHydrationCharge(digest, hydration.postCharge)
	publicationTransitionHashUint64(digest, hydration.postDependencies)
	publicationTransitionHashBool(digest, hydration.candidateVisible)
}

func publicationTransitionHashHydrationCharge(
	digest hash.Hash,
	charge publicationTransitionHydrationCharge,
) {
	publicationTransitionHashUint64(digest, charge.definitionBytes)
	publicationTransitionHashUint64(digest, charge.projectionBytes)
	publicationTransitionHashUint64(digest, charge.selectorPatterns)
	publicationTransitionHashUint64(digest, charge.selectorValueBytes)
	publicationTransitionHashUint64(digest, charge.dependencies)
}

func publicationTransitionHashIndexSignature(
	digest hash.Hash,
	signature *publicationIndexORSignature,
) {
	if signature == nil {
		publicationTransitionHashBool(digest, false)
		return
	}
	publicationTransitionHashBool(digest, true)
	for _, word := range signature.before {
		publicationTransitionHashUint64(digest, word)
	}
	for _, word := range signature.after {
		publicationTransitionHashUint64(digest, word)
	}
	publicationTransitionHashUint64(digest, uint64(signature.minimumIndexes))
}

func publicationTransitionHashCandidateDependencies(
	digest hash.Hash,
	authority candidateDependencyAuthority,
) {
	publicationTransitionHashBool(digest, !authority.IsZero())
	if authority.IsZero() {
		return
	}
	candidate := authority.state.candidate
	publicationTransitionHashString(digest, candidate.objectID)
	publicationTransitionHashUint64(digest, uint64(candidate.version))
	_, _ = digest.Write(candidate.definitionDigest[:])
	publicationTransitionHashString(digest, candidate.ownerID)
	publicationTransitionHashUint64(digest, uint64(authority.sourceStage()))
	targets := authority.state.targets
	publicationTransitionHashUint64(digest, uint64(len(targets)))
	for _, target := range targets {
		publicationTransitionHashString(digest, target.objectID)
		publicationTransitionHashUint64(digest, uint64(target.version))
		_, _ = digest.Write(target.definitionDigest[:])
		publicationTransitionHashString(digest, target.ownerID)
		publicationTransitionHashUint64(digest, uint64(target.role))
		publicationTransitionHashUint64(digest, uint64(target.targetStage))
	}
}

func publicationTransitionDerivedTargetCount(authority candidateDependencyAuthority) int {
	if authority.state == nil {
		return 0
	}
	return len(authority.state.targets)
}

func publicationTransitionHashString(digest hash.Hash, value string) {
	writeDigestFrame(digest, []byte(value))
}

func publicationTransitionHashBool(digest hash.Hash, value bool) {
	if value {
		_, _ = digest.Write([]byte{1})
		return
	}
	_, _ = digest.Write([]byte{0})
}

func publicationTransitionHashUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func invalidPublicationTransition(reason string) error {
	return fmt.Errorf("%w: publication ACTIVE transition %s", ErrCorrupt, reason)
}
