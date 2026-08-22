package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"fortio.org/safecast"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

// publicationWinnerCohort is one already-enumerated, complete visibility
// cohort after precedence has selected exactly one winner for every visible
// type/name slot. expectedWinnerCount is a separate cohort-local completeness
// authority supplied by the future transactional reader; this helper only
// compares it with the slice and cannot independently prove enumeration.
//
// This pure helper neither enumerates affected visibility cohorts nor owns a
// MaximumPublicationCohorts or aggregate-work budget. A single search
// Resolution is never sufficient publication authority. A future
// transactional caller may skip a cohort in which the candidate is shadowed
// only after independently proving that cohort's winner inventory and every
// existing dependency authority are unchanged.
type publicationWinnerCohort struct {
	expectedWinnerCount uint32
	winners             []publicationWinner
}

type publicationWinner struct {
	object                      knowledgesnapshot.Object
	existingDependenciesPresent bool
	existingDependencies        []publicationPersistedDependency
}

// publicationPersistedDependency retains the scalar authority of one existing
// immutable dependency row. Slice position is not silently accepted as its
// ordinal: ordinal and canonical target order must both already agree.
type publicationPersistedDependency struct {
	ordinal        int64
	targetObjectID string
	targetVersion  int64
	role           opensplunk.KnowledgeDependencyRole
}

type publicationCandidateAuthority struct {
	objectID         string
	version          int64
	definitionDigest [sha256.Size]byte
	ownerID          string
}

type publicationCohortCandidateMode uint8

const (
	publicationCohortCandidatePresent publicationCohortCandidateMode = iota
	publicationCohortCandidateAbsent
)

// candidateDependencyAuthority is pointer-backed so an absent result remains
// distinct from a successfully compiled candidate with zero dependencies.
// Its private state retains richer compiler authority separately from the
// smaller database projection.
type candidateDependencyAuthority struct {
	state *candidateDependencyAuthorityState
}

type candidateDependencyAuthorityState struct {
	candidate   publicationCandidateAuthority
	sourceStage opensplunk.KnowledgeSearchStage
	targets     []publicationDerivedDependencyTarget
	projection  []publicationDependency
}

// publicationWinnerCohortAuthority is pointer-backed so a successfully
// validated cohort in which the transition candidate does not win remains
// distinct from an uninitialized result. candidateDependencies is itself
// pointer-backed, preserving the further distinction between a non-winning
// candidate and an exact winning candidate with zero edges.
type publicationWinnerCohortAuthority struct {
	state *publicationWinnerCohortAuthorityState
}

type publicationWinnerCohortAuthorityState struct {
	programCommitment     [sha256.Size]byte
	transitionCandidate   publicationCandidateAuthority
	candidateWins         bool
	candidateDependencies candidateDependencyAuthority
}

type publicationDerivedDependencyTarget struct {
	objectID         string
	version          int64
	definitionDigest [sha256.Size]byte
	ownerID          string
	role             opensplunk.KnowledgeDependencyRole
	targetStage      opensplunk.KnowledgeSearchStage
}

func (authority candidateDependencyAuthority) IsZero() bool {
	return authority.state == nil
}

func (authority candidateDependencyAuthority) Equal(other candidateDependencyAuthority) bool {
	if authority.state == nil || other.state == nil {
		return authority.state == nil && other.state == nil
	}
	return authority.state.candidate == other.state.candidate &&
		authority.state.sourceStage == other.state.sourceStage &&
		slices.Equal(authority.state.targets, other.state.targets) &&
		slices.Equal(authority.state.projection, other.state.projection)
}

func (authority candidateDependencyAuthority) candidateAuthority() publicationCandidateAuthority {
	if authority.state == nil {
		return publicationCandidateAuthority{}
	}
	result := authority.state.candidate
	result.objectID = strings.Clone(result.objectID)
	result.ownerID = strings.Clone(result.ownerID)
	return result
}

func (authority candidateDependencyAuthority) sourceStage() opensplunk.KnowledgeSearchStage {
	if authority.state == nil {
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_UNSPECIFIED
	}
	return authority.state.sourceStage
}

func (authority candidateDependencyAuthority) derivedTargets() []publicationDerivedDependencyTarget {
	if authority.state == nil {
		return nil
	}
	result := slices.Clone(authority.state.targets)
	for index := range result {
		result[index].objectID = strings.Clone(result[index].objectID)
		result[index].ownerID = strings.Clone(result[index].ownerID)
	}
	return result
}

func (authority candidateDependencyAuthority) databaseProjection() []publicationDependency {
	if authority.state == nil {
		return nil
	}
	result := slices.Clone(authority.state.projection)
	for index := range result {
		result[index].targetObjectID = strings.Clone(result[index].targetObjectID)
	}
	return result
}

func (authority candidateDependencyAuthority) detached() candidateDependencyAuthority {
	if authority.state == nil {
		return candidateDependencyAuthority{}
	}
	return candidateDependencyAuthority{state: &candidateDependencyAuthorityState{
		candidate:   authority.candidateAuthority(),
		sourceStage: authority.sourceStage(),
		targets:     authority.derivedTargets(),
		projection:  authority.databaseProjection(),
	}}
}

func (authority publicationWinnerCohortAuthority) IsZero() bool {
	return authority.state == nil
}

func (authority publicationWinnerCohortAuthority) Equal(other publicationWinnerCohortAuthority) bool {
	if authority.state == nil || other.state == nil {
		return authority.state == nil && other.state == nil
	}
	return authority.state.programCommitment == other.state.programCommitment &&
		authority.state.transitionCandidate == other.state.transitionCandidate &&
		authority.state.candidateWins == other.state.candidateWins &&
		authority.state.candidateDependencies.Equal(other.state.candidateDependencies)
}

func (authority publicationWinnerCohortAuthority) candidateDependencies() candidateDependencyAuthority {
	if authority.state == nil {
		return candidateDependencyAuthority{}
	}
	return authority.state.candidateDependencies.detached()
}

func (authority publicationWinnerCohortAuthority) candidateBinding() (
	candidate publicationCandidateAuthority,
	wins bool,
	present bool,
) {
	if authority.state == nil {
		return publicationCandidateAuthority{}, false, false
	}
	result := authority.state.transitionCandidate
	result.objectID = strings.Clone(result.objectID)
	result.ownerID = strings.Clone(result.ownerID)
	return result, authority.state.candidateWins, true
}

func (authority publicationWinnerCohortAuthority) programCommitment() ([sha256.Size]byte, bool) {
	if authority.state == nil {
		return [sha256.Size]byte{}, false
	}
	return authority.state.programCommitment, true
}

type canonicalPublicationWinner struct {
	existingDependenciesPresent bool
	existingDependencies        []publicationPersistedDependency
	key                         dependencyVersionKey
	object                      *opensplunk.KnowledgeSnapshotObject
	semantics                   resolutionDefinitionSemantics
	definitionBytes             uint64
	selectorWork                uint64
	stageRank                   uint8
}

// compilePublicationWinnerCohort derives dependency authority without reading
// or trusting candidate dependency rows. Program-global depth and ordinals are
// deliberately not projected into the per-source persisted row order.
func compilePublicationWinnerCohort(
	ctx context.Context,
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
) (candidateDependencyAuthority, error) {
	return compilePublicationWinnerCohortTransition(ctx, cohort, candidate, true, nil)
}

// validatePublicationWinnerCohort validates one complete post-publication
// winner cohort. candidateWins is an assertion that this helper verifies: the
// exact transition candidate must occur once when true and must not occur when
// false. When false, every winner must carry exact persisted dependency
// authority, including an explicit present marker for an empty set.
func validatePublicationWinnerCohort(
	ctx context.Context,
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
	candidateWins bool,
) (publicationWinnerCohortAuthority, error) {
	var programCommitment [sha256.Size]byte
	compiled, err := compilePublicationWinnerCohortTransition(
		ctx,
		cohort,
		candidate,
		candidateWins,
		&programCommitment,
	)
	if err != nil {
		return publicationWinnerCohortAuthority{}, err
	}
	return publicationWinnerCohortAuthority{state: &publicationWinnerCohortAuthorityState{
		programCommitment: programCommitment,
		transitionCandidate: publicationCandidateAuthority{
			objectID:         strings.Clone(candidate.objectID),
			version:          candidate.version,
			definitionDigest: candidate.definitionDigest,
			ownerID:          strings.Clone(candidate.ownerID),
		},
		candidateWins:         candidateWins,
		candidateDependencies: compiled.detached(),
	}}, nil
}

func compilePublicationWinnerCohortTransition(
	ctx context.Context,
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
	candidateWins bool,
	programCommitmentOutput *[sha256.Size]byte,
) (candidateDependencyAuthority, error) {
	return compilePublicationWinnerCohortMode(
		ctx,
		cohort,
		candidate,
		candidateWins,
		publicationCohortCandidatePresent,
		programCommitmentOutput,
	)
}

// validatePublicationExistingWinnerCohort recompiles one complete cohort with
// no transition candidate. Every winner must therefore carry exact persisted
// dependency authority. The explicit absent mode prevents topology validators
// from fabricating a sentinel candidate identity merely to reach that path.
func validatePublicationExistingWinnerCohort(
	ctx context.Context,
	cohort publicationWinnerCohort,
) ([sha256.Size]byte, error) {
	var commitment [sha256.Size]byte
	_, err := compilePublicationWinnerCohortMode(
		ctx,
		cohort,
		publicationCandidateAuthority{},
		false,
		publicationCohortCandidateAbsent,
		&commitment,
	)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return commitment, nil
}

func compilePublicationWinnerCohortMode(
	ctx context.Context,
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
	candidateWins bool,
	candidateMode publicationCohortCandidateMode,
	programCommitmentOutput *[sha256.Size]byte,
) (candidateDependencyAuthority, error) {
	if programCommitmentOutput != nil {
		*programCommitmentOutput = [sha256.Size]byte{}
	}
	if ctx == nil {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication compilation context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return candidateDependencyAuthority{}, err
	}
	candidateValue := candidate
	candidatePresent := candidateMode == publicationCohortCandidatePresent
	if candidateMode != publicationCohortCandidatePresent &&
		candidateMode != publicationCohortCandidateAbsent {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication cohort candidate mode is invalid",
			control.ErrInvalidArgument,
		)
	}
	if !candidatePresent && (candidateWins || candidateValue != (publicationCandidateAuthority{})) {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: candidate-absent publication cohort carries candidate authority",
			control.ErrInvalidArgument,
		)
	}
	if int(cohort.expectedWinnerCount) != len(cohort.winners) ||
		(candidateWins && cohort.expectedWinnerCount == 0) {
		return candidateDependencyAuthority{}, invalidPublicationWinnerCohort("is incomplete")
	}
	if cohort.expectedWinnerCount > knowledgeprogram.MaximumObjects {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication winner cohort exceeds its object limit",
			control.ErrCapacityExceeded,
		)
	}
	if candidatePresent && (!validIdentity(candidateValue.objectID, maximumObjectIDBytes) ||
		candidateValue.version < 1 ||
		candidateValue.definitionDigest == ([sha256.Size]byte{}) ||
		!validIdentity(candidateValue.ownerID, maximumOwnerIDBytes)) {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication candidate authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	candidateMatches := 0
	for index := range cohort.winners {
		if candidatePresent && publicationWinnerMatchesCandidate(cohort.winners[index].object, candidateValue) {
			candidateMatches++
		}
	}
	if candidateMatches > 1 {
		return candidateDependencyAuthority{}, invalidPublicationWinnerCohort(
			"duplicates the publication candidate",
		)
	}
	if candidateWins {
		if candidateMatches == 0 {
			return candidateDependencyAuthority{}, fmt.Errorf(
				"%w: publication candidate is not the exact cohort winner",
				control.ErrDependencyConflict,
			)
		}
	} else if candidateMatches != 0 {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication candidate unexpectedly wins its cohort",
			control.ErrDependencyConflict,
		)
	}
	if err := preflightPublicationWinnerCohortTransition(
		cohort,
		candidateValue,
		candidateWins,
		candidateMode,
	); err != nil {
		return candidateDependencyAuthority{}, err
	}

	winners := make([]*canonicalPublicationWinner, 0, len(cohort.winners))
	semanticWinners := make([]resolutionCandidate, 0, len(cohort.winners))
	byKey := make(map[dependencyVersionKey]*canonicalPublicationWinner, len(cohort.winners))
	identities := make(map[string]struct{}, len(cohort.winners))
	names := make(map[string]struct{}, len(cohort.winners))
	var definitionBytes uint64
	var selectorWork uint64
	var candidateWinner *canonicalPublicationWinner
	for index := range cohort.winners {
		input := cohort.winners[index]
		isCandidate := candidatePresent && candidateWins &&
			publicationWinnerMatchesCandidate(input.object, candidateValue)
		winner, err := canonicalizePublicationWinner(
			input,
			isCandidate,
		)
		if err != nil {
			return candidateDependencyAuthority{}, err
		}
		if !addPublicationResource(
			&definitionBytes,
			winner.definitionBytes,
			knowledgeprogram.MaximumDefinitionBytes,
		) || !addPublicationResource(
			&selectorWork,
			winner.selectorWork,
			knowledge.MaximumSelectorWildcardWorkUnits,
		) {
			return candidateDependencyAuthority{}, fmt.Errorf(
				"%w: publication winner cohort exceeds aggregate definition or selector limits",
				control.ErrCapacityExceeded,
			)
		}
		if _, duplicate := identities[winner.key.objectID]; duplicate {
			return candidateDependencyAuthority{}, invalidPublicationWinnerCohort("duplicates an object identity")
		}
		nameKey := fmt.Sprintf("%d\x00%s", winner.object.GetObjectType(), winner.object.GetName())
		if _, duplicate := names[nameKey]; duplicate {
			return candidateDependencyAuthority{}, invalidPublicationWinnerCohort("contains an ambiguous winner slot")
		}
		identities[winner.key.objectID] = struct{}{}
		names[nameKey] = struct{}{}
		stored := new(canonicalPublicationWinner)
		*stored = winner
		winners = append(winners, stored)
		semanticWinners = append(semanticWinners, resolutionCandidate{semantics: stored.semantics})
		byKey[stored.key] = stored

		if isCandidate && stored.key.objectID == candidateValue.objectID &&
			stored.key.version == candidateValue.version &&
			bytes.Equal(stored.object.GetDefinitionSha256(), candidateValue.definitionDigest[:]) {
			candidateWinner = stored
		}
	}
	// Every persisted row and executable definition is detached before the next
	// caller-controlled context callback.
	if err := ctx.Err(); err != nil {
		return candidateDependencyAuthority{}, err
	}
	if candidateWins && candidateWinner == nil {
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication candidate is not the exact cohort winner",
			control.ErrDependencyConflict,
		)
	}
	if _, err := aggregateResolutionStaticCharges(semanticWinners); err != nil {
		return candidateDependencyAuthority{}, err
	}

	for index := range winners {
		if err := ctx.Err(); err != nil {
			return candidateDependencyAuthority{}, err
		}
		winner := winners[index]
		if winner == candidateWinner {
			// Candidate rows are structurally absent. A non-nil empty slice is
			// intentionally rejected so it cannot masquerade as trusted empty
			// persistence authority.
			if winner.existingDependenciesPresent || winner.existingDependencies != nil {
				return candidateDependencyAuthority{}, invalidPublicationWinnerCohort("submits candidate dependency rows")
			}
			continue
		}
		if !winner.existingDependenciesPresent {
			return candidateDependencyAuthority{}, invalidPublicationWinnerCohort("omits existing dependency authority")
		}
		if err := validatePersistedPublicationDependencies(
			winner,
			winner.existingDependencies,
			byKey,
		); err != nil {
			return candidateDependencyAuthority{}, err
		}
	}

	sort.Slice(winners, func(left, right int) bool {
		if winners[left].stageRank != winners[right].stageRank {
			return winners[left].stageRank < winners[right].stageRank
		}
		if winners[left].object.GetName() != winners[right].object.GetName() {
			return winners[left].object.GetName() < winners[right].object.GetName()
		}
		return winners[left].object.GetKnowledgeObjectId() < winners[right].object.GetKnowledgeObjectId()
	})
	objects := make([]*opensplunk.KnowledgeSnapshotObject, len(winners))
	stageOrdinals := make(map[opensplunk.KnowledgeSearchStage]uint32, 3)
	for index := range winners {
		if err := ctx.Err(); err != nil {
			return candidateDependencyAuthority{}, err
		}
		object := winners[index].object
		object.ResolutionOrdinal = uint32(index)
		object.StageOrdinal = stageOrdinals[object.GetStage()]
		stageOrdinals[object.GetStage()]++
		objects[index] = object
	}
	if err := ctx.Err(); err != nil {
		return candidateDependencyAuthority{}, err
	}
	program, compileErr := knowledgeprogram.Compile(objects)
	if err := ctx.Err(); err != nil {
		return candidateDependencyAuthority{}, err
	}
	if compileErr != nil {
		if errors.Is(compileErr, knowledgeprogram.ErrResourceLimit) {
			return candidateDependencyAuthority{}, fmt.Errorf(
				"%w: publication winner cohort exceeds semantic limits",
				control.ErrCapacityExceeded,
			)
		}
		if candidateWins {
			return candidateDependencyAuthority{}, fmt.Errorf(
				"%w: publication candidate conflicts with its winner cohort",
				control.ErrDependencyConflict,
			)
		}
		return candidateDependencyAuthority{}, fmt.Errorf(
			"%w: publication transition conflicts with its winner cohort",
			control.ErrDependencyConflict,
		)
	}
	programCommitment, present := program.Commitment()
	if !present || programCommitment == ([sha256.Size]byte{}) {
		return candidateDependencyAuthority{}, invalidPublicationWinnerCohort(
			"compiler program commitment is absent",
		)
	}

	derived, err := publicationDependenciesFromProgram(ctx, program, byKey)
	if err != nil {
		return candidateDependencyAuthority{}, err
	}
	for index := range winners {
		if err := ctx.Err(); err != nil {
			return candidateDependencyAuthority{}, err
		}
		winner := winners[index]
		if winner == candidateWinner {
			continue
		}
		if !persistedPublicationDependenciesEqualDerived(
			winner.existingDependencies,
			derived[winner.key],
		) {
			if candidateWins {
				return candidateDependencyAuthority{}, fmt.Errorf(
					"%w: candidate changes an existing winner dependency authority",
					control.ErrDependencyConflict,
				)
			}
			return candidateDependencyAuthority{}, fmt.Errorf(
				"%w: publication transition changes an existing winner dependency authority",
				control.ErrDependencyConflict,
			)
		}
	}
	if !candidateWins {
		if programCommitmentOutput != nil {
			*programCommitmentOutput = programCommitment
		}
		return candidateDependencyAuthority{}, nil
	}

	candidateDerived := derived[dependencyVersionKey{
		objectID: candidateValue.objectID,
		version:  candidateValue.version,
	}]
	targets := make([]publicationDerivedDependencyTarget, len(candidateDerived))
	projection := make([]publicationDependency, len(candidateDerived))
	for index, dependency := range candidateDerived {
		targets[index] = dependency
		targets[index].objectID = strings.Clone(dependency.objectID)
		targets[index].ownerID = strings.Clone(dependency.ownerID)
		projection[index] = publicationDependency{
			targetObjectID: strings.Clone(dependency.objectID),
			targetVersion:  dependency.version,
		}
	}
	result := candidateDependencyAuthority{state: &candidateDependencyAuthorityState{
		candidate: publicationCandidateAuthority{
			objectID:         strings.Clone(candidateValue.objectID),
			version:          candidateValue.version,
			definitionDigest: candidateValue.definitionDigest,
			ownerID:          strings.Clone(candidateValue.ownerID),
		},
		sourceStage: candidateWinner.object.GetStage(),
		targets:     targets,
		projection:  projection,
	}}
	if programCommitmentOutput != nil {
		*programCommitmentOutput = programCommitment
	}
	return result, nil
}

func preflightPublicationWinnerCohortTransition(
	cohort publicationWinnerCohort,
	candidate publicationCandidateAuthority,
	candidateWins bool,
	candidateMode publicationCohortCandidateMode,
) error {
	var existingDependencies uint64
	for index := range cohort.winners {
		winner := cohort.winners[index]
		isCandidate := candidateMode == publicationCohortCandidatePresent &&
			candidateWins && publicationWinnerMatchesCandidate(winner.object, candidate)
		if isCandidate {
			if winner.existingDependenciesPresent || winner.existingDependencies != nil {
				return invalidPublicationWinnerCohort("submits candidate dependency rows")
			}
		} else {
			if !addPublicationResource(
				&existingDependencies,
				uint64(len(winner.existingDependencies)),
				knowledgeprogram.MaximumDependencies,
			) {
				return fmt.Errorf(
					"%w: publication winner cohort exceeds its dependency limit",
					control.ErrCapacityExceeded,
				)
			}
			if err := validatePersistedPublicationDependencyScalars(
				winner.existingDependencies,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func publicationWinnerMatchesCandidate(
	object knowledgesnapshot.Object,
	candidate publicationCandidateAuthority,
) bool {
	candidateVersion := safecast.MustConv[uint64](candidate.version)
	return object.KnowledgeObjectID == candidate.objectID &&
		object.Version == candidateVersion &&
		bytes.Equal(object.DefinitionSHA256, candidate.definitionDigest[:]) &&
		object.OwnerID == candidate.ownerID
}

func addPublicationResource(total *uint64, value, maximum uint64) bool {
	if total == nil || *total > maximum || value > maximum-*total {
		return false
	}
	*total += value
	return true
}

func canonicalizePublicationWinner(
	input publicationWinner,
	candidate bool,
) (canonicalPublicationWinner, error) {
	object := input.object
	if !validIdentity(object.KnowledgeObjectID, maximumObjectIDBytes) ||
		object.Version == 0 || object.Version > math.MaxInt64 ||
		!validIdentity(object.AppID, maximumAppIDBytes) ||
		!validIdentity(object.OwnerID, maximumOwnerIDBytes) ||
		object.Definition == nil || len(object.DefinitionSHA256) != sha256.Size {
		return canonicalPublicationWinner{}, invalidPublicationWinnerAuthority(
			candidate,
			false,
			"contains an invalid object authority",
		)
	}
	stage, stageRank, ok := publicationStageForObjectType(object.ObjectType)
	if !ok {
		return canonicalPublicationWinner{}, invalidPublicationWinnerAuthority(
			candidate,
			false,
			"contains an unsupported object type",
		)
	}
	normalized, err := knowledgedefinition.Normalize(object.Definition)
	if err != nil {
		return canonicalPublicationWinner{}, invalidPublicationWinnerAuthority(
			candidate,
			errors.Is(err, knowledgedefinition.ErrDefinitionTooLarge),
			"contains a noncanonical definition",
		)
	}
	if !proto.Equal(normalized.Definition, object.Definition) ||
		!bytes.Equal(normalized.Digest[:], object.DefinitionSHA256) ||
		normalized.ObjectType != object.ObjectType || normalized.Name != object.Name ||
		normalized.AppID != object.AppID || normalized.SharingScope != object.SharingScope {
		return canonicalPublicationWinner{}, invalidPublicationWinnerAuthority(
			candidate,
			false,
			"contains inconsistent definition authority",
		)
	}
	semantics, err := compileResolutionDefinition(normalized)
	if err != nil {
		return canonicalPublicationWinner{}, invalidPublicationWinnerAuthority(
			candidate,
			false,
			"contains a nonexecutable definition",
		)
	}
	var dependencies []publicationPersistedDependency
	if input.existingDependencies != nil {
		dependencies = make([]publicationPersistedDependency, len(input.existingDependencies))
		for index, dependency := range input.existingDependencies {
			dependencies[index] = dependency
			dependencies[index].targetObjectID = strings.Clone(dependency.targetObjectID)
		}
	}
	return canonicalPublicationWinner{
		existingDependenciesPresent: input.existingDependenciesPresent,
		existingDependencies:        dependencies,
		key: dependencyVersionKey{
			objectID: strings.Clone(object.KnowledgeObjectID),
			version:  int64(object.Version),
		},
		object: &opensplunk.KnowledgeSnapshotObject{
			Stage:             stage,
			KnowledgeObjectId: strings.Clone(object.KnowledgeObjectID),
			Version:           object.Version,
			ObjectType:        object.ObjectType,
			Name:              strings.Clone(object.Name),
			AppId:             strings.Clone(object.AppID),
			OwnerId:           strings.Clone(object.OwnerID),
			SharingScope:      object.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
		},
		semantics:       semantics,
		definitionBytes: uint64(len(normalized.Bytes)),
		selectorWork:    normalized.Selector.Stats().WildcardWorkUnits,
		stageRank:       stageRank,
	}, nil
}

func invalidPublicationWinnerAuthority(
	candidate bool,
	resource bool,
	reason string,
) error {
	if !candidate {
		return invalidPublicationWinnerCohort(reason)
	}
	if resource {
		return fmt.Errorf("%w: publication candidate %s", control.ErrCapacityExceeded, reason)
	}
	return fmt.Errorf("%w: publication candidate %s", control.ErrInvalidArgument, reason)
}

func publicationStageForObjectType(
	objectType opensplunk.KnowledgeObjectType,
) (opensplunk.KnowledgeSearchStage, uint8, bool) {
	switch objectType {
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, true
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, true
	case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, true
	default:
		return 0, 0, false
	}
}

// publicationCohortDependencyTargetAbsentError distinguishes a well-formed
// persisted endpoint that is absent from one compiled winner cohort. It still
// unwraps to ErrCorrupt for general cohort callers; topology admission may
// reclassify it only after independently validating the endpoint against the
// complete durable ACTIVE inventory.
type publicationCohortDependencyTargetAbsentError struct{}

func (*publicationCohortDependencyTargetAbsentError) Error() string {
	return "publication winner cohort has an incomplete existing dependency closure"
}

func (*publicationCohortDependencyTargetAbsentError) Unwrap() error {
	return ErrCorrupt
}

func validatePersistedPublicationDependencies(
	source *canonicalPublicationWinner,
	dependencies []publicationPersistedDependency,
	winners map[dependencyVersionKey]*canonicalPublicationWinner,
) error {
	if source == nil {
		return invalidPublicationWinnerCohort("contains invalid existing dependency authority")
	}
	if err := validatePersistedPublicationDependencyScalars(dependencies); err != nil {
		return err
	}
	for _, dependency := range dependencies {
		target := winners[dependencyVersionKey{
			objectID: dependency.targetObjectID,
			version:  dependency.targetVersion,
		}]
		if target == nil {
			return &publicationCohortDependencyTargetAbsentError{}
		}
		if source.key == target.key || source.stageRank <= target.stageRank {
			return invalidPublicationWinnerCohort("has an incomplete existing dependency closure")
		}
	}
	return nil
}

func validatePersistedPublicationDependencyScalars(
	dependencies []publicationPersistedDependency,
) error {
	if len(dependencies) > maximumDependenciesPerVersion {
		return invalidPublicationWinnerCohort("contains invalid existing dependency authority")
	}
	for index, dependency := range dependencies {
		if dependency.ordinal != int64(index) ||
			!validIdentity(dependency.targetObjectID, maximumObjectIDBytes) ||
			dependency.targetVersion < 1 ||
			dependency.role != opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT {
			return invalidPublicationWinnerCohort("contains malformed existing dependency rows")
		}
		if index > 0 && !publicationPersistedDependencyAfter(dependencies[index-1], dependency) {
			return invalidPublicationWinnerCohort("contains noncanonical existing dependency rows")
		}
	}
	return nil
}

func publicationPersistedDependencyAfter(
	previous, current publicationPersistedDependency,
) bool {
	if previous.targetObjectID != current.targetObjectID {
		return previous.targetObjectID < current.targetObjectID
	}
	if previous.targetVersion != current.targetVersion {
		return previous.targetVersion < current.targetVersion
	}
	return previous.role < current.role
}

func publicationDependenciesFromProgram(
	ctx context.Context,
	program knowledgeprogram.Program,
	winners map[dependencyVersionKey]*canonicalPublicationWinner,
) (map[dependencyVersionKey][]publicationDerivedDependencyTarget, error) {
	result := make(map[dependencyVersionKey][]publicationDerivedDependencyTarget, len(winners))
	dependencies := program.Dependencies()
	for index, dependency := range dependencies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if dependency == nil || dependency.GetCanonicalOrdinal() != uint32(index) ||
			dependency.GetRole() != opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT {
			return nil, invalidPublicationWinnerCohort("compiler dependency authority is invalid")
		}
		sourceReference := dependency.GetSource()
		targetReference := dependency.GetTarget().GetObject()
		sourceVersion := safecast.MustConv[int64](sourceReference.GetVersion())
		targetVersion := safecast.MustConv[int64](targetReference.GetVersion())
		sourceKey := dependencyVersionKey{
			objectID: sourceReference.GetKnowledgeObjectId(),
			version:  sourceVersion,
		}
		targetKey := dependencyVersionKey{
			objectID: targetReference.GetKnowledgeObjectId(),
			version:  targetVersion,
		}
		source, target := winners[sourceKey], winners[targetKey]
		if source == nil || target == nil ||
			!bytes.Equal(sourceReference.GetDefinitionSha256(), source.object.GetDefinitionSha256()) ||
			!bytes.Equal(targetReference.GetDefinitionSha256(), target.object.GetDefinitionSha256()) ||
			dependency.GetSourceStage() != source.object.GetStage() ||
			dependency.GetTargetStage() != target.object.GetStage() ||
			source.stageRank <= target.stageRank {
			return nil, invalidPublicationWinnerCohort("compiler dependency endpoints disagree")
		}
		var targetDigest [sha256.Size]byte
		copy(targetDigest[:], targetReference.GetDefinitionSha256())
		result[sourceKey] = append(result[sourceKey], publicationDerivedDependencyTarget{
			objectID:         strings.Clone(targetKey.objectID),
			version:          targetKey.version,
			definitionDigest: targetDigest,
			ownerID:          strings.Clone(target.object.GetOwnerId()),
			role:             dependency.GetRole(),
			targetStage:      dependency.GetTargetStage(),
		})
	}
	for key := range winners {
		sort.Slice(result[key], func(left, right int) bool {
			if result[key][left].objectID != result[key][right].objectID {
				return result[key][left].objectID < result[key][right].objectID
			}
			if result[key][left].version != result[key][right].version {
				return result[key][left].version < result[key][right].version
			}
			return result[key][left].role < result[key][right].role
		})
	}
	return result, nil
}

func persistedPublicationDependenciesEqualDerived(
	persisted []publicationPersistedDependency,
	derived []publicationDerivedDependencyTarget,
) bool {
	if len(persisted) != len(derived) {
		return false
	}
	for index := range persisted {
		if persisted[index].ordinal != int64(index) ||
			persisted[index].targetObjectID != derived[index].objectID ||
			persisted[index].targetVersion != derived[index].version ||
			persisted[index].role != derived[index].role {
			return false
		}
	}
	return true
}

func invalidPublicationWinnerCohort(reason string) error {
	return fmt.Errorf("%w: publication winner cohort %s", ErrCorrupt, reason)
}
