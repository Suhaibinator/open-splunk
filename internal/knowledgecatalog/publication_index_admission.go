package knowledgecatalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sort"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// publicationIndexNameAdmissionInventory is one complete ACTIVE tenant
// inventory observed before a previously unknown physical index name is
// inserted. Existing names are possible on both sides of the topology change;
// newlyPotentiallySearchableIndexName exists only after it.
//
// The future transactional adapter remains responsible for proving that the
// object, app, and global index inventories and their scalar totals are exact.
type publicationIndexNameAdmissionInventory struct {
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
	expectedPotentiallySearchableIndexCount uint16
	potentiallySearchableIndexNames         []string
	newlyPotentiallySearchableIndexName     string
}

// publicationIndexNameAdmissionAuthority is pointer-backed so a successful
// admission in which the new name matches no object cannot collapse into an
// uninitialized result.
type publicationIndexNameAdmissionAuthority struct {
	state *publicationIndexNameAdmissionAuthorityState
}

type publicationIndexNameAdmissionAuthorityState struct {
	commitment [sha256.Size]byte
	tenantID   string
	indexName  string
}

func (authority publicationIndexNameAdmissionAuthority) IsZero() bool {
	return authority.state == nil
}

func (authority publicationIndexNameAdmissionAuthority) Equal(
	other publicationIndexNameAdmissionAuthority,
) bool {
	if authority.state == nil || other.state == nil {
		return authority.state == nil && other.state == nil
	}
	return authority.state.commitment == other.state.commitment &&
		authority.state.tenantID == other.state.tenantID &&
		authority.state.indexName == other.state.indexName
}

// validatePublicationIndexNameAdmission proves every winner cohort newly
// reachable when one physical index name joins the global potential-name
// inventory. It never derives or writes dependency rows: every selected
// winner is existing persisted authority and must exactly match a fresh
// candidate-free program compilation.
func validatePublicationIndexNameAdmission(
	ctx context.Context,
	input publicationIndexNameAdmissionInventory,
) (publicationIndexNameAdmissionAuthority, error) {
	if ctx == nil {
		return publicationIndexNameAdmissionAuthority{}, fmt.Errorf(
			"%w: publication index-name admission context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := preflightPublicationIndexNameAdmission(input); err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}

	// Normalize and detach every persisted body, selector, dependency row, and
	// scalar before the first caller-controlled context callback.
	detached, slots, err := admitPublicationIndexNameAdmissionInventory(input)
	if err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	if err := ctx.Err(); err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}

	classes, err := publicationTransitionPrincipalClasses(
		ctx,
		detached.activeAppIDs,
		slots,
		slots,
	)
	if err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	classHydration, err := validatePublicationTransitionClassHydration(
		ctx,
		classes,
		slots,
		slots,
		nil,
	)
	if err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	atoms, err := publicationIndexNameAdmissionAtoms(
		ctx,
		detached.potentiallySearchableIndexNames,
		detached.newlyPotentiallySearchableIndexName,
		slots,
	)
	if err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	signatures, err := enumeratePublicationIndexORSignatures(ctx, atoms)
	if err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	classStates, ok := publicationTransitionProductWithin(
		uint64(len(classes)),
		uint64(len(signatures)),
		maximumPublicationTransitionClassStates,
	)
	if !ok {
		return publicationIndexNameAdmissionAuthority{}, fmt.Errorf(
			"%w: publication index-name admission exceeds its visibility-state limit",
			control.ErrCapacityExceeded,
		)
	}

	inventoryCommitment := publicationIndexNameAdmissionInventoryCommitment(
		detached,
		slots,
	)
	semanticHasher := sha256.New()
	publicationTransitionHashString(
		semanticHasher,
		"open-splunk/publication-index-name-admission-semantic/v1",
	)
	_, _ = semanticHasher.Write(inventoryCommitment[:])
	publicationTransitionHashUint64(semanticHasher, classStates)
	if err := validatePublicationIndexNameAdmissionCohorts(
		ctx,
		classes,
		classHydration,
		signatures,
		slots,
		semanticHasher,
	); err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}
	if err := ctx.Err(); err != nil {
		return publicationIndexNameAdmissionAuthority{}, err
	}

	var commitment [sha256.Size]byte
	copy(commitment[:], semanticHasher.Sum(nil))
	return publicationIndexNameAdmissionAuthority{
		state: &publicationIndexNameAdmissionAuthorityState{
			commitment: commitment,
			tenantID:   strings.Clone(detached.tenantID),
			indexName: strings.Clone(
				detached.newlyPotentiallySearchableIndexName,
			),
		},
	}, nil
}

func preflightPublicationIndexNameAdmission(
	input publicationIndexNameAdmissionInventory,
) error {
	if !validIdentity(input.tenantID, maximumTenantIDBytes) {
		return fmt.Errorf(
			"%w: publication index-name admission tenant is invalid",
			control.ErrInvalidArgument,
		)
	}
	canonicalNew, err := control.NormalizeIndexName(
		input.newlyPotentiallySearchableIndexName,
	)
	if err != nil || canonicalNew != input.newlyPotentiallySearchableIndexName {
		return fmt.Errorf(
			"%w: newly potentially-searchable index name is not canonical",
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
			"%w: publication index-name admission scalar authority exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	if int(input.expectedActiveAppCount) != len(input.activeAppIDs) {
		return invalidPublicationIndexNameAdmission(
			"active-app inventory is incomplete",
		)
	}
	if len(input.activeAppIDs) == 0 {
		return invalidPublicationIndexNameAdmission(
			"has no active-app authority for its ACTIVE inventory",
		)
	}
	if len(input.activeAppIDs) > maximumReadableApps {
		return fmt.Errorf(
			"%w: publication index-name admission active-app inventory exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	for _, appID := range input.activeAppIDs {
		if !validIdentity(appID, maximumAppIDBytes) {
			return invalidPublicationIndexNameAdmission(
				"contains an invalid active-app identity",
			)
		}
	}
	if int(input.expectedCurrentActiveCount) != len(input.currentActive) {
		return invalidPublicationIndexNameAdmission(
			"current ACTIVE inventory is incomplete",
		)
	}
	if len(input.currentActive) == 0 {
		return invalidPublicationIndexNameAdmission(
			"has no current ACTIVE object authority",
		)
	}
	if len(input.currentActive) > MaximumResolutionCandidates {
		return fmt.Errorf(
			"%w: publication index-name admission exceeds its ACTIVE object limit",
			control.ErrCapacityExceeded,
		)
	}
	if int(input.expectedPotentiallySearchableIndexCount) !=
		len(input.potentiallySearchableIndexNames) {
		return invalidPublicationIndexNameAdmission(
			"potentially-searchable index inventory is incomplete",
		)
	}
	if len(input.potentiallySearchableIndexNames) >= maximumPublicationIndexAtoms {
		return fmt.Errorf(
			"%w: publication index-name admission exceeds its post-index atom limit",
			control.ErrCapacityExceeded,
		)
	}
	seenNames := make(map[string]struct{}, len(input.potentiallySearchableIndexNames))
	for _, name := range input.potentiallySearchableIndexNames {
		canonical, normalizeErr := control.NormalizeIndexName(name)
		if normalizeErr != nil || canonical != name {
			return invalidPublicationIndexNameAdmission(
				"contains a noncanonical potentially-searchable index name",
			)
		}
		if name == canonicalNew {
			return fmt.Errorf(
				"%w: newly potentially-searchable index name already exists",
				control.ErrAlreadyExists,
			)
		}
		if _, duplicate := seenNames[name]; duplicate {
			return invalidPublicationIndexNameAdmission(
				"duplicates a potentially-searchable index name",
			)
		}
		seenNames[name] = struct{}{}
	}

	var dependencies uint64
	for index := range input.currentActive {
		winner := input.currentActive[index]
		if !winner.existingDependenciesPresent {
			return invalidPublicationIndexNameAdmission(
				"current ACTIVE object omits dependency authority",
			)
		}
		if err := validatePersistedPublicationDependencyScalars(
			winner.existingDependencies,
		); err != nil {
			return err
		}
		if !addPublicationResource(
			&dependencies,
			uint64(len(winner.existingDependencies)),
			maximumPublicationTransitionDependencies,
		) {
			return fmt.Errorf(
				"%w: publication index-name admission exceeds its dependency limit",
				control.ErrCapacityExceeded,
			)
		}
	}
	if dependencies != input.expectedDependencyCount {
		return invalidPublicationIndexNameAdmission(
			"aggregate dependency scalar authority disagrees",
		)
	}
	return nil
}

func admitPublicationIndexNameAdmissionInventory(
	input publicationIndexNameAdmissionInventory,
) (
	publicationIndexNameAdmissionInventory,
	[]*publicationTransitionCanonicalObject,
	error,
) {
	detached := publicationIndexNameAdmissionInventory{
		tenantID:                                strings.Clone(input.tenantID),
		expectedDefinitionBytes:                 input.expectedDefinitionBytes,
		expectedProjectionBytes:                 input.expectedProjectionBytes,
		expectedSelectorPatterns:                input.expectedSelectorPatterns,
		expectedSelectorValueBytes:              input.expectedSelectorValueBytes,
		expectedCanonicalSelectorBytes:          input.expectedCanonicalSelectorBytes,
		expectedSelectorWork:                    input.expectedSelectorWork,
		expectedDependencyCount:                 input.expectedDependencyCount,
		expectedActiveAppCount:                  input.expectedActiveAppCount,
		activeAppIDs:                            make([]string, len(input.activeAppIDs)),
		expectedCurrentActiveCount:              input.expectedCurrentActiveCount,
		expectedPotentiallySearchableIndexCount: input.expectedPotentiallySearchableIndexCount,
		potentiallySearchableIndexNames: make(
			[]string,
			len(input.potentiallySearchableIndexNames),
		),
		newlyPotentiallySearchableIndexName: strings.Clone(
			input.newlyPotentiallySearchableIndexName,
		),
	}
	for index, appID := range input.activeAppIDs {
		detached.activeAppIDs[index] = strings.Clone(appID)
	}
	for index, name := range input.potentiallySearchableIndexNames {
		detached.potentiallySearchableIndexNames[index] = strings.Clone(name)
	}

	byID := make(
		map[string]*publicationTransitionCanonicalObject,
		len(input.currentActive),
	)
	byKey := make(
		map[dependencyVersionKey]*canonicalPublicationWinner,
		len(input.currentActive),
	)
	var aggregate publicationTransitionAggregateCharge
	for index := range input.currentActive {
		object, err := canonicalizePublicationTransitionObject(
			input.currentActive[index],
			false,
		)
		if err != nil {
			return publicationIndexNameAdmissionInventory{}, nil, err
		}
		if !publicationTransitionAddAggregateCharge(
			&aggregate,
			publicationTransitionAggregateCharge{
				definitionBytes:        object.canonical.definitionBytes,
				projectionBytes:        object.projectionBytes,
				selectorPatterns:       object.selectorPatterns,
				selectorValueBytes:     object.selectorValueBytes,
				canonicalSelectorBytes: object.canonicalSelectorBytes,
				selectorWork:           object.canonical.selectorWork,
				dependencies: uint64(
					len(object.winner.existingDependencies),
				),
			},
		) {
			return publicationIndexNameAdmissionInventory{}, nil, fmt.Errorf(
				"%w: publication index-name admission exceeds canonical aggregate limits",
				control.ErrCapacityExceeded,
			)
		}
		objectID := object.canonical.key.objectID
		if _, duplicate := byID[objectID]; duplicate {
			return publicationIndexNameAdmissionInventory{}, nil,
				invalidPublicationIndexNameAdmission(
					"duplicates a current ACTIVE object identity",
				)
		}
		byID[objectID] = object
		byKey[object.canonical.key] = &object.canonical
	}
	if aggregate.definitionBytes != input.expectedDefinitionBytes ||
		aggregate.projectionBytes != input.expectedProjectionBytes ||
		aggregate.selectorPatterns != input.expectedSelectorPatterns ||
		aggregate.selectorValueBytes != input.expectedSelectorValueBytes ||
		aggregate.canonicalSelectorBytes != input.expectedCanonicalSelectorBytes ||
		aggregate.selectorWork != input.expectedSelectorWork ||
		aggregate.dependencies != input.expectedDependencyCount {
		return publicationIndexNameAdmissionInventory{}, nil,
			invalidPublicationIndexNameAdmission(
				"aggregate projection, definition, or dependency authority disagrees",
			)
	}

	objectIDs := make([]string, 0, len(byID))
	for objectID := range byID {
		objectIDs = append(objectIDs, objectID)
	}
	slices.Sort(objectIDs)
	// Establish durable endpoint and stage closure against the complete ACTIVE
	// inventory before any cohort-local validation. A missing global endpoint,
	// self-edge, or forward-stage edge remains corrupt persistence authority;
	// only an endpoint absent from a later newly reachable cohort is a topology
	// conflict.
	for _, objectID := range objectIDs {
		object := byID[objectID]
		if err := validatePersistedPublicationDependencies(
			&object.canonical,
			object.winner.existingDependencies,
			byKey,
		); err != nil {
			return publicationIndexNameAdmissionInventory{}, nil, err
		}
	}
	slots := make([]*publicationTransitionCanonicalObject, len(objectIDs))
	for index, objectID := range objectIDs {
		slots[index] = byID[objectID]
	}
	return detached, slots, nil
}

func publicationIndexNameAdmissionAtoms(
	ctx context.Context,
	existingNames []string,
	newName string,
	slots []*publicationTransitionCanonicalObject,
) ([]publicationIndexAtom, error) {
	names := make([]string, 0, len(existingNames)+1)
	names = append(names, existingNames...)
	names = append(names, newName)
	slices.Sort(names)
	newOrdinal, found := slices.BinarySearch(names, newName)
	if !found {
		return nil, invalidPublicationIndexNameAdmission(
			"lost the newly potentially-searchable index name",
		)
	}
	atoms, err := publicationTransitionIndexAtoms(ctx, names, slots, slots)
	if err != nil {
		return nil, err
	}
	if newOrdinal < 0 || newOrdinal >= len(atoms) {
		return nil, invalidPublicationIndexNameAdmission(
			"new index atom ordinal is invalid",
		)
	}
	// The candidate name has no pre-insert physical identity. Its after
	// membership remains the selector result produced by the shared atom
	// builder; its before membership is the actual empty set.
	atoms[newOrdinal].before = publicationIndexMembership{}
	return atoms, nil
}

func validatePublicationIndexNameAdmissionCohorts(
	ctx context.Context,
	classes []publicationTransitionPrincipalClass,
	classHydration []publicationTransitionClassHydration,
	signatures []publicationIndexORSignature,
	slots []*publicationTransitionCanonicalObject,
	semanticHasher hash.Hash,
) error {
	if len(classes) != len(classHydration) || semanticHasher == nil {
		return invalidPublicationIndexNameAdmission(
			"semantic traversal authority is incomplete",
		)
	}
	seen := make(
		map[[sha256.Size]byte][]publicationTransitionValidatedCohort,
	)
	winnerCommitments := make(
		map[*publicationTransitionCanonicalObject][sha256.Size]byte,
	)
	var work publicationTransitionWork
	for classIndex, class := range classes {
		if err := ctx.Err(); err != nil {
			return err
		}
		publicationTransitionHashPrincipalClass(semanticHasher, class)
		publicationTransitionHashHydration(
			semanticHasher,
			classHydration[classIndex],
		)
		for signatureIndex := range signatures {
			signature := &signatures[signatureIndex]
			if err := ctx.Err(); err != nil {
				return err
			}
			beforeWinners, err := selectPublicationTransitionWinners(
				ctx,
				class,
				&signature.before,
				slots,
				nil,
				false,
				&work,
			)
			if err != nil {
				return err
			}
			afterWinners, err := selectPublicationTransitionWinners(
				ctx,
				class,
				&signature.after,
				slots,
				nil,
				true,
				&work,
			)
			if err != nil {
				return err
			}

			publicationTransitionHashIndexSignature(semanticHasher, signature)
			publicationTransitionHashWinnerPointers(
				semanticHasher,
				beforeWinners,
				winnerCommitments,
			)
			publicationTransitionHashWinnerPointers(
				semanticHasher,
				afterWinners,
				winnerCommitments,
			)
			changed := !slices.Equal(beforeWinners, afterWinners)
			publicationTransitionHashBool(semanticHasher, changed)
			if !changed {
				continue
			}

			cohortCommitment := publicationTransitionCohortCommitment(
				afterWinners,
				false,
				winnerCommitments,
			)
			var prior *publicationTransitionValidatedCohort
			bucket := seen[cohortCommitment]
			for index := range bucket {
				record := &bucket[index]
				if slices.Equal(record.winners, afterWinners) {
					prior = record
					break
				}
			}
			if prior != nil {
				_, _ = semanticHasher.Write(prior.programCommitment[:])
				continue
			}
			if err := work.chargeChangedCohort(afterWinners); err != nil {
				return err
			}
			cohort := publicationWinnerCohort{
				expectedWinnerCount: uint32(len(afterWinners)),
				winners:             make([]publicationWinner, len(afterWinners)),
			}
			for index, winner := range afterWinners {
				cohort.winners[index] = winner.winner
			}
			programCommitment, err := validatePublicationExistingWinnerCohort(
				ctx,
				cohort,
			)
			if err != nil {
				var absentTarget *publicationCohortDependencyTargetAbsentError
				if errors.As(err, &absentTarget) {
					return fmt.Errorf(
						"%w: newly reachable index topology excludes a persisted dependency target",
						control.ErrDependencyConflict,
					)
				}
				return err
			}
			if programCommitment == ([sha256.Size]byte{}) {
				return invalidPublicationIndexNameAdmission(
					"changed cohort program commitment is absent",
				)
			}
			seen[cohortCommitment] = append(
				seen[cohortCommitment],
				publicationTransitionValidatedCohort{
					winners:           slices.Clone(afterWinners),
					programCommitment: programCommitment,
				},
			)
			_, _ = semanticHasher.Write(programCommitment[:])
		}
	}
	return nil
}

func publicationIndexNameAdmissionInventoryCommitment(
	input publicationIndexNameAdmissionInventory,
	objects []*publicationTransitionCanonicalObject,
) [sha256.Size]byte {
	apps := slices.Clone(input.activeAppIDs)
	slices.Sort(apps)
	names := slices.Clone(input.potentiallySearchableIndexNames)
	slices.Sort(names)
	orderedObjects := slices.Clone(objects)
	sort.Slice(orderedObjects, func(left, right int) bool {
		return orderedObjects[left].canonical.key.objectID <
			orderedObjects[right].canonical.key.objectID
	})

	digest := sha256.New()
	publicationTransitionHashString(
		digest,
		"open-splunk/publication-index-name-admission/v1",
	)
	publicationTransitionHashString(digest, input.tenantID)
	publicationTransitionHashString(
		digest,
		input.newlyPotentiallySearchableIndexName,
	)
	publicationTransitionHashUint64(digest, input.expectedDefinitionBytes)
	publicationTransitionHashUint64(digest, input.expectedProjectionBytes)
	publicationTransitionHashUint64(digest, input.expectedSelectorPatterns)
	publicationTransitionHashUint64(digest, input.expectedSelectorValueBytes)
	publicationTransitionHashUint64(
		digest,
		input.expectedCanonicalSelectorBytes,
	)
	publicationTransitionHashUint64(digest, input.expectedSelectorWork)
	publicationTransitionHashUint64(digest, input.expectedDependencyCount)
	publicationTransitionHashUint64(digest, uint64(len(apps)))
	for _, appID := range apps {
		publicationTransitionHashString(digest, appID)
	}
	publicationTransitionHashUint64(digest, uint64(len(names)))
	for _, name := range names {
		publicationTransitionHashString(digest, name)
	}
	publicationTransitionHashUint64(digest, uint64(len(orderedObjects)))
	for _, object := range orderedObjects {
		publicationTransitionHashObject(digest, object)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func invalidPublicationIndexNameAdmission(reason string) error {
	return fmt.Errorf(
		"%w: publication index-name admission %s",
		ErrCorrupt,
		reason,
	)
}
