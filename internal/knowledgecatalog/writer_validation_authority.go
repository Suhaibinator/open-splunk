package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"fortio.org/safecast"
	"gorm.io/gorm"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
)

func (writer *Writer) validateFixedSnapshot(
	ctx context.Context,
	tx *gorm.DB,
	scope normalizedValidationScope,
	request normalizedValidationRequest,
	authorizedContext **AuthorizedContext,
) (knowledgevalidation.Result, uint64, error) {
	if err := ctx.Err(); err != nil {
		return knowledgevalidation.Result{}, 0, err
	}

	definition := request.definition
	var current *validationCurrentCandidate
	if request.update {
		loaded, err := writer.readValidationUpdateCurrent(
			tx,
			scope.write,
			request,
			authorizedContext,
		)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		current = &loaded
		definition, err = applyKnowledgeDefinitionMask(
			loaded.object.Definition,
			request.definition,
			request.updatePaths,
		)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
	}

	switch request.intent {
	case opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE:
		result, err := knowledgevalidation.BuildInactive(ctx, definition)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		projection, err := result.Proto(ctx)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		if projection.GetValid() {
			normalizedDefinition := projection.GetNormalizedDefinition()
			if normalizedDefinition == nil ||
				!validIdentity(normalizedDefinition.GetAppId(), maximumAppIDBytes) {
				return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
			}
			appID := strings.Clone(normalizedDefinition.GetAppId())
			if err := authorizeDefinitionApp(tx, scope.write, appID, false); err != nil {
				return knowledgevalidation.Result{}, 0, err
			}
			if current == nil && authorizedContext != nil {
				value := authorizedAppContext(appID)
				*authorizedContext = &value
			}
		}
		revision, err := readValidationCatalogRevisionBookends(tx, scope.write.tenantID)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		return result, revision, nil

	case opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION:
		preparation, err := knowledgevalidation.PrepareActive(ctx, definition)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		if invalid, ok := preparation.Invalid(); ok {
			revision, revisionErr := readValidationCatalogRevisionBookends(
				tx,
				scope.write.tenantID,
			)
			if revisionErr != nil {
				return knowledgevalidation.Result{}, 0, revisionErr
			}
			return invalid, revision, nil
		}
		candidate, ok := preparation.Candidate()
		if !ok {
			return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
		}
		return writer.validateActiveCandidate(
			ctx,
			tx,
			scope,
			current,
			candidate,
			authorizedContext,
		)
	default:
		return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
	}
}

type validationCurrentCandidate struct {
	registry     registryRecord
	object       Object
	version      versionRecord
	dependencies []publicationDependency
}

func (writer *Writer) readValidationUpdateCurrent(
	tx *gorm.DB,
	scope normalizedWriteScope,
	request normalizedValidationRequest,
	authorized **AuthorizedContext,
) (validationCurrentCandidate, error) {
	current, err := readAuthorizedMutationRegistry(tx, scope, request.objectID)
	if err != nil {
		return validationCurrentCandidate{}, err
	}
	if authorized != nil {
		value := authorizedObjectContext(current)
		*authorized = &value
	}
	if current.CurrentVersion != request.expectedVersion {
		return validationCurrentCandidate{}, control.ErrVersionConflict
	}
	if current.State == StateQuarantined ||
		request.intent == opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION &&
			current.State == StateDeleted {
		return validationCurrentCandidate{}, control.ErrVersionConflict
	}
	object, version, definition, err := writer.hydrateAuthorizedCurrentStateOnly(tx, current)
	if err != nil {
		return validationCurrentCandidate{}, err
	}
	if definition.opaque {
		return validationCurrentCandidate{}, fmt.Errorf(
			"%w: opaque future definitions cannot be validated by this server",
			control.ErrInvalidArgument,
		)
	}
	var dependencies []publicationDependency
	if request.intent == opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION {
		dependencies, err = dependenciesFromCurrent(tx, version)
		if err != nil {
			return validationCurrentCandidate{}, err
		}
	}
	return validationCurrentCandidate{
		registry:     current,
		object:       object,
		version:      version,
		dependencies: dependencies,
	}, nil
}

func (writer *Writer) validateActiveCandidate(
	ctx context.Context,
	tx *gorm.DB,
	scope normalizedValidationScope,
	current *validationCurrentCandidate,
	candidate knowledgevalidation.ActiveCandidate,
	authorizedContext **AuthorizedContext,
) (knowledgevalidation.Result, uint64, error) {
	normalizedDefinition, candidateDigest, err := candidate.Normalized(ctx)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	normalized, err := knowledgedefinition.Normalize(normalizedDefinition)
	if err != nil || normalized.Digest != candidateDigest {
		return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
	}
	authority, err := authorityFromNormalized(normalized)
	if err != nil {
		return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
	}
	if err := authorizeDefinitionApp(tx, scope.write, authority.appID, true); err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	if current == nil && authorizedContext != nil {
		value := authorizedAppContext(authority.appID)
		*authorizedContext = &value
	}

	objectID := ""
	version := int64(1)
	ownerID := scope.write.ownerID
	before := publicationTransitionEndpoint{}
	if current == nil {
		objectID, err = selectValidationCandidateID(tx, scope.write.tenantID)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
	} else {
		if current.version.ObjectVersion == math.MaxInt64 {
			return knowledgevalidation.Result{}, 0, control.ErrCapacityExceeded
		}
		objectID = current.registry.KnowledgeObjectID
		version = current.version.ObjectVersion + 1
		ownerID = current.registry.OwnerID
		before, err = recognizedPublicationTransitionEndpoint(
			current.object,
			current.registry.State,
			current.dependencies,
			true,
		)
		if err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
	}
	afterObject, err := recognizedPublicationObjectFromAuthority(
		scope.write.tenantID,
		objectID,
		version,
		ownerID,
		StateActive,
		authority,
	)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	after, err := recognizedPublicationTransitionEndpoint(
		afterObject,
		StateActive,
		nil,
		false,
	)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	read, err := writer.reader.readPublicationActiveTransitionInventory(
		tx,
		scope.write.tenantID,
		before,
		after,
	)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	catalogRevision := safecast.MustConv[uint64](read.catalog.revision)
	decision, err := validatePublicationActiveCandidate(ctx, read.inventory)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	if conflict, found := decision.conflict(); found {
		invalid, candidateInvalid, conflictErr := validationTransitionConflictResult(
			ctx,
			candidate,
			conflict,
		)
		if conflictErr != nil {
			return knowledgevalidation.Result{}, 0, conflictErr
		}
		if candidateInvalid {
			if err := matchValidationCatalogBookend(tx, scope.write.tenantID, read.catalog); err != nil {
				return knowledgevalidation.Result{}, 0, err
			}
			return invalid, catalogRevision, nil
		}
		return knowledgevalidation.Result{}, 0, knowledgevalidation.ErrInvariant
	}
	dependencies, present := decision.candidateDependencies()
	if !present {
		return knowledgevalidation.Result{}, 0, ErrCorrupt
	}
	validated, err := validatePublicationTransitionPersistenceTargets(
		ctx,
		tx,
		scope.write.tenantID,
		dependencies.candidateAuthority(),
		dependencies.sourceStage(),
		dependencies,
	)
	if err != nil {
		return knowledgevalidation.Result{}, 0, validationTargetIntegrityError(err)
	}
	projected, dependencyAuthorized, err := projectAuthorizedValidationDependencies(
		ctx,
		tx,
		scope.read,
		validated,
	)
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	if !dependencyAuthorized {
		invalid, buildErr := candidate.BuildDependencyUnavailable(ctx)
		if buildErr != nil {
			return knowledgevalidation.Result{}, 0, buildErr
		}
		if err := matchValidationCatalogBookend(tx, scope.write.tenantID, read.catalog); err != nil {
			return knowledgevalidation.Result{}, 0, err
		}
		return invalid, catalogRevision, nil
	}
	result, err := candidate.BuildValid(ctx, knowledgevalidation.ActivePublication{
		Candidate: knowledgevalidation.ExactIdentity{
			KnowledgeObjectID: objectID,
			Version:           uint64(version),
		},
		Dependencies: projected,
	})
	if err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	if err := matchValidationCatalogBookend(tx, scope.write.tenantID, read.catalog); err != nil {
		return knowledgevalidation.Result{}, 0, err
	}
	return result, catalogRevision, nil
}

func validationTransitionConflictResult(
	ctx context.Context,
	candidate knowledgevalidation.ActiveCandidate,
	conflict publicationActiveValidationConflict,
) (knowledgevalidation.Result, bool, error) {
	if conflict != publicationActiveValidationCohortDependencyTargetAbsent {
		return knowledgevalidation.Result{}, false, control.ErrDependencyConflict
	}
	result, err := candidate.BuildDependencyUnavailable(ctx)
	if err != nil {
		return knowledgevalidation.Result{}, false, err
	}
	return result, true, nil
}

func validationTargetIntegrityError(err error) error {
	if errors.Is(err, control.ErrDependencyConflict) {
		return fmt.Errorf(
			"%w: knowledge validation target closure is inconsistent",
			ErrCorrupt,
		)
	}
	return err
}

func readValidationCatalogRevisionBookends(tx *gorm.DB, tenantID string) (uint64, error) {
	initial, err := readCatalogState(tx, tenantID)
	if err != nil {
		return 0, err
	}
	if !initial.found || initial.revision < 0 {
		return 0, fmt.Errorf("%w: knowledge validation catalog authority is missing", ErrCorrupt)
	}
	if err := matchValidationCatalogBookend(tx, tenantID, initial); err != nil {
		return 0, err
	}
	return uint64(initial.revision), nil
}

func matchValidationCatalogBookend(tx *gorm.DB, tenantID string, initial catalogState) error {
	if !initial.found || initial.revision < 0 {
		return fmt.Errorf("%w: knowledge validation catalog authority is invalid", ErrCorrupt)
	}
	final, err := readCatalogState(tx, tenantID)
	if err != nil {
		return err
	}
	if !final.equal(initial) {
		return fmt.Errorf("%w: knowledge validation catalog authority changed", ErrCorrupt)
	}
	if initial.revision == 0 {
		var records []struct {
			Present int64 `gorm:"column:present"`
		}
		if err := tx.Raw(`
			SELECT EXISTS (
				SELECT 1
				FROM knowledge_objects
				WHERE tenant_id = ?
				LIMIT 1
			) AS present
		`, tenantID).Scan(&records).Error; err != nil {
			return err
		}
		if len(records) != 1 || records[0].Present != 0 {
			return fmt.Errorf(
				"%w: knowledge validation catalog authority is inconsistent",
				ErrCorrupt,
			)
		}
	}
	return nil
}

func selectValidationCandidateID(tx *gorm.DB, tenantID string) (string, error) {
	type identityRecord struct {
		KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
		ObjectIDBytes     int64  `gorm:"column:knowledge_object_id_bytes"`
	}
	tenant, found, err := readCatalogTenantRecord(tx, tenantID)
	if err != nil {
		return "", err
	}
	physical, err := readBoundedKnowledgeObjectCount(tx, tenantID)
	if err != nil {
		return "", err
	}
	if !found || physical > maximumObjectsPerTenant || physical != tenant.IdentityCount {
		return "", fmt.Errorf(
			"%w: validation candidate identity authority is inconsistent",
			ErrCorrupt,
		)
	}
	var records []identityRecord
	if err := tx.Table("knowledge_objects").Select(fmt.Sprintf(`
		CASE WHEN length(CAST(knowledge_object_id AS BLOB)) BETWEEN 1 AND %[1]d
			THEN knowledge_object_id ELSE '' END AS knowledge_object_id,
		length(CAST(knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes`,
		maximumObjectIDBytes,
	)).Where("tenant_id = ?", tenantID).
		Order("knowledge_object_id ASC").
		Limit(int(physical) + 1).
		Find(&records).Error; err != nil {
		return "", err
	}
	if len(records) != int(physical) {
		return "", fmt.Errorf("%w: validation candidate identity inventory exceeds its limit", ErrCorrupt)
	}
	existing := make(map[string]struct{}, len(records))
	for index, record := range records {
		if record.ObjectIDBytes != int64(len(record.KnowledgeObjectID)) ||
			!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) ||
			index > 0 && records[index-1].KnowledgeObjectID >= record.KnowledgeObjectID {
			return "", fmt.Errorf("%w: validation candidate identity inventory is invalid", ErrCorrupt)
		}
		existing[record.KnowledgeObjectID] = struct{}{}
	}
	for ordinal := 0; ordinal <= maximumObjectsPerTenant; ordinal++ {
		candidate := fmt.Sprintf("knowledge-validation-candidate-%04x", ordinal)
		if _, found := existing[candidate]; !found {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%w: no validation candidate identity is available", ErrCorrupt)
}

func projectAuthorizedValidationDependencies(
	ctx context.Context,
	tx *gorm.DB,
	scope normalizedScope,
	dependencies []publicationDependency,
) ([]*opensplunk.KnowledgeValidationDependency, bool, error) {
	if len(dependencies) == 0 {
		return []*opensplunk.KnowledgeValidationDependency{}, true, nil
	}
	if len(dependencies) > maximumDependenciesPerVersion {
		return nil, false, ErrCorrupt
	}
	objectIDs := make([]string, len(dependencies))
	for index, dependency := range dependencies {
		if !validIdentity(dependency.targetObjectID, maximumObjectIDBytes) ||
			dependency.targetVersion < 1 ||
			index > 0 && dependencies[index-1].targetObjectID >= dependency.targetObjectID {
			return nil, false, ErrCorrupt
		}
		objectIDs[index] = dependency.targetObjectID
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	type targetRecord struct {
		KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
		CurrentVersion    int64  `gorm:"column:current_version"`
		State             State  `gorm:"column:state"`
	}
	query := tx.Model(&registryRecord{}).
		Select("knowledge_object_id, current_version, state").
		Where("tenant_id = ? AND knowledge_object_id IN ?", scope.tenantID, objectIDs)
	query = applyPointGetAuthorization(query, scope)
	var records []targetRecord
	if err := query.Order("knowledge_object_id ASC").Limit(len(objectIDs) + 1).Find(&records).Error; err != nil {
		return nil, false, err
	}
	if len(records) != len(dependencies) {
		return nil, false, nil
	}
	result := make([]*opensplunk.KnowledgeValidationDependency, len(records))
	for index, record := range records {
		expected := dependencies[index]
		if record.KnowledgeObjectID != expected.targetObjectID ||
			record.CurrentVersion != expected.targetVersion || record.State != StateActive {
			return nil, false, ErrCorrupt
		}
		result[index] = &opensplunk.KnowledgeValidationDependency{
			Target: &opensplunk.KnowledgeManagementObjectVersionIdentity{
				KnowledgeObjectId: strings.Clone(record.KnowledgeObjectID),
				Version:           safecast.MustConv[uint64](record.CurrentVersion),
			},
			Role: opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}
	}
	if !slices.IsSortedFunc(result, func(left, right *opensplunk.KnowledgeValidationDependency) int {
		return strings.Compare(left.GetTarget().GetKnowledgeObjectId(), right.GetTarget().GetKnowledgeObjectId())
	}) || len(result) != len(dependencies) {
		return nil, false, ErrCorrupt
	}
	return slices.Clip(result), true, nil
}
