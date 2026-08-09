package knowledgecatalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"gorm.io/gorm"
)

// publicationTransitionPersistenceAuthority is an opaque, transaction-bound
// capability. It retains only the validated transition and a separately
// detached database projection; callers cannot recover its revision facts or
// richer dependency authority.
type publicationTransitionPersistenceAuthority struct {
	state *publicationTransitionPersistenceAuthorityState
}

type publicationTransitionPersistenceAuthorityState struct {
	transaction               *sql.Tx
	tenantID                  string
	catalog                   catalogState
	appCatalogRevision        int64
	indexCatalogRevision      int64
	indexCatalogPhysicalCount uint16
	transition                publicationActiveTransitionAuthority
	validatedProjection       []publicationDependency
}

type publicationTransitionPersistenceFacts struct {
	catalog                   catalogState
	appCatalogRevision        int64
	indexCatalogRevision      int64
	indexCatalogPhysicalCount uint16
}

func (authority publicationTransitionPersistenceAuthority) IsZero() bool {
	return authority.state == nil
}

// mintPublicationTransitionPersistenceAuthority is the sole conversion from
// a complete transactional inventory read to persistence authority. The rich
// target check runs while its exact source transaction is still current, and
// the retained projection is cloned independently from both validators.
func mintPublicationTransitionPersistenceAuthority(
	ctx context.Context,
	tx *gorm.DB,
	read publicationActiveTransitionInventoryRead,
) (publicationTransitionPersistenceAuthority, error) {
	if ctx == nil {
		return publicationTransitionPersistenceAuthority{}, fmt.Errorf(
			"%w: publication transition persistence context is nil",
			control.ErrInvalidArgument,
		)
	}
	transaction, err := publicationTransitionSQLTransaction(tx)
	if err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}
	if read.transaction == nil ||
		!validIdentity(read.inventory.tenantID, maximumTenantIDBytes) ||
		!validPublicationTransitionPersistenceReadFacts(read) {
		return publicationTransitionPersistenceAuthority{}, fmt.Errorf(
			"%w: publication transition inventory read authority is malformed",
			control.ErrInvalidArgument,
		)
	}
	if transaction != read.transaction {
		return publicationTransitionPersistenceAuthority{}, fmt.Errorf(
			"%w: publication transition inventory belongs to another transaction",
			control.ErrVersionConflict,
		)
	}
	if err := ctx.Err(); err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}

	boundFacts := publicationTransitionPersistenceFactsFromRead(read)
	currentFacts, err := readPublicationTransitionPersistenceFacts(
		ctx,
		tx,
		read.inventory.tenantID,
	)
	if err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}
	if err := matchPublicationTransitionPersistenceFacts(boundFacts, currentFacts); err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}

	transition, err := validatePublicationActiveTransition(ctx, read.inventory)
	if err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}
	if transition.IsZero() || transition.state == nil {
		return publicationTransitionPersistenceAuthority{}, fmt.Errorf(
			"%w: publication transition validator returned no authority",
			ErrCorrupt,
		)
	}

	candidateDependencies := transition.candidateDependencies()
	validatedProjection, err := validatePublicationTransitionAuthorityProjection(
		ctx,
		tx,
		read.inventory.tenantID,
		transition,
		candidateDependencies,
	)
	if err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}
	retainedProjection := clonePublicationTransitionProjection(validatedProjection)
	if err := ctx.Err(); err != nil {
		return publicationTransitionPersistenceAuthority{}, err
	}

	return publicationTransitionPersistenceAuthority{
		state: &publicationTransitionPersistenceAuthorityState{
			transaction:               transaction,
			tenantID:                  strings.Clone(read.inventory.tenantID),
			catalog:                   clonePublicationTransitionCatalogState(read.catalog),
			appCatalogRevision:        read.appCatalogRevision,
			indexCatalogRevision:      read.indexCatalogRevision,
			indexCatalogPhysicalCount: read.indexCatalogPhysicalCount,
			transition:                transition,
			validatedProjection:       retainedProjection,
		},
	}, nil
}

// validateAndProject is the only Writer-facing use of the opaque authority.
// It rechecks the three revision domains in the originating transaction,
// matches the exact persistence plan, and returns a fresh bounded projection.
func (authority publicationTransitionPersistenceAuthority) validateAndProject(
	ctx context.Context,
	tx *gorm.DB,
	oldCatalogState catalogState,
	tenantID string,
	plan publicationPlan,
	dependencies []publicationDependency,
) ([]publicationDependency, error) {
	if ctx == nil || authority.state == nil {
		return nil, fmt.Errorf(
			"%w: publication transition persistence authority is absent",
			control.ErrInvalidArgument,
		)
	}
	transaction, err := publicationTransitionSQLTransaction(tx)
	if err != nil {
		return nil, err
	}
	if transaction != authority.state.transaction {
		return nil, fmt.Errorf(
			"%w: publication transition persistence transaction changed",
			control.ErrVersionConflict,
		)
	}
	if !validPublicationTransitionCatalogState(oldCatalogState) ||
		!validIdentity(tenantID, maximumTenantIDBytes) ||
		!validPublicationTransitionDependencyProjection(dependencies) ||
		plan.state == StateActive && len(dependencies) != 0 {
		return nil, fmt.Errorf(
			"%w: publication transition persistence input is malformed",
			control.ErrInvalidArgument,
		)
	}
	if tenantID != authority.state.tenantID {
		return nil, fmt.Errorf(
			"%w: publication transition persistence tenant disagrees",
			control.ErrDependencyConflict,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	currentFacts, err := readPublicationTransitionPersistenceFacts(
		ctx,
		tx,
		authority.state.tenantID,
	)
	if err != nil {
		return nil, err
	}
	boundFacts := publicationTransitionPersistenceFacts{
		catalog:                   authority.state.catalog,
		appCatalogRevision:        authority.state.appCatalogRevision,
		indexCatalogRevision:      authority.state.indexCatalogRevision,
		indexCatalogPhysicalCount: authority.state.indexCatalogPhysicalCount,
	}
	if err := matchPublicationTransitionPersistenceFacts(boundFacts, currentFacts); err != nil {
		return nil, err
	}
	if !oldCatalogState.equal(authority.state.catalog) {
		return nil, fmt.Errorf(
			"%w: publication transition catalog plan is stale",
			control.ErrVersionConflict,
		)
	}
	binding, err := publicationTransitionPersistenceBindingFromPlan(
		ctx,
		tx.WithContext(ctx),
		tenantID,
		plan,
		dependencies,
	)
	if err != nil {
		return nil, err
	}
	if !validPublicationTransitionPersistenceBinding(binding) {
		return nil, fmt.Errorf(
			"%w: publication transition persistence binding is malformed",
			control.ErrInvalidArgument,
		)
	}
	matchedBinding := binding
	if binding.after.state == StateActive {
		if len(binding.dependencies) != 0 {
			return nil, fmt.Errorf(
				"%w: ACTIVE publication plan supplies dependency rows",
				control.ErrInvalidArgument,
			)
		}
		matchedBinding.dependencies = clonePublicationTransitionProjection(
			authority.state.validatedProjection,
		)
	} else if !slices.Equal(authority.state.validatedProjection, binding.dependencies) {
		return nil, fmt.Errorf(
			"%w: publication transition persistence binding disagrees",
			control.ErrDependencyConflict,
		)
	}
	if !authority.state.transition.matchesPersistence(matchedBinding) {
		return nil, fmt.Errorf(
			"%w: publication transition persistence binding disagrees",
			control.ErrDependencyConflict,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return clonePublicationTransitionProjection(authority.state.validatedProjection), nil
}

func validatePublicationTransitionAuthorityProjection(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	transition publicationActiveTransitionAuthority,
	candidateDependencies candidateDependencyAuthority,
) ([]publicationDependency, error) {
	if transition.state == nil {
		return nil, fmt.Errorf(
			"%w: publication transition authority is absent",
			ErrCorrupt,
		)
	}
	after := transition.state.afterPersistence
	if after.state == StateActive {
		if candidateDependencies.IsZero() {
			return nil, fmt.Errorf(
				"%w: ACTIVE publication transition has no dependency authority",
				ErrCorrupt,
			)
		}
		return validatePublicationTransitionPersistenceTargets(
			ctx,
			tx.WithContext(ctx),
			tenantID,
			candidateDependencies.candidateAuthority(),
			candidateDependencies.sourceStage(),
			candidateDependencies,
		)
	}
	if !candidateDependencies.IsZero() {
		return nil, fmt.Errorf(
			"%w: inactive publication transition retained candidate dependency authority",
			ErrCorrupt,
		)
	}
	if !after.existingDependenciesPresent ||
		len(after.existingDependencies) > maximumDependenciesPerVersion {
		return nil, fmt.Errorf(
			"%w: inactive publication transition dependency projection is invalid",
			ErrCorrupt,
		)
	}
	projection := make([]publicationDependency, len(after.existingDependencies))
	for index, dependency := range after.existingDependencies {
		projection[index] = publicationDependency{
			targetObjectID: strings.Clone(dependency.targetObjectID),
			targetVersion:  dependency.targetVersion,
		}
	}
	return projection[:len(projection):len(projection)], nil
}

func publicationTransitionPersistenceFactsFromRead(
	read publicationActiveTransitionInventoryRead,
) publicationTransitionPersistenceFacts {
	return publicationTransitionPersistenceFacts{
		catalog:                   read.catalog,
		appCatalogRevision:        read.appCatalogRevision,
		indexCatalogRevision:      read.indexCatalogRevision,
		indexCatalogPhysicalCount: read.indexCatalogPhysicalCount,
	}
}

func readPublicationTransitionPersistenceFacts(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
) (publicationTransitionPersistenceFacts, error) {
	if ctx == nil || tx == nil || !validIdentity(tenantID, maximumTenantIDBytes) {
		return publicationTransitionPersistenceFacts{}, fmt.Errorf(
			"%w: publication transition fact read input is malformed",
			control.ErrInvalidArgument,
		)
	}
	database := tx.WithContext(ctx)
	catalog, err := readCatalogState(database, tenantID)
	if err != nil {
		return publicationTransitionPersistenceFacts{}, err
	}
	if !catalog.found {
		return publicationTransitionPersistenceFacts{}, fmt.Errorf(
			"%w: publication transition catalog authority is missing",
			ErrCorrupt,
		)
	}

	type appRevisionRecord struct {
		TenantID      string `gorm:"column:tenant_id"`
		TenantIDBytes int64  `gorm:"column:tenant_id_bytes"`
		Revision      int64  `gorm:"column:revision"`
	}
	var appRevisions []appRevisionRecord
	if err := database.Table("app_catalog_revisions AS authority").Select(`
		authority.tenant_id AS tenant_id,
		length(CAST(authority.tenant_id AS BLOB)) AS tenant_id_bytes,
		authority.revision AS revision`).
		Where("authority.tenant_id = ?", tenantID).
		Limit(2).
		Find(&appRevisions).Error; err != nil {
		return publicationTransitionPersistenceFacts{}, err
	}
	if len(appRevisions) != 1 || appRevisions[0].TenantID != tenantID ||
		appRevisions[0].TenantIDBytes != int64(len(tenantID)) ||
		appRevisions[0].Revision < 1 || appRevisions[0].Revision > math.MaxInt64 {
		return publicationTransitionPersistenceFacts{}, fmt.Errorf(
			"%w: publication transition app-catalog authority is missing or malformed",
			ErrCorrupt,
		)
	}

	type indexStateRecord struct {
		SingletonID   int64 `gorm:"column:singleton_id"`
		Revision      int64 `gorm:"column:revision"`
		PhysicalCount int64 `gorm:"column:physical_count"`
	}
	var indexStates []indexStateRecord
	if err := database.Table("index_catalog_state AS authority").
		Select("authority.singleton_id, authority.revision, authority.physical_count").
		Where("authority.singleton_id = ?", 1).
		Limit(2).
		Find(&indexStates).Error; err != nil {
		return publicationTransitionPersistenceFacts{}, err
	}
	if len(indexStates) != 1 || indexStates[0].SingletonID != 1 ||
		indexStates[0].Revision < 1 || indexStates[0].Revision > math.MaxInt64 ||
		indexStates[0].PhysicalCount < 0 ||
		indexStates[0].PhysicalCount > maximumPublicationIndexAtoms {
		return publicationTransitionPersistenceFacts{}, fmt.Errorf(
			"%w: publication transition index-catalog authority is missing or malformed",
			ErrCorrupt,
		)
	}
	if err := ctx.Err(); err != nil {
		return publicationTransitionPersistenceFacts{}, err
	}
	return publicationTransitionPersistenceFacts{
		catalog:                   catalog,
		appCatalogRevision:        appRevisions[0].Revision,
		indexCatalogRevision:      indexStates[0].Revision,
		indexCatalogPhysicalCount: uint16(indexStates[0].PhysicalCount),
	}, nil
}

func matchPublicationTransitionPersistenceFacts(
	bound publicationTransitionPersistenceFacts,
	current publicationTransitionPersistenceFacts,
) error {
	stale := false
	switch {
	case current.catalog.revision < bound.catalog.revision:
		return fmt.Errorf("%w: publication transition catalog revision regressed", ErrCorrupt)
	case current.catalog.revision == bound.catalog.revision &&
		current.catalog.token != bound.catalog.token:
		return fmt.Errorf("%w: publication transition catalog token changed without revision", ErrCorrupt)
	case current.catalog.revision > bound.catalog.revision:
		if current.catalog.token == bound.catalog.token {
			return fmt.Errorf("%w: publication transition catalog revision reused its token", ErrCorrupt)
		}
		stale = true
	}

	switch {
	case current.appCatalogRevision < bound.appCatalogRevision:
		return fmt.Errorf("%w: publication transition app-catalog revision regressed", ErrCorrupt)
	case current.appCatalogRevision > bound.appCatalogRevision:
		stale = true
	}

	switch {
	case current.indexCatalogRevision < bound.indexCatalogRevision:
		return fmt.Errorf("%w: publication transition index-catalog revision regressed", ErrCorrupt)
	case current.indexCatalogRevision == bound.indexCatalogRevision:
		if current.indexCatalogPhysicalCount != bound.indexCatalogPhysicalCount {
			return fmt.Errorf("%w: publication transition index count changed without revision", ErrCorrupt)
		}
	default:
		revisionDelta := current.indexCatalogRevision - bound.indexCatalogRevision
		if current.indexCatalogPhysicalCount < bound.indexCatalogPhysicalCount {
			return fmt.Errorf("%w: publication transition index count regressed", ErrCorrupt)
		}
		countDelta := int64(current.indexCatalogPhysicalCount -
			bound.indexCatalogPhysicalCount)
		if countDelta > revisionDelta {
			return fmt.Errorf("%w: publication transition index count outran its revision", ErrCorrupt)
		}
		stale = true
	}
	if stale {
		return fmt.Errorf(
			"%w: publication transition catalog facts advanced",
			control.ErrVersionConflict,
		)
	}
	return nil
}

func validPublicationTransitionPersistenceReadFacts(
	read publicationActiveTransitionInventoryRead,
) bool {
	return validPublicationTransitionCatalogState(read.catalog) &&
		read.appCatalogRevision >= 1 && read.appCatalogRevision <= math.MaxInt64 &&
		read.indexCatalogRevision >= 1 && read.indexCatalogRevision <= math.MaxInt64 &&
		uint64(read.indexCatalogPhysicalCount) <= maximumPublicationIndexAtoms
}

func validPublicationTransitionCatalogState(state catalogState) bool {
	if !state.found || state.revision < 0 || state.revision > math.MaxInt64-1 {
		return false
	}
	_, err := catalogStateTokenValue(state)
	return err == nil
}

func validPublicationTransitionPersistenceBinding(
	binding publicationTransitionPersistenceBinding,
) bool {
	if !validIdentity(binding.tenantID, maximumTenantIDBytes) ||
		!validPublicationTransitionPersistenceEndpoint(binding.before) ||
		!validPublicationTransitionPersistenceEndpoint(binding.after) ||
		len(binding.dependencies) > maximumDependenciesPerVersion {
		return false
	}
	if !binding.after.present ||
		(binding.before.present && !binding.before.existingDependenciesPresent) ||
		(binding.after.state == StateActive &&
			(binding.after.existingDependenciesPresent ||
				binding.after.existingDependencies != nil)) ||
		(binding.after.state != StateActive &&
			!binding.after.existingDependenciesPresent) {
		return false
	}
	return validPublicationTransitionDependencyProjection(binding.dependencies)
}

func validPublicationTransitionDependencyProjection(dependencies []publicationDependency) bool {
	if len(dependencies) > maximumDependenciesPerVersion {
		return false
	}
	for index, dependency := range dependencies {
		if !validIdentity(dependency.targetObjectID, maximumObjectIDBytes) ||
			dependency.targetVersion < 1 || dependency.targetVersion > math.MaxInt64 ||
			(index > 0 && !publicationDependencyAfter(dependencies[index-1], dependency)) {
			return false
		}
	}
	return true
}

func validPublicationTransitionPersistenceEndpoint(
	endpoint publicationTransitionPersistenceEndpoint,
) bool {
	if !endpoint.present {
		return endpoint.state == "" && endpoint.objectID == "" && endpoint.version == 0 &&
			endpoint.objectType == 0 && endpoint.name == "" && endpoint.appID == "" &&
			endpoint.ownerID == "" && endpoint.sharingScope == 0 &&
			endpoint.definitionDigest == ([sha256.Size]byte{}) &&
			!endpoint.existingDependenciesPresent && endpoint.existingDependencies == nil
	}
	canonicalName, nameErr := knowledge.NormalizeName(endpoint.name)
	_, typeValid := objectTypeFromProto(endpoint.objectType)
	_, scopeValid := sharingScopeFromProto(endpoint.sharingScope)
	_, stateValid := stateToProto(endpoint.state)
	if !validIdentity(endpoint.objectID, maximumObjectIDBytes) ||
		endpoint.version < 1 || endpoint.version > math.MaxInt64 ||
		!typeValid || nameErr != nil || canonicalName.String() != endpoint.name ||
		!control.ValidCanonicalAppID(endpoint.appID) ||
		!validIdentity(endpoint.ownerID, maximumOwnerIDBytes) || !scopeValid || !stateValid ||
		endpoint.definitionDigest == ([sha256.Size]byte{}) ||
		!endpoint.existingDependenciesPresent && endpoint.existingDependencies != nil {
		return false
	}
	if endpoint.existingDependenciesPresent {
		return validatePersistedPublicationDependencyScalars(endpoint.existingDependencies) == nil
	}
	return true
}

func publicationDependencyAfter(previous, current publicationDependency) bool {
	if previous.targetObjectID != current.targetObjectID {
		return previous.targetObjectID < current.targetObjectID
	}
	return previous.targetVersion < current.targetVersion
}

func clonePublicationTransitionCatalogState(state catalogState) catalogState {
	return catalogState{
		revision: state.revision,
		token:    strings.Clone(state.token),
		found:    state.found,
	}
}

func clonePublicationTransitionProjection(
	projection []publicationDependency,
) []publicationDependency {
	result := make([]publicationDependency, len(projection))
	for index, dependency := range projection {
		result[index] = publicationDependency{
			targetObjectID: strings.Clone(dependency.targetObjectID),
			targetVersion:  dependency.targetVersion,
		}
	}
	return result[:len(result):len(result)]
}

func publicationTransitionSQLTransaction(tx *gorm.DB) (*sql.Tx, error) {
	if tx == nil || tx.Statement == nil {
		return nil, fmt.Errorf(
			"%w: publication transition persistence requires a transaction",
			control.ErrInvalidArgument,
		)
	}
	transaction, transactional := tx.Statement.ConnPool.(*sql.Tx)
	if !transactional || transaction == nil {
		return nil, fmt.Errorf(
			"%w: publication transition persistence requires one fixed transaction snapshot",
			control.ErrInvalidArgument,
		)
	}
	return transaction, nil
}
