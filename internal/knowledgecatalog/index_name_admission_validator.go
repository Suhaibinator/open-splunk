package knowledgecatalog

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

const (
	maximumPublicationIndexAdmissionTenants = MaximumResolutionCandidates
	maximumPublicationIndexAdmissionAppRows = uint64(64 << 10)
)

var (
	publicationIndexAdmissionTenantDriverSQL = fmt.Sprintf(`
		SELECT
			CASE WHEN length(CAST(tenant.tenant_id AS BLOB)) BETWEEN 1 AND %[1]d
				THEN tenant.tenant_id ELSE '' END AS tenant_id,
			length(CAST(tenant.tenant_id AS BLOB)) AS tenant_id_bytes,
			tenant.catalog_revision,
			tenant.active_object_count
		FROM knowledge_catalog_tenants AS tenant
		     INDEXED BY knowledge_catalog_tenants_nonempty_active_idx
		WHERE tenant.active_object_count > 0
		ORDER BY tenant.tenant_id
		LIMIT %[2]d`, maximumTenantIDBytes, maximumPublicationIndexAdmissionTenants+1)

	publicationIndexAdmissionRegistryDriverSQL = fmt.Sprintf(`
		SELECT
			CASE WHEN length(CAST(object.tenant_id AS BLOB)) BETWEEN 1 AND %[1]d
				THEN object.tenant_id ELSE '' END AS tenant_id,
			length(CAST(object.tenant_id AS BLOB)) AS tenant_id_bytes,
			CASE WHEN length(CAST(object.knowledge_object_id AS BLOB)) BETWEEN 1 AND %[2]d
				THEN object.knowledge_object_id ELSE '' END AS knowledge_object_id,
			length(CAST(object.knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
			object.current_version
		FROM knowledge_objects AS object
		     INDEXED BY knowledge_objects_active_tenant_idx
		WHERE object.state = 'active'
		ORDER BY object.tenant_id, object.knowledge_object_id
		LIMIT %[3]d`, maximumTenantIDBytes, maximumObjectIDBytes, MaximumResolutionCandidates+1)
)

// IndexNameAdmissionValidator proves, inside control's caller-owned index
// creation transaction, that one previously unknown immutable index name does
// not change the semantics of any ACTIVE knowledge object in any tenant.
type IndexNameAdmissionValidator struct {
	database *control.DB
}

var _ control.IndexNameAdmissionValidator = (*IndexNameAdmissionValidator)(nil)

// NewIndexNameAdmissionValidator constructs the deployment-global knowledge
// validator over the same control-plane database used by index administration.
func NewIndexNameAdmissionValidator(
	database *control.DB,
) (*IndexNameAdmissionValidator, error) {
	if database == nil || database.GORMDB() == nil || database.SQLDB() == nil {
		return nil, fmt.Errorf(
			"%w: index-name admission control database is required",
			control.ErrInvalidArgument,
		)
	}
	return &IndexNameAdmissionValidator{database: database}, nil
}

type publicationIndexAdmissionTenantDriver struct {
	tenantID          string
	tenantIDBytes     int64
	catalogRevision   int64
	activeObjectCount int64
}

type publicationIndexAdmissionRegistryDriver struct {
	tenantID               string
	tenantIDBytes          int64
	knowledgeObjectID      string
	knowledgeObjectIDBytes int64
	currentVersion         int64
}

type publicationIndexAdmissionAppFacts struct {
	revision int64
	records  []publicationTransitionAppRecord
	active   []string
}

type publicationIndexAdmissionTenantFacts struct {
	driver            publicationIndexAdmissionTenantDriver
	catalog           catalogState
	apps              publicationIndexAdmissionAppFacts
	projectionScalars []publicationTransitionProjectionScalar
	aggregate         publicationTransitionAggregateScalars
	winners           []publicationWinner
	selectorWork      uint64
}

type publicationIndexAdmissionIndexFacts struct {
	names         []string
	revision      int64
	physicalCount uint16
}

type publicationIndexAdmissionScalarBudget struct {
	definitionBytes        uint64
	projectionBytes        uint64
	selectorPatterns       uint64
	selectorValueBytes     uint64
	canonicalSelectorBytes uint64
	dependencies           uint64
	appRows                uint64
	selectorWork           uint64
}

// ValidateIndexNameAdmissionInTransaction implements control's same-*sql.Tx
// validation seam. Every query below is deliberately issued through tx.
func (validator *IndexNameAdmissionValidator) ValidateIndexNameAdmissionInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	request control.IndexNameAdmissionRequest,
) error {
	database, _, err := validator.validateInvocation(ctx, tx, request)
	if err != nil {
		return err
	}

	indexFacts, err := readPublicationIndexAdmissionIndexFacts(database)
	if err != nil {
		return err
	}
	// #nosec G115 -- readPublicationIndexAdmissionIndexFacts validates a positive revision.
	if uint64(indexFacts.revision) != request.IndexCatalogRevision ||
		indexFacts.physicalCount != request.IndexCatalogPhysicalCount {
		return fmt.Errorf(
			"%w: index-name admission request facts are stale",
			control.ErrVersionConflict,
		)
	}
	if err := requirePublicationIndexAdmissionCandidateAbsent(
		database,
		request.CanonicalName,
	); err != nil {
		return err
	}

	tenantDrivers, registryDrivers, err := readPublicationIndexAdmissionDrivers(database)
	if err != nil {
		return err
	}
	if len(tenantDrivers) == 0 {
		return validator.finalPublicationIndexAdmissionCheck(
			ctx,
			database,
			request.CanonicalName,
			indexFacts,
			tenantDrivers,
			registryDrivers,
			nil,
		)
	}

	// Phase one retains every scalar authority for every tenant. No definition,
	// selector, dependency, or semantic-program body is read in this phase.
	tenantFacts := make([]publicationIndexAdmissionTenantFacts, len(tenantDrivers))
	var aggregateBudget publicationIndexAdmissionScalarBudget
	for index, driver := range tenantDrivers {
		if err := ctx.Err(); err != nil {
			return err
		}
		catalog, err := readCatalogState(database, driver.tenantID)
		if err != nil {
			return err
		}
		if !catalog.found || catalog.revision != driver.catalogRevision {
			return fmt.Errorf(
				"%w: index-name admission catalog authority disagrees with its driver",
				ErrCorrupt,
			)
		}
		appRecords, appActive, appRevision, err := readPublicationApps(database, driver.tenantID)
		if err != nil {
			return err
		}
		apps := publicationIndexAdmissionAppFacts{
			revision: appRevision,
			records:  appRecords,
			active:   appActive,
		}
		scalars, aggregate, err := readPublicationTransitionProjectionScalars(
			database,
			driver.tenantID,
			driver.activeObjectCount,
		)
		if err != nil {
			return err
		}
		if !chargePublicationIndexAdmissionScalars(
			&aggregateBudget,
			aggregate,
			uint64(len(apps.records)),
		) {
			return fmt.Errorf(
				"%w: index-name admission aggregate scalar preflight exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
		tenantFacts[index] = publicationIndexAdmissionTenantFacts{
			driver:            driver,
			catalog:           clonePublicationTransitionCatalogState(catalog),
			apps:              apps,
			projectionScalars: scalars,
			aggregate:         aggregate,
		}
	}

	// Phase two hydrates each tenant sequentially on the fixed SQLite
	// transaction. All global scalar ceilings have already been accepted.
	for index := range tenantFacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		facts := &tenantFacts[index]
		projections, err := readProjectionRecordsBounded(
			publicationTransitionActiveProjectionQuery(database, facts.driver.tenantID).
				Order("projection.knowledge_object_id ASC"),
			MaximumResolutionCandidates+1,
			int64(maximumPublicationTransitionProjectionBytes),
		)
		if err != nil {
			return err
		}
		if err := matchPublicationTransitionProjectionPreflight(
			facts.projectionScalars,
			projections,
		); err != nil {
			return err
		}
		objects, authorities, err := objectsFromProjectionsWithAuthorities(
			database,
			projections,
			listHydrationBudget{
				definitionBytes:    int64(maximumPublicationTransitionDefinitionBytes),
				projectionBytes:    int64(maximumPublicationTransitionProjectionBytes),
				selectorPatterns:   int64(maximumPublicationTransitionSelectorPatterns),
				selectorValueBytes: int64(maximumPublicationTransitionSelectorBytes),
				dependencies:       int64(maximumPublicationTransitionDependencies),
			},
		)
		if err != nil {
			return err
		}
		winners, selectorWork, err := publicationTransitionWinnersFromHydration(
			facts.driver.tenantID,
			objects,
			authorities,
		)
		if err != nil {
			return err
		}
		if len(winners) != int(facts.driver.activeObjectCount) {
			return fmt.Errorf(
				"%w: index-name admission hydration disagrees with its driver",
				ErrCorrupt,
			)
		}
		if !addPublicationResource(
			&aggregateBudget.selectorWork,
			selectorWork,
			maximumPublicationTransitionSelectorWork,
		) {
			return fmt.Errorf(
				"%w: index-name admission aggregate selector work exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
		facts.winners = winners
		facts.selectorWork = selectorWork
	}

	// One shared budget bounds semantic work over the deployment-global batch.
	batchBudget := &publicationIndexNameAdmissionBatchBudget{}
	for index := range tenantFacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		facts := &tenantFacts[index]
		// Counts are bounded by the app/index inventory limits before this batch is assembled.
		// #nosec G115 -- both lengths fit in uint16 by those validated limits.
		expectedActiveAppCount := uint16(len(facts.apps.active))
		// #nosec G115 -- the physical-index inventory is bounded below uint16 capacity.
		expectedPotentiallySearchableIndexCount := uint16(len(indexFacts.names))
		// #nosec G115 -- winner hydration is bounded by MaximumResolutionCandidates.
		expectedCurrentActiveCount := uint32(len(facts.winners))
		authority, err := validatePublicationIndexNameAdmissionWithBudget(
			ctx,
			publicationIndexNameAdmissionInventory{
				tenantID:                                strings.Clone(facts.driver.tenantID),
				expectedDefinitionBytes:                 facts.aggregate.definitionBytes,
				expectedProjectionBytes:                 facts.aggregate.projectionBytes,
				expectedSelectorPatterns:                facts.aggregate.selectorPatterns,
				expectedSelectorValueBytes:              facts.aggregate.selectorValueBytes,
				expectedCanonicalSelectorBytes:          facts.aggregate.canonicalSelectorBytes,
				expectedSelectorWork:                    facts.selectorWork,
				expectedDependencyCount:                 facts.aggregate.dependencies,
				expectedActiveAppCount:                  expectedActiveAppCount,
				activeAppIDs:                            facts.apps.active,
				expectedCurrentActiveCount:              expectedCurrentActiveCount,
				currentActive:                           facts.winners,
				expectedPotentiallySearchableIndexCount: expectedPotentiallySearchableIndexCount,
				potentiallySearchableIndexNames:         indexFacts.names,
				newlyPotentiallySearchableIndexName:     request.CanonicalName,
			},
			batchBudget,
		)
		if err != nil {
			return err
		}
		if authority.state == nil || authority.state.tenantID != facts.driver.tenantID ||
			authority.state.indexName != request.CanonicalName {
			return fmt.Errorf(
				"%w: index-name admission semantic authority is invalid",
				ErrCorrupt,
			)
		}
	}

	return validator.finalPublicationIndexAdmissionCheck(
		ctx,
		database,
		request.CanonicalName,
		indexFacts,
		tenantDrivers,
		registryDrivers,
		tenantFacts,
	)
}

func (validator *IndexNameAdmissionValidator) validateInvocation(
	ctx context.Context,
	tx *gorm.DB,
	request control.IndexNameAdmissionRequest,
) (*gorm.DB, *sql.Tx, error) {
	if validator == nil || validator.database == nil ||
		validator.database.GORMDB() == nil || validator.database.SQLDB() == nil {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission validator is uninitialized",
			control.ErrInvalidArgument,
		)
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	canonical, err := control.NormalizeIndexName(request.CanonicalName)
	if err != nil || canonical != request.CanonicalName ||
		request.IndexCatalogRevision < 1 ||
		request.IndexCatalogRevision > math.MaxInt64 ||
		request.IndexCatalogPhysicalCount >= control.MaximumPhysicalIndexRecords {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission request is malformed",
			control.ErrInvalidArgument,
		)
	}
	if tx == nil || tx.Error != nil || tx.Statement == nil || tx.Config == nil ||
		tx.ConnPool != validator.database.SQLDB() {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission requires this control database transaction",
			control.ErrInvalidArgument,
		)
	}
	transaction, transactional := tx.Statement.ConnPool.(*sql.Tx)
	if !transactional || transaction == nil {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission requires one fixed SQL transaction",
			control.ErrInvalidArgument,
		)
	}
	database := tx.WithContext(ctx)
	boundTransaction, bound := database.Statement.ConnPool.(*sql.Tx)
	if !bound || boundTransaction != transaction {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission transaction binding changed",
			control.ErrInvalidArgument,
		)
	}
	return database, transaction, nil
}

func readPublicationIndexAdmissionIndexFacts(
	database *gorm.DB,
) (publicationIndexAdmissionIndexFacts, error) {
	names, revision, physicalCount, err := readPublicationTransitionPotentialIndexes(database)
	if err != nil {
		return publicationIndexAdmissionIndexFacts{}, err
	}
	return publicationIndexAdmissionIndexFacts{
		names:         names,
		revision:      revision,
		physicalCount: physicalCount,
	}, nil
}

func requirePublicationIndexAdmissionCandidateAbsent(
	database *gorm.DB,
	canonicalName string,
) error {
	var records []struct {
		Name      string `gorm:"column:name"`
		NameBytes int64  `gorm:"column:name_bytes"`
	}
	if err := database.Table("indexes AS candidate").Select(`
		candidate.name AS name,
		length(CAST(candidate.name AS BLOB)) AS name_bytes`).
		Where("candidate.name = ?", canonicalName).
		Limit(2).
		Find(&records).Error; err != nil {
		return err
	}
	if len(records) != 0 {
		return fmt.Errorf(
			"%w: index-name admission candidate is already present",
			ErrCorrupt,
		)
	}
	return nil
}

func readPublicationIndexAdmissionDrivers(
	database *gorm.DB,
) ([]publicationIndexAdmissionTenantDriver, []publicationIndexAdmissionRegistryDriver, error) {
	tenantDrivers, totalActive, err := readPublicationIndexAdmissionTenantDrivers(database)
	if err != nil {
		return nil, nil, err
	}
	registryDrivers, err := readPublicationIndexAdmissionRegistryDrivers(database)
	if err != nil {
		return nil, nil, err
	}
	counts := make(map[string]int64, len(tenantDrivers))
	for _, driver := range registryDrivers {
		counts[driver.tenantID]++
	}
	if int64(len(registryDrivers)) != totalActive || len(counts) != len(tenantDrivers) {
		return nil, nil, fmt.Errorf(
			"%w: index-name admission tenant ledger and ACTIVE registry disagree",
			ErrCorrupt,
		)
	}
	for _, driver := range tenantDrivers {
		if counts[driver.tenantID] != driver.activeObjectCount {
			return nil, nil, fmt.Errorf(
				"%w: index-name admission tenant ledger and ACTIVE registry disagree",
				ErrCorrupt,
			)
		}
	}
	return tenantDrivers, registryDrivers, nil
}

func readPublicationIndexAdmissionTenantDrivers(
	database *gorm.DB,
) ([]publicationIndexAdmissionTenantDriver, int64, error) {
	type tenantRecord struct {
		TenantID          string `gorm:"column:tenant_id"`
		TenantIDBytes     int64  `gorm:"column:tenant_id_bytes"`
		CatalogRevision   int64  `gorm:"column:catalog_revision"`
		ActiveObjectCount int64  `gorm:"column:active_object_count"`
	}
	var tenantRecords []tenantRecord
	if err := database.Raw(publicationIndexAdmissionTenantDriverSQL).
		Scan(&tenantRecords).Error; err != nil {
		return nil, 0, err
	}
	if len(tenantRecords) > maximumPublicationIndexAdmissionTenants {
		return nil, 0, fmt.Errorf(
			"%w: index-name admission nonempty tenant inventory exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	tenantDrivers := make([]publicationIndexAdmissionTenantDriver, len(tenantRecords))
	var totalActive int64
	for index, record := range tenantRecords {
		if record.TenantIDBytes != int64(len(record.TenantID)) ||
			!validIdentity(record.TenantID, maximumTenantIDBytes) ||
			record.CatalogRevision < 1 || record.CatalogRevision > math.MaxInt64-1 ||
			record.ActiveObjectCount < 1 ||
			record.ActiveObjectCount > MaximumResolutionCandidates ||
			(index > 0 && tenantRecords[index-1].TenantID >= record.TenantID) {
			return nil, 0, fmt.Errorf(
				"%w: index-name admission nonempty tenant driver is invalid",
				ErrCorrupt,
			)
		}
		if totalActive > MaximumResolutionCandidates-record.ActiveObjectCount {
			return nil, 0, fmt.Errorf(
				"%w: index-name admission ACTIVE inventory exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
		totalActive += record.ActiveObjectCount
		tenantDrivers[index] = publicationIndexAdmissionTenantDriver{
			tenantID:          strings.Clone(record.TenantID),
			tenantIDBytes:     record.TenantIDBytes,
			catalogRevision:   record.CatalogRevision,
			activeObjectCount: record.ActiveObjectCount,
		}
	}
	return tenantDrivers, totalActive, nil
}

func readPublicationIndexAdmissionRegistryDrivers(
	database *gorm.DB,
) ([]publicationIndexAdmissionRegistryDriver, error) {
	type registryRecord struct {
		TenantID               string `gorm:"column:tenant_id"`
		TenantIDBytes          int64  `gorm:"column:tenant_id_bytes"`
		KnowledgeObjectID      string `gorm:"column:knowledge_object_id"`
		KnowledgeObjectIDBytes int64  `gorm:"column:knowledge_object_id_bytes"`
		CurrentVersion         int64  `gorm:"column:current_version"`
	}
	var registryRecords []registryRecord
	if err := database.Raw(publicationIndexAdmissionRegistryDriverSQL).
		Scan(&registryRecords).Error; err != nil {
		return nil, err
	}
	if len(registryRecords) > MaximumResolutionCandidates {
		return nil, fmt.Errorf(
			"%w: index-name admission ACTIVE registry exceeds its limit",
			control.ErrCapacityExceeded,
		)
	}
	registryDrivers := make([]publicationIndexAdmissionRegistryDriver, len(registryRecords))
	for index, record := range registryRecords {
		if record.TenantIDBytes != int64(len(record.TenantID)) ||
			!validIdentity(record.TenantID, maximumTenantIDBytes) ||
			record.KnowledgeObjectIDBytes != int64(len(record.KnowledgeObjectID)) ||
			!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) ||
			record.CurrentVersion < 1 || record.CurrentVersion > maximumVersionsPerTenant ||
			(index > 0 && !publicationIndexAdmissionRegistryAfter(
				registryRecords[index-1].TenantID,
				registryRecords[index-1].KnowledgeObjectID,
				record.TenantID,
				record.KnowledgeObjectID,
			)) {
			return nil, fmt.Errorf(
				"%w: index-name admission ACTIVE registry driver is invalid",
				ErrCorrupt,
			)
		}
		registryDrivers[index] = publicationIndexAdmissionRegistryDriver{
			tenantID:               strings.Clone(record.TenantID),
			tenantIDBytes:          record.TenantIDBytes,
			knowledgeObjectID:      strings.Clone(record.KnowledgeObjectID),
			knowledgeObjectIDBytes: record.KnowledgeObjectIDBytes,
			currentVersion:         record.CurrentVersion,
		}
	}
	return registryDrivers, nil
}

func publicationIndexAdmissionRegistryAfter(
	previousTenant, previousObject, currentTenant, currentObject string,
) bool {
	return previousTenant < currentTenant ||
		previousTenant == currentTenant && previousObject < currentObject
}

func chargePublicationIndexAdmissionScalars(
	total *publicationIndexAdmissionScalarBudget,
	current publicationTransitionAggregateScalars,
	appRows uint64,
) bool {
	return total != nil &&
		addPublicationResource(&total.definitionBytes, current.definitionBytes, maximumPublicationTransitionDefinitionBytes) &&
		addPublicationResource(&total.projectionBytes, current.projectionBytes, maximumPublicationTransitionProjectionBytes) &&
		addPublicationResource(&total.selectorPatterns, current.selectorPatterns, maximumPublicationTransitionSelectorPatterns) &&
		addPublicationResource(&total.selectorValueBytes, current.selectorValueBytes, maximumPublicationTransitionSelectorBytes) &&
		addPublicationResource(&total.canonicalSelectorBytes, current.canonicalSelectorBytes, maximumPublicationTransitionSelectorBytes) &&
		addPublicationResource(&total.dependencies, current.dependencies, maximumPublicationTransitionDependencies) &&
		addPublicationResource(&total.appRows, appRows, maximumPublicationIndexAdmissionAppRows)
}

func (validator *IndexNameAdmissionValidator) finalPublicationIndexAdmissionCheck(
	ctx context.Context,
	database *gorm.DB,
	canonicalName string,
	boundIndexes publicationIndexAdmissionIndexFacts,
	boundTenants []publicationIndexAdmissionTenantDriver,
	boundRegistry []publicationIndexAdmissionRegistryDriver,
	boundFacts []publicationIndexAdmissionTenantFacts,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	currentIndexes, err := readPublicationIndexAdmissionIndexFacts(database)
	if err != nil {
		return err
	}
	if err := matchPublicationIndexAdmissionIndexFacts(boundIndexes, currentIndexes); err != nil {
		return err
	}
	if err := requirePublicationIndexAdmissionCandidateAbsent(database, canonicalName); err != nil {
		return err
	}

	for _, bound := range boundFacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		catalog, err := readCatalogState(database, bound.driver.tenantID)
		if err != nil {
			return err
		}
		if err := matchPublicationIndexAdmissionCatalogFact(bound.catalog, catalog); err != nil {
			return err
		}
		appRecords, appActive, appRevision, err := readPublicationApps(database, bound.driver.tenantID)
		if err != nil {
			return err
		}
		apps := publicationIndexAdmissionAppFacts{
			revision: appRevision,
			records:  appRecords,
			active:   appActive,
		}
		if err := matchPublicationIndexAdmissionAppFacts(bound.apps, apps); err != nil {
			return err
		}
		scalars, aggregate, err := readPublicationTransitionProjectionScalars(
			database,
			bound.driver.tenantID,
			bound.driver.activeObjectCount,
		)
		if err != nil {
			return err
		}
		if !slices.Equal(scalars, bound.projectionScalars) || aggregate != bound.aggregate {
			return fmt.Errorf(
				"%w: index-name admission projection facts changed without catalog advancement",
				ErrCorrupt,
			)
		}
	}

	currentTenants, currentRegistry, err := readPublicationIndexAdmissionDrivers(database)
	if err != nil {
		return err
	}
	if !slices.Equal(currentTenants, boundTenants) ||
		!slices.Equal(currentRegistry, boundRegistry) {
		return fmt.Errorf(
			"%w: index-name admission ACTIVE inventory changed",
			control.ErrVersionConflict,
		)
	}
	return ctx.Err()
}

func matchPublicationIndexAdmissionIndexFacts(
	bound, current publicationIndexAdmissionIndexFacts,
) error {
	switch {
	case current.revision < bound.revision:
		return fmt.Errorf("%w: index-name admission index revision regressed", ErrCorrupt)
	case current.revision == bound.revision:
		if current.physicalCount != bound.physicalCount ||
			!slices.Equal(current.names, bound.names) {
			return fmt.Errorf(
				"%w: index-name admission index facts changed without revision advancement",
				ErrCorrupt,
			)
		}
		return nil
	default:
		if current.physicalCount < bound.physicalCount ||
			int64(current.physicalCount-bound.physicalCount) > current.revision-bound.revision {
			return fmt.Errorf("%w: index-name admission index accounting is invalid", ErrCorrupt)
		}
		return fmt.Errorf(
			"%w: index-name admission index facts advanced",
			control.ErrVersionConflict,
		)
	}
}

func matchPublicationIndexAdmissionCatalogFact(bound, current catalogState) error {
	if !current.found {
		return fmt.Errorf("%w: index-name admission catalog authority disappeared", ErrCorrupt)
	}
	switch {
	case current.revision < bound.revision:
		return fmt.Errorf("%w: index-name admission catalog revision regressed", ErrCorrupt)
	case current.revision == bound.revision:
		if current.token != bound.token {
			return fmt.Errorf(
				"%w: index-name admission catalog token changed without revision advancement",
				ErrCorrupt,
			)
		}
		return nil
	default:
		if current.token == bound.token {
			return fmt.Errorf("%w: index-name admission catalog token was reused", ErrCorrupt)
		}
		return fmt.Errorf(
			"%w: index-name admission catalog facts advanced",
			control.ErrVersionConflict,
		)
	}
}

func matchPublicationIndexAdmissionAppFacts(
	bound, current publicationIndexAdmissionAppFacts,
) error {
	switch {
	case current.revision < bound.revision:
		return fmt.Errorf("%w: index-name admission app revision regressed", ErrCorrupt)
	case current.revision > bound.revision:
		return fmt.Errorf(
			"%w: index-name admission app facts advanced",
			control.ErrVersionConflict,
		)
	default:
		if !slices.Equal(current.records, bound.records) ||
			!slices.Equal(current.active, bound.active) {
			return fmt.Errorf(
				"%w: index-name admission app facts changed without revision advancement",
				ErrCorrupt,
			)
		}
		return nil
	}
}
