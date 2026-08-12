package knowledgecatalog

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"gorm.io/gorm"
)

// publicationActiveTransitionInventoryRead carries an inventory alongside the
// catalog facts and exact SQL transaction that observed it. It is not itself
// persistence authority: only the private persistence-authority mint may
// validate and retain these facts.
type publicationActiveTransitionInventoryRead struct {
	transaction               *sql.Tx
	inventory                 publicationActiveTransitionInventory
	catalog                   catalogState
	appCatalogRevision        int64
	indexCatalogRevision      int64
	indexCatalogPhysicalCount uint16
}

type publicationTransitionProjectionScalar struct {
	knowledgeObjectID      string
	definitionBytes        uint64
	projectionBytes        uint64
	selectorPatterns       uint64
	selectorValueBytes     uint64
	canonicalSelectorBytes uint64
	dependencyCount        uint64
}

type publicationTransitionAggregateScalars struct {
	definitionBytes        uint64
	projectionBytes        uint64
	selectorPatterns       uint64
	selectorValueBytes     uint64
	canonicalSelectorBytes uint64
	dependencies           uint64
}

type publicationTransitionAppRecord struct {
	AppID         string `gorm:"column:app_id"`
	AppIDBytes    int64  `gorm:"column:app_id_bytes"`
	TenantID      string `gorm:"column:tenant_id"`
	TenantIDBytes int64  `gorm:"column:tenant_id_bytes"`
	State         string `gorm:"column:state"`
	StateBytes    int64  `gorm:"column:state_bytes"`
}

type publicationTransitionIndexRecord struct {
	IndexID                 string `gorm:"column:index_id"`
	IndexIDBytes            int64  `gorm:"column:index_id_bytes"`
	Name                    string `gorm:"column:name"`
	NameBytes               int64  `gorm:"column:name_bytes"`
	Version                 int64  `gorm:"column:version"`
	State                   string `gorm:"column:state"`
	StateBytes              int64  `gorm:"column:state_bytes"`
	TombstonePresent        int64  `gorm:"column:tombstone_present"`
	TombstoneName           string `gorm:"column:tombstone_name"`
	TombstoneNameBytes      int64  `gorm:"column:tombstone_name_bytes"`
	TombstoneDeletedVersion int64  `gorm:"column:tombstone_deleted_version"`
}

// readPublicationActiveTransitionInventory reads through tx but never begins,
// commits, or rolls it back. Its caller must supply the already-open IMMEDIATE
// Writer transaction. Candidate endpoints remain caller-owned because binding
// them to an exact persistence plan belongs at the future write boundary.
func (store *Store) readPublicationActiveTransitionInventory(
	tx *gorm.DB,
	tenantID string,
	before publicationTransitionEndpoint,
	after publicationTransitionEndpoint,
) (publicationActiveTransitionInventoryRead, error) {
	if store == nil || !validIdentity(tenantID, maximumTenantIDBytes) {
		return publicationActiveTransitionInventoryRead{}, fmt.Errorf(
			"%w: publication transition inventory reader input is invalid",
			control.ErrInvalidArgument,
		)
	}
	if tx == nil || tx.Statement == nil {
		return publicationActiveTransitionInventoryRead{}, fmt.Errorf(
			"%w: publication transition inventory requires a transaction",
			control.ErrInvalidArgument,
		)
	}
	transaction, transactional := tx.Statement.ConnPool.(*sql.Tx)
	if !transactional || transaction == nil {
		return publicationActiveTransitionInventoryRead{}, fmt.Errorf(
			"%w: publication transition inventory requires one fixed transaction snapshot",
			control.ErrInvalidArgument,
		)
	}
	if err := listDatabaseContextError(tx); err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}

	catalog, activeCount, err := readPublicationTransitionCatalogFacts(tx, tenantID)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}
	projectionScalars, aggregate, err := readPublicationTransitionProjectionScalars(
		tx,
		tenantID,
		activeCount,
	)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}
	activeAppIDs, appRevision, err := readPublicationTransitionActiveApps(tx, tenantID)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}
	indexNames, indexRevision, indexPhysicalCount, err :=
		readPublicationTransitionPotentialIndexes(tx)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}

	projections, err := readProjectionRecordsBounded(
		publicationTransitionActiveProjectionQuery(tx, tenantID).
			Order("projection.knowledge_object_id ASC"),
		MaximumResolutionCandidates+1,
		int64(maximumPublicationTransitionProjectionBytes),
	)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}
	if err := matchPublicationTransitionProjectionPreflight(projectionScalars, projections); err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}

	objects, authorities, err := store.objectsFromProjectionsWithAuthorities(
		tx,
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
		return publicationActiveTransitionInventoryRead{}, err
	}
	winners, selectorWork, err := publicationTransitionWinnersFromHydration(
		tenantID,
		objects,
		authorities,
	)
	if err != nil {
		return publicationActiveTransitionInventoryRead{}, err
	}
	// #nosec G115 -- the app inventory is bounded by maximumReadableApps.
	expectedActiveAppCount := uint16(len(activeAppIDs))
	// #nosec G115 -- the index inventory is bounded by maximumPublicationIndexAtoms.
	expectedPotentiallySearchableIndexCount := uint16(len(indexNames))
	// #nosec G115 -- hydrated winners are bounded by MaximumResolutionCandidates.
	expectedCurrentActiveCount := uint32(len(winners))

	inventory := publicationActiveTransitionInventory{
		tenantID:                                strings.Clone(tenantID),
		expectedDefinitionBytes:                 aggregate.definitionBytes,
		expectedProjectionBytes:                 aggregate.projectionBytes,
		expectedSelectorPatterns:                aggregate.selectorPatterns,
		expectedSelectorValueBytes:              aggregate.selectorValueBytes,
		expectedCanonicalSelectorBytes:          aggregate.canonicalSelectorBytes,
		expectedSelectorWork:                    selectorWork,
		expectedDependencyCount:                 aggregate.dependencies,
		expectedActiveAppCount:                  expectedActiveAppCount,
		activeAppIDs:                            activeAppIDs,
		expectedCurrentActiveCount:              expectedCurrentActiveCount,
		currentActive:                           winners,
		candidateBefore:                         before,
		candidateAfter:                          after,
		expectedPotentiallySearchableIndexCount: expectedPotentiallySearchableIndexCount,
		potentiallySearchableIndexNames:         indexNames,
	}
	return publicationActiveTransitionInventoryRead{
		transaction:               transaction,
		inventory:                 inventory,
		catalog:                   catalog,
		appCatalogRevision:        appRevision,
		indexCatalogRevision:      indexRevision,
		indexCatalogPhysicalCount: indexPhysicalCount,
	}, nil
}

// publicationTransitionActiveProjectionQuery materializes at most 4,097 exact
// ACTIVE registry/projection identities before the wider projection joins and
// payload ordering. The tenant ledger was already checked before this query.
func publicationTransitionActiveProjectionQuery(tx *gorm.DB, tenantID string) *gorm.DB {
	const exactProjectionJoin = `candidate.tenant_id = active_registry.tenant_id
		AND candidate.knowledge_object_id = active_registry.knowledge_object_id
		AND candidate.object_version = active_registry.current_version
		AND candidate.app_id = active_registry.app_id
		AND candidate.owner_id = active_registry.owner_id
		AND candidate.object_type = active_registry.object_type
		AND candidate.name = active_registry.name
		AND candidate.sharing_scope = active_registry.sharing_scope
		AND candidate.state = active_registry.state`
	driver := fmt.Sprintf(`(
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS active_registry INDEXED BY knowledge_objects_resolution_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE active_registry.tenant_id = ? AND active_registry.state = 'active'
		LIMIT %d
	) AS publication_active_projection
	CROSS JOIN knowledge_object_list_projections AS projection`,
		exactProjectionJoin,
		MaximumResolutionCandidates+1,
	)
	return baseProjectionQuery(tx).Table(driver, tenantID).
		Where(`publication_active_projection.tenant_id = projection.tenant_id
			AND publication_active_projection.knowledge_object_id = projection.knowledge_object_id
			AND publication_active_projection.object_version = projection.object_version`).
		Where("projection.tenant_id = ? AND projection.state = ?", tenantID, StateActive)
}

func readPublicationTransitionCatalogFacts(
	tx *gorm.DB,
	tenantID string,
) (catalogState, int64, error) {
	catalog, err := readCatalogState(tx, tenantID)
	if err != nil {
		return catalogState{}, 0, err
	}
	if !catalog.found {
		return catalogState{}, 0, fmt.Errorf(
			"%w: publication transition catalog authority is missing",
			ErrCorrupt,
		)
	}
	var ledgers []struct {
		CatalogRevision   int64 `gorm:"column:catalog_revision"`
		ActiveObjectCount int64 `gorm:"column:active_object_count"`
	}
	if err := tx.Table("knowledge_catalog_tenants").
		Select("catalog_revision, active_object_count").
		Where("tenant_id = ?", tenantID).
		Limit(2).
		Find(&ledgers).Error; err != nil {
		return catalogState{}, 0, err
	}
	if len(ledgers) != 1 || ledgers[0].CatalogRevision != catalog.revision ||
		ledgers[0].ActiveObjectCount < 0 ||
		ledgers[0].ActiveObjectCount > MaximumResolutionCandidates {
		return catalogState{}, 0, fmt.Errorf(
			"%w: publication transition tenant ledger is invalid",
			ErrCorrupt,
		)
	}

	bounded := tx.Table("knowledge_objects AS object INDEXED BY knowledge_objects_resolution_idx").
		Select("1").
		Where("object.tenant_id = ? AND object.state = ?", tenantID, StateActive).
		Limit(MaximumResolutionCandidates + 1)
	var physicalActive int64
	if err := tx.Table("(?) AS bounded_publication_active", bounded).
		Count(&physicalActive).Error; err != nil {
		return catalogState{}, 0, err
	}
	if physicalActive != ledgers[0].ActiveObjectCount {
		return catalogState{}, 0, fmt.Errorf(
			"%w: publication transition ACTIVE registry disagrees with tenant ledger",
			ErrCorrupt,
		)
	}
	return catalog, physicalActive, nil
}

func readPublicationTransitionProjectionScalars(
	tx *gorm.DB,
	tenantID string,
	expectedCount int64,
) ([]publicationTransitionProjectionScalar, publicationTransitionAggregateScalars, error) {
	type scalarRecord struct {
		KnowledgeObjectID       string `gorm:"column:knowledge_object_id"`
		KnowledgeObjectIDBytes  int64  `gorm:"column:knowledge_object_id_bytes"`
		DefinitionDigestBytes   int64  `gorm:"column:definition_digest_bytes"`
		DefinitionBytes         int64  `gorm:"column:definition_bytes"`
		DefinitionProtoBytes    int64  `gorm:"column:definition_proto_bytes"`
		DescriptionPresent      int64  `gorm:"column:description_present"`
		DescriptionBytes        int64  `gorm:"column:description_bytes"`
		IndexSelectorCount      int64  `gorm:"column:index_selector_count"`
		HostSelectorCount       int64  `gorm:"column:host_selector_count"`
		SourceSelectorCount     int64  `gorm:"column:source_selector_count"`
		SourcetypeSelectorCount int64  `gorm:"column:sourcetype_selector_count"`
		SelectorValueBytes      int64  `gorm:"column:selector_value_bytes"`
		CanonicalSelectorBytes  int64  `gorm:"column:canonical_selector_bytes"`
		ProjectionBytes         int64  `gorm:"column:projection_bytes"`
		SealProjectionBytes     int64  `gorm:"column:seal_projection_bytes"`
		SealCanonicalBytes      int64  `gorm:"column:seal_canonical_selector_bytes"`
		DependencyCount         int64  `gorm:"column:dependency_count"`
	}
	query := tx.Table("knowledge_objects AS object INDEXED BY knowledge_objects_resolution_idx").
		Joins(`CROSS JOIN knowledge_object_versions AS version
			ON version.tenant_id = object.tenant_id
		   AND version.knowledge_object_id = object.knowledge_object_id
		   AND version.object_version = object.current_version`).
		Joins(`CROSS JOIN knowledge_object_list_projections AS projection
			ON projection.tenant_id = object.tenant_id
		   AND projection.knowledge_object_id = object.knowledge_object_id
		   AND projection.object_version = object.current_version
		   AND projection.app_id = object.app_id
		   AND projection.owner_id = object.owner_id
		   AND projection.object_type = object.object_type
		   AND projection.name = object.name
		   AND projection.sharing_scope = object.sharing_scope
		   AND projection.state = object.state`).
		Joins(`CROSS JOIN knowledge_object_list_order_keys AS ordering
			ON ordering.tenant_id = projection.tenant_id
		   AND ordering.knowledge_object_id = projection.knowledge_object_id
		   AND ordering.object_version = projection.object_version`).
		Joins(`CROSS JOIN knowledge_object_list_projection_seals AS seal
			ON seal.tenant_id = projection.tenant_id
		   AND seal.knowledge_object_id = projection.knowledge_object_id
		   AND seal.object_version = projection.object_version`).
		Joins(`LEFT JOIN knowledge_definition_blobs AS blob
			ON blob.tenant_id = version.tenant_id
		   AND blob.definition_digest = version.definition_digest`).
		Where("object.tenant_id = ? AND object.state = ?", tenantID, StateActive).
		Order("object.knowledge_object_id ASC")
	var records []scalarRecord
	if err := query.Select(fmt.Sprintf(`
		CASE WHEN length(CAST(object.knowledge_object_id AS BLOB)) BETWEEN 1 AND %[1]d
			THEN object.knowledge_object_id ELSE '' END AS knowledge_object_id,
		length(CAST(object.knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
		coalesce(length(version.definition_digest), -1) AS definition_digest_bytes,
		coalesce(blob.definition_bytes, -1) AS definition_bytes,
		coalesce(length(blob.definition_proto), -1) AS definition_proto_bytes,
		projection.description_present AS description_present,
		length(CAST(projection.description AS BLOB)) AS description_bytes,
		projection.index_selector_count AS index_selector_count,
		projection.host_selector_count AS host_selector_count,
		projection.source_selector_count AS source_selector_count,
		projection.sourcetype_selector_count AS sourcetype_selector_count,
		projection.selector_value_bytes AS selector_value_bytes,
		projection.canonical_selector_bytes AS canonical_selector_bytes,
		projection.projection_bytes AS projection_bytes,
		seal.projection_bytes AS seal_projection_bytes,
		seal.canonical_selector_bytes AS seal_canonical_selector_bytes,
		version.dependency_count AS dependency_count`, maximumObjectIDBytes)).
		Limit(MaximumResolutionCandidates + 1).
		Find(&records).Error; err != nil {
		return nil, publicationTransitionAggregateScalars{}, err
	}
	if int64(len(records)) != expectedCount {
		return nil, publicationTransitionAggregateScalars{}, fmt.Errorf(
			"%w: publication transition ACTIVE projection coverage disagrees with tenant ledger",
			ErrCorrupt,
		)
	}

	scalars := make([]publicationTransitionProjectionScalar, len(records))
	var aggregate publicationTransitionAggregateScalars
	for index, record := range records {
		selectorPatterns := record.IndexSelectorCount + record.HostSelectorCount +
			record.SourceSelectorCount + record.SourcetypeSelectorCount
		if record.KnowledgeObjectIDBytes != int64(len(record.KnowledgeObjectID)) ||
			!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) ||
			(index > 0 && records[index-1].KnowledgeObjectID >= record.KnowledgeObjectID) ||
			record.DefinitionDigestBytes != persistedKnowledgeDefinitionDigestBytes ||
			record.DefinitionBytes < 1 || record.DefinitionBytes > maximumDefinitionBytes ||
			record.DefinitionProtoBytes != record.DefinitionBytes ||
			record.DescriptionPresent < 0 || record.DescriptionPresent > 1 ||
			record.DescriptionBytes < 0 || record.DescriptionBytes > maximumDescriptionBytes ||
			record.IndexSelectorCount < 0 || record.IndexSelectorCount > maximumSelectorPatternsPerDimension ||
			record.HostSelectorCount < 0 || record.HostSelectorCount > maximumSelectorPatternsPerDimension ||
			record.SourceSelectorCount < 0 || record.SourceSelectorCount > maximumSelectorPatternsPerDimension ||
			record.SourcetypeSelectorCount < 0 || record.SourcetypeSelectorCount > maximumSelectorPatternsPerDimension ||
			record.SelectorValueBytes < 0 || record.SelectorValueBytes > maximumSelectorAggregateValueBytes ||
			record.CanonicalSelectorBytes < 0 || record.CanonicalSelectorBytes > maximumCanonicalSelectorBytes ||
			record.ProjectionBytes != record.DescriptionBytes+record.SelectorValueBytes ||
			record.SealProjectionBytes != record.ProjectionBytes ||
			record.SealCanonicalBytes != record.CanonicalSelectorBytes ||
			record.DependencyCount < 0 || record.DependencyCount > maximumDependenciesPerVersion {
			return nil, publicationTransitionAggregateScalars{}, fmt.Errorf(
				"%w: publication transition projection scalar preflight is invalid",
				ErrCorrupt,
			)
		}
		// #nosec G115 -- the scalar preflight above validates all values as nonnegative and bounded.
		definitionBytes := uint64(record.DefinitionBytes)
		// #nosec G115 -- the scalar preflight above validates all values as nonnegative and bounded.
		projectionBytes := uint64(record.ProjectionBytes)
		// #nosec G115 -- selector pattern counts above are nonnegative and bounded.
		selectorPatternCount := uint64(selectorPatterns)
		// #nosec G115 -- the scalar preflight above validates all values as nonnegative and bounded.
		selectorValueBytes := uint64(record.SelectorValueBytes)
		// #nosec G115 -- the scalar preflight above validates all values as nonnegative and bounded.
		canonicalSelectorBytes := uint64(record.CanonicalSelectorBytes)
		// #nosec G115 -- the scalar preflight above validates all values as nonnegative and bounded.
		dependencyCount := uint64(record.DependencyCount)
		if !addPublicationResource(&aggregate.definitionBytes, definitionBytes, maximumPublicationTransitionDefinitionBytes) ||
			!addPublicationResource(&aggregate.projectionBytes, projectionBytes, maximumPublicationTransitionProjectionBytes) ||
			!addPublicationResource(&aggregate.selectorPatterns, selectorPatternCount, maximumPublicationTransitionSelectorPatterns) ||
			!addPublicationResource(&aggregate.selectorValueBytes, selectorValueBytes, maximumPublicationTransitionSelectorBytes) ||
			!addPublicationResource(&aggregate.canonicalSelectorBytes, canonicalSelectorBytes, maximumPublicationTransitionSelectorBytes) ||
			!addPublicationResource(&aggregate.dependencies, dependencyCount, maximumPublicationTransitionDependencies) {
			return nil, publicationTransitionAggregateScalars{}, fmt.Errorf(
				"%w: publication transition aggregate scalar preflight exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
		scalars[index] = publicationTransitionProjectionScalar{
			knowledgeObjectID:      strings.Clone(record.KnowledgeObjectID),
			definitionBytes:        definitionBytes,
			projectionBytes:        projectionBytes,
			selectorPatterns:       selectorPatternCount,
			selectorValueBytes:     selectorValueBytes,
			canonicalSelectorBytes: canonicalSelectorBytes,
			dependencyCount:        dependencyCount,
		}
	}
	return scalars, aggregate, nil
}

func matchPublicationTransitionProjectionPreflight(
	expected []publicationTransitionProjectionScalar,
	projections []projectionRecord,
) error {
	if len(expected) != len(projections) {
		return fmt.Errorf("%w: publication transition projection payload changed after preflight", ErrCorrupt)
	}
	for index, projection := range projections {
		scalar := expected[index]
		selectorPatterns := projectionSelectorCount(projection)
		if projection.DefinitionBytes < 0 || projection.ProjectionBytes < 0 || selectorPatterns < 0 ||
			projection.SelectorValueBytes < 0 || projection.CanonicalSelectorBytes < 0 ||
			projection.DependencyCount < 0 {
			return fmt.Errorf("%w: publication transition projection payload changed after preflight", ErrCorrupt)
		}
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		definitionBytes := uint64(projection.DefinitionBytes)
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		projectionBytes := uint64(projection.ProjectionBytes)
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		selectorPatternCount := uint64(selectorPatterns)
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		selectorValueBytes := uint64(projection.SelectorValueBytes)
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		canonicalSelectorBytes := uint64(projection.CanonicalSelectorBytes)
		// #nosec G115 -- the nonnegative checks above make these conversions lossless.
		dependencyCount := uint64(projection.DependencyCount)
		if projection.KnowledgeObjectID != scalar.knowledgeObjectID ||
			definitionBytes != scalar.definitionBytes || projectionBytes != scalar.projectionBytes ||
			selectorPatternCount != scalar.selectorPatterns || selectorValueBytes != scalar.selectorValueBytes ||
			canonicalSelectorBytes != scalar.canonicalSelectorBytes || dependencyCount != scalar.dependencyCount {
			return fmt.Errorf("%w: publication transition projection payload changed after preflight", ErrCorrupt)
		}
	}
	return nil
}

func readPublicationTransitionActiveApps(
	tx *gorm.DB,
	tenantID string,
) ([]string, int64, error) {
	type revisionRecord struct {
		TenantID      string `gorm:"column:tenant_id"`
		TenantIDBytes int64  `gorm:"column:tenant_id_bytes"`
		Revision      int64  `gorm:"column:revision"`
	}
	var revisions []revisionRecord
	if err := tx.Table("app_catalog_revisions AS authority").Select(fmt.Sprintf(`
		CASE WHEN length(CAST(authority.tenant_id AS BLOB)) BETWEEN 1 AND %[1]d
			THEN authority.tenant_id ELSE '' END AS tenant_id,
		length(CAST(authority.tenant_id AS BLOB)) AS tenant_id_bytes,
		authority.revision AS revision`, maximumTenantIDBytes)).
		Where("authority.tenant_id = ?", tenantID).
		Limit(2).
		Find(&revisions).Error; err != nil {
		return nil, 0, err
	}
	if len(revisions) != 1 || revisions[0].TenantID != tenantID ||
		revisions[0].TenantIDBytes != int64(len(tenantID)) ||
		revisions[0].Revision < 1 {
		return nil, 0, fmt.Errorf(
			"%w: publication transition app-catalog revision authority is missing or invalid",
			ErrCorrupt,
		)
	}

	var records []publicationTransitionAppRecord
	if err := publicationTransitionAppPreflightQuery(tx, tenantID).
		Find(&records).Error; err != nil {
		return nil, 0, err
	}
	if len(records) > maximumReadableApps {
		return nil, 0, fmt.Errorf("%w: publication transition app inventory exceeds its limit", ErrCorrupt)
	}
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if record.AppIDBytes != int64(len(record.AppID)) || !control.ValidCanonicalAppID(record.AppID) ||
			record.TenantID != tenantID || record.TenantIDBytes != int64(len(tenantID)) ||
			record.StateBytes != int64(len(record.State)) ||
			(record.State != string(control.AppStateActive) && record.State != string(control.AppStateArchived)) {
			return nil, 0, fmt.Errorf("%w: publication transition app inventory is invalid", ErrCorrupt)
		}
		if _, duplicate := seen[record.AppID]; duplicate {
			return nil, 0, fmt.Errorf("%w: publication transition app inventory is invalid", ErrCorrupt)
		}
		seen[record.AppID] = struct{}{}
	}
	// The forced tenant-leading index bounds corrupt archived rows without an
	// unbounded SQL sort. Only admitted rows are sorted for deterministic input.
	sort.Slice(records, func(left, right int) bool {
		return records[left].AppID < records[right].AppID
	})
	active := make([]string, 0, len(records))
	for _, record := range records {
		if record.State == string(control.AppStateActive) {
			active = append(active, strings.Clone(record.AppID))
		}
	}
	if len(active) == 0 {
		return nil, 0, fmt.Errorf(
			"%w: publication transition has no active app authority",
			ErrCorrupt,
		)
	}
	return active[:len(active):len(active)], revisions[0].Revision, nil
}

func publicationTransitionAppPreflightQuery(tx *gorm.DB, tenantID string) *gorm.DB {
	return tx.Table(
		"app_workspaces AS app INDEXED BY app_workspaces_tenant_display_id_idx",
	).Select(fmt.Sprintf(`
		CASE WHEN length(CAST(app.app_id AS BLOB)) BETWEEN 1 AND %[1]d
			THEN app.app_id ELSE '' END AS app_id,
		length(CAST(app.app_id AS BLOB)) AS app_id_bytes,
		CASE WHEN length(CAST(app.tenant_id AS BLOB)) BETWEEN 1 AND %[2]d
			THEN app.tenant_id ELSE '' END AS tenant_id,
		length(CAST(app.tenant_id AS BLOB)) AS tenant_id_bytes,
		CASE WHEN length(CAST(app.state AS BLOB)) BETWEEN 1 AND %[3]d
			THEN app.state ELSE '' END AS state,
		length(CAST(app.state AS BLOB)) AS state_bytes`,
		maximumAppIDBytes,
		maximumTenantIDBytes,
		len(string(control.AppStateArchived)),
	)).Where("app.tenant_id = ?", tenantID).
		Limit(maximumReadableApps + 1)
}

func readPublicationTransitionPotentialIndexes(
	tx *gorm.DB,
) ([]string, int64, uint16, error) {
	type stateRecord struct {
		SingletonID   int64 `gorm:"column:singleton_id"`
		Revision      int64 `gorm:"column:revision"`
		PhysicalCount int64 `gorm:"column:physical_count"`
	}
	var states []stateRecord
	if err := tx.Table("index_catalog_state").
		Select("singleton_id, revision, physical_count").
		Where("singleton_id = ?", 1).
		Limit(2).
		Find(&states).Error; err != nil {
		return nil, 0, 0, err
	}
	if len(states) != 1 || states[0].SingletonID != 1 || states[0].Revision < 1 ||
		states[0].PhysicalCount < 0 ||
		states[0].PhysicalCount > maximumPublicationIndexAtoms {
		return nil, 0, 0, fmt.Errorf("%w: publication transition index-catalog state is invalid", ErrCorrupt)
	}

	var records []publicationTransitionIndexRecord
	if err := publicationTransitionPhysicalIndexQuery(tx).
		Find(&records).Error; err != nil {
		return nil, 0, 0, err
	}
	if int64(len(records)) != states[0].PhysicalCount {
		return nil, 0, 0, fmt.Errorf(
			"%w: publication transition physical index inventory disagrees with its ledger",
			ErrCorrupt,
		)
	}
	var orphanTombstones int64
	if err := tx.Raw(`SELECT count(*) FROM (
		SELECT 1 FROM index_deletion_tombstones AS tombstone
		LEFT JOIN indexes AS target ON target.index_id = tombstone.index_id
		WHERE target.index_id IS NULL LIMIT ?
	) AS bounded_orphan_tombstones`, maximumPublicationIndexAtoms+1).
		Scan(&orphanTombstones).Error; err != nil {
		return nil, 0, 0, err
	}
	if orphanTombstones != 0 {
		return nil, 0, 0, fmt.Errorf("%w: publication transition index tombstone is orphaned", ErrCorrupt)
	}

	names := make([]string, 0, len(records))
	for index, record := range records {
		canonical, normalizeErr := control.NormalizeIndexName(record.Name)
		if record.IndexIDBytes != int64(len(record.IndexID)) ||
			!validIdentity(record.IndexID, maximumObjectIDBytes) ||
			record.NameBytes != int64(len(record.Name)) || normalizeErr != nil || canonical != record.Name ||
			record.Version < 1 ||
			record.StateBytes != int64(len(record.State)) ||
			(record.State != string(control.IndexStateActive) &&
				record.State != string(control.IndexStateArchived) &&
				record.State != string(control.IndexStateDeleting)) ||
			(index > 0 && records[index-1].Name >= record.Name) {
			return nil, 0, 0, fmt.Errorf("%w: publication transition physical index inventory is invalid", ErrCorrupt)
		}
		switch record.TombstonePresent {
		case 0:
			if record.TombstoneName != "" || record.TombstoneNameBytes != -1 ||
				record.TombstoneDeletedVersion != -1 {
				return nil, 0, 0, fmt.Errorf("%w: publication transition index tombstone sentinel is invalid", ErrCorrupt)
			}
			if record.State == string(control.IndexStateActive) ||
				record.State == string(control.IndexStateArchived) {
				names = append(names, strings.Clone(record.Name))
			}
		case 1:
			if record.TombstoneNameBytes != int64(len(record.TombstoneName)) ||
				record.TombstoneName != record.Name ||
				record.TombstoneDeletedVersion != record.Version ||
				(record.State != string(control.IndexStateArchived) &&
					record.State != string(control.IndexStateDeleting)) {
				return nil, 0, 0, fmt.Errorf("%w: publication transition index tombstone is invalid", ErrCorrupt)
			}
		default:
			return nil, 0, 0, fmt.Errorf("%w: publication transition index tombstone marker is invalid", ErrCorrupt)
		}
	}
	// #nosec G115 -- PhysicalCount was checked against maximumPublicationIndexAtoms above.
	physicalCount := uint16(states[0].PhysicalCount)
	return names[:len(names):len(names)], states[0].Revision, physicalCount, nil
}

func publicationTransitionPhysicalIndexQuery(tx *gorm.DB) *gorm.DB {
	return tx.Table("indexes AS target INDEXED BY indexes_name_id_idx").
		Joins(`LEFT JOIN index_deletion_tombstones AS tombstone
			ON tombstone.index_id = target.index_id`).
		Select(fmt.Sprintf(`
			CASE WHEN length(CAST(target.index_id AS BLOB)) BETWEEN 1 AND %[1]d
				THEN target.index_id ELSE '' END AS index_id,
			length(CAST(target.index_id AS BLOB)) AS index_id_bytes,
			CASE WHEN length(CAST(target.name AS BLOB)) BETWEEN 1 AND %[2]d
				THEN target.name ELSE '' END AS name,
			length(CAST(target.name AS BLOB)) AS name_bytes,
			target.version AS version,
			CASE WHEN length(CAST(target.state AS BLOB)) BETWEEN 1 AND %[3]d
				THEN target.state ELSE '' END AS state,
			length(CAST(target.state AS BLOB)) AS state_bytes,
			CASE WHEN tombstone.index_id IS NULL THEN 0 ELSE 1 END AS tombstone_present,
			CASE WHEN length(CAST(tombstone.name AS BLOB)) BETWEEN 1 AND %[2]d
				THEN tombstone.name ELSE '' END AS tombstone_name,
			coalesce(length(CAST(tombstone.name AS BLOB)), -1) AS tombstone_name_bytes,
			coalesce(tombstone.deleted_version, -1) AS tombstone_deleted_version`,
			maximumObjectIDBytes,
			maximumFilterBytes,
			len(string(control.IndexStateDeleting)),
		)).Order("target.name ASC").
		Limit(maximumPublicationIndexAtoms + 1)
}

func publicationTransitionWinnersFromHydration(
	tenantID string,
	objects []Object,
	authorities projectionHydrationAuthorities,
) ([]publicationWinner, uint64, error) {
	if len(objects) != len(authorities.versions) || len(objects) != len(authorities.dependencies) {
		return nil, 0, fmt.Errorf("%w: publication transition hydration authority is incomplete", ErrCorrupt)
	}
	winners := make([]publicationWinner, len(objects))
	var selectorWork uint64
	for index, object := range objects {
		version, versionFound := authorities.versions[object.KnowledgeObjectID]
		dependencies, dependenciesFound := authorities.dependencies[object.KnowledgeObjectID]
		if !versionFound || !dependenciesFound || version.TenantID != tenantID ||
			version.KnowledgeObjectID != object.KnowledgeObjectID ||
			version.ObjectVersion < 1 || uint64(version.ObjectVersion) != object.Version {
			return nil, 0, fmt.Errorf("%w: publication transition hydration authority is incomplete", ErrCorrupt)
		}
		normalized, err := knowledgedefinition.Normalize(object.Definition)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: publication transition hydrated definition is invalid", ErrCorrupt)
		}
		objectType, typeOK := objectTypeToProto(object.ObjectType)
		sharingScope, scopeOK := knowledgeSharingScopeToProto(object.SharingScope)
		if !typeOK || !scopeOK || normalized.ObjectType != objectType ||
			normalized.Name != object.Name || normalized.AppID != object.AppID ||
			normalized.SharingScope != sharingScope ||
			!bytes.Equal(normalized.Digest[:], object.DefinitionSHA256) {
			return nil, 0, fmt.Errorf("%w: publication transition hydrated definition authority disagrees", ErrCorrupt)
		}
		snapshotObject := knowledgesnapshot.Object{
			KnowledgeObjectID: strings.Clone(object.KnowledgeObjectID),
			Version:           object.Version,
			ObjectType:        objectType,
			Name:              strings.Clone(object.Name),
			AppID:             strings.Clone(object.AppID),
			OwnerID:           strings.Clone(object.OwnerID),
			SharingScope:      sharingScope,
			Definition:        normalized.Definition,
			DefinitionSHA256:  bytes.Clone(normalized.Digest[:]),
		}
		if !addPublicationResource(
			&selectorWork,
			normalized.Selector.Stats().WildcardWorkUnits,
			maximumPublicationTransitionSelectorWork,
		) {
			return nil, 0, fmt.Errorf(
				"%w: publication transition selector work exceeds its limit",
				control.ErrCapacityExceeded,
			)
		}
		persisted := make([]publicationPersistedDependency, len(dependencies))
		for dependencyIndex, dependency := range dependencies {
			if dependency.TenantID != tenantID || dependency.SourceObjectID != object.KnowledgeObjectID ||
				dependency.SourceObjectVersion != version.ObjectVersion {
				return nil, 0, fmt.Errorf("%w: publication transition dependency authority is incomplete", ErrCorrupt)
			}
			persisted[dependencyIndex] = publicationPersistedDependency{
				ordinal:        dependency.Ordinal,
				targetObjectID: strings.Clone(dependency.TargetObjectID),
				targetVersion:  dependency.TargetObjectVersion,
				role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}
		}
		if err := validatePersistedPublicationDependencyScalars(persisted); err != nil {
			return nil, 0, err
		}
		winners[index] = publicationWinner{
			object:                      snapshotObject,
			existingDependenciesPresent: true,
			existingDependencies:        persisted,
		}
	}
	return winners[:len(winners):len(winners)], selectorWork, nil
}
