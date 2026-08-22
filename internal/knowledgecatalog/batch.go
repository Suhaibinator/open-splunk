package knowledgecatalog

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const listHydrationChunkSize = 512

type listHydrationBudget struct {
	definitionBytes    int64
	projectionBytes    int64
	selectorPatterns   int64
	selectorValueBytes int64
	dependencies       int64
}

var listResponseHydrationBudget = listHydrationBudget{
	definitionBytes:    MaximumListResponseCanonicalDefinitionBytes,
	projectionBytes:    int64(MaximumPageSize) * maximumProjectionBytes,
	selectorPatterns:   int64(MaximumPageSize) * maximumSelectorPatterns,
	selectorValueBytes: int64(MaximumPageSize) * maximumSelectorAggregateValueBytes,
	dependencies:       MaximumListResponseDependencies,
}

var listFilterIntegrityHydrationBudget = listHydrationBudget{
	definitionBytes:    MaximumListFilterIntegrityDefinitionBytes,
	projectionBytes:    MaximumListFilterIntegrityProjectionBytes,
	selectorPatterns:   MaximumListFilterIntegritySelectorPatterns,
	selectorValueBytes: MaximumListFilterIntegritySelectorBytes,
	dependencies:       MaximumListFilterIntegrityDependencies,
}

type projectionHydrationAuthorities struct {
	versions     map[string]versionRecord
	dependencies map[string][]dependencyRecord
}

func objectsFromProjections(
	database *gorm.DB,
	projections []projectionRecord,
	budget listHydrationBudget,
) ([]Object, error) {
	objects, _, err := objectsFromProjectionsInternal(database, projections, budget, false, nil)
	return objects, err
}

func objectsFromProjectionsWithAuthorities(
	database *gorm.DB,
	projections []projectionRecord,
	budget listHydrationBudget,
) ([]Object, projectionHydrationAuthorities, error) {
	var authorities projectionHydrationAuthorities
	objects, _, err := objectsFromProjectionsInternal(
		database,
		projections,
		budget,
		false,
		&authorities,
	)
	if err != nil {
		return nil, projectionHydrationAuthorities{}, err
	}
	return objects, authorities, nil
}

// objectsFromProjectionsPage permits a response page to stop at the preceding
// object when validating that object's transitive dependency closure would
// cross the response work budget. Complete integrity sweeps use
// objectsFromProjections and continue to fail closed instead of returning a
// partially validated candidate set.
func objectsFromProjectionsPage(
	database *gorm.DB,
	projections []projectionRecord,
	budget listHydrationBudget,
) ([]Object, int, error) {
	return objectsFromProjectionsInternal(database, projections, budget, true, nil)
}

func objectsFromProjectionsInternal(
	database *gorm.DB,
	projections []projectionRecord,
	budget listHydrationBudget,
	allowSemanticBoundary bool,
	authorities *projectionHydrationAuthorities,
) ([]Object, int, error) {
	if len(projections) == 0 {
		return []Object{}, 0, nil
	}
	tenantID := projections[0].TenantID
	registries := make(map[string]registryRecord, len(projections))
	projectionByID := make(map[string]projectionRecord, len(projections))
	objectIDs := make([]string, len(projections))
	for index, projection := range projections {
		if err := listDatabaseContextError(database); err != nil {
			return nil, 0, err
		}
		if projection.TenantID != tenantID {
			return nil, 0, fmt.Errorf("%w: mixed-tenant list projection batch", ErrCorrupt)
		}
		if _, duplicate := projectionByID[projection.KnowledgeObjectID]; duplicate {
			return nil, 0, fmt.Errorf("%w: duplicate list projection identity", ErrCorrupt)
		}
		registry := registryFromProjection(projection)
		if err := validateRegistry(registry); err != nil {
			return nil, 0, err
		}
		if err := validateProjectionScalars(projection, registry); err != nil {
			return nil, 0, err
		}
		registries[projection.KnowledgeObjectID] = registry
		projectionByID[projection.KnowledgeObjectID] = projection
		objectIDs[index] = projection.KnowledgeObjectID
	}
	if err := validateListHydrationBudget(projections, budget); err != nil {
		return nil, 0, err
	}

	versions, err := readCurrentVersionRecordsBatch(database, tenantID, objectIDs)
	if err != nil {
		return nil, 0, err
	}
	for objectID, registry := range registries {
		version, found := versions[objectID]
		if !found || validateCurrentVersion(registry, version) != nil ||
			version.DependencyCount != projectionByID[objectID].DependencyCount {
			return nil, 0, fmt.Errorf("%w: current immutable version disagrees with list projection", ErrCorrupt)
		}
	}
	if err := validateCurrentChronologiesBatch(
		database,
		tenantID,
		objectIDs,
		registries,
		versions,
	); err != nil {
		return nil, 0, err
	}
	lifecycles, err := readCurrentVersionLifecyclesBatch(database, tenantID, objectIDs, versions)
	if err != nil {
		return nil, 0, err
	}
	for objectID, registry := range registries {
		if err := validateCurrentLifecycle(registry, versions[objectID], lifecycles[objectID]); err != nil {
			return nil, 0, err
		}
	}
	dependencyRecords, err := validateCurrentDependenciesBatch(
		database,
		tenantID,
		objectIDs,
		versions,
		budget.dependencies,
	)
	if err != nil {
		return nil, 0, err
	}
	selectors, err := readCurrentSelectorRecordsBatch(
		database,
		tenantID,
		objectIDs,
		budget.selectorPatterns,
		budget.selectorValueBytes,
	)
	if err != nil {
		return nil, 0, err
	}
	digests := make([][]byte, 0, len(projections))
	seenDigests := make(map[string]struct{}, len(projections))
	for _, projection := range projections {
		if projection.State == StateQuarantined {
			continue
		}
		key := string(projection.DefinitionDigest)
		if _, found := seenDigests[key]; found {
			continue
		}
		seenDigests[key] = struct{}{}
		digests = append(digests, bytes.Clone(projection.DefinitionDigest))
	}
	blobs, err := readDefinitionBlobsBatch(database, tenantID, digests, budget.definitionBytes)
	if err != nil {
		return nil, 0, err
	}

	objects := make([]Object, len(projections))
	decodedByObjectID := make(map[string]decodedDefinition, len(projections))
	for index, projection := range projections {
		if err := listDatabaseContextError(database); err != nil {
			return nil, 0, err
		}
		registry := registries[projection.KnowledgeObjectID]
		version := versions[projection.KnowledgeObjectID]
		selectorRows := selectors[projection.KnowledgeObjectID]
		if registry.State == StateQuarantined {
			if err := validateQuarantinedProjection(projection, selectorRows); err != nil {
				return nil, 0, err
			}
			objects[index], err = objectFromCurrentScalars(registry, nil, nil)
			if err != nil {
				return nil, 0, err
			}
			continue
		}
		blob, found := blobs[string(version.DefinitionDigest)]
		if !found {
			return nil, 0, fmt.Errorf("%w: current definition blob is missing", ErrCorrupt)
		}
		normalized, definitionBytes, decodeErr := decodeVersionDefinitionBlob(version, blob)
		if decodeErr != nil {
			return nil, 0, decodeErr
		}
		if definitionBytes != projection.DefinitionBytes {
			return nil, 0, fmt.Errorf("%w: projected definition byte authority disagrees", ErrCorrupt)
		}
		if err := validateDecodedIdentity(normalized, version); err != nil {
			return nil, 0, err
		}
		if err := validateProjectionDefinition(projection, selectorRows, normalized); err != nil {
			return nil, 0, err
		}
		decodedByObjectID[projection.KnowledgeObjectID] = normalized
	}
	maximumSemanticNodes := int64(len(projections)) + budget.dependencies
	semanticValidator := newDependencySemanticValidator(database, tenantID, dependencySemanticWorkBudget{
		nodes:           maximumSemanticNodes,
		edges:           budget.dependencies,
		definitionBytes: max(budget.definitionBytes, int64(maximumDefinitionBytes)),
		queries:         maximumSemanticNodes * 8,
	})
	if !allowSemanticBoundary {
		// A complete sweep has already decoded every admitted current body and
		// validated every immutable dependency set. Seed all of those authorities
		// before walking any root so a lexically earlier source cannot re-query or
		// re-decode a later target that is already inside the same bounded batch.
		for _, projection := range projections {
			normalized, found := decodedByObjectID[projection.KnowledgeObjectID]
			if !found {
				continue
			}
			if err := semanticValidator.seedDecodedVersion(
				versions[projection.KnowledgeObjectID],
				normalized,
				dependencyRecords[projection.KnowledgeObjectID],
			); err != nil {
				return nil, 0, err
			}
		}
	}
	// Roots are seeded immediately before their graph walk. This makes the
	// validator's cumulative work at index N exactly the closure needed for the
	// first N returned objects, so a later root can become a continuation
	// boundary without charging future roots against the current page.
	for index, projection := range projections {
		normalized, found := decodedByObjectID[projection.KnowledgeObjectID]
		if !found {
			continue
		}
		version := versions[projection.KnowledgeObjectID]
		if err := semanticValidator.validateDecodedVersionDependencies(
			version,
			normalized,
			dependencyRecords[projection.KnowledgeObjectID],
		); err != nil {
			if allowSemanticBoundary && index > 0 && errors.Is(err, control.ErrCapacityExceeded) {
				return objects[:index:index], index, nil
			}
			return nil, 0, err
		}
		registry := registries[projection.KnowledgeObjectID]
		objects[index], err = objectFromCurrentScalars(registry, normalized.Definition, normalized.Digest)
		if err != nil {
			return nil, 0, err
		}
	}
	if authorities != nil {
		authorities.versions = versions
		authorities.dependencies = dependencyRecords
	}
	return slices.Clip(objects), len(objects), nil
}

func validateListHydrationBudget(projections []projectionRecord, budget listHydrationBudget) error {
	if budget.definitionBytes < maximumDefinitionBytes ||
		budget.projectionBytes < maximumProjectionBytes ||
		budget.selectorPatterns < maximumSelectorPatterns ||
		budget.selectorValueBytes < maximumSelectorAggregateValueBytes ||
		budget.dependencies < maximumDependenciesPerVersion {
		return fmt.Errorf("%w: invalid list hydration budget", ErrCorrupt)
	}
	var definitions, projectionBytes, selectorPatterns, selectorBytes, dependencies int64
	for _, projection := range projections {
		charge, err := listDefinitionCharge(projection)
		if err != nil {
			return err
		}
		if definitions > budget.definitionBytes-charge ||
			projectionBytes > budget.projectionBytes-projection.ProjectionBytes ||
			selectorPatterns > budget.selectorPatterns-projectionSelectorCount(projection) ||
			selectorBytes > budget.selectorValueBytes-projection.SelectorValueBytes ||
			dependencies > budget.dependencies-projection.DependencyCount {
			return fmt.Errorf("%w: aggregate list hydration budget exceeded", control.ErrCapacityExceeded)
		}
		definitions += charge
		projectionBytes += projection.ProjectionBytes
		selectorPatterns += projectionSelectorCount(projection)
		selectorBytes += projection.SelectorValueBytes
		dependencies += projection.DependencyCount
	}
	return nil
}

func projectionSelectorCount(projection projectionRecord) int64 {
	return projection.IndexSelectorCount + projection.HostSelectorCount +
		projection.SourceSelectorCount + projection.SourcetypeSelectorCount
}

func listDatabaseContextError(database *gorm.DB) error {
	if database == nil {
		return fmt.Errorf("%w: nil list hydration database", ErrCorrupt)
	}
	if database.Statement != nil && database.Statement.Context != nil {
		return database.Statement.Context.Err()
	}
	return nil
}

func listChunks[T any](values []T, visit func([]T) error) error {
	for start := 0; start < len(values); start += listHydrationChunkSize {
		end := min(start+listHydrationChunkSize, len(values))
		if err := visit(values[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func readCurrentVersionRecordsBatch(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
) (map[string]versionRecord, error) {
	recordsByID := make(map[string]versionRecord, len(objectIDs))
	err := listChunks(objectIDs, func(chunk []string) error {
		if err := listDatabaseContextError(database); err != nil {
			return err
		}
		query := database.Table("knowledge_object_versions AS version").
			Joins(`JOIN knowledge_objects AS object
				ON object.tenant_id = version.tenant_id
			   AND object.knowledge_object_id = version.knowledge_object_id
			   AND object.current_version = version.object_version`).
			Where("version.tenant_id = ? AND version.knowledge_object_id IN ?", tenantID, chunk).
			Order("version.knowledge_object_id ASC")
		var sizes []versionSizeRecord
		if err := query.Session(&gorm.Session{}).Select(`
			length(CAST(version.knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
			length(CAST(version.tenant_id AS BLOB)) AS tenant_id_bytes,
			length(CAST(version.app_id AS BLOB)) AS app_id_bytes,
			length(CAST(version.owner_id AS BLOB)) AS owner_id_bytes,
			length(CAST(version.object_type AS BLOB)) AS object_type_bytes,
			length(CAST(version.name AS BLOB)) AS name_bytes,
			length(CAST(version.sharing_scope AS BLOB)) AS sharing_scope_bytes,
			length(CAST(version.state AS BLOB)) AS state_bytes,
			coalesce(length(version.definition_digest), 0) AS digest_bytes,
			coalesce(length(CAST(version.quarantine_reason AS BLOB)), 0) AS quarantine_reason_bytes,
			length(CAST(version.mutation_kind AS BLOB)) AS mutation_kind_bytes
		`).Limit(len(chunk) + 1).Find(&sizes).Error; err != nil {
			return err
		}
		if len(sizes) > len(chunk) {
			return fmt.Errorf("%w: duplicate current immutable version", ErrCorrupt)
		}
		for _, size := range sizes {
			if !validVersionSize(size) {
				return fmt.Errorf("%w: current version row width is invalid", ErrCorrupt)
			}
		}
		var records []versionRecord
		if err := query.Session(&gorm.Session{}).Select("version.*").Limit(len(chunk) + 1).Find(&records).Error; err != nil {
			return err
		}
		if len(records) != len(sizes) {
			return fmt.Errorf("%w: current version preflight changed", ErrCorrupt)
		}
		for _, record := range records {
			if _, duplicate := recordsByID[record.KnowledgeObjectID]; duplicate {
				return fmt.Errorf("%w: duplicate current version identity", ErrCorrupt)
			}
			recordsByID[record.KnowledgeObjectID] = record
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(recordsByID) != len(objectIDs) {
		return nil, fmt.Errorf("%w: current immutable version is missing", ErrCorrupt)
	}
	return recordsByID, nil
}

type batchSelectorSizeRecord struct {
	ObjectIDBytes  int64 `gorm:"column:object_id_bytes"`
	DimensionBytes int64 `gorm:"column:dimension_bytes"`
	MatchKindBytes int64 `gorm:"column:match_kind_bytes"`
	ValueBytes     int64 `gorm:"column:value_bytes"`
}

type batchSelectorRecord struct {
	KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
	ObjectVersion     int64  `gorm:"column:object_version"`
	Dimension         string `gorm:"column:dimension"`
	Ordinal           int64  `gorm:"column:ordinal"`
	MatchKind         string `gorm:"column:match_kind"`
	Value             string `gorm:"column:value"`
	ValueBytes        int64  `gorm:"column:value_bytes"`
}

func readCurrentSelectorRecordsBatch(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
	maximumRows, maximumBytes int64,
) (map[string][]selectorRecord, error) {
	rowsByID := make(map[string][]selectorRecord, len(objectIDs))
	var rowsRead, bytesRead int64
	err := listChunks(objectIDs, func(chunk []string) error {
		if err := listDatabaseContextError(database); err != nil {
			return err
		}
		// Do not apply the canonical CASE ordering until this chunk's physical
		// cardinality and widths are proven bounded.  The preflight follows the
		// selector primary-key suffix, allowing LIMIT to stop the index walk.
		preflightQuery := currentSelectorRecordPreflightQuery(database, tenantID, chunk)
		remaining := maximumRows - rowsRead
		if remaining < 0 {
			return fmt.Errorf("%w: selector row budget exceeded", control.ErrCapacityExceeded)
		}
		var sizes []batchSelectorSizeRecord
		if err := preflightQuery.Session(&gorm.Session{}).Select(`
			length(CAST(selector.knowledge_object_id AS BLOB)) AS object_id_bytes,
			length(CAST(selector.dimension AS BLOB)) AS dimension_bytes,
			length(CAST(selector.match_kind AS BLOB)) AS match_kind_bytes,
			length(CAST(selector.value AS BLOB)) AS value_bytes
		`).Limit(int(remaining + 1)).Find(&sizes).Error; err != nil {
			return err
		}
		if int64(len(sizes)) > remaining {
			return fmt.Errorf("%w: selector row budget exceeded", control.ErrCapacityExceeded)
		}
		for _, size := range sizes {
			if size.ObjectIDBytes < 1 || size.ObjectIDBytes > maximumObjectIDBytes ||
				!validSelectorProjectionWidth(size.DimensionBytes, size.MatchKindBytes, size.ValueBytes) {
				return fmt.Errorf("%w: selector projection width is invalid", ErrCorrupt)
			}
			if bytesRead > maximumBytes-size.ValueBytes {
				return fmt.Errorf("%w: selector byte budget exceeded", control.ErrCapacityExceeded)
			}
			bytesRead += size.ValueBytes
		}
		payloadQuery := currentSelectorRecordBaseQuery(database, tenantID, chunk).
			Order("selector.knowledge_object_id ASC").
			Order(`CASE selector.dimension
				WHEN 'index' THEN 1 WHEN 'host' THEN 2
				WHEN 'source' THEN 3 WHEN 'sourcetype' THEN 4 ELSE 5 END ASC`).
			Order("selector.ordinal ASC")
		var records []batchSelectorRecord
		if err := payloadQuery.Session(&gorm.Session{}).Select(`
			selector.knowledge_object_id,
			selector.object_version,
			selector.dimension,
			selector.ordinal,
			selector.match_kind,
			selector.value,
			selector.value_bytes
		`).Limit(len(sizes) + 1).Find(&records).Error; err != nil {
			return err
		}
		if len(records) != len(sizes) {
			return fmt.Errorf("%w: selector projection preflight changed", ErrCorrupt)
		}
		rowsRead += int64(len(records))
		for _, record := range records {
			rowsByID[record.KnowledgeObjectID] = append(rowsByID[record.KnowledgeObjectID], selectorRecord{
				Dimension:  record.Dimension,
				Ordinal:    record.Ordinal,
				MatchKind:  record.MatchKind,
				Value:      record.Value,
				ValueBytes: record.ValueBytes,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rowsByID, nil
}

func currentSelectorRecordBaseQuery(database *gorm.DB, tenantID string, objectIDs []string) *gorm.DB {
	return database.Table("knowledge_object_list_selector_patterns AS selector").
		Joins(`JOIN knowledge_objects AS object
			ON object.tenant_id = selector.tenant_id
		   AND object.knowledge_object_id = selector.knowledge_object_id
		   AND object.current_version = selector.object_version`).
		Where("selector.tenant_id = ? AND selector.knowledge_object_id IN ?", tenantID, objectIDs)
}

func currentSelectorRecordPreflightQuery(database *gorm.DB, tenantID string, objectIDs []string) *gorm.DB {
	return currentSelectorRecordBaseQuery(database, tenantID, objectIDs).
		Order("selector.knowledge_object_id ASC").
		Order("selector.object_version ASC").
		Order("selector.dimension ASC").
		Order("selector.ordinal ASC")
}

type batchBlobSizeRecord struct {
	TenantIDBytes   int64 `gorm:"column:tenant_id_bytes"`
	DigestBytes     int64 `gorm:"column:digest_bytes"`
	ProtoBytes      int64 `gorm:"column:proto_bytes"`
	DefinitionBytes int64 `gorm:"column:definition_bytes"`
}

func readDefinitionBlobsBatch(
	database *gorm.DB,
	tenantID string,
	digests [][]byte,
	maximumBytes int64,
) (map[string]definitionBlobRecord, error) {
	recordsByDigest := make(map[string]definitionBlobRecord, len(digests))
	var bytesRead int64
	err := listChunks(digests, func(chunk [][]byte) error {
		if err := listDatabaseContextError(database); err != nil {
			return err
		}
		query := database.Model(&definitionBlobRecord{}).
			Where("tenant_id = ? AND definition_digest IN ?", tenantID, chunk).
			Order("definition_digest ASC")
		var sizes []batchBlobSizeRecord
		if err := query.Session(&gorm.Session{}).Select(`
			length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
			length(definition_digest) AS digest_bytes,
			length(definition_proto) AS proto_bytes,
			definition_bytes AS definition_bytes
		`).Limit(len(chunk) + 1).Find(&sizes).Error; err != nil {
			return err
		}
		if len(sizes) > len(chunk) {
			return fmt.Errorf("%w: duplicate definition blob", ErrCorrupt)
		}
		for _, size := range sizes {
			if !validDefinitionBlobWidth(size.TenantIDBytes, size.DigestBytes, size.ProtoBytes) ||
				size.DefinitionBytes != size.ProtoBytes {
				return fmt.Errorf("%w: definition blob is missing or oversized", ErrCorrupt)
			}
			if bytesRead > maximumBytes-size.ProtoBytes {
				return fmt.Errorf("%w: definition blob byte budget exceeded", control.ErrCapacityExceeded)
			}
			bytesRead += size.ProtoBytes
		}
		var records []definitionBlobRecord
		if err := query.Session(&gorm.Session{}).Limit(len(chunk) + 1).Find(&records).Error; err != nil {
			return err
		}
		if len(records) != len(sizes) {
			return fmt.Errorf("%w: definition blob preflight changed", ErrCorrupt)
		}
		for _, record := range records {
			key := string(record.DefinitionDigest)
			if _, duplicate := recordsByDigest[key]; duplicate {
				return fmt.Errorf("%w: duplicate definition blob digest", ErrCorrupt)
			}
			if record.TenantID != tenantID || int64(len(record.DefinitionProto)) != record.DefinitionBytes {
				return fmt.Errorf("%w: definition blob identity disagrees", ErrCorrupt)
			}
			recordsByDigest[key] = record
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(recordsByDigest) != len(digests) {
		return nil, fmt.Errorf("%w: definition blob is missing", ErrCorrupt)
	}
	return recordsByDigest, nil
}

type dependencyAggregateRecord struct {
	KnowledgeObjectID    string `gorm:"column:knowledge_object_id"`
	ObjectVersion        int64  `gorm:"column:object_version"`
	SealFound            int64  `gorm:"column:seal_found"`
	SealDependencyCount  int64  `gorm:"column:seal_dependency_count"`
	DependencyCount      int64  `gorm:"column:dependency_count"`
	DistinctOrdinalCount int64  `gorm:"column:distinct_ordinal_count"`
	MinimumOrdinal       int64  `gorm:"column:minimum_ordinal"`
	MaximumOrdinal       int64  `gorm:"column:maximum_ordinal"`
	OrdinalSum           int64  `gorm:"column:ordinal_sum"`
	InvalidCount         int64  `gorm:"column:invalid_count"`
}

type dependencyPhysicalRecord struct {
	KnowledgeObjectID   string `gorm:"column:knowledge_object_id"`
	ObjectVersion       int64  `gorm:"column:object_version"`
	SealFound           int64  `gorm:"column:seal_found"`
	SealDependencyCount int64  `gorm:"column:seal_dependency_count"`
	DependencyFound     int64  `gorm:"column:dependency_found"`
	TargetKindBytes     int64  `gorm:"column:target_kind_bytes"`
	TargetIDBytes       int64  `gorm:"column:target_id_bytes"`
	DependencyRoleBytes int64  `gorm:"column:dependency_role_bytes"`
}

func validateCurrentDependenciesBatch(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
	versions map[string]versionRecord,
	maximumRows int64,
) (map[string][]dependencyRecord, error) {
	var rowsRead int64
	recordsByObjectID := make(map[string][]dependencyRecord, len(objectIDs))
	for _, objectID := range objectIDs {
		recordsByObjectID[objectID] = []dependencyRecord{}
	}
	err := listChunks(objectIDs, func(chunk []string) error {
		if err := listDatabaseContextError(database); err != nil {
			return err
		}
		remaining := maximumRows - rowsRead
		if remaining < 0 {
			return fmt.Errorf("%w: dependency row budget exceeded", control.ErrCapacityExceeded)
		}
		// A grouped aggregate cannot enforce a physical work bound: SQLite must
		// consume the entire group before LIMIT can apply.  Walk the source and
		// dependency primary keys first, retaining one LEFT-JOIN sentinel for a
		// dependency-free source so the same bounded query also proves its seal.
		physicalQuery := currentDependencyPhysicalPreflightQuery(database, tenantID, chunk)
		var physical []dependencyPhysicalRecord
		if err := physicalQuery.Session(&gorm.Session{}).Select(`
			source.knowledge_object_id AS knowledge_object_id,
			source.object_version AS object_version,
			CASE WHEN seal.knowledge_object_id IS NULL THEN 0 ELSE 1 END AS seal_found,
			coalesce(seal.dependency_count, -1) AS seal_dependency_count,
			CASE WHEN dependency.ordinal IS NULL THEN 0 ELSE 1 END AS dependency_found,
			coalesce(length(CAST(dependency.target_kind AS BLOB)), -1) AS target_kind_bytes,
			coalesce(length(CAST(dependency.target_object_id AS BLOB)), -1) AS target_id_bytes,
			coalesce(length(CAST(dependency.dependency_role AS BLOB)), -1) AS dependency_role_bytes
		`).Limit(int(remaining) + len(chunk) + 1).Find(&physical).Error; err != nil {
			return err
		}
		seenPhysicalSources := make(map[string]struct{}, len(chunk))
		var physicalDependencyRows int64
		for _, record := range physical {
			version, found := versions[record.KnowledgeObjectID]
			if !found || record.ObjectVersion != version.ObjectVersion ||
				record.SealFound != 1 || record.SealDependencyCount != version.DependencyCount {
				return fmt.Errorf("%w: dependency physical preflight identity disagrees", ErrCorrupt)
			}
			seenPhysicalSources[record.KnowledgeObjectID] = struct{}{}
			switch record.DependencyFound {
			case 0:
				if record.TargetKindBytes != -1 || record.TargetIDBytes != -1 ||
					record.DependencyRoleBytes != -1 {
					return fmt.Errorf("%w: dependency physical preflight sentinel is invalid", ErrCorrupt)
				}
			case 1:
				if record.TargetKindBytes != int64(len("object")) ||
					record.TargetIDBytes < 1 || record.TargetIDBytes > maximumObjectIDBytes ||
					record.DependencyRoleBytes != int64(len("field_input")) {
					return fmt.Errorf("%w: immutable dependency row width is invalid", ErrCorrupt)
				}
				physicalDependencyRows++
				if physicalDependencyRows > remaining {
					return fmt.Errorf("%w: dependency row budget exceeded", control.ErrCapacityExceeded)
				}
			default:
				return fmt.Errorf("%w: dependency physical preflight marker is invalid", ErrCorrupt)
			}
		}
		if len(seenPhysicalSources) != len(chunk) {
			return fmt.Errorf("%w: dependency physical preflight set is incomplete", ErrCorrupt)
		}
		var aggregates []dependencyAggregateRecord
		err := database.Table("knowledge_objects AS source_registry").
			Joins(`JOIN knowledge_object_versions AS source
				ON source.tenant_id = source_registry.tenant_id
			   AND source.knowledge_object_id = source_registry.knowledge_object_id
			   AND source.object_version = source_registry.current_version`).
			Joins(`LEFT JOIN knowledge_object_dependency_seals AS seal
				ON seal.tenant_id = source.tenant_id
			   AND seal.knowledge_object_id = source.knowledge_object_id
			   AND seal.object_version = source.object_version`).
			Joins(`LEFT JOIN knowledge_object_dependencies AS dependency
				ON dependency.tenant_id = source.tenant_id
			   AND dependency.source_object_id = source.knowledge_object_id
			   AND dependency.source_object_version = source.object_version`).
			Joins(`LEFT JOIN knowledge_object_versions AS target
				ON target.tenant_id = dependency.tenant_id
			   AND target.knowledge_object_id = dependency.target_object_id
			   AND target.object_version = dependency.target_object_version`).
			Joins(`LEFT JOIN knowledge_objects AS target_registry
				ON target_registry.tenant_id = target.tenant_id
			   AND target_registry.knowledge_object_id = target.knowledge_object_id
			   AND target_registry.current_version >= target.object_version`).
			Where("source_registry.tenant_id = ? AND source_registry.knowledge_object_id IN ?", tenantID, chunk).
			Group("source.knowledge_object_id, source.object_version").
			Order("source.knowledge_object_id ASC").
			Select(`
				source.knowledge_object_id AS knowledge_object_id,
				source.object_version AS object_version,
				max(CASE WHEN seal.knowledge_object_id IS NULL THEN 0 ELSE 1 END) AS seal_found,
				coalesce(max(seal.dependency_count), -1) AS seal_dependency_count,
				count(dependency.ordinal) AS dependency_count,
				count(DISTINCT dependency.ordinal) AS distinct_ordinal_count,
				coalesce(min(dependency.ordinal), -1) AS minimum_ordinal,
				coalesce(max(dependency.ordinal), -1) AS maximum_ordinal,
				coalesce(sum(dependency.ordinal), 0) AS ordinal_sum,
				coalesce(sum(CASE WHEN dependency.ordinal IS NOT NULL AND (
					length(CAST(dependency.tenant_id AS BLOB)) NOT BETWEEN 1 AND 255
					OR length(CAST(dependency.source_object_id AS BLOB)) NOT BETWEEN 1 AND 128
					OR dependency.ordinal NOT BETWEEN 0 AND 1023
					OR dependency.target_kind <> 'object'
					OR length(CAST(dependency.target_object_id AS BLOB)) NOT BETWEEN 1 AND 128
					OR instr(CAST(dependency.target_object_id AS BLOB), X'00') <> 0
					OR dependency.target_object_id <> trim(dependency.target_object_id)
					OR dependency.target_object_id GLOB (
						'*[' || char(1) || '-' || char(31)
						|| char(127) || '-' || char(159) || ']*'
					)
					OR dependency.target_object_version < 1
					OR dependency.dependency_role <> 'field_input'
					OR target.knowledge_object_id IS NULL
					OR target_registry.knowledge_object_id IS NULL
					OR ` + invalidPinnedActiveDependencySQL + `
					OR ` + invalidCurrentActiveDependencySQL + `
					OR (dependency.target_object_id = source.knowledge_object_id
						AND dependency.target_object_version = source.object_version)
				) THEN 1 ELSE 0 END), 0) AS invalid_count
			`).
			Limit(len(chunk) + 1).
			Find(&aggregates).Error
		if err != nil {
			return err
		}
		if len(aggregates) != len(chunk) {
			return fmt.Errorf("%w: dependency aggregate set is incomplete", ErrCorrupt)
		}
		seen := make(map[string]struct{}, len(aggregates))
		var expectedRows int64
		for _, aggregate := range aggregates {
			version, found := versions[aggregate.KnowledgeObjectID]
			if !found || aggregate.ObjectVersion != version.ObjectVersion {
				return fmt.Errorf("%w: dependency aggregate identity disagrees", ErrCorrupt)
			}
			if _, duplicate := seen[aggregate.KnowledgeObjectID]; duplicate {
				return fmt.Errorf("%w: duplicate dependency aggregate", ErrCorrupt)
			}
			seen[aggregate.KnowledgeObjectID] = struct{}{}
			expectedSum := version.DependencyCount * (version.DependencyCount - 1) / 2
			minimumOrdinal, maximumOrdinal := int64(-1), int64(-1)
			if version.DependencyCount > 0 {
				minimumOrdinal = 0
				maximumOrdinal = version.DependencyCount - 1
			}
			if aggregate.SealFound != 1 || aggregate.SealDependencyCount != version.DependencyCount ||
				aggregate.DependencyCount != version.DependencyCount ||
				aggregate.DistinctOrdinalCount != version.DependencyCount ||
				aggregate.MinimumOrdinal != minimumOrdinal || aggregate.MaximumOrdinal != maximumOrdinal ||
				aggregate.OrdinalSum != expectedSum || aggregate.InvalidCount != 0 {
				return fmt.Errorf("%w: immutable dependency set is invalid", ErrCorrupt)
			}
			if expectedRows > maximumRows-version.DependencyCount {
				return fmt.Errorf("%w: dependency row budget exceeded", control.ErrCapacityExceeded)
			}
			expectedRows += version.DependencyCount
		}
		if expectedRows > remaining {
			return fmt.Errorf("%w: dependency row budget exceeded", control.ErrCapacityExceeded)
		}
		if expectedRows != physicalDependencyRows {
			return fmt.Errorf("%w: dependency aggregate and physical row set disagree", ErrCorrupt)
		}
		// The aggregate is already the complete dependency/seal authority for
		// this chunk. When every sealed count is zero, it proves there are no
		// rows to hydrate, so avoid a redundant empty dependency payload scan.
		if expectedRows == 0 {
			return nil
		}

		dependencyQuery := database.Table("knowledge_object_dependencies AS dependency").
			Joins(`JOIN knowledge_objects AS current
				ON current.tenant_id = dependency.tenant_id
			   AND current.knowledge_object_id = dependency.source_object_id
			   AND current.current_version = dependency.source_object_version`).
			Where("current.tenant_id = ? AND current.knowledge_object_id IN ?", tenantID, chunk)
		var records []dependencyRecord
		if err := dependencyQuery.Session(&gorm.Session{}).
			Select("dependency.*, 1 AS target_found").
			Order("dependency.source_object_id ASC").
			Order("dependency.ordinal ASC").
			Limit(int(expectedRows) + 1).
			Find(&records).Error; err != nil {
			return err
		}
		if int64(len(records)) != expectedRows {
			return fmt.Errorf("%w: dependency target preflight changed", ErrCorrupt)
		}
		for _, record := range records {
			if !validIdentity(record.TargetObjectID, maximumObjectIDBytes) {
				return fmt.Errorf("%w: immutable dependency target identity is invalid", ErrCorrupt)
			}
			if _, found := recordsByObjectID[record.SourceObjectID]; !found {
				return fmt.Errorf("%w: unexpected dependency source in batch", ErrCorrupt)
			}
			recordsByObjectID[record.SourceObjectID] = append(
				recordsByObjectID[record.SourceObjectID],
				record,
			)
		}
		rowsRead += int64(len(records))
		for _, aggregate := range aggregates {
			version := versions[aggregate.KnowledgeObjectID]
			if err := validateDependencyRecords(version, recordsByObjectID[aggregate.KnowledgeObjectID]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recordsByObjectID, nil
}

func currentDependencyPhysicalPreflightQuery(database *gorm.DB, tenantID string, objectIDs []string) *gorm.DB {
	return database.Table("knowledge_objects AS source_registry").
		Joins(`JOIN knowledge_object_versions AS source
			ON source.tenant_id = source_registry.tenant_id
		   AND source.knowledge_object_id = source_registry.knowledge_object_id
		   AND source.object_version = source_registry.current_version`).
		Joins(`LEFT JOIN knowledge_object_dependency_seals AS seal
			ON seal.tenant_id = source.tenant_id
		   AND seal.knowledge_object_id = source.knowledge_object_id
		   AND seal.object_version = source.object_version`).
		Joins(`LEFT JOIN knowledge_object_dependencies AS dependency
			ON dependency.tenant_id = source.tenant_id
		   AND dependency.source_object_id = source.knowledge_object_id
		   AND dependency.source_object_version = source.object_version`).
		Where("source_registry.tenant_id = ? AND source_registry.knowledge_object_id IN ?", tenantID, objectIDs).
		Order("source_registry.knowledge_object_id ASC").
		Order("dependency.ordinal ASC")
}
