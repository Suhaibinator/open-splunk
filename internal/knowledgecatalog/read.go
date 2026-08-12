package knowledgecatalog

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

type registrySizeRecord struct {
	KnowledgeObjectIDBytes int64 `gorm:"column:knowledge_object_id_bytes"`
	TenantIDBytes          int64 `gorm:"column:tenant_id_bytes"`
	AppIDBytes             int64 `gorm:"column:app_id_bytes"`
	OwnerIDBytes           int64 `gorm:"column:owner_id_bytes"`
	ObjectTypeBytes        int64 `gorm:"column:object_type_bytes"`
	NameBytes              int64 `gorm:"column:name_bytes"`
	SharingScopeBytes      int64 `gorm:"column:sharing_scope_bytes"`
	StateBytes             int64 `gorm:"column:state_bytes"`
	DigestBytes            int64 `gorm:"column:digest_bytes"`
	QuarantineReasonBytes  int64 `gorm:"column:quarantine_reason_bytes"`
}

func readRegistryRecord(query *gorm.DB) (registryRecord, bool, error) {
	if query == nil {
		return registryRecord{}, false, fmt.Errorf("%w: nil registry query", ErrCorrupt)
	}
	var sizes []registrySizeRecord
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(app_id AS BLOB)) AS app_id_bytes,
		length(CAST(owner_id AS BLOB)) AS owner_id_bytes,
		length(CAST(object_type AS BLOB)) AS object_type_bytes,
		length(CAST(name AS BLOB)) AS name_bytes,
		length(CAST(sharing_scope AS BLOB)) AS sharing_scope_bytes,
		length(CAST(state AS BLOB)) AS state_bytes,
		coalesce(length(definition_digest), 0) AS digest_bytes,
		coalesce(length(CAST(quarantine_reason AS BLOB)), 0) AS quarantine_reason_bytes
	`).Limit(2).Find(&sizes).Error; err != nil {
		return registryRecord{}, false, err
	}
	if len(sizes) == 0 {
		return registryRecord{}, false, nil
	}
	if len(sizes) != 1 || !validRegistrySize(sizes[0]) {
		return registryRecord{}, false, fmt.Errorf("%w: registry row width is invalid", ErrCorrupt)
	}
	var records []registryRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&records).Error; err != nil {
		return registryRecord{}, false, err
	}
	if len(records) != 1 {
		return registryRecord{}, false, fmt.Errorf("%w: registry preflight changed", ErrCorrupt)
	}
	return records[0], true, nil
}

func validRegistrySize(size registrySizeRecord) bool {
	return size.KnowledgeObjectIDBytes >= 1 && size.KnowledgeObjectIDBytes <= maximumObjectIDBytes &&
		size.TenantIDBytes >= 1 && size.TenantIDBytes <= maximumTenantIDBytes &&
		size.AppIDBytes >= 1 && size.AppIDBytes <= maximumAppIDBytes &&
		size.OwnerIDBytes >= 1 && size.OwnerIDBytes <= maximumOwnerIDBytes &&
		size.ObjectTypeBytes >= 1 && size.ObjectTypeBytes <= int64(len(ObjectTypeFieldExtraction)) &&
		size.NameBytes >= 1 && size.NameBytes <= maximumFilterBytes &&
		size.SharingScopeBytes >= int64(len(SharingScopeApp)) && size.SharingScopeBytes <= int64(len(SharingScopePrivate)) &&
		size.StateBytes >= int64(len(StateDraft)) && size.StateBytes <= int64(len(StateQuarantined)) &&
		(size.DigestBytes == 0 || size.DigestBytes == persistedKnowledgeDefinitionDigestBytes) &&
		size.QuarantineReasonBytes <= maximumPersistedQuarantineReasonBytes
}

type versionSizeRecord struct {
	RegistrySizeRecord registrySizeRecord `gorm:"embedded"`
	MutationKindBytes  int64              `gorm:"column:mutation_kind_bytes"`
}

func validVersionSize(size versionSizeRecord) bool {
	return validRegistrySize(size.RegistrySizeRecord) &&
		size.MutationKindBytes >= minimumPersistedMutationKindBytes &&
		size.MutationKindBytes <= maximumPersistedMutationKindBytes
}

func readVersionRecord(database *gorm.DB, tenantID, objectID string, version int64) (versionRecord, bool, error) {
	query := database.Model(&versionRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
		tenantID, objectID, version,
	)
	var sizes []versionSizeRecord
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(app_id AS BLOB)) AS app_id_bytes,
		length(CAST(owner_id AS BLOB)) AS owner_id_bytes,
		length(CAST(object_type AS BLOB)) AS object_type_bytes,
		length(CAST(name AS BLOB)) AS name_bytes,
		length(CAST(sharing_scope AS BLOB)) AS sharing_scope_bytes,
		length(CAST(state AS BLOB)) AS state_bytes,
		coalesce(length(definition_digest), 0) AS digest_bytes,
		coalesce(length(CAST(quarantine_reason AS BLOB)), 0) AS quarantine_reason_bytes,
		length(CAST(mutation_kind AS BLOB)) AS mutation_kind_bytes
	`).Limit(2).Find(&sizes).Error; err != nil {
		return versionRecord{}, false, err
	}
	if len(sizes) == 0 {
		return versionRecord{}, false, nil
	}
	if len(sizes) != 1 || !validVersionSize(sizes[0]) {
		return versionRecord{}, false, fmt.Errorf("%w: version row width is invalid", ErrCorrupt)
	}
	var records []versionRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&records).Error; err != nil {
		return versionRecord{}, false, err
	}
	if len(records) != 1 {
		return versionRecord{}, false, fmt.Errorf("%w: version preflight changed", ErrCorrupt)
	}
	return records[0], true, nil
}

func readDefinitionBlob(database *gorm.DB, tenantID string, digest []byte) (definitionBlobRecord, error) {
	query := database.Model(&definitionBlobRecord{}).Where(
		"tenant_id = ? AND definition_digest = ?", tenantID, digest,
	)
	var sizes []struct {
		TenantIDBytes int64 `gorm:"column:tenant_id_bytes"`
		DigestBytes   int64 `gorm:"column:digest_bytes"`
		ProtoBytes    int64 `gorm:"column:proto_bytes"`
	}
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(definition_digest) AS digest_bytes,
		length(definition_proto) AS proto_bytes
	`).Limit(2).Find(&sizes).Error; err != nil {
		return definitionBlobRecord{}, err
	}
	if len(sizes) != 1 || !validDefinitionBlobWidth(
		sizes[0].TenantIDBytes,
		sizes[0].DigestBytes,
		sizes[0].ProtoBytes,
	) {
		return definitionBlobRecord{}, fmt.Errorf("%w: definition blob is missing or oversized", ErrCorrupt)
	}
	var records []definitionBlobRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&records).Error; err != nil {
		return definitionBlobRecord{}, err
	}
	if len(records) != 1 || int64(len(records[0].DefinitionProto)) != sizes[0].ProtoBytes ||
		records[0].DefinitionBytes != sizes[0].ProtoBytes {
		return definitionBlobRecord{}, fmt.Errorf("%w: definition blob preflight changed", ErrCorrupt)
	}
	return records[0], nil
}

func validDefinitionBlobWidth(tenantIDBytes, digestBytes, protoBytes int64) bool {
	return tenantIDBytes >= 1 && tenantIDBytes <= maximumTenantIDBytes &&
		digestBytes == persistedKnowledgeDefinitionDigestBytes &&
		protoBytes >= 1 && protoBytes <= maximumDefinitionBytes
}

type projectionSizeRecord struct {
	RegistrySizeRecord         registrySizeRecord `gorm:"embedded"`
	DescriptionBytes           int64              `gorm:"column:description_bytes"`
	SelectorValueBytes         int64              `gorm:"column:selector_value_bytes"`
	ProjectionBytes            int64              `gorm:"column:projection_bytes"`
	SealProjectionBytes        int64              `gorm:"column:seal_projection_bytes"`
	CanonicalSelectorBytes     int64              `gorm:"column:canonical_selector_bytes"`
	SealCanonicalSelectorBytes int64              `gorm:"column:seal_canonical_selector_bytes"`
}

// projectionReadRecord keeps the detached projection and its defensive width
// aliases in one SQLite result row.  The ordered authorized query can require
// a bounded sorter, so executing a separate width preflight would duplicate
// that work and reopen a same-transaction consistency seam for no benefit.
type projectionReadRecord struct {
	Record projectionRecord `gorm:"embedded"`

	KnowledgeObjectIDBytes     int64 `gorm:"column:width_knowledge_object_id_bytes"`
	TenantIDBytes              int64 `gorm:"column:width_tenant_id_bytes"`
	AppIDBytes                 int64 `gorm:"column:width_app_id_bytes"`
	OwnerIDBytes               int64 `gorm:"column:width_owner_id_bytes"`
	ObjectTypeBytes            int64 `gorm:"column:width_object_type_bytes"`
	NameBytes                  int64 `gorm:"column:width_name_bytes"`
	SharingScopeBytes          int64 `gorm:"column:width_sharing_scope_bytes"`
	StateBytes                 int64 `gorm:"column:width_state_bytes"`
	DigestBytes                int64 `gorm:"column:width_digest_bytes"`
	QuarantineReasonBytes      int64 `gorm:"column:width_quarantine_reason_bytes"`
	DescriptionBytes           int64 `gorm:"column:width_description_bytes"`
	SelectorValueBytes         int64 `gorm:"column:width_selector_value_bytes"`
	ProjectionBytes            int64 `gorm:"column:width_projection_bytes"`
	SealProjectionBytes        int64 `gorm:"column:width_seal_projection_bytes"`
	CanonicalSelectorBytes     int64 `gorm:"column:width_canonical_selector_bytes"`
	SealCanonicalSelectorBytes int64 `gorm:"column:width_seal_canonical_selector_bytes"`
}

func (record projectionReadRecord) sizeRecord() projectionSizeRecord {
	return projectionSizeRecord{
		RegistrySizeRecord: registrySizeRecord{
			KnowledgeObjectIDBytes: record.KnowledgeObjectIDBytes,
			TenantIDBytes:          record.TenantIDBytes,
			AppIDBytes:             record.AppIDBytes,
			OwnerIDBytes:           record.OwnerIDBytes,
			ObjectTypeBytes:        record.ObjectTypeBytes,
			NameBytes:              record.NameBytes,
			SharingScopeBytes:      record.SharingScopeBytes,
			StateBytes:             record.StateBytes,
			DigestBytes:            record.DigestBytes,
			QuarantineReasonBytes:  record.QuarantineReasonBytes,
		},
		DescriptionBytes:           record.DescriptionBytes,
		SelectorValueBytes:         record.SelectorValueBytes,
		ProjectionBytes:            record.ProjectionBytes,
		SealProjectionBytes:        record.SealProjectionBytes,
		CanonicalSelectorBytes:     record.CanonicalSelectorBytes,
		SealCanonicalSelectorBytes: record.SealCanonicalSelectorBytes,
	}
}

func readProjectionRecords(query *gorm.DB, limit int) ([]projectionRecord, error) {
	return readProjectionRecordsBounded(
		query,
		limit,
		int64(limit)*maximumProjectionBytes,
	)
}

func readProjectionRecordsBounded(
	query *gorm.DB,
	limit int,
	maximumAggregateProjectionBytes int64,
) ([]projectionRecord, error) {
	if query == nil || limit < 1 || limit > maximumObjectsPerTenant+1 {
		return nil, fmt.Errorf("%w: invalid projection read bound", ErrCorrupt)
	}
	if maximumAggregateProjectionBytes < maximumProjectionBytes {
		return nil, fmt.Errorf("%w: invalid aggregate projection read bound", ErrCorrupt)
	}
	readQuery := query.Session(&gorm.Session{}).Limit(limit)
	rows, err := readQuery.Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]projectionRecord, 0, min(limit, MaximumPageSize+1))
	var aggregateProjectionBytes int64
	for rows.Next() {
		var scannedRecord projectionReadRecord
		if err := readQuery.ScanRows(rows, &scannedRecord); err != nil {
			return nil, err
		}
		size := scannedRecord.sizeRecord()
		if !validRegistrySize(size.RegistrySizeRecord) || size.DescriptionBytes < 0 ||
			size.DescriptionBytes > maximumDescriptionBytes || size.SelectorValueBytes < 0 ||
			size.SelectorValueBytes > maximumSelectorAggregateValueBytes ||
			size.ProjectionBytes != size.DescriptionBytes+size.SelectorValueBytes ||
			size.SealProjectionBytes != size.ProjectionBytes ||
			size.CanonicalSelectorBytes < 0 || size.CanonicalSelectorBytes > maximumCanonicalSelectorBytes ||
			size.SealCanonicalSelectorBytes != size.CanonicalSelectorBytes {
			return nil, fmt.Errorf("%w: list projection width is invalid", ErrCorrupt)
		}
		if aggregateProjectionBytes > maximumAggregateProjectionBytes-size.ProjectionBytes {
			return nil, fmt.Errorf("%w: aggregate list projection byte bound exceeded", control.ErrCapacityExceeded)
		}
		aggregateProjectionBytes += size.ProjectionBytes
		records = append(records, scannedRecord.Record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func readDependencySeal(
	database *gorm.DB,
	tenantID, objectID string,
	version int64,
) (dependencySealRecord, error) {
	var records []dependencySealRecord
	err := database.Table("knowledge_object_dependency_seals").Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
		tenantID, objectID, version,
	).Limit(2).Find(&records).Error
	if err != nil {
		return dependencySealRecord{}, err
	}
	if len(records) != 1 || records[0].TenantID != tenantID ||
		records[0].KnowledgeObjectID != objectID || records[0].ObjectVersion != version ||
		records[0].DependencyCount < 0 || records[0].DependencyCount > maximumDependenciesPerVersion {
		return dependencySealRecord{}, fmt.Errorf("%w: immutable dependency seal is missing or invalid", ErrCorrupt)
	}
	return records[0], nil
}

func readDependencyRecords(
	database *gorm.DB,
	source versionRecord,
) ([]dependencyRecord, error) {
	query := database.Table("knowledge_object_dependencies AS dependency").
		Joins(`JOIN knowledge_object_versions AS source
			ON source.tenant_id = dependency.tenant_id
		   AND source.knowledge_object_id = dependency.source_object_id
		   AND source.object_version = dependency.source_object_version`).
		Joins(`LEFT JOIN knowledge_objects AS source_registry
			ON source_registry.tenant_id = source.tenant_id
		   AND source_registry.knowledge_object_id = source.knowledge_object_id
		   AND source_registry.current_version = source.object_version`).
		Joins(`LEFT JOIN knowledge_object_versions AS target
			ON target.tenant_id = dependency.tenant_id
		   AND target.knowledge_object_id = dependency.target_object_id
		   AND target.object_version = dependency.target_object_version`).
		Joins(`LEFT JOIN knowledge_objects AS target_registry
			ON target_registry.tenant_id = target.tenant_id
		   AND target_registry.knowledge_object_id = target.knowledge_object_id
		   AND target_registry.current_version >= target.object_version`).
		Where(`dependency.tenant_id = ?
			AND dependency.source_object_id = ?
			AND dependency.source_object_version = ?`, source.TenantID, source.KnowledgeObjectID, source.ObjectVersion).
		Order("dependency.ordinal ASC")
	var sizes []struct {
		TenantIDBytes       int64 `gorm:"column:tenant_id_bytes"`
		SourceIDBytes       int64 `gorm:"column:source_id_bytes"`
		TargetKindBytes     int64 `gorm:"column:target_kind_bytes"`
		TargetIDBytes       int64 `gorm:"column:target_id_bytes"`
		DependencyRoleBytes int64 `gorm:"column:dependency_role_bytes"`
	}
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(dependency.tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(dependency.source_object_id AS BLOB)) AS source_id_bytes,
		length(CAST(dependency.target_kind AS BLOB)) AS target_kind_bytes,
		length(CAST(dependency.target_object_id AS BLOB)) AS target_id_bytes,
		length(CAST(dependency.dependency_role AS BLOB)) AS dependency_role_bytes
	`).Limit(maximumDependenciesPerVersion + 1).Find(&sizes).Error; err != nil {
		return nil, err
	}
	if len(sizes) > maximumDependenciesPerVersion {
		return nil, fmt.Errorf("%w: immutable dependency set exceeds its read bound", ErrCorrupt)
	}
	for _, size := range sizes {
		if size.TenantIDBytes < 1 || size.TenantIDBytes > maximumTenantIDBytes ||
			size.SourceIDBytes < 1 || size.SourceIDBytes > maximumObjectIDBytes ||
			size.TargetKindBytes != int64(len("object")) ||
			size.TargetIDBytes < 1 || size.TargetIDBytes > maximumObjectIDBytes ||
			size.DependencyRoleBytes != int64(len("field_input")) {
			return nil, fmt.Errorf("%w: immutable dependency row width is invalid", ErrCorrupt)
		}
	}
	var records []dependencyRecord
	if err := query.Session(&gorm.Session{}).Select(`
		dependency.*,
		CASE WHEN target.knowledge_object_id IS NULL OR target_registry.knowledge_object_id IS NULL OR ` +
		invalidPinnedActiveDependencySQL + ` OR ` + invalidCurrentActiveDependencySQL + `
			THEN 0 ELSE 1 END AS target_found
	`).Limit(maximumDependenciesPerVersion + 1).Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: immutable dependency preflight changed", ErrCorrupt)
	}
	return records, nil
}

const invalidPinnedActiveDependencySQL = `(source.state = 'active' AND (
	target.state <> 'active'
	OR NOT (
		(source.sharing_scope = 'private' AND (
			target.sharing_scope = 'global'
			OR (target.sharing_scope = 'app' AND target.app_id = source.app_id)
			OR (target.sharing_scope = 'private'
				AND target.app_id = source.app_id
				AND target.owner_id = source.owner_id)
		))
		OR (source.sharing_scope = 'app' AND (
			target.sharing_scope = 'global'
			OR (target.sharing_scope = 'app' AND target.app_id = source.app_id)
		))
		OR (source.sharing_scope = 'global' AND target.sharing_scope = 'global')
	)
))`

const invalidCurrentActiveDependencySQL = `(source_registry.state = 'active'
	AND target_registry.state <> 'active')`

func readSelectorRecords(database *gorm.DB, tenantID, objectID string, version int64) ([]selectorRecord, error) {
	// Cardinality and width are corruption boundaries, so prove them with a
	// primary-key-compatible walk before asking SQLite to perform the canonical
	// dimension ordering.  In particular, a check-bypassed oversized selector
	// set must hit LIMIT without first filling a temporary CASE-order sorter.
	preflightQuery := selectorRecordPreflightQuery(database, tenantID, objectID, version)
	var sizes []struct {
		DimensionBytes int64 `gorm:"column:dimension_bytes"`
		MatchKindBytes int64 `gorm:"column:match_kind_bytes"`
		ValueBytes     int64 `gorm:"column:value_bytes"`
	}
	if err := preflightQuery.Session(&gorm.Session{}).Select(`
		length(CAST(dimension AS BLOB)) AS dimension_bytes,
		length(CAST(match_kind AS BLOB)) AS match_kind_bytes,
		length(CAST(value AS BLOB)) AS value_bytes
	`).Limit(maximumSelectorPatterns + 1).Find(&sizes).Error; err != nil {
		return nil, err
	}
	if len(sizes) > maximumSelectorPatterns {
		return nil, fmt.Errorf("%w: too many selector projection rows", ErrCorrupt)
	}
	for _, size := range sizes {
		if !validSelectorProjectionWidth(size.DimensionBytes, size.MatchKindBytes, size.ValueBytes) {
			return nil, fmt.Errorf("%w: selector projection width is invalid", ErrCorrupt)
		}
	}
	payloadQuery := selectorRecordBaseQuery(database, tenantID, objectID, version).Order(`CASE dimension
		WHEN 'index' THEN 1 WHEN 'host' THEN 2
		WHEN 'source' THEN 3 WHEN 'sourcetype' THEN 4 ELSE 5 END ASC`).
		Order("ordinal ASC")
	var records []selectorRecord
	if err := payloadQuery.Session(&gorm.Session{}).Select("dimension, ordinal, match_kind, value, value_bytes").
		Limit(maximumSelectorPatterns + 1).Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: selector projection preflight changed", ErrCorrupt)
	}
	return records, nil
}

func selectorRecordBaseQuery(database *gorm.DB, tenantID, objectID string, version int64) *gorm.DB {
	return database.Table("knowledge_object_list_selector_patterns").Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
		tenantID, objectID, version,
	)
}

func selectorRecordPreflightQuery(database *gorm.DB, tenantID, objectID string, version int64) *gorm.DB {
	return selectorRecordBaseQuery(database, tenantID, objectID, version).
		Order("dimension ASC").
		Order("ordinal ASC")
}

func validSelectorProjectionWidth(dimensionBytes, matchKindBytes, valueBytes int64) bool {
	return dimensionBytes >= minimumPersistedSelectorDimensionBytes &&
		dimensionBytes <= maximumPersistedSelectorDimensionBytes &&
		matchKindBytes >= minimumPersistedSelectorMatchKindBytes &&
		matchKindBytes <= maximumPersistedSelectorMatchKindBytes &&
		valueBytes >= 1 && valueBytes <= maximumSelectorValueBytes
}
