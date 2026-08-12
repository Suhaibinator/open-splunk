package knowledgecatalog

import (
	"fmt"

	"gorm.io/gorm"
)

type graphDependencyReadRecord struct {
	Record dependencyRecord `gorm:"embedded"`

	TenantIDBytes       int64 `gorm:"column:width_tenant_id_bytes"`
	SourceIDBytes       int64 `gorm:"column:width_source_id_bytes"`
	TargetKindBytes     int64 `gorm:"column:width_target_kind_bytes"`
	TargetIDBytes       int64 `gorm:"column:width_target_id_bytes"`
	DependencyRoleBytes int64 `gorm:"column:width_dependency_role_bytes"`
}

func incomingGraphBaseQuery(
	database *gorm.DB,
	scope normalizedScope,
	root versionRecord,
) *gorm.DB {
	query := applyCurrentAuthorization(database, scope, currentAuthorizationDriver{
		table:           "knowledge_objects",
		globalIndex:     "knowledge_objects_authorized_global_idx",
		appIndex:        "knowledge_objects_authorized_app_idx",
		privateIndex:    "knowledge_objects_authorized_private_idx",
		identityColumns: "tenant_id, knowledge_object_id",
		driverAlias:     "authorized_source",
		outerTable:      "knowledge_objects AS source",
		tenantColumn:    "source.tenant_id",
		joinIdentity: `authorized_source.tenant_id = source.tenant_id
			AND authorized_source.knowledge_object_id = source.knowledge_object_id`,
	})
	// Keep target_kind in the exact source-index probe. Non-object rows are
	// outside this v1 object graph and are rejected when their authorized source
	// is hydrated or read outgoing. Discovering such constraint-bypassed rows
	// from the target side would require a new bounded current-inverse authority;
	// dropping this predicate would instead scan every current source edge.
	return query.Joins(`CROSS JOIN knowledge_object_dependencies AS dependency
		INDEXED BY knowledge_object_dependencies_source_target_idx`).
		Where("source.state <> ?", StateQuarantined).
		Where(`dependency.tenant_id = source.tenant_id
			AND dependency.source_object_id = source.knowledge_object_id
			AND dependency.source_object_version = source.current_version
			AND dependency.target_kind = 'object'
			AND dependency.target_object_id = ?
			AND dependency.target_object_version = ?`, root.KnowledgeObjectID, root.ObjectVersion)
}

func readIncomingGraphCandidates(
	database *gorm.DB,
	request normalizedDependencyListRequest,
	root versionRecord,
	cursor graphCursor,
	limit int,
) ([]dependencyRecord, error) {
	if limit < 1 || limit > MaximumPageSize+1 {
		return nil, fmt.Errorf("%w: incoming graph page bound is invalid", ErrCorrupt)
	}
	query := incomingGraphCandidateReadQuery(database, request, root, cursor, limit)
	rows, err := query.Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]dependencyRecord, 0, limit)
	for rows.Next() {
		var scanned graphDependencyReadRecord
		if err := query.ScanRows(rows, &scanned); err != nil {
			return nil, err
		}
		record := scanned.Record
		if scanned.TenantIDBytes < 1 || scanned.TenantIDBytes > maximumTenantIDBytes ||
			scanned.SourceIDBytes < 1 || scanned.SourceIDBytes > maximumObjectIDBytes ||
			scanned.TargetKindBytes != int64(len("object")) ||
			scanned.TargetIDBytes < 1 || scanned.TargetIDBytes > maximumObjectIDBytes ||
			scanned.DependencyRoleBytes != int64(len("field_input")) ||
			record.TenantID != request.scope.tenantID ||
			!validIdentity(record.SourceObjectID, maximumObjectIDBytes) ||
			record.SourceObjectVersion < 1 || record.SourceObjectVersion > maximumVersionsPerTenant ||
			record.Ordinal < 0 || record.Ordinal >= maximumDependenciesPerVersion ||
			record.TargetKind != "object" || record.TargetObjectID != root.KnowledgeObjectID ||
			record.TargetObjectVersion != root.ObjectVersion || record.DependencyRole != "field_input" ||
			record.TargetFound != 1 ||
			(record.SourceObjectID == record.TargetObjectID &&
				record.SourceObjectVersion == record.TargetObjectVersion) {
			return nil, fmt.Errorf("%w: incoming graph edge is invalid", ErrCorrupt)
		}
		if len(records) > 0 && records[len(records)-1].SourceObjectID >= record.SourceObjectID {
			return nil, fmt.Errorf("%w: incoming graph edge order is invalid", ErrCorrupt)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func incomingGraphCandidateReadQuery(
	database *gorm.DB,
	request normalizedDependencyListRequest,
	root versionRecord,
	cursor graphCursor,
	limit int,
) *gorm.DB {
	query := incomingGraphBaseQuery(database, request.scope, root)
	if cursor.CatalogRevision != 0 {
		// Source identities are unique in the current registry, so this leading
		// component is a complete keyset predicate. The signed cursor still binds
		// the entire edge key and cursorMatchesGraphRoot verifies its fixed target.
		query = query.Where("source.knowledge_object_id > ?", cursor.LastSourceObjectID)
	}
	query = query.Order("source.knowledge_object_id ASC").
		Order("source.current_version ASC").
		Order("dependency.target_object_id ASC").
		Order("dependency.target_object_version ASC").
		Order("dependency.dependency_role ASC")
	return query.Session(&gorm.Session{}).Select(fmt.Sprintf(`
		CASE WHEN length(CAST(dependency.tenant_id AS BLOB)) BETWEEN 1 AND %[1]d
			THEN dependency.tenant_id ELSE '' END AS tenant_id,
		CASE WHEN length(CAST(dependency.source_object_id AS BLOB)) BETWEEN 1 AND %[2]d
			THEN dependency.source_object_id ELSE '' END AS source_object_id,
		dependency.source_object_version,
		dependency.ordinal,
		CASE WHEN length(CAST(dependency.target_kind AS BLOB)) = %[3]d
			THEN dependency.target_kind ELSE '' END AS target_kind,
		CASE WHEN length(CAST(dependency.target_object_id AS BLOB)) BETWEEN 1 AND %[2]d
			THEN dependency.target_object_id ELSE '' END AS target_object_id,
		dependency.target_object_version,
		CASE WHEN length(CAST(dependency.dependency_role AS BLOB)) = %[4]d
			THEN dependency.dependency_role ELSE '' END AS dependency_role,
		1 AS target_found,
		length(CAST(dependency.tenant_id AS BLOB)) AS width_tenant_id_bytes,
		length(CAST(dependency.source_object_id AS BLOB)) AS width_source_id_bytes,
		length(CAST(dependency.target_kind AS BLOB)) AS width_target_kind_bytes,
		length(CAST(dependency.target_object_id AS BLOB)) AS width_target_id_bytes,
		length(CAST(dependency.dependency_role AS BLOB)) AS width_dependency_role_bytes
	`, maximumTenantIDBytes, maximumObjectIDBytes, len("object"), len("field_input"))).Limit(limit)
}

func countIncomingGraphCandidates(
	database *gorm.DB,
	scope normalizedScope,
	root versionRecord,
) (int64, error) {
	bounded := incomingGraphBaseQuery(database, scope, root).
		Select("1").
		Limit(maximumObjectsPerTenant + 1)
	var count int64
	if err := database.Table("(?) AS bounded_incoming_graph", bounded).Count(&count).Error; err != nil {
		return 0, err
	}
	if count < 0 || count > maximumObjectsPerTenant {
		return 0, fmt.Errorf("%w: disclosed incoming graph count is invalid", ErrCorrupt)
	}
	return count, nil
}
