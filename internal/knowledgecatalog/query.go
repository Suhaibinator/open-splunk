package knowledgecatalog

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

type currentAuthorizationDriver struct {
	table           string
	globalIndex     string
	appIndex        string
	privateIndex    string
	identityColumns string
	driverAlias     string
	outerTable      string
	tenantColumn    string
	joinIdentity    string
}

func applyObjectAuthorization(query *gorm.DB, scope normalizedScope) *gorm.DB {
	return applyCurrentAuthorization(query, scope, currentAuthorizationDriver{
		table:           "knowledge_objects",
		globalIndex:     "knowledge_objects_authorized_global_idx",
		appIndex:        "knowledge_objects_authorized_app_idx",
		privateIndex:    "knowledge_objects_authorized_private_idx",
		identityColumns: "tenant_id, knowledge_object_id",
		driverAlias:     "authorized_object",
		outerTable:      "knowledge_objects AS object",
		tenantColumn:    "object.tenant_id",
		joinIdentity: `authorized_object.tenant_id = object.tenant_id
			AND authorized_object.knowledge_object_id = object.knowledge_object_id`,
	})
}

func applyProjectionAuthorization(query *gorm.DB, scope normalizedScope) *gorm.DB {
	// Authorization is derived from the current registry authority, then joined
	// to its exact immutable projection inside each forced-index branch. A stale,
	// orphaned, or scope-escalated projection can therefore neither enter nor
	// consume the bounded driver before the outer integrity joins run.
	exactProjectionJoin := `candidate.tenant_id = authorized_registry.tenant_id
		AND candidate.knowledge_object_id = authorized_registry.knowledge_object_id
		AND candidate.object_version = authorized_registry.current_version
		AND candidate.app_id = authorized_registry.app_id
		AND candidate.owner_id = authorized_registry.owner_id
		AND candidate.object_type = authorized_registry.object_type
		AND candidate.name = authorized_registry.name
		AND candidate.sharing_scope = authorized_registry.sharing_scope
		AND candidate.state = authorized_registry.state`
	driver := fmt.Sprintf(`(
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_global_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ? AND authorized_registry.sharing_scope = 'global'
		UNION ALL
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_app_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ? AND authorized_registry.sharing_scope = 'app'
		  AND authorized_registry.app_id IN ?
		UNION ALL
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_private_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ? AND authorized_registry.sharing_scope = 'private'
		  AND authorized_registry.owner_id = ? AND authorized_registry.app_id IN ?
		LIMIT %d
	) AS authorized_projection
	CROSS JOIN knowledge_object_list_projections AS projection`,
		exactProjectionJoin,
		exactProjectionJoin,
		exactProjectionJoin,
		maximumObjectsPerTenant+1,
	)
	return query.Table(
		driver,
		scope.tenantID,
		scope.tenantID, scope.readableAppIDs,
		scope.tenantID, scope.ownerID, scope.readableAppIDs,
	).Where(`authorized_projection.tenant_id = projection.tenant_id
		AND authorized_projection.knowledge_object_id = projection.knowledge_object_id
		AND authorized_projection.object_version = projection.object_version`).
		Where("projection.tenant_id = ?", scope.tenantID)
}

// applyCurrentAuthorization uses three mutually exclusive, authorization-leading
// index branches. This prevents a tenant with many hidden identities from making
// authorization itself a full-tenant scan.
func applyCurrentAuthorization(
	query *gorm.DB,
	scope normalizedScope,
	driverConfig currentAuthorizationDriver,
) *gorm.DB {
	// Keep the authorized identities as the outer query's finite driver. An IN
	// membership predicate lets SQLite choose a tenant-wide order index and test
	// hidden rows one by one; this derived UNION instead scans only the three
	// authorized branches. Any ORDER BY sorter is therefore bounded by the
	// already-validated visible identity count. The defensive LIMIT both caps
	// projection corruption and prevents the SQLite flattener from turning the
	// driver back into membership probes.
	driver := fmt.Sprintf(`(
		SELECT %s
		FROM %s INDEXED BY %s
		WHERE tenant_id = ? AND sharing_scope = 'global'
		UNION ALL
		SELECT %s
		FROM %s INDEXED BY %s
		WHERE tenant_id = ? AND sharing_scope = 'app' AND app_id IN ?
		UNION ALL
		SELECT %s
		FROM %s INDEXED BY %s
		WHERE tenant_id = ? AND sharing_scope = 'private'
		  AND owner_id = ? AND app_id IN ?
		LIMIT %d
	) AS %s
	CROSS JOIN %s`,
		driverConfig.identityColumns, driverConfig.table, driverConfig.globalIndex,
		driverConfig.identityColumns, driverConfig.table, driverConfig.appIndex,
		driverConfig.identityColumns, driverConfig.table, driverConfig.privateIndex,
		maximumObjectsPerTenant+1,
		driverConfig.driverAlias, driverConfig.outerTable)
	return query.Table(
		driver,
		scope.tenantID,
		scope.tenantID, scope.readableAppIDs,
		scope.tenantID, scope.ownerID, scope.readableAppIDs,
	).Where(driverConfig.joinIdentity).Where(driverConfig.tenantColumn+" = ?", scope.tenantID)
}

// Point Get starts from one exact current-registry identity and therefore keeps
// the direct predicate; historical versions are never an authorization source.
// Keeping this separate prevents a broad current-table scan from accidentally
// losing its authorization-leading driver through a string prefix.
func applyPointGetAuthorization(query *gorm.DB, scope normalizedScope) *gorm.DB {
	return query.Where("tenant_id = ?", scope.tenantID).Where(
		`(sharing_scope = 'global'
			OR (sharing_scope = 'app' AND app_id IN ?)
			OR (sharing_scope = 'private' AND owner_id = ? AND app_id IN ?))`,
		scope.readableAppIDs,
		scope.ownerID,
		scope.readableAppIDs,
	)
}

type catalogState struct {
	revision int64
	token    string
	found    bool
}

func (state catalogState) equal(other catalogState) bool {
	return state.revision == other.revision && state.token == other.token && state.found == other.found
}

func readCatalogState(database *gorm.DB, tenantID string) (catalogState, error) {
	query := database.Table("knowledge_catalog_tenants AS tenant").
		Joins(`LEFT JOIN knowledge_catalog_revision_heads AS head
			ON head.tenant_id = tenant.tenant_id`).
		Where("tenant.tenant_id = ?", tenantID)

	var records []struct {
		TenantID        string        `gorm:"column:tenant_id"`
		TenantIDBytes   int64         `gorm:"column:tenant_id_bytes"`
		HeadRevision    sql.NullInt64 `gorm:"column:head_catalog_revision"`
		StateTokenBytes int64         `gorm:"column:state_token_bytes"`
		TenantRevision  int64         `gorm:"column:tenant_catalog_revision"`
		StateToken      []byte        `gorm:"column:state_token"`
	}
	if err := query.Session(&gorm.Session{}).Select(`
		tenant.tenant_id AS tenant_id,
		length(CAST(tenant.tenant_id AS BLOB)) AS tenant_id_bytes,
		tenant.catalog_revision AS tenant_catalog_revision,
		head.catalog_revision AS head_catalog_revision,
		coalesce(length(head.state_token), -1) AS state_token_bytes,
		CASE WHEN length(head.state_token) = ? THEN head.state_token END AS state_token
	`, catalogStateTokenBytes).Limit(2).Find(&records).Error; err != nil {
		return catalogState{}, err
	}
	if len(records) == 0 {
		return catalogState{}, nil
	}
	corrupt := func() (catalogState, error) {
		return catalogState{}, fmt.Errorf("%w: knowledge catalog revision authority is invalid", ErrCorrupt)
	}
	record := records[0]
	if len(records) != 1 || record.TenantID != tenantID || record.TenantIDBytes < 1 ||
		record.TenantIDBytes > maximumTenantIDBytes || record.TenantRevision < 0 ||
		record.TenantRevision > math.MaxInt64-1 || !record.HeadRevision.Valid ||
		record.HeadRevision.Int64 != record.TenantRevision ||
		record.StateTokenBytes != catalogStateTokenBytes || len(record.StateToken) != catalogStateTokenBytes {
		return corrupt()
	}
	return catalogState{
		revision: record.TenantRevision,
		token:    base64.RawURLEncoding.EncodeToString(record.StateToken),
		found:    true,
	}, nil
}

func readCatalogTenantRecord(database *gorm.DB, tenantID string) (tenantRecord, bool, error) {
	var record tenantRecord
	err := database.Where("tenant_id = ?", tenantID).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return tenantRecord{}, false, nil
	}
	if err != nil {
		return tenantRecord{}, false, err
	}
	if record.TenantID != tenantID || record.CatalogRevision < 0 ||
		record.CatalogRevision > math.MaxInt64-1 || record.IdentityCount < 0 ||
		record.IdentityCount > maximumObjectsPerTenant {
		return tenantRecord{}, false, fmt.Errorf("%w: knowledge catalog tenant ledger is invalid", ErrCorrupt)
	}
	return record, true, nil
}

func readBoundedKnowledgeObjectCount(database *gorm.DB, tenantID string) (int64, error) {
	var count int64
	err := database.Raw(`
		SELECT count(*)
		FROM (
			SELECT 1
			FROM knowledge_objects
			WHERE tenant_id = ?
			LIMIT ?
		) AS bounded_knowledge_objects
	`, tenantID, maximumObjectsPerTenant+1).Scan(&count).Error
	if err != nil {
		return 0, err
	}
	if count < 0 || count > maximumObjectsPerTenant+1 {
		return 0, fmt.Errorf("%w: bounded knowledge object count is invalid", ErrCorrupt)
	}
	return count, nil
}

func validateVisibleProjectionCoverage(database *gorm.DB, scope normalizedScope) (int64, error) {
	count, err := readBoundedVisibleKnowledgeObjectCount(database, scope)
	if err != nil {
		return 0, err
	}
	if count > maximumObjectsPerTenant {
		return 0, fmt.Errorf("%w: visible knowledge catalog exceeds its identity bound", ErrCorrupt)
	}
	query := database.Table("knowledge_objects AS object").
		Joins(`LEFT JOIN knowledge_object_versions AS version
			ON version.tenant_id = object.tenant_id
		   AND version.knowledge_object_id = object.knowledge_object_id
		   AND version.object_version = object.current_version`).
		Joins(`LEFT JOIN knowledge_object_list_projections AS projection
			ON projection.tenant_id = object.tenant_id
		   AND projection.knowledge_object_id = object.knowledge_object_id
		   AND projection.object_version = object.current_version`).
		Joins(`LEFT JOIN knowledge_object_list_projection_seals AS seal
			ON seal.tenant_id = projection.tenant_id
		   AND seal.knowledge_object_id = projection.knowledge_object_id
		   AND seal.object_version = projection.object_version`).
		Joins(`LEFT JOIN knowledge_object_list_order_keys AS ordering
			ON ordering.tenant_id = projection.tenant_id
		   AND ordering.knowledge_object_id = projection.knowledge_object_id
		   AND ordering.object_version = projection.object_version`)
	query = applyObjectAuthorization(query, scope)
	var invalid int64
	err = query.Where(`(
		version.knowledge_object_id IS NULL
		OR projection.knowledge_object_id IS NULL
		OR seal.knowledge_object_id IS NULL
		OR ordering.knowledge_object_id IS NULL
		OR ordering.created_at_unix_micro <> object.created_at_unix_micro
		OR ordering.updated_at_unix_micro <> object.updated_at_unix_micro
		OR projection.app_id <> object.app_id
		OR projection.owner_id <> object.owner_id
		OR projection.object_type <> object.object_type
		OR projection.name <> object.name
		OR projection.sharing_scope <> object.sharing_scope
		OR projection.state <> object.state
		OR seal.projection_bytes <> projection.projection_bytes
		OR seal.canonical_selector_bytes <> projection.canonical_selector_bytes
		OR version.app_id <> object.app_id
		OR version.owner_id <> object.owner_id
		OR version.object_type <> object.object_type
		OR version.name <> object.name
		OR version.sharing_scope <> object.sharing_scope
		OR version.state <> object.state
		OR version.definition_digest_key <> object.definition_digest_key
	)`).Select("1").Limit(1).Scan(&invalid).Error
	if err != nil {
		return 0, err
	}
	if invalid != 0 {
		return 0, fmt.Errorf("%w: visible knowledge object lacks an exact current projection", ErrCorrupt)
	}
	return count, nil
}

func readBoundedVisibleKnowledgeObjectCount(database *gorm.DB, scope normalizedScope) (int64, error) {
	bounded := applyObjectAuthorization(
		database.Table("knowledge_objects AS object").Select("1"),
		scope,
	).Limit(maximumObjectsPerTenant + 1)
	var count int64
	if err := database.Table("(?) AS bounded_visible_knowledge_objects", bounded).Count(&count).Error; err != nil {
		return 0, err
	}
	if count < 0 || count > maximumObjectsPerTenant+1 {
		return 0, fmt.Errorf("%w: bounded visible knowledge object count is invalid", ErrCorrupt)
	}
	return count, nil
}

func baseProjectionQuery(database *gorm.DB) *gorm.DB {
	return database.Table("knowledge_object_list_projections AS projection").
		Joins(`CROSS JOIN knowledge_object_list_order_keys AS ordering
			ON ordering.tenant_id = projection.tenant_id
		   AND ordering.knowledge_object_id = projection.knowledge_object_id
		   AND ordering.object_version = projection.object_version`).
		Joins(`CROSS JOIN knowledge_object_list_projection_seals AS seal
			ON seal.tenant_id = projection.tenant_id
		   AND seal.knowledge_object_id = projection.knowledge_object_id
		   AND seal.object_version = projection.object_version`).
		Joins(`CROSS JOIN knowledge_objects AS object
			ON object.tenant_id = projection.tenant_id
		   AND object.knowledge_object_id = projection.knowledge_object_id
		   AND object.current_version = projection.object_version
		   AND object.app_id = projection.app_id
		   AND object.owner_id = projection.owner_id
		   AND object.object_type = projection.object_type
		   AND object.name = projection.name
		   AND object.sharing_scope = projection.sharing_scope
		   AND object.state = projection.state`).
		Joins(`CROSS JOIN knowledge_object_versions AS version
			ON version.tenant_id = object.tenant_id
		   AND version.knowledge_object_id = object.knowledge_object_id
		   AND version.object_version = object.current_version`).
		Joins(`LEFT JOIN knowledge_definition_blobs AS blob
			ON blob.tenant_id = version.tenant_id
		   AND blob.definition_digest = version.definition_digest`).
		Select(fmt.Sprintf(`
			CASE WHEN length(CAST(projection.tenant_id AS BLOB)) BETWEEN 1 AND %[1]d
				THEN projection.tenant_id ELSE '' END AS tenant_id,
			CASE WHEN length(CAST(projection.knowledge_object_id AS BLOB)) BETWEEN 1 AND %[2]d
				THEN projection.knowledge_object_id ELSE '' END AS knowledge_object_id,
			projection.object_version AS object_version,
			CASE WHEN length(CAST(projection.app_id AS BLOB)) BETWEEN 1 AND %[3]d
				THEN projection.app_id ELSE '' END AS app_id,
			CASE WHEN length(CAST(projection.owner_id AS BLOB)) BETWEEN 1 AND %[4]d
				THEN projection.owner_id ELSE '' END AS owner_id,
			CASE WHEN length(CAST(projection.object_type AS BLOB)) BETWEEN 1 AND %[5]d
				THEN projection.object_type ELSE '' END AS object_type,
			CASE WHEN length(CAST(projection.name AS BLOB)) BETWEEN 1 AND %[6]d
				THEN projection.name ELSE '' END AS name,
			CASE WHEN length(CAST(projection.sharing_scope AS BLOB)) BETWEEN 1 AND %[7]d
				THEN projection.sharing_scope ELSE '' END AS sharing_scope,
			CASE WHEN length(CAST(projection.state AS BLOB)) BETWEEN 1 AND %[8]d
				THEN projection.state ELSE '' END AS state,
			projection.description_present AS description_present,
			CASE WHEN length(CAST(projection.description AS BLOB)) <= %[9]d
				THEN projection.description ELSE '' END AS description,
			projection.index_selector_count AS index_selector_count,
			projection.host_selector_count AS host_selector_count,
			projection.source_selector_count AS source_selector_count,
			projection.sourcetype_selector_count AS sourcetype_selector_count,
			projection.selector_value_bytes AS selector_value_bytes,
			projection.canonical_selector_bytes AS canonical_selector_bytes,
			projection.projection_bytes AS projection_bytes,
			seal.projection_bytes AS seal_projection_bytes,
			seal.canonical_selector_bytes AS seal_canonical_selector_bytes,
			CASE WHEN length(object.definition_digest) <= %[10]d
				THEN object.definition_digest ELSE X'' END AS definition_digest,
			object.created_at_unix_micro AS created_at_unix_micro,
			object.updated_at_unix_micro AS updated_at_unix_micro,
			object.disabled_at_unix_micro AS disabled_at_unix_micro,
			object.quarantined_at_unix_micro AS quarantined_at_unix_micro,
			object.deleted_at_unix_micro AS deleted_at_unix_micro,
			CASE
				WHEN object.quarantine_reason IS NULL THEN NULL
				WHEN length(CAST(object.quarantine_reason AS BLOB)) <= %[11]d
					THEN object.quarantine_reason
				ELSE NULL
			END AS quarantine_reason,
			coalesce(length(blob.definition_proto), 0) AS definition_bytes,
			version.dependency_count AS dependency_count,
			length(CAST(projection.knowledge_object_id AS BLOB)) AS width_knowledge_object_id_bytes,
			length(CAST(projection.tenant_id AS BLOB)) AS width_tenant_id_bytes,
			length(CAST(projection.app_id AS BLOB)) AS width_app_id_bytes,
			length(CAST(projection.owner_id AS BLOB)) AS width_owner_id_bytes,
			length(CAST(projection.object_type AS BLOB)) AS width_object_type_bytes,
			length(CAST(projection.name AS BLOB)) AS width_name_bytes,
			length(CAST(projection.sharing_scope AS BLOB)) AS width_sharing_scope_bytes,
			length(CAST(projection.state AS BLOB)) AS width_state_bytes,
			coalesce(length(object.definition_digest), 0) AS width_digest_bytes,
			coalesce(length(CAST(object.quarantine_reason AS BLOB)), 0) AS width_quarantine_reason_bytes,
			length(CAST(projection.description AS BLOB)) AS width_description_bytes,
			projection.selector_value_bytes AS width_selector_value_bytes,
			projection.projection_bytes AS width_projection_bytes,
			seal.projection_bytes AS width_seal_projection_bytes,
			projection.canonical_selector_bytes AS width_canonical_selector_bytes,
			seal.canonical_selector_bytes AS width_seal_canonical_selector_bytes
		`,
			maximumTenantIDBytes,
			maximumObjectIDBytes,
			maximumAppIDBytes,
			maximumOwnerIDBytes,
			len(ObjectTypeFieldExtraction),
			maximumFilterBytes,
			len(SharingScopePrivate),
			len(StateQuarantined),
			maximumDescriptionBytes,
			persistedKnowledgeDefinitionDigestBytes,
			maximumPersistedQuarantineReasonBytes,
		))
}

func applyListFilters(query *gorm.DB, request normalizedListRequest) *gorm.DB {
	query = applyProjectionAuthorization(query, request.scope)
	predicate, arguments := listFilterPredicate(request)
	if predicate != "" {
		query = query.Where(predicate, arguments...)
	}
	return query
}

func listFilterPredicate(request normalizedListRequest) (string, []any) {
	predicates := make([]string, 0, 3)
	scalarPredicate, arguments := listScalarFilterPredicate(request)
	if scalarPredicate != "" {
		predicates = append(predicates, scalarPredicate)
	}
	if request.textFilter != nil {
		predicates = append(predicates, `(
			instr(CAST(projection.name AS BLOB), CAST(? AS BLOB)) > 0 OR
			instr(CAST(projection.description AS BLOB), CAST(? AS BLOB)) > 0
		)`)
		arguments = append(arguments, *request.textFilter, *request.textFilter)
	}
	if request.selectorTextFilter != nil {
		predicates = append(predicates, `EXISTS (
			SELECT 1 FROM knowledge_object_list_selector_patterns AS selector
			WHERE selector.tenant_id = projection.tenant_id
			  AND selector.knowledge_object_id = projection.knowledge_object_id
			  AND selector.object_version = projection.object_version
			  AND instr(CAST(selector.value AS BLOB), CAST(? AS BLOB)) > 0
		)`)
		arguments = append(arguments, *request.selectorTextFilter)
	}
	return strings.Join(predicates, " AND "), arguments
}

// bodyFilterIntegrityQuery returns every visible row that can participate in a
// definition-derived filter after authorization and trusted scalar filters.
// The caller integrity-validates this complete bounded set before relying on
// description or selector projection membership. Scalar-only filters do not
// need this sweep because registry/version equality is already checked by
// validateVisibleProjectionCoverage.
func bodyFilterIntegrityQuery(database *gorm.DB, request normalizedListRequest) *gorm.DB {
	if request.textFilter == nil && request.selectorTextFilter == nil {
		return nil
	}
	query := applyProjectionAuthorization(baseProjectionQuery(database), request.scope)
	predicate, arguments := listScalarFilterPredicate(request)
	if predicate != "" {
		query = query.Where(predicate, arguments...)
	}
	return query
}

func listScalarFilterPredicate(request normalizedListRequest) (string, []any) {
	predicates := make([]string, 0, 5)
	arguments := make([]any, 0, 5)
	if request.appIDFilter != nil {
		predicates = append(predicates, "projection.app_id = ?")
		arguments = append(arguments, *request.appIDFilter)
	}
	if request.ownerIDFilter != nil {
		predicates = append(predicates, "projection.owner_id = ?")
		arguments = append(arguments, *request.ownerIDFilter)
	}
	if len(request.objectTypeFilters) != 0 {
		predicates = append(predicates, "projection.object_type IN ?")
		arguments = append(arguments, request.objectTypeFilters)
	}
	if len(request.stateFilters) != 0 {
		predicates = append(predicates, "projection.state IN ?")
		arguments = append(arguments, request.stateFilters)
	}
	if len(request.sharingScopeFilters) != 0 {
		predicates = append(predicates, "projection.sharing_scope IN ?")
		arguments = append(arguments, request.sharingScopeFilters)
	}
	return strings.Join(predicates, " AND "), arguments
}

func applyListCursor(query *gorm.DB, request normalizedListRequest, cursor listCursor) *gorm.DB {
	if cursor.ObjectID == "" {
		return query
	}
	comparison := ">"
	if request.direction == SortDescending {
		comparison = "<"
	}
	switch request.sortBy {
	case SortByName:
		return query.Where("(projection.name "+comparison+" ? OR (projection.name = ? AND projection.knowledge_object_id "+comparison+" ?))", cursor.PrimaryString, cursor.PrimaryString, cursor.ObjectID)
	case SortByCreatedAt:
		return query.Where("(ordering.created_at_unix_micro "+comparison+" ? OR (ordering.created_at_unix_micro = ? AND ordering.knowledge_object_id "+comparison+" ?))", *cursor.PrimaryInteger, *cursor.PrimaryInteger, cursor.ObjectID)
	case SortByUpdatedAt:
		return query.Where("(ordering.updated_at_unix_micro "+comparison+" ? OR (ordering.updated_at_unix_micro = ? AND ordering.knowledge_object_id "+comparison+" ?))", *cursor.PrimaryInteger, *cursor.PrimaryInteger, cursor.ObjectID)
	case SortByObjectType:
		return query.Where("(projection.object_type "+comparison+" ? OR (projection.object_type = ? AND (projection.name "+comparison+" ? OR (projection.name = ? AND projection.knowledge_object_id "+comparison+" ?))))", cursor.PrimaryString, cursor.PrimaryString, cursor.SecondaryString, cursor.SecondaryString, cursor.ObjectID)
	default:
		return query
	}
}

func applyListOrder(query *gorm.DB, request normalizedListRequest) *gorm.DB {
	direction := "ASC"
	if request.direction == SortDescending {
		direction = "DESC"
	}
	switch request.sortBy {
	case SortByName:
		return query.Order("projection.name " + direction).Order("projection.knowledge_object_id " + direction)
	case SortByCreatedAt:
		return query.Order("ordering.created_at_unix_micro " + direction).Order("ordering.knowledge_object_id " + direction)
	case SortByUpdatedAt:
		return query.Order("ordering.updated_at_unix_micro " + direction).Order("ordering.knowledge_object_id " + direction)
	case SortByObjectType:
		return query.Order("projection.object_type " + direction).Order("projection.name " + direction).Order("projection.knowledge_object_id " + direction)
	default:
		return query
	}
}

func mapError(ctxErr error, operation string, err error) error {
	if ctxErr != nil {
		return copyCatalogErrorMetadata(ctxErr, err)
	}
	if errors.Is(err, control.ErrNotFound) || errors.Is(err, control.ErrPageInvalidated) ||
		errors.Is(err, control.ErrCapacityExceeded) || errors.Is(err, ErrInvalidCursor) || errors.Is(err, ErrCorrupt) {
		return err
	}
	return fmt.Errorf("knowledge catalog %s: %w", strings.TrimSpace(operation), err)
}
