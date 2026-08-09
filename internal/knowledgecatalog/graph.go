package knowledgecatalog

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// ListDependencies returns the disclosed direct outgoing edges of one exact
// source version. The source's current registry identity is the sole authority
// for both current and historical reads.
func (store *Store) ListDependencies(
	ctx context.Context,
	scope ReadScope,
	request DependencyListRequest,
) (DependencyPage, error) {
	return store.listGraph(ctx, scope, request, graphDirectionDependencies)
}

// ListDependents returns disclosed direct incoming edges to one exact target
// version. Only currently registered source versions are eligible; every
// nonquarantined lifecycle state participates.
func (store *Store) ListDependents(
	ctx context.Context,
	scope ReadScope,
	request DependencyListRequest,
) (DependencyPage, error) {
	return store.listGraph(ctx, scope, request, graphDirectionDependents)
}

func (store *Store) listGraph(
	ctx context.Context,
	scope ReadScope,
	request DependencyListRequest,
	direction graphDirection,
) (page DependencyPage, returnedErr error) {
	var authorized *AuthorizedContext
	defer func() {
		returnedErr = withDefaultErrorDisposition(
			returnedErr,
			ErrorDispositionDefinitiveRejection,
		)
		if returnedErr != nil && authorized != nil {
			returnedErr = withAuthorizedContext(returnedErr, *authorized)
		}
	}()
	if err := validateContext(ctx); err != nil {
		return DependencyPage{}, err
	}
	normalized, err := normalizeDependencyListRequest(scope, request, direction)
	if err != nil {
		return DependencyPage{}, err
	}
	fingerprint, err := graphRequestFingerprint(normalized)
	if err != nil {
		return DependencyPage{}, err
	}
	cursor := graphCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeGraphCursor(store.cursorKey, normalized.pageToken, fingerprint)
		if err != nil {
			return DependencyPage{}, err
		}
	}

	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return DependencyPage{}, mapError(ctx.Err(), "begin graph read", tx.Error)
	}
	defer finishReadTransaction(tx, &returnedErr)
	catalog, err := readCatalogState(tx, normalized.scope.tenantID)
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "read graph revision", err)
	}
	if cursor.CatalogRevision != 0 && (!catalog.found || cursor.CatalogRevision != catalog.revision ||
		cursor.CatalogState != catalog.token) {
		return DependencyPage{}, control.ErrPageInvalidated
	}
	rootQuery := tx.Model(&registryRecord{}).Where("knowledge_object_id = ?", normalized.knowledgeObjectID)
	rootQuery = applyPointGetAuthorization(rootQuery, normalized.scope)
	registry, found, err := readRegistryRecord(rootQuery)
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "read graph root registry", err)
	}
	if !found {
		return DependencyPage{}, control.ErrNotFound
	}
	if !catalog.found || catalog.revision < 1 {
		return DependencyPage{}, fmt.Errorf("%w: visible knowledge graph has no revision authority", ErrCorrupt)
	}
	if err := validateRegistry(registry); err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "validate graph root registry", err)
	}
	if registry.State == StateQuarantined {
		return DependencyPage{}, control.ErrNotFound
	}
	authorizedValue := authorizedObjectContext(registry)
	authorized = &authorizedValue
	selected, err := store.readGraphRootVersion(tx, normalized, registry)
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "read graph root version", err)
	}
	resolved := ObjectVersionIdentity{
		KnowledgeObjectID: strings.Clone(selected.KnowledgeObjectID),
		Version:           uint64(selected.ObjectVersion),
	}
	if cursor.CatalogRevision != 0 {
		if cursor.ResolvedObjectID != selected.KnowledgeObjectID ||
			cursor.ResolvedVersion != selected.ObjectVersion ||
			!cursorMatchesGraphRoot(cursor, direction, selected) {
			return DependencyPage{}, fmt.Errorf("%w: graph cursor root authority disagrees", ErrCorrupt)
		}
	}
	visibleCount, err := readBoundedVisibleKnowledgeObjectCount(tx, normalized.scope)
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "read visible graph identity bound", err)
	}
	if visibleCount > maximumObjectsPerTenant {
		return DependencyPage{}, fmt.Errorf("%w: visible knowledge graph exceeds its identity bound", ErrCorrupt)
	}

	page = DependencyPage{
		Edges:           []DependencyEdge{},
		ResolvedObject:  resolved,
		ResolvedCurrent: currentRegistryAuthorityFromRegistry(registry),
		CatalogRevision: uint64(catalog.revision),
	}
	var hasMore bool
	switch direction {
	case graphDirectionDependencies:
		page.Edges, hasMore, page.TotalSize, err = store.listOutgoingGraphEdges(
			tx,
			normalized,
			registry,
			selected,
			cursor,
		)
	case graphDirectionDependents:
		page.Edges, hasMore, page.TotalSize, err = store.listIncomingGraphEdges(
			tx,
			normalized,
			registry,
			selected,
			cursor,
		)
	default:
		err = fmt.Errorf("%w: normalized graph direction is invalid", ErrCorrupt)
	}
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "list graph edges", err)
	}
	if normalized.includeTotal {
		page.TotalSizeExact = true
	}
	if err := validateDependencyPageAuthorities(page, direction); err != nil {
		return DependencyPage{}, err
	}
	if hasMore {
		if len(page.Edges) == 0 {
			return DependencyPage{}, fmt.Errorf("%w: graph page made no pagination progress", ErrCorrupt)
		}
		last := page.Edges[len(page.Edges)-1]
		page.NextPageToken, err = encodeGraphCursor(store.cursorKey, graphCursor{
			Fingerprint:        fingerprint,
			CatalogRevision:    catalog.revision,
			CatalogState:       catalog.token,
			ResolvedObjectID:   selected.KnowledgeObjectID,
			ResolvedVersion:    selected.ObjectVersion,
			LastSourceObjectID: last.Source.KnowledgeObjectID,
			LastSourceVersion:  int64(last.Source.Version),
			LastTargetObjectID: last.Target.KnowledgeObjectID,
			LastTargetVersion:  int64(last.Target.Version),
			LastDependencyRole: int32(last.Role),
		})
		if err != nil {
			return DependencyPage{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return DependencyPage{}, err
	}
	finalCatalog, err := readCatalogState(tx, normalized.scope.tenantID)
	if err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "re-read graph revision", err)
	}
	if !finalCatalog.equal(catalog) {
		return DependencyPage{}, fmt.Errorf("%w: catalog revision changed inside one graph snapshot", ErrCorrupt)
	}
	if err := tx.Commit().Error; err != nil {
		return DependencyPage{}, mapError(ctx.Err(), "commit graph read", err)
	}
	return page, nil
}

func (store *Store) readGraphRootVersion(
	database *gorm.DB,
	request normalizedDependencyListRequest,
	registry registryRecord,
) (versionRecord, error) {
	if _, err := store.objectFromCurrentRegistry(database, registry); err != nil {
		return versionRecord{}, err
	}
	selectedVersion := registry.CurrentVersion
	if request.requestedVersionPresent {
		if request.requestedVersion > registry.CurrentVersion {
			return versionRecord{}, control.ErrNotFound
		}
		selectedVersion = request.requestedVersion
	}
	selected, found, err := readVersionRecord(
		database,
		request.scope.tenantID,
		request.knowledgeObjectID,
		selectedVersion,
	)
	if err != nil {
		return versionRecord{}, err
	}
	if !found {
		return versionRecord{}, fmt.Errorf("%w: selected immutable graph version is missing", ErrCorrupt)
	}
	if selectedVersion == registry.CurrentVersion {
		if err := validateCurrentVersion(registry, selected); err != nil {
			return versionRecord{}, err
		}
	} else if _, err := store.objectFromHistoricalVersion(database, registry, selected); err != nil {
		return versionRecord{}, err
	}
	return selected, nil
}

func cursorMatchesGraphRoot(cursor graphCursor, direction graphDirection, root versionRecord) bool {
	switch direction {
	case graphDirectionDependencies:
		return cursor.LastSourceObjectID == root.KnowledgeObjectID &&
			cursor.LastSourceVersion == root.ObjectVersion
	case graphDirectionDependents:
		return cursor.LastTargetObjectID == root.KnowledgeObjectID &&
			cursor.LastTargetVersion == root.ObjectVersion
	default:
		return false
	}
}

func (store *Store) listOutgoingGraphEdges(
	database *gorm.DB,
	request normalizedDependencyListRequest,
	rootCurrent registryRecord,
	root versionRecord,
	cursor graphCursor,
) ([]DependencyEdge, bool, *uint64, error) {
	records, err := readValidatedVersionDependencies(database, root)
	if err != nil {
		return nil, false, nil, err
	}
	targetIDs := make([]string, 0, len(records))
	for _, record := range records {
		targetIDs = append(targetIDs, record.TargetObjectID)
	}
	targetIDs = slices.Compact(targetIDs)
	visibleTargets, err := readAuthorizedGraphRegistries(database, request.scope, targetIDs)
	if err != nil {
		return nil, false, nil, err
	}
	visible := make([]DependencyEdge, 0, len(visibleTargets))
	disclosedTotal := uint64(0)
	for _, record := range records {
		if _, disclosed := visibleTargets[record.TargetObjectID]; !disclosed {
			continue
		}
		disclosedTotal++
		edge := dependencyEdgeFromRecord(
			record,
			currentRegistryAuthorityFromRegistry(rootCurrent),
			currentRegistryAuthorityFromRegistry(visibleTargets[record.TargetObjectID]),
		)
		if cursor.CatalogRevision != 0 && !graphEdgeAfterCursor(edge, cursor) {
			continue
		}
		visible = append(visible, edge)
	}
	var total *uint64
	if request.includeTotal {
		value := disclosedTotal
		total = &value
	}
	pageSize := int(request.pageSize)
	hasMore := len(visible) > pageSize
	if hasMore {
		visible = visible[:pageSize:pageSize]
	} else {
		visible = visible[:len(visible):len(visible)]
	}
	return visible, hasMore, total, nil
}

func dependencyEdgeFromRecord(
	record dependencyRecord,
	sourceCurrent CurrentRegistryAuthority,
	targetCurrent CurrentRegistryAuthority,
) DependencyEdge {
	return DependencyEdge{
		Source: ObjectVersionIdentity{
			KnowledgeObjectID: strings.Clone(record.SourceObjectID),
			Version:           uint64(record.SourceObjectVersion),
		},
		Target: ObjectVersionIdentity{
			KnowledgeObjectID: strings.Clone(record.TargetObjectID),
			Version:           uint64(record.TargetObjectVersion),
		},
		Role:          DependencyRoleFieldInput,
		SourceCurrent: sourceCurrent,
		TargetCurrent: targetCurrent,
	}
}

func currentRegistryAuthorityFromRegistry(registry registryRecord) CurrentRegistryAuthority {
	return CurrentRegistryAuthority{
		TenantID:          strings.Clone(registry.TenantID),
		KnowledgeObjectID: strings.Clone(registry.KnowledgeObjectID),
		CurrentVersion:    uint64(registry.CurrentVersion),
		AppID:             strings.Clone(registry.AppID),
		OwnerID:           strings.Clone(registry.OwnerID),
		ObjectType:        registry.ObjectType,
		SharingScope:      registry.SharingScope,
		State:             registry.State,
	}
}

func validateDependencyPageAuthorities(page DependencyPage, direction graphDirection) error {
	if (direction != graphDirectionDependencies && direction != graphDirectionDependents) ||
		page.CatalogRevision < 1 ||
		len(page.Edges) > MaximumPageSize ||
		(page.TotalSize == nil) != !page.TotalSizeExact ||
		page.ResolvedObject.KnowledgeObjectID != page.ResolvedCurrent.KnowledgeObjectID ||
		page.ResolvedObject.Version == 0 ||
		page.ResolvedObject.Version > page.ResolvedCurrent.CurrentVersion ||
		page.ResolvedCurrent.CurrentVersion > page.CatalogRevision ||
		!validDetachedCurrentAuthority(page.ResolvedCurrent) {
		return fmt.Errorf("%w: resolved graph authority is invalid", ErrCorrupt)
	}
	if page.TotalSize != nil {
		maximumTotal := uint64(MaximumDependencyEdgesPerVersion)
		if direction == graphDirectionDependents {
			maximumTotal = MaximumObjectsPerTenant
		}
		if *page.TotalSize < uint64(len(page.Edges)) || *page.TotalSize > maximumTotal {
			return fmt.Errorf("%w: graph total authority is invalid", ErrCorrupt)
		}
	}
	for _, edge := range page.Edges {
		if edge.Source.KnowledgeObjectID != edge.SourceCurrent.KnowledgeObjectID ||
			edge.Target.KnowledgeObjectID != edge.TargetCurrent.KnowledgeObjectID ||
			edge.SourceCurrent.TenantID != page.ResolvedCurrent.TenantID ||
			edge.TargetCurrent.TenantID != page.ResolvedCurrent.TenantID ||
			edge.Source.Version == 0 || edge.Source.Version > edge.SourceCurrent.CurrentVersion ||
			edge.Target.Version == 0 || edge.Target.Version > edge.TargetCurrent.CurrentVersion ||
			edge.SourceCurrent.CurrentVersion > page.CatalogRevision ||
			edge.TargetCurrent.CurrentVersion > page.CatalogRevision ||
			edge.Role != DependencyRoleFieldInput ||
			!validDetachedCurrentAuthority(edge.SourceCurrent) ||
			!validDetachedCurrentAuthority(edge.TargetCurrent) {
			return fmt.Errorf("%w: detached graph edge authority is invalid", ErrCorrupt)
		}
		switch direction {
		case graphDirectionDependencies:
			if edge.Source != page.ResolvedObject || edge.SourceCurrent != page.ResolvedCurrent {
				return fmt.Errorf("%w: outgoing graph root authority disagrees", ErrCorrupt)
			}
		case graphDirectionDependents:
			if edge.Target != page.ResolvedObject || edge.TargetCurrent != page.ResolvedCurrent ||
				edge.Source.Version != edge.SourceCurrent.CurrentVersion {
				return fmt.Errorf("%w: incoming graph current-source authority disagrees", ErrCorrupt)
			}
		default:
			return fmt.Errorf("%w: detached graph direction is invalid", ErrCorrupt)
		}
	}
	return nil
}

func validDetachedCurrentAuthority(authority CurrentRegistryAuthority) bool {
	return validIdentity(authority.TenantID, maximumTenantIDBytes) &&
		validIdentity(authority.KnowledgeObjectID, maximumObjectIDBytes) &&
		authority.CurrentVersion >= 1 && authority.CurrentVersion <= maximumVersionsPerTenant &&
		validIdentity(authority.AppID, maximumAppIDBytes) &&
		validIdentity(authority.OwnerID, maximumOwnerIDBytes) &&
		authority.ObjectType.Valid() && authority.SharingScope.Valid() &&
		authority.State.valid() && authority.State != StateQuarantined
}

func graphEdgeAfterCursor(edge DependencyEdge, cursor graphCursor) bool {
	if edge.Source.KnowledgeObjectID != cursor.LastSourceObjectID {
		return edge.Source.KnowledgeObjectID > cursor.LastSourceObjectID
	}
	if edge.Source.Version != uint64(cursor.LastSourceVersion) {
		return edge.Source.Version > uint64(cursor.LastSourceVersion)
	}
	if edge.Target.KnowledgeObjectID != cursor.LastTargetObjectID {
		return edge.Target.KnowledgeObjectID > cursor.LastTargetObjectID
	}
	if edge.Target.Version != uint64(cursor.LastTargetVersion) {
		return edge.Target.Version > uint64(cursor.LastTargetVersion)
	}
	return int32(edge.Role) > cursor.LastDependencyRole
}

func readAuthorizedGraphRegistries(
	database *gorm.DB,
	scope normalizedScope,
	objectIDs []string,
) (map[string]registryRecord, error) {
	result := make(map[string]registryRecord, len(objectIDs))
	if len(objectIDs) == 0 {
		return result, nil
	}
	err := listChunks(objectIDs, func(chunk []string) error {
		query := applyObjectAuthorization(database.Table("knowledge_objects AS object"), scope).
			Where("object.knowledge_object_id IN ?", chunk).
			Where("object.state <> ?", StateQuarantined).
			Order("object.knowledge_object_id ASC")
		records, err := readAliasedRegistryRecords(query, "object", len(chunk)+1)
		if err != nil {
			return err
		}
		if len(records) > len(chunk) {
			return fmt.Errorf("%w: authorized graph registry batch exceeds its identity bound", ErrCorrupt)
		}
		for _, record := range records {
			if _, duplicate := result[record.KnowledgeObjectID]; duplicate {
				return fmt.Errorf("%w: duplicate authorized graph registry", ErrCorrupt)
			}
			result[record.KnowledgeObjectID] = record
		}
		return nil
	})
	if err != nil || len(result) == 0 {
		return result, err
	}
	visibleIDs := make([]string, 0, len(result))
	for objectID := range result {
		visibleIDs = append(visibleIDs, objectID)
	}
	slices.Sort(visibleIDs)
	versions, err := readCurrentVersionRecordsBatch(database, scope.tenantID, visibleIDs)
	if err != nil {
		return nil, err
	}
	for objectID, registry := range result {
		if err := validateCurrentVersion(registry, versions[objectID]); err != nil {
			return nil, err
		}
	}
	if err := validateCurrentChronologiesBatch(database, scope.tenantID, visibleIDs, result, versions); err != nil {
		return nil, err
	}
	lifecycles, err := readCurrentVersionLifecyclesBatch(database, scope.tenantID, visibleIDs, versions)
	if err != nil {
		return nil, err
	}
	for objectID, registry := range result {
		if err := validateCurrentLifecycle(registry, versions[objectID], lifecycles[objectID]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func readAliasedRegistryRecords(query *gorm.DB, alias string, limit int) ([]registryRecord, error) {
	if query == nil || alias != "object" || limit < 1 || limit > maximumObjectsPerTenant+1 {
		return nil, fmt.Errorf("%w: invalid aliased registry read", ErrCorrupt)
	}
	var sizes []registrySizeRecord
	if err := query.Session(&gorm.Session{}).Select(`
		length(CAST(object.knowledge_object_id AS BLOB)) AS knowledge_object_id_bytes,
		length(CAST(object.tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(object.app_id AS BLOB)) AS app_id_bytes,
		length(CAST(object.owner_id AS BLOB)) AS owner_id_bytes,
		length(CAST(object.object_type AS BLOB)) AS object_type_bytes,
		length(CAST(object.name AS BLOB)) AS name_bytes,
		length(CAST(object.sharing_scope AS BLOB)) AS sharing_scope_bytes,
		length(CAST(object.state AS BLOB)) AS state_bytes,
		coalesce(length(object.definition_digest), 0) AS digest_bytes,
		coalesce(length(CAST(object.quarantine_reason AS BLOB)), 0) AS quarantine_reason_bytes
	`).Limit(limit).Find(&sizes).Error; err != nil {
		return nil, err
	}
	for _, size := range sizes {
		if !validRegistrySize(size) {
			return nil, fmt.Errorf("%w: authorized graph registry width is invalid", ErrCorrupt)
		}
	}
	var records []registryRecord
	if err := query.Session(&gorm.Session{}).Select("object.*").Limit(limit).Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: authorized graph registry preflight changed", ErrCorrupt)
	}
	for _, record := range records {
		if err := validateRegistry(record); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (store *Store) listIncomingGraphEdges(
	database *gorm.DB,
	request normalizedDependencyListRequest,
	rootCurrent registryRecord,
	root versionRecord,
	cursor graphCursor,
) ([]DependencyEdge, bool, *uint64, error) {
	limit := int(request.pageSize) + 1
	candidates, err := readIncomingGraphCandidates(database, request, root, cursor, limit)
	if err != nil {
		return nil, false, nil, err
	}
	var total *uint64
	if request.includeTotal {
		count, err := countIncomingGraphCandidates(database, request.scope, root)
		if err != nil {
			return nil, false, nil, err
		}
		value := uint64(count)
		total = &value
	}
	pageCandidateCount := min(len(candidates), int(request.pageSize))
	if pageCandidateCount == 0 {
		return []DependencyEdge{}, false, total, nil
	}
	pageCandidates := candidates[:pageCandidateCount:pageCandidateCount]
	projections, err := readIncomingCandidateProjections(database, request.scope, pageCandidates)
	if err != nil {
		return nil, false, nil, err
	}
	boundedProjections, budgetHasMore, err := boundedListResponseRecords(projections, pageCandidateCount)
	if err != nil {
		return nil, false, nil, err
	}
	projections = boundedProjections
	pageCandidates = pageCandidates[:len(projections):len(projections)]
	_, semanticCount, err := store.objectsFromProjectionsPage(
		database,
		projections,
		listResponseHydrationBudget,
	)
	if err != nil {
		return nil, false, nil, err
	}
	edges := make([]DependencyEdge, semanticCount)
	for index := 0; index < semanticCount; index++ {
		edges[index] = dependencyEdgeFromRecord(
			pageCandidates[index],
			currentRegistryAuthorityFromRegistry(registryFromProjection(projections[index])),
			currentRegistryAuthorityFromRegistry(rootCurrent),
		)
	}
	hasMore := len(candidates) > pageCandidateCount || budgetHasMore || semanticCount < len(projections)
	return edges, hasMore, total, nil
}

func readIncomingCandidateProjections(
	database *gorm.DB,
	scope normalizedScope,
	candidates []dependencyRecord,
) ([]projectionRecord, error) {
	objectIDs := make([]string, len(candidates))
	for index, candidate := range candidates {
		objectIDs[index] = candidate.SourceObjectID
	}
	query := applyProjectionAuthorization(baseProjectionQuery(database), scope).
		Where("projection.knowledge_object_id IN ?", objectIDs).
		Where("projection.state <> ?", StateQuarantined)
	records, err := readProjectionRecords(query, len(objectIDs)+1)
	if err != nil {
		return nil, err
	}
	if len(records) != len(objectIDs) {
		return nil, fmt.Errorf("%w: incoming graph source projection set is incomplete", ErrCorrupt)
	}
	byID := make(map[string]projectionRecord, len(records))
	for _, record := range records {
		if _, duplicate := byID[record.KnowledgeObjectID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate incoming graph source projection", ErrCorrupt)
		}
		byID[record.KnowledgeObjectID] = record
	}
	ordered := make([]projectionRecord, len(objectIDs))
	for index, objectID := range objectIDs {
		record, found := byID[objectID]
		if !found || record.ObjectVersion != candidates[index].SourceObjectVersion {
			return nil, fmt.Errorf("%w: incoming graph source projection identity disagrees", ErrCorrupt)
		}
		ordered[index] = record
	}
	return ordered, nil
}
