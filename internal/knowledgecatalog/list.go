package knowledgecatalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// List returns one bounded, detached current-object page. Authorization and all
// normalized filters are applied to the sealed projection before keyset LIMIT.
// The opaque continuation binds caller scope, filters, page bound, sort order,
// and the first page's tenant catalog revision plus its exact restore-fork-safe
// state commitment.
func (store *Store) List(
	ctx context.Context,
	scope ReadScope,
	request ListRequest,
) (page ListPage, returnedErr error) {
	defer func() {
		returnedErr = withDefaultErrorDisposition(
			returnedErr,
			ErrorDispositionDefinitiveRejection,
		)
	}()
	if err := validateContext(ctx); err != nil {
		return ListPage{}, err
	}
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		return ListPage{}, err
	}
	fingerprint, err := requestFingerprint(normalized)
	if err != nil {
		return ListPage{}, err
	}
	cursor := listCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeCursor(store.cursorKey, normalized.pageToken, fingerprint, normalized.sortBy)
		if err != nil {
			return ListPage{}, err
		}
	}

	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return ListPage{}, mapError(ctx.Err(), "begin list", tx.Error)
	}
	defer finishReadTransaction(tx, &returnedErr)
	catalog, err := readCatalogState(tx, normalized.scope.tenantID)
	if err != nil {
		return ListPage{}, mapError(ctx.Err(), "read list revision", err)
	}
	if cursor.CatalogRevision != 0 && (!catalog.found || cursor.CatalogRevision != catalog.revision ||
		cursor.CatalogState != catalog.token) {
		return ListPage{}, control.ErrPageInvalidated
	}
	visibleCount, err := validateVisibleProjectionCoverage(tx, normalized.scope)
	if err != nil {
		return ListPage{}, mapError(ctx.Err(), "validate visible projection coverage", err)
	}
	if visibleCount != 0 && (!catalog.found || catalog.revision == 0) {
		return ListPage{}, fmt.Errorf("%w: visible knowledge catalog has no revision authority", ErrCorrupt)
	}
	var integrityObjects map[string]Object
	if integrityQuery := bodyFilterIntegrityQuery(tx, normalized); integrityQuery != nil {
		integrityRecords, readErr := readProjectionRecordsBounded(
			integrityQuery,
			maximumObjectsPerTenant+1,
			MaximumListFilterIntegrityProjectionBytes,
		)
		if readErr != nil {
			return ListPage{}, mapError(ctx.Err(), "read body-filter integrity set", readErr)
		}
		if len(integrityRecords) > maximumObjectsPerTenant {
			return ListPage{}, fmt.Errorf("%w: body-filter integrity set exceeds its object bound", ErrCorrupt)
		}
		hydrated, hydrateErr := store.objectsFromProjections(
			tx,
			integrityRecords,
			listFilterIntegrityHydrationBudget,
		)
		if hydrateErr != nil {
			return ListPage{}, mapError(ctx.Err(), "validate body-filter candidates", hydrateErr)
		}
		integrityObjects = make(map[string]Object, len(integrityRecords))
		for index, record := range integrityRecords {
			object := hydrated[index]
			if _, duplicate := integrityObjects[record.KnowledgeObjectID]; duplicate {
				return ListPage{}, fmt.Errorf("%w: duplicate body-filter candidate", ErrCorrupt)
			}
			integrityObjects[record.KnowledgeObjectID] = object
		}
	}
	query := applyListFilters(baseProjectionQuery(tx), normalized)
	query = applyListCursor(query, normalized, cursor)
	query = applyListOrder(query, normalized)
	limit := int(normalized.pageSize) + 1
	records, err := readProjectionRecords(query, limit)
	if err != nil {
		return ListPage{}, mapError(ctx.Err(), "read list projections", err)
	}
	returnedRecords, hasMore, err := boundedListResponseRecords(records, int(normalized.pageSize))
	if err != nil {
		return ListPage{}, err
	}
	objects := make([]Object, len(returnedRecords))
	if integrityObjects != nil {
		for index, record := range returnedRecords {
			object, found := integrityObjects[record.KnowledgeObjectID]
			if !found {
				return ListPage{}, fmt.Errorf("%w: filtered object escaped its integrity set", ErrCorrupt)
			}
			objects[index] = object
		}
	} else {
		var semanticCount int
		objects, semanticCount, err = store.objectsFromProjectionsPage(
			tx,
			returnedRecords,
			listResponseHydrationBudget,
		)
		if err != nil {
			return ListPage{}, mapError(ctx.Err(), "validate listed objects", err)
		}
		if semanticCount < len(returnedRecords) {
			returnedRecords = returnedRecords[:semanticCount:semanticCount]
			hasMore = true
		}
	}
	if err := ctx.Err(); err != nil {
		return ListPage{}, err
	}
	page = ListPage{
		Objects:         objects,
		CatalogRevision: uint64(catalog.revision),
	}
	if hasMore {
		last := returnedRecords[len(returnedRecords)-1]
		next := listCursor{
			Fingerprint:     fingerprint,
			CatalogRevision: catalog.revision,
			CatalogState:    catalog.token,
			ObjectID:        last.KnowledgeObjectID,
		}
		switch normalized.sortBy {
		case SortByName:
			next.PrimaryString = last.Name
		case SortByCreatedAt:
			value := last.CreatedAtUnixMicro
			next.PrimaryInteger = &value
		case SortByUpdatedAt:
			value := last.UpdatedAtUnixMicro
			next.PrimaryInteger = &value
		case SortByObjectType:
			next.PrimaryString = string(last.ObjectType)
			next.SecondaryString = last.Name
		}
		page.NextPageToken, err = encodeCursor(store.cursorKey, next)
		if err != nil {
			return ListPage{}, err
		}
	}
	if normalized.includeTotal {
		var count int64
		countQuery := applyListFilters(baseProjectionQuery(tx), normalized).Count(&count)
		if countQuery.Error != nil {
			return ListPage{}, mapError(ctx.Err(), "count list projections", countQuery.Error)
		}
		if count < 0 || count > maximumObjectsPerTenant {
			return ListPage{}, fmt.Errorf("%w: filtered object count is invalid", ErrCorrupt)
		}
		total := uint64(count)
		page.TotalSize = &total
		page.TotalSizeExact = true
	}
	finalCatalog, err := readCatalogState(tx, normalized.scope.tenantID)
	if err != nil {
		return ListPage{}, mapError(ctx.Err(), "re-read list revision", err)
	}
	if !finalCatalog.equal(catalog) {
		return ListPage{}, fmt.Errorf("%w: catalog revision changed inside one read snapshot", ErrCorrupt)
	}
	if err := tx.Commit().Error; err != nil {
		return ListPage{}, mapError(ctx.Err(), "commit list", err)
	}
	return page, nil
}

func validateListDefinitionBudget(maximumBytes int64, groups ...[]projectionRecord) error {
	if maximumBytes < maximumDefinitionBytes {
		return fmt.Errorf("%w: knowledge catalog list definition budget is invalid", ErrCorrupt)
	}
	var total int64
	for _, records := range groups {
		for _, record := range records {
			charge, err := listDefinitionCharge(record)
			if err != nil {
				return err
			}
			if total > maximumBytes-charge {
				return fmt.Errorf("%w: knowledge catalog list definition budget exceeded", control.ErrCapacityExceeded)
			}
			total += charge
		}
	}
	return nil
}

func boundedListResponseRecords(
	records []projectionRecord,
	pageSize int,
) ([]projectionRecord, bool, error) {
	if pageSize < 1 || pageSize > MaximumPageSize || len(records) > pageSize+1 {
		return nil, false, fmt.Errorf("%w: invalid list response bound", ErrCorrupt)
	}
	count := 0
	var totalDefinitionBytes, totalDependencies int64
	for count < len(records) && count < pageSize {
		charge, err := listDefinitionCharge(records[count])
		if err != nil {
			return nil, false, err
		}
		if records[count].DependencyCount < 0 || records[count].DependencyCount > maximumDependenciesPerVersion {
			return nil, false, fmt.Errorf("%w: projected dependency count is invalid", ErrCorrupt)
		}
		if count > 0 && (totalDefinitionBytes > MaximumListResponseCanonicalDefinitionBytes-charge ||
			totalDependencies > MaximumListResponseDependencies-records[count].DependencyCount) {
			break
		}
		totalDefinitionBytes += charge
		totalDependencies += records[count].DependencyCount
		count++
	}
	return records[:count:count], count < len(records), nil
}

func listDefinitionCharge(record projectionRecord) (int64, error) {
	if record.State == StateQuarantined {
		if record.DefinitionBytes != 0 {
			return 0, fmt.Errorf("%w: quarantined projection has definition bytes", ErrCorrupt)
		}
		return 0, nil
	}
	if record.DefinitionBytes < 1 || record.DefinitionBytes > maximumDefinitionBytes {
		return 0, fmt.Errorf("%w: projected definition byte authority is invalid", ErrCorrupt)
	}
	return record.DefinitionBytes, nil
}
