package collectorfleet

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"fortio.org/safecast"
	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// ListResult is one detached, revision-stable collector catalog page.
type ListResult struct {
	Entries         []CatalogEntry
	NextPageToken   *string
	TotalSize       *uint64
	TotalSizeExact  bool
	CatalogRevision uint64
}

// Catalog owns tenant-scoped GORM reads over the migrated control database.
// SQL migrations remain the schema authority.
type Catalog struct {
	orm       *gorm.DB
	cursorKey []byte
}

// NewCatalog constructs a collector catalog without changing the SQL schema.
func NewCatalog(
	database *control.DB,
	options CatalogOptions,
) (*Catalog, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, invalid("control database is required")
	}
	cursorKey, err := normalizeCatalogCursorKey(options.CursorKey)
	if err != nil {
		return nil, err
	}
	return &Catalog{
		orm:       database.GORMDB(),
		cursorKey: cursorKey,
	}, nil
}

// Get returns one collector with an exact process-local connection-state
// overlay. A liveness tuple that does not match the complete durable lease is
// ignored and the enabled collector is reported offline.
func (catalog *Catalog) Get(
	ctx context.Context,
	scope Scope,
	collectorID string,
	liveness []CollectorLiveness,
) (result CatalogEntry, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return CatalogEntry{}, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return CatalogEntry{}, err
	}
	if !validIdentifier(collectorID) {
		return CatalogEntry{}, invalid(
			"collector ID is not a canonical identifier",
		)
	}
	view, err := newCatalogLivenessView(scope, liveness)
	if err != nil {
		return CatalogEntry{}, err
	}

	tx := catalog.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return CatalogEntry{}, mapContextError(
			ctx,
			"begin collector catalog read",
			tx.Error,
		)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	collector, err := loadCollector(tx, scope, collectorID)
	if err != nil {
		return CatalogEntry{}, err
	}
	result = view.entry(collector)
	if err := tx.Commit().Error; err != nil {
		return CatalogEntry{}, mapContextError(
			ctx,
			"commit collector catalog read",
			err,
		)
	}
	finished = true
	return result, nil
}

// List returns one bounded keyset page. The first page's database revision,
// rows, optional exact count, and child hydration share one SQLite read
// snapshot. The revision row also validates fleet/runtime parent integrity
// with one constant-size indexed read. Signed continuations carry the exact
// count without recounting and are invalidated when either durable list-visible
// state or the complete bounded process-liveness view changes.
func (catalog *Catalog) List(
	ctx context.Context,
	scope Scope,
	liveness []CollectorLiveness,
	request ListRequest,
) (result ListResult, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return ListResult{}, err
	}
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		return ListResult{}, err
	}
	normalizedScope := Scope{TenantID: normalized.tenantID}
	view, err := newCatalogLivenessView(normalizedScope, liveness)
	if err != nil {
		return ListResult{}, err
	}
	requestHash, err := collectorListFilterHash(normalized)
	if err != nil {
		return ListResult{}, err
	}

	tx := catalog.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return ListResult{}, mapContextError(
			ctx,
			"begin collector catalog list",
			tx.Error,
		)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	revision, err := readCollectorCatalogRevision(tx, normalized.tenantID)
	if err != nil {
		return ListResult{}, mapContextError(
			ctx,
			"read collector catalog revision",
			err,
		)
	}
	cursor := collectorListCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeCollectorListCursor(
			catalog.cursorKey,
			normalized.pageToken,
			requestHash,
			revision,
			view.digest,
			normalized.sortBy,
			normalized.includeTotal,
		)
		if err != nil {
			return ListResult{}, err
		}
	}

	query := collectorCatalogPageQuery(tx, normalized, view)
	query = applyCollectorCatalogCursor(query, normalized, cursor)
	query = applyCollectorCatalogOrder(query, normalized)
	var pageRecords []catalogPageRecord
	if err := query.
		Limit(int(normalized.pageSize) + 1).
		Find(&pageRecords).Error; err != nil {
		return ListResult{}, mapContextError(
			ctx,
			"list collector catalog",
			err,
		)
	}
	hasMore := len(pageRecords) > int(normalized.pageSize)
	if hasMore {
		pageRecords = pageRecords[:normalized.pageSize]
	}
	collectorIDs := make([]string, len(pageRecords))
	for index, record := range pageRecords {
		collectorIDs[index] = record.CollectorID
	}
	collectors, err := loadCollectors(tx, normalizedScope, collectorIDs)
	if err != nil {
		return ListResult{}, fmt.Errorf(
			"hydrate collector catalog page: %w",
			err,
		)
	}
	result = ListResult{
		Entries: make([]CatalogEntry, len(collectors)),

		CatalogRevision: safecast.MustConv[uint64](revision),
	}
	for index, collector := range collectors {
		if collector.CollectorID != pageRecords[index].CollectorID {
			return ListResult{}, errors.New(
				"collector catalog hydration order is inconsistent",
			)
		}
		result.Entries[index] = view.entry(collector)
	}
	var totalSize uint64
	if normalized.includeTotal {
		if normalized.pageToken != "" {
			totalSize = *cursor.TotalSize
		} else {
			var count int64
			if err := collectorCatalogFilteredQuery(tx, normalized, view).
				Count(&count).Error; err != nil {
				return ListResult{}, mapContextError(
					ctx,
					"count collector catalog",
					err,
				)
			}
			if count < 0 {
				return ListResult{}, errors.New(
					"count collector catalog returned a negative result",
				)
			}
			totalSize = uint64(count)
		}
		result.TotalSize = &totalSize
		result.TotalSizeExact = true
	}
	if hasMore {
		last := pageRecords[len(pageRecords)-1]
		nextCursor := collectorCursorForPageRecord(
			last,
			normalized,
			requestHash,
			revision,
			view.digest,
		)
		if normalized.includeTotal {
			nextCursor.TotalSize = &totalSize
		}
		token, encodeErr := encodeCollectorListCursor(
			catalog.cursorKey,
			nextCursor,
			normalized.sortBy,
		)
		if encodeErr != nil {
			return ListResult{}, encodeErr
		}
		result.NextPageToken = cloneString(&token)
	}
	if err := tx.Commit().Error; err != nil {
		return ListResult{}, mapContextError(
			ctx,
			"commit collector catalog list",
			err,
		)
	}
	finished = true
	return result, nil
}

func readCollectorCatalogRevision(
	database *gorm.DB,
	tenantID string,
) (int64, error) {
	record, exists, err := readCollectorCatalogMarker(database, tenantID)
	if err != nil || !exists {
		return 0, err
	}
	return record.Revision, nil
}

// readCollectorCatalogMarker returns the trigger-maintained parent counts
// used by both list-snapshot validation and new-identity admission. Absence is
// valid only for a genuinely never-seen tenant.
func readCollectorCatalogMarker(
	database *gorm.DB,
	tenantID string,
) (collectorCatalogRevisionRecord, bool, error) {
	var record collectorCatalogRevisionRecord
	err := database.
		Where("tenant_id = ?", tenantID).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		var parentMarker struct {
			Value int `gorm:"column:value"`
		}
		parentResult := database.Raw(`
			SELECT 1 AS value
			FROM collector_fleet
			WHERE tenant_id = ?
			UNION ALL
			SELECT 1 AS value
			FROM collector_runtime
			WHERE tenant_id = ?
			LIMIT 1`,
			tenantID,
			tenantID,
		).Scan(&parentMarker)
		if parentResult.Error != nil {
			return collectorCatalogRevisionRecord{}, false, parentResult.Error
		}
		if parentResult.RowsAffected != 0 {
			return collectorCatalogRevisionRecord{}, false, errors.New(
				"collector catalog fleet/runtime revision is missing from the control-plane database",
			)
		}
		return collectorCatalogRevisionRecord{}, false, nil
	}
	if err != nil {
		return collectorCatalogRevisionRecord{}, false, err
	}
	if record.Revision < 1 ||
		record.FleetCount < 0 ||
		record.FleetCount > MaximumDurableCollectorsPerTenant ||
		record.RuntimeCount < 0 ||
		record.RuntimeCount > MaximumDurableCollectorsPerTenant {
		return collectorCatalogRevisionRecord{}, false, errors.New(
			"collector catalog fleet/runtime revision is invalid in the control-plane database",
		)
	}
	if record.FleetCount != record.RuntimeCount {
		return collectorCatalogRevisionRecord{}, false, errors.New(
			"collector catalog fleet/runtime counts are inconsistent in the control-plane database",
		)
	}
	return record, true, nil
}
