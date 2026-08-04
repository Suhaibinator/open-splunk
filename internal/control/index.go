package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"gorm.io/gorm"
)

const (
	maxIndexNameBytes            = indexname.MaximumBytes
	maximumIndexIDBytes          = 128
	maximumIndexDisplayNameBytes = 255
	maximumIndexDescriptionBytes = 8 << 10
	maximumIndexSourcetypeBytes  = indexpolicy.MaximumDefaultSourcetypeBytes
)

var errIndexIDCollision = errors.New("control: index ID collision")

// IndexState is the lifecycle state of a logical event index.
type IndexState string

const (
	IndexStateActive   IndexState = "active"
	IndexStateArchived IndexState = "archived"
	IndexStateDeleting IndexState = "deleting"
)

// IndexLimits contains optional per-event validation limits. A zero value
// means the server-wide default is used.
type IndexLimits = indexpolicy.Limits

// IndexDefinition contains the mutable configuration of an index except for
// Name, whose normalized value is immutable after creation.
type IndexDefinition struct {
	Name                string
	DisplayName         string
	Description         string
	RetentionPeriod     time.Duration
	IngestionEnabled    bool
	SearchEnabled       bool
	DefaultSourcetype   string
	Limits              IndexLimits
	IngestionRateLimits ingestquota.Limits
}

// Index is an optimistic-versioned logical index record.
type Index struct {
	ID         string
	Version    uint64
	Definition IndexDefinition
	State      IndexState
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// NormalizeIndexName canonicalizes a user index name while enforcing Splunk's
// user-index character restrictions: lowercase ASCII letters, numbers,
// underscores, and hyphens; a leading letter or number; no "kvstore".
func NormalizeIndexName(input string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(input))
	if len(name) == 0 || len(name) > maxIndexNameBytes {
		return "", fmt.Errorf("%w: index name must contain between 1 and %d ASCII characters", ErrInvalidArgument, maxIndexNameBytes)
	}
	if !indexname.ValidSyntax(name) {
		return "", fmt.Errorf("%w: index name must start with a letter or number and contain only lowercase letters, numbers, underscores, and hyphens", ErrInvalidArgument)
	}
	if indexname.ContainsReservedWord(name) {
		return "", fmt.Errorf("%w: index name contains a reserved word", ErrInvalidArgument)
	}
	return name, nil
}

// CreateIndex creates an active logical index at version 1.
func (db *DB) CreateIndex(ctx context.Context, definition IndexDefinition) (Index, error) {
	return db.createIndex(ctx, definition, nil)
}

func (db *DB) createIndex(
	ctx context.Context,
	definition IndexDefinition,
	auditPublisher indexMutationAuditPublisher,
) (Index, error) {
	if ctx == nil {
		return Index{}, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return Index{}, err
	}
	now := databaseTime(time.Now())
	definition, err := validateIndexDefinition(definition, now)
	if err != nil {
		return Index{}, err
	}

	for attempt := 0; attempt < 3; attempt++ {
		id, err := randomID("idx_", 16)
		if err != nil {
			return Index{}, fmt.Errorf("generate index ID: %w", err)
		}
		created, createErr := db.createIndexOnce(
			ctx,
			id,
			definition,
			now,
			auditPublisher,
		)
		if errors.Is(createErr, errIndexIDCollision) {
			continue
		}
		return created, createErr
	}
	return Index{}, errors.New("create index: repeated random ID collision")
}

func (db *DB) createIndexOnce(
	ctx context.Context,
	id string,
	definition IndexDefinition,
	now time.Time,
	auditPublisher indexMutationAuditPublisher,
) (result Index, returnedErr error) {
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Index{}, fmt.Errorf("begin index creation: %w", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	before, err := readIndexCatalogIntegrity(tx)
	if err != nil {
		return Index{}, fmt.Errorf("read index catalog before creation: %w", err)
	}
	var existing indexRecord
	nameErr := tx.
		Select("index_id").
		Where("name = ?", definition.Name).
		Take(&existing).Error
	switch {
	case nameErr == nil:
		return Index{}, fmt.Errorf(
			"%w: index name %q",
			ErrAlreadyExists,
			definition.Name,
		)
	case !errors.Is(nameErr, gorm.ErrRecordNotFound):
		return Index{}, fmt.Errorf("check duplicate index name: %w", nameErr)
	}
	if before.Revision == math.MaxInt64 {
		return Index{}, errors.New("index catalog revision is exhausted")
	}
	if before.PhysicalCount == MaximumPhysicalIndexRecords {
		return Index{}, ErrCapacityExceeded
	}
	idErr := tx.
		Select("index_id").
		Where("index_id = ?", id).
		Take(&existing).Error
	switch {
	case idErr == nil:
		return Index{}, errIndexIDCollision
	case !errors.Is(idErr, gorm.ErrRecordNotFound):
		return Index{}, fmt.Errorf("check duplicate index ID: %w", idErr)
	}

	record := newIndexRecord(id, definition, now)
	if err := tx.Create(&record).Error; err != nil {
		return Index{}, fmt.Errorf("create index: %w", err)
	}
	after, err := readIndexCatalogState(tx)
	if err != nil {
		return Index{}, fmt.Errorf("read index catalog after creation: %w", err)
	}
	if after.Revision != before.Revision+1 ||
		after.PhysicalCount != before.PhysicalCount+1 {
		return Index{}, errors.New(
			"create index: index catalog accounting did not advance",
		)
	}
	result, err = indexFromRecord(record)
	if err != nil {
		return Index{}, fmt.Errorf("read created index: %w", err)
	}
	if err := publishIndexMutationAudit(
		ctx,
		tx,
		auditPublisher,
		IndexMutationAuditEvent{
			OccurredAt:   result.CreatedAt,
			Action:       IndexMutationAuditActionCreate,
			IndexID:      result.ID,
			IndexVersion: result.Version,
		},
	); err != nil {
		return Index{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return Index{}, fmt.Errorf("commit index creation: %w", err)
	}
	return result, nil
}

// GetIndex gets an index by stable ID.
func (db *DB) GetIndex(ctx context.Context, id string) (Index, error) {
	if strings.TrimSpace(id) == "" {
		return Index{}, fmt.Errorf("%w: index ID is required", ErrInvalidArgument)
	}
	record, err := takeIndexRecord(db.orm.WithContext(ctx), "index_id = ?", id)
	if err != nil {
		return Index{}, indexLookupError(err)
	}
	index, err := indexFromRecord(record)
	return index, indexLookupError(err)
}

// GetIndexByName gets an index by its normalized immutable name.
func (db *DB) GetIndexByName(ctx context.Context, name string) (Index, error) {
	normalized, err := NormalizeIndexName(name)
	if err != nil {
		return Index{}, err
	}
	record, err := takeIndexRecord(db.orm.WithContext(ctx), "name = ?", normalized)
	if err != nil {
		return Index{}, indexLookupError(err)
	}
	index, err := indexFromRecord(record)
	return index, indexLookupError(err)
}

// GetIndexesByNames gets a bounded, unique set of live indexes by canonical
// immutable name using one catalog query. Results preserve the caller's exact
// input order. If any requested name is absent or terminally tombstoned, the
// method returns no partial records and ErrNotFound.
func (db *DB) GetIndexesByNames(
	ctx context.Context,
	names []string,
) ([]Index, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(names) == 0 || len(names) > MaximumPhysicalIndexRecords {
		return nil, fmt.Errorf(
			"%w: index-name batch must contain between 1 and %d entries",
			ErrInvalidArgument,
			MaximumPhysicalIndexRecords,
		)
	}

	requested := make([]string, len(names))
	seen := make(map[string]struct{}, len(names))
	for position, name := range names {
		normalized, err := NormalizeIndexName(name)
		if err != nil || normalized != name {
			return nil, fmt.Errorf(
				"%w: index name at position %d is not canonical",
				ErrInvalidArgument,
				position,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate index name %q",
				ErrInvalidArgument,
				name,
			)
		}
		requested[position] = strings.Clone(name)
		seen[requested[position]] = struct{}{}
	}

	var records []indexRecord
	query := visibleIndexRecords(db.orm.WithContext(ctx)).
		Where("name IN ?", requested).
		Order("name").
		Limit(len(requested) + 1).
		Find(&records)
	if query.Error != nil {
		return nil, fmt.Errorf("get indexes by names: %w", query.Error)
	}
	if len(records) != len(requested) {
		return nil, ErrNotFound
	}

	byName := make(map[string]Index, len(records))
	for _, record := range records {
		index, err := indexFromRecord(record)
		if err != nil {
			return nil, fmt.Errorf("get indexes by names: %w", err)
		}
		name := index.Definition.Name
		if _, requestedName := seen[name]; !requestedName {
			return nil, errors.New(
				"get indexes by names: catalog returned an unexpected name",
			)
		}
		if _, duplicate := byName[name]; duplicate {
			return nil, errors.New(
				"get indexes by names: catalog returned a duplicate name",
			)
		}
		byName[name] = index
	}

	indexes := make([]Index, len(requested))
	for position, name := range requested {
		index, ok := byName[name]
		if !ok {
			return nil, ErrNotFound
		}
		indexes[position] = index
	}
	return indexes, nil
}

// ListIndexes lists every live, non-tombstoned index in normalized-name order.
// The hard physical catalog ceiling makes this legacy bootstrap/authorization
// view bounded; administrative clients should use ListIndexPage.
func (db *DB) ListIndexes(
	ctx context.Context,
) (result []Index, returnedErr error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx := db.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return nil, fmt.Errorf("begin index list: %w", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	if _, err := readIndexCatalogIntegrity(tx); err != nil {
		return nil, fmt.Errorf("read index catalog state: %w", err)
	}

	var records []indexRecord
	query := visibleIndexRecords(tx).
		Order("name").
		Limit(MaximumPhysicalIndexRecords + 1).
		Find(&records)
	if query.Error != nil {
		return nil, fmt.Errorf("list indexes: %w", query.Error)
	}
	if len(records) > MaximumPhysicalIndexRecords {
		return nil, errors.New("list indexes: index catalog exceeds its bound")
	}
	indexes := make([]Index, 0, len(records))
	for _, record := range records {
		index, err := indexFromRecord(record)
		if err != nil {
			return nil, fmt.Errorf("scan listed index: %w", err)
		}
		indexes = append(indexes, index)
	}
	if err := tx.Commit().Error; err != nil {
		return nil, fmt.Errorf("commit index list: %w", err)
	}
	return indexes, nil
}

// UpdateIndex replaces mutable index configuration when expectedVersion is
// current. The normalized name must match the existing immutable name.
func (db *DB) UpdateIndex(ctx context.Context, id string, expectedVersion uint64, definition IndexDefinition) (result Index, err error) {
	return db.updateIndex(ctx, id, expectedVersion, definition, nil)
}

func (db *DB) updateIndex(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	definition IndexDefinition,
	auditPublisher indexMutationAuditPublisher,
) (result Index, err error) {
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return Index{}, err
	}
	now := databaseTime(time.Now())
	definition, err = validateIndexDefinition(definition, now)
	if err != nil {
		return Index{}, err
	}
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Index{}, fmt.Errorf("begin index update: %w", tx.Error)
	}
	defer finishGORMTx(tx, &err)

	currentRecord, err := takeIndexRecord(tx, "index_id = ?", id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Index{}, ErrNotFound
	}
	if err != nil {
		return Index{}, fmt.Errorf("read index for update: %w", err)
	}
	current, err := indexFromRecord(currentRecord)
	if err != nil {
		return Index{}, fmt.Errorf("read index for update: %w", err)
	}
	if current.Version != expectedVersion {
		return Index{}, ErrVersionConflict
	}
	if current.State == IndexStateDeleting {
		return Index{}, ErrDependencyConflict
	}
	if current.Definition.Name != definition.Name {
		return Index{}, ErrImmutableName
	}
	if !definition.SearchEnabled {
		dependent, dependencyErr := activeAppUsesIndex(tx, id)
		if dependencyErr != nil {
			return Index{}, fmt.Errorf("check active app index dependency: %w", dependencyErr)
		}
		if dependent {
			return Index{}, ErrDependencyConflict
		}
	}

	// #nosec G115 -- validateExpectedVersion bounds expectedVersion by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	update := tx.Model(&indexRecord{}).
		Where("index_id = ? AND version = ?", id, expectedVersionDB).
		Updates(indexDefinitionUpdates(definition, now))
	if update.Error != nil {
		return Index{}, fmt.Errorf("update index: %w", update.Error)
	}
	if err := requireOneUpdated(update.RowsAffected); err != nil {
		return Index{}, err
	}
	updatedRecord, err := takeIndexRecord(tx, "index_id = ?", id)
	if err != nil {
		return Index{}, fmt.Errorf("read updated index: %w", err)
	}
	result, err = indexFromRecord(updatedRecord)
	if err != nil {
		return Index{}, fmt.Errorf("read updated index: %w", err)
	}
	if err := publishIndexMutationAudit(
		ctx,
		tx,
		auditPublisher,
		IndexMutationAuditEvent{
			OccurredAt:   result.UpdatedAt,
			Action:       IndexMutationAuditActionUpdate,
			IndexID:      result.ID,
			IndexVersion: result.Version,
		},
	); err != nil {
		return Index{}, err
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return Index{}, fmt.Errorf("commit index update: %w", commitErr)
	}
	return result, nil
}

// SetIndexState changes an index lifecycle state under optimistic locking.
func (db *DB) SetIndexState(ctx context.Context, id string, expectedVersion uint64, state IndexState) (result Index, err error) {
	return db.setIndexState(ctx, id, expectedVersion, state, nil)
}

func (db *DB) setIndexState(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	state IndexState,
	auditPublisher indexMutationAuditPublisher,
) (result Index, err error) {
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return Index{}, err
	}
	if !validIndexState(state) {
		return Index{}, fmt.Errorf("%w: unknown index state", ErrInvalidArgument)
	}
	if state == IndexStateDeleting {
		return Index{}, fmt.Errorf("%w: deleting is reserved for the physical deletion coordinator", ErrInvalidArgument)
	}
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Index{}, fmt.Errorf("begin index state update: %w", tx.Error)
	}
	defer finishGORMTx(tx, &err)

	currentRecord, err := takeIndexRecord(tx, "index_id = ?", id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Index{}, ErrNotFound
	}
	if err != nil {
		return Index{}, fmt.Errorf("read index for state update: %w", err)
	}
	current, err := indexFromRecord(currentRecord)
	if err != nil {
		return Index{}, fmt.Errorf("read index for state update: %w", err)
	}
	if current.Version != expectedVersion {
		return Index{}, ErrVersionConflict
	}
	if current.State == IndexStateDeleting {
		return Index{}, ErrDependencyConflict
	}
	if state != IndexStateActive {
		dependent, dependencyErr := activeAppUsesIndex(tx, id)
		if dependencyErr != nil {
			return Index{}, fmt.Errorf("check active app index dependency: %w", dependencyErr)
		}
		if dependent {
			return Index{}, ErrDependencyConflict
		}
	}

	now := databaseTime(time.Now())
	// #nosec G115 -- validateExpectedVersion bounds expectedVersion by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	update := tx.Model(&indexRecord{}).
		Where("index_id = ? AND version = ?", id, expectedVersionDB).
		Updates(map[string]any{
			"state":                 state,
			"updated_at_unix_micro": now.UnixMicro(),
			"version":               gorm.Expr("version + 1"),
		})
	if update.Error != nil {
		return Index{}, fmt.Errorf("set index state: %w", update.Error)
	}
	if err := requireOneUpdated(update.RowsAffected); err != nil {
		if errors.Is(err, ErrVersionConflict) {
			var existing indexRecord
			lookupErr := tx.Select("index_id").Where("index_id = ?", id).Take(&existing).Error
			if errors.Is(lookupErr, gorm.ErrRecordNotFound) {
				return Index{}, ErrNotFound
			}
			if lookupErr != nil {
				return Index{}, fmt.Errorf("check index after state conflict: %w", lookupErr)
			}
		}
		return Index{}, err
	}
	updatedRecord, err := takeIndexRecord(tx, "index_id = ?", id)
	if err != nil {
		return Index{}, fmt.Errorf("read index after state update: %w", err)
	}
	result, err = indexFromRecord(updatedRecord)
	if err != nil {
		return Index{}, fmt.Errorf("read index after state update: %w", err)
	}
	action := IndexMutationAuditActionActivate
	if state == IndexStateArchived {
		action = IndexMutationAuditActionArchive
	}
	if err := publishIndexMutationAudit(
		ctx,
		tx,
		auditPublisher,
		IndexMutationAuditEvent{
			OccurredAt:   result.UpdatedAt,
			Action:       action,
			IndexID:      result.ID,
			IndexVersion: result.Version,
		},
	); err != nil {
		return Index{}, err
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return Index{}, fmt.Errorf("commit index state update: %w", commitErr)
	}
	return result, nil
}

// DeleteIndex performs a terminal logical deletion while retaining physical
// event data. The archived index row remains in place to preserve references
// and permanently reserve its ClickHouse-facing canonical name; a durable
// tombstone hides it from the live catalog and makes it immutable.
func (db *DB) DeleteIndex(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	confirmationName string,
) (deletedID string, err error) {
	return db.deleteIndex(ctx, id, expectedVersion, confirmationName, nil)
}

func (db *DB) deleteIndex(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	confirmationName string,
	auditPublisher indexMutationAuditPublisher,
) (deletedID string, err error) {
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return "", err
	}
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("%w: index ID is required", ErrInvalidArgument)
	}
	canonicalConfirmation, err := NormalizeIndexName(confirmationName)
	if err != nil || canonicalConfirmation != confirmationName {
		return "", fmt.Errorf("%w: index deletion confirmation is invalid", ErrInvalidArgument)
	}
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return "", fmt.Errorf("begin index deletion: %w", tx.Error)
	}
	defer finishGORMTx(tx, &err)

	currentRecord, err := takeIndexRecord(tx, "index_id = ?", id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read index for deletion: %w", err)
	}
	current, err := indexFromRecord(currentRecord)
	if err != nil {
		return "", fmt.Errorf("read index for deletion: %w", err)
	}
	if current.Version != expectedVersion {
		return "", ErrVersionConflict
	}
	if current.State != IndexStateArchived {
		return "", ErrDependencyConflict
	}
	if canonicalConfirmation != current.Definition.Name {
		return "", fmt.Errorf("%w: confirmation name must exactly match the canonical index name", ErrInvalidArgument)
	}

	tombstone := indexDeletionTombstoneRecord{
		IndexID:            current.ID,
		Name:               current.Definition.Name,
		DeletedVersion:     currentRecord.Version,
		DeletedAtUnixMicro: databaseTime(time.Now()).UnixMicro(),
	}
	if create := tx.Create(&tombstone); create.Error != nil {
		return "", fmt.Errorf("create index deletion tombstone: %w", create.Error)
	}
	if err := publishIndexMutationAudit(
		ctx,
		tx,
		auditPublisher,
		IndexMutationAuditEvent{
			OccurredAt:   time.UnixMicro(tombstone.DeletedAtUnixMicro).UTC(),
			Action:       IndexMutationAuditActionDeleteKeepData,
			IndexID:      current.ID,
			IndexVersion: current.Version,
		},
	); err != nil {
		return "", err
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return "", fmt.Errorf("commit index deletion: %w", commitErr)
	}
	return current.ID, nil
}

func activeAppUsesIndex(database *gorm.DB, indexID string) (bool, error) {
	var dependency appDefaultIndexRecord
	result := database.Table("app_default_indexes AS app_index").
		Select("app_index.tenant_id, app_index.app_id, app_index.index_id").
		Joins(`
			JOIN app_workspaces AS app
			  ON app.tenant_id = app_index.tenant_id
			 AND app.app_id = app_index.app_id`).
		Where("app_index.index_id = ? AND app.state = ?", indexID, AppStateActive).
		Take(&dependency)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if result.Error != nil {
		return false, result.Error
	}
	return true, nil
}

func takeIndexRecord(database *gorm.DB, query string, args ...any) (indexRecord, error) {
	var record indexRecord
	result := visibleIndexRecords(database).Where(query, args...).Take(&record)
	return record, result.Error
}

func visibleIndexRecords(database *gorm.DB) *gorm.DB {
	return database.Where(`
		NOT EXISTS (
			SELECT 1
			FROM index_deletion_tombstones
			WHERE index_deletion_tombstones.index_id = indexes.index_id
		)`)
}

func newIndexRecord(id string, definition IndexDefinition, now time.Time) indexRecord {
	return indexRecord{
		IndexID:                             id,
		Version:                             1,
		Name:                                definition.Name,
		DisplayName:                         definition.DisplayName,
		Description:                         definition.Description,
		RetentionNanoseconds:                int64(definition.RetentionPeriod),
		IngestionEnabled:                    boolInteger(definition.IngestionEnabled),
		SearchEnabled:                       boolInteger(definition.SearchEnabled),
		DefaultSourcetype:                   definition.DefaultSourcetype,
		MaxEventBytes:                       int64(definition.Limits.MaxEventBytes), // #nosec G115 -- validation bounds this value.
		MaxFieldCount:                       int64(definition.Limits.MaxFieldCount),
		MaxNestingDepth:                     int64(definition.Limits.MaxNestingDepth),
		MaximumFutureSkewNanoseconds:        int64(definition.Limits.MaximumFutureSkew),
		MaximumEventAgeNanoseconds:          int64(definition.Limits.MaximumEventAge),
		State:                               IndexStateActive,
		CreatedAtUnixMicro:                  now.UnixMicro(),
		UpdatedAtUnixMicro:                  now.UnixMicro(),
		MaxIngestEventsPerSecond:            int64(definition.IngestionRateLimits.MaxEventsPerSecond),            // #nosec G115 -- validation bounds this value.
		MaxIngestUncompressedBytesPerSecond: int64(definition.IngestionRateLimits.MaxUncompressedBytesPerSecond), // #nosec G115 -- validation bounds this value.
	}
}

func indexDefinitionUpdates(definition IndexDefinition, now time.Time) map[string]any {
	return map[string]any{
		"default_sourcetype":                       definition.DefaultSourcetype,
		"description":                              definition.Description,
		"display_name":                             definition.DisplayName,
		"ingestion_enabled":                        boolInteger(definition.IngestionEnabled),
		"max_event_bytes":                          int64(definition.Limits.MaxEventBytes), // #nosec G115 -- validation bounds this value.
		"max_field_count":                          int64(definition.Limits.MaxFieldCount),
		"max_nesting_depth":                        int64(definition.Limits.MaxNestingDepth),
		"maximum_event_age_nanoseconds":            int64(definition.Limits.MaximumEventAge),
		"maximum_future_skew_nanoseconds":          int64(definition.Limits.MaximumFutureSkew),
		"max_ingest_events_per_second":             int64(definition.IngestionRateLimits.MaxEventsPerSecond),            // #nosec G115 -- validation bounds this value.
		"max_ingest_uncompressed_bytes_per_second": int64(definition.IngestionRateLimits.MaxUncompressedBytesPerSecond), // #nosec G115 -- validation bounds this value.
		"retention_nanoseconds":                    int64(definition.RetentionPeriod),
		"search_enabled":                           boolInteger(definition.SearchEnabled),
		"updated_at_unix_micro":                    now.UnixMicro(),
		"version":                                  gorm.Expr("version + 1"),
	}
}

func indexFromRecord(record indexRecord) (Index, error) {
	normalizedName, nameErr := NormalizeIndexName(record.Name)
	if record.Version < 1 ||
		!validIndexIdentifier(record.IndexID) ||
		nameErr != nil ||
		normalizedName != record.Name ||
		validateIndexText(
			record.DisplayName,
			maximumIndexDisplayNameBytes,
			false,
			false,
		) != nil ||
		strings.TrimSpace(record.DisplayName) != record.DisplayName ||
		validateIndexText(
			record.Description,
			maximumIndexDescriptionBytes,
			true,
			true,
		) != nil ||
		!indexpolicy.ValidDefaultSourcetype(record.DefaultSourcetype) ||
		record.CreatedAtUnixMicro <= 0 ||
		record.CreatedAtUnixMicro > maximumControlTimestampUnixMicro ||
		record.UpdatedAtUnixMicro < record.CreatedAtUnixMicro ||
		record.UpdatedAtUnixMicro > maximumControlTimestampUnixMicro ||
		record.RetentionNanoseconds < 0 ||
		record.RetentionNanoseconds%int64(time.Millisecond) != 0 ||
		record.MaxEventBytes < 0 ||
		record.MaxFieldCount < 0 ||
		record.MaxFieldCount > math.MaxUint32 ||
		record.MaxNestingDepth < 0 ||
		record.MaxNestingDepth > math.MaxUint32 ||
		record.MaximumFutureSkewNanoseconds < 0 ||
		record.MaximumEventAgeNanoseconds < 0 ||
		record.MaxIngestEventsPerSecond < 0 ||
		record.MaxIngestUncompressedBytesPerSecond < 0 ||
		(record.IngestionEnabled != 0 && record.IngestionEnabled != 1) ||
		(record.SearchEnabled != 0 && record.SearchEnabled != 1) ||
		!validIndexState(record.State) {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	limits := IndexLimits{
		// #nosec G115 -- the signed database scalar is checked nonnegative above.
		MaxEventBytes: uint64(record.MaxEventBytes),
		// #nosec G115 -- the scalar is checked in [0, MaxUint32] above.
		MaxFieldCount: uint32(record.MaxFieldCount),
		// #nosec G115 -- the scalar is checked in [0, MaxUint32] above.
		MaxNestingDepth:   uint32(record.MaxNestingDepth),
		MaximumFutureSkew: time.Duration(record.MaximumFutureSkewNanoseconds),
		MaximumEventAge:   time.Duration(record.MaximumEventAgeNanoseconds),
	}
	if err := limits.Validate(); err != nil {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	ingestionRateLimits := ingestquota.Limits{
		// #nosec G115 -- the signed database scalars are checked nonnegative above.
		MaxEventsPerSecond:            uint64(record.MaxIngestEventsPerSecond),
		MaxUncompressedBytesPerSecond: uint64(record.MaxIngestUncompressedBytesPerSecond),
	}
	if err := ingestionRateLimits.Validate(); err != nil {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	updatedAt := time.UnixMicro(record.UpdatedAtUnixMicro).UTC()
	if err := indexpolicy.ValidateRetentionAt(
		time.Duration(record.RetentionNanoseconds),
		updatedAt,
		true,
	); err != nil {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	return Index{
		ID:      record.IndexID,
		Version: uint64(record.Version),
		Definition: IndexDefinition{
			Name:                record.Name,
			DisplayName:         record.DisplayName,
			Description:         record.Description,
			RetentionPeriod:     time.Duration(record.RetentionNanoseconds),
			IngestionEnabled:    record.IngestionEnabled == 1,
			SearchEnabled:       record.SearchEnabled == 1,
			DefaultSourcetype:   record.DefaultSourcetype,
			Limits:              limits,
			IngestionRateLimits: ingestionRateLimits,
		},
		State:     record.State,
		CreatedAt: time.UnixMicro(record.CreatedAtUnixMicro).UTC(),
		UpdatedAt: updatedAt,
	}, nil
}

func indexLookupError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		return ErrNotFound
	default:
		return fmt.Errorf("get index: %w", err)
	}
}

func validateIndexDefinition(
	definition IndexDefinition,
	reference time.Time,
) (IndexDefinition, error) {
	name, err := NormalizeIndexName(definition.Name)
	if err != nil {
		return IndexDefinition{}, err
	}
	definition.Name = name
	definition.DisplayName = strings.TrimSpace(definition.DisplayName)
	if definition.DisplayName == "" {
		definition.DisplayName = name
	}
	definition.DefaultSourcetype = strings.TrimSpace(definition.DefaultSourcetype)
	if err := validateIndexText(
		definition.DisplayName,
		maximumIndexDisplayNameBytes,
		false,
		false,
	); err != nil {
		return IndexDefinition{}, err
	}
	if err := validateIndexText(
		definition.Description,
		maximumIndexDescriptionBytes,
		true,
		true,
	); err != nil {
		return IndexDefinition{}, err
	}
	if !indexpolicy.ValidDefaultSourcetype(definition.DefaultSourcetype) {
		return IndexDefinition{}, fmt.Errorf("%w: default sourcetype is invalid", ErrInvalidArgument)
	}
	if err := indexpolicy.ValidateRetentionAt(definition.RetentionPeriod, reference, true); err != nil {
		return IndexDefinition{}, fmt.Errorf("%w: index retention: %w", ErrInvalidArgument, err)
	}
	if err := definition.Limits.Validate(); err != nil {
		return IndexDefinition{}, fmt.Errorf("%w: index limits: %w", ErrInvalidArgument, err)
	}
	if err := definition.IngestionRateLimits.Validate(); err != nil {
		return IndexDefinition{}, fmt.Errorf("%w: index ingestion rate limits: %w", ErrInvalidArgument, err)
	}
	return definition, nil
}

func validIndexIdentifier(value string) bool {
	if value == "" ||
		len(value) > maximumIndexIDBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validateIndexText(
	value string,
	maximumBytes int,
	allowEmpty bool,
	allowLineBreaks bool,
) error {
	if (!allowEmpty && value == "") ||
		len(value) > maximumBytes ||
		!utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: index text is invalid", ErrInvalidArgument)
	}
	for _, character := range value {
		if unicode.IsControl(character) &&
			(!allowLineBreaks ||
				(character != '\n' &&
					character != '\r' &&
					character != '\t')) {
			return fmt.Errorf("%w: index text is invalid", ErrInvalidArgument)
		}
	}
	return nil
}

func validateExpectedVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return fmt.Errorf("%w: expected version is outside the supported range", ErrInvalidArgument)
	}
	return nil
}

func validIndexState(state IndexState) bool {
	switch state {
	case IndexStateActive, IndexStateArchived, IndexStateDeleting:
		return true
	default:
		return false
	}
}

func requireOneUpdated(count int64) error {
	if count != 1 {
		return ErrVersionConflict
	}
	return nil
}

func finishGORMTx(tx *gorm.DB, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
		*returnedErr = errors.Join(*returnedErr, fmt.Errorf("roll back transaction: %w", err))
	}
}

func databaseTime(value time.Time) time.Time {
	return time.UnixMicro(value.UTC().UnixMicro()).UTC()
}

func boolInteger(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func randomID(prefix string, randomBytes int) (string, error) {
	buffer := make([]byte, randomBytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(buffer), nil
}
