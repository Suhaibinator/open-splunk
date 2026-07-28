package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"gorm.io/gorm"
)

const maxIndexNameBytes = 255

var splunkIndexName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// IndexState is the lifecycle state of a logical event index.
type IndexState string

const (
	IndexStateActive   IndexState = "active"
	IndexStateArchived IndexState = "archived"
	IndexStateDeleting IndexState = "deleting"
)

// IndexLimits contains optional per-event validation limits. A zero value
// means the server-wide default is used.
type IndexLimits struct {
	MaxEventBytes     uint64
	MaxFieldCount     uint32
	MaxNestingDepth   uint32
	MaximumFutureSkew time.Duration
	MaximumEventAge   time.Duration
}

// IndexDefinition contains the mutable configuration of an index except for
// Name, whose normalized value is immutable after creation.
type IndexDefinition struct {
	Name              string
	DisplayName       string
	Description       string
	RetentionPeriod   time.Duration
	IngestionEnabled  bool
	SearchEnabled     bool
	DefaultSourcetype string
	Limits            IndexLimits
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
	if !splunkIndexName.MatchString(name) {
		return "", fmt.Errorf("%w: index name must start with a letter or number and contain only lowercase letters, numbers, underscores, and hyphens", ErrInvalidArgument)
	}
	if strings.Contains(name, "kvstore") {
		return "", fmt.Errorf("%w: index name contains a reserved word", ErrInvalidArgument)
	}
	return name, nil
}

// CreateIndex creates an active logical index at version 1.
func (db *DB) CreateIndex(ctx context.Context, definition IndexDefinition) (Index, error) {
	definition, err := validateIndexDefinition(definition)
	if err != nil {
		return Index{}, err
	}
	now := databaseTime(time.Now())

	for attempt := 0; attempt < 3; attempt++ {
		id, err := randomID("idx_", 16)
		if err != nil {
			return Index{}, fmt.Errorf("generate index ID: %w", err)
		}
		record := newIndexRecord(id, definition, now)
		create := db.orm.WithContext(ctx).Create(&record)
		if create.Error == nil {
			index, conversionErr := indexFromRecord(record)
			if conversionErr != nil {
				return Index{}, fmt.Errorf("read created index: %w", conversionErr)
			}
			return index, nil
		}

		var existing indexRecord
		nameErr := db.orm.WithContext(ctx).
			Select("index_id").
			Where("name = ?", definition.Name).
			Take(&existing).Error
		if nameErr == nil {
			return Index{}, fmt.Errorf("%w: index name %q", ErrAlreadyExists, definition.Name)
		}
		if !errors.Is(nameErr, gorm.ErrRecordNotFound) {
			return Index{}, fmt.Errorf("check duplicate index name: %w", nameErr)
		}
		idErr := db.orm.WithContext(ctx).
			Select("index_id").
			Where("index_id = ?", id).
			Take(&existing).Error
		if errors.Is(idErr, gorm.ErrRecordNotFound) {
			return Index{}, fmt.Errorf("create index: %w", create.Error)
		}
		if idErr != nil {
			return Index{}, fmt.Errorf("check duplicate index ID: %w", idErr)
		}
		// An ID collision is extraordinarily unlikely, but retrying avoids
		// turning randomness into an availability edge case.
	}
	return Index{}, errors.New("create index: repeated random ID collision")
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

// ListIndexes lists every index in normalized-name order.
func (db *DB) ListIndexes(ctx context.Context) ([]Index, error) {
	var records []indexRecord
	query := db.orm.WithContext(ctx).Order("name").Find(&records)
	if query.Error != nil {
		return nil, fmt.Errorf("list indexes: %w", query.Error)
	}
	indexes := make([]Index, 0, len(records))
	for _, record := range records {
		index, err := indexFromRecord(record)
		if err != nil {
			return nil, fmt.Errorf("scan listed index: %w", err)
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// UpdateIndex replaces mutable index configuration when expectedVersion is
// current. The normalized name must match the existing immutable name.
func (db *DB) UpdateIndex(ctx context.Context, id string, expectedVersion uint64, definition IndexDefinition) (result Index, err error) {
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return Index{}, err
	}
	definition, err = validateIndexDefinition(definition)
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

	now := databaseTime(time.Now())
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
	if commitErr := tx.Commit().Error; commitErr != nil {
		return Index{}, fmt.Errorf("commit index update: %w", commitErr)
	}
	return result, nil
}

// SetIndexState changes an index lifecycle state under optimistic locking.
func (db *DB) SetIndexState(ctx context.Context, id string, expectedVersion uint64, state IndexState) (result Index, err error) {
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return Index{}, err
	}
	if !validIndexState(state) {
		return Index{}, fmt.Errorf("%w: unknown index state", ErrInvalidArgument)
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
	if commitErr := tx.Commit().Error; commitErr != nil {
		return Index{}, fmt.Errorf("commit index state update: %w", commitErr)
	}
	return result, nil
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
	result := database.Where(query, args...).Take(&record)
	return record, result.Error
}

func newIndexRecord(id string, definition IndexDefinition, now time.Time) indexRecord {
	return indexRecord{
		IndexID:                      id,
		Version:                      1,
		Name:                         definition.Name,
		DisplayName:                  definition.DisplayName,
		Description:                  definition.Description,
		RetentionNanoseconds:         int64(definition.RetentionPeriod),
		IngestionEnabled:             boolInteger(definition.IngestionEnabled),
		SearchEnabled:                boolInteger(definition.SearchEnabled),
		DefaultSourcetype:            definition.DefaultSourcetype,
		MaxEventBytes:                int64(definition.Limits.MaxEventBytes), // #nosec G115 -- validation bounds this value.
		MaxFieldCount:                int64(definition.Limits.MaxFieldCount),
		MaxNestingDepth:              int64(definition.Limits.MaxNestingDepth),
		MaximumFutureSkewNanoseconds: int64(definition.Limits.MaximumFutureSkew),
		MaximumEventAgeNanoseconds:   int64(definition.Limits.MaximumEventAge),
		State:                        IndexStateActive,
		CreatedAtUnixMicro:           now.UnixMicro(),
		UpdatedAtUnixMicro:           now.UnixMicro(),
	}
}

func indexDefinitionUpdates(definition IndexDefinition, now time.Time) map[string]any {
	return map[string]any{
		"default_sourcetype":              definition.DefaultSourcetype,
		"description":                     definition.Description,
		"display_name":                    definition.DisplayName,
		"ingestion_enabled":               boolInteger(definition.IngestionEnabled),
		"max_event_bytes":                 int64(definition.Limits.MaxEventBytes), // #nosec G115 -- validation bounds this value.
		"max_field_count":                 int64(definition.Limits.MaxFieldCount),
		"max_nesting_depth":               int64(definition.Limits.MaxNestingDepth),
		"maximum_event_age_nanoseconds":   int64(definition.Limits.MaximumEventAge),
		"maximum_future_skew_nanoseconds": int64(definition.Limits.MaximumFutureSkew),
		"retention_nanoseconds":           int64(definition.RetentionPeriod),
		"search_enabled":                  boolInteger(definition.SearchEnabled),
		"updated_at_unix_micro":           now.UnixMicro(),
		"version":                         gorm.Expr("version + 1"),
	}
}

func indexFromRecord(record indexRecord) (Index, error) {
	if record.Version < 1 || record.RetentionNanoseconds < 0 || record.RetentionNanoseconds%int64(time.Millisecond) != 0 || record.MaxEventBytes < 0 || record.MaxFieldCount < 0 || record.MaxFieldCount > math.MaxUint32 || record.MaxNestingDepth < 0 || record.MaxNestingDepth > math.MaxUint32 || record.MaximumFutureSkewNanoseconds < 0 || record.MaximumEventAgeNanoseconds < 0 || (record.IngestionEnabled != 0 && record.IngestionEnabled != 1) || (record.SearchEnabled != 0 && record.SearchEnabled != 1) || !validIndexState(record.State) {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	return Index{
		ID:      record.IndexID,
		Version: uint64(record.Version),
		Definition: IndexDefinition{
			Name:              record.Name,
			DisplayName:       record.DisplayName,
			Description:       record.Description,
			RetentionPeriod:   time.Duration(record.RetentionNanoseconds),
			IngestionEnabled:  record.IngestionEnabled == 1,
			SearchEnabled:     record.SearchEnabled == 1,
			DefaultSourcetype: record.DefaultSourcetype,
			Limits: IndexLimits{
				MaxEventBytes:     uint64(record.MaxEventBytes),
				MaxFieldCount:     uint32(record.MaxFieldCount),
				MaxNestingDepth:   uint32(record.MaxNestingDepth),
				MaximumFutureSkew: time.Duration(record.MaximumFutureSkewNanoseconds),
				MaximumEventAge:   time.Duration(record.MaximumEventAgeNanoseconds),
			},
		},
		State:     record.State,
		CreatedAt: time.UnixMicro(record.CreatedAtUnixMicro).UTC(),
		UpdatedAt: time.UnixMicro(record.UpdatedAtUnixMicro).UTC(),
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

func validateIndexDefinition(definition IndexDefinition) (IndexDefinition, error) {
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
	if definition.RetentionPeriod < 0 || definition.Limits.MaximumFutureSkew < 0 || definition.Limits.MaximumEventAge < 0 {
		return IndexDefinition{}, fmt.Errorf("%w: index durations cannot be negative", ErrInvalidArgument)
	}
	if definition.RetentionPeriod%time.Millisecond != 0 {
		return IndexDefinition{}, fmt.Errorf("%w: index retention must use whole milliseconds", ErrInvalidArgument)
	}
	if definition.Limits.MaxEventBytes > math.MaxInt64 {
		return IndexDefinition{}, fmt.Errorf("%w: max event bytes exceeds SQLite integer range", ErrInvalidArgument)
	}
	return definition, nil
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
