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
		_, err = db.sql.ExecContext(ctx, `
			INSERT INTO indexes (
				index_id, version, name, display_name, description,
				retention_nanoseconds, ingestion_enabled, search_enabled,
				default_sourcetype, max_event_bytes, max_field_count,
				max_nesting_depth, maximum_future_skew_nanoseconds,
				maximum_event_age_nanoseconds, state,
				created_at_unix_micro, updated_at_unix_micro
			) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id,
			definition.Name,
			definition.DisplayName,
			definition.Description,
			int64(definition.RetentionPeriod),
			boolInteger(definition.IngestionEnabled),
			boolInteger(definition.SearchEnabled),
			definition.DefaultSourcetype,
			int64(definition.Limits.MaxEventBytes),
			int64(definition.Limits.MaxFieldCount),
			int64(definition.Limits.MaxNestingDepth),
			int64(definition.Limits.MaximumFutureSkew),
			int64(definition.Limits.MaximumEventAge),
			IndexStateActive,
			now.UnixMicro(),
			now.UnixMicro(),
		)
		if err == nil {
			return Index{
				ID: id, Version: 1, Definition: definition, State: IndexStateActive,
				CreatedAt: now, UpdatedAt: now,
			}, nil
		}

		var existingID string
		nameErr := db.sql.QueryRowContext(ctx, `SELECT index_id FROM indexes WHERE name = ?`, definition.Name).Scan(&existingID)
		if nameErr == nil {
			return Index{}, fmt.Errorf("%w: index name %q", ErrAlreadyExists, definition.Name)
		}
		if nameErr != nil && !errors.Is(nameErr, sql.ErrNoRows) {
			return Index{}, fmt.Errorf("check duplicate index name: %w", nameErr)
		}
		idErr := db.sql.QueryRowContext(ctx, `SELECT index_id FROM indexes WHERE index_id = ?`, id).Scan(&existingID)
		if errors.Is(idErr, sql.ErrNoRows) {
			return Index{}, fmt.Errorf("create index: %w", err)
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
	index, err := scanIndex(db.sql.QueryRowContext(ctx, indexSelect+` WHERE index_id = ?`, id))
	return index, indexLookupError(err)
}

// GetIndexByName gets an index by its normalized immutable name.
func (db *DB) GetIndexByName(ctx context.Context, name string) (Index, error) {
	normalized, err := NormalizeIndexName(name)
	if err != nil {
		return Index{}, err
	}
	index, err := scanIndex(db.sql.QueryRowContext(ctx, indexSelect+` WHERE name = ?`, normalized))
	return index, indexLookupError(err)
}

// ListIndexes lists every index in normalized-name order.
func (db *DB) ListIndexes(ctx context.Context) ([]Index, error) {
	rows, err := db.sql.QueryContext(ctx, indexSelect+` ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	defer rows.Close()

	indexes := make([]Index, 0)
	for rows.Next() {
		index, err := scanIndex(rows)
		if err != nil {
			return nil, fmt.Errorf("scan listed index: %w", err)
		}
		indexes = append(indexes, index)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexes: %w", err)
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
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return Index{}, fmt.Errorf("begin index update: %w", err)
	}
	defer finishTx(tx, &err)

	current, err := scanIndex(tx.QueryRowContext(ctx, indexSelect+` WHERE index_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Index{}, ErrNotFound
	}
	if err != nil {
		return Index{}, fmt.Errorf("read index for update: %w", err)
	}
	if current.Version != expectedVersion {
		return Index{}, ErrVersionConflict
	}
	if current.Definition.Name != definition.Name {
		return Index{}, ErrImmutableName
	}

	now := databaseTime(time.Now())
	updateResult, err := tx.ExecContext(ctx, `
		UPDATE indexes SET
			version = version + 1,
			display_name = ?, description = ?, retention_nanoseconds = ?,
			ingestion_enabled = ?, search_enabled = ?, default_sourcetype = ?,
			max_event_bytes = ?, max_field_count = ?, max_nesting_depth = ?,
			maximum_future_skew_nanoseconds = ?, maximum_event_age_nanoseconds = ?,
			updated_at_unix_micro = ?
		WHERE index_id = ? AND version = ?`,
		definition.DisplayName,
		definition.Description,
		int64(definition.RetentionPeriod),
		boolInteger(definition.IngestionEnabled),
		boolInteger(definition.SearchEnabled),
		definition.DefaultSourcetype,
		int64(definition.Limits.MaxEventBytes),
		int64(definition.Limits.MaxFieldCount),
		int64(definition.Limits.MaxNestingDepth),
		int64(definition.Limits.MaximumFutureSkew),
		int64(definition.Limits.MaximumEventAge),
		now.UnixMicro(), id, int64(expectedVersion),
	)
	if err != nil {
		return Index{}, fmt.Errorf("update index: %w", err)
	}
	if err := requireOneUpdated(updateResult); err != nil {
		return Index{}, err
	}
	result, err = scanIndex(tx.QueryRowContext(ctx, indexSelect+` WHERE index_id = ?`, id))
	if err != nil {
		return Index{}, fmt.Errorf("read updated index: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Index{}, fmt.Errorf("commit index update: %w", err)
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
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return Index{}, fmt.Errorf("begin index state update: %w", err)
	}
	defer finishTx(tx, &err)

	now := databaseTime(time.Now())
	updateResult, err := tx.ExecContext(ctx, `
		UPDATE indexes
		SET state = ?, version = version + 1, updated_at_unix_micro = ?
		WHERE index_id = ? AND version = ?`, state, now.UnixMicro(), id, int64(expectedVersion))
	if err != nil {
		return Index{}, fmt.Errorf("set index state: %w", err)
	}
	if err := requireOneUpdated(updateResult); err != nil {
		if errors.Is(err, ErrVersionConflict) {
			var exists int
			lookupErr := tx.QueryRowContext(ctx, `SELECT 1 FROM indexes WHERE index_id = ?`, id).Scan(&exists)
			if errors.Is(lookupErr, sql.ErrNoRows) {
				return Index{}, ErrNotFound
			}
			if lookupErr != nil {
				return Index{}, fmt.Errorf("check index after state conflict: %w", lookupErr)
			}
		}
		return Index{}, err
	}
	result, err = scanIndex(tx.QueryRowContext(ctx, indexSelect+` WHERE index_id = ?`, id))
	if err != nil {
		return Index{}, fmt.Errorf("read index after state update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Index{}, fmt.Errorf("commit index state update: %w", err)
	}
	return result, nil
}

const indexSelect = `
	SELECT
		index_id, version, name, display_name, description,
		retention_nanoseconds, ingestion_enabled, search_enabled,
		default_sourcetype, max_event_bytes, max_field_count,
		max_nesting_depth, maximum_future_skew_nanoseconds,
		maximum_event_age_nanoseconds, state,
		created_at_unix_micro, updated_at_unix_micro
	FROM indexes`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanIndex(row rowScanner) (Index, error) {
	var (
		index                                  Index
		version                                int64
		retention, maxEventBytes               int64
		maxFieldCount, maxNestingDepth         int64
		maximumFutureSkew, maximumEventAge     int64
		ingestionEnabled, searchEnabled        int64
		createdAtUnixMicro, updatedAtUnixMicro int64
	)
	err := row.Scan(
		&index.ID, &version, &index.Definition.Name,
		&index.Definition.DisplayName, &index.Definition.Description,
		&retention, &ingestionEnabled, &searchEnabled,
		&index.Definition.DefaultSourcetype, &maxEventBytes,
		&maxFieldCount, &maxNestingDepth, &maximumFutureSkew,
		&maximumEventAge, &index.State, &createdAtUnixMicro, &updatedAtUnixMicro,
	)
	if err != nil {
		return Index{}, err
	}
	if version < 1 || retention < 0 || maxEventBytes < 0 || maxFieldCount < 0 || maxFieldCount > math.MaxUint32 || maxNestingDepth < 0 || maxNestingDepth > math.MaxUint32 || maximumFutureSkew < 0 || maximumEventAge < 0 || (ingestionEnabled != 0 && ingestionEnabled != 1) || (searchEnabled != 0 && searchEnabled != 1) || !validIndexState(index.State) {
		return Index{}, errors.New("invalid index record in control-plane database")
	}
	index.Version = uint64(version)
	index.Definition.RetentionPeriod = time.Duration(retention)
	index.Definition.IngestionEnabled = ingestionEnabled == 1
	index.Definition.SearchEnabled = searchEnabled == 1
	index.Definition.Limits = IndexLimits{
		MaxEventBytes:     uint64(maxEventBytes),
		MaxFieldCount:     uint32(maxFieldCount),
		MaxNestingDepth:   uint32(maxNestingDepth),
		MaximumFutureSkew: time.Duration(maximumFutureSkew),
		MaximumEventAge:   time.Duration(maximumEventAge),
	}
	index.CreatedAt = time.UnixMicro(createdAtUnixMicro).UTC()
	index.UpdatedAt = time.UnixMicro(updatedAtUnixMicro).UTC()
	return index, nil
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

func indexLookupError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, sql.ErrNoRows):
		return ErrNotFound
	default:
		return fmt.Errorf("get index: %w", err)
	}
}

func requireOneUpdated(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read updated row count: %w", err)
	}
	if count != 1 {
		return ErrVersionConflict
	}
	return nil
}

func finishTx(tx *sql.Tx, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
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
