package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"gorm.io/gorm"
)

const (
	defaultIndexListPageSize  = 50
	maximumIndexListTextBytes = 255

	// MaximumIndexListPageSize bounds one control-plane metadata page.
	MaximumIndexListPageSize = 64
	// MaximumPhysicalIndexRecords bounds all index identities, including
	// tombstoned identities whose names remain reserved.
	MaximumPhysicalIndexRecords = 1024
)

// IndexSortBy selects the stable primary key for a metadata-only index page.
// Native statistics sorts are intentionally outside this control-plane API.
type IndexSortBy string

const (
	IndexSortByName      IndexSortBy = "name"
	IndexSortByCreatedAt IndexSortBy = "created_at"
	IndexSortByUpdatedAt IndexSortBy = "updated_at"
)

// IndexSortDirection controls both components of the composite keyset.
type IndexSortDirection string

const (
	IndexSortAscending  IndexSortDirection = "ascending"
	IndexSortDescending IndexSortDirection = "descending"
)

// IndexListCursor is a trusted, decoded continuation boundary. The browser
// server authenticates this value before it crosses the control-plane
// interface; DB still validates its shape and catalog revision independently.
type IndexListCursor struct {
	CatalogRevision uint64
	StringKey       string
	TimeKey         *int64
	IndexID         string
}

// IndexListRequest defines one bounded, revision-stable metadata page.
type IndexListRequest struct {
	PageSize     uint32
	IncludeTotal bool
	StateFilters []IndexState
	TextFilter   *string
	SortBy       IndexSortBy
	Direction    IndexSortDirection
	Cursor       *IndexListCursor
}

// IndexListResult contains detached rows and the next trusted keyset
// boundary. TotalSize is non-nil only when explicitly requested.
type IndexListResult struct {
	Indexes         []Index
	NextCursor      *IndexListCursor
	TotalSize       *uint64
	TotalSizeExact  bool
	CatalogRevision uint64
}

type normalizedIndexListRequest struct {
	pageSize     uint32
	includeTotal bool
	stateFilters []IndexState
	textFilter   *string
	sortBy       IndexSortBy
	direction    IndexSortDirection
	cursor       *IndexListCursor
}

type indexCatalogIntegrity struct {
	SingletonID           int64 `gorm:"column:singleton_id"`
	Revision              int64 `gorm:"column:revision"`
	PhysicalCount         int64 `gorm:"column:physical_count"`
	ObservedPhysicalCount int64 `gorm:"column:observed_physical_count"`
}

// ListIndexPage returns at most one metadata page. The revision, rows, and
// optional exact total are read in the same SQLite transaction. Any catalog
// mutation before a continuation invalidates it rather than risking
// duplicates or omissions from mutable sort keys and tombstone visibility.
func (db *DB) ListIndexPage(
	ctx context.Context,
	request IndexListRequest,
) (result IndexListResult, returnedErr error) {
	if ctx == nil {
		return IndexListResult{}, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return IndexListResult{}, err
	}
	normalized, err := normalizeIndexListRequest(request)
	if err != nil {
		return IndexListResult{}, err
	}

	tx := db.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return IndexListResult{}, fmt.Errorf("begin index list: %w", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	state, err := readIndexCatalogState(tx)
	if err != nil {
		return IndexListResult{}, fmt.Errorf("read index catalog state: %w", err)
	}
	// #nosec G115 -- readIndexCatalogState requires a positive int64 revision.
	catalogRevision := uint64(state.Revision)
	if normalized.cursor != nil &&
		normalized.cursor.CatalogRevision != catalogRevision {
		return IndexListResult{}, ErrPageInvalidated
	}

	pageQuery := applyIndexListCursor(
		applyIndexListFilters(
			visibleIndexRecords(tx.Model(&indexRecord{})),
			normalized,
		),
		normalized,
	)
	pageQuery = applyIndexListOrder(pageQuery, normalized)
	var records []indexRecord
	if err := pageQuery.
		Limit(int(normalized.pageSize) + 1).
		Find(&records).Error; err != nil {
		return IndexListResult{}, fmt.Errorf("list index page: %w", err)
	}
	hasMore := len(records) > int(normalized.pageSize)
	if hasMore {
		records = records[:normalized.pageSize]
	}

	result = IndexListResult{
		Indexes:         make([]Index, 0, len(records)),
		CatalogRevision: catalogRevision,
	}
	for _, record := range records {
		index, conversionErr := indexFromRecord(record)
		if conversionErr != nil {
			return IndexListResult{}, fmt.Errorf(
				"scan listed index: %w",
				conversionErr,
			)
		}
		result.Indexes = append(result.Indexes, index)
	}
	if hasMore {
		last := records[len(records)-1]
		cursor := &IndexListCursor{
			CatalogRevision: catalogRevision,
			IndexID:         last.IndexID,
		}
		switch normalized.sortBy {
		case IndexSortByName:
			cursor.StringKey = last.Name
		case IndexSortByCreatedAt:
			value := last.CreatedAtUnixMicro
			cursor.TimeKey = &value
		case IndexSortByUpdatedAt:
			value := last.UpdatedAtUnixMicro
			cursor.TimeKey = &value
		}
		result.NextCursor = cursor
	}
	if normalized.includeTotal {
		var count int64
		if err := applyIndexListFilters(
			visibleIndexRecords(tx.Model(&indexRecord{})),
			normalized,
		).Count(&count).Error; err != nil {
			return IndexListResult{}, fmt.Errorf(
				"count listed indexes: %w",
				err,
			)
		}
		if count < 0 || count > MaximumPhysicalIndexRecords {
			return IndexListResult{}, errors.New(
				"count listed indexes: invalid result",
			)
		}
		total := uint64(count)
		result.TotalSize = &total
		result.TotalSizeExact = true
	}
	if err := tx.Commit().Error; err != nil {
		return IndexListResult{}, fmt.Errorf("commit index list: %w", err)
	}
	return result, nil
}

func normalizeIndexListRequest(
	request IndexListRequest,
) (normalizedIndexListRequest, error) {
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultIndexListPageSize
	}
	if pageSize > MaximumIndexListPageSize {
		return normalizedIndexListRequest{}, fmt.Errorf(
			"%w: index page size exceeds the supported maximum",
			ErrInvalidArgument,
		)
	}

	if len(request.StateFilters) > 3 {
		return normalizedIndexListRequest{}, fmt.Errorf(
			"%w: too many index-state filters",
			ErrInvalidArgument,
		)
	}
	states := slices.Clone(request.StateFilters)
	for _, state := range states {
		if !validIndexState(state) {
			return normalizedIndexListRequest{}, fmt.Errorf(
				"%w: index-state filter is invalid",
				ErrInvalidArgument,
			)
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)

	text := cloneString(request.TextFilter)
	if text != nil {
		normalizedText := strings.TrimSpace(*text)
		if len(normalizedText) > maximumIndexListTextBytes ||
			!utf8.ValidString(normalizedText) ||
			strings.IndexByte(normalizedText, 0) >= 0 {
			return normalizedIndexListRequest{}, fmt.Errorf(
				"%w: index text filter is invalid",
				ErrInvalidArgument,
			)
		}
		for _, character := range normalizedText {
			if unicode.IsControl(character) {
				return normalizedIndexListRequest{}, fmt.Errorf(
					"%w: index text filter is invalid",
					ErrInvalidArgument,
				)
			}
		}
		if normalizedText == "" {
			text = nil
		} else {
			text = &normalizedText
		}
	}

	sortBy := request.SortBy
	if sortBy == "" {
		sortBy = IndexSortByName
	}
	switch sortBy {
	case IndexSortByName, IndexSortByCreatedAt, IndexSortByUpdatedAt:
	default:
		return normalizedIndexListRequest{}, fmt.Errorf(
			"%w: index sort field is invalid",
			ErrInvalidArgument,
		)
	}
	direction := request.Direction
	if direction == "" {
		direction = IndexSortAscending
	}
	if direction != IndexSortAscending &&
		direction != IndexSortDescending {
		return normalizedIndexListRequest{}, fmt.Errorf(
			"%w: index sort direction is invalid",
			ErrInvalidArgument,
		)
	}
	cursor := cloneIndexListCursor(request.Cursor)
	if err := ValidateIndexListCursor(cursor, sortBy); err != nil {
		return normalizedIndexListRequest{}, err
	}
	return normalizedIndexListRequest{
		pageSize:     pageSize,
		includeTotal: request.IncludeTotal,
		stateFilters: states,
		textFilter:   text,
		sortBy:       sortBy,
		direction:    direction,
		cursor:       cursor,
	}, nil
}

// ValidateIndexListCursor validates the complete trusted cursor shape shared
// by the browser boundary and the GORM control-plane implementation.
func ValidateIndexListCursor(
	cursor *IndexListCursor,
	sortBy IndexSortBy,
) error {
	invalid := func() error {
		return fmt.Errorf("%w: index page cursor is invalid", ErrInvalidArgument)
	}
	if cursor == nil {
		return nil
	}
	if cursor.CatalogRevision == 0 ||
		cursor.CatalogRevision > math.MaxInt64 ||
		!validIndexIdentifier(cursor.IndexID) {
		return invalid()
	}
	switch sortBy {
	case IndexSortByName:
		normalized, err := NormalizeIndexName(cursor.StringKey)
		if err != nil || normalized != cursor.StringKey ||
			cursor.TimeKey != nil {
			return invalid()
		}
	case IndexSortByCreatedAt, IndexSortByUpdatedAt:
		if cursor.StringKey != "" || cursor.TimeKey == nil ||
			*cursor.TimeKey <= 0 ||
			*cursor.TimeKey > maximumControlTimestampUnixMicro {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func applyIndexListFilters(
	query *gorm.DB,
	request normalizedIndexListRequest,
) *gorm.DB {
	if len(request.stateFilters) != 0 {
		query = query.Where("state IN ?", request.stateFilters)
	}
	if request.textFilter != nil {
		query = query.Where(
			`(
				instr(lower(name), lower(?)) > 0
				OR instr(lower(display_name), lower(?)) > 0
				OR instr(lower(description), lower(?)) > 0
			)`,
			*request.textFilter,
			*request.textFilter,
			*request.textFilter,
		)
	}
	return query
}

// applyKeysetCursor emits the two-column keyset predicate shared by every list
// endpoint: strictly past the sort key, or tied on it and past the tiebreak.
func applyKeysetCursor(
	query *gorm.DB,
	column, idColumn, comparison string,
	value any,
	id string,
) *gorm.DB {
	return query.Where(
		"("+column+" "+comparison+" ? OR ("+column+" = ? AND "+
			idColumn+" "+comparison+" ?))",
		value, value, id,
	)
}

func applyIndexListCursor(
	query *gorm.DB,
	request normalizedIndexListRequest,
) *gorm.DB {
	if request.cursor == nil {
		return query
	}
	comparison := ">"
	if request.direction == IndexSortDescending {
		comparison = "<"
	}
	switch request.sortBy {
	case IndexSortByName:
		return applyKeysetCursor(
			query, "name", "index_id", comparison,
			request.cursor.StringKey, request.cursor.IndexID,
		)
	case IndexSortByCreatedAt:
		return applyKeysetCursor(
			query, "created_at_unix_micro", "index_id", comparison,
			*request.cursor.TimeKey, request.cursor.IndexID,
		)
	case IndexSortByUpdatedAt:
		return applyKeysetCursor(
			query, "updated_at_unix_micro", "index_id", comparison,
			*request.cursor.TimeKey, request.cursor.IndexID,
		)
	default:
		return query
	}
}

func applyIndexListOrder(
	query *gorm.DB,
	request normalizedIndexListRequest,
) *gorm.DB {
	direction := "ASC"
	if request.direction == IndexSortDescending {
		direction = "DESC"
	}
	column := "name"
	switch request.sortBy {
	case IndexSortByCreatedAt:
		column = "created_at_unix_micro"
	case IndexSortByUpdatedAt:
		column = "updated_at_unix_micro"
	}
	return query.
		Order(column + " " + direction).
		Order("index_id " + direction)
}

func readIndexCatalogIntegrity(
	database *gorm.DB,
) (indexCatalogIntegrity, error) {
	var integrity indexCatalogIntegrity
	err := database.
		Model(&indexCatalogStateRecord{}).
		Select(`
			index_catalog_state.singleton_id,
			index_catalog_state.revision,
			index_catalog_state.physical_count,
			(
				SELECT COUNT(*)
				FROM (
					SELECT 1
					FROM indexes
					LIMIT ?
				)
			) AS observed_physical_count`,
			MaximumPhysicalIndexRecords+1,
		).
		Where("index_catalog_state.singleton_id = ?", 1).
		Take(&integrity).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return indexCatalogIntegrity{}, errors.New(
				"invalid index catalog state in control-plane database",
			)
		}
		return indexCatalogIntegrity{}, err
	}
	if integrity.SingletonID != 1 ||
		integrity.Revision < 1 ||
		integrity.PhysicalCount < 0 ||
		integrity.PhysicalCount > MaximumPhysicalIndexRecords ||
		integrity.ObservedPhysicalCount != integrity.PhysicalCount {
		return indexCatalogIntegrity{}, errors.New(
			"invalid index catalog state in control-plane database",
		)
	}
	return integrity, nil
}

func readIndexCatalogState(
	database *gorm.DB,
) (indexCatalogIntegrity, error) {
	var state indexCatalogIntegrity
	err := database.
		Model(&indexCatalogStateRecord{}).
		Select("singleton_id", "revision", "physical_count").
		Where("singleton_id = ?", 1).
		Take(&state).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return indexCatalogIntegrity{}, errors.New(
				"invalid index catalog state in control-plane database",
			)
		}
		return indexCatalogIntegrity{}, err
	}
	if state.SingletonID != 1 ||
		state.Revision < 1 ||
		state.PhysicalCount < 0 ||
		state.PhysicalCount > MaximumPhysicalIndexRecords {
		return indexCatalogIntegrity{}, errors.New(
			"invalid index catalog state in control-plane database",
		)
	}
	return state, nil
}

func cloneIndexListCursor(input *IndexListCursor) *IndexListCursor {
	if input == nil {
		return nil
	}
	result := *input
	if input.TimeKey != nil {
		value := *input.TimeKey
		result.TimeKey = &value
	}
	return &result
}
