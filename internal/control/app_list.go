package control

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"
	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	defaultAppListPageSize  = 50
	maximumAppListPageSize  = 256
	maximumAppListTextBytes = 255
	maximumAppCursorBytes   = 2 << 10
	appListCursorVersion    = 1
)

// AppSortBy is the stable primary key used by ListApps.
type AppSortBy string

const (
	AppSortByDisplayName AppSortBy = "display_name"
	AppSortByCreatedAt   AppSortBy = "created_at"
	AppSortByUpdatedAt   AppSortBy = "updated_at"
)

// AppSortDirection controls composite keyset ordering.
type AppSortDirection string

const (
	AppSortAscending  AppSortDirection = "ascending"
	AppSortDescending AppSortDirection = "descending"
)

// AppListRequest defines one tenant-scoped, revision-stable keyset page.
type AppListRequest struct {
	PageSize                uint32
	PageToken               string
	RequiredCatalogRevision *uint64
	IncludeTotal            bool
	StateFilters            []AppState
	TextFilter              *string
	SortBy                  AppSortBy
	Direction               AppSortDirection
}

// AppListResult contains detached rows and an authenticated continuation
// token. TotalSize is non-nil only when requested.
type AppListResult struct {
	Apps            []AppWorkspace
	NextPageToken   *string
	TotalSize       *uint64
	TotalSizeExact  bool
	CatalogRevision uint64
}

// AppIdentityListResult is one bounded identity snapshot. Complete is false
// when at least one additional tenant app exists beyond the requested bound.
type AppIdentityListResult struct {
	AppIDs   []string
	Complete bool
}

type appIdentityRecord struct {
	AppID string   `gorm:"column:app_id"`
	State AppState `gorm:"column:state"`
}

const listAppIdentitiesSQL = `
SELECT
	substr(app_id, 1, ?) AS app_id,
	substr(state, 1, ?) AS state
FROM app_workspaces INDEXED BY app_workspaces_tenant_display_id_idx
WHERE tenant_id = ?
LIMIT ?`

type normalizedAppListRequest struct {
	tenantID                string
	pageSize                uint32
	pageToken               string
	requiredCatalogRevision *int64
	includeTotal            bool
	stateFilters            []AppState
	textFilter              *string
	sortBy                  AppSortBy
	direction               AppSortDirection
}

type appListCursor struct {
	Version    int    `json:"v"`
	FilterHash string `json:"f"`
	Revision   int64  `json:"r"`
	StringKey  string `json:"s,omitempty"`
	IntegerKey *int64 `json:"n,omitempty"`
	AppID      string `json:"i"`
}

type appListFingerprint struct {
	Version   int      `json:"v"`
	TenantID  string   `json:"t"`
	PageSize  uint32   `json:"p"`
	Total     bool     `json:"z"`
	States    []string `json:"s"`
	Text      *string  `json:"x"`
	SortBy    string   `json:"b"`
	Direction string   `json:"d"`
}

// ListAppIdentities reads only stable app identity and lifecycle authority.
// One statement supplies the complete snapshot decision, so no cursor,
// catalog-revision read, definition conversion, or default-index hydration is
// required.
func (catalog *AppCatalog) ListAppIdentities(
	ctx context.Context,
	scope AppAccessScope,
	maximum uint32,
) (AppIdentityListResult, error) {
	if err := validateAppContext(ctx); err != nil {
		return AppIdentityListResult{}, err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return AppIdentityListResult{}, err
	}
	if maximum == 0 || maximum > MaximumAppsPerTenant {
		return AppIdentityListResult{}, fmt.Errorf(
			"%w: app identity list bound is invalid",
			ErrInvalidArgument,
		)
	}

	records := make([]appIdentityRecord, 0, int(maximum)+1)
	query := catalog.orm.WithContext(ctx).Raw(
		listAppIdentitiesSQL,
		canonicalAppIDBytes+1,
		len(AppStateArchived)+1,
		tenantID,
		int64(maximum)+1,
	).Scan(&records)
	if query.Error != nil {
		return AppIdentityListResult{}, appContextError(
			ctx,
			"list app identities",
			query.Error,
		)
	}
	if len(records) > int(maximum) {
		return AppIdentityListResult{Complete: false}, nil
	}
	appIDs := make([]string, len(records))
	for index, record := range records {
		if !ValidCanonicalAppID(record.AppID) || !validAppState(record.State) {
			return AppIdentityListResult{}, invalidAppRecordError()
		}
		appIDs[index] = record.AppID
	}
	slices.Sort(appIDs)
	for index := 1; index < len(appIDs); index++ {
		if appIDs[index-1] == appIDs[index] {
			return AppIdentityListResult{}, invalidAppRecordError()
		}
	}
	return AppIdentityListResult{AppIDs: appIDs, Complete: true}, nil
}

// ListApps returns one bounded page. Page-one rows, exact count, and catalog
// revision are read in one transaction. Any create/update/state/delete before
// continuation changes the tenant revision and returns ErrPageInvalidated
// rather than silently duplicating or omitting a row.
func (catalog *AppCatalog) ListApps(
	ctx context.Context,
	scope AppAccessScope,
	request AppListRequest,
) (result AppListResult, returnedErr error) {
	if err := validateAppContext(ctx); err != nil {
		return AppListResult{}, err
	}
	normalized, err := normalizeAppListRequest(scope, request)
	if err != nil {
		return AppListResult{}, err
	}
	filterHash, err := appListFilterHash(normalized)
	if err != nil {
		return AppListResult{}, err
	}
	cursor := appListCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeAppListCursor(
			catalog.cursorKey,
			normalized.pageToken,
			filterHash,
			normalized.sortBy,
		)
		if err != nil {
			return AppListResult{}, err
		}
		if normalized.requiredCatalogRevision != nil &&
			cursor.Revision != *normalized.requiredCatalogRevision {
			return AppListResult{}, fmt.Errorf(
				"%w: app page token revision does not match the request",
				ErrInvalidArgument,
			)
		}
	}

	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AppListResult{}, appContextError(ctx, "begin app list", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	revision, err := readAppCatalogRevision(tx, normalized.tenantID)
	if err != nil {
		return AppListResult{}, appContextError(ctx, "read app catalog revision", err)
	}
	if cursor.Revision != 0 && cursor.Revision != revision {
		return AppListResult{}, ErrPageInvalidated
	}
	if normalized.requiredCatalogRevision != nil &&
		revision != *normalized.requiredCatalogRevision {
		return AppListResult{}, ErrPageInvalidated
	}

	query := applyAppListFilters(
		tx.Model(&appRecord{}),
		normalized,
	)
	query = applyAppListCursor(query, normalized, cursor)
	query = applyAppListOrder(query, normalized)
	var records []appRecord
	if err := query.Limit(int(normalized.pageSize) + 1).Find(&records).Error; err != nil {
		return AppListResult{}, appContextError(ctx, "list apps", err)
	}
	hasMore := len(records) > int(normalized.pageSize)
	if hasMore {
		records = records[:normalized.pageSize]
	}
	namesByApp, err := loadAppDefaultIndexNames(
		tx,
		normalized.tenantID,
		records,
		false,
	)
	if err != nil {
		return AppListResult{}, fmt.Errorf("list apps: %w", err)
	}
	result = AppListResult{
		Apps: make([]AppWorkspace, 0, len(records)),

		CatalogRevision: safecast.MustConv[uint64](revision),
	}
	for _, record := range records {
		app, convertErr := appFromRecord(record, namesByApp[record.AppID])
		if convertErr != nil {
			return AppListResult{}, fmt.Errorf("scan listed app: %w", convertErr)
		}
		result.Apps = append(result.Apps, app)
	}
	if hasMore {
		last := records[len(records)-1]
		next := appListCursor{
			FilterHash: filterHash,
			Revision:   revision,
			AppID:      last.AppID,
		}
		switch normalized.sortBy {
		case AppSortByDisplayName:
			next.StringKey = last.DisplayName
		case AppSortByCreatedAt:
			value := last.CreatedAtUnixMicro
			next.IntegerKey = &value
		case AppSortByUpdatedAt:
			value := last.UpdatedAtUnixMicro
			next.IntegerKey = &value
		}
		token, encodeErr := encodeAppListCursor(catalog.cursorKey, next)
		if encodeErr != nil {
			return AppListResult{}, encodeErr
		}
		result.NextPageToken = cloneString(&token)
	}
	if normalized.includeTotal {
		var count int64
		if err := applyAppListFilters(
			tx.Model(&appRecord{}),
			normalized,
		).Count(&count).Error; err != nil {
			return AppListResult{}, appContextError(ctx, "count listed apps", err)
		}
		if count < 0 {
			return AppListResult{}, errors.New("count listed apps: invalid negative result")
		}
		total := uint64(count)
		result.TotalSize = &total
		result.TotalSizeExact = true
	}
	if err := tx.Commit().Error; err != nil {
		return AppListResult{}, appContextError(ctx, "commit app list", err)
	}
	return result, nil
}

func normalizeAppListRequest(
	scope AppAccessScope,
	request AppListRequest,
) (normalizedAppListRequest, error) {
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return normalizedAppListRequest{}, err
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultAppListPageSize
	}
	if pageSize > maximumAppListPageSize {
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: app page size exceeds the supported maximum",
			ErrInvalidArgument,
		)
	}
	if len(request.PageToken) > maximumAppCursorBytes ||
		strings.TrimSpace(request.PageToken) != request.PageToken {
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: app page token is invalid",
			ErrInvalidArgument,
		)
	}
	var requiredRevision *int64
	if request.RequiredCatalogRevision != nil &&
		(*request.RequiredCatalogRevision == 0 ||
			*request.RequiredCatalogRevision > math.MaxInt64) {
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: app catalog revision is invalid",
			ErrInvalidArgument,
		)
	}
	if request.RequiredCatalogRevision != nil {

		converted := safecast.MustConv[int64](*request.RequiredCatalogRevision)
		requiredRevision = &converted
	}
	if len(request.StateFilters) > 2 {
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: too many app-state filters",
			ErrInvalidArgument,
		)
	}
	states := slices.Clone(request.StateFilters)
	for _, state := range states {
		if !validAppState(state) {
			return normalizedAppListRequest{}, fmt.Errorf(
				"%w: app-state filter is invalid",
				ErrInvalidArgument,
			)
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)
	text := cloneString(request.TextFilter)
	if text != nil {
		if len(*text) > maximumAppListTextBytes || !utf8.ValidString(*text) {
			return normalizedAppListRequest{}, fmt.Errorf(
				"%w: app text filter is invalid",
				ErrInvalidArgument,
			)
		}
		normalizedText := strings.TrimSpace(*text)
		if len(normalizedText) > maximumAppListTextBytes ||
			strings.IndexByte(normalizedText, 0) >= 0 {
			return normalizedAppListRequest{}, fmt.Errorf(
				"%w: app text filter is invalid",
				ErrInvalidArgument,
			)
		}
		for _, character := range normalizedText {
			if unicode.IsControl(character) {
				return normalizedAppListRequest{}, fmt.Errorf(
					"%w: app text filter is invalid",
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
		sortBy = AppSortByDisplayName
	}
	switch sortBy {
	case AppSortByDisplayName, AppSortByCreatedAt, AppSortByUpdatedAt:
	default:
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: app sort field is invalid",
			ErrInvalidArgument,
		)
	}
	direction := request.Direction
	if direction == "" {
		direction = AppSortAscending
	}
	if direction != AppSortAscending && direction != AppSortDescending {
		return normalizedAppListRequest{}, fmt.Errorf(
			"%w: app sort direction is invalid",
			ErrInvalidArgument,
		)
	}
	return normalizedAppListRequest{
		tenantID:                tenantID,
		pageSize:                pageSize,
		pageToken:               request.PageToken,
		requiredCatalogRevision: requiredRevision,
		includeTotal:            request.IncludeTotal,
		stateFilters:            states,
		textFilter:              text,
		sortBy:                  sortBy,
		direction:               direction,
	}, nil
}

func applyAppListFilters(
	query *gorm.DB,
	request normalizedAppListRequest,
) *gorm.DB {
	query = query.Where("tenant_id = ?", request.tenantID)
	if len(request.stateFilters) != 0 {
		query = query.Where("state IN ?", request.stateFilters)
	}
	if request.textFilter != nil {
		query = query.Where(
			`(
				instr(lower(slug), lower(?)) > 0
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

func applyAppListCursor(
	query *gorm.DB,
	request normalizedAppListRequest,
	cursor appListCursor,
) *gorm.DB {
	if cursor.AppID == "" {
		return query
	}
	ascending := request.direction == AppSortAscending
	comparison := ">"
	if !ascending {
		comparison = "<"
	}
	switch request.sortBy {
	case AppSortByDisplayName:
		return applyKeysetCursor(
			query, "display_name", "app_id", comparison,
			cursor.StringKey, cursor.AppID,
		)
	case AppSortByCreatedAt:
		return applyKeysetCursor(
			query, "created_at_unix_micro", "app_id", comparison,
			*cursor.IntegerKey, cursor.AppID,
		)
	case AppSortByUpdatedAt:
		return applyKeysetCursor(
			query, "updated_at_unix_micro", "app_id", comparison,
			*cursor.IntegerKey, cursor.AppID,
		)
	default:
		return query
	}
}

func applyAppListOrder(
	query *gorm.DB,
	request normalizedAppListRequest,
) *gorm.DB {
	direction := "ASC"
	if request.direction == AppSortDescending {
		direction = "DESC"
	}
	column := "display_name"
	switch request.sortBy {
	case AppSortByCreatedAt:
		column = "created_at_unix_micro"
	case AppSortByUpdatedAt:
		column = "updated_at_unix_micro"
	}
	return query.Order(column + " " + direction).Order("app_id " + direction)
}

func readAppCatalogRevision(database *gorm.DB, tenantID string) (int64, error) {
	var record appCatalogRevisionRecord
	err := database.Where("tenant_id = ?", tenantID).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		var app appRecord
		appErr := database.Select("app_id").
			Where("tenant_id = ?", tenantID).
			Take(&app).Error
		if appErr == nil {
			return 0, errors.New("invalid app catalog revision in control-plane database")
		}
		if !errors.Is(appErr, gorm.ErrRecordNotFound) {
			return 0, appErr
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if record.Revision < 1 {
		return 0, errors.New("invalid app catalog revision in control-plane database")
	}
	return record.Revision, nil
}

func appListFilterHash(request normalizedAppListRequest) (string, error) {
	states := make([]string, len(request.stateFilters))
	for index, state := range request.stateFilters {
		states[index] = string(state)
	}
	payload, err := json.Marshal(appListFingerprint{
		Version:   appListCursorVersion,
		TenantID:  request.tenantID,
		PageSize:  request.pageSize,
		Total:     request.includeTotal,
		States:    states,
		Text:      request.textFilter,
		SortBy:    string(request.sortBy),
		Direction: string(request.direction),
	})
	if err != nil {
		return "", fmt.Errorf("encode app list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeAppListCursor(key []byte, cursor appListCursor) (string, error) {
	cursor.Version = appListCursorVersion
	token, err := cursorcodec.Encode(
		key,
		"app-list-cursor",
		appListCursorVersion,
		maximumAppCursorBytes,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("encode app list cursor: %w", err)
	}
	return token, nil
}

func decodeAppListCursor(
	key []byte,
	token string,
	filterHash string,
	sortBy AppSortBy,
) (appListCursor, error) {
	invalid := func() (appListCursor, error) {
		return appListCursor{}, fmt.Errorf(
			"%w: app page token is invalid or does not match the request",
			ErrInvalidArgument,
		)
	}
	var cursor appListCursor
	if err := cursorcodec.Decode(
		key,
		"app-list-cursor",
		appListCursorVersion,
		maximumAppCursorBytes,
		token,
		&cursor,
	); err != nil {
		return invalid()
	}
	if cursor.Version != appListCursorVersion ||
		cursor.FilterHash != filterHash ||
		cursor.Revision < 1 ||
		!ValidCanonicalAppID(cursor.AppID) {
		return invalid()
	}
	if sortBy == AppSortByDisplayName {
		if cursor.IntegerKey != nil ||
			validateCanonicalAppText(cursor.StringKey, maximumAppDisplayNameBytes, false) != nil {
			return invalid()
		}
	} else if cursor.IntegerKey == nil || cursor.StringKey != "" || *cursor.IntegerKey <= 0 {
		return invalid()
	}
	return cursor, nil
}
