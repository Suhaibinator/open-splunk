package lookupcatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
)

const listProjectionFrom = `
	FROM knowledge_lookup_definitions AS registry
	JOIN knowledge_lookup_definition_versions AS version
	  ON version.tenant_id = registry.tenant_id
	 AND version.lookup_id = registry.lookup_id
	 AND version.definition_version = registry.current_version
	JOIN knowledge_lookup_asset_versions AS asset
	  ON asset.tenant_id = version.tenant_id
	 AND asset.lookup_asset_id = version.lookup_asset_id
	 AND asset.asset_version = version.asset_version
	 AND asset.content_sha256 = version.asset_content_sha256
	 AND asset.canonical_bytes = version.asset_size_bytes`

const projectionSelectColumns = `registry.tenant_id, registry.lookup_id, registry.owner_id,
		registry.app_id, registry.name, registry.sharing_scope,
		registry.automatic, registry.current_version, registry.state,
		registry.created_at_unix_micro, registry.updated_at_unix_micro,
		registry.disabled_at_unix_micro, registry.deleted_at_unix_micro,
		version.definition_version, version.lookup_asset_id,
		version.asset_version, version.asset_size_bytes,
		version.asset_content_sha256, version.definition_proto,
		version.columns_blob, version.mutation_kind, version.state,
		version.disabled_at_unix_micro, version.deleted_at_unix_micro,
		version.created_at_unix_micro, asset.source_sha256,
		asset.row_count, asset.column_count`

const listProjectionSelect = `
	SELECT ` + projectionSelectColumns + listProjectionFrom

type normalizedListPageRequest struct {
	ListPageRequest
	states []string
}

// ListPage returns one joined, filtered, keyset-bounded management page. The
// owner catalog commitment, projections, and optional total are read from one
// SQLite snapshot; no immutable CSV body is loaded.
func (catalog *Catalog) ListPage(
	ctx context.Context,
	request ListPageRequest,
) (page ListPage, returnedErr error) {
	if err := catalog.validateContext(ctx); err != nil {
		return ListPage{}, err
	}
	normalized, err := normalizeListPageRequest(request)
	if err != nil {
		return ListPage{}, err
	}
	tx, err := catalog.database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ListPage{}, fmt.Errorf("begin lookup list-page snapshot: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && returnedErr == nil {
			returnedErr = fmt.Errorf("roll back lookup list-page snapshot: %w", rollbackErr)
		}
	}()

	snapshot, err := readLookupListSnapshot(ctx, tx, normalized.TenantID, normalized.OwnerID)
	if err != nil {
		return ListPage{}, err
	}
	if normalized.ExpectedSnapshot != nil && *normalized.ExpectedSnapshot != snapshot {
		return ListPage{}, ErrPageInvalidated
	}

	query, arguments := lookupListPageSQL(normalized)
	rows, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return ListPage{}, fmt.Errorf("read lookup list page: %w", err)
	}
	records := make([]persistedProjection, 0, normalized.Limit+1)
	for rows.Next() {
		registry, version, scanErr := scanJoinedProjection(rows)
		if scanErr != nil {
			_ = rows.Close()
			return ListPage{}, fmt.Errorf("scan lookup list page: %w", scanErr)
		}
		record, projectionErr := projectPersisted(registry, version)
		if projectionErr != nil {
			_ = rows.Close()
			return ListPage{}, projectionErr
		}
		if record.lookup.GetOwnerId() != normalized.OwnerID {
			_ = rows.Close()
			return ListPage{}, ErrCorrupt
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return ListPage{}, fmt.Errorf("iterate lookup list page: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ListPage{}, fmt.Errorf("close lookup list page: %w", err)
	}
	if len(records) > int(normalized.Limit)+1 {
		return ListPage{}, ErrCorrupt
	}
	hasMore := len(records) > int(normalized.Limit)
	if hasMore {
		records = records[:normalized.Limit:normalized.Limit]
	}
	page = ListPage{
		Lookups:  make([]*opensplunk.Lookup, len(records)),
		Snapshot: snapshot,
	}
	for index, record := range records {
		page.Lookups[index] = record.lookup
	}
	if hasMore {
		page.NextPosition = listPosition(records[len(records)-1].lookup, normalized.SortBy)
		if page.NextPosition == nil {
			return ListPage{}, ErrCorrupt
		}
	}
	if normalized.IncludeTotal {
		total, countErr := countLookupListPage(ctx, tx, normalized)
		if countErr != nil {
			return ListPage{}, countErr
		}
		page.TotalSize = &total
	}
	finalSnapshot, err := readLookupListSnapshot(ctx, tx, normalized.TenantID, normalized.OwnerID)
	if err != nil {
		return ListPage{}, err
	}
	if finalSnapshot != snapshot {
		return ListPage{}, ErrCorrupt
	}
	if err := tx.Commit(); err != nil {
		return ListPage{}, fmt.Errorf("commit lookup list-page snapshot: %w", err)
	}
	return page, nil
}

func normalizeListPageRequest(request ListPageRequest) (normalizedListPageRequest, error) {
	if !validIdentity(request.TenantID, 255) || !validIdentity(request.OwnerID, 255) ||
		(request.AppID != "" && !validIdentity(request.AppID, 128)) ||
		request.Limit == 0 || request.Limit > MaximumManagedLookups+1 ||
		!utf8.ValidString(request.TextFilter) || len(request.TextFilter) > MaximumListTextBytes ||
		strings.IndexByte(request.TextFilter, 0) >= 0 ||
		request.TextFilter != strings.ToLower(request.TextFilter) {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page scope is invalid", ErrInvalid)
	}
	if request.SortBy != opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME &&
		request.SortBy != opensplunk.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT &&
		request.SortBy != opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page sort is invalid", ErrInvalid)
	}
	if request.SortDirection != opensplunk.SortDirection_SORT_DIRECTION_ASCENDING &&
		request.SortDirection != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page direction is invalid", ErrInvalid)
	}
	if len(request.States) > 3 {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page states are invalid", ErrInvalid)
	}
	states := slices.Clone(request.States)
	slices.Sort(states)
	states = slices.Compact(states)
	persistedStates := make([]string, len(states))
	for index, state := range states {
		switch state {
		case opensplunk.LookupState_LOOKUP_STATE_ACTIVE:
			persistedStates[index] = "ACTIVE"
		case opensplunk.LookupState_LOOKUP_STATE_DISABLED:
			persistedStates[index] = "DISABLED"
		case opensplunk.LookupState_LOOKUP_STATE_DELETED:
			persistedStates[index] = "DELETED"
		default:
			return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page state is invalid", ErrInvalid)
		}
	}
	if request.Position != nil && !validListPosition(*request.Position, request.SortBy) {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page position is invalid", ErrInvalid)
	}
	if request.ExpectedSnapshot != nil && request.ExpectedSnapshot.Revision == 0 {
		return normalizedListPageRequest{}, fmt.Errorf("%w: lookup list-page snapshot is invalid", ErrInvalid)
	}
	request.States = states
	if request.Position != nil {
		position := *request.Position
		position.LookupID = strings.Clone(position.LookupID)
		position.Name = strings.Clone(position.Name)
		request.Position = &position
	}
	if request.ExpectedSnapshot != nil {
		snapshot := *request.ExpectedSnapshot
		request.ExpectedSnapshot = &snapshot
	}
	request.TenantID = strings.Clone(request.TenantID)
	request.OwnerID = strings.Clone(request.OwnerID)
	request.AppID = strings.Clone(request.AppID)
	request.TextFilter = strings.Clone(request.TextFilter)
	return normalizedListPageRequest{ListPageRequest: request, states: persistedStates}, nil
}

func validListPosition(position ListPosition, sortBy opensplunk.LookupSortBy) bool {
	if !validIdentity(position.LookupID, 128) {
		return false
	}
	switch sortBy {
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME:
		return lookupdefinition.IsValidLookupName(position.Name) && position.UnixMicro == 0
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT,
		opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT:
		return position.Name == "" && position.UnixMicro >= 1 && position.UnixMicro <= maximumUnixMicro
	default:
		return false
	}
}

func lookupListPageSQL(request normalizedListPageRequest) (string, []any) {
	where, arguments := lookupListWhere(request, true)
	column := lookupListSortColumn(request.SortBy)
	direction := "ASC"
	if request.SortDirection == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		direction = "DESC"
	}
	query := listProjectionSelect + where + " ORDER BY " + column + " " + direction +
		", registry.lookup_id " + direction + " LIMIT ?"
	arguments = append(arguments, uint64(request.Limit)+1)
	return query, arguments
}

func lookupListWhere(request normalizedListPageRequest, includePosition bool) (string, []any) {
	clauses := []string{"registry.tenant_id = ?", "registry.owner_id = ?"}
	arguments := []any{request.TenantID, request.OwnerID}
	if request.AppID != "" {
		clauses = append(clauses, "registry.app_id = ?")
		arguments = append(arguments, request.AppID)
	}
	if len(request.states) != 0 {
		placeholders := make([]string, len(request.states))
		for index, state := range request.states {
			placeholders[index] = "?"
			arguments = append(arguments, state)
		}
		clauses = append(clauses, "registry.state IN ("+strings.Join(placeholders, ",")+")")
	}
	if request.TextFilter != "" {
		clauses = append(clauses, lookupDefinitionTextContainsFunction+"(version.definition_proto, ?) = 1")
		arguments = append(arguments, request.TextFilter)
	}
	if includePosition && request.Position != nil {
		column := lookupListSortColumn(request.SortBy)
		operator := ">"
		if request.SortDirection == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
			operator = "<"
		}
		var primary any = request.Position.Name
		if request.SortBy != opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME {
			primary = request.Position.UnixMicro
		}
		clauses = append(clauses, "("+column+" "+operator+" ? OR ("+column+" = ? AND registry.lookup_id "+operator+" ?))")
		arguments = append(arguments, primary, primary, request.Position.LookupID)
	}
	return " WHERE " + strings.Join(clauses, " AND "), arguments
}

func lookupListSortColumn(sortBy opensplunk.LookupSortBy) string {
	switch sortBy {
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT:
		return "registry.created_at_unix_micro"
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT:
		return "registry.updated_at_unix_micro"
	default:
		return "registry.name"
	}
}

func listPosition(
	lookup *opensplunk.Lookup,
	sortBy opensplunk.LookupSortBy,
) *ListPosition {
	if lookup == nil || lookup.GetDefinition() == nil {
		return nil
	}
	position := &ListPosition{LookupID: strings.Clone(lookup.GetLookupId())}
	switch sortBy {
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME:
		position.Name = strings.Clone(lookup.GetDefinition().GetName())
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT:
		position.UnixMicro = lookup.GetCreatedAt().AsTime().UnixMicro()
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT:
		position.UnixMicro = lookup.GetUpdatedAt().AsTime().UnixMicro()
	default:
		return nil
	}
	return position
}

func countLookupListPage(
	ctx context.Context,
	tx *sql.Tx,
	request normalizedListPageRequest,
) (uint64, error) {
	where, arguments := lookupListWhere(request, false)
	var count int64
	if err := tx.QueryRowContext(ctx, "SELECT count(*)"+listProjectionFrom+where, arguments...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count lookup list page: %w", err)
	}
	if count < 0 || count > MaximumManagedLookups {
		return 0, ErrCorrupt
	}
	return uint64(count), nil
}

func readLookupListSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	ownerID string,
) (ListSnapshot, error) {
	var count, revision int64
	var state []byte
	err := tx.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(sum(current_version), 0),
			`+lookupCatalogStateFunction+`(lookup_id, current_version)
		FROM knowledge_lookup_definitions
		WHERE tenant_id = ? AND owner_id = ?`, tenantID, ownerID).Scan(&count, &revision, &state)
	if err != nil {
		return ListSnapshot{}, fmt.Errorf("read lookup list snapshot: %w", err)
	}
	if count < 0 || count > MaximumManagedLookups || revision < count || revision > MaximumVersions {
		return ListSnapshot{}, ErrCorrupt
	}
	// The query result is validated as nonnegative and capped above.

	snapshot := ListSnapshot{Revision: safecast.MustConv[uint64](revision)}
	if count == 0 && len(state) == 0 {
		snapshot.State = emptyLookupCatalogState()
	} else if len(state) != len(snapshot.State) {
		return ListSnapshot{}, ErrCorrupt
	} else {
		copy(snapshot.State[:], state)
	}
	return snapshot, nil
}
