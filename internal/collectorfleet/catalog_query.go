package collectorfleet

import (
	"strings"

	"gorm.io/gorm"
)

// catalogPageRecord is the bounded parent-row projection used to construct a
// stable keyset before the selected collector IDs are batch hydrated.
type catalogPageRecord struct {
	CollectorID         string  `gorm:"column:collector_id"`
	DisplayName         *string `gorm:"column:display_name"`
	Hostname            string  `gorm:"column:hostname"`
	LastSeenAtUnixMicro int64   `gorm:"column:last_seen_at_unix_micro"`
	QueuedBytes         int64   `gorm:"column:queued_bytes"`
}

// collectorCatalogFilteredQuery is the single source of truth for page and
// exact-count filters. request and view have already been normalized and
// detached from caller-owned state.
func collectorCatalogFilteredQuery(
	database *gorm.DB,
	request normalizedListRequest,
	view catalogLivenessView,
) *gorm.DB {
	query := database.
		Table("collector_fleet AS fleet").
		Joins(`
			INNER JOIN collector_runtime AS runtime
				ON runtime.tenant_id = fleet.tenant_id
				AND runtime.collector_id = fleet.collector_id
		`).
		Where(
			"fleet.tenant_id = ? AND runtime.tenant_id = ?",
			request.tenantID,
			request.tenantID,
		)

	if request.indexNameFilter != nil {
		query = query.Where(`
			EXISTS (
				SELECT 1
				FROM collector_authorized_indexes AS authorized
				WHERE authorized.tenant_id = fleet.tenant_id
					AND authorized.index_name = ?
					AND authorized.collector_id = fleet.collector_id
			)
		`, *request.indexNameFilter)
	}
	if request.textFilter != nil {
		query = query.Where(`
			(
				instr(lower(fleet.collector_id), lower(?)) > 0
				OR instr(lower(fleet.display_name), lower(?)) > 0
				OR instr(lower(runtime.hostname), lower(?)) > 0
			)
		`,
			*request.textFilter,
			*request.textFilter,
			*request.textFilter,
		)
	}
	if predicate, arguments := collectorCatalogStatePredicate(
		request.stateFilters,
		view.entries,
	); predicate != "" {
		query = query.Where(predicate, arguments...)
	}
	return query
}

// collectorCatalogPageQuery selects only the indexed keyset and hydration
// identities. The caller adds a validated cursor, whitelisted ordering, and
// page-size-plus-one limit.
func collectorCatalogPageQuery(
	database *gorm.DB,
	request normalizedListRequest,
	view catalogLivenessView,
) *gorm.DB {
	return collectorCatalogFilteredQuery(database, request, view).Select(
		"fleet.collector_id AS collector_id",
		"fleet.display_name AS display_name",
		"runtime.hostname AS hostname",
		"runtime.last_seen_at_unix_micro AS last_seen_at_unix_micro",
		"runtime.queued_bytes AS queued_bytes",
	)
}

func collectorCatalogStatePredicate(
	filters []ConnectionState,
	entries []CollectorLiveness,
) (string, []any) {
	if len(filters) == 0 {
		return "", nil
	}

	online := make([]CollectorLiveness, 0, len(entries))
	stale := make([]CollectorLiveness, 0, len(entries))
	for _, entry := range entries {
		switch entry.State {
		case LivenessStateOnline:
			online = append(online, entry)
		case LivenessStateStale:
			stale = append(stale, entry)
		}
	}

	branches := make([]string, 0, len(filters))
	arguments := make([]any, 0, len(filters)+len(entries)*10)
	for _, state := range filters {
		switch state {
		case ConnectionStateDisabled:
			branches = append(
				branches,
				"fleet.administrative_state = ?",
			)
			arguments = append(arguments, AdministrativeStateDisabled)
		case ConnectionStateOnline:
			branch, branchArguments := collectorCatalogLiveStateBranch(online)
			branches = append(branches, branch)
			arguments = append(arguments, branchArguments...)
		case ConnectionStateStale:
			branch, branchArguments := collectorCatalogLiveStateBranch(stale)
			branches = append(branches, branch)
			arguments = append(arguments, branchArguments...)
		case ConnectionStateOffline:
			if len(entries) == 0 {
				branches = append(
					branches,
					"fleet.administrative_state = ?",
				)
				arguments = append(arguments, AdministrativeStateEnabled)
				continue
			}
			tuples, tupleArguments := collectorCatalogLeaseTuples(entries)
			branches = append(
				branches,
				"(fleet.administrative_state = ? AND NOT ("+tuples+"))",
			)
			arguments = append(
				arguments,
				AdministrativeStateEnabled,
			)
			arguments = append(arguments, tupleArguments...)
		}
	}
	return "(" + strings.Join(branches, " OR ") + ")", arguments
}

func collectorCatalogLiveStateBranch(
	entries []CollectorLiveness,
) (string, []any) {
	if len(entries) == 0 {
		return "0", nil
	}
	tuples, arguments := collectorCatalogLeaseTuples(entries)
	return "(fleet.administrative_state = ? AND (" + tuples + "))",
		append([]any{AdministrativeStateEnabled}, arguments...)
}

func collectorCatalogLeaseTuples(
	entries []CollectorLiveness,
) (string, []any) {
	tuples := make([]string, 0, len(entries))
	arguments := make([]any, 0, len(entries)*5)
	for _, entry := range entries {
		tuples = append(tuples, `(
			runtime.boot_epoch IS NOT NULL
			AND runtime.stream_id IS NOT NULL
			AND runtime.tenant_id = ?
			AND runtime.collector_id = ?
			AND runtime.boot_epoch = ?
			AND runtime.stream_id = ?
			AND runtime.lease_generation = ?
		)`)
		arguments = append(
			arguments,
			entry.Lease.TenantID,
			entry.Lease.CollectorID,
			entry.Lease.BootEpoch,
			entry.Lease.StreamID,
			boundedUint64ToInt64(entry.Lease.Generation),
		)
	}
	return strings.Join(tuples, " OR "), arguments
}

func applyCollectorCatalogCursor(
	query *gorm.DB,
	request normalizedListRequest,
	cursor collectorListCursor,
) *gorm.DB {
	if cursor.CollectorID == "" {
		return query
	}

	switch request.sortBy {
	case CollectorSortByDisplayName:
		key := cursor.StringKey
		value := ""
		if key.Valid {
			value = key.Value
		}
		return applyCollectorTupleCursor(
			query,
			request.direction,
			"fleet.display_name_sort_key",
			"fleet.collector_id",
			value,
			cursor.CollectorID,
		)
	case CollectorSortByHostname:
		return applyCollectorTupleCursor(
			query,
			request.direction,
			"runtime.hostname",
			"runtime.collector_id",
			cursor.StringKey.Value,
			cursor.CollectorID,
		)
	case CollectorSortByLastSeenAt:
		return applyCollectorTupleCursor(
			query,
			request.direction,
			"runtime.last_seen_at_unix_micro",
			"runtime.collector_id",
			cursor.IntegerKey.Value,
			cursor.CollectorID,
		)
	case CollectorSortByQueueBytes:
		return applyCollectorTupleCursor(
			query,
			request.direction,
			"runtime.queued_bytes",
			"runtime.collector_id",
			cursor.IntegerKey.Value,
			cursor.CollectorID,
		)
	default:
		return query
	}
}

func applyCollectorTupleCursor(
	query *gorm.DB,
	direction SortDirection,
	sortColumn string,
	collectorColumn string,
	value any,
	collectorID string,
) *gorm.DB {
	switch direction {
	case SortAscending:
		return query.Where(
			"("+sortColumn+", "+collectorColumn+") > (?, ?)",
			value,
			collectorID,
		)
	case SortDescending:
		return query.Where(
			"("+sortColumn+", "+collectorColumn+") < (?, ?)",
			value,
			collectorID,
		)
	default:
		return query
	}
}

func applyCollectorCatalogOrder(
	query *gorm.DB,
	request normalizedListRequest,
) *gorm.DB {
	switch {
	case request.sortBy == CollectorSortByDisplayName &&
		request.direction == SortAscending:
		return query.Order(
			"fleet.display_name_sort_key ASC, fleet.collector_id ASC",
		)
	case request.sortBy == CollectorSortByDisplayName &&
		request.direction == SortDescending:
		return query.Order(
			"fleet.display_name_sort_key DESC, fleet.collector_id DESC",
		)
	case request.sortBy == CollectorSortByHostname &&
		request.direction == SortAscending:
		return query.Order(
			"runtime.hostname ASC, runtime.collector_id ASC",
		)
	case request.sortBy == CollectorSortByHostname &&
		request.direction == SortDescending:
		return query.Order(
			"runtime.hostname DESC, runtime.collector_id DESC",
		)
	case request.sortBy == CollectorSortByLastSeenAt &&
		request.direction == SortAscending:
		return query.Order(
			"runtime.last_seen_at_unix_micro ASC, runtime.collector_id ASC",
		)
	case request.sortBy == CollectorSortByLastSeenAt &&
		request.direction == SortDescending:
		return query.Order(
			"runtime.last_seen_at_unix_micro DESC, runtime.collector_id DESC",
		)
	case request.sortBy == CollectorSortByQueueBytes &&
		request.direction == SortAscending:
		return query.Order(
			"runtime.queued_bytes ASC, runtime.collector_id ASC",
		)
	case request.sortBy == CollectorSortByQueueBytes &&
		request.direction == SortDescending:
		return query.Order(
			"runtime.queued_bytes DESC, runtime.collector_id DESC",
		)
	default:
		return query
	}
}

func collectorCursorForPageRecord(
	record catalogPageRecord,
	request normalizedListRequest,
	requestHash string,
	revision int64,
	livenessDigest string,
) collectorListCursor {
	cursor := collectorListCursor{
		Version:        collectorListCursorVersion,
		RequestHash:    requestHash,
		Revision:       revision,
		LivenessDigest: livenessDigest,
		CollectorID:    record.CollectorID,
	}
	switch request.sortBy {
	case CollectorSortByDisplayName:
		cursor.StringKey = &collectorListNullableString{}
		if record.DisplayName != nil {
			cursor.StringKey.Valid = true
			cursor.StringKey.Value = *record.DisplayName
		}
	case CollectorSortByHostname:
		cursor.StringKey = &collectorListNullableString{
			Valid: true,
			Value: record.Hostname,
		}
	case CollectorSortByLastSeenAt:
		cursor.IntegerKey = &collectorListNullableInt64{
			Valid: true,
			Value: record.LastSeenAtUnixMicro,
		}
	case CollectorSortByQueueBytes:
		cursor.IntegerKey = &collectorListNullableInt64{
			Valid: true,
			Value: record.QueuedBytes,
		}
	}
	return cursor
}
