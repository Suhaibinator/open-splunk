package collectorfleet

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestCollectorCatalogQueryUsesTrustedTenantAndExactPredicates(
	t *testing.T,
) {
	t.Parallel()

	database, _ := openTestStore(t)
	indexName := "main"
	text := "EDGE"
	request := normalizedListRequest{
		tenantID: "tenant-a",
		pageSize: 4,
		stateFilters: []ConnectionState{
			ConnectionStateDisabled,
			ConnectionStateOffline,
			ConnectionStateOnline,
			ConnectionStateStale,
		},
		indexNameFilter: &indexName,
		textFilter:      &text,
		sortBy:          CollectorSortByHostname,
		direction:       SortDescending,
	}
	view := catalogLivenessView{entries: []CollectorLiveness{
		{
			Lease: Lease{
				Scope:       Scope{TenantID: "tenant-a"},
				CollectorID: "collector-online",
				BootEpoch:   "boot-online",
				StreamID:    "stream-online",
				Generation:  7,
			},
			State: LivenessStateOnline,
		},
		{
			Lease: Lease{
				Scope:       Scope{TenantID: "tenant-a"},
				CollectorID: "collector-stale",
				BootEpoch:   "boot-stale",
				StreamID:    "stream-stale",
				Generation:  9,
			},
			State: LivenessStateStale,
		},
	}}
	cursor := collectorListCursor{
		CollectorID: "collector-middle",
		StringKey: &collectorListNullableString{
			Valid: true,
			Value: "middle.example",
		},
	}

	query := collectorCatalogPageQuery(
		database.GORMDB().Session(&gorm.Session{DryRun: true}),
		request,
		view,
	)
	query = applyCollectorCatalogCursor(query, request, cursor)
	query = applyCollectorCatalogOrder(query, request)
	var rows []catalogPageRecord
	generated := query.Limit(int(request.pageSize) + 1).Find(&rows)
	if generated.Error != nil {
		t.Fatalf("build collector catalog query: %v", generated.Error)
	}
	statement := strings.Join(
		strings.Fields(generated.Statement.SQL.String()),
		" ",
	)
	for _, fragment := range []string{
		"FROM collector_fleet AS fleet",
		"INNER JOIN collector_runtime AS runtime",
		"runtime.tenant_id = fleet.tenant_id",
		"runtime.collector_id = fleet.collector_id",
		"fleet.tenant_id = ? AND runtime.tenant_id = ?",
		"FROM collector_authorized_indexes AS authorized",
		"authorized.tenant_id = fleet.tenant_id",
		"authorized.index_name = ?",
		"authorized.collector_id = fleet.collector_id",
		"instr(lower(fleet.collector_id), lower(?)) > 0",
		"instr(lower(fleet.display_name), lower(?)) > 0",
		"instr(lower(runtime.hostname), lower(?)) > 0",
		"runtime.boot_epoch IS NOT NULL",
		"runtime.stream_id IS NOT NULL",
		"runtime.boot_epoch = ?",
		"runtime.stream_id = ?",
		"runtime.lease_generation = ?",
		"fleet.administrative_state = ?",
		"(runtime.hostname, runtime.collector_id) < (?, ?)",
		"ORDER BY runtime.hostname DESC, runtime.collector_id DESC",
		"LIMIT 5",
	} {
		if !strings.Contains(statement, fragment) {
			t.Errorf("query SQL = %q, want fragment %q", statement, fragment)
		}
	}
	if strings.Contains(statement, request.tenantID) {
		t.Errorf("trusted tenant was interpolated into query SQL: %q", statement)
	}
	if strings.Contains(statement, "INDEXED BY") {
		t.Errorf("query SQL pins an SQLite index: %q", statement)
	}
	if countCatalogQueryArgument(generated.Statement.Vars, "tenant-a") < 4 {
		t.Errorf(
			"query args = %#v, want outer and exact-tuple tenant bindings",
			generated.Statement.Vars,
		)
	}
	if countCatalogQueryArgument(
		generated.Statement.Vars,
		"tenant-b",
	) != 0 {
		t.Errorf(
			"query args unexpectedly contain another tenant: %#v",
			generated.Statement.Vars,
		)
	}
	for _, value := range []any{
		indexName,
		text,
		"collector-online",
		"boot-online",
		"stream-online",
		int64(7),
		"collector-stale",
		"boot-stale",
		"stream-stale",
		int64(9),
		AdministrativeStateDisabled,
		AdministrativeStateEnabled,
	} {
		if countCatalogQueryArgument(generated.Statement.Vars, value) == 0 {
			t.Errorf(
				"query args = %#v, want exact value %#v",
				generated.Statement.Vars,
				value,
			)
		}
	}
}

func TestCollectorCatalogCursorsUseRowValueSeeks(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	tests := []struct {
		sortBy     CollectorSortBy
		sortColumn string
		cursor     collectorListCursor
	}{
		{
			sortBy:     CollectorSortByDisplayName,
			sortColumn: "fleet.display_name_sort_key",
			cursor: collectorListCursor{
				CollectorID: "collector-cursor",
				StringKey: &collectorListNullableString{
					Valid: true,
					Value: "Display Cursor",
				},
			},
		},
		{
			sortBy:     CollectorSortByHostname,
			sortColumn: "runtime.hostname",
			cursor: collectorListCursor{
				CollectorID: "collector-cursor",
				StringKey: &collectorListNullableString{
					Valid: true,
					Value: "cursor.example",
				},
			},
		},
		{
			sortBy:     CollectorSortByLastSeenAt,
			sortColumn: "runtime.last_seen_at_unix_micro",
			cursor: collectorListCursor{
				CollectorID: "collector-cursor",
				IntegerKey: &collectorListNullableInt64{
					Valid: true,
					Value: 424242,
				},
			},
		},
		{
			sortBy:     CollectorSortByQueueBytes,
			sortColumn: "runtime.queued_bytes",
			cursor: collectorListCursor{
				CollectorID: "collector-cursor",
				IntegerKey: &collectorListNullableInt64{
					Valid: true,
					Value: 515151,
				},
			},
		},
	}
	for _, test := range tests {
		for _, direction := range []SortDirection{
			SortAscending,
			SortDescending,
		} {
			test := test
			direction := direction
			t.Run(
				string(test.sortBy)+"/"+string(direction),
				func(t *testing.T) {
					request := normalizedListRequest{
						tenantID:  "tenant-a",
						sortBy:    test.sortBy,
						direction: direction,
					}
					query := collectorCatalogPageQuery(
						database.GORMDB().Session(
							&gorm.Session{DryRun: true},
						),
						request,
						catalogLivenessView{},
					)
					query = applyCollectorCatalogCursor(
						query,
						request,
						test.cursor,
					)
					var records []catalogPageRecord
					generated := query.Find(&records)
					if generated.Error != nil {
						t.Fatalf("build cursor query: %v", generated.Error)
					}
					statement := strings.Join(
						strings.Fields(
							generated.Statement.SQL.String(),
						),
						" ",
					)
					operator := ">"
					if direction == SortDescending {
						operator = "<"
					}
					collectorColumn := "runtime.collector_id"
					if test.sortBy == CollectorSortByDisplayName {
						collectorColumn = "fleet.collector_id"
					}
					want := "(" + test.sortColumn + ", " +
						collectorColumn + ") " + operator + " (?, ?)"
					if !strings.Contains(statement, want) {
						t.Fatalf(
							"cursor SQL = %q, want row-value seek %q",
							statement,
							want,
						)
					}
				},
			)
		}
	}

	request := normalizedListRequest{
		tenantID:  "tenant-a",
		sortBy:    CollectorSortByDisplayName,
		direction: SortAscending,
	}
	query := collectorCatalogPageQuery(
		database.GORMDB().Session(&gorm.Session{DryRun: true}),
		request,
		catalogLivenessView{},
	)
	query = applyCollectorCatalogCursor(
		query,
		request,
		collectorListCursor{
			CollectorID: "collector-null",
			StringKey:   &collectorListNullableString{},
		},
	)
	var records []catalogPageRecord
	generated := query.Find(&records)
	if generated.Error != nil {
		t.Fatalf("build null display cursor query: %v", generated.Error)
	}
	gotArguments := generated.Statement.Vars
	if got := gotArguments[len(gotArguments)-2:]; !slices.Equal(
		got,
		[]any{"", "collector-null"},
	) {
		t.Fatalf("null display cursor args = %#v, want empty sort sentinel", got)
	}
}

func TestCollectorCatalogStateFiltersAreExactAndUnioned(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	leases := make(map[string]Lease)
	for offset, collectorID := range []string{
		"collector-disabled",
		"collector-disconnected",
		"collector-mismatch",
		"collector-online",
		"collector-stale",
	} {
		leases[collectorID] = claimCatalogQueryCollector(
			t,
			store,
			"tenant-a",
			collectorID,
			collectorID+".example",
			now.Add(time.Duration(offset)*time.Second),
		)
	}
	if err := database.GORMDB().
		Model(&fleetRecord{}).
		Where(
			"tenant_id = ? AND collector_id = ?",
			"tenant-a",
			"collector-disabled",
		).
		Update(
			"administrative_state",
			AdministrativeStateDisabled,
		).Error; err != nil {
		t.Fatalf("make active collector administratively disabled: %v", err)
	}
	if applied, err := store.Disconnect(
		ctx,
		leases["collector-disconnected"],
		now.Add(time.Minute),
	); err != nil || !applied {
		t.Fatalf("Disconnect() = (%v, %v), want (true, nil)", applied, err)
	}

	mismatched := leases["collector-mismatch"]
	mismatched.StreamID = "other-stream"
	view, err := newCatalogLivenessView(
		Scope{TenantID: "tenant-a"},
		[]CollectorLiveness{
			{
				Lease: leases["collector-disabled"],
				State: LivenessStateOnline,
			},
			{
				Lease: mismatched,
				State: LivenessStateOnline,
			},
			{
				Lease: leases["collector-online"],
				State: LivenessStateOnline,
			},
			{
				Lease: leases["collector-stale"],
				State: LivenessStateStale,
			},
		},
	)
	if err != nil {
		t.Fatalf("newCatalogLivenessView(): %v", err)
	}

	tests := []struct {
		name    string
		filters []ConnectionState
		want    []string
	}{
		{
			name:    "disabled wins over exact online lease",
			filters: []ConnectionState{ConnectionStateDisabled},
			want:    []string{"collector-disabled"},
		},
		{
			name:    "online requires complete exact enabled lease",
			filters: []ConnectionState{ConnectionStateOnline},
			want:    []string{"collector-online"},
		},
		{
			name:    "stale requires complete exact enabled lease",
			filters: []ConnectionState{ConnectionStateStale},
			want:    []string{"collector-stale"},
		},
		{
			name:    "offline includes mismatched and disconnected null lease",
			filters: []ConnectionState{ConnectionStateOffline},
			want: []string{
				"collector-disconnected",
				"collector-mismatch",
			},
		},
		{
			name: "online and offline union",
			filters: []ConnectionState{
				ConnectionStateOffline,
				ConnectionStateOnline,
			},
			want: []string{
				"collector-disconnected",
				"collector-mismatch",
				"collector-online",
			},
		},
		{
			name: "all states union",
			filters: []ConnectionState{
				ConnectionStateDisabled,
				ConnectionStateOffline,
				ConnectionStateOnline,
				ConnectionStateStale,
			},
			want: []string{
				"collector-disabled",
				"collector-disconnected",
				"collector-mismatch",
				"collector-online",
				"collector-stale",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			request := normalizedListRequest{
				tenantID:     "tenant-a",
				stateFilters: test.filters,
				sortBy:       CollectorSortByDisplayName,
				direction:    SortAscending,
			}
			got := runCatalogPageQuery(t, database.GORMDB(), request, view)
			if !slices.Equal(got, test.want) {
				t.Fatalf("collector IDs = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCollectorCatalogDisplayNameCursorMatchesSQLiteNullOrdering(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	now := time.Date(2026, time.July, 28, 13, 0, 0, 0, time.UTC)
	displayNames := map[string]*string{
		"collector-alpha-a": new("Alpha"),
		"collector-alpha-b": new("Alpha"),
		"collector-alpha-c": new("Alpha"),
		"collector-null-a":  nil,
		"collector-null-b":  nil,
		"collector-null-c":  nil,
		"collector-zulu-a":  new("Zulu"),
		"collector-zulu-b":  new("Zulu"),
		"collector-zulu":    new("Zulu"),
	}
	for collectorID, displayName := range displayNames {
		claimCatalogQueryCollector(
			t,
			store,
			"tenant-a",
			collectorID,
			collectorID+".example",
			now,
		)
		if err := database.GORMDB().
			Model(&fleetRecord{}).
			Where(
				"tenant_id = ? AND collector_id = ?",
				"tenant-a",
				collectorID,
			).
			Update("display_name", displayName).Error; err != nil {
			t.Fatalf("set %s display name: %v", collectorID, err)
		}
	}

	tests := []struct {
		name      string
		direction SortDirection
		want      []string
	}{
		{
			name:      "ascending null first",
			direction: SortAscending,
			want: []string{
				"collector-null-a",
				"collector-null-b",
				"collector-null-c",
				"collector-alpha-a",
				"collector-alpha-b",
				"collector-alpha-c",
				"collector-zulu",
				"collector-zulu-a",
				"collector-zulu-b",
			},
		},
		{
			name:      "descending null last",
			direction: SortDescending,
			want: []string{
				"collector-zulu-b",
				"collector-zulu-a",
				"collector-zulu",
				"collector-alpha-c",
				"collector-alpha-b",
				"collector-alpha-a",
				"collector-null-c",
				"collector-null-b",
				"collector-null-a",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			request := normalizedListRequest{
				tenantID:  "tenant-a",
				sortBy:    CollectorSortByDisplayName,
				direction: test.direction,
			}
			records := runCatalogPageRecords(
				t,
				database.GORMDB(),
				request,
				catalogLivenessView{},
				collectorListCursor{},
			)
			if got := catalogPageRecordIDs(records); !slices.Equal(
				got,
				test.want,
			) {
				t.Fatalf("full order = %v, want %v", got, test.want)
			}
			for index := range records {
				cursor := collectorCursorForPageRecord(
					records[index],
					request,
					collectorSHA256Digest([]byte("request")),
					1,
					collectorSHA256Digest([]byte("liveness")),
				)
				tail := runCatalogPageRecords(
					t,
					database.GORMDB(),
					request,
					catalogLivenessView{},
					cursor,
				)
				if got, want := catalogPageRecordIDs(tail),
					test.want[index+1:]; !slices.Equal(got, want) {
					t.Fatalf(
						"tail after %s = %v, want %v",
						records[index].CollectorID,
						got,
						want,
					)
				}
			}
			for _, pageSize := range []int{1, 2, 4} {
				if got := walkCatalogPageRecordIDs(
					t,
					database.GORMDB(),
					request,
					pageSize,
				); !slices.Equal(got, test.want) {
					t.Fatalf(
						"page-size %d traversal = %v, want %v",
						pageSize,
						got,
						test.want,
					)
				}
			}
		})
	}
}

func TestCollectorCatalogUnfilteredSortsUseCompositeIndexes(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	tests := []struct {
		sortBy    CollectorSortBy
		direction SortDirection
		wantIndex string
	}{
		{
			sortBy:    CollectorSortByDisplayName,
			direction: SortAscending,
			wantIndex: "collector_fleet_tenant_display_id_idx",
		},
		{
			sortBy:    CollectorSortByDisplayName,
			direction: SortDescending,
			wantIndex: "collector_fleet_tenant_display_id_idx",
		},
		{
			sortBy:    CollectorSortByHostname,
			direction: SortAscending,
			wantIndex: "collector_runtime_tenant_hostname_id_idx",
		},
		{
			sortBy:    CollectorSortByHostname,
			direction: SortDescending,
			wantIndex: "collector_runtime_tenant_hostname_id_idx",
		},
		{
			sortBy:    CollectorSortByLastSeenAt,
			direction: SortAscending,
			wantIndex: "collector_runtime_tenant_last_seen_id_idx",
		},
		{
			sortBy:    CollectorSortByLastSeenAt,
			direction: SortDescending,
			wantIndex: "collector_runtime_tenant_last_seen_id_idx",
		},
		{
			sortBy:    CollectorSortByQueueBytes,
			direction: SortAscending,
			wantIndex: "collector_runtime_tenant_queued_bytes_id_idx",
		},
		{
			sortBy:    CollectorSortByQueueBytes,
			direction: SortDescending,
			wantIndex: "collector_runtime_tenant_queued_bytes_id_idx",
		},
	}
	for _, test := range tests {
		test := test
		for _, withCursor := range []bool{false, true} {
			withCursor := withCursor
			name := string(test.sortBy) + "/" + string(test.direction)
			if withCursor {
				name += "/cursor"
			} else {
				name += "/first-page"
			}
			t.Run(name, func(t *testing.T) {
				request := normalizedListRequest{
					tenantID:  "tenant-a",
					pageSize:  MaximumCollectorListPageSize,
					sortBy:    test.sortBy,
					direction: test.direction,
				}
				query := collectorCatalogPageQuery(
					database.GORMDB().Session(&gorm.Session{DryRun: true}),
					request,
					catalogLivenessView{},
				)
				if withCursor {
					query = applyCollectorCatalogCursor(
						query,
						request,
						catalogQueryPlanCursor(test.sortBy),
					)
				}
				query = applyCollectorCatalogOrder(query, request)
				var records []catalogPageRecord
				generated := query.
					Limit(int(request.pageSize) + 1).
					Find(&records)
				if generated.Error != nil {
					t.Fatalf("build GORM query: %v", generated.Error)
				}
				var planRows []struct {
					ID     int
					Parent int
					Unused int
					Detail string
				}
				if err := database.GORMDB().
					Raw(
						"EXPLAIN QUERY PLAN "+
							generated.Statement.SQL.String(),
						generated.Statement.Vars...,
					).
					Scan(&planRows).Error; err != nil {
					t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
				}
				details := make([]string, len(planRows))
				for index, row := range planRows {
					details[index] = row.Detail
				}
				plan := strings.Join(details, "\n")
				if !strings.Contains(plan, test.wantIndex) {
					t.Errorf(
						"query plan = %q, want index %q",
						plan,
						test.wantIndex,
					)
				}
				if strings.Contains(plan, "USE TEMP B-TREE") {
					t.Errorf(
						"query plan performs a temporary sort: %q",
						plan,
					)
				}
			})
		}
	}
}

func TestCollectorCatalogAuthorizedIndexFilterUsesIndexedSearch(
	t *testing.T,
) {
	t.Parallel()

	database, _ := openTestStore(t)
	indexName := "main"
	request := normalizedListRequest{
		tenantID:        "tenant-a",
		pageSize:        MaximumCollectorListPageSize,
		indexNameFilter: &indexName,
		sortBy:          CollectorSortByDisplayName,
		direction:       SortAscending,
	}
	query := collectorCatalogPageQuery(
		database.GORMDB().Session(&gorm.Session{DryRun: true}),
		request,
		catalogLivenessView{},
	)
	query = applyCollectorCatalogOrder(query, request)
	var records []catalogPageRecord
	generated := query.
		Limit(int(request.pageSize) + 1).
		Find(&records)
	if generated.Error != nil {
		t.Fatalf("build GORM query: %v", generated.Error)
	}
	var planRows []struct {
		ID     int
		Parent int
		Unused int
		Detail string
	}
	if err := database.GORMDB().
		Raw(
			"EXPLAIN QUERY PLAN "+generated.Statement.SQL.String(),
			generated.Statement.Vars...,
		).
		Scan(&planRows).Error; err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	details := make([]string, len(planRows))
	for index, row := range planRows {
		details[index] = row.Detail
	}
	plan := strings.Join(details, "\n")
	authorizedUsesIndexedSearch := false
	for _, detail := range details {
		if strings.Contains(detail, "SEARCH authorized") &&
			strings.Contains(detail, " USING ") {
			authorizedUsesIndexedSearch = true
			break
		}
	}
	if !authorizedUsesIndexedSearch {
		t.Errorf(
			"authorized-index filter plan = %q, want indexed SEARCH",
			plan,
		)
	}
	if strings.Contains(generated.Statement.SQL.String(), "INDEXED BY") {
		t.Errorf(
			"authorized-index filter SQL pins an index: %q",
			generated.Statement.SQL.String(),
		)
	}
}

func TestCollectorCatalogTextFilterPlansStayTenantAndSortIndexed(
	t *testing.T,
) {
	t.Parallel()

	database, _ := openTestStore(t)
	text := "needle"
	request := normalizedListRequest{
		tenantID:   "tenant-a",
		pageSize:   MaximumCollectorListPageSize,
		textFilter: &text,
		sortBy:     CollectorSortByDisplayName,
		direction:  SortAscending,
	}
	pageQuery := collectorCatalogPageQuery(
		database.GORMDB().Session(&gorm.Session{DryRun: true}),
		request,
		catalogLivenessView{},
	)
	pageQuery = applyCollectorCatalogOrder(pageQuery, request)
	var records []catalogPageRecord
	generatedPage := pageQuery.
		Limit(int(request.pageSize) + 1).
		Find(&records)
	if generatedPage.Error != nil {
		t.Fatalf("build text-filter page query: %v", generatedPage.Error)
	}
	pagePlan := explainCollectorCatalogQuery(t, database.GORMDB(), generatedPage)
	if !strings.Contains(
		pagePlan,
		"collector_fleet_tenant_display_id_idx",
	) ||
		!strings.Contains(pagePlan, "SEARCH runtime") ||
		strings.Contains(pagePlan, "USE TEMP B-TREE") {
		t.Fatalf(
			"text-filter page plan = %q, want tenant display index, indexed runtime join, and no temporary sort",
			pagePlan,
		)
	}

	var count int64
	generatedCount := collectorCatalogFilteredQuery(
		database.GORMDB().Session(&gorm.Session{DryRun: true}),
		request,
		catalogLivenessView{},
	).Count(&count)
	if generatedCount.Error != nil {
		t.Fatalf("build text-filter count query: %v", generatedCount.Error)
	}
	countPlan := explainCollectorCatalogQuery(
		t,
		database.GORMDB(),
		generatedCount,
	)
	if !strings.Contains(countPlan, "SEARCH fleet") ||
		!strings.Contains(countPlan, "SEARCH runtime") ||
		strings.Contains(countPlan, "SCAN fleet") ||
		strings.Contains(countPlan, "SCAN runtime") {
		t.Fatalf(
			"text-filter count plan = %q, want tenant-keyed parent searches",
			countPlan,
		)
	}
}

func explainCollectorCatalogQuery(
	t *testing.T,
	database *gorm.DB,
	generated *gorm.DB,
) string {
	t.Helper()
	var planRows []struct {
		ID     int
		Parent int
		Unused int
		Detail string
	}
	if err := database.
		Raw(
			"EXPLAIN QUERY PLAN "+generated.Statement.SQL.String(),
			generated.Statement.Vars...,
		).
		Scan(&planRows).Error; err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	details := make([]string, len(planRows))
	for index, row := range planRows {
		details[index] = row.Detail
	}
	return strings.Join(details, "\n")
}

func claimCatalogQueryCollector(
	t *testing.T,
	store *Store,
	tenantID string,
	collectorID string,
	hostname string,
	receivedAt time.Time,
) Lease {
	t.Helper()
	request := testClaim(receivedAt)
	request.TenantID = tenantID
	request.CollectorID = collectorID
	request.BootEpoch = "boot-" + collectorID
	request.StreamID = "stream-" + collectorID
	request.Hello.InstanceID = "instance-" + collectorID
	request.Hello.Hostname = hostname
	_, lease, err := store.Claim(context.Background(), request)
	if err != nil {
		t.Fatalf("Claim(%s): %v", collectorID, err)
	}
	return lease
}

func runCatalogPageQuery(
	t *testing.T,
	database *gorm.DB,
	request normalizedListRequest,
	view catalogLivenessView,
) []string {
	t.Helper()
	return catalogPageRecordIDs(runCatalogPageRecords(
		t,
		database,
		request,
		view,
		collectorListCursor{},
	))
}

func runCatalogPageRecords(
	t *testing.T,
	database *gorm.DB,
	request normalizedListRequest,
	view catalogLivenessView,
	cursor collectorListCursor,
) []catalogPageRecord {
	t.Helper()
	query := collectorCatalogPageQuery(database, request, view)
	query = applyCollectorCatalogCursor(query, request, cursor)
	query = applyCollectorCatalogOrder(query, request)
	var records []catalogPageRecord
	if err := query.Find(&records).Error; err != nil {
		t.Fatalf("run collector catalog query: %v", err)
	}
	return records
}

func catalogPageRecordIDs(records []catalogPageRecord) []string {
	result := make([]string, len(records))
	for index, record := range records {
		result[index] = record.CollectorID
	}
	return result
}

func walkCatalogPageRecordIDs(
	t *testing.T,
	database *gorm.DB,
	request normalizedListRequest,
	pageSize int,
) []string {
	t.Helper()

	var result []string
	cursor := collectorListCursor{}
	for range 100 {
		query := collectorCatalogPageQuery(
			database,
			request,
			catalogLivenessView{},
		)
		query = applyCollectorCatalogCursor(query, request, cursor)
		query = applyCollectorCatalogOrder(query, request)
		var records []catalogPageRecord
		if err := query.Limit(pageSize).Find(&records).Error; err != nil {
			t.Fatalf("run collector catalog page: %v", err)
		}
		result = append(result, catalogPageRecordIDs(records)...)
		if len(records) < pageSize {
			return result
		}
		cursor = collectorCursorForPageRecord(
			records[len(records)-1],
			request,
			collectorSHA256Digest([]byte("request")),
			1,
			collectorSHA256Digest([]byte("liveness")),
		)
	}
	t.Fatal("collector catalog traversal did not terminate")
	return nil
}

func countCatalogQueryArgument(arguments []any, want any) int {
	count := 0
	for _, argument := range arguments {
		if argument == want {
			count++
		}
	}
	return count
}

func catalogQueryPlanCursor(sortBy CollectorSortBy) collectorListCursor {
	cursor := collectorListCursor{CollectorID: "collector-middle"}
	switch sortBy {
	case CollectorSortByDisplayName:
		cursor.StringKey = &collectorListNullableString{
			Valid: true,
			Value: "Middle",
		}
	case CollectorSortByHostname:
		cursor.StringKey = &collectorListNullableString{
			Valid: true,
			Value: "middle.example",
		}
	case CollectorSortByLastSeenAt:
		cursor.IntegerKey = &collectorListNullableInt64{
			Valid: true,
			Value: 1,
		}
	case CollectorSortByQueueBytes:
		cursor.IntegerKey = &collectorListNullableInt64{
			Valid: true,
			Value: 1,
		}
	}
	return cursor
}
