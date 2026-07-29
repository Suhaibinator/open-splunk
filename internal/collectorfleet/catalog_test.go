package collectorfleet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

var catalogTestCursorKey = bytes.Repeat(
	[]byte{0x6b},
	minimumCollectorCursorKeyBytes,
)

type catalogTestCollector struct {
	collector Collector
	lease     Lease
	inputIDs  []string
}

func TestNewCatalogValidatesConfigurationAndDetachesCursorKey(t *testing.T) {
	t.Parallel()

	if _, err := NewCatalog(nil, CatalogOptions{
		CursorKey: catalogTestCursorKey,
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("NewCatalog(nil) error = %v, want ErrInvalidArgument", err)
	}

	database, store := openTestStore(t)
	for _, size := range []int{
		0,
		minimumCollectorCursorKeyBytes - 1,
		maximumCollectorCursorKeyBytes + 1,
	} {
		if _, err := NewCatalog(database, CatalogOptions{
			CursorKey: make([]byte, size),
		}); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"NewCatalog(%d-byte key) error = %v, want ErrInvalidArgument",
				size,
				err,
			)
		}
	}

	key := bytes.Repeat([]byte{0x42}, minimumCollectorCursorKeyBytes)
	catalog, err := NewCatalog(database, CatalogOptions{CursorKey: key})
	if err != nil {
		t.Fatalf("NewCatalog(): %v", err)
	}
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for index := range 3 {
		claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			fmt.Sprintf("collector-%c", 'a'+index),
			fmt.Sprintf("host-%c", 'a'+index),
			nil,
			base.Add(time.Duration(index)*time.Minute),
			uint64(index),
			"main",
		)
	}
	first, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{PageSize: 1},
	)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if first.NextPageToken == nil {
		t.Fatal("List(first) did not return a continuation token")
	}
	key[0] ^= 0xff
	second, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{
			PageSize:  1,
			PageToken: *first.NextPageToken,
		},
	)
	if err != nil {
		t.Fatalf("List(after caller key mutation): %v", err)
	}
	if got := catalogEntryIDs(second.Entries); !slices.Equal(
		got,
		[]string{"collector-b"},
	) {
		t.Fatalf("second page IDs = %v, want [collector-b]", got)
	}
}

func TestCatalogGetIsTenantScopedAndProjectsExactLiveness(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	ctx := context.Background()
	base := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	tenantA := claimCatalogTestCollector(
		t,
		store,
		"tenant-a",
		"shared-collector",
		"tenant-a.example",
		stringPointerForCatalogTest("Tenant A"),
		base,
		17,
		"audit",
		"main",
	)
	tenantB := claimCatalogTestCollector(
		t,
		store,
		"tenant-b",
		"shared-collector",
		"tenant-b.example",
		stringPointerForCatalogTest("Tenant B"),
		base.Add(time.Minute),
		29,
		"main",
	)

	entry, err := catalog.Get(
		ctx,
		tenantA.lease.Scope,
		tenantA.lease.CollectorID,
		nil,
	)
	if err != nil {
		t.Fatalf("Get(offline): %v", err)
	}
	if entry.ConnectionState != ConnectionStateOffline {
		t.Fatalf(
			"offline connection state = %q, want %q",
			entry.ConnectionState,
			ConnectionStateOffline,
		)
	}
	if entry.Collector.TenantID != "tenant-a" ||
		entry.Collector.Hostname != "tenant-a.example" {
		t.Fatalf("tenant-a entry = %#v", entry)
	}

	for _, test := range []struct {
		name string
		live CollectorLiveness
		want ConnectionState
	}{
		{
			name: "online exact lease",
			live: CollectorLiveness{
				Lease: tenantA.lease,
				State: LivenessStateOnline,
			},
			want: ConnectionStateOnline,
		},
		{
			name: "stale exact lease",
			live: CollectorLiveness{
				Lease: tenantA.lease,
				State: LivenessStateStale,
			},
			want: ConnectionStateStale,
		},
		{
			name: "mismatched generation",
			live: CollectorLiveness{
				Lease: func() Lease {
					lease := tenantA.lease
					lease.Generation++
					return lease
				}(),
				State: LivenessStateOnline,
			},
			want: ConnectionStateOffline,
		},
		{
			name: "mismatched stream",
			live: CollectorLiveness{
				Lease: func() Lease {
					lease := tenantA.lease
					lease.StreamID = "replacement-stream"
					return lease
				}(),
				State: LivenessStateOnline,
			},
			want: ConnectionStateOffline,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, getErr := catalog.Get(
				ctx,
				tenantA.lease.Scope,
				tenantA.lease.CollectorID,
				[]CollectorLiveness{test.live},
			)
			if getErr != nil {
				t.Fatalf("Get(): %v", getErr)
			}
			if got.ConnectionState != test.want {
				t.Fatalf(
					"connection state = %q, want %q",
					got.ConnectionState,
					test.want,
				)
			}
		})
	}

	gotB, err := catalog.Get(
		ctx,
		tenantB.lease.Scope,
		tenantB.lease.CollectorID,
		nil,
	)
	if err != nil {
		t.Fatalf("Get(tenant-b): %v", err)
	}
	if gotB.Collector.TenantID != "tenant-b" ||
		gotB.Collector.Hostname != "tenant-b.example" {
		t.Fatalf("tenant-b entry = %#v", gotB)
	}
	if _, err := catalog.Get(
		ctx,
		Scope{TenantID: "tenant-c"},
		tenantA.lease.CollectorID,
		nil,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(cross tenant) error = %v, want ErrNotFound", err)
	}

	disabled, err := store.SetAdministrativeState(
		ctx,
		tenantA.lease.Scope,
		tenantA.lease.CollectorID,
		tenantA.collector.Version,
		AdministrativeStateDisabled,
		base.Add(10*time.Minute),
	)
	if err != nil {
		t.Fatalf("SetAdministrativeState(disabled): %v", err)
	}
	if disabled.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("disabled administration = %#v", disabled)
	}
	gotDisabled, err := catalog.Get(
		ctx,
		tenantA.lease.Scope,
		tenantA.lease.CollectorID,
		[]CollectorLiveness{{
			Lease: tenantA.lease,
			State: LivenessStateOnline,
		}},
	)
	if err != nil {
		t.Fatalf("Get(disabled): %v", err)
	}
	if gotDisabled.ConnectionState != ConnectionStateDisabled {
		t.Fatalf(
			"disabled exact-live state = %q, want %q",
			gotDisabled.ConnectionState,
			ConnectionStateDisabled,
		)
	}
}

func TestCatalogListSortsAndPaginatesWithoutDuplicatesOrOmissions(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
	fixtures := []struct {
		id          string
		displayName *string
		hostname    string
		lastSeen    time.Time
		queueBytes  uint64
	}{
		{
			id:         "collector-a",
			hostname:   "zeta",
			lastSeen:   base.Add(time.Minute),
			queueBytes: 100,
		},
		{
			id:         "collector-b",
			hostname:   "alpha",
			lastSeen:   base.Add(2 * time.Minute),
			queueBytes: 10,
		},
		{
			id:          "collector-c",
			displayName: stringPointerForCatalogTest("Alpha"),
			hostname:    "beta",
			lastSeen:    base.Add(3 * time.Minute),
			queueBytes:  10,
		},
		{
			id:          "collector-d",
			displayName: stringPointerForCatalogTest("Alpha"),
			hostname:    "beta",
			lastSeen:    base.Add(3 * time.Minute),
			queueBytes:  20,
		},
		{
			id:          "collector-e",
			displayName: stringPointerForCatalogTest("Zulu"),
			hostname:    "",
			lastSeen:    base.Add(5 * time.Minute),
			queueBytes:  0,
		},
		{
			id:          "collector-f",
			displayName: stringPointerForCatalogTest("Beta"),
			hostname:    "beta",
			lastSeen:    base.Add(4 * time.Minute),
			queueBytes:  20,
		},
	}
	for _, fixture := range fixtures {
		claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			fixture.id,
			fixture.hostname,
			fixture.displayName,
			fixture.lastSeen,
			fixture.queueBytes,
			"main",
		)
	}

	tests := []struct {
		name      string
		sortBy    CollectorSortBy
		direction SortDirection
		want      []string
	}{
		{
			name:      "display ascending with nulls and ties",
			sortBy:    CollectorSortByDisplayName,
			direction: SortAscending,
			want: []string{
				"collector-a",
				"collector-b",
				"collector-c",
				"collector-d",
				"collector-f",
				"collector-e",
			},
		},
		{
			name:      "display descending with nulls and ties",
			sortBy:    CollectorSortByDisplayName,
			direction: SortDescending,
			want: []string{
				"collector-e",
				"collector-f",
				"collector-d",
				"collector-c",
				"collector-b",
				"collector-a",
			},
		},
		{
			name:      "hostname ascending with ties",
			sortBy:    CollectorSortByHostname,
			direction: SortAscending,
			want: []string{
				"collector-e",
				"collector-b",
				"collector-c",
				"collector-d",
				"collector-f",
				"collector-a",
			},
		},
		{
			name:      "hostname descending with ties",
			sortBy:    CollectorSortByHostname,
			direction: SortDescending,
			want: []string{
				"collector-a",
				"collector-f",
				"collector-d",
				"collector-c",
				"collector-b",
				"collector-e",
			},
		},
		{
			name:      "last seen ascending with ties",
			sortBy:    CollectorSortByLastSeenAt,
			direction: SortAscending,
			want: []string{
				"collector-a",
				"collector-b",
				"collector-c",
				"collector-d",
				"collector-f",
				"collector-e",
			},
		},
		{
			name:      "last seen descending with ties",
			sortBy:    CollectorSortByLastSeenAt,
			direction: SortDescending,
			want: []string{
				"collector-e",
				"collector-f",
				"collector-d",
				"collector-c",
				"collector-b",
				"collector-a",
			},
		},
		{
			name:      "queue ascending with ties",
			sortBy:    CollectorSortByQueueBytes,
			direction: SortAscending,
			want: []string{
				"collector-e",
				"collector-b",
				"collector-c",
				"collector-d",
				"collector-f",
				"collector-a",
			},
		},
		{
			name:      "queue descending with ties",
			sortBy:    CollectorSortByQueueBytes,
			direction: SortDescending,
			want: []string{
				"collector-a",
				"collector-f",
				"collector-d",
				"collector-c",
				"collector-b",
				"collector-e",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries := listAllCatalogTestPages(
				t,
				catalog,
				scope,
				nil,
				ListRequest{
					PageSize:  2,
					SortBy:    test.sortBy,
					Direction: test.direction,
				},
			)
			got := catalogEntryIDs(entries)
			if !slices.Equal(got, test.want) {
				t.Fatalf("ordered IDs = %v, want %v", got, test.want)
			}
			unique := make(map[string]struct{}, len(got))
			for _, collectorID := range got {
				unique[collectorID] = struct{}{}
			}
			if len(unique) != len(fixtures) {
				t.Fatalf(
					"keyset pages produced %d unique IDs from %d rows: %v",
					len(unique),
					len(fixtures),
					got,
				)
			}
		})
	}
}

func TestCatalogListAppliesConnectionStateBeforeLimit(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 15, 0, 0, 0, time.UTC)
	fixtures := make(map[string]catalogTestCollector)
	for index, collectorID := range []string{
		"collector-a",
		"collector-b",
		"collector-c",
		"collector-d",
		"collector-e",
		"collector-f",
		"collector-g",
		"collector-h",
	} {
		fixtures[collectorID] = claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			collectorID,
			collectorID+".example",
			nil,
			base.Add(time.Duration(index)*time.Minute),
			uint64(index),
			"main",
		)
	}
	for _, collectorID := range []string{"collector-g", "collector-h"} {
		fixture := fixtures[collectorID]
		if _, err := store.SetAdministrativeState(
			context.Background(),
			scope,
			collectorID,
			fixture.collector.Version,
			AdministrativeStateDisabled,
			base.Add(20*time.Minute),
		); err != nil {
			t.Fatalf("disable %s: %v", collectorID, err)
		}
	}
	mismatchedC := fixtures["collector-c"].lease
	mismatchedC.Generation++
	liveness := []CollectorLiveness{
		{
			Lease: fixtures["collector-a"].lease,
			State: LivenessStateOnline,
		},
		{
			Lease: fixtures["collector-b"].lease,
			State: LivenessStateStale,
		},
		{Lease: mismatchedC, State: LivenessStateOnline},
		{
			Lease: fixtures["collector-d"].lease,
			State: LivenessStateOnline,
		},
		{
			Lease: fixtures["collector-e"].lease,
			State: LivenessStateStale,
		},
		{
			Lease: fixtures["collector-g"].lease,
			State: LivenessStateOnline,
		},
	}

	tests := []struct {
		state ConnectionState
		want  []string
	}{
		{
			state: ConnectionStateOnline,
			want:  []string{"collector-a", "collector-d"},
		},
		{
			state: ConnectionStateStale,
			want:  []string{"collector-b", "collector-e"},
		},
		{
			state: ConnectionStateOffline,
			want:  []string{"collector-c", "collector-f"},
		},
		{
			state: ConnectionStateDisabled,
			want:  []string{"collector-g", "collector-h"},
		},
	}
	for _, test := range tests {
		t.Run(string(test.state), func(t *testing.T) {
			entries := listAllCatalogTestPages(
				t,
				catalog,
				scope,
				liveness,
				ListRequest{
					PageSize:     1,
					StateFilters: []ConnectionState{test.state},
				},
			)
			if got := catalogEntryIDs(entries); !slices.Equal(got, test.want) {
				t.Fatalf("%s IDs = %v, want %v", test.state, got, test.want)
			}
			for _, entry := range entries {
				if entry.ConnectionState != test.state {
					t.Fatalf(
						"%s projected as %q",
						entry.Collector.CollectorID,
						entry.ConnectionState,
					)
				}
			}
		})
	}

	combined := listAllCatalogTestPages(
		t,
		catalog,
		scope,
		liveness,
		ListRequest{
			PageSize: 1,
			StateFilters: []ConnectionState{
				ConnectionStateOffline,
				ConnectionStateOnline,
			},
		},
	)
	if got, want := catalogEntryIDs(combined), []string{
		"collector-a",
		"collector-c",
		"collector-d",
		"collector-f",
	}; !slices.Equal(got, want) {
		t.Fatalf("combined filtered IDs = %v, want %v", got, want)
	}
}

func TestCatalogListFiltersByAuthorizedIndexAndText(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 16, 0, 0, 0, time.UTC)
	fixtures := []struct {
		id          string
		displayName string
		hostname    string
		indexes     []string
	}{
		{
			id:          "edge-collector",
			displayName: "North",
			hostname:    "one.example",
			indexes:     []string{"audit"},
		},
		{
			id:          "collector-b",
			displayName: "Edge Display",
			hostname:    "two.example",
			indexes:     []string{"main"},
		},
		{
			id:          "collector-c",
			displayName: "South",
			hostname:    "edge-host.example",
			indexes:     []string{"audit", "main"},
		},
		{
			id:          "collector-d",
			displayName: "EDGE CASE",
			hostname:    "four.example",
			indexes:     []string{"other"},
		},
	}
	for index, fixture := range fixtures {
		claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			fixture.id,
			fixture.hostname,
			&fixture.displayName,
			base.Add(time.Duration(index)*time.Minute),
			uint64(index),
			fixture.indexes...,
		)
	}

	text := "edge"
	entries := listAllCatalogTestPages(
		t,
		catalog,
		scope,
		nil,
		ListRequest{PageSize: 2, TextFilter: &text},
	)
	if got, want := catalogEntryIDs(entries), []string{
		"collector-d",
		"collector-b",
		"edge-collector",
		"collector-c",
	}; !slices.Equal(got, want) {
		t.Fatalf("text-filtered IDs = %v, want %v", got, want)
	}

	indexName := " Main "
	entries = listAllCatalogTestPages(
		t,
		catalog,
		scope,
		nil,
		ListRequest{PageSize: 1, IndexNameFilter: &indexName},
	)
	if got, want := catalogEntryIDs(entries), []string{
		"collector-b",
		"collector-c",
	}; !slices.Equal(got, want) {
		t.Fatalf("index-filtered IDs = %v, want %v", got, want)
	}

	entries = listAllCatalogTestPages(
		t,
		catalog,
		scope,
		nil,
		ListRequest{
			PageSize:        1,
			IndexNameFilter: &indexName,
			TextFilter:      &text,
		},
	)
	if got, want := catalogEntryIDs(entries), []string{
		"collector-b",
		"collector-c",
	}; !slices.Equal(got, want) {
		t.Fatalf("combined-filtered IDs = %v, want %v", got, want)
	}
}

func TestCatalogListExactTotalIsOptIn(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	for index := range 3 {
		claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			fmt.Sprintf("collector-%c", 'a'+index),
			"host",
			nil,
			base.Add(time.Duration(index)*time.Minute),
			uint64(index),
			"main",
		)
	}
	withoutTotal, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{PageSize: 2},
	)
	if err != nil {
		t.Fatalf("List(without total): %v", err)
	}
	if withoutTotal.TotalSize != nil || withoutTotal.TotalSizeExact {
		t.Fatalf("unrequested total = %v/%t", withoutTotal.TotalSize, withoutTotal.TotalSizeExact)
	}

	withTotal, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{PageSize: 2, IncludeTotal: true},
	)
	if err != nil {
		t.Fatalf("List(with total): %v", err)
	}
	if withTotal.TotalSize == nil ||
		*withTotal.TotalSize != 3 ||
		!withTotal.TotalSizeExact {
		t.Fatalf(
			"requested total = %v/%t, want 3/true",
			withTotal.TotalSize,
			withTotal.TotalSizeExact,
		)
	}
}

func TestCatalogListUsesConstantSizeRevisionIntegrityOnEveryPage(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 17, 30, 0, 0, time.UTC)
	for index := range 3 {
		claimCatalogTestCollector(
			t,
			store,
			scope.TenantID,
			fmt.Sprintf("collector-%c", 'a'+index),
			"host",
			nil,
			base.Add(time.Duration(index)*time.Minute),
			uint64(index),
			"main",
		)
	}

	var statementCount atomic.Int64
	var exactCount atomic.Int64
	const queryCallbackName = "collectorfleet:test-catalog-page-query-count"
	if err := catalog.orm.Callback().Query().After("gorm:query").Register(
		queryCallbackName,
		func(database *gorm.DB) {
			statementCount.Add(1)
			if strings.Contains(
				strings.ToLower(database.Statement.SQL.String()),
				"count(*)",
			) {
				exactCount.Add(1)
			}
		},
	); err != nil {
		t.Fatalf("register catalog query counter: %v", err)
	}
	t.Cleanup(func() {
		if err := catalog.orm.Callback().Query().Remove(
			queryCallbackName,
		); err != nil {
			t.Errorf("remove catalog query counter: %v", err)
		}
	})

	first, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{PageSize: 1, IncludeTotal: true},
	)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if first.NextPageToken == nil {
		t.Fatal("List(first) did not return a continuation")
	}
	if first.TotalSize == nil ||
		*first.TotalSize != 3 ||
		!first.TotalSizeExact {
		t.Fatalf(
			"first-page total = %v/%t, want 3/true",
			first.TotalSize,
			first.TotalSizeExact,
		)
	}
	if got := statementCount.Load(); got != 9 {
		t.Fatalf(
			"first-page read statements = %d, want 9 (revision with integrity counts, page, six hydration, count)",
			got,
		)
	}
	if got := exactCount.Load(); got != 1 {
		t.Fatalf("first-page exact-count statements = %d, want 1", got)
	}

	statementCount.Store(0)
	exactCount.Store(0)
	second, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{
			PageSize:     1,
			PageToken:    *first.NextPageToken,
			IncludeTotal: true,
		},
	)
	if err != nil {
		t.Fatalf("List(continuation): %v", err)
	}
	if second.NextPageToken == nil {
		t.Fatal("List(continuation) did not carry a next continuation")
	}
	if second.TotalSize == nil ||
		*second.TotalSize != 3 ||
		!second.TotalSizeExact {
		t.Fatalf(
			"continuation total = %v/%t, want 3/true",
			second.TotalSize,
			second.TotalSizeExact,
		)
	}
	if got := statementCount.Load(); got != 8 {
		t.Fatalf(
			"continuation read statements = %d, want 8 (revision, page, six hydration)",
			got,
		)
	}
	if got := exactCount.Load(); got != 0 {
		t.Fatalf("continuation exact-count statements = %d, want 0", got)
	}
}

func TestCatalogForcedMarkerLossCannotRecreateRevisionOrAcceptCursor(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	ctx := context.Background()
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 17, 45, 0, 0, time.UTC)
	for index := range 2 {
		claim := catalogClaimForTest(
			scope.TenantID,
			fmt.Sprintf("collector-%c", 'a'+index),
			"host",
			[]string{"main"},
			base.Add(time.Duration(index)*time.Minute),
		)
		if _, _, err := store.Claim(ctx, claim); err != nil {
			t.Fatalf("Claim(%s): %v", claim.CollectorID, err)
		}
	}
	if revision := readCatalogRevisionForTest(
		t,
		database,
		scope.TenantID,
	); revision != 4 {
		t.Fatalf("catalog revision before marker loss = %d, want 4", revision)
	}

	request := ListRequest{PageSize: 1}
	first, err := catalog.List(ctx, scope, nil, request)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if first.NextPageToken == nil {
		t.Fatal("List(first) did not return a valid continuation")
	}
	request.PageToken = *first.NextPageToken

	forceCatalogRevisionMarkerLossForTest(
		t,
		database.SQLDB(),
		scope.TenantID,
	)
	claim := catalogClaimForTest(
		scope.TenantID,
		"collector-c",
		"new.example",
		[]string{"main"},
		base.Add(10*time.Minute),
	)
	if _, _, claimErr := store.Claim(ctx, claim); claimErr == nil ||
		!strings.Contains(
			claimErr.Error(),
			"fleet/runtime revision is missing",
		) {
		t.Fatalf("Claim(C after forced marker loss) error = %v", claimErr)
	}
	assertCollectorParentCounts(
		t,
		database.SQLDB(),
		scope.TenantID,
		2,
		2,
	)

	var markerCount int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT count(*)
		FROM collector_catalog_revisions
		WHERE tenant_id = ?`,
		scope.TenantID,
	).Scan(&markerCount); err != nil {
		t.Fatalf("count revision markers: %v", err)
	}
	if markerCount != 0 {
		t.Fatalf("revision marker count = %d, want 0", markerCount)
	}

	if _, err := catalog.List(
		ctx,
		scope,
		nil,
		request,
	); err == nil ||
		!strings.Contains(err.Error(), "fleet/runtime") {
		t.Fatalf(
			"List(old cursor after forced marker loss) error = %v, want fail-closed fleet/runtime error",
			err,
		)
	}
}

func TestCatalogListCursorRejectsTamperingAndRequestReplay(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	base := time.Date(2026, 7, 28, 18, 0, 0, 0, time.UTC)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		for index := range 3 {
			claimCatalogTestCollector(
				t,
				store,
				tenantID,
				fmt.Sprintf("collector-%c", 'a'+index),
				"host",
				nil,
				base.Add(time.Duration(index)*time.Minute),
				uint64(index),
				"main",
			)
		}
	}
	scope := Scope{TenantID: "tenant-a"}
	request := ListRequest{PageSize: 1}
	first, err := catalog.List(context.Background(), scope, nil, request)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if first.NextPageToken == nil {
		t.Fatal("List(first) did not return a continuation token")
	}
	token := *first.NextPageToken
	tampered := []byte(token)
	tampered[len(tampered)/2] ^= 1

	text := "collector"
	replays := []struct {
		name    string
		scope   Scope
		request ListRequest
	}{
		{
			name:  "tampered",
			scope: scope,
			request: ListRequest{
				PageSize:  1,
				PageToken: string(tampered),
			},
		},
		{
			name:  "cross tenant",
			scope: Scope{TenantID: "tenant-b"},
			request: ListRequest{
				PageSize:  1,
				PageToken: token,
			},
		},
		{
			name:  "cross filter",
			scope: scope,
			request: ListRequest{
				PageSize:   1,
				PageToken:  token,
				TextFilter: &text,
			},
		},
		{
			name:  "cross sort",
			scope: scope,
			request: ListRequest{
				PageSize:  1,
				PageToken: token,
				SortBy:    CollectorSortByHostname,
			},
		},
		{
			name:  "cross page size",
			scope: scope,
			request: ListRequest{
				PageSize:  2,
				PageToken: token,
			},
		},
		{
			name:  "cross total mode",
			scope: scope,
			request: ListRequest{
				PageSize:     1,
				PageToken:    token,
				IncludeTotal: true,
			},
		},
	}
	for _, replay := range replays {
		t.Run(replay.name, func(t *testing.T) {
			_, listErr := catalog.List(
				context.Background(),
				replay.scope,
				nil,
				replay.request,
			)
			if !errors.Is(listErr, control.ErrInvalidArgument) {
				t.Fatalf(
					"List(replayed token) error = %v, want ErrInvalidArgument",
					listErr,
				)
			}
		})
	}
}

func TestCatalogListCursorInvalidatesAfterDurableMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(
			t *testing.T,
			store *Store,
			fixtures []catalogTestCollector,
			base time.Time,
		)
	}{
		{
			name: "administration",
			mutate: func(
				t *testing.T,
				store *Store,
				fixtures []catalogTestCollector,
				base time.Time,
			) {
				t.Helper()
				displayName := "Renamed"
				if _, err := store.UpdateDisplayName(
					context.Background(),
					fixtures[0].lease.Scope,
					fixtures[0].lease.CollectorID,
					fixtures[0].collector.Version,
					&displayName,
					base.Add(20*time.Minute),
				); err != nil {
					t.Fatalf("UpdateDisplayName(): %v", err)
				}
			},
		},
		{
			name: "heartbeat",
			mutate: func(
				t *testing.T,
				store *Store,
				fixtures []catalogTestCollector,
				base time.Time,
			) {
				t.Helper()
				heartbeat := catalogHeartbeatForTest(
					fixtures[0],
					base.Add(20*time.Minute),
					2,
					9_999,
				)
				if applied, err := store.RecordHeartbeat(
					context.Background(),
					fixtures[0].lease,
					heartbeat,
				); err != nil || !applied {
					t.Fatalf("RecordHeartbeat() = %t, %v", applied, err)
				}
			},
		},
		{
			name: "replacement claim",
			mutate: func(
				t *testing.T,
				store *Store,
				fixtures []catalogTestCollector,
				base time.Time,
			) {
				t.Helper()
				request := catalogClaimForTest(
					fixtures[0].lease.TenantID,
					fixtures[0].lease.CollectorID,
					"replacement.example",
					[]string{"main"},
					base.Add(20*time.Minute),
				)
				request.StreamID = "replacement-stream"
				if _, _, err := store.Claim(
					context.Background(),
					request,
				); err != nil {
					t.Fatalf("Claim(replacement): %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			database, store := openTestStore(t)
			catalog := newCatalogForTest(t, database)
			scope := Scope{TenantID: "tenant-a"}
			base := time.Date(2026, 7, 28, 19, 0, 0, 0, time.UTC)
			fixtures := make([]catalogTestCollector, 3)
			for index := range fixtures {
				fixtures[index] = claimCatalogTestCollector(
					t,
					store,
					scope.TenantID,
					fmt.Sprintf("collector-%c", 'a'+index),
					"host",
					nil,
					base.Add(time.Duration(index)*time.Minute),
					uint64(index),
					"main",
				)
			}
			first, err := catalog.List(
				context.Background(),
				scope,
				nil,
				ListRequest{PageSize: 1},
			)
			if err != nil {
				t.Fatalf("List(first): %v", err)
			}
			if first.NextPageToken == nil {
				t.Fatal("List(first) did not return a continuation token")
			}
			before := first.CatalogRevision
			test.mutate(t, store, fixtures, base)
			after := readCatalogRevisionForTest(t, database, scope.TenantID)
			if after <= before {
				t.Fatalf(
					"catalog revision after mutation = %d, want > %d",
					after,
					before,
				)
			}
			if _, err := catalog.List(
				context.Background(),
				scope,
				nil,
				ListRequest{
					PageSize:  1,
					PageToken: *first.NextPageToken,
				},
			); !errors.Is(err, control.ErrPageInvalidated) {
				t.Fatalf(
					"List(after %s) error = %v, want ErrPageInvalidated",
					test.name,
					err,
				)
			}
		})
	}
}

func TestCatalogListCursorInvalidatesAfterLivenessTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(CollectorLiveness) []CollectorLiveness
	}{
		{
			name: "online to stale",
			change: func(live CollectorLiveness) []CollectorLiveness {
				live.State = LivenessStateStale
				return []CollectorLiveness{live}
			},
		},
		{
			name: "release",
			change: func(CollectorLiveness) []CollectorLiveness {
				return nil
			},
		},
		{
			name: "generation",
			change: func(live CollectorLiveness) []CollectorLiveness {
				live.Lease.Generation++
				return []CollectorLiveness{live}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			database, store := openTestStore(t)
			catalog := newCatalogForTest(t, database)
			scope := Scope{TenantID: "tenant-a"}
			base := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
			fixtures := make([]catalogTestCollector, 3)
			for index := range fixtures {
				fixtures[index] = claimCatalogTestCollector(
					t,
					store,
					scope.TenantID,
					fmt.Sprintf("collector-%c", 'a'+index),
					"host",
					nil,
					base.Add(time.Duration(index)*time.Minute),
					uint64(index),
					"main",
				)
			}
			live := CollectorLiveness{
				Lease: fixtures[0].lease,
				State: LivenessStateOnline,
			}
			first, err := catalog.List(
				context.Background(),
				scope,
				[]CollectorLiveness{live},
				ListRequest{PageSize: 1},
			)
			if err != nil {
				t.Fatalf("List(first): %v", err)
			}
			if first.NextPageToken == nil {
				t.Fatal("List(first) did not return a continuation token")
			}
			if got := readCatalogRevisionForTest(
				t,
				database,
				scope.TenantID,
			); got != first.CatalogRevision {
				t.Fatalf(
					"durable revision changed without mutation: got %d, want %d",
					got,
					first.CatalogRevision,
				)
			}
			if _, err := catalog.List(
				context.Background(),
				scope,
				test.change(live),
				ListRequest{
					PageSize:  1,
					PageToken: *first.NextPageToken,
				},
			); !errors.Is(err, control.ErrPageInvalidated) {
				t.Fatalf(
					"List(after %s) error = %v, want ErrPageInvalidated",
					test.name,
					err,
				)
			}
		})
	}
}

func TestCatalogHydratesCompleteDetachedChildren(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	fixture := claimCatalogTestCollector(
		t,
		store,
		scope.TenantID,
		"collector-children",
		"children.example",
		stringPointerForCatalogTest("Child Snapshot"),
		time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC),
		1234,
		"audit",
		"main",
	)
	want, err := store.Get(
		context.Background(),
		scope,
		fixture.lease.CollectorID,
	)
	if err != nil {
		t.Fatalf("Store.Get(): %v", err)
	}
	liveness := []CollectorLiveness{{
		Lease: fixture.lease,
		State: LivenessStateOnline,
	}}
	got, err := catalog.Get(
		context.Background(),
		scope,
		fixture.lease.CollectorID,
		liveness,
	)
	if err != nil {
		t.Fatalf("Catalog.Get(): %v", err)
	}
	if got.ConnectionState != ConnectionStateOnline ||
		!reflect.DeepEqual(got.Collector, want) {
		t.Fatalf("Catalog.Get() = %#v, want collector %#v/online", got, want)
	}

	page, err := catalog.List(
		context.Background(),
		scope,
		liveness,
		ListRequest{},
	)
	if err != nil {
		t.Fatalf("Catalog.List(): %v", err)
	}
	if len(page.Entries) != 1 ||
		page.Entries[0].ConnectionState != ConnectionStateOnline ||
		!reflect.DeepEqual(page.Entries[0].Collector, want) {
		t.Fatalf("Catalog.List() entries = %#v, want complete collector", page.Entries)
	}

	// Mutating one returned projection must not mutate a later read or any
	// caller-owned liveness input.
	*got.Collector.DisplayName = "mutated"
	got.Collector.Capabilities[0] = 999
	got.Collector.AuthorizedIndexes[0] = "mutated"
	got.Collector.Inputs[0].IndexName = "mutated"
	*got.Collector.Inputs[0].Source = "mutated"
	got.Collector.InputHealth[0].StatusMessage = "mutated"
	got.Collector.ActiveLease.StreamID = "mutated"
	liveness[0].Lease.StreamID = "caller-mutated"
	fresh, err := catalog.Get(
		context.Background(),
		scope,
		fixture.lease.CollectorID,
		[]CollectorLiveness{{
			Lease: fixture.lease,
			State: LivenessStateOnline,
		}},
	)
	if err != nil {
		t.Fatalf("Catalog.Get(fresh): %v", err)
	}
	if fresh.ConnectionState != ConnectionStateOnline ||
		!reflect.DeepEqual(fresh.Collector, want) {
		t.Fatalf("fresh detached collector = %#v, want %#v", fresh, want)
	}
}

func TestCatalogRejectsInvalidScopeAndCanceledContext(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	for _, operation := range []struct {
		name string
		call func(context.Context, Scope) error
	}{
		{
			name: "get",
			call: func(ctx context.Context, scope Scope) error {
				_, err := catalog.Get(ctx, scope, "collector-a", nil)
				return err
			},
		},
		{
			name: "list",
			call: func(ctx context.Context, scope Scope) error {
				_, err := catalog.List(ctx, scope, nil, ListRequest{})
				return err
			},
		},
	} {
		t.Run(operation.name+" invalid scope", func(t *testing.T) {
			if err := operation.call(
				context.Background(),
				Scope{},
			); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("operation error = %v, want ErrInvalidArgument", err)
			}
		})
		t.Run(operation.name+" canceled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := operation.call(
				ctx,
				Scope{TenantID: "tenant-a"},
			); !errors.Is(err, context.Canceled) {
				t.Fatalf("operation error = %v, want context.Canceled", err)
			}
		})
	}

	empty, err := catalog.List(
		context.Background(),
		Scope{TenantID: "empty-tenant"},
		nil,
		ListRequest{IncludeTotal: true},
	)
	if err != nil {
		t.Fatalf("List(empty tenant): %v", err)
	}
	if len(empty.Entries) != 0 ||
		empty.NextPageToken != nil ||
		empty.CatalogRevision != 0 ||
		empty.TotalSize == nil ||
		*empty.TotalSize != 0 ||
		!empty.TotalSizeExact {
		t.Fatalf("empty tenant result = %#v", empty)
	}
}

func TestCatalogFailsClosedForMissingOrCorruptFleetRuntimeState(t *testing.T) {
	t.Parallel()

	t.Run("missing runtime", func(t *testing.T) {
		database, store := openTestStore(t)
		catalog := newCatalogForTest(t, database)
		fixture := claimCatalogTestCollector(
			t,
			store,
			"tenant-a",
			"collector-missing-runtime",
			"missing.example",
			nil,
			time.Date(2026, 7, 28, 22, 0, 0, 0, time.UTC),
			1,
			"main",
		)
		if _, err := database.SQLDB().ExecContext(
			context.Background(),
			`DELETE FROM collector_runtime
			 WHERE tenant_id = ? AND collector_id = ?`,
			fixture.lease.TenantID,
			fixture.lease.CollectorID,
		); err != nil {
			t.Fatalf("delete runtime fixture: %v", err)
		}
		assertCatalogReadFailsClosed(
			t,
			catalog,
			fixture.lease.Scope,
			fixture.lease.CollectorID,
			"runtime",
		)
	})

	t.Run("corrupt runtime", func(t *testing.T) {
		database, store := openTestStore(t)
		catalog := newCatalogForTest(t, database)
		fixture := claimCatalogTestCollector(
			t,
			store,
			"tenant-a",
			"collector-corrupt-runtime",
			"corrupt.example",
			nil,
			time.Date(2026, 7, 28, 22, 30, 0, 0, time.UTC),
			1,
			"main",
		)
		installCatalogRuntimeCorruption(t, database, fixture.lease, `
			UPDATE collector_runtime
			SET queued_bytes = -1
			WHERE tenant_id = ? AND collector_id = ?`)
		assertCatalogReadFailsClosed(
			t,
			catalog,
			fixture.lease.Scope,
			fixture.lease.CollectorID,
			"negative collector telemetry",
		)
	})

	t.Run("missing fleet runtime revision", func(t *testing.T) {
		database, store := openTestStore(t)
		catalog := newCatalogForTest(t, database)
		fixture := claimCatalogTestCollector(
			t,
			store,
			"tenant-a",
			"collector-missing-revision",
			"missing-revision.example",
			nil,
			time.Date(2026, 7, 28, 22, 40, 0, 0, time.UTC),
			1,
			"main",
		)
		forceCatalogRevisionMarkerLossForTest(
			t,
			database.SQLDB(),
			fixture.lease.TenantID,
		)
		assertCatalogListFailsClosed(
			t,
			catalog,
			fixture.lease.Scope,
			"fleet/runtime",
		)
	})

	for _, test := range []struct {
		name      string
		statement string
	}{
		{
			name: "invalid revision",
			statement: `
				UPDATE collector_catalog_revisions
				SET revision = 0
				WHERE tenant_id = ?`,
		},
		{
			name: "negative fleet runtime counts",
			statement: `
				UPDATE collector_catalog_revisions
				SET fleet_count = -1, runtime_count = -1
				WHERE tenant_id = ?`,
		},
		{
			name: "inconsistent fleet runtime counts",
			statement: `
				UPDATE collector_catalog_revisions
				SET fleet_count = fleet_count + 1
				WHERE tenant_id = ?`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, store := openTestStore(t)
			catalog := newCatalogForTest(t, database)
			fixture := claimCatalogTestCollector(
				t,
				store,
				"tenant-a",
				"collector-corrupt-revision",
				"corrupt-revision.example",
				nil,
				time.Date(2026, 7, 28, 22, 50, 0, 0, time.UTC),
				1,
				"main",
			)
			installCatalogRevisionCorruption(
				t,
				database,
				fixture.lease.TenantID,
				test.statement,
			)
			assertCatalogListFailsClosed(
				t,
				catalog,
				fixture.lease.Scope,
				"fleet/runtime",
			)
		})
	}
}

func TestCatalogListReadsCoherentHeartbeatSnapshotsUnderWAL(t *testing.T) {
	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	scope := Scope{TenantID: "tenant-a"}
	base := time.Date(2026, 7, 28, 23, 0, 0, 0, time.UTC)
	fixture := claimCatalogTestCollector(
		t,
		store,
		scope.TenantID,
		"collector-concurrent",
		"concurrent.example",
		nil,
		base,
		100,
		"main",
	)

	const finalSequence = uint64(24)
	start := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		<-start
		for sequence := uint64(2); sequence <= finalSequence; sequence++ {
			heartbeat := catalogHeartbeatForTest(
				fixture,
				base.Add(time.Duration(sequence)*time.Second),
				sequence,
				sequence*100,
			)
			applied, err := store.RecordHeartbeat(
				context.Background(),
				fixture.lease,
				heartbeat,
			)
			if err != nil {
				writerDone <- err
				return
			}
			if !applied {
				writerDone <- fmt.Errorf(
					"heartbeat sequence %d was not applied",
					sequence,
				)
				return
			}
		}
		writerDone <- nil
	}()
	close(start)

	for iteration := 0; iteration < int(finalSequence); iteration++ {
		page, err := catalog.List(
			context.Background(),
			scope,
			nil,
			ListRequest{},
		)
		if err != nil {
			t.Fatalf("List(iteration %d): %v", iteration, err)
		}
		if len(page.Entries) != 1 {
			t.Fatalf("List(iteration %d) entries = %d, want 1", iteration, len(page.Entries))
		}
		collector := page.Entries[0].Collector
		sequence := collector.ObservationSequence
		if sequence == 0 {
			if collector.Queue.QueuedBytes != 0 || len(collector.InputHealth) != 0 {
				t.Fatalf("partial claim snapshot = %#v", collector)
			}
			continue
		}
		if collector.Queue.QueuedBytes != sequence*100 ||
			len(collector.InputHealth) != len(fixture.inputIDs) {
			t.Fatalf(
				"partial heartbeat snapshot at sequence %d: queue=%d health=%#v",
				sequence,
				collector.Queue.QueuedBytes,
				collector.InputHealth,
			)
		}
		wantStatus := fmt.Sprintf("sequence-%d", sequence)
		for _, health := range collector.InputHealth {
			if health.StatusMessage != wantStatus ||
				health.EventsReadTotal != sequence*1_000 {
				t.Fatalf(
					"partial health snapshot at sequence %d: %#v",
					sequence,
					health,
				)
			}
		}
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("concurrent heartbeat writer: %v", err)
	}
}

func newCatalogForTest(t *testing.T, database *control.DB) *Catalog {
	t.Helper()
	catalog, err := NewCatalog(database, CatalogOptions{
		CursorKey: catalogTestCursorKey,
	})
	if err != nil {
		t.Fatalf("NewCatalog(): %v", err)
	}
	return catalog
}

func claimCatalogTestCollector(
	t *testing.T,
	store *Store,
	tenantID string,
	collectorID string,
	hostname string,
	displayName *string,
	lastSeenAt time.Time,
	queueBytes uint64,
	indexes ...string,
) catalogTestCollector {
	t.Helper()
	request := catalogClaimForTest(
		tenantID,
		collectorID,
		hostname,
		indexes,
		lastSeenAt.Add(-time.Second),
	)
	collector, lease, err := store.Claim(context.Background(), request)
	if err != nil {
		t.Fatalf("Claim(%s/%s): %v", tenantID, collectorID, err)
	}
	fixture := catalogTestCollector{
		collector: collector,
		lease:     lease,
		inputIDs:  make([]string, len(request.Hello.Inputs)),
	}
	for index, input := range request.Hello.Inputs {
		fixture.inputIDs[index] = input.InputID
	}
	heartbeat := catalogHeartbeatForTest(
		fixture,
		lastSeenAt,
		1,
		queueBytes,
	)
	if applied, heartbeatErr := store.RecordHeartbeat(
		context.Background(),
		lease,
		heartbeat,
	); heartbeatErr != nil || !applied {
		t.Fatalf(
			"RecordHeartbeat(%s/%s) = %t, %v",
			tenantID,
			collectorID,
			applied,
			heartbeatErr,
		)
	}
	if displayName != nil {
		if _, err := store.UpdateDisplayName(
			context.Background(),
			lease.Scope,
			lease.CollectorID,
			collector.Version,
			displayName,
			lastSeenAt.Add(time.Second),
		); err != nil {
			t.Fatalf("UpdateDisplayName(%s/%s): %v", tenantID, collectorID, err)
		}
	}
	fixture.collector, err = store.Get(
		context.Background(),
		lease.Scope,
		lease.CollectorID,
	)
	if err != nil {
		t.Fatalf("Get(%s/%s): %v", tenantID, collectorID, err)
	}
	return fixture
}

func catalogClaimForTest(
	tenantID string,
	collectorID string,
	hostname string,
	indexes []string,
	receivedAt time.Time,
) ClaimRequest {
	if len(indexes) == 0 {
		indexes = []string{"main"}
	}
	request := testClaim(receivedAt)
	request.Scope = Scope{TenantID: tenantID}
	request.CollectorID = collectorID
	request.BootEpoch = "boot-" + collectorID
	request.StreamID = "stream-" + collectorID
	request.Hello.InstanceID = "instance-" + collectorID
	request.Hello.Hostname = hostname
	request.Hello.Capabilities = []uint32{8, 2, 8}
	request.Hello.AuthorizedIndexes = slices.Clone(indexes)
	request.Hello.Inputs = make([]InputRegistration, len(indexes))
	for index, indexName := range indexes {
		source := fmt.Sprintf("/var/log/%s/%s.log", collectorID, indexName)
		sourcetype := "catalog:test"
		request.Hello.Inputs[index] = InputRegistration{
			InputID:    fmt.Sprintf("input-%d", index),
			InputType:  uint32(index + 1),
			IndexName:  indexName,
			Source:     &source,
			Sourcetype: &sourcetype,
		}
	}
	return request
}

func catalogHeartbeatForTest(
	fixture catalogTestCollector,
	receivedAt time.Time,
	sequence uint64,
	queueBytes uint64,
) Heartbeat {
	heartbeat := testHeartbeat(receivedAt, sequence)
	heartbeat.Queue.QueuedBytes = queueBytes
	heartbeat.Queue.QueuedEvents = sequence
	heartbeat.Inputs = make([]InputHealth, len(fixture.inputIDs))
	status := fmt.Sprintf("sequence-%d", sequence)
	for index, inputID := range fixture.inputIDs {
		lastEventAt := receivedAt.Add(-time.Duration(index+1) * time.Second)
		heartbeat.Inputs[index] = InputHealth{
			InputID:           inputID,
			State:             uint32(index + 1),
			StatusMessage:     status,
			DiscoveredSources: sequence + uint64(index) + 1,
			ActiveSources:     sequence,
			EventsReadTotal:   sequence * 1_000,
			BytesReadTotal:    sequence * 10_000,
			LastEventAt:       &lastEventAt,
		}
	}
	return heartbeat
}

func listAllCatalogTestPages(
	t *testing.T,
	catalog *Catalog,
	scope Scope,
	liveness []CollectorLiveness,
	request ListRequest,
) []CatalogEntry {
	t.Helper()
	var result []CatalogEntry
	var revision uint64
	seenTokens := make(map[string]struct{})
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		page, err := catalog.List(
			context.Background(),
			scope,
			liveness,
			request,
		)
		if err != nil {
			t.Fatalf("List(page %d): %v", pageNumber+1, err)
		}
		if pageNumber == 0 {
			revision = page.CatalogRevision
		} else if page.CatalogRevision != revision {
			t.Fatalf(
				"page %d revision = %d, want %d",
				pageNumber+1,
				page.CatalogRevision,
				revision,
			)
		}
		if request.PageSize > 0 &&
			len(page.Entries) > int(request.PageSize) {
			t.Fatalf(
				"page %d contains %d entries, exceeds requested %d",
				pageNumber+1,
				len(page.Entries),
				request.PageSize,
			)
		}
		result = append(result, page.Entries...)
		if page.NextPageToken == nil {
			return result
		}
		if _, duplicate := seenTokens[*page.NextPageToken]; duplicate {
			t.Fatalf("List repeated continuation token on page %d", pageNumber+1)
		}
		seenTokens[*page.NextPageToken] = struct{}{}
		request.PageToken = *page.NextPageToken
	}
	t.Fatal("List did not terminate within 100 pages")
	return nil
}

func catalogEntryIDs(entries []CatalogEntry) []string {
	result := make([]string, len(entries))
	for index, entry := range entries {
		result[index] = entry.Collector.CollectorID
	}
	return result
}

func readCatalogRevisionForTest(
	t *testing.T,
	database *control.DB,
	tenantID string,
) uint64 {
	t.Helper()
	var revision int64
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT revision
		 FROM collector_catalog_revisions
		 WHERE tenant_id = ?`,
		tenantID,
	).Scan(&revision); err != nil {
		t.Fatalf("read catalog revision for %s: %v", tenantID, err)
	}
	if revision < 1 {
		t.Fatalf("catalog revision for %s = %d, want positive", tenantID, revision)
	}
	return uint64(revision)
}

func assertCatalogReadFailsClosed(
	t *testing.T,
	catalog *Catalog,
	scope Scope,
	collectorID string,
	errorFragment string,
) {
	t.Helper()
	if _, err := catalog.Get(
		context.Background(),
		scope,
		collectorID,
		nil,
	); err == nil ||
		errors.Is(err, control.ErrNotFound) ||
		!strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorFragment)) {
		t.Fatalf(
			"Get(corrupt %s) error = %v, want fail-closed corruption error",
			errorFragment,
			err,
		)
	}
	if _, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{},
	); err == nil ||
		errors.Is(err, control.ErrNotFound) ||
		!strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorFragment)) {
		t.Fatalf(
			"List(corrupt %s) error = %v, want fail-closed corruption error",
			errorFragment,
			err,
		)
	}
}

func assertCatalogListFailsClosed(
	t *testing.T,
	catalog *Catalog,
	scope Scope,
	errorFragment string,
) {
	t.Helper()
	if _, err := catalog.List(
		context.Background(),
		scope,
		nil,
		ListRequest{},
	); err == nil ||
		errors.Is(err, control.ErrNotFound) ||
		!strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorFragment)) {
		t.Fatalf(
			"List(corrupt %s) error = %v, want fail-closed corruption error",
			errorFragment,
			err,
		)
	}
}

func installCatalogRevisionCorruption(
	t *testing.T,
	database *control.DB,
	tenantID string,
	statement string,
) {
	t.Helper()
	ctx := context.Background()
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire revision corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = ON`,
	); err != nil {
		t.Fatalf("disable revision check constraints: %v", err)
	}
	defer func() {
		_, _ = connection.ExecContext(
			context.Background(),
			`PRAGMA ignore_check_constraints = OFF`,
		)
	}()
	result, err := connection.ExecContext(
		ctx,
		statement,
		tenantID,
	)
	if err != nil {
		t.Fatalf("install revision corruption: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("read revision corruption rows affected: %v", err)
	}
	if rows != 1 {
		t.Fatalf("revision corruption rows affected = %d, want 1", rows)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = OFF`,
	); err != nil {
		t.Fatalf("restore revision check constraints: %v", err)
	}
}

func installCatalogRuntimeCorruption(
	t *testing.T,
	database *control.DB,
	lease Lease,
	statement string,
) {
	t.Helper()
	ctx := context.Background()
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = ON`,
	); err != nil {
		t.Fatalf("disable check constraints: %v", err)
	}
	defer func() {
		_, _ = connection.ExecContext(
			context.Background(),
			`PRAGMA ignore_check_constraints = OFF`,
		)
	}()
	result, err := connection.ExecContext(
		ctx,
		statement,
		lease.TenantID,
		lease.CollectorID,
	)
	if err != nil {
		t.Fatalf("install runtime corruption: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("read corruption rows affected: %v", err)
	}
	if rows != 1 {
		t.Fatalf("corruption rows affected = %d, want 1", rows)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA ignore_check_constraints = OFF`,
	); err != nil {
		t.Fatalf("restore check constraints: %v", err)
	}
}

func stringPointerForCatalogTest(value string) *string {
	return &value
}
