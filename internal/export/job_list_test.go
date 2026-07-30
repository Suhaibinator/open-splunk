package export

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestManagerListOrdersScopesFiltersTotalsAndDetaches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return now }
	})
	otherAccess := searchjobs.AccessScope{TenantID: testAccess.TenantID, OwnerID: "other-owner"}
	otherTenant := searchjobs.AccessScope{TenantID: "other-tenant", OwnerID: testAccess.OwnerID}
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "export-a", Version: 1, SearchJobID: "search-1", State: StateQueued,
		Columns: []string{"message"}, CreatedAt: now,
	})
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "export-b", Version: 2, SearchJobID: "search-1", State: StateCompleted,
		Columns: []string{"message"}, CreatedAt: now,
		Artifact: &Artifact{FileName: "export-b.csv", MediaType: "text/csv", SizeBytes: 11, RowCount: 1},
	})
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "export-c", Version: 3, SearchJobID: "search-2", State: StateFailed,
		Columns: []string{"count"}, CreatedAt: now.Add(time.Second),
		Failure: &Failure{Code: FailureInternal, Message: "safe failure"},
	})
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "export-d", Version: 4, SearchJobID: "search-1", State: StateCompleted,
		Columns: []string{"host"}, CreatedAt: now.Add(2 * time.Second),
	})
	installExportListTestEntry(t, manager, otherAccess, Job{
		ID: "export-z", Version: 1, SearchJobID: "search-1", State: StateCompleted,
		CreatedAt: now.Add(3 * time.Second),
	})
	installExportListTestEntry(t, manager, otherTenant, Job{
		ID: "export-y", Version: 1, SearchJobID: "search-1", State: StateCompleted,
		CreatedAt: now.Add(4 * time.Second),
	})

	first, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize:     2,
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(first.Jobs); !reflect.DeepEqual(got, []string{"export-d", "export-c"}) {
		t.Fatalf("first page IDs = %v", got)
	}
	if first.TotalSize == nil || *first.TotalSize != 4 || !first.TotalSizeExact || first.NextPageToken == "" {
		t.Fatalf("first page metadata = %#v", first)
	}
	for _, item := range first.Jobs {
		if item.TenantID != testAccess.TenantID ||
			item.OwnerID != testAccess.OwnerID {
			t.Fatalf("first page leaked or omitted scope metadata: %#v", item)
		}
	}

	second, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize:  3,
		PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(second.Jobs); !reflect.DeepEqual(got, []string{"export-b", "export-a"}) {
		t.Fatalf("second page IDs = %v", got)
	}
	if second.NextPageToken != "" || second.TotalSize != nil || second.TotalSizeExact {
		t.Fatalf("second page metadata = %#v", second)
	}

	searchID := "search-1"
	filtered, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize:          MaximumListPageSize,
		IncludeTotal:      true,
		StateFilters:      []State{StateCompleted, StateCompleted},
		SearchJobIDFilter: &searchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(filtered.Jobs); !reflect.DeepEqual(got, []string{"export-d", "export-b"}) {
		t.Fatalf("filtered IDs = %v", got)
	}
	if filtered.TotalSize == nil || *filtered.TotalSize != 2 || !filtered.TotalSizeExact {
		t.Fatalf("filtered total = %#v", filtered)
	}

	filtered.Jobs[1].Columns[0] = "mutated"
	filtered.Jobs[1].Artifact.FileName = "mutated"
	refetched, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateCompleted}, SearchJobIDFilter: &searchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refetched.Jobs[1].Columns[0] != "message" ||
		refetched.Jobs[1].Artifact.FileName != "export-b.csv" {
		t.Fatalf("list result aliases retained state: %#v", refetched.Jobs[1])
	}
}

func TestManagerListCursorBindsScopeFiltersEpochPurposeAndHighWater(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 30, 13, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return base }
	})
	for index := range 3 {
		installExportListTestEntry(t, manager, testAccess, Job{
			ID: fmt.Sprintf("original-%d", index+1), Version: 1,
			SearchJobID: "search", State: []State{StateCompleted, StateFailed, StateCompleted}[index],
			CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
	}
	searchID := "search"
	states := []State{StateCompleted, StateFailed, StateCompleted}
	first, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize: 1, StateFilters: states, SearchJobIDFilter: &searchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(first.Jobs); !reflect.DeepEqual(got, []string{"original-3"}) ||
		first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	// Both a clock-forward and clock-reversed admission happen after the first
	// page. The cursor's immutable admission high-water excludes both.
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "later-newest", Version: 1, SearchJobID: "search", State: StateCompleted,
		CreatedAt: base.Add(time.Hour),
	})
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "later-oldest", Version: 1, SearchJobID: "search", State: StateCompleted,
		CreatedAt: base.Add(-time.Hour),
	})

	reorderedStates := []State{StateFailed, StateCompleted}
	second, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize:          MaximumListPageSize,
		PageToken:         first.NextPageToken,
		IncludeTotal:      true,
		StateFilters:      reorderedStates,
		SearchJobIDFilter: &searchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(second.Jobs); !reflect.DeepEqual(got, []string{"original-2", "original-1"}) {
		t.Fatalf("continuation IDs = %v", got)
	}
	if second.TotalSize == nil || *second.TotalSize != 3 || !second.TotalSizeExact {
		t.Fatalf("continuation total = %#v", second)
	}

	changedSearch := "other-search"
	replays := []struct {
		name    string
		access  searchjobs.AccessScope
		request ListRequest
	}{
		{"tenant", searchjobs.AccessScope{TenantID: "other", OwnerID: testAccess.OwnerID}, ListRequest{StateFilters: reorderedStates, SearchJobIDFilter: &searchID}},
		{"owner", searchjobs.AccessScope{TenantID: testAccess.TenantID, OwnerID: "other"}, ListRequest{StateFilters: reorderedStates, SearchJobIDFilter: &searchID}},
		{"state", testAccess, ListRequest{StateFilters: []State{StateQueued}, SearchJobIDFilter: &searchID}},
		{"search", testAccess, ListRequest{StateFilters: reorderedStates, SearchJobIDFilter: &changedSearch}},
	}
	for _, replay := range replays {
		t.Run(replay.name, func(t *testing.T) {
			replay.request.PageToken = first.NextPageToken
			if _, listErr := manager.List(context.Background(), replay.access, replay.request); !errors.Is(listErr, ErrInvalidCursor) {
				t.Fatalf("List() error = %v, want ErrInvalidCursor", listErr)
			}
		})
	}
	tamperedByte := byte('A')
	if first.NextPageToken[len(first.NextPageToken)-1] == tamperedByte {
		tamperedByte = 'B'
	}
	tampered := first.NextPageToken[:len(first.NextPageToken)-1] + string(tamperedByte)
	if _, err := manager.List(context.Background(), testAccess, ListRequest{
		PageToken: tampered, StateFilters: reorderedStates, SearchJobIDFilter: &searchID,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("tampered cursor error = %v", err)
	}

	other := newExportTestManager(t, &exportTestSource{}, nil)
	other.cursorKey = slices.Clone(manager.cursorKey)
	if _, err := other.List(context.Background(), testAccess, ListRequest{
		PageToken: first.NextPageToken, StateFilters: reorderedStates, SearchJobIDFilter: &searchID,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cross-manager cursor error = %v", err)
	}

	filterHash, err := exportListFilterHash(testAccess, normalizedExportListRequest{
		states:      []State{StateCompleted, StateFailed},
		searchJobID: &searchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload exportListCursor
	if err := cursorcodec.Decode(
		manager.cursorKey,
		exportListCursorDomain,
		exportListCursorVersion,
		maximumExportListTokenSize,
		first.NextPageToken,
		&payload,
	); err != nil {
		t.Fatal(err)
	}
	payload.FilterHash = filterHash
	manager.mu.RLock()
	futureHighWater := manager.nextGeneration + 1
	manager.mu.RUnlock()
	futurePayload := payload
	futurePayload.HighWater = futureHighWater
	futureCursor, err := cursorcodec.Encode(
		manager.cursorKey,
		exportListCursorDomain,
		exportListCursorVersion,
		maximumExportListTokenSize,
		futurePayload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.List(context.Background(), testAccess, ListRequest{
		PageToken: futureCursor, StateFilters: reorderedStates,
		SearchJobIDFilter: &searchID,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("future-high-water cursor error = %v", err)
	}
	wrongPurpose, err := cursorcodec.Encode(
		manager.cursorKey,
		"other-export-cursor-purpose",
		exportListCursorVersion,
		maximumExportListTokenSize,
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.List(context.Background(), testAccess, ListRequest{
		PageToken: wrongPurpose, StateFilters: reorderedStates, SearchJobIDFilter: &searchID,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("wrong-purpose cursor error = %v", err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1}`))
	if _, err := manager.List(context.Background(), testAccess, ListRequest{
		PageToken: unsigned, StateFilters: reorderedStates, SearchJobIDFilter: &searchID,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("unsigned cursor error = %v", err)
	}
}

func TestManagerCreateAndListShareCanonicalIdentityBoundaries(t *testing.T) {
	t.Parallel()

	access := searchjobs.AccessScope{
		TenantID: strings.Repeat("t", maximumAccessIDBytes),
		OwnerID:  strings.Repeat("o", maximumAccessIDBytes),
	}
	searchJobID := strings.Repeat("s", maximumSearchIDBytes)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		searchJobID: {schema: basicExportSchema()},
	}}
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(
		context.Background(),
		access,
		CreateRequest{SearchJobID: searchJobID, Format: FormatCSV},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := manager.List(context.Background(), access, ListRequest{
		SearchJobIDFilter: &searchJobID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(page.Jobs); !reflect.DeepEqual(
		got,
		[]string{created.ID},
	) {
		t.Fatalf("boundary identity list = %v", got)
	}
}

func TestManagerListRemovedAnchorAndReusedIDPreserveTraversal(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 30, 14, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	for index := range 3 {
		installExportListTestEntry(t, manager, testAccess, Job{
			ID: fmt.Sprintf("export-%d", index+1), Version: 1,
			SearchJobID: "search", State: StateCompleted,
			CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
	}
	first, err := manager.List(context.Background(), testAccess, ListRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(first.Jobs); !reflect.DeepEqual(got, []string{"export-3"}) {
		t.Fatalf("first IDs = %v", got)
	}
	removeExportListTestEntry(manager, "export-3")
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "export-3", Version: 1, SearchJobID: "search", State: StateCompleted,
		CreatedAt: base.Add(-time.Hour),
	})
	second, err := manager.List(context.Background(), testAccess, ListRequest{
		PageSize: MaximumListPageSize, PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(second.Jobs); !reflect.DeepEqual(got, []string{"export-2", "export-1"}) {
		t.Fatalf("continuation IDs = %v", got)
	}
}

func TestManagerListExpiresBeforeFiltering(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 15, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return now }
	})
	expiring := installExportListTestEntry(t, manager, testAccess, Job{
		ID: "expired", Version: 1, SearchJobID: "search", State: StateCompleted,
		CreatedAt: now.Add(-time.Hour), FinishedAt: now.Add(-time.Minute), ExpiresAt: now,
		Artifact: &Artifact{FileName: "expired.csv", MediaType: "text/csv", SizeBytes: 1, RowCount: 1},
	})
	installExportListTestEntry(t, manager, testAccess, Job{
		ID: "running", Version: 1, SearchJobID: "search", State: StateRunning,
		CreatedAt: now, ExpiresAt: now,
	})

	page, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateExpired}, IncludeTotal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(page.Jobs); !reflect.DeepEqual(got, []string{"expired"}) ||
		page.TotalSize == nil || *page.TotalSize != 1 {
		t.Fatalf("expired page = %#v", page)
	}
	if page.Jobs[0].Artifact != nil || page.Jobs[0].Version != 2 {
		t.Fatalf("expired projection = %#v", page.Jobs[0])
	}
	expiring.mu.RLock()
	state, expiredAt := expiring.job.State, expiring.expiredAt
	expiring.mu.RUnlock()
	if state != StateExpired || !expiredAt.Equal(now) {
		t.Fatalf("retained expiration = %s at %s", state, expiredAt)
	}
	completed, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateCompleted},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Jobs) != 0 {
		t.Fatalf("completed filter retained due job: %#v", completed.Jobs)
	}
	repeated, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateExpired},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(repeated.Jobs) != 1 || repeated.Jobs[0].Version != 2 {
		t.Fatalf("repeated expiration changed version: %#v", repeated.Jobs)
	}
}

func TestManagerListDoesNotExpireUnacknowledgedTerminalJob(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 15, 15, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return now }
	})
	entry := installExportListTestEntry(t, manager, testAccess, Job{
		ID:          "finishing",
		Version:     3,
		SearchJobID: "search",
		State:       StateCompleted,
		CreatedAt:   now.Add(-time.Minute),
		FinishedAt:  now.Add(-time.Second),
		ExpiresAt:   now,
		Artifact: &Artifact{
			FileName:  "finishing.csv",
			MediaType: "text/csv",
			SizeBytes: 1,
			RowCount:  1,
		},
	})
	entry.mu.Lock()
	entry.workerDone = false
	entry.leaseReleased = false
	entry.mu.Unlock()

	expired, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateExpired},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expired.Jobs) != 0 {
		t.Fatalf("unacknowledged terminal job expired: %#v", expired.Jobs)
	}
	completed, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateCompleted},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Jobs) != 1 ||
		completed.Jobs[0].Version != 3 ||
		completed.Jobs[0].Artifact == nil {
		t.Fatalf("unacknowledged terminal projection = %#v", completed.Jobs)
	}
}

func TestManagerListExpiryInvalidatesGrantAndPreservesPinnedDownload(t *testing.T) {
	t.Parallel()

	clock := &exportTestClock{
		now: time.Date(2026, time.July, 30, 15, 30, 0, 0, time.UTC),
	}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {
			schema: basicExportSchema(),
			rows:   basicExportRows(),
		},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Second
		config.ExpiredRetention = time.Second
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	pinnedGrant, err := manager.CreateDownloadGrant(
		context.Background(),
		testAccess,
		completed.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	outstandingGrant, err := manager.CreateDownloadGrant(
		context.Background(),
		testAccess,
		completed.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.RedeemDownload(
		context.Background(),
		pinnedGrant.Token,
	)
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(
		manager.artifactDir,
		completed.Artifact.FileName,
	)
	originalRemove := manager.removePath
	var artifactRemovals int
	manager.removePath = func(path string) error {
		if path == artifactPath {
			artifactRemovals++
		}
		return originalRemove(path)
	}

	clock.Advance(2 * time.Second)
	page, err := manager.List(context.Background(), testAccess, ListRequest{
		StateFilters: []State{StateExpired},
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := exportListIDs(page.Jobs); !reflect.DeepEqual(got, []string{completed.ID}) ||
		page.TotalSize == nil ||
		*page.TotalSize != 1 {
		t.Fatalf("expired list page = %#v", page)
	}
	if page.Jobs[0].State != StateExpired ||
		page.Jobs[0].Version != completed.Version+1 ||
		page.Jobs[0].Artifact != nil {
		t.Fatalf("expired list projection = %#v", page.Jobs[0])
	}
	if artifactRemovals != 0 {
		t.Fatalf("List() artifact removals = %d, want 0", artifactRemovals)
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("pinned artifact after List() Stat = %v", err)
	}

	if _, err := manager.RedeemDownload(
		context.Background(),
		outstandingGrant.Token,
	); !errors.Is(err, ErrInvalidDownloadGrant) {
		t.Fatalf(
			"RedeemDownload(grant invalidated by list expiry) = %v, want ErrInvalidDownloadGrant",
			err,
		)
	}
	contents, err := io.ReadAll(lease)
	if err != nil || len(contents) == 0 {
		t.Fatalf("pinned ReadAll(after list expiry) = %q, %v", contents, err)
	}
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if artifactRemovals != 0 {
		t.Fatalf("Cleanup() removed pinned artifact %d times", artifactRemovals)
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("pinned artifact after Cleanup() Stat = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if artifactRemovals != 1 {
		t.Fatalf("final lease Close() removals = %d, want 1", artifactRemovals)
	}
	if _, err := os.Stat(artifactPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact after final lease Close() Stat = %v, want not exist", err)
	}
}

func TestManagerListValidatesContextScopeFiltersAndBounds(t *testing.T) {
	t.Parallel()

	manager := newExportTestManager(t, &exportTestSource{}, nil)
	invalidUTF8 := string([]byte{0xff})
	tooManyStates := make([]State, maximumExportListStates+1)
	for index := range tooManyStates {
		tooManyStates[index] = StateCompleted
	}
	emptySearch := ""
	tests := []struct {
		name    string
		access  searchjobs.AccessScope
		request ListRequest
		want    error
	}{
		{"negative page", testAccess, ListRequest{PageSize: -1}, ErrPageSize},
		{"large page", testAccess, ListRequest{PageSize: MaximumListPageSize + 1}, ErrPageSize},
		{"large token", testAccess, ListRequest{PageToken: strings.Repeat("x", maximumExportListTokenSize+1)}, ErrInvalidCursor},
		{"empty tenant", searchjobs.AccessScope{OwnerID: "owner"}, ListRequest{}, ErrInvalidListFilter},
		{"empty owner", searchjobs.AccessScope{TenantID: "tenant"}, ListRequest{}, ErrInvalidListFilter},
		{"spaced owner", searchjobs.AccessScope{TenantID: "tenant", OwnerID: " owner "}, ListRequest{}, ErrInvalidListFilter},
		{"control owner", searchjobs.AccessScope{TenantID: "tenant", OwnerID: "own\ner"}, ListRequest{}, ErrInvalidListFilter},
		{"invalid tenant utf8", searchjobs.AccessScope{TenantID: invalidUTF8, OwnerID: "owner"}, ListRequest{}, ErrInvalidListFilter},
		{"long owner", searchjobs.AccessScope{TenantID: "tenant", OwnerID: strings.Repeat("o", maximumAccessIDBytes+1)}, ListRequest{}, ErrInvalidListFilter},
		{"too many states", testAccess, ListRequest{StateFilters: tooManyStates}, ErrInvalidListFilter},
		{"invalid state zero", testAccess, ListRequest{StateFilters: []State{StateInvalid}}, ErrInvalidListFilter},
		{"invalid state high", testAccess, ListRequest{StateFilters: []State{StateExpired + 1}}, ErrInvalidListFilter},
		{"empty search filter", testAccess, ListRequest{SearchJobIDFilter: &emptySearch}, ErrInvalidListFilter},
		{"invalid search utf8", testAccess, ListRequest{SearchJobIDFilter: &invalidUTF8}, ErrInvalidListFilter},
		{"long search", testAccess, ListRequest{SearchJobIDFilter: stringPointer(strings.Repeat("s", maximumSearchIDBytes+1))}, ErrInvalidListFilter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := manager.List(context.Background(), test.access, test.request); !errors.Is(err, test.want) {
				t.Fatalf("List() error = %v, want %v", err, test.want)
			}
		})
	}
	//nolint:staticcheck // Explicitly verifies the public nil-context guard.
	if _, err := manager.List(nil, testAccess, ListRequest{}); err == nil {
		t.Fatal("nil context succeeded")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.List(canceled, testAccess, ListRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context error = %v", err)
	}
}

func TestManagerListIndexedTraversalSkipsUnneededScopedTail(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 30, 16, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	otherAccess := searchjobs.AccessScope{TenantID: testAccess.TenantID, OwnerID: "other-owner"}
	for index := range 4_096 {
		installExportListTestEntry(t, manager, testAccess, Job{
			ID: fmt.Sprintf("export-%04d", index), Version: 1, SearchJobID: "search",
			State: StateCompleted, CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
		installExportListTestEntry(t, manager, otherAccess, Job{
			ID: fmt.Sprintf("other-%04d", index), Version: 1, SearchJobID: "search",
			State: StateCompleted, CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
	}
	counting := &exportListCountingContext{after: 1 << 20}
	page, err := manager.List(counting, testAccess, ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != defaultExportListPageSize ||
		page.Jobs[0].ID != "export-4095" ||
		page.Jobs[len(page.Jobs)-1].ID != fmt.Sprintf("export-%04d", 4096-defaultExportListPageSize) {
		t.Fatalf("indexed first page = %v", exportListIDs(page.Jobs))
	}
	if counting.checks > MaximumListPageSize+4 {
		t.Fatalf("context checks = %d, want bounded indexed traversal", counting.checks)
	}
}

func TestManagerListSparseExactTotalDoesNotCloneRetainedColumns(t *testing.T) {
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	base := time.Date(2026, time.July, 30, 16, 30, 0, 0, time.UTC)
	columns := make([]string, 256)
	for index := range columns {
		columns[index] = fmt.Sprintf("column-%03d", index)
	}
	for index := range 512 {
		installExportListTestEntry(t, manager, testAccess, Job{
			ID:          fmt.Sprintf("export-%04d", index),
			Version:     1,
			SearchJobID: "present-search",
			State:       StateCompleted,
			Columns:     columns,
			CreatedAt:   base.Add(time.Duration(index) * time.Second),
		})
	}
	missingSearch := "missing-search"
	var page ListPage
	var listErr error
	allocations := testing.AllocsPerRun(3, func() {
		page, listErr = manager.List(context.Background(), testAccess, ListRequest{
			IncludeTotal:      true,
			SearchJobIDFilter: &missingSearch,
		})
	})
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(page.Jobs) != 0 ||
		page.TotalSize == nil ||
		*page.TotalSize != 0 ||
		!page.TotalSizeExact {
		t.Fatalf("sparse exact-total page = %#v", page)
	}
	// A clone per retained Columns slice would exceed 512 allocations. Keep a
	// generous bound for request normalization, cursor fingerprinting, the
	// shallow retained-entry snapshot, and the result itself.
	if allocations > 96 {
		t.Fatalf("List() allocations = %.0f, want no per-retained-job deep clones", allocations)
	}
}

func TestExportListIndexSurvivesConcurrentMutation(t *testing.T) {
	t.Parallel()

	manager := newExportTestManager(t, &exportTestSource{}, nil)
	base := time.Date(2026, time.July, 30, 17, 0, 0, 0, time.UTC)
	const workers = defaultMaxConcurrentExportLists
	const iterations = 300
	var wait sync.WaitGroup
	start := make(chan struct{})
	for worker := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for index := range iterations {
				id := fmt.Sprintf("worker-%02d-%04d", worker, index)
				entry := installExportListTestEntry(t, manager, testAccess, Job{
					ID: id, Version: 1, SearchJobID: "search", State: StateCompleted,
					CreatedAt: base.Add(time.Duration(worker*iterations+index) * time.Nanosecond),
				})
				if index%2 == 0 {
					manager.mu.Lock()
					if manager.jobs[id] == entry {
						manager.removeExportListEntryLocked(entry)
						delete(manager.jobs, id)
					}
					manager.mu.Unlock()
				}
				if _, err := manager.List(context.Background(), testAccess, ListRequest{PageSize: 1}); err != nil {
					t.Errorf("List() = %v", err)
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()

	manager.mu.RLock()
	root := manager.jobsByScope[testAccess]
	gotSize := exportListIndexSize(root)
	wantSize := 0
	for _, entry := range manager.jobs {
		if entry.access == testAccess {
			wantSize++
		}
	}
	manager.mu.RUnlock()
	if gotSize != wantSize {
		t.Fatalf("list index size = %d, want %d", gotSize, wantSize)
	}
}

func TestManagerCreateRejectsExportListGenerationOverflow(t *testing.T) {
	t.Parallel()

	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema()},
	}}
	manager := newExportTestManager(t, source, nil)
	manager.mu.Lock()
	manager.nextGeneration = math.MaxUint64
	manager.mu.Unlock()
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "search",
		Format:      FormatCSV,
	}); !errors.Is(err, ErrCapacity) ||
		!strings.Contains(err.Error(), "generation space is exhausted") {
		t.Fatalf("Create() error = %v, want classified generation exhaustion", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "overflow source lease release")
	manager.mu.RLock()
	jobCount := len(manager.jobs)
	scopeCount := len(manager.jobsByScope)
	reservedIDCount := len(manager.reservedIDs)
	reservations := manager.reservations
	queueReservations := manager.queueReservations
	manager.mu.RUnlock()
	if jobCount != 0 ||
		scopeCount != 0 ||
		reservedIDCount != 0 ||
		reservations != 0 ||
		queueReservations != 0 {
		t.Fatalf(
			"overflow leaked jobs=%d indexes=%d IDs=%d reservations=%d queue=%d",
			jobCount,
			scopeCount,
			reservedIDCount,
			reservations,
			queueReservations,
		)
	}
	manager.budgetMu.Lock()
	artifactBytes, metadataBytes := manager.totalBytes, manager.totalMetadata
	manager.budgetMu.Unlock()
	if artifactBytes != 0 || metadataBytes != 0 {
		t.Fatalf("overflow retained accounting = %d/%d", artifactBytes, metadataBytes)
	}
}

func TestManagerCleanupRemovesExportListIndexEntry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 18, 0, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return now }
	})
	entry := installExportListTestEntry(t, manager, testAccess, Job{
		ID:          "expired",
		Version:     2,
		SearchJobID: "search",
		State:       StateExpired,
		CreatedAt:   now.Add(-time.Hour),
		FinishedAt:  now.Add(-time.Hour),
		ExpiresAt:   now.Add(-time.Hour),
	})
	entry.mu.Lock()
	entry.expiredAt = now.Add(-manager.expiredRetention)
	entry.mu.Unlock()

	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	_, retained := manager.jobs["expired"]
	_, scoped := manager.jobsByScope[testAccess]
	manager.mu.RUnlock()
	if retained || scoped {
		t.Fatalf("cleanup retained job/index: job=%t scope=%t", retained, scoped)
	}
	page, err := manager.List(context.Background(), testAccess, ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 0 {
		t.Fatalf("cleanup list = %#v", page.Jobs)
	}
}

func TestManagerExportListScopeKeyMovesToRetainedEntry(t *testing.T) {
	t.Parallel()

	manager := newExportTestManager(t, &exportTestSource{}, nil)
	firstAccess := searchjobs.AccessScope{
		TenantID: strings.Clone(testAccess.TenantID),
		OwnerID:  strings.Clone(testAccess.OwnerID),
	}
	retainedAccess := searchjobs.AccessScope{
		TenantID: strings.Clone(testAccess.TenantID),
		OwnerID:  strings.Clone(testAccess.OwnerID),
	}
	first := installExportListTestEntry(t, manager, firstAccess, Job{
		ID: "first", Version: 1, SearchJobID: "search",
		State: StateCompleted, CreatedAt: time.Now().UTC(),
	})
	retained := installExportListTestEntry(t, manager, retainedAccess, Job{
		ID: "retained", Version: 1, SearchJobID: "search",
		State: StateCompleted, CreatedAt: time.Now().UTC().Add(time.Second),
	})

	manager.mu.Lock()
	manager.removeExportListEntryLocked(first)
	delete(manager.jobs, first.job.ID)
	var storedScope searchjobs.AccessScope
	for scope := range manager.jobsByScope {
		storedScope = scope
	}
	manager.mu.Unlock()

	if unsafe.StringData(storedScope.TenantID) !=
		unsafe.StringData(retained.access.TenantID) ||
		unsafe.StringData(storedScope.OwnerID) !=
			unsafe.StringData(retained.access.OwnerID) {
		t.Fatal("scope index retained identity storage from the removed entry")
	}
}

func TestManagerCompactsSparseExportListScopeIndex(t *testing.T) {
	t.Parallel()

	manager := newExportTestManager(t, &exportTestSource{}, nil)
	const scopes = 64
	entries := make([]*jobEntry, 0, scopes)
	for index := range scopes {
		access := searchjobs.AccessScope{
			TenantID: testAccess.TenantID,
			OwnerID:  fmt.Sprintf("owner-%02d", index),
		}
		entries = append(entries, installExportListTestEntry(
			t,
			manager,
			access,
			Job{
				ID: fmt.Sprintf("export-%02d", index), Version: 1,
				SearchJobID: "search", State: StateCompleted,
				CreatedAt: time.Now().UTC().Add(time.Duration(index)),
			},
		))
	}

	manager.mu.Lock()
	const retainedScopes = scopes / 4
	for _, entry := range entries[:len(entries)-retainedScopes] {
		manager.removeExportListEntryLocked(entry)
		delete(manager.jobs, entry.job.ID)
	}
	if got := manager.scopeIndexHighWater; got != scopes {
		manager.mu.Unlock()
		t.Fatalf("pre-compaction scope high water = %d, want %d", got, scopes)
	}
	manager.compactExportListScopeIndexLocked()
	live, highWater := len(manager.jobsByScope), manager.scopeIndexHighWater
	manager.mu.Unlock()
	if live != retainedScopes || highWater != retainedScopes {
		t.Fatalf(
			"compacted scope index live/high-water = %d/%d, want %d/%d",
			live,
			highWater,
			retainedScopes,
			retainedScopes,
		)
	}
}

func TestManagerListConcurrentCloseIsRaceSafe(t *testing.T) {
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	entry := installExportListTestEntry(t, manager, testAccess, Job{
		ID:          "blocked",
		Version:     1,
		SearchJobID: "search",
		State:       StateCompleted,
		CreatedAt:   time.Date(2026, time.July, 30, 19, 0, 0, 0, time.UTC),
	})
	entry.mu.Lock()
	listDone := make(chan error, 1)
	go func() {
		_, err := manager.List(context.Background(), testAccess, ListRequest{
			IncludeTotal: true,
		})
		listDone <- err
	}()
	waitFor(t, func() bool { return len(manager.listGate) == 1 }, "list gate acquisition")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- manager.Close()
	}()
	waitFor(t, manager.isClosed, "manager close start")
	entry.mu.Unlock()

	if err := <-listDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("racing List() error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := manager.List(context.Background(), testAccess, ListRequest{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close List() error = %v, want ErrClosed", err)
	}
}

func TestManagerListGateFailsFastWithoutWaiters(t *testing.T) {
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	for range cap(manager.listGate) {
		manager.listGate <- struct{}{}
	}
	started := time.Now()
	if _, err := manager.List(
		context.Background(),
		testAccess,
		ListRequest{},
	); !errors.Is(err, ErrListCapacity) {
		t.Fatalf("full-gate List() error = %v, want ErrListCapacity", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("full-gate List() waited for %s", elapsed)
	}
	if got := len(manager.listGate); got != cap(manager.listGate) {
		t.Fatalf("full-gate List() changed occupancy to %d", got)
	}
}

func TestManagerListCancellationBehindEntryLockDoesNotExpire(t *testing.T) {
	now := time.Date(2026, time.July, 30, 19, 30, 0, 0, time.UTC)
	manager := newExportTestManager(t, &exportTestSource{}, func(config *Config) {
		config.Now = func() time.Time { return now }
	})
	entry := installExportListTestEntry(t, manager, testAccess, Job{
		ID: "due", Version: 1, SearchJobID: "search",
		State: StateCompleted, CreatedAt: now.Add(-time.Hour),
		FinishedAt: now.Add(-time.Minute), ExpiresAt: now,
		Artifact: &Artifact{
			FileName: "due.csv", MediaType: "text/csv; charset=utf-8",
			SizeBytes: 1, RowCount: 1, ExpiresAt: now,
		},
	})
	entry.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	listDone := make(chan error, 1)
	go func() {
		_, err := manager.List(ctx, testAccess, ListRequest{})
		listDone <- err
	}()
	waitFor(t, func() bool { return len(manager.listGate) == 1 }, "list gate acquisition")
	cancel()
	entry.mu.Unlock()

	if err := <-listDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled List() error = %v, want context.Canceled", err)
	}
	entry.mu.RLock()
	state, version, artifact := entry.job.State, entry.job.Version, entry.job.Artifact
	entry.mu.RUnlock()
	if state != StateCompleted || version != 1 || artifact == nil {
		t.Fatalf(
			"canceled list mutated due job: state=%s version=%d artifact=%#v",
			state,
			version,
			artifact,
		)
	}
}

func installExportListTestEntry(
	t *testing.T,
	manager *Manager,
	access searchjobs.AccessScope,
	job Job,
) *jobEntry {
	t.Helper()
	entry := &jobEntry{
		access:        access,
		job:           cloneJob(job),
		ctx:           context.Background(),
		cancel:        func() {},
		workerDone:    true,
		leaseReleased: true,
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.jobs[job.ID]; exists {
		t.Fatalf("duplicate test export ID %q", job.ID)
	}
	if manager.nextGeneration == math.MaxUint64 {
		t.Fatal("test generation space exhausted")
	}
	manager.nextGeneration++
	entry.generation = manager.nextGeneration
	manager.jobs[job.ID] = entry
	manager.insertExportListEntryLocked(entry)
	return entry
}

func removeExportListTestEntry(manager *Manager, id string) {
	manager.mu.Lock()
	entry := manager.jobs[id]
	if entry != nil {
		manager.removeExportListEntryLocked(entry)
		delete(manager.jobs, id)
	}
	manager.mu.Unlock()
}

func exportListIDs(jobs []ListItem) []string {
	result := make([]string, len(jobs))
	for index := range jobs {
		result[index] = jobs[index].ID
	}
	return result
}

func stringPointer(value string) *string {
	return &value
}

type exportListCountingContext struct {
	checks int
	after  int
}

func (*exportListCountingContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*exportListCountingContext) Done() <-chan struct{}       { return nil }
func (counting *exportListCountingContext) Err() error {
	counting.checks++
	if counting.checks >= counting.after {
		return context.Canceled
	}
	return nil
}
func (*exportListCountingContext) Value(any) any { return nil }
