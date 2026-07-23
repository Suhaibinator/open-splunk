package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestListPageForMatchesReferenceOrdering(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return base })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	random := rand.New(rand.NewSource(8675309))
	reference := make([]Job, 0, 257)
	for index := range 257 {
		job := Job{
			ID:        fmt.Sprintf("job-%03d", index),
			Version:   1,
			TenantID:  access.TenantID,
			OwnerID:   access.OwnerID,
			SPL:       fmt.Sprintf("index=main sequence=%d", index),
			State:     StateCompleted,
			CreatedAt: base.Add(time.Duration(random.Intn(23)-11) * time.Second),
		}
		installJobListTestEntry(manager, job)
		reference = append(reference, job)
	}
	sort.Slice(reference, func(left, right int) bool {
		if reference[left].CreatedAt.Equal(reference[right].CreatedAt) {
			return reference[left].ID > reference[right].ID
		}
		return reference[left].CreatedAt.After(reference[right].CreatedAt)
	})

	var got []string
	token := ""
	for pageNumber := 0; ; pageNumber++ {
		pageSize := 1 + (pageNumber*7)%maximumJobListPageSize
		page, err := manager.ListPageFor(context.Background(), access, JobListRequest{
			PageSize:     pageSize,
			PageToken:    token,
			IncludeTotal: pageNumber == 0,
		})
		if err != nil {
			t.Fatalf("ListPageFor(page %d) error = %v", pageNumber, err)
		}
		if len(page.Jobs) > pageSize {
			t.Fatalf("page %d returned %d jobs for size %d", pageNumber, len(page.Jobs), pageSize)
		}
		if pageNumber == 0 {
			if page.TotalSize == nil || *page.TotalSize != uint64(len(reference)) || !page.TotalSizeExact {
				t.Fatalf("first-page total = (%v, %t), want (%d, true)", page.TotalSize, page.TotalSizeExact, len(reference))
			}
		} else if page.TotalSize != nil || page.TotalSizeExact {
			t.Fatalf("page %d unexpectedly returned a total", pageNumber)
		}
		for _, item := range page.Jobs {
			got = append(got, item.ID)
		}
		if page.NextPageToken == "" {
			break
		}
		if page.NextPageToken == token {
			t.Fatalf("page %d repeated its input cursor", pageNumber)
		}
		token = page.NextPageToken
		if pageNumber > len(reference) {
			t.Fatal("pagination did not terminate")
		}
	}
	want := make([]string, len(reference))
	for index := range reference {
		want[index] = reference[index].ID
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listed IDs differ from reference\n got: %v\nwant: %v", got, want)
	}
}

func TestListPageForFiltersAndReturnsDetachedSafeSummaries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 13, 0, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return now })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	matching := Job{
		ID:               "matching",
		Version:          7,
		TenantID:         access.TenantID,
		OwnerID:          access.OwnerID,
		SPL:              "INDEX=main error=Café",
		NormalizedSPL:    "index=main error=Café",
		RequestedIndexes: []string{"main"},
		EffectiveIndexes: []string{"main", "archive"},
		AppID:            "search",
		State:            StateFailed,
		ScannedRows:      123,
		ScannedBytes:     4567,
		Schema:           &Schema{Columns: []Column{{Name: "message", Kind: ValueKindString}}},
		Failure: &Failure{
			Code:        FailureExecution,
			Message:     "safe summary",
			Retryable:   true,
			Diagnostics: []Diagnostic{{Code: "SECRET", Message: "detail", Suggestions: []string{"secret"}}},
		},
		CreatedAt: now,
	}
	installJobListTestEntry(manager, matching)
	installJobListTestEntry(manager, Job{
		ID: "different-unicode-case", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main error=CAFÉ", AppID: "search", State: StateFailed, CreatedAt: now.Add(-time.Second),
	})
	installJobListTestEntry(manager, Job{
		ID: "other-owner", Version: 1, TenantID: access.TenantID, OwnerID: "intruder",
		SPL: matching.SPL, AppID: "search", State: StateFailed, CreatedAt: now.Add(time.Second),
	})
	installJobListTestEntry(manager, Job{
		ID: "empty-app", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", State: StateCompleted, CreatedAt: now.Add(-2 * time.Second),
	})
	app, text := "search", "  index=MAIN error=Café  "
	page, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		StateFilters: []State{StateFailed, StateFailed},
		AppIDFilter:  &app,
		TextFilter:   &text,
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != matching.ID {
		t.Fatalf("filtered jobs = %#v, want only %q", page.Jobs, matching.ID)
	}
	if page.TotalSize == nil || *page.TotalSize != 1 || !page.TotalSizeExact {
		t.Fatalf("filtered total = (%v, %t), want (1, true)", page.TotalSize, page.TotalSizeExact)
	}
	item := &page.Jobs[0]
	if item.SPL != matching.SPL || item.NormalizedSPL != matching.NormalizedSPL ||
		!reflect.DeepEqual(item.RequestedIndexes, matching.RequestedIndexes) ||
		!reflect.DeepEqual(item.EffectiveIndexes, matching.EffectiveIndexes) ||
		item.ScannedRows != matching.ScannedRows || item.ScannedBytes != matching.ScannedBytes {
		t.Fatalf("list summary lost query/scope metadata: %#v", item)
	}
	if item.Failure == nil || item.Failure.Message != "safe summary" {
		t.Fatalf("list summary failure = %#v", item.Failure)
	}

	item.RequestedIndexes[0] = "mutated"
	item.EffectiveIndexes[0] = "mutated"
	item.Failure.Message = "mutated"
	stored, err := manager.Get(matching.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.RequestedIndexes, matching.RequestedIndexes) ||
		!reflect.DeepEqual(stored.EffectiveIndexes, matching.EffectiveIndexes) ||
		stored.Failure == nil || stored.Failure.Message != "safe summary" {
		t.Fatalf("caller mutation reached retained job: %#v", stored)
	}

	emptyApp := ""
	emptyAppPage, err := manager.ListPageFor(context.Background(), access, JobListRequest{AppIDFilter: &emptyApp})
	if err != nil {
		t.Fatal(err)
	}
	if got := jobListItemIDs(emptyAppPage.Jobs); !reflect.DeepEqual(got, []string{"empty-app"}) {
		t.Fatalf("explicit empty-app filter jobs = %v, want [empty-app]", got)
	}

	oversizedFailure := installJobListTestEntry(manager, Job{
		ID: "oversized-failure", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", State: StateFailed, CreatedAt: now.Add(-3 * time.Second),
		Failure: &Failure{Code: FailureUnsupportedSPL, Message: strings.Repeat("x", maximumJobListFailureMessageBytes+1)},
	})
	failurePage, err := manager.ListPageFor(context.Background(), access, JobListRequest{StateFilters: []State{StateFailed}})
	if err != nil {
		t.Fatal(err)
	}
	var found *JobListFailure
	for index := range failurePage.Jobs {
		if failurePage.Jobs[index].ID == oversizedFailure.job.ID {
			found = failurePage.Jobs[index].Failure
		}
	}
	if found == nil || found.Message != "search SPL contains an unsupported command" {
		t.Fatalf("oversized failure summary = %#v", found)
	}
}

func TestListPageForCursorReplayHighWaterAndDeletedAnchor(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 14, 0, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return now })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for index := 3; index >= 1; index-- {
		installJobListTestEntry(manager, Job{
			ID: fmt.Sprintf("original-%d", index), Version: 1,
			TenantID: access.TenantID, OwnerID: access.OwnerID, SPL: "index=main",
			AppID: "search", State: StateRunning, CreatedAt: now.Add(time.Duration(index) * time.Second),
		})
	}
	states := []State{StateRunning, StateRunning}
	app, text := "search", " index=MAIN "
	first, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageSize: 1, StateFilters: states, AppIDFilter: &app, TextFilter: &text,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Jobs) != 1 || first.Jobs[0].ID != "original-3" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	// Keyset traversal must not require the anchor to remain retained.
	manager.mu.Lock()
	manager.removeJobListEntryLocked(manager.jobs["original-3"])
	delete(manager.jobs, "original-3")
	manager.mu.Unlock()
	installJobListTestEntry(manager, Job{
		ID: "original-3", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", AppID: "search", State: StateRunning, CreatedAt: now.Add(30 * time.Second),
	})
	installJobListTestEntry(manager, Job{
		ID: "new-admission-with-old-sort-key", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", AppID: "search", State: StateRunning, CreatedAt: now.Add(-30 * time.Second),
	})

	// Canonically equivalent filters, a changed page size, and IncludeTotal are
	// valid cursor replay because neither affects membership or sort order.
	reorderedStates := []State{StateRunning}
	second, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageSize: 15, PageToken: first.NextPageToken, IncludeTotal: true,
		StateFilters: reorderedStates, AppIDFilter: &app, TextFilter: &text,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := jobListItemIDs(second.Jobs)
	if want := []string{"original-2", "original-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("continuation jobs = %v, want %v", got, want)
	}
	if second.TotalSize == nil || *second.TotalSize != 2 || !second.TotalSizeExact {
		t.Fatalf("continuation total = (%v, %t), want current high-water total 2", second.TotalSize, second.TotalSizeExact)
	}

	changedApp := "other"
	replays := []struct {
		name    string
		access  AccessScope
		request JobListRequest
	}{
		{"owner", AccessScope{TenantID: access.TenantID, OwnerID: "other"}, JobListRequest{AppIDFilter: &app, TextFilter: &text, StateFilters: reorderedStates}},
		{"tenant", AccessScope{TenantID: "other", OwnerID: access.OwnerID}, JobListRequest{AppIDFilter: &app, TextFilter: &text, StateFilters: reorderedStates}},
		{"app", access, JobListRequest{AppIDFilter: &changedApp, TextFilter: &text, StateFilters: reorderedStates}},
		{"text", access, JobListRequest{AppIDFilter: &app, TextFilter: stringPointer("different"), StateFilters: reorderedStates}},
		{"state", access, JobListRequest{AppIDFilter: &app, TextFilter: &text, StateFilters: []State{StateQueued}}},
	}
	for _, replay := range replays {
		t.Run(replay.name, func(t *testing.T) {
			replay.request.PageToken = first.NextPageToken
			if _, err := manager.ListPageFor(context.Background(), replay.access, replay.request); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("replay error = %v, want ErrInvalidCursor", err)
			}
		})
	}
	tamperedByte := byte('A')
	if first.NextPageToken[len(first.NextPageToken)-1] == tamperedByte {
		tamperedByte = 'B'
	}
	tampered := first.NextPageToken[:len(first.NextPageToken)-1] + string(tamperedByte)
	if _, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageToken: tampered, AppIDFilter: &app, TextFilter: &text, StateFilters: reorderedStates,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("tampered cursor error = %v, want ErrInvalidCursor", err)
	}

	unfilteredFirst, err := manager.ListPageFor(context.Background(), access, JobListRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if unfilteredFirst.NextPageToken == "" {
		t.Fatal("unfiltered first page did not produce a cursor")
	}
	// Nil and empty repeated-state inputs are the same canonical no-filter.
	if _, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageToken: unfilteredFirst.NextPageToken, StateFilters: []State{},
	}); err != nil {
		t.Fatalf("nil-to-empty canonical state replay error = %v", err)
	}
	emptyApp := ""
	if _, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageToken: unfilteredFirst.NextPageToken, AppIDFilter: &emptyApp,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("nil-to-empty app replay error = %v, want ErrInvalidCursor", err)
	}

	otherManager := newJobListTestManager(t, func() time.Time { return now })
	otherManager.cursorScope = manager.cursorScope
	otherManager.cursorKey = append([]byte(nil), manager.cursorKey...)
	if _, err := otherManager.ListPageFor(context.Background(), access, JobListRequest{
		PageToken: first.NextPageToken, AppIDFilter: &app, TextFilter: &text, StateFilters: reorderedStates,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cross-manager cursor error = %v, want ErrInvalidCursor", err)
	}
}

func TestListPageForIndexedTraversalSkipsUnneededScopedTail(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 22, 14, 30, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return base })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for index := range 2_048 {
		installJobListTestEntry(manager, Job{
			ID: fmt.Sprintf("job-%04d", index), Version: 1,
			TenantID: access.TenantID, OwnerID: access.OwnerID,
			SPL: "index=main", State: StateCompleted, CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
	}
	for index := range 2_048 {
		installJobListTestEntry(manager, Job{
			ID: fmt.Sprintf("other-%04d", index), Version: 1,
			TenantID: access.TenantID, OwnerID: "other-owner",
			SPL: "index=main", State: StateCompleted, CreatedAt: base.Add(time.Duration(index) * time.Second),
		})
	}

	counting := &cancelAfterChecksContext{after: 1 << 20}
	page, err := manager.ListPageFor(counting, access, JobListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != defaultJobListPageSize || page.Jobs[0].ID != "job-2047" || page.Jobs[len(page.Jobs)-1].ID != "job-2033" {
		t.Fatalf("indexed first page = %v", jobListItemIDs(page.Jobs))
	}
	if counting.checks > 5 {
		t.Fatalf("context checks = %d, want bounded first-page traversal", counting.checks)
	}
}

func TestListPageForCleanupRemovalAndReusedIDPreserveHighWater(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 14, 45, 0, 0, time.UTC)}
	manager := newJobListTestManager(t, clock.Now)
	manager.expiredRetention = time.Second
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	anchor := installJobListTestEntry(manager, Job{
		ID: "reused", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", State: StateExpired, CreatedAt: clock.Now().Add(3 * time.Second),
	})
	anchor.expiredAt = clock.Now().Add(-2 * time.Second)
	for index := 2; index >= 1; index-- {
		installJobListTestEntry(manager, Job{
			ID: fmt.Sprintf("original-%d", index), Version: 1,
			TenantID: access.TenantID, OwnerID: access.OwnerID,
			SPL: "index=main", State: StateCompleted, CreatedAt: clock.Now().Add(time.Duration(index) * time.Second),
		})
	}

	first, err := manager.ListPageFor(context.Background(), access, JobListRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Jobs) != 1 || first.Jobs[0].ID != "reused" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("Cleanup() changed %d jobs, want 1", changed)
	}
	installJobListTestEntry(manager, Job{
		ID: "reused", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", State: StateRunning, CreatedAt: clock.Now().Add(-time.Second),
	})

	second, err := manager.ListPageFor(context.Background(), access, JobListRequest{
		PageSize: 15, PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := jobListItemIDs(second.Jobs); !reflect.DeepEqual(got, []string{"original-2", "original-1"}) {
		t.Fatalf("continuation jobs = %v, want original retained jobs", got)
	}
}

func TestListPageForCloseDuringTextScanReturnsErrClosedAndReleasesGate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 14, 50, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return now })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	installJobListTestEntry(manager, Job{
		ID: "long-scan", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: strings.Repeat("x", 64<<10), State: StateCompleted, CreatedAt: now,
	})
	ctx := &blockingErrContext{
		Context: context.Background(),
		blockAt: 3,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	text := "not-present"
	result := make(chan error, 1)
	go func() {
		_, listErr := manager.ListPageFor(ctx, access, JobListRequest{TextFilter: &text})
		result <- listErr
	}()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		ctx.unblock()
		t.Fatal("list did not enter the text matcher")
	}
	if err := manager.Close(); err != nil {
		ctx.unblock()
		t.Fatal(err)
	}
	ctx.unblock()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ListPageFor error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("list did not return after Close")
	}
	if got := len(manager.listGate); got != 0 {
		t.Fatalf("list gate occupancy = %d, want 0", got)
	}
}

func TestListPageForValidatesInputAndCancellation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 15, 0, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return now })
	validAccess := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	invalidUTF8 := string([]byte{0xff})
	tooManyStates := make([]State, maximumJobListStates+1)
	for index := range tooManyStates {
		tooManyStates[index] = StateRunning
	}
	tests := []struct {
		name    string
		access  AccessScope
		request JobListRequest
		want    error
	}{
		{"negative page", validAccess, JobListRequest{PageSize: -1}, ErrPageSize},
		{"large page", validAccess, JobListRequest{PageSize: maximumJobListPageSize + 1}, ErrPageSize},
		{"large token", validAccess, JobListRequest{PageToken: strings.Repeat("x", maximumJobListTokenSize+1)}, ErrInvalidCursor},
		{"empty tenant", AccessScope{OwnerID: "owner"}, JobListRequest{}, ErrInvalidListFilter},
		{"empty owner", AccessScope{TenantID: "tenant"}, JobListRequest{}, ErrInvalidListFilter},
		{"spaced owner", AccessScope{TenantID: "tenant", OwnerID: " owner "}, JobListRequest{}, ErrInvalidListFilter},
		{"control owner", AccessScope{TenantID: "tenant", OwnerID: "own\ner"}, JobListRequest{}, ErrInvalidListFilter},
		{"invalid tenant utf8", AccessScope{TenantID: invalidUTF8, OwnerID: "owner"}, JobListRequest{}, ErrInvalidListFilter},
		{"long owner", AccessScope{TenantID: "tenant", OwnerID: strings.Repeat("o", defaultMaxIdentityBytes+1)}, JobListRequest{}, ErrInvalidListFilter},
		{"too many states", validAccess, JobListRequest{StateFilters: tooManyStates}, ErrInvalidListFilter},
		{"invalid state zero", validAccess, JobListRequest{StateFilters: []State{StateInvalid}}, ErrInvalidListFilter},
		{"invalid state high", validAccess, JobListRequest{StateFilters: []State{StateExpired + 1}}, ErrInvalidListFilter},
		{"spaced app", validAccess, JobListRequest{AppIDFilter: stringPointer(" search ")}, ErrInvalidListFilter},
		{"invalid app utf8", validAccess, JobListRequest{AppIDFilter: &invalidUTF8}, ErrInvalidListFilter},
		{"long app", validAccess, JobListRequest{AppIDFilter: stringPointer(strings.Repeat("a", maximumJobAppIDBytes+1))}, ErrInvalidListFilter},
		{"invalid text utf8", validAccess, JobListRequest{TextFilter: &invalidUTF8}, ErrInvalidListFilter},
		{"nul text", validAccess, JobListRequest{TextFilter: stringPointer("bad\x00text")}, ErrInvalidListFilter},
		{"control text", validAccess, JobListRequest{TextFilter: stringPointer("bad\u0085text")}, ErrInvalidListFilter},
		{"long text", validAccess, JobListRequest{TextFilter: stringPointer(strings.Repeat("q", maximumJobListTextBytes+1))}, ErrInvalidListFilter},
		{"long whitespace text", validAccess, JobListRequest{TextFilter: stringPointer(strings.Repeat(" ", maximumJobListTextBytes+1))}, ErrInvalidListFilter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := manager.ListPageFor(context.Background(), test.access, test.request)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := manager.ListPageFor(nil, validAccess, JobListRequest{}); err == nil {
		t.Fatal("nil context succeeded")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.ListPageFor(canceled, validAccess, JobListRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context error = %v, want context.Canceled", err)
	}

	for range cap(manager.listGate) {
		manager.listGate <- struct{}{}
	}
	waiting, cancelWaiting := context.WithCancel(context.Background())
	cancelWaiting()
	if _, err := manager.ListPageFor(waiting, validAccess, JobListRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("gate-wait cancellation error = %v, want context.Canceled", err)
	}
	for range cap(manager.listGate) {
		<-manager.listGate
	}
}

func BenchmarkNormalizeJobListRequestRejectsOversizedFilters(b *testing.B) {
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for name, request := range map[string]JobListRequest{
		"text": {TextFilter: stringPointer(strings.Repeat(" ", 1<<20))},
		"app":  {AppIDFilter: stringPointer(strings.Repeat("a", 1<<20))},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := normalizeJobListRequest(access, request); !errors.Is(err, ErrInvalidListFilter) {
					b.Fatalf("error = %v, want ErrInvalidListFilter", err)
				}
			}
		})
	}
}

func BenchmarkJobListIndexInsertAndRemove100K(b *testing.B) {
	const entryCount = 100_000
	timestamp := time.Date(2026, time.July, 22, 15, 30, 0, 0, time.UTC)
	entries := make([]*jobEntry, entryCount)
	for index := range entries {
		entries[index] = &jobEntry{
			job: Job{
				ID: fmt.Sprintf("job-%06d", index), TenantID: "tenant", OwnerID: "owner",
				CreatedAt: timestamp,
			},
			generation: uint64(index + 1),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		manager := &Manager{jobsByScope: make(map[AccessScope]*jobListIndexNode)}
		for _, entry := range entries {
			manager.insertJobListEntryLocked(entry)
		}
		if got := jobListIndexSize(manager.jobsByScope[AccessScope{TenantID: "tenant", OwnerID: "owner"}]); got != entryCount {
			b.Fatalf("index size = %d, want %d", got, entryCount)
		}
		for _, entry := range entries {
			manager.removeJobListEntryLocked(entry)
		}
		if len(manager.jobsByScope) != 0 {
			b.Fatalf("scope roots after removal = %d, want 0", len(manager.jobsByScope))
		}
	}
}

func TestListPageForExpiresBeforeFilteringAndSurvivesConcurrentMutation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 16, 0, 0, 0, time.UTC)
	manager := newJobListTestManager(t, func() time.Time { return now })
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	entry := installJobListTestEntry(manager, Job{
		ID: "mutable", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", State: StateCompleted, CreatedAt: now.Add(-time.Minute),
		ExpiresAt: now,
	})
	page, err := manager.ListPageFor(context.Background(), access, JobListRequest{StateFilters: []State{StateExpired}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].State != StateExpired {
		t.Fatalf("expired-filter page = %#v", page.Jobs)
	}

	var wait sync.WaitGroup
	wait.Add(2)
	start := make(chan struct{})
	go func() {
		defer wait.Done()
		<-start
		for index := range 500 {
			entry.mu.Lock()
			entry.job.Version++
			if index%2 == 0 {
				entry.job.State = StateRunning
				entry.job.EffectiveIndexes = []string{"main"}
				entry.job.Failure = nil
			} else {
				entry.job.State = StateFailed
				entry.job.EffectiveIndexes = []string{"archive"}
				entry.job.Failure = &Failure{Code: FailureExecution, Message: "safe", Diagnostics: []Diagnostic{{Code: "detail"}}}
			}
			entry.mu.Unlock()
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for range 500 {
			_, listErr := manager.ListPageFor(context.Background(), access, JobListRequest{PageSize: 1})
			if listErr != nil {
				t.Errorf("concurrent ListPageFor error = %v", listErr)
				return
			}
			// JobListFailure has no diagnostics field; a successful projection is
			// structurally unable to expose the concurrently changing details.
		}
	}()
	close(start)
	wait.Wait()
}

func TestJobListTextMatcherASCIIInsensitiveNonASCIIExact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		source  string
		want    bool
	}{
		{"INDEX=MAIN", "prefix index=main suffix", true},
		{"Café", "error=Café", true},
		{"CAFÉ", "error=Café", false},
		{"aaaaab", strings.Repeat("a", 4096) + "b", true},
		{"aaaaab", strings.Repeat("a", 4096), false},
		{"longer", "short", false},
		{"", "anything", true},
	}
	for _, test := range tests {
		matcher := newJobListTextMatcher(test.pattern)
		if got := matcher.Contains(test.source); got != test.want {
			t.Errorf("Contains(%q, %q) = %t, want %t", test.pattern, test.source, got, test.want)
		}
	}

	cancelingContext := &cancelAfterChecksContext{after: 3}
	matcher := newJobListTextMatcher("never-matches")
	if _, err := matcher.containsContext(
		cancelingContext,
		context.Background(),
		strings.Repeat("x", 32<<10),
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-SPL cancellation error = %v, want context.Canceled", err)
	}
}

func TestJobListTextMatcherMatchesNaiveASCIIFoldReference(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(0x5eed))
	alphabet := []rune("abcXYZ012 _=-|éÉλ")
	randomString := func(maximum int) string {
		length := random.Intn(maximum + 1)
		value := make([]rune, length)
		for index := range value {
			value[index] = alphabet[random.Intn(len(alphabet))]
		}
		return string(value)
	}
	for iteration := range 10_000 {
		source := randomString(96)
		pattern := randomString(16)
		matcher := newJobListTextMatcher(pattern)
		want := strings.Contains(foldASCIIReference(source), foldASCIIReference(pattern))
		if got := matcher.Contains(source); got != want {
			t.Fatalf("iteration %d: Contains(%q, %q) = %t, want %t", iteration, source, pattern, got, want)
		}
	}
}

func foldASCIIReference(value string) string {
	folded := []byte(value)
	for index, character := range folded {
		if character >= 'A' && character <= 'Z' {
			folded[index] = character + ('a' - 'A')
		}
	}
	return string(folded)
}

type cancelAfterChecksContext struct {
	checks int
	after  int
}

func (*cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelAfterChecksContext) Done() <-chan struct{}       { return nil }
func (canceling *cancelAfterChecksContext) Err() error {
	canceling.checks++
	if canceling.checks >= canceling.after {
		return context.Canceled
	}
	return nil
}
func (*cancelAfterChecksContext) Value(any) any { return nil }

type blockingErrContext struct {
	context.Context
	checks  int
	blockAt int
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *blockingErrContext) Err() error {
	ctx.checks++
	if ctx.checks == ctx.blockAt {
		close(ctx.entered)
		<-ctx.release
	}
	return ctx.Context.Err()
}

func (ctx *blockingErrContext) unblock() {
	ctx.once.Do(func() { close(ctx.release) })
}

func newJobListTestManager(t *testing.T, now func() time.Time) *Manager {
	t.Helper()
	return newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return nil
		}),
		MaxConcurrent:   1,
		MaxJobs:         1_000,
		CleanupInterval: -1,
		Now:             now,
		CursorKey:       testCursorKey,
		CursorScope:     "shared-configured-scope",
	})
}

func installJobListTestEntry(manager *Manager, job Job) *jobEntry {
	if job.Version == 0 {
		job.Version = 1
	}
	entry := &jobEntry{
		job:     cloneJob(job),
		history: []State{job.State},
		cancel:  func() {},
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.jobs[job.ID]; exists {
		panic("duplicate test job ID: " + job.ID)
	}
	manager.nextGeneration++
	entry.generation = manager.nextGeneration
	manager.jobs[job.ID] = entry
	manager.insertJobListEntryLocked(entry)
	return entry
}

func jobListItemIDs(items []JobListItem) []string {
	ids := make([]string, len(items))
	for index := range items {
		ids[index] = items[index].ID
	}
	return ids
}

func stringPointer(value string) *string {
	return &value
}
