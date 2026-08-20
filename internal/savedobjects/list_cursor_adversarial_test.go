package savedobjects

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const adversarialOwner = "cursor-owner"

var (
	adversarialSorts = []opensplunk.SavedSearchSortBy{
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME,
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT,
	}
	adversarialDirections = []opensplunk.SortDirection{
		opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
		opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
	}
	adversarialNames = []string{"dup", "dup", "zeta", "dup", "alpha", "mid"}
	adversarialApps  = []string{"app_a", "app_b", "app_a", "app_c", "app_b", "app_c"}
)

// openTiedStore freezes the clock so every record shares one
// created_at/updated_at value while names repeat across apps, forcing both
// collapsed helpers onto the saved_search_id tiebreaker for every sort.
func openTiedStore(t *testing.T) (*Store, []string) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	frozen := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	index := 0
	store, err := New(database, Options{
		CursorKey: testCursorKey,
		Clock:     func() time.Time { return frozen },
		IDGenerator: func() (string, error) {
			index++
			return fmt.Sprintf("ss_tie_%04d", index), nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ids := make([]string, 0, len(adversarialNames))
	for position, name := range adversarialNames {
		created, err := store.Create(
			context.Background(),
			AccessScope{OwnerID: adversarialOwner},
			savedSearchDefinition(name, adversarialApps[position]),
		)
		if err != nil {
			t.Fatalf("Create(%s/%s) error = %v", adversarialApps[position], name, err)
		}
		ids = append(ids, created.SavedSearchId)
	}
	return store, ids
}

// adversarialToken forges a page token that carries the correct filter
// fingerprint for the request but whatever key shape the caller chose.
func adversarialToken(t *testing.T, request ListRequest, cursor listCursor) string {
	t.Helper()
	normalized, err := normalizeListRequest(AccessScope{OwnerID: adversarialOwner}, request)
	if err != nil {
		t.Fatalf("normalizeListRequest() error = %v", err)
	}
	cursor.FilterHash, err = listFilterHash(normalized)
	if err != nil {
		t.Fatalf("listFilterHash() error = %v", err)
	}
	token, err := encodeListCursor(testCursorKey, cursor)
	if err != nil {
		t.Fatalf("encodeListCursor() error = %v", err)
	}
	return token
}

func adversarialList(t *testing.T, store *Store, request ListRequest) (ListResult, error) {
	t.Helper()
	return store.List(context.Background(), AccessScope{OwnerID: adversarialOwner}, request)
}

// TestListRejectsMismatchedCursorKeyShapes replays hash-valid cursors whose key
// shape does not match the sort field. The collapsed cursor helper dereferences
// IntegerKey unconditionally for time sorts, so a hash-valid integer-less
// cursor is the panic hazard.
func TestListRejectsMismatchedCursorKeyShapes(t *testing.T) {
	store, ids := openTiedStore(t)
	integer := int64(1)
	shapes := map[string]listCursor{
		"no key":      {SavedSearch: ids[0]},
		"string key":  {SavedSearch: ids[0], StringKey: "dup"},
		"integer key": {SavedSearch: ids[0], IntegerKey: &integer},
		"both keys":   {SavedSearch: ids[0], StringKey: "dup", IntegerKey: &integer},
		"no id":       {StringKey: "dup", IntegerKey: &integer},
		"blank id":    {SavedSearch: " ", StringKey: "dup", IntegerKey: &integer},
	}
	for _, sortBy := range adversarialSorts {
		for _, direction := range adversarialDirections {
			stringSort := sortBy == opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME
			for shape, cursor := range shapes {
				request := ListRequest{PageSize: 2, SortBy: sortBy, SortDirection: direction}
				request.PageToken = adversarialToken(t, request, cursor)
				_, err := adversarialList(t, store, request)
				if shape == "string key" && stringSort || shape == "integer key" && !stringSort {
					if err != nil {
						t.Fatalf("List(%v/%v, %s) error = %v, want success", sortBy, direction, shape, err)
					}
					continue
				}
				if !errors.Is(err, control.ErrInvalidArgument) {
					t.Fatalf("List(%v/%v, %s) error = %v, want ErrInvalidArgument", sortBy, direction, shape, err)
				}
			}
		}
	}
}

// TestListAcceptsExtremeCursorIntegerKeys drives the collapsed comparison
// operator with int64 saturation values no clock can produce.
func TestListAcceptsExtremeCursorIntegerKeys(t *testing.T) {
	store, ids := openTiedStore(t)
	ascending := opensplunk.SortDirection_SORT_DIRECTION_ASCENDING
	descending := opensplunk.SortDirection_SORT_DIRECTION_DESCENDING
	for _, sortBy := range adversarialSorts[1:] {
		for _, test := range []struct {
			key       int64
			direction opensplunk.SortDirection
			want      int
		}{
			{math.MinInt64, ascending, len(ids)}, {math.MinInt64, descending, 0},
			{math.MaxInt64, ascending, 0}, {math.MaxInt64, descending, len(ids)},
			{0, ascending, len(ids)}, {0, descending, 0},
		} {
			key := test.key
			request := ListRequest{PageSize: maximumListPageSize, SortBy: sortBy, SortDirection: test.direction}
			request.PageToken = adversarialToken(t, request, listCursor{SavedSearch: ids[0], IntegerKey: &key})
			page, err := adversarialList(t, store, request)
			if err != nil {
				t.Fatalf("List(%v/%v, key %d) error = %v", sortBy, test.direction, test.key, err)
			}
			if len(page.SavedSearches) != test.want || page.NextPageToken != nil {
				t.Fatalf("List(%v/%v, key %d) = %d rows (next %v), want %d rows and no next page",
					sortBy, test.direction, test.key, len(page.SavedSearches), page.NextPageToken, test.want)
			}
		}
	}
}

// tiedOrder is the total order the collapsed helpers must produce over the
// openTiedStore fixture: sort key first, saved_search_id as the only tiebreak.
func tiedOrder(
	ids []string,
	sortBy opensplunk.SavedSearchSortBy,
	direction opensplunk.SortDirection,
) []string {
	names := make(map[string]string, len(ids))
	for position, id := range ids {
		names[id] = adversarialNames[position]
	}
	want := slices.Clone(ids)
	slices.SortFunc(want, func(left, right string) int {
		if sortBy == opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME &&
			names[left] != names[right] {
			return strings.Compare(names[left], names[right])
		}
		return strings.Compare(left, right)
	})
	if direction == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		slices.Reverse(want)
	}
	return want
}

// TestListKeysetSurvivesTotalKeyTies walks every sort field and direction one
// row at a time over a constant sort key, proving the single collapsed
// predicate still emits each row exactly once.
func TestListKeysetSurvivesTotalKeyTies(t *testing.T) {
	store, ids := openTiedStore(t)
	for _, sortBy := range adversarialSorts {
		for _, direction := range adversarialDirections {
			want := tiedOrder(ids, sortBy, direction)
			request := ListRequest{PageSize: 1, SortBy: sortBy, SortDirection: direction}
			got := make([]string, 0, len(want))
			for range len(want) + 2 {
				page, err := adversarialList(t, store, request)
				if err != nil {
					t.Fatalf("List(%v/%v) error = %v", sortBy, direction, err)
				}
				for _, savedSearch := range page.SavedSearches {
					got = append(got, savedSearch.SavedSearchId)
				}
				if page.NextPageToken == nil {
					break
				}
				request.PageToken = *page.NextPageToken
			}
			if !slices.Equal(got, want) {
				t.Fatalf("List(%v/%v) page order = %v, want %v", sortBy, direction, got, want)
			}
		}
	}
}

// TestListRejectsCursorReplayAcrossSortAndDirection replays every issued token
// against every other sort field and direction: with the per-combination
// branches gone, the filter fingerprint alone stops a foreign reinterpretation.
func TestListRejectsCursorReplayAcrossSortAndDirection(t *testing.T) {
	store, _ := openTiedStore(t)
	type issued struct {
		request ListRequest
		token   string
	}
	tokens := make([]issued, 0, len(adversarialSorts)*len(adversarialDirections))
	for _, sortBy := range adversarialSorts {
		for _, direction := range adversarialDirections {
			request := ListRequest{PageSize: 1, SortBy: sortBy, SortDirection: direction}
			page, err := adversarialList(t, store, request)
			if err != nil || page.NextPageToken == nil {
				t.Fatalf("List(%v/%v) = (%+v, %v), want a next page token", sortBy, direction, page, err)
			}
			tokens = append(tokens, issued{request, *page.NextPageToken})
		}
	}
	for _, source := range tokens {
		for _, target := range tokens {
			replay := target.request
			replay.PageToken = source.token
			_, err := adversarialList(t, store, replay)
			same := source.request.SortBy == target.request.SortBy &&
				source.request.SortDirection == target.request.SortDirection
			if same && err != nil {
				t.Fatalf("List(%v own token) error = %v", replay, err)
			}
			if !same && !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("List(%v/%v with %v/%v token) error = %v, want ErrInvalidArgument",
					target.request.SortBy, target.request.SortDirection,
					source.request.SortBy, source.request.SortDirection, err)
			}
		}
	}
}

// TestListConcurrentSortsDoNotShareQueryState hammers one Store with every
// sort/direction pair at once. The collapsed helpers now build their predicate
// and ORDER BY by formatting a shared handle, so a leaked gorm clause would
// show up as one goroutine's ordering appearing in another's page.
func TestListConcurrentSortsDoNotShareQueryState(t *testing.T) {
	store, ids := openTiedStore(t)
	const rounds = 12
	var group sync.WaitGroup
	failures := make(chan string, rounds*len(adversarialSorts)*len(adversarialDirections))
	for range rounds {
		for _, sortBy := range adversarialSorts {
			for _, direction := range adversarialDirections {
				group.Go(func() {
					want := tiedOrder(ids, sortBy, direction)
					page, err := store.List(context.Background(), AccessScope{OwnerID: adversarialOwner}, ListRequest{
						PageSize: uint32(len(ids)), SortBy: sortBy, SortDirection: direction, IncludeTotal: true,
					})
					if err != nil {
						failures <- fmt.Sprintf("List(%v/%v) error = %v", sortBy, direction, err)
						return
					}
					got := make([]string, 0, len(want))
					for _, savedSearch := range page.SavedSearches {
						got = append(got, savedSearch.SavedSearchId)
					}
					if !slices.Equal(got, want) || page.TotalSize == nil || *page.TotalSize != uint64(len(ids)) {
						failures <- fmt.Sprintf("List(%v/%v) = %v (total %v), want %v",
							sortBy, direction, got, page.TotalSize, want)
					}
				})
			}
		}
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
}
