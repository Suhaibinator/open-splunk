package searchhistory

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var testCursorKey = []byte("search-history-test-cursor-key-32-bytes-minimum")

func openTestStore(t *testing.T, options Options) (*control.DB, *Store) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	if options.CursorKey == nil {
		options.CursorKey = testCursorKey
	}
	store, err := New(database, options)
	if err != nil {
		_ = database.Close()
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database, store
}

func historyEntry(id, spl, app string, state opensplunkv1.SearchJobState, created time.Time) *opensplunkv1.SearchHistoryEntry {
	appID := app
	finished := created.Add(5 * time.Second)
	entry := &opensplunkv1.SearchHistoryEntry{
		SearchJobId: id,
		Definition: &opensplunkv1.SearchDefinition{
			Spl:        spl,
			AppId:      &appID,
			IndexScope: []string{"main"},
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: stringPointer("-15m"),
				Latest:   stringPointer("now"),
			},
		},
		Source:              &opensplunkv1.SearchJobSource{Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC},
		EffectiveIndexScope: []string{"main"},
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: timestamppb.New(created.Add(-15 * time.Minute)),
			Latest:   timestamppb.New(created),
			Timezone: "UTC",
		},
		FinalState:      state,
		MatchedEvents:   12,
		ScannedRows:     20,
		ScannedBytes:    2_000,
		ProducedRows:    12,
		Duration:        durationpb.New(5 * time.Second),
		CompilerVersion: "0.1",
		CreatedAt:       timestamppb.New(created),
		StartedAt:       timestamppb.New(created.Add(time.Second)),
		FinishedAt:      timestamppb.New(finished),
	}
	if state == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED {
		entry.Failure = &opensplunkv1.SearchFailure{
			Code:    opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION,
			Message: "search execution failed",
		}
	}
	return entry
}

func TestNewValidatesOptionsAndClonesCursorKey(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	if _, err := New(nil, Options{CursorKey: testCursorKey}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(nil) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := New(database, Options{CursorKey: make([]byte, 31)}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(short key) error = %v, want ErrInvalidArgument", err)
	}
	for _, options := range []Options{
		{CursorKey: testCursorKey, MaximumAge: -time.Second},
		{CursorKey: testCursorKey, MaximumEntriesPerOwner: -1},
	} {
		if _, err := New(database, options); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("New(%+v) error = %v, want ErrInvalidArgument", options, err)
		}
	}
	key := slices.Clone(testCursorKey)
	store, err := New(database, Options{CursorKey: key})
	if err != nil {
		t.Fatal(err)
	}
	key[0] ^= 0xff
	if store.cursorKey[0] == key[0] {
		t.Fatal("New retained caller cursor-key storage")
	}
}

func TestRecordGetIsImmutableDeterministicScopedAndDetached(t *testing.T) {
	database, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: " tenant ", OwnerID: " owner "}
	created := time.Date(2026, time.July, 22, 12, 0, 0, 123_456_789, time.UTC)
	input := historyEntry("job-1", " index=main | head 10 ", " search ", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)
	recorded, err := store.Record(ctx, scope, input)
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if recorded.Definition.Spl != " index=main | head 10 " || recorded.Definition.GetAppId() != "search" {
		t.Fatalf("Record() did not normalize definition: %+v", recorded.Definition)
	}
	if recorded.CreatedAt.Nanos != 123_456_000 {
		t.Fatalf("created_at nanos = %d, want 123456000", recorded.CreatedAt.Nanos)
	}
	input.Definition.Spl = "mutated input"
	recorded.Definition.Spl = "mutated output"
	got, err := store.Get(ctx, AccessScope{TenantID: "tenant", OwnerID: "owner"}, "job-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Definition.Spl != " index=main | head 10 " {
		t.Fatalf("stored entry aliased caller: %+v", got)
	}

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	var stored []byte
	var checksum []byte
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT entry_proto, entry_sha256 FROM search_history WHERE search_job_id = ?`, "job-1").Scan(&stored, &checksum); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stored, encoded) || !slices.Equal(checksum, digest[:]) {
		t.Fatal("history entry was not stored deterministically with its checksum")
	}

	if _, err := store.Get(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, "job-1"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Get() error = %v, want ErrNotFound", err)
	}
	if _, err := store.Get(ctx, AccessScope{TenantID: "other", OwnerID: "owner"}, "job-1"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-tenant Get() error = %v, want ErrNotFound", err)
	}

	// Retrying the exact terminal callback is idempotent. A different terminal
	// snapshot cannot silently rewrite history.
	if _, err := store.Record(ctx, scope, got); err != nil {
		t.Fatalf("idempotent Record() error = %v", err)
	}
	changed := proto.Clone(got).(*opensplunkv1.SearchHistoryEntry)
	changed.MatchedEvents++
	if _, err := store.Record(ctx, scope, changed); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("changed Record() error = %v, want ErrVersionConflict", err)
	}
	if _, err := store.Record(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, got); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("cross-owner Record() error = %v, want ErrAlreadyExists", err)
	}
}

func TestRecordValidationRejectsUnsafeOrIncompleteEntries(t *testing.T) {
	_, store := openTestStore(t, Options{})
	valid := historyEntry("job", "index=main", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, time.Now().UTC())
	tests := []struct {
		name   string
		mutate func(*opensplunkv1.SearchHistoryEntry)
	}{
		{name: "missing definition", mutate: func(entry *opensplunkv1.SearchHistoryEntry) { entry.Definition = nil }},
		{name: "empty spl", mutate: func(entry *opensplunkv1.SearchHistoryEntry) { entry.Definition.Spl = " \n" }},
		{name: "nonterminal", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING
		}},
		{name: "bad resolved range", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.ResolvedTimeRange.Latest = entry.ResolvedTimeRange.Earliest
		}},
		{name: "negative duration", mutate: func(entry *opensplunkv1.SearchHistoryEntry) { entry.Duration = durationpb.New(-time.Second) }},
		{name: "duration overflow", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Duration = &durationpb.Duration{Seconds: int64(^uint64(0)>>1)/int64(time.Second) + 1}
		}},
		{name: "finished before created", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.FinishedAt = timestamppb.New(entry.CreatedAt.AsTime().Add(-time.Second))
		}},
		{name: "source ID mismatch", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Source.SavedSearchId = stringPointer("saved-unrelated")
		}},
		{name: "too many indexes", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.EffectiveIndexScope = make([]string, maximumIndexScope+1)
		}},
		{name: "failure secret-sized", mutate: func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Failure = &opensplunkv1.SearchFailure{
				Code:    opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_EXECUTION,
				Message: string(make([]byte, maximumFailureMessageBytes+1)),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := proto.Clone(valid).(*opensplunkv1.SearchHistoryEntry)
			test.mutate(entry)
			if _, err := store.Record(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, entry); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Record() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if _, err := store.Record(context.Background(), AccessScope{}, valid); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Record(empty scope) error = %v, want ErrInvalidArgument", err)
	}
}

func TestRecordRejectsUnknownProtobufFieldsRecursively(t *testing.T) {
	_, store := openTestStore(t, Options{})
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for name, mutate := range map[string]func(*opensplunkv1.SearchHistoryEntry){
		"root": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
		},
		"nested": func(entry *opensplunkv1.SearchHistoryEntry) {
			entry.Definition.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
		},
	} {
		t.Run(name, func(t *testing.T) {
			entry := historyEntry("unknown-"+name, "index=main", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, time.Now().UTC())
			mutate(entry)
			if _, err := store.Record(context.Background(), scope, entry); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Record() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestIdleOwnerHistoryIsPrunedOnRead(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	database, store := openTestStore(t, Options{
		Clock: func() time.Time { return now }, MaximumAge: time.Hour,
	})
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	if _, err := store.Record(context.Background(), scope, historyEntry(
		"idle", "  index=main | head 1\n", "search",
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, now,
	)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	page, err := store.List(context.Background(), scope, ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("expired entries = %v, want none", entryIDs(page.Entries))
	}
	var rows int
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history WHERE search_job_id = 'idle'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("read path mutated expired on-disk rows = %d, want 1", rows)
	}
	deleted, err := store.Prune(context.Background(), scope)
	if err != nil || deleted != 1 {
		t.Fatalf("Prune() = (%d,%v), want (1,nil)", deleted, err)
	}
	if err := database.SQLDB().QueryRow(`SELECT COUNT(*) FROM search_history WHERE search_job_id = 'idle'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("expired rows after Prune = %d, want 0", rows)
	}
}

func TestListKeysetPaginationFiltersSortsAndBindsCursor(t *testing.T) {
	_, store := openTestStore(t, Options{})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	base := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	entries := []*opensplunkv1.SearchHistoryEntry{
		historyEntry("job-a", "index=main error", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, base),
		historyEntry("job-b", "index=main warning", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, base.Add(time.Minute)),
		historyEntry("job-c", "index=audit error", "audit", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED, base.Add(2*time.Minute)),
		historyEntry("job-d", "index=main Error timeout", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, base.Add(3*time.Minute)),
	}
	savedID := "saved-1"
	entries[3].Source = &opensplunkv1.SearchJobSource{Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH, SavedSearchId: &savedID}
	entries[0].MatchedEvents = 2
	entries[1].MatchedEvents = 20
	entries[2].MatchedEvents = 1
	entries[3].MatchedEvents = 10
	for _, entry := range entries {
		if _, err := store.Record(ctx, scope, entry); err != nil {
			t.Fatalf("Record(%s) error = %v", entry.SearchJobId, err)
		}
	}
	if _, err := store.Record(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, historyEntry("job-other", "error", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, base)); err != nil {
		t.Fatal(err)
	}

	request := ListRequest{PageSize: 2, IncludeTotal: true}
	var ids []string
	for {
		page, err := store.List(ctx, scope, request)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if page.TotalSize == nil || *page.TotalSize != 4 || !page.TotalSizeExact {
			t.Fatalf("List total = (%v,%v), want exact 4", page.TotalSize, page.TotalSizeExact)
		}
		for _, entry := range page.Entries {
			ids = append(ids, entry.SearchJobId)
		}
		if page.NextPageToken == nil {
			break
		}
		request.PageToken = *page.NextPageToken
	}
	if !slices.Equal(ids, []string{"job-d", "job-c", "job-b", "job-a"}) {
		t.Fatalf("default pages = %v", ids)
	}

	app := "search"
	text := "ERROR"
	filtered := ListRequest{
		PageSize:    10,
		AppIDFilter: &app,
		TextFilter:  &text,
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
		},
		SortBy:        opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
	}
	page, err := store.List(ctx, scope, filtered)
	if err != nil {
		t.Fatal(err)
	}
	if got := entryIDs(page.Entries); !slices.Equal(got, []string{"job-d"}) {
		t.Fatalf("filtered List() IDs = %v", got)
	}

	savedPage, err := store.List(ctx, scope, ListRequest{SavedSearchIDFilter: &savedID, PageSize: 10})
	if err != nil || !slices.Equal(entryIDs(savedPage.Entries), []string{"job-d"}) {
		t.Fatalf("saved-search List() = (%v,%v)", entryIDs(savedPage.Entries), err)
	}

	first, err := store.List(ctx, scope, ListRequest{PageSize: 1})
	if err != nil || first.NextPageToken == nil {
		t.Fatalf("first page = (%+v,%v)", first, err)
	}
	if _, err := store.List(ctx, scope, ListRequest{PageSize: 1, PageToken: *first.NextPageToken, AppIDFilter: &app}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("cursor replay with changed filter error = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.List(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, ListRequest{PageSize: 1, PageToken: *first.NextPageToken}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("cross-owner cursor replay error = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.List(ctx, scope, ListRequest{PageSize: maximumPageSize + 1}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("oversized page error = %v, want ErrInvalidArgument", err)
	}
}

func TestListFilterQueriesUseCompositeIndexes(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for name, request := range map[string]ListRequest{
		"app":   {AppIDFilter: stringPointer("search")},
		"saved": {SavedSearchIDFilter: stringPointer("saved-1")},
	} {
		t.Run(name, func(t *testing.T) {
			normalized, err := normalizeListRequest(scope, request)
			if err != nil {
				t.Fatal(err)
			}
			floor := time.Now().Add(-time.Hour).UnixMicro()
			normalized.filter.retentionFloor = &floor
			query, args := listQuery(normalized, listCursor{})
			rows, err := database.SQLDB().Query(`EXPLAIN QUERY PLAN `+query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var details []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				details = append(details, detail)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			wantIndex := "search_history_owner_" + name + "_created_idx"
			if !strings.Contains(strings.Join(details, "\n"), wantIndex) {
				t.Fatalf("query plan = %v, want %s", details, wantIndex)
			}
		})
	}
}

func TestDeleteClearPruneAndCorruptionDetection(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	database, store := openTestStore(t, Options{
		Clock:                  func() time.Time { return now },
		MaximumAge:             2 * time.Hour,
		MaximumEntriesPerOwner: 2,
	})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for index, created := range []time.Time{now.Add(-3 * time.Hour), now.Add(-90 * time.Minute), now.Add(-30 * time.Minute), now} {
		id := fmt.Sprintf("job-%d", index)
		if _, err := store.Record(ctx, scope, historyEntry(id, "index=main", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, created)); err != nil {
			t.Fatalf("Record(%s) error = %v", id, err)
		}
	}
	page, err := store.List(ctx, scope, ListRequest{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := entryIDs(page.Entries); !slices.Equal(got, []string{"job-3", "job-2"}) {
		t.Fatalf("bounded history IDs = %v", got)
	}
	if err := store.Delete(ctx, scope, "job-2"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := store.Delete(ctx, scope, "job-2"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Delete(missing) error = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, AccessScope{TenantID: "tenant", OwnerID: "other"}, "job-3"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Delete() error = %v, want ErrNotFound", err)
	}

	for _, entry := range []*opensplunkv1.SearchHistoryEntry{
		historyEntry("clear-a", "index=main alpha", "search", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, now),
		historyEntry("clear-b", "index=audit beta", "audit", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, now),
	} {
		if _, err := store.Record(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
	}
	app := "search"
	deleted, err := store.Clear(ctx, scope, Filter{AppID: &app})
	if err != nil || deleted != 1 {
		t.Fatalf("Clear() = (%d,%v), want (1,nil)", deleted, err)
	}

	if _, err := database.SQLDB().ExecContext(ctx, `UPDATE search_history SET entry_proto = X'00' WHERE search_job_id = 'clear-b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, scope, "clear-b"); err == nil || errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(corrupt) error = %v, want unavailable/corruption error", err)
	}
}

func entryIDs(entries []*opensplunkv1.SearchHistoryEntry) []string {
	ids := make([]string, len(entries))
	for index, entry := range entries {
		ids[index] = entry.SearchJobId
	}
	return ids
}

func stringPointer(value string) *string { return &value }
