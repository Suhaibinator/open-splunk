package searchhistory

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type modelIndexColumn struct {
	name       string
	descending bool
}

func TestGORMModelsMatchMigratedSearchHistorySchema(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t, Options{})
	tests := []struct {
		table       string
		model       any
		indexes     map[string][]modelIndexColumn
		checks      map[string]string
		primaryKeys []string
	}{
		{
			table:       "search_history",
			model:       &historyRecord{},
			primaryKeys: []string{"search_job_id"},
			indexes: map[string][]modelIndexColumn{
				"search_history_created_idx": {
					{name: "created_at_unix_micro"}, {name: "search_job_id"},
				},
				"search_history_owner_created_idx": {
					{name: "tenant_id"}, {name: "owner_id"},
					{name: "created_at_unix_micro", descending: true},
					{name: "search_job_id", descending: true},
				},
				"search_history_owner_finished_idx": {
					{name: "tenant_id"}, {name: "owner_id"},
					{name: "finished_at_unix_micro", descending: true},
					{name: "search_job_id", descending: true},
				},
				"search_history_owner_duration_idx": {
					{name: "tenant_id"}, {name: "owner_id"},
					{name: "duration_nanoseconds", descending: true},
					{name: "search_job_id", descending: true},
				},
				"search_history_owner_matched_idx": {
					{name: "tenant_id"}, {name: "owner_id"},
					{name: "matched_events", descending: true},
					{name: "search_job_id", descending: true},
				},
				"search_history_owner_app_created_idx": {
					{name: "tenant_id"}, {name: "owner_id"}, {name: "app_id"},
					{name: "created_at_unix_micro", descending: true},
					{name: "search_job_id", descending: true},
				},
				"search_history_owner_saved_created_idx": {
					{name: "tenant_id"}, {name: "owner_id"}, {name: "saved_search_id"},
					{name: "created_at_unix_micro", descending: true},
					{name: "search_job_id", descending: true},
				},
			},
			checks: map[string]string{
				"search_history_job_id_length":          "length(search_job_id) BETWEEN 1 AND 256",
				"search_history_tenant_id_length":       "length(tenant_id) BETWEEN 1 AND 1024",
				"search_history_owner_id_length":        "length(owner_id) BETWEEN 1 AND 255",
				"search_history_app_id_length":          "length(app_id) <= 255",
				"search_history_saved_search_id_length": "length(saved_search_id) <= 128",
				"search_history_final_state_terminal":   "final_state BETWEEN 6 AND 9",
				"search_history_search_text_length":     "length(search_text) BETWEEN 1 AND 65536",
				"search_history_finish_not_before_create": "finished_at_unix_micro >= " +
					"created_at_unix_micro",
				"search_history_duration_nonnegative": "duration_nanoseconds >= 0",
				"search_history_matched_nonnegative":  "matched_events >= 0",
				"search_history_entry_proto_length":   "length(entry_proto) BETWEEN 1 AND 524288",
				"search_history_entry_sha256_length":  "length(entry_sha256) = 32",
			},
		},
		{
			table:       "search_history_owner_counts",
			model:       &historyOwnerCountRecord{},
			indexes:     map[string][]modelIndexColumn{},
			primaryKeys: []string{"tenant_id", "owner_id"},
			checks: map[string]string{
				"search_history_owner_count_tenant_id_length": "length(tenant_id) BETWEEN 1 AND 1024",
				"search_history_owner_count_owner_id_length":  "length(owner_id) BETWEEN 1 AND 255",
				"search_history_owner_count_positive":         "terminal_count > 0",
			},
		},
		{
			table:       "search_history_pending",
			model:       &pendingHistoryRecord{},
			primaryKeys: []string{"search_job_id"},
			indexes: map[string][]modelIndexColumn{
				"search_history_pending_owner_created_idx": {
					{name: "tenant_id"}, {name: "owner_id"},
					{name: "created_at_unix_micro"}, {name: "search_job_id"},
				},
			},
			checks: map[string]string{
				"search_history_pending_job_id_length":       "length(search_job_id) BETWEEN 1 AND 256",
				"search_history_pending_tenant_id_length":    "length(tenant_id) BETWEEN 1 AND 1024",
				"search_history_pending_owner_id_length":     "length(owner_id) BETWEEN 1 AND 255",
				"search_history_pending_state_nonterminal":   "state BETWEEN 1 AND 5",
				"search_history_pending_entry_proto_length":  "length(entry_proto) BETWEEN 1 AND 524288",
				"search_history_pending_entry_sha256_length": "length(entry_sha256) = 32",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			statement := &gorm.Statement{DB: database.GORMDB()}
			if err := statement.Parse(test.model); err != nil {
				t.Fatalf("parse GORM model: %v", err)
			}
			type columnRow struct {
				Name string
			}
			var migratedColumns []columnRow
			columns := database.GORMDB().Raw(
				fmt.Sprintf("SELECT name FROM pragma_table_info('%s') ORDER BY cid", test.table),
			).Scan(&migratedColumns)
			if columns.Error != nil {
				t.Fatalf("read migrated columns: %v", columns.Error)
			}
			columnNames := make([]string, len(migratedColumns))
			for index, column := range migratedColumns {
				columnNames[index] = column.Name
			}
			if !slices.Equal(statement.Schema.DBNames, columnNames) {
				t.Fatalf("GORM columns = %v, migrated columns = %v", statement.Schema.DBNames, columnNames)
			}
			primaryKeys := make([]string, len(statement.Schema.PrimaryFields))
			for index, field := range statement.Schema.PrimaryFields {
				primaryKeys[index] = field.DBName
			}
			if !slices.Equal(primaryKeys, test.primaryKeys) {
				t.Fatalf(
					"GORM primary keys = %v, want %v",
					primaryKeys,
					test.primaryKeys,
				)
			}
			assertSearchHistoryIndexesMatchModel(t, database, statement, test.indexes)
			assertSearchHistoryChecksMatchModel(t, database, statement, test.table, test.checks)
		})
	}
}

func assertSearchHistoryChecksMatchModel(
	t *testing.T,
	database *control.DB,
	statement *gorm.Statement,
	table string,
	expected map[string]string,
) {
	t.Helper()
	checks := statement.Schema.ParseCheckConstraints()
	if len(checks) != len(expected) {
		t.Fatalf("GORM checks = %v, want exactly %v", checks, expected)
	}
	for name, expression := range expected {
		check, ok := checks[name]
		if !ok {
			t.Errorf("GORM check %s is missing", name)
			continue
		}
		if check.Constraint != expression {
			t.Errorf("GORM check %s = %q, want %q", name, check.Constraint, expression)
		}
	}
	type schemaRow struct {
		Definition string `gorm:"column:definition"`
	}
	var migrated schemaRow
	query := database.GORMDB().Raw(
		`SELECT sql AS definition FROM sqlite_schema WHERE type = 'table' AND name = ?`,
		table,
	).Scan(&migrated)
	if query.Error != nil {
		t.Fatalf("read migrated table definition: %v", query.Error)
	}
	for name, expression := range expected {
		if !strings.Contains(migrated.Definition, "CHECK ("+expression+")") {
			t.Errorf("migrated table does not contain GORM check %s expression %q", name, expression)
		}
	}
}

func assertSearchHistoryIndexesMatchModel(
	t *testing.T,
	database *control.DB,
	statement *gorm.Statement,
	expected map[string][]modelIndexColumn,
) {
	t.Helper()
	modelIndexes := make(map[string][]modelIndexColumn)
	for _, index := range statement.Schema.ParseIndexes() {
		fields := make([]modelIndexColumn, len(index.Fields))
		for fieldIndex, option := range index.Fields {
			fields[fieldIndex] = modelIndexColumn{
				name:       option.DBName,
				descending: strings.EqualFold(option.Sort, "DESC"),
			}
		}
		modelIndexes[index.Name] = fields
	}
	if len(modelIndexes) != len(expected) {
		t.Fatalf("GORM indexes = %v, want exactly %v", modelIndexes, expected)
	}
	for name, want := range expected {
		if got := modelIndexes[name]; !slices.Equal(got, want) {
			t.Errorf("GORM index %s columns = %v, want %v", name, got, want)
		}
		type migratedIndexColumn struct {
			Name       string
			Descending int64 `gorm:"column:descending"`
		}
		var migrated []migratedIndexColumn
		indexQuery := database.GORMDB().Raw(
			fmt.Sprintf(
				`SELECT name, "desc" AS descending
				 FROM pragma_index_xinfo('%s')
				 WHERE "key" = 1
				 ORDER BY seqno`,
				name,
			),
		).Scan(&migrated)
		if indexQuery.Error != nil {
			t.Fatalf("read migrated index %s: %v", name, indexQuery.Error)
		}
		got := make([]modelIndexColumn, len(migrated))
		for columnIndex, column := range migrated {
			got[columnIndex] = modelIndexColumn{
				name:       column.Name,
				descending: column.Descending == 1,
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("migrated index %s columns = %v, want %v", name, got, want)
		}
	}
}

func TestGORMStoreReopenPreservesTerminalHistoryAndCursor(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "search-history-reopen.sqlite")
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	options := Options{Clock: func() time.Time { return now }, CursorKey: testCursorKey}

	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(database, options)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	for index, id := range []string{"reopen-a", "reopen-b", "reopen-c"} {
		entry := historyEntry(
			id,
			"index=main | head 1",
			"search",
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now.Add(time.Duration(index-3)*time.Minute),
		)
		if _, err := store.Record(ctx, scope, entry); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	first, err := store.List(ctx, scope, ListRequest{PageSize: 1})
	if err != nil || first.NextPageToken == nil || len(first.Entries) != 1 {
		_ = database.Close()
		t.Fatalf("first page = (%+v, %v)", first, err)
	}
	first.Entries[0].Definition.Spl = "mutated detached page"
	token := *first.NextPageToken
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedStore, err := New(reopened, options)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopenedStore.Get(ctx, scope, "reopen-c")
	if err != nil {
		t.Fatal(err)
	}
	if got.Definition.GetSpl() != "index=main | head 1" {
		t.Fatalf("reopened entry aliased prior result: %+v", got)
	}
	second, err := reopenedStore.List(ctx, scope, ListRequest{PageSize: 1, PageToken: token})
	if err != nil {
		t.Fatal(err)
	}
	if ids := entryIDs(second.Entries); !slices.Equal(ids, []string{"reopen-b"}) {
		t.Fatalf("page after reopen = %v, want [reopen-b]", ids)
	}
}

func TestGORMListKeysetPaginationPreservesEverySortAndTieBreaker(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	_, store := openTestStore(t, Options{Clock: func() time.Time { return base.Add(time.Hour) }})
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	entries := []*opensplunk.SearchHistoryEntry{
		historyEntry("sort-a", "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, base),
		historyEntry("sort-b", "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, base),
		historyEntry("sort-c", "index=main", "search", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, base.Add(time.Minute)),
	}
	entries[0].FinishedAt = timestamppb.New(base.Add(20 * time.Second))
	entries[1].FinishedAt = timestamppb.New(base.Add(10 * time.Second))
	entries[2].FinishedAt = timestamppb.New(base.Add(70 * time.Second))
	entries[0].Duration = durationpb.New(20 * time.Second)
	entries[1].Duration = durationpb.New(10 * time.Second)
	entries[2].Duration = durationpb.New(10 * time.Second)
	entries[0].MatchedEvents = 2
	entries[1].MatchedEvents = 2
	entries[2].MatchedEvents = 1
	for _, entry := range entries {
		if _, err := store.Record(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		sortBy    opensplunk.SearchHistorySortBy
		direction opensplunk.SortDirection
		want      []string
	}{
		{
			name: "created ascending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
			direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			want:      []string{"sort-a", "sort-b", "sort-c"},
		},
		{
			name: "created descending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
			direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			want:      []string{"sort-c", "sort-b", "sort-a"},
		},
		{
			name: "finished ascending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT,
			direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			want:      []string{"sort-b", "sort-a", "sort-c"},
		},
		{
			name: "finished descending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT,
			direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			want:      []string{"sort-c", "sort-a", "sort-b"},
		},
		{
			name: "duration ascending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION,
			direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			want:      []string{"sort-b", "sort-c", "sort-a"},
		},
		{
			name: "duration descending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION,
			direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			want:      []string{"sort-a", "sort-c", "sort-b"},
		},
		{
			name: "matched ascending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS,
			direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
			want:      []string{"sort-c", "sort-a", "sort-b"},
		},
		{
			name: "matched descending", sortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS,
			direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			want:      []string{"sort-b", "sort-a", "sort-c"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := ListRequest{PageSize: 1, SortBy: test.sortBy, SortDirection: test.direction}
			var got []string
			for len(got) <= len(test.want) {
				page, err := store.List(ctx, scope, request)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, entryIDs(page.Entries)...)
				if page.NextPageToken == nil {
					break
				}
				request.PageToken = *page.NextPageToken
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("keyset order = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGORMConcurrentPendingAdmissionPreservesCapacityAndIdempotency(t *testing.T) {
	_, store := openTestStore(t, Options{MaximumEntriesPerOwner: 1})
	ctx := context.Background()
	created := time.Now().UTC()

	const workers = 12
	run := func(scope AccessScope, entries []*pendingHistoryRecord) []error {
		t.Helper()
		start := make(chan struct{})
		results := make(chan error, len(entries))
		var wait sync.WaitGroup
		for _, record := range entries {
			wait.Go(func() {
				<-start
				entry, _, err := decodePendingEntry(record.EntryProto, record.EntrySHA256)
				if err == nil {
					_, err = store.BeginAttempt(ctx, scope, entry)
				}
				results <- err
			})
		}
		close(start)
		wait.Wait()
		close(results)
		errs := make([]error, 0, len(entries))
		for err := range results {
			errs = append(errs, err)
		}
		return errs
	}
	pendingRecords := make([]*pendingHistoryRecord, workers)
	for index := range pendingRecords {
		entry := pendingHistoryEntry(fmt.Sprintf("capacity-race-%02d", index), "index=main", created)
		_, indexed, err := normalizePendingEntry(entry)
		if err != nil {
			t.Fatal(err)
		}
		pendingRecords[index] = &pendingHistoryRecord{
			EntryProto: slices.Clone(indexed.encoded), EntrySHA256: slices.Clone(indexed.checksum[:]),
		}
	}
	errs := run(AccessScope{TenantID: "tenant", OwnerID: "capacity-owner"}, pendingRecords)
	var admitted, full int
	for _, err := range errs {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrCapacity):
			full++
		default:
			t.Fatalf("concurrent capacity admission error = %v", err)
		}
	}
	if admitted != 1 || full != workers-1 {
		t.Fatalf("concurrent admissions = %d admitted, %d full; want 1 and %d", admitted, full, workers-1)
	}

	entry := pendingHistoryEntry("idempotent-race", "index=main", created)
	_, indexed, err := normalizePendingEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	idempotentRecords := make([]*pendingHistoryRecord, workers)
	for index := range idempotentRecords {
		idempotentRecords[index] = &pendingHistoryRecord{
			EntryProto: slices.Clone(indexed.encoded), EntrySHA256: slices.Clone(indexed.checksum[:]),
		}
	}
	for _, err := range run(AccessScope{TenantID: "tenant", OwnerID: "idempotent-owner"}, idempotentRecords) {
		if err != nil {
			t.Fatalf("concurrent idempotent admission error = %v", err)
		}
	}
	var count int64
	query := store.orm.Model(&pendingHistoryRecord{}).
		Where("tenant_id = ? AND owner_id = ?", "tenant", "idempotent-owner").
		Count(&count)
	if query.Error != nil {
		t.Fatal(query.Error)
	}
	if count != 1 {
		t.Fatalf("idempotent pending rows = %d, want 1", count)
	}
}
