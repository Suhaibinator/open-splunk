package control

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

	"gorm.io/gorm"
)

func TestListIndexPageUsesFilteredRevisionStableKeysets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	definitions := []IndexDefinition{
		{
			Name:             "alpha",
			DisplayName:      "Alpha",
			Description:      "development",
			IngestionEnabled: true,
			SearchEnabled:    true,
		},
		{
			Name:             "bravo",
			DisplayName:      "Bravo",
			Description:      "Production audit",
			IngestionEnabled: true,
			SearchEnabled:    true,
		},
		{
			Name:             "charlie",
			DisplayName:      "Production Charlie",
			Description:      "application logs",
			IngestionEnabled: true,
			SearchEnabled:    true,
		},
		{
			Name:             "delta",
			DisplayName:      "Production Delta",
			Description:      "terminal",
			IngestionEnabled: true,
			SearchEnabled:    true,
		},
	}
	created := make([]Index, 0, len(definitions))
	for _, definition := range definitions {
		record, err := db.CreateIndex(ctx, definition)
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", definition.Name, err)
		}
		created = append(created, record)
	}
	bravo, err := db.SetIndexState(
		ctx,
		created[1].ID,
		created[1].Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive bravo: %v", err)
	}
	created[1] = bravo
	delta, err := db.SetIndexState(
		ctx,
		created[3].ID,
		created[3].Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive delta: %v", err)
	}
	if _, err := db.DeleteIndex(
		ctx,
		delta.ID,
		delta.Version,
		delta.Definition.Name,
	); err != nil {
		t.Fatalf("tombstone delta: %v", err)
	}

	text := "PRODUCTION"
	request := IndexListRequest{
		PageSize:     1,
		IncludeTotal: true,
		StateFilters: []IndexState{
			IndexStateArchived,
			IndexStateActive,
			IndexStateArchived,
		},
		TextFilter: &text,
		SortBy:     IndexSortByName,
		Direction:  IndexSortAscending,
	}
	first, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(first): %v", err)
	}
	if got := indexNames(first.Indexes); !slices.Equal(got, []string{"bravo"}) {
		t.Fatalf("first names = %v, want [bravo]", got)
	}
	if first.TotalSize == nil || *first.TotalSize != 2 ||
		!first.TotalSizeExact || first.NextCursor == nil ||
		first.CatalogRevision == 0 {
		t.Fatalf("first page metadata = %#v", first)
	}

	request.Cursor = first.NextCursor
	second, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(second): %v", err)
	}
	if got := indexNames(second.Indexes); !slices.Equal(got, []string{"charlie"}) {
		t.Fatalf("second names = %v, want [charlie]", got)
	}
	if second.TotalSize == nil || *second.TotalSize != 2 ||
		!second.TotalSizeExact || second.NextCursor != nil ||
		second.CatalogRevision != first.CatalogRevision {
		t.Fatalf("second page metadata = %#v", second)
	}

	request.Cursor = nil
	request.PageSize = 2
	request.Direction = IndexSortDescending
	descending, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(descending): %v", err)
	}
	if got := indexNames(descending.Indexes); !slices.Equal(
		got,
		[]string{"charlie", "bravo"},
	) {
		t.Fatalf("descending names = %v", got)
	}
}

func TestListIndexPageUsesIndexIDAsTimestampTieBreaker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created := make([]Index, 0, 3)
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		record, err := db.CreateIndex(ctx, enabledIndex(name))
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
		created = append(created, record)
	}
	const timestamp = int64(1_700_000_000_000_000)
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexRecord{}).
		Where("index_id IN ?", []string{
			created[0].ID,
			created[1].ID,
			created[2].ID,
		}).
		Updates(map[string]any{
			"created_at_unix_micro": timestamp,
			"updated_at_unix_micro": timestamp,
		}).Error; err != nil {
		t.Fatalf("align index timestamps: %v", err)
	}

	ascendingIDs := []string{created[0].ID, created[1].ID, created[2].ID}
	slices.Sort(ascendingIDs)
	for _, sortBy := range []IndexSortBy{
		IndexSortByCreatedAt,
		IndexSortByUpdatedAt,
	} {
		for _, direction := range []IndexSortDirection{
			IndexSortAscending,
			IndexSortDescending,
		} {
			t.Run(
				string(sortBy)+"/"+string(direction),
				func(t *testing.T) {
					wantIDs := slices.Clone(ascendingIDs)
					if direction == IndexSortDescending {
						slices.Reverse(wantIDs)
					}
					request := IndexListRequest{
						PageSize:     1,
						SortBy:       sortBy,
						Direction:    direction,
						IncludeTotal: true,
					}
					var gotIDs []string
					for {
						page, err := db.ListIndexPage(ctx, request)
						if err != nil {
							t.Fatalf("ListIndexPage(): %v", err)
						}
						if len(page.Indexes) != 1 {
							t.Fatalf("page indexes = %#v", page.Indexes)
						}
						gotIDs = append(gotIDs, page.Indexes[0].ID)
						if page.TotalSize == nil ||
							*page.TotalSize != uint64(len(wantIDs)) {
							t.Fatalf("page total = %#v", page.TotalSize)
						}
						if page.NextCursor == nil {
							break
						}
						request.Cursor = page.NextCursor
					}
					if !slices.Equal(gotIDs, wantIDs) {
						t.Fatalf(
							"timestamp tie order = %v, want %v",
							gotIDs,
							wantIDs,
						)
					}
				},
			)
		}
	}
}

func TestListIndexPageUsesSQLiteASCIITextFolding(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	definition := enabledIndex("unicode")
	definition.DisplayName = "MÜNCHEN"
	if _, err := db.CreateIndex(ctx, definition); err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}

	exactNonASCII := "MÜN"
	page, err := db.ListIndexPage(ctx, IndexListRequest{
		TextFilter: &exactNonASCII,
	})
	if err != nil {
		t.Fatalf("ListIndexPage(exact): %v", err)
	}
	if got := indexNames(page.Indexes); !slices.Equal(got, []string{"unicode"}) {
		t.Fatalf("exact non-ASCII filter = %v", got)
	}

	differentNonASCIICase := "mün"
	page, err = db.ListIndexPage(ctx, IndexListRequest{
		TextFilter: &differentNonASCIICase,
	})
	if err != nil {
		t.Fatalf("ListIndexPage(case): %v", err)
	}
	if len(page.Indexes) != 0 {
		t.Fatalf("non-ASCII case was folded by SQLite: %#v", page.Indexes)
	}

	literalDefinition := enabledIndex("literal")
	literalDefinition.Description = "percent% underscore_ quote'"
	if _, err := db.CreateIndex(ctx, literalDefinition); err != nil {
		t.Fatalf("CreateIndex(literal): %v", err)
	}
	for _, literal := range []string{"%", "_", "quote'"} {
		page, err := db.ListIndexPage(ctx, IndexListRequest{
			TextFilter: &literal,
		})
		if err != nil {
			t.Fatalf("ListIndexPage(%q): %v", literal, err)
		}
		if got := indexNames(page.Indexes); !slices.Equal(
			got,
			[]string{"literal"},
		) {
			t.Fatalf("literal filter %q = %v", literal, got)
		}
	}
}

func TestIndexListCursorValidationRejectsUnboundedKeys(t *testing.T) {
	t.Parallel()

	valid := IndexListCursor{
		CatalogRevision: 1,
		StringKey:       "alpha",
		IndexID:         "idx_alpha",
	}
	tests := map[string]IndexListCursor{
		"zero revision": func() IndexListCursor {
			value := valid
			value.CatalogRevision = 0
			return value
		}(),
		"overflow revision": func() IndexListCursor {
			value := valid
			value.CatalogRevision = math.MaxUint64
			return value
		}(),
		"oversized ID": func() IndexListCursor {
			value := valid
			value.IndexID = strings.Repeat("i", maximumIndexIDBytes+1)
			return value
		}(),
		"control ID": func() IndexListCursor {
			value := valid
			value.IndexID = "idx_\x01hidden"
			return value
		}(),
		"noncanonical name": func() IndexListCursor {
			value := valid
			value.StringKey = " Alpha "
			return value
		}(),
		"name with time": func() IndexListCursor {
			value := valid
			timeKey := int64(1)
			value.TimeKey = &timeKey
			return value
		}(),
	}
	for name, cursor := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeIndexListRequest(IndexListRequest{
				Cursor: &cursor,
			}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf(
					"normalizeIndexListRequest() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}

	for name, timeKey := range map[string]int64{
		"zero":     0,
		"overflow": maximumControlTimestampUnixMicro + 1,
	} {
		t.Run("time/"+name, func(t *testing.T) {
			cursor := IndexListCursor{
				CatalogRevision: 1,
				TimeKey:         &timeKey,
				IndexID:         "idx_alpha",
			}
			if _, err := normalizeIndexListRequest(IndexListRequest{
				SortBy: IndexSortByCreatedAt,
				Cursor: &cursor,
			}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf(
					"normalizeIndexListRequest() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}
}

func TestListIndexPageInvalidatesCursorAfterAnyCatalogMutation(t *testing.T) {
	t.Parallel()

	mutations := map[string]func(
		context.Context,
		*testing.T,
		*DB,
		Index,
	){
		"create": func(ctx context.Context, t *testing.T, db *DB, _ Index) {
			t.Helper()
			if _, err := db.CreateIndex(ctx, enabledIndex("charlie")); err != nil {
				t.Fatalf("CreateIndex(): %v", err)
			}
		},
		"update": func(ctx context.Context, t *testing.T, db *DB, target Index) {
			t.Helper()
			definition := target.Definition
			definition.DisplayName = "updated"
			if _, err := db.UpdateIndex(
				ctx,
				target.ID,
				target.Version,
				definition,
			); err != nil {
				t.Fatalf("UpdateIndex(): %v", err)
			}
		},
		"state": func(ctx context.Context, t *testing.T, db *DB, target Index) {
			t.Helper()
			if _, err := db.SetIndexState(
				ctx,
				target.ID,
				target.Version,
				IndexStateArchived,
			); err != nil {
				t.Fatalf("SetIndexState(): %v", err)
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db := openTestDB(t)
			for _, indexName := range []string{"alpha", "bravo"} {
				if _, err := db.CreateIndex(
					ctx,
					enabledIndex(indexName),
				); err != nil {
					t.Fatalf("CreateIndex(%q): %v", indexName, err)
				}
			}
			target, err := db.GetIndexByName(ctx, "bravo")
			if err != nil {
				t.Fatalf("GetIndexByName(): %v", err)
			}
			request := IndexListRequest{PageSize: 1}
			first, err := db.ListIndexPage(ctx, request)
			if err != nil {
				t.Fatalf("ListIndexPage(first): %v", err)
			}
			if first.NextCursor == nil {
				t.Fatal("first page has no cursor")
			}
			mutate(ctx, t, db, target)
			request.Cursor = first.NextCursor
			if _, err := db.ListIndexPage(
				ctx,
				request,
			); !errors.Is(err, ErrPageInvalidated) {
				t.Fatalf(
					"ListIndexPage(after mutation) error = %v, want ErrPageInvalidated",
					err,
				)
			}
		})
	}
}

func TestIndexTombstoneAdvancesRevisionAndInvalidatesCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := db.CreateIndex(ctx, enabledIndex("alpha")); err != nil {
		t.Fatalf("CreateIndex(alpha): %v", err)
	}
	target, err := db.CreateIndex(ctx, enabledIndex("bravo"))
	if err != nil {
		t.Fatalf("CreateIndex(bravo): %v", err)
	}
	target, err = db.SetIndexState(
		ctx,
		target.ID,
		target.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive target: %v", err)
	}
	request := IndexListRequest{PageSize: 1}
	first, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(first): %v", err)
	}
	if first.NextCursor == nil {
		t.Fatal("first page has no cursor")
	}
	before := first.CatalogRevision
	if _, err := db.DeleteIndex(
		ctx,
		target.ID,
		target.Version,
		target.Definition.Name,
	); err != nil {
		t.Fatalf("DeleteIndex(): %v", err)
	}
	after, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog after tombstone: %v", err)
	}
	if after.Revision != int64(before)+1 {
		t.Fatalf(
			"revision after tombstone = %d, want %d",
			after.Revision,
			before+1,
		)
	}
	request.Cursor = first.NextCursor
	if _, err := db.ListIndexPage(
		ctx,
		request,
	); !errors.Is(err, ErrPageInvalidated) {
		t.Fatalf(
			"ListIndexPage(after tombstone) error = %v, want ErrPageInvalidated",
			err,
		)
	}
}

func TestFailedIndexTombstoneDoesNotAdvanceRevision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("active"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	before, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog before failed tombstone: %v", err)
	}
	invalid := indexDeletionTombstoneRecord{
		IndexID:            active.ID,
		Name:               active.Definition.Name,
		DeletedVersion:     int64(active.Version),
		DeletedAtUnixMicro: time.Now().UnixMicro(),
	}
	if err := db.GORMDB().WithContext(ctx).Create(&invalid).Error; err == nil {
		t.Fatal("tombstone for an active index succeeded")
	}
	after, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog after failed tombstone: %v", err)
	}
	if after.Revision != before.Revision ||
		after.PhysicalCount != before.PhysicalCount {
		t.Fatalf(
			"catalog changed after failed tombstone: before %#v after %#v",
			before,
			after,
		)
	}
}

func TestPhysicalIndexDeletionTransitionsInvalidateCursors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := db.CreateIndex(ctx, enabledIndex("alpha")); err != nil {
		t.Fatalf("CreateIndex(alpha): %v", err)
	}
	target, err := db.CreateIndex(ctx, enabledIndex("bravo"))
	if err != nil {
		t.Fatalf("CreateIndex(bravo): %v", err)
	}
	target, err = db.SetIndexState(
		ctx,
		target.ID,
		target.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive target: %v", err)
	}

	request := IndexListRequest{PageSize: 1}
	beforeAdmission, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(before admission): %v", err)
	}
	if beforeAdmission.NextCursor == nil {
		t.Fatal("pre-admission page has no cursor")
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		target.ID,
		target.Version,
		target.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	afterAdmission, err := readIndexCatalogIntegrity(
		db.GORMDB().WithContext(ctx),
	)
	if err != nil {
		t.Fatalf("read catalog after admission: %v", err)
	}
	if afterAdmission.Revision !=
		int64(beforeAdmission.CatalogRevision)+1 {
		t.Fatalf(
			"admission revision = %d, want %d",
			afterAdmission.Revision,
			beforeAdmission.CatalogRevision+1,
		)
	}
	request.Cursor = beforeAdmission.NextCursor
	if _, err := db.ListIndexPage(
		ctx,
		request,
	); !errors.Is(err, ErrPageInvalidated) {
		t.Fatalf(
			"continuation after admission error = %v, want ErrPageInvalidated",
			err,
		)
	}

	request.Cursor = nil
	beforeCompletion, err := db.ListIndexPage(ctx, request)
	if err != nil {
		t.Fatalf("ListIndexPage(before completion): %v", err)
	}
	if beforeCompletion.NextCursor == nil {
		t.Fatal("pre-completion page has no cursor")
	}
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "91234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.CompleteIndexDataDeletion(ctx, attempt); err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	afterCompletion, err := readIndexCatalogIntegrity(
		db.GORMDB().WithContext(ctx),
	)
	if err != nil {
		t.Fatalf("read catalog after completion: %v", err)
	}
	if afterCompletion.Revision !=
		int64(beforeCompletion.CatalogRevision)+1 {
		t.Fatalf(
			"completion revision = %d, want %d",
			afterCompletion.Revision,
			beforeCompletion.CatalogRevision+1,
		)
	}
	request.Cursor = beforeCompletion.NextCursor
	if _, err := db.ListIndexPage(
		ctx,
		request,
	); !errors.Is(err, ErrPageInvalidated) {
		t.Fatalf(
			"continuation after completion error = %v, want ErrPageInvalidated",
			err,
		)
	}
}

func TestIndexCatalogAccountingRollsBackFailedMultirowMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("alpha"))
	if err != nil {
		t.Fatalf("CreateIndex(alpha): %v", err)
	}
	terminal, err := db.CreateIndex(ctx, enabledIndex("bravo"))
	if err != nil {
		t.Fatalf("CreateIndex(bravo): %v", err)
	}
	terminal, err = db.SetIndexState(
		ctx,
		terminal.ID,
		terminal.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive terminal: %v", err)
	}
	if _, err := db.DeleteIndex(
		ctx,
		terminal.ID,
		terminal.Version,
		terminal.Definition.Name,
	); err != nil {
		t.Fatalf("tombstone terminal: %v", err)
	}
	before, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog before failed update: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexRecord{}).
		Where("index_id IN ?", []string{active.ID, terminal.ID}).
		Update("description", "must-roll-back").Error; err == nil {
		t.Fatal("multirow update including tombstoned index succeeded")
	}
	after, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog after failed update: %v", err)
	}
	if after != before {
		t.Fatalf(
			"catalog changed after rolled-back update: before %#v after %#v",
			before,
			after,
		)
	}
	reloaded, err := db.GetIndex(ctx, active.ID)
	if err != nil {
		t.Fatalf("GetIndex(active): %v", err)
	}
	if reloaded.Definition.Description == "must-roll-back" {
		t.Fatal("first row of failed multirow update was committed")
	}
}

func TestIndexCatalogMarkerCannotBeResetOrReplaced(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := db.CreateIndex(ctx, enabledIndex("alpha")); err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	before, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog before attacks: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Exec(
			`UPDATE index_catalog_state
			 SET revision = ?
			 WHERE singleton_id = 1`,
			before.Revision-1,
		).Error; err == nil {
		t.Fatal("catalog revision reset succeeded")
	}
	if err := db.GORMDB().WithContext(ctx).
		Exec(
			`INSERT OR REPLACE INTO index_catalog_state (
				singleton_id,
				revision,
				physical_count
			) VALUES (1, ?, ?)`,
			before.Revision,
			before.PhysicalCount,
		).Error; err == nil {
		t.Fatal("catalog marker replacement succeeded")
	}
	after, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog after attacks: %v", err)
	}
	if after != before {
		t.Fatalf(
			"catalog changed after reset/replacement: before %#v after %#v",
			before,
			after,
		)
	}
}

func TestIndexCatalogIdentityCannotBeReplaced(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("alpha"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	before, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog before replacement: %v", err)
	}
	replacement := newIndexRecord(
		created.ID,
		enabledIndex(created.Definition.Name),
		databaseTime(time.Now()),
	)
	replacement.DisplayName = "replaced"
	if err := db.GORMDB().WithContext(ctx).
		Exec(
			`INSERT OR REPLACE INTO indexes (
				index_id,
				version,
				name,
				display_name,
				description,
				retention_nanoseconds,
				ingestion_enabled,
				search_enabled,
				default_sourcetype,
				max_event_bytes,
				max_field_count,
				max_nesting_depth,
				maximum_future_skew_nanoseconds,
				maximum_event_age_nanoseconds,
				state,
				created_at_unix_micro,
				updated_at_unix_micro
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			replacement.IndexID,
			replacement.Version,
			replacement.Name,
			replacement.DisplayName,
			replacement.Description,
			replacement.RetentionNanoseconds,
			replacement.IngestionEnabled,
			replacement.SearchEnabled,
			replacement.DefaultSourcetype,
			replacement.MaxEventBytes,
			replacement.MaxFieldCount,
			replacement.MaxNestingDepth,
			replacement.MaximumFutureSkewNanoseconds,
			replacement.MaximumEventAgeNanoseconds,
			replacement.State,
			replacement.CreatedAtUnixMicro,
			replacement.UpdatedAtUnixMicro,
		).Error; err == nil {
		t.Fatal("index identity replacement succeeded")
	}
	after, err := readIndexCatalogIntegrity(db.GORMDB().WithContext(ctx))
	if err != nil {
		t.Fatalf("read catalog after replacement: %v", err)
	}
	if after != before {
		t.Fatalf(
			"catalog changed after replacement: before %#v after %#v",
			before,
			after,
		)
	}
	reloaded, err := db.GetIndex(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if reloaded.Definition.DisplayName == replacement.DisplayName {
		t.Fatal("replacement changed the retained index")
	}
}

func TestIndexCatalogCapacityIsAtomicAndRetainsTerminalIdentities(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	seedIndexCatalog(t, db, MaximumPhysicalIndexRecords-1)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, name := range []string{"last-a", "last-b"} {
		name := name
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := db.CreateIndex(ctx, enabledIndex(name))
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, exhausted int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrCapacityExceeded):
			exhausted++
		default:
			t.Fatalf("concurrent CreateIndex() error = %v", err)
		}
	}
	if successes != 1 || exhausted != 1 {
		t.Fatalf(
			"concurrent create results = success %d capacity %d, want 1/1",
			successes,
			exhausted,
		)
	}

	first, err := db.GetIndexByName(ctx, "seed-0000")
	if err != nil {
		t.Fatalf("GetIndexByName(seed): %v", err)
	}
	archived, err := db.SetIndexState(
		ctx,
		first.ID,
		first.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive seed: %v", err)
	}
	if _, err := db.DeleteIndex(
		ctx,
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); err != nil {
		t.Fatalf("tombstone seed: %v", err)
	}
	if _, err := db.CreateIndex(
		ctx,
		enabledIndex("after-tombstone"),
	); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf(
			"CreateIndex(after tombstone) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	if _, err := db.CreateIndex(
		ctx,
		enabledIndex("seed-0001"),
	); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf(
			"duplicate CreateIndex(at capacity) error = %v, want ErrAlreadyExists",
			err,
		)
	}
}

func TestIndexCatalogRejectsStructuralCountDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	seedIndexCatalog(t, db, MaximumPhysicalIndexRecords)

	overflow := newIndexRecord(
		"idx_overflow",
		enabledIndex("overflow"),
		time.UnixMicro(MaximumPhysicalIndexRecords+1).UTC(),
	)
	if err := db.GORMDB().WithContext(ctx).Create(&overflow).Error; err == nil {
		t.Fatal("direct index insert beyond the schema ceiling succeeded")
	}
	var physicalCount int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexRecord{}).
		Count(&physicalCount).Error; err != nil {
		t.Fatalf("count after rejected overflow: %v", err)
	}
	if physicalCount != MaximumPhysicalIndexRecords {
		t.Fatalf(
			"physical count after rejected overflow = %d, want %d",
			physicalCount,
			MaximumPhysicalIndexRecords,
		)
	}

	if err := db.GORMDB().WithContext(ctx).
		Exec("DROP TRIGGER index_catalog_state_transition_is_valid").
		Error; err != nil {
		t.Fatalf("remove test-owned marker guard: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexCatalogStateRecord{}).
		Where("singleton_id = ?", 1).
		Update(
			"physical_count",
			MaximumPhysicalIndexRecords-1,
		).Error; err != nil {
		t.Fatalf("corrupt catalog marker: %v", err)
	}

	if _, err := db.CreateIndex(
		ctx,
		enabledIndex("overflow-create"),
	); err == nil || errors.Is(err, ErrCapacityExceeded) ||
		!strings.Contains(err.Error(), "catalog") {
		t.Fatalf("CreateIndex(overflow) error = %v", err)
	}
	if _, err := db.ListIndexes(ctx); err == nil ||
		!strings.Contains(err.Error(), "catalog") {
		t.Fatalf("ListIndexes(overflow) error = %v", err)
	}
	page, err := db.ListIndexPage(
		ctx,
		IndexListRequest{},
	)
	if err != nil ||
		len(page.Indexes) != defaultIndexListPageSize ||
		page.NextCursor == nil {
		t.Fatalf(
			"bounded state-only ListIndexPage(overflow) = %#v, %v",
			page,
			err,
		)
	}
}

func TestOpenRejectsIndexCatalogStructuralCountDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if _, err := db.CreateIndex(ctx, enabledIndex("alpha")); err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Exec("DROP TRIGGER index_catalog_state_transition_is_valid").
		Error; err != nil {
		t.Fatalf("remove test-owned marker guard: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexCatalogStateRecord{}).
		Where("singleton_id = ?", 1).
		Update("physical_count", 0).Error; err != nil {
		t.Fatalf("corrupt catalog marker: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	reopened, err := Open(ctx, path)
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("Open() returned a database for a corrupt catalog")
	}
	if err == nil || !strings.Contains(err.Error(), "index catalog") {
		t.Fatalf("Open(corrupt catalog) error = %v", err)
	}
}

func TestIndexCatalogGORMStateMatchesMigratedSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	assertGORMModelColumnsMatchTable(
		t,
		ctx,
		db,
		"index_catalog_state",
		&indexCatalogStateRecord{},
	)
	for name, want := range map[string][]string{
		"indexes_name_id_idx":    {"name", "index_id"},
		"indexes_created_id_idx": {"created_at_unix_micro", "index_id"},
		"indexes_updated_id_idx": {"updated_at_unix_micro", "index_id"},
	} {
		assertSQLiteIndexColumns(t, ctx, db, name, want)
	}
}

func TestIndexCatalogBoundsDefinitionsAndPersistedRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	maximum := enabledIndex("bounded")
	maximum.DisplayName = strings.Repeat("d", maximumIndexDisplayNameBytes)
	maximum.Description = strings.Repeat("x", maximumIndexDescriptionBytes)
	maximum.DefaultSourcetype = strings.Repeat(
		"s",
		maximumIndexSourcetypeBytes,
	)
	created, err := db.CreateIndex(ctx, maximum)
	if err != nil {
		t.Fatalf("CreateIndex(maximum): %v", err)
	}
	if len(created.Definition.DisplayName) != maximumIndexDisplayNameBytes ||
		len(created.Definition.Description) != maximumIndexDescriptionBytes ||
		len(created.Definition.DefaultSourcetype) !=
			maximumIndexSourcetypeBytes {
		t.Fatalf("maximum definition changed = %#v", created.Definition)
	}

	tests := map[string]IndexDefinition{
		"display name": func() IndexDefinition {
			value := enabledIndex("display")
			value.DisplayName = strings.Repeat(
				"d",
				maximumIndexDisplayNameBytes+1,
			)
			return value
		}(),
		"description": func() IndexDefinition {
			value := enabledIndex("description")
			value.Description = strings.Repeat(
				"x",
				maximumIndexDescriptionBytes+1,
			)
			return value
		}(),
		"sourcetype": func() IndexDefinition {
			value := enabledIndex("sourcetype")
			value.DefaultSourcetype = strings.Repeat(
				"s",
				maximumIndexSourcetypeBytes+1,
			)
			return value
		}(),
		"invalid UTF-8": func() IndexDefinition {
			value := enabledIndex("invalid-utf8")
			value.Description = string([]byte{0xff})
			return value
		}(),
		"control": func() IndexDefinition {
			value := enabledIndex("control")
			value.DisplayName = "hidden\x01value"
			return value
		}(),
	}
	for name, definition := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := db.CreateIndex(
				ctx,
				definition,
			); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf(
					"CreateIndex() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}

	if err := db.GORMDB().WithContext(ctx).
		Model(&indexRecord{}).
		Where("index_id = ?", created.ID).
		Update(
			"description",
			strings.Repeat("x", maximumIndexDescriptionBytes+1),
		).Error; err == nil {
		t.Fatal("direct oversized index update succeeded")
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexRecord{}).
		Where("index_id = ?", created.ID).
		Update("index_id", "rewritten-index-id").Error; err == nil {
		t.Fatal("direct index-ID rewrite succeeded")
	}
}

func seedIndexCatalog(t *testing.T, db *DB, count int) {
	t.Helper()
	records := make([]indexRecord, 0, count)
	for index := range count {
		name := fmt.Sprintf("seed-%04d", index)
		records = append(
			records,
			newIndexRecord(
				fmt.Sprintf("idx_seed_%04d", index),
				enabledIndex(name),
				time.UnixMicro(int64(index+1)).UTC(),
			),
		)
	}
	if err := db.GORMDB().
		CreateInBatches(records, 32).Error; err != nil {
		t.Fatalf("seed index catalog: %v", err)
	}
}

func indexNames(indexes []Index) []string {
	names := make([]string, len(indexes))
	for index, record := range indexes {
		names[index] = record.Definition.Name
	}
	return names
}

func assertGORMModelColumnsMatchTable(
	t *testing.T,
	ctx context.Context,
	db *DB,
	table string,
	model any,
) {
	t.Helper()
	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(model); err != nil {
		t.Fatalf("parse GORM model for %s: %v", table, err)
	}
	rows, err := db.SQLDB().QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT name FROM pragma_table_info('%s') ORDER BY cid`,
			table,
		),
	)
	if err != nil {
		t.Fatalf("read migrated %s columns: %v", table, err)
	}
	defer rows.Close()
	var migrated []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated %s column: %v", table, err)
		}
		migrated = append(migrated, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated %s columns: %v", table, err)
	}
	if !slices.Equal(statement.Schema.DBNames, migrated) {
		t.Fatalf(
			"GORM %s columns = %v, migrated columns = %v",
			table,
			statement.Schema.DBNames,
			migrated,
		)
	}
}

func assertSQLiteIndexColumns(
	t *testing.T,
	ctx context.Context,
	db *DB,
	name string,
	want []string,
) {
	t.Helper()
	rows, err := db.SQLDB().QueryContext(
		ctx,
		`SELECT name FROM pragma_index_info(?) ORDER BY seqno`,
		name,
	)
	if err != nil {
		t.Fatalf("read SQLite index %s: %v", name, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan SQLite index %s: %v", name, err)
		}
		got = append(got, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite index %s: %v", name, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("SQLite index %s = %v, want %v", name, got, want)
	}
}
