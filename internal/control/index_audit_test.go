package control

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

var errTestIndexAuditAppend = errors.New("test index audit append failure")

type recordedIndexMutationAudit struct {
	tenantID string
	event    IndexMutationAuditEvent
}

type recordingIndexMutationAuditAppender struct {
	mu         sync.Mutex
	calls      []recordedIndexMutationAudit
	failAction IndexMutationAuditAction
}

func (appender *recordingIndexMutationAuditAppender) AppendIndexMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event IndexMutationAuditEvent,
) error {
	if ctx == nil || tx == nil || tx.Statement == nil {
		return errors.New("test audit appender received an invalid transaction")
	}
	if _, ok := tx.Statement.ConnPool.(*sql.Tx); !ok {
		return errors.New("test audit appender was not called inside a SQL transaction")
	}
	appender.mu.Lock()
	defer appender.mu.Unlock()
	appender.calls = append(appender.calls, recordedIndexMutationAudit{
		tenantID: strings.Clone(tenantID),
		event: IndexMutationAuditEvent{
			OccurredAt:   event.OccurredAt,
			Action:       event.Action,
			IndexID:      strings.Clone(event.IndexID),
			IndexVersion: event.IndexVersion,
		},
	})
	if appender.failAction == "" || appender.failAction == event.Action {
		if appender.failAction != "" {
			return errTestIndexAuditAppend
		}
	}
	return nil
}

func (appender *recordingIndexMutationAuditAppender) snapshot() []recordedIndexMutationAudit {
	appender.mu.Lock()
	defer appender.mu.Unlock()
	return slices.Clone(appender.calls)
}

func TestAuditedIndexAdministrationPublishesSuccessfulLifecycleInMutationTransactions(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministration(t, db, appender)

	created, err := administration.CreateIndex(ctx, enabledIndex("lifecycle"))
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	byID, err := administration.GetIndex(ctx, created.ID)
	if err != nil || byID != created {
		t.Fatalf("GetIndex() = %#v, %v, want %#v", byID, err, created)
	}
	byName, err := administration.GetIndexByName(ctx, created.Definition.Name)
	if err != nil || byName != created {
		t.Fatalf("GetIndexByName() = %#v, %v, want %#v", byName, err, created)
	}
	listed, err := administration.ListIndexPage(ctx, IndexListRequest{PageSize: 1})
	if err != nil || len(listed.Indexes) != 1 || listed.Indexes[0] != created {
		t.Fatalf("ListIndexPage() = %#v, %v", listed, err)
	}

	replacement := created.Definition
	replacement.DisplayName = "updated lifecycle"
	updated, err := administration.UpdateIndex(
		ctx,
		created.ID,
		created.Version,
		replacement,
	)
	if err != nil {
		t.Fatalf("UpdateIndex() error = %v", err)
	}
	archived, err := administration.SetIndexState(
		ctx,
		updated.ID,
		updated.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState(archived) error = %v", err)
	}
	active, err := administration.SetIndexState(
		ctx,
		archived.ID,
		archived.Version,
		IndexStateActive,
	)
	if err != nil {
		t.Fatalf("SetIndexState(active) error = %v", err)
	}
	archivedAgain, err := administration.SetIndexState(
		ctx,
		active.ID,
		active.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState(archived again) error = %v", err)
	}
	deletedID, err := administration.DeleteIndex(
		ctx,
		archivedAgain.ID,
		archivedAgain.Version,
		archivedAgain.Definition.Name,
	)
	if err != nil || deletedID != archivedAgain.ID {
		t.Fatalf("DeleteIndex() = %q, %v", deletedID, err)
	}

	physical := mustCreateIndex(t, db, enabledIndex("physical-delete"))
	physical, err = db.SetIndexState(
		ctx,
		physical.ID,
		physical.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("raw SetIndexState(physical) error = %v", err)
	}
	operation, err := administration.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-a"},
		physical.ID,
		physical.Version,
		physical.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion() error = %v", err)
	}
	retry, err := administration.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-a"},
		physical.ID,
		physical.Version,
		physical.Definition.Name,
	)
	if err != nil || retry != operation {
		t.Fatalf("retry BeginIndexDataDeletion() = %#v, %v, want %#v", retry, err, operation)
	}

	var tombstone indexDeletionTombstoneRecord
	if err := db.GORMDB().Where("index_id = ?", archivedAgain.ID).Take(&tombstone).Error; err != nil {
		t.Fatalf("read KEEP_DATA tombstone: %v", err)
	}
	calls := appender.snapshot()
	wantActions := []IndexMutationAuditAction{
		IndexMutationAuditActionCreate,
		IndexMutationAuditActionUpdate,
		IndexMutationAuditActionArchive,
		IndexMutationAuditActionActivate,
		IndexMutationAuditActionArchive,
		IndexMutationAuditActionDeleteKeepData,
		IndexMutationAuditActionDeleteData,
	}
	if len(calls) != len(wantActions) {
		t.Fatalf("audit calls = %#v, want %d", calls, len(wantActions))
	}
	wantIDs := []string{
		created.ID,
		created.ID,
		created.ID,
		created.ID,
		created.ID,
		created.ID,
		physical.ID,
	}
	wantVersions := []uint64{
		created.Version,
		updated.Version,
		archived.Version,
		active.Version,
		archivedAgain.Version,
		archivedAgain.Version,
		operation.DeletingVersion,
	}
	wantTimes := []int64{
		created.CreatedAt.UnixMicro(),
		updated.UpdatedAt.UnixMicro(),
		archived.UpdatedAt.UnixMicro(),
		active.UpdatedAt.UnixMicro(),
		archivedAgain.UpdatedAt.UnixMicro(),
		tombstone.DeletedAtUnixMicro,
		operation.CreatedAt.UnixMicro(),
	}
	for position, call := range calls {
		if call.tenantID != "tenant-a" ||
			call.event.Action != wantActions[position] ||
			call.event.IndexID != wantIDs[position] ||
			call.event.IndexVersion != wantVersions[position] ||
			call.event.OccurredAt.UnixMicro() != wantTimes[position] ||
			call.event.OccurredAt.Location() != time.UTC {
			t.Fatalf("audit call %d = %#v", position, call)
		}
	}
}

func TestAuditedIndexAdministrationRollsBackEveryMutationWhenAuditFails(
	t *testing.T,
) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db := openTestDB(t)
		appender := failingIndexAuditAppender(IndexMutationAuditActionCreate)
		administration := newTestAuditedIndexAdministration(t, db, appender)
		before := readTestIndexCatalogState(t, db)

		_, err := administration.CreateIndex(ctx, enabledIndex("rollback-create"))
		if !errors.Is(err, errTestIndexAuditAppend) {
			t.Fatalf("CreateIndex() error = %v, want audit failure", err)
		}
		if _, err := db.GetIndexByName(ctx, "rollback-create"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetIndexByName() error = %v, want ErrNotFound", err)
		}
		assertTestIndexCatalogState(t, db, before)
	})

	t.Run("update", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db := openTestDB(t)
		created := mustCreateIndex(t, db, enabledIndex("rollback-update"))
		appender := failingIndexAuditAppender(IndexMutationAuditActionUpdate)
		administration := newTestAuditedIndexAdministration(t, db, appender)
		before := readTestIndexCatalogState(t, db)
		replacement := created.Definition
		replacement.DisplayName = "must roll back"

		_, err := administration.UpdateIndex(
			ctx,
			created.ID,
			created.Version,
			replacement,
		)
		if !errors.Is(err, errTestIndexAuditAppend) {
			t.Fatalf("UpdateIndex() error = %v, want audit failure", err)
		}
		got, err := db.GetIndex(ctx, created.ID)
		if err != nil || got != created {
			t.Fatalf("GetIndex() = %#v, %v, want %#v", got, err, created)
		}
		assertTestIndexCatalogState(t, db, before)
	})

	t.Run("state", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db := openTestDB(t)
		created := mustCreateIndex(t, db, enabledIndex("rollback-state"))
		appender := failingIndexAuditAppender(IndexMutationAuditActionArchive)
		administration := newTestAuditedIndexAdministration(t, db, appender)
		before := readTestIndexCatalogState(t, db)

		_, err := administration.SetIndexState(
			ctx,
			created.ID,
			created.Version,
			IndexStateArchived,
		)
		if !errors.Is(err, errTestIndexAuditAppend) {
			t.Fatalf("SetIndexState() error = %v, want audit failure", err)
		}
		got, err := db.GetIndex(ctx, created.ID)
		if err != nil || got != created {
			t.Fatalf("GetIndex() = %#v, %v, want %#v", got, err, created)
		}
		assertTestIndexCatalogState(t, db, before)
	})

	t.Run("keep data deletion", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db := openTestDB(t)
		archived := mustCreateIndex(t, db, enabledIndex("rollback-keep-delete"))
		var err error
		archived, err = db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
		if err != nil {
			t.Fatalf("raw SetIndexState() error = %v", err)
		}
		appender := failingIndexAuditAppender(IndexMutationAuditActionDeleteKeepData)
		administration := newTestAuditedIndexAdministration(t, db, appender)
		before := readTestIndexCatalogState(t, db)

		_, err = administration.DeleteIndex(
			ctx,
			archived.ID,
			archived.Version,
			archived.Definition.Name,
		)
		if !errors.Is(err, errTestIndexAuditAppend) {
			t.Fatalf("DeleteIndex() error = %v, want audit failure", err)
		}
		got, err := db.GetIndex(ctx, archived.ID)
		if err != nil || got != archived {
			t.Fatalf("GetIndex() = %#v, %v, want %#v", got, err, archived)
		}
		assertTableCount(t, db, "index_deletion_tombstones", 0)
		assertTestIndexCatalogState(t, db, before)
	})

	t.Run("delete data admission", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db := openTestDB(t)
		archived := mustCreateIndex(t, db, enabledIndex("rollback-data-delete"))
		var err error
		archived, err = db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
		if err != nil {
			t.Fatalf("raw SetIndexState() error = %v", err)
		}
		appender := failingIndexAuditAppender(IndexMutationAuditActionDeleteData)
		administration := newTestAuditedIndexAdministration(t, db, appender)
		before := readTestIndexCatalogState(t, db)

		_, err = administration.BeginIndexDataDeletion(
			ctx,
			IndexDataDeletionScope{TenantID: "tenant-a"},
			archived.ID,
			archived.Version,
			archived.Definition.Name,
		)
		if !errors.Is(err, errTestIndexAuditAppend) {
			t.Fatalf("BeginIndexDataDeletion() error = %v, want audit failure", err)
		}
		got, err := db.GetIndex(ctx, archived.ID)
		if err != nil || got != archived {
			t.Fatalf("GetIndex() = %#v, %v, want %#v", got, err, archived)
		}
		assertTableCount(t, db, "index_deletion_operations", 0)
		assertTestIndexCatalogState(t, db, before)
	})
}

func TestAuditedIndexAdministrationDoesNotAuditRejectedMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministration(t, db, appender)

	if _, err := administration.CreateIndex(ctx, IndexDefinition{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("CreateIndex(invalid) error = %v, want ErrInvalidArgument", err)
	}
	created := mustCreateIndex(t, db, enabledIndex("rejected"))
	if _, err := administration.UpdateIndex(
		ctx,
		created.ID,
		created.Version+1,
		created.Definition,
	); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("UpdateIndex(stale) error = %v, want ErrVersionConflict", err)
	}

	appCatalog := newTestAppCatalog(t, db)
	if _, err := appCatalog.CreateApp(
		ctx,
		AppAccessScope{TenantID: "tenant-a"},
		AppDefinition{
			Slug:           "dependent-app",
			DisplayName:    "Dependent app",
			DefaultIndexes: []string{created.Definition.Name},
		},
	); err != nil {
		t.Fatalf("CreateApp() error = %v", err)
	}
	if _, err := administration.SetIndexState(
		ctx,
		created.ID,
		created.Version,
		IndexStateArchived,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("SetIndexState(dependent) error = %v, want ErrDependencyConflict", err)
	}
	if _, err := administration.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-b"},
		created.ID,
		created.Version,
		created.Definition.Name,
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("BeginIndexDataDeletion(scope drift) error = %v, want ErrInvalidArgument", err)
	}
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("rejected mutations emitted audit calls: %#v", calls)
	}
}

func TestAuditedIndexAdministrationConcurrentCASPublishesOnlyWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created := mustCreateIndex(t, db, enabledIndex("concurrent-audit"))
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministration(t, db, appender)

	start := make(chan struct{})
	errorsByAttempt := make(chan error, 2)
	for _, displayName := range []string{"first", "second"} {
		go func() {
			<-start
			replacement := created.Definition
			replacement.DisplayName = displayName
			_, err := administration.UpdateIndex(
				ctx,
				created.ID,
				created.Version,
				replacement,
			)
			errorsByAttempt <- err
		}()
	}
	close(start)
	var succeeded, conflicted int
	for range 2 {
		err := <-errorsByAttempt
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrVersionConflict):
			conflicted++
		default:
			t.Fatalf("concurrent UpdateIndex() error = %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent outcomes = success %d conflict %d", succeeded, conflicted)
	}
	calls := appender.snapshot()
	if len(calls) != 1 ||
		calls[0].event.Action != IndexMutationAuditActionUpdate ||
		calls[0].event.IndexID != created.ID ||
		calls[0].event.IndexVersion != created.Version+1 {
		t.Fatalf("concurrent audit calls = %#v", calls)
	}
}

func TestAuditedIndexAdministrationConcurrentDeleteDataRetryAuditsOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	archived := mustCreateIndex(t, db, enabledIndex("concurrent-delete-audit"))
	var err error
	archived, err = db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("raw SetIndexState() error = %v", err)
	}
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministration(t, db, appender)

	start := make(chan struct{})
	results := make(chan IndexDeletionOperation, 2)
	errorsByAttempt := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			operation, operationErr := administration.BeginIndexDataDeletion(
				ctx,
				IndexDataDeletionScope{TenantID: "tenant-a"},
				archived.ID,
				archived.Version,
				archived.Definition.Name,
			)
			results <- operation
			errorsByAttempt <- operationErr
		}()
	}
	close(start)
	first := <-results
	second := <-results
	for range 2 {
		if err := <-errorsByAttempt; err != nil {
			t.Fatalf("concurrent BeginIndexDataDeletion() error = %v", err)
		}
	}
	if first == (IndexDeletionOperation{}) || first != second {
		t.Fatalf("concurrent operations = %#v and %#v", first, second)
	}
	third, err := administration.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-a"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil || third != first {
		t.Fatalf("sequential retry = %#v, %v, want %#v", third, err, first)
	}
	calls := appender.snapshot()
	if len(calls) != 1 ||
		calls[0].event.Action != IndexMutationAuditActionDeleteData ||
		calls[0].event.IndexVersion != first.DeletingVersion ||
		!calls[0].event.OccurredAt.Equal(first.CreatedAt) {
		t.Fatalf("delete-data audit calls = %#v", calls)
	}
}

func TestNewAuditedIndexAdministrationRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	valid := &recordingIndexMutationAuditAppender{}
	var typedNil *recordingIndexMutationAuditAppender
	var typedNilValidator *typedNilIndexNameAdmissionValidator
	for name, test := range map[string]struct {
		db      *DB
		options AuditedIndexAdministrationOptions
	}{
		"nil database": {options: AuditedIndexAdministrationOptions{
			TenantID: "tenant-a", Appender: valid,
		}},
		"empty tenant": {db: db, options: AuditedIndexAdministrationOptions{
			Appender: valid,
		}},
		"noncanonical tenant": {db: db, options: AuditedIndexAdministrationOptions{
			TenantID: " tenant-a", Appender: valid,
		}},
		"oversized tenant": {db: db, options: AuditedIndexAdministrationOptions{
			TenantID: strings.Repeat("t", maximumTenantIDBytes+1), Appender: valid,
		}},
		"nil appender": {db: db, options: AuditedIndexAdministrationOptions{
			TenantID: "tenant-a",
		}},
		"typed nil appender": {db: db, options: AuditedIndexAdministrationOptions{
			TenantID: "tenant-a", Appender: typedNil,
		}},
		"typed nil validator": {db: db, options: AuditedIndexAdministrationOptions{
			TenantID: "tenant-a", Appender: valid, Validator: typedNilValidator,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			administration, err := NewAuditedIndexAdministration(test.db, test.options)
			if administration != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("NewAuditedIndexAdministration() = %v, %v, want nil/ErrInvalidArgument", administration, err)
			}
		})
	}
}

func failingIndexAuditAppender(
	action IndexMutationAuditAction,
) *recordingIndexMutationAuditAppender {
	return &recordingIndexMutationAuditAppender{failAction: action}
}

func newTestAuditedIndexAdministration(
	t *testing.T,
	db *DB,
	appender IndexMutationAuditAppender,
) *AuditedIndexAdministration {
	t.Helper()
	administration, err := NewAuditedIndexAdministration(
		db,
		AuditedIndexAdministrationOptions{
			TenantID: "tenant-a",
			Appender: appender,
		},
	)
	if err != nil {
		t.Fatalf("NewAuditedIndexAdministration() error = %v", err)
	}
	return administration
}

func readTestIndexCatalogState(t *testing.T, db *DB) indexCatalogStateRecord {
	t.Helper()
	var state indexCatalogStateRecord
	if err := db.GORMDB().Take(&state).Error; err != nil {
		t.Fatalf("read index catalog state: %v", err)
	}
	return state
}

func assertTestIndexCatalogState(
	t *testing.T,
	db *DB,
	want indexCatalogStateRecord,
) {
	t.Helper()
	if got := readTestIndexCatalogState(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("index catalog state = %#v, want %#v", got, want)
	}
}

func assertTableCount(t *testing.T, db *DB, table string, want int64) {
	t.Helper()
	var count int64
	if err := db.GORMDB().Table(table).Count(&count).Error; err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if count != want {
		t.Fatalf("%s count = %d, want %d", table, count, want)
	}
}
