package savedobjects

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

var errTestSavedSearchAuditAppend = errors.New("test saved-search audit append failure")

const savedSearchAuditTestTenant = "tenant-saved-search-audit"

type recordedSavedSearchAudit struct {
	tenantID  string
	event     SavedSearchMutationAuditEvent
	rowFound  bool
	row       savedSearchRecord
	insideSQL bool
}

type recordingSavedSearchAuditAppender struct {
	mu         sync.Mutex
	calls      []recordedSavedSearchAudit
	failAction SavedSearchMutationAuditAction
}

var _ SavedSearchMutationAuditAppender = (*recordingSavedSearchAuditAppender)(nil)

func (appender *recordingSavedSearchAuditAppender) AppendSavedSearchMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event SavedSearchMutationAuditEvent,
) error {
	if ctx == nil || tx == nil || tx.Statement == nil {
		return errors.New("test saved-search audit appender received an invalid transaction")
	}
	_, insideSQLTransaction := tx.Statement.ConnPool.(*sql.Tx)
	if !insideSQLTransaction {
		return errors.New("test saved-search audit appender was not called inside a SQL transaction")
	}

	recorded := recordedSavedSearchAudit{
		tenantID: strings.Clone(tenantID),
		event: SavedSearchMutationAuditEvent{
			OccurredAt:         event.OccurredAt,
			Action:             event.Action,
			SavedSearchID:      strings.Clone(event.SavedSearchID),
			SavedSearchVersion: event.SavedSearchVersion,
		},
		insideSQL: true,
	}
	rowErr := tx.Where("saved_search_id = ?", event.SavedSearchID).Take(&recorded.row).Error
	switch {
	case rowErr == nil:
		recorded.rowFound = true
		recorded.row.DefinitionProto = slices.Clone(recorded.row.DefinitionProto)
	case errors.Is(rowErr, gorm.ErrRecordNotFound):
	default:
		return fmt.Errorf("test saved-search audit appender read target row: %w", rowErr)
	}

	appender.mu.Lock()
	appender.calls = append(appender.calls, recorded)
	appender.mu.Unlock()
	if appender.failAction != "" && appender.failAction == event.Action {
		return errTestSavedSearchAuditAppend
	}
	return nil
}

func (appender *recordingSavedSearchAuditAppender) snapshot() []recordedSavedSearchAudit {
	appender.mu.Lock()
	defer appender.mu.Unlock()
	result := slices.Clone(appender.calls)
	for index := range result {
		result[index].row.DefinitionProto = slices.Clone(result[index].row.DefinitionProto)
	}
	return result
}

type savedSearchAuditDependencies struct {
	base       time.Time
	clockCalls atomic.Int64
	idCalls    atomic.Int64
	ids        []string
}

func (dependencies *savedSearchAuditDependencies) options() Options {
	return Options{
		CursorKey: testCursorKey,
		Clock: func() time.Time {
			call := dependencies.clockCalls.Add(1)
			return dependencies.base.Add(time.Duration(call) * time.Microsecond)
		},
		IDGenerator: func() (string, error) {
			call := dependencies.idCalls.Add(1)
			if int(call) <= len(dependencies.ids) {
				return dependencies.ids[call-1], nil
			}
			return fmt.Sprintf("ss_saved_audit_%04d", call), nil
		},
	}
}

func TestAuditedStorePublishesCompleteSavedSearchLifecycleInsideMutationTransactions(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	dependencies := &savedSearchAuditDependencies{
		base: time.Date(2026, time.August, 4, 18, 0, 0, 0, time.UTC),
		ids:  []string{"ss_audit_lifecycle", "ss_audit_lifecycle_copy"},
	}
	store := newSavedSearchAuditRawStore(t, database, dependencies.options())
	appender := &recordingSavedSearchAuditAppender{}
	audited := newSavedSearchAuditStore(t, store, appender)
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner-a"}

	created, err := audited.Create(ctx, scope, savedSearchDefinition("lifecycle", ""))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := audited.Get(ctx, scope, created.SavedSearchId)
	if err != nil || !proto.Equal(got, created) {
		t.Fatalf("Get() = %+v, %v, want %+v", got, err, created)
	}
	page, err := audited.List(ctx, scope, ListRequest{IncludeTotal: true})
	if err != nil || len(page.SavedSearches) != 1 {
		t.Fatalf("List() = %+v, %v", page, err)
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("read operations emitted audit events: %#v", calls)
	}

	replacement := proto.Clone(created.Definition).(*opensplunkv1.SavedSearchDefinition)
	replacement.Name = "lifecycle-updated"
	updated, err := audited.Update(
		ctx,
		scope,
		created.SavedSearchId,
		created.Version,
		replacement,
		nil,
	)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	duplicate, err := audited.Duplicate(
		ctx,
		scope,
		updated.SavedSearchId,
		"lifecycle-copy",
		nil,
	)
	if err != nil {
		t.Fatalf("Duplicate() error = %v", err)
	}
	if err := audited.Delete(ctx, scope, updated.SavedSearchId, updated.Version); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	calls := appender.snapshot()
	wantActions := []SavedSearchMutationAuditAction{
		SavedSearchMutationAuditActionCreate,
		SavedSearchMutationAuditActionUpdate,
		SavedSearchMutationAuditActionDuplicate,
		SavedSearchMutationAuditActionDelete,
	}
	wantIDs := []string{
		created.SavedSearchId,
		updated.SavedSearchId,
		duplicate.SavedSearchId,
		updated.SavedSearchId,
	}
	wantVersions := []uint64{1, 2, 1, 2}
	if len(calls) != len(wantActions) {
		t.Fatalf("saved-search audit calls = %#v, want %d", calls, len(wantActions))
	}
	for position, call := range calls {
		wantTime := dependencies.base.Add(time.Duration(position+1) * time.Microsecond)
		if !call.insideSQL ||
			call.tenantID != savedSearchAuditTestTenant ||
			call.event.Action != wantActions[position] ||
			call.event.SavedSearchID != wantIDs[position] ||
			call.event.SavedSearchVersion != wantVersions[position] ||
			!call.event.OccurredAt.Equal(wantTime) ||
			call.event.OccurredAt.Location() != time.UTC {
			t.Fatalf("saved-search audit call %d = %#v", position, call)
		}
		if wantActions[position] == SavedSearchMutationAuditActionDelete {
			if call.rowFound {
				t.Fatalf("delete audit observed retained target row: %#v", call)
			}
			continue
		}
		if !call.rowFound ||
			call.row.SavedSearchID != wantIDs[position] ||
			call.row.Version != int64(wantVersions[position]) ||
			call.row.UpdatedAtUnixMicro != wantTime.UnixMicro() {
			t.Fatalf("audit call %d did not observe completed mutation: %#v", position, call)
		}
		if wantActions[position] == SavedSearchMutationAuditActionCreate ||
			wantActions[position] == SavedSearchMutationAuditActionDuplicate {
			if call.row.CreatedAtUnixMicro != wantTime.UnixMicro() {
				t.Fatalf("create-like audit call %d timestamp = %#v", position, call)
			}
		} else if call.row.CreatedAtUnixMicro != created.CreatedAt.AsTime().UnixMicro() {
			t.Fatalf("update audit changed creation time: %#v", call)
		}
	}
	if dependencies.clockCalls.Load() != int64(len(wantActions)) ||
		dependencies.idCalls.Load() != 2 {
		t.Fatalf(
			"saved-search lifecycle dependency calls = clock %d ID %d, want 4/2",
			dependencies.clockCalls.Load(),
			dependencies.idCalls.Load(),
		)
	}
	if _, err := audited.Get(ctx, scope, updated.SavedSearchId); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(deleted source) error = %v, want ErrNotFound", err)
	}
	if got, err := audited.Get(ctx, scope, duplicate.SavedSearchId); err != nil || got.Version != 1 {
		t.Fatalf("Get(duplicate) = %+v, %v", got, err)
	}
}

func TestAuditedStoreRollsBackEverySavedSearchMutationWhenAuditFails(t *testing.T) {
	t.Parallel()

	for _, action := range []SavedSearchMutationAuditAction{
		SavedSearchMutationAuditActionCreate,
		SavedSearchMutationAuditActionUpdate,
		SavedSearchMutationAuditActionDuplicate,
		SavedSearchMutationAuditActionDelete,
	} {
		t.Run(string(action), func(t *testing.T) {
			t.Parallel()
			database, _ := openTestStore(t)
			dependencies := &savedSearchAuditDependencies{
				base: time.Date(2026, time.August, 4, 20, 0, 0, 0, time.UTC),
			}
			store := newSavedSearchAuditRawStore(t, database, dependencies.options())
			ctx := context.Background()
			scope := AccessScope{OwnerID: "owner"}
			var existing *opensplunkv1.SavedSearch
			if action != SavedSearchMutationAuditActionCreate {
				var err error
				existing, err = store.Create(ctx, scope, savedSearchDefinition("rollback-source", ""))
				if err != nil {
					t.Fatalf("seed Create() error = %v", err)
				}
			}
			before := readSavedSearchAuditPersistence(t, database)
			appender := &recordingSavedSearchAuditAppender{failAction: action}
			audited := newSavedSearchAuditStore(t, store, appender)

			var mutationErr error
			switch action {
			case SavedSearchMutationAuditActionCreate:
				_, mutationErr = audited.Create(
					ctx,
					scope,
					savedSearchDefinition("rollback-create", ""),
				)
			case SavedSearchMutationAuditActionUpdate:
				replacement := proto.Clone(existing.Definition).(*opensplunkv1.SavedSearchDefinition)
				replacement.Name = "must-roll-back"
				_, mutationErr = audited.Update(
					ctx,
					scope,
					existing.SavedSearchId,
					existing.Version,
					replacement,
					nil,
				)
			case SavedSearchMutationAuditActionDuplicate:
				_, mutationErr = audited.Duplicate(
					ctx,
					scope,
					existing.SavedSearchId,
					"must-roll-back",
					nil,
				)
			case SavedSearchMutationAuditActionDelete:
				mutationErr = audited.Delete(
					ctx,
					scope,
					existing.SavedSearchId,
					existing.Version,
				)
			}
			if !errors.Is(mutationErr, errTestSavedSearchAuditAppend) {
				t.Fatalf("%s error = %v, want audit failure", action, mutationErr)
			}
			calls := appender.snapshot()
			if len(calls) != 1 || calls[0].event.Action != action {
				t.Fatalf("%s audit calls = %#v, want one failed call", action, calls)
			}
			if action == SavedSearchMutationAuditActionDelete {
				if calls[0].rowFound {
					t.Fatalf("failed delete audit did not observe deletion inside tx: %#v", calls[0])
				}
			} else if !calls[0].rowFound {
				t.Fatalf("failed %s audit did not observe mutation inside tx: %#v", action, calls[0])
			}
			after := readSavedSearchAuditPersistence(t, database)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("%s persistence after rollback = %#v, want %#v", action, after, before)
			}
		})
	}
}

func TestAuditedStoreDoesNotAuditRejectedSavedSearchOperations(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	owner := AccessScope{OwnerID: "owner-a"}
	first, err := store.Create(ctx, owner, savedSearchDefinition("same", ""))
	if err != nil {
		t.Fatalf("seed first Create() error = %v", err)
	}
	second, err := store.Create(ctx, owner, savedSearchDefinition("second", ""))
	if err != nil {
		t.Fatalf("seed second Create() error = %v", err)
	}
	before := readSavedSearchAuditPersistence(t, database)
	appender := &recordingSavedSearchAuditAppender{}
	audited := newSavedSearchAuditStore(t, store, appender)

	if _, err := audited.Get(ctx, owner, first.SavedSearchId); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := audited.List(ctx, owner, ListRequest{}); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if _, err := audited.Create(ctx, owner, nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("invalid Create() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := audited.Create(ctx, owner, savedSearchDefinition("same", "")); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("conflicting Create() error = %v, want ErrAlreadyExists", err)
	}
	if _, err := audited.Update(
		ctx,
		owner,
		first.SavedSearchId,
		first.Version+1,
		first.Definition,
		nil,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale Update() error = %v, want ErrVersionConflict", err)
	}
	conflicting := proto.Clone(second.Definition).(*opensplunkv1.SavedSearchDefinition)
	conflicting.Name = first.Definition.Name
	if _, err := audited.Update(
		ctx,
		owner,
		second.SavedSearchId,
		second.Version,
		conflicting,
		nil,
	); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("conflicting Update() error = %v, want ErrAlreadyExists", err)
	}
	otherOwner := AccessScope{OwnerID: "owner-b"}
	if _, err := audited.Update(
		ctx,
		otherOwner,
		first.SavedSearchId,
		first.Version,
		first.Definition,
		nil,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Update() error = %v, want ErrNotFound", err)
	}
	if _, err := audited.Duplicate(
		ctx,
		owner,
		first.SavedSearchId,
		first.Definition.Name,
		nil,
	); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("conflicting Duplicate() error = %v, want ErrAlreadyExists", err)
	}
	if _, err := audited.Duplicate(
		ctx,
		otherOwner,
		first.SavedSearchId,
		"cross-owner-copy",
		nil,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Duplicate() error = %v, want ErrNotFound", err)
	}
	if err := audited.Delete(ctx, owner, first.SavedSearchId, first.Version+1); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale Delete() error = %v, want ErrVersionConflict", err)
	}
	if err := audited.Delete(ctx, otherOwner, first.SavedSearchId, first.Version); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Delete() error = %v, want ErrNotFound", err)
	}
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("rejected or read saved-search operations emitted audit calls: %#v", calls)
	}
	after := readSavedSearchAuditPersistence(t, database)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected operations changed persistence = %#v, want %#v", after, before)
	}
}

func TestAuditedStoreIDCollisionsPublishOnlyTheCommittedSavedSearch(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		action     SavedSearchMutationAuditAction
		exhausted  bool
		wantTarget string
	}{
		{
			name:       "create retry succeeds",
			action:     SavedSearchMutationAuditActionCreate,
			wantTarget: "ss_create_after_collision",
		},
		{
			name:      "create retry exhausted",
			action:    SavedSearchMutationAuditActionCreate,
			exhausted: true,
		},
		{
			name:       "duplicate retry succeeds",
			action:     SavedSearchMutationAuditActionDuplicate,
			wantTarget: "ss_duplicate_after_collision",
		},
		{
			name:      "duplicate retry exhausted",
			action:    SavedSearchMutationAuditActionDuplicate,
			exhausted: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			database, _ := openTestStore(t)
			seedIDs := []string{"ss_collision"}
			if test.action == SavedSearchMutationAuditActionDuplicate {
				seedIDs = []string{"ss_source", "ss_collision"}
			}
			seedIndex := 0
			seed := newSavedSearchAuditRawStore(t, database, Options{
				CursorKey: testCursorKey,
				Clock: func() time.Time {
					return time.Date(2026, time.August, 4, 21, 0, 0, 0, time.UTC)
				},
				IDGenerator: func() (string, error) {
					id := seedIDs[seedIndex]
					seedIndex++
					return id, nil
				},
			})
			ctx := context.Background()
			scope := AccessScope{OwnerID: "owner"}
			var source *opensplunkv1.SavedSearch
			if test.action == SavedSearchMutationAuditActionDuplicate {
				var err error
				source, err = seed.Create(ctx, scope, savedSearchDefinition("source", ""))
				if err != nil {
					t.Fatalf("seed source: %v", err)
				}
			}
			if _, err := seed.Create(ctx, scope, savedSearchDefinition("collision-owner", "")); err != nil {
				t.Fatalf("seed collision: %v", err)
			}

			var idCalls atomic.Int64
			store := newSavedSearchAuditRawStore(t, database, Options{
				CursorKey: testCursorKey,
				Clock: func() time.Time {
					return time.Date(2026, time.August, 4, 21, 30, 0, 0, time.UTC)
				},
				IDGenerator: func() (string, error) {
					call := idCalls.Add(1)
					if !test.exhausted && call == 2 {
						return test.wantTarget, nil
					}
					return "ss_collision", nil
				},
			})
			appender := &recordingSavedSearchAuditAppender{}
			audited := newSavedSearchAuditStore(t, store, appender)
			var result *opensplunkv1.SavedSearch
			var err error
			if test.action == SavedSearchMutationAuditActionCreate {
				result, err = audited.Create(ctx, scope, savedSearchDefinition("collision-retry", ""))
			} else {
				result, err = audited.Duplicate(ctx, scope, source.SavedSearchId, "collision-copy", nil)
			}
			calls := appender.snapshot()
			if test.exhausted {
				if err == nil || result != nil {
					t.Fatalf("exhausted mutation = %+v, %v, want failure", result, err)
				}
				if len(calls) != 0 {
					t.Fatalf("exhausted collision attempts emitted audit calls: %#v", calls)
				}
				if idCalls.Load() != maximumIDAttempts {
					t.Fatalf("exhausted ID calls = %d, want %d", idCalls.Load(), maximumIDAttempts)
				}
				return
			}
			if err != nil || result == nil || result.SavedSearchId != test.wantTarget {
				t.Fatalf("collision retry mutation = %+v, %v, want target %q", result, err, test.wantTarget)
			}
			if idCalls.Load() != 2 ||
				len(calls) != 1 ||
				calls[0].event.Action != test.action ||
				calls[0].event.SavedSearchID != test.wantTarget ||
				calls[0].event.SavedSearchVersion != 1 ||
				!calls[0].rowFound {
				t.Fatalf("collision retry audit calls = %#v, ID calls %d", calls, idCalls.Load())
			}
		})
	}
}

func TestAuditedStoreConcurrentSavedSearchMutationsPublishOnlyTheWinner(t *testing.T) {
	t.Parallel()

	t.Run("optimistic update", func(t *testing.T) {
		_, store := openTestStore(t)
		ctx := context.Background()
		scope := AccessScope{OwnerID: "owner"}
		created, err := store.Create(ctx, scope, savedSearchDefinition("concurrent-update", ""))
		if err != nil {
			t.Fatalf("seed Create() error = %v", err)
		}
		appender := &recordingSavedSearchAuditAppender{}
		audited := newSavedSearchAuditStore(t, store, appender)
		start := make(chan struct{})
		errorsByWriter := make(chan error, 2)
		for _, name := range []string{"winner-a", "winner-b"} {
			go func() {
				replacement := proto.Clone(created.Definition).(*opensplunkv1.SavedSearchDefinition)
				replacement.Name = name
				<-start
				_, updateErr := audited.Update(
					ctx,
					scope,
					created.SavedSearchId,
					created.Version,
					replacement,
					nil,
				)
				errorsByWriter <- updateErr
			}()
		}
		close(start)
		var succeeded, conflicted int
		for range 2 {
			switch err := <-errorsByWriter; {
			case err == nil:
				succeeded++
			case errors.Is(err, control.ErrVersionConflict):
				conflicted++
			default:
				t.Fatalf("concurrent Update() error = %v", err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("concurrent Update() outcomes = success %d conflict %d", succeeded, conflicted)
		}
		calls := appender.snapshot()
		if len(calls) != 1 ||
			calls[0].event.Action != SavedSearchMutationAuditActionUpdate ||
			calls[0].event.SavedSearchID != created.SavedSearchId ||
			calls[0].event.SavedSearchVersion != created.Version+1 ||
			!calls[0].rowFound {
			t.Fatalf("concurrent Update() audit calls = %#v", calls)
		}
	})

	t.Run("unique name create", func(t *testing.T) {
		_, store := openTestStore(t)
		ctx := context.Background()
		scope := AccessScope{OwnerID: "owner"}
		appender := &recordingSavedSearchAuditAppender{}
		audited := newSavedSearchAuditStore(t, store, appender)
		start := make(chan struct{})
		errorsByWriter := make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				_, createErr := audited.Create(ctx, scope, savedSearchDefinition("concurrent-create", ""))
				errorsByWriter <- createErr
			}()
		}
		close(start)
		var succeeded, conflicted int
		for range 2 {
			switch err := <-errorsByWriter; {
			case err == nil:
				succeeded++
			case errors.Is(err, control.ErrAlreadyExists):
				conflicted++
			default:
				t.Fatalf("concurrent Create() error = %v", err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("concurrent Create() outcomes = success %d conflict %d", succeeded, conflicted)
		}
		calls := appender.snapshot()
		if len(calls) != 1 ||
			calls[0].event.Action != SavedSearchMutationAuditActionCreate ||
			calls[0].event.SavedSearchVersion != 1 ||
			!calls[0].rowFound {
			t.Fatalf("concurrent Create() audit calls = %#v", calls)
		}
	})
}

func TestNewAuditedStoreRejectsInvalidSavedSearchAuditConfiguration(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	validAppender := &recordingSavedSearchAuditAppender{}
	var typedNil *recordingSavedSearchAuditAppender
	tests := map[string]struct {
		store    *Store
		tenantID string
		appender SavedSearchMutationAuditAppender
	}{
		"nil store": {
			tenantID: savedSearchAuditTestTenant,
			appender: validAppender,
		},
		"invalid store": {
			store:    &Store{},
			tenantID: savedSearchAuditTestTenant,
			appender: validAppender,
		},
		"empty tenant": {
			store:    store,
			appender: validAppender,
		},
		"noncanonical tenant": {
			store:    store,
			tenantID: " " + savedSearchAuditTestTenant,
			appender: validAppender,
		},
		"oversized tenant": {
			store:    store,
			tenantID: strings.Repeat("t", 256),
			appender: validAppender,
		},
		"nil appender": {
			store:    store,
			tenantID: savedSearchAuditTestTenant,
		},
		"typed nil appender": {
			store:    store,
			tenantID: savedSearchAuditTestTenant,
			appender: typedNil,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			audited, err := NewAuditedStore(test.store, test.tenantID, test.appender)
			if audited != nil || !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("NewAuditedStore() = %v, %v, want nil/ErrInvalidArgument", audited, err)
			}
		})
	}
	valid, err := NewAuditedStore(store, savedSearchAuditTestTenant, validAppender)
	if err != nil || valid == nil {
		t.Fatalf("NewAuditedStore(valid) = %v, %v", valid, err)
	}
	if database.GORMDB() == nil {
		t.Fatal("test control database unexpectedly lacks GORM")
	}
}

func newSavedSearchAuditRawStore(t *testing.T, database *control.DB, options Options) *Store {
	t.Helper()
	store, err := New(database, options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func newSavedSearchAuditStore(
	t *testing.T,
	store *Store,
	appender SavedSearchMutationAuditAppender,
) *AuditedStore {
	t.Helper()
	audited, err := NewAuditedStore(store, savedSearchAuditTestTenant, appender)
	if err != nil {
		t.Fatalf("NewAuditedStore() error = %v", err)
	}
	return audited
}

func readSavedSearchAuditPersistence(t *testing.T, database *control.DB) []savedSearchRecord {
	t.Helper()
	var records []savedSearchRecord
	if err := database.GORMDB().Order("saved_search_id ASC").Find(&records).Error; err != nil {
		t.Fatalf("read saved-search persistence: %v", err)
	}
	for index := range records {
		records[index].DefinitionProto = slices.Clone(records[index].DefinitionProto)
	}
	return records
}
