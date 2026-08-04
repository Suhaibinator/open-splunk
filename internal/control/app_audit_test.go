package control

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

	"gorm.io/gorm"
)

var errTestAppAuditAppend = errors.New("test app audit append failure")

type recordedAppMutationAudit struct {
	tenantID       string
	event          AppMutationAuditEvent
	rowPresent     bool
	rowVersion     int64
	rowState       AppState
	rowOccurredAt  int64
	membershipRows int64
}

type recordingAppMutationAuditAppender struct {
	mu         sync.Mutex
	calls      []recordedAppMutationAudit
	failAction AppMutationAuditAction
}

func (appender *recordingAppMutationAuditAppender) AppendAppMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event AppMutationAuditEvent,
) error {
	if ctx == nil || tx == nil || tx.Statement == nil {
		return errors.New("test app audit appender received an invalid transaction")
	}
	if _, ok := tx.Statement.ConnPool.(*sql.Tx); !ok {
		return errors.New("test app audit appender was not called inside a SQL transaction")
	}

	recorded := recordedAppMutationAudit{
		tenantID: strings.Clone(tenantID),
		event: AppMutationAuditEvent{
			OccurredAt: event.OccurredAt,
			Action:     event.Action,
			AppID:      strings.Clone(event.AppID),
			AppVersion: event.AppVersion,
		},
	}
	var row appRecord
	rowErr := tx.Where(
		"tenant_id = ? AND app_id = ?",
		tenantID,
		event.AppID,
	).Take(&row).Error
	switch {
	case rowErr == nil:
		recorded.rowPresent = true
		recorded.rowVersion = row.Version
		recorded.rowState = row.State
		if event.Action == AppMutationAuditActionCreate {
			recorded.rowOccurredAt = row.CreatedAtUnixMicro
		} else {
			recorded.rowOccurredAt = row.UpdatedAtUnixMicro
		}
	case errors.Is(rowErr, gorm.ErrRecordNotFound):
	default:
		return fmt.Errorf("test app audit appender read app row: %w", rowErr)
	}
	if err := tx.Model(&appDefaultIndexRecord{}).
		Where("tenant_id = ? AND app_id = ?", tenantID, event.AppID).
		Count(&recorded.membershipRows).Error; err != nil {
		return fmt.Errorf("test app audit appender count memberships: %w", err)
	}

	appender.mu.Lock()
	defer appender.mu.Unlock()
	appender.calls = append(appender.calls, recorded)
	if appender.failAction != "" && appender.failAction == event.Action {
		return errTestAppAuditAppend
	}
	return nil
}

func (appender *recordingAppMutationAuditAppender) snapshot() []recordedAppMutationAudit {
	appender.mu.Lock()
	defer appender.mu.Unlock()
	return slices.Clone(appender.calls)
}

func TestAuditedAppCatalogPublishesSuccessfulLifecycleInMutationTransactions(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	mainIndex := mustCreateIndex(t, db, enabledIndex("app-audit-main"))
	auditIndex := mustCreateIndex(t, db, enabledIndex("app-audit-events"))
	base := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	var clockCalls atomic.Int64
	catalog := newAppAuditTestCatalog(
		t,
		db,
		func() time.Time {
			return base.Add(time.Duration(clockCalls.Add(1)) * time.Microsecond)
		},
		func() (string, error) { return appAuditTestID(1), nil },
	)
	appender := &recordingAppMutationAuditAppender{}
	audited := newTestAuditedAppCatalog(t, catalog, appender)
	scope := AppAccessScope{TenantID: "tenant-a"}

	created, err := audited.CreateApp(
		ctx,
		scope,
		appDefinitionWithIndexes("audited-lifecycle", mainIndex.Definition.Name),
	)
	if err != nil {
		t.Fatalf("CreateApp() error = %v", err)
	}
	read, err := audited.GetApp(ctx, scope, AppSelector{AppID: created.ID})
	if err != nil || !reflect.DeepEqual(read, created) {
		t.Fatalf("GetApp() = %#v, %v, want %#v", read, err, created)
	}
	listed, err := audited.ListApps(ctx, scope, AppListRequest{PageSize: 1})
	if err != nil || len(listed.Apps) != 1 || !reflect.DeepEqual(listed.Apps[0], created) {
		t.Fatalf("ListApps() = %#v, %v", listed, err)
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("delegated reads emitted audit events: %#v", calls)
	}

	replacement := created.Definition
	replacement.DisplayName = "Audited lifecycle updated"
	replacement.DefaultIndexes = []string{auditIndex.Definition.Name}
	updated, err := audited.UpdateApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		created.Version,
		replacement,
	)
	if err != nil {
		t.Fatalf("UpdateApp() error = %v", err)
	}
	archived, err := audited.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		updated.Version,
		AppStateArchived,
	)
	if err != nil {
		t.Fatalf("SetAppState(archive) error = %v", err)
	}
	active, err := audited.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archived.Version,
		AppStateActive,
	)
	if err != nil {
		t.Fatalf("SetAppState(activate) error = %v", err)
	}
	archivedAgain, err := audited.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		active.Version,
		AppStateArchived,
	)
	if err != nil {
		t.Fatalf("SetAppState(archive again) error = %v", err)
	}
	deletedID, err := audited.DeleteApp(
		ctx,
		scope,
		AppSelector{AppID: created.ID},
		archivedAgain.Version,
		archivedAgain.Definition.Slug,
	)
	if err != nil || deletedID != created.ID {
		t.Fatalf("DeleteApp() = %q, %v, want %q", deletedID, err, created.ID)
	}

	calls := appender.snapshot()
	wantActions := []AppMutationAuditAction{
		AppMutationAuditActionCreate,
		AppMutationAuditActionUpdate,
		AppMutationAuditActionArchive,
		AppMutationAuditActionActivate,
		AppMutationAuditActionArchive,
		AppMutationAuditActionDelete,
	}
	wantVersions := []uint64{1, 2, 3, 4, 5, 5}
	wantStates := []AppState{
		AppStateActive,
		AppStateActive,
		AppStateArchived,
		AppStateActive,
		AppStateArchived,
		"",
	}
	if len(calls) != len(wantActions) {
		t.Fatalf("app audit calls = %#v, want %d", calls, len(wantActions))
	}
	for position, call := range calls {
		wantTime := base.Add(time.Duration(position+1) * time.Microsecond)
		if call.tenantID != scope.TenantID ||
			call.event.Action != wantActions[position] ||
			call.event.AppID != created.ID ||
			call.event.AppVersion != wantVersions[position] ||
			!call.event.OccurredAt.Equal(wantTime) ||
			call.event.OccurredAt.Location() != time.UTC {
			t.Fatalf("app audit call %d = %#v", position, call)
		}
		if position == len(calls)-1 {
			if call.rowPresent || call.membershipRows != 0 {
				t.Fatalf("delete audit observed retained persistence: %#v", call)
			}
			continue
		}
		if !call.rowPresent ||
			call.rowVersion != int64(wantVersions[position]) ||
			call.rowState != wantStates[position] ||
			call.rowOccurredAt != wantTime.UnixMicro() ||
			call.membershipRows != 1 {
			t.Fatalf("app audit call %d did not observe completed mutation: %#v", position, call)
		}
	}
	if clockCalls.Load() != int64(len(wantActions)) {
		t.Fatalf("app mutation clock calls = %d, want %d", clockCalls.Load(), len(wantActions))
	}
}

func TestAuditedAppCatalogRollsBackEveryMutationWhenAuditFails(t *testing.T) {
	t.Parallel()

	for _, action := range []AppMutationAuditAction{
		AppMutationAuditActionCreate,
		AppMutationAuditActionUpdate,
		AppMutationAuditActionArchive,
		AppMutationAuditActionActivate,
		AppMutationAuditActionDelete,
	} {
		action := action
		t.Run(string(action), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openTestDB(t)
			firstIndex := mustCreateIndex(t, db, enabledIndex("app-audit-first"))
			secondIndex := mustCreateIndex(t, db, enabledIndex("app-audit-second"))
			catalog := newTestAppCatalog(t, db)
			scope := AppAccessScope{TenantID: "tenant-a"}
			var existing AppWorkspace
			if action != AppMutationAuditActionCreate {
				var err error
				existing, err = catalog.CreateApp(
					ctx,
					scope,
					appDefinitionWithIndexes("audit-rollback", firstIndex.Definition.Name),
				)
				if err != nil {
					t.Fatalf("seed CreateApp() error = %v", err)
				}
				if action == AppMutationAuditActionActivate ||
					action == AppMutationAuditActionDelete {
					existing, err = catalog.SetAppState(
						ctx,
						scope,
						AppSelector{AppID: existing.ID},
						existing.Version,
						AppStateArchived,
					)
					if err != nil {
						t.Fatalf("seed SetAppState() error = %v", err)
					}
				}
			}
			before := readAppAuditPersistence(t, db)
			appender := &recordingAppMutationAuditAppender{failAction: action}
			audited := newTestAuditedAppCatalog(t, catalog, appender)

			var mutationErr error
			switch action {
			case AppMutationAuditActionCreate:
				_, mutationErr = audited.CreateApp(
					ctx,
					scope,
					appDefinitionWithIndexes("audit-rollback-create", firstIndex.Definition.Name),
				)
			case AppMutationAuditActionUpdate:
				replacement := existing.Definition
				replacement.DisplayName = "must roll back"
				replacement.DefaultIndexes = []string{secondIndex.Definition.Name}
				_, mutationErr = audited.UpdateApp(
					ctx,
					scope,
					AppSelector{AppID: existing.ID},
					existing.Version,
					replacement,
				)
			case AppMutationAuditActionArchive:
				_, mutationErr = audited.SetAppState(
					ctx,
					scope,
					AppSelector{AppID: existing.ID},
					existing.Version,
					AppStateArchived,
				)
			case AppMutationAuditActionActivate:
				_, mutationErr = audited.SetAppState(
					ctx,
					scope,
					AppSelector{AppID: existing.ID},
					existing.Version,
					AppStateActive,
				)
			case AppMutationAuditActionDelete:
				_, mutationErr = audited.DeleteApp(
					ctx,
					scope,
					AppSelector{AppID: existing.ID},
					existing.Version,
					existing.Definition.Slug,
				)
			}
			if !errors.Is(mutationErr, errTestAppAuditAppend) {
				t.Fatalf("%s error = %v, want audit failure", action, mutationErr)
			}
			if calls := appender.snapshot(); len(calls) != 1 {
				t.Fatalf("%s audit calls = %#v, want one failed call", action, calls)
			}
			after := readAppAuditPersistence(t, db)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("%s persistence after rollback = %#v, want %#v", action, after, before)
			}
		})
	}
}

func TestAuditedAppCatalogDoesNotAuditRejectedMutationsOrReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	scope := AppAccessScope{TenantID: "tenant-a"}
	existing, err := catalog.CreateApp(ctx, scope, validAppDefinition("rejected-audit"))
	if err != nil {
		t.Fatalf("seed CreateApp() error = %v", err)
	}
	appender := &recordingAppMutationAuditAppender{}
	audited := newTestAuditedAppCatalog(t, catalog, appender)

	if _, err := audited.GetApp(ctx, scope, AppSelector{AppID: existing.ID}); err != nil {
		t.Fatalf("GetApp() error = %v", err)
	}
	if _, err := audited.ListApps(ctx, scope, AppListRequest{}); err != nil {
		t.Fatalf("ListApps() error = %v", err)
	}
	if _, err := audited.CreateApp(ctx, scope, validAppDefinition(" REJECTED-AUDIT ")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate CreateApp() error = %v, want ErrAlreadyExists", err)
	}
	if _, err := audited.UpdateApp(
		ctx,
		scope,
		AppSelector{AppID: existing.ID},
		existing.Version+1,
		existing.Definition,
	); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale UpdateApp() error = %v, want ErrVersionConflict", err)
	}
	rename := existing.Definition
	rename.Slug = "renamed"
	if _, err := audited.UpdateApp(
		ctx,
		scope,
		AppSelector{AppID: existing.ID},
		existing.Version,
		rename,
	); !errors.Is(err, ErrImmutableSlug) {
		t.Fatalf("immutable UpdateApp() error = %v, want ErrImmutableSlug", err)
	}
	if _, err := audited.SetAppState(
		ctx,
		scope,
		AppSelector{AppID: existing.ID},
		existing.Version+1,
		AppStateArchived,
	); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale SetAppState() error = %v, want ErrVersionConflict", err)
	}
	if _, err := audited.DeleteApp(
		ctx,
		scope,
		AppSelector{AppID: existing.ID},
		existing.Version,
		existing.Definition.Slug,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("active DeleteApp() error = %v, want ErrDependencyConflict", err)
	}
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("rejected app operations emitted audit calls: %#v", calls)
	}
}

func TestAuditedAppCatalogIDCollisionsPublishOnlyCommittedCreate(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		succeed    bool
		wantIDCall int64
	}{
		{name: "retry succeeds", succeed: true, wantIDCall: 2},
		{name: "retry exhausted", wantIDCall: maximumAppIDAttempts},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openTestDB(t)
			collisionID := appAuditTestID(10)
			seed := newAppAuditTestCatalog(
				t,
				db,
				func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
				func() (string, error) { return collisionID, nil },
			)
			if _, err := seed.CreateApp(
				ctx,
				AppAccessScope{TenantID: "seed-tenant"},
				validAppDefinition("collision-owner"),
			); err != nil {
				t.Fatalf("seed collision ID: %v", err)
			}

			base := time.Date(2026, time.August, 4, 14, 0, 0, 0, time.UTC)
			var clockCalls atomic.Int64
			var idCalls atomic.Int64
			successID := appAuditTestID(11)
			catalog := newAppAuditTestCatalog(
				t,
				db,
				func() time.Time {
					clockCalls.Add(1)
					return base
				},
				func() (string, error) {
					call := idCalls.Add(1)
					if test.succeed && call == 2 {
						return successID, nil
					}
					return collisionID, nil
				},
			)
			appender := &recordingAppMutationAuditAppender{}
			audited := newTestAuditedAppCatalog(t, catalog, appender)
			created, err := audited.CreateApp(
				ctx,
				AppAccessScope{TenantID: "tenant-a"},
				validAppDefinition("collision-retry"),
			)
			if test.succeed {
				if err != nil || created.ID != successID {
					t.Fatalf("CreateApp() = %#v, %v, want ID %q", created, err, successID)
				}
				calls := appender.snapshot()
				if len(calls) != 1 ||
					calls[0].event.AppID != successID ||
					calls[0].event.AppVersion != 1 ||
					!calls[0].event.OccurredAt.Equal(base) {
					t.Fatalf("collision retry audit calls = %#v", calls)
				}
			} else {
				if err == nil || !reflect.DeepEqual(created, AppWorkspace{}) {
					t.Fatalf("exhausted CreateApp() = %#v, %v, want failure", created, err)
				}
				if calls := appender.snapshot(); len(calls) != 0 {
					t.Fatalf("colliding attempts emitted audit calls: %#v", calls)
				}
			}
			if idCalls.Load() != test.wantIDCall || clockCalls.Load() != 1 {
				t.Fatalf(
					"collision calls = ID %d clock %d, want %d/1",
					idCalls.Load(),
					clockCalls.Load(),
					test.wantIDCall,
				)
			}
		})
	}
}

func TestNewAuditedAppCatalogRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	valid := &recordingAppMutationAuditAppender{}
	var typedNil *recordingAppMutationAuditAppender
	for name, test := range map[string]struct {
		catalog  *AppCatalog
		appender AppMutationAuditAppender
	}{
		"nil catalog":        {appender: valid},
		"nil appender":       {catalog: catalog},
		"typed nil appender": {catalog: catalog, appender: typedNil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			audited, err := NewAuditedAppCatalog(test.catalog, test.appender)
			if audited != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("NewAuditedAppCatalog() = %v, %v, want nil/ErrInvalidArgument", audited, err)
			}
		})
	}
}

type appAuditPersistence struct {
	apps        []appRecord
	memberships []appDefaultIndexRecord
	revisions   []appCatalogRevisionRecord
}

func readAppAuditPersistence(t *testing.T, db *DB) appAuditPersistence {
	t.Helper()
	result := appAuditPersistence{}
	if err := db.GORMDB().Order("tenant_id, app_id").Find(&result.apps).Error; err != nil {
		t.Fatalf("read app audit workspace snapshot: %v", err)
	}
	if err := db.GORMDB().Order("tenant_id, app_id, index_id").
		Find(&result.memberships).Error; err != nil {
		t.Fatalf("read app audit membership snapshot: %v", err)
	}
	if err := db.GORMDB().Order("tenant_id").Find(&result.revisions).Error; err != nil {
		t.Fatalf("read app audit revision snapshot: %v", err)
	}
	return result
}

func newTestAuditedAppCatalog(
	t *testing.T,
	catalog *AppCatalog,
	appender AppMutationAuditAppender,
) *AuditedAppCatalog {
	t.Helper()
	audited, err := NewAuditedAppCatalog(catalog, appender)
	if err != nil {
		t.Fatalf("NewAuditedAppCatalog() error = %v", err)
	}
	return audited
}

func newAppAuditTestCatalog(
	t *testing.T,
	db *DB,
	clock func() time.Time,
	idGenerator func() (string, error),
) *AppCatalog {
	t.Helper()
	catalog, err := NewAppCatalog(db, AppCatalogOptions{
		CursorKey:   []byte("app-audit-test-cursor-key-32-bytes-minimum"),
		Clock:       clock,
		IDGenerator: idGenerator,
	})
	if err != nil {
		t.Fatalf("NewAppCatalog() error = %v", err)
	}
	return catalog
}

func appAuditTestID(value int) string {
	return fmt.Sprintf("app_%021dA", value)
}
