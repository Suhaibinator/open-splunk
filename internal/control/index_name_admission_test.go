package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

var errTestIndexNameAdmission = errors.New("test index-name admission failure")

type indexNameAdmissionValidatorFunc func(
	context.Context,
	*gorm.DB,
	IndexNameAdmissionRequest,
) error

func (validator indexNameAdmissionValidatorFunc) ValidateIndexNameAdmissionInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	request IndexNameAdmissionRequest,
) error {
	return validator(ctx, tx, request)
}

type typedNilIndexNameAdmissionValidator struct{}

func (*typedNilIndexNameAdmissionValidator) ValidateIndexNameAdmissionInTransaction(
	context.Context,
	*gorm.DB,
	IndexNameAdmissionRequest,
) error {
	return nil
}

type indexMutationAuditAppenderFunc func(
	context.Context,
	*gorm.DB,
	string,
	IndexMutationAuditEvent,
) error

func (appender indexMutationAuditAppenderFunc) AppendIndexMutationInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event IndexMutationAuditEvent,
) error {
	return appender(ctx, tx, tenantID, event)
}

func TestIndexNameAdmissionValidatorUsesExactCreationTransactionAndOrder(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	var mu sync.Mutex
	var order []string
	var admissionSQLTx *sql.Tx
	validator := indexNameAdmissionValidatorFunc(func(
		ctx context.Context,
		tx *gorm.DB,
		request IndexNameAdmissionRequest,
	) error {
		if ctx == nil || tx == nil || tx.Statement == nil {
			return errors.New("invalid admission transaction")
		}
		var ok bool
		admissionSQLTx, ok = tx.Statement.ConnPool.(*sql.Tx)
		if !ok || admissionSQLTx == nil {
			return errors.New("admission did not receive a SQL transaction")
		}
		want := IndexNameAdmissionRequest{
			CanonicalName:             "candidate_one",
			IndexCatalogRevision:      1,
			IndexCatalogPhysicalCount: 0,
		}
		if !reflect.DeepEqual(request, want) {
			return fmt.Errorf("admission request = %#v, want %#v", request, want)
		}
		var count int64
		if err := tx.Table("indexes").
			Where("name = ?", request.CanonicalName).
			Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("candidate exists before admission: %d", count)
		}
		mu.Lock()
		order = append(order, "validator")
		mu.Unlock()
		return nil
	})
	appender := indexMutationAuditAppenderFunc(func(
		ctx context.Context,
		tx *gorm.DB,
		tenantID string,
		event IndexMutationAuditEvent,
	) error {
		if ctx == nil || tx == nil || tx.Statement == nil {
			return errors.New("invalid audit transaction")
		}
		auditSQLTx, ok := tx.Statement.ConnPool.(*sql.Tx)
		if !ok || auditSQLTx == nil || auditSQLTx != admissionSQLTx {
			return errors.New("audit did not receive the admission SQL transaction")
		}
		if tenantID != "tenant-a" ||
			event.Action != IndexMutationAuditActionCreate ||
			event.IndexVersion != 1 {
			return fmt.Errorf("unexpected audit projection: %q/%#v", tenantID, event)
		}
		var count int64
		if err := tx.Table("indexes").
			Where("name = ?", "candidate_one").
			Count(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("candidate count at audit = %d, want 1", count)
		}
		mu.Lock()
		order = append(order, "audit")
		mu.Unlock()
		return nil
	})
	administration := newTestAuditedIndexAdministrationWithValidator(
		t,
		database,
		appender,
		validator,
	)

	definition := enabledIndex("candidate_one")
	definition.Name = " Candidate_One "
	created, err := administration.CreateIndex(context.Background(), definition)
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	if created.Definition.Name != "candidate_one" {
		t.Fatalf("created name = %q, want candidate_one", created.Definition.Name)
	}
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	mu.Unlock()
	if !reflect.DeepEqual(gotOrder, []string{"validator", "audit"}) {
		t.Fatalf("callback order = %v, want [validator audit]", gotOrder)
	}
}

func TestIndexNameAdmissionFailureAndCancellationRollBackCreation(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		validator func(context.CancelFunc) IndexNameAdmissionValidator
		want      error
	}{
		"validator error": {
			validator: func(context.CancelFunc) IndexNameAdmissionValidator {
				return indexNameAdmissionValidatorFunc(func(
					context.Context,
					*gorm.DB,
					IndexNameAdmissionRequest,
				) error {
					return errTestIndexNameAdmission
				})
			},
			want: errTestIndexNameAdmission,
		},
		"validator cancellation": {
			validator: func(cancel context.CancelFunc) IndexNameAdmissionValidator {
				return indexNameAdmissionValidatorFunc(func(
					context.Context,
					*gorm.DB,
					IndexNameAdmissionRequest,
				) error {
					cancel()
					return nil
				})
			},
			want: context.Canceled,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			database := openTestDB(t)
			before := readTestIndexCatalogState(t, database)
			appender := &recordingIndexMutationAuditAppender{}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			administration := newTestAuditedIndexAdministrationWithValidator(
				t,
				database,
				appender,
				test.validator(cancel),
			)

			_, err := administration.CreateIndex(ctx, enabledIndex("rejected"))
			if !errors.Is(err, test.want) {
				t.Fatalf("CreateIndex() error = %v, want %v", err, test.want)
			}
			assertIndexNameAdmissionRollback(t, database, appender, before, "rejected")
		})
	}
}

func TestIndexNameAdmissionDetectsCatalogFactDrift(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	before := readTestIndexCatalogState(t, database)
	appender := &recordingIndexMutationAuditAppender{}
	validator := indexNameAdmissionValidatorFunc(func(
		_ context.Context,
		tx *gorm.DB,
		_ IndexNameAdmissionRequest,
	) error {
		record := newIndexRecord(
			"idx_admission_drift",
			enabledIndex("admission-drift"),
			databaseTime(time.Now()),
		)
		return tx.Create(&record).Error
	})
	administration := newTestAuditedIndexAdministrationWithValidator(
		t,
		database,
		appender,
		validator,
	)

	_, err := administration.CreateIndex(
		context.Background(),
		enabledIndex("candidate-after-drift"),
	)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("CreateIndex() error = %v, want ErrVersionConflict", err)
	}
	assertIndexNameAdmissionRollback(
		t,
		database,
		appender,
		before,
		"candidate-after-drift",
	)
	assertTableCount(t, database, "indexes", 0)
}

func TestIndexNameAdmissionRunsForSearchDisabledIndex(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	var requests []IndexNameAdmissionRequest
	validator := indexNameAdmissionValidatorFunc(func(
		_ context.Context,
		_ *gorm.DB,
		request IndexNameAdmissionRequest,
	) error {
		requests = append(requests, request)
		return nil
	})
	administration := newTestAuditedIndexAdministrationWithValidator(
		t,
		database,
		&recordingIndexMutationAuditAppender{},
		validator,
	)
	definition := enabledIndex("search-disabled")
	definition.SearchEnabled = false

	created, err := administration.CreateIndex(context.Background(), definition)
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	if created.Definition.SearchEnabled || len(requests) != 1 ||
		requests[0].CanonicalName != "search-disabled" {
		t.Fatalf("created/requests = %#v/%#v", created, requests)
	}
}

func TestIndexNameAdmissionDuplicatePrecedesValidator(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	if _, err := database.CreateIndex(
		context.Background(),
		enabledIndex("duplicate-admission"),
	); err != nil {
		t.Fatalf("seed index: %v", err)
	}
	before := readTestIndexCatalogState(t, database)
	var validatorCalls int
	validator := indexNameAdmissionValidatorFunc(func(
		context.Context,
		*gorm.DB,
		IndexNameAdmissionRequest,
	) error {
		validatorCalls++
		return errTestIndexNameAdmission
	})
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministrationWithValidator(
		t,
		database,
		appender,
		validator,
	)

	_, err := administration.CreateIndex(
		context.Background(),
		enabledIndex("duplicate-admission"),
	)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateIndex() error = %v, want ErrAlreadyExists", err)
	}
	if validatorCalls != 0 {
		t.Fatalf("validator calls = %d, want 0", validatorCalls)
	}
	assertIndexNameAdmissionRollback(
		t,
		database,
		appender,
		before,
		"not-created",
	)
}

func TestIndexNameAdmissionCapacityAndIDChecksPrecedeValidator(t *testing.T) {
	t.Parallel()

	for name, run := range map[string]func(
		*testing.T,
		*DB,
		IndexNameAdmissionValidator,
	) error{
		"capacity": func(
			t *testing.T,
			database *DB,
			validator IndexNameAdmissionValidator,
		) error {
			seedIndexCatalog(t, database, MaximumPhysicalIndexRecords)
			_, err := database.createIndexOnce(
				context.Background(),
				"idx_capacity_precedence",
				enabledIndex("capacity-precedence"),
				databaseTime(time.Now()),
				nil,
				validator,
			)
			return err
		},
		"ID collision": func(
			t *testing.T,
			database *DB,
			validator IndexNameAdmissionValidator,
		) error {
			created, err := database.CreateIndex(
				context.Background(),
				enabledIndex("existing-id"),
			)
			if err != nil {
				t.Fatalf("seed index: %v", err)
			}
			_, err = database.createIndexOnce(
				context.Background(),
				created.ID,
				enabledIndex("id-precedence"),
				databaseTime(time.Now()),
				nil,
				validator,
			)
			return err
		},
	} {
		name, run := name, run
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			database := openTestDB(t)
			var calls int
			validator := indexNameAdmissionValidatorFunc(func(
				context.Context,
				*gorm.DB,
				IndexNameAdmissionRequest,
			) error {
				calls++
				return errTestIndexNameAdmission
			})
			err := run(t, database, validator)
			want := ErrCapacityExceeded
			if name == "ID collision" {
				want = errIndexIDCollision
			}
			if !errors.Is(err, want) {
				t.Fatalf("createIndexOnce() error = %v, want %v", err, want)
			}
			if calls != 0 {
				t.Fatalf("validator calls = %d, want 0", calls)
			}
		})
	}
}

func TestNilIndexNameAdmissionAllowsEmptyKnowledgeCatalog(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	if _, err := database.CreateIndex(
		context.Background(),
		enabledIndex("raw-empty-catalog"),
	); err != nil {
		t.Fatalf("raw CreateIndex() error = %v", err)
	}
	appender := &recordingIndexMutationAuditAppender{}
	administration := newTestAuditedIndexAdministration(
		t,
		database,
		"tenant-a",
		appender,
	)
	if _, err := administration.CreateIndex(
		context.Background(),
		enabledIndex("audited-empty-catalog"),
	); err != nil {
		t.Fatalf("audited CreateIndex() error = %v", err)
	}
	if calls := appender.snapshot(); len(calls) != 1 ||
		calls[0].event.Action != IndexMutationAuditActionCreate {
		t.Fatalf("audit calls = %#v", calls)
	}
}

func TestNilIndexNameAdmissionRejectsEitherActiveKnowledgeDriver(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		name         string
		activeLedger bool
		activeRow    bool
	}{
		{name: "ledger only", activeLedger: true},
		{name: "registry only", activeRow: true},
		{name: "both", activeLedger: true, activeRow: true},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			database := openTestDB(t)
			seedActiveKnowledgeAdmissionEvidence(t, database)
			corruptActiveKnowledgeAdmissionEvidence(
				t,
				database,
				fixture.activeLedger,
				fixture.activeRow,
			)
			before := readTestIndexCatalogState(t, database)

			_, err := database.CreateIndex(
				context.Background(),
				enabledIndex("blocked-by-knowledge"),
			)
			if !errors.Is(err, ErrDependencyConflict) {
				t.Fatalf("CreateIndex() error = %v, want ErrDependencyConflict", err)
			}
			assertTestIndexCatalogState(t, database, before)
			assertIndexNameCount(t, database, "blocked-by-knowledge", 0)
		})
	}
}

func TestNilIndexNameAdmissionFailsClosedWhenDriverIsMissing(t *testing.T) {
	t.Parallel()

	database := openTestDB(t)
	if err := database.GORMDB().Exec(
		"DROP INDEX knowledge_objects_active_tenant_idx",
	).Error; err != nil {
		t.Fatalf("drop admission driver: %v", err)
	}
	before := readTestIndexCatalogState(t, database)

	_, err := database.CreateIndex(
		context.Background(),
		enabledIndex("missing-driver"),
	)
	if err == nil || !strings.Contains(err.Error(), "knowledge_objects_active_tenant_idx") {
		t.Fatalf("CreateIndex() error = %v, want missing forced driver", err)
	}
	assertTestIndexCatalogState(t, database, before)
	assertIndexNameCount(t, database, "missing-driver", 0)
}

func newTestAuditedIndexAdministrationWithValidator(
	t *testing.T,
	database *DB,
	appender IndexMutationAuditAppender,
	validator IndexNameAdmissionValidator,
) *AuditedIndexAdministration {
	t.Helper()
	administration, err := NewAuditedIndexAdministration(
		database,
		AuditedIndexAdministrationOptions{
			TenantID:  "tenant-a",
			Appender:  appender,
			Validator: validator,
		},
	)
	if err != nil {
		t.Fatalf("NewAuditedIndexAdministration() error = %v", err)
	}
	return administration
}

func assertIndexNameAdmissionRollback(
	t *testing.T,
	database *DB,
	appender *recordingIndexMutationAuditAppender,
	wantState indexCatalogStateRecord,
	name string,
) {
	t.Helper()
	assertTestIndexCatalogState(t, database, wantState)
	assertIndexNameCount(t, database, name, 0)
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("audit calls = %#v, want none", calls)
	}
}

func assertIndexNameCount(t *testing.T, database *DB, name string, want int64) {
	t.Helper()
	var count int64
	if err := database.GORMDB().Table("indexes").
		Where("name = ?", name).
		Count(&count).Error; err != nil {
		t.Fatalf("count index name %q: %v", name, err)
	}
	if count != want {
		t.Fatalf("index name %q count = %d, want %d", name, count, want)
	}
}

func seedActiveKnowledgeAdmissionEvidence(t *testing.T, database *DB) {
	t.Helper()
	seedKnowledgeStatePrerequisites(t, database.SQLDB())
	insertKnowledgeStateVersion(t, database.SQLDB(), stateVersionFixture{
		objectID:  "ko-index-admission",
		version:   1,
		state:     "active",
		mutation:  "create",
		timestamp: 10,
	})
}

func corruptActiveKnowledgeAdmissionEvidence(
	t *testing.T,
	database *DB,
	keepLedger bool,
	keepRegistry bool,
) {
	t.Helper()
	if keepLedger && keepRegistry {
		return
	}
	database.SQLDB().SetMaxOpenConns(1)
	database.SQLDB().SetMaxIdleConns(1)
	conn, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(
		context.Background(),
		"PRAGMA foreign_keys = OFF",
	); err != nil {
		t.Fatalf("disable fixture foreign keys: %v", err)
	}
	if !keepRegistry {
		dropTableTriggersForIndexAdmission(t, conn, "knowledge_objects")
		if _, err := conn.ExecContext(
			context.Background(),
			"DELETE FROM knowledge_objects",
		); err != nil {
			t.Fatalf("remove ACTIVE registry evidence: %v", err)
		}
	}
	if !keepLedger {
		dropTableTriggersForIndexAdmission(t, conn, "knowledge_catalog_tenants")
		if _, err := conn.ExecContext(
			context.Background(),
			"UPDATE knowledge_catalog_tenants SET active_object_count = 0",
		); err != nil {
			t.Fatalf("remove ACTIVE ledger evidence: %v", err)
		}
	}
	var ledgerRows, registryRows int
	if err := conn.QueryRowContext(context.Background(), `
		SELECT count(*) FROM knowledge_catalog_tenants
		WHERE active_object_count > 0`).Scan(&ledgerRows); err != nil {
		t.Fatalf("count ACTIVE ledger evidence: %v", err)
	}
	if err := conn.QueryRowContext(context.Background(), `
		SELECT count(*) FROM knowledge_objects
		WHERE state = 'active'`).Scan(&registryRows); err != nil {
		t.Fatalf("count ACTIVE registry evidence: %v", err)
	}
	if (ledgerRows > 0) != keepLedger || (registryRows > 0) != keepRegistry {
		t.Fatalf(
			"ACTIVE evidence ledger/registry = %d/%d, want %t/%t",
			ledgerRows,
			registryRows,
			keepLedger,
			keepRegistry,
		)
	}
}

func dropTableTriggersForIndexAdmission(t *testing.T, conn *sql.Conn, table string) {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), `
		SELECT name FROM sqlite_schema
		WHERE type = 'trigger' AND tbl_name = ?`, table)
	if err != nil {
		t.Fatalf("list %s triggers: %v", table, err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan %s trigger: %v", table, err)
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s trigger rows: %v", table, err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s triggers: %v", table, err)
	}
	if len(names) == 0 {
		t.Fatalf("%s trigger list is empty", table)
	}
	for _, name := range names {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if _, err := conn.ExecContext(
			context.Background(),
			"DROP TRIGGER "+quoted,
		); err != nil {
			t.Fatalf("drop %s trigger %q: %v", table, name, err)
		}
	}
}
