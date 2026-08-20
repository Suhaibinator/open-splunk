package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"gorm.io/gorm"
)

type indexAdmissionTestAppender struct{}

func (indexAdmissionTestAppender) AppendIndexMutationInTransaction(
	context.Context,
	*gorm.DB,
	string,
	control.IndexMutationAuditEvent,
) error {
	return nil
}

func TestIndexNameAdmissionValidatorConstructorAndTransactionContract(t *testing.T) {
	if _, err := NewIndexNameAdmissionValidator(nil); !errors.Is(
		err,
		control.ErrInvalidArgument,
	) {
		t.Fatalf("NewIndexNameAdmissionValidator(nil) error = %v", err)
	}

	database, _ := newCatalogTestStore(t)
	validator := newIndexAdmissionTestValidator(t, database)
	tx := beginIndexAdmissionTestTransaction(t, database)
	request := indexAdmissionTestRequest(t, tx, "future-index")
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		tx,
		request,
	); err != nil {
		t.Fatalf("validate empty ACTIVE inventory: %v", err)
	}

	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		database.GORMDB(),
		request,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nontransactional validation error = %v, want invalid argument", err)
	}

	other, _ := newCatalogTestStore(t)
	otherTx := beginIndexAdmissionTestTransaction(t, other)
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		otherTx,
		request,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("foreign-database transaction error = %v, want invalid argument", err)
	}

	stale := request
	stale.IndexCatalogRevision++
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		tx,
		stale,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale request error = %v, want version conflict", err)
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		canceled,
		tx,
		request,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled validation error = %v, want canceled", err)
	}
}

func TestIndexNameAdmissionValidatorHydratesMultipleObjectsInCanonicalOrder(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "main")
	for _, fixture := range []struct {
		id   string
		name string
	}{
		{id: "ko-zulu-admission", name: "zulu_admission"},
		{id: "ko-alpha-admission", name: "alpha_admission"},
	} {
		insertFixtureObject(t, database, fixtureObject{
			id:    fixture.id,
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: indexAdmissionTestDefinition(
					aliasDefinition(
						testApp,
						fixture.name,
						SharingScopePrivate,
						nil,
						"",
					),
					"main",
				),
				state: StateActive, mutation: "create", timestamp: 10,
			}},
		})
	}

	validator := newIndexAdmissionTestValidator(t, database)
	tx := beginIndexAdmissionTestTransaction(t, database)
	rawTx, ok := tx.Statement.ConnPool.(*sql.Tx)
	if !ok {
		t.Fatal("test transaction has no *sql.Tx")
	}
	var wrongTransaction atomic.Bool
	callbackName := "index_admission_exact_transaction"
	if err := database.GORMDB().Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(query *gorm.DB) {
			observed, transactional := query.Statement.ConnPool.(*sql.Tx)
			if !transactional || observed != rawTx {
				wrongTransaction.Store(true)
			}
		},
	); err != nil {
		t.Fatalf("register exact-transaction callback: %v", err)
	}
	t.Cleanup(func() {
		if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
			t.Errorf("remove exact-transaction callback: %v", err)
		}
	})

	request := indexAdmissionTestRequest(t, tx, "audit")
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		tx,
		request,
	); err != nil {
		t.Fatalf("validate two-object tenant: %v", err)
	}
	if wrongTransaction.Load() {
		t.Fatal("validator query escaped the caller's exact *sql.Tx")
	}
}

func TestIndexNameAdmissionValidatorRejectsConflictFromDeploymentForeignTenant(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "stage-prod")
	fixtures := []struct {
		name    string
		pattern string
	}{
		{name: "existing_output", pattern: "*prod"},
		{name: "new_output", pattern: "main*"},
	}
	for index, fixture := range fixtures {
		insertFixtureObject(t, database, fixtureObject{
			id:    fmt.Sprintf("ko-new-index-conflict-%d", index),
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: indexAdmissionTestDefinition(
					dependencyExtractionDefinition(
						testApp,
						fixture.name,
						SharingScopeApp,
						nil,
						"",
						"shared_conflicting_output",
					),
					fixture.pattern,
				),
				state: StateActive, mutation: "create", timestamp: int64(20 + index),
			}},
		})
	}

	validator := newIndexAdmissionTestValidator(t, database)
	administration, err := control.NewAuditedIndexAdministration(
		database,
		control.AuditedIndexAdministrationOptions{
			TenantID:  "deployment-tenant-not-knowledge-tenant",
			Appender:  indexAdmissionTestAppender{},
			Validator: validator,
		},
	)
	if err != nil {
		t.Fatalf("construct audited index administration: %v", err)
	}
	if _, err := administration.CreateIndex(t.Context(), control.IndexDefinition{
		Name:             "main-dev",
		DisplayName:      "main-dev",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("foreign-tenant conflict error = %v, want dependency conflict", err)
	}
	if _, err := database.GetIndexByName(t.Context(), "main-dev"); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf("rejected candidate lookup error = %v, want not found", err)
	}
}

func TestIndexNameAdmissionValidatorBatchesDistinctKnowledgeTenants(t *testing.T) {
	const (
		safeTenant     = "tenant-b-index-admission-safe"
		conflictTenant = "tenant-z-index-admission-conflict"
		safeApp        = "app_000000000200000000001A"
		conflictApp    = "app_000000000200000000002A"
	)

	t.Run("two safe tenants", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		createPublicationTransitionTestIndex(t, database, "stage-prod")
		createIndexAdmissionTenantApp(t, database, safeTenant, safeApp, "safe-app")
		createIndexAdmissionTenantApp(t, database, conflictTenant, conflictApp, "second-safe-app")
		insertIndexAdmissionTenantObject(
			t, database, safeTenant, "ko-safe-tenant-b", "owner-safe", 10,
			indexAdmissionTestDefinition(
				dependencyExtractionDefinition(
					safeApp, "safe_tenant_b", SharingScopeApp, nil, "", "safe_output_b",
				),
				"stage-prod",
			),
		)
		insertIndexAdmissionTenantObject(
			t, database, conflictTenant, "ko-safe-tenant-z", "owner-safe", 20,
			indexAdmissionTestDefinition(
				dependencyExtractionDefinition(
					conflictApp, "safe_tenant_z", SharingScopeApp, nil, "", "safe_output_z",
				),
				"stage-prod",
			),
		)

		validator := newIndexAdmissionTestValidator(t, database)
		tx := beginIndexAdmissionTestTransaction(t, database)
		request := indexAdmissionTestRequest(t, tx, "main-dev")
		if err := validator.ValidateIndexNameAdmissionInTransaction(
			t.Context(), tx, request,
		); err != nil {
			t.Fatalf("validate two distinct safe knowledge tenants: %v", err)
		}
	})

	t.Run("conflict only in second tenant", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		createPublicationTransitionTestIndex(t, database, "stage-prod")
		createIndexAdmissionTenantApp(t, database, safeTenant, safeApp, "safe-app")
		createIndexAdmissionTenantApp(t, database, conflictTenant, conflictApp, "conflict-app")
		insertIndexAdmissionTenantObject(
			t, database, safeTenant, "ko-safe-first-tenant", "owner-safe", 10,
			indexAdmissionTestDefinition(
				dependencyExtractionDefinition(
					safeApp, "safe_first_tenant", SharingScopeApp, nil, "", "safe_output",
				),
				"stage-prod",
			),
		)
		for index, fixture := range []struct {
			name    string
			pattern string
		}{
			{name: "existing_output", pattern: "*prod"},
			{name: "new_output", pattern: "main*"},
		} {
			insertIndexAdmissionTenantObject(
				t,
				database,
				conflictTenant,
				fmt.Sprintf("ko-conflict-second-tenant-%d", index),
				"owner-conflict",
				int64(20+index),
				indexAdmissionTestDefinition(
					dependencyExtractionDefinition(
						conflictApp,
						fixture.name,
						SharingScopeApp,
						nil,
						"",
						"shared_second_tenant_output",
					),
					fixture.pattern,
				),
			)
		}

		validator := newIndexAdmissionTestValidator(t, database)
		administration, err := control.NewAuditedIndexAdministration(
			database,
			control.AuditedIndexAdministrationOptions{
				TenantID:  "deployment-tenant",
				Appender:  indexAdmissionTestAppender{},
				Validator: validator,
			},
		)
		if err != nil {
			t.Fatalf("construct global index administration: %v", err)
		}
		if _, err := administration.CreateIndex(t.Context(), control.IndexDefinition{
			Name:             "main-dev",
			DisplayName:      "main-dev",
			IngestionEnabled: true,
			SearchEnabled:    true,
		}); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("second-tenant conflict error = %v, want dependency conflict", err)
		}
	})
}

func TestIndexNameAdmissionValidatorDetectsFinalFactDrift(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	validator := newIndexAdmissionTestValidator(t, database)
	tx := beginIndexAdmissionTestTransaction(t, database)
	request := indexAdmissionTestRequest(t, tx, "fact-drift")

	var advanced atomic.Bool
	callbackName := "index_admission_fact_drift"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(
		callbackName,
		func(query *gorm.DB) {
			if query.Statement.Table != "index_catalog_state" ||
				!advanced.CompareAndSwap(false, true) {
				return
			}
			if err := query.Exec(`UPDATE index_catalog_state
				SET revision = revision + 1 WHERE singleton_id = 1`).Error; err != nil {
				_ = query.AddError(err)
			}
		},
	); err != nil {
		t.Fatalf("register fact-drift callback: %v", err)
	}
	t.Cleanup(func() {
		if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
			t.Errorf("remove fact-drift callback: %v", err)
		}
	})

	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		tx,
		request,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("fact-drift error = %v, want version conflict", err)
	}
	if !advanced.Load() {
		t.Fatal("fact-drift callback did not run")
	}
}

func TestIndexNameAdmissionValidatorGlobalScalarPreflightPrecedesHydration(t *testing.T) {
	const (
		firstTenant  = "tenant-b-scalar-preflight"
		secondTenant = "tenant-z-scalar-preflight"
		firstApp     = "app_000000000200000000003A"
		secondApp    = "app_000000000200000000004A"
	)
	database, _ := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "main")
	createIndexAdmissionTenantApp(t, database, firstTenant, firstApp, "scalar-first")
	createIndexAdmissionTenantApp(t, database, secondTenant, secondApp, "scalar-second")
	for index := range 17 {
		tenantID, appID, tenantOffset := firstTenant, firstApp, 0
		if index >= 9 {
			tenantID, appID, tenantOffset = secondTenant, secondApp, 9
		}
		local := index - tenantOffset
		insertIndexAdmissionTenantObject(
			t,
			database,
			tenantID,
			fmt.Sprintf("ko-scalar-preflight-%02d", index),
			"owner-scalar",
			int64(100+index),
			indexAdmissionTestDefinition(
				aliasDefinition(
					appID,
					fmt.Sprintf("scalar_preflight_%02d", local),
					SharingScopeApp,
					nil,
					"",
				),
				"main",
			),
		)
	}

	var scalarQueries atomic.Int64
	var hydrationQueries atomic.Int64
	callbackName := "index_admission_scalar_preflight"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(
		callbackName,
		func(query *gorm.DB) {
			sqlText := query.Statement.SQL.String()
			if strings.Contains(sqlText, "publication_active_projection") {
				hydrationQueries.Add(1)
			}
			if !strings.Contains(sqlText, "definition_proto_bytes") ||
				!strings.Contains(sqlText, "seal_projection_bytes") {
				return
			}
			scalarQueries.Add(1)
			destination := reflect.ValueOf(query.Statement.Dest)
			if destination.Kind() != reflect.Pointer || destination.IsNil() ||
				destination.Elem().Kind() != reflect.Slice {
				_ = query.AddError(errors.New("unexpected scalar preflight destination"))
				return
			}
			rows := destination.Elem()
			for index := 0; index < rows.Len(); index++ {
				row := rows.Index(index)
				row.FieldByName("DefinitionBytes").SetInt(maximumDefinitionBytes)
				row.FieldByName("DefinitionProtoBytes").SetInt(maximumDefinitionBytes)
			}
		},
	); err != nil {
		t.Fatalf("register scalar-preflight callback: %v", err)
	}
	t.Cleanup(func() {
		if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
			t.Errorf("remove scalar-preflight callback: %v", err)
		}
	})

	validator := newIndexAdmissionTestValidator(t, database)
	tx := beginIndexAdmissionTestTransaction(t, database)
	request := indexAdmissionTestRequest(t, tx, "audit")
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(), tx, request,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("global scalar preflight error = %v, want capacity exceeded", err)
	}
	if scalarQueries.Load() != 2 || hydrationQueries.Load() != 0 {
		t.Fatalf(
			"global scalar preflight observed %d scalar/%d hydration queries, want 2/0",
			scalarQueries.Load(), hydrationQueries.Load(),
		)
	}
}

func TestIndexNameAdmissionValidatorCorruptAuthorityTaxonomy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *control.DB)
	}{
		{
			name: "missing catalog token",
			mutate: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_catalog_revision_head_delete_is_forbidden")
				mustExec(t, database, `DELETE FROM knowledge_catalog_revision_heads
					WHERE tenant_id = ?`, testTenant)
			},
		},
		{
			name: "missing app revision",
			mutate: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "app_catalog_revision_delete_is_forbidden")
				mustExec(t, database, `DELETE FROM app_catalog_revisions
					WHERE tenant_id = ?`, testTenant)
			},
		},
		{
			name: "missing projection seal",
			mutate: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_projection_current_seal_delete_is_forbidden")
				mustExec(t, database, `DELETE FROM knowledge_object_list_projection_seals
					WHERE tenant_id = ? AND knowledge_object_id = ?`,
					testTenant, "ko-corrupt-admission-authority")
			},
		},
		{
			name: "missing dependency seal",
			mutate: func(t *testing.T, database *control.DB) {
				database.SQLDB().SetMaxOpenConns(1)
				connection, err := database.SQLDB().Conn(t.Context())
				if err != nil {
					t.Fatalf("reserve corrupt dependency connection: %v", err)
				}
				if _, err := connection.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF"); err != nil {
					_ = connection.Close()
					t.Fatalf("disable fixture foreign keys: %v", err)
				}
				if _, err := connection.ExecContext(
					t.Context(),
					"DROP TRIGGER knowledge_dependency_seal_delete_is_forbidden",
				); err != nil {
					_ = connection.Close()
					t.Fatalf("drop dependency-seal guard: %v", err)
				}
				if _, err := connection.ExecContext(t.Context(), `
					DELETE FROM knowledge_object_dependency_seals
					WHERE tenant_id = ? AND knowledge_object_id = ?`,
					testTenant, "ko-corrupt-admission-authority",
				); err != nil {
					_ = connection.Close()
					t.Fatalf("delete dependency seal: %v", err)
				}
				if err := connection.Close(); err != nil {
					t.Fatalf("release corrupt dependency connection: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			createPublicationTransitionTestIndex(t, database, "main")
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-corrupt-admission-authority", owner: testOwner,
				versions: []fixtureVersion{{
					definition: indexAdmissionTestDefinition(
						aliasDefinition(
							testApp,
							"corrupt_admission_authority",
							SharingScopePrivate,
							nil,
							"",
						),
						"main",
					),
					state: StateActive, mutation: "create", timestamp: 10,
				}},
			})
			test.mutate(t, database)

			validator := newIndexAdmissionTestValidator(t, database)
			tx := beginIndexAdmissionTestTransaction(t, database)
			request := indexAdmissionTestRequest(t, tx, "audit")
			if err := validator.ValidateIndexNameAdmissionInTransaction(
				t.Context(), tx, request,
			); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt authority error = %v, want corrupt", err)
			}
		})
	}
}

func TestIndexNameAdmissionValidatorMidWorkCancellationAndAppFactDrift(t *testing.T) {
	newFixture := func(t *testing.T) (*control.DB, *IndexNameAdmissionValidator, *gorm.DB, control.IndexNameAdmissionRequest) {
		database, _ := newCatalogTestStore(t)
		createPublicationTransitionTestIndex(t, database, "main")
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-admission-callback", owner: testOwner,
			versions: []fixtureVersion{{
				definition: indexAdmissionTestDefinition(
					aliasDefinition(
						testApp, "admission_callback", SharingScopePrivate, nil, "",
					),
					"main",
				),
				state: StateActive, mutation: "create", timestamp: 10,
			}},
		})
		validator := newIndexAdmissionTestValidator(t, database)
		tx := beginIndexAdmissionTestTransaction(t, database)
		return database, validator, tx, indexAdmissionTestRequest(t, tx, "audit")
	}

	t.Run("mid-work cancellation", func(t *testing.T) {
		database, validator, tx, request := newFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		var canceled atomic.Bool
		callbackName := "index_admission_mid_work_cancel"
		if err := database.GORMDB().Callback().Query().After("gorm:query").Register(
			callbackName,
			func(query *gorm.DB) {
				if strings.Contains(query.Statement.SQL.String(), "seal_projection_bytes") &&
					canceled.CompareAndSwap(false, true) {
					cancel()
				}
			},
		); err != nil {
			t.Fatalf("register cancellation callback: %v", err)
		}
		t.Cleanup(func() {
			if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
				t.Errorf("remove cancellation callback: %v", err)
			}
		})
		if err := validator.ValidateIndexNameAdmissionInTransaction(
			ctx, tx, request,
		); !errors.Is(err, context.Canceled) {
			t.Fatalf("mid-work cancellation error = %v, want canceled", err)
		}
		if !canceled.Load() {
			t.Fatal("mid-work cancellation callback did not run")
		}
	})

	t.Run("app fact drift", func(t *testing.T) {
		database, validator, tx, request := newFixture(t)
		var advanced atomic.Bool
		callbackName := "index_admission_app_fact_drift"
		if err := database.GORMDB().Callback().Query().After("gorm:query").Register(
			callbackName,
			func(query *gorm.DB) {
				if !strings.Contains(query.Statement.SQL.String(), "app_catalog_revisions") ||
					!strings.Contains(query.Statement.SQL.String(), "authority.revision AS revision") ||
					!advanced.CompareAndSwap(false, true) {
					return
				}
				if err := query.Exec(`UPDATE app_catalog_revisions
					SET revision = revision + 1 WHERE tenant_id = ?`, testTenant).Error; err != nil {
					_ = query.AddError(err)
				}
			},
		); err != nil {
			t.Fatalf("register app-drift callback: %v", err)
		}
		t.Cleanup(func() {
			if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
				t.Errorf("remove app-drift callback: %v", err)
			}
		})
		if err := validator.ValidateIndexNameAdmissionInTransaction(
			t.Context(), tx, request,
		); !errors.Is(err, control.ErrVersionConflict) {
			t.Fatalf("app fact-drift error = %v, want version conflict", err)
		}
		if !advanced.Load() {
			t.Fatal("app fact-drift callback did not run")
		}
	})
}

func TestIndexNameAdmissionDriversAreBoundedClippedAndIndexed(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	details := explainSQLiteQueryPlan(
		t,
		database.SQLDB(),
		publicationIndexAdmissionTenantDriverSQL,
		nil,
	)
	assertIndexAdmissionDriverPlan(
		t,
		details,
		"knowledge_catalog_tenants_nonempty_active_idx",
	)
	details = explainSQLiteQueryPlan(
		t,
		database.SQLDB(),
		publicationIndexAdmissionRegistryDriverSQL,
		nil,
	)
	assertIndexAdmissionDriverPlan(
		t,
		details,
		"knowledge_objects_active_tenant_idx",
	)

	tx := beginIndexAdmissionTestTransaction(t, database)
	if err := tx.Exec("PRAGMA ignore_check_constraints = ON").Error; err != nil {
		t.Fatalf("enable corrupt-width fixture: %v", err)
	}
	oversized := strings.Repeat("t", maximumTenantIDBytes+1<<20)
	if err := tx.Exec(
		"INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES (?)",
		oversized,
	).Error; err != nil {
		t.Fatalf("insert oversized tenant fixture: %v", err)
	}
	if err := tx.Exec(`UPDATE knowledge_catalog_tenants
		SET catalog_revision = 1, active_object_count = 1
		WHERE tenant_id = ?`, oversized).Error; err != nil {
		t.Fatalf("activate oversized tenant fixture: %v", err)
	}
	if _, _, err := readPublicationIndexAdmissionDrivers(tx); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized tenant driver error = %v, want corrupt", err)
	}

	objectDatabase, _ := newCatalogTestStore(t)
	objectTx := beginIndexAdmissionTestTransaction(t, objectDatabase)
	if err := objectTx.Exec(`CREATE TEMP TABLE knowledge_objects (
		tenant_id TEXT NOT NULL,
		knowledge_object_id TEXT NOT NULL,
		current_version INTEGER NOT NULL,
		state TEXT NOT NULL
	)`).Error; err != nil {
		t.Fatalf("create shadow registry driver table: %v", err)
	}
	if err := objectTx.Exec(`CREATE INDEX knowledge_objects_active_tenant_idx
		ON knowledge_objects (tenant_id, knowledge_object_id, current_version)
		WHERE state = 'active'`).Error; err != nil {
		t.Fatalf("create shadow registry driver index: %v", err)
	}
	oversizedObject := strings.Repeat("o", maximumObjectIDBytes+1<<20)
	if err := objectTx.Exec(`INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, state
	) VALUES ('tenant-shadow', ?, 1, 'active')`, oversizedObject).Error; err != nil {
		t.Fatalf("insert oversized registry fixture: %v", err)
	}
	if _, err := readPublicationIndexAdmissionRegistryDrivers(objectTx); !errors.Is(
		err,
		ErrCorrupt,
	) {
		t.Fatalf("oversized registry driver error = %v, want corrupt", err)
	}
}

func TestIndexNameAdmissionTenantDriverExactAndPlusOneBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		lastValue int
		want      error
	}{
		{name: "exact", lastValue: 4095},
		{name: "plus one", lastValue: 4096, want: control.ErrCapacityExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			tx := beginIndexAdmissionTestTransaction(t, database)
			if err := tx.Exec(`WITH RECURSIVE sequence(value) AS (
				SELECT 0
				UNION ALL
				SELECT value + 1 FROM sequence WHERE value < ?
			)
			INSERT INTO knowledge_catalog_tenants (tenant_id)
			SELECT printf('capacity-tenant-%04d', value) FROM sequence`,
				test.lastValue,
			).Error; err != nil {
				t.Fatalf("insert tenant-driver capacity fixtures: %v", err)
			}
			if err := tx.Exec(`UPDATE knowledge_catalog_tenants
				SET catalog_revision = 1, active_object_count = 1
				WHERE tenant_id LIKE 'capacity-tenant-%'`).Error; err != nil {
				t.Fatalf("activate tenant-driver capacity fixtures: %v", err)
			}
			drivers, total, err := readPublicationIndexAdmissionTenantDrivers(tx)
			if test.want != nil {
				if !errors.Is(err, test.want) {
					t.Fatalf("tenant-driver boundary error = %v, want %v", err, test.want)
				}
				return
			}
			if err != nil || len(drivers) != MaximumResolutionCandidates ||
				total != MaximumResolutionCandidates {
				t.Fatalf(
					"exact tenant-driver boundary = %d drivers/%d total, %v",
					len(drivers), total, err,
				)
			}
		})
	}
}

func TestIndexNameAdmissionZeroActiveAppAuthorityIsCorrupt(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "main")
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-no-active-app-authority", owner: testOwner,
		versions: []fixtureVersion{{
			definition: indexAdmissionTestDefinition(
				aliasDefinition(
					testApp,
					"no_active_app_authority",
					SharingScopePrivate,
					nil,
					"",
				),
				"main",
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	dropTrigger(t, database, "knowledge_active_app_workspace_cannot_be_archived")
	mustExec(t, database, `UPDATE app_workspaces
		SET state = 'archived', updated_at_unix_micro = updated_at_unix_micro + 100,
			archived_at_unix_micro = updated_at_unix_micro + 100
		WHERE tenant_id = ?`, testTenant)

	validator := newIndexAdmissionTestValidator(t, database)
	tx := beginIndexAdmissionTestTransaction(t, database)
	request := indexAdmissionTestRequest(t, tx, "audit")
	if err := validator.ValidateIndexNameAdmissionInTransaction(
		t.Context(),
		tx,
		request,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("zero-active-app validation error = %v, want corrupt", err)
	}
	if _, _, _, err := readPublicationApps(
		tx,
		testTenant,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("transition app reader error = %v, want corrupt", err)
	}
}

func newIndexAdmissionTestValidator(
	t *testing.T,
	database *control.DB,
) *IndexNameAdmissionValidator {
	t.Helper()
	validator, err := NewIndexNameAdmissionValidator(database)
	if err != nil {
		t.Fatalf("NewIndexNameAdmissionValidator(): %v", err)
	}
	return validator
}

func beginIndexAdmissionTestTransaction(
	t *testing.T,
	database *control.DB,
) *gorm.DB {
	t.Helper()
	tx := database.GORMDB().WithContext(t.Context()).Begin()
	if tx.Error != nil {
		t.Fatalf("begin index-admission transaction: %v", tx.Error)
	}
	t.Cleanup(func() {
		if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back index-admission transaction: %v", err)
		}
	})
	return tx
}

func indexAdmissionTestRequest(
	t *testing.T,
	tx *gorm.DB,
	canonicalName string,
) control.IndexNameAdmissionRequest {
	t.Helper()
	facts, err := readPublicationIndexAdmissionIndexFacts(tx)
	if err != nil {
		t.Fatalf("read index-admission request facts: %v", err)
	}
	return control.IndexNameAdmissionRequest{
		CanonicalName:             canonicalName,
		IndexCatalogRevision:      uint64(facts.revision),
		IndexCatalogPhysicalCount: facts.physicalCount,
	}
}

func indexAdmissionTestDefinition(
	definition *opensplunk.KnowledgeObjectDefinition,
	indexPattern string,
) *opensplunk.KnowledgeObjectDefinition {
	definition.Selector = &opensplunk.KnowledgeSelector{
		IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: indexPattern}},
	}
	return definition
}

func createIndexAdmissionTenantApp(
	t *testing.T,
	database *control.DB,
	tenantID, appID, slug string,
) {
	t.Helper()
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: testCursorKey,
		Clock:     func() time.Time { return time.UnixMicro(10_000).UTC() },
		IDGenerator: func() (string, error) {
			return appID, nil
		},
	})
	if err != nil {
		t.Fatalf("construct app catalog for %s: %v", tenantID, err)
	}
	if _, err := catalog.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{
			Slug:        slug,
			DisplayName: slug,
			DefaultTimeRange: &control.AppTimeRange{
				Earliest: new("-24h"),
				Latest:   new("now"),
			},
		},
	); err != nil {
		t.Fatalf("create app for %s: %v", tenantID, err)
	}
}

func insertIndexAdmissionTenantObject(
	t *testing.T,
	database *control.DB,
	tenantID, objectID, ownerID string,
	timestamp int64,
	definition *opensplunk.KnowledgeObjectDefinition,
) {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("normalize %s/%s: %v", tenantID, objectID, err)
	}
	appID, name, scope, objectType := fixtureIdentity(t, objectID, "v1", normalized)
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin %s/%s fixture: %v", tenantID, objectID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	) VALUES (?, ?, ?, ?, ?)`,
		tenantID, normalized.Digest[:], normalized.Bytes, len(normalized.Bytes), timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s definition: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, dependency_count, mutation_kind,
		quarantine_reason, created_at_unix_micro
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, 0, 'create', NULL, ?)`,
		tenantID, objectID, appID, ownerID, objectType, name, scope,
		normalized.Digest[:], timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s version: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	) VALUES (?, ?, 1, 0)`, tenantID, objectID); err != nil {
		t.Fatalf("seal %s/%s dependencies: %v", tenantID, objectID, err)
	}

	description, descriptionPresent := "", 0
	if normalized.Description != nil {
		description = *normalized.Description
		descriptionPresent = 1
	}
	dimensions := []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	}
	counts := [4]int{}
	selectorValueBytes := 0
	for index, dimension := range dimensions {
		patterns := normalized.Selector.Patterns(dimension)
		counts[index] = len(patterns)
		for _, pattern := range patterns {
			selectorValueBytes += len(pattern)
		}
	}
	canonicalSelectorBytes := len(normalized.Selector.CanonicalBytes())
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id, object_type, name,
		sharing_scope, state, description_present, description, index_selector_count,
		host_selector_count, source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)`,
		tenantID, objectID, appID, ownerID, objectType, name, scope,
		descriptionPresent, description, counts[0], counts[1], counts[2], counts[3],
		selectorValueBytes, canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("insert %s/%s projection: %v", tenantID, objectID, err)
	}
	insertIndexAdmissionTenantSelectorRows(t, tx, tenantID, objectID, normalized)
	projectionBytes := len(description) + selectorValueBytes
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version, projection_bytes, canonical_selector_bytes
	) VALUES (?, ?, 1, ?, ?)`,
		tenantID, objectID, projectionBytes, canonicalSelectorBytes,
	); err != nil {
		t.Fatalf("seal %s/%s projection: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL, NULL, NULL, NULL)`,
		tenantID, objectID, appID, ownerID, objectType, name, scope,
		normalized.Digest[:], timestamp, timestamp,
	); err != nil {
		t.Fatalf("insert %s/%s registry: %v", tenantID, objectID, err)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, tenantID); err != nil {
		t.Fatalf("advance %s catalog revision: %v", tenantID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s/%s fixture: %v", tenantID, objectID, err)
	}
}

func insertIndexAdmissionTenantSelectorRows(
	t *testing.T,
	tx *sql.Tx,
	tenantID, objectID string,
	normalized knowledgedefinition.Normalized,
) {
	t.Helper()
	dimensions := []struct {
		name      string
		dimension knowledge.Dimension
	}{
		{name: "index", dimension: knowledge.DimensionIndex},
		{name: "host", dimension: knowledge.DimensionHost},
		{name: "source", dimension: knowledge.DimensionSource},
		{name: "sourcetype", dimension: knowledge.DimensionSourcetype},
	}
	for _, dimension := range dimensions {
		for ordinal, value := range normalized.Selector.Patterns(dimension.dimension) {
			pattern, err := knowledge.NormalizePattern(value)
			if err != nil {
				t.Fatalf("normalize %s/%s selector: %v", tenantID, objectID, err)
			}
			matchKind := "wildcard"
			if pattern.IsLiteral() {
				matchKind = "exact"
			}
			if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_object_list_selector_patterns (
				tenant_id, knowledge_object_id, object_version, dimension, ordinal, match_kind, value
			) VALUES (?, ?, 1, ?, ?, ?, ?)`,
				tenantID, objectID, dimension.name, ordinal, matchKind, value,
			); err != nil {
				t.Fatalf("insert %s/%s selector: %v", tenantID, objectID, err)
			}
		}
	}
}

func assertIndexAdmissionDriverPlan(
	t *testing.T,
	details []string,
	indexName string,
) {
	t.Helper()
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, indexName) ||
		!strings.Contains(strings.ToUpper(joined), "COVERING INDEX") ||
		strings.Contains(strings.ToUpper(joined), "TEMP B-TREE") {
		t.Fatalf("driver query plan = %v, want covering %s without temp sort", details, indexName)
	}
}
