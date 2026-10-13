package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"gorm.io/gorm"
)

func TestReadPublicationActiveTransitionInventoryBindsExactCatalogs(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-inventory-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"inventory_target",
				SharingScopePrivate,
				nil,
				"inventory-*",
				dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-inventory-source",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp,
				"inventory_source",
				SharingScopePrivate,
				nil,
				"inventory-*",
				dependencyFixtureInputField,
				"inventory_alias",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{
				targetObjectID: "ko-inventory-target",
				targetVersion:  1,
			}},
		}},
	})

	active := createPublicationTransitionTestIndex(t, database, "inventory-active")
	searchDisabled := createPublicationTransitionTestIndex(t, database, "inventory-search-disabled")
	disabledDefinition := searchDisabled.Definition
	disabledDefinition.SearchEnabled = false
	searchDisabled, err := database.UpdateIndex(
		t.Context(),
		searchDisabled.ID,
		searchDisabled.Version,
		disabledDefinition,
	)
	if err != nil {
		t.Fatalf("disable test index search: %v", err)
	}
	archived := createPublicationTransitionTestIndex(t, database, "inventory-archived")
	archived, err = database.SetIndexState(
		t.Context(),
		archived.ID,
		archived.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive test index: %v", err)
	}
	tombstoned := createPublicationTransitionTestIndex(t, database, "inventory-tombstoned")
	tombstoned, err = database.SetIndexState(
		t.Context(),
		tombstoned.ID,
		tombstoned.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive tombstone test index: %v", err)
	}
	mustExec(t, database, `INSERT INTO index_deletion_tombstones (
		index_id, name, deleted_version, deleted_at_unix_micro
	) VALUES ($1, $2, $3, $4)`, tombstoned.ID, tombstoned.Definition.Name, tombstoned.Version, 100)

	read := readPublicationTransitionTestInventory(t, database, store)
	inventory := read.inventory
	if !read.catalog.found || read.catalog.revision != 2 || read.catalog.token == "" ||
		read.appCatalogRevision < 2 || read.indexCatalogRevision < 1 ||
		read.indexCatalogPhysicalCount != 4 {
		t.Fatalf("catalog facts = %#v", read)
	}
	if !slices.Equal(inventory.activeAppIDs, []string{testApp, testAppTwo}) ||
		inventory.expectedActiveAppCount != 2 || inventory.expectedCurrentActiveCount != 2 ||
		len(inventory.currentActive) != 2 || inventory.expectedDependencyCount != 1 ||
		inventory.expectedDefinitionBytes == 0 || inventory.expectedProjectionBytes == 0 ||
		inventory.expectedSelectorPatterns == 0 || inventory.expectedSelectorWork == 0 {
		t.Fatalf("ACTIVE inventory = %#v", inventory)
	}
	if !slices.Equal(inventory.potentiallySearchableIndexNames, []string{
		active.Definition.Name,
		archived.Definition.Name,
		searchDisabled.Definition.Name,
	}) {
		t.Fatalf("potentially searchable indexes = %v", inventory.potentiallySearchableIndexNames)
	}
	if inventory.currentActive[1].object.KnowledgeObjectID != "ko-inventory-target" ||
		len(inventory.currentActive[0].existingDependencies) != 1 ||
		inventory.currentActive[0].existingDependencies[0].targetObjectID != "ko-inventory-target" ||
		inventory.currentActive[0].existingDependencies[0].targetVersion != 1 {
		t.Fatalf("winner dependency authorities = %#v", inventory.currentActive)
	}

	before := publicationTransitionEndpoint{
		present: true,
		state:   StateActive,
		winner:  inventory.currentActive[0],
	}
	afterWinner := before.winner
	afterWinner.object.Version++
	after := publicationTransitionEndpoint{
		present: true,
		state:   StateDisabled,
		winner:  afterWinner,
	}
	bound := readPublicationTransitionTestInventoryFor(
		t,
		database,
		store,
		before,
		after,
	)
	authority, err := validatePublicationActiveTransition(t.Context(), bound.inventory)
	if err != nil || authority.IsZero() {
		t.Fatalf("validate reader-built ACTIVE inventory = %#v, %v", authority, err)
	}
}

func TestReadPublicationActiveTransitionInventoryRejectsAuthorityDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *control.DB)
	}{
		{
			name: "active tenant ledger",
			mutate: func(t *testing.T, database *control.DB) {
				mustExec(t, database, `UPDATE knowledge_catalog_tenants
					SET active_object_count = 0 WHERE tenant_id = $1`, testTenant)
			},
		},
		{
			name: "missing app revision",
			mutate: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "app_catalog_revision_delete_is_forbidden")
				mustExec(t, database, `DELETE FROM app_catalog_revisions WHERE tenant_id = $1`, testTenant)
			},
		},
		{
			name: "physical index ledger",
			mutate: func(t *testing.T, database *control.DB) {
				createPublicationTransitionTestIndex(t, database, "inventory-drift")
				dropTrigger(t, database, "index_catalog_state_transition_is_valid")
				mustExec(t, database, `UPDATE index_catalog_state SET physical_count = 0
					WHERE singleton_id = 1`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-inventory-drift", owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyExtractionDefinition(
						testApp,
						"inventory_drift",
						SharingScopePrivate,
						nil,
						"drift-*",
						"drift_output",
					),
					state: StateActive, mutation: "create", timestamp: 10,
				}},
			})
			test.mutate(t, database)

			err := readPublicationTransitionTestInventoryError(t, database, store)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("read drifted inventory error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestPublicationTransitionAppPreflightIsTenantDrivenAndBounded(t *testing.T) {
	database, store := newCatalogTestStore(t)
	query := publicationTransitionAppPreflightQuery(database.GORMDB(), testTenant)
	dryRun := query.Session(&gorm.Session{DryRun: true}).Find(&[]publicationTransitionAppRecord{})
	if dryRun.Error != nil {
		t.Fatalf("compile app preflight query: %v", dryRun.Error)
	}
	sqlText := dryRun.Statement.SQL.String()
	details := explainPostgreSQLQueryPlan(t, database.SQLDB(), sqlText, dryRun.Statement.Vars)
	joined := strings.Join(details, "\n")
	if (!strings.Contains(joined, "table app_workspaces using app_workspaces_tenant_id_app_id_key") && !strings.Contains(joined, "app_workspaces_tenant_display_id_idx")) ||
		strings.Contains(joined, "Seq Scan on app") || strings.Contains(joined, "Sort") ||
		strings.Contains(strings.ToUpper(sqlText), "ORDER BY") {
		t.Fatalf("app preflight is not a bounded tenant-index walk:\n%s\nSQL:\n%s", joined, sqlText)
	}
	if !nativeBoundLimit(sqlText, dryRun.Statement.Vars, maximumReadableApps+1) {
		t.Fatalf("app preflight SQL lacks the %d-row bound: %s / %#v", maximumReadableApps+1, sqlText, dryRun.Statement.Vars)
	}

	for index := range maximumReadableApps - 1 {
		appID := fmt.Sprintf("app_%021dA", index+1)
		slug := fmt.Sprintf("inventory-overflow-%03d", index+1)
		mustExec(t, database, `INSERT INTO app_workspaces (organization_id,
			app_id, tenant_id, version, slug, display_name, description,
			default_time_range_present, default_earliest, default_latest,
			default_timezone, state, created_at_unix_micro, updated_at_unix_micro,
			archived_at_unix_micro
		)
		SELECT 1, $1, tenant_id, version, $2, $3, description,
			default_time_range_present, default_earliest, default_latest,
			default_timezone, state, created_at_unix_micro, updated_at_unix_micro,
			archived_at_unix_micro
		FROM app_workspaces WHERE app_id = $4`, appID, slug, slug, testApp)
	}
	token, visits := newProjectionVisitCounter(t, database)
	var records []publicationTransitionAppRecord
	if err := publicationTransitionAppPreflightQuery(database.GORMDB(), testTenant).
		Where(projectionVisitFunction+"(?, app.version) = 1", token).
		Find(&records).Error; err != nil {
		t.Fatalf("execute app progress preflight: %v", err)
	}
	if len(records) != maximumReadableApps+1 || visits.Load() != int64(maximumReadableApps+1) {
		t.Fatalf(
			"app preflight progress = rows:%d visits:%d, want %d/%d",
			len(records),
			visits.Load(),
			maximumReadableApps+1,
			maximumReadableApps+1,
		)
	}
	tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if tx.Error != nil {
		t.Fatalf("begin over-bound inventory transaction: %v", tx.Error)
	}
	defer func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back over-bound inventory transaction: %v", err)
		}
	}()
	if _, _, _, err := readPublicationApps(tx, testTenant); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("over-bound app inventory error = %v, want ErrCorrupt", err)
	}

	// The complete inventory reader reaches the same bounded rejection without
	// opening a Writer gate or allocating the corrupt 257-row ACTIVE slice.
	if _, err := store.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{},
		1,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("over-bound complete inventory error = %v, want ErrCorrupt", err)
	}
}

func TestReadPublicationActiveTransitionInventoryRejectsBaseHandle(t *testing.T) {
	database, store := newCatalogTestStore(t)
	if _, err := store.readPublicationActiveTransitionInventory(
		database.GORMDB().WithContext(t.Context()),
		testTenant,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{},
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("base-handle inventory error = %v, want ErrInvalidArgument", err)
	}
}

func TestPublicationTransitionActiveHydrationPlanBoundsRegistryDriver(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	dryRun := publicationTransitionActiveProjectionQuery(
		database.GORMDB(),
		testTenant,
	).Order("projection.knowledge_object_id ASC").
		Session(&gorm.Session{DryRun: true}).
		Limit(MaximumResolutionCandidates + 1).
		Find(&[]projectionReadRecord{})
	if dryRun.Error != nil {
		t.Fatalf("compile ACTIVE hydration query: %v", dryRun.Error)
	}
	sqlText := dryRun.Statement.SQL.String()
	details := explainPostgreSQLQueryPlan(t, database.SQLDB(), sqlText, dryRun.Statement.Vars)
	joined := strings.Join(details, "\n")
	for _, required := range []string{
		"publication_active_projection",
		"on publication_active_projection",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("ACTIVE hydration plan lacks %q:\n%s\nSQL:\n%s", required, joined, sqlText)
		}
	}
	if !strings.Contains(joined, "knowledge_objects_active_tenant_idx") && !strings.Contains(joined, "knowledge_objects_resolution_idx") {
		t.Fatalf("ACTIVE hydration lacks a tenant/active registry index: %s", joined)
	}
	for _, alias := range []string{"candidate", "projection"} {
		var exact bool
		for _, detail := range details {
			if strings.Contains(detail, "Index") && (strings.Contains(detail, " on "+alias+" ") || strings.Contains(detail, " on "+alias+"_")) && strings.Contains(detail, "tenant_id =") && strings.Contains(detail, "knowledge_object_id =") && strings.Contains(detail, "object_version =") {
				exact = true
			}
		}
		if !exact {
			t.Fatalf("ACTIVE hydration lacks exact current %s identity index: %s", alias, joined)
		}
	}
	if strings.Count(sqlText, "JOIN LATERAL") != 2 || strings.Count(sqlText, fmt.Sprintf("LIMIT %d", MaximumResolutionCandidates+1)) != 1 {
		t.Fatalf("ACTIVE hydration lacks dependent bounded registry/projection barriers: %s", sqlText)
	}
	if strings.Contains(joined, "Seq Scan on active_registry") ||
		strings.Contains(joined, "Seq Scan on candidate") ||
		!nativeBoundLimit(sqlText, dryRun.Statement.Vars, MaximumResolutionCandidates+1) {
		t.Fatalf("ACTIVE hydration is not registry-driven and bounded:\n%s\nSQL:\n%s", joined, sqlText)
	}
}

func TestPublicationTransitionPhysicalIndexPlanUsesNameIndexBeforeLimit(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	dryRun := publicationTransitionPhysicalIndexQuery(database.GORMDB()).
		Session(&gorm.Session{DryRun: true}).
		Find(&[]publicationTransitionIndexRecord{})
	if dryRun.Error != nil {
		t.Fatalf("compile physical index preflight query: %v", dryRun.Error)
	}
	sqlText := dryRun.Statement.SQL.String()
	details := explainPostgreSQLQueryPlan(t, database.SQLDB(), sqlText, dryRun.Statement.Vars)
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "indexes_name_id_idx") ||
		strings.Contains(joined, "Sort") ||
		!nativeBoundLimit(sqlText, dryRun.Statement.Vars, maximumPublicationIndexAtoms+1) {
		t.Fatalf("physical index preflight is not a bounded name-index walk:\n%s\nSQL:\n%s", joined, sqlText)
	}
}

func readPublicationTransitionTestInventory(
	t *testing.T,
	database *control.DB,
	store *Store,
) publicationActiveTransitionInventoryRead {
	return readPublicationTransitionTestInventoryFor(
		t,
		database,
		store,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{},
	)
}

func readPublicationTransitionTestInventoryFor(
	t *testing.T,
	database *control.DB,
	store *Store,
	before publicationTransitionEndpoint,
	after publicationTransitionEndpoint,
) publicationActiveTransitionInventoryRead {
	t.Helper()
	tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if tx.Error != nil {
		t.Fatalf("begin inventory test transaction: %v", tx.Error)
	}
	defer func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back inventory test transaction: %v", err)
		}
	}()
	read, err := store.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		before,
		after,
		1,
	)
	if err != nil {
		t.Fatalf("read publication transition inventory: %v", err)
	}
	return read
}

func readPublicationTransitionTestInventoryError(
	t *testing.T,
	database *control.DB,
	store *Store,
) error {
	t.Helper()
	tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if tx.Error != nil {
		t.Fatalf("begin rejected inventory transaction: %v", tx.Error)
	}
	defer func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back rejected inventory transaction: %v", err)
		}
	}()
	_, err := store.readPublicationActiveTransitionInventory(
		tx,
		testTenant,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{},
		1,
	)
	return err
}

func createPublicationTransitionTestIndex(
	t *testing.T,
	database *control.DB,
	name string,
) control.Index {
	t.Helper()
	validator, err := NewIndexNameAdmissionValidator(database)
	if err != nil {
		t.Fatalf("construct test index-name validator: %v", err)
	}
	administration, err := control.NewAuditedIndexAdministration(
		database,
		control.AuditedIndexAdministrationOptions{
			TenantID:  testTenant,
			Appender:  publicationTransitionTestIndexAuditAppender{},
			Validator: validator,
			BindCreatedIndex: func(ctx context.Context, tx *gorm.DB, indexID string) error {
				return tx.WithContext(ctx).Exec("INSERT INTO os_index_organizations(index_id,organization_id) VALUES (?,1)", indexID).Error
			},
		},
	)
	if err != nil {
		t.Fatalf("construct test index administration: %v", err)
	}
	index, err := administration.CreateIndex(t.Context(), control.IndexDefinition{
		Name:             name,
		DisplayName:      name,
		IngestionEnabled: true,
		SearchEnabled:    true,
	})
	if err != nil {
		t.Fatalf("create test index %q: %v", name, err)
	}
	return index
}

type publicationTransitionTestIndexAuditAppender struct{}

func (publicationTransitionTestIndexAuditAppender) AppendIndexMutationInTransaction(
	context.Context,
	*gorm.DB,
	string,
	control.IndexMutationAuditEvent,
) error {
	return nil
}
