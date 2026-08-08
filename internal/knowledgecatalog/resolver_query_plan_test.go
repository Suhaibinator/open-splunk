package knowledgecatalog

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestResolverHydrationAuthoritiesCrossOneChunkWithoutPerObjectQueries(t *testing.T) {
	database, store := newCatalogTestStore(t)
	const objectCount = listHydrationChunkSize + 1
	insertIntegrationBatchObjects(t, database, objectCount, false)

	projections, err := readProjectionRecordsBounded(
		baseProjectionQuery(database.GORMDB()).
			Where("projection.tenant_id = ? AND projection.state = ?", testTenant, StateDraft).
			Order("projection.knowledge_object_id ASC"),
		objectCount+1,
		resolutionHydrationBudget.definitionBytes,
	)
	if err != nil {
		t.Fatalf("read resolver hydration projections: %v", err)
	}
	if len(projections) != objectCount {
		t.Fatalf("resolver hydration projections = %d, want %d", len(projections), objectCount)
	}

	var versionQueries, dependencyQueries, selectorQueries, definitionBlobQueries atomic.Int64
	const callbackName = "test:resolver-hydration-chunk-count"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		sqlText := tx.Statement.SQL.String()
		if strings.Contains(sqlText, "FROM knowledge_object_versions AS version") &&
			strings.Contains(sqlText, "object.current_version = version.object_version") {
			versionQueries.Add(1)
		}
		if strings.Contains(sqlText, "knowledge_object_dependencies AS dependency") {
			dependencyQueries.Add(1)
		}
		if strings.Contains(sqlText, "knowledge_object_list_selector_patterns AS selector") {
			selectorQueries.Add(1)
		}
		if strings.Contains(sqlText, "knowledge_definition_blobs") {
			definitionBlobQueries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register resolver hydration query observer: %v", err)
	}
	objects, authorities, hydrateErr := store.objectsFromProjectionsWithAuthorities(
		database.GORMDB(),
		projections,
		resolutionHydrationBudget,
	)
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove resolver hydration query observer: %v", removeErr)
	}
	if hydrateErr != nil {
		t.Fatalf("hydrate resolver authorities: %v", hydrateErr)
	}
	if len(objects) != objectCount || len(authorities.versions) != objectCount ||
		len(authorities.dependencies) != objectCount {
		t.Fatalf(
			"resolver hydration cardinality = objects:%d versions:%d dependencies:%d, want %d each",
			len(objects),
			len(authorities.versions),
			len(authorities.dependencies),
			objectCount,
		)
	}
	// Crossing the 512-identity boundary yields exactly two bulk chunks. Each
	// version, selector, and distinct-blob authority has a width and payload
	// query; dependency-free chunks need only physical and aggregate proofs.
	if got := versionQueries.Load(); got != 4 {
		t.Fatalf("current-version queries = %d, want 4 across two chunks", got)
	}
	if got := dependencyQueries.Load(); got != 4 {
		t.Fatalf("dependency queries = %d, want 4 across two dependency-free chunks", got)
	}
	if got := selectorQueries.Load(); got != 4 {
		t.Fatalf("selector queries = %d, want 4 across two chunks", got)
	}
	if got := definitionBlobQueries.Load(); got != 4 {
		t.Fatalf("definition-blob queries = %d, want 4 across two chunks", got)
	}
}

func TestResolverActiveCountPlanUsesThreeBoundedAuthorizationIndexes(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	scope, err := normalizeScope(testReadScope())
	if err != nil {
		t.Fatal(err)
	}
	query, arguments := captureBoundedActiveResolutionCountQuery(t, database, scope)
	details := explainSQLiteQueryPlan(t, database.SQLDB(), query, arguments)
	joined := strings.Join(details, "\n")

	for _, required := range []string{
		"CO-ROUTINE bounded_active_resolution_objects",
		"knowledge_objects_authorized_global_idx",
		"knowledge_objects_authorized_app_idx",
		"knowledge_objects_authorized_private_idx",
		"SCAN bounded_active_resolution_objects",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("bounded active-resolution count plan lacks %q:\n%s\nSQL:\n%s", required, joined, query)
		}
	}
	if strings.Contains(joined, "SCAN knowledge_objects") ||
		strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf("bounded active-resolution count plan scans or sorts before its shared limit:\n%s", joined)
	}
	for _, indexName := range []string{
		"knowledge_objects_authorized_global_idx",
		"knowledge_objects_authorized_app_idx",
		"knowledge_objects_authorized_private_idx",
	} {
		if strings.Count(joined, indexName) != 1 {
			t.Fatalf("bounded active-resolution count plan uses %s %d times, want exactly once:\n%s", indexName, strings.Count(joined, indexName), joined)
		}
	}
	if strings.Count(query, "LIMIT 4097") != 1 {
		t.Fatalf("bounded active-resolution count SQL has %d 4,097-row limits, want exactly one shared limit:\n%s", strings.Count(query, "LIMIT 4097"), query)
	}
}

func TestResolverProjectionPlanIsAuthorizationDrivenAndSortsOnlyBoundedRows(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	scope, err := normalizeScope(testReadScope())
	if err != nil {
		t.Fatal(err)
	}
	query, arguments := compiledActiveResolutionProjectionQuery(t, database, scope)
	details := explainSQLiteQueryPlan(t, database.SQLDB(), query, arguments)
	joined := strings.Join(details, "\n")

	for _, required := range []string{
		"CO-ROUTINE authorized_active_resolution_projection",
		"knowledge_objects_authorized_global_idx",
		"knowledge_objects_authorized_app_idx",
		"knowledge_objects_authorized_private_idx",
		"SEARCH defining_app USING",
		"SCAN authorized_active_resolution_projection",
		"SEARCH projection USING PRIMARY KEY",
		"USE TEMP B-TREE FOR ORDER BY",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("active-resolution projection plan lacks %q:\n%s\nSQL:\n%s", required, joined, query)
		}
	}
	if strings.Contains(joined, "SCAN authorized_registry") ||
		strings.Contains(joined, "SCAN candidate") ||
		strings.Contains(joined, "SCAN projection") {
		t.Fatalf("active-resolution projection plan contains an unbounded authority/projection scan:\n%s", joined)
	}
	if strings.Count(joined, "SEARCH candidate USING") != 3 {
		t.Fatalf("active-resolution projection plan has %d indexed candidate joins, want 3:\n%s", strings.Count(joined, "SEARCH candidate USING"), joined)
	}
	for _, indexName := range []string{
		"knowledge_objects_authorized_global_idx",
		"knowledge_objects_authorized_app_idx",
		"knowledge_objects_authorized_private_idx",
	} {
		if strings.Count(joined, indexName) != 1 {
			t.Fatalf("active-resolution projection plan uses %s %d times, want exactly once:\n%s", indexName, strings.Count(joined, indexName), joined)
		}
	}
	definingAppIndex := indexContaining(details, "SEARCH defining_app USING")
	if definingAppIndex < 0 ||
		!strings.Contains(details[definingAppIndex], "tenant_id=? AND app_id=?") {
		t.Fatalf("active-resolution defining-app authorization is not an exact tenant/app lookup:\n%s", joined)
	}
	for _, detail := range details {
		if !strings.Contains(detail, "SEARCH candidate USING") {
			continue
		}
		if !strings.Contains(detail, "tenant_id=?") ||
			!strings.Contains(detail, "knowledge_object_id=?") ||
			!strings.Contains(detail, "object_version=?)") {
			t.Fatalf("active-resolution candidate projection is not an exact identity lookup: %s\n%s", detail, joined)
		}
	}
	for _, exactJoin := range []string{
		"candidate.tenant_id = authorized_registry.tenant_id",
		"candidate.knowledge_object_id = authorized_registry.knowledge_object_id",
		"candidate.object_version = authorized_registry.current_version",
		"candidate.app_id = authorized_registry.app_id",
		"candidate.owner_id = authorized_registry.owner_id",
		"candidate.object_type = authorized_registry.object_type",
		"candidate.name = authorized_registry.name",
		"candidate.sharing_scope = authorized_registry.sharing_scope",
		"candidate.state = authorized_registry.state",
	} {
		if strings.Count(query, exactJoin) != 3 {
			t.Fatalf("active-resolution projection SQL contains exact join %q %d times, want once per authorization branch:\n%s", exactJoin, strings.Count(query, exactJoin), query)
		}
	}
	for _, detail := range details {
		if strings.HasPrefix(detail, "SCAN ") &&
			detail != "SCAN authorized_active_resolution_projection" {
			t.Fatalf("active-resolution projection plan contains a scan outside its bounded driver: %s\n%s", detail, joined)
		}
	}
	if strings.Count(joined, "USE TEMP B-TREE FOR ORDER BY") != 1 {
		t.Fatalf("active-resolution projection sorter count is not exactly one:\n%s", joined)
	}
	driverIndex := indexContaining(details, "SCAN authorized_active_resolution_projection")
	sortIndex := indexContaining(details, "USE TEMP B-TREE FOR ORDER BY")
	if driverIndex < 0 || sortIndex <= driverIndex {
		t.Fatalf("active-resolution projection sorter is not downstream of its bounded authorization driver:\n%s", joined)
	}
	if strings.Count(query, "LIMIT 4097") != 2 {
		t.Fatalf("active-resolution projection SQL has %d 4,097-row limits, want one shared authorization limit and one outer read limit:\n%s", strings.Count(query, "LIMIT 4097"), query)
	}
}

func captureBoundedActiveResolutionCountQuery(
	t *testing.T,
	database *control.DB,
	scope normalizedScope,
) (string, []any) {
	t.Helper()
	const callbackName = "test:capture-bounded-active-resolution-count"
	var query string
	var arguments []any
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "bounded_active_resolution_objects") {
			query = tx.Statement.SQL.String()
			arguments = append([]any(nil), tx.Statement.Vars...)
		}
	}); err != nil {
		t.Fatalf("register active-resolution count capture: %v", err)
	}
	_, countErr := readBoundedActiveResolutionCount(database.GORMDB(), scope)
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove active-resolution count capture: %v", removeErr)
	}
	if countErr != nil {
		t.Fatalf("read bounded active-resolution count: %v", countErr)
	}
	if query == "" {
		t.Fatal("bounded active-resolution count query was not captured")
	}
	return query, arguments
}

func compiledActiveResolutionProjectionQuery(
	t *testing.T,
	database *control.DB,
	scope normalizedScope,
) (string, []any) {
	t.Helper()
	query := applyActiveResolutionProjectionAuthorization(baseProjectionQuery(database.GORMDB()), scope).
		Order("projection.knowledge_object_id ASC")
	result := query.Session(&gorm.Session{DryRun: true}).
		Limit(MaximumResolutionCandidates + 1).
		Find(&[]projectionReadRecord{})
	if result.Error != nil {
		t.Fatalf("compile active-resolution projection query: %v", result.Error)
	}
	if result.Statement.SQL.Len() == 0 {
		t.Fatal("compiled active-resolution projection query is empty")
	}
	arguments := append([]any(nil), result.Statement.Vars...)
	return result.Statement.SQL.String(), arguments
}
