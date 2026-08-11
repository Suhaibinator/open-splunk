package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"gorm.io/gorm"
)

func TestIntegrationBatchHydrationQueryCountScalesByChunksNotObjects(t *testing.T) {
	smallDatabase, smallStore := newCatalogTestStore(t)
	insertIntegrationBatchObjects(t, smallDatabase, 1, false)
	largeDatabase, largeStore := newCatalogTestStore(t)
	const largeObjectCount = listHydrationChunkSize + 1
	insertIntegrationBatchObjects(t, largeDatabase, largeObjectCount, true)

	pageRequest := ListRequest{PageSize: MaximumPageSize}
	smallScalar, smallScalarQueries := integrationCountCatalogQueries(t, smallDatabase, func() (ListPage, error) {
		return smallStore.List(context.Background(), testReadScope(), pageRequest)
	})
	largeScalar, largeScalarQueries := integrationCountCatalogQueries(t, largeDatabase, func() (ListPage, error) {
		return largeStore.List(context.Background(), testReadScope(), pageRequest)
	})
	if len(smallScalar.Objects) != 1 || len(largeScalar.Objects) != MaximumPageSize ||
		largeScalar.NextPageToken == "" {
		t.Fatalf("scalar pages = small:%d large:%d token:%t", len(smallScalar.Objects), len(largeScalar.Objects), largeScalar.NextPageToken != "")
	}
	// Scalar hydration uses 16 bounded authority queries for every current
	// lifecycle state, regardless of whether the page contains one object or
	// the 256-object maximum. Keep that exact ceiling intentional so a future
	// authority silently becoming per-object (or regaining a duplicate ordered
	// scan) fails the gate.
	if largeScalarQueries > smallScalarQueries || largeScalarQueries > 16 {
		t.Fatalf("scalar query count grew with page rows: small=%d large=%d", smallScalarQueries, largeScalarQueries)
	}

	needle := "batch-"
	bodyRequest := ListRequest{PageSize: MaximumPageSize, TextFilter: &needle}
	smallBody, smallBodyQueries := integrationCountCatalogQueries(t, smallDatabase, func() (ListPage, error) {
		return smallStore.List(context.Background(), testReadScope(), bodyRequest)
	})
	largeBody, largeBodyQueries := integrationCountCatalogQueries(t, largeDatabase, func() (ListPage, error) {
		return largeStore.List(context.Background(), testReadScope(), bodyRequest)
	})
	if len(smallBody.Objects) != 1 || len(largeBody.Objects) != MaximumPageSize ||
		largeBody.NextPageToken == "" {
		t.Fatalf("body-filter pages = small:%d large:%d token:%t", len(smallBody.Objects), len(largeBody.Objects), largeBody.NextPageToken != "")
	}
	// Crossing the 512-row hydration boundary adds one chunk to each bulk
	// authority read. It must not add a query per object.
	if largeBodyQueries > smallBodyQueries+10 || largeBodyQueries > 26 {
		t.Fatalf("body-filter query count is not chunk-bounded: small=%d large=%d", smallBodyQueries, largeBodyQueries)
	}
	t.Logf(
		"GORM query counts: scalar 1=%d 513=%d; body-filter 1=%d 513=%d",
		smallScalarQueries,
		largeScalarQueries,
		smallBodyQueries,
		largeBodyQueries,
	)

	seen := make(map[string]struct{}, largeObjectCount)
	request := pageRequest
	for {
		page, err := largeStore.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(mixed-version page %d): %v", len(seen)/MaximumPageSize, err)
		}
		if cap(page.Objects) != len(page.Objects) {
			t.Fatalf("page retained sentinel capacity: len/cap=%d/%d", len(page.Objects), cap(page.Objects))
		}
		for _, object := range page.Objects {
			if _, duplicate := seen[object.KnowledgeObjectID]; duplicate {
				t.Fatalf("pagination repeated %q", object.KnowledgeObjectID)
			}
			seen[object.KnowledgeObjectID] = struct{}{}
			var ordinal int
			if _, err := fmt.Sscanf(object.Name, "batch-%04d", &ordinal); err != nil {
				t.Fatalf("parse batch name %q: %v", object.Name, err)
			}
			wantVersion := uint64(1)
			wantDescription := fmt.Sprintf("batch body %04d v1", ordinal)
			if ordinal%2 == 0 {
				wantVersion = 2
				wantDescription = fmt.Sprintf("batch body %04d v2", ordinal)
			}
			if object.Version != wantVersion || object.Definition.GetDescription() != wantDescription {
				t.Fatalf("mixed current version %s", describeIntegrationObject(object))
			}
		}
		if page.NextPageToken == "" {
			break
		}
		request.PageToken = page.NextPageToken
	}
	if len(seen) != largeObjectCount {
		t.Fatalf("paginated objects = %d, want %d", len(seen), largeObjectCount)
	}
}

func TestIntegrationBatchHydrationDisabledBodyFilterQueryCountCrossesOneChunk(t *testing.T) {
	smallDatabase, smallStore := newCatalogTestStore(t)
	insertIntegrationDisabledBatchObjects(t, smallDatabase, 1)
	largeDatabase, largeStore := newCatalogTestStore(t)
	const largeObjectCount = listHydrationChunkSize + 1
	insertIntegrationDisabledBatchObjects(t, largeDatabase, largeObjectCount)

	needle := "disabled-batch-"
	request := ListRequest{PageSize: MaximumPageSize, TextFilter: &needle}
	smallPage, smallQueries := integrationCountCatalogQueries(t, smallDatabase, func() (ListPage, error) {
		return smallStore.List(context.Background(), testReadScope(), request)
	})
	largePage, largeQueries := integrationCountCatalogQueries(t, largeDatabase, func() (ListPage, error) {
		return largeStore.List(context.Background(), testReadScope(), request)
	})
	if len(smallPage.Objects) != 1 || len(largePage.Objects) != MaximumPageSize ||
		largePage.NextPageToken == "" {
		t.Fatalf(
			"disabled body-filter pages = small:%d large:%d token:%t",
			len(smallPage.Objects),
			len(largePage.Objects),
			largePage.NextPageToken != "",
		)
	}
	for _, object := range append(slices.Clone(smallPage.Objects), largePage.Objects...) {
		if object.State != StateDisabled || object.DisabledAt == nil {
			t.Fatalf("disabled body-filter object = %s", describeIntegrationObject(object))
		}
	}
	// Latest-disable validity is folded into the lifecycle width preflight, so
	// disabled objects have the same query shape as other states. A second
	// 512-object chunk must retain the same exact ceiling as the active corpus.
	if largeQueries > smallQueries+10 || largeQueries > 26 {
		t.Fatalf(
			"disabled body-filter query count is not chunk-bounded: small=%d large=%d",
			smallQueries,
			largeQueries,
		)
	}
	t.Logf("disabled body-filter GORM query counts: 1=%d; %d=%d", smallQueries, largeObjectCount, largeQueries)
}

func TestIntegrationBatchHydrationVisibleCandidateCorruptionFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*testing.T, *control.DB)
	}{
		{
			name: "definition blob",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_definition_blobs
					SET definition_proto = X'00', definition_bytes = 1
					WHERE tenant_id = ? AND definition_digest = (
						SELECT definition_digest FROM knowledge_objects
						WHERE tenant_id = ? AND knowledge_object_id = 'ko-corrupt-source'
					)`, testTenant, testTenant)
			},
		},
		{
			name: "selector row",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_selector_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_list_selector_patterns
					SET value = 'xxxxxxx-*'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-corrupt-source'
					  AND dimension = 'host'`, testTenant)
			},
		},
		{
			name: "dependency ordinal",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_dependencies SET ordinal = 1
					WHERE tenant_id = ? AND source_object_id = 'ko-corrupt-source'
					  AND source_object_version = 1`, testTenant)
			},
		},
		{
			name: "dependency target identity",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
				execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_dependencies
					SET target_object_id = 'ko-missing-target'
					WHERE tenant_id = ? AND source_object_id = 'ko-corrupt-source'
					  AND source_object_version = 1`, testTenant)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-dependency-target", owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyExtractionDefinition(
						testApp, "dependency-target", SharingScopePrivate, nil, "target-*",
						dependencyFixtureInputField,
					),
					state: StateActive, mutation: "create", timestamp: 10,
				}},
			})
			goodDescription := "the only matching description"
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-good", owner: testOwner,
				versions: []fixtureVersion{{
					definition: aliasDefinition(testApp, "only-good", SharingScopePrivate, &goodDescription, "good-*"),
					state:      StateActive, mutation: "create", timestamp: 11,
				}},
			})
			corruptDescription := "excluded candidate"
			insertFixtureObject(t, database, fixtureObject{
				id: "ko-corrupt-source", owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyAliasDefinition(
						testApp, "corrupt-source", SharingScopePrivate, &corruptDescription, "corrupt-*",
						dependencyFixtureInputField, "dependency_alias",
					),
					state: StateActive, mutation: "create", timestamp: 12,
					dependencies: []fixtureDependency{{
						targetObjectID: "ko-dependency-target", targetVersion: 1,
					}},
				}},
			})

			test.corrupt(t, database)
			needle := "only matching"
			if _, err := store.List(context.Background(), testReadScope(), ListRequest{
				PageSize: 1, TextFilter: &needle,
			}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("List(body filter with excluded corrupt candidate) error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestIntegrationBatchHydrationHiddenCorruptionIsNotAnOracle(t *testing.T) {
	database, store := newCatalogTestStore(t)
	for index, name := range []string{"visible-alpha", "visible-zulu"} {
		description := "visible body"
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-" + name, owner: testOwner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, name, SharingScopePrivate, &description, name+"-*"),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}},
		})
	}
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-hidden-target", owner: "owner-b",
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "hidden-target", SharingScopePrivate, nil, "target-*", dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 20,
		}},
	})
	hiddenDescription := "secret body"
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-hidden-source", owner: "owner-b",
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp, "hidden-source", SharingScopePrivate, &hiddenDescription, "secret-*",
				dependencyFixtureInputField, "dependency_alias",
			),
			state: StateActive, mutation: "create", timestamp: 21,
			dependencies: []fixtureDependency{{targetObjectID: "ko-hidden-target", targetVersion: 1}},
		}},
	})

	text := "visible"
	selector := "visible-"
	requests := []ListRequest{
		{PageSize: 1, IncludeTotal: true, TextFilter: &text},
		{PageSize: 1, IncludeTotal: true, SelectorTextFilter: &selector},
	}
	baseline := make([]ListPage, len(requests))
	for index, request := range requests {
		page, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(baseline %d): %v", index, err)
		}
		baseline[index] = page
	}

	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	dropTrigger(t, database, "knowledge_list_selector_update_is_forbidden")
	dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
	dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = X'00', definition_bytes = 1
		WHERE tenant_id = ? AND definition_digest = (
			SELECT definition_digest FROM knowledge_objects
			WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'
		)`, testTenant, testTenant)
	mustExec(t, database, `UPDATE knowledge_object_list_selector_patterns SET value = 'xxxxxx-*'
		WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'`, testTenant)
	mustExec(t, database, `UPDATE knowledge_object_dependencies SET ordinal = 1
		WHERE tenant_id = ? AND source_object_id = 'ko-hidden-source'`, testTenant)
	execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_list_projections
		SET owner_id = ? WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'`, testOwner, testTenant)

	for index, request := range requests {
		page, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(after hidden corruption %d): %v", index, err)
		}
		integrationAssertPagesEqual(t, page, baseline[index])
	}
	if _, err := store.Get(context.Background(), testReadScope(), "ko-hidden-source", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(hidden corrupt object) error = %v, want ErrNotFound", err)
	}
}

func TestIntegrationCatalogTenantHealthDoesNotDisclosePrivateRows(t *testing.T) {
	t.Run("identity ledger mismatch is diagnostic only", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		visibleIDs := privacyContractSeedReadablePair(t, database, "ledger")
		hiddenDescription := "ledger-hidden-secret"
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-ledger-hidden", owner: "owner-b",
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, "ledger-hidden", SharingScopePrivate, &hiddenDescription, "ledger-hidden-*"),
				state:      StateActive, mutation: "create", timestamp: 10,
			}},
		})
		baselineGets := privacyContractCaptureGets(t, store, visibleIDs)
		requests := privacyContractListRequests("ledger-visible")
		baselineLists := privacyContractCaptureLists(t, store, requests)
		mustExec(t, database, `UPDATE knowledge_catalog_tenants
			SET identity_count = identity_count - 1 WHERE tenant_id = ?`, testTenant)
		privacyContractAssertPhysicalCountDiagnostic(t, database, 3)
		privacyContractAssertGets(t, store, visibleIDs, baselineGets)
		privacyContractAssertLists(t, store, requests, baselineLists)
		for _, version := range []*uint64{nil, integrationUint64Pointer(1)} {
			if _, err := store.Get(context.Background(), testReadScope(), "ko-ledger-hidden", version); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("Get(hidden ledger row, version=%v) error = %v, want policy-neutral ErrNotFound", version, err)
			}
		}
	})

	t.Run("hidden over-cap physical identities are diagnostic only", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		visibleIDs := privacyContractSeedReadablePair(t, database, "over-cap")
		baselineGets := privacyContractCaptureGets(t, store, visibleIDs)
		requests := privacyContractListRequests("over-cap-visible")
		baselineLists := privacyContractCaptureLists(t, store, requests)
		for _, trigger := range []string{
			"knowledge_object_identity_capacity_is_available",
			"knowledge_object_after_insert_count_identity",
			"knowledge_object_insert_requires_sealed_list_projection",
		} {
			dropTrigger(t, database, trigger)
		}
		connection, err := database.SQLDB().Conn(context.Background())
		if err != nil {
			t.Fatalf("acquire over-cap fixture connection: %v", err)
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
			_ = connection.Close()
			t.Fatalf("disable over-cap fixture foreign keys: %v", err)
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
			_ = connection.Close()
			t.Fatalf("disable over-cap fixture checks: %v", err)
		}
		const physicalRows = maximumObjectsPerTenant + 808
		for start := 0; start < physicalRows; start += 250 {
			end := min(start+250, physicalRows)
			var statement strings.Builder
			statement.WriteString(`INSERT INTO knowledge_objects (
				tenant_id, knowledge_object_id, current_version, app_id, owner_id,
				object_type, name, sharing_scope, state, definition_digest,
				created_at_unix_micro, updated_at_unix_micro,
				disabled_at_unix_micro, quarantined_at_unix_micro,
				deleted_at_unix_micro, quarantine_reason
			) VALUES `)
			arguments := make([]any, 0, (end-start)*4)
			for index := start; index < end; index++ {
				if index != start {
					statement.WriteByte(',')
				}
				statement.WriteString(`(?, ?, 1, ?, 'owner-over-cap', 'field_alias', ?,
					'private', 'draft', zeroblob(32), 1, 1, NULL, NULL, NULL, NULL)`)
				objectID := fmt.Sprintf("ko-over-cap-%05d", index)
				arguments = append(arguments, testTenant, objectID, testApp, objectID)
			}
			if _, err := connection.ExecContext(context.Background(), statement.String(), arguments...); err != nil {
				_ = connection.Close()
				t.Fatalf("insert over-cap fixture rows [%d,%d): %v", start, end, err)
			}
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
			_ = connection.Close()
			t.Fatalf("restore over-cap fixture foreign keys: %v", err)
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
			_ = connection.Close()
			t.Fatalf("restore over-cap fixture check constraints: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("close over-cap fixture connection: %v", err)
		}

		privacyContractAssertPhysicalCountDiagnostic(t, database, maximumObjectsPerTenant+1)
		privacyContractAssertGets(t, store, visibleIDs, baselineGets)
		privacyContractAssertLists(t, store, requests, baselineLists)
		for _, version := range []*uint64{nil, integrationUint64Pointer(1)} {
			if _, err := store.Get(context.Background(), testReadScope(), "ko-over-cap-00000", version); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("Get(hidden over-cap row, version=%v) error = %v, want policy-neutral ErrNotFound", version, err)
			}
		}
	})
}

func TestBatchHydrationAggregateBudgetsAcceptExactAndRejectNextObject(t *testing.T) {
	base := func() projectionRecord {
		return projectionRecord{State: StateActive, DefinitionBytes: 1}
	}
	baseBudget := listHydrationBudget{
		definitionBytes:    maximumDefinitionBytes,
		projectionBytes:    maximumDescriptionBytes + (8 << 10),
		selectorPatterns:   maximumSelectorPatterns,
		selectorValueBytes: 8 << 10,
		dependencies:       maximumDependenciesPerVersion,
	}
	tests := []struct {
		name  string
		exact []projectionRecord
		next  projectionRecord
	}{
		{
			name:  "canonical definition bytes",
			exact: []projectionRecord{{State: StateActive, DefinitionBytes: maximumDefinitionBytes}},
			next:  base(),
		},
		{
			name: "projection bytes",
			exact: func() []projectionRecord {
				record := base()
				record.ProjectionBytes = baseBudget.projectionBytes
				return []projectionRecord{record}
			}(),
			next: func() projectionRecord {
				record := base()
				record.ProjectionBytes = 1
				return record
			}(),
		},
		{
			name: "selector patterns",
			exact: func() []projectionRecord {
				record := base()
				record.IndexSelectorCount = 16
				record.HostSelectorCount = 16
				record.SourceSelectorCount = 16
				record.SourcetypeSelectorCount = 16
				return []projectionRecord{record}
			}(),
			next: func() projectionRecord {
				record := base()
				record.IndexSelectorCount = 1
				return record
			}(),
		},
		{
			name: "selector value bytes",
			exact: func() []projectionRecord {
				record := base()
				record.SelectorValueBytes = baseBudget.selectorValueBytes
				return []projectionRecord{record}
			}(),
			next: func() projectionRecord {
				record := base()
				record.SelectorValueBytes = 1
				return record
			}(),
		},
		{
			name: "dependencies",
			exact: func() []projectionRecord {
				record := base()
				record.DependencyCount = baseBudget.dependencies
				return []projectionRecord{record}
			}(),
			next: func() projectionRecord {
				record := base()
				record.DependencyCount = 1
				return record
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateListHydrationBudget(test.exact, baseBudget); err != nil {
				t.Fatalf("exact boundary rejected: %v", err)
			}
			over := append(slices.Clone(test.exact), test.next)
			if err := validateListHydrationBudget(over, baseBudget); !errors.Is(err, control.ErrCapacityExceeded) {
				t.Fatalf("next object error = %v, want CapacityExceeded", err)
			}
		})
	}
}

func TestIntegrationBatchHydrationCancellationRollsBackAndReleasesConnection(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertIntegrationBatchObjects(t, database, 40, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var queryCount atomic.Int64
	const callbackName = "test:cancel-batch-hydration"
	if err := database.GORMDB().Callback().Query().Before("gorm:query").Register(callbackName, func(*gorm.DB) {
		if queryCount.Add(1) == 6 {
			cancel()
		}
	}); err != nil {
		t.Fatalf("register cancellation callback: %v", err)
	}
	needle := "batch-"
	_, err := store.List(ctx, testReadScope(), ListRequest{PageSize: MaximumPageSize, TextFilter: &needle})
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove cancellation callback: %v", removeErr)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("List(mid-hydration cancellation) error = %v, want context.Canceled", err)
	}
	if queryCount.Load() < 6 {
		t.Fatalf("cancellation callback ran only %d queries", queryCount.Load())
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: MaximumPageSize})
	if err != nil || len(page.Objects) != 40 {
		t.Fatalf("List(after cancellation) = %d objects, %v", len(page.Objects), err)
	}
}

func TestIntegrationConcurrentBatchHydrationReturnsDetachedStableObjects(t *testing.T) {
	database, store := newCatalogTestStore(t)
	const objectCount = 40
	insertIntegrationBatchObjects(t, database, objectCount, true)
	needle := "batch-"
	const workers = 12
	const iterations = 3
	start := make(chan struct{})
	errorsSeen := make(chan error, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			ready.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				request := ListRequest{PageSize: objectCount}
				if (worker+iteration)%2 == 0 {
					request.TextFilter = &needle
				}
				page, err := store.List(context.Background(), testReadScope(), request)
				if err != nil {
					errorsSeen <- err
					return
				}
				if len(page.Objects) != objectCount || page.NextPageToken != "" {
					errorsSeen <- fmt.Errorf("worker %d iteration %d page shape = %d/%t", worker, iteration, len(page.Objects), page.NextPageToken != "")
					return
				}
				page.Objects[0].Definition.Name = fmt.Sprintf("mutated-%d-%d", worker, iteration)
				page.Objects[0].DefinitionSHA256[0] ^= byte(worker + iteration + 1)
			}
			errorsSeen <- nil
		}(worker)
	}
	ready.Wait()
	close(start)
	for worker := 0; worker < workers; worker++ {
		if err := <-errorsSeen; err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: objectCount})
	if err != nil || len(page.Objects) != objectCount || page.Objects[0].Definition.GetName() != "batch-0000" {
		t.Fatalf("List(after concurrent caller mutations) = %#v, %v", page, err)
	}
}

func insertIntegrationBatchObjects(t *testing.T, database *control.DB, count int, mixedVersions bool) {
	t.Helper()
	if count < 1 || count > maximumObjectsPerTenant {
		t.Fatalf("invalid batch fixture count %d", count)
	}
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin batch fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)
	mutations := int64(0)
	for index := 0; index < count; index++ {
		objectID := fmt.Sprintf("ko-batch-%04d", index)
		name := fmt.Sprintf("batch-%04d", index)
		descriptionV1 := fmt.Sprintf("batch body %04d v1", index)
		v1, normalizeErr := knowledgedefinition.Normalize(aliasDefinition(
			testApp,
			name,
			SharingScopePrivate,
			&descriptionV1,
			fmt.Sprintf("batch-%04d-*", index),
		))
		if normalizeErr != nil {
			t.Fatalf("normalize batch v1 %d: %v", index, normalizeErr)
		}
		timestamp := int64(10_000 + index*3)
		insertIntegrationDefinitionBlob(t, tx, v1.Bytes, v1.Digest[:], timestamp)
		insertIntegrationVersion(t, tx, objectID, 1, StateDraft, "create", v1.Digest[:], v1, timestamp)
		current := v1
		currentVersion := int64(1)
		updatedAt := timestamp
		mutations++
		if mixedVersions && index%2 == 0 {
			descriptionV2 := fmt.Sprintf("batch body %04d v2", index)
			v2, normalizeErr := knowledgedefinition.Normalize(aliasDefinition(
				testApp,
				name,
				SharingScopePrivate,
				&descriptionV2,
				fmt.Sprintf("batch-%04d-*", index),
			))
			if normalizeErr != nil {
				t.Fatalf("normalize batch v2 %d: %v", index, normalizeErr)
			}
			updatedAt++
			insertIntegrationDefinitionBlob(t, tx, v2.Bytes, v2.Digest[:], updatedAt)
			insertIntegrationVersion(t, tx, objectID, 2, StateDraft, "update", v2.Digest[:], v2, updatedAt)
			current = v2
			currentVersion = 2
			mutations++
		}
		insertIntegrationProjection(t, tx, objectID, currentVersion, testOwner, StateDraft, current)
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
			sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
		) VALUES (?, ?, ?, ?, ?, 'field_alias', ?, 'private', 'draft', ?, ?, ?, NULL, NULL, NULL, NULL)`,
			testTenant, objectID, currentVersion, testApp, testOwner, name, current.Digest[:], timestamp, updatedAt,
		); err != nil {
			t.Fatalf("insert batch registry %d: %v", index, err)
		}
	}
	for mutation := int64(0); mutation < mutations; mutation++ {
		if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
			SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant); err != nil {
			t.Fatalf("advance batch catalog revision %d: %v", mutation, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit batch fixture: %v", err)
	}
}

func insertIntegrationDisabledBatchObjects(t *testing.T, database *control.DB, count int) {
	t.Helper()
	if count < 1 || count > maximumObjectsPerTenant || int64(count)*2 > maximumVersionsPerTenant {
		t.Fatalf("invalid disabled batch fixture count %d", count)
	}
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin disabled batch fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)
	for index := 0; index < count; index++ {
		objectID := fmt.Sprintf("ko-disabled-batch-%04d", index)
		name := fmt.Sprintf("disabled-batch-%04d", index)
		description := fmt.Sprintf("disabled batch body %04d", index)
		normalized, normalizeErr := knowledgedefinition.Normalize(aliasDefinition(
			testApp,
			name,
			SharingScopePrivate,
			&description,
			fmt.Sprintf("disabled-batch-%04d-*", index),
		))
		if normalizeErr != nil {
			t.Fatalf("normalize disabled batch %d: %v", index, normalizeErr)
		}
		createdAt := int64(20_000 + index*3)
		disabledAt := createdAt + 1
		insertIntegrationDefinitionBlob(t, tx, normalized.Bytes, normalized.Digest[:], createdAt)
		insertIntegrationVersion(t, tx, objectID, 1, StateActive, "create", normalized.Digest[:], normalized, createdAt)
		insertIntegrationVersion(t, tx, objectID, 2, StateDisabled, "disable", normalized.Digest[:], normalized, disabledAt)
		insertIntegrationProjection(t, tx, objectID, 2, testOwner, StateDisabled, normalized)
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
			sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
		) VALUES (?, ?, 2, ?, ?, 'field_alias', ?, 'private', 'disabled', ?, ?, ?, ?, NULL, NULL, NULL)`,
			testTenant,
			objectID,
			testApp,
			testOwner,
			name,
			normalized.Digest[:],
			createdAt,
			disabledAt,
			disabledAt,
		); err != nil {
			t.Fatalf("insert disabled batch registry %d: %v", index, err)
		}
	}
	for mutation := 0; mutation < count*2; mutation++ {
		if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
			SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant); err != nil {
			t.Fatalf("advance disabled batch catalog revision %d: %v", mutation, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit disabled batch fixture: %v", err)
	}
}

func integrationCountCatalogQueries(
	t *testing.T,
	database *control.DB,
	operation func() (ListPage, error),
) (ListPage, int64) {
	t.Helper()
	var queryCount atomic.Int64
	callbackName := "test:count-knowledge-batch-queries"
	if err := database.GORMDB().Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(*gorm.DB) { queryCount.Add(1) },
	); err != nil {
		t.Fatalf("register query counter: %v", err)
	}
	page, err := operation()
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove query counter: %v", removeErr)
	}
	if err != nil {
		t.Fatalf("counted List: %v", err)
	}
	return page, queryCount.Load()
}
