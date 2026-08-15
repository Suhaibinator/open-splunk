package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestListPageStopsBeforeTransitiveDependencyClosureBudget(t *testing.T) {
	database, store := newCatalogTestStore(t)
	firstID := seedDenseSemanticBudgetGraph(t, database, "a", 10_000)
	secondID := seedDenseSemanticBudgetGraph(t, database, "b", 20_000)
	normalized, err := normalizeListRequest(testReadScope(), ListRequest{
		PageSize:          2,
		ObjectTypeFilters: []ObjectType{ObjectTypeCalculatedField},
		SortBy:            SortByName,
		SortDirection:     SortAscending,
	})
	if err != nil {
		t.Fatalf("normalize dense dependency page: %v", err)
	}
	query := applyListOrder(
		applyListFilters(baseProjectionQuery(database.GORMDB()), normalized),
		normalized,
	)
	records, err := readProjectionRecords(query, 3)
	if err != nil {
		t.Fatalf("read dense dependency roots: %v", err)
	}
	if got := projectionIDs(records); !slices.Equal(got, []string{firstID, secondID}) {
		t.Fatalf("dense dependency root order = %v", got)
	}

	budget := listResponseHydrationBudget
	budget.dependencies = maximumDependencyGraphEdges
	objects, boundary, err := objectsFromProjectionsPage(database.GORMDB(), records, budget)
	if err != nil {
		t.Fatalf("hydrate dependency-bounded page: %v", err)
	}
	if boundary != 1 || len(objects) != 1 || objects[0].KnowledgeObjectID != firstID {
		t.Fatalf("dependency page boundary/objects = %d/%v, want 1/[%s]", boundary, objectIDs(objects), firstID)
	}
	if _, err := objectsFromProjections(database.GORMDB(), records, budget); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("strict dense dependency hydration error = %v, want CapacityExceeded", err)
	}
	objects, boundary, err = objectsFromProjectionsPage(database.GORMDB(), records[1:], budget)
	if err != nil || boundary != 1 || len(objects) != 1 || objects[0].KnowledgeObjectID != secondID {
		t.Fatalf("continued dependency page = boundary:%d objects:%v error:%v", boundary, objectIDs(objects), err)
	}

	// Top-level non-parallel tests do not overlap Go's t.Parallel tests. Lower
	// the private response work budget to exercise the production List boundary
	// without constructing 110 disjoint maximum-sized graphs, then restore it.
	previousBudget := listResponseHydrationBudget
	listResponseHydrationBudget.dependencies = maximumDependencyGraphEdges
	t.Cleanup(func() { listResponseHydrationBudget = previousBudget })
	request := ListRequest{
		PageSize:          2,
		ObjectTypeFilters: []ObjectType{ObjectTypeCalculatedField},
		SortBy:            SortByName,
		SortDirection:     SortAscending,
	}
	firstPage, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(first semantic-budget page): %v", err)
	}
	if got := objectIDs(firstPage.Objects); !slices.Equal(got, []string{firstID}) ||
		firstPage.NextPageToken == "" {
		t.Fatalf("first semantic-budget page = objects:%v token:%q", got, firstPage.NextPageToken)
	}
	continuation := request
	continuation.PageToken = firstPage.NextPageToken
	secondPage, err := store.List(context.Background(), testReadScope(), continuation)
	if err != nil {
		t.Fatalf("List(second semantic-budget page): %v", err)
	}
	if got := objectIDs(secondPage.Objects); !slices.Equal(got, []string{secondID}) ||
		secondPage.NextPageToken != "" || secondPage.CatalogRevision != firstPage.CatalogRevision {
		t.Fatalf("second semantic-budget page = objects:%v token:%q revision:%d, first revision:%d",
			got, secondPage.NextPageToken, secondPage.CatalogRevision, firstPage.CatalogRevision)
	}
}

func seedDenseSemanticBudgetGraph(
	t *testing.T,
	database *control.DB,
	prefix string,
	baseTimestamp int64,
) string {
	t.Helper()
	const width = 24
	extractionIDs := make([]string, width)
	for index := range width {
		field := "raw_" + prefix
		extractionIDs[index] = fmt.Sprintf("ko-budget-%s-extraction-%02d", prefix, index)
		insertFixtureObject(t, database, fixtureObject{
			id: extractionIDs[index], owner: testOwner,
			versions: []fixtureVersion{{
				definition: dependencyExtractionDefinition(
					testApp,
					fmt.Sprintf("budget-%s-extraction-%02d", prefix, index),
					SharingScopePrivate,
					nil,
					"budget-*",
					field,
				),
				state: StateActive, mutation: "create", timestamp: baseTimestamp + int64(index),
			}},
		})
	}

	aliasIDs := make([]string, width)
	rootInputs := make([]string, width)
	for index := range width {
		aliasIDs[index] = fmt.Sprintf("ko-budget-%s-alias-%02d", prefix, index)
		rootInputs[index] = fmt.Sprintf("alias_%s_%02d", prefix, index)
		dependencies := make([]fixtureDependency, len(extractionIDs))
		for targetIndex, targetID := range extractionIDs {
			dependencies[targetIndex] = fixtureDependency{targetObjectID: targetID, targetVersion: 1}
		}
		insertFixtureObject(t, database, fixtureObject{
			id: aliasIDs[index], owner: testOwner,
			versions: []fixtureVersion{{
				definition: dependencyAliasDefinition(
					testApp,
					fmt.Sprintf("budget-%s-alias-%02d", prefix, index),
					SharingScopePrivate,
					nil,
					"budget-*",
					"raw_"+prefix,
					rootInputs[index],
				),
				state: StateActive, mutation: "create",
				timestamp: baseTimestamp + width + int64(index), dependencies: dependencies,
			}},
		})
	}

	rootDependencies := make([]fixtureDependency, len(aliasIDs))
	for index, targetID := range aliasIDs {
		rootDependencies[index] = fixtureDependency{targetObjectID: targetID, targetVersion: 1}
	}
	rootID := "ko-budget-" + prefix + "-root"
	insertFixtureObject(t, database, fixtureObject{
		id: rootID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyCalculatedDefinition(
				testApp,
				"budget-"+prefix+"-root",
				SharingScopePrivate,
				nil,
				"budget-*",
				"coalesce("+strings.Join(rootInputs, ",")+")",
				"calculated_"+prefix,
			),
			state: StateActive, mutation: "create",
			timestamp: baseTimestamp + 2*width, dependencies: rootDependencies,
		}},
	})
	return rootID
}

func projectionIDs(records []projectionRecord) []string {
	ids := make([]string, len(records))
	for index, record := range records {
		ids[index] = record.KnowledgeObjectID
	}
	return ids
}

func objectIDs(objects []Object) []string {
	ids := make([]string, len(objects))
	for index, object := range objects {
		ids[index] = object.KnowledgeObjectID
	}
	return ids
}
