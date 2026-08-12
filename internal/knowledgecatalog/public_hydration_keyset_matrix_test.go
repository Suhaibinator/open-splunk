package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
)

func TestPersistedTierOneBodyHydrationAndFilters(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)

	type tierOneFixture struct {
		id            string
		body          string
		owner         string
		definition    *opensplunkv1.KnowledgeObjectDefinition
		objectType    ObjectType
		sharingScope  SharingScope
		overwrite     opensplunkv1.KnowledgeOverwriteBehavior
		normalized    knowledgedefinition.Normalized
		selectorToken string
	}
	fixtures := []tierOneFixture{
		{
			id: "ko-tier1-regex", body: "regex", owner: testOwner,
			definition: tierOneMatrixDefinition("tier1-regex", SharingScopePrivate, "regex", "selector-regex-*"),
			objectType: ObjectTypeFieldExtraction, sharingScope: SharingScopePrivate,
			overwrite:     opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			selectorToken: "selector-regex",
		},
		{
			id: "ko-tier1-json", body: "json", owner: "owner-b",
			definition: tierOneMatrixDefinition("tier1-json", SharingScopeApp, "json", "selector-json-*"),
			objectType: ObjectTypeFieldExtraction, sharingScope: SharingScopeApp,
			overwrite:     opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			selectorToken: "selector-json",
		},
		{
			id: "ko-tier1-alias", body: "alias", owner: "owner-b",
			definition: tierOneMatrixDefinition("tier1-alias", SharingScopeGlobal, "alias", "selector-alias-*"),
			objectType: ObjectTypeFieldAlias, sharingScope: SharingScopeGlobal,
			overwrite:     opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			selectorToken: "selector-alias",
		},
		{
			id: "ko-tier1-calculated", body: "calculated", owner: testOwner,
			definition: tierOneMatrixDefinition("tier1-calculated", SharingScopePrivate, "calculated", "selector-calculated-*"),
			objectType: ObjectTypeCalculatedField, sharingScope: SharingScopePrivate,
			overwrite:     opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			selectorToken: "selector-calculated",
		},
	}

	for index := range fixtures {
		fixture := &fixtures[index]
		normalized, err := knowledgedefinition.Normalize(fixture.definition)
		if err != nil {
			t.Fatalf("Normalize(%s): %v", fixture.body, err)
		}
		fixture.normalized = normalized
		insertFixtureObject(t, database, fixtureObject{
			id: fixture.id, owner: fixture.owner,
			versions: []fixtureVersion{{
				definition: fixture.definition,
				state:      StateActive,
				mutation:   "create",
				timestamp:  int64(10 + index),
			}},
		})
	}

	listedPage, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 16})
	if err != nil {
		t.Fatalf("List(all Tier-1 bodies): %v", err)
	}
	if got, want := tierOneMatrixObjectIDs(listedPage.Objects), []string{
		"ko-tier1-alias", "ko-tier1-calculated", "ko-tier1-json", "ko-tier1-regex",
	}; !slices.Equal(got, want) {
		t.Fatalf("List(all Tier-1 bodies) IDs = %v, want %v", got, want)
	}
	listedByID := make(map[string]Object, len(listedPage.Objects))
	for _, object := range listedPage.Objects {
		listedByID[object.KnowledgeObjectID] = object
	}

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.body+" hydration and detachment", func(t *testing.T) {
			got, err := store.Get(context.Background(), testReadScope(), fixture.id, nil)
			if err != nil {
				t.Fatalf("Get(%s): %v", fixture.id, err)
			}
			listed, found := listedByID[fixture.id]
			if !found {
				t.Fatalf("List omitted %s", fixture.id)
			}
			tierOneMatrixAssertObject(t, got, fixture.id, fixture.objectType, fixture.sharingScope, fixture.overwrite, fixture.normalized)
			tierOneMatrixAssertObject(t, listed, fixture.id, fixture.objectType, fixture.sharingScope, fixture.overwrite, fixture.normalized)

			if got.Definition == listed.Definition || &got.DefinitionSHA256[0] == &listed.DefinitionSHA256[0] {
				t.Fatal("Get and List shared definition or digest storage")
			}
			tierOneMatrixMutateDefinition(got.Definition)
			got.DefinitionSHA256[0] ^= 0xff
			tierOneMatrixAssertObject(t, listed, fixture.id, fixture.objectType, fixture.sharingScope, fixture.overwrite, fixture.normalized)

			tierOneMatrixMutateDefinition(listed.Definition)
			listed.DefinitionSHA256[0] ^= 0xff
			again, err := store.Get(context.Background(), testReadScope(), fixture.id, nil)
			if err != nil {
				t.Fatalf("Get(%s after caller mutations): %v", fixture.id, err)
			}
			tierOneMatrixAssertObject(t, again, fixture.id, fixture.objectType, fixture.sharingScope, fixture.overwrite, fixture.normalized)
		})
	}

	filterTests := []struct {
		name    string
		request ListRequest
		wantIDs []string
	}{
		{"type extraction", ListRequest{ObjectTypeFilters: []ObjectType{ObjectTypeFieldExtraction}}, []string{"ko-tier1-json", "ko-tier1-regex"}},
		{"type alias", ListRequest{ObjectTypeFilters: []ObjectType{ObjectTypeFieldAlias}}, []string{"ko-tier1-alias"}},
		{"type calculated", ListRequest{ObjectTypeFilters: []ObjectType{ObjectTypeCalculatedField}}, []string{"ko-tier1-calculated"}},
		{"scope private", ListRequest{SharingScopeFilters: []SharingScope{SharingScopePrivate}}, []string{"ko-tier1-calculated", "ko-tier1-regex"}},
		{"scope app", ListRequest{SharingScopeFilters: []SharingScope{SharingScopeApp}}, []string{"ko-tier1-json"}},
		{"scope global", ListRequest{SharingScopeFilters: []SharingScope{SharingScopeGlobal}}, []string{"ko-tier1-alias"}},
	}
	for _, test := range filterTests {
		t.Run("filter "+test.name, func(t *testing.T) {
			page, err := store.List(context.Background(), testReadScope(), test.request)
			if err != nil {
				t.Fatalf("List(%s): %v", test.name, err)
			}
			if got := tierOneMatrixObjectIDs(page.Objects); !slices.Equal(got, test.wantIDs) {
				t.Fatalf("List(%s) IDs = %v, want %v", test.name, got, test.wantIDs)
			}
		})
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run("selector filter "+fixture.body, func(t *testing.T) {
			page, err := store.List(context.Background(), testReadScope(), ListRequest{
				SelectorTextFilter: &fixture.selectorToken,
			})
			if err != nil {
				t.Fatalf("List(selector %s): %v", fixture.body, err)
			}
			if got := tierOneMatrixObjectIDs(page.Objects); !slices.Equal(got, []string{fixture.id}) {
				t.Fatalf("List(selector %s) IDs = %v, want [%s]", fixture.body, got, fixture.id)
			}
		})
	}
}

func TestListKeysetTraversalSortMatrix(t *testing.T) {
	t.Parallel()
	database, store := newCatalogTestStore(t)

	fixtures := []struct {
		id         string
		name       string
		body       string
		createdAt  int64
		updatedAt  int64
		objectType ObjectType
	}{
		{"ko-keyset-a", "same", "alias", 10, 100, ObjectTypeFieldAlias},
		{"ko-keyset-b", "same", "alias", 10, 120, ObjectTypeFieldAlias},
		{"ko-keyset-c", "same", "calculated", 20, 100, ObjectTypeCalculatedField},
		{"ko-keyset-d", "alpha", "regex", 30, 110, ObjectTypeFieldExtraction},
		{"ko-keyset-e", "zulu", "json", 40, 120, ObjectTypeFieldExtraction},
		{"ko-keyset-f", "middle", "alias", 40, 130, ObjectTypeFieldAlias},
	}
	for _, fixture := range fixtures {
		previous := tierOneMatrixDefinition(fixture.name, SharingScopePrivate, fixture.body, "")
		current := tierOneMatrixDefinition(fixture.name, SharingScopePrivate, fixture.body, "")
		*previous.Description += " v1 " + fixture.id
		*current.Description += " v2 " + fixture.id
		insertFixtureObject(t, database, fixtureObject{
			id: fixture.id, owner: testOwner,
			versions: []fixtureVersion{
				{
					definition: previous,
					state:      StateDraft, mutation: "create", timestamp: fixture.createdAt,
				},
				{
					definition: current,
					state:      StateDraft, mutation: "update", timestamp: fixture.updatedAt,
				},
			},
		})
	}

	fixturePage, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 16})
	if err != nil {
		t.Fatalf("List(keyset fixtures): %v", err)
	}
	if len(fixturePage.Objects) != len(fixtures) {
		t.Fatalf("List(keyset fixtures) objects = %d, want %d", len(fixturePage.Objects), len(fixtures))
	}
	objectsByID := make(map[string]Object, len(fixturePage.Objects))
	for _, object := range fixturePage.Objects {
		objectsByID[object.KnowledgeObjectID] = object
	}
	for _, fixture := range fixtures {
		if got := objectsByID[fixture.id].ObjectType; got != fixture.objectType {
			t.Fatalf("fixture %s ObjectType = %q, want %q", fixture.id, got, fixture.objectType)
		}
	}
	a, b, c := objectsByID["ko-keyset-a"], objectsByID["ko-keyset-b"], objectsByID["ko-keyset-c"]
	e, f := objectsByID["ko-keyset-e"], objectsByID["ko-keyset-f"]
	if a.Name != b.Name || b.Name != c.Name {
		t.Fatalf("name tie fixture = %q/%q/%q", a.Name, b.Name, c.Name)
	}
	if a.ObjectType != b.ObjectType || a.Name != b.Name {
		t.Fatalf("type+name tie fixture = %q/%q and %q/%q", a.ObjectType, a.Name, b.ObjectType, b.Name)
	}
	if !a.CreatedAt.Equal(b.CreatedAt) || !e.CreatedAt.Equal(f.CreatedAt) {
		t.Fatalf("created-at ties missing: a/b=%v/%v e/f=%v/%v", a.CreatedAt, b.CreatedAt, e.CreatedAt, f.CreatedAt)
	}
	if !a.UpdatedAt.Equal(c.UpdatedAt) || !b.UpdatedAt.Equal(e.UpdatedAt) {
		t.Fatalf("updated-at ties missing: a/c=%v/%v b/e=%v/%v", a.UpdatedAt, c.UpdatedAt, b.UpdatedAt, e.UpdatedAt)
	}

	ascending := map[SortBy][]string{
		SortByName:       {"ko-keyset-d", "ko-keyset-f", "ko-keyset-a", "ko-keyset-b", "ko-keyset-c", "ko-keyset-e"},
		SortByCreatedAt:  {"ko-keyset-a", "ko-keyset-b", "ko-keyset-c", "ko-keyset-d", "ko-keyset-e", "ko-keyset-f"},
		SortByUpdatedAt:  {"ko-keyset-a", "ko-keyset-c", "ko-keyset-d", "ko-keyset-b", "ko-keyset-e", "ko-keyset-f"},
		SortByObjectType: {"ko-keyset-c", "ko-keyset-f", "ko-keyset-a", "ko-keyset-b", "ko-keyset-d", "ko-keyset-e"},
	}
	type staleContinuation struct {
		name    string
		request ListRequest
	}
	staleContinuations := make([]staleContinuation, 0, 16)
	for _, sortBy := range []SortBy{SortByName, SortByCreatedAt, SortByUpdatedAt, SortByObjectType} {
		for _, direction := range []SortDirection{SortAscending, SortDescending} {
			for _, pageSize := range []uint32{1, 2} {
				caseName := fmt.Sprintf("%s/%s/page-%d", sortBy, direction, pageSize)
				t.Run(caseName, func(t *testing.T) {
					want := slices.Clone(ascending[sortBy])
					if direction == SortDescending {
						slices.Reverse(want)
					}
					request := ListRequest{PageSize: pageSize, SortBy: sortBy, SortDirection: direction}
					seenIDs := make(map[string]struct{}, len(want))
					seenTokens := make(map[string]struct{})
					gotAll := make([]string, 0, len(want))
					var revision uint64
					for offset := 0; offset < len(want); {
						page, err := store.List(context.Background(), testReadScope(), request)
						if err != nil {
							t.Fatalf("List(offset %d): %v", offset, err)
						}
						if revision == 0 {
							revision = page.CatalogRevision
						} else if page.CatalogRevision != revision {
							t.Fatalf("catalog revision changed from %d to %d", revision, page.CatalogRevision)
						}
						end := min(offset+int(pageSize), len(want))
						pageIDs := tierOneMatrixObjectIDs(page.Objects)
						if !slices.Equal(pageIDs, want[offset:end]) {
							t.Fatalf("page at offset %d IDs = %v, want %v", offset, pageIDs, want[offset:end])
						}
						for _, id := range pageIDs {
							if _, duplicate := seenIDs[id]; duplicate {
								t.Fatalf("duplicate object %s", id)
							}
							seenIDs[id] = struct{}{}
						}
						gotAll = append(gotAll, pageIDs...)
						if end < len(want) {
							if page.NextPageToken == "" {
								t.Fatalf("page at offset %d omitted continuation", offset)
							}
							if _, duplicate := seenTokens[page.NextPageToken]; duplicate {
								t.Fatal("List repeated a continuation token")
							}
							seenTokens[page.NextPageToken] = struct{}{}
							if offset == 0 {
								continuation := request
								continuation.PageToken = page.NextPageToken
								staleContinuations = append(staleContinuations, staleContinuation{name: caseName, request: continuation})
							}
						} else if page.NextPageToken != "" {
							t.Fatalf("exhausted page returned continuation %q", page.NextPageToken)
						}
						request.PageToken = page.NextPageToken
						offset = end
					}
					if !slices.Equal(gotAll, want) || len(seenIDs) != len(want) {
						t.Fatalf("complete traversal = %v (%d unique), want %v", gotAll, len(seenIDs), want)
					}
				})
			}
		}
	}
	if len(staleContinuations) != 16 {
		t.Fatalf("saved continuations = %d, want 16", len(staleContinuations))
	}
	mustExec(t, database, `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = ?`, testTenant)
	for _, continuation := range staleContinuations {
		continuation := continuation
		t.Run("revision invalidates "+continuation.name, func(t *testing.T) {
			if _, err := store.List(context.Background(), testReadScope(), continuation.request); !errors.Is(err, control.ErrPageInvalidated) {
				t.Fatalf("List(stale continuation) error = %v, want ErrPageInvalidated", err)
			}
		})
	}
}

func tierOneMatrixDefinition(name string, scope SharingScope, body, selector string) *opensplunkv1.KnowledgeObjectDefinition {
	description := "persisted Tier-1 " + body + " body for " + name
	definition := aliasDefinition(testApp, name, scope, &description, selector)
	switch body {
	case "regex":
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern: `status=(?<status>[0-9]+)`, OutputFields: []string{"status"},
					},
				},
			},
		}
	case "json":
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
					Json: &opensplunkv1.JsonFieldExtractionDefinition{Path: "payload.user", OutputField: "user.name"},
				},
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		}
	case "alias":
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "source", DestinationField: "source_alias",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		}
	case "calculated":
		definition.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "latency.class", Expression: `if(latency > 100, "slow", "fast")`,
			},
		}
	default:
		panic("unsupported Tier-1 test body: " + body)
	}
	return definition
}

func tierOneMatrixAssertObject(
	t *testing.T,
	object Object,
	wantID string,
	wantType ObjectType,
	wantScope SharingScope,
	wantOverwrite opensplunkv1.KnowledgeOverwriteBehavior,
	want knowledgedefinition.Normalized,
) {
	t.Helper()
	if object.KnowledgeObjectID != wantID || object.ObjectType != wantType ||
		object.SharingScope != wantScope || object.Name != want.Name {
		t.Fatalf("object identity/type/scope = %q/%q/%q/%q, want %q/%q/%q/%q",
			object.KnowledgeObjectID, object.ObjectType, object.SharingScope, object.Name,
			wantID, wantType, wantScope, want.Name)
	}
	if object.Definition == nil || !proto.Equal(object.Definition, want.Definition) {
		t.Fatalf("definition = %v, want %v", object.Definition, want.Definition)
	}
	if !bytes.Equal(object.DefinitionSHA256, want.Digest[:]) || len(object.DefinitionSHA256) != 32 {
		t.Fatalf("definition digest = %x, want %x", object.DefinitionSHA256, want.Digest)
	}
	var overwrite opensplunkv1.KnowledgeOverwriteBehavior
	switch wantType {
	case ObjectTypeFieldExtraction:
		overwrite = object.Definition.GetFieldExtraction().GetOverwriteBehavior()
	case ObjectTypeFieldAlias:
		overwrite = object.Definition.GetFieldAlias().GetOverwriteBehavior()
	case ObjectTypeCalculatedField:
		overwrite = object.Definition.GetCalculatedField().GetOverwriteBehavior()
	default:
		t.Fatalf("unexpected ObjectType %q", wantType)
	}
	if overwrite != wantOverwrite {
		t.Fatalf("overwrite enum = %v, want %v", overwrite, wantOverwrite)
	}
	patterns := object.Definition.GetSelector().GetHostPatterns()
	if len(patterns) != 1 || patterns[0].GetMatchKind() != opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD {
		t.Fatalf("canonical selector enum = %v, want one wildcard", patterns)
	}
}

func tierOneMatrixMutateDefinition(definition *opensplunkv1.KnowledgeObjectDefinition) {
	definition.Name = "caller-mutated"
	switch body := definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		body.FieldExtraction.InputField = "caller-mutated"
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		body.FieldAlias.SourceField = "caller-mutated"
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		body.CalculatedField.Expression = "caller-mutated"
	}
}

func tierOneMatrixObjectIDs(objects []Object) []string {
	ids := make([]string, len(objects))
	for index := range objects {
		ids[index] = objects[index].KnowledgeObjectID
	}
	return ids
}
