package knowledgecatalog

import (
	"slices"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestPublicationGlobalSemanticsRemainInOwningOrganization(t *testing.T) {
	harness := newWriterActiveRouteEmptyHarness(t)
	const appTwo = "app_000000000100000000003A"
	apps, err := control.NewAppCatalog(harness.database, control.AppCatalogOptions{CursorKey: testCursorKey, Clock: func() time.Time { return time.UnixMicro(2000).UTC() }, IDGenerator: func() (string, error) { return appTwo, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: testTenant, OrganizationID: 2}, control.AppDefinition{Slug: "organization-two", DisplayName: "Organization Two", DefaultTimeRange: &control.AppTimeRange{Earliest: new("-24h"), Latest: new("now")}}); err != nil {
		t.Fatal(err)
	}
	indexTwo := createPublicationTransitionTestIndex(t, harness.database, "organization-two-main")
	if err := harness.database.GORMDB().Exec("UPDATE os_index_organizations SET organization_id = 2 WHERE index_id = ?", indexTwo.ID).Error; err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct{ app, id string }{{testApp, "org-one-input"}, {appTwo, "org-two-input"}} {
		insertFixtureObject(t, harness.database, fixtureObject{id: fixture.id, owner: testOwner, versions: []fixtureVersion{{definition: writerActiveRouteDefinition(dependencyExtractionDefinition(fixture.app, "same-global-input", SharingScopeGlobal, nil, "", dependencyFixtureInputField), "*"), state: StateActive, mutation: "create", timestamp: 10}}})
	}
	definitions := []*opensplunk.KnowledgeObjectDefinition{
		writerActiveRouteDefinition(dependencyAliasDefinition(testApp, "same-global-alias", SharingScopeGlobal, nil, "", dependencyFixtureInputField, "organization_alias"), "*"),
		writerActiveRouteDefinition(dependencyAliasDefinition(appTwo, "same-global-alias", SharingScopeGlobal, nil, "", dependencyFixtureInputField, "organization_alias"), "*"),
	}
	var ids []string
	for index, definition := range definitions {
		scope := WriteScope{TenantID: testTenant, OwnerID: testOwner, OrganizationID: int64(index + 1), WritableAppIDs: []string{definition.AppId}}
		created, err := harness.writer.Create(harness.actorContext, scope, &opensplunk.CreateKnowledgeObjectRequest{Definition: definition, InitialState: opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE, ClientRequestId: []string{"org-one-global-alias", "org-two-global-alias"}[index]})
		if err != nil {
			t.Fatalf("organization %d GLOBAL publication: %v", index+1, err)
		}
		ids = append(ids, created.KnowledgeObject.KnowledgeObjectId)
	}
	// Site privilege authorizes management, while semantic witnesses and
	// dependency identities still belong to the resource's organization.
	definition := definitions[1]
	definition.Description = new("updated by site administrator")
	if _, err := harness.writer.Update(harness.actorContext, WriteScope{TenantID: testTenant, OwnerID: "site-owner", OrganizationID: 2, SiteAdministrator: true, WritableAppIDs: []string{appTwo}}, &opensplunk.UpdateKnowledgeObjectRequest{KnowledgeObjectId: ids[1], ExpectedVersion: 1, Definition: definition, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}}, ClientRequestId: "site-origin-organization-update"}); err != nil {
		t.Fatalf("site update in origin organization: %v", err)
	}
	store, err := New(harness.database, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(store, ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for index, fixture := range []struct{ app, name, input string }{{testApp, "main", "org-one-input"}, {appTwo, "organization-two-main", "org-two-input"}} {
		resolved, err := resolver.Resolve(t.Context(), ResolutionScope{TenantID: testTenant, PrincipalID: "site-owner", OrganizationID: int64(index + 1), SiteAdministrator: true, AppID: fixture.app, EffectiveAuthorizedIndexes: []string{fixture.name}})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, object := range resolved.Objects() {
			got = append(got, object.KnowledgeObjectID)
		}
		if len(got) != 2 || !slices.Contains(got, fixture.input) || !slices.Contains(got, ids[index]) || slices.Contains(got, ids[1-index]) {
			t.Fatalf("organization %d resolution contains foreign definitions: %v", index+1, got)
		}
	}
	// Index admission must evaluate the two namespaces independently too.
	createPublicationTransitionTestIndex(t, harness.database, "future-index")
}
