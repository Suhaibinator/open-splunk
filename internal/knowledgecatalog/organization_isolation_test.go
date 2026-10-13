package knowledgecatalog

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestKnowledgeOrganizationScopesProtectGlobalPrivateAndMutation(t *testing.T) {
	h := newWriterActiveRouteEmptyHarness(t)
	h.scope.OrganizationID = 1
	store, err := New(h.database, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	apps, err := control.NewAppCatalog(h.database, control.AppCatalogOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: testTenant, OrganizationID: 2}, control.AppDefinition{Slug: "foreign", DisplayName: "Foreign"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := h.writer.Create(h.actorContext, h.scope, &opensplunk.CreateKnowledgeObjectRequest{Definition: aliasDefinition(testApp, "globalorg", SharingScopeGlobal, nil, ""), InitialState: opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "org-scope-create-0001"})
	if err != nil {
		t.Fatal(err)
	}
	id := created.GetKnowledgeObject().GetKnowledgeObjectId()
	read := ReadScope{TenantID: testTenant, OrganizationID: 2, OwnerID: testOwner, ReadableAppIDs: []string{testApp, foreign.ID}}
	if _, err := store.Get(t.Context(), read, id, nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("foreign GLOBAL get: %v", err)
	}
	page, err := store.List(t.Context(), read, ListRequest{})
	if err != nil || len(page.Objects) != 0 {
		t.Fatalf("foreign GLOBAL list %#v %v", page, err)
	}
	// A forged writable-app allowlist must not authorize another organization's app.
	foreignWrite := h.scope
	foreignWrite.OrganizationID = 2
	foreignWrite.WritableAppIDs = []string{testApp, foreign.ID}
	if _, err := h.writer.Create(h.actorContext, foreignWrite, &opensplunk.CreateKnowledgeObjectRequest{Definition: aliasDefinition(testApp, "forged", SharingScopeGlobal, nil, ""), InitialState: opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "org-scope-create-0002"}); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("forged writable app: %v", err)
	}
	private, err := h.writer.Create(h.actorContext, h.scope, &opensplunk.CreateKnowledgeObjectRequest{Definition: aliasDefinition(testApp, "privateorg", SharingScopePrivate, nil, ""), InitialState: opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "org-scope-create-0003"})
	if err != nil {
		t.Fatal(err)
	}
	admin := ReadScope{TenantID: testTenant, OrganizationID: 1, OwnerID: "siteadmin", ReadableAppIDs: []string{testApp}, SiteAdministrator: true}
	if _, err := store.Get(t.Context(), admin, private.GetKnowledgeObject().GetKnowledgeObjectId(), nil); err != nil {
		t.Fatalf("siteadmin owner bypass: %v", err)
	}
	admin.SiteAdministrator = false
	if _, err := store.Get(t.Context(), admin, private.GetKnowledgeObject().GetKnowledgeObjectId(), nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("private owner check: %v", err)
	}
}
