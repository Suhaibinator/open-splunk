package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestViewerDelegatedManageCommitsNativeIndexMutationAndUserAudit(t *testing.T) {
	database, err := control.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	index, err := database.CreateIndex(t.Context(), control.IndexDefinition{Name: "delegated", SearchEnabled: true, IngestionEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := audit.NewStore(database, audit.StoreOptions{CursorKey: []byte("delegated-index-audit-cursor-key-32-bytes")})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := control.NewAuditedIndexAdministration(database, control.AuditedIndexAdministrationOptions{TenantID: "physical", Appender: journal})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := authorization.NewService(viewerManagementPolicyStore{}, orgMembershipStub{role: authorization.ViewerRole, member: true}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	handler := &apiHandler{authorization: policy, indexAdmin: admin, indexes: database, tenantID: "physical"}
	ctx := authorization.WithScope(t.Context(), authorization.Scope{Subject: authorization.Subject{UserID: 91, UserType: 12}, OrganizationID: 7, PhysicalTenant: "physical"})
	ctx, err = audit.WithActor(ctx, audit.Actor{Kind: audit.ActorKindBrowser, ID: "91", Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, capabilityContextKey{}, authorization.Manage)
	description := "updated with an explicit index capability"
	input := &opensplunk.UpdateIndexRequest{Selector: &opensplunk.IndexSelector{Selector: &opensplunk.IndexSelector_IndexId{IndexId: index.ID}}, ExpectedVersion: 1, Definition: &opensplunk.IndexDefinition{Description: &description}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}}}
	result, err := handler.updateIndex(httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/indexes/update", nil), input)
	if err != nil || result.GetIndex().GetVersion() != 2 {
		t.Fatalf("delegated native mutation failed: %+v %v", result, err)
	}
	page, err := journal.List(t.Context(), "physical", audit.ListRequest{})
	if err != nil || len(page.Events) != 1 || page.Events[0].Actor.Role != audit.ActorRoleUser || page.Events[0].Actor.ID != "91" {
		t.Fatalf("delegated actor audit: %+v %v", page, err)
	}
	handler.authorization, err = authorization.NewService(sourceReadPolicyStore{}, orgMembershipStub{role: authorization.ViewerRole, member: true}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	input.ExpectedVersion = 2
	if _, err := handler.updateIndex(httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/indexes/update", nil), input); err == nil {
		t.Fatal("Viewer without Manage mutated index")
	}
	persisted, err := database.GetIndex(t.Context(), index.ID)
	if err != nil || persisted.Version != 2 {
		t.Fatalf("denied mutation committed: %+v %v", persisted, err)
	}
}
