package server

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

func TestKnowledgePrivateReadScopeHonorsSiteAdministrator(t *testing.T) {
	scope := knowledgeScopes{apps: []string{"app-selected"}, read: knowledgecatalog.ReadScope{OrganizationID: 1, OwnerID: "1009"}}
	if scope.allowsRead(knowledgecatalog.SharingScopePrivate, "app-selected", "1010") {
		t.Fatal("ordinary user read another user's private knowledge")
	}
	scope.read.SiteAdministrator = true
	if !scope.allowsRead(knowledgecatalog.SharingScopePrivate, "app-selected", "1010") {
		t.Fatal("site administrator cannot read authorized private knowledge")
	}
	if scope.allowsRead(knowledgecatalog.SharingScopePrivate, "app-outside", "1010") {
		t.Fatal("scope lost selected app boundary")
	}
}
