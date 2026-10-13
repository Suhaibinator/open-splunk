package knowledgepreview

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestPreviewRejectsOrganizationAndPrivilegeMismatchBeforeAcquisition(t *testing.T) {
	fixture := newPreviewFixture(t)
	searches := &countingExecutionSource{}
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{Searches: searches, Writer: fixture.writer, Compiler: neverCompiler(&compilerCalls), Executor: neverExecutor(&executorCalls)})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"organization", "site privilege"} {
		scope := fixture.scope
		if field == "organization" {
			scope.Read.OrganizationID, scope.Write.OrganizationID = 2, 2
		} else {
			scope.Read.SiteAdministrator, scope.Write.SiteAdministrator = true, true
		}
		if _, err := service.Preview(t.Context(), fixture.access, scope, invalidPreviewRequest()); !errors.Is(err, ErrNotFoundOrForbidden) {
			t.Fatalf("mismatched %s: %v", field, err)
		}
	}
	if searches.calls.Load() != 0 || compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatal("mismatched authority reached retained acquisition or execution")
	}
}

func TestPreviewSiteAuthoritySurvivesNativeScopeCloneAndRetainedAcquisition(t *testing.T) {
	fixture := newPreviewFixture(t)
	access := fixture.access
	access.OwnerID, access.SiteAdministrator = "site-preview-user", true
	scope := fixture.scope
	scope.Read.OwnerID, scope.Write.OwnerID = access.OwnerID, access.OwnerID
	scope.Read.SiteAdministrator, scope.Write.SiteAdministrator = true, true
	var compilerCalls, executorCalls atomic.Int64
	service, err := NewService(Config{Searches: fixture.manager, Writer: fixture.writer, Compiler: neverCompiler(&compilerCalls), Executor: neverExecutor(&executorCalls)})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := service.Preview(t.Context(), access, scope, invalidPreviewRequest())
	if err != nil {
		t.Fatal(err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil || response.GetValidation().GetValid() || compilerCalls.Load() != 0 || executorCalls.Load() != 0 {
		t.Fatalf("native site validation-only preview: %v, %v", response, err)
	}
}
