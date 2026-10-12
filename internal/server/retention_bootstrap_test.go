package server

import (
	"context"
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestBootstrapRetentionUsageBindsSelectedOrganization(t *testing.T) {
	ctx := authorization.WithScope(context.Background(), authorization.Scope{Subject: authorization.Subject{UserID: 42, UserType: 1}, OrganizationID: 7, PhysicalTenant: "instance"})
	handler := &apiHandler{retentionUsage: func(_ context.Context, scope searchjobs.AccessScope) (searchartifacts.RetentionUsage, error) {
		if scope.OwnerID != "42" || scope.OrganizationID != "7" || scope.TenantID != "instance" || scope.SiteAdministrator {
			t.Fatalf("wrong quota scope: %#v", scope)
		}
		// A budget reduced by an administrator can legitimately be below current use.
		return searchartifacts.RetentionUsage{OrganizationBytes: 4096, GlobalBytes: 8192, OrganizationLimit: 1024, GlobalLimit: 16384}, nil
	}}
	response := &opensplunk.GetSystemBootstrapResponse{}
	if err := handler.populateRetentionUsage(ctx, response); err != nil {
		t.Fatal(err)
	}
	status := response.GetRetainedStorageUsage()
	if status == nil || status.GetOrganizationId() != "7" || status.GetOrganizationUsedBytes() != 4096 || status.GetGlobalUsedBytes() != 8192 || status.GetOrganizationLimitBytes() != 1024 || status.GetGlobalLimitBytes() != 16384 {
		t.Fatalf("quota status=%#v", status)
	}
}
func TestBootstrapRetentionUsageRequiresSelectionAndCurrentMembership(t *testing.T) {
	calls := 0
	handler := &apiHandler{retentionUsage: func(context.Context, searchjobs.AccessScope) (searchartifacts.RetentionUsage, error) {
		calls++
		return searchartifacts.RetentionUsage{}, authorization.ErrForbidden
	}}
	response := &opensplunk.GetSystemBootstrapResponse{}
	if err := handler.populateRetentionUsage(context.Background(), response); err != nil || calls != 0 || response.GetRetainedStorageUsage() != nil {
		t.Fatalf("unselected quota: %v calls%d", err, calls)
	}
	ctx := authorization.WithScope(context.Background(), authorization.Scope{Subject: authorization.Subject{UserID: 42, UserType: 1}, OrganizationID: 7, PhysicalTenant: "instance"})
	if err := handler.populateRetentionUsage(ctx, response); !errors.Is(err, authorization.ErrForbidden) || response.GetRetainedStorageUsage() != nil {
		t.Fatalf("revoked membership exposed usage: %v %#v", err, response)
	}
}
