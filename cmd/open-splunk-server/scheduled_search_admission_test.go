package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

func TestAuthorizeRuntimeSearchAppUsesPointLookup(t *testing.T) {
	t.Parallel()
	var calls int
	adapter := &runtimeAppCatalog{catalog: &stubControlAppCatalog{get: func(
		_ context.Context,
		scope control.AppAccessScope,
		selector control.AppSelector,
	) (control.AppWorkspace, error) {
		calls++
		if scope.TenantID != "tenant-1" || selector.AppID != "app-main" || selector.Slug != "" {
			t.Fatalf("point lookup authority = scope %#v selector %#v", scope, selector)
		}
		return control.AppWorkspace{ID: "app-main", State: control.AppStateActive}, nil
	}}}
	if err := authorizeRuntimeSearchApp(context.Background(), adapter, "tenant-1", "app-main"); err != nil {
		t.Fatalf("authorizeRuntimeSearchApp: %v", err)
	}
	if calls != 1 {
		t.Fatalf("point lookup calls = %d, want 1", calls)
	}
}

func TestAuthorizeRuntimeSearchAppRejectsUnavailableAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		result  control.AppWorkspace
		err     error
		wantErr error
	}{
		{name: "missing", err: control.ErrNotFound, wantErr: server.ErrTrustedSearchAppUnavailable},
		{name: "archived", result: control.AppWorkspace{ID: "app-main", State: control.AppStateArchived}, wantErr: server.ErrTrustedSearchAppUnavailable},
		{name: "wrong identity", result: control.AppWorkspace{ID: "app-other", State: control.AppStateActive}, wantErr: server.ErrTrustedSearchAppUnavailable},
		{name: "storage", err: errors.New("storage failed"), wantErr: server.ErrTrustedSearchAuthorityUnavailable},
		{name: "canceled", err: context.Canceled, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter := &runtimeAppCatalog{catalog: &stubControlAppCatalog{get: func(
				context.Context,
				control.AppAccessScope,
				control.AppSelector,
			) (control.AppWorkspace, error) {
				return test.result, test.err
			}}}
			err := authorizeRuntimeSearchApp(context.Background(), adapter, "tenant-1", "app-main")
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("authorizeRuntimeSearchApp error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
