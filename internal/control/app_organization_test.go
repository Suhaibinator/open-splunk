package control

import (
	"context"
	"errors"
	"testing"
)

func TestAppOrganizationIsolationSharesPhysicalTenant(t *testing.T) {
	db := openTestDB(t)
	catalog := newTestAppCatalog(t, db)
	ctx := context.Background()
	first := AppAccessScope{TenantID: "physical", OrganizationID: 1}
	second := AppAccessScope{TenantID: "physical", OrganizationID: 2}
	a, err := catalog.CreateApp(ctx, first, validAppDefinition("shared"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := catalog.CreateApp(ctx, second, validAppDefinition("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID || a.OrganizationID != 1 || b.OrganizationID != 2 {
		t.Fatalf("ownership %#v %#v", a, b)
	}
	for _, selector := range []AppSelector{{AppID: a.ID}, {Slug: "shared"}} {
		got, err := catalog.GetApp(ctx, second, selector)
		if selector.AppID != "" {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("foreign ID: %v", err)
			}
		} else if err != nil || got.ID != b.ID {
			t.Fatalf("same slug scoped get: %#v %v", got, err)
		}
	}
	if _, err := catalog.UpdateApp(ctx, second, AppSelector{AppID: a.ID}, a.Version, a.Definition); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign update: %v", err)
	}
	ids, err := catalog.ListAppIdentities(ctx, second, 10)
	if err != nil || len(ids.AppIDs) != 1 || ids.AppIDs[0] != b.ID {
		t.Fatalf("identities %#v %v", ids, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, "UPDATE app_workspaces SET organization_id=2 WHERE app_id=$1", a.ID); err == nil {
		t.Fatal("immutable app org accepted update")
	}
	if _, err := catalog.GetApp(ctx, AppAccessScope{TenantID: "physical"}, AppSelector{AppID: a.ID}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing org accepted: %v", err)
	}
}
