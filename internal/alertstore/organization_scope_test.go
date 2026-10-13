package alertstore

import (
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
)

func TestNativeAlertOrganizationOwnerAndSiteAdministratorScopes(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	repository, database := openSQLRepository(t, now)
	const foreignApp = "app_000000000000000000002A"
	seedAlertApp(t, database, "tenant-1", 2, foreignApp)
	one := validCreateRecord("alert-org-one", "same name", now)
	two := validCreateRecord("alert-org-two", "same name", now)
	two.OrganizationID = "2"
	two.Definition.Application = foreignApp
	for _, record := range []alerts.CreateRecord{one, two} {
		if _, err := repository.Create(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	scope := alerts.AccessScope{OwnerID: "owner-1", OrganizationID: "1"}
	ctx, err := alerts.WithAccessScope(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	list, err := repository.List(ctx, scope.OwnerID, 64)
	if err != nil || len(list) != 1 || list[0].ID != one.ID || list[0].OrganizationID != "1" {
		t.Fatalf("organization list: %#v %v", list, err)
	}
	if _, err := repository.GetSummary(ctx, scope.OwnerID, two.ID); !errors.Is(err, alerts.ErrNotFound) {
		t.Fatalf("same user read foreign organization: %v", err)
	}
	if err := repository.DeleteIfIdle(ctx, alerts.DeleteRecord{ID: two.ID, OwnerID: scope.OwnerID, ExpectedVersion: 1}); !errors.Is(err, alerts.ErrNotFound) {
		t.Fatalf("same user mutated foreign organization: %v", err)
	}
	service := newSQLService(t, repository, now)
	admin := alerts.AccessScope{OwnerID: "99", SiteAdministrator: true}
	if _, err := service.Get(t.Context(), admin, two.ID); err != nil {
		t.Fatalf("site administrator foreign organization: %v", err)
	}
	if _, err := service.SetEnabled(t.Context(), admin, two.ID, 1, true); err != nil {
		t.Fatalf("site administrator cross-owner mutation: %v", err)
	}
}
