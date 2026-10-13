package alertstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
)

func TestNativeSiteAlertPagesIncludeEveryOwnerAndExactFilteredTotal(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	repository, database := openSQLRepository(t, now)
	service := newSQLService(t, repository, now)
	seedAlertApp(t, database, "tenant-1", 2, testAlertSecondAppID)
	for n := range 143 {
		record := validCreateRecord(fmt.Sprintf("site-alert-%03d", n), fmt.Sprintf("alarm %03d", n), now)
		record.OwnerID = fmt.Sprintf("owner-%d", n/60+1)
		record.Definition.Description = "matching description"
		if n >= 140 {
			record.OrganizationID = "2"
			record.Definition.Application = testAlertSecondAppID
		}
		if _, err := repository.Create(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	scope := alerts.AccessScope{OwnerID: "site", OrganizationID: "1", SiteAdministrator: true}
	request := alerts.ListPageRequest{Limit: 31, IncludeTotal: true, Text: "DESCRIPTION"}
	seen := map[string]bool{}
	for {
		page, err := service.ListPage(t.Context(), scope, request)
		if err != nil || page.TotalSize == nil || *page.TotalSize != 140 || len(page.Alerts) > request.Limit {
			t.Fatalf("native site page: %#v %v", page, err)
		}
		for _, alert := range page.Alerts {
			if seen[alert.ID] || alert.OrganizationID != "1" {
				t.Fatalf("duplicate or foreign alert: %#v", alert)
			}
			seen[alert.ID] = true
		}
		if page.NextAlertID == "" {
			break
		}
		request.AfterAlertID = page.NextAlertID
	}
	if len(seen) != 140 {
		t.Fatalf("site catalog truncated at owner cap: %d", len(seen))
	}
	global := scope
	global.OrganizationID = ""
	page, err := service.ListPage(t.Context(), global, alerts.ListPageRequest{Limit: 10, IncludeTotal: true})
	if err != nil || page.TotalSize == nil || *page.TotalSize != 143 {
		t.Fatalf("site global count: %#v %v", page, err)
	}
	page, err = service.ListPage(t.Context(), global, alerts.ListPageRequest{Limit: 10, Application: testAlertSecondAppID, Text: "description", IncludeTotal: true})
	if err != nil || len(page.Alerts) != 3 || page.TotalSize == nil || *page.TotalSize != 3 {
		t.Fatalf("site filters/count: %#v %v", page, err)
	}
	if _, err := service.ListPage(t.Context(), alerts.AccessScope{OwnerID: "owner-1", OrganizationID: "1"}, alerts.ListPageRequest{Limit: 10}); err == nil {
		t.Fatal("ordinary user entered site-only catalog pagination")
	}
	if _, err := service.ListPage(t.Context(), scope, alerts.ListPageRequest{Limit: 10, AfterAlertID: "site-alert-140"}); err == nil {
		t.Fatal("cursor boundary crossed organization")
	}
}
