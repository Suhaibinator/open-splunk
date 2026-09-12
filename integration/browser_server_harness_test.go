//go:build !windows

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const browserSearchAppID = "app_000000000000000000001Q"

var browserSearchControlCursorKey = []byte("browser-search-control-fixture-cursor-key")

type browserSearchControlPlane struct {
	appCatalog server.AppCatalog
	history    *searchhistory.Store
	journal    searchjobs.JobJournal
}

type browserControlAppCatalog struct {
	catalog *control.AppCatalog
}

func (catalog browserControlAppCatalog) ListActiveApps(
	ctx context.Context,
	tenantID string,
	maximum uint32,
) (server.AppCatalogResult, error) {
	result, err := catalog.catalog.ListApps(
		ctx,
		control.AppAccessScope{TenantID: tenantID},
		control.AppListRequest{
			PageSize:     maximum,
			StateFilters: []control.AppState{control.AppStateActive},
			SortBy:       control.AppSortByDisplayName,
			Direction:    control.AppSortAscending,
		},
	)
	if err != nil {
		return server.AppCatalogResult{}, err
	}
	apps := make([]server.AppCatalogSummary, len(result.Apps))
	for index, app := range result.Apps {
		apps[index] = server.AppCatalogSummary{
			AppID:             app.ID,
			Slug:              app.Definition.Slug,
			DisplayName:       app.Definition.DisplayName,
			DefaultIndexNames: append([]string(nil), app.Definition.DefaultIndexes...),
		}
	}
	return server.AppCatalogResult{Apps: apps, Complete: result.NextPageToken == nil}, nil
}

func newBrowserSearchControlPlane(
	t *testing.T,
	ctx context.Context,
	database *control.DB,
	tenantID, indexName string,
	anchor time.Time,
) browserSearchControlPlane {
	t.Helper()
	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: browserSearchControlCursorKey,
		Clock:     func() time.Time { return anchor },
		IDGenerator: func() (string, error) {
			return browserSearchAppID, nil
		},
	})
	if err != nil {
		t.Fatalf("create browser search app catalog: %v", err)
	}
	if _, err := appCatalog.CreateApp(
		ctx,
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{
			Slug:           "browser-search",
			DisplayName:    "Browser search",
			DefaultIndexes: []string{indexName},
		},
	); err != nil {
		t.Fatalf("create browser search app: %v", err)
	}
	attempts, err := searchaudit.New(database, searchaudit.Options{
		CursorKey: browserSearchControlCursorKey,
	})
	if err != nil {
		t.Fatalf("create browser search-attempt audit store: %v", err)
	}
	history, err := searchhistory.New(database, searchhistory.Options{
		Clock:                     func() time.Time { return anchor },
		CursorKey:                 browserSearchControlCursorKey,
		AuditAppender:             attempts,
		RequireSearchAttemptAudit: true,
	})
	if err != nil {
		t.Fatalf("create browser search history: %v", err)
	}
	historyJournal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		t.Fatalf("create browser search-history journal: %v", err)
	}
	artifacts, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB:              database.SQLDB(),
		Directory:       filepath.Join(t.TempDir(), "search-artifacts"),
		Clock:           func() time.Time { return anchor },
		CleanupInterval: -1,
	})
	if err != nil {
		t.Fatalf("create browser search-artifact store: %v", err)
	}
	t.Cleanup(func() {
		if err := artifacts.Close(); err != nil {
			t.Errorf("close browser search-artifact store: %v", err)
		}
	})
	return browserSearchControlPlane{
		appCatalog: browserControlAppCatalog{catalog: appCatalog},
		history:    history,
		journal:    searchjobs.NewCompositeJournal(artifacts, historyJournal),
	}
}

// browserSearchOnlyCatalog hides optional IndexAdministration methods so
// NewHandler cannot infer and expose administrative routes in search fixtures.
func browserSearchOnlyCatalog(catalog server.IndexCatalog) server.IndexCatalog {
	return browserSearchOnlyIndexCatalog{IndexCatalog: catalog}
}

type browserSearchOnlyIndexCatalog struct {
	server.IndexCatalog
}

func TestBrowserSearchOnlyCatalogHidesIndexAdministration(t *testing.T) {
	t.Parallel()

	var database *control.DB
	catalog := browserSearchOnlyCatalog(database)
	if _, exposesAdministration := catalog.(server.IndexAdministration); exposesAdministration {
		t.Fatal("search-only catalog exposes index administration")
	}
}
