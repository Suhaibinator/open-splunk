//go:build !windows

package integration_test

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

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
