package knowledgecatalog

import (
	"net/url"
	"os"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func restoreCatalogSnapshot(t *testing.T, source *control.DB, archive string, readOnly bool) *control.DB {
	t.Helper()
	var databaseName, schema string
	if err := source.SQLDB().QueryRowContext(t.Context(), "SELECT current_database(),current_schema()").Scan(&databaseName, &schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(os.Getenv("OPEN_SPLUNK_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + databaseName
	parameters := parsed.Query()
	parameters.Set("search_path", schema)
	parsed.RawQuery = parameters.Encode()
	target := pgtest.EmptyCopy(t, parsed.String())
	if err := control.RestoreFrom(t.Context(), target, archive); err != nil {
		t.Fatal(err)
	}
	var restored *control.DB
	if readOnly {
		restored, err = control.OpenReadOnly(t.Context(), target)
	} else {
		restored, err = control.Open(t.Context(), target)
	}
	if err != nil {
		t.Fatal(err)
	}
	return restored
}
