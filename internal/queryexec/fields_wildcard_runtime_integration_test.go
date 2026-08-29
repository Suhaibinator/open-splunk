package queryexec

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func queryIntegrationTestFieldsWildcard(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	base, indexTime time.Time,
) {
	t.Helper()
	officialWildcardInclude := queryIntegrationOfficialSPLFragment(t, "fields.explicit-wildcard-include")
	run := func(id, source string) searchjobs.ResultPage {
		t.Helper()
		job, page := queryIntegrationRunGradeThisSearchRange(
			t, ctx, executor, indexTime, id, source,
			base, base.Add(15*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("%s state = %v, failure = %#v", id, job.State, job.Failure)
		}
		return page
	}

	t.Run("open-schema inclusion filters sparse fields", func(t *testing.T) {
		page := run(
			"queryexec-fields-wildcard-include",
			`index=gradethis | `+officialWildcardInclude+` | sort event_id`,
		)
		queryIntegrationAssertColumns(t, page, []string{"event_id", "_time", "_raw", "fields"})
		if len(page.Rows) != 9 {
			t.Fatalf("rows = %d, want 9", len(page.Rows))
		}
		for rowIndex, row := range page.Rows {
			fields, ok := row.Values[3].Object()
			if !ok {
				t.Fatalf("row %d fields = %#v, want object", rowIndex, row.Values[3])
			}
			for _, field := range fields {
				if field.Name != "status" {
					t.Fatalf("row %d leaked nonmatching field %q", rowIndex, field.Name)
				}
			}
		}
	})

	t.Run("hidden fields do not affect downstream commands", func(t *testing.T) {
		page := run(
			"queryexec-fields-wildcard-hidden-filter",
			`index=gradethis | `+officialWildcardInclude+` | where logger="http" | table event_id`,
		)
		if len(page.Rows) != 0 {
			t.Fatalf("hidden logger affected downstream where: %#v", page.Rows)
		}
	})

	t.Run("mixed exact and wildcard fields keep exact presence", func(t *testing.T) {
		for _, test := range []struct {
			id, field string
		}{
			{id: "queryexec-fields-wildcard-canonical-count", field: "event_id"},
			{id: "queryexec-fields-wildcard-dynamic-count", field: "logger"},
		} {
			page := run(test.id, `index=gradethis | fields + `+test.field+`, status* | stats count(`+test.field+`) AS events`)
			queryIntegrationAssertColumns(t, page, []string{"events"})
			if len(page.Rows) != 1 {
				t.Fatalf("%s rows = %#v", test.field, page.Rows)
			}
			count, ok := page.Rows[0].Values[0].Unsigned()
			if !ok || count != 9 {
				t.Fatalf("count(%s) = %d/%t, want 9", test.field, count, ok)
			}
		}
	})

	t.Run("chained mixed projections keep exact dynamic presence", func(t *testing.T) {
		page := run(
			"queryexec-fields-wildcard-chained-exact-count",
			`index=gradethis | fields logger,status* | fields logger,error* | stats count(logger) AS events`,
		)
		queryIntegrationAssertColumns(t, page, []string{"events"})
		if len(page.Rows) != 1 {
			t.Fatalf("rows = %#v", page.Rows)
		}
		count, ok := page.Rows[0].Values[0].Unsigned()
		if !ok || count != 9 {
			t.Fatalf("count(logger) = %d/%t, want 9", count, ok)
		}
	})

	t.Run("exact exclusion preserves unrelated dynamic fields", func(t *testing.T) {
		page := run(
			"queryexec-fields-exact-exclusion-preserves-dynamic",
			`index=gradethis | fields - logger | sort event_id`,
		)
		if len(page.Rows) != 9 {
			t.Fatalf("rows = %d, want 9", len(page.Rows))
		}
		fieldsColumn := queryIntegrationColumnIndex(t, page, "fields")
		for rowIndex, row := range page.Rows {
			fields, ok := row.Values[fieldsColumn].Object()
			if !ok {
				t.Fatalf("row %d fields = %#v, want object", rowIndex, row.Values[fieldsColumn])
			}
			seenLayer := false
			for _, field := range fields {
				if field.Name == "logger" {
					t.Fatalf("row %d retained excluded logger", rowIndex)
				}
				seenLayer = seenLayer || field.Name == "layer"
			}
			if !seenLayer {
				t.Fatalf("row %d dropped unrelated dynamic layer: %#v", rowIndex, fields)
			}
		}
	})

	t.Run("broad exclusion preserves internal time and raw", func(t *testing.T) {
		page := run(
			"queryexec-fields-wildcard-broad-exclude",
			`index=gradethis | fields - * | head 1`,
		)
		if len(page.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(page.Rows))
		}
		queryIntegrationColumnIndex(t, page, "_time")
		queryIntegrationColumnIndex(t, page, "_raw")
	})

	t.Run("later wildcard cannot resurrect an exact exclusion", func(t *testing.T) {
		page := run(
			"queryexec-fields-wildcard-no-resurrection",
			`index=gradethis | fields - logger | fields + * | search logger=* | table event_id`,
		)
		if len(page.Rows) != 0 {
			t.Fatalf("excluded logger resurfaced: %#v", page.Rows)
		}
	})
}
