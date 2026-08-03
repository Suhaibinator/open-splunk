package queryexec

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// queryIntegrationTestStreamStatsTransport runs the grouped nullable result
// through both Executor and Manager. The store integration owns the broader
// ordering and resource-bound matrix; this pins the public schema and the
// current=false zero value at the transport boundary.
func queryIntegrationTestStreamStatsTransport(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
) {
	t.Helper()

	job, page := queryIntegrationRunSearch(
		t,
		ctx,
		executor,
		indexTime,
		"queryexec-streamstats-transport",
		`index=main source="source"`+
			` | streamstats current=false count AS preceding BY path`+
			` | table event_id preceding`,
	)
	if job.State != searchjobs.StateCompleted {
		t.Fatalf(
			"streamstats transport state = %v, failure=%#v",
			job.State,
			job.Failure,
		)
	}
	wantColumns := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "preceding", Kind: searchjobs.ValueKindUnsigned, Nullable: true},
	}
	if len(page.Schema.Columns) != len(wantColumns) {
		t.Fatalf(
			"streamstats transport schema = %#v, want %#v",
			page.Schema.Columns,
			wantColumns,
		)
	}
	for index := range wantColumns {
		if page.Schema.Columns[index] != wantColumns[index] {
			t.Fatalf(
				"streamstats transport column %d = %#v, want %#v",
				index,
				page.Schema.Columns[index],
				wantColumns[index],
			)
		}
	}
	if len(page.Rows) != 1 {
		t.Fatalf("streamstats transport rows = %#v, want one row", page.Rows)
	}
	if eventID, ok := page.Rows[0].Values[0].String(); !ok || eventID != "queryexec-event" {
		t.Fatalf("streamstats transport event_id = %q, string=%v", eventID, ok)
	}
	if preceding, ok := page.Rows[0].Values[1].Unsigned(); !ok || preceding != 0 {
		t.Fatalf("streamstats transport preceding = %d, unsigned=%v", preceding, ok)
	}
}
