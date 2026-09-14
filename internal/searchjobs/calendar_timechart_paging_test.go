package searchjobs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestCalendarTimechartPublicPagingPreservesIrregularUTCBoundaries(t *testing.T) {
	t.Parallel()

	searchTimezone := "America/New_York"
	resolvedRange, err := searchtime.Resolve(
		"2026-03-07T05:00:00Z",
		"2026-03-10T04:00:00Z",
		&searchTimezone,
		time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantSchema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "count", Kind: ValueKindUnsigned},
	}}
	wantTimes := []time.Time{
		time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
	}
	wantCounts := []uint64{2, 0, 3}
	executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.HasValidExecutionSeal() || query.Timechart == nil {
			return fmt.Errorf("calendar timechart execution contract is missing or unsealed")
		}
		if query.Timechart.Mode != clickhouse.TimechartModeFixedCount ||
			!query.Timechart.Calendar || query.Timechart.Span != 0 ||
			query.Timechart.BucketCount != uint64(len(wantTimes)) ||
			!query.Timechart.FirstBucket.Equal(wantTimes[0]) {
			return fmt.Errorf("calendar timechart execution contract = %#v", query.Timechart)
		}
		if !slices.Equal(query.OutputFields, []string{"_time", "count"}) {
			return fmt.Errorf("calendar timechart public fields = %v", query.OutputFields)
		}
		for _, field := range query.OutputFields {
			if strings.HasPrefix(strings.ToLower(field), "__os_") {
				return fmt.Errorf("calendar timechart leaked private field %q", field)
			}
		}
		if err := sink.SetSchema(wantSchema); err != nil {
			return err
		}
		for index, boundary := range wantTimes {
			if err := sink.AddRow([]Value{
				TimeValue(boundary),
				UnsignedValue(wantCounts[index]),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		DefaultPageSize: 2,
		MaxPageSize:     2,
		CleanupInterval: -1,
		NewID:           sequenceIDs("calendar-timechart-paging"),
	})
	request := validRequest()
	request.SPL = `index=main | timechart span=1d count`
	request.TimeRange = resolvedRange
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create calendar timechart job: %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.ResultsTruncated || completed.RowCount != uint64(len(wantTimes)) ||
		completed.Schema == nil || !slices.Equal(completed.Schema.Columns, wantSchema.Columns) {
		t.Fatalf("completed calendar results = %#v", completed)
	}

	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	var (
		cursor    string
		gotRows   []ResultRow
		pageCount int
	)
	for pageIndex := 0; ; pageIndex++ {
		page, pageErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 2, Cursor: cursor})
		if pageErr != nil {
			t.Fatalf("ResultsFor page %d: %v", pageIndex, pageErr)
		}
		if !slices.Equal(page.Schema.Columns, wantSchema.Columns) {
			t.Fatalf("page %d schema = %#v, want %#v", pageIndex, page.Schema, wantSchema)
		}
		if page.TotalRows != uint64(len(wantTimes)) {
			t.Fatalf("page %d total rows = %d, want %d", pageIndex, page.TotalRows, len(wantTimes))
		}
		gotRows = append(gotRows, page.Rows...)
		pageCount++
		if page.Complete {
			if page.NextCursor != "" {
				t.Fatalf("complete page retained cursor %q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			t.Fatalf("incomplete page %d returned invalid cursor %q", pageIndex, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if pageCount != 2 || len(gotRows) != len(wantTimes) {
		t.Fatalf("paging returned %d pages and %d rows, want 2/%d", pageCount, len(gotRows), len(wantTimes))
	}
	gotTimes := make([]time.Time, len(gotRows))
	for index, row := range gotRows {
		if row.Ordinal != uint64(index) || len(row.Values) != 2 {
			t.Fatalf("paged row %d = %#v", index, row)
		}
		boundary, timeOK := row.Values[0].Time()
		count, countOK := row.Values[1].Unsigned()
		if !timeOK || !boundary.Equal(wantTimes[index]) || !countOK || count != wantCounts[index] {
			t.Fatalf("paged row %d values = %#v, want (%s, %d)", index, row.Values, wantTimes[index], wantCounts[index])
		}
		gotTimes[index] = boundary
	}
	if got := gotTimes[1].Sub(gotTimes[0]); got != 24*time.Hour {
		t.Fatalf("pre-DST UTC gap = %s, want 24h", got)
	}
	if got := gotTimes[2].Sub(gotTimes[1]); got != 23*time.Hour {
		t.Fatalf("spring-forward UTC gap = %s, want 23h", got)
	}
}
