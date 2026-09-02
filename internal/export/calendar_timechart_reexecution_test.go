package export

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestCalendarTimechartReexecutionExportPreservesIrregularUTCBoundaries(t *testing.T) {
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
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | timechart span=1d count`
	searches.job.TimeRange = resolvedRange.Intent()
	searches.job.Earliest = resolvedRange.Earliest()
	searches.job.Latest = resolvedRange.Latest()
	searches.resolvedRange = &resolvedRange
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	wantTimes := []time.Time{
		time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
	}
	wantCounts := []uint64{2, 0, 3}
	compiledQueries := make(chan clickhouse.CompiledQuery, 1)
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		compiledQueries <- query
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for index, boundary := range wantTimes {
			if err := sink.AddRow([]searchjobs.Value{
				searchjobs.TimeValue(boundary),
				searchjobs.UnsignedValue(wantCounts[index]),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(context.Background(), access, CreateRequest{
		SearchJobID: searches.job.ID,
		Format:      FormatJSONLines,
	})
	if err != nil {
		t.Fatalf("Create calendar timechart export: %v", err)
	}
	completed := waitExportState(t, manager, access, created.ID, StateCompleted)
	if completed.Artifact == nil || completed.Artifact.RowCount != uint64(len(wantTimes)) ||
		!slices.Equal(completed.Columns, []string{"_time", "count"}) {
		t.Fatalf("completed calendar export = %#v", completed)
	}

	compiled := <-compiledQueries
	if !compiled.HasValidExecutionSeal() || compiled.Timechart == nil ||
		compiled.Timechart.Mode != clickhouse.TimechartModeFixedCount ||
		!compiled.Timechart.Calendar || compiled.Timechart.Span != 0 ||
		compiled.Timechart.BucketCount != uint64(len(wantTimes)) ||
		!compiled.Timechart.FirstBucket.Equal(wantTimes[0]) {
		t.Fatalf("re-executed calendar contract = %#v", compiled.Timechart)
	}
	if !slices.Equal(compiled.OutputFields, []string{"_time", "count"}) {
		t.Fatalf("re-executed public fields = %v", compiled.OutputFields)
	}
	for _, field := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("re-executed public fields leaked private name %q", field)
		}
	}
	timezoneBound := false
	for _, argument := range compiled.Args {
		if value, ok := argument.(string); ok && value == searchTimezone {
			timezoneBound = true
			break
		}
	}
	if !timezoneBound {
		t.Fatalf("re-executed calendar query did not bind search timezone; args = %#v", compiled.Args)
	}

	contents, err := os.ReadFile(filepath.Join(manager.artifactDir, completed.Artifact.FileName))
	if err != nil {
		t.Fatalf("read calendar export artifact: %v", err)
	}
	want := "{\"_time\":\"2026-03-07T05:00:00Z\",\"count\":2}\n" +
		"{\"_time\":\"2026-03-08T05:00:00Z\",\"count\":0}\n" +
		"{\"_time\":\"2026-03-09T04:00:00Z\",\"count\":3}\n"
	if string(contents) != want {
		t.Fatalf("calendar JSONL = %q, want %q", contents, want)
	}
	if got := wantTimes[1].Sub(wantTimes[0]); got != 24*time.Hour {
		t.Fatalf("pre-DST UTC gap = %s, want 24h", got)
	}
	if got := wantTimes[2].Sub(wantTimes[1]); got != 23*time.Hour {
		t.Fatalf("spring-forward UTC gap = %s, want 23h", got)
	}
}
