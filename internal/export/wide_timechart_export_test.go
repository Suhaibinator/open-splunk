package export

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestCompletedWideTimechartExportsSelectedSnapshotColumn(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	access := searchjobs.AccessScope{TenantID: "tenant-a", OwnerID: "owner-a"}
	bounds := searchjobs.TimeBucketBounds{
		Earliest: start.Format(time.RFC3339Nano),
		Latest:   start.Add(time.Hour).Format(time.RFC3339Nano),
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "_time",
		Kind: searchjobs.ValueKindTime,
	}}}
	values := []searchjobs.Value{searchjobs.TimeValue(start)}
	for index := range 1_024 {
		schema.Columns = append(schema.Columns, searchjobs.Column{
			Name: fmt.Sprintf("series_%04d", index),
			Kind: searchjobs.ValueKindUnsigned,
		})
		values = append(values, searchjobs.UnsignedValue(1))
	}
	searchManager, err := searchjobs.New(searchjobs.Config{
		Executor: reexecutionTestExecutor(func(
			_ context.Context,
			_ clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			return sink.(searchjobs.TimeBucketResultSink).AddRowWithTimeBucket(
				values,
				bounds,
			)
		}),
		Snapshotter:     integrationSnapshotter(func(context.Context) (uint64, error) { return 42, nil }),
		MaxConcurrent:   1,
		MaxRows:         10,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             func() time.Time { return now },
		NewID:           func() string { return "wide-timechart-export" },
		CursorKey:       []byte("wide-timechart-export-snapshot-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searchManager.Close() })
	rangeValue, err := searchtime.NewAbsoluteRange(start, now)
	if err != nil {
		t.Fatal(err)
	}
	created, err := searchManager.Create(ctx, searchjobs.CreateRequest{
		SPL:               "index=main | timechart span=1h count by host limit=0",
		OwnerID:           access.OwnerID,
		TenantID:          access.TenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         rangeValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		job, getErr := searchManager.GetFor(access, created.ID)
		return getErr == nil && job.State == searchjobs.StateCompleted
	}, "wide timechart completion")

	var reexecutions atomic.Int32
	source := newReexecutionTestSource(
		t,
		searchManager,
		reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
			reexecutions.Add(1)
			return searchjobs.ErrInvalidResult
		}),
		nil,
	)
	lease, err := source.AcquireResultsFor(ctx, access, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := lease.Schema(); len(got.Columns) != len(schema.Columns) ||
		!slices.Equal(got.Columns, schema.Columns) ||
		lease.Generation() == 0 || !lease.RowCountExact() || lease.RowCount() != 1 {
		t.Fatalf("resolved wide snapshot = schema %d, generation %d, rows %d", len(got.Columns), lease.Generation(), lease.RowCount())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}

	exportManager := newExportTestManager(t, source, nil)
	exportJob, err := exportManager.Create(ctx, access, CreateRequest{
		SearchJobID: created.ID,
		Format:      FormatCSV,
		Columns:     []string{"_time"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportState(t, exportManager, access, exportJob.ID, StateCompleted)
	if completed.Artifact == nil || completed.Artifact.RowCount != 1 ||
		!slices.Equal(completed.Columns, []string{"_time"}) {
		t.Fatalf("completed wide export = %#v", completed)
	}
	artifact, err := os.Open(filepath.Join(exportManager.artifactDir, completed.Artifact.FileName))
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(artifact).ReadAll()
	closeErr := artifact.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read wide export = (%v, close %v)", err, closeErr)
	}
	if len(records) != 2 || !slices.Equal(records[0], []string{"_time"}) ||
		len(records[1]) != 1 || records[1][0] != start.Format(time.RFC3339Nano) {
		t.Fatalf("wide export records = %#v", records)
	}
	if reexecutions.Load() != 0 {
		t.Fatalf("wide timechart export re-executed %d times", reexecutions.Load())
	}
}

func TestUntrustedWideSourceCannotUseSubsetSelection(t *testing.T) {
	t.Parallel()

	columns := make([]searchjobs.Column, maximumColumns+1)
	for index := range columns {
		columns[index] = searchjobs.Column{
			Name: fmt.Sprintf("field_%04d", index),
			Kind: searchjobs.ValueKindString,
		}
	}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"wide": {schema: searchjobs.Schema{Columns: columns}},
	}}
	manager := newExportTestManager(t, source, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "wide",
		Format:      FormatCSV,
		Columns:     []string{columns[0].Name},
	}); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("Create(untrusted wide subset) = %v, want ErrInvalidColumns", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "untrusted wide lease release")
}

func TestSchemaMatchesWideLimitZeroTimechartAndRejectsMalformedSource(t *testing.T) {
	t.Parallel()

	columns := make([]searchjobs.Column, 1, maximumColumns+1)
	columns[0] = searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}
	for index := range maximumColumns {
		columns = append(columns, searchjobs.Column{
			Name: fmt.Sprintf("series_%04d", index),
			Kind: searchjobs.ValueKindUnsigned,
		})
	}
	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"_time"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:          clickhouse.TimechartModeRuntimeWide,
			SeriesLimit:   0,
			MaxSeries:     0,
			MaxLabelBytes: clickhouse.MaximumTimechartLabelBytes,
		},
	}
	schema := searchjobs.Schema{Columns: columns}
	if !schemaMatchesCompiledQuery(schema, compiled) {
		t.Fatal("valid wide limit=0 timechart schema was rejected")
	}
	malformed := searchjobs.Schema{Columns: slices.Clone(columns)}
	malformed.Columns[len(malformed.Columns)-1].Name = malformed.Columns[1].Name
	if schemaMatchesCompiledQuery(malformed, compiled) {
		t.Fatal("wide timechart schema with duplicate runtime labels was accepted")
	}
}
