package export

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type truncatedPatternOrdinarySource struct {
	store *searchartifacts.Store
	calls atomic.Int32
}

func (source *truncatedPatternOrdinarySource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	source.calls.Add(1)
	return source.store.Acquire(ctx, access, id)
}

func TestTruncatedPatternExportsPreserveExactDurableRelation(t *testing.T) {
	const retainedRows = 10000
	ctx := t.Context()
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: database.SQLDB(), Directory: filepath.Join(directory, "retained"), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	timeRange, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	job := searchjobs.Job{ID: "truncated-pattern-source", Version: 1, TenantID: testAccess.TenantID, OwnerID: testAccess.OwnerID, SPL: "index=main | table _raw sequence", TimeRange: timeRange.Intent(), State: searchjobs.StateQueued, CreatedAt: now}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}, {Name: "sequence", Kind: searchjobs.ValueKindUnsigned}}}
	rows := make([]searchjobs.ResultRow, retainedRows)
	for index := range rows {
		rows[index] = searchjobs.ResultRow{Ordinal: uint64(index), Values: []searchjobs.Value{searchjobs.StringValue(fmt.Sprintf("request %d", index)), searchjobs.UnsignedValue(uint64(index))}}
	}
	job.State, job.Version = searchjobs.StateCompleted, 2
	job.StartedAt, job.FinishedAt, job.ExpiresAt = now, now, now.Add(time.Hour)
	job.Schema, job.RowCount, job.ResultsTruncated = &schema, retainedRows, true
	if err := store.Finalize(ctx, job); err != nil {
		t.Fatal(err)
	}
	// The durable source retains only its first 10,000 rows after an original
	// search exceeded the cap. Truncation remains provenance of that exact set.
	retained := &exportTestLease{schema: schema, rows: rows, rowCount: retainedRows, truncated: true, closedSignal: make(chan struct{})}
	if _, err := store.PersistResults(ctx, testAccess, job.ID, retained); err != nil {
		t.Fatal(err)
	}
	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	service, err := patterns.New(patterns.Config{Source: store, CursorKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	catalog, err := service.List(ctx, testAccess, patterns.ListRequest{SearchJobID: job.ID, Generation: 1, Sensitivity: patterns.Balanced, IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	if !catalog.RetainedTruncated || catalog.SnapshotComplete || catalog.RetainedEventCount != retainedRows || len(catalog.Patterns) != 1 || catalog.Patterns[0].EventCount != retainedRows {
		t.Fatalf("retained pattern coverage = %+v", catalog)
	}
	patternID := catalog.Patterns[0].ID
	catalog.Close()
	ordinary := &truncatedPatternOrdinarySource{store: store}
	manager := newExportTestManager(t, ordinary, func(config *Config) { config.PatternSource = service })
	if _, err := manager.Create(ctx, testAccess, CreateRequest{SearchJobID: job.ID, Format: FormatCSV}); !errors.Is(err, ErrSourceTruncated) {
		t.Fatalf("ordinary truncated export = %v", err)
	}
	for _, kind := range []SourceKind{SourcePatternSummary, SourcePatternMembers} {
		t.Run(string(kind), func(t *testing.T) {
			source := patterns.ExportRequest{SearchJobID: job.ID, SnapshotRef: "retained-snapshot-reference", Generation: 1, Sensitivity: patterns.Balanced}
			var lease searchjobs.ResultLease
			var acquireErr error
			if kind == SourcePatternMembers {
				source.PatternID = patternID
				lease, acquireErr = service.AcquirePatternMembers(ctx, testAccess, source)
			} else {
				lease, acquireErr = service.AcquirePatternSummary(ctx, testAccess, source)
			}
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			if !lease.ResultsTruncated() {
				t.Error("pattern lease lost source truncation provenance")
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			accepted, err := manager.Create(ctx, testAccess, CreateRequest{SearchJobID: job.ID, SourceKind: kind, Pattern: &source, Format: FormatCSV})
			if err != nil {
				t.Fatal(err)
			}
			completed := waitExportState(t, manager, testAccess, accepted.ID, StateCompleted, StateFailed)
			if completed.State != StateCompleted || completed.Pattern == nil || *completed.Pattern != source {
				t.Fatalf("pattern export outcome = %+v", completed)
			}
			grant, err := manager.CreateDownloadGrant(ctx, testAccess, completed.ID)
			if err != nil {
				t.Fatal(err)
			}
			download, err := manager.RedeemDownload(ctx, grant.Token)
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(download)
			closeErr := download.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("download = %v/%v", readErr, closeErr)
			}
			records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if kind == SourcePatternSummary {
				if len(records) != 2 || len(records[0]) != 3 || records[0][0] != "pattern" || records[0][1] != "count" || records[0][2] != "percent" || len(records[1]) != 3 || records[1][0] != "request <int>" || records[1][1] != "10000" || records[1][2] != "100" {
					t.Fatalf("summary CSV = %v", records)
				}
			} else {
				if len(records) != retainedRows+1 || len(records[0]) != 2 || records[0][0] != "_raw" || records[0][1] != "sequence" {
					t.Fatalf("member CSV shape = %d rows, header %v", len(records), records[:min(1, len(records))])
				}
				for index, record := range records[1:] {
					if len(record) != 2 || record[0] != fmt.Sprintf("request %d", index) || record[1] != strconv.Itoa(index) {
						t.Fatalf("member row %d = %v", index, record)
					}
				}
			}
		})
	}
	if ordinary.calls.Load() != 1 {
		t.Fatalf("Pattern export entered ordinary source: %d calls", ordinary.calls.Load())
	}
}
