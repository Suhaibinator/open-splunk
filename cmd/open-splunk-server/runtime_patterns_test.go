package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/exportjournal"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type runtimePatternFixtureLease struct {
	schema searchjobs.Schema
	rows   []searchjobs.ResultRow
	offset int
}

func (lease *runtimePatternFixtureLease) Schema() searchjobs.Schema { return lease.schema }
func (lease *runtimePatternFixtureLease) RowCount() uint64          { return uint64(len(lease.rows)) }
func (*runtimePatternFixtureLease) RowCountExact() bool             { return true }
func (*runtimePatternFixtureLease) ResultsTruncated() bool          { return false }
func (*runtimePatternFixtureLease) Generation() uint64              { return 17 }
func (*runtimePatternFixtureLease) Close() error                    { return nil }
func (lease *runtimePatternFixtureLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.offset == len(lease.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := lease.rows[lease.offset]
	lease.offset++
	return row, true, nil
}

func TestRuntimePatternsRetainsIdentityAndTypedMembersAcrossServiceRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	db, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: db.SQLDB(), Directory: filepath.Join(directory, "artifacts"), Clock: func() time.Time { return now }, CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	rangeValue, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	job := searchjobs.Job{ID: "retained-runtime", Version: 1, TenantID: "tenant", OwnerID: "owner", SPL: "index=main", TimeRange: rangeValue.Intent(), State: searchjobs.StateQueued, CreatedAt: now}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}, {Name: "large", Kind: searchjobs.ValueKindUnsigned}}}
	job.Version = 2
	job.State = searchjobs.StateCompleted
	job.StartedAt = now
	job.FinishedAt = now
	job.ExpiresAt = now.Add(time.Hour)
	job.Schema = &schema
	job.RowCount = 3
	if err := store.Finalize(ctx, job); err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: job.TenantID, OwnerID: job.OwnerID}
	if _, err := store.PersistResults(ctx, access, job.ID, &runtimePatternFixtureLease{schema: schema, rows: []searchjobs.ResultRow{
		{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("request 1"), searchjobs.UnsignedValue(math.MaxUint64)}},
		{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("request 2"), searchjobs.UnsignedValue(9007199254740993)}},
		{Ordinal: 2, Values: []searchjobs.Value{searchjobs.StringValue("failure 3"), searchjobs.UnsignedValue(3)}},
	}}); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "server.key")
	first, err := newRuntimePatternService(ctx, db, keyPath, store)
	if err != nil {
		t.Fatal(err)
	}
	page, err := first.List(ctx, access, patterns.ListRequest{SearchJobID: job.ID, Generation: 17, Sensitivity: patterns.Balanced, PageSize: 1, IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Patterns) != 1 || page.Patterns[0].EventCount != 2 || page.NextPageToken == "" {
		t.Fatalf("first groups = %+v", page)
	}
	patternID, cursor := page.Patterns[0].ID, page.NextPageToken
	page.Close()
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := newRuntimePatternService(ctx, db, keyPath, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	continued, err := second.List(ctx, access, patterns.ListRequest{SearchJobID: job.ID, Generation: 17, Sensitivity: patterns.Balanced, PageSize: 1, PageToken: cursor, IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(continued.Patterns) != 1 || continued.Patterns[0].EventCount != 1 || continued.NextPageToken != "" {
		t.Fatalf("continued groups = %+v", continued)
	}
	continued.Close()
	members, err := second.Members(ctx, access, patterns.MemberRequest{SearchJobID: job.ID, Generation: 17, Sensitivity: patterns.Balanced, PatternID: patternID, Columns: []string{"large"}, IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer members.Close()
	if len(members.Rows) != 2 || members.Rows[0].Ordinal != 0 || members.Rows[1].Ordinal != 1 {
		t.Fatalf("exact member rows = %+v", members.Rows)
	}
	if value, ok := members.Rows[0].Values[0].Unsigned(); !ok || value != math.MaxUint64 {
		t.Fatalf("exact integer = %d/%t", value, ok)
	}
	_, err = second.Members(ctx, searchjobs.AccessScope{TenantID: "tenant", OwnerID: "other"}, patterns.MemberRequest{SearchJobID: job.ID, Generation: 17, Sensitivity: patterns.Balanced, PatternID: patternID})
	if !errors.Is(err, searchartifacts.ErrNotFound) {
		t.Fatalf("cross-owner source = %v", err)
	}
	members.Close()
	verifyRuntimePatternExportRestart(t, db, directory, second, access, patterns.ExportRequest{
		SearchJobID: job.ID, SnapshotRef: "accepted-public-reference", Generation: 17,
		Sensitivity: patterns.Balanced, PatternID: patternID,
	})
}

type runtimeNoOrdinaryExport struct{ calls int }

func (source *runtimeNoOrdinaryExport) AcquireResultsFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ResultLease, error) {
	source.calls++
	return nil, searchjobs.ErrResultsUnavailable
}

func verifyRuntimePatternExportRestart(t *testing.T, db *control.DB, directory string, service *patterns.Service, access searchjobs.AccessScope, source patterns.ExportRequest) {
	t.Helper()
	ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: access.OwnerID, Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	events, err := audit.NewStore(db, audit.StoreOptions{CursorKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := exportjournal.New(db.GORMDB(), events)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := &runtimeNoOrdinaryExport{}
	config := exportjobs.Config{Source: ordinary, Journal: journal, PatternSource: service, ArtifactDir: filepath.Join(directory, "exports")}
	manager, err := exportjobs.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	intent, err := requestidempotency.NewIntent(access.TenantID, "browser", access.OwnerID, requestidempotency.RouteCreateExportJob, "runtime-pattern-export-action", &opensplunk.CreateExportJobRequest{Definition: &opensplunk.ExportDefinition{SearchJobId: source.SearchJobID}})
	if err != nil {
		t.Fatal(err)
	}
	accepted, replayed, err := manager.CreateIdempotent(ctx, access, exportjobs.CreateRequest{SearchJobID: source.SearchJobID, SourceKind: exportjobs.SourcePatternMembers, Pattern: &source, Format: exportjobs.FormatCSV, Columns: []string{"large"}}, intent)
	if err != nil || replayed {
		t.Fatalf("first durable admission = %v, replayed %t", err, replayed)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, err := manager.Get(ctx, access, accepted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == exportjobs.StateCompleted {
			break
		}
		if current.State == exportjobs.StateFailed || time.Now().After(deadline) {
			t.Fatalf("export did not complete: %+v", current)
		}
		time.Sleep(time.Millisecond)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	config.PatternSource = nil // Restoring a receipt must not reopen or rebuild its source.
	reopened, err := exportjobs.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	receipt, found, err := reopened.ReplayIdempotent(ctx, access, intent)
	if err != nil || !found || receipt.ID != accepted.ID || receipt.Pattern == nil || *receipt.Pattern != source {
		t.Fatalf("restored receipt = %+v, found %t, error %v", receipt, found, err)
	}
	grant, err := reopened.CreateDownloadGrant(ctx, access, receipt.ID)
	if err != nil {
		t.Fatal(err)
	}
	download, err := reopened.RedeemDownload(ctx, grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(download)
	closeErr := download.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("download = %v/%v", readErr, closeErr)
	}
	if !strings.Contains(string(data), "18446744073709551615") || !strings.Contains(string(data), "9007199254740993") || ordinary.calls != 0 {
		t.Fatalf("exact persisted export changed: %q, ordinary calls %d", data, ordinary.calls)
	}
}
