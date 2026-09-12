package patterns

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestDurabilityReviewRealArtifactRestartPreservesGroupsAndCursors(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.sqlite")
	artifactPath := filepath.Join(directory, "artifacts")
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	key := []byte("patterns-durable-review-key-32-bytes")
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "sequence", Kind: searchjobs.ValueKindUnsigned},
	}}
	raws := []searchjobs.Value{
		searchjobs.StringValue("request 1"), searchjobs.StringValue("request 2"), searchjobs.NullValue(),
		searchjobs.StringValue("other 3"), searchjobs.StringValue("request 4"), searchjobs.MissingValue(),
	}
	rows := make([]searchjobs.ResultRow, len(raws))
	for index, raw := range raws {
		rows[index] = searchjobs.ResultRow{Ordinal: uint64(index), Values: []searchjobs.Value{raw, searchjobs.UnsignedValue(^uint64(0) - uint64(index))}}
	}
	open := func() (*control.DB, *searchartifacts.Store, *Service) {
		database, err := control.Open(ctx, databasePath)
		if err != nil {
			t.Fatal(err)
		}
		store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: database.SQLDB(), Directory: artifactPath, Clock: func() time.Time { return now }, CleanupInterval: -1})
		if err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
		service, err := New(Config{Source: store, CursorKey: key})
		if err != nil {
			_ = store.Close()
			_ = database.Close()
			t.Fatal(err)
		}
		return database, store, service
	}
	database, store, service := open()
	defer func() {
		_ = service.Close(context.Background())
		_ = store.Close()
		_ = database.Close()
	}()
	rangeValue, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	job := searchjobs.Job{ID: "durable-patterns", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", TimeRange: rangeValue.Intent(), State: searchjobs.StateQueued, CreatedAt: now}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Version, job.State, job.Schema = 6, searchjobs.StateCompleted, &schema
	job.StartedAt, job.FinishedAt, job.ExpiresAt = now, now, now.Add(time.Hour)
	job.RowCount, job.ResultsTruncated = uint64(len(rows)), true
	if err := store.Finalize(ctx, job); err != nil {
		t.Fatal(err)
	}
	original := &testSource{schema: schema, rows: rows, generation: 7, truncated: true}
	if _, err := store.PersistResults(ctx, access, job.ID, &testLease{source: original}); err != nil {
		t.Fatal(err)
	}
	request := ListRequest{SearchJobID: job.ID, Generation: 7, Sensitivity: Balanced, PageSize: 1, IncludeTotal: true}
	before, err := service.List(ctx, access, request)
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if before.RetainedEventCount != 6 || before.EligibleEventCount != 4 || before.ExcludedEventCount != 2 || !before.RetainedTruncated || len(before.Patterns) != 1 || before.Patterns[0].EventCount != 3 {
		t.Fatalf("unexpected retained grouping: %+v", before)
	}
	memberRequest := MemberRequest{SearchJobID: job.ID, Generation: 7, Sensitivity: Balanced, PatternID: before.Patterns[0].ID, PageSize: 2, IncludeTotal: true}
	firstMembers, err := service.Members(ctx, access, memberRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer firstMembers.Close()
	if !reflect.DeepEqual(firstMembers.Rows, rows[:2]) || firstMembers.NextPageToken == "" {
		t.Fatalf("first exact members = %+v", firstMembers)
	}
	memberRequest.PageToken = firstMembers.NextPageToken
	expectedPatterns := slices.Clone(before.Patterns)
	expectedGroupCursor := before.NextPageToken
	expectedEligible, expectedExcluded := before.EligibleEventCount, before.ExcludedEventCount
	firstMembers.Close()
	before.Close()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	// Destroy the original in-memory relation; only the persisted artifact can
	// supply the restarted service. No executor or search manager is available.
	original.rows = nil
	original.schema = searchjobs.Schema{}
	database, store, service = open()
	after, err := service.List(ctx, access, request)
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if !reflect.DeepEqual(after.Patterns, expectedPatterns) || after.NextPageToken != expectedGroupCursor || after.EligibleEventCount != expectedEligible || after.ExcludedEventCount != expectedExcluded {
		t.Fatalf("restart changed immutable grouping: expected patterns=%+v after=%+v", expectedPatterns, after)
	}
	lastMembers, err := service.Members(ctx, access, memberRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer lastMembers.Close()
	if !reflect.DeepEqual(lastMembers.Rows, rows[4:5]) || lastMembers.NextPageToken != "" || lastMembers.TotalSize == nil || *lastMembers.TotalSize != 3 {
		t.Fatalf("restart changed exact member cursor relation: %+v", lastMembers)
	}
	request.PageToken = expectedGroupCursor
	lastGroup, err := service.List(ctx, access, request)
	if err != nil {
		t.Fatal(err)
	}
	defer lastGroup.Close()
	if len(lastGroup.Patterns) != 1 || lastGroup.Patterns[0].Signature != "other <int>" || lastGroup.Patterns[0].EventCount != 1 || lastGroup.NextPageToken != "" {
		t.Fatalf("restart changed group cursor relation: %+v", lastGroup)
	}
}

func TestDurabilityReviewBoundedExportStreamsTenThousandRetainedMembers(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: database.SQLDB(), Directory: filepath.Join(directory, "artifacts"), Clock: func() time.Time { return now }, CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := newReviewService(t, Config{Source: store})
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}}
	rows := make([]searchjobs.ResultRow, 10000)
	for index := range rows {
		rows[index] = searchjobs.ResultRow{Ordinal: uint64(index), Values: []searchjobs.Value{searchjobs.StringValue("event " + strconv.Itoa(index))}}
	}
	rangeValue, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	job := searchjobs.Job{ID: "retained-ten-thousand", Version: 1, TenantID: access.TenantID, OwnerID: access.OwnerID,
		SPL: "index=main", TimeRange: rangeValue.Intent(), State: searchjobs.StateQueued, CreatedAt: now}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Version, job.State, job.Schema = 6, searchjobs.StateCompleted, &schema
	job.StartedAt, job.FinishedAt, job.ExpiresAt = now, now, now.Add(time.Hour)
	job.RowCount, job.ResultsTruncated = uint64(len(rows)), true
	if err := store.Finalize(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, access, job.ID, &testLease{source: &testSource{schema: schema, rows: rows, generation: 7, truncated: true}}); err != nil {
		t.Fatal(err)
	}
	listed, err := service.List(ctx, access, ListRequest{SearchJobID: job.ID, Generation: 7, Sensitivity: Balanced})
	if err != nil {
		t.Fatal(err)
	}
	if listed.RetainedEventCount != 10000 || listed.EligibleEventCount != 10000 || !listed.RetainedTruncated || len(listed.Patterns) != 1 {
		t.Fatalf("retained coverage=%+v", listed)
	}
	selected := listed.Patterns[0].ID
	listed.Close()
	lease, err := service.AcquirePatternMembers(ctx, access, ExportRequest{SearchJobID: job.ID, Generation: 7, SnapshotRef: "snapshot", Sensitivity: Balanced, PatternID: selected})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if lease.RowCount() != 10000 || !lease.ResultsTruncated() {
		t.Fatalf("export metadata rows=%d truncated=%t", lease.RowCount(), lease.ResultsTruncated())
	}
	for index := range rows {
		row, ok, err := lease.Next(ctx)
		if err != nil || !ok || !reflect.DeepEqual(row, rows[index]) {
			t.Fatalf("bounded export row%d = %+v, %t, %v", index, row, ok, err)
		}
	}
	if _, ok, err := lease.Next(ctx); err != nil || ok {
		t.Fatalf("export terminator present=%t error=%v", ok, err)
	}
}
