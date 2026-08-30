package main

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type alertRuntimeStore struct {
	record          searchartifacts.Record
	lease           searchartifacts.ResultLease
	getErr          error
	mode            searchartifacts.AccessMode
	access          searchjobs.AccessScope
	settings        searchartifacts.Settings
	conflicts       int
	updateAttempts  int
	expectedVersion uint64
}

func (store *alertRuntimeStore) Get(_ context.Context, access searchjobs.AccessScope, _ string, mode searchartifacts.AccessMode) (searchartifacts.Record, error) {
	store.access = access
	store.mode = mode
	return store.record, store.getErr
}

func (store *alertRuntimeStore) Acquire(_ context.Context, access searchjobs.AccessScope, _ string) (searchartifacts.ResultLease, error) {
	store.access = access
	return store.lease, nil
}

func (store *alertRuntimeStore) UpdateSettingsExpected(_ context.Context, access searchjobs.AccessScope, _ string, settings searchartifacts.Settings, expectedVersion uint64) (searchartifacts.Record, error) {
	store.access = access
	store.settings = settings
	store.expectedVersion = expectedVersion
	store.updateAttempts++
	if store.conflicts > 0 {
		store.conflicts--
		store.record.Job.Version++
		return searchartifacts.Record{}, searchartifacts.ErrConflict
	}
	updated := store.record
	updated.Visibility = settings.Visibility
	updated.RetentionClass = settings.RetentionClass
	updated.Lifetime = settings.Lifetime
	updated.Job.Version++
	updated.ExpiresAt = time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	store.record = updated
	return updated, nil
}

type alertRuntimeLease struct {
	schema searchjobs.Schema
	rows   []searchjobs.ResultRow
	next   int
	closed bool
}

func (lease *alertRuntimeLease) Schema() searchjobs.Schema { return lease.schema }
func (lease *alertRuntimeLease) RowCount() uint64          { return uint64(len(lease.rows)) }
func (*alertRuntimeLease) RowCountExact() bool             { return true }
func (*alertRuntimeLease) ResultsTruncated() bool          { return false }
func (*alertRuntimeLease) Generation() uint64              { return 1 }
func (lease *alertRuntimeLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	if lease.next >= len(lease.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := lease.rows[lease.next]
	lease.next++
	return row, true, nil
}
func (lease *alertRuntimeLease) Close() error { lease.closed = true; return nil }

func TestRuntimeAlertArtifactsProjectsExactExpiryAsTerminal(t *testing.T) {
	t.Parallel()
	store := &alertRuntimeStore{getErr: searchartifacts.ErrExpired}
	runtime, err := newRuntimeAlertArtifacts(store, "tenant-1")
	if err != nil {
		t.Fatalf("newRuntimeAlertArtifacts() error = %v", err)
	}
	job, err := runtime.ReadAlertSearchJob(context.Background(), "owner-1", "job-1")
	if err != nil {
		t.Fatalf("ReadAlertSearchJob() error = %v", err)
	}
	if job.ID != "job-1" || job.State != alerts.SearchJobExpired || store.mode != searchartifacts.AccessInspect {
		t.Fatalf("expired job=%+v mode=%d", job, store.mode)
	}
}

func TestRuntimeAlertArtifactsObserveWithoutRefreshThenExtendTriggeredRetention(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	store := &alertRuntimeStore{record: searchartifacts.Record{
		Job:   searchjobs.Job{ID: "job-1", Version: 3, RowCount: 7, ResultsTruncated: true, StartedAt: now, FinishedAt: now.Add(time.Second)},
		State: searchartifacts.StateCompleted, Visibility: searchartifacts.VisibilityPrivate,
		RetentionClass: searchartifacts.RetentionScheduledAlert, Lifetime: 10 * time.Minute,
		ExpiresAt: now.Add(10 * time.Minute),
	}}
	runtime, err := newRuntimeAlertArtifacts(store, "tenant-1")
	if err != nil {
		t.Fatalf("newRuntimeAlertArtifacts() error = %v", err)
	}
	job, err := runtime.ReadAlertSearchJob(context.Background(), "owner-1", "job-1")
	if err != nil {
		t.Fatalf("ReadAlertSearchJob() error = %v", err)
	}
	if store.mode != searchartifacts.AccessInspect || store.access.TenantID != "tenant-1" || store.access.OwnerID != "owner-1" || job.State != alerts.SearchJobCompleted || job.ResultCount != 7 || !job.ResultsTruncated {
		t.Fatalf("inspect mode=%d access=%+v job=%+v", store.mode, store.access, job)
	}
	wantLifetime := 50 * time.Minute
	expires, err := runtime.ExtendAlertSearchJob(context.Background(), "owner-1", "job-1", wantLifetime)
	if err != nil {
		t.Fatalf("ExtendAlertSearchJob() error = %v", err)
	}
	if store.settings.Visibility != searchartifacts.VisibilityPrivate || store.settings.RetentionClass != searchartifacts.RetentionTriggeredWebhook || store.settings.Lifetime != wantLifetime || store.expectedVersion != 3 || expires.IsZero() {
		t.Fatalf("triggered settings=%+v expires=%v", store.settings, expires)
	}
}

func TestRuntimeAlertArtifactsNeverShortensRetentionAndRetriesConflicts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	store := &alertRuntimeStore{conflicts: 1, record: searchartifacts.Record{
		Job:            searchjobs.Job{ID: "job-1", Version: 7},
		State:          searchartifacts.StateCompleted,
		Visibility:     searchartifacts.VisibilityEveryone,
		RetentionClass: searchartifacts.RetentionShared,
		Lifetime:       7 * 24 * time.Hour,
		ExpiresAt:      now.Add(7 * 24 * time.Hour),
	}}
	runtime, err := newRuntimeAlertArtifacts(store, "tenant-1")
	if err != nil {
		t.Fatalf("newRuntimeAlertArtifacts() error = %v", err)
	}
	if _, err := runtime.ExtendAlertSearchJob(context.Background(), "owner-1", "job-1", 50*time.Minute); err != nil {
		t.Fatalf("ExtendAlertSearchJob() error = %v", err)
	}
	if store.updateAttempts != 2 || store.expectedVersion != 8 {
		t.Fatalf("update attempts=%d expected version=%d, want 2 and 8", store.updateAttempts, store.expectedVersion)
	}
	if store.settings.Visibility != searchartifacts.VisibilityEveryone || store.settings.RetentionClass != searchartifacts.RetentionShared || store.settings.Lifetime != 7*24*time.Hour {
		t.Fatalf("retention shortened after trigger: %+v", store.settings)
	}
}

func TestRuntimeAlertArtifactsReadBoundedTypedSample(t *testing.T) {
	t.Parallel()
	lease := &alertRuntimeLease{
		schema: searchjobs.Schema{Columns: []searchjobs.Column{
			{Name: "host", Kind: searchjobs.ValueKindString},
			{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		}},
		rows: []searchjobs.ResultRow{
			{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("api-1"), searchjobs.UnsignedValue(4)}},
			{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("api-2"), searchjobs.UnsignedValue(8)}},
		},
	}
	store := &alertRuntimeStore{lease: lease}
	runtime, err := newRuntimeAlertArtifacts(store, "tenant-1")
	if err != nil {
		t.Fatalf("newRuntimeAlertArtifacts() error = %v", err)
	}
	result, err := runtime.ReadAlertSearchResults(context.Background(), "owner-1", "job-1", 1)
	if err != nil {
		t.Fatalf("ReadAlertSearchResults() error = %v", err)
	}
	if len(result.Schema) != 2 || result.Schema[1].Type != "uint64" || len(result.Rows) != 1 || result.Rows[0]["host"] != "api-1" || result.Rows[0]["count"] != uint64(4) || !result.More || !lease.closed {
		t.Fatalf("result=%+v lease.closed=%t", result, lease.closed)
	}
}
