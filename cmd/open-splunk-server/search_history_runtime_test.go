package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNormalizeRuntimeOptionsAppliesSearchHistoryRetentionDefaults(t *testing.T) {
	t.Parallel()

	config := validSearchHistoryRuntimeOptions()
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatalf("normalizeRuntimeOptions() error = %v", err)
	}
	if config.searchHistoryMaximumAge != searchhistory.DefaultMaximumAge {
		t.Fatalf(
			"search-history maximum age = %s, want %s",
			config.searchHistoryMaximumAge,
			searchhistory.DefaultMaximumAge,
		)
	}
	if config.searchHistoryMaximumEntriesPerOwner != searchhistory.DefaultMaximumEntriesPerOwner {
		t.Fatalf(
			"search-history maximum entries = %d, want %d",
			config.searchHistoryMaximumEntriesPerOwner,
			searchhistory.DefaultMaximumEntriesPerOwner,
		)
	}
}

func TestNormalizeRuntimeOptionsRejectsInvalidSearchHistoryRetention(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*options){
		"negative age": func(config *options) {
			config.searchHistoryMaximumAge = -time.Nanosecond
		},
		"age above hard bound": func(config *options) {
			config.searchHistoryMaximumAge = searchhistory.MaximumAllowedAge + time.Nanosecond
		},
		"negative entries": func(config *options) {
			config.searchHistoryMaximumEntriesPerOwner = -1
		},
		"entries above hard bound": func(config *options) {
			config.searchHistoryMaximumEntriesPerOwner = searchhistory.MaximumAllowedEntriesPerOwner + 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := validSearchHistoryRuntimeOptions()
			mutate(&config)
			if err := normalizeRuntimeOptions(&config); err == nil {
				t.Fatal("normalizeRuntimeOptions() unexpectedly succeeded")
			}
		})
	}
}

func TestOpenSearchHistoryStorePropagatesRetentionOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
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

	store, err := openSearchHistoryStore(
		ctx,
		database,
		filepath.Join(directory, "server.key"),
		searchhistory.Options{
			MaximumAge:             time.Hour,
			MaximumEntriesPerOwner: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	scope := searchhistory.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	expired := runtimeSearchHistoryEntry(
		"expired",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		time.Now().UTC().Add(-2*time.Hour),
	)
	if _, err := store.Record(ctx, scope, expired); err != nil {
		t.Fatalf("Record(expired) error = %v", err)
	}
	if _, err := store.Get(ctx, scope, expired.GetSearchJobId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(expired) error = %v, want control.ErrNotFound", err)
	}

	first := runtimeSearchHistoryEntry(
		"pending-first",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		time.Now().UTC(),
	)
	if _, err := store.BeginAttempt(ctx, scope, first); err != nil {
		t.Fatalf("BeginAttempt(first) error = %v", err)
	}
	second := runtimeSearchHistoryEntry(
		"pending-second",
		opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		time.Now().UTC(),
	)
	if _, err := store.BeginAttempt(ctx, scope, second); !errors.Is(err, searchhistory.ErrCapacity) {
		t.Fatalf("BeginAttempt(second) error = %v, want searchhistory.ErrCapacity", err)
	}
}

func TestSearchHistoryTerminalAndPendingLimitsAreIndependent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
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
	store, err := openSearchHistoryStore(
		ctx,
		database,
		filepath.Join(directory, "server.key"),
		searchhistory.Options{
			MaximumAge:             time.Hour,
			MaximumEntriesPerOwner: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	scope := searchhistory.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	now := time.Now().UTC()
	if _, err := store.Record(
		ctx,
		scope,
		runtimeSearchHistoryEntry(
			"terminal",
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginAttempt(
		ctx,
		scope,
		runtimeSearchHistoryEntry(
			"pending",
			opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	var terminal, pending int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM search_history`,
	).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM search_history_pending`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if terminal != 1 || pending != 1 {
		t.Fatalf(
			"independent retention rows = terminal:%d pending:%d, want 1 and 1",
			terminal,
			pending,
		)
	}
}

func validSearchHistoryRuntimeOptions() options {
	return options{
		httpAddress:    "127.0.0.1:8080",
		indexRetention: time.Hour,
		tenantID:       "tenant",
	}
}

func runtimeSearchHistoryEntry(
	id string,
	state opensplunk.SearchJobState,
	created time.Time,
) *opensplunk.SearchHistoryEntry {
	appID := "search"
	earliest := "-15m"
	latest := "now"
	entry := &opensplunk.SearchHistoryEntry{
		SearchJobId: id,
		Definition: &opensplunk.SearchDefinition{
			Spl:        "index=main | head 1",
			AppId:      &appID,
			IndexScope: []string{"main"},
			TimeRange: &opensplunk.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
			},
		},
		Source: &opensplunk.SearchJobSource{
			Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC,
		},
		EffectiveIndexScope: []string{"main"},
		ResolvedTimeRange: &opensplunk.ResolvedTimeRange{
			Earliest: timestamppb.New(created.Add(-15 * time.Minute)),
			Latest:   timestamppb.New(created),
			Timezone: "UTC",
		},
		FinalState: state,
		CreatedAt:  timestamppb.New(created),
	}
	if state == opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED {
		return entry
	}
	entry.StartedAt = timestamppb.New(created.Add(time.Second))
	entry.FinishedAt = timestamppb.New(created.Add(2 * time.Second))
	entry.Duration = durationpb.New(time.Second)
	return entry
}
