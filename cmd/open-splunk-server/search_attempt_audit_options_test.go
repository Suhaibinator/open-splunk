package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

func TestSearchAttemptAuditMaximumRetainedFlagHasExplicitDefault(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want int
	}{
		{
			name: "default",
			want: int(searchaudit.DefaultMaximumRetainedAttempts),
		},
		{
			name: "configured",
			args: []string{"-search-attempt-audit-maximum-retained-attempts=17"},
			want: 17,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet("open-splunk-server", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			var config options
			registerSearchAttemptAuditMaximumRetainedFlag(flags, &config)
			if err := flags.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			if config.searchAttemptAuditMaximumRetainedAttempts != test.want {
				t.Fatalf(
					"search-attempt audit maximum = %d, want %d",
					config.searchAttemptAuditMaximumRetainedAttempts,
					test.want,
				)
			}
		})
	}
}

func TestNormalizeRuntimeOptionsBoundsSearchAttemptAuditMaximum(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		maximum int
		want    int
		wantErr bool
	}{
		{
			name: "programmatic zero selects default",
			want: int(searchaudit.DefaultMaximumRetainedAttempts),
		},
		{name: "minimum", maximum: 1, want: 1},
		{
			name:    "hard maximum",
			maximum: int(searchaudit.MaximumRetainedAttempts),
			want:    int(searchaudit.MaximumRetainedAttempts),
		},
		{name: "negative", maximum: -1, wantErr: true},
		{
			name:    "above hard maximum",
			maximum: int(searchaudit.MaximumRetainedAttempts) + 1,
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := validSearchHistoryRuntimeOptions()
			config.searchAttemptAuditMaximumRetainedAttempts = test.maximum
			err := normalizeRuntimeOptions(&config)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeRuntimeOptions() unexpectedly succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeRuntimeOptions() error = %v", err)
			}
			if config.searchAttemptAuditMaximumRetainedAttempts != test.want {
				t.Fatalf(
					"normalized search-attempt audit maximum = %d, want %d",
					config.searchAttemptAuditMaximumRetainedAttempts,
					test.want,
				)
			}
		})
	}
}

func TestRuntimeSearchAttemptAuditMaximumPersistsAndRejectsReopenMismatch(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")
	database, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	databaseClosed := false
	t.Cleanup(func() {
		if !databaseClosed {
			if err := database.Close(); err != nil {
				t.Errorf("close control database: %v", err)
			}
		}
	})

	stores, err := openRuntimeSecurityStoresWithSearchAttemptMaximum(
		ctx,
		database,
		masterKeyPath,
		"tenant-search-attempt-cap",
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	appendErr := stores.searchAttemptAuditEvents.AppendSearchAttemptInTransaction(
		ctx,
		tx,
		"tenant-search-attempt-cap",
		searchhistory.SearchAttemptAuditEvent{
			OccurredAt:  time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC),
			OwnerID:     "single-user",
			SearchJobID: "job-configured-cap",
		},
	)
	if appendErr != nil {
		_ = tx.Rollback().Error
		t.Fatal(appendErr)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	var persistedMaximum int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT maximum_retained_attempts
		FROM search_attempt_audit_tenant_state
		WHERE tenant_id = ?
	`, "tenant-search-attempt-cap").Scan(&persistedMaximum); err != nil {
		t.Fatal(err)
	}
	if persistedMaximum != 2 {
		t.Fatalf("persisted search-attempt audit maximum = %d, want 2", persistedMaximum)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	databaseClosed = true

	reopened, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened control database: %v", err)
		}
	})
	matching, err := openRuntimeSecurityStoresWithSearchAttemptMaximum(
		ctx,
		reopened,
		masterKeyPath,
		"tenant-search-attempt-cap",
		2,
	)
	if err != nil || matching.searchAttemptAuditEvents == nil {
		t.Fatalf(
			"openRuntimeSecurityStoresWithSearchAttemptMaximum(match) = (%+v, %v)",
			matching,
			err,
		)
	}
	mismatched, err := openRuntimeSecurityStoresWithSearchAttemptMaximum(
		ctx,
		reopened,
		masterKeyPath,
		"tenant-search-attempt-cap",
		3,
	)
	if mismatched != (securityStoreSet{}) || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf(
			"openRuntimeSecurityStoresWithSearchAttemptMaximum(mismatch) = (%+v, %v)",
			mismatched,
			err,
		)
	}
}
