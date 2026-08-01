package auth

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func TestAuthenticateReturnsDetachedCurrentAuthorizedIndexPolicies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	auditDefinition := indexPolicyTestDefinition("audit", 14*24*time.Hour, "audit:json", 128<<10)
	mainDefinition := indexPolicyTestDefinition("main", 30*24*time.Hour, "go:zap:json", 256<<10)
	audit, err := database.CreateIndex(ctx, auditDefinition)
	if err != nil {
		t.Fatalf("CreateIndex(audit): %v", err)
	}
	main, err := database.CreateIndex(ctx, mainDefinition)
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(database, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "collector",
		AllowedIndexNames: []string{"main", "audit"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	got, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(): %v", err)
	}
	want := []AuthorizedIndexPolicy{
		indexPolicyFromControlIndex(audit),
		indexPolicyFromControlIndex(main),
	}
	if !reflect.DeepEqual(got.AuthorizedIndexes, want) {
		t.Fatalf("authorized policies = %#v, want %#v", got.AuthorizedIndexes, want)
	}

	got.AuthorizedIndexes[0].Name = "mutated"
	got.AuthorizedIndexes[0].DefaultSourcetype = "mutated"
	got.AuthorizedIndexes[0].Limits.MaxEventBytes = math.MaxUint64
	fresh, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(fresh): %v", err)
	}
	if !reflect.DeepEqual(fresh.AuthorizedIndexes, want) {
		t.Fatalf("fresh authorized policies = %#v, want detached %#v", fresh.AuthorizedIndexes, want)
	}

	replacement := main.Definition
	replacement.RetentionPeriod = 7 * 24 * time.Hour
	replacement.DefaultSourcetype = "main:ndjson"
	replacement.Limits = control.IndexLimits{
		MaxEventBytes:     64 << 10,
		MaxFieldCount:     17,
		MaxNestingDepth:   5,
		MaximumFutureSkew: 17 * time.Second,
		MaximumEventAge:   17 * time.Hour,
	}
	updatedMain, err := database.UpdateIndex(ctx, main.ID, main.Version, replacement)
	if err != nil {
		t.Fatalf("UpdateIndex(main policy): %v", err)
	}
	refreshed, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(updated policy): %v", err)
	}
	want = []AuthorizedIndexPolicy{
		indexPolicyFromControlIndex(audit),
		indexPolicyFromControlIndex(updatedMain),
	}
	if !reflect.DeepEqual(refreshed.AuthorizedIndexes, want) {
		t.Fatalf("updated authorized policies = %#v, want %#v", refreshed.AuthorizedIndexes, want)
	}

	if _, err := database.SetIndexState(ctx, audit.ID, audit.Version, control.IndexStateArchived); err != nil {
		t.Fatalf("SetIndexState(audit archived): %v", err)
	}
	activeOnly, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(active only): %v", err)
	}
	want = []AuthorizedIndexPolicy{indexPolicyFromControlIndex(updatedMain)}
	if !reflect.DeepEqual(activeOnly.AuthorizedIndexes, want) {
		t.Fatalf("active authorized policies = %#v, want %#v", activeOnly.AuthorizedIndexes, want)
	}
}

func TestAuthenticateFailsClosedForCorruptAuthorizedIndexPolicy(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		statement            string
		value                any
		dropBoundedTrigger   bool
		dropRetentionTrigger bool
	}{
		"version": {
			statement: `UPDATE indexes SET version = ? WHERE name = 'main'`,
			value:     int64(0),
		},
		"retention": {
			statement:            `UPDATE indexes SET retention_nanoseconds = ? WHERE name = 'main'`,
			value:                int64(-time.Millisecond),
			dropRetentionTrigger: true,
		},
		"unaligned retention": {
			statement:            `UPDATE indexes SET retention_nanoseconds = ? WHERE name = 'main'`,
			value:                int64(time.Millisecond + time.Nanosecond),
			dropRetentionTrigger: true,
		},
		"retention past storage horizon": {
			statement: `UPDATE indexes SET retention_nanoseconds = ? WHERE name = 'main'`,
			value:     int64(8_000_000_000 * time.Second),
		},
		"default sourcetype": {
			statement:          `UPDATE indexes SET default_sourcetype = ? WHERE name = 'main'`,
			value:              " surrounding-space ",
			dropBoundedTrigger: true,
		},
		"max event bytes": {
			statement: `UPDATE indexes SET max_event_bytes = ? WHERE name = 'main'`,
			value:     int64(-1),
		},
		"max event bytes above hard ceiling": {
			statement: `UPDATE indexes SET max_event_bytes = ? WHERE name = 'main'`,
			value:     int64(indexpolicy.HardMaxEventBytes + 1),
		},
		"max field count": {
			statement: `UPDATE indexes SET max_field_count = ? WHERE name = 'main'`,
			value:     int64(math.MaxUint32) + 1,
		},
		"max field count above hard ceiling": {
			statement: `UPDATE indexes SET max_field_count = ? WHERE name = 'main'`,
			value:     int64(indexpolicy.HardMaxFieldCount + 1),
		},
		"max nesting depth": {
			statement: `UPDATE indexes SET max_nesting_depth = ? WHERE name = 'main'`,
			value:     int64(math.MaxUint32) + 1,
		},
		"max nesting depth above hard ceiling": {
			statement: `UPDATE indexes SET max_nesting_depth = ? WHERE name = 'main'`,
			value:     int64(indexpolicy.HardMaxNestingDepth + 1),
		},
		"maximum future skew": {
			statement: `UPDATE indexes SET maximum_future_skew_nanoseconds = ? WHERE name = 'main'`,
			value:     int64(-1),
		},
		"maximum future skew above hard ceiling": {
			statement: `UPDATE indexes SET maximum_future_skew_nanoseconds = ? WHERE name = 'main'`,
			value:     int64(indexpolicy.HardMaxFutureSkew + time.Nanosecond),
		},
		"maximum event age": {
			statement: `UPDATE indexes SET maximum_event_age_nanoseconds = ? WHERE name = 'main'`,
			value:     int64(-1),
		},
		"maximum event age above hard ceiling": {
			statement: `UPDATE indexes SET maximum_event_age_nanoseconds = ? WHERE name = 'main'`,
			value:     int64(indexpolicy.HardMaxEventAge + time.Nanosecond),
		},
		"max ingest events per second negative": {
			statement: `UPDATE indexes SET max_ingest_events_per_second = ? WHERE name = 'main'`,
			value:     int64(-1),
		},
		"max ingest events per second above hard ceiling": {
			statement: `UPDATE indexes SET max_ingest_events_per_second = ? WHERE name = 'main'`,
			value:     int64(ingestquota.HardMaxEventsPerSecond + 1),
		},
		"max ingest uncompressed bytes per second negative": {
			statement: `UPDATE indexes SET max_ingest_uncompressed_bytes_per_second = ? WHERE name = 'main'`,
			value:     int64(-1),
		},
		"max ingest uncompressed bytes per second above hard ceiling": {
			statement: `UPDATE indexes SET max_ingest_uncompressed_bytes_per_second = ? WHERE name = 'main'`,
			value:     int64(ingestquota.HardMaxUncompressedBytesPerSecond + 1),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			database := openControlDB(t)
			if _, err := database.CreateIndex(ctx, indexPolicyTestDefinition("main", 24*time.Hour, "json", 64<<10)); err != nil {
				t.Fatalf("CreateIndex(main): %v", err)
			}
			store, err := NewStore(database, []byte("0123456789abcdef0123456789abcdef"))
			if err != nil {
				t.Fatalf("NewStore(): %v", err)
			}
			issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name: "collector", AllowedIndexNames: []string{"main"}, BoundCollectorID: testCollectorID,
			})
			if err != nil {
				t.Fatalf("CreateCollectorToken(): %v", err)
			}
			connection, err := database.SQLDB().Conn(ctx)
			if err != nil {
				t.Fatalf("acquire corrupting connection: %v", err)
			}
			if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
				_ = connection.Close()
				t.Fatalf("ignore check constraints: %v", err)
			}
			if test.dropBoundedTrigger {
				if _, err := connection.ExecContext(ctx, `DROP TRIGGER index_catalog_record_update_is_bounded`); err != nil {
					_ = connection.Close()
					t.Fatalf("drop bounded index trigger: %v", err)
				}
			}
			if test.dropRetentionTrigger {
				if _, err := connection.ExecContext(ctx, `DROP TRIGGER indexes_retention_is_millisecond_aligned_on_update`); err != nil {
					_ = connection.Close()
					t.Fatalf("drop retention alignment trigger: %v", err)
				}
			}
			if _, err := connection.ExecContext(ctx, test.statement, test.value); err != nil {
				_ = connection.Close()
				t.Fatalf("corrupt %s: %v", name, err)
			}
			if err := connection.Close(); err != nil {
				t.Fatalf("close corrupting connection: %v", err)
			}

			_, err = store.Authenticate(ctx, issued.Secret.Plaintext())
			if err == nil || errors.Is(err, ErrUnauthorized) {
				t.Fatalf("Authenticate(corrupt %s) error = %v, want fail-closed backend error", name, err)
			}
		})
	}
}

func indexPolicyTestDefinition(
	name string,
	retention time.Duration,
	defaultSourcetype string,
	maxEventBytes uint64,
) control.IndexDefinition {
	return control.IndexDefinition{
		Name:              name,
		DisplayName:       name,
		RetentionPeriod:   retention,
		IngestionEnabled:  true,
		SearchEnabled:     true,
		DefaultSourcetype: defaultSourcetype,
		Limits: control.IndexLimits{
			MaxEventBytes:     maxEventBytes,
			MaxFieldCount:     256,
			MaxNestingDepth:   16,
			MaximumFutureSkew: 45 * time.Second,
			MaximumEventAge:   90 * 24 * time.Hour,
		},
	}
}

func indexPolicyFromControlIndex(index control.Index) AuthorizedIndexPolicy {
	return AuthorizedIndexPolicy{
		Name:              index.Definition.Name,
		Version:           index.Version,
		RetentionPeriod:   index.Definition.RetentionPeriod,
		DefaultSourcetype: index.Definition.DefaultSourcetype,
		Limits:            index.Definition.Limits,
	}
}
