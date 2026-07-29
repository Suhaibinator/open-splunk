package collectorfleet

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestLoadCollectorsPreservesRequestedOrderAndReturnsDetachedSnapshots(
	t *testing.T,
) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	first := claimCollectorForBatchTest(
		t,
		store,
		"tenant-a",
		"collector-a",
		"alpha.example",
		baseTime,
	)
	second := claimCollectorForBatchTest(
		t,
		store,
		"tenant-a",
		"collector-b",
		"bravo.example",
		baseTime.Add(time.Second),
	)
	for index, collector := range []Collector{first, second} {
		displayName := "collector " + collector.CollectorID
		if _, err := store.UpdateDisplayName(
			ctx,
			Scope{TenantID: collector.TenantID},
			collector.CollectorID,
			collector.Version,
			&displayName,
			baseTime.Add(time.Duration(index+2)*time.Second),
		); err != nil {
			t.Fatalf("UpdateDisplayName(%q): %v", collector.CollectorID, err)
		}
	}

	orderedIDs := []string{"collector-b", "collector-a"}
	got, err := loadCollectors(
		store.orm.WithContext(ctx),
		Scope{TenantID: "tenant-a"},
		orderedIDs,
	)
	if err != nil {
		t.Fatalf("loadCollectors(): %v", err)
	}
	if len(got) != len(orderedIDs) ||
		got[0].CollectorID != orderedIDs[0] ||
		got[1].CollectorID != orderedIDs[1] {
		t.Fatalf("collector order = %#v, want %v", got, orderedIDs)
	}
	for _, collector := range got {
		if !slices.Equal(collector.Capabilities, []uint32{1, 2, 6}) {
			t.Fatalf(
				"%q capability order = %v",
				collector.CollectorID,
				collector.Capabilities,
			)
		}
		if !slices.Equal(
			collector.AuthorizedIndexes,
			[]string{"audit", "main"},
		) {
			t.Fatalf(
				"%q authorized-index order = %v",
				collector.CollectorID,
				collector.AuthorizedIndexes,
			)
		}
		if len(collector.Inputs) != 2 ||
			collector.Inputs[0].InputID != "input-app" ||
			collector.Inputs[1].InputID != "input-audit" {
			t.Fatalf(
				"%q input order = %#v",
				collector.CollectorID,
				collector.Inputs,
			)
		}
		if len(collector.InputHealth) != 1 ||
			collector.InputHealth[0].InputID != "input-app" {
			t.Fatalf(
				"%q input-health rows = %#v",
				collector.CollectorID,
				collector.InputHealth,
			)
		}
	}

	originalDisplayName := *got[0].DisplayName
	originalSource := *got[0].Inputs[0].Source
	originalStreamID := got[0].ActiveLease.StreamID
	originalLastEvent := *got[0].InputHealth[0].LastEventAt
	got[0].Capabilities[0] = 999
	got[0].AuthorizedIndexes[0] = "changed"
	*got[0].DisplayName = "changed"
	*got[0].Inputs[0].Source = "changed"
	got[0].ActiveLease.StreamID = "changed"
	*got[0].InputHealth[0].LastEventAt = time.Unix(1, 0).UTC()

	reloaded, err := loadCollectors(
		store.orm.WithContext(ctx),
		Scope{TenantID: "tenant-a"},
		orderedIDs,
	)
	if err != nil {
		t.Fatalf("loadCollectors(reload): %v", err)
	}
	if reloaded[0].Capabilities[0] != 1 ||
		reloaded[0].AuthorizedIndexes[0] != "audit" ||
		*reloaded[0].DisplayName != originalDisplayName ||
		*reloaded[0].Inputs[0].Source != originalSource ||
		reloaded[0].ActiveLease.StreamID != originalStreamID ||
		!reloaded[0].InputHealth[0].LastEventAt.Equal(originalLastEvent) {
		t.Fatalf("mutating result changed persisted/reloaded snapshot: %#v", reloaded[0])
	}
	if reloaded[1].Capabilities[0] != 1 ||
		reloaded[1].AuthorizedIndexes[0] != "audit" {
		t.Fatalf("collector result slices alias each other: %#v", reloaded[1])
	}
}

func TestLoadCollectorsEnforcesTenantAndParentBoundaries(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 29, 3, 10, 0, 0, time.UTC)
	claimCollectorForBatchTest(
		t,
		store,
		"tenant-a",
		"collector-shared",
		"alpha.example",
		baseTime,
	)
	claimCollectorForBatchTest(
		t,
		store,
		"tenant-b",
		"collector-shared",
		"bravo.example",
		baseTime.Add(time.Second),
	)
	claimCollectorForBatchTest(
		t,
		store,
		"tenant-b",
		"collector-b-only",
		"private.example",
		baseTime.Add(2*time.Second),
	)

	got, err := loadCollectors(
		store.orm.WithContext(ctx),
		Scope{TenantID: "tenant-a"},
		[]string{"collector-shared"},
	)
	if err != nil {
		t.Fatalf("loadCollectors(tenant A): %v", err)
	}
	if len(got) != 1 ||
		got[0].TenantID != "tenant-a" ||
		got[0].Hostname != "alpha.example" {
		t.Fatalf("tenant A result = %#v", got)
	}

	_, batchErr := loadCollectors(
		store.orm.WithContext(ctx),
		Scope{TenantID: "tenant-a"},
		[]string{"collector-shared", "collector-b-only"},
	)
	if !errors.Is(batchErr, errCollectorBatchMissingIdentity) {
		t.Fatalf(
			"cross-tenant missing parent error = %v, want internal missing sentinel",
			batchErr,
		)
	}
	if errors.Is(batchErr, control.ErrNotFound) {
		t.Fatalf("batch missing parent leaked public ErrNotFound: %v", batchErr)
	}
	if _, err := store.Get(
		ctx,
		Scope{TenantID: "tenant-a"},
		"collector-b-only",
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(cross tenant) error = %v, want ErrNotFound", err)
	}

	for name, ids := range map[string][]string{
		"duplicate": {"collector-shared", "collector-shared"},
		"oversized": {
			"collector-1",
			"collector-2",
			"collector-3",
			"collector-4",
			"collector-5",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadCollectors(
				store.orm.WithContext(ctx),
				Scope{TenantID: "tenant-a"},
				ids,
			)
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("loadCollectors(%s) error = %v", name, err)
			}
		})
	}
}

func TestLoadCollectorsRejectsMissingOrCorruptPersistence(t *testing.T) {
	t.Parallel()

	t.Run("missing runtime", func(t *testing.T) {
		database, store := openTestStore(t)
		ctx := context.Background()
		collector := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-missing-runtime",
			"missing.example",
			time.Date(2026, 7, 29, 3, 20, 0, 0, time.UTC),
		)
		if _, err := database.SQLDB().ExecContext(
			ctx,
			`DELETE FROM collector_runtime
			 WHERE tenant_id = ? AND collector_id = ?`,
			collector.TenantID,
			collector.CollectorID,
		); err != nil {
			t.Fatalf("delete runtime fixture: %v", err)
		}
		_, err := loadCollectors(
			store.orm.WithContext(ctx),
			Scope{TenantID: collector.TenantID},
			[]string{collector.CollectorID},
		)
		if err == nil || !strings.Contains(err.Error(), "missing a runtime row") {
			t.Fatalf("loadCollectors(missing runtime) error = %v", err)
		}
	})

	t.Run("corrupt runtime", func(t *testing.T) {
		_, store := openTestStore(t)
		ctx := context.Background()
		collector := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-corrupt-runtime",
			"healthy.example",
			time.Date(2026, 7, 29, 3, 21, 0, 0, time.UTC),
		)
		tx := beginIgnoredCheckTransaction(t, store.orm.WithContext(ctx))
		defer rollbackIgnoredCheckTransaction(t, tx)
		if err := tx.Model(&runtimeRecord{}).
			Where(
				"tenant_id = ? AND collector_id = ?",
				collector.TenantID,
				collector.CollectorID,
			).
			Update("hostname", strings.Repeat("h", maximumHostnameBytes+1)).
			Error; err != nil {
			t.Fatalf("install corrupt runtime fixture: %v", err)
		}
		_, err := loadCollectors(
			tx,
			Scope{TenantID: collector.TenantID},
			[]string{collector.CollectorID},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "invalid collector hello metadata") {
			t.Fatalf("loadCollectors(corrupt runtime) error = %v", err)
		}
	})

	t.Run("missing authorized child", func(t *testing.T) {
		database, store := openTestStore(t)
		ctx := context.Background()
		collector := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-missing-child",
			"missing-child.example",
			time.Date(2026, 7, 29, 3, 22, 0, 0, time.UTC),
		)
		if _, err := database.SQLDB().ExecContext(
			ctx,
			`DELETE FROM collector_authorized_indexes
			 WHERE tenant_id = ? AND collector_id = ?`,
			collector.TenantID,
			collector.CollectorID,
		); err != nil {
			t.Fatalf("delete authorized indexes fixture: %v", err)
		}
		_, err := loadCollectors(
			store.orm.WithContext(ctx),
			Scope{TenantID: collector.TenantID},
			[]string{collector.CollectorID},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "authorized index snapshot is empty") {
			t.Fatalf("loadCollectors(missing child) error = %v", err)
		}
	})

	t.Run("corrupt child", func(t *testing.T) {
		_, store := openTestStore(t)
		ctx := context.Background()
		collector := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-corrupt-child",
			"corrupt-child.example",
			time.Date(2026, 7, 29, 3, 23, 0, 0, time.UTC),
		)
		tx := beginIgnoredCheckTransaction(t, store.orm.WithContext(ctx))
		defer rollbackIgnoredCheckTransaction(t, tx)
		if err := tx.Model(&capabilityRecord{}).
			Where(
				"tenant_id = ? AND collector_id = ? AND capability = ?",
				collector.TenantID,
				collector.CollectorID,
				1,
			).
			Update("capability", 0).
			Error; err != nil {
			t.Fatalf("install corrupt capability fixture: %v", err)
		}
		_, err := loadCollectors(
			tx,
			Scope{TenantID: collector.TenantID},
			[]string{collector.CollectorID},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "invalid collector capability") {
			t.Fatalf("loadCollectors(corrupt child) error = %v", err)
		}
	})
}

func TestLoadCollectorsRejectsAggregateAndPerCollectorChildOverflow(
	t *testing.T,
) {
	t.Parallel()

	t.Run("aggregate", func(t *testing.T) {
		_, store := openTestStore(t)
		collector := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-overflow",
			"overflow.example",
			time.Date(2026, 7, 29, 3, 30, 0, 0, time.UTC),
		)
		addCapabilityRowsForBatchTest(
			t,
			store.orm,
			collector.TenantID,
			collector.CollectorID,
			maximumCapabilities-len(collector.Capabilities)+1,
		)
		_, err := loadCollectors(
			store.orm.WithContext(context.Background()),
			Scope{TenantID: collector.TenantID},
			[]string{collector.CollectorID},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "aggregate persisted bounds") {
			t.Fatalf("loadCollectors(aggregate overflow) error = %v", err)
		}
	})

	t.Run("per collector", func(t *testing.T) {
		_, store := openTestStore(t)
		baseTime := time.Date(2026, 7, 29, 3, 31, 0, 0, time.UTC)
		overfull := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-a-overfull",
			"overfull.example",
			baseTime,
		)
		normal := claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			"collector-b-normal",
			"normal.example",
			baseTime.Add(time.Second),
		)
		addCapabilityRowsForBatchTest(
			t,
			store.orm,
			overfull.TenantID,
			overfull.CollectorID,
			maximumCapabilities-len(overfull.Capabilities)+1,
		)
		_, err := loadCollectors(
			store.orm.WithContext(context.Background()),
			Scope{TenantID: overfull.TenantID},
			[]string{overfull.CollectorID, normal.CollectorID},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "per-collector persisted bounds") {
			t.Fatalf("loadCollectors(per-collector overflow) error = %v", err)
		}
	})

	if _, err := aggregateCollectorChildLimit(2, math.MaxInt); !errors.Is(
		err,
		control.ErrCapacityExceeded,
	) {
		t.Fatalf("aggregateCollectorChildLimit(overflow) error = %v", err)
	}
}

func TestLoadCollectorsUsesConstantQueryCount(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 29, 3, 40, 0, 0, time.UTC)
	ids := make([]string, 0, maximumCollectorBatchSize)
	for index := range maximumCollectorBatchSize {
		collectorID := "collector-query-" + string(rune('a'+index))
		claimCollectorForBatchTest(
			t,
			store,
			"tenant-a",
			collectorID,
			collectorID+".example",
			baseTime.Add(time.Duration(index)*time.Second),
		)
		ids = append(ids, collectorID)
	}

	var queryCount atomic.Int64
	const callbackName = "collectorfleet:test-count-batch-queries"
	if err := store.orm.Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(*gorm.DB) {
			queryCount.Add(1)
		},
	); err != nil {
		t.Fatalf("register query counter: %v", err)
	}
	t.Cleanup(func() {
		if err := store.orm.Callback().Query().Remove(callbackName); err != nil {
			t.Errorf("remove query counter: %v", err)
		}
	})

	queryCount.Store(0)
	empty, err := loadCollectors(
		store.orm.WithContext(ctx),
		Scope{TenantID: "tenant-a"},
		nil,
	)
	if err != nil || len(empty) != 0 {
		t.Fatalf("loadCollectors(empty) = %#v, %v", empty, err)
	}
	if got := queryCount.Load(); got != 0 {
		t.Fatalf("empty batch query count = %d, want 0", got)
	}

	for name, selected := range map[string][]string{
		"one":  ids[:1],
		"four": ids,
	} {
		queryCount.Store(0)
		got, err := loadCollectors(
			store.orm.WithContext(ctx),
			Scope{TenantID: "tenant-a"},
			selected,
		)
		if err != nil {
			t.Fatalf("loadCollectors(%s): %v", name, err)
		}
		if len(got) != len(selected) {
			t.Fatalf(
				"loadCollectors(%s) length = %d, want %d",
				name,
				len(got),
				len(selected),
			)
		}
		if count := queryCount.Load(); count != 6 {
			t.Fatalf(
				"loadCollectors(%s) query count = %d, want 6",
				name,
				count,
			)
		}
	}
}

func claimCollectorForBatchTest(
	t *testing.T,
	store *Store,
	tenantID string,
	collectorID string,
	hostname string,
	receivedAt time.Time,
) Collector {
	t.Helper()
	request := testClaim(receivedAt)
	request.Scope = Scope{TenantID: tenantID}
	request.CollectorID = collectorID
	request.BootEpoch = "boot-" + collectorID
	request.StreamID = "stream-" + collectorID
	request.Hello.InstanceID = "instance-" + collectorID
	request.Hello.Hostname = hostname
	_, lease, err := store.Claim(context.Background(), request)
	if err != nil {
		t.Fatalf("Claim(%q/%q): %v", tenantID, collectorID, err)
	}
	heartbeat := testHeartbeat(receivedAt.Add(time.Second), 1)
	if applied, err := store.RecordHeartbeat(
		context.Background(),
		lease,
		heartbeat,
	); err != nil || !applied {
		t.Fatalf(
			"RecordHeartbeat(%q/%q) = %t, %v",
			tenantID,
			collectorID,
			applied,
			err,
		)
	}
	reloaded, err := store.Get(
		context.Background(),
		Scope{TenantID: tenantID},
		collectorID,
	)
	if err != nil {
		t.Fatalf("Get(%q/%q): %v", tenantID, collectorID, err)
	}
	return reloaded
}

func addCapabilityRowsForBatchTest(
	t *testing.T,
	database *gorm.DB,
	tenantID string,
	collectorID string,
	count int,
) {
	t.Helper()
	rows := make([]capabilityRecord, count)
	for index := range rows {
		rows[index] = capabilityRecord{
			TenantID:    tenantID,
			CollectorID: collectorID,
			Capability:  int64(1_000 + index),
		}
	}
	if err := database.Create(&rows).Error; err != nil {
		t.Fatalf("add capability rows: %v", err)
	}
}

func beginIgnoredCheckTransaction(t *testing.T, database *gorm.DB) *gorm.DB {
	t.Helper()
	tx := database.Begin()
	if tx.Error != nil {
		t.Fatalf("begin corruption fixture transaction: %v", tx.Error)
	}
	if err := tx.Exec("PRAGMA ignore_check_constraints = ON").Error; err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("disable check constraints for fixture: %v", err)
	}
	return tx
}

func rollbackIgnoredCheckTransaction(t *testing.T, tx *gorm.DB) {
	t.Helper()
	if err := tx.Exec("PRAGMA ignore_check_constraints = OFF").Error; err != nil {
		t.Errorf("restore check constraints after fixture: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Errorf("roll back corruption fixture: %v", err)
	}
}
