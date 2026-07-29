package collectorfleet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestStoreEnforcesDurableCollectorCapacityPerTenant(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	fillDurableCollectorCapacity(
		t,
		store,
		"tenant-capacity",
		now,
		MaximumDurableCollectorsPerTenant,
	)

	// Capacity applies only to a new durable identity. A known enabled
	// collector can still replace its stream lease.
	existing := durableCollectorCapacityClaim(
		"tenant-capacity",
		0,
		now.Add(time.Minute),
	)
	existing.BootEpoch = "replacement-boot"
	existing.StreamID = "replacement-stream"
	existing.Hello.InstanceID = "replacement-instance"
	if _, _, err := store.Claim(ctx, existing); err != nil {
		t.Fatalf("Claim(existing at capacity): %v", err)
	}

	disabledID := durableCollectorCapacityID(1)
	if _, err := store.SetAdministrativeState(
		ctx,
		Scope{TenantID: "tenant-capacity"},
		disabledID,
		1,
		AdministrativeStateDisabled,
		now.Add(2*time.Minute),
	); err != nil {
		t.Fatalf("disable existing collector: %v", err)
	}
	disabled := durableCollectorCapacityClaim(
		"tenant-capacity",
		1,
		now.Add(3*time.Minute),
	)
	if _, _, err := store.Claim(ctx, disabled); !errors.Is(
		err,
		ErrCollectorDisabled,
	) {
		t.Fatalf(
			"Claim(disabled at capacity) error = %v, want ErrCollectorDisabled",
			err,
		)
	}

	beforeRevision, beforeFleet, beforeRuntime := readCapacityMarker(
		t,
		database,
		"tenant-capacity",
	)
	rejected := durableCollectorCapacityClaim(
		"tenant-capacity",
		MaximumDurableCollectorsPerTenant,
		now.Add(4*time.Minute),
	)
	if _, _, err := store.Claim(ctx, rejected); !errors.Is(
		err,
		control.ErrCapacityExceeded,
	) {
		t.Fatalf(
			"Claim(new identity over capacity) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	afterRevision, afterFleet, afterRuntime := readCapacityMarker(
		t,
		database,
		"tenant-capacity",
	)
	if afterRevision != beforeRevision ||
		afterFleet != beforeFleet ||
		afterRuntime != beforeRuntime {
		t.Fatalf(
			"rejected claim changed marker from (%d,%d,%d) to (%d,%d,%d)",
			beforeRevision,
			beforeFleet,
			beforeRuntime,
			afterRevision,
			afterFleet,
			afterRuntime,
		)
	}
	assertCapacityCollectorAbsent(
		t,
		database,
		rejected.TenantID,
		rejected.CollectorID,
	)

	otherTenant := durableCollectorCapacityClaim(
		"tenant-independent",
		MaximumDurableCollectorsPerTenant,
		now.Add(5*time.Minute),
	)
	if _, _, err := store.Claim(ctx, otherTenant); err != nil {
		t.Fatalf("Claim(other tenant): %v", err)
	}
}

func TestStoreConcurrentClaimsCannotExceedDurableCollectorCapacity(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	fillDurableCollectorCapacity(
		t,
		store,
		"tenant-concurrent-capacity",
		now,
		MaximumDurableCollectorsPerTenant-1,
	)

	type claimResult struct {
		collectorID string
		err         error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var workers sync.WaitGroup
	for _, ordinal := range []int{
		MaximumDurableCollectorsPerTenant - 1,
		MaximumDurableCollectorsPerTenant,
	} {
		ordinal := ordinal
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			request := durableCollectorCapacityClaim(
				"tenant-concurrent-capacity",
				ordinal,
				now.Add(time.Minute),
			)
			_, _, err := store.Claim(ctx, request)
			results <- claimResult{
				collectorID: request.CollectorID,
				err:         err,
			}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	var succeeded string
	var rejected string
	for result := range results {
		switch {
		case result.err == nil:
			if succeeded != "" {
				t.Fatalf(
					"multiple claims crossed capacity: %q and %q",
					succeeded,
					result.collectorID,
				)
			}
			succeeded = result.collectorID
		case errors.Is(result.err, control.ErrCapacityExceeded):
			rejected = result.collectorID
		default:
			t.Fatalf(
				"Claim(%s) error = %v",
				result.collectorID,
				result.err,
			)
		}
	}
	if succeeded == "" || rejected == "" {
		t.Fatalf(
			"concurrent claims success/rejection = %q/%q",
			succeeded,
			rejected,
		)
	}
	_, fleetCount, runtimeCount := readCapacityMarker(
		t,
		database,
		"tenant-concurrent-capacity",
	)
	if fleetCount != MaximumDurableCollectorsPerTenant ||
		runtimeCount != MaximumDurableCollectorsPerTenant {
		t.Fatalf(
			"final parent counts = %d/%d, want %d/%d",
			fleetCount,
			runtimeCount,
			MaximumDurableCollectorsPerTenant,
			MaximumDurableCollectorsPerTenant,
		)
	}
	assertCapacityCollectorAbsent(
		t,
		database,
		"tenant-concurrent-capacity",
		rejected,
	)
	if _, err := store.Get(
		ctx,
		Scope{TenantID: "tenant-concurrent-capacity"},
		succeeded,
	); err != nil {
		t.Fatalf("Get(successful capacity claim): %v", err)
	}
}

func fillDurableCollectorCapacity(
	t *testing.T,
	store *Store,
	tenantID string,
	now time.Time,
	count int,
) {
	t.Helper()
	for ordinal := 0; ordinal < count; ordinal++ {
		request := durableCollectorCapacityClaim(
			tenantID,
			ordinal,
			now.Add(time.Duration(ordinal)*time.Microsecond),
		)
		if _, _, err := store.Claim(
			context.Background(),
			request,
		); err != nil {
			t.Fatalf("Claim(capacity seed %d): %v", ordinal, err)
		}
	}
}

func durableCollectorCapacityClaim(
	tenantID string,
	ordinal int,
	receivedAt time.Time,
) ClaimRequest {
	identifier := durableCollectorCapacityID(ordinal)
	request := testClaim(receivedAt)
	request.Scope = Scope{TenantID: tenantID}
	request.CollectorID = identifier
	request.BootEpoch = "boot-" + identifier
	request.StreamID = "stream-" + identifier
	request.Hello.InstanceID = "instance-" + identifier
	request.Hello.Hostname = identifier + ".example"
	request.Hello.Capabilities = nil
	request.Hello.AuthorizedIndexes = []string{"main"}
	request.Hello.Inputs = nil
	request.Hello.LastAcknowledgedBatchSequence = nil
	return request
}

func durableCollectorCapacityID(ordinal int) string {
	return fmt.Sprintf("collector-capacity-%03d", ordinal)
}

func readCapacityMarker(
	t *testing.T,
	database *control.DB,
	tenantID string,
) (int64, int64, int64) {
	t.Helper()
	var revision int64
	var fleetCount int64
	var runtimeCount int64
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT revision, fleet_count, runtime_count
		 FROM collector_catalog_revisions
		 WHERE tenant_id = ?`,
		tenantID,
	).Scan(&revision, &fleetCount, &runtimeCount); err != nil {
		t.Fatalf("read capacity marker: %v", err)
	}
	return revision, fleetCount, runtimeCount
}

func assertCapacityCollectorAbsent(
	t *testing.T,
	database *control.DB,
	tenantID string,
	collectorID string,
) {
	t.Helper()
	var counts [6]int
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT
			(SELECT count(*) FROM collector_fleet
			 WHERE tenant_id = ? AND collector_id = ?),
			(SELECT count(*) FROM collector_runtime
			 WHERE tenant_id = ? AND collector_id = ?),
			(SELECT count(*) FROM collector_capabilities
			 WHERE tenant_id = ? AND collector_id = ?),
			(SELECT count(*) FROM collector_authorized_indexes
			 WHERE tenant_id = ? AND collector_id = ?),
			(SELECT count(*) FROM collector_inputs
			 WHERE tenant_id = ? AND collector_id = ?),
			(SELECT count(*) FROM collector_input_health
			 WHERE tenant_id = ? AND collector_id = ?)`,
		tenantID,
		collectorID,
		tenantID,
		collectorID,
		tenantID,
		collectorID,
		tenantID,
		collectorID,
		tenantID,
		collectorID,
		tenantID,
		collectorID,
	).Scan(
		&counts[0],
		&counts[1],
		&counts[2],
		&counts[3],
		&counts[4],
		&counts[5],
	); err != nil {
		t.Fatalf("count rejected collector rows: %v", err)
	}
	for index, count := range counts {
		if count != 0 {
			t.Fatalf("rejected collector child count[%d] = %d", index, count)
		}
	}
}
