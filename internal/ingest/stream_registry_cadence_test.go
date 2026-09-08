package ingest

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectorHeartbeatCadenceSurvivesTakeoverAndRelease(t *testing.T) {
	t.Parallel()
	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	start := time.Now()
	interval := 7 * time.Second
	first, err := registry.Activate(durableStreamLease(key, "boot-a", "stream-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	assertHeartbeatAdmission(t, registry, first, start, interval, true, nil)
	second, err := registry.Activate(durableStreamLease(key, "boot-b", "stream-b", 2))
	if err != nil {
		t.Fatal(err)
	}
	assertHeartbeatAdmission(t, registry, second, start.Add(interval-1), interval, false, nil)
	// A stale predecessor cannot reserve an eligible successor slot.
	assertHeartbeatAdmission(t, registry, first, start.Add(interval), interval, false, ErrCollectorLeaseNotCurrent)
	registry.Release(first)
	assertHeartbeatAdmission(t, registry, second, start.Add(interval), interval, true, nil)
	registry.Release(second)
	assertHeartbeatAdmission(t, registry, second, start.Add(2*interval), interval, false, ErrCollectorLeaseNotCurrent)
	third, err := registry.Activate(durableStreamLease(key, "boot-c", "stream-c", 3))
	if err != nil {
		t.Fatal(err)
	}
	assertHeartbeatAdmission(t, registry, third, start.Add(2*interval-1), interval, false, nil)
	assertHeartbeatAdmission(t, registry, third, start.Add(2*interval), interval, true, nil)
}

func TestCollectorHeartbeatCadenceScopesAndFencesAdmissions(t *testing.T) {
	t.Parallel()
	registry := NewInMemoryCollectorStreamRegistry()
	start := time.Now()
	interval := time.Second
	for _, key := range []CollectorStreamKey{
		{TenantID: "tenant-a", CollectorID: "collector-a"},
		{TenantID: "tenant-a", CollectorID: "collector-b"},
		{TenantID: "tenant-b", CollectorID: "collector-a"},
	} {
		lease, err := registry.Activate(durableStreamLease(key, "boot-a", "stream-a", 1))
		if err != nil {
			t.Fatal(err)
		}
		forged := lease
		forged.Superseded = make(chan struct{})
		assertHeartbeatAdmission(t, registry, forged, start, interval, false, ErrCollectorLeaseNotCurrent)
		assertHeartbeatAdmission(t, registry, lease, start, interval, true, nil)
		assertHeartbeatAdmission(t, registry, lease, start.Add(-time.Hour), interval, false, nil)
		assertHeartbeatAdmission(t, registry, lease, start.Add(interval-1), interval, false, nil)
		assertHeartbeatAdmission(t, registry, lease, start.Add(interval), interval, true, nil)
	}
}

func TestCollectorHeartbeatCadenceConcurrentAdmissionReservesOneSlot(t *testing.T) {
	t.Parallel()
	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	lease, err := registry.Activate(durableStreamLease(key, "boot-a", "stream-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var admitted atomic.Uint32
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			allowed, err := registry.AdmitHeartbeat(lease, start, time.Nanosecond)
			if err != nil {
				t.Errorf("admission failed: %v", err)
			}
			if allowed {
				admitted.Add(1)
			}
		})
	}
	workers.Wait()
	if got := admitted.Load(); got != 1 {
		t.Fatalf("admitted %d concurrent heartbeats, want 1", got)
	}
	assertHeartbeatAdmission(t, registry, lease, start.Add(time.Nanosecond), time.Nanosecond, true, nil)
	if allowed, err := registry.AdmitHeartbeat(lease, start.Add(2*time.Nanosecond), 0); allowed || err == nil {
		t.Fatalf("zero interval = %t, %v; want rejection", allowed, err)
	}
	assertHeartbeatAdmission(t, registry, lease, start.Add(2*time.Nanosecond), time.Nanosecond, true, nil)
}

func assertHeartbeatAdmission(
	t *testing.T,
	registry *InMemoryCollectorStreamRegistry,
	lease CollectorStreamLease,
	now time.Time,
	interval time.Duration,
	wantAllowed bool,
	wantErr error,
) {
	t.Helper()
	allowed, err := registry.AdmitHeartbeat(lease, now, interval)
	if allowed != wantAllowed || !errors.Is(err, wantErr) {
		t.Fatalf("heartbeat admission = (%t, %v), want (%t, %v)", allowed, err, wantAllowed, wantErr)
	}
}
