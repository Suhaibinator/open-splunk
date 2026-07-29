package ingest

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func TestStreamAdmissionsBoundDistinctKeysAndPromoteOnlyNewestAttempt(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	service := &Service{
		config:         Config{MaxStreamsPerSubject: 1},
		streamRegistry: registry,
		admissions:     make(map[string]map[CollectorStreamKey]collectorStreamAdmissionEntry),
	}
	subject := authorizationSubjectKey("subject-a")
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	first, ok := service.acquireStreamAdmission(subject, key)
	if !ok {
		t.Fatal("first admission was rejected")
	}
	if _, secondOK := service.acquireStreamAdmission(
		subject,
		CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-b"},
	); secondOK {
		t.Fatal("distinct collector bypassed per-subject admission limit")
	}
	second, ok := service.acquireStreamAdmission(subject, key)
	if !ok {
		t.Fatal("same-key replacement admission was rejected")
	}
	select {
	case <-first.Superseded:
	default:
		t.Fatal("same-key replacement did not supersede the older admission")
	}
	service.releaseStreamAdmission(first)
	if _, err := service.promoteStreamAdmission(first, "stream-old"); !errors.Is(err, errStreamAdmissionSuperseded) {
		t.Fatalf("stale promotion error = %v, want superseded", err)
	}
	lease, err := service.promoteStreamAdmission(second, "stream-current")
	if err != nil {
		t.Fatal(err)
	}
	if !registry.IsCurrent(lease) {
		t.Fatal("newest admission did not become the active lease")
	}
	service.releaseStreamAdmission(second)
	if !registry.IsCurrent(lease) {
		t.Fatal("promoted admission cleanup released its active lease")
	}
	registry.Release(lease)
}

func TestInMemoryCollectorStreamRegistrySupersedesAndConditionallyReleases(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}

	first, err := registry.Claim(key, "stream-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation == 0 || first.StreamID != "stream-a" || !registry.IsCurrent(first) {
		t.Fatalf("first lease = %#v, current = %t", first, registry.IsCurrent(first))
	}

	second, err := registry.Claim(key, "stream-b")
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation || !registry.IsCurrent(second) || registry.IsCurrent(first) {
		t.Fatalf("leases after takeover: first=%#v second=%#v", first, second)
	}
	select {
	case <-first.Superseded:
	case <-time.After(time.Second):
		t.Fatal("successor claim did not wake the prior lease")
	}
	select {
	case <-second.Superseded:
		t.Fatal("successor lease was spuriously superseded")
	default:
	}

	registry.Release(first)
	if !registry.IsCurrent(second) {
		t.Fatal("stale conditional release deleted its successor")
	}
	registry.Release(second)
	if registry.IsCurrent(second) {
		t.Fatal("current lease remained after release")
	}

	third, err := registry.Claim(key, "stream-a")
	if err != nil {
		t.Fatal(err)
	}
	if third.Generation <= second.Generation {
		t.Fatalf("generation after inactive interval = %d, want > %d", third.Generation, second.Generation)
	}
}

func TestInMemoryCollectorStreamRegistryScopesLeasesByTenantAndCollector(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	keys := []CollectorStreamKey{
		{TenantID: "tenant-a", CollectorID: "collector-a"},
		{TenantID: "tenant-a", CollectorID: "collector-b"},
		{TenantID: "tenant-b", CollectorID: "collector-a"},
	}
	leases := make([]CollectorStreamLease, 0, len(keys))
	for index, key := range keys {
		lease, err := registry.Claim(key, "stream-"+string(rune('a'+index)))
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	for _, lease := range leases {
		if !registry.IsCurrent(lease) {
			t.Fatalf("independent lease is not current: %#v", lease)
		}
		select {
		case <-lease.Superseded:
			t.Fatalf("independent lease was superseded: %#v", lease)
		default:
		}
	}
}

func TestInMemoryCollectorStreamRegistryConcurrentClaimsLeaveOneCurrent(t *testing.T) {
	t.Parallel()

	const claims = 64
	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	leases := make([]CollectorStreamLease, claims)
	errs := make([]error, claims)
	var ready sync.WaitGroup
	var start sync.WaitGroup
	ready.Add(claims)
	start.Add(1)
	var workers sync.WaitGroup
	workers.Add(claims)
	for index := range claims {
		go func() {
			defer workers.Done()
			ready.Done()
			start.Wait()
			leases[index], errs[index] = registry.Claim(key, "stream")
		}()
	}
	ready.Wait()
	start.Done()
	workers.Wait()

	current := 0
	var highest uint64
	generations := make(map[uint64]struct{}, claims)
	for index, lease := range leases {
		if errs[index] != nil {
			t.Fatalf("Claim(%d): %v", index, errs[index])
		}
		if _, duplicate := generations[lease.Generation]; duplicate {
			t.Fatalf("duplicate generation %d", lease.Generation)
		}
		generations[lease.Generation] = struct{}{}
		if lease.Generation > highest {
			highest = lease.Generation
		}
	}
	for _, lease := range leases {
		if registry.IsCurrent(lease) {
			current++
			if lease.Generation != highest {
				t.Fatalf("current generation = %d, highest = %d", lease.Generation, highest)
			}
		}
	}
	if current != 1 {
		t.Fatalf("current leases = %d, want 1", current)
	}
}

func TestInMemoryCollectorStreamRegistryRejectsInvalidClaimsAndExhaustion(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	for _, test := range []struct {
		name     string
		key      CollectorStreamKey
		streamID string
	}{
		{name: "empty tenant", key: CollectorStreamKey{CollectorID: "collector-a"}, streamID: "stream-a"},
		{name: "empty collector", key: CollectorStreamKey{TenantID: "tenant-a"}, streamID: "stream-a"},
		{name: "empty stream", key: CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := registry.Claim(test.key, test.streamID); err == nil {
				t.Fatal("Claim() error = nil")
			}
		})
	}

	registry.nextGeneration = math.MaxUint64
	if _, err := registry.Claim(
		CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
		"stream-a",
	); err == nil {
		t.Fatal("Claim() at generation exhaustion error = nil")
	}
}

func TestReceiveCollectRequestsStopsAfterStreamContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	receiver := blockingCollectRequestReceiver{ctx: ctx}
	received, stop := receiveCollectRequests(receiver)
	stop()

	select {
	case <-received:
		t.Fatal("receive pump exited before the blocked Recv was canceled")
	default:
	}
	cancel()
	select {
	case _, ok := <-received:
		if ok {
			t.Fatal("stopped receive pump forwarded the cancellation result")
		}
	case <-time.After(time.Second):
		t.Fatal("receive pump did not exit after stream context cancellation")
	}
}

type blockingCollectRequestReceiver struct {
	ctx context.Context
}

func (receiver blockingCollectRequestReceiver) Recv() (*opensplunkv1.CollectRequest, error) {
	<-receiver.ctx.Done()
	return nil, receiver.ctx.Err()
}
