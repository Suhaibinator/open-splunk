package ingest

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
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
	if _, err := service.promoteStreamAdmission(
		first,
		durableStreamLease(key, "boot-a", "stream-old", 1),
	); !errors.Is(err, errStreamAdmissionSuperseded) {
		t.Fatalf("stale promotion error = %v, want superseded", err)
	}
	lease, err := service.promoteStreamAdmission(
		second,
		durableStreamLease(key, "boot-a", "stream-current", 2),
	)
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

	firstDurable := durableStreamLease(key, "boot-a", "stream-a", 41)
	first, err := registry.Activate(firstDurable)
	if err != nil {
		t.Fatal(err)
	}
	if first.Lease != firstDurable || !registry.IsCurrent(first) {
		t.Fatalf("first lease = %#v, current = %t", first, registry.IsCurrent(first))
	}

	secondDurable := durableStreamLease(key, "boot-b", "stream-b", 42)
	second, err := registry.Activate(secondDurable)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lease != secondDurable ||
		!registry.IsCurrent(second) ||
		registry.IsCurrent(first) {
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

	for name, stale := range map[string]CollectorStreamLease{
		"prior activation": first,
		"wrong boot": {
			Lease: collectorfleet.Lease{
				Scope:       second.Scope,
				CollectorID: second.CollectorID,
				BootEpoch:   "boot-c",
				StreamID:    second.StreamID,
				Generation:  second.Generation,
			},
			Superseded: second.Superseded,
		},
		"wrong stream": {
			Lease: collectorfleet.Lease{
				Scope:       second.Scope,
				CollectorID: second.CollectorID,
				BootEpoch:   second.BootEpoch,
				StreamID:    "stream-c",
				Generation:  second.Generation,
			},
			Superseded: second.Superseded,
		},
		"wrong generation": {
			Lease: collectorfleet.Lease{
				Scope:       second.Scope,
				CollectorID: second.CollectorID,
				BootEpoch:   second.BootEpoch,
				StreamID:    second.StreamID,
				Generation:  second.Generation + 1,
			},
			Superseded: second.Superseded,
		},
		"forged activation token": {
			Lease:      second.Lease,
			Superseded: make(chan struct{}),
		},
	} {
		if registry.IsCurrent(stale) {
			t.Fatalf("%s lease was spuriously current", name)
		}
		registry.Release(stale)
		if !registry.IsCurrent(second) {
			t.Fatalf("%s conditional release deleted its successor", name)
		}
	}
	registry.Release(second)
	if registry.IsCurrent(second) {
		t.Fatal("current lease remained after release")
	}

	thirdDurable := durableStreamLease(key, "boot-c", "stream-c", 43)
	third, err := registry.Activate(thirdDurable)
	if err != nil {
		t.Fatal(err)
	}
	if third.Lease != thirdDurable || !registry.IsCurrent(third) {
		t.Fatalf("third lease = %#v, current = %t", third, registry.IsCurrent(third))
	}
	select {
	case <-second.Superseded:
		t.Fatal("new activation canceled a handler that had already released")
	default:
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
		lease, err := registry.Activate(durableStreamLease(
			key,
			"boot-"+string(rune('a'+index)),
			"stream-"+string(rune('a'+index)),
			1,
		))
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

func TestInMemoryCollectorStreamRegistryConcurrentActivationsLeaveHighestCurrent(t *testing.T) {
	t.Parallel()

	const activations = 64
	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	leases := make([]CollectorStreamLease, activations)
	errs := make([]error, activations)
	var ready sync.WaitGroup
	var start sync.WaitGroup
	ready.Add(activations)
	start.Add(1)
	var workers sync.WaitGroup
	workers.Add(activations)
	for index := range activations {
		go func() {
			defer workers.Done()
			ready.Done()
			start.Wait()
			generation := uint64(index + 1)
			leases[index], errs[index] = registry.Activate(durableStreamLease(
				key,
				"boot-a",
				"stream-"+strconv.Itoa(index+1),
				generation,
			))
		}()
	}
	ready.Wait()
	start.Done()
	workers.Wait()

	current := 0
	var highestSuccessful uint64
	for index, lease := range leases {
		generation := uint64(index + 1)
		if errors.Is(errs[index], ErrCollectorStreamActivationStale) {
			continue
		}
		if errs[index] != nil {
			t.Fatalf("Activate(%d): %v", generation, errs[index])
		}
		if lease.Generation != generation {
			t.Fatalf("Activate(%d) installed generation %d", generation, lease.Generation)
		}
		if lease.Generation > highestSuccessful {
			highestSuccessful = lease.Generation
		}
	}
	if highestSuccessful != activations {
		t.Fatalf("highest successful generation = %d, want %d", highestSuccessful, activations)
	}
	for _, lease := range leases {
		if lease.Generation == 0 {
			continue
		}
		if registry.IsCurrent(lease) {
			current++
			if lease.Generation != activations {
				t.Fatalf("current generation = %d, highest = %d", lease.Generation, activations)
			}
		} else {
			select {
			case <-lease.Superseded:
			default:
				t.Fatalf("successful generation %d is neither current nor canceled", lease.Generation)
			}
		}
	}
	if current != 1 {
		t.Fatalf("current leases = %d, want 1", current)
	}
}

func TestInMemoryCollectorStreamRegistryRejectsIncompleteOrUnsupportedLeases(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	for _, test := range []struct {
		name  string
		lease collectorfleet.Lease
	}{
		{
			name: "empty tenant",
			lease: durableStreamLease(
				CollectorStreamKey{CollectorID: "collector-a"},
				"boot-a",
				"stream-a",
				1,
			),
		},
		{
			name: "empty collector",
			lease: durableStreamLease(
				CollectorStreamKey{TenantID: "tenant-a"},
				"boot-a",
				"stream-a",
				1,
			),
		},
		{
			name: "empty boot",
			lease: durableStreamLease(
				CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
				"",
				"stream-a",
				1,
			),
		},
		{
			name: "empty stream",
			lease: durableStreamLease(
				CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
				"boot-a",
				"",
				1,
			),
		},
		{
			name: "zero generation",
			lease: durableStreamLease(
				CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
				"boot-a",
				"stream-a",
				0,
			),
		},
		{
			name: "generation outside durable range",
			lease: durableStreamLease(
				CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"},
				"boot-a",
				"stream-a",
				uint64(math.MaxInt64)+1,
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := registry.Activate(test.lease); err == nil {
				t.Fatal("Activate() error = nil")
			}
		})
	}
}

func TestInMemoryCollectorStreamRegistryRejectsReverseCompletionAndEqualGeneration(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	key := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	newerDurable := durableStreamLease(key, "boot-a", "stream-new", 2)
	newer, err := registry.Activate(newerDurable)
	if err != nil {
		t.Fatal(err)
	}

	olderDurable := durableStreamLease(key, "boot-a", "stream-old", 1)
	if _, err := registry.Activate(olderDurable); !errors.Is(
		err,
		ErrCollectorStreamActivationStale,
	) {
		t.Fatalf("reverse-completed older activation error = %v, want stale", err)
	}
	if !registry.IsCurrent(newer) {
		t.Fatal("reverse-completed older activation displaced the newer handler")
	}
	select {
	case <-newer.Superseded:
		t.Fatal("rejected older activation canceled the newer handler")
	default:
	}

	for name, equal := range map[string]collectorfleet.Lease{
		"identical": newerDurable,
		"different stream": durableStreamLease(
			key,
			"boot-a",
			"stream-conflict",
			newer.Generation,
		),
		"different boot": durableStreamLease(
			key,
			"boot-b",
			newer.StreamID,
			newer.Generation,
		),
	} {
		if _, err := registry.Activate(equal); !errors.Is(
			err,
			ErrCollectorStreamActivationConflict,
		) {
			t.Fatalf("%s equal-generation activation error = %v, want conflict", name, err)
		}
		if !registry.IsCurrent(newer) {
			t.Fatalf("%s equal-generation activation displaced the newer handler", name)
		}
	}
	select {
	case <-newer.Superseded:
		t.Fatal("rejected equal-generation activation canceled the current handler")
	default:
	}

	registry.Release(newer)
	if _, err := registry.Activate(olderDurable); !errors.Is(
		err,
		ErrCollectorStreamActivationStale,
	) {
		t.Fatalf("older activation after release error = %v, want stale", err)
	}
	if _, err := registry.Activate(newerDurable); !errors.Is(
		err,
		ErrCollectorStreamActivationConflict,
	) {
		t.Fatalf("equal activation after release error = %v, want conflict", err)
	}
}

func TestInMemoryCollectorStreamRegistryCancelsOnlyPreviousHandlerForKey(t *testing.T) {
	t.Parallel()

	registry := NewInMemoryCollectorStreamRegistry()
	firstKey := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-a"}
	otherKey := CollectorStreamKey{TenantID: "tenant-a", CollectorID: "collector-b"}
	first, err := registry.Activate(durableStreamLease(
		firstKey,
		"boot-a",
		"stream-a",
		1,
	))
	if err != nil {
		t.Fatal(err)
	}
	other, err := registry.Activate(durableStreamLease(
		otherKey,
		"boot-a",
		"stream-other",
		1,
	))
	if err != nil {
		t.Fatal(err)
	}
	successor, err := registry.Activate(durableStreamLease(
		firstKey,
		"boot-a",
		"stream-b",
		2,
	))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-first.Superseded:
	case <-time.After(time.Second):
		t.Fatal("successor did not cancel the exact previous handler")
	}
	select {
	case <-other.Superseded:
		t.Fatal("successor canceled a handler for another collector")
	default:
	}
	if !registry.IsCurrent(successor) || !registry.IsCurrent(other) {
		t.Fatal("independent current handlers were not preserved")
	}
}

func durableStreamLease(
	key CollectorStreamKey,
	bootEpoch string,
	streamID string,
	generation uint64,
) collectorfleet.Lease {
	return collectorfleet.Lease{
		TenantID:    key.TenantID,
		CollectorID: key.CollectorID,
		BootEpoch:   bootEpoch,
		StreamID:    streamID,
		Generation:  generation,
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

func (receiver blockingCollectRequestReceiver) Recv() (*opensplunk.CollectRequest, error) {
	<-receiver.ctx.Done()
	return nil, receiver.ctx.Err()
}
