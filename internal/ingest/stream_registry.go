package ingest

import (
	"errors"
	"math"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
)

// CollectorStreamKey is the trusted identity scope for one active native
// collector stream. Both fields come from authenticated authorization state,
// never from CollectorHello.
type CollectorStreamKey struct {
	TenantID    string
	CollectorID string
}

// CollectorStreamLease proves that one handler installed an already-committed
// durable collector lease in this process. The embedded lease is the complete
// durable fencing identity; Superseded is a process-only cancellation signal.
type CollectorStreamLease struct {
	collectorfleet.Lease
	Superseded <-chan struct{}
}

// CollectorStreamRegistry fences simultaneous native collector connections.
// Activate accepts only a durable generation newer than every generation this
// process has observed for the collector. Release is conditional on both the
// complete durable identity and the process activation token, so delayed
// cleanup cannot remove a successor.
type CollectorStreamRegistry interface {
	Activate(collectorfleet.Lease) (CollectorStreamLease, error)
	IsCurrent(CollectorStreamLease) bool
	Release(CollectorStreamLease)
}

type collectorStreamEntry struct {
	lease      CollectorStreamLease
	superseded chan struct{}
	active     bool
}

// InMemoryCollectorStreamRegistry is a concurrency-safe process-local
// implementation. It retains the greatest generation observed for each
// trusted collector key after release so a slow older admission cannot become
// current after its successor has disconnected.
type InMemoryCollectorStreamRegistry struct {
	mu      sync.RWMutex
	entries map[CollectorStreamKey]collectorStreamEntry
}

// NewInMemoryCollectorStreamRegistry constructs an empty stream registry.
func NewInMemoryCollectorStreamRegistry() *InMemoryCollectorStreamRegistry {
	return &InMemoryCollectorStreamRegistry{
		entries: make(map[CollectorStreamKey]collectorStreamEntry),
	}
}

var (
	// ErrCollectorStreamActivationStale means a newer durable generation has
	// already reached this process.
	ErrCollectorStreamActivationStale = errors.New(
		"collector stream activation generation is stale",
	)
	// ErrCollectorStreamActivationConflict means the same durable generation
	// was already activated. A second handler must never share its authority,
	// even when it presents an identical durable identity.
	ErrCollectorStreamActivationConflict = errors.New(
		"collector stream activation generation conflicts with an observed generation",
	)
)

// Activate installs an already-committed durable lease and wakes the previous
// active handler, if any. The registry never allocates lease generations.
func (r *InMemoryCollectorStreamRegistry) Activate(
	durable collectorfleet.Lease,
) (CollectorStreamLease, error) {
	if durable.TenantID == "" ||
		durable.CollectorID == "" ||
		durable.BootEpoch == "" ||
		durable.StreamID == "" ||
		durable.Generation == 0 ||
		durable.Generation > math.MaxInt64 {
		return CollectorStreamLease{}, errors.New("collector stream activation is incomplete")
	}
	key := CollectorStreamKey{
		TenantID:    durable.TenantID,
		CollectorID: durable.CollectorID,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	previous, observed := r.entries[key]
	if observed {
		switch {
		case durable.Generation < previous.lease.Generation:
			return CollectorStreamLease{}, ErrCollectorStreamActivationStale
		case durable.Generation == previous.lease.Generation:
			return CollectorStreamLease{}, ErrCollectorStreamActivationConflict
		}
	}
	superseded := make(chan struct{})
	lease := CollectorStreamLease{
		Lease:      durable,
		Superseded: superseded,
	}
	if observed && previous.active {
		close(previous.superseded)
	}
	r.entries[key] = collectorStreamEntry{
		lease:      lease,
		superseded: superseded,
		active:     true,
	}
	return lease, nil
}

// IsCurrent reports whether lease is still the authoritative active
// generation for its trusted collector key.
func (r *InMemoryCollectorStreamRegistry) IsCurrent(lease CollectorStreamLease) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := streamKey(lease.Lease)
	current, ok := r.entries[key]
	return ok && current.active && sameCollectorStreamLease(current.lease, lease)
}

// Release deactivates lease only when it is still current. Its generation
// remains as the key's high-water mark.
func (r *InMemoryCollectorStreamRegistry) Release(lease CollectorStreamLease) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := streamKey(lease.Lease)
	current, ok := r.entries[key]
	if !ok || !current.active || !sameCollectorStreamLease(current.lease, lease) {
		return
	}
	current.active = false
	r.entries[key] = current
}

func streamKey(lease collectorfleet.Lease) CollectorStreamKey {
	return CollectorStreamKey{
		TenantID:    lease.TenantID,
		CollectorID: lease.CollectorID,
	}
}

func sameCollectorStreamLease(left, right CollectorStreamLease) bool {
	return left.TenantID == right.TenantID &&
		left.CollectorID == right.CollectorID &&
		left.BootEpoch == right.BootEpoch &&
		left.StreamID == right.StreamID &&
		left.Generation == right.Generation &&
		left.Superseded == right.Superseded
}

var _ CollectorStreamRegistry = (*InMemoryCollectorStreamRegistry)(nil)
