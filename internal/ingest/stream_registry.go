package ingest

import (
	"errors"
	"math"
	"sync"
)

// CollectorStreamKey is the trusted identity scope for one active native
// collector stream. Both fields come from authenticated authorization state,
// never from CollectorHello.
type CollectorStreamKey struct {
	TenantID    string
	CollectorID string
}

// CollectorStreamLease proves that one handler held the active process-local
// generation for a collector when it crossed a request-admission boundary.
type CollectorStreamLease struct {
	Key        CollectorStreamKey
	Generation uint64
	StreamID   string
	Superseded <-chan struct{}
}

// CollectorStreamRegistry fences simultaneous native collector connections.
// Claim uses last-valid-claim-wins semantics, and Release must be conditional
// on the complete lease so delayed cleanup cannot remove a successor.
type CollectorStreamRegistry interface {
	Claim(CollectorStreamKey, string) (CollectorStreamLease, error)
	IsCurrent(CollectorStreamLease) bool
	Release(CollectorStreamLease)
}

type collectorStreamEntry struct {
	lease      CollectorStreamLease
	superseded chan struct{}
}

// InMemoryCollectorStreamRegistry is a concurrency-safe process-local
// implementation. Generations are globally monotonic within this registry,
// which also makes them monotonic for every individual collector key without
// retaining inactive keys.
type InMemoryCollectorStreamRegistry struct {
	mu             sync.Mutex
	nextGeneration uint64
	current        map[CollectorStreamKey]collectorStreamEntry
}

// NewInMemoryCollectorStreamRegistry constructs an empty stream registry.
func NewInMemoryCollectorStreamRegistry() *InMemoryCollectorStreamRegistry {
	return &InMemoryCollectorStreamRegistry{
		current: make(map[CollectorStreamKey]collectorStreamEntry),
	}
}

// Claim installs a fresh active lease and wakes the previous handler, if any.
func (r *InMemoryCollectorStreamRegistry) Claim(
	key CollectorStreamKey,
	streamID string,
) (CollectorStreamLease, error) {
	if key.TenantID == "" || key.CollectorID == "" || streamID == "" {
		return CollectorStreamLease{}, errors.New("collector stream claim is incomplete")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextGeneration == math.MaxUint64 {
		return CollectorStreamLease{}, errors.New("collector stream generation is exhausted")
	}
	r.nextGeneration++
	superseded := make(chan struct{})
	lease := CollectorStreamLease{
		Key:        key,
		Generation: r.nextGeneration,
		StreamID:   streamID,
		Superseded: superseded,
	}
	if previous, ok := r.current[key]; ok {
		close(previous.superseded)
	}
	r.current[key] = collectorStreamEntry{
		lease:      lease,
		superseded: superseded,
	}
	return lease, nil
}

// IsCurrent reports whether lease is still the authoritative active
// generation for its trusted collector key.
func (r *InMemoryCollectorStreamRegistry) IsCurrent(lease CollectorStreamLease) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.current[lease.Key]
	return ok &&
		current.lease.Generation == lease.Generation &&
		current.lease.StreamID == lease.StreamID
}

// Release removes lease only when it is still current.
func (r *InMemoryCollectorStreamRegistry) Release(lease CollectorStreamLease) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.current[lease.Key]
	if !ok ||
		current.lease.Generation != lease.Generation ||
		current.lease.StreamID != lease.StreamID {
		return
	}
	delete(r.current, lease.Key)
}

var _ CollectorStreamRegistry = (*InMemoryCollectorStreamRegistry)(nil)
