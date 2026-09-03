package clickhouse

import (
	"errors"
	"sync"
)

var errCommitWaiterCapacity = errors.New("ClickHouse native commit waiter capacity reached")

// commitWaiters is a bounded process-local notification index. Durable
// visibility state remains authoritative: callers always re-read SQLite after
// registering and again after a notification. A sequence may have multiple
// waiters because exact retries can overlap the original request.
type commitWaiters struct {
	mu         sync.Mutex
	bySequence map[uint64]map[*commitWaiter]struct{}
	count      uint32
	limit      uint32
}

type commitWaiter struct {
	ready chan struct{}
}

func newCommitWaiters(limit uint32) *commitWaiters {
	return &commitWaiters{
		bySequence: make(map[uint64]map[*commitWaiter]struct{}),
		limit:      limit,
	}
}

func (waiters *commitWaiters) register(sequence uint64) (<-chan struct{}, func(), error) {
	if waiters == nil || sequence == 0 {
		return nil, nil, errors.New("register ClickHouse commit waiter: positive visibility sequence is required")
	}
	waiters.mu.Lock()
	if waiters.limit == 0 || waiters.count >= waiters.limit {
		waiters.mu.Unlock()
		return nil, nil, errCommitWaiterCapacity
	}
	waiter := &commitWaiter{ready: make(chan struct{})}
	sequenceWaiters := waiters.bySequence[sequence]
	if sequenceWaiters == nil {
		sequenceWaiters = make(map[*commitWaiter]struct{})
		waiters.bySequence[sequence] = sequenceWaiters
	}
	sequenceWaiters[waiter] = struct{}{}
	waiters.count++
	waiters.mu.Unlock()

	var cancelOnce sync.Once
	return waiter.ready, func() {
		cancelOnce.Do(func() {
			waiters.remove(sequence, waiter)
		})
	}, nil
}

func (waiters *commitWaiters) remove(sequence uint64, waiter *commitWaiter) {
	waiters.mu.Lock()
	sequenceWaiters := waiters.bySequence[sequence]
	if _, exists := sequenceWaiters[waiter]; exists {
		delete(sequenceWaiters, waiter)
		waiters.count--
		if len(sequenceWaiters) == 0 {
			delete(waiters.bySequence, sequence)
		}
	}
	waiters.mu.Unlock()
}

func (waiters *commitWaiters) notify(sequences []uint64) uint32 {
	if waiters == nil {
		return 0
	}
	waiters.mu.Lock()
	var notified uint32
	for _, sequence := range sequences {
		sequenceWaiters := waiters.bySequence[sequence]
		delete(waiters.bySequence, sequence)
		for waiter := range sequenceWaiters {
			close(waiter.ready)
			notified++
			waiters.count--
		}
	}
	waiters.mu.Unlock()
	return notified
}

func (waiters *commitWaiters) size() uint32 {
	if waiters == nil {
		return 0
	}
	waiters.mu.Lock()
	defer waiters.mu.Unlock()
	return waiters.count
}
