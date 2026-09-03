package clickhouse

import (
	"errors"
	"sync"
)

var errNativeWaiterCapacity = errors.New("ClickHouse native commit waiter capacity reached")

// commitWaiters is a bounded, process-local latency optimization. Durable
// reservation state remains authoritative: a waiter may disappear at any time
// without changing whether a batch is accepted, committed, or replayable.
type commitWaiters struct {
	mu       sync.Mutex
	bySeq    map[uint64]map[*commitWaiter]struct{}
	count    uint32
	maxCount uint32
}

type commitWaiter struct {
	sequence uint64
	done     chan struct{}
}

func newCommitWaiters(maxCount uint32) *commitWaiters {
	return &commitWaiters{
		bySeq:    make(map[uint64]map[*commitWaiter]struct{}),
		maxCount: maxCount,
	}
}

func (waiters *commitWaiters) register(sequence uint64) (*commitWaiter, error) {
	return waiters.registerObserved(sequence, nil)
}

func (waiters *commitWaiters) registerObserved(
	sequence uint64,
	onRegistered func(),
) (*commitWaiter, error) {
	if waiters == nil || sequence == 0 || waiters.maxCount == 0 {
		return nil, errNativeWaiterCapacity
	}
	waiters.mu.Lock()
	defer waiters.mu.Unlock()
	if waiters.count >= waiters.maxCount {
		return nil, errNativeWaiterCapacity
	}
	waiter := &commitWaiter{sequence: sequence, done: make(chan struct{})}
	sequenceWaiters := waiters.bySeq[sequence]
	if sequenceWaiters == nil {
		sequenceWaiters = make(map[*commitWaiter]struct{})
		waiters.bySeq[sequence] = sequenceWaiters
	}
	sequenceWaiters[waiter] = struct{}{}
	waiters.count++
	// Keep external waiter gauges ordered with registry visibility. A notifier
	// cannot remove this waiter until its corresponding increment has completed.
	if onRegistered != nil {
		onRegistered()
	}
	return waiter, nil
}

func (waiters *commitWaiters) remove(waiter *commitWaiter) bool {
	if waiters == nil || waiter == nil {
		return false
	}
	waiters.mu.Lock()
	defer waiters.mu.Unlock()
	sequenceWaiters := waiters.bySeq[waiter.sequence]
	if _, found := sequenceWaiters[waiter]; !found {
		return false
	}
	delete(sequenceWaiters, waiter)
	waiters.count--
	if len(sequenceWaiters) == 0 {
		delete(waiters.bySeq, waiter.sequence)
	}
	return true
}

func (waiters *commitWaiters) notify(sequences []uint64) uint64 {
	if waiters == nil || len(sequences) == 0 {
		return 0
	}
	waiters.mu.Lock()
	var notified uint64
	for _, sequence := range sequences {
		sequenceWaiters := waiters.bySeq[sequence]
		if len(sequenceWaiters) == 0 {
			continue
		}
		delete(waiters.bySeq, sequence)
		for waiter := range sequenceWaiters {
			close(waiter.done)
			waiters.count--
			notified++
		}
	}
	waiters.mu.Unlock()
	return notified
}

func (waiters *commitWaiters) notifyAll() uint64 {
	if waiters == nil {
		return 0
	}
	waiters.mu.Lock()
	var notified uint64
	for sequence, sequenceWaiters := range waiters.bySeq {
		delete(waiters.bySeq, sequence)
		for waiter := range sequenceWaiters {
			close(waiter.done)
			notified++
		}
	}
	waiters.count = 0
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
