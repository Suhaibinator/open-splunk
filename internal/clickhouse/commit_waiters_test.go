package clickhouse

import (
	"errors"
	"testing"
)

func TestCommitWaitersAreBoundedAndSequenceScoped(t *testing.T) {
	waiters := newCommitWaiters(2)
	first, err := waiters.register(11)
	if err != nil {
		t.Fatalf("register first waiter: %v", err)
	}
	second, err := waiters.register(12)
	if err != nil {
		t.Fatalf("register second waiter: %v", err)
	}
	if _, err := waiters.register(13); !errors.Is(err, errNativeWaiterCapacity) {
		t.Fatalf("register beyond capacity = %v, want %v", err, errNativeWaiterCapacity)
	}
	if got := waiters.notify([]uint64{11}); got != 1 {
		t.Fatalf("notified first sequence = %d, want 1", got)
	}
	select {
	case <-first.done:
	default:
		t.Fatal("first waiter was not notified")
	}
	select {
	case <-second.done:
		t.Fatal("second sequence was notified early")
	default:
	}
	third, err := waiters.register(13)
	if err != nil {
		t.Fatalf("register after notification released capacity: %v", err)
	}
	if !waiters.remove(second) || waiters.remove(second) {
		t.Fatal("waiter removal was not exactly once")
	}
	if got := waiters.notifyAll(); got != 1 {
		t.Fatalf("notifyAll = %d, want one remaining waiter", got)
	}
	select {
	case <-third.done:
	default:
		t.Fatal("remaining waiter was not notified")
	}
	if got := waiters.size(); got != 0 {
		t.Fatalf("waiter count = %d, want 0", got)
	}
}

func TestCommitWaitersRejectInvalidOrDisabledRegistration(t *testing.T) {
	for _, waiters := range []*commitWaiters{nil, newCommitWaiters(0)} {
		if _, err := waiters.register(1); !errors.Is(err, errNativeWaiterCapacity) {
			t.Fatalf("disabled register = %v, want %v", err, errNativeWaiterCapacity)
		}
	}
	waiters := newCommitWaiters(1)
	if _, err := waiters.register(0); !errors.Is(err, errNativeWaiterCapacity) {
		t.Fatalf("zero-sequence register = %v, want %v", err, errNativeWaiterCapacity)
	}
}

func TestCommitWaiterMetricIncrementPrecedesConcurrentNotification(t *testing.T) {
	waiters := newCommitWaiters(1)
	metrics := NewCoalescerMetrics()
	registered := make(chan struct{})
	releaseRegistration := make(chan struct{})
	registerDone := make(chan error, 1)
	go func() {
		_, err := waiters.registerObserved(41, func() {
			metrics.AddNativeWaiter()
			close(registered)
			<-releaseRegistration
		})
		registerDone <- err
	}()
	<-registered
	notifyStarted := make(chan struct{})
	notifyDone := make(chan uint64, 1)
	go func() {
		close(notifyStarted)
		notified := waiters.notify([]uint64{41})
		for range notified {
			metrics.RemoveNativeWaiter()
		}
		notifyDone <- notified
	}()
	<-notifyStarted
	select {
	case notified := <-notifyDone:
		t.Fatalf("notification bypassed in-progress metric registration: %d", notified)
	default:
	}
	close(releaseRegistration)
	if err := <-registerDone; err != nil {
		t.Fatalf("registerObserved: %v", err)
	}
	if notified := <-notifyDone; notified != 1 {
		t.Fatalf("notified waiters = %d, want 1", notified)
	}
	if snapshot := metrics.Snapshot(); snapshot.NativeWaiters != 0 ||
		snapshot.PeakNativeWaiters != 1 {
		t.Fatalf("waiter metrics after concurrent notification = %#v", snapshot)
	}
}
