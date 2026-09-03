package clickhouse

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCommitWaitersNotifyEveryWaiterAndReleaseCapacity(t *testing.T) {
	t.Parallel()
	waiters := newCommitWaiters(3)
	first, cancelFirst, err := waiters.register(7)
	if err != nil {
		t.Fatal(err)
	}
	second, cancelSecond, err := waiters.register(7)
	if err != nil {
		t.Fatal(err)
	}
	third, cancelThird, err := waiters.register(9)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelFirst()
	defer cancelSecond()
	defer cancelThird()

	if _, _, err := waiters.register(10); !errors.Is(err, errCommitWaiterCapacity) {
		t.Fatalf("register above capacity error = %v, want errCommitWaiterCapacity", err)
	}
	if got := waiters.notify([]uint64{7}); got != 2 {
		t.Fatalf("notify count = %d, want 2", got)
	}
	for index, ready := range []<-chan struct{}{first, second} {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d was not notified", index)
		}
	}
	select {
	case <-third:
		t.Fatal("unrelated sequence waiter was notified")
	default:
	}
	if got := waiters.size(); got != 1 {
		t.Fatalf("size after notification = %d, want 1", got)
	}
	if _, cancel, err := waiters.register(10); err != nil {
		t.Fatalf("register after notification: %v", err)
	} else {
		cancel()
	}
}

func TestCommitWaiterCancelIsIdempotentAndRaceSafeWithNotify(t *testing.T) {
	t.Parallel()
	waiters := newCommitWaiters(1)
	for range 1_000 {
		ready, cancel, err := waiters.register(11)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			cancel()
			cancel()
		}()
		go func() {
			defer group.Done()
			<-start
			waiters.notify([]uint64{11, 11})
		}()
		close(start)
		group.Wait()
		select {
		case <-ready:
		default:
		}
		if got := waiters.size(); got != 0 {
			t.Fatalf("size after cancel/notify race = %d, want 0", got)
		}
	}
}

func TestCommitWaitersRejectInvalidConstructionAndSequence(t *testing.T) {
	t.Parallel()
	if _, _, err := newCommitWaiters(0).register(1); !errors.Is(err, errCommitWaiterCapacity) {
		t.Fatalf("zero-capacity register error = %v, want errCommitWaiterCapacity", err)
	}
	if _, _, err := newCommitWaiters(1).register(0); err == nil {
		t.Fatal("zero-sequence register succeeded")
	}
	var nilWaiters *commitWaiters
	if _, _, err := nilWaiters.register(1); err == nil {
		t.Fatal("nil registry register succeeded")
	}
}
