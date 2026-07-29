package searchjobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestContextSnapshotReadsCancelWhileWaitingForReadCapacity(t *testing.T) {
	tests := []struct {
		name string
		read func(context.Context, *Manager) error
	}{
		{
			name: "metadata",
			read: func(ctx context.Context, manager *Manager) error {
				_, err := manager.GetForContext(
					ctx,
					AccessScope{TenantID: "tenant", OwnerID: "owner"},
					"missing",
				)
				return err
			},
		},
		{
			name: "preview",
			read: func(ctx context.Context, manager *Manager) error {
				_, err := manager.PreviewForBytesContext(
					ctx,
					AccessScope{TenantID: "tenant", OwnerID: "owner"},
					"missing",
					1,
					1,
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(
					context.Context,
					clickhouse.CompiledQuery,
					ResultSink,
				) error {
					return nil
				}),
				MaxConcurrentReads: 1,
				CleanupInterval:    -1,
			})
			manager.readGate <- struct{}{}
			gateHeld := true
			defer func() {
				if gateHeld {
					<-manager.readGate
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				close(started)
				result <- test.read(ctx, manager)
			}()
			<-started
			waitForActiveContextReads(t, manager, 1)
			cancel()

			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("contextual read error = %v, want context canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("contextual read remained blocked behind saturated capacity")
			}
			waitForActiveContextReads(t, manager, 0)
			if got := len(manager.readGate); got != 1 {
				t.Fatalf("read gate occupancy = %d, want held capacity token only", got)
			}
			<-manager.readGate
			gateHeld = false
		})
	}
}

func TestContextSnapshotReadHonorsCancellationAfterReadAdmission(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		MaxConcurrentReads: 1,
		CleanupInterval:    -1,
	})
	entryContext, cancelEntry := context.WithCancel(manager.ctx)
	defer cancelEntry()
	entry := &jobEntry{
		job: Job{
			ID:       "locked-snapshot",
			TenantID: "tenant",
			OwnerID:  "owner",
			State:    StateQueued,
		},
		ctx:    entryContext,
		cancel: cancelEntry,
	}
	manager.mu.Lock()
	manager.jobs[entry.job.ID] = entry
	manager.mu.Unlock()
	t.Cleanup(func() {
		manager.mu.Lock()
		delete(manager.jobs, entry.job.ID)
		manager.mu.Unlock()
	})

	entry.mu.Lock()
	entryLocked := true
	defer func() {
		if entryLocked {
			entry.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := manager.GetForContext(
			ctx,
			AccessScope{TenantID: "tenant", OwnerID: "owner"},
			entry.job.ID,
		)
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(manager.readGate) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("contextual read did not acquire read capacity")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		t.Fatalf("GetForContext returned before its admitted entry lock was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	entry.mu.Unlock()
	entryLocked = false
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetForContext() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetForContext did not finish after the entry lock was released")
	}
	waitForActiveContextReads(t, manager, 0)
	if got := len(manager.readGate); got != 0 {
		t.Fatalf("read gate occupancy = %d, want 0", got)
	}
}

func TestManagerCloseCancelsContextSnapshotWaitingForReadCapacity(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		MaxConcurrentReads: 1,
		CleanupInterval:    -1,
	})
	manager.readGate <- struct{}{}
	gateHeld := true
	defer func() {
		if gateHeld {
			<-manager.readGate
		}
	}()

	readResult := make(chan error, 1)
	go func() {
		_, err := manager.GetForContext(
			context.Background(),
			AccessScope{TenantID: "tenant", OwnerID: "owner"},
			"missing",
		)
		readResult <- err
	}()
	waitForActiveContextReads(t, manager, 1)

	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.Close() }()
	select {
	case err := <-readResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetForContext() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager shutdown did not cancel the capacity-blocked read")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after the contextual read exited")
	}
	if got := len(manager.readGate); got != 1 {
		t.Fatalf("read gate occupancy after close = %d, want held capacity token only", got)
	}
	<-manager.readGate
	gateHeld = false
}

func waitForActiveContextReads(t *testing.T, manager *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.RLock()
		active := manager.activeOperations
		manager.mu.RUnlock()
		if active == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("active contextual reads = %d, want %d", active, want)
		}
		time.Sleep(time.Millisecond)
	}
}
