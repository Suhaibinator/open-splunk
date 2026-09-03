package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/indexes"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

// indexDataDeletionRuntimeStore is the single native ClickHouse Store shared
// by ingestion and physical index deletion. The runtime owns its final close.
type indexDataDeletionRuntimeStore interface {
	indexes.DeletionStore
	Shutdown(context.Context) error
	Close() error
}

// indexDataDeletionRuntime owns exactly one physical-deletion coordinator and
// the native Store borrowed by that worker. HTTP admission remains separate:
// composing this recovery runtime does not enable the DELETE_DATA endpoint.
type indexDataDeletionRuntime struct {
	coordinator *indexes.IndexDataDeletionCoordinator
	store       indexDataDeletionRuntimeStore

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	lifecycleMu sync.RWMutex
	closing     bool
}

func newIndexDataDeletionRuntime(
	controlPlane indexes.DeletionControl,
	store indexDataDeletionRuntimeStore,
	readRetirement indexread.Retirement,
	tenantID string,
	onError func(error),
) (*indexDataDeletionRuntime, error) {
	coordinator, err := indexes.NewIndexDataDeletionCoordinator(
		controlPlane,
		store,
		indexes.IndexDataDeletionCoordinatorConfig{
			TenantID:       tenantID,
			ReadRetirement: readRetirement,
			OnError:        onError,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create index data deletion coordinator: %w",
			err,
		)
	}
	return &indexDataDeletionRuntime{
		coordinator: coordinator,
		store:       store,
		closeDone:   make(chan struct{}),
	}, nil
}

// Wake requests prompt reconciliation after HTTP has durably admitted new
// work. The coordinator coalesces requests and remains safe during or after
// shutdown; its periodic scan is still the recovery backstop.
func (runtime *indexDataDeletionRuntime) Wake() {
	if runtime == nil || runtime.coordinator == nil {
		return
	}
	runtime.lifecycleMu.RLock()
	defer runtime.lifecycleMu.RUnlock()
	if runtime.closing {
		return
	}
	runtime.coordinator.Wake()
}

// Close starts one asynchronous shutdown that joins the coordinator before
// closing the shared Store. A caller deadline remains effective even if a
// worker or driver close blocks; later callers wait for the same final result.
func (runtime *indexDataDeletionRuntime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return errors.New(
			"index data deletion runtime close context is required",
		)
	}
	runtime.closeOnce.Do(func() {
		runtime.lifecycleMu.Lock()
		runtime.closing = true
		runtime.lifecycleMu.Unlock()
		go runtime.close(ctx)
	})
	select {
	case <-runtime.closeDone:
		return runtime.closeErr
	default:
	}
	select {
	case <-runtime.closeDone:
		return runtime.closeErr
	case <-ctx.Done():
		select {
		case <-runtime.closeDone:
			return runtime.closeErr
		default:
		}
		return fmt.Errorf(
			"close index data deletion runtime: %w",
			ctx.Err(),
		)
	}
}

func (runtime *indexDataDeletionRuntime) close(ctx context.Context) {
	defer close(runtime.closeDone)
	if err := runtime.coordinator.Close(context.Background()); err != nil {
		runtime.closeErr = fmt.Errorf(
			"close index data deletion coordinator: %w",
			err,
		)
		return
	}
	shutdownErr := runtime.store.Shutdown(ctx)
	closeErr := runtime.store.Close()
	if ctx.Err() != nil && errors.Is(shutdownErr, ctx.Err()) {
		// Close force-joins the Store's already-started shutdown. Read its final
		// result once more without the initiating caller's expired wait budget so
		// later joiners retain a real drain failure but not caller-local timeout.
		shutdownErr = runtime.store.Shutdown(context.Background())
		if errors.Is(shutdownErr, ctx.Err()) {
			shutdownErr = nil
		}
	}
	if err := errors.Join(shutdownErr, closeErr); err != nil {
		runtime.closeErr = fmt.Errorf(
			"close index data deletion ClickHouse store: %w",
			err,
		)
	}
}

// finalizeIndexDataDeletionRuntime gives graceful shutdown a bounded attempt,
// then retains all borrowed dependencies behind an unbounded retry when that
// budget expires. The process restores default signal handling before deferred
// finalizers run, so an operator can still force termination with a second
// signal if a dependency ignores cancellation forever.
func finalizeIndexDataDeletionRuntime(
	runtime *indexDataDeletionRuntime,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	closeErr := runtime.Close(ctx)
	retry := closeErr != nil && ctx.Err() != nil
	cancel()
	if !retry {
		return closeErr
	}
	return errors.Join(
		closeErr,
		runtime.Close(context.Background()),
	)
}
