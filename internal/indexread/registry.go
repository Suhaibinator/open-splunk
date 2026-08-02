// Package indexread coordinates process-local read admission with permanent
// index retirement. It is the runtime fence between query execution and
// physical index deletion; durable tombstone checks belong to its callers.
package indexread

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUnavailable means at least one index in a requested read scope has
	// been permanently retired in this process. Retiring an active scope also
	// uses this value as its context cancellation cause.
	ErrUnavailable = errors.New("index read is unavailable")

	// ErrInvalidArgument means read admission or retirement was requested with
	// a nil context, malformed tenant or index, empty scope, or unbounded scope.
	ErrInvalidArgument = errors.New("index read: invalid argument")
)

// Admission admits a bounded read scope until its returned release function
// is called. Callers must always release an admitted scope, including after
// the returned context is canceled.
type Admission interface {
	Acquire(
		context.Context,
		string,
		[]string,
	) (context.Context, func(), error)
}

// Retirement permanently closes read admission for one tenant/index pair,
// cancels every active overlapping scope, and waits for those scopes to be
// released.
type Retirement interface {
	Retire(context.Context, string, string) error
}

// Registry is an in-process read admission and retirement registry. Its zero
// value is ready for use. Retired keys are intentionally never removed.
type Registry struct {
	mu      sync.Mutex
	retired map[scopeKey]struct{}
	active  map[scopeKey]map[*lease]struct{}
}

type scopeKey struct {
	tenantID  string
	indexName string
}

type lease struct {
	keys   []scopeKey
	cancel context.CancelCauseFunc
	done   chan struct{}
	once   sync.Once
}

// NewRegistry returns an empty process-local read registry.
func NewRegistry() *Registry {
	return &Registry{
		retired: make(map[scopeKey]struct{}),
		active:  make(map[scopeKey]map[*lease]struct{}),
	}
}

// Acquire atomically admits all canonical index names in the tenant scope. It
// rejects the whole request if any key is retired. The input is copied,
// deduplicated, and sorted before it is retained.
func (registry *Registry) Acquire(
	ctx context.Context,
	tenantID string,
	indexNames []string,
) (context.Context, func(), error) {
	if registry == nil {
		return nil, nil, fmt.Errorf("%w: nil registry", ErrInvalidArgument)
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	scope, err := NormalizeScope(tenantID, indexNames)
	if err != nil {
		return nil, nil, err
	}
	keys := scopeKeys(scope)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	for _, key := range keys {
		if _, retired := registry.retired[key]; retired {
			return nil, nil, fmt.Errorf(
				"%w: tenant %q index %q is retired",
				ErrUnavailable,
				key.tenantID,
				key.indexName,
			)
		}
	}

	admittedContext, cancel := context.WithCancelCause(ctx)
	acquired := &lease{
		keys:   keys,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	if registry.active == nil {
		registry.active = make(map[scopeKey]map[*lease]struct{})
	}
	for _, key := range acquired.keys {
		leases := registry.active[key]
		if leases == nil {
			leases = make(map[*lease]struct{})
			registry.active[key] = leases
		}
		leases[acquired] = struct{}{}
	}

	return admittedContext, func() {
		registry.release(acquired)
	}, nil
}

// Retire permanently rejects future reads of the tenant/index pair. Context
// cancellation only bounds the drain wait: once validated, retirement and
// cancellation of active leases happen even when ctx is already canceled.
// A caller may retry a timed-out drain with a fresh context.
func (registry *Registry) Retire(
	ctx context.Context,
	tenantID string,
	indexName string,
) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrInvalidArgument)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	key, err := normalizeKey(tenantID, indexName)
	if err != nil {
		return err
	}

	registry.mu.Lock()
	if registry.retired == nil {
		registry.retired = make(map[scopeKey]struct{})
	}
	registry.retired[key] = struct{}{}
	activeSet := registry.active[key]
	active := make([]*lease, 0, len(activeSet))
	for acquired := range activeSet {
		// Cancellation is inside the registry critical section so a concurrent
		// release has a clear ordering: whichever operation obtains the lock
		// first supplies the admitted context's cancellation cause.
		acquired.cancel(ErrUnavailable)
		active = append(active, acquired)
	}
	registry.mu.Unlock()

	for _, acquired := range active {
		select {
		case <-acquired.done:
			continue
		default:
		}
		select {
		case <-acquired.done:
		case <-ctx.Done():
			// Prefer a drain that completed concurrently with cancellation.
			select {
			case <-acquired.done:
				continue
			default:
				return ctx.Err()
			}
		}
	}
	return nil
}

func (registry *Registry) release(acquired *lease) {
	acquired.once.Do(func() {
		registry.mu.Lock()
		for _, key := range acquired.keys {
			leases := registry.active[key]
			delete(leases, acquired)
			if len(leases) == 0 {
				delete(registry.active, key)
			}
		}
		close(acquired.done)
		registry.mu.Unlock()

		// Release is also the ownership boundary for resources held by the
		// derived context. A retirement cancellation that linearized first
		// retains ErrUnavailable because the first cancellation cause wins.
		acquired.cancel(context.Canceled)
	})
}

func scopeKeys(scope NormalizedScope) []scopeKey {
	keys := make([]scopeKey, len(scope.IndexNames))
	for index, indexName := range scope.IndexNames {
		keys[index] = scopeKey{
			tenantID:  scope.TenantID,
			indexName: indexName,
		}
	}
	return keys
}

func normalizeKey(tenantID, indexName string) (scopeKey, error) {
	scope, err := NormalizeScope(tenantID, []string{indexName})
	if err != nil {
		return scopeKey{}, err
	}
	return scopeKeys(scope)[0], nil
}

var (
	_ Admission  = (*Registry)(nil)
	_ Retirement = (*Registry)(nil)
)
