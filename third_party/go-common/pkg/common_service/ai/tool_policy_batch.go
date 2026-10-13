package ai

import (
	"context"
	"slices"
	"sync"
)

type toolPolicyBatchKey struct{}

type toolPolicyKey struct {
	executor *ToolCallExecutor
	session  uint64
	version  uint64
}

type toolPolicyBatch struct {
	mu      sync.Mutex
	rows    map[toolPolicyKey][]SessionToolConfig
	loading map[toolPolicyKey]chan struct{}
}

// WithToolPolicyBatch starts an independent cache of successfully loaded immutable
// captured policies. Use once per continuation drive or bounded recovery sweep,
// never on a service-lifetime context. Nested batches deliberately start fresh.
// Approval, registry/schema validation, and execution decisions are never cached.
func WithToolPolicyBatch(ctx context.Context) context.Context {
	return context.WithValue(ctx, toolPolicyBatchKey{}, &toolPolicyBatch{
		rows:    make(map[toolPolicyKey][]SessionToolConfig),
		loading: make(map[toolPolicyKey]chan struct{}),
	})
}

// SessionToolConfigCachePolicy opts a reader into reuse of committed immutable
// snapshots. Readers must return false for ambient transactions or other mutable
// sources. Readers without this capability retain one read per claimed call.
type SessionToolConfigCachePolicy interface {
	CanCacheSessionToolConfigs(context.Context) bool
}

func (e *ToolCallExecutor) capturedToolConfigs(ctx context.Context, session, version uint64) ([]SessionToolConfig, error) {
	batch, _ := ctx.Value(toolPolicyBatchKey{}).(*toolPolicyBatch)
	if policy, ok := e.sessionConfig.(SessionToolConfigCachePolicy); !ok || !policy.CanCacheSessionToolConfigs(ctx) {
		batch = nil
	}
	if batch == nil {
		return e.sessionConfig.GetSessionToolConfigs(ctx, session, version)
	}
	key := toolPolicyKey{executor: e, session: session, version: version}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch.mu.Lock()
		if rows, ok := batch.rows[key]; ok {
			batch.mu.Unlock()
			return slices.Clone(rows), nil
		}
		if done, ok := batch.loading[key]; ok {
			batch.mu.Unlock()
			// Only identical snapshots share a load. Each follower retains its own
			// deadline, and a failed leader leaves it free to retry with its context.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		batch.loading[key] = done
		batch.mu.Unlock()
		return batch.load(ctx, key, done)
	}
}

func (b *toolPolicyBatch) load(ctx context.Context, key toolPolicyKey, done chan struct{}) ([]SessionToolConfig, error) {
	// Always release followers, including when an application reader panics and
	// the executor's outer safety boundary recovers it. Failures are never cached.
	defer func() {
		b.mu.Lock()
		delete(b.loading, key)
		close(done)
		b.mu.Unlock()
	}()
	rows, err := key.executor.sessionConfig.GetSessionToolConfigs(ctx, key.session, key.version)
	if err != nil {
		return nil, err
	}
	cached := slices.Clone(rows)
	b.mu.Lock()
	b.rows[key] = cached
	b.mu.Unlock()
	return slices.Clone(cached), nil
}
