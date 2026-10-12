package controlplane

import (
	"fmt"
	"sync"
)

// policyBucketState is the per-scope request/token bucket pair together with the
// policy each bucket was built from, so a runtime policy change can be detected and
// the affected bucket rebuilt.
type policyBucketState struct {
	requestPolicy BucketRateLimitPolicy
	requestBucket *tokenBucket
	tokenPolicy   BucketRateLimitPolicy
	tokenBucket   *tokenBucket
}

// bucketRegistry holds process-local request and token buckets keyed by scope.
// Routes targeting the same model share a request bucket and a token bucket.
// Buckets do not coordinate across processes.
type bucketRegistry struct {
	mu     sync.Mutex
	scopes map[string]*policyBucketState
}

func newBucketRegistry() *bucketRegistry {
	return &bucketRegistry{scopes: make(map[string]*policyBucketState)}
}

// bucketsForScope returns the shared request and token buckets for one scope,
// building or rebuilding a bucket whenever the incoming policy differs from the one
// its bucket was constructed with. A cleared limit drops the corresponding bucket.
// Because a rebuilt bucket starts at full capacity, a policy change may temporarily
// reset a scope to full capacity.
//
// A returned nil bucket means the corresponding limit is not configured for the
// scope; callers must gate their bucket use on the policy's Has* flags.
func (r *bucketRegistry) bucketsForScope(scopeKey string, policy ModelRateLimitPolicy) (*tokenBucket, *tokenBucket, error) {
	scopeKey = normalizeScopeKey(scopeKey)
	if scopeKey == "" {
		return nil, nil, fmt.Errorf("bucket scope key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.scopes[scopeKey]
	if !ok || state == nil {
		state = &policyBucketState{}
		r.scopes[scopeKey] = state
	}

	if !policy.HasRequestLimit {
		state.requestBucket = nil
		state.requestPolicy = BucketRateLimitPolicy{}
	} else if state.requestBucket == nil || state.requestPolicy != policy.RequestLimit {
		requestBucket, err := newTokenBucket(policy.RequestLimit)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize request bucket for scope %q: %w", scopeKey, err)
		}
		state.requestBucket = requestBucket
		state.requestPolicy = policy.RequestLimit
	}

	if !policy.HasTokenLimit {
		state.tokenBucket = nil
		state.tokenPolicy = BucketRateLimitPolicy{}
	} else if state.tokenBucket == nil || state.tokenPolicy != policy.TokenLimit {
		tokenBucket, err := newTokenBucket(policy.TokenLimit)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize token bucket for scope %q: %w", scopeKey, err)
		}
		state.tokenBucket = tokenBucket
		state.tokenPolicy = policy.TokenLimit
	}

	return state.requestBucket, state.tokenBucket, nil
}
