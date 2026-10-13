package organization

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// cacheGenerationGuard couples the RWMutex and invalidation generation shared
// by every generation-guarded cache of one owner. It is embedded so
// invalidation paths that clear several caches in one generation can keep
// taking the lock and bumping the generation directly, while the standard
// lookup/fill pattern goes through generationCache.
type cacheGenerationGuard struct {
	cacheMu         sync.RWMutex
	cacheGeneration uint64
}

// fillIfCurrent runs fill under the write lock only when no invalidation has
// run since generation was observed. Fills that lost the race are discarded so
// a stale persistence read can never overwrite a fresher invalidation.
func (g *cacheGenerationGuard) fillIfCurrent(generation uint64, fill func()) {
	g.cacheMu.Lock()
	defer g.cacheMu.Unlock()
	if g.cacheGeneration == generation {
		fill()
	}
}

// generationCache pairs one TTL cache with its owner's generation guard and
// carries the generation-guarded fill pattern shared by every organization
// cache: read under the read lock, observe the generation on a miss, and fill
// only when no invalidation ran in between.
//
// Get, Set, Delete, and DeleteAll are raw passthroughs that take no lock; they
// serve test seeding and invalidation paths that already hold the guard.
type generationCache[K comparable, V any] struct {
	guard     *cacheGenerationGuard
	cache     atomic.Pointer[ttlcache.Cache[K, V]]
	policy    func() orgCachePolicy
	applied   orgCachePolicy
	configure func(*ttlcache.Cache[K, V])
}

// newGenerationCache wraps cache with the shared guard. Callers keep a nil
// *generationCache when the underlying cache is disabled so existing nil
// checks stay meaningful.
func newGenerationCache[K comparable, V any](
	guard *cacheGenerationGuard,
	cache *ttlcache.Cache[K, V],
) *generationCache[K, V] {
	c := &generationCache[K, V]{guard: guard}
	c.cache.Store(cache)
	return c
}

// Lookup returns the cached value for key plus the generation observed under
// the read lock. Callers pass that generation to FillIfCurrent (or the guard's
// fillIfCurrent for multi-cache fills) after re-reading from persistence.
func (c *generationCache[K, V]) Lookup(key K) (V, bool, uint64) {
	value, found, generation, _ := c.lookupWithPolicy(key)
	return value, found, generation
}

// lookupWithPolicy returns the policy under the same lock as the observed
// generation, so a cache fill can use exactly the policy that authorized it.
func (c *generationCache[K, V]) lookupWithPolicy(key K) (V, bool, uint64, orgCachePolicy) {
	c.refresh()
	c.guard.cacheMu.RLock()
	defer c.guard.cacheMu.RUnlock()
	var zero V
	if c.policy != nil && c.applied.capacity == 0 {
		return zero, false, c.guard.cacheGeneration, c.applied
	}
	if item := c.cache.Load().Get(key); item != nil {
		return item.Value(), true, c.guard.cacheGeneration, c.applied
	}
	return zero, false, c.guard.cacheGeneration, c.applied
}

// Generation observes the current invalidation generation for fills that do
// not begin with a cache read.
func (c *generationCache[K, V]) Generation() uint64 {
	c.refresh()
	c.guard.cacheMu.RLock()
	defer c.guard.cacheMu.RUnlock()
	return c.guard.cacheGeneration
}

// FillIfCurrent stores value only when no invalidation has run since
// generation was observed. The entry uses the cache's configured default TTL.
func (c *generationCache[K, V]) FillIfCurrent(key K, value V, generation uint64) {
	c.guard.fillIfCurrent(generation, func() {
		if c.policy == nil || c.applied.capacity > 0 {
			c.cache.Load().Set(key, value, ttlcache.DefaultTTL)
		}
	})
}

// Invalidate drops the given keys under a single generation bump so in-flight
// fills that observed an earlier generation are rejected. A nil cache leaves
// the generation untouched: there is nothing to invalidate and unrelated
// in-flight fills must stay eligible.
func (c *generationCache[K, V]) Invalidate(keys ...K) {
	if c == nil || c.cache.Load() == nil {
		return
	}
	c.guard.cacheMu.Lock()
	defer c.guard.cacheMu.Unlock()
	c.guard.cacheGeneration++
	for _, key := range keys {
		c.cache.Load().Delete(key)
	}
}

// InvalidateAll drops every entry under a single generation bump.
func (c *generationCache[K, V]) InvalidateAll() {
	if c == nil || c.cache.Load() == nil {
		return
	}
	c.guard.cacheMu.Lock()
	defer c.guard.cacheMu.Unlock()
	c.guard.cacheGeneration++
	c.cache.Load().DeleteAll()
}

// Drop removes a stale entry under the write lock without bumping the
// generation: the caller is about to re-fill the corrected value and that fill
// must stay eligible against the generation it observed.
func (c *generationCache[K, V]) Drop(key K) {
	c.guard.cacheMu.Lock()
	defer c.guard.cacheMu.Unlock()
	c.cache.Load().Delete(key)
}

// Get reads directly from the underlying cache without locking the guard.
func (c *generationCache[K, V]) Get(key K) *ttlcache.Item[K, V] {
	return c.cache.Load().Get(key)
}

// Set writes directly to the underlying cache without locking the guard.
func (c *generationCache[K, V]) Set(key K, value V, ttl time.Duration) *ttlcache.Item[K, V] {
	if c.policy != nil && c.applied.capacity == 0 {
		return nil
	}
	return c.cache.Load().Set(key, value, ttl)
}

// Delete removes key directly; multi-cache invalidation paths call this while
// already holding the guard.
func (c *generationCache[K, V]) Delete(key K) {
	c.cache.Load().Delete(key)
}

// DeleteAll clears the underlying cache directly; see Delete.
func (c *generationCache[K, V]) DeleteAll() {
	c.cache.Load().DeleteAll()
}

// Len returns the number of live entries in the underlying cache.
func (c *generationCache[K, V]) Len() int {
	return c.cache.Load().Len()
}

type orgCachePolicy struct {
	capacity    uint64
	ttl         time.Duration
	inheritance bool
}

func (c *generationCache[K, V]) refresh() {
	if c.policy == nil {
		return
	}
	policy := c.policy()
	c.guard.cacheMu.Lock()
	defer c.guard.cacheMu.Unlock()
	if c.applied == policy {
		return
	}
	c.guard.cacheGeneration++
	next := ttlcache.New(ttlcache.WithTTL[K, V](policy.ttl), ttlcache.WithCapacity[K, V](policy.capacity), ttlcache.WithDisableTouchOnHit[K, V]())
	if c.configure != nil {
		c.configure(next)
	}
	c.cache.Store(next)
	c.applied = policy
}
