package searchanalysis

import (
	"container/heap"
	"container/list"
	"time"
)

// cacheEntry is one retained analysis-cache value. generation orders entries
// that share an expiry instant, element is its LRU position, and expiryIndex is
// its position in the owning cache's expiry heap.
type cacheEntry[K comparable, V any] struct {
	key         K
	value       V
	generation  uint64
	retained    uint64
	expiresAt   time.Time
	element     *list.Element
	expiryIndex int
}

// expiryHeap orders retained entries by deadline so the cheapest expiry check
// is the heap root. Ties break on generation to keep eviction deterministic.
type expiryHeap[K comparable, V any] []*cacheEntry[K, V]

func (entries expiryHeap[K, V]) Len() int { return len(entries) }

func (entries expiryHeap[K, V]) Less(left, right int) bool {
	if entries[left].expiresAt.Equal(entries[right].expiresAt) {
		return entries[left].generation < entries[right].generation
	}
	return entries[left].expiresAt.Before(entries[right].expiresAt)
}

func (entries expiryHeap[K, V]) Swap(left, right int) {
	entries[left], entries[right] = entries[right], entries[left]
	entries[left].expiryIndex = left
	entries[right].expiryIndex = right
}

func (entries *expiryHeap[K, V]) Push(value any) {
	entry := value.(*cacheEntry[K, V])
	entry.expiryIndex = len(*entries)
	*entries = append(*entries, entry)
}

func (entries *expiryHeap[K, V]) Pop() any {
	old := *entries
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.expiryIndex = -1
	*entries = old[:last]
	return entry
}

// expiringCache is a bounded TTL cache with LRU eviction and a deadline heap.
// It carries no lock of its own: every method requires the owning service's
// mutex to be held, and publication (assigning generation/expiresAt/element,
// inserting into entries, adding to bytes, and pushing onto expirations) stays
// with the caller that also owns the single-flight bookkeeping.
type expiringCache[K comparable, V any] struct {
	entries        map[K]*cacheEntry[K, V]
	lru            list.List
	expirations    expiryHeap[K, V]
	bytes          uint64
	maxBytes       uint64
	maxEntries     int
	nextGeneration uint64
}

// live returns the unexpired entry for key, removing and reporting nil when the
// entry's own capped deadline has passed. The caller holds service.mu.
func (cache *expiringCache[K, V]) live(key K, now time.Time) *cacheEntry[K, V] {
	entry := cache.entries[key]
	if entry != nil && !now.Before(entry.expiresAt) {
		cache.remove(entry)
		return nil
	}
	return entry
}

// expire removes every entry whose deadline has passed. The heap root makes
// this an O(1) check when nothing has expired. The caller holds service.mu.
func (cache *expiringCache[K, V]) expire(now time.Time) {
	for len(cache.expirations) != 0 {
		entry := cache.expirations[0]
		if now.Before(entry.expiresAt) {
			return
		}
		cache.remove(entry)
	}
}

// enforceBounds evicts least-recently-used entries until both the entry-count
// and retained-byte ceilings hold. The caller holds service.mu.
func (cache *expiringCache[K, V]) enforceBounds() {
	for len(cache.entries) > cache.maxEntries || cache.bytes > cache.maxBytes {
		element := cache.lru.Back()
		if element == nil {
			return
		}
		cache.remove(element.Value.(*cacheEntry[K, V]))
	}
}

// remove drops entry from the map, the LRU list, the expiry heap, and the
// retained-byte total. Entries already replaced under their key are ignored so
// a stale pointer never evicts its successor. The caller holds service.mu.
func (cache *expiringCache[K, V]) remove(entry *cacheEntry[K, V]) {
	if entry == nil || cache.entries[entry.key] != entry {
		return
	}
	delete(cache.entries, entry.key)
	cache.lru.Remove(entry.element)
	heap.Remove(&cache.expirations, entry.expiryIndex)
	if entry.retained > cache.bytes {
		cache.bytes = 0
	} else {
		cache.bytes -= entry.retained
	}
}

// drain removes every retained entry, least-recently-used first, so shutdown
// invalidates the cache and every cursor derived from it. The caller holds
// service.mu.
func (cache *expiringCache[K, V]) drain() {
	for element := cache.lru.Back(); element != nil; {
		previous := element.Prev()
		cache.remove(element.Value.(*cacheEntry[K, V]))
		element = previous
	}
}

// allocateGeneration reserves the next monotonic generation, reporting false
// when the counter would wrap. The caller holds service.mu.
func (cache *expiringCache[K, V]) allocateGeneration() (uint64, bool) {
	if cache.nextGeneration == ^uint64(0) {
		return 0, false
	}
	cache.nextGeneration++
	return cache.nextGeneration, true
}
