package common_ws

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

const chatRecipientCacheTTL = 60 * time.Second
const chatRecipientCacheCapacity = 2048

type recipientCacheKey struct {
	channelID uint64
	hidden    bool
}
type recipientCacheValue struct {
	ids     []uint64
	expires time.Time
	loaded  bool
}
type recipientCacheEntry struct {
	channelID uint64
	values    [2]recipientCacheValue
}
type recipientCacheLoad struct{ done chan struct{} }

// ChatRecipientCache shares bounded participant lookups across messages, streams and typing.
// Invalidations fence in-flight loads; failed loads never install a cached audience.
// Returned slices must be treated as immutable.
type chatRecipientCacheMetrics interface{ RecordChatRecipientCache(string) }

type ChatRecipientCache struct {
	metrics     chatRecipientCacheMetrics
	persistence PersistenceForRecipientResolver
	mu          sync.Mutex
	entries     map[uint64]*list.Element
	lru         *list.List
	loading     map[recipientCacheKey]*recipientCacheLoad
	generation  uint64
	now         func() time.Time
}

func NewChatRecipientCache(persistence PersistenceForRecipientResolver, metrics ...interface{ RecordChatRecipientCache(string) }) *ChatRecipientCache {
	var recorder chatRecipientCacheMetrics
	if len(metrics) > 0 {
		recorder = metrics[0]
	}
	return &ChatRecipientCache{metrics: recorder, persistence: persistence, entries: make(map[uint64]*list.Element), lru: list.New(), loading: make(map[recipientCacheKey]*recipientCacheLoad), now: time.Now}
}

func (c *ChatRecipientCache) InvalidateChatRealtimeAccess(change ports.ChatRealtimeAccessInvalidation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	if change.ChannelID != 0 {
		if elem := c.entries[change.ChannelID]; elem != nil {
			c.lru.Remove(elem)
			delete(c.entries, change.ChannelID)
		}
	} else {
		clear(c.entries)
		c.lru.Init()
	}
}
func (c *ChatRecipientCache) GetActiveChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error) {
	return c.get(ctx, channelID, false)
}
func (c *ChatRecipientCache) GetHiddenAuthorizedChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error) {
	return c.get(ctx, channelID, true)
}
func (c *ChatRecipientCache) get(ctx context.Context, channelID uint64, hidden bool) ([]uint64, error) {
	key := recipientCacheKey{channelID, hidden}
	index := 0
	if hidden {
		index = 1
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if elem := c.entries[channelID]; elem != nil {
			value := elem.Value.(*recipientCacheEntry).values[index]
			if value.loaded && c.now().Before(value.expires) {
				c.lru.MoveToFront(elem)
				c.mu.Unlock()
				if c.metrics != nil {
					c.metrics.RecordChatRecipientCache("hit")
				}
				return value.ids, nil
			}
		}
		if pending := c.loading[key]; pending != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending.done:
				continue
			}
		}
		pending := &recipientCacheLoad{done: make(chan struct{})}
		c.loading[key] = pending
		generation := c.generation
		c.mu.Unlock()
		if c.metrics != nil {
			c.metrics.RecordChatRecipientCache("miss")
		}
		var ids []uint64
		var err error
		if hidden {
			ids, err = c.persistence.GetHiddenAuthorizedChannelParticipantUserIDs(ctx, channelID)
		} else {
			ids, err = c.persistence.GetActiveChannelParticipantUserIDs(ctx, channelID)
		}
		if err != nil && c.metrics != nil {
			c.metrics.RecordChatRecipientCache("error")
		}
		c.mu.Lock()
		delete(c.loading, key)
		close(pending.done)
		if generation != c.generation {
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
			continue
		}
		if err == nil {
			elem := c.entries[channelID]
			if elem == nil {
				if c.lru.Len() >= chatRecipientCacheCapacity {
					old := c.lru.Back()
					delete(c.entries, old.Value.(*recipientCacheEntry).channelID)
					c.lru.Remove(old)
				}
				elem = c.lru.PushFront(&recipientCacheEntry{channelID: channelID})
				c.entries[channelID] = elem
			}
			// Own the storage even when persistence reuses a backing slice.
			ids = append([]uint64(nil), ids...)
			elem.Value.(*recipientCacheEntry).values[index] = recipientCacheValue{ids: ids, expires: c.now().Add(chatRecipientCacheTTL), loaded: true}
			c.lru.MoveToFront(elem)
		}
		c.mu.Unlock()
		return ids, err
	}
}
