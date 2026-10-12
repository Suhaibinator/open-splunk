package common_ws

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	chatChannelPrefix               = "chat.channel."
	chatGrantLifetime               = 60 * time.Second
	chatRenewalInterval             = 30 * time.Second
	EventTypeSubscriptionState      = "realtime.subscription_state"
	TopicErrorSubscriptionSuspended = "subscription_suspended"
)

// ChatChannelTopic identifies the connection-scoped ordinary transcript audience.
func ChatChannelTopic(channelID uint64) string {
	return chatChannelPrefix + strconv.FormatUint(channelID, 10)
}

// ParseChatChannelTopic accepts only canonical positive channel IDs in the internal namespace.
func ParseChatChannelTopic(topic string) (uint64, error) {
	if !strings.HasPrefix(topic, chatChannelPrefix) || !ValidTopicName(topic) {
		return 0, ErrInvalidTopic
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(topic, chatChannelPrefix), 10, 64)
	if err != nil || id == 0 || ChatChannelTopic(id) != topic {
		return 0, ErrInvalidTopic
	}
	return id, nil
}

// ChatReadAuthorizer must make a fresh read decision, including application policy.
// It is called at subscription and renewal boundaries, never during event delivery.
type ChatReadAuthorizer[U common_service_models.UserLike] func(context.Context, U, uint64) (ports.ChatRealtimeCategory, error)

type chatGrantKey struct {
	userID    common_service_models.UserIdType
	channelID uint64
}
type chatGrant[U common_service_models.UserLike] struct {
	user             U
	categories       ports.ChatRealtimeCategory
	expires, renewAt time.Time
	version          uint64
	checking         bool
	suspended        bool
}
type preparedChatGrant struct {
	categories ports.ChatRealtimeCategory
	generation uint64
	expires    time.Time
}
type chatRenewal[U common_service_models.UserLike] struct {
	key     chatGrantKey
	grant   *chatGrant[U]
	version uint64
}
type chatSubscriptions[U common_service_models.UserLike] struct {
	authorize  ChatReadAuthorizer[U]
	grants     map[chatGrantKey]*chatGrant[U]
	generation uint64
	loads      singleflight.Group
	ctx        context.Context
	cancel     context.CancelFunc
	jobs       chan chatRenewal[U]
	wake       chan struct{}
}

type chatRealtimeMetrics interface {
	RecordChatRealtimeGrants(int)
	RecordChatRealtimeRenewal(string)
	RecordChatRealtimeInvalidation(string)
}

// RegisterChatReadAuthorizer installs the internal leased chat subscription policy.
// Applications still cannot claim chat through RegisterTopicAuthorizer. Register before serving.
func (h *Hub[U]) RegisterChatReadAuthorizer(authorize ChatReadAuthorizer[U]) error {
	if h == nil {
		return ErrHubClosed
	}
	if authorize == nil {
		return ErrNilTopicAuthorizer
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		return ErrHubClosed
	}
	if h.started {
		return ErrHubStarted
	}
	if h.chat != nil {
		return ErrAuthorizerAlreadyBound
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.chat = &chatSubscriptions[U]{authorize: authorize, grants: make(map[chatGrantKey]*chatGrant[U]), ctx: ctx, cancel: cancel, jobs: make(chan chatRenewal[U], 64), wake: make(chan struct{}, 1)}
	h.wg.Add(5)
	for range 4 {
		go h.chatRenewalWorker()
	}
	go h.chatRenewalLoop()
	return nil
}

func (h *Hub[U]) prepareChatGrant(ctx context.Context, c *connection[U], channelID uint64) (*preparedChatGrant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.RLock()
	state := h.chat
	if state == nil {
		h.mu.RUnlock()
		return nil, ErrInvalidTopic
	}
	generation := state.generation
	key := chatGrantKey{c.userID, channelID}
	if grant := state.grants[key]; grant != nil && !grant.suspended && time.Now().Before(grant.expires) {
		result := &preparedChatGrant{grant.categories, generation, grant.expires}
		h.mu.RUnlock()
		return result, nil
	}
	h.mu.RUnlock()
	// Each caller can abandon a shared lookup without cancelling another connection's request.
	result := state.loads.DoChan(strconv.FormatUint(uint64(c.userID), 10)+":"+strconv.FormatUint(channelID, 10)+":"+strconv.FormatUint(generation, 10), func() (any, error) {
		authCtx, cancel := context.WithTimeout(state.ctx, c.cfg.CommandAuthBudget)
		defer cancel()
		started := time.Now()
		categories, err := h.authorizeChatSafely(authCtx, c.user, channelID)
		h.metrics.RecordRealtimeAuthorizerCall("chat.channel", time.Since(started).Seconds())
		if err == nil {
			err = authCtx.Err()
		}
		if err != nil {
			return nil, err
		}
		return &preparedChatGrant{categories, generation, time.Now().Add(chatGrantLifetime)}, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*preparedChatGrant), nil
	}
}

// commitChatGrantLocked fences authorization against any intervening local revocation.
func (h *Hub[U]) commitChatGrantLocked(c *connection[U], topic string, prepared *preparedChatGrant) bool {
	id, err := ParseChatChannelTopic(topic)
	if err != nil || prepared == nil || h.chat == nil || prepared.generation != h.chat.generation || !time.Now().Before(prepared.expires) {
		return false
	}
	key := chatGrantKey{c.userID, id}
	if current := h.chat.grants[key]; current == nil {
		h.chat.grants[key] = &chatGrant[U]{user: c.user, categories: prepared.categories, expires: prepared.expires, renewAt: chatRenewAt(key, time.Now())}
	} else if current.suspended || !time.Now().Before(current.expires) {
		current.version++
		current.categories = prepared.categories
		current.expires = prepared.expires
		current.renewAt = chatRenewAt(key, time.Now())
		current.suspended = false
	}
	return true
}

func chatRenewAt(key chatGrantKey, now time.Time) time.Time {
	// Stable jitter spreads refreshes without shared random state or timers per grant.
	jitter := time.Duration((uint64(key.userID)*31+key.channelID)%10001)*time.Millisecond - 5*time.Second
	return now.Add(chatRenewalInterval + jitter)
}

func (h *Hub[U]) hasChatSubscriberLocked(key chatGrantKey) bool {
	topic := ChatChannelTopic(key.channelID)
	for c := range h.connsByUser[key.userID] {
		if _, subscribed := c.topics[topic]; subscribed {
			return true
		}
	}
	return false
}

// pruneChatGrantLocked releases the user/channel grant when its final connection unsubscribes.
func (h *Hub[U]) pruneChatGrantLocked(c *connection[U], topic string) {
	if h.chat == nil {
		return
	}
	id, err := ParseChatChannelTopic(topic)
	if err != nil {
		return
	}
	key := chatGrantKey{c.userID, id}
	if !h.hasChatSubscriberLocked(key) {
		delete(h.chat.grants, key)
	}
}

// InvalidateChatRealtimeAccess suspends affected grants synchronously. The caller first invalidates
// recipient caches and invokes this after commit, before access-change publication.
// AudiencesOnly changes leave grants live.
func (h *Hub[U]) InvalidateChatRealtimeAccess(change ports.ChatRealtimeAccessInvalidation) {
	if h == nil || change.AudiencesOnly {
		return
	}
	var slow []*connection[U]
	h.mu.Lock()
	if h.chat == nil {
		h.mu.Unlock()
		return
	}
	h.chat.generation++
	for key, grant := range h.chat.grants {
		if change.ChannelID != 0 && key.channelID != change.ChannelID {
			continue
		}
		if change.UserID != 0 && key.userID != change.UserID {
			continue
		}
		grant.version++
		grant.expires = time.Time{}
		grant.renewAt = time.Time{}
		if !grant.suspended {
			slow = append(slow, h.chatStateLocked(key, false, TopicErrorSubscriptionSuspended)...)
		}
		grant.suspended = true
	}
	select {
	case h.chat.wake <- struct{}{}:
	default:
	}
	h.mu.Unlock()
	h.closeChatSlowConnections(slow)
	if m, ok := h.metrics.(chatRealtimeMetrics); ok {
		scope := "all"
		if change.ChannelID != 0 {
			scope = "channel"
		} else if change.UserID != 0 {
			scope = "user"
		}
		m.RecordChatRealtimeInvalidation(scope)
	}
}

// ChatAudienceGeneration is captured before resolving participants and verified at final enqueue.
// Any access change, even for an unrelated channel, drops an event whose audience was resolving
// concurrently; clients recover through their REST refetch on reconnect and focus.
func (h *HubRealtimePublisher[U]) ChatAudienceGeneration() uint64 {
	if h == nil || h.hub == nil {
		return 0
	}
	h.hub.mu.RLock()
	defer h.hub.mu.RUnlock()
	if h.hub.chat == nil {
		return 0
	}
	return h.hub.chat.generation
}

func (h *Hub[U]) chatRenewalLoop() {
	defer h.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-h.chat.ctx.Done():
			return
		case <-ticker.C:
		case <-h.chat.wake:
		}
		h.scheduleChatRenewals(time.Now())
	}
}

func (h *Hub[U]) scheduleChatRenewals(now time.Time) {
	var slow []*connection[U]
	active := 0
	h.mu.Lock()
	for key, grant := range h.chat.grants {
		if !now.Before(grant.expires) && !grant.suspended {
			grant.suspended = true
			slow = append(slow, h.chatStateLocked(key, false, TopicErrorSubscriptionSuspended)...)
		}
		if !grant.suspended {
			active++
		}
		if grant.checking || now.Before(grant.renewAt) {
			continue
		}
		select {
		case h.chat.jobs <- chatRenewal[U]{key, grant, grant.version}:
			grant.checking = true
		default:
		}
	}
	h.mu.Unlock()
	h.closeChatSlowConnections(slow)
	if m, ok := h.metrics.(chatRealtimeMetrics); ok {
		m.RecordChatRealtimeGrants(active)
	}
}

func (h *Hub[U]) chatRenewalWorker() {
	defer h.wg.Done()
	for {
		select {
		case <-h.chat.ctx.Done():
			return
		case job := <-h.chat.jobs:
			h.mu.RLock()
			current := h.chat.grants[job.key]
			valid := current == job.grant && current.version == job.version
			if !valid && current == job.grant { // release a job invalidated while queued
				h.mu.RUnlock()
				h.mu.Lock()
				if h.chat.grants[job.key] == job.grant {
					job.grant.checking = false
				}
				h.mu.Unlock()
				continue
			}
			h.mu.RUnlock()
			if !valid {
				continue
			}
			ctx, cancel := context.WithTimeout(h.chat.ctx, DefaultCommandAuthBudget)
			started := time.Now()
			categories, err := h.authorizeChatSafely(ctx, job.grant.user, job.key.channelID)
			if err == nil {
				err = ctx.Err()
			}
			cancel()
			h.metrics.RecordRealtimeAuthorizerCall("chat.channel", time.Since(started).Seconds())
			h.completeChatRenewal(job, categories, err, time.Now())
		}
	}
}

func (h *Hub[U]) completeChatRenewal(job chatRenewal[U], categories ports.ChatRealtimeCategory, err error, now time.Time) {
	var slow []*connection[U]
	removed := 0
	outcome := "allowed"
	h.mu.Lock()
	current := h.chat.grants[job.key]
	if current != job.grant {
		h.mu.Unlock()
		return
	}
	current.checking = false
	if current.version != job.version || h.closed.Load() {
		h.mu.Unlock()
		return
	}
	if err == nil {
		restored := current.suspended
		current.categories = categories
		current.expires = now.Add(chatGrantLifetime)
		current.renewAt = chatRenewAt(job.key, now)
		current.suspended = false
		if restored {
			slow = h.chatStateLocked(job.key, true, "")
		}
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, ports.ErrChatRealtimeUnavailable) {
		outcome = "error"
		current.renewAt = now.Add(5 * time.Second)
		// A transient failure retains only the original lease, never an extension.
	} else {
		outcome = "denied"
		// A concurrent subscribe may have prepared a copy of this live grant before
		// renewal discovered the denial. Fence that copy before removing the grant.
		h.chat.generation++
		slow = h.chatStateLocked(job.key, false, TopicErrorUnauthorizedTopic)
		topic := ChatChannelTopic(job.key.channelID)
		for c := range h.connsByUser[job.key.userID] {
			if _, subscribed := c.topics[topic]; subscribed {
				delete(c.topics, topic)
				delete(h.connsByTopic[topic], c)
				removed++
			}
		}
		if len(h.connsByTopic[topic]) == 0 {
			delete(h.connsByTopic, topic)
		}
		delete(h.chat.grants, job.key)
	}
	h.mu.Unlock()
	h.closeChatSlowConnections(slow)
	for i := 0; i < removed; i++ {
		h.metrics.RecordRealtimeUnsubscribed("chat.channel")
	}
	if m, ok := h.metrics.(chatRealtimeMetrics); ok {
		m.RecordChatRealtimeRenewal(outcome)
	}
}

// Callback failures are isolated at every invocation, including shared subscribe loads and
// background renewal jobs. A panic is a denial, never permission to retain an existing lease.
func (h *Hub[U]) authorizeChatSafely(ctx context.Context, user U, channelID uint64) (categories ports.ChatRealtimeCategory, err error) {
	defer func() {
		if recover() != nil {
			categories = 0
			err = errors.New("chat realtime authorization callback panicked")
			h.logger.Error("chat realtime authorization callback panicked")
		}
	}()
	return h.chat.authorize(ctx, user, channelID)
}

// chatStateLocked queues state transitions under the same lock as fan-out and revocation.
func (h *Hub[U]) chatStateLocked(key chatGrantKey, ok bool, code string) []*connection[U] {
	topic := ChatChannelTopic(key.channelID)
	payload, _ := proto.Marshal(&proto_models.CommandResult{Ok: ok, Topics: []*proto_models.TopicResult{{Topic: topic, Ok: ok, ErrorCode: code}}})
	frame, _ := proto.Marshal(&proto_models.RealtimeEvent{Topic: TopicControl, EventType: EventTypeSubscriptionState, OccurredAt: timestamppb.Now(), Payload: payload})
	var slow []*connection[U]
	for c := range h.connsByUser[key.userID] {
		if _, subscribed := c.topics[topic]; subscribed && errors.Is(c.enqueue(frame), ErrBackpressure) {
			slow = append(slow, c)
		}
	}
	return slow
}

func (h *Hub[U]) closeChatSlowConnections(connections []*connection[U]) {
	if h.disconnectOnFullQueue {
		for _, c := range connections {
			c.close(disconnectReasonBackpressure)
		}
	}
}
