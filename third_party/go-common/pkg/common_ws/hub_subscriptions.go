package common_ws

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleSubscribe processes a hub-owned "subscribe" command. It runs before any command-router
// dispatch and works with no router installed at all, which is exactly the chat-less configuration
// the realtime transport targets.
//
// Every requested topic gets an explicit outcome. Validation, the reserved-namespace check and the
// per-connection cap all run before the authorizer so a malformed or over-cap batch never reaches
// application code (authorizers may hit the database, on the read goroutine).
func (h *Hub[U]) handleSubscribe(c *connection[U], requestID string, payload []byte) {
	var cmd proto_models.RealtimeSubscribeCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		if ce := c.logger.Check(zap.DebugLevel, "failed to unmarshal realtime subscribe command"); ce != nil {
			ce.Write(
				zap.Error(err),
				zap.Int(appLogger.KeySize, len(payload)),
				zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			)
		}
		return
	}

	topics := cmd.GetTopics()
	if len(topics) > MaxTopicsPerCommand {
		// The batch cap rejects the whole command rather than truncating it, so the client never has
		// to guess which of its topics survived.
		results := rejectAllTopics(topics, TopicErrorSubscriptionLimit)
		h.commitSubscriptions(c, requestID, results, nil)
		return
	}

	// D4: one budget for the whole command, not one per topic. Authorization blocks the read
	// goroutine, and gorilla dispatches pongs from inside ReadMessage, so a per-topic budget would
	// let a full batch outlive the keepalive window and disconnect the connection mid-command.
	//
	// The budget comes from the connection's own captured configuration, never from the hub's startup
	// copy: it was captured together with this connection's ping interval and pong timeout, and those
	// three are only guaranteed to satisfy the keepalive invariant as a set.
	authCtx, cancel := context.WithTimeout(c.ctx, c.cfg.CommandAuthBudget)
	defer cancel()

	current := h.snapshotConnectionTopics(c)
	limit := h.subscriptionLimit()
	results := make([]*proto_models.TopicResult, 0, len(topics))
	chatGrants := make(map[string]*preparedChatGrant)

	for _, topic := range topics {
		switch {
		case !ValidTopicName(topic):
			results = append(results, failedTopic(topic, TopicErrorInvalidTopic))
		case isReservedTopic(topic):
			// The connection's own user topic is subscribed at registration and anyone else's is
			// forbidden, so the whole reserved space is closed to clients.
			results = append(results, failedTopic(topic, TopicErrorReservedTopic))
		default:
			channelID, chatParseErr := ParseChatChannelTopic(topic)
			isChatTopic := chatParseErr == nil && h.chat != nil
			if _, already := current[topic]; already && !isChatTopic {
				// Re-subscribing is idempotent and must not consume budget or cap headroom.
				results = append(results, succeededTopic(topic))
				continue
			}
			if isChatTopic {
				_, already := current[topic]
				if !already && limit > 0 && len(current) >= limit {
					results = append(results, failedTopic(topic, TopicErrorSubscriptionLimit))
					continue
				}
				grant, err := h.prepareChatGrant(authCtx, c, channelID)
				if err != nil {
					code := TopicErrorUnauthorizedTopic
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, ports.ErrChatRealtimeUnavailable) {
						code = TopicErrorAuthorizerTimeout
					}
					results = append(results, failedTopic(topic, code))
					continue
				}
				chatGrants[topic] = grant
				current[topic] = struct{}{}
				results = append(results, succeededTopic(topic))
				continue
			}
			authorizer, namespace := h.authorizerFor(topic)
			if authorizer == nil {
				results = append(results, failedTopic(topic, TopicErrorUnknownNamespace))
				continue
			}
			if limit > 0 && len(current) >= limit {
				results = append(results, failedTopic(topic, TopicErrorSubscriptionLimit))
				continue
			}
			started := time.Now()
			err := authorizer.AuthorizeTopic(authCtx, c.user, topic)
			h.metrics.RecordRealtimeAuthorizerCall(namespace, time.Since(started).Seconds())
			if err != nil {
				// A budget that ran out (or a connection that went away) is not a statement about this
				// user's access to this topic. Reporting it as unauthorized_topic would have clients cache
				// a denial they were never given and would hide a server-side capacity problem inside the
				// denial metric, so it gets its own wire code and its own severity.
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					c.logger.Warn("realtime topic authorizer did not finish within the command auth budget",
						zap.String(logkeys.Operation, "subscribe_realtime_topic"),
						zap.String(logkeys.Stage, "authorize_topic"),
						zap.String(appLogger.KeyOutcome, "deferred"),
						zap.String(logkeys.Reason, TopicErrorAuthorizerTimeout),
						zap.String(appLogger.KeyTopic, topic),
						appLogger.DurationMS(appLogger.KeyCommandAuthBudgetMS, c.cfg.CommandAuthBudget),
						zap.String(logkeys.Fallback, "reject_subscription_as_timeout"),
						zap.Error(err),
						zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
					)
					results = append(results, failedTopic(topic, TopicErrorAuthorizerTimeout))
					continue
				}
				// A denied topic subscription is an expected authorization outcome, but
				// repeated attempts are a meaningful abuse/security signal.
				c.logger.Warn("realtime topic subscribe denied",
					zap.String(logkeys.Operation, "subscribe_realtime_topic"),
					zap.String(appLogger.KeyOutcome, "denied"),
					zap.String(logkeys.Reason, TopicErrorUnauthorizedTopic),
					zap.String(appLogger.KeyTopic, topic),
					zap.Error(err),
					zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
				)
				results = append(results, failedTopic(topic, TopicErrorUnauthorizedTopic))
				continue
			}
			current[topic] = struct{}{}
			results = append(results, succeededTopic(topic))
		}
	}

	h.commitSubscriptions(c, requestID, results, chatGrants)
}

// recordSubscribeDenials reports every per-topic denial of one subscribe command. The label is the
// TopicError* code, a closed server-owned set, so the metric stays bounded no matter what a client
// asks for.
func (h *Hub[U]) recordSubscribeDenials(results []*proto_models.TopicResult) {
	for _, result := range results {
		if !result.GetOk() {
			h.metrics.RecordRealtimeSubscribeDenied(result.GetErrorCode())
		}
	}
}

// handleUnsubscribe processes a hub-owned "unsubscribe" command. Unsubscribing needs no
// authorization, so it only enforces the name rules, the reserved namespaces (a client can no more
// leave its user topic than join someone else's), and the per-command batch cap.
func (h *Hub[U]) handleUnsubscribe(c *connection[U], requestID string, payload []byte) {
	var cmd proto_models.RealtimeUnsubscribeCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		if ce := c.logger.Check(zap.DebugLevel, "failed to unmarshal realtime unsubscribe command"); ce != nil {
			ce.Write(
				zap.Error(err),
				zap.Int(appLogger.KeySize, len(payload)),
				zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			)
		}
		return
	}

	topics := cmd.GetTopics()
	if len(topics) > MaxTopicsPerCommand {
		h.commitUnsubscriptions(c, nil, h.encodeCommandResult(c, requestID, rejectAllTopics(topics, TopicErrorSubscriptionLimit)))
		return
	}

	results := make([]*proto_models.TopicResult, 0, len(topics))
	removed := make([]string, 0, len(topics))
	for _, topic := range topics {
		switch {
		case !ValidTopicName(topic):
			results = append(results, failedTopic(topic, TopicErrorInvalidTopic))
		case isReservedTopic(topic):
			results = append(results, failedTopic(topic, TopicErrorReservedTopic))
		default:
			// Unsubscribing from a topic the connection never joined is a no-op, not a failure.
			removed = append(removed, topic)
			results = append(results, succeededTopic(topic))
		}
	}

	h.commitUnsubscriptions(c, removed, h.encodeCommandResult(c, requestID, results))
}

// commitSubscriptions writes the accepted topic memberships and enqueues the ack.
//
// D5: both happen inside one critical section, so no Publish can interleave between a topic
// becoming live and its ack reaching the send queue — the client is guaranteed ack-then-events. The
// backpressure disconnect is deferred until after the unlock because connection.close() re-enters
// the hub through unregister's write lock, and the subscription metric is likewise resolved under
// the lock but recorded after it: an application-supplied recorder must never run inside the hub's
// write lock.
func (h *Hub[U]) commitSubscriptions(c *connection[U], requestID string, results []*proto_models.TopicResult, chatGrants map[string]*preparedChatGrant) {
	if c == nil {
		return
	}

	backpressured := false
	var restoredSlow []*connection[U]
	namespaces := make([]string, 0, len(results))
	h.mu.Lock()
	if h.isRegisteredLocked(c) {
		for _, result := range results {
			if !result.GetOk() {
				continue
			}
			topic := result.Topic
			if grant, isChat := chatGrants[topic]; isChat {
				id, _ := ParseChatChannelTopic(topic)
				key := chatGrantKey{c.userID, id}
				current := h.chat.grants[key]
				restoring := current != nil && (current.suspended || !time.Now().Before(current.expires))
				if !h.commitChatGrantLocked(c, topic, grant) {
					result.Ok = false
					result.ErrorCode = TopicErrorAuthorizerTimeout
					continue
				}
				if restoring {
					restoredSlow = append(restoredSlow, h.chatStateLocked(key, true, "")...)
				}
			}
			if c.topics == nil {
				c.topics = make(map[string]struct{}, len(results))
			}
			if _, already := c.topics[topic]; already {
				continue
			}
			c.topics[topic] = struct{}{}
			set := h.connsByTopic[topic]
			if set == nil {
				set = make(map[*connection[U]]struct{})
				h.connsByTopic[topic] = set
			}
			set[c] = struct{}{}
			namespaces = append(namespaces, h.topicMetricNamespace(topic))
		}
		backpressured = h.enqueueAckLocked(c, h.encodeCommandResult(c, requestID, results))
	}
	h.mu.Unlock()

	h.recordSubscribeDenials(results)
	h.closeChatSlowConnections(restoredSlow)
	for _, namespace := range namespaces {
		h.metrics.RecordRealtimeSubscribed(namespace)
	}

	if backpressured && h.disconnectOnFullQueue {
		c.close(disconnectReasonBackpressure)
	}
}

// commitUnsubscriptions drops the requested topic memberships and enqueues the ack under the same
// critical section, mirroring commitSubscriptions — including keeping the metric recorder outside
// the lock.
func (h *Hub[U]) commitUnsubscriptions(c *connection[U], removed []string, ack []byte) {
	if c == nil {
		return
	}

	backpressured := false
	namespaces := make([]string, 0, len(removed))
	h.mu.Lock()
	if h.isRegisteredLocked(c) {
		for _, topic := range removed {
			if _, subscribed := c.topics[topic]; !subscribed {
				continue
			}
			delete(c.topics, topic)
			namespaces = append(namespaces, h.topicMetricNamespace(topic))
			set := h.connsByTopic[topic]
			if set == nil {
				continue
			}
			delete(set, c)
			h.pruneChatGrantLocked(c, topic)
			if len(set) == 0 {
				delete(h.connsByTopic, topic)
			}
		}
		backpressured = h.enqueueAckLocked(c, ack)
	}
	h.mu.Unlock()

	for _, namespace := range namespaces {
		h.metrics.RecordRealtimeUnsubscribed(namespace)
	}

	if backpressured && h.disconnectOnFullQueue {
		c.close(disconnectReasonBackpressure)
	}
}

// enqueueAckLocked queues a control frame and reports whether it hit backpressure. Callers must
// hold h.mu; acting on the result is their job, outside the lock.
//
// The test hook fires here rather than at the call site on purpose: it is the ack enqueue that has
// to stay inside the membership critical section, so anchoring the hook to this function makes any
// future move of the enqueue take the hook — and therefore the D5 regression test — with it.
func (h *Hub[U]) enqueueAckLocked(c *connection[U], ack []byte) bool {
	if hook := h.testHookBeforeAckEnqueue; hook != nil {
		hook()
	}
	if len(ack) == 0 {
		return false
	}
	return errors.Is(c.enqueue(ack), ErrBackpressure)
}

// snapshotConnectionTopics copies a connection's current topic set so the subscribe evaluation can
// reason about cap headroom and idempotent re-subscribes without holding the hub lock across
// authorizer calls.
func (h *Hub[U]) snapshotConnectionTopics(c *connection[U]) map[string]struct{} {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]struct{}, len(c.topics))
	for topic := range c.topics {
		out[topic] = struct{}{}
	}
	return out
}

// encodeCommandResult serializes a CommandResult into a RealtimeEvent frame on the control topic.
// A frame that cannot be built is dropped rather than failing the command, which has already been
// applied — but it is a server fault, not client behavior: every field of this frame is server-owned
// (a request ID and topic strings that already passed unmarshal, plus package constants), so a
// failure here means go-common built an invalid message and the client silently loses its ack. It is
// logged at Error and counted, never buried at Debug.
func (h *Hub[U]) encodeCommandResult(c *connection[U], requestID string, results []*proto_models.TopicResult) []byte {
	ok := true
	for _, result := range results {
		if !result.GetOk() {
			ok = false
			break
		}
	}

	payload, err := proto.Marshal(&proto_models.CommandResult{
		RequestId: requestID,
		Ok:        ok,
		Topics:    results,
	})
	if err != nil {
		h.metrics.RecordRealtimePublishFailure(publishFailureMarshalCommandResult)
		c.logger.Error("failed to marshal realtime command result",
			zap.String(logkeys.Operation, "encode_realtime_command_result"),
			zap.String(logkeys.Stage, "marshal_payload"),
			zap.String(logkeys.Fallback, "drop_ack"),
			zap.String(appLogger.KeyRequestID, requestID),
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, uint64(c.userID)))
		return nil
	}

	frame, err := proto.Marshal(&proto_models.RealtimeEvent{
		Topic:      TopicControl,
		EventType:  EventTypeCommandResult,
		OccurredAt: timestamppb.Now(),
		Payload:    payload,
	})
	if err != nil {
		h.metrics.RecordRealtimePublishFailure(publishFailureMarshalCommandResult)
		c.logger.Error("failed to marshal realtime control frame",
			zap.String(logkeys.Operation, "encode_realtime_command_result"),
			zap.String(logkeys.Stage, "marshal_envelope"),
			zap.String(logkeys.Fallback, "drop_ack"),
			zap.String(appLogger.KeyRequestID, requestID),
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, uint64(c.userID)))
		return nil
	}
	return frame
}

// succeededTopic builds an ok TopicResult.
func succeededTopic(topic string) *proto_models.TopicResult {
	return &proto_models.TopicResult{Topic: topic, Ok: true}
}

// failedTopic builds a failed TopicResult carrying one of the TopicError* codes.
func failedTopic(topic string, errorCode string) *proto_models.TopicResult {
	return &proto_models.TopicResult{Topic: topic, Ok: false, ErrorCode: errorCode}
}

// rejectAllTopics fails every topic in an over-sized batch with the same code, so each of the
// client's independent consumers still learns its own outcome.
func rejectAllTopics(topics []string, errorCode string) []*proto_models.TopicResult {
	results := make([]*proto_models.TopicResult, 0, len(topics))
	for _, topic := range topics {
		results = append(results, failedTopic(topic, errorCode))
	}
	return results
}
