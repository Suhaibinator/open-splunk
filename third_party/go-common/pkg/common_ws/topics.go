package common_ws

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// Subprotocol is the websocket subprotocol every realtime client must request during the upgrade.
// The handshake — not the envelope — carries the protocol version: a stale browser bundle that
// would otherwise decode the new envelope as the old message (proto3 decoders skip mismatched
// fields instead of failing) is rejected at connect time so its reconnect loop surfaces the
// failure. Future breaking wire changes bump this string.
const Subprotocol = "go-common.realtime.v1"

const (
	// TopicUser is the namespace of the server-side per-user topics ("user.{id}") and the literal
	// topic stamped on user-topic envelopes. The stamped value is recipient-relative ("your own
	// topic") precisely so a single envelope marshal can be shared by every recipient of a fan-out.
	// Clients may never subscribe to this namespace: their own topic is automatic, anyone else's is
	// forbidden.
	TopicUser = "user"

	// TopicNamespaceRealtime is the namespace the transport reserves for itself. Clients may never
	// subscribe to it.
	TopicNamespaceRealtime = "realtime"

	// TopicNamespaceChat is reserved against application registration and generic publishing.
	// Only the internal leased chat.channel.<id> policy can admit subscriptions; its ordinary
	// viewer audience never receives hidden messages or provider-state records.
	TopicNamespaceChat = "chat"

	// TopicControl carries server-to-client control frames (currently CommandResult acks).
	TopicControl = "realtime.control"

	// EventTypeCommandResult is the event type of a CommandResult frame delivered on TopicControl.
	EventTypeCommandResult = "realtime.command_result"
)

const (
	// CommandTypeSubscribe is the RealtimeCommand type the hub handles itself, before any command
	// router dispatch, so it works in a configuration with no registered command handlers at all.
	CommandTypeSubscribe = "subscribe"

	// CommandTypeUnsubscribe is the hub-owned counterpart of CommandTypeSubscribe.
	CommandTypeUnsubscribe = "unsubscribe"
)

// Per-topic failure codes reported on proto_models.TopicResult. A subscribe never fails silently:
// every requested topic gets an explicit outcome, because clients batch topics from independent
// consumers into a single command and each consumer needs its own result.
const (
	// TopicErrorInvalidTopic means the topic name violates the charset/length/segment rules.
	TopicErrorInvalidTopic = "invalid_topic"
	// TopicErrorReservedTopic means the topic lives in a namespace clients may not subscribe to.
	TopicErrorReservedTopic = "reserved_topic"
	// TopicErrorUnknownNamespace means no authorizer is registered for the topic (deny by default).
	TopicErrorUnknownNamespace = "unknown_namespace"
	// TopicErrorUnauthorizedTopic means the registered authorizer rejected this user.
	TopicErrorUnauthorizedTopic = "unauthorized_topic"
	// TopicErrorAuthorizerTimeout means the authorizer never reached a decision: the command's shared
	// auth budget expired (or the connection went away) while it was running. It is deliberately
	// distinct from TopicErrorUnauthorizedTopic — a timeout is a server-side capacity problem, not a
	// statement about this user's access, and a client may retry it rather than treating the topic as
	// forbidden.
	TopicErrorAuthorizerTimeout = "authorizer_timeout"
	// TopicErrorSubscriptionLimit means a per-connection or per-command subscription cap was hit.
	TopicErrorSubscriptionLimit = "subscription_limit"
)

const (
	// MaxTopicsPerCommand bounds one subscribe/unsubscribe command. Authorization runs synchronously
	// on the connection's read loop and may hit the database, so an unbounded batch inside the 4KB
	// message limit would be hundreds of DB round trips per frame. Exceeding it rejects the whole
	// command rather than truncating it.
	MaxTopicsPerCommand = 32

	// MaxTopicLength is the maximum length of a topic name in bytes.
	MaxTopicLength = 128

	// MaxTopicSegments is the maximum number of dot-separated segments in a topic name.
	MaxTopicSegments = 8

	// DefaultMaxSubscriptionsPerConnection is the fallback cap on entity-topic subscriptions held by
	// one connection when HubConfig does not set one.
	DefaultMaxSubscriptionsPerConnection = 256

	// DefaultSendQueueSize is the fallback bound on a connection's outbound queue when HubConfig does
	// not set one. The queue is allocated at accept time, so this is a per-connection memory cost.
	DefaultSendQueueSize = 64

	// DefaultMaxConnectionsPerUser is the fallback cap on concurrent connections held by one user when
	// HubConfig leaves MaxConnectionsPerUser at zero. Defaulting rather than treating zero as
	// "unlimited" is deliberate: an uncapped per-user connection count is an unbounded memory
	// commitment driven by client behavior. A HubConfig that really wants no cap sets a negative
	// value, which both enforcement sites read as unlimited.
	DefaultMaxConnectionsPerUser = 8

	// DefaultCommandAuthBudget is the single context budget shared by every authorizer call of one
	// command — not per topic. The binding constraint is the keepalive budget: gorilla dispatches
	// pong control frames from inside ReadMessage, so while the read goroutine blocks in a
	// synchronous authorizer the last-pong timestamp goes stale and the ping loop disconnects after
	// PingInterval + PongTimeout. A per-topic budget would blow through that; a shared command
	// budget stays inside it.
	//
	// This value must match config.defaultRealtimeWSCommandAuthBudget. pkg/config cannot import
	// common_ws, so the reload-time candidate validator carries its own copy of the fallback and
	// judges an omitted realtime_ws_command_auth_budget by the value the hub would actually run with.
	// Changing one without the other makes the validator reason about a budget nobody spends.
	DefaultCommandAuthBudget = 10 * time.Second
)

// TopicAuthorizer decides whether a connection's user may subscribe to a topic. Authorizers are
// registered per topic namespace at hub construction and matched at segment boundaries, longest
// registered prefix first: an authorizer for "chat.channel" matches "chat.channel.42" and never
// "chat.channelfoo.42". A namespace with no registered authorizer is denied by default.
//
// Authorization is checked at subscribe time only, so entity-topic events should carry thin
// payloads (ids/versions): the residual exposure window after an access revocation is metadata,
// not content. Features needing stricter semantics publish to user topics instead, whose recipients
// are recomputed per event.
type TopicAuthorizer[U common_service_models.UserLike] interface {
	// AuthorizeTopic returns nil to allow the subscription; any error denies it.
	AuthorizeTopic(ctx context.Context, user U, topic string) error
}

// TopicAuthorizerFunc adapts a function into a TopicAuthorizer for authorizers that do not need a
// dedicated struct type.
type TopicAuthorizerFunc[U common_service_models.UserLike] func(ctx context.Context, user U, topic string) error

// AuthorizeTopic executes this method as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - user: The connection's authenticated user.
//   - topic: The topic being subscribed to.
//
// Returns:
//   - result: Error encountered while executing this operation; nil allows the subscription.
func (f TopicAuthorizerFunc[U]) AuthorizeTopic(ctx context.Context, user U, topic string) error {
	return f(ctx, user, topic)
}

// UserTopic returns the reserved per-user topic name for a user ID. Publishing to it reaches the
// user on every connection they hold, regardless of what page is open.
//
// Parameters:
//   - userID: Identifier of the user whose topic is being addressed.
//
// Returns:
//   - result: The "user.{id}" topic name.
func UserTopic(userID common_service_models.UserIdType) string {
	return TopicUser + "." + strconv.FormatUint(uint64(userID), 10)
}

// BuildEntityTopic builds a canonical application-owned entity topic in the
// form "{namespace}.{positive decimal ID}". Application entity namespaces may
// contain multiple segments, but cannot use a namespace reserved by go-common.
func BuildEntityTopic(namespace string, entityID uint64) (string, error) {
	if !ValidTopicName(namespace) {
		return "", ErrInvalidTopic
	}
	if isRegistrationReservedTopic(namespace) {
		return "", ErrReservedTopic
	}
	if entityID == 0 {
		return "", ErrInvalidTopic
	}

	topic := namespace + "." + strconv.FormatUint(entityID, 10)
	if !ValidTopicName(topic) {
		return "", ErrInvalidTopic
	}
	return topic, nil
}

// ParseEntityTopic parses a canonical application-owned entity topic produced
// by BuildEntityTopic and returns its positive uint64 entity ID. The topic must
// match namespace exactly; sibling namespaces and extra trailing segments are
// rejected.
func ParseEntityTopic(namespace, topic string) (uint64, error) {
	if _, err := BuildEntityTopic(namespace, 1); err != nil {
		return 0, err
	}
	if !ValidTopicName(topic) {
		return 0, ErrInvalidTopic
	}

	value, found := strings.CutPrefix(topic, namespace+".")
	if !found || strings.Contains(value, ".") {
		return 0, ErrInvalidTopic
	}
	entityID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || entityID == 0 || strconv.FormatUint(entityID, 10) != value {
		return 0, ErrInvalidTopic
	}
	return entityID, nil
}

// ValidTopicName reports whether topic satisfies the wire contract: charset [a-z0-9_.-], at most
// MaxTopicLength bytes, and between 1 and MaxTopicSegments non-empty dot-separated segments. It is
// checked before any authorizer lookup so malformed names never reach application code.
//
// Parameters:
//   - topic: The topic name to validate.
//
// Returns:
//   - result: Boolean result indicating whether the name is well formed.
func ValidTopicName(topic string) bool {
	if topic == "" || len(topic) > MaxTopicLength {
		return false
	}
	segments := 1
	segmentLength := 0
	for i := 0; i < len(topic); i++ {
		ch := topic[i]
		if ch == '.' {
			if segmentLength == 0 {
				return false
			}
			segments++
			if segments > MaxTopicSegments {
				return false
			}
			segmentLength = 0
			continue
		}
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9':
		case ch == '_' || ch == '-':
		default:
			return false
		}
		segmentLength++
	}
	return segmentLength > 0
}

// parseUserTopic reports whether topic is a per-user topic and, if so, the user ID it addresses.
// The hub routes those publishes through the per-user connection index rather than the topic
// registry: that index is the user-topic subscription, which is why auto-subscribe is atomic with
// registration by construction.
func parseUserTopic(topic string) (common_service_models.UserIdType, bool) {
	rest, ok := strings.CutPrefix(topic, TopicUser+".")
	if !ok {
		return 0, false
	}
	userID, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || userID == 0 {
		return 0, false
	}
	return common_service_models.UserIdType(userID), true
}

// topicNamespace returns the first segment of a topic name.
func topicNamespace(topic string) string {
	if before, _, ok := strings.Cut(topic, "."); ok {
		return before
	}
	return topic
}

// isReservedTopic reports whether a topic belongs to a namespace go-common owns and clients may
// never subscribe to.
func isReservedTopic(topic string) bool {
	switch topicNamespace(topic) {
	case TopicUser, TopicNamespaceRealtime:
		return true
	default:
		return false
	}
}

// isRegistrationReservedTopic reports whether a namespace belongs to go-common and therefore cannot
// be claimed by an application authorizer.
//
// It is deliberately wider than isReservedTopic: clients may not name user.* / realtime.* topics at
// all, whereas chat.* stays unclaimable by applications. Only the internal chat.channel.<id>
// authorizer admits chat subscriptions; every other chat topic remains deny-by-default.
func isRegistrationReservedTopic(topic string) bool {
	if isReservedTopic(topic) {
		return true
	}
	return topicNamespace(topic) == TopicNamespaceChat
}
