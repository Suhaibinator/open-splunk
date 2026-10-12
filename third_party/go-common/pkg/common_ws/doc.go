// Package common_ws is go-common's single-instance realtime WebSocket transport: the hub, the
// connection lifecycle (read/write/keepalive loops, bounded send queues, graceful shutdown), the
// topic registry, and the generic RealtimeEvent/RealtimeCommand envelopes that carry application
// payloads without go-common ever enumerating them.
//
// Chat is the first consumer of this layer, not the definition of it. Applications register their
// own topic namespaces, authorizers, and command handlers at construction time and publish through
// ports.RealtimePublisher.
//
// # Topics
//
// Topic names are dot-separated. There are two classes:
//
//   - User topics ("user.{id}"). Every connection is subscribed to its own as part of registration —
//     the per-user connection index *is* that subscription, so there is no registered-but-not-yet-
//     subscribed window. Clients can never name them: their own is automatic and anyone else's is
//     forbidden.
//   - Entity topics (application-defined, e.g. "grading.submission.42"). Clients subscribe
//     explicitly, the registered TopicAuthorizer for the namespace decides, and the subscription is
//     dropped on disconnect. A namespace with no registered authorizer is denied by default.
//     BuildEntityTopic and ParseEntityTopic own the canonical namespace-plus-positive-uint64 shape.
//
// go-common reserves the user, realtime, and chat namespaces; applications own everything else.
// Internal chat.channel.<id> subscriptions have read-authorized, renewable leases. Their dedicated
// audience publisher excludes participant recipients from viewer fan-out and never widens hidden
// messages to the viewer audience. Generic Publish cannot address these internal topics.
//
// # Delivery semantics (the contract)
//
// This is a best-effort push channel, not a queue. The guarantees are deliberately narrow, and code
// that cannot live within them does not belong on this transport:
//
//   - At-most-once, with no replay. Per-connection send queues are bounded, and a connection under
//     sustained backpressure is disconnected rather than allowed to consume unbounded memory. The
//     client's contract after any disconnect is: reconnect, resubscribe, and catch up over REST.
//   - Per-connection frame order is preserved by the single writer goroutine. There is no
//     cross-connection and no cross-topic ordering guarantee, and there is no sequence field on the
//     envelope — an arbitrary multi-producer topic has no authority that could assign one.
//     Applications needing versions or sequences put them in their own payload message, where they
//     can define that authority.
//   - The subscribe acknowledgement for a topic is enqueued before any event for that topic, so a
//     client always observes ack-then-events.
//   - A given logical event is published to user topics *or* to an entity topic, never both. Double
//     publishing would double-deliver to any client holding both, against a bounded queue whose
//     overflow disconnects them.
//
// Two event shapes are sanctioned:
//
//  1. Thin invalidation events. The payload carries identifiers and versions; the client refetches
//     over REST. Loss-tolerant and idempotent by construction, and the default for job/entity status.
//  2. Self-healing accumulative payloads. Each frame carries enough state to recover from a dropped
//     predecessor — chat's MessageStreamEvent.accumulated_text is the model.
//
// Fat one-shot payloads, where state is lost for good if a frame is dropped, are not supported.
//
// # Concurrency invariants
//
// The publish path is deadlock-free only because it snapshots the target connections under a read
// lock, releases it, and only then enqueues and closes. connection.close re-enters the hub through
// unregister, which takes the write lock, so iterating a connection set while holding the lock and
// closing inline would self-deadlock.
//
// Registration of command handlers and topic authorizers is construction-time wiring: it is rejected
// once the hub has served its first connection, which is what makes those registries safe to read
// without a lock from connection goroutines.
//
// # Configuration
//
// HubConfig splits by reload policy. The origin allowlist, ping interval, pong timeout, read
// timeout, message-size limit and command auth budget are re-captured from RealtimeView for every
// handshake, so a configuration reload governs connections accepted afterwards while an accepted
// connection keeps the settings it started with. The send queue size, per-user connection cap and
// per-connection subscription cap are startup-only: queues are allocated at accept time and the caps
// need a single authority, so the hub captures them once at construction.
//
// The command auth budget must stay below PingInterval + PongTimeout, because authorizers run
// synchronously on the read goroutine while pongs are dispatched from inside ReadMessage. All three
// are hot and are captured together, so the invariant is a property of each connection: a reload
// that lowers the keepalive window and the budget in the same generation can never leave a live
// connection spending the old budget against the new window. The hub still validates the triple at
// construction and again for a hand-supplied handshake config, and config.ValidateConfig rejects any
// candidate generation whose own three values are incoherent.
//
// # Scale
//
// The hub is in-memory and single-instance. Horizontally scaling the API without a broker-backed
// ports.RealtimePublisher silently breaks fan-out, because each instance only knows its own
// connections. That interface is the seam where a multi-instance implementation would slot in.
package common_ws
