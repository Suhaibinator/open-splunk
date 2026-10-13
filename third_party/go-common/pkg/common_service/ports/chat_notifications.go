package ports

import "context"

// ChatNotificationSink persists notifications in the source operation's ambient
// transaction. An error must abort the source mutation. Install only when enabled.
// This is deliberately separate from best-effort post-commit realtime events.
type ChatNotificationSink interface {
	// ChatMessageRecorded records the emission for a durable transcript row in the
	// ambient source transaction. It is never called for ephemeral deltas.
	ChatMessageRecorded(context.Context, uint64) error
	ChatApprovalResolved(context.Context, uint64) error
	// ChatActivityRetired retires a reusable channel entry in the membership
	// mutation transaction. recipientID zero addresses every recipient in the
	// channel and queues durable cleanup instead of enumerating members inline.
	ChatActivityRetired(ctx context.Context, channelID, recipientID uint64) error
}
