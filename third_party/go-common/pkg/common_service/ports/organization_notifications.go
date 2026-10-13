package ports

import "context"

// OrganizationNotificationSink joins invitation lifecycle writes to durable
// notifications. Errors must abort the caller's source transaction.
type OrganizationNotificationSink interface {
	RequireInvitationNotificationTransaction(context.Context) error
	OrganizationInvitationRecorded(ctx context.Context, invitationID, recipientID uint64) error
	OrganizationInvitationResolved(ctx context.Context, invitationID uint64) error
}
