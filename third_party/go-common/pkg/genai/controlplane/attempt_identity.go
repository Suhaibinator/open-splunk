package controlplane

import "uuid"

// NewInvocationID mints an invocation ledger correlation key in the form the control plane
// uses when ExecuteRequest.InvocationID is empty.
//
// One provider attempt is identified by the invocation ID together with the one-based attempt
// number the control plane passes to AttemptFunc/StreamAttemptFunc. The invocation ID stays
// constant across the retries of one Execute/ExecuteStream call and distinguishes a new
// invocation of the same durable agent step after an interrupted drive. A caller that has to
// report that identity while attempts are still running mints the ID here, sets
// ExecuteRequest.InvocationID, and closes over it in its attempt callback; the control plane
// does not publish attempt identity through the context.
//
// The ID is a V7 UUID: its leading timestamp keeps inserts into the attempt ledger's
// invocation index append-mostly and makes the IDs sort by creation time.
func NewInvocationID() string {
	return uuid.NewV7().String()
}
