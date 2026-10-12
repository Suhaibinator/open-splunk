package common_context

import "context"

// clearValues returns a child of ctx whose state lacks the bits in mask, with
// reset zeroing the matching fields of the copy. It returns ctx unchanged without
// creating a wrapper when ctx carries none of those values.
func clearValues(ctx context.Context, mask presenceBits, reset func(*commonContext)) context.Context {
	if ctx == nil {
		return context.Background()
	}
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence&mask == 0 {
		return ctx
	}
	next := new(commonContext)
	*next = *cc
	reset(next)
	next.presence &^= mask
	return attach(ctx, next)
}

// ClearWorkflowAgentRun removes the stored workflow-owned agent run ID and its presence.
// It returns ctx unchanged without creating a wrapper when no ID is stored. Otherwise it
// returns a child context that shadows the value; unlike scontext's Clear helpers it never
// mutates ctx, so parent and sibling operations retain their values without a copy step.
func ClearWorkflowAgentRun(ctx context.Context) context.Context {
	return clearValues(ctx, presentWorkflowAgentRun, func(cc *commonContext) {
		cc.workflowAgentRunID = 0
	})
}

// ClearChatAgentRunDriveLease removes the stored run-drive lease, run ID and owner together,
// and its presence. It returns ctx unchanged without creating a wrapper when no lease is
// stored. Otherwise it returns a child context that shadows the lease; unlike scontext's
// Clear helpers it never mutates ctx, so parent and sibling operations retain their lease
// without a copy step.
func ClearChatAgentRunDriveLease(ctx context.Context) context.Context {
	return clearValues(ctx, presentChatAgentRunDriveLease, func(cc *commonContext) {
		cc.chatAgentRunDriveLease = ChatAgentRunDriveLease{}
	})
}

// ClearChatAgentDelegationOrigin removes source authority without affecting a
// destination drive lease or changing parent and sibling contexts. Absent
// authority returns ctx unchanged without allocating a wrapper.
func ClearChatAgentDelegationOrigin(ctx context.Context) context.Context {
	return clearValues(ctx, presentChatAgentDelegationOrigin, func(cc *commonContext) {
		cc.chatAgentDelegationOrigin = ChatAgentRunDriveLease{}
	})
}

// ClearAgentToolInvocation removes the stored tool invocation and its presence.
// It returns ctx unchanged without creating a wrapper when no invocation is stored.
// Otherwise it returns a child context that shadows the value; unlike scontext's Clear
// helpers it never mutates ctx, so parent and sibling operations retain their values
// without a copy step.
func ClearAgentToolInvocation(ctx context.Context) context.Context {
	return clearValues(ctx, presentAgentToolInvocation, func(cc *commonContext) {
		cc.agentToolInvocation = nil
	})
}

// ClearAttemptActorUserID removes the stored attempt actor and its presence, so the recorder
// falls back to the request user. It returns ctx unchanged without creating a wrapper when
// no actor is stored. Otherwise it returns a child context that shadows the value; unlike
// scontext's Clear helpers it never mutates ctx, so parent and sibling operations retain
// their values without a copy step.
func ClearAttemptActorUserID(ctx context.Context) context.Context {
	return clearValues(ctx, presentAttemptActorUserID, func(cc *commonContext) {
		cc.attemptActorUserID = 0
	})
}

// WithoutState returns a child of ctx that carries no go-common state: the workflow run, the
// run-drive lease, delegation origin, tool invocation, and attempt actor are all absent from it, not stored
// as zero. It also drops the drive lease, so a caller that must keep a lease cannot use it.
// A nil ctx yields context.Background(). It returns ctx unchanged without creating a wrapper when
// ctx carries no values. Like the Clear helpers it never mutates ctx, so parent and sibling
// contexts retain their values; deadlines, cancellation, and unrelated values are inherited.
func WithoutState(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	cc := getCommonContext(ctx)
	if cc == nil || cc.presence == 0 {
		return ctx
	}
	return attach(ctx, &commonContext{})
}
