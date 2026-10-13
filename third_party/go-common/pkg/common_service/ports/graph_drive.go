package ports

import "github.com/Suhaibinator/go-common/pkg/proto_models"

// GraphResult is how one DriveGraph call left an owner-driven delegation graph.
type GraphResult struct {
	// Outcome is COMPLETED (the root's ResultReady is true), STALLED (nothing is drivable now or
	// later, nothing waits for an external event, and there is no result), OUT_OF_BUDGET (a limit of
	// the graph's budget pool is used up and no tool call it already paid for is left to drive),
	// PARKED (nothing is drivable until an external event), or CLAIM_LOST (the caller's context was
	// cancelled with worker.ErrClaimLost).
	Outcome proto_models.GraphOutcome
	// ParkReasons lists, for PARKED, every reason that applies: APPROVAL when runs wait for an
	// approval decision, INFERENCE_BLOCKED when a session is inference-blocked with a deferred wake
	// or an undrivable run.
	ParkReasons []proto_models.GraphParkReason
	// BudgetReason names, for OUT_OF_BUDGET, the pool limit that is used up.
	BudgetReason proto_models.GraphBudgetReason
	// Revision is the root's resume revision, read in the same snapshot as the final check. Park
	// the graph at it (ParkGraph), which refuses when an event committed after the read.
	Revision uint64
	// Outstanding is the work the graph still has.
	Outstanding OutstandingWork
}

// OutstandingWork is the unfinished work of a graph when DriveGraph returned.
type OutstandingWork struct {
	// ChildSessionIDs are the live children with a nonterminal run.
	ChildSessionIDs []uint64
	// ApprovalRunIDs are the runs waiting for an approval decision.
	ApprovalRunIDs []uint64
}
