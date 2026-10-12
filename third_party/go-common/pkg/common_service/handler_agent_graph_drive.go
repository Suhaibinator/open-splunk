package common_service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/go-common/pkg/worker"
	"go.uber.org/zap"
)

// DriveGraph drives the owner-driven delegation graph rooted at rootSessionID, the root of a
// workflow-owned type with ResultReady, until nothing in it can make progress without an external
// event, and reports why. It drives up to the root delegation's MaxConcurrentDrives runs at once,
// each through RunWorkflowAgentTurn's ownership checks with its in-memory share of the graph's
// remaining active time. A run in retry backoff or under another attempt's live lease is waited for
// and driven; a run whose drive changed nothing is driven again after
// chat_agent_workflow_wait_poll_interval. One drive's error does not stop the others: the run is
// left alone for the rest of the call, and the error is returned when the graph ends with that run
// still unfinished and no result. Once a pool limit is used up, only runs with tool calls their steps
// were already charged for (ToolWork) are driven, each once those calls are recoverable and no
// abandoned drive in this process still executes one, and none once the pool's active time is spent.
//
// The root's ResultReady is asked only after its OwnerValidator admits the caller. When it reports
// true, DriveGraph stops claiming, suspends the drives still in flight (their abandonments are
// uncounted and leave the runs drivable), waits for them, and durably revokes every drive lease in
// the graph before it returns COMPLETED; it returns an error when that revocation cannot commit. The application then, in one
// transaction, accepts the result, moves its job to a state its OwnerValidator rejects, and
// retires the graph. ctx cancelled with worker.ErrClaimLost suspends the drives and returns
// CLAIM_LOST; any other cancellation suspends them and returns ctx's cause.
func (h *CommonServiceHandler[U]) DriveGraph(ctx context.Context, rootSessionID uint64) (result ports.GraphResult, err error) {
	if h == nil || h.agentRuntime == nil {
		return ports.GraphResult{}, commonai.ErrContinuationNotInitialized
	}
	defer func() {
		outcome := result.Outcome
		if err != nil {
			outcome = proto_models.GraphOutcome_GRAPH_OUTCOME_UNSPECIFIED
		}
		h.commonPersistenceHandler.GetMetricsHandler().RecordChatAgentGraphDrive(outcome, result.ParkReasons)
	}()
	if rootSessionID == 0 || h.agentRegistry == nil {
		return ports.GraphResult{}, serviceErrors.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return callerCancelled(ctx)
	}
	ctx = common_persistence.WithoutRequestTransaction[U](ctx)
	root, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, rootSessionID)
	if err != nil {
		return setupFailed(ctx, err)
	}
	if root == nil {
		return ports.GraphResult{}, serviceErrors.ErrChatAgentSessionNotFound
	}
	def, ok := h.agentRegistry.GetAgentType(root.AgentType)
	if !ok || !def.IsWorkflowOwned() || !def.CanBeRoot() || def.Workflow.ResultReady == nil ||
		root.IsDelegatedChild() || root.Status != models.ChatAgentSessionStatusActive {
		return ports.GraphResult{}, serviceErrors.ErrInvalidRequest
	}
	drives := 1
	delegation, err := chatpkg.SessionConfigDelegation(ctx, h.commonPersistenceHandler, def, root.ID, root.ActiveConfigVersion)
	if err != nil {
		return setupFailed(ctx, err)
	}
	if delegation != nil && delegation.MaxConcurrentDrives > 0 {
		drives = int(delegation.MaxConcurrentDrives)
	}

	// The graph context outlives ctx's cancellation so that claim loss and Completed cleanup both
	// cancel it with errGraphDriveSuspended, the cause the runtime treats like Stop.
	graphCtx, suspend := context.WithCancelCause(context.WithoutCancel(ctx))
	defer suspend(errGraphDriveSuspended)
	d := &graphDrive[U]{
		h: h, ctx: graphCtx, suspend: suspend, root: root.ID, def: def,
		drives: drives, policy: h.agentRuntime.recoveryPolicy(), inert: h.agentRuntime.generatorFactory == nil,
		inFlight: map[uint64]time.Duration{}, claimed: map[uint64]common_persistence.ChatAgentGraphRun{}, errored: map[uint64]error{},
	}
	defer context.AfterFunc(ctx, func() {
		cause := proto_models.GraphOutcome_GRAPH_OUTCOME_UNSPECIFIED
		if errors.Is(context.Cause(ctx), worker.ErrClaimLost) {
			cause = proto_models.GraphOutcome_GRAPH_OUTCOME_CLAIM_LOST
		}
		d.stop(cause)
	})()
	return d.run(ctx)
}

// graphDrive is one DriveGraph call. Its maps are guarded by mu.
type graphDrive[U common_service_models.UserLike] struct {
	h *CommonServiceHandler[U]
	// ctx is the graph context; suspend cancels it with errGraphDriveSuspended.
	ctx     context.Context
	suspend context.CancelCauseFunc
	root    uint64
	def     ports.AgentDefinition
	drives  int
	policy  agentRecoveryPolicy
	// inert records a runtime with no generator factory, whose drives execute nothing.
	inert bool

	mu sync.Mutex
	// inFlight maps each run being driven to its share of the pool's remaining active time.
	inFlight map[uint64]time.Duration
	// returned counts the drives that have returned. Each records its active time before it leaves
	// inFlight, so a claim whose graph read overlapped a return reads again: its pool snapshot may
	// predate that time while its reservation is already gone.
	returned uint64
	// claimed is each run's progress when it was last claimed: a run still there was not changed by
	// its drive, and is driven again only after the poll interval.
	claimed map[uint64]common_persistence.ChatAgentGraphRun
	// errored holds runs whose drive failed; they are left alone for the rest of the call.
	errored   map[uint64]error
	rootRunID uint64
	// cause is why the graph context was first suspended: COMPLETED, CLAIM_LOST, or UNSPECIFIED for
	// another cancellation or a failed claim.
	cause proto_models.GraphOutcome
	// budgetReason is the first pool limit seen used up or refusing a drive's charge; activeSpent
	// records that the pool's active time is spent, and paidWork that the last graph read found the
	// pool used up and paid tool work this call can drive.
	budgetReason proto_models.GraphBudgetReason
	activeSpent  bool
	paidWork     bool
	retired      bool
	completed    bool
	resultErr    error
}

// graphDriveClaim is one claimed run and its share of the pool's remaining active time (zero: no
// pool).
type graphDriveClaim struct {
	runID uint64
	share time.Duration
}

// sameProgress reports whether a drive changed nothing that progress changes on a run.
func sameProgress(a, b common_persistence.ChatAgentGraphRun) bool {
	return a.Status == b.Status && a.GenerationCount == b.GenerationCount && a.ToolCallCount == b.ToolCallCount &&
		(a.LastSealedStepIndex == nil) == (b.LastSealedStepIndex == nil) &&
		common_util.Deref(a.LastSealedStepIndex) == common_util.Deref(b.LastSealedStepIndex)
}

func (d *graphDrive[U]) run(callerCtx context.Context) (ports.GraphResult, error) {
	state, err := d.h.commonPersistenceHandler.GetChatAgentGraphDriveState(d.ctx, d.root)
	if err != nil {
		return setupFailed(callerCtx, err)
	}
	if !d.stopped(state) {
		// A result that is already ready needs no drive.
		d.checkResult()
	}
	for d.ctx.Err() == nil {
		err := worker.Drain(d.ctx, d.drives, d.claim, d.process, worker.DrainOptions{Logger: d.h.logger, IdlePollInterval: d.policy.graphClaimPollInterval})
		d.mu.Lock()
		completed := d.completed
		d.mu.Unlock()
		if err != nil && callerCtx.Err() == nil && !completed {
			return ports.GraphResult{}, fmt.Errorf("drive graph %d: %w", d.root, err)
		}
		if d.ctx.Err() != nil {
			break
		}
		state, err := d.h.commonPersistenceHandler.GetChatAgentGraphDriveState(d.ctx, d.root)
		if err != nil {
			if d.ctx.Err() != nil {
				break
			}
			return ports.GraphResult{}, err
		}
		stopped := d.stopped(state)
		wait, now := d.nextDue(state)
		if stopped || (!now && wait == 0) {
			return d.finish(callerCtx, state)
		}
		if !now {
			// Not due yet: wait for the earliest backoff or lease, or the poll interval for a run
			// whose drive changed nothing, then drive again.
			select {
			case <-d.ctx.Done():
			case <-time.After(wait):
			}
			d.mu.Lock()
			clear(d.claimed)
			d.mu.Unlock()
		}
	}
	// Suspended: by the caller's cancellation, or by Completed cleanup.
	return d.complete(callerCtx)
}

// stopped reports, under one read of state, whether the drive must stop claiming: its result is
// ready or unreadable, the root was retired, the pool's active time is spent, or the pool has
// refused work and no drivable run has paid tool work left that this call can execute (see paid).
// Paid tool work counts only while the pool reads used up, so that a run's next generation is
// refused too: a charge refused for not fitting a pool with room stops at once. Without state it
// checks what the last read recorded.
func (d *graphDrive[U]) stopped(state *common_persistence.ChatAgentGraphDriveState) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state != nil {
		d.rootRunID = state.RootRunID
		if state.Root.Status != models.ChatAgentSessionStatusActive {
			d.retired = true
		}
		reason := state.Pool.ExhaustedReason()
		if reason != proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED && d.budgetReason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
			d.budgetReason = reason
		}
		d.activeSpent = d.activeSpent || (state.Pool != nil && state.Pool.RemainingActive() == 0)
		d.paidWork = reason != proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED &&
			slices.ContainsFunc(state.Runs, func(run common_persistence.ChatAgentGraphRun) bool {
				_, failed := d.errored[run.ID]
				return !failed && graphRunDrivable(state, run) && d.paid(run)
			})
	}
	return d.completed || d.resultErr != nil || d.retired || d.activeSpent ||
		(d.budgetReason != proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED && !d.paidWork)
}

// drivable reports whether this call may drive run once it is due: graphRunDrivable, and, once a
// pool limit is used up, paid tool work to execute, which charges nothing more. The caller holds mu.
func (d *graphDrive[U]) drivable(state *common_persistence.ChatAgentGraphDriveState, run common_persistence.ChatAgentGraphRun) bool {
	return graphRunDrivable(state, run) && (d.budgetReason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED || d.paid(run))
}

// paid reports whether this call can execute tool calls run's steps were already charged for: run
// has some, the runtime is not inert (it executes none), and, unless this call's drive of run is in
// flight, no abandoned drive in this process is still executing one, which makes the run's tool
// recovery skip them all until it returns. The caller holds mu.
func (d *graphDrive[U]) paid(run common_persistence.ChatAgentGraphRun) bool {
	if !run.ToolWork || d.inert {
		return false
	}
	_, inFlight := d.inFlight[run.ID]
	chat, ok := d.h.chatHandler.(interface{ WorkflowAgentToolsBusy(uint64) bool })
	return inFlight || !ok || !chat.WorkflowAgentToolsBusy(run.ID)
}

// dueIn is how long until this call may claim run: its DueIn and, once a pool limit is used up and
// only its paid tool work is driven, until that work is recoverable, so that no drive spends the
// pool's active time waiting for it. The caller holds mu.
func (d *graphDrive[U]) dueIn(run common_persistence.ChatAgentGraphRun) time.Duration {
	if d.budgetReason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		return run.DueIn
	}
	return max(run.DueIn, run.ToolWorkDueIn)
}

// claim picks the first drivable run of the graph that is due, not in flight, not failed in this
// call, and not unchanged since its last drive, and gives it an equal share of the pool time the
// in-flight drives have not been given.
func (d *graphDrive[U]) claim(ctx context.Context) (graphDriveClaim, bool, error) {
	for {
		if d.ctx.Err() != nil || d.stopped(nil) {
			return graphDriveClaim{}, false, nil
		}
		d.mu.Lock()
		returned := d.returned
		d.mu.Unlock()
		state, err := d.h.commonPersistenceHandler.GetChatAgentGraphDriveState(ctx, d.root)
		if err != nil {
			if d.ctx.Err() != nil {
				return graphDriveClaim{}, false, nil
			}
			return graphDriveClaim{}, false, err
		}
		if d.stopped(state) {
			return graphDriveClaim{}, false, nil
		}
		if c, ok, current := d.claimFrom(state, returned); current {
			return c, ok, nil
		}
	}
}

// claimFrom claims from state, read when returned drives had returned; current is false when
// another drive has returned since, and the claim must read the graph again.
func (d *graphDrive[U]) claimFrom(state *common_persistence.ChatAgentGraphDriveState, returned uint64) (c graphDriveClaim, ok, current bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.returned != returned {
		return graphDriveClaim{}, false, false
	}
	for _, run := range state.Runs {
		if _, busy := d.inFlight[run.ID]; busy || d.dueIn(run) > 0 || !d.drivable(state, run) || d.skip(run) {
			continue
		}
		var share time.Duration
		if state.Pool != nil {
			available := state.Pool.RemainingActive()
			for _, given := range d.inFlight {
				available -= given
			}
			free := d.drives - len(d.inFlight)
			if available <= 0 || free <= 0 {
				return graphDriveClaim{}, false, true
			}
			share = max(available/time.Duration(free), time.Millisecond)
		}
		d.inFlight[run.ID] = share
		d.claimed[run.ID] = run
		return graphDriveClaim{runID: run.ID, share: share}, true, true
	}
	return graphDriveClaim{}, false, true
}

// skip reports a run this call leaves alone for now: its drive failed, or its last drive changed
// nothing. The caller holds mu.
func (d *graphDrive[U]) skip(run common_persistence.ChatAgentGraphRun) bool {
	if _, failed := d.errored[run.ID]; failed {
		return true
	}
	last, ok := d.claimed[run.ID]
	return ok && sameProgress(last, run)
}

// process drives one claimed run and records its outcome; it never returns an error, because
// Drain would abort every other drive. After each drive it evaluates the root's ResultReady.
func (d *graphDrive[U]) process(ctx context.Context, c graphDriveClaim) error {
	// The graph context, not Drain's, so that the drive is abandoned as suspended, not as if its
	// deadline had passed. Drain cancels its own when a claim fails or a callback panics, and the
	// call is then ending with that error: suspend the graph rather than let this drive run on.
	defer context.AfterFunc(ctx, func() { d.stop(proto_models.GraphOutcome_GRAPH_OUTCOME_UNSPECIFIED) })()
	err := d.h.runWorkflowAgentTurn(d.ctx, c.runID, d.ctx.Done(), c.share)
	suspended := d.ctx.Err() != nil
	exhausted, isExhausted := errors.AsType[*common_persistence.GraphBudgetExhaustedError](err)
	failed := err != nil && !suspended && !isExhausted && !errors.Is(err, errGraphDriveShareElapsed)
	d.mu.Lock()
	if suspended {
		d.h.commonPersistenceHandler.GetMetricsHandler().RecordChatAgentGraphDriveSuspension(d.cause)
	}
	delete(d.inFlight, c.runID)
	d.returned++
	if isExhausted && !suspended && d.budgetReason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		d.budgetReason = exhausted.Reason
	}
	d.activeSpent = d.activeSpent || (isExhausted && !suspended && exhausted.Reason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION)
	if failed {
		d.errored[c.runID] = err
	}
	d.mu.Unlock()
	if failed {
		appLogger.For(ctx, d.h.logger, agentRuntimeLoggerName).Warn("drive graph: run drive failed; leaving the run for the rest of this drive",
			zap.String(logkeys.Stage, "drive_graph"),
			zap.Uint64(appLogger.KeyAgentSessionID, d.root),
			zap.Uint64(appLogger.KeyAgentRunID, c.runID), zap.Error(err))
	}
	if !suspended {
		d.checkResult()
	}
	return nil
}

// checkResult evaluates the root's ResultReady, after its OwnerValidator admits the caller (a
// refusal fails the drive closed), and, when it is true, starts Completed cleanup at once by
// suspending the graph rather than waiting for slower drives to reach their deadlines.
func (d *graphDrive[U]) checkResult() {
	d.mu.Lock()
	check := ports.OwnerCheck{RunID: d.rootRunID, SessionID: d.root, RootSessionID: d.root}
	d.mu.Unlock()
	ready, err := false, ports.ValidateGraphOwner(d.ctx, d.h.agentRegistry, d.def, "", check)
	if err == nil {
		ready, err = d.def.Workflow.ResultReady(d.ctx, check)
	}
	if d.ctx.Err() != nil && !ready {
		return
	}
	d.mu.Lock()
	switch {
	case err != nil:
		if d.resultErr == nil {
			d.resultErr = fmt.Errorf("drive graph %d: root result check: %w", d.root, err)
		}
	case ready:
		d.completed = true
	}
	d.mu.Unlock()
	if ready && err == nil {
		d.stop(proto_models.GraphOutcome_GRAPH_OUTCOME_COMPLETED)
	}
}

// stop suspends the graph context, recording cause when it is the first suspension.
func (d *graphDrive[U]) stop(cause proto_models.GraphOutcome) {
	d.mu.Lock()
	if d.ctx.Err() == nil && d.cause == proto_models.GraphOutcome_GRAPH_OUTCOME_UNSPECIFIED {
		d.cause = cause
	}
	d.mu.Unlock()
	d.suspend(errGraphDriveSuspended)
}

// nextDue reports when the graph has drivable work again after a drain: now, when a run became
// due after the drain's last claim, or after the wait until the earliest backoff or lease expiry,
// or the poll interval for a run its last drive did not change. Zero and false: no drivable work.
func (d *graphDrive[U]) nextDue(state *common_persistence.ChatAgentGraphDriveState) (time.Duration, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var wait time.Duration
	for _, run := range state.Runs {
		if _, failed := d.errored[run.ID]; failed || !d.drivable(state, run) {
			continue
		}
		due := d.dueIn(run)
		if due == 0 {
			if !d.skip(run) {
				return 0, true
			}
			due = d.policy.workflowPollInterval
		}
		if wait == 0 || due < wait {
			wait = due
		}
	}
	return wait, false
}

// finish reports a graph with nothing left to drive now or later, from the snapshot of that final
// check, whose revision it returns.
func (d *graphDrive[U]) finish(callerCtx context.Context, state *common_persistence.ChatAgentGraphDriveState) (ports.GraphResult, error) {
	d.checkResult()
	if d.ctx.Err() != nil {
		return d.complete(callerCtx)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.retired {
		return ports.GraphResult{}, fmt.Errorf("%w: graph root %d was retired", serviceErrors.ErrInvalidRequest, d.root)
	}
	if d.resultErr != nil {
		return ports.GraphResult{}, d.resultErr
	}
	result := graphResultOf(state)
	if d.budgetReason != proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		result.Outcome, result.BudgetReason, result.ParkReasons = proto_models.GraphOutcome_GRAPH_OUTCOME_OUT_OF_BUDGET, d.budgetReason, nil
		return result, nil
	}
	var errs []error
	for _, run := range state.Runs {
		if err, failed := d.errored[run.ID]; failed {
			errs = append(errs, fmt.Errorf("run %d: %w", run.ID, err))
		}
	}
	if len(errs) > 0 {
		return ports.GraphResult{}, fmt.Errorf("drive graph %d: %w", d.root, errors.Join(errs...))
	}
	if len(result.ParkReasons) > 0 {
		result.Outcome = proto_models.GraphOutcome_GRAPH_OUTCOME_PARKED
	} else {
		result.Outcome = proto_models.GraphOutcome_GRAPH_OUTCOME_STALLED
	}
	return result, nil
}

// complete ends a suspended graph with the caller's cancellation or, after Completed cleanup once
// every drive has returned, with the revocation barrier. A returned drive does not prove its attempt
// was revoked (a suspended abandonment does not retry a failed revocation), and an expired lease
// does not stop a transaction that passed its token check earlier from committing, so DriveGraph
// durably revokes every drive lease in the graph, waiting for any transaction holding a run's row;
// when that cannot commit, it returns an error rather than COMPLETED.
func (d *graphDrive[U]) complete(callerCtx context.Context) (ports.GraphResult, error) {
	if callerCtx.Err() != nil {
		return callerCancelled(callerCtx)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(callerCtx), d.policy.dbTimeout)
	defer cancel()
	err := d.h.commonPersistenceHandler.RevokeChatAgentGraphDriveLeases(readCtx, d.root)
	var state *common_persistence.ChatAgentGraphDriveState
	if err == nil {
		state, err = d.h.commonPersistenceHandler.GetChatAgentGraphDriveState(readCtx, d.root)
	}
	if callerCtx.Err() != nil {
		return callerCancelled(callerCtx)
	}
	if err != nil {
		return ports.GraphResult{}, fmt.Errorf("drive graph %d: confirm its drives were revoked: %w", d.root, err)
	}
	result := graphResultOf(state)
	result.Outcome, result.ParkReasons = proto_models.GraphOutcome_GRAPH_OUTCOME_COMPLETED, nil
	return result, nil
}

// setupFailed reports an error before the graph is driven, or the caller's cancellation when that
// is what interrupted the setup.
func setupFailed(ctx context.Context, err error) (ports.GraphResult, error) {
	if ctx.Err() != nil {
		return callerCancelled(ctx)
	}
	return ports.GraphResult{}, err
}

// callerCancelled reports the caller's cancellation: CLAIM_LOST for worker.ErrClaimLost, otherwise
// the cancellation's cause.
func callerCancelled(ctx context.Context) (ports.GraphResult, error) {
	cause := context.Cause(ctx)
	if errors.Is(cause, worker.ErrClaimLost) {
		return ports.GraphResult{Outcome: proto_models.GraphOutcome_GRAPH_OUTCOME_CLAIM_LOST}, nil
	}
	return ports.GraphResult{}, cause
}

// graphRunDrivable reports whether an owner drive can advance run now or once it is due: its
// session is live, not inference-blocked, and not stopped on it, and it is running, retryable, or
// waiting, with no call awaiting an approval decision or with tool work to execute meanwhile (an
// approved call, or a mixed batch's non-approval calls). A run waiting only for decisions, whatever
// its status, and a blocked run wait for an external event instead.
func graphRunDrivable(state *common_persistence.ChatAgentGraphDriveState, run common_persistence.ChatAgentGraphRun) bool {
	session := state.Session(run.AgentSessionID)
	if session == nil || session.Status != models.ChatAgentSessionStatusActive ||
		session.InferenceBlockedReason != proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED ||
		(session.SuspendedRunID != nil && *session.SuspendedRunID == run.ID) {
		return false
	}
	switch run.Status {
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL:
		return !run.AwaitingApproval || run.ToolWork
	}
	return false
}

// graphResultOf fills a result's revision, outstanding work, and park reasons from state.
func graphResultOf(state *common_persistence.ChatAgentGraphDriveState) ports.GraphResult {
	result := ports.GraphResult{Revision: state.Root.ResumeRevision}
	blocked := false
	for _, run := range state.Runs {
		session := state.Session(run.AgentSessionID)
		if session == nil || session.Status != models.ChatAgentSessionStatusActive {
			continue
		}
		if session.ID != state.Root.ID && !slices.Contains(result.Outstanding.ChildSessionIDs, session.ID) {
			result.Outstanding.ChildSessionIDs = append(result.Outstanding.ChildSessionIDs, session.ID)
		}
		if run.AwaitingApproval {
			result.Outstanding.ApprovalRunIDs = append(result.Outstanding.ApprovalRunIDs, run.ID)
		}
		blocked = blocked || session.InferenceBlockedReason != proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED
	}
	for _, session := range append([]models.ChatAgentSession{state.Root}, state.Children...) {
		blocked = blocked || (session.Status == models.ChatAgentSessionStatusActive && session.PendingWakeMessageID != nil &&
			session.InferenceBlockedReason != proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED)
	}
	if len(result.Outstanding.ApprovalRunIDs) > 0 {
		result.ParkReasons = append(result.ParkReasons, proto_models.GraphParkReason_GRAPH_PARK_REASON_APPROVAL)
	}
	if blocked {
		result.ParkReasons = append(result.ParkReasons, proto_models.GraphParkReason_GRAPH_PARK_REASON_INFERENCE_BLOCKED)
	}
	return result
}
