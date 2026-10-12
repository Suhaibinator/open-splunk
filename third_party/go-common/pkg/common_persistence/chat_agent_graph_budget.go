package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrGraphBudgetExhausted means a limit of the delegation graph's budget pool is used up, so
	// the graph admits no new work: no spawn, send, resume, or model step.
	ErrGraphBudgetExhausted = errors.New("delegation graph budget exhausted")
	// ErrGraphBudgetInvalid means SetChatAgentGraphBudget was given a session that is not a live
	// graph root, or a limit of zero.
	ErrGraphBudgetInvalid = errors.New("delegation graph budget is invalid")
)

// GraphBudgetExhaustedError is ErrGraphBudgetExhausted with the pool limit that is used up.
type GraphBudgetExhaustedError struct {
	Reason proto_models.GraphBudgetReason
}

func (e *GraphBudgetExhaustedError) Error() string {
	return fmt.Sprintf("%s: %s", ErrGraphBudgetExhausted.Error(), e.Reason)
}

func (e *GraphBudgetExhaustedError) Unwrap() error { return ErrGraphBudgetExhausted }

// ChatAgentGraphBudgetPersistence is the delegation graph's optional budget pool: one row per root
// session that every run in the graph draws on, beside its own per-run limits.
type ChatAgentGraphBudgetPersistence interface {
	// SetChatAgentGraphBudget creates the root's pool or replaces its limits, keeping what was
	// used, and in the same transaction drains the work the graph deferred while the pool was used up,
	// when it has room again: each child's retained follow-up, then queued input, then the root's wake.
	// It returns the minted runs and the delivered input for the caller to announce and start after
	// commit. ErrGraphBudgetInvalid for a non-root,
	// archived, or missing session or a zero limit. It locks the pool before the channel, the order
	// step reservations take (run -> pool -> channel), so a transaction composing it with other
	// graph mutations must call it before any of them takes the channel lock.
	SetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64, limits pm.ChatAgentGraphBudget) (ChatAgentGraphBudgetDrain, error)
	// GetChatAgentGraphBudget returns the root's pool, or nil when the graph has none.
	GetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) (*pm.ChatAgentGraphBudget, error)
	// CheckChatAgentGraphBudget returns a *GraphBudgetExhaustedError when a limit of the root's
	// pool is used up, and nil when the pool has room or the graph has none. Spawns, sends, and
	// parent resumes call it before admitting work.
	CheckChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) error
}

// ChatAgentGraphBudgetDrain is what raising a graph's pool drained: Runs to start after commit, and
// Inputs, each session's queued input delivered (or discarded), to announce before them.
type ChatAgentGraphBudgetDrain struct {
	Runs   []*pm.ChatAgentRun
	Inputs []ChatAgentSessionInputDelivery
}

// ChatAgentSessionInputDelivery is one session's drained queued input.
type ChatAgentSessionInputDelivery struct {
	Session  *pm.ChatAgentSession
	Delivery ChatAgentQueuedInputDelivery
}

// SetChatAgentGraphBudget implements ChatAgentGraphBudgetPersistence. The pool row is locked
// first: a step reservation holds it while its input admission takes the admission locks. The
// drains then follow settlement's order, channel, child, parent, and always read the pending wakes
// under those locks: a settlement that deferred a wake against the old pool either committed before
// the channel lock (and its wake is drained here) or reads the raised pool after this commits.
func (h *CommonPersistenceHandler[U]) SetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64, limits pm.ChatAgentGraphBudget) (ChatAgentGraphBudgetDrain, error) {
	var drained ChatAgentGraphBudgetDrain
	if rootSessionID == 0 || limits.MaxGenerations == 0 || limits.MaxToolCalls == 0 || limits.MaxActiveMs == 0 {
		return drained, ErrGraphBudgetInvalid
	}
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		root, err := loadChatAgentSessionTx(tx, rootSessionID)
		if err != nil {
			return err
		}
		if root == nil || !root.IsMainTimeline() || root.Status != pm.ChatAgentSessionStatusActive {
			return ErrGraphBudgetInvalid
		}
		pool := pm.ChatAgentGraphBudget{RootSessionID: rootSessionID, MaxGenerations: limits.MaxGenerations, MaxToolCalls: limits.MaxToolCalls, MaxActiveMs: limits.MaxActiveMs}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "root_session_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"max_generations", "max_tool_calls", "max_active_ms", "updated_at"}),
		}).Create(&pool).Error; err != nil {
			return err
		}
		if _, err := lockChatChannelForAgentInputTx(tx, root.ChannelID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		// Retirement archives the root under this lock: one that committed before it is seen here.
		if root, err = loadChatAgentSessionTx(tx, rootSessionID); err != nil {
			return err
		}
		if root == nil || !root.IsMainTimeline() || root.Status != pm.ChatAgentSessionStatusActive {
			return ErrGraphBudgetInvalid
		}
		if drained.Runs, err = h.drainDeferredChildWakesTx(txCtx, tx, root); err != nil {
			return err
		}
		// Then the input the graph's sessions queued while the pool was used up and, as settlement
		// does, the root's wake only when no input fed the root a run.
		var queued []pm.ChatAgentSession
		if err := tx.Where("(id = ? OR parent_agent_session_id = ?) AND EXISTS (SELECT 1 FROM "+pm.TableNameChatAgentQueuedInputs+
			" q WHERE q.agent_session_id = "+pm.TableNameChatAgentSessions+".id)", root.ID, root.ID).Order("id").Find(&queued).Error; err != nil {
			return err
		}
		rootFed := false
		for i := range queued {
			run, delivery, err := h.drainChatAgentInputTx(txCtx, tx, &queued[i], 0)
			if err != nil {
				return err
			}
			if delivery.Changed() {
				drained.Inputs = append(drained.Inputs, ChatAgentSessionInputDelivery{Session: &queued[i], Delivery: delivery})
			}
			if run != nil {
				drained.Runs = append(drained.Runs, run)
				rootFed = rootFed || queued[i].ID == root.ID
			}
		}
		if rootFed {
			return nil
		}
		run, err := h.drainPendingParentWakeTx(txCtx, tx, root.ID, nil)
		if run != nil {
			drained.Runs = append(drained.Runs, run)
		}
		return err
	})
	if err != nil {
		if errors.Is(err, ErrGraphBudgetInvalid) {
			return ChatAgentGraphBudgetDrain{}, err
		}
		return ChatAgentGraphBudgetDrain{}, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "set_chat_agent_graph_budget"),
			zap.Uint64(appLogger.KeyAgentSessionID, rootSessionID))
	}
	return drained, nil
}

// drainDeferredChildWakesTx drains the follow-ups root's children retained while the graph's pool
// was used up (drainPendingChildWakeTx), each against the child's latest run, whose ending deferred
// it. A child whose latest run is still active is skipped: that run's ending drains the wake. The
// caller holds the channel lock.
func (h *CommonPersistenceHandler[U]) drainDeferredChildWakesTx(ctx context.Context, tx *gorm.DB, root *pm.ChatAgentSession) ([]*pm.ChatAgentRun, error) {
	var children []pm.ChatAgentSession
	if err := tx.Where("parent_agent_session_id = ? AND pending_wake_message_id IS NOT NULL", root.ID).Order("id").Find(&children).Error; err != nil || len(children) == 0 {
		return nil, err
	}
	var drained []*pm.ChatAgentRun
	for i := range children {
		child := &children[i]
		var last pm.ChatAgentRun
		if err := tx.Where("agent_session_id = ?", child.ID).Order("id DESC").Take(&last).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, err
		}
		if !IsTerminalChatAgentRunStatus(last.Status) {
			continue
		}
		follow, err := h.drainPendingChildWakeTx(ctx, tx, &last, child)
		if err != nil {
			return nil, err
		}
		if follow != nil {
			drained = append(drained, follow)
		}
	}
	return drained, nil
}

// GetChatAgentGraphBudget implements ChatAgentGraphBudgetPersistence.
func (h *CommonPersistenceHandler[U]) GetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) (*pm.ChatAgentGraphBudget, error) {
	pool, err := loadChatAgentGraphBudgetTx(h.getDBFromContext(ctx), rootSessionID)
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_graph_budget"),
			zap.Uint64(appLogger.KeyAgentSessionID, rootSessionID))
	}
	return pool, nil
}

// CheckChatAgentGraphBudget implements ChatAgentGraphBudgetPersistence.
func (h *CommonPersistenceHandler[U]) CheckChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) error {
	pool, err := h.GetChatAgentGraphBudget(ctx, rootSessionID)
	if err != nil {
		return err
	}
	return graphBudgetExhaustedError(pool)
}

// graphBudgetExhaustedError is the admission refusal for pool, nil while it has room or is nil.
func graphBudgetExhaustedError(pool *pm.ChatAgentGraphBudget) error {
	if reason := pool.ExhaustedReason(); reason != proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		return &GraphBudgetExhaustedError{Reason: reason}
	}
	return nil
}

// loadChatAgentGraphBudgetTx reads the root's pool without locking it, nil when there is none.
func loadChatAgentGraphBudgetTx(tx *gorm.DB, rootSessionID uint64) (*pm.ChatAgentGraphBudget, error) {
	if rootSessionID == 0 {
		return nil, nil
	}
	var pool pm.ChatAgentGraphBudget
	if err := tx.Where("root_session_id = ?", rootSessionID).Take(&pool).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &pool, nil
}

// loadChatAgentRunGraphBudgetTx reads the pool of runID's graph (chatAgentGraphRunPoolSQL) without
// locking it, nil when there is none.
func loadChatAgentRunGraphBudgetTx(tx *gorm.DB, runID uint64) (*pm.ChatAgentGraphBudget, error) {
	var pool pm.ChatAgentGraphBudget
	if err := tx.Where("root_session_id IN ("+chatAgentGraphRunPoolSQL+")", sql.Named("run", runID)).Take(&pool).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &pool, nil
}

// chatAgentGraphRunPoolSQL resolves a run's pool through its session's graph root.
var chatAgentGraphRunPoolSQL = fmt.Sprintf(`SELECT b.root_session_id FROM %s r
	JOIN %s s ON s.id = r.agent_session_id
	JOIN %s b ON b.root_session_id = COALESCE(s.parent_agent_session_id, s.id)
	WHERE r.id = @run`, pm.TableNameChatAgentRuns, pm.TableNameChatAgentSessions, pm.TableNameChatAgentGraphBudgets)

// chargeChatAgentGraphBudgetSQL charges a run's work to its graph's pool in one statement: the
// conditional UPDATE re-evaluates its limits on the latest row version after waiting for the row
// lock, so concurrent charges for the last unit cannot both pass. A generation is new work, so it
// also needs a tool call left, like every other admission (ExhaustedReason); a step's own tool
// calls need only fit. It reports whether the graph has a pool and whether the charge was made.
var chargeChatAgentGraphBudgetSQL = fmt.Sprintf(`WITH pool AS (%s), charged AS (
	UPDATE %s b SET used_generations = b.used_generations + @generations, used_tool_calls = b.used_tool_calls + @tool_calls, updated_at = @now
	FROM pool WHERE b.root_session_id = pool.root_session_id
		AND b.used_generations + @generations <= b.max_generations
		AND b.used_tool_calls + @tool_calls <= b.max_tool_calls
		AND b.used_active_ms < b.max_active_ms
		AND (@generations = 0 OR b.used_tool_calls < b.max_tool_calls)
	RETURNING b.root_session_id)
SELECT (SELECT root_session_id FROM pool) AS root_session_id, EXISTS (SELECT 1 FROM charged) AS charged`,
	chatAgentGraphRunPoolSQL, pm.TableNameChatAgentGraphBudgets)

// chargeChatAgentGraphBudgetTx charges generations and tool calls to the pool of runID's graph
// after the run's own charge, in the same transaction. It returns a *GraphBudgetExhaustedError
// when the pool refuses, which the caller must roll back with so neither charge is kept, and nil
// when the charge was made or the graph has no pool.
func chargeChatAgentGraphBudgetTx(tx *gorm.DB, runID, generations, toolCalls uint64) error {
	var result struct {
		RootSessionID *uint64
		Charged       bool
	}
	if err := tx.Raw(chargeChatAgentGraphBudgetSQL,
		sql.Named("run", runID), sql.Named("generations", generations), sql.Named("tool_calls", toolCalls), sql.Named("now", time.Now()),
	).Scan(&result).Error; err != nil {
		return err
	}
	if result.RootSessionID == nil || result.Charged {
		return nil
	}
	pool, err := loadChatAgentGraphBudgetTx(tx, *result.RootSessionID)
	if err != nil {
		return err
	}
	reason := pool.ChargeRefusal(generations, toolCalls)
	if reason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		// The pool was raised between the refused charge and this read; the step still lost.
		reason = pool.ExhaustedReason()
	}
	if reason == proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED {
		reason = proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_GENERATIONS
		if generations == 0 {
			reason = proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_TOOL_CALLS
		}
	}
	return &GraphBudgetExhaustedError{Reason: reason}
}

// recordGraphBudgetRefusal counts a step charge err reports the graph budget pool refused.
func (h *CommonPersistenceHandler[U]) recordGraphBudgetRefusal(err error) {
	if exhausted, ok := errors.AsType[*GraphBudgetExhaustedError](err); ok {
		h.metricsHandler.RecordChatAgentGraphBudgetExhausted(exhausted.Reason)
	}
}

// addChatAgentGraphBudgetActiveTx adds a run's measured active time to its graph's pool. Nothing
// happens when the graph has none.
func addChatAgentGraphBudgetActiveTx(tx *gorm.DB, runID uint64, elapsedMs uint64) error {
	if elapsedMs == 0 {
		return nil
	}
	return tx.Exec(fmt.Sprintf(`UPDATE %s SET used_active_ms = used_active_ms + @ms, updated_at = @now WHERE root_session_id IN (%s)`,
		pm.TableNameChatAgentGraphBudgets, chatAgentGraphRunPoolSQL),
		sql.Named("ms", elapsedMs), sql.Named("now", time.Now()), sql.Named("run", runID)).Error
}
