package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// CHAT: AGENT STOP AND RESUME
// ============================================================================
//
// A person's Stop cancels a session's active run and suspends the session in one transaction,
// together with every delegated child working for it. While suspended, nothing starts a run
// automatically: sends queue their input as held, the terminal drains leave queued input and
// deferred delegation wakes in place, a child's wake is deferred, and a new active run cannot be
// created. Resume clears the suspension and, in the same transaction, drains what waited. Both
// take the admission locks: the run (Stop), then the channel FOR NO KEY UPDATE, then the session
// FOR UPDATE.
//
// A session's suspension also records the Stop's outstanding work: a child's suspended run is
// cancelled after commit, and until it is, the run stays active under its suspended session
// (chatAgentRunSuspendedOnSQL). Resume is refused (ErrChatAgentSessionRunActive) until that
// cancellation commits, so no Resume can erase it.

// ChatAgentRunStop is what StopChatAgentRun committed, for the events its caller publishes after
// commit.
type ChatAgentRunStop struct {
	// ChatAgentRunCancellation is the run's cancellation: the run, now CANCELLED with
	// CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER, and the streams and tool calls it ended.
	ChatAgentRunCancellation
	// Session is the run's session, now suspended: by the person who stopped it, or, for a
	// delegated child its parent's Stop had already suspended on the run, by that Stop.
	Session *common_persistence_models.ChatAgentSession
	// Children are the session's delegated children that were working when it stopped and were not
	// already suspended, in run id order, each suspended on its active run: the child work the
	// caller cancels after commit.
	Children []ChatAgentRunStopChild
}

// ChatAgentRunStopChild is a delegated child a Stop suspended on its active run.
type ChatAgentRunStopChild struct {
	RunID           uint64
	SessionID       uint64
	ChannelID       uint64
	ParentMessageID uint64
}

// StopChatAgentRun stops an agent session's active run for a person and suspends the session, in
// one transaction: it locks the run, then the channel FOR NO KEY UPDATE and the session FOR UPDATE,
// records the suspension (when, stoppedByUserID, and the run), and cancels the run with
// CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER the way CancelChatAgentRun does: pending approvals
// expire, outstanding tool work fails, and dangling streams fail. The suspension is written first,
// so the ending's settlement leaves the session's queued input and deferred delegation wakes for
// Resume. A stopped delegated child still posts its notice and wakes its parent.
//
// Each delegated child with an active run is suspended too, on that run, which the caller cancels
// after commit (Children). A child that ends on its own first therefore starts no follow-up,
// and its thread holds input until a person resumes the child; resuming the parent leaves its
// children stopped.
//
// A Stop never replaces a suspension: stopping the run a parent's Stop suspended a child on
// cancels it at once but leaves the child suspended by the parent's Stop, and a child already
// suspended is not suspended again. A session's suspended run therefore identifies its
// suspension until Resume clears it.
//
// It returns nil with no error when the run is no longer an active session run, so a client
// holding a stale run id cannot stop a newer run.
func (h *CommonPersistenceHandler[U]) StopChatAgentRun(ctx context.Context, runID, stoppedByUserID uint64) (*ChatAgentRunStop, error) {
	if stoppedByUserID == 0 {
		return nil, appLogger.InvariantError("chat_agent_run_stop_names_person",
			errors.Join(ErrDatabaseError, errors.New("a stop needs the person who stopped the run")),
			zap.String(logkeys.Stage, "stop_chat_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.String(logkeys.Expected, "stopped_by_user_id > 0"),
			zap.Uint64(logkeys.Actual, stoppedByUserID))
	}
	if runID == 0 {
		return nil, nil
	}
	var stop *ChatAgentRunStop
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var run common_persistence_models.ChatAgentRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "channel_id", "agent_session_id").
			Where("id = ? AND agent_session_id IS NOT NULL AND status IN ?", runID, ActiveChatAgentRunStatuses()).
			Take(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		session, _, err := lockChatAgentSessionForAdmissionTx(tx, run.ChannelID, *run.AgentSessionID)
		if err != nil || session == nil {
			return err
		}
		now := time.Now()
		// A suspended session's only active run is the one its parent's Stop suspended it on, still
		// to be cancelled after that Stop committed. Stopping it completes that cancellation and
		// keeps the suspension as recorded, so a suspended run names one suspension, which Resume
		// pins its authorization to.
		if !session.IsSuspended() {
			session.SuspendedAt, session.SuspendedByUserID, session.SuspendedRunID, session.UpdatedAt = &now, &stoppedByUserID, &run.ID, now
			if err := tx.Model(session).Select(chatAgentSessionSuspensionColumns).Updates(session).Error; err != nil {
				return err
			}
		}
		cancellation, err := h.cancelChatAgentRunTx(txCtx, tx, run.ID, ActiveChatAgentRunStatuses(),
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER, "")
		if err != nil {
			return err
		}
		if cancellation == nil {
			// The run lock is held, so an active run cannot have left the active set.
			return appLogger.InvariantError("chat_agent_run_stop_cancels_locked_run",
				errors.Join(ErrDatabaseError, errors.New("locked active run was not cancelled")),
				zap.String(logkeys.Stage, "stop_chat_agent_run"),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.String(logkeys.Expected, "cancelled"),
				zap.String(logkeys.Actual, "not cancellable"))
		}
		stop = &ChatAgentRunStop{ChatAgentRunCancellation: *cancellation, Session: session}
		// The session lock orders this after every spawn under the session and before any later
		// one, which sees the suspension and is refused. Locking the children after their parent
		// is safe under the channel lock, which every path that locks a child and then its parent
		// holds first. A child already suspended keeps its suspension and the Stop that left it, as
		// above. The SET list is chatAgentSessionSuspensionColumns.
		return tx.Raw(`WITH stopped AS (UPDATE `+common_persistence_models.TableNameChatAgentSessions+` AS c
				SET suspended_at = @now, suspended_by_user_id = @user, suspended_run_id = r.id, updated_at = @now
				FROM `+common_persistence_models.TableNameChatAgentRuns+` r
				WHERE c.parent_agent_session_id = @parent AND c.suspended_at IS NULL
					AND r.agent_session_id = c.id AND r.status IN @active
				RETURNING r.id AS run_id, c.id AS session_id, c.channel_id, c.parent_message_id)
			SELECT * FROM stopped ORDER BY run_id`,
			sql.Named("now", now), sql.Named("user", stoppedByUserID), sql.Named("parent", session.ID),
			sql.Named("active", ActiveChatAgentRunStatuses())).Scan(&stop.Children).Error
	})
	if err != nil {
		if appLogger.IsInvariantError(err) {
			return nil, err
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "stop_chat_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return stop, nil
}

// ChatAgentSessionResume is what ResumeChatAgentSession committed.
type ChatAgentSessionResume struct {
	// Session is the session as it now reads.
	Session *common_persistence_models.ChatAgentSession
	// Resumed reports that Resume lifted the expected Stop suspension. For retarget drains it
	// reports that retained input was delivered or a pending wake started a run.
	Resumed bool
	// Run is the run Resume started, which the caller drives: the held input delivered into one run,
	// or, when none was held, the session's deferred delegation wake. Nil when nothing waited.
	Run *common_persistence_models.ChatAgentRun
	// Delivery names the messages the held input became, for the caller to announce after commit.
	Delivery ChatAgentQueuedInputDelivery
}

// ResumeChatAgentSession lifts the suspension that stopping suspendedRunID left on a session, in
// one transaction under the admission locks (the channel FOR NO KEY UPDATE, then the session FOR
// UPDATE): it clears the suspension, delivers the input held while suspended into one new run,
// and, when no input was held, drains the session's deferred delegation wake. The stopped run and
// the delegated children the Stop cancelled are not restarted. A Stop never replaces a
// suspension, so the run names one: a session that is not suspended by that Stop (already
// resumed, or stopped again since) is returned unchanged with Resumed false.
//
// Returns ErrChatAgentSessionNotFound when the session does not exist, and
// ErrChatAgentSessionRunActive while the stopped run is still active: a delegated child stopped
// with its parent resumes once its run's cancellation has committed.
func (h *CommonPersistenceHandler[U]) ResumeChatAgentSession(ctx context.Context, sessionID, suspendedRunID uint64) (*ChatAgentSessionResume, error) {
	var resume *ChatAgentSessionResume
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		session, _, err := lockChatAgentSessionByIDTx(tx, sessionID)
		if err != nil {
			return err
		}
		resume = &ChatAgentSessionResume{Session: session}
		if !session.IsSuspended() || common_util.Deref(session.SuspendedRunID) != suspendedRunID {
			return nil
		}
		// The run's cancellation locks the run, then this channel, so a read under the channel
		// lock that finds it terminal finds it settled.
		var stopping bool
		if err := tx.Raw("SELECT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatAgentRuns+" WHERE id = ? AND status IN ?)",
			suspendedRunID, ActiveChatAgentRunStatuses()).Scan(&stopping).Error; err != nil {
			return err
		}
		if stopping {
			return ErrChatAgentSessionRunActive
		}
		session.SuspendedAt, session.SuspendedByUserID, session.SuspendedRunID, session.UpdatedAt = nil, nil, nil, time.Now()
		if err := tx.Model(session).Select(chatAgentSessionSuspensionColumns).Updates(session).Error; err != nil {
			return err
		}
		resume.Resumed = true
		if resume.Run, resume.Delivery, err = h.drainChatAgentInputTx(txCtx, tx, session, 0); err != nil {
			return err
		}
		if resume.Run == nil && session.IsMainTimeline() {
			resume.Run, err = h.drainPendingParentWakeTx(ctx, tx, session.ID, nil)
		}
		return err
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentSessionNotFound) || errors.Is(err, ErrChatAgentSessionRunActive) {
			return nil, err
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "resume_chat_agent_session"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return resume, nil
}

// HasChatAgentRunAdmittedInputFrom reports whether input that userID queued for the run's session
// was delivered into runID: a message with that AdmittedRunID and sender, deleted or not. The
// partial index idx_chat_messages_admitted_run serves it.
func (h *CommonPersistenceHandler[U]) HasChatAgentRunAdmittedInputFrom(ctx context.Context, runID, userID uint64) (bool, error) {
	if runID == 0 || userID == 0 {
		return false, nil
	}
	var admitted bool
	err := h.getDBFromContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatMessages+
		" WHERE admitted_run_id = ? AND sender_id = ?)", runID, userID).Scan(&admitted).Error
	if err != nil {
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "has_chat_agent_run_admitted_input_from"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return admitted, nil
}

// chatAgentSessionSuspensionColumns are the columns a Stop sets and a Resume clears.
var chatAgentSessionSuspensionColumns = []string{"suspended_at", "suspended_by_user_id", "suspended_run_id", "updated_at"}

// chatAgentRunSuspendedOnSQL matches a chat_agent_runs row that is its session's suspended
// run. A session's own Stop cancels that run in the same transaction, so an active match is a
// delegated child's run that its parent's Stop left to cancel after commit.
const chatAgentRunSuspendedOnSQL = "EXISTS (SELECT 1 FROM " + common_persistence_models.TableNameChatAgentSessions +
	" stop_s WHERE stop_s.id = " + common_persistence_models.TableNameChatAgentRuns + ".agent_session_id AND stop_s.suspended_run_id = " +
	common_persistence_models.TableNameChatAgentRuns + ".id)"

// ListSuspendedChatAgentRuns lists, in id order after afterRunID, at most limit active runs their
// suspended session is stopped on (chatAgentRunSuspendedOnSQL): delegated children's runs a
// parent's Stop left to cancel after commit, which a crash, a stalled worker or a failed
// cancellation may have left undone. The recovery sweep reconciles with it; no drive claims them.
func (h *CommonPersistenceHandler[U]) ListSuspendedChatAgentRuns(ctx context.Context, afterRunID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	var runIDs []uint64
	if err := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatAgentRun{}).
		Select("id").
		Where("status IN ? AND id > ?", ActiveChatAgentRunStatuses(), afterRunID).
		Where(chatAgentRunSuspendedOnSQL).
		Order("id").Limit(limit).Scan(&runIDs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_suspended_chat_agent_runs"),
			zap.Uint64(appLogger.KeyAfterID, afterRunID), zap.Int(logkeys.Limit, limit))
	}
	return runIDs, nil
}
