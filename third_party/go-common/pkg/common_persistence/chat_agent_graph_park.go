package common_persistence

import (
	"context"
	"errors"
	"slices"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrChatAgentGraphRevisionAdvanced reports that a resume event committed after the application
// read the revision it tried to park a graph at, so the graph may be drivable again.
var ErrChatAgentGraphRevisionAdvanced = errors.New("chat agent graph resume revision advanced past the park revision")

// ChatAgentGraphEventNotifier is called after an event's transaction commits when the event advanced
// the resume revision of a parked, unarchived graph root that is still parked and unarchived when
// the hook runs. The service installs it to call the root agent type's OnGraphEvent. It is the
// best-effort fast path; the durable signal is the revision, which ListUnparkableChatAgentGraphs
// reads.
type ChatAgentGraphEventNotifier func(ctx context.Context, rootAgentType string, rootSessionID uint64)

// ChatAgentGraphParkPersistence keeps the park and resume state of delegation graphs on the root
// session row. Events advance the root's resume_revision in the transaction that makes their state
// change; an application parks a graph at the revision it read and later lists the parked roots that
// have advanced since.
type ChatAgentGraphParkPersistence interface {
	// SetChatAgentGraphEventNotifier installs the post-commit fast path; nil uninstalls it.
	SetChatAgentGraphEventNotifier(ChatAgentGraphEventNotifier)
	// AdvanceChatAgentGraphResumeRevision advances the resume revision of the root of sessionID's
	// graph (the session itself when it is not a delegated child) for an event of kind. Call it in
	// the transaction of the event that may make the graph drivable again; when the root is parked
	// and unarchived, the installed notifier runs after that transaction commits unless the root was
	// unparked or retired in the meantime.
	AdvanceChatAgentGraphResumeRevision(ctx context.Context, sessionID uint64, kind proto_models.GraphEventKind) error
	// ParkChatAgentGraph locks the live root and records it as parked at revision, or returns
	// ErrChatAgentGraphRevisionAdvanced when the root's resume revision is already past revision.
	// A child, an archived session, or a revision the root has not reached is
	// ErrChatAgentSessionScopeInvalid.
	ParkChatAgentGraph(ctx context.Context, rootSessionID, revision uint64) error
	// UnparkChatAgentGraph clears the root's parked revision.
	UnparkChatAgentGraph(ctx context.Context, rootSessionID uint64) error
	// ListUnparkableChatAgentGraphs pages, by ascending id after afterRootSessionID, the unarchived
	// parked roots whose resume revision advanced past the parked one.
	ListUnparkableChatAgentGraphs(ctx context.Context, afterRootSessionID uint64, limit int) ([]uint64, error)
}

func (h *CommonPersistenceHandler[U]) SetChatAgentGraphEventNotifier(notifier ChatAgentGraphEventNotifier) {
	if notifier == nil {
		h.graphEventNotifier.Store(nil)
	} else {
		h.graphEventNotifier.Store(&notifier)
	}
}

func (h *CommonPersistenceHandler[U]) AdvanceChatAgentGraphResumeRevision(ctx context.Context, sessionID uint64, kind proto_models.GraphEventKind) error {
	if sessionID == 0 {
		return ErrChatAgentSessionNotFound
	}
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		return h.advanceChatAgentGraphResumeRevisionTx(txCtx, tx, []uint64{sessionID}, kind)
	})
	return chatAgentGraphParkError(err, "advance_chat_agent_graph_resume_revision")
}

// advanceChatAgentGraphResumeRevisionTx advances, once each, the roots of the given sessions' graphs
// for an event of kind and registers the notifier on ctx's transaction for every root that is parked
// and unarchived. Callers make it the event's last write, so the root lock follows every run,
// channel, and member session lock the event took (run -> channel -> child -> parent). Interactive
// roots advance too: nothing parks them, so their revision is never read, and only owner-driven
// roots count as graph events in chat_agent_graph_events_total.
func (h *CommonPersistenceHandler[U]) advanceChatAgentGraphResumeRevisionTx(ctx context.Context, tx *gorm.DB, sessionIDs []uint64, kind proto_models.GraphEventKind) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	sessions := models.TableNameChatAgentSessions
	var roots []struct {
		ID        uint64
		AgentType string
		Parked    bool
	}
	if err := tx.Raw(`UPDATE `+sessions+` AS root SET resume_revision = root.resume_revision + 1
		FROM `+sessions+` AS member
		WHERE member.id IN ? AND root.id = COALESCE(member.parent_agent_session_id, member.id)
		RETURNING root.id, root.agent_type, (root.parked_revision IS NOT NULL AND root.status = ?) AS parked`,
		sessionIDs, models.ChatAgentSessionStatusActive).Scan(&roots).Error; err != nil {
		return err
	}
	ownerDriven, workflowTypes, notifier := 0, h.chatAgentWorkflowTypes(), h.graphEventNotifier.Load()
	for _, root := range roots {
		if slices.Contains(workflowTypes, root.AgentType) {
			ownerDriven++
		}
		if notifier == nil || !root.Parked {
			continue
		}
		AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
			// The root may have been unparked, reparked past the advance or retired since, later
			// in the same transaction or after its commit; only a root still parked behind an
			// unseen advance and live is woken, as ListUnparkableChatAgentGraphs selects.
			var parked bool
			if err := h.BaseDB(hookCtx).Raw(`SELECT EXISTS (SELECT 1 FROM `+sessions+`
				WHERE id = ? AND parked_revision IS NOT NULL AND resume_revision > parked_revision AND status = ?)`,
				root.ID, models.ChatAgentSessionStatusActive).Scan(&parked).Error; err != nil {
				return err
			}
			if parked {
				(*notifier)(hookCtx, root.AgentType, root.ID)
			}
			return nil
		})
	}
	h.recordAfterCommit(ctx, func(metrics *common_metrics.CommonMetricsHandler) {
		metrics.RecordChatAgentGraphEvents(kind, ownerDriven)
	})
	return nil
}

func (h *CommonPersistenceHandler[U]) ParkChatAgentGraph(ctx context.Context, rootSessionID, revision uint64) error {
	if rootSessionID == 0 {
		return ErrChatAgentSessionNotFound
	}
	err := h.transaction(ctx, func(_ context.Context, tx *gorm.DB) error {
		var root models.ChatAgentSession
		if err := takeChatAgentSessionTx(&root, tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "parent_agent_session_id", "status", "resume_revision").
			Where("id = ?", rootSessionID)); err != nil {
			return err
		}
		if root.ParentAgentSessionID != nil || root.Status != models.ChatAgentSessionStatusActive || revision > root.ResumeRevision {
			return ErrChatAgentSessionScopeInvalid
		}
		if root.ResumeRevision > revision {
			return ErrChatAgentGraphRevisionAdvanced
		}
		return tx.Model(&root).UpdateColumn("parked_revision", revision).Error
	})
	return chatAgentGraphParkError(err, "park_chat_agent_graph")
}

func (h *CommonPersistenceHandler[U]) UnparkChatAgentGraph(ctx context.Context, rootSessionID uint64) error {
	result := h.getDBFromContext(ctx).Model(&models.ChatAgentSession{}).
		Where("id = ? AND parent_agent_session_id IS NULL", rootSessionID).
		UpdateColumn("parked_revision", gorm.Expr("NULL"))
	if result.Error != nil {
		return chatAgentGraphParkError(result.Error, "unpark_chat_agent_graph")
	}
	if result.RowsAffected == 0 {
		return ErrChatAgentSessionNotFound
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) ListUnparkableChatAgentGraphs(ctx context.Context, afterRootSessionID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, ErrChatAgentSessionScopeInvalid
	}
	roots := []uint64{}
	// idx_chat_agent_sessions_parked_roots holds only parked roots.
	err := h.getDBFromContext(ctx).Model(&models.ChatAgentSession{}).
		Where("parked_revision IS NOT NULL AND resume_revision > parked_revision").
		Where("parent_agent_session_id IS NULL AND status = ? AND id > ?", models.ChatAgentSessionStatusActive, afterRootSessionID).
		Order("id").Limit(limit).Pluck("id", &roots).Error
	if err != nil {
		return nil, chatAgentGraphParkError(err, "list_unparkable_chat_agent_graphs")
	}
	return roots, nil
}

func chatAgentGraphParkError(err error, stage string) error {
	return knownOrChatDatabaseError(err, stage, ErrChatAgentSessionNotFound, ErrChatAgentSessionScopeInvalid, ErrChatAgentGraphRevisionAdvanced)
}
