package common_persistence

import (
	"context"
	"strconv"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// A parent's archive is itself durable cascade intent: even children the archiving process never
// reached must stop. This predicate also refuses drive claims during that recovery window.
var chatAgentRunRetiringChildSQL = retiringChildRunSQL(models.TableNameChatAgentRuns, false)

// chatAgentRunRetiringOwnerDrivenSQL is the owner-driven claim's form: it also refuses the runs of
// an archived root, which RetireGraph's cascade cancels after the application's commit.
var chatAgentRunRetiringOwnerDrivenSQL = retiringChildRunSQL(models.TableNameChatAgentRuns, true)

// chatAgentRunDriveFenceSQL is the retirement fence a token-checked drive of runID renews and
// writes under. An owner drive (ctx carries the workflow marker for runID) is fenced like its claim,
// so a drive that outlived RetireGraph cannot write into the retired graph; any other drive keeps
// the child-only form, which lets an interactive root's in-flight run finish after its archive.
func chatAgentRunDriveFenceSQL(ctx context.Context, runID uint64) string {
	if workflowRunID, ok := common_context.GetWorkflowAgentRun(ctx); ok && workflowRunID == runID {
		return chatAgentRunRetiringOwnerDrivenSQL
	}
	return chatAgentRunRetiringChildSQL
}

// retiringChildRunSQL matches a run whose retirement is pending: a delegated child's run once the
// child or its parent is archived. archivedRoots also matches a run of an archived root. Only
// owner-driven runs pass it: an interactive root's archive lets its in-flight run finish.
func retiringChildRunSQL(runReference string, archivedRoots bool) string {
	archived := strconv.FormatInt(int64(models.ChatAgentSessionStatusArchived), 10)
	own := "retire_child.parent_agent_session_id IS NOT NULL AND (retire_child.status = " + archived + " OR "
	if archivedRoots {
		own = "(retire_child.status = " + archived + " OR "
	}
	return "EXISTS (SELECT 1 FROM " + models.TableNameChatAgentSessions +
		" retire_child WHERE retire_child.id = " + runReference + ".agent_session_id" +
		" AND " + own + "EXISTS (SELECT 1 FROM " +
		models.TableNameChatAgentSessions + " retire_parent WHERE retire_parent.id = retire_child.parent_agent_session_id AND retire_parent.status = " + archived + ")))"
}

// RetiringDelegatedChatAgentSession is a delegated child, or an archived owner-driven root, whose
// retirement still needs work. ParentArchived tells the parent's cascade apart from an archive of the
// child itself, which is all recovery needs to choose the cancellation the interrupted retirement
// would have recorded. A root (no parent) needs its own active runs cancelled.
type RetiringDelegatedChatAgentSession struct {
	models.ChatAgentSession
	ParentArchived bool `gorm:"column:parent_archived;->"`
}

// ListRetiringDelegatedChatAgentSessions keyset-pages sessions whose retirement still needs work. A
// live child of an archived parent needs its archive even if it has no active run; an already
// archived child needs work only while one of its runs remains active. An archived root of a
// workflow-owned type (SetChatAgentWorkflowTypesProvider) needs work while one of its own runs
// remains active: RetireGraph cancels them after the application's commit, and a crash can cut that
// short.
func (h *CommonPersistenceHandler[U]) ListRetiringDelegatedChatAgentSessions(ctx context.Context, afterSessionID uint64, limit int) ([]*RetiringDelegatedChatAgentSession, error) {
	if limit <= 0 {
		return nil, nil
	}
	sessions := models.TableNameChatAgentSessions
	archived := models.ChatAgentSessionStatusArchived
	parentArchived := "EXISTS (SELECT 1 FROM " + sessions + " p WHERE p.id = " + sessions + ".parent_agent_session_id AND p.status = ?)"
	activeRun := "EXISTS (SELECT 1 FROM " + models.TableNameChatAgentRuns + " r WHERE r.agent_session_id = " + sessions + ".id AND r.status IN ?)"
	children := sessions + ".parent_agent_session_id IS NOT NULL AND ((" + sessions + ".status = ? AND " + activeRun + ") OR (" + sessions + ".status <> ? AND " + parentArchived + "))"
	roots := sessions + ".parent_agent_session_id IS NULL AND " + sessions + ".status = ? AND " + sessions + ".agent_type IN ? AND " + activeRun
	var retiring []*RetiringDelegatedChatAgentSession
	err := h.getDBFromContext(ctx).Table(sessions).
		Select(sessions+".*, "+parentArchived+" AS parent_archived", archived).
		Where(sessions+".id > ?", afterSessionID).
		Where("("+children+") OR ("+roots+")", archived, ActiveChatAgentRunStatuses(), archived, archived,
			archived, h.chatAgentWorkflowTypes(), ActiveChatAgentRunStatuses()).
		Order(sessions + ".id").Limit(limit).Find(&retiring).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "list_retiring_delegated_chat_agent_sessions"), zap.Uint64(appLogger.KeyAfterID, afterSessionID), zap.Int(logkeys.Limit, limit))
	}
	return retiring, nil
}

// ListDelegatedChatAgentSessionsNeedingRetirementByParent pages only unfinished
// retirement work. Archived children remain eligible while cancellation needs
// repair; idle unarchived children must also be retired to prevent future runs.
func (h *CommonPersistenceHandler[U]) ListDelegatedChatAgentSessionsNeedingRetirementByParent(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*models.ChatAgentSession, error) {
	if parentSessionID == 0 || limit <= 0 {
		return nil, nil
	}
	sessions := models.TableNameChatAgentSessions
	var children []*models.ChatAgentSession
	err := h.getDBFromContext(ctx).
		Where("parent_agent_session_id = ? AND id > ?", parentSessionID, afterSessionID).
		Where("status <> ? OR EXISTS (SELECT 1 FROM "+models.TableNameChatAgentRuns+" r WHERE r.agent_session_id = "+sessions+".id AND r.status IN ?)", models.ChatAgentSessionStatusArchived, ActiveChatAgentRunStatuses()).
		Order("id ASC").Limit(limit).Find(&children).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "list_delegated_children_needing_retirement"), zap.Uint64(appLogger.KeyAgentSessionID, parentSessionID))
	}
	return children, nil
}
