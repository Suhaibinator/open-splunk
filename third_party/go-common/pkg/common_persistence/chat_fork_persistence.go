package common_persistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrChatForkTooLarge indicates a fork selected more source messages than its
	// MaxCopiedMessages; nothing was written.
	ErrChatForkTooLarge = errors.New("chat channel fork is too large")
	// ErrChatForkPointInvalid indicates the fork point is not a live, forkable message of the
	// source channel.
	ErrChatForkPointInvalid = errors.New("chat channel fork point is invalid")
	// ErrChatForkRequestKeyConflict indicates a concurrent fork with the same request key
	// committed first; nothing was written and a retry replays that fork.
	ErrChatForkRequestKeyConflict = errors.New("chat channel fork request key conflict")
)

// CopyChatChannelForkInput names one fork copy. The destination channel was created in the same
// transaction, with its participants, and holds no messages yet.
type CopyChatChannelForkInput struct {
	SourceChannelID      uint64
	DestinationChannelID uint64
	// ForkPointMessageID is M, the requested fork point; in-flight work at M is dropped. On the main
	// timeline it cuts the main timeline at M; inside thread R it cuts the main timeline at R and
	// R's replies at M.
	ForkPointMessageID uint64
	// ForkerUserID created the fork. It becomes the UserID and CreatedByUserID of every cloned
	// agent session, so the forker owns the agents of the fork.
	ForkerUserID uint64
	// KeepContext reports whether the destination kept the source's context. Without it the cloned
	// agent sessions drop their ContextID and ContextEntityID too.
	KeepContext bool
	// RequestKey is the forker's idempotency key, stored as the destination's ForkRequestKey with
	// its provenance before anything is copied. Nil when the request carried none.
	RequestKey *string
	// MaxCopiedMessages is the sampled chat_fork_max_copied_messages. A selection larger than it
	// returns ErrChatForkTooLarge before any row is written.
	MaxCopiedMessages int
}

// CopyChatChannelForkResult describes a finished fork copy.
type CopyChatChannelForkResult struct {
	CopiedMessageCount int
	// LastCopiedMessageID is the greatest new message ID: the read pointer every member of the
	// destination now holds.
	LastCopiedMessageID uint64
	// ClonedAgentSessionIDs are the destination's new agent sessions, ascending.
	ClonedAgentSessionIDs []uint64
	// BotUserIDs are the distinct bot users of the cloned sessions, ascending. The copier adds
	// any that were not already participants as MEMBER participants.
	BotUserIDs []uint64
	// OrganizationIDs are the distinct organizations of the cloned sessions, ascending.
	OrganizationIDs []int64
}

// CopyChatChannelFork copies a source channel's history up to a fork point into an empty
// destination channel inside the caller's transaction. It goes through no send path, so it
// triggers no notifications, mentions, or agent admission. It:
//
//  1. Takes the source channel's agent-input lock (lockLiveChatChannelForAgentInputTx, which send,
//     edit and delete take), so the fork point cannot be deleted between its validation and the
//     copy, and stores the destination's provenance (ForkedFromChannelID, ForkPointMessageID) and
//     ForkRequestKey.
//  2. Selects source rows in ascending ID order: the main timeline up to the cut, every reply
//     of each copied root (thread R's replies only up to M), excluding soft-deleted and
//     PROVIDER_STATE rows; hidden and excluded-from-AI rows are kept. The selected rows are locked
//     FOR SHARE so later statements read the selected versions. A selection larger than
//     MaxCopiedMessages returns ErrChatForkTooLarge.
//  3. Drops in-flight agent work: a run step is copied only when complete (no streaming row, every
//     TOOL_CALL settled and answered in the selection, no row of the step past the cut, sealed
//     when the run is still active), and the first incomplete step drops every later step of its
//     run. Streaming rows, unsettled TOOL_CALLs, replies of a dropped thread root and queued inputs
//     are never copied.
//  4. Inserts the copies with an old-to-new ID map, remapping thread parents, reply targets,
//     attachment IDs, agent run IDs, admitted run IDs, compaction payload IDs (each to the
//     greatest copied ID not above the original) and the IDs delegation tool calls and results
//     name (remapForkDelegationJSON), and clearing client message IDs and the pre-edit original
//     content (edit history is not copied).
//  5. Copies terminal tool-call side rows, reactions, mentions and embeds.
//  6. Clones vetted attachment rows, locked FOR SHARE, onto the copies, sharing the stored
//     objects. A copied TOOL_RESULT's internal tool media is cloned under the new run's entity key
//     and its payload points at the clone.
//  7. Creates terminal agent runs without leases or inference accounting (a run the fork cut, or
//     one still active in the source, becomes CANCELLED with INCOMPLETE_STEP), and clones every
//     agent session whose location was copied with origin FORK, owned by the forker (delegated
//     children stay idle and linked to their cloned parent), with their config snapshots, tool
//     configs, graph budget pool, agent context and context operations. A pool keeps its limits
//     and is charged the copied runs of its graph, capped at what the source pool recorded; a run
//     the fork cut counts only its kept steps (one generation each) and TOOL_CALLs, and no active
//     time.
//     SpawnedByAgentRunID is remapped through the old-to-new run ID map. A delegated child whose
//     spawn step the fork cut is not cloned, nor are the sessions under or replacing it; its
//     thread is copied as plain history.
//     Clones are idle: no pending wake, suspension, park, or create idempotency key carries over,
//     and they drop their context unless KeepContext.
//  8. Adds the cloned sessions' bot users as participants, refreshes the last-message fields
//     (a non-empty fork's LastMessageAt is its creation time), and sets every participant's read
//     pointer to the last copy.
//
// Returns ErrChatChannelNotFound when the source or destination is missing or deleted,
// ErrChatForkPointInvalid when M is not a live, forkable message of the source, and
// ErrChatForkRequestKeyConflict when the step-1 ForkRequestKey write loses a race on
// idx_chat_channels_fork_request_key (isUniqueViolationOn) to a concurrent fork with the same key.
// Every error leaves nothing written.
func (h *CommonPersistenceHandler[U]) CopyChatChannelFork(ctx context.Context, input CopyChatChannelForkInput) (*CopyChatChannelForkResult, error) {
	var result *CopyChatChannelForkResult
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		result, err = h.copyChatChannelFork(ctx, tx, input)
		return err
	})
	if err != nil {
		return nil, knownOrChatDatabaseError(err, "copy_chat_channel_fork",
			ErrChatForkTooLarge, ErrChatForkPointInvalid, ErrChatForkRequestKeyConflict, ErrChatChannelNotFound)
	}
	return result, nil
}

// forkSourceRow is one selected source message with the step-cut inputs of its side row.
type forkSourceRow struct {
	ID              uint64
	ParentMessageID uint64
	Kind            proto_models.MessageKind
	StreamState     proto_models.MessageStreamState
	AgentRunID      *uint64
	AgentStepIndex  *uint32
	AttachmentIDs   pq.Int64Array `gorm:"type:bigint[]"`
	ToolCallID      *string
	ApprovalStatus  proto_models.ToolCallApprovalStatus
	ExecutionState  proto_models.ToolCallExecutionState
}

// forkStep is the (run, step) identity transcript reconstruction groups by.
type forkStep struct {
	run  uint64
	step uint32
}

// forkIDMap maps ascending old IDs to the ascending new IDs allocated for them, so copies keep
// their sources' relative order.
type forkIDMap struct{ old, new []int64 }

func (m forkIDMap) get(id uint64) (uint64, bool) {
	i, ok := slices.BinarySearch(m.old, int64(id))
	if !ok {
		return 0, false
	}
	return uint64(m.new[i]), true
}

// ptr maps an optional reference, dropping it when its target was not copied.
func (m forkIDMap) ptr(id *uint64) *uint64 {
	if id == nil {
		return nil
	}
	if mapped, ok := m.get(*id); ok {
		return &mapped
	}
	return nil
}

// floor maps a watermark to the copy of the greatest copied ID not above it, or 0.
func (m forkIDMap) floor(id uint64) uint64 {
	i, ok := slices.BinarySearch(m.old, int64(id))
	if ok {
		return uint64(m.new[i])
	}
	if i == 0 {
		return 0
	}
	return uint64(m.new[i-1])
}

// ceil maps an exclusive upper bound to the copy of the least copied ID not below it, or past the
// last copy when none is.
func (m forkIDMap) ceil(id uint64) uint64 {
	i, _ := slices.BinarySearch(m.old, int64(id))
	if i < len(m.new) {
		return uint64(m.new[i])
	}
	if i == 0 {
		return 0
	}
	return uint64(m.new[i-1]) + 1
}

// allocateForkIDs reserves one sequence value per old ID of table, ascending.
func allocateForkIDs(tx *gorm.DB, table string, old []int64) (forkIDMap, error) {
	m := forkIDMap{old: old}
	if len(old) == 0 {
		return m, nil
	}
	err := tx.Raw("SELECT nextval(pg_get_serial_sequence(?, 'id')) FROM generate_series(1, ?) ORDER BY 1", table, len(old)).Scan(&m.new).Error
	if err == nil && len(m.new) != len(old) {
		err = fmt.Errorf("allocated %d of %d %s ids", len(m.new), len(old), table)
	}
	return m, err
}

const forkMessageIDsCTE = "ids(old_id, new_id) AS (SELECT * FROM unnest(CAST(@ids_old AS bigint[]), CAST(@ids_new AS bigint[])))"
const forkRunIDsCTE = "runs(old_id, new_id) AS (SELECT * FROM unnest(CAST(@runs_old AS bigint[]), CAST(@runs_new AS bigint[])))"

func (h *CommonPersistenceHandler[U]) copyChatChannelFork(ctx context.Context, tx *gorm.DB, in CopyChatChannelForkInput) (*CopyChatChannelForkResult, error) {
	tx = tx.WithContext(ctx)
	if err := lockLiveChatChannelForAgentInputTx(tx, in.SourceChannelID); err != nil {
		return nil, err
	}
	stamped := tx.Model(&common_persistence_models.ChatChannel{ID: in.DestinationChannelID}).
		Select("forked_from_channel_id", "fork_point_message_id", "fork_request_key").
		Updates(&common_persistence_models.ChatChannel{ForkedFromChannelID: &in.SourceChannelID, ForkPointMessageID: &in.ForkPointMessageID, ForkRequestKey: in.RequestKey})
	if isUniqueViolationOn(stamped.Error, "idx_chat_channels_fork_request_key") {
		return nil, ErrChatForkRequestKeyConflict
	}
	if stamped.Error != nil {
		return nil, stamped.Error
	}
	if stamped.RowsAffected == 0 {
		return nil, ErrChatChannelNotFound
	}

	providerState := proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE
	var point []uint64
	if err := tx.Raw(`SELECT COALESCE(m.parent_message_id, 0) FROM gocommon_chat_messages m
WHERE m.id = ? AND m.channel_id = ? AND m.deleted_at IS NULL AND m.kind <> ? AND (COALESCE(m.parent_message_id, 0) = 0 OR EXISTS (
	SELECT 1 FROM gocommon_chat_messages r WHERE r.id = m.parent_message_id AND r.channel_id = m.channel_id
	AND r.deleted_at IS NULL AND COALESCE(r.parent_message_id, 0) = 0))`,
		in.ForkPointMessageID, in.SourceChannelID, providerState).Scan(&point).Error; err != nil {
		return nil, err
	}
	if len(point) == 0 {
		return nil, ErrChatForkPointInvalid
	}
	thread, mainCut := point[0], in.ForkPointMessageID
	if thread != 0 {
		mainCut = thread
	}

	var rows []forkSourceRow
	if err := tx.Raw(`SELECT m.id, COALESCE(m.parent_message_id, 0) AS parent_message_id, m.kind, m.stream_state, m.agent_run_id, m.agent_step_index, m.attachment_ids, t.tool_call_id,
	COALESCE(t.approval_status, 0) AS approval_status, COALESCE(t.execution_state, 0) AS execution_state
FROM gocommon_chat_messages m LEFT JOIN gocommon_chat_message_tool_calls t ON t.message_id = m.id
WHERE m.channel_id = @source AND m.deleted_at IS NULL AND m.kind <> @provider_state AND (
	(COALESCE(m.parent_message_id, 0) = 0 AND m.id <= @main_cut) OR
	(m.parent_message_id IN (SELECT r.id FROM gocommon_chat_messages r WHERE r.channel_id = @source
		AND COALESCE(r.parent_message_id, 0) = 0 AND r.id <= @main_cut AND r.deleted_at IS NULL)
	AND (m.parent_message_id <> @thread OR m.id <= @point)))
ORDER BY m.id LIMIT @limit FOR SHARE OF m`,
		sql.Named("source", in.SourceChannelID), sql.Named("provider_state", providerState), sql.Named("main_cut", mainCut),
		sql.Named("thread", thread), sql.Named("point", in.ForkPointMessageID), sql.Named("limit", in.MaxCopiedMessages+1)).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) > in.MaxCopiedMessages {
		return nil, ErrChatForkTooLarge
	}

	runByID, copied, err := cutForkInFlightWork(tx, rows)
	if err != nil {
		return nil, err
	}
	oldMessages := make([]int64, len(copied))
	var oldRuns []int64
	lastKeptStep := make(map[uint64]uint64) // run -> its greatest copied step row
	for i, row := range copied {
		oldMessages[i] = int64(row.ID)
		if row.AgentRunID != nil {
			oldRuns = append(oldRuns, int64(*row.AgentRunID))
		}
		if row.AgentRunID != nil && row.AgentStepIndex != nil {
			lastKeptStep[*row.AgentRunID] = row.ID
		}
	}
	slices.Sort(oldRuns)
	oldRuns = slices.Compact(oldRuns)
	messages, err := allocateForkIDs(tx, common_persistence_models.TableNameChatMessages, oldMessages)
	if err != nil {
		return nil, err
	}
	runs, err := allocateForkIDs(tx, common_persistence_models.TableNameChatAgentRuns, oldRuns)
	if err != nil {
		return nil, err
	}
	var sessionRows []common_persistence_models.ChatAgentSession
	if err := tx.Where("channel_id = ?", in.SourceChannelID).Order("id").Find(&sessionRows).Error; err != nil {
		return nil, err
	}
	sources := make([]common_persistence_models.ChatAgentSession, 0, len(sessionRows))
	var oldSessions []int64
	// A spawn's TOOL_RESULT follows the child's thread root and a run keeps a prefix of its steps,
	// so a delegated child's spawn survived only when its spawning run kept a step row past that
	// root. A child whose spawn was cut is not
	// cloned, and neither are the sessions under or replacing an uncloned one: the cloned parent
	// never spawned them. Sessions are ascending, so parents and replaced children come first.
	uncloned := make(map[uint64]bool)
	for _, session := range sessionRows {
		_, copiedRoot := messages.get(session.ParentMessageID)
		spawnCut := session.SpawnedByAgentRunID != nil && lastKeptStep[*session.SpawnedByAgentRunID] <= session.ParentMessageID
		if (session.ParentMessageID != 0 && !copiedRoot) || spawnCut ||
			uncloned[common_util.Deref(session.ParentAgentSessionID)] || uncloned[common_util.Deref(session.ReplacesChildSessionID)] {
			uncloned[session.ID] = true
			continue
		}
		sources = append(sources, session)
		oldSessions = append(oldSessions, int64(session.ID))
	}
	sessions, err := allocateForkIDs(tx, common_persistence_models.TableNameChatAgentSessions, oldSessions)
	if err != nil {
		return nil, err
	}

	plan, err := planForkSideRows(tx, copied, messages, runs, sessions)
	if err != nil {
		return nil, err
	}
	if err := insertForkRuns(tx, in.DestinationChannelID, runByID, messages, runs, sessions); err != nil {
		return nil, err
	}
	if err := insertForkSessions(tx, in, sources, runByID, messages, runs, sessions); err != nil {
		return nil, err
	}

	if err := tx.Exec(`WITH `+forkMessageIDsCTE+`, `+forkRunIDsCTE+`,
att(message_id, ids) AS (SELECT message_id, array_agg(attachment_id ORDER BY ord)
	FROM unnest(CAST(@att_messages AS bigint[]), CAST(@att_ids AS bigint[])) WITH ORDINALITY AS a(message_id, attachment_id, ord) GROUP BY message_id)
INSERT INTO gocommon_chat_messages (id, channel_id, sender_id, content, content_type, stream_state, parent_message_id,
	reply_to_message_id, broadcast_to_channel, is_edited, edited_at, created_at, updated_at, attachment_ids,
	kind, agent_note_subtype, role, exclude_from_ai_context, hidden_from_users, agent_run_id, agent_step_index, admitted_run_id)
SELECT ids.new_id, @channel, m.sender_id, m.content, m.content_type, m.stream_state, parent.new_id,
	reply.new_id, m.broadcast_to_channel, m.is_edited, m.edited_at, m.created_at, m.updated_at, att.ids,
	m.kind, m.agent_note_subtype, m.role, m.exclude_from_ai_context, m.hidden_from_users, run.new_id, m.agent_step_index, admitted.new_id
FROM ids JOIN gocommon_chat_messages m ON m.id = ids.old_id
LEFT JOIN ids parent ON parent.old_id = m.parent_message_id
LEFT JOIN ids reply ON reply.old_id = m.reply_to_message_id
LEFT JOIN runs run ON run.old_id = m.agent_run_id
LEFT JOIN runs admitted ON admitted.old_id = m.admitted_run_id
LEFT JOIN att ON att.message_id = ids.new_id`,
		sql.Named("ids_old", pq.Int64Array(messages.old)), sql.Named("ids_new", pq.Int64Array(messages.new)),
		sql.Named("runs_old", pq.Int64Array(runs.old)), sql.Named("runs_new", pq.Int64Array(runs.new)), sql.Named("channel", in.DestinationChannelID),
		sql.Named("att_messages", plan.messages), sql.Named("att_ids", plan.ids)).Error; err != nil {
		return nil, err
	}
	// Side rows depend only on the copied messages, so one statement writes them all.
	if err := tx.Exec(`WITH `+forkMessageIDsCTE+`, `+forkRunIDsCTE+`,
payloads(message_id, payload) AS (SELECT * FROM unnest(CAST(@payload_messages AS bigint[]), CAST(@payloads AS bytea[]))),
tool_calls AS (INSERT INTO gocommon_chat_message_tool_calls (message_id, channel_id, agent_run_id, kind, tool_call_id, model_payload,
	payload_type, approval_status, triggering_user_id, resolved_by_user_id, resolved_at, resolution_reason, approval_expires_at,
	execution_state, execution_attempt_count, execution_started_at, executed_at, last_execution_error, arguments_rejected,
	created_at, updated_at)
	SELECT ids.new_id, @channel, run.new_id, t.kind, t.tool_call_id, COALESCE(p.payload, t.model_payload),
	t.payload_type, t.approval_status, t.triggering_user_id, t.resolved_by_user_id, t.resolved_at, t.resolution_reason, t.approval_expires_at,
	t.execution_state, t.execution_attempt_count, t.execution_started_at, t.executed_at, t.last_execution_error, t.arguments_rejected,
	t.created_at, t.updated_at
	FROM ids JOIN gocommon_chat_message_tool_calls t ON t.message_id = ids.old_id
	LEFT JOIN runs run ON run.old_id = t.agent_run_id LEFT JOIN payloads p ON p.message_id = ids.new_id),
reactions AS (INSERT INTO gocommon_chat_message_reactions (message_id, user_id, emoji, created_at)
	SELECT ids.new_id, r.user_id, r.emoji, r.created_at FROM ids JOIN gocommon_chat_message_reactions r ON r.message_id = ids.old_id),
mentions AS (INSERT INTO gocommon_chat_message_mentions (message_id, user_id, created_at)
	SELECT ids.new_id, x.user_id, x.created_at FROM ids JOIN gocommon_chat_message_mentions x ON x.message_id = ids.old_id),
embeds AS (INSERT INTO gocommon_chat_message_embeds (message_id, embed_type, embed_data, fallback_text, position, created_at)
	SELECT ids.new_id, e.embed_type, e.embed_data, e.fallback_text, e.position, e.created_at
	FROM ids JOIN gocommon_chat_message_embeds e ON e.message_id = ids.old_id)
INSERT INTO gocommon_attachments (id, created_at, updated_at, user_id, original_file_name, content_type, size,
	temporary_object_name, final_object_name, file_hash, declared_file_hash, object_type, internal_only, visibility, entity_id,
	upload_status, upload_time, upload_url_expires_at, processing_start_time, processing_end_time, validation_details)
SELECT c.new_id, now(), now(), a.user_id, a.original_file_name, a.content_type, a.size,
	a.temporary_object_name, a.final_object_name, a.file_hash, a.declared_file_hash, a.object_type, a.internal_only, a.visibility, c.entity_id,
	a.upload_status, a.upload_time, a.upload_url_expires_at, a.processing_start_time, a.processing_end_time, a.validation_details
FROM unnest(CAST(@clone_new AS bigint[]), CAST(@clone_old AS bigint[]), CAST(@clone_entities AS text[])) AS c(new_id, old_id, entity_id)
JOIN gocommon_attachments a ON a.id = c.old_id`,
		sql.Named("ids_old", pq.Int64Array(messages.old)), sql.Named("ids_new", pq.Int64Array(messages.new)),
		sql.Named("runs_old", pq.Int64Array(runs.old)), sql.Named("runs_new", pq.Int64Array(runs.new)), sql.Named("channel", in.DestinationChannelID),
		sql.Named("payload_messages", plan.payloadMessages), sql.Named("payloads", plan.payloads),
		sql.Named("clone_new", pq.Int64Array(plan.clones)), sql.Named("clone_old", pq.Int64Array(plan.sources)),
		sql.Named("clone_entities", plan.entities)).Error; err != nil {
		return nil, err
	}

	result := &CopyChatChannelForkResult{CopiedMessageCount: len(copied)}
	var botUsers []common_persistence_models.ChatChannelParticipant
	for i, session := range sources {
		result.ClonedAgentSessionIDs = append(result.ClonedAgentSessionIDs, uint64(sessions.new[i]))
		result.OrganizationIDs = append(result.OrganizationIDs, session.OrganizationID)
		if !slices.Contains(result.BotUserIDs, session.BotUserID) {
			result.BotUserIDs = append(result.BotUserIDs, session.BotUserID)
			botUsers = append(botUsers, common_persistence_models.ChatChannelParticipant{
				ChannelID: in.DestinationChannelID, UserID: session.BotUserID, Role: proto_models.ChannelRole_CHANNEL_ROLE_MEMBER})
		}
	}
	slices.Sort(result.BotUserIDs)
	slices.Sort(result.OrganizationIDs)
	result.OrganizationIDs = slices.Compact(result.OrganizationIDs)
	if len(botUsers) > 0 {
		if err := h.createStampedChatParticipants(tx, botUsers, &clause.OnConflict{DoNothing: true}); err != nil {
			return nil, err
		}
	}
	if len(copied) > 0 {
		result.LastCopiedMessageID = uint64(messages.new[len(messages.new)-1])
		if err := tx.Model(&common_persistence_models.ChatChannelParticipant{}).
			Where("channel_id = ? AND left_at IS NULL", in.DestinationChannelID).
			Update("last_read_message_id", result.LastCopiedMessageID).Error; err != nil {
			return nil, err
		}
	}
	if err := h.refreshChatChannelLastMessage(ctx, tx, in.DestinationChannelID); err != nil {
		return nil, err
	}
	// The preview describes the last copy, but the fork sorts by when it was made: the copies keep
	// their source timestamps, which would bury a fork of an old conversation in the channel list.
	if err := tx.Model(&common_persistence_models.ChatChannel{}).Where("id = ? AND last_message_at IS NOT NULL", in.DestinationChannelID).
		Update("last_message_at", gorm.Expr("created_at")).Error; err != nil {
		return nil, err
	}
	return result, nil
}

// cutForkInFlightWork drops the selection's in-flight agent work and returns the selection's
// runs by ID, a truncated run's usage counters reduced to its kept work, with the rows to copy. A
// step is incomplete when a row of it is in flight, sits past the cut, or is an unanswered
// TOOL_CALL, or when its run is still active and has not sealed it; the first incomplete step of
// a run drops it and every later step. In-flight rows without a step and the replies of a dropped
// thread root are dropped too.
func cutForkInFlightWork(tx *gorm.DB, rows []forkSourceRow) (map[uint64]*common_persistence_models.ChatAgentRun, []forkSourceRow, error) {
	var runIDs pq.Int64Array
	selected := make(map[uint64]struct{}, len(rows))
	type call struct {
		step forkStep
		id   string
	}
	answered := make(map[call]struct{})
	for _, row := range rows {
		selected[row.ID] = struct{}{}
		if row.AgentRunID == nil {
			continue
		}
		runIDs = append(runIDs, int64(*row.AgentRunID))
		if row.AgentStepIndex != nil && row.ToolCallID != nil && row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
			answered[call{forkStep{*row.AgentRunID, *row.AgentStepIndex}, *row.ToolCallID}] = struct{}{}
		}
	}
	runByID := make(map[uint64]*common_persistence_models.ChatAgentRun)
	truncated := make(map[uint64]bool)
	incomplete := make(map[forkStep]bool)
	if len(runIDs) > 0 {
		slices.Sort(runIDs)
		runIDs = slices.Compact(runIDs)
		var runs []*common_persistence_models.ChatAgentRun
		if err := tx.Where("id = ANY(?)", runIDs).Find(&runs).Error; err != nil {
			return nil, nil, err
		}
		for _, run := range runs {
			runByID[run.ID] = run
		}
		var runRows []forkSourceRow
		if err := tx.Model(&common_persistence_models.ChatMessage{}).Select("id", "agent_run_id", "agent_step_index").
			Where("agent_run_id = ANY(?) AND kind <> ?", runIDs, proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE).Scan(&runRows).Error; err != nil {
			return nil, nil, err
		}
		for _, row := range runRows {
			// A stepless row past the cut, such as the run's terminal notice, leaves its run whole.
			if _, ok := selected[row.ID]; !ok && row.AgentStepIndex != nil {
				truncated[*row.AgentRunID] = true
				incomplete[forkStep{*row.AgentRunID, *row.AgentStepIndex}] = true
			}
		}
	}
	for _, row := range rows {
		if row.AgentRunID == nil || row.AgentStepIndex == nil {
			continue
		}
		step := forkStep{*row.AgentRunID, *row.AgentStepIndex}
		run := runByID[step.run]
		_, answeredCall := answered[call{step, common_util.Deref(row.ToolCallID)}]
		incomplete[step] = incomplete[step] || row.inFlight() ||
			(run != nil && !IsTerminalChatAgentRunStatus(run.Status) && (run.LastSealedStepIndex == nil || step.step > *run.LastSealedStepIndex)) ||
			(row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL && (row.ToolCallID == nil || !answeredCall))
	}
	firstCut := make(map[uint64]uint32)
	for step, cut := range incomplete {
		if first, ok := firstCut[step.run]; cut && (!ok || step.step < first) {
			firstCut[step.run] = step.step
		}
	}
	copied := make([]forkSourceRow, 0, len(rows))
	dropped := make(map[uint64]bool) // rows are ascending, so a root is decided before its replies
	for _, row := range rows {
		cut := row.inFlight() || dropped[row.ParentMessageID]
		if row.AgentRunID != nil && row.AgentStepIndex != nil {
			first, ok := firstCut[*row.AgentRunID]
			cut = cut || (ok && *row.AgentStepIndex >= first)
		}
		if !cut {
			copied = append(copied, row)
			continue
		}
		dropped[row.ID] = true
		if row.AgentRunID != nil {
			truncated[*row.AgentRunID] = true
		}
	}
	for id, run := range runByID {
		if truncated[id] {
			// The source's counters include the work the fork cut, so a truncated run is charged
			// only its kept steps (one generation each) and TOOL_CALLs; their active time is unknown.
			run.GenerationCount, run.ToolCallCount, run.ActiveDurationMs = 0, 0, 0
		}
		if truncated[id] || !IsTerminalChatAgentRunStatus(run.Status) {
			run.Status = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED
			run.StatusReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INCOMPLETE_STEP
			run.StatusReasonCode = nil
			run.CompletedAt = nil
		}
	}
	keptSteps := make(map[forkStep]bool)
	for _, row := range copied {
		run := runByID[common_util.Deref(row.AgentRunID)]
		if run == nil || !truncated[run.ID] {
			continue
		}
		if row.AgentStepIndex != nil && !keptSteps[forkStep{run.ID, *row.AgentStepIndex}] {
			keptSteps[forkStep{run.ID, *row.AgentStepIndex}] = true
			run.GenerationCount++
		}
		if row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL {
			run.ToolCallCount++
		}
	}
	return runByID, copied, nil
}

// inFlight reports a row that is still streaming or a TOOL_CALL that still owes its approval or
// execution.
func (r forkSourceRow) inFlight() bool {
	return r.StreamState == proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING ||
		(r.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL && (r.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING ||
			slices.Contains(unsettledToolCallExecutionStates, r.ExecutionState)))
}

// unsettledToolCallExecutionStates are the TOOL_CALL execution states that still owe a result.
var unsettledToolCallExecutionStates = []proto_models.ToolCallExecutionState{
	proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_WAITING_FOR_APPROVAL,
	proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION,
	proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
}

// forkSideRowPlan lists the rewritten side-row payloads by new message ID, the attachment clones
// (source, clone and entity ID), and, flattened in order, each copy's new attachment IDs.
type forkSideRowPlan struct {
	sources, clones                []int64
	entities                       pq.StringArray
	messages, ids, payloadMessages pq.Int64Array
	payloads                       pq.ByteaArray
}

// planForkSideRows plans the side-row payload rewrites and attachment clones of the copies.
// Compaction watermark IDs map to the greatest copied ID not above them, delegation tool calls
// and results name the copies (remapForkDelegationJSON), and a TOOL_RESULT's internal tool media
// is cloned under the new run's entity key, which tool-media resolution validates, with its
// payload pointing at the clone. Vetted, live user attachments are cloned onto their new message.
// Other references (pending uploads, provider state) are dropped from the copy.
func planForkSideRows(tx *gorm.DB, copied []forkSourceRow, messages, runs, sessions forkIDMap) (forkSideRowPlan, error) {
	var plan forkSideRowPlan
	var attachmentIDs, payloadMessages, tools pq.Int64Array
	for _, row := range copied {
		attachmentIDs = append(attachmentIDs, row.AttachmentIDs...)
		if row.Kind == proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY ||
			(row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT && len(row.AttachmentIDs) > 0) {
			payloadMessages = append(payloadMessages, int64(row.ID))
		} else if row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL || row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
			tools = append(tools, int64(row.ID))
		}
	}
	attachments := make(map[uint64]*common_persistence_models.Attachment)
	if len(attachmentIDs) > 0 {
		var rows []*common_persistence_models.Attachment
		// FOR SHARE holds the vetted rows until the fork commits: a concurrent delete waits, and its
		// object cleanup then sees the committed clones.
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ANY(?) AND upload_status = ?", attachmentIDs, common_persistence_models.UploadStatusVetted).
			Order("id").Find(&rows).Error; err != nil {
			return plan, err
		}
		for _, attachment := range rows {
			attachments[attachment.ID] = attachment
		}
	}
	clone := func(source uint64, entity string) int {
		plan.sources = append(plan.sources, int64(source))
		plan.entities = append(plan.entities, entity)
		return len(plan.sources) - 1
	}
	var sideRows []common_persistence_models.ChatMessageToolCall
	if len(payloadMessages) > 0 || len(tools) > 0 {
		// Every delegation call names its tool, and every delegation result naming an ID names a
		// child session (child_session_id, after_child_session_id) or a thread root.
		needles := append(pq.StringArray{`child_session_id"`, `"thread_root_id"`}, delegationToolNames...)
		if err := tx.Select("message_id", "tool_call_id", "model_payload").Where("message_id = ANY(?) OR (message_id = ANY(?) AND "+
			"EXISTS (SELECT 1 FROM unnest(CAST(? AS text[])) AS n(needle) WHERE position(convert_to(n.needle, 'UTF8') in model_payload) > 0))",
			payloadMessages, tools, needles).
			Order("message_id").Find(&sideRows).Error; err != nil {
			return plan, err
		}
	}
	type mediaClone struct {
		media *proto_models.MediaPart
		clone int
	}
	type toolCall struct {
		run uint64
		id  string
	}
	var media []mediaClone
	envelopes := make(map[uint64]*proto_models.MessageModelPayload, len(sideRows))
	toolMedia := make(map[uint64]map[uint64]int) // new message -> source attachment -> clone
	delegationCalls := make(map[toolCall]bool)
	runOf := make(map[uint64]*uint64, len(sideRows))
	for _, row := range copied {
		runOf[row.ID] = row.AgentRunID
	}
	for _, side := range sideRows {
		newMessage, _ := messages.get(side.MessageID)
		var envelope proto_models.MessageModelPayload
		if err := proto.Unmarshal(side.ModelPayload, &envelope); err != nil {
			return plan, fmt.Errorf("decode fork payload of message %d: %w", side.MessageID, err)
		}
		if compaction := envelope.GetCompaction(); compaction != nil {
			compaction.CoversThroughMessageId = messages.floor(compaction.CoversThroughMessageId)
			compaction.RetainedUserMessageId = messages.floor(compaction.RetainedUserMessageId)
			compaction.WorkflowStateMessageId = messages.floor(compaction.WorkflowStateMessageId)
			envelopes[newMessage] = &envelope
			continue
		}
		// Only delegation tools' IDs are remapped; an application tool's IDs are its own. A result
		// belongs to its run's TOOL_CALL of the same tool_call_id, which precedes it.
		call := toolCall{common_util.Deref(runOf[side.MessageID]), common_util.Deref(side.ToolCallID)}
		delegationCalls[call] = delegationCalls[call] || IsDelegationToolName(envelope.GetToolCall().GetToolName())
		var delegation *string
		if result := envelope.GetToolResult(); result != nil && delegationCalls[call] {
			delegation = &result.ContentJson
		} else if envelope.GetToolCall() != nil && delegationCalls[call] {
			delegation = &envelope.GetToolCall().ArgumentsJson
		}
		if remapped, err := remapForkDelegationJSON(delegation, messages, runs, sessions); err != nil {
			return plan, err
		} else if remapped {
			envelopes[newMessage] = &envelope
		}
		newRun := runs.ptr(runOf[side.MessageID])
		if envelope.GetToolResult() == nil || newRun == nil || side.ToolCallID == nil {
			continue
		}
		for partIndex, part := range envelope.GetToolResult().GetContentParts() {
			source, err := strconv.ParseUint(part.GetMedia().GetAttachmentId(), 10, 64)
			attachment := attachments[source]
			if err != nil || attachment == nil || !attachment.InternalOnly || attachment.ObjectType != "agent-tool-media" {
				continue
			}
			index := clone(source, toolMediaEntityID(*newRun, *side.ToolCallID, partIndex))
			media = append(media, mediaClone{part.GetMedia(), index})
			if toolMedia[newMessage] == nil {
				toolMedia[newMessage] = make(map[uint64]int)
			}
			toolMedia[newMessage][source] = index
			envelopes[newMessage] = &envelope
		}
	}
	var cloneOf []int // flattened clone index of each plan.messages/plan.ids entry
	for _, row := range copied {
		newMessage, _ := messages.get(row.ID)
		for _, raw := range row.AttachmentIDs {
			index, ok := toolMedia[newMessage][uint64(raw)]
			if attachment := attachments[uint64(raw)]; !ok && attachment != nil && !attachment.InternalOnly {
				index, ok = clone(uint64(raw), strconv.FormatUint(newMessage, 10)), true
			}
			if ok {
				plan.messages = append(plan.messages, int64(newMessage))
				cloneOf = append(cloneOf, index)
			}
		}
	}
	allocated, err := allocateForkIDs(tx, common_persistence_models.TableNameAttachments, plan.sources)
	if err != nil {
		return plan, err
	}
	plan.clones = allocated.new
	for _, index := range cloneOf {
		plan.ids = append(plan.ids, plan.clones[index])
	}
	for _, m := range media {
		m.media.Source = &proto_models.MediaPart_AttachmentId{AttachmentId: strconv.FormatInt(plan.clones[m.clone], 10)}
	}
	for _, side := range sideRows {
		newMessage, _ := messages.get(side.MessageID)
		if envelope := envelopes[newMessage]; envelope != nil {
			body, err := proto.Marshal(envelope)
			if err != nil {
				return plan, err
			}
			plan.payloadMessages = append(plan.payloadMessages, int64(newMessage))
			plan.payloads = append(plan.payloads, body)
		}
	}
	return plan, nil
}

// forkDelegationIDs is a delegation tool's arguments or result (spawn_subagent,
// send_subagent_message, read_subagent_messages, subagent_status, cancel_subagent,
// report_to_parent), or one nested member of it, reduced to the members that name delegation IDs;
// Rest keeps every other member. Zero and null members are omitted when it is re-encoded.
type forkDelegationIDs struct {
	ChildSessionID          uint64              `json:"child_session_id,omitzero"`
	AfterChildSessionID     uint64              `json:"after_child_session_id,omitzero"`
	NextAfterChildSessionID uint64              `json:"next_after_child_session_id,omitzero"`
	ThreadRootID            uint64              `json:"thread_root_id,omitzero"`
	AgentRunID              uint64              `json:"agent_run_id,omitzero"`
	MessageID               uint64              `json:"message_id,omitzero"`
	PendingMessageID        uint64              `json:"pending_message_id,omitzero"`
	BeforeMessageID         uint64              `json:"before_message_id,omitzero"`
	AfterMessageID          uint64              `json:"after_message_id,omitzero"`
	CancelledRunIDs         []uint64            `json:"cancelled_run_ids,omitzero"`
	Children                []forkDelegationIDs `json:"children,omitzero"`
	Messages                []forkDelegationIDs `json:"messages,omitzero"`
	Run                     *forkDelegationIDs  `json:"run,omitzero"`
	LastReport              *forkDelegationIDs  `json:"last_report,omitzero"`
	LastDiscardedMessage    *forkDelegationIDs  `json:"last_discarded_message,omitzero"`
	Rest                    jsontext.Value      `json:",embed"`
}

// remapForkDelegationJSON points the IDs a tool call's arguments or result names at their copies,
// so the cloned parent's transcript names the cloned children, threads, runs and messages it can
// address. Exclusive lower paging cursors (after_*) map to the greatest copied ID not above them,
// and before_message_id, an exclusive upper bound, to the least copied ID not below it. Other IDs
// the fork did not copy, and JSON that is not an object, stay as they were. Strings holding JSON,
// such as a read child message's arguments or result, are not rewritten. Reports whether it
// rewrote the JSON.
func remapForkDelegationJSON(raw *string, messages, runs, sessions forkIDMap) (bool, error) {
	var ids forkDelegationIDs
	if raw == nil || json.Unmarshal([]byte(*raw), &ids) != nil || !ids.remap(messages, runs, sessions) {
		return false, nil
	}
	body, err := json.Marshal(&ids)
	if err != nil {
		return false, err
	}
	*raw = string(body)
	return true, nil
}

// remap rewrites d's IDs and those of its nested members in place and reports whether any changed.
func (d *forkDelegationIDs) remap(messages, runs, sessions forkIDMap) bool {
	remapped := false
	exact := func(id *uint64, m forkIDMap) {
		if mapped, ok := m.get(*id); ok {
			*id, remapped = mapped, true
		}
	}
	cursor := func(id *uint64, bound func(uint64) uint64) {
		if *id != 0 {
			*id, remapped = bound(*id), true
		}
	}
	exact(&d.ChildSessionID, sessions)
	exact(&d.ThreadRootID, messages)
	exact(&d.AgentRunID, runs)
	exact(&d.MessageID, messages)
	exact(&d.PendingMessageID, messages)
	cursor(&d.AfterChildSessionID, sessions.floor)
	cursor(&d.NextAfterChildSessionID, sessions.floor)
	cursor(&d.BeforeMessageID, messages.ceil)
	cursor(&d.AfterMessageID, messages.floor)
	for i := range d.CancelledRunIDs {
		exact(&d.CancelledRunIDs[i], runs)
	}
	nested := []*forkDelegationIDs{d.Run, d.LastReport, d.LastDiscardedMessage}
	for i := range d.Children {
		nested = append(nested, &d.Children[i])
	}
	for i := range d.Messages {
		nested = append(nested, &d.Messages[i])
	}
	for _, member := range nested {
		if member != nil && member.remap(messages, runs, sessions) {
			remapped = true
		}
	}
	return remapped
}

// insertForkRuns writes the copied runs onto the destination: terminal, lease-free, and pointing
// at the cloned sessions and copied messages.
func insertForkRuns(tx *gorm.DB, destination uint64, runByID map[uint64]*common_persistence_models.ChatAgentRun, messages, runs, sessions forkIDMap) error {
	now := time.Now()
	clones := make([]common_persistence_models.ChatAgentRun, 0, len(runs.old))
	for i, old := range runs.old {
		source := runByID[uint64(old)]
		if source == nil {
			return fmt.Errorf("fork run %d was not found", old)
		}
		run := *source
		run.ID, run.ChannelID = uint64(runs.new[i]), destination
		if run.ParentMessageID != 0 {
			root, ok := messages.get(run.ParentMessageID)
			if !ok {
				return fmt.Errorf("fork run %d: thread root %d was not copied", old, run.ParentMessageID)
			}
			run.ParentMessageID = root
		}
		if run.AgentSessionID = sessions.ptr(run.AgentSessionID); run.AgentSessionID == nil {
			run.AgentSessionConfigVersion = nil
		}
		if run.CompletedAt == nil {
			run.CompletedAt = &now
		}
		run.TriggeringMessageID = messages.ptr(run.TriggeringMessageID)
		run.DelegationNoticeMessageID = messages.ptr(run.DelegationNoticeMessageID)
		run.ConsumedPendingMessageID = messages.floor(run.ConsumedPendingMessageID)
		run.WokenByAgentRunID, run.InputDrainedFromRunID = runs.ptr(run.WokenByAgentRunID), runs.ptr(run.InputDrainedFromRunID)
		run.DriveLeaseOwner, run.DriveLeaseExpiresAt, run.DriveNotBefore = nil, nil, nil
		run.DriveRetryReason, run.DriveAbandonCount = 0, 0
		run.StreamRecoveryStepIndex, run.StreamRecoveryCount = 0, 0
		run.CreatedAt, run.UpdatedAt = time.Time{}, time.Time{}
		clones = append(clones, run)
	}
	if len(clones) == 0 {
		return nil
	}
	return tx.CreateInBatches(&clones, 500).Error
}

// insertForkSessions clones the sessions at copied locations, idle, with their configuration,
// graph budget pool, agent context and context operations. A cloned pool keeps its limits and is
// charged the lesser of its graph's copied runs' usage and what the source pool recorded, so
// neither usage the source spent after the fork point nor runs that predate the pool exhaust the
// fork.
func insertForkSessions(tx *gorm.DB, in CopyChatChannelForkInput, sources []common_persistence_models.ChatAgentSession,
	runByID map[uint64]*common_persistence_models.ChatAgentRun, messages, runs, sessions forkIDMap) error {
	if len(sources) == 0 {
		return nil
	}
	parentOf := make(map[uint64]*uint64, len(sources))
	for _, source := range sources {
		parentOf[source.ID] = source.ParentAgentSessionID
	}
	var usedRoots, usedGenerations, usedToolCalls, usedActiveMs pq.Int64Array
	for _, old := range runs.old {
		run := runByID[uint64(old)]
		if run == nil || run.AgentSessionID == nil {
			continue
		}
		root := *run.AgentSessionID
		for parent := parentOf[root]; parent != nil; parent = parentOf[root] {
			root = *parent
		}
		newRoot, ok := sessions.get(root)
		if !ok {
			continue
		}
		i := slices.Index(usedRoots, int64(newRoot))
		if i < 0 {
			i = len(usedRoots)
			usedRoots, usedGenerations = append(usedRoots, int64(newRoot)), append(usedGenerations, 0)
			usedToolCalls, usedActiveMs = append(usedToolCalls, 0), append(usedActiveMs, 0)
		}
		usedGenerations[i] += int64(run.GenerationCount)
		usedToolCalls[i] += int64(run.ToolCallCount)
		usedActiveMs[i] += int64(run.ActiveDurationMs)
	}
	clones := make([]common_persistence_models.ChatAgentSession, len(sources))
	for i, source := range sources {
		clone := source
		clone.ID, clone.ChannelID = uint64(sessions.new[i]), in.DestinationChannelID
		clone.ParentMessageID, _ = messages.get(source.ParentMessageID)
		clone.UserID, clone.CreatedByUserID, clone.InferenceActorUserID = in.ForkerUserID, in.ForkerUserID, nil
		clone.CreationOrigin = proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_FORK
		clone.CreateIdempotencyKey, clone.CreateIdempotencyFingerprint, clone.IsDefaultScoped = nil, nil, false
		clone.InitialMessageID, clone.InitialRunID = messages.ptr(source.InitialMessageID), runs.ptr(source.InitialRunID)
		clone.ParentAgentSessionID = sessions.ptr(source.ParentAgentSessionID)
		clone.SpawnedByAgentRunID = runs.ptr(source.SpawnedByAgentRunID)
		clone.ReplacesChildSessionID = sessions.ptr(source.ReplacesChildSessionID)
		clone.ResumeRevision, clone.ParkedRevision = 0, nil
		clone.PendingWakeMessageID, clone.PendingWakeCount, clone.DeferredWakeGroups = nil, 0, nil
		clone.SuspendedAt, clone.SuspendedByUserID, clone.SuspendedRunID = nil, nil, nil
		if !in.KeepContext {
			clone.ContextID, clone.ContextEntityID = nil, nil
		}
		clone.CreatedAt, clone.UpdatedAt = time.Time{}, time.Time{}
		clones[i] = clone
	}
	if err := tx.CreateInBatches(&clones, 500).Error; err != nil {
		return err
	}
	if err := tx.Exec(`WITH s(old_id, new_id) AS (SELECT * FROM unnest(CAST(@sessions_old AS bigint[]), CAST(@sessions_new AS bigint[]))),
snapshots AS (INSERT INTO gocommon_chat_agent_session_config_snapshots (agent_session_id, version, tool_config_payload,
	compaction_payload, delegation_override_payload, input_mode, allow_user_stop, created_by_user_id, created_at)
	SELECT s.new_id, c.version, c.tool_config_payload, c.compaction_payload, c.delegation_override_payload, c.input_mode,
	c.allow_user_stop, c.created_by_user_id, c.created_at
	FROM s JOIN gocommon_chat_agent_session_config_snapshots c ON c.agent_session_id = s.old_id),
used(root_id, generations, tool_calls, active_ms) AS (SELECT * FROM unnest(CAST(@used_roots AS bigint[]),
	CAST(@used_generations AS bigint[]), CAST(@used_tool_calls AS bigint[]), CAST(@used_active_ms AS bigint[]))),
budgets AS (INSERT INTO gocommon_chat_agent_graph_budgets (root_session_id, max_generations, used_generations, max_tool_calls,
	used_tool_calls, max_active_ms, used_active_ms, created_at, updated_at)
	SELECT s.new_id, b.max_generations, LEAST(COALESCE(u.generations, 0), b.used_generations), b.max_tool_calls,
	LEAST(COALESCE(u.tool_calls, 0), b.used_tool_calls), b.max_active_ms, LEAST(COALESCE(u.active_ms, 0), b.used_active_ms), now(), now()
	FROM s JOIN gocommon_chat_agent_graph_budgets b ON b.root_session_id = s.old_id LEFT JOIN used u ON u.root_id = s.new_id)
INSERT INTO gocommon_chat_agent_session_tool_configs (agent_session_id, version, tool_name, enabled, requires_approval, renderer_key, created_at)
SELECT s.new_id, t.version, t.tool_name, t.enabled, t.requires_approval, t.renderer_key, t.created_at
FROM s JOIN gocommon_chat_agent_session_tool_configs t ON t.agent_session_id = s.old_id ORDER BY t.id`,
		sql.Named("sessions_old", pq.Int64Array(sessions.old)), sql.Named("sessions_new", pq.Int64Array(sessions.new)),
		sql.Named("used_roots", usedRoots), sql.Named("used_generations", usedGenerations),
		sql.Named("used_tool_calls", usedToolCalls), sql.Named("used_active_ms", usedActiveMs)).Error; err != nil {
		return err
	}

	var contexts []common_persistence_models.ChatAgentContext
	if err := tx.Where("agent_session_id = ANY(?)", pq.Int64Array(sessions.old)).Find(&contexts).Error; err != nil {
		return err
	}
	for i := range contexts {
		state := &contexts[i]
		state.AgentSessionID, _ = sessions.get(state.AgentSessionID)
		if state.WorkflowStateMessageID, _ = messages.get(state.WorkflowStateMessageID); state.WorkflowStateMessageID == 0 {
			state.WorkflowState = "" // the fork lacks the note, so the next run posts the state again
		}
		state.ManualCompactionRunID, _ = runs.get(state.ManualCompactionRunID)
		// Observations describe inference calls, which a fork does not copy.
		state.ObservationRunID, state.ObservedInputTokens, state.ObservedEstimate = 0, 0, 0
		state.ObservedCheckpointID, state.ObservedCallID, state.ObservationPrefixSHA256 = 0, "", ""
	}
	if len(contexts) > 0 {
		if err := tx.CreateInBatches(&contexts, 500).Error; err != nil {
			return err
		}
	}

	var operations []common_persistence_models.ChatContextOperation
	if len(runs.old) > 0 {
		if err := tx.Where("agent_run_id = ANY(?)", pq.Int64Array(runs.old)).Find(&operations).Error; err != nil {
			return err
		}
	}
	for i := range operations {
		op := &operations[i]
		// Keys hash the scope that minted them; the copy's key only has to stay unique.
		sum := sha256.Sum256(fmt.Appendf(nil, "fork:%d:%s", in.DestinationChannelID, op.Key))
		op.Key = hex.EncodeToString(sum[:])
		op.AgentRunID, _ = runs.get(op.AgentRunID)
		op.PredecessorMessageID, _ = messages.get(op.PredecessorMessageID)
		op.CoversThroughMessageID = messages.floor(op.CoversThroughMessageID)
		op.MessageID, _ = messages.get(op.MessageID)
		if len(op.OmittedMessageIDsJSON) > 0 {
			var omitted, kept []uint64
			if err := json.Unmarshal(op.OmittedMessageIDsJSON, &omitted); err != nil {
				return fmt.Errorf("decode omitted messages of context operation: %w", err)
			}
			for _, id := range omitted {
				if mapped, ok := messages.get(id); ok {
					kept = append(kept, mapped)
				}
			}
			raw, err := json.Marshal(kept)
			if err != nil {
				return err
			}
			op.OmittedMessageIDsJSON = raw
		}
	}
	if len(operations) == 0 {
		return nil
	}
	return tx.CreateInBatches(&operations, 500).Error
}

// GetChatChannelByForkRequestKey returns the fork createdByUserID made with an idempotency key,
// soft-deleted ones included (DeletedAt set). Returns ErrChatChannelNotFound when there is none.
func (h *CommonPersistenceHandler[U]) GetChatChannelByForkRequestKey(ctx context.Context, createdByUserID uint64, requestKey string) (*common_persistence_models.ChatChannel, error) {
	var channel common_persistence_models.ChatChannel
	err := h.getDBFromContext(ctx).Unscoped().
		Where("created_by_user_id = ? AND fork_request_key = ?", createdByUserID, requestKey).Take(&channel).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrChatChannelNotFound
	}
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "get_chat_channel_by_fork_request_key"))
	}
	return &channel, nil
}
