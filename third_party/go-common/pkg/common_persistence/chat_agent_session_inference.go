package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_context"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A blocked session retains its nonterminal work until an explicit retarget remedies the gate.
const chatAgentRunInferenceBlockedSQL = "EXISTS (SELECT 1 FROM " + models.TableNameChatAgentSessions +
	" blocked_session WHERE blocked_session.id = " + models.TableNameChatAgentRuns +
	".agent_session_id AND blocked_session.inference_blocked_reason <> 0)"

var (
	ErrChatAgentSessionInferencePinInvalid         = errors.New("invalid chat agent session inference pin")
	ErrChatAgentSessionInferencePinVersionConflict = errors.New("chat agent session inference pin version conflict")
)

func validateChatAgentSessionInferencePin(pin models.ChatAgentSessionInferencePin) error {
	selected, err := pin.ExecutionModelSelection()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrChatAgentSessionInferencePinInvalid, err)
	}
	if selected == nil {
		return ErrChatAgentSessionInferencePinInvalid
	}
	return nil
}

// lockContextDriveRunForSessionTx lets a drive publish a session decision only while it still
// owns its run. It locks the contextual run before the session, matching drive write ordering.
func (h *CommonPersistenceHandler[U]) lockContextDriveRunForSessionTx(ctx context.Context, tx *gorm.DB, sessionID uint64) error {
	lease, ok := common_context.GetChatAgentRunDriveLease(ctx)
	if !ok {
		return nil
	}
	run, err := h.lockLeasedChatAgentRunTx(ctx, tx, lease.RunID, "id", "agent_session_id")
	if err != nil {
		return err
	}
	if run.AgentSessionID == nil || *run.AgentSessionID != sessionID {
		return ErrChatTranscriptRunMismatch
	}
	return nil
}

// InitializeChatAgentSessionInferencePin pins a legacy session once. Concurrent initializers
// receive the winner's complete pin rather than changing it. It never repairs a partial pin.
func (h *CommonPersistenceHandler[U]) InitializeChatAgentSessionInferencePin(ctx context.Context, sessionID uint64, pin models.ChatAgentSessionInferencePin) (*models.ChatAgentSession, error) {
	if sessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	if pin.InferencePinVersion > 1 {
		return nil, ErrChatAgentSessionInferencePinInvalid
	}
	pin.InferencePinVersion = 1
	if err := validateChatAgentSessionInferencePin(pin); err != nil {
		return nil, err
	}
	var session *models.ChatAgentSession
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.lockContextDriveRunForSessionTx(txCtx, tx, sessionID); err != nil {
			return err
		}
		var err error
		session, err = lockChatAgentSessionRowTx(tx, sessionID)
		if err != nil {
			return err
		}
		if session == nil {
			return ErrChatAgentSessionNotFound
		}
		selected, err := session.ExecutionModelSelection()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrChatAgentSessionInferencePinInvalid, err)
		}
		if selected != nil {
			return nil
		}
		updates := chatAgentSessionInferencePinUpdates(pin)
		result := tx.Model(session).Where("inference_pin_version = 0").Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChatAgentSessionInferencePinVersionConflict
		}
		session.ChatAgentSessionInferencePin = pin
		return nil
	})
	if err != nil {
		return nil, chatAgentSessionInferenceError(err, "initialize_chat_agent_session_inference_pin")
	}
	return session, nil
}

// BlockChatAgentSessionInference persists a dispatch rejection against the observed pin.
// A stale decision cannot block a session that has since been explicitly retargeted.
func (h *CommonPersistenceHandler[U]) BlockChatAgentSessionInference(ctx context.Context, sessionID, expectedPinVersion uint64, reason proto_models.InferenceBlockReason) error {
	if sessionID == 0 {
		return ErrChatAgentSessionNotFound
	}
	if !validInferenceBlockReason(reason) {
		return ErrChatAgentSessionInferencePinInvalid
	}
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.lockContextDriveRunForSessionTx(txCtx, tx, sessionID); err != nil {
			return err
		}
		result := tx.Model(&models.ChatAgentSession{}).Where("id = ? AND inference_pin_version = ?", sessionID, expectedPinVersion).
			Updates(map[string]any{"inference_blocked_reason": reason, "inference_blocked_at": gorm.Expr("CURRENT_TIMESTAMP"), "updated_at": gorm.Expr("CURRENT_TIMESTAMP")})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChatAgentSessionInferencePinVersionConflict
		}
		return nil
	})
	return chatAgentSessionInferenceError(err, "block_chat_agent_session_inference")
}

// GetChatAgentSessionsByIDs reads one organization-scoped selection in a single query.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionsByIDs(ctx context.Context, organizationID int64, sessionIDs []uint64) ([]models.ChatAgentSession, error) {
	if organizationID <= 0 {
		return nil, ErrChatAgentSessionScopeInvalid
	}
	if len(sessionIDs) == 0 {
		return []models.ChatAgentSession{}, nil
	}
	var sessions []models.ChatAgentSession
	err := h.DB(ctx).Where("organization_id = ? AND id IN ?", organizationID, sessionIDs).Order("id").Find(&sessions).Error
	return sessions, chatAgentSessionInferenceError(err, "get_chat_agent_sessions_by_ids")
}

// BatchRetargetChatAgentSessionInferencePins applies one resolved target to an authorized group,
// atomically auditing each change, clearing its block, and invalidating opaque replay.
// Every ID and expected version must match: an invalid, stale, or busy member rolls back the
// entire group. The service bounds the request and groups sessions by resolved target/policy.
// Clearing a block is the single unblock path, so a blocked session that retained work (a
// nonterminal run or a deferred wake) advances its graph root's resume revision here, which resumes
// a graph parked on the block.
func (h *CommonPersistenceHandler[U]) BatchRetargetChatAgentSessionInferencePins(ctx context.Context, organizationID int64, sessionIDs []uint64, expectedPinVersions map[uint64]uint64, pin models.ChatAgentSessionInferencePin, actorUserID uint64) ([]models.ChatAgentSession, error) {
	if organizationID <= 0 {
		return nil, ErrChatAgentSessionScopeInvalid
	}
	if len(sessionIDs) == 0 {
		return []models.ChatAgentSession{}, nil
	}
	var sessions []models.ChatAgentSession
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var err error
		sessions, err = h.retargetChatAgentSessionInferencePinsTx(txCtx, tx, organizationID, sessionIDs, expectedPinVersions, pin, actorUserID)
		return err
	})
	if err != nil {
		return nil, chatAgentSessionInferenceError(err, "batch_retarget_chat_agent_session_inference_pins")
	}
	return sessions, nil
}

func (h *CommonPersistenceHandler[U]) retargetChatAgentSessionInferencePinsTx(ctx context.Context, tx *gorm.DB, organizationID int64, sessionIDs []uint64, expected map[uint64]uint64, pin models.ChatAgentSessionInferencePin, actorUserID uint64) ([]models.ChatAgentSession, error) {
	if actorUserID == 0 {
		return nil, ErrChatAgentSessionInferencePinInvalid
	}
	// The trusted service carries explicit override authority under SITE_OVERRIDE or RETARGET.
	// A normal retarget supplies no provenance and clears the old override actor.
	switch pin.SelectionSource {
	case proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE:
		if pin.SiteOverrideUserID == nil || *pin.SiteOverrideUserID != actorUserID {
			return nil, ErrChatAgentSessionInferencePinInvalid
		}
	case proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET:
		if pin.SiteOverrideUserID != nil && *pin.SiteOverrideUserID != actorUserID {
			return nil, ErrChatAgentSessionInferencePinInvalid
		}
	default:
		pin.SiteOverrideUserID = nil
	}
	pin.SelectionSource = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET
	pin.SelectingUserID = &actorUserID
	pin.InferencePinVersion = 1 // Each session receives its own next version below.
	if err := validateChatAgentSessionInferencePin(pin); err != nil {
		return nil, err
	}
	ids := slices.Clone(sessionIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(expected) != len(ids) {
		return nil, ErrChatAgentSessionInferencePinVersionConflict
	}
	for _, id := range ids {
		version, ok := expected[id]
		if !validBigintID(id) || !ok || !validInferencePolicyVersion(version) {
			return nil, ErrChatAgentSessionInferencePinVersionConflict
		}
	}
	channels, err := lockChatAgentInferenceChannelsTx(tx, organizationID, ids)
	if err != nil {
		return nil, err
	}
	// Session locks serialize admission. NOWAIT on run locks avoids waiting in the opposite
	// direction to run endings (run -> channel -> session), and reports that work is in flight.
	var sessions []models.ChatAgentSession
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("organization_id = ? AND id IN ?", organizationID, ids).Order("id").Find(&sessions).Error; err != nil {
		return nil, err
	}
	if len(sessions) != len(ids) {
		return nil, ErrChatAgentSessionNotFound
	}
	for _, session := range sessions {
		if _, ok := channels[session.ChannelID]; !ok {
			return nil, ErrChatAgentSessionScopeInvalid
		}
		if session.Status != models.ChatAgentSessionStatusActive {
			return nil, ErrChatAgentSessionScopeInvalid
		}
		if session.InferenceOperation != 0 && session.InferenceOperation != pin.InferenceOperation {
			return nil, ErrChatAgentSessionInferencePinInvalid
		}
		if session.InferencePinVersion != expected[session.ID] {
			return nil, ErrChatAgentSessionInferencePinVersionConflict
		}
		if _, err := session.ExecutionModelSelection(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrChatAgentSessionInferencePinInvalid, err)
		}
	}
	var runs []struct {
		ID             uint64
		AgentSessionID uint64
		HasLiveLease   bool
	}
	if err := tx.Model(&models.ChatAgentRun{}).Select("id, agent_session_id, (drive_lease_owner IS NOT NULL AND drive_lease_expires_at > CURRENT_TIMESTAMP) AS has_live_lease").
		Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).Where("agent_session_id IN ? AND status NOT IN ?", ids, TerminalChatAgentRunStatuses()).Order("id").Find(&runs).Error; err != nil {
		if isLockNotAvailable(err) {
			return nil, ErrChatAgentSessionRunActive
		}
		return nil, err
	}
	withRun := make(map[uint64]bool, len(runs))
	for _, run := range runs {
		if run.HasLiveLease {
			return nil, ErrChatAgentSessionRunActive
		}
		withRun[run.AgentSessionID] = true
	}
	var resumed []uint64
	for _, session := range sessions {
		if session.IsInferenceBlocked() && (withRun[session.ID] || session.PendingWakeMessageID != nil) {
			resumed = append(resumed, session.ID)
		}
	}
	now := time.Now()
	audits := make([]models.ChatAgentSessionInferenceRetarget, 0, len(sessions))
	for i := range sessions {
		next := pin
		next.InferencePinVersion = sessions[i].InferencePinVersion + 1
		audits = append(audits, models.ChatAgentSessionInferenceRetarget{AgentSessionID: sessions[i].ID, ActorUserID: actorUserID, OldPin: sessions[i].ChatAgentSessionInferencePin, NewPin: next, Version: next.InferencePinVersion, CreatedAt: now})
		sessions[i].ChatAgentSessionInferencePin = next
		sessions[i].InferenceBlockedReason = 0
		sessions[i].InferenceBlockedAt = nil
		sessions[i].UpdatedAt = now
	}
	if err := tx.Create(&audits).Error; err != nil {
		return nil, err
	}
	updates := chatAgentSessionInferencePinUpdates(pin)
	updates["inference_pin_version"] = gorm.Expr("inference_pin_version + 1")
	updates["inference_blocked_reason"] = 0
	updates["inference_blocked_at"] = nil
	updates["updated_at"] = now
	if err := tx.Model(&models.ChatAgentSession{}).Where("id IN ?", ids).Updates(updates).Error; err != nil {
		return nil, err
	}
	// Clear only opaque replay markers. Soft deletion retains historical payloads/attachments;
	// canonical text, tools, and compaction summaries remain available to reconstruction.
	sessionRuns := tx.Model(&models.ChatAgentRun{}).Select("id").Where("agent_session_id IN ?", ids)
	if err := tx.Where("agent_run_id IN (?) AND kind = ?", sessionRuns, proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE).Delete(&models.ChatMessage{}).Error; err != nil {
		return nil, err
	}
	// Unfinished runs use the authoritative session pin and can dispatch immediately, without
	// the former target's request pin or retry backoff. Work ceilings and counters remain intact.
	if err := tx.Model(&models.ChatAgentRun{}).Where("agent_session_id IN ? AND status NOT IN ?", ids, TerminalChatAgentRunStatuses()).Updates(map[string]any{
		"requested_inference_model_id": nil, "requested_inference_api": nil, "execution_inference_model_id": nil, "execution_inference_api": nil, "execution_provider_id": nil, "execution_provider_model_id": nil,
		"drive_not_before": nil, "drive_retry_reason": proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED,
	}).Error; err != nil {
		return nil, err
	}
	// Token observations are specific to the former model and must not constrain the new target.
	if err := tx.Model(&models.ChatAgentContext{}).Where("agent_session_id IN ?", ids).Updates(map[string]any{
		"tool_manifest_sha256": "", "observation_run_id": 0, "observed_input_tokens": 0, "observed_estimate": 0, "observed_checkpoint_id": 0, "observed_call_id": "", "observation_prefix_sha256": "",
	}).Error; err != nil {
		return nil, err
	}
	return sessions, h.advanceChatAgentGraphResumeRevisionTx(ctx, tx, resumed, proto_models.GraphEventKind_GRAPH_EVENT_KIND_INFERENCE_RETARGET)
}

// DrainRetargetedChatAgentSessions starts idle work that a blocked session retained before
// its retarget. The caller supplies authorized, successfully retargeted sessions. Stop remains
// suspended. Results carry newly minted runs and delivered messages for publication after commit.
// One batch probe avoids per-session empty queue reads; only sessions with idle work take locks.
func (h *CommonPersistenceHandler[U]) DrainRetargetedChatAgentSessions(ctx context.Context, sessionIDs []uint64) ([]ChatAgentSessionResume, error) {
	if len(sessionIDs) == 0 {
		return []ChatAgentSessionResume{}, nil
	}
	var results []ChatAgentSessionResume
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var candidates []uint64
		if err := h.retargetedChatAgentDrainCandidatesTx(tx, sessionIDs).Pluck("id", &candidates).Error; err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}
		channels, err := lockChatAgentInferenceChannelsTx(tx, 0, candidates)
		if err != nil {
			return err
		}
		var sessions []struct {
			Session         models.ChatAgentSession `gorm:"embedded"`
			HasQueuedInputs bool
		}
		// Recheck after the admission channel locks: a competing send may have started a run,
		// queued additional input, or suspended the session while the probe was unlocked.
		if err := h.retargetedChatAgentDrainCandidatesTx(tx, candidates).
			Select(models.TableNameChatAgentSessions + ".*, EXISTS (SELECT 1 FROM " + models.TableNameChatAgentQueuedInputs + " q WHERE q.agent_session_id = " + models.TableNameChatAgentSessions + ".id) AS has_queued_inputs").
			Clauses(clause.Locking{Strength: "UPDATE"}).Find(&sessions).Error; err != nil {
			return err
		}
		for i := range sessions {
			session := &sessions[i].Session
			channel, ok := channels[session.ChannelID]
			if !ok || channel.DeletedAt.Valid || channel.IsArchived {
				continue
			}
			resume := ChatAgentSessionResume{Session: session}
			if sessions[i].HasQueuedInputs {
				resume.Run, resume.Delivery, err = h.drainLockedChatAgentInputTx(txCtx, tx, session, false, 0)
				if err != nil {
					return err
				}
			}
			if resume.Run == nil && session.IsMainTimeline() && session.PendingWakeMessageID != nil {
				resume.Run, err = h.drainPendingParentWakeTx(txCtx, tx, session.ID, nil)
				if err != nil {
					return err
				}
				// The helper rereads the session; reflect its successful wake drain in the result.
				if resume.Run != nil {
					session.PendingWakeMessageID = nil
					session.PendingWakeCount = 0
				}
			}
			resume.Resumed = resume.Run != nil || len(resume.Delivery.Delivered) != 0
			results = append(results, resume)
		}
		return nil
	})
	if err != nil {
		return nil, chatAgentSessionInferenceError(err, "drain_retargeted_chat_agent_sessions")
	}
	return results, nil
}

// retargetedChatAgentDrainCandidatesTx selects the sessions with idle retained work. An owner-driven
// session (workflow-owned type) qualifies whatever its auto-run policy, like its wakes: workflow
// roots run with auto-run disabled, and the run a drain mints is left for the owner's drive.
func (h *CommonPersistenceHandler[U]) retargetedChatAgentDrainCandidatesTx(tx *gorm.DB, sessionIDs []uint64) *gorm.DB {
	sessions := models.TableNameChatAgentSessions
	return tx.Model(&models.ChatAgentSession{}).Where(sessions+".id IN ?", sessionIDs).
		Where(sessions+".status = ? AND "+sessions+".inference_blocked_reason = 0 AND "+sessions+".suspended_at IS NULL AND "+sessions+".inference_pin_version > 0", models.ChatAgentSessionStatusActive).
		Where("("+sessions+".auto_run_policy = ? OR "+sessions+".agent_type = ANY(?))", models.ChatAgentSessionAutoRunPolicyAutomatic, pq.Array(h.chatAgentWorkflowTypes())).
		Where("NOT EXISTS (SELECT 1 FROM "+models.TableNameChatAgentRuns+" r WHERE r.agent_session_id = "+sessions+".id AND r.status IN ?)", ActiveChatAgentRunStatuses()).
		Where("(" + sessions + ".pending_wake_message_id IS NOT NULL OR EXISTS (SELECT 1 FROM " + models.TableNameChatAgentQueuedInputs + " q WHERE q.agent_session_id = " + sessions + ".id))").Order(sessions + ".id")
}

// lockChatAgentInferenceChannelsTx takes admission locks in channel-ID order before session
// locks. NOWAIT also prevents inversion when an ambient bulk transaction already holds another
// group's channel/session locks. A busy writer rejects the whole retarget for a safe retry.
func lockChatAgentInferenceChannelsTx(tx *gorm.DB, organizationID int64, sessionIDs []uint64) (map[uint64]models.ChatChannel, error) {
	scope := tx.Model(&models.ChatAgentSession{}).Select("channel_id").Where("id IN ?", sessionIDs)
	if organizationID > 0 {
		scope = scope.Where("organization_id = ?", organizationID)
	}
	var channels []models.ChatChannel
	if err := tx.Unscoped().Model(&models.ChatChannel{}).Select("id", "is_archived", "deleted_at").
		Clauses(clause.Locking{Strength: "NO KEY UPDATE", Options: "NOWAIT"}).
		Where("id IN (?)", scope).Order("id").Find(&channels).Error; err != nil {
		if isLockNotAvailable(err) {
			return nil, ErrChatAgentSessionRunActive
		}
		return nil, err
	}
	result := make(map[uint64]models.ChatChannel, len(channels))
	for _, channel := range channels {
		result[channel.ID] = channel
	}
	return result, nil
}

func chatAgentSessionInferencePinUpdates(pin models.ChatAgentSessionInferencePin) map[string]any {
	return map[string]any{
		"inference_operation": pin.InferenceOperation, "execution_inference_model_id": pin.ExecutionInferenceModelID, "execution_inference_api": pin.ExecutionInferenceAPI,
		"execution_provider_id": pin.ExecutionProviderID, "execution_provider_model_id": pin.ExecutionProviderModelID,
		"member_choice_selection": pin.MemberChoiceSelection, "selection_source": pin.SelectionSource, "selecting_user_id": pin.SelectingUserID, "site_override_user_id": pin.SiteOverrideUserID, "operation_list_version": pin.OperationListVersion,
		"organization_policy_version": pin.OrganizationPolicyVersion, "organization_operation_policy_version": pin.OrganizationOperationPolicyVersion,
		"inference_pin_version": pin.InferencePinVersion,
	}
}

func chatAgentSessionInferenceError(err error, stage string) error {
	return knownOrChatDatabaseError(err, stage, ErrDatabaseError, ErrChatAgentSessionNotFound, ErrChatAgentSessionScopeInvalid, ErrChatAgentSessionRunActive, ErrChatAgentSessionInferencePinInvalid, ErrChatAgentSessionInferencePinVersionConflict, ErrChatAgentRunDriveLeaseLost, ErrChatTranscriptRunMismatch)
}
