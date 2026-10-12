package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// CHAT: AGENT SESSIONS + CONFIG SNAPSHOTS
// ============================================================================

// CreateChatAgentSession inserts a new agent session row. The caller is responsible for
// validating agent type / participants at the service layer; persistence only writes the row.
// A scoped-context collision surfaces as ErrChatAgentSessionScopedConflict so callers can
// roll back any just-created channel and reload the winning scoped session. An unset (zero)
// Status is stored as ACTIVE.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - session: The session row to insert. Its ID is populated on success.
func (h *CommonPersistenceHandler[U]) CreateChatAgentSession(ctx context.Context, session *common_persistence_models.ChatAgentSession) error {
	if session == nil {
		return appLogger.InvariantError("chat_agent_session_create_input_present",
			errors.New("chat agent session is required"),
			zap.String(logkeys.Stage, "create_chat_agent_session"),
			zap.String(logkeys.Expected, "non-nil session"),
			zap.String(logkeys.Actual, "nil session"))
	}
	// Partial execution identities must not create sessions that can silently fall back to routing.
	if session.ExecutionInferenceModelID != nil && session.InferencePinVersion == 0 {
		session.InferencePinVersion = 1
	}
	if _, err := session.ExecutionModelSelection(); err != nil {
		return fmt.Errorf("%w: %w", ErrChatAgentSessionInferencePinInvalid, err)
	}
	if session.OrganizationID <= 0 {
		return appLogger.InvariantError("chat_agent_session_has_organization",
			errors.New("chat agent session organization_id must be positive"),
			zap.String(logkeys.Stage, "create_chat_agent_session"),
			zap.Uint64(appLogger.KeyChannelID, session.ChannelID),
			zap.String(logkeys.Expected, "organization_id > 0"),
			zap.Int64(logkeys.Actual, session.OrganizationID))
	}
	if session.Status == common_persistence_models.ChatAgentSessionStatusUnspecified {
		session.Status = common_persistence_models.ChatAgentSessionStatusActive
	}
	err := h.withChatLocationTransaction(ctx, func(txCtx context.Context, db *gorm.DB) error {
		channel, err := h.lockChatLocationChannel(txCtx, session.ChannelID)
		if err != nil {
			return err
		}
		if err := h.validateAgentSessionLocation(txCtx, channel, session); err != nil {
			return err
		}
		return db.Create(session).Error
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentSessionScopeInvalid) || errors.Is(err, ErrChatChannelNotFound) {
			return err
		}
		if isUniqueViolationOn(err, "idx_chat_agent_sessions_location") {
			return ErrChatAgentSessionLocationConflict
		}
		if session.CreateIdempotencyKey != nil && *session.CreateIdempotencyKey != "" && isUniqueViolationOn(err, "idx_chat_agent_sessions_create_idempotency_org", "idx_chat_agent_sessions_parent_create_idempotency", "idx_chat_agent_sessions_replaces_child") {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("chat agent session create lost idempotency race",
					zap.Int64(appLogger.KeyOrganizationID, session.OrganizationID),
					zap.String(appLogger.KeyAgentType, session.AgentType))
			}
			return ErrChatAgentSessionIdempotencyConflict
		}
		if isUniqueViolationOn(err, "idx_chat_agent_sessions_default_scoped_context_active_org_v2") {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("chat agent session create lost scoped-context race",
					zap.Int64(appLogger.KeyOrganizationID, session.OrganizationID),
					zap.String(appLogger.KeyAgentType, session.AgentType))
			}
			return ErrChatAgentSessionScopedConflict
		}
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_chat_agent_session"),
			zap.Int64(appLogger.KeyOrganizationID, session.OrganizationID),
			zap.Uint64(appLogger.KeyChannelID, session.ChannelID),
			zap.String(appLogger.KeyAgentType, session.AgentType))
	}
	return nil
}

// GetChatAgentSessionByID fetches a session by its primary key.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - sessionID: The session identifier.
//
// Returns:
//   - result1: The session row, or nil.
//   - result2: ErrChatAgentSessionNotFound if absent, ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionByID(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if sessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session",
		[]zap.Field{zap.Uint64(appLogger.KeyAgentSessionID, sessionID)}, "id = ?", sessionID)
}

// GetChatAgentSessionByIDForShare reads one session under a FOR SHARE row lock. A concurrent
// UPDATE of the row (an archive) waits until the caller's transaction ends, and a read that waited
// sees the committed row. It must run inside a transaction; without one the lock ends with the
// statement.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionByIDForShare(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if sessionID == 0 {
		return nil, ErrChatAgentSessionNotFound
	}
	var session common_persistence_models.ChatAgentSession
	if err := h.getDBFromContext(ctx).Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", sessionID).Take(&session).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatAgentSessionNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_session_for_share"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return &session, nil
}

// getChatAgentSessionWhere runs the shared "fetch one session by predicate" query: same
// not-found / DB-error mapping and trace logging for every GetChatAgentSession* lookup. The
// query, args, log message, and log fields are the only things that vary between callers.
func (h *CommonPersistenceHandler[U]) getChatAgentSessionWhere(ctx context.Context, stage string, fields []zap.Field, query string, args ...any) (*common_persistence_models.ChatAgentSession, error) {
	db := h.getDBFromContext(ctx)
	var session common_persistence_models.ChatAgentSession
	if err := db.Where(query, args...).First(&session).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatAgentSessionNotFound
		}
		fields = append([]zap.Field{zap.String(logkeys.Stage, stage)}, fields...)
		return nil, chatPersistenceDatabaseError(err, fields...)
	}
	return &session, nil
}

// GetChatAgentSessionByScopedKey resolves the one default reusable session for a fully scoped
// GetOrCreateByContext key. Organization and all four legacy scope fields are required; the service must reject nil
// context fields before calling (NULL context values must not participate in broad matching).
// Ordinary sessions may carry the same context metadata with IsDefaultScoped=false; those are
// ignored here. Archived sessions are ignored so a stale canonical chat can be replaced without
// resurrecting history that the effective user left.
// The backing partial-unique index is idx_chat_agent_sessions_default_scoped_context_active_org_v2.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - organizationID: The positive owning organization identifier.
//   - agentType: The stable agent type string.
//   - userID: The effective chat user.
//   - contextID: The non-zero context identifier.
//   - contextEntityID: The non-zero context entity identifier.
//
// Returns:
//   - result1: The matching session, or nil.
//   - result2: ErrChatAgentSessionNotFound if absent, ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionByScopedKey(
	ctx context.Context,
	organizationID int64,
	agentType string,
	userID uint64,
	contextID uint64,
	contextEntityID uint64,
) (*common_persistence_models.ChatAgentSession, error) {
	if organizationID <= 0 || agentType == "" || userID == 0 || contextID == 0 || contextEntityID == 0 {
		// Incomplete scope must never match broadly; treat as "no scoped session".
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session_by_scoped_key",
		[]zap.Field{zap.Int64(appLogger.KeyOrganizationID, organizationID), zap.String(appLogger.KeyAgentType, agentType)},
		"organization_id = ? AND agent_type = ? AND user_id = ? AND context_id = ? AND context_entity_id = ? AND is_default_scoped = ? AND status = ?",
		organizationID,
		agentType,
		userID,
		contextID,
		contextEntityID,
		true,
		common_persistence_models.ChatAgentSessionStatusActive)
}

// ArchiveChatAgentSession marks an agent session as retired while preserving its channel/history.
// Archived scoped sessions are ignored by GetChatAgentSessionByScopedKey and by the scoped-context
// uniqueness index, allowing a fresh canonical session to be created for the same key. No run
// starts for an archived session, so, like disabling automatic runs, it posts the session's queued
// input as ordinary messages in the same transaction, under the admission locks; in a
// soft-deleted channel it discards the queue instead. The archive also clears parked_revision and
// deletes the receipts of the session's children's messages. Archiving the last member holding a
// WAKE_POLICY_WITH_SIBLINGS group open releases the group's deferred endings to the parent
// (releaseArchivedWakeGroupTx). It joins an ambient transaction.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - sessionID: The session identifier.
//
// Returns:
//   - result1: The messages the queued input became and the parent run the release minted, for
//     the caller to announce and start after commit.
//   - result2: nil on success; ErrChatAgentSessionNotFound if absent; ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ArchiveChatAgentSession(ctx context.Context, sessionID uint64) (ChatAgentSessionArchive, error) {
	return h.archiveChatAgentSession(ctx, sessionID, true)
}

// ArchiveReplacedChatAgentSession is ArchiveChatAgentSession for the child a replacement takes
// over in the same transaction. The replacement joins the old child's wake group after the archive,
// so the group stays open and the archive releases nothing.
func (h *CommonPersistenceHandler[U]) ArchiveReplacedChatAgentSession(ctx context.Context, sessionID uint64) (ChatAgentQueuedInputDelivery, error) {
	archive, err := h.archiveChatAgentSession(ctx, sessionID, false)
	return archive.ChatAgentQueuedInputDelivery, err
}

// ChatAgentSessionArchive is what archiving a session did.
type ChatAgentSessionArchive struct {
	// ChatAgentQueuedInputDelivery is the session's queued input the archive posted or discarded.
	ChatAgentQueuedInputDelivery
	// ParentWakeRun is the parent run minted when the archive released the archived child's wake
	// group, nil otherwise.
	ParentWakeRun *common_persistence_models.ChatAgentRun
}

func (h *CommonPersistenceHandler[U]) archiveChatAgentSession(ctx context.Context, sessionID uint64, releaseWakeGroup bool) (ChatAgentSessionArchive, error) {
	var archive ChatAgentSessionArchive
	if sessionID == 0 {
		return archive, ErrChatAgentSessionNotFound
	}
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.fenceContextualDelegationOrigin(txCtx); err != nil {
			return err
		}
		session, channelDeleted, err := lockChatAgentSessionByIDTx(tx, sessionID)
		if err != nil {
			return err
		}
		// A retired root is never resumed, so its park ends with it.
		if err := tx.Model(&common_persistence_models.ChatAgentSession{}).
			Where("id = ?", sessionID).
			Select("status", "parked_revision", "updated_at").
			Updates(struct {
				Status         common_persistence_models.ChatAgentSessionStatus `gorm:"column:status"`
				ParkedRevision *uint64                                          `gorm:"column:parked_revision"`
				UpdatedAt      time.Time                                        `gorm:"column:updated_at;autoUpdateTime:false"`
			}{
				Status:    common_persistence_models.ChatAgentSessionStatusArchived,
				UpdatedAt: time.Now(),
			}).Error; err != nil {
			return err
		}
		if err := deleteChildMessageReceiptsTx(tx, sessionID); err != nil {
			return err
		}
		if archive.ChatAgentQueuedInputDelivery, err = h.postChatAgentQueuedInputsTx(txCtx, tx, sessionID, channelDeleted); err != nil || !releaseWakeGroup {
			return err
		}
		archive.ParentWakeRun, err = h.releaseArchivedWakeGroupTx(txCtx, tx, session)
		return err
	})
	if err != nil && !errors.Is(err, ErrChatAgentSessionNotFound) {
		return ChatAgentSessionArchive{}, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "archive_chat_agent_session"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID))
	}
	return archive, err
}

// UpdateChatAgentSessionAutoRunPolicy updates the mutable follow-up-run policy on one session.
// getDBFromContext keeps the write inside the service's required ambient transaction. Disabling
// automatic runs also posts the session's queued input as ordinary messages: the application now
// starts every run itself, and those runs must read the input as ordinary context rather than wait
// for a delivery that never comes (in a soft-deleted channel the queue is discarded instead).
// Child-message receipts remain available for the live parent's send replays and discard history;
// only archiving the parent ends that receipt lifetime.
// Disabling therefore takes the admission locks (channel, then
// session) before it writes, and the caller must not hold the session row already.
//
// Returns the messages the queued input became, for the caller to announce after commit.
func (h *CommonPersistenceHandler[U]) UpdateChatAgentSessionAutoRunPolicy(
	ctx context.Context,
	sessionID uint64,
	policy common_persistence_models.ChatAgentSessionAutoRunPolicy,
) (ChatAgentQueuedInputDelivery, error) {
	var posted ChatAgentQueuedInputDelivery
	if sessionID == 0 {
		return posted, ErrChatAgentSessionNotFound
	}
	if policy != common_persistence_models.ChatAgentSessionAutoRunPolicyAutomatic &&
		policy != common_persistence_models.ChatAgentSessionAutoRunPolicyDisabled {
		return posted, appLogger.InvariantError("chat_agent_session_auto_run_policy_valid",
			errors.Join(ErrDatabaseError, errors.New("invalid chat agent session auto-run policy")),
			zap.String(logkeys.Stage, "update_chat_agent_session_auto_run_policy"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.String(logkeys.Expected, "automatic or disabled"),
			zap.Uint8(logkeys.Actual, uint8(policy)))
	}
	disabling := policy == common_persistence_models.ChatAgentSessionAutoRunPolicyDisabled
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var channelDeleted bool
		if disabling {
			var err error
			if _, channelDeleted, err = lockChatAgentSessionByIDTx(tx, sessionID); err != nil {
				return err
			}
		}
		result := tx.Model(&common_persistence_models.ChatAgentSession{}).
			Where("id = ?", sessionID).
			Select("auto_run_policy", "updated_at").
			Updates(struct {
				AutoRunPolicy common_persistence_models.ChatAgentSessionAutoRunPolicy `gorm:"column:auto_run_policy"`
				UpdatedAt     time.Time                                               `gorm:"column:updated_at;autoUpdateTime:false"`
			}{
				AutoRunPolicy: policy,
				UpdatedAt:     time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrChatAgentSessionNotFound
		}
		if !disabling {
			return nil
		}
		var err error
		posted, err = h.postChatAgentQueuedInputsTx(txCtx, tx, sessionID, channelDeleted)
		return err
	})
	if err != nil && !errors.Is(err, ErrChatAgentSessionNotFound) {
		return ChatAgentQueuedInputDelivery{}, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "update_chat_agent_session_auto_run_policy"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Uint8(appLogger.KeyAutoRunPolicy, uint8(policy)))
	}
	return posted, err
}

// GetChatAgentSessionByCreateIdempotencyKey resolves an idempotent session-create replay key for
// initial-message creates. The key is scoped to the organization, creator, effective chat user, and agent type so
// callers can safely reuse application-level request identifiers across other users or agents.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - organizationID: The positive owning organization identifier.
//   - createdByUserID: The actor that submitted the create request.
//   - userID: The effective chat user the session is for.
//   - agentType: The registered agent type string.
//   - key: The non-empty idempotency key.
//
// Returns:
//   - result1: The matching session, or nil.
//   - result2: ErrChatAgentSessionNotFound if absent, ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionByCreateIdempotencyKey(
	ctx context.Context,
	organizationID int64,
	createdByUserID uint64,
	userID uint64,
	agentType string,
	key string,
) (*common_persistence_models.ChatAgentSession, error) {
	if organizationID <= 0 || createdByUserID == 0 || userID == 0 || agentType == "" || key == "" {
		return nil, ErrChatAgentSessionNotFound
	}
	return h.getChatAgentSessionWhere(ctx, "get_chat_agent_session_by_create_idempotency_key",
		[]zap.Field{zap.Int64(appLogger.KeyOrganizationID, organizationID), zap.Uint64(appLogger.KeyCreatedByUserID, createdByUserID), zap.Uint64(appLogger.KeyUserID, userID), zap.String(appLogger.KeyAgentType, agentType)},
		"organization_id = ? AND created_by_user_id = ? AND user_id = ? AND agent_type = ? AND create_idempotency_key = ?",
		organizationID, createdByUserID, userID, agentType, key)
}

// SetChatAgentSessionInitialArtifacts records the initial message/run produced during an
// idempotent session create. It is called in the same outer transaction as channel/session creation,
// so replay metadata commits atomically with the artifacts it points at.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - sessionID: The created session.
//   - initialMessageID: The initial STANDARD user message.
//   - initialRunID: The first agent run created from the initial message.
//
// Returns:
//   - result: nil on success; ErrDatabaseError when the row cannot be updated.
func (h *CommonPersistenceHandler[U]) SetChatAgentSessionInitialArtifacts(
	ctx context.Context,
	sessionID uint64,
	initialMessageID uint64,
	initialRunID uint64,
) error {
	if sessionID == 0 || initialMessageID == 0 || initialRunID == 0 {
		return appLogger.InvariantError("chat_agent_session_initial_artifact_ids_present",
			errors.Join(ErrDatabaseError, errors.New("initial artifact ids must be non-zero")),
			zap.String(logkeys.Stage, "set_chat_agent_session_initial_artifacts"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Uint64(appLogger.KeyInitialMessageID, initialMessageID),
			zap.Uint64(appLogger.KeyInitialRunID, initialRunID),
			zap.String(logkeys.Expected, "all ids > 0"),
			zap.String(logkeys.Actual, "one or more zero ids"))
	}
	db := h.getDBFromContext(ctx)
	result := db.Model(&common_persistence_models.ChatAgentSession{}).
		Where("id = ?", sessionID).
		Select("initial_message_id", "initial_run_id", "updated_at").
		Updates(struct {
			InitialMessageID *uint64   `gorm:"column:initial_message_id"`
			InitialRunID     *uint64   `gorm:"column:initial_run_id"`
			UpdatedAt        time.Time `gorm:"column:updated_at;autoUpdateTime:false"`
		}{
			InitialMessageID: &initialMessageID,
			InitialRunID:     &initialRunID,
			UpdatedAt:        time.Now(),
		})
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "set_chat_agent_session_initial_artifacts"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Uint64(appLogger.KeyInitialMessageID, initialMessageID),
			zap.Uint64(appLogger.KeyInitialRunID, initialRunID))
	}
	if result.RowsAffected == 0 {
		return appLogger.InvariantError("chat_agent_session_exists_for_initial_artifacts", ErrDatabaseError,
			zap.String(logkeys.Stage, "set_chat_agent_session_initial_artifacts"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Uint64(appLogger.KeyInitialMessageID, initialMessageID),
			zap.Uint64(appLogger.KeyInitialRunID, initialRunID),
			zap.String(logkeys.Expected, "one updated agent session row"),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}
	return nil
}

// InsertChatAgentSessionConfigSnapshot writes an append-only config snapshot plus its optional
// normalized tool-config rows in one transaction. The (agent_session_id, version) composite
// unique index guards against duplicate versions.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - snapshot: The snapshot row to insert (AgentSessionID + Version must be set).
//   - toolConfigs: Optional normalized per-tool rows for the same version.
func (h *CommonPersistenceHandler[U]) InsertChatAgentSessionConfigSnapshot(
	ctx context.Context,
	snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot,
	toolConfigs []common_persistence_models.ChatAgentSessionToolConfig,
) error {
	if snapshot == nil {
		return appLogger.InvariantError("chat_agent_session_config_snapshot_present",
			errors.New("chat agent session config snapshot is required"),
			zap.String(logkeys.Stage, "insert_chat_agent_session_config_snapshot"),
			zap.String(logkeys.Expected, "non-nil snapshot"),
			zap.String(logkeys.Actual, "nil snapshot"))
	}
	// The column default exists only to backfill legacy rows; a writer must store a resolved mode.
	if snapshot.InputMode == proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_UNSPECIFIED {
		return appLogger.InvariantError("chat_agent_session_config_snapshot_input_mode_resolved",
			errors.New("chat agent session config snapshot input mode is unresolved"),
			zap.String(logkeys.Stage, "insert_chat_agent_session_config_snapshot"),
			zap.Uint64(appLogger.KeyAgentSessionID, snapshot.AgentSessionID),
			zap.String(logkeys.Expected, "resolved input mode"),
			zap.Stringer(logkeys.Actual, snapshot.InputMode))
	}
	db := h.getDBFromContext(ctx)
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(snapshot).Error; err != nil {
			return err
		}
		if len(toolConfigs) > 0 {
			for i := range toolConfigs {
				toolConfigs[i].AgentSessionID = snapshot.AgentSessionID
				toolConfigs[i].Version = snapshot.Version
			}
			if err := tx.Create(&toolConfigs).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "insert_chat_agent_session_config_snapshot"),
			zap.Uint64(appLogger.KeyAgentSessionID, snapshot.AgentSessionID),
			zap.Uint64(appLogger.KeyVersion, snapshot.Version),
			zap.Int(appLogger.KeyToolConfigCount, len(toolConfigs)))
	}
	return nil
}

// BumpChatAgentSessionActiveConfigVersion atomically advances a session's active config version
// using optimistic concurrency: it only updates when the current active version still equals
// expectedCurrentVersion. A RowsAffected of 0 means either the session does not exist or another
// writer advanced the version first — surfaced as ErrChatAgentSessionVersionConflict so the
// service can return a 409-style conflict rather than silently overwriting.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - sessionID: The session identifier.
//   - expectedCurrentVersion: The version the caller believes is currently active.
//   - newVersion: The version to set (normally expectedCurrentVersion+1).
//
// Returns:
//   - result: nil on success; ErrChatAgentSessionVersionConflict on stale/missing; ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) BumpChatAgentSessionActiveConfigVersion(
	ctx context.Context,
	sessionID uint64,
	expectedCurrentVersion uint64,
	newVersion uint64,
) error {
	db := h.getDBFromContext(ctx)
	result := db.Model(&common_persistence_models.ChatAgentSession{}).
		Where("id = ? AND active_config_version = ?", sessionID, expectedCurrentVersion).
		Select("active_config_version", "updated_at").
		Updates(struct {
			ActiveConfigVersion uint64    `gorm:"column:active_config_version"`
			UpdatedAt           time.Time `gorm:"column:updated_at;autoUpdateTime:false"`
		}{
			ActiveConfigVersion: newVersion,
			UpdatedAt:           time.Now(),
		})
	if result.Error != nil {
		return chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "bump_chat_agent_session_config_version"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Uint64(appLogger.KeyExpectedConfigVersion, expectedCurrentVersion),
			zap.Uint64(appLogger.KeyNewConfigVersion, newVersion))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent session config compare-and-set did not match",
				zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
				zap.Uint64(appLogger.KeyExpectedConfigVersion, expectedCurrentVersion),
				zap.Uint64(appLogger.KeyNewConfigVersion, newVersion))
		}
		return ErrChatAgentSessionVersionConflict
	}
	return nil
}

// GetChatAgentSessionConfigSnapshot fetches a specific config version snapshot plus its
// normalized tool-config rows.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - sessionID: The session identifier.
//   - version: The config version to fetch.
//
// Returns:
//   - result1: The snapshot row, or nil.
//   - result2: The normalized tool-config rows for that version (possibly empty).
//   - result3: ErrChatAgentSessionConfigNotFound if absent, ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatAgentSessionConfigSnapshot(
	ctx context.Context,
	sessionID uint64,
	version uint64,
) (*common_persistence_models.ChatAgentSessionConfigSnapshot, []common_persistence_models.ChatAgentSessionToolConfig, error) {
	db := h.getDBFromContext(ctx)
	var snapshot common_persistence_models.ChatAgentSessionConfigSnapshot
	if err := db.Where("agent_session_id = ? AND version = ?", sessionID, version).First(&snapshot).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrChatAgentSessionConfigNotFound
		}
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_session_config_snapshot"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID), zap.Uint64(appLogger.KeyVersion, version))
	}
	var tools []common_persistence_models.ChatAgentSessionToolConfig
	if err := db.Where("agent_session_id = ? AND version = ?", sessionID, version).Order("tool_name ASC").Find(&tools).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_agent_session_tool_configs"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID), zap.Uint64(appLogger.KeyVersion, version))
	}
	return &snapshot, tools, nil
}

// GetChatAgentInteractionPolicy reads only the interaction policy of one config version. It is the
// send path's policy lookup and loads neither the snapshot payloads nor the normalized tool rows.
//
// Returns ErrChatAgentSessionConfigNotFound when the version does not exist.
func (h *CommonPersistenceHandler[U]) GetChatAgentInteractionPolicy(
	ctx context.Context,
	sessionID uint64,
	version uint64,
) (common_persistence_models.ChatAgentInteractionPolicy, error) {
	policy, err := chatAgentInteractionPolicyTx(h.getDBFromContext(ctx), sessionID, version)
	if err != nil && !errors.Is(err, ErrChatAgentSessionConfigNotFound) {
		return policy, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_interaction_policy"),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID), zap.Uint64(appLogger.KeyVersion, version))
	}
	return policy, err
}

// chatAgentInteractionPolicyTx reads the interaction policy of one config version on db, returning
// ErrChatAgentSessionConfigNotFound when the version does not exist.
func chatAgentInteractionPolicyTx(db *gorm.DB, sessionID, version uint64) (common_persistence_models.ChatAgentInteractionPolicy, error) {
	var policy common_persistence_models.ChatAgentInteractionPolicy
	err := db.Model(&common_persistence_models.ChatAgentSessionConfigSnapshot{}).
		Select("input_mode", "allow_user_stop").
		Where("agent_session_id = ? AND version = ?", sessionID, version).
		Take(&policy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return policy, ErrChatAgentSessionConfigNotFound
	}
	return policy, err
}

// ============================================================================
// CHAT: AGENT RUNS
// ============================================================================

// CreateChatAgentRun inserts a sessionless run. Session-bound runs must use
// CreateChatAgentRunCapturingConfig to validate their location and capture configuration atomically.
// A zero AgentSessionID is stored as NULL, so sessionless runs never share a session key.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - run: The run row to insert. Its ID is populated on success; a zero AgentSessionID becomes nil.
func (h *CommonPersistenceHandler[U]) CreateChatAgentRun(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	if run == nil {
		return appLogger.InvariantError("chat_agent_run_create_input_present",
			errors.New("chat agent run is required"),
			zap.String(logkeys.Stage, "create_chat_agent_run"),
			zap.String(logkeys.Expected, "non-nil run"),
			zap.String(logkeys.Actual, "nil run"))
	}
	// Session-bound runs must atomically capture their configuration and validate location.
	if run.AgentSessionID != nil {
		if *run.AgentSessionID != 0 {
			return ErrChatAgentSessionScopeInvalid
		}
		run.AgentSessionID = nil
	}
	if run.OrganizationID <= 0 {
		return appLogger.InvariantError("chat_agent_run_has_organization",
			errors.New("chat agent run organization_id must be positive"),
			zap.String(logkeys.Stage, "create_chat_agent_run"),
			zap.Uint64(appLogger.KeyChannelID, run.ChannelID),
			zap.String(logkeys.Expected, "organization_id > 0"),
			zap.Int64(logkeys.Actual, run.OrganizationID))
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now()
	}
	db := h.getDBFromContext(ctx)
	if err := db.Create(run).Error; err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_chat_agent_run"),
			zap.Int64(appLogger.KeyOrganizationID, run.OrganizationID),
			zap.Uint64(appLogger.KeyChannelID, run.ChannelID))
	}
	return nil
}

// CreateChatAgentRunCapturingConfig inserts a run row that belongs to an agent session, capturing
// the session's current active config version atomically in the same transaction as the insert.
// This guarantees the run pins the snapshot that was active at run start even if a concurrent
// config update lands a moment later (Decision 0/6 approval-resume determinism).
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - run: The run row to insert; AgentSessionID must be set. AgentSessionConfigVersion is
//     overwritten with the captured active version.
//
// Returns:
//   - result: ErrChatAgentSessionNotFound if the session is missing, ErrChatAgentSessionRunActive
//     if the run is active and the session already has a RUNNING, WAITING_FOR_APPROVAL or
//     RETRYABLE run, ErrChatAgentSessionSuspended if the run is active and a person's Stop left
//     the session suspended (only Resume starts its next run), ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) CreateChatAgentRunCapturingConfig(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	return h.createChatAgentRunCapturingConfig(ctx, run, true)
}

// CreateChatAgentRunForSend is CreateChatAgentRunCapturingConfig for a send that already holds the
// channel from LockChatChannelForSend and the session from LockChatAgentSessionsForSend and has
// written its message, in the send's transaction. It does not lock the channel FOR UPDATE: another
// transaction's insert may hold FOR KEY SHARE on the channel row by now, and it would wait on the
// send at its own last-message update.
func (h *CommonPersistenceHandler[U]) CreateChatAgentRunForSend(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return ErrChatAgentSessionTransactionRequired
	}
	return h.createChatAgentRunCapturingConfig(ctx, run, false)
}

// createChatAgentRunCapturingConfig inserts a session run under its location's channel lock,
// taken here unless the caller already holds it (lockChannel false).
func (h *CommonPersistenceHandler[U]) createChatAgentRunCapturingConfig(ctx context.Context, run *common_persistence_models.ChatAgentRun, lockChannel bool) error {
	if run == nil {
		return appLogger.InvariantError("chat_agent_run_create_input_present",
			errors.New("chat agent run is required"),
			zap.String(logkeys.Stage, "create_chat_agent_run_capturing_config"),
			zap.String(logkeys.Expected, "non-nil run"),
			zap.String(logkeys.Actual, "nil run"))
	}
	if run.AgentSessionID == nil || *run.AgentSessionID == 0 {
		return appLogger.InvariantError("chat_agent_run_has_agent_session",
			errors.New("agent session id is required to capture config version"),
			zap.String(logkeys.Stage, "create_chat_agent_run_capturing_config"),
			zap.Uint64(appLogger.KeyChannelID, run.ChannelID),
			zap.String(logkeys.Expected, "agent_session_id > 0"),
			zap.String(logkeys.Actual, "missing or zero agent_session_id"))
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now()
	}
	err := h.withChatLocationTransaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		channel := &common_persistence_models.ChatChannel{}
		var err error
		if lockChannel {
			channel, err = h.lockChatLocationChannel(txCtx, run.ChannelID)
		} else if err = tx.Where("id = ?", run.ChannelID).First(channel).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			err = ErrChatChannelNotFound
		}
		if err != nil {
			return err
		}

		var session common_persistence_models.ChatAgentSession
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", *run.AgentSessionID).First(&session).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatAgentSessionNotFound
			}
			return err
		}
		if run.ChannelID != session.ChannelID || run.ParentMessageID != session.ParentMessageID || run.BotUserID != session.BotUserID ||
			(run.OrganizationID != 0 && run.OrganizationID != session.OrganizationID) ||
			!session.IsLive() {
			return ErrChatAgentSessionScopeInvalid
		}
		captured := session.ActiveConfigVersion
		run.AgentSessionConfigVersion = &captured
		if session.OrganizationID <= 0 {
			return appLogger.InvariantError(
				"chat_agent_session_has_organization",
				errors.New("agent session organization_id must be positive"),
				zap.String(logkeys.Stage, "capture_chat_agent_session_config"),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
				zap.String(logkeys.Expected, "organization_id > 0"),
				zap.Int64(logkeys.Actual, session.OrganizationID),
			)
		}
		if err := h.validateAgentSessionLocation(txCtx, channel, &session); err != nil {
			return err
		}
		if run.TriggeringMessageID != nil && *run.TriggeringMessageID != 0 {
			var trigger common_persistence_models.ChatMessage
			// A broadcast thread reply is also a main-timeline message, so it may trigger the
			// main session's run; it never triggers a session in a different thread.
			err := tx.Where("id = ? AND channel_id = ? AND (COALESCE(parent_message_id, 0) = ? OR (? = 0 AND broadcast_to_channel))",
				*run.TriggeringMessageID, session.ChannelID, session.ParentMessageID, session.ParentMessageID).First(&trigger).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrChatAgentSessionScopeInvalid
			}
			if err != nil {
				return err
			}
		}
		// The channel and session locks serialize this check against the session's other run
		// starts, so the one-active-run index never aborts the caller's transaction.
		if slices.Contains(ActiveChatAgentRunStatuses(), run.Status) {
			if session.IsSuspended() {
				return ErrChatAgentSessionSuspended
			}
			active, err := sessionHasActiveChatAgentRunTx(tx, session.ID, 0)
			if err != nil {
				return err
			}
			if active {
				return ErrChatAgentSessionRunActive
			}
		}
		run.OrganizationID = session.OrganizationID
		return tx.Create(run).Error
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentSessionNotFound) || errors.Is(err, ErrChatAgentSessionScopeInvalid) ||
			errors.Is(err, ErrChatAgentSessionRunActive) || errors.Is(err, ErrChatAgentSessionSuspended) {
			return err
		}
		if appLogger.IsInvariantError(err) {
			return err
		}
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_chat_agent_run_capturing_config"),
			zap.Uint64(appLogger.KeyChannelID, run.ChannelID),
			zap.Uint64(appLogger.KeyAgentSessionID, *run.AgentSessionID))
	}
	return nil
}

// GetChatAgentRunByID fetches a run by its primary key.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - runID: The run identifier.
//
// Returns:
//   - result1: The run row, or nil.
//   - result2: ErrChatAgentRunNotFound if absent, ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatAgentRunByID(ctx context.Context, runID uint64) (*common_persistence_models.ChatAgentRun, error) {
	if runID == 0 {
		return nil, ErrChatAgentRunNotFound
	}
	db := h.getDBFromContext(ctx)
	var run common_persistence_models.ChatAgentRun
	if err := db.Where("id = ?", runID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatAgentRunNotFound
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return &run, nil
}

// CompareAndSetChatAgentRunStatus conditionally transitions a run's status, implementing the
// Decision 6 run-level state machine without races. It only updates when the current status is
// in fromStatuses (empty fromStatuses means "any current status"). When the target status is
// terminal (COMPLETED/FAILED/CANCELLED) and completed_at is unset, it is stamped to now.
//
// RowsAffected of 0 means the run was not in an expected source state — the caller treats this
// as a no-op/conflict rather than an error, so concurrent transitions stay idempotent-safe.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - runID: The run identifier.
//   - fromStatuses: Allowed current statuses; empty means unconditional.
//   - toStatus: The status to set.
//   - reason: The structured cause recorded with the transition. It replaces any earlier reason,
//     so a transition back to RUNNING passes UNSPECIFIED.
//
// Returns:
//   - result1: true if the row transitioned; false if no row matched.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) CompareAndSetChatAgentRunStatus(
	ctx context.Context,
	runID uint64,
	fromStatuses []proto_models.ChatAgentRunStatus,
	toStatus proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
) (bool, error) {
	db := h.getDBFromContext(ctx)
	now := time.Now()
	updates := struct {
		Status           proto_models.ChatAgentRunStatus
		StatusReason     proto_models.ChatAgentRunStatusReason
		StatusReasonCode *string
		CompletedAt      clause.Expr `gorm:"type:timestamptz"`
		UpdatedAt        time.Time   `gorm:"autoUpdateTime:false"`
	}{Status: toStatus, StatusReason: reason, UpdatedAt: now}
	columns := []string{"status", "status_reason", "status_reason_code", "updated_at"}
	terminal := IsTerminalChatAgentRunStatus(toStatus)
	if terminal {
		// Stamp completed_at only if not already set, so a re-transition does not move it.
		updates.CompletedAt = gorm.Expr("COALESCE(completed_at, ?)", now)
		columns = append(columns, "completed_at")
	}
	lease, leased := common_context.GetChatAgentRunDriveLease(ctx)
	if leased && lease.RunID != runID {
		return false, ErrChatAgentRunDriveLeaseLost
	}
	apply := func(db *gorm.DB) (int64, error) {
		query := db.Model(&common_persistence_models.ChatAgentRun{}).Where("id = ?", runID)
		if leased {
			// Bind runtime-owned status transitions to the same live token as transcript writes. A
			// canceled/expired worker must not complete or fail a run after another replica took over.
			query = query.
				Where("drive_lease_owner = ?", lease.Owner).
				Where("drive_lease_expires_at > CURRENT_TIMESTAMP")
		}
		if len(fromStatuses) > 0 {
			query = query.Where("status IN ?", fromStatuses)
		}
		result := query.Select(columns).Updates(updates)
		return result.RowsAffected, result.Error
	}
	var affected int64
	var err error
	if terminal {
		// A terminal target settles delegation in the same transaction as the transition: the
		// conditional UPDATE holds the run row lock, then the settlement takes the session row.
		err = h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
			var txErr error
			affected, txErr = apply(tx)
			if txErr != nil || affected == 0 {
				return txErr
			}
			return h.settleAgentRunTerminalTx(txCtx, tx, runID)
		})
	} else {
		affected, err = apply(db)
	}
	if err != nil {
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "compare_and_set_chat_agent_run_status"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Stringer(appLogger.KeyTargetStatus, toStatus),
			zap.Int(appLogger.KeySourceStatusCount, len(fromStatuses)))
	}
	transitioned := affected > 0
	if !transitioned {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run status compare-and-set did not match",
				zap.Uint64(appLogger.KeyAgentRunID, runID),
				zap.Stringer(appLogger.KeyTargetStatus, toStatus))
		}
	}
	return transitioned, nil
}

const cancelledAgentRunToolError = "agent run cancelled"

// CancelChatAgentRun atomically terminalizes a cancellable run and every unresolved TOOL_CALL it
// owns. Pending approvals become EXPIRED; pending/executing work becomes EXECUTION_FAILED and loses
// all lease/retry authority. Assistant streams still STREAMING become FAILED, since the drive loses
// the lease it would finish them under. Locking the run first serializes this control-plane
// transition with approval resolution and claimed-result commit, which use the same run-first
// ordering.
//
// CommonPersistenceInterface requires this atomic operation so cancellation never skips
// outstanding-tool cleanup for custom persistence implementations.
//
// reason and reasonCode are recorded on the run with the transition. reasonCode is the caller's
// already-validated identifier-like code; empty stores NULL. It returns what the cancellation
// ended, for the caller's post-commit events, or nil with no error when the run is missing or not
// in fromStatuses.
func (h *CommonPersistenceHandler[U]) CancelChatAgentRun(
	ctx context.Context,
	runID uint64,
	fromStatuses []proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
	reasonCode string,
) (*ChatAgentRunCancellation, error) {
	if runID == 0 {
		return nil, ErrChatAgentRunNotFound
	}
	var cancellation *ChatAgentRunCancellation
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var err error
		cancellation, err = h.cancelChatAgentRunTx(txCtx, tx, runID, fromStatuses, reason, reasonCode)
		return err
	})
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "cancel_chat_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Int(appLogger.KeySourceStatusCount, len(fromStatuses)))
	}
	if cancellation == nil {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run cancellation did not match a cancellable run",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
	}
	return cancellation, nil
}

// ChatAgentRunCancellation is what cancelling a run ended together with it, for the events its
// caller publishes after commit.
type ChatAgentRunCancellation struct {
	// Run is the cancelled run as the cancellation left it.
	Run *common_persistence_models.ChatAgentRun
	// FailedStreams are the run's assistant streams moved from STREAMING to FAILED, read with
	// their id, channel, thread and visibility only.
	FailedStreams []*common_persistence_models.ChatMessage
	// SettledToolCalls are the run's TOOL_CALL rows whose approval expired or whose execution
	// failed, with their tool-call rows, read with their id, channel, thread and visibility only.
	SettledToolCalls []*common_persistence_models.ChatMessage
}

// cancelChatAgentRunTx is CancelChatAgentRun inside the caller's transaction: it locks the run,
// then its channel, writes the CANCELLED transition, fails its outstanding tool work and dangling
// streams, and settles the ending. It returns nil with no error when the run is missing or not in
// fromStatuses.
func (h *CommonPersistenceHandler[U]) cancelChatAgentRunTx(
	ctx context.Context,
	tx *gorm.DB,
	runID uint64,
	fromStatuses []proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
	reasonCode string,
) (*ChatAgentRunCancellation, error) {
	now := time.Now()
	if err := h.fenceContextualDelegationOrigin(ctx); err != nil {
		return nil, err
	}
	run := &common_persistence_models.ChatAgentRun{}
	query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "channel_id", "status").
		Where("id = ?", runID)
	if len(fromStatuses) > 0 {
		query = query.Where("status IN ?", fromStatuses)
	}
	if err := query.Take(run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if err := lockChatAgentRunChannelTx(tx, run.ChannelID); err != nil {
		return nil, err
	}
	// A run waiting for approval has pending approvals, which the cancellation expires.
	expiresApprovals := run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL

	// RETURNING reads the run back as cancelled, for the caller's events.
	runUpdate := tx.Model(run).Clauses(clause.Returning{})
	if len(fromStatuses) > 0 {
		runUpdate = runUpdate.Where("status IN ?", fromStatuses)
	}
	var storedReasonCode *string
	if reasonCode != "" {
		storedReasonCode = &reasonCode
	}
	result := runUpdate.Select("status", "status_reason", "status_reason_code", "completed_at", "updated_at").Updates(struct {
		Status           proto_models.ChatAgentRunStatus       `gorm:"column:status"`
		StatusReason     proto_models.ChatAgentRunStatusReason `gorm:"column:status_reason"`
		StatusReasonCode *string                               `gorm:"column:status_reason_code"`
		CompletedAt      clause.Expr                           `gorm:"column:completed_at;type:timestamptz"`
		UpdatedAt        time.Time                             `gorm:"column:updated_at;autoUpdateTime:false"`
	}{
		Status:           proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED,
		StatusReason:     reason,
		StatusReasonCode: storedReasonCode,
		CompletedAt:      gorm.Expr("COALESCE(completed_at, ?)", now),
		UpdatedAt:        now,
	})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, nil
	}

	pendingApproval := proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING
	var settled []common_persistence_models.ChatMessageToolCall
	result = tx.Model(&settled).
		Clauses(clause.Returning{Columns: []clause.Column{{Name: "message_id"}}}).
		Where("agent_run_id = ? AND kind = ?", runID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where("execution_state NOT IN ?", []proto_models.ToolCallExecutionState{
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
		}).
		Select("approval_status", "resolved_at", "resolution_reason", "execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "last_execution_error", "updated_at").
		Updates(struct {
			ApprovalStatus          clause.Expr                         `gorm:"column:approval_status;type:smallint"`
			ResolvedAt              clause.Expr                         `gorm:"column:resolved_at;type:timestamptz"`
			ResolutionReason        clause.Expr                         `gorm:"column:resolution_reason;type:text"`
			ExecutionState          proto_models.ToolCallExecutionState `gorm:"column:execution_state"`
			ExecutionLeaseOwner     *string                             `gorm:"column:execution_lease_owner"`
			ExecutionLeaseExpiresAt *time.Time                          `gorm:"column:execution_lease_expires_at"`
			ExecutionNotBefore      *time.Time                          `gorm:"column:execution_not_before"`
			LastExecutionError      string                              `gorm:"column:last_execution_error"`
			UpdatedAt               time.Time                           `gorm:"column:updated_at;autoUpdateTime:false"`
		}{
			ApprovalStatus: gorm.Expr(
				"CASE WHEN approval_status = ? THEN ? ELSE approval_status END",
				pendingApproval,
				proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
			),
			ResolvedAt: gorm.Expr(
				"CASE WHEN approval_status = ? THEN COALESCE(resolved_at, ?) ELSE resolved_at END",
				pendingApproval, now,
			),
			ResolutionReason: gorm.Expr(
				"CASE WHEN approval_status = ? THEN COALESCE(resolution_reason, ?) ELSE resolution_reason END",
				pendingApproval, cancelledAgentRunToolError,
			),
			ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
			ExecutionLeaseOwner:     nil,
			ExecutionLeaseExpiresAt: nil,
			ExecutionNotBefore:      nil,
			LastExecutionError:      cancelledAgentRunToolError,
			UpdatedAt:               now,
		})
	if result.Error != nil {
		return nil, result.Error
	}
	cancellation := &ChatAgentRunCancellation{Run: run}
	if len(settled) > 0 {
		settledIDs := make([]uint64, len(settled))
		for i := range settled {
			settledIDs[i] = settled[i].MessageID
		}
		if err := tx.Select("id", "channel_id", "parent_message_id", "hidden_from_users").Preload("ToolCall").
			Where("id IN ?", settledIDs).Order("id").Find(&cancellation.SettledToolCalls).Error; err != nil {
			return nil, err
		}
	}
	var err error
	if cancellation.FailedStreams, err = failChatAgentRunStreamsTx(tx, runID); err != nil {
		return nil, err
	}
	// A cancelled delegated child announces the stop (and wakes its parent unless the parent
	// asked for it); a cancelled parent run still owes any wake it deferred.
	if err := h.settleAgentRunTerminalTx(ctx, tx, runID); err != nil {
		return nil, err
	}
	// Expired approvals are a graph event, like any other approval expiry. Last write: the root
	// lock follows the run, channel, and session locks above. A retired root's park is already
	// cleared, so its revision advances without resuming it.
	if expiresApprovals && cancellation.Run.AgentSessionID != nil {
		if err := h.advanceChatAgentGraphResumeRevisionTx(ctx, tx, []uint64{*cancellation.Run.AgentSessionID}, proto_models.GraphEventKind_GRAPH_EVENT_KIND_APPROVAL); err != nil {
			return nil, err
		}
	}
	return cancellation, nil
}

// LockChatAgentRunForUpdate takes a row-level lock on one run for the lifetime of the transaction
// carried by ctx. Tool-step finalizers use the run as their serialization point so two parallel
// resolutions cannot both observe the other sibling as unresolved and then both defer forever.
func (h *CommonPersistenceHandler[U]) LockChatAgentRunForUpdate(ctx context.Context, runID uint64) error {
	if runID == 0 {
		return ErrChatAgentRunNotFound
	}
	var run common_persistence_models.ChatAgentRun
	err := h.getDBFromContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id").
		Where("id = ?", runID).
		Take(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrChatAgentRunNotFound
		}
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "lock_chat_agent_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return nil
}

// ReserveChatAgentRunBudget atomically charges generation and tool-call work when both resulting
// counters remain within their ceilings and the run's graph budget pool, if any, has room. A false
// result means no work was reserved; a refusal by the pool is a *GraphBudgetExhaustedError.
func (h *CommonPersistenceHandler[U]) ReserveChatAgentRunBudget(
	ctx context.Context,
	runID uint64,
	generationDelta uint64,
	toolCallDelta uint64,
	maxGenerations uint64,
	maxToolCalls uint64,
) (bool, error) {
	reserved := false
	err := h.transaction(ctx, func(_ context.Context, tx *gorm.DB) error {
		var err error
		reserved, err = reserveChatAgentRunBudgetTx(tx, runID, generationDelta, toolCallDelta, maxGenerations, maxToolCalls)
		return err
	})
	if errors.Is(err, ErrGraphBudgetExhausted) {
		h.recordGraphBudgetRefusal(err)
		return false, err
	}
	if err != nil {
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "reserve_chat_agent_run_budget"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint64(appLogger.KeyGenerationDelta, generationDelta),
			zap.Uint64(appLogger.KeyToolCallDelta, toolCallDelta))
	}
	if !reserved {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run budget reservation did not match",
				zap.Uint64(appLogger.KeyAgentRunID, runID),
				zap.Uint64(appLogger.KeyGenerationDelta, generationDelta),
				zap.Uint64(appLogger.KeyToolCallDelta, toolCallDelta))
		}
	}
	return reserved, nil
}

// reserveChatAgentRunBudgetTx is ReserveChatAgentRunBudget's conditional charge in tx, without
// its error mapping: the run's own charge, then its graph pool's. A pool refusal returns a
// *GraphBudgetExhaustedError, and the caller rolls tx back so the run's charge is not kept either.
func reserveChatAgentRunBudgetTx(tx *gorm.DB, runID, generationDelta, toolCallDelta, maxGenerations, maxToolCalls uint64) (bool, error) {
	if runID == 0 || (generationDelta == 0 && toolCallDelta == 0) {
		return false, nil
	}
	if generationDelta > maxGenerations || toolCallDelta > maxToolCalls {
		return false, nil
	}
	updates := struct {
		GenerationCount clause.Expr `gorm:"type:bigint"`
		ToolCallCount   clause.Expr `gorm:"type:bigint"`
		UpdatedAt       time.Time   `gorm:"autoUpdateTime:false"`
	}{UpdatedAt: time.Now()}
	columns := []string{"updated_at"}
	if generationDelta > 0 {
		updates.GenerationCount = gorm.Expr("generation_count + ?", generationDelta)
		columns = append(columns, "generation_count")
	}
	if toolCallDelta > 0 {
		updates.ToolCallCount = gorm.Expr("tool_call_count + ?", toolCallDelta)
		columns = append(columns, "tool_call_count")
	}
	result := tx.Model(&common_persistence_models.ChatAgentRun{}).
		Where("id = ? AND status IN ?", runID, DrivableChatAgentRunStatuses()).
		Where("generation_count <= ?", maxGenerations-generationDelta).
		Where("tool_call_count <= ?", maxToolCalls-toolCallDelta).
		Select(columns).
		Updates(updates)
	if result.Error != nil || result.RowsAffected == 0 {
		return false, result.Error
	}
	if err := chargeChatAgentGraphBudgetTx(tx, runID, generationDelta, toolCallDelta); err != nil {
		return false, err
	}
	return true, nil
}

// AddChatAgentRunActiveDuration adds elapsed active processing time using millisecond precision,
// to the run and, in the same transaction, to its graph's budget pool.
func (h *CommonPersistenceHandler[U]) AddChatAgentRunActiveDuration(ctx context.Context, runID uint64, elapsed time.Duration) error {
	if runID == 0 || elapsed <= 0 {
		return nil
	}
	elapsedMs := activeDurationMillis(elapsed)
	var rows int64
	err := h.transaction(ctx, func(_ context.Context, tx *gorm.DB) error {
		result := tx.Model(&common_persistence_models.ChatAgentRun{}).
			Where("id = ?", runID).
			Select("active_duration_ms", "updated_at").
			Updates(struct {
				ActiveDurationMs clause.Expr `gorm:"column:active_duration_ms;type:bigint"`
				UpdatedAt        time.Time   `gorm:"column:updated_at;autoUpdateTime:false"`
			}{
				ActiveDurationMs: gorm.Expr("active_duration_ms + ?", elapsedMs),
				UpdatedAt:        time.Now(),
			})
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		rows = result.RowsAffected
		return addChatAgentGraphBudgetActiveTx(tx, runID, elapsedMs)
	})
	if err != nil {
		return chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "add_chat_agent_run_active_duration"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			appLogger.DurationMS(appLogger.KeyElapsedMS, elapsed))
	}
	if rows == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run active duration update found no run",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
	}
	return nil
}

// IsTerminalChatAgentRunStatus reports whether a run status is terminal (sets completed_at).
func IsTerminalChatAgentRunStatus(status proto_models.ChatAgentRunStatus) bool {
	switch status {
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED:
		return true
	default:
		return false
	}
}

// FinalizeChatAgentSessionPrompt finishes a newly inserted session inside the caller's
// creation transaction. The predicate prevents overwriting a finalized prompt.
func (h *CommonPersistenceHandler[U]) FinalizeChatAgentSessionPrompt(ctx context.Context, sessionID uint64, prompt string, customization []byte) error {
	if strings.TrimSpace(prompt) == "" {
		return ErrChatAgentSessionPromptInvalid
	}
	result := h.DB(ctx).Model(&common_persistence_models.ChatAgentSession{}).
		Where("id = ? AND effective_system_prompt = ''", sessionID).
		Select("effective_system_prompt", "prompt_customization_json").
		Updates(common_persistence_models.ChatAgentSession{EffectiveSystemPrompt: prompt, PromptCustomizationJSON: customization})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrChatAgentSessionPromptFinalized
	}
	return nil
}
