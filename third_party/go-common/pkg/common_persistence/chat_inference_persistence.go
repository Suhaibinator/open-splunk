package common_persistence

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MessageInferenceAggregate is the persistence-layer aggregate of all inference-call rows that
// link to one chat message. It is derived data (the service maps it to the proto
// MessageInferenceSummary); usage is never stored as sparse columns on ChatMessage.
type MessageInferenceAggregate struct {
	MessageID           uint64
	ChannelID           uint64
	CallCount           int64
	InputTokens         int64
	OutputTokens        int64
	TotalTokens         int64
	ThoughtsTokens      int64
	CachedContentTokens int64
	// ProviderID/model/finish describe the most recent linked call (for the badge label). They are
	// populated from the latest call_id ordered by id; zero/empty when no rows exist.
	ProviderID      string
	ProviderModelID string
	FinishReason    string
	LatencyMs       int64
}

// InferenceUsageBreakdown is a grouped usage aggregate over the ledger. The grouping dimension
// (run / purpose / provider / model) is identified by whichever pointer fields are set; the
// service maps this to proto AgentSessionUsageBreakdown.
type InferenceUsageBreakdown struct {
	// AgentSessionID is set for the InferenceBreakdownBySession axis (a delegation parent and
	// its children); the other axes leave it nil.
	AgentSessionID      *uint64
	AgentRunID          *uint64
	Purpose             *proto_models.InferenceCallPurpose
	ProviderID          *string
	ProviderModelID     *string
	CallCount           int64
	InputTokens         int64
	OutputTokens        int64
	TotalTokens         int64
	ThoughtsTokens      int64
	CachedContentTokens int64
}

// RecordChatInferenceCall appends a ledger row idempotently and, only when a new row was
// actually inserted, increments the per-channel totals cache — all in one transaction
// (Decision 2.5). The idempotency key is call_id: a retry with the same call_id is a no-op for
// both the ledger and the totals, so stream-finish retries / post-commit hook retries never
// double-count.
//
// Returns inserted=false when the call_id already existed (duplicate), inserted=true when a new
// ledger row + totals bump were committed.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - call: The ledger row to record. CallID and ChannelID must be set. ID is populated on a
//     real insert.
//
// Returns:
//   - result1: true if a new row was inserted (and totals bumped); false on duplicate.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) RecordChatInferenceCall(ctx context.Context, call *common_persistence_models.ChatInferenceCall) (bool, error) {
	if call == nil {
		return false, appLogger.InvariantError("chat_inference_call_present",
			errors.New("chat inference call is required"),
			zap.String(logkeys.Stage, "record_chat_inference_call"),
			zap.String(logkeys.Expected, "non-nil inference call"),
			zap.String(logkeys.Actual, "nil inference call"))
	}
	if call.CallID == "" {
		return false, appLogger.InvariantError("chat_inference_call_has_call_id",
			errors.New("chat inference call call_id is required"),
			zap.String(logkeys.Stage, "record_chat_inference_call"),
			zap.Uint64(appLogger.KeyChannelID, call.ChannelID),
			zap.String(logkeys.Expected, "non-empty call_id"),
			zap.String(logkeys.Actual, "empty call_id"))
	}
	if call.ChannelID == 0 {
		return false, appLogger.InvariantError("chat_inference_call_has_channel",
			errors.New("chat inference call channel_id is required"),
			zap.String(logkeys.Stage, "record_chat_inference_call"),
			zap.String(appLogger.KeyCallID, call.CallID),
			zap.String(logkeys.Expected, "channel_id > 0"),
			zap.Uint64(logkeys.Actual, call.ChannelID))
	}
	if call.OrganizationID <= 0 {
		return false, appLogger.InvariantError("chat_inference_call_has_organization",
			errors.New("chat inference call organization_id must be positive"),
			zap.String(logkeys.Stage, "record_chat_inference_call"),
			zap.String(appLogger.KeyCallID, call.CallID),
			zap.Uint64(appLogger.KeyChannelID, call.ChannelID),
			zap.String(logkeys.Expected, "organization_id > 0"),
			zap.Int64(logkeys.Actual, call.OrganizationID))
	}
	if call.CreatedAt.IsZero() {
		call.CreatedAt = time.Now()
	}

	db := h.getDBFromContext(ctx)
	inserted := false
	err := db.Transaction(func(tx *gorm.DB) error {
		if call.AgentRunID != nil {
			if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
				if lease.RunID != *call.AgentRunID {
					return ErrChatAgentRunDriveLeaseLost
				}
				if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, *call.AgentRunID); err != nil {
					return err
				}
			}
		}
		// Idempotent insert: a duplicate call_id is silently ignored (no error, RowsAffected 0).
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "call_id"}},
			DoNothing: true,
		}).Create(call)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			// Duplicate call_id — do NOT touch totals.
			return nil
		}
		inserted = true

		// Bump the per-channel totals cache in the same transaction. The upsert adds this call's
		// usage to the existing row, or seeds a new row at this call's usage.
		if err := h.upsertChatInferenceTotals(tx, call); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "record_chat_inference_call"),
			zap.String(appLogger.KeyCallID, call.CallID),
			zap.Uint64(appLogger.KeyChannelID, call.ChannelID),
			zap.Int64(appLogger.KeyOrganizationID, call.OrganizationID))
	}
	return inserted, nil
}

// upsertChatInferenceTotals increments (or seeds) the per-channel totals cache for one inserted
// ledger row, inside the caller's transaction. Uses ON CONFLICT(channel_id) DO UPDATE so the
// row is created on first call and accumulated thereafter.
func (h *CommonPersistenceHandler[U]) upsertChatInferenceTotals(tx *gorm.DB, call *common_persistence_models.ChatInferenceCall) error {
	now := time.Now()
	totals := &common_persistence_models.ChatInferenceTotals{
		ChannelID:                call.ChannelID,
		CallCount:                1,
		InputTokensTotal:         call.InputTokens,
		OutputTokensTotal:        call.OutputTokens,
		TotalTokensTotal:         call.TotalTokens,
		ThoughtsTokensTotal:      call.ThoughtsTokens,
		CachedContentTokensTotal: call.CachedContentTokens,
		InputCharactersTotal:     call.InputCharacters,
		OutputCharactersTotal:    call.OutputCharacters,
		InputImagesTotal:         call.InputImages,
		OutputImagesTotal:        call.OutputImages,
		InputVideosTotal:         call.InputVideos,
		OutputVideosTotal:        call.OutputVideos,
		InputAudioFilesTotal:     call.InputAudioFiles,
		OutputAudioFilesTotal:    call.OutputAudioFiles,
		UpdatedAt:                now,
	}
	totalsTable := common_persistence_models.TableNameChatInferenceTotals
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "channel_id"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "cached_content_tokens_total"}, Value: gorm.Expr(totalsTable+".cached_content_tokens_total + ?", call.CachedContentTokens)},
			{Column: clause.Column{Name: "call_count"}, Value: gorm.Expr(totalsTable+".call_count + ?", 1)},
			{Column: clause.Column{Name: "input_audio_files_total"}, Value: gorm.Expr(totalsTable+".input_audio_files_total + ?", call.InputAudioFiles)},
			{Column: clause.Column{Name: "input_characters_total"}, Value: gorm.Expr(totalsTable+".input_characters_total + ?", call.InputCharacters)},
			{Column: clause.Column{Name: "input_images_total"}, Value: gorm.Expr(totalsTable+".input_images_total + ?", call.InputImages)},
			{Column: clause.Column{Name: "input_tokens_total"}, Value: gorm.Expr(totalsTable+".input_tokens_total + ?", call.InputTokens)},
			{Column: clause.Column{Name: "input_videos_total"}, Value: gorm.Expr(totalsTable+".input_videos_total + ?", call.InputVideos)},
			{Column: clause.Column{Name: "output_audio_files_total"}, Value: gorm.Expr(totalsTable+".output_audio_files_total + ?", call.OutputAudioFiles)},
			{Column: clause.Column{Name: "output_characters_total"}, Value: gorm.Expr(totalsTable+".output_characters_total + ?", call.OutputCharacters)},
			{Column: clause.Column{Name: "output_images_total"}, Value: gorm.Expr(totalsTable+".output_images_total + ?", call.OutputImages)},
			{Column: clause.Column{Name: "output_tokens_total"}, Value: gorm.Expr(totalsTable+".output_tokens_total + ?", call.OutputTokens)},
			{Column: clause.Column{Name: "output_videos_total"}, Value: gorm.Expr(totalsTable+".output_videos_total + ?", call.OutputVideos)},
			{Column: clause.Column{Name: "thoughts_tokens_total"}, Value: gorm.Expr(totalsTable+".thoughts_tokens_total + ?", call.ThoughtsTokens)},
			{Column: clause.Column{Name: "total_tokens_total"}, Value: gorm.Expr(totalsTable+".total_tokens_total + ?", call.TotalTokens)},
			{Column: clause.Column{Name: "updated_at"}, Value: now},
		},
	}).Create(totals).Error
}

// GetChatInferenceTotals returns the per-channel totals cache. When no ledger rows exist yet, it
// returns a zero-valued totals row (not an error): absence of agent calls is not a failure.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//
// Returns:
//   - result1: The totals row (zero-valued for the channel when no rows exist).
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetChatInferenceTotals(ctx context.Context, channelID uint64) (*common_persistence_models.ChatInferenceTotals, error) {
	db := h.getDBFromContext(ctx)
	var totals common_persistence_models.ChatInferenceTotals
	err := db.Where("channel_id = ?", channelID).First(&totals).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &common_persistence_models.ChatInferenceTotals{ChannelID: channelID}, nil
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_inference_totals"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return &totals, nil
}

// GetChatInferenceTotalsForSessions aggregates the ledger rows attributed to any of the given
// sessions, which is how a delegation parent's usage rolls up its children. Zero ids are ignored;
// an empty set yields zero totals rather than a channel-wide sum.
func (h *CommonPersistenceHandler[U]) GetChatInferenceTotalsForSessions(ctx context.Context, agentSessionIDs []uint64) (*common_persistence_models.ChatInferenceTotals, error) {
	var totals common_persistence_models.ChatInferenceTotals
	agentSessionIDs = common_util.DeduplicateNonZero(agentSessionIDs)
	if len(agentSessionIDs) == 0 {
		return &totals, nil
	}
	if err := h.getDBFromContext(ctx).Model(&common_persistence_models.ChatInferenceCall{}).
		Select("COUNT(*) AS call_count, "+
			"COALESCE(SUM(input_tokens),0) AS input_tokens_total, "+
			"COALESCE(SUM(output_tokens),0) AS output_tokens_total, "+
			"COALESCE(SUM(total_tokens),0) AS total_tokens_total, "+
			"COALESCE(SUM(thoughts_tokens),0) AS thoughts_tokens_total, "+
			"COALESCE(SUM(cached_content_tokens),0) AS cached_content_tokens_total, "+
			"COALESCE(SUM(input_characters),0) AS input_characters_total, "+
			"COALESCE(SUM(output_characters),0) AS output_characters_total, "+
			"COALESCE(SUM(input_images),0) AS input_images_total, "+
			"COALESCE(SUM(output_images),0) AS output_images_total, "+
			"COALESCE(SUM(input_videos),0) AS input_videos_total, "+
			"COALESCE(SUM(output_videos),0) AS output_videos_total, "+
			"COALESCE(SUM(input_audio_files),0) AS input_audio_files_total, "+
			"COALESCE(SUM(output_audio_files),0) AS output_audio_files_total, "+
			"MAX(created_at) AS updated_at").
		Where("agent_session_id IN ?", agentSessionIDs).
		Scan(&totals).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_chat_inference_totals_for_sessions"),
			zap.Int(appLogger.KeyCount, len(agentSessionIDs)))
	}
	return &totals, nil
}

// GetLatestChatInferenceInputTokens returns the most recently recorded positive input-token count
// for a run. A zero result means the run has no usable ledger signal yet.
func (h *CommonPersistenceHandler[U]) GetLatestChatInferenceInputTokens(ctx context.Context, runID uint64) (int64, error) {
	db := h.getDBFromContext(ctx)
	var inputTokens int64
	if err := db.Model(&common_persistence_models.ChatInferenceCall{}).
		Select("input_tokens").
		Where("agent_run_id = ? AND input_tokens > 0 AND purpose <> ?", runID, proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_COMPACTION).
		Order("id DESC").
		Limit(1).
		Scan(&inputTokens).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_latest_chat_inference_input_tokens"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return inputTokens, nil
}

// usageSelectColumns is the SELECT list shared by the aggregate-by-message queries.
const usageSelectColumns = "" +
	"COUNT(*) AS call_count, " +
	"COALESCE(SUM(input_tokens),0) AS input_tokens, " +
	"COALESCE(SUM(output_tokens),0) AS output_tokens, " +
	"COALESCE(SUM(total_tokens),0) AS total_tokens, " +
	"COALESCE(SUM(thoughts_tokens),0) AS thoughts_tokens, " +
	"COALESCE(SUM(cached_content_tokens),0) AS cached_content_tokens"

// GetMessageInferenceAggregate derives the usage summary for one message by aggregating its
// linked ledger rows. Returns a zero-valued aggregate (CallCount 0) when no rows link to the
// message — not an error.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The message identifier.
//
// Returns:
//   - result1: The derived aggregate (zero-valued when no linked rows).
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetMessageInferenceAggregate(ctx context.Context, messageID uint64) (*MessageInferenceAggregate, error) {
	db := h.getDBFromContext(ctx)
	out := &MessageInferenceAggregate{MessageID: messageID}

	var sums struct {
		CallCount           int64
		InputTokens         int64
		OutputTokens        int64
		TotalTokens         int64
		ThoughtsTokens      int64
		CachedContentTokens int64
	}
	if err := db.Model(&common_persistence_models.ChatInferenceCall{}).
		Select(usageSelectColumns).
		Where("message_id = ?", messageID).
		Scan(&sums).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "aggregate_message_inference"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	out.CallCount = sums.CallCount
	out.InputTokens = sums.InputTokens
	out.OutputTokens = sums.OutputTokens
	out.TotalTokens = sums.TotalTokens
	out.ThoughtsTokens = sums.ThoughtsTokens
	out.CachedContentTokens = sums.CachedContentTokens
	if sums.CallCount == 0 {
		return out, nil
	}

	// Label fields come from the most recent linked call.
	var latest common_persistence_models.ChatInferenceCall
	if err := db.Where("message_id = ?", messageID).Order("id DESC").Limit(1).First(&latest).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "read_latest_message_inference_call"),
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	} else {
		applyLatestCallLabel(out, &latest)
	}
	return out, nil
}

// BatchGetMessageInferenceAggregates derives usage summaries for a bounded set of message IDs in
// one round trip. The map omits IDs with no linked ledger rows (callers treat them as "no usage")
// and IDs whose message is hidden from users or soft-deleted, so usage metadata (token counts,
// provider, model) of hidden transcript rows never leaks; the single-message path enforces the
// same visibility through the actor-default single-message read.
func (h *CommonPersistenceHandler[U]) BatchGetMessageInferenceAggregates(
	ctx context.Context,
	channelID uint64,
	messageIDs []uint64,
) (map[uint64]*MessageInferenceAggregate, error) {
	out := make(map[uint64]*MessageInferenceAggregate)
	if len(messageIDs) == 0 {
		return out, nil
	}
	db := h.getDBFromContext(ctx)

	var rows []struct {
		MessageID           uint64
		CallCount           int64
		InputTokens         int64
		OutputTokens        int64
		TotalTokens         int64
		ThoughtsTokens      int64
		CachedContentTokens int64
	}
	// The enrich query below inherits this visibility filter implicitly: it only queries the
	// message IDs this aggregate emitted.
	visibleMessages := "message_id IN (SELECT id FROM " + common_persistence_models.TableNameChatMessages +
		" WHERE id IN ? AND channel_id = ? AND hidden_from_users IS NOT TRUE AND deleted_at IS NULL)"
	if err := db.Model(&common_persistence_models.ChatInferenceCall{}).
		Select("message_id, "+usageSelectColumns).
		Where("channel_id = ? AND "+visibleMessages, channelID, messageIDs, channelID).
		Group("message_id").
		Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "batch_aggregate_message_inference"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMessageCount, len(messageIDs)))
	}
	for _, r := range rows {
		if r.MessageID == 0 {
			continue
		}
		out[r.MessageID] = &MessageInferenceAggregate{
			MessageID:           r.MessageID,
			ChannelID:           channelID,
			CallCount:           r.CallCount,
			InputTokens:         r.InputTokens,
			OutputTokens:        r.OutputTokens,
			TotalTokens:         r.TotalTokens,
			ThoughtsTokens:      r.ThoughtsTokens,
			CachedContentTokens: r.CachedContentTokens,
		}
	}
	if len(out) == 0 {
		return out, nil
	}

	// Enrich each message with the descriptive label from its most recent linked call
	// (provider/model/finish/latency for badge display), in ONE round trip — DISTINCT ON keeps the
	// "latest call per message" logic identical to the single-message path. Only the IDs that
	// produced sums above are queried.
	enrichIDs := make([]uint64, 0, len(out))
	for id := range out {
		enrichIDs = append(enrichIDs, id)
	}
	var latestRows []common_persistence_models.ChatInferenceCall
	if err := db.
		Clauses(clause.OrderBy{Expression: clause.Expr{SQL: "message_id, id DESC"}}).
		Distinct("ON (message_id) *").
		Where("channel_id = ? AND message_id IN ?", channelID, enrichIDs).
		Find(&latestRows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "batch_read_latest_message_inference_calls"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMessageCount, len(enrichIDs)))
	}
	for i := range latestRows {
		if latestRows[i].MessageID == nil {
			continue
		}
		if agg, ok := out[*latestRows[i].MessageID]; ok {
			applyLatestCallLabel(agg, &latestRows[i])
		}
	}
	return out, nil
}

// applyLatestCallLabel copies the descriptive badge-label fields from a ledger row onto an
// aggregate. It is the single source of the "latest call → label" mapping shared by the
// single-message and batched aggregate paths so the two stay symmetric (Get and Batch return the
// same label fields for the same message).
func applyLatestCallLabel(agg *MessageInferenceAggregate, latest *common_persistence_models.ChatInferenceCall) {
	agg.ChannelID = latest.ChannelID
	agg.ProviderID = latest.ProviderID
	agg.ProviderModelID = latest.ProviderModelID
	agg.FinishReason = latest.FinishReason
	agg.LatencyMs = latest.LatencyMs
}

// ListChatInferenceCallsByRun returns the append-only ledger rows for one run, ordered by
// ascending row ID, with cursor pagination on the same ID. CallIndex remains replay/debug metadata.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - runID: The run identifier.
//   - pagination: Cursor pagination (cursor is an id boundary; ascending).
//
// Returns:
//   - result1: The ledger rows for the page.
//   - result2: Pagination response with has_next_page / next_cursor.
//   - result3: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ListChatInferenceCallsByRun(
	ctx context.Context,
	runID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.ChatInferenceCall, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("list_chat_inference_calls_by_run", pagination); err != nil {
		return nil, nil, err
	}
	limit, cursor := pagination.GetLimit(), pagination.GetCursor()

	query := db.Model(&common_persistence_models.ChatInferenceCall{}).
		Where("agent_run_id = ?", runID)
	if cursor != 0 {
		query = query.Where("id > ?", cursor)
	}

	var rows []common_persistence_models.ChatInferenceCall
	if err := query.
		Order("id ASC").
		Limit(int(limit + 1)).
		Find(&rows).Error; err != nil {
		return nil, nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_chat_inference_calls_by_run"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint32(logkeys.Limit, limit), zap.Uint64(appLogger.KeyCursor, cursor))
	}

	hasNext := false
	if uint32(len(rows)) > limit {
		hasNext = true
		rows = rows[:limit]
	}
	out := make([]*common_persistence_models.ChatInferenceCall, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	var nextCursor uint64
	if hasNext && len(rows) > 0 {
		nextCursor = rows[len(rows)-1].ID
	}
	resp := &proto_models.PaginationResponse{
		ItemsPerPage:       limit,
		HasNextPage:        hasNext,
		HasPrevPage:        cursor != 0,
		NextCursor:         nextCursor,
		TotalCountIncluded: false,
	}
	return out, resp, nil
}

// InferenceBreakdownDimension selects the GROUP BY dimension for session usage breakdowns.
type InferenceBreakdownDimension uint8

const (
	// InferenceBreakdownByRun groups by agent_run_id.
	InferenceBreakdownByRun InferenceBreakdownDimension = 1
	// InferenceBreakdownByPurpose groups by purpose.
	InferenceBreakdownByPurpose InferenceBreakdownDimension = 2
	// InferenceBreakdownByProvider groups by provider.
	InferenceBreakdownByProvider InferenceBreakdownDimension = 3
	// InferenceBreakdownByModel groups by provider_model_id.
	InferenceBreakdownByModel InferenceBreakdownDimension = 4
	// InferenceBreakdownBySession groups by agent_session_id: the per-member axis of a delegation
	// rollup. Every row of a session query carries a session id, so no NULL filter is needed.
	InferenceBreakdownBySession InferenceBreakdownDimension = 5
)

// assign writes the parsed group key into the appropriate breakdown pointer field.
func (d InferenceBreakdownDimension) assign(b *InferenceUsageBreakdown, key *string) {
	if key == nil {
		return
	}
	switch d {
	case InferenceBreakdownBySession:
		if v, err := strconv.ParseUint(*key, 10, 64); err == nil {
			b.AgentSessionID = &v
		}
	case InferenceBreakdownByRun:
		if v, err := strconv.ParseUint(*key, 10, 64); err == nil {
			b.AgentRunID = &v
		}
	case InferenceBreakdownByPurpose:
		if v, err := strconv.ParseInt(*key, 10, 32); err == nil {
			p := proto_models.InferenceCallPurpose(int32(v))
			b.Purpose = &p
		}
	case InferenceBreakdownByProvider:
		provider := *key
		b.ProviderID = &provider
	case InferenceBreakdownByModel:
		model := *key
		b.ProviderModelID = &model
	}
}
