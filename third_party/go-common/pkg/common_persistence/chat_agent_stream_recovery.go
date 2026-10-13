package common_persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrAgentStreamRecoveryExhausted means every recovery generation allowed for this step was used.
var ErrAgentStreamRecoveryExhausted = errors.New("agent runtime: interrupted stream recovery exhausted")

// AgentStreamRecoveryPersistence prepares a fresh generation without replaying sealed work.
type AgentStreamRecoveryPersistence interface {
	PrepareChatAgentStreamRecovery(ctx context.Context, runID uint64, step uint32, maxAttempts int) (*AgentStreamRecovery, error)
	RecoverInterruptedChatAgentRunStep(ctx context.Context, runID uint64, maxAttempts int) (*AgentStreamRecovery, error)
}

// AgentStreamRecovery contains committed cleanup results for post-commit notifications.
type AgentStreamRecovery struct {
	Attempt   uint32
	Discarded []common_persistence_models.ChatMessage
}

// PrepareChatAgentStreamRecovery consumes one of maxAttempts durable allowances and removes only
// the failed stream projection. The caller samples chat_agent_stream_recovery_max_attempts per
// drive attempt; maxAttempts must be 1..config.MaxChatAgentStreamRecoveryMaxAttempts. Lowering
// the key exhausts a run already at the new count on its next recovery. No artifact is also valid: a committed callback need not create text.
// A crash after commit leaves the same step eligible, with the allowance already consumed.
func (h *CommonPersistenceHandler[U]) PrepareChatAgentStreamRecovery(ctx context.Context, runID uint64, step uint32, maxAttempts int) (*AgentStreamRecovery, error) {
	if maxAttempts < 1 || maxAttempts > config.MaxChatAgentStreamRecoveryMaxAttempts {
		return nil, fmt.Errorf("prepare chat agent stream recovery: max attempts %d outside 1..%d", maxAttempts, config.MaxChatAgentStreamRecoveryMaxAttempts)
	}
	var result *AgentStreamRecovery
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		var run common_persistence_models.ChatAgentRun
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if next, ok := unsealedChatAgentRunStep(&run); !ok || step != next {
			return ErrChatAgentRunStepSealInvalid
		}
		var err error
		result, err = consumeStreamRecoveryAllowanceTx(tx, &run, step, maxAttempts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RecoverInterruptedChatAgentRunStep is PrepareChatAgentStreamRecovery for a stream an earlier
// drive left unfinished: a crash, deploy, or timeout ended it before it could fail its own stream.
// Under the live lease nothing else writes the step after the last seal, so that step's STREAMING
// assistant rows are marked FAILED and then discarded under the same allowance and guards.
// ErrChatAgentRunStepSealInvalid (tool rows, nothing to discard, or an exhausted step index),
// ErrChatMessageAgentSessionRootPinned, and ErrAgentStreamRecoveryExhausted roll back every
// change; the caller then fails the run as an incomplete step.
func (h *CommonPersistenceHandler[U]) RecoverInterruptedChatAgentRunStep(ctx context.Context, runID uint64, maxAttempts int) (*AgentStreamRecovery, error) {
	if maxAttempts < 1 || maxAttempts > config.MaxChatAgentStreamRecoveryMaxAttempts {
		return nil, fmt.Errorf("recover interrupted chat agent run step: max attempts %d outside 1..%d", maxAttempts, config.MaxChatAgentStreamRecoveryMaxAttempts)
	}
	var result *AgentStreamRecovery
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		var run common_persistence_models.ChatAgentRun
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		step, ok := unsealedChatAgentRunStep(&run)
		if !ok {
			return ErrChatAgentRunStepSealInvalid
		}
		// Keep the run -> channel -> message lock order before touching the stream rows.
		if err := lockChatAgentRunChannelTx(tx, run.ChannelID); err != nil {
			return err
		}
		if err := tx.Model(&common_persistence_models.ChatMessage{}).
			Where("agent_run_id = ? AND agent_step_index = ? AND kind = ? AND stream_state = ?", runID, step,
				proto_models.MessageKind_MESSAGE_KIND_STANDARD,
				proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING).
			Select("stream_state", "updated_at").
			Updates(struct {
				StreamState proto_models.MessageStreamState `gorm:"column:stream_state"`
				UpdatedAt   clause.Expr                     `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
			}{
				StreamState: proto_models.MessageStreamState_MESSAGE_STREAM_STATE_FAILED,
				UpdatedAt:   gorm.Expr("CURRENT_TIMESTAMP"),
			}).Error; err != nil {
			return err
		}
		var err error
		result, err = consumeStreamRecoveryAllowanceTx(tx, &run, step, maxAttempts)
		if err != nil {
			return err
		}
		if len(result.Discarded) == 0 {
			// An incomplete run with no unsealed projection is a corrupt seal, not an interrupted stream.
			return ErrChatAgentRunStepSealInvalid
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// unsealedChatAgentRunStep is the step after the run's last seal, or 0 before any seal. It is not
// ok once the step index is exhausted.
func unsealedChatAgentRunStep(run *common_persistence_models.ChatAgentRun) (uint32, bool) {
	if run.LastSealedStepIndex == nil {
		return 0, true
	}
	if *run.LastSealedStepIndex == ^uint32(0) {
		return 0, false
	}
	return *run.LastSealedStepIndex + 1, true
}

// consumeStreamRecoveryAllowanceTx charges one recovery of step against maxAttempts and discards
// its failed stream projection. The caller holds the leased run lock and has validated step.
func consumeStreamRecoveryAllowanceTx(tx *gorm.DB, run *common_persistence_models.ChatAgentRun, step uint32, maxAttempts int) (*AgentStreamRecovery, error) {
	if run.StreamRecoveryCount != 0 && run.StreamRecoveryStepIndex != step {
		return nil, ErrChatAgentRunStepSealInvalid
	}
	if int64(run.StreamRecoveryCount) >= int64(maxAttempts) {
		return nil, ErrAgentStreamRecoveryExhausted
	}
	discarded, err := discardUnsealedAgentProjection(tx, run, &step)
	if err != nil {
		return nil, err
	}
	result := &AgentStreamRecovery{Attempt: run.StreamRecoveryCount + 1, Discarded: discarded}
	if err := tx.Model(run).Select("stream_recovery_step_index", "stream_recovery_count").
		Updates(common_persistence_models.ChatAgentRun{StreamRecoveryStepIndex: step, StreamRecoveryCount: result.Attempt}).Error; err != nil {
		return nil, err
	}
	return result, nil
}
