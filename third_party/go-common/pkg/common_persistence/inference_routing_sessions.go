package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

var ErrInferenceRoutingTransactionRequired = errors.New("inference routing: ambient transaction required")

// ListInferencePolicySessions optimistically reads a bounded keyset page of
// active session pins in the caller's policy/action transaction. Writers recheck
// pin versions after taking the normal run-before-session locks. Organization zero
// means all organizations; a positive org matches that organization only.
// Operation zero and a nil model list omit their filters. An empty nonnil model
// list matches only member-selected pins when includeUserChoices is true and
// matches nothing when false. Member selection includes retargets that retain
// its provenance. Pure member-choice closure callers must pass an empty nonnil
// model list; nil intentionally retains the storage wildcard contract.
// Listed removed models exclude site-override pins, which bypass preference
// narrowing. Archived sessions are excluded; blocked sessions remain.
func (s *ControlPlaneStore) ListInferencePolicySessions(ctx context.Context, organizationID uint64, operation int32, modelIDs []uint64, includeUserChoices bool, afterID uint64, limit int) ([]cpm.ChatAgentSession, error) {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return nil, ErrInferenceRoutingTransactionRequired
	}
	if organizationID > math.MaxInt64 || afterID > math.MaxInt64 || limit <= 0 {
		return nil, ErrInferenceRoutingInvalidPolicy
	}
	if modelIDs != nil && len(modelIDs) == 0 && !includeUserChoices {
		return []cpm.ChatAgentSession{}, nil
	}
	query := s.routingSession(ctx).Model(&cpm.ChatAgentSession{}).
		Where("status = ? AND id > ?", cpm.ChatAgentSessionStatusActive, afterID).
		Where(`organization_id IN (SELECT id FROM gocommon_organizations WHERE deleted_at IS NULL)`)
	if organizationID > 0 {
		query = query.Where("organization_id = ?", organizationID)
	}
	if operation != 0 {
		query = query.Where("inference_operation = ?", operation)
	}
	if modelIDs != nil {
		ids := make([]int64, len(modelIDs))
		for position, id := range modelIDs {
			if !validBigintID(id) {
				return nil, ErrInferenceRoutingInvalidPolicy
			}
			ids[position] = int64(id)
		}
		if includeUserChoices {
			query = query.Where("((selection_source <> ? AND site_override_user_id IS NULL AND execution_inference_model_id = ANY(?::bigint[])) OR ((selection_source = ? OR member_choice_selection = true) AND site_override_user_id IS NULL))", proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE, pq.Array(ids), proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE)
		} else {
			query = query.Where("(selection_source <> ? AND site_override_user_id IS NULL AND execution_inference_model_id = ANY(?::bigint[]))", proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE, pq.Array(ids))
		}
	}
	var sessions []cpm.ChatAgentSession
	if err := query.Order("id ASC").Limit(limit).Find(&sessions).Error; err != nil {
		return nil, fmt.Errorf("list inference policy sessions: %w", err)
	}
	return sessions, nil
}

// BlockInferencePolicySessions performs one bulk update guarded by every
// session's expected pin version. Any missing, archived or retargeted session
// rolls back the whole update to a savepoint in the ambient transaction.
func (s *ControlPlaneStore) BlockInferencePolicySessions(ctx context.Context, sessionIDs []uint64, expectedPins map[uint64]uint64, reason proto_models.InferenceBlockReason) error {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return ErrInferenceRoutingTransactionRequired
	}
	if !validInferenceBlockReason(reason) {
		return ErrInferenceRoutingInvalidPolicy
	}
	if len(sessionIDs) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(sessionIDs))
	values := make([]string, len(sessionIDs))
	args := make([]interface{}, 0, 4+2*len(sessionIDs))
	now := time.Now()
	args = append(args, reason, now, now)
	for position, id := range sessionIDs {
		version, found := expectedPins[id]
		if !validBigintID(id) || !found || !validBigintID(version) {
			return ErrInferenceRoutingInvalidPolicy
		}
		if _, duplicate := seen[id]; duplicate {
			return ErrInferenceRoutingInvalidPolicy
		}
		seen[id] = struct{}{}
		values[position] = "(?::bigint, ?::bigint)"
		args = append(args, id, version)
	}
	args = append(args, cpm.ChatAgentSessionStatusActive)
	query := `UPDATE gocommon_chat_agent_sessions AS sessions
 SET inference_blocked_reason = ?, inference_blocked_at = ?, updated_at = ?
 FROM (VALUES ` + strings.Join(values, ",") + `) AS pins(id, version)
 WHERE sessions.id = pins.id AND sessions.inference_pin_version = pins.version AND sessions.status = ?
 RETURNING sessions.id`
	if err := s.routingSession(ctx).Transaction(func(tx *gorm.DB) error {
		var updated []uint64
		if err := tx.Raw(query, args...).Scan(&updated).Error; err != nil {
			return err
		}
		if len(updated) != len(sessionIDs) {
			return ErrInferencePolicyVersionConflict
		}
		return nil
	}); err != nil {
		return fmt.Errorf("block inference policy sessions: %w", err)
	}
	return nil
}
