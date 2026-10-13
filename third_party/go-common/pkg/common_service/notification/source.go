package notification

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// SourceCandidate identifies one recipient/source pair. Payload belongs to the caller;
// evaluators must not mutate or retain it. All callbacks must honor context cancellation.
type SourceCandidate struct {
	OrganizationID int64
	RecipientID    uint64
	Payload        []byte
}

// SourceOptions keeps emission authorization independent of source resolution checks.
type SourceOptions struct {
	CheckResolution bool
	// IncludeDeliveryPreference requests optional interruption metadata from the
	// same loaded source snapshot. It is used only within this evaluation.
	IncludeDeliveryPreference bool
	// IncludeView requests the optional per-type display view. Only inbox reads
	// that return entries to a client set it; emission, counts and delivery
	// never do, so evaluators may skip building views when it is false.
	IncludeView bool
}

// SourceDecision describes access at this evaluation boundary, never a durable grant.
type SourceDecision struct {
	Authorized, Resolved bool
	DeliveryPreference   *DeliveryPreference
	// View is an optional per-type display view built from the same snapshot
	// that authorized the row. Inbox reads copy it to Notification.view only
	// for authorized rows; it is dropped otherwise and never persisted.
	View []byte
}

// sourceLimits bounds source-authorization callback chunks. The calling
// operation samples it once with its other configuration and passes it in, so
// every chunk of one operation uses the same bounds.
type sourceLimits struct {
	rows, bytes int
}

// sourceLimitsFor copies notification_source_batch_rows and
// notification_source_batch_bytes from one configuration sample.
func sourceLimitsFor(cfg *Config) sourceLimits {
	return sourceLimits{rows: cfg.SourceBatchRows, bytes: cfg.SourceBatchBytes}
}

// evaluateType bounds both candidate count and payload bytes passed to application
// callbacks. It never carries a decision across operations or rendering boundaries.
func (h *Handler) evaluateType(ctx context.Context, t Type, candidates []SourceCandidate, options SourceOptions, limits sourceLimits) ([]SourceDecision, error) {
	allowed, err := h.organizationCandidates(ctx, candidates, limits)
	if err != nil {
		return nil, err
	}
	filtered := make([]SourceCandidate, 0, len(candidates))
	indices := make([]int, 0, len(candidates))
	for i, c := range candidates {
		if allowed[i] {
			filtered = append(filtered, c)
			indices = append(indices, i)
		}
	}
	decisions, err := h.evaluateAuthorizedType(ctx, t, filtered, options, limits)
	if err != nil {
		return nil, err
	}
	out := make([]SourceDecision, len(candidates))
	for i, index := range indices {
		out[index] = decisions[i]
	}
	return out, nil
}

func (h *Handler) evaluateAuthorizedType(ctx context.Context, t Type, candidates []SourceCandidate, options SourceOptions, limits sourceLimits) ([]SourceDecision, error) {
	decisions := make([]SourceDecision, len(candidates))
	for start := 0; start < len(candidates); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start
		size := 0
		for end < len(candidates) && end-start < limits.rows {
			payloadSize := len(candidates[end].Payload)
			if end > start && size+payloadSize > limits.bytes {
				break
			}
			size += payloadSize
			end++
		}
		batch := candidates[start:end]
		if t.EvaluateSources != nil {
			evaluated, err := t.EvaluateSources(ctx, batch, options)
			if err != nil {
				return nil, safeCallbackError("evaluate_sources", err)
			}
			if len(evaluated) != len(batch) {
				return nil, appLogger.InvariantError("notification_source_decision_cardinality", errors.New("notification source evaluator returned mismatched decisions"), zap.String(logkeys.Stage, "evaluate_sources"), zap.Int(logkeys.Expected, len(batch)), zap.Int(logkeys.Actual, len(evaluated)), zap.Uint32(appLogger.KeyNotificationType, t.ID))
			}
			copy(decisions[start:end], evaluated)
		} else {
			// Registration guarantees one of the two callbacks; an unregistered
			// Type reaching this point fails closed instead of dereferencing nil.
			if t.Authorize == nil {
				return nil, appLogger.InvariantError("notification_source_authorizer_registered", errors.New("notification type has neither source authorizer nor evaluator"), zap.String(logkeys.Stage, "authorize"), zap.Uint32(appLogger.KeyNotificationType, t.ID))
			}
			for index, candidate := range batch {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				allowed, err := t.Authorize(ctx, candidate.RecipientID, candidate.Payload)
				if err != nil {
					return nil, safeCallbackError("authorize", err)
				}
				decision := SourceDecision{Authorized: allowed}
				if allowed && options.CheckResolution && t.Resolved != nil {
					decision.Resolved, err = t.Resolved(ctx, candidate.Payload)
					if err != nil {
						return nil, safeCallbackError("resolved", err)
					}
				}
				decisions[start+index] = decision
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start = end
	}
	return decisions, nil
}

// evaluateRows groups operation-local rows by registered type without interpreting
// opaque payloads. Unknown types are unavailable. Stored resolution is independent.
func (h *Handler) evaluateRows(ctx context.Context, rows []models.Notification, options SourceOptions, limits sourceLimits) ([]SourceDecision, error) {
	decisions := make([]SourceDecision, len(rows))
	groups := make(map[uint32][]int)
	var typeOrder []uint32
	for index := range rows {
		row := &rows[index]
		decisions[index].Resolved = row.Resolved
		if _, exists := groups[row.Type]; !exists {
			typeOrder = append(typeOrder, row.Type)
		}
		groups[row.Type] = append(groups[row.Type], index)
	}
	for _, id := range typeOrder {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t, known := h.registry.get(id)
		if !known {
			continue
		}
		indices := groups[id]
		candidates := make([]SourceCandidate, len(indices))
		for index, rowIndex := range indices {
			row := &rows[rowIndex]
			candidates[index] = SourceCandidate{OrganizationID: row.OrganizationID, RecipientID: row.UserID, Payload: row.Payload}
		}
		evaluated, err := h.evaluateType(ctx, t, candidates, options, limits)
		if err != nil {
			return nil, err
		}
		for index, rowIndex := range indices {
			decisions[rowIndex].Authorized = evaluated[index].Authorized
			decisions[rowIndex].Resolved = decisions[rowIndex].Resolved || evaluated[index].Resolved
			// A view describes only what the recipient may currently see.
			if options.IncludeView && evaluated[index].Authorized && len(evaluated[index].View) > 0 {
				decisions[rowIndex].View = evaluated[index].View
			}
		}
	}
	return decisions, nil
}
