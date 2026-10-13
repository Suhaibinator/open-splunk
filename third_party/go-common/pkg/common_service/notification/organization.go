package notification

import (
	"context"
	"encoding/json/v2"
	"errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
)

// OrganizationEmitInput is a trusted backend request. The application authorizes
// the producing operation; registered types still authorize every recipient.
type OrganizationEmitInput struct {
	OrganizationID          int64
	Type                    uint32
	Payload                 []byte
	FallbackText, SourceKey string
}

func (h *Handler) organizationsEnabled() bool {
	return h.enabled() && h.deps.Config().OrganizationsEnabled
}

// EmitToOrganization emits one event through the internal organization producer
// to an organization's direct members. It always runs async: the emission is
// enqueued in the ambient source transaction and the receipt reports its id;
// snapshotting and inbox expansion happen later with no per-member work here.
// Inspect or resume it with GetEmission and RetryEmission.
func (h *Handler) EmitToOrganization(ctx context.Context, in OrganizationEmitInput) (EmitReceipt, error) {
	if !h.organizationsEnabled() {
		return EmitReceipt{}, ErrDisabled
	}
	limits := emissionLimitsFor(h.deps.Config())
	if in.OrganizationID <= 0 || in.SourceKey == "" || len(in.SourceKey) > config.MaxNotificationSourceKeyBytes || len(in.Payload) > limits.payload || len(in.FallbackText) > limits.fallbackText {
		return EmitReceipt{}, ErrInvalid
	}
	typ, known := h.registry.get(in.Type)
	if !known {
		return EmitReceipt{}, ErrUnknownType
	}
	if err := typ.Validate(ctx, in.Payload); err != nil {
		return EmitReceipt{}, safeCallbackError("validate", err)
	}
	return h.emitEvent(ctx, EventInput{Producer: "organization",
		OrganizationID: in.OrganizationID, Type: in.Type, Payload: in.Payload,
		FallbackText: in.FallbackText, SourceKey: in.SourceKey, SourceTime: h.now()}, true, true)
}

// ResolveOrganizationSource first fences expansion, then resolves entries whose
// latest occurrence matches the source. Previously committed inbox history remains.
func (h *Handler) ResolveOrganizationSource(ctx context.Context, organizationID int64, typeID uint32, sourceKey string) error {
	if h == nil || h.store == nil {
		return ErrDisabled
	}
	if organizationID <= 0 || typeID == 0 || sourceKey == "" || len(sourceKey) > config.MaxNotificationSourceKeyBytes {
		return ErrInvalid
	}
	pageSize := h.deps.Config().SourceResolutionPageSize
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		rows, err := h.store.ResolveOrganizationSource(tx, organizationID, typeID, sourceKey, pageSize)
		if err != nil {
			return err
		}
		for i := range rows {
			h.publish(tx, &rows[i])
		}
		return nil
	})
}

// organizationCandidates is shared by every source-authorizing operation. No
// cache can outlive this decision, and application callbacks cannot widen it.
func (h *Handler) organizationCandidates(ctx context.Context, candidates []SourceCandidate, limits sourceLimits) ([]bool, error) {
	allowed := make([]bool, len(candidates))
	var scoped []persistence.NotificationOrganizationRecipient
	var indices []int
	for i, c := range candidates {
		if c.OrganizationID < 0 {
			return nil, ErrInvalid
		}
		if c.OrganizationID == 0 {
			allowed[i] = true
			continue
		}
		indices = append(indices, i)
		scoped = append(scoped, persistence.NotificationOrganizationRecipient{OrganizationID: c.OrganizationID, UserID: c.RecipientID})
	}
	if len(scoped) == 0 {
		return allowed, nil
	}
	// Organization disablement hides scoped content, rather than exposing it or
	// preventing unrelated unscoped notifications from being read.
	if !h.deps.Config().OrganizationsEnabled {
		return allowed, nil
	}
	for start := 0; start < len(scoped); start += limits.rows {
		end := min(start+limits.rows, len(scoped))
		decisions, err := h.store.NotificationOrganizationMembers(ctx, scoped[start:end])
		if err != nil {
			return nil, err
		}
		if len(decisions) != end-start {
			return nil, appLogger.InvariantError("notification_org_membership_cardinality", errors.New("organization membership result count mismatch"))
		}
		for i, decision := range decisions {
			allowed[indices[start+i]] = decision
		}
	}
	return allowed, nil
}

// organizationCursor is the organization producer's opaque keyset cursor.
type organizationCursor struct {
	After uint64 `json:"after"`
}

func (h *Handler) registerOrganizationProducer() {
	_ = h.producers.register(Producer{Name: "organization",
		Validate: func(ctx context.Context, event EventInput) error {
			if event.OrganizationID <= 0 {
				return ErrInvalid
			}
			exists, err := h.store.NotificationOrganizationExists(ctx, event.OrganizationID)
			if err != nil {
				return err
			}
			if !exists {
				return ErrNotFound
			}
			return nil
		},
		Snapshot: func(ctx context.Context, event ProducerEvent, cursor []byte) (RecipientPage, error) {
			var position organizationCursor
			if len(cursor) > 0 {
				if err := json.Unmarshal(cursor, &position); err != nil {
					return RecipientPage{}, ErrInvalid
				}
			}
			ids, err := h.store.NotificationOrganizationMemberPage(ctx, event.OrganizationID, position.After, maxEmitCandidates)
			if err != nil {
				return RecipientPage{}, err
			}
			page := RecipientPage{Candidates: make([]RecipientCandidate, len(ids)), Done: len(ids) < maxEmitCandidates}
			for i, id := range ids {
				page.Candidates[i] = RecipientCandidate{RecipientID: id}
			}
			if len(ids) > 0 {
				position.After = ids[len(ids)-1]
				page.Cursor, err = json.Marshal(position)
				if err != nil {
					return RecipientPage{}, err
				}
			}
			return page, nil
		},
		Prepare: func(_ context.Context, event ProducerEvent, candidates []RecipientCandidate) ([]EmitInput, error) {
			inputs := make([]EmitInput, len(candidates))
			for i, candidate := range candidates {
				inputs[i] = EmitInput{OrganizationID: event.OrganizationID, RecipientID: candidate.RecipientID, Type: event.Type,
					Payload: event.Payload, FallbackText: event.FallbackText, SourceKey: event.SourceKey}
			}
			return inputs, nil
		},
	}, true)
}
