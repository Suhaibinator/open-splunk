package notification

import (
	"context"
	"errors"
	"fmt"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

var errProducerUnavailable = errors.New("notification producer unavailable")

// producerAvailable reports whether this process registers the emission's
// producer name. An unregistered name defers rather than failing the emission.
func (h *Handler) producerAvailable(job models.NotificationEmission) bool {
	_, ok := h.producers.get(job.Producer)
	return ok
}

func producerEvent(job models.NotificationEmission) ProducerEvent {
	return ProducerEvent{Producer: job.Producer,
		OrganizationID: job.OrganizationID, Type: job.Type, Payload: job.Payload,
		FallbackText: job.FallbackText, SourceKey: job.SourceKey, EmissionKey: job.EmissionKey, SourceTime: job.SourceTime,
		Async: true, SourceStamp: job.SourceStamp, SourceSnapshot: job.SourceSnapshot}
}

// advanceNotificationEmission runs one materialization step with the recipient
// batch size from the tuning sampled by the step's claim.
func (h *Handler) advanceNotificationEmission(ctx context.Context, store persistence.NotificationStore, claim models.NotificationEmission, tuning fanoutTuning) (result *models.NotificationEmission, err error) {
	batch := tuning.batchRecipients
	defer func() {
		if recover() != nil {
			h.log(ctx).Error("notification fan-out callback panicked", zap.Stack("stack"))
			result, err = nil, errCallbackPanic
		}
	}()
	result, err = store.AdvanceNotificationEmission(ctx, claim, func(tx context.Context, job *models.NotificationEmission) (persistence.NotificationEmissionProgress, error) {
		progress := persistence.NotificationEmissionProgress{}
		cfg := h.deps.Config()
		// One byte-limit sample per step: snapshot, prepare, and materialize agree.
		limits := emissionLimitsFor(cfg)
		if !cfg.Enabled || cfg.FanoutPaused || (job.OrganizationID != 0 && !cfg.OrganizationsEnabled) {
			return progress, ErrDisabled
		}
		var recipients []models.NotificationEmissionRecipient
		var producer Producer
		if job.Event {
			var ok bool
			producer, ok = h.producers.get(job.Producer)
			if !ok {
				return progress, errProducerUnavailable
			}
		}
		if !job.SnapshotReady {
			if !job.Event {
				return progress, errProducerUnavailable
			}
			page, err := snapshotResolvedProducer(tx, producer, producerEvent(*job), job.Cursor, limits)
			if err != nil {
				return progress, err
			}
			progress.Cursor, progress.SnapshotDone = page.Cursor, page.Done
			progress.Recipients = make([]models.NotificationEmissionRecipient, len(page.Candidates))
			for i, candidate := range page.Candidates {
				progress.Recipients[i] = models.NotificationEmissionRecipient{EmissionID: job.ID, UserID: candidate.RecipientID, Metadata: candidate.Metadata}
			}
			// Once enumeration has progressed beyond its first page, all candidates
			// must be read from the persisted snapshot before materializing.
			if !page.Done || len(job.Cursor) != 0 || len(page.Candidates) > batch {
				return progress, nil
			}
			recipients = progress.Recipients
		} else {
			var err error
			recipients, err = store.ListNotificationEmissionRecipients(tx, job.ID, batch)
			if err != nil {
				return progress, err
			}
			if len(recipients) == 0 && job.Processed < job.AudienceSize {
				return progress, persistence.ErrNotificationInput
			}
		}
		inputs := make([]materializationInput, 0, len(recipients))
		if job.Event {
			candidates := make([]RecipientCandidate, len(recipients))
			for i, recipient := range recipients {
				candidates[i] = RecipientCandidate{RecipientID: recipient.UserID, Metadata: recipient.Metadata}
			}
			var err error
			inputs, err = prepareResolvedProducer(tx, producer, producerEvent(*job), candidates, limits)
			if err != nil {
				return progress, err
			}
		} else {
			for _, recipient := range recipients {
				input := materializationInput{OrganizationID: job.OrganizationID, RecipientID: recipient.UserID, Type: job.Type,
					Payload: job.Payload, FallbackText: job.FallbackText, SourceKey: job.SourceKey, DeliveryTag: recipient.DeliveryTag}
				if recipient.Override {
					input.Payload, input.FallbackText = recipient.Payload, recipient.FallbackText
				}
				inputs = append(inputs, input)
			}
		}
		// Prepare may expand payloads or omit unauthorized candidates. Enforce the
		// cumulative byte bound on its output while preserving a candidate prefix;
		// the remaining persisted candidates are resumed by the next step.
		byRecipient := make(map[uint64]materializationInput, len(inputs))
		for _, input := range inputs {
			if _, exists := byRecipient[input.RecipientID]; exists {
				return progress, ErrInvalid
			}
			byRecipient[input.RecipientID] = input
		}
		bounded := make([]materializationInput, 0, len(inputs))
		bytes := 0
		for _, recipient := range recipients {
			input, prepared := byRecipient[recipient.UserID]
			if prepared {
				if len(input.Payload) > limits.payload {
					return progress, ErrInvalid
				}
				if bytes+emitStoredBytes(input.EmitInput) > limits.total {
					if len(progress.ProcessedUserIDs) == 0 {
						// A total below one input (only possible from an
						// unvalidated custom provider) fails the step instead of
						// spinning without progress.
						return progress, fmt.Errorf("%w: one prepared input exceeds notification_emit_max_total_bytes", ErrInvalid)
					}
					break
				}
				bytes += emitStoredBytes(input.EmitInput)
				input.sourceStamp, input.SourceTime = job.SourceStamp, job.SourceTime
				bounded = append(bounded, input)
			}
			progress.ProcessedUserIDs = append(progress.ProcessedUserIDs, recipient.UserID)
		}
		results, err := h.materializeMany(tx, bounded, true, limits)
		if err != nil {
			return progress, err
		}
		for _, r := range results {
			if r.Allowed {
				progress.Accepted++
			}
		}
		return progress, nil
	})
	if err == nil && (result == nil || result.ID != claim.ID || result.Processed < claim.Processed) {
		return nil, appLogger.InvariantError("notification_fanout_progress_result", errors.New("fan-out store returned invalid progress"))
	}
	return result, err
}
