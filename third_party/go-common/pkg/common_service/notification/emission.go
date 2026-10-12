package notification

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// EmitMany bounds retained input preparation and returned row payloads. Producers
// with larger audiences can call it in chunks inside their source transaction.
const maxEmitCandidates = config.MaxNotificationFanoutBatchRecipients

// emissionLimits is one sample of the emission byte limits and the source
// authorization chunking. Each operation (an emission operation, an update, an
// organization emit, a fan-out step) samples it once and passes it to every
// validator and source evaluation it runs.
type emissionLimits struct {
	payload, fallbackText, metadata, total int
	source                                 sourceLimits
}

// emissionLimitsFor copies the byte keys from one configuration sample. KMS
// validation guarantees the total holds one maximal input (payload, fallback
// text, and delivery tag, as charged by emitStoredBytes).
func emissionLimitsFor(cfg *Config) emissionLimits {
	return emissionLimits{
		payload:      cfg.MaxPayloadBytes,
		fallbackText: cfg.MaxFallbackTextBytes,
		metadata:     cfg.MaxMetadataBytes,
		total:        cfg.EmitMaxTotalBytes,
		source:       sourceLimitsFor(cfg),
	}
}

// emitStoredBytes is one input's charge against notification_emit_max_total_bytes:
// every variable-length value its inbox entry or fan-out recipient row stores.
// Inline materialization, async enqueue, and fan-out steps all charge inputs
// through it, so a batch accepted in one mode is accepted in the other.
func emitStoredBytes(in EmitInput) int {
	return len(in.Payload) + len(in.FallbackText) + len(in.DeliveryTag)
}

// EmitReceipt describes inline materialization or durable async acceptance.
// Mode FanoutInline means the input was materialized before return; Mode
// FanoutAsync means it was durably accepted as emission EmissionID. Allowed, Entry, and Changed describe inline results only; an accepted async
// receipt has no recipient authorization decision yet. Inline denial is a
// successful receipt with Allowed false, Entry nil, and Changed false. Inline
// retries return their original Entry with Changed false.
type EmitReceipt struct {
	Mode       string
	EmissionID uint64
	Entry      *models.Notification
	Allowed    bool
	Changed    bool
}

type sourcePreparationKey struct {
	typeID  uint32
	payload string
}
type sourcePreparation struct{ group, scope string }

// materializationInput owns trusted persistence controls. Public emission and
// producer inputs cannot supply transaction stamps or callbacks under row locks.
type materializationInput struct {
	EmitInput
	sourceStamp         models.NotificationTransactionStamp
	freshCheck          func(context.Context) (bool, error)
	skipRetirementFence bool
}

func materializationInputs(inputs []EmitInput) []materializationInput {
	prepared := make([]materializationInput, len(inputs))
	for i, input := range inputs {
		prepared[i].EmitInput = input
	}
	return prepared
}

func validateEmitInputShape(input EmitInput, limits emissionLimits) error {
	if input.OrganizationID < 0 || input.RecipientID == 0 || input.SourceKey == "" || len(input.SourceKey) > config.MaxNotificationSourceKeyBytes || len(input.Payload) > limits.payload || len(input.FallbackText) > limits.fallbackText || len(input.Producer) > config.MaxNotificationProducerBytes || len(input.DeliveryTag) > config.MaxNotificationDeliveryTagBytes {
		return ErrInvalid
	}
	return nil
}

// admitEmission shares pure admission at each trust boundary. Whole-call mode
// separation charges bytes first; materialization also validates input shape.
func admitEmission(count int, input func(int) EmitInput, limits emissionLimits, checkShape bool) error {
	if count > maxEmitCandidates {
		return fmt.Errorf("%w: emission batch exceeds candidate limit", ErrInvalid)
	}
	total := 0
	for i := range count {
		in := input(i)
		if checkShape {
			if err := validateEmitInputShape(in, limits); err != nil {
				return err
			}
		}
		total += emitStoredBytes(in)
		if total > limits.total {
			return fmt.Errorf("%w: emission batch exceeds byte budget", ErrInvalid)
		}
	}
	return nil
}

func (h *Handler) validateEmissionPayloads(ctx context.Context, count int, input func(int) EmitInput) (map[uint32]Type, map[uint32][]int, []uint32, error) {
	types := make(map[uint32]Type)
	groups := make(map[uint32][]int)
	var order []uint32
	validated := make(map[sourcePreparationKey]bool)
	for index := range count {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		in := input(index)
		t, known := types[in.Type]
		if !known {
			var ok bool
			t, ok = h.registry.get(in.Type)
			if !ok {
				return nil, nil, nil, ErrUnknownType
			}
			types[in.Type] = t
			order = append(order, in.Type)
		}
		key := sourcePreparationKey{in.Type, string(in.Payload)}
		if !validated[key] {
			if err := t.Validate(ctx, in.Payload); err != nil {
				return nil, nil, nil, safeCallbackError("validate", err)
			}
			validated[key] = true
		}
		groups[in.Type] = append(groups[in.Type], index)
	}
	return types, groups, order, nil
}

// materializeMany writes an entire batch atomically with the ambient transaction,
// preserving the same preparation pipeline for inline and worker calls. Denied recipients are skipped. Any validation,
// callback, or database error aborts the whole batch and returns no partial results.
// Limits are 1,000 candidates and notification_emit_max_total_bytes of
// cumulative stored input bytes per call, from the caller's operation sample.
func (h *Handler) materializeMany(ctx context.Context, inputs []materializationInput, checkResolution bool, limits emissionLimits) (results []EmitReceipt, err error) {
	defer func() {
		if h != nil {
			if metrics := h.telemetry.batch; metrics != nil {
				outcome := "returned"
				if err != nil {
					outcome = "error"
				}
				metrics.RecordNotificationEmitBatchRecipients(outcome, len(inputs))
			}
		}
	}()
	inputAt := func(i int) EmitInput { return inputs[i].EmitInput }
	if err := admitEmission(len(inputs), inputAt, limits, true); err != nil {
		return nil, err
	}
	results = make([]EmitReceipt, len(inputs))
	for i := range results {
		results[i].Mode = FanoutInline
	}
	if len(inputs) == 0 {
		return results, nil
	}
	types, groups, typeOrder, err := h.validateEmissionPayloads(ctx, len(inputs), inputAt)
	if err != nil {
		return nil, err
	}
	deliveryPreferences := make([]DeliveryPreference, len(inputs))
	for _, id := range typeOrder {
		indices := groups[id]
		candidates := make([]SourceCandidate, len(indices))
		for index, inputIndex := range indices {
			input := &inputs[inputIndex]
			candidates[index] = SourceCandidate{OrganizationID: input.OrganizationID, RecipientID: input.RecipientID, Payload: input.Payload}
		}
		decisions, e := h.evaluateType(ctx, types[id], candidates, SourceOptions{CheckResolution: checkResolution, IncludeDeliveryPreference: types[id].DeliveryPreference != nil}, limits.source)
		if e != nil {
			return nil, e
		}
		for index, inputIndex := range indices {
			results[inputIndex].Allowed = decisions[index].Authorized && !decisions[index].Resolved
			if results[inputIndex].Allowed && types[id].DeliveryPreference != nil {
				preference, err := h.sourceDeliveryPreference(ctx, types[id], candidates[index], decisions[index].DeliveryPreference)
				if err != nil {
					return nil, err
				}
				deliveryPreferences[inputIndex] = preference
			}
		}
	}
	policies := make(map[uint32]config.NotificationPolicy)
	// While email is off, no job is scheduled; qualifying activity after the
	// gate reopens schedules or re-arms the episode's delivery.
	emailGateOpen := h.emailGateOpen()
	preparedSources := make(map[sourcePreparationKey]sourcePreparation)
	now := h.now()
	records := make([]persistence.NotificationRecordInput, 0, len(inputs))
	recordIndices := make([]int, 0, len(inputs))
	for index, input := range inputs {
		if !results[index].Allowed {
			continue
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		t := types[input.Type]
		policy, exists := policies[input.Type]
		if !exists {
			policy, err = h.policy(ctx, t)
			if err != nil {
				return nil, err
			}
			policies[input.Type] = policy
		}
		key := sourcePreparationKey{input.Type, string(input.Payload)}
		prepared, exists := preparedSources[key]
		if !exists {
			if t.GroupKey != nil && (t.ReusableGroup || policy.GroupWhileUnread) {
				prepared.group, err = t.GroupKey(ctx, input.Payload)
				if err != nil {
					return nil, safeCallbackError("group_key", err)
				}
			}
			if t.ActivityScope != nil {
				prepared.scope, err = t.ActivityScope(ctx, input.Payload)
				if err != nil {
					return nil, safeCallbackError("activity_scope", err)
				}
			}
			if (t.ReusableGroup && prepared.group == "") || len(prepared.group) > config.MaxNotificationGroupKeyBytes || (prepared.scope != "" && !notificationkey.ValidActivityScope(prepared.scope)) {
				return nil, fmt.Errorf("%w: invalid notification group or activity scope", ErrInvalid)
			}
			preparedSources[key] = prepared
		}
		record := persistence.NotificationRecordInput{OrganizationID: input.OrganizationID, UserID: input.RecipientID, Type: input.Type, SourceEventKey: input.SourceKey, Payload: input.Payload, FallbackText: input.FallbackText, GroupKey: prepared.group, ActivityScope: prepared.scope, Now: now}
		record.ReusableGroup = t.ReusableGroup
		record.SourceStamp = input.sourceStamp
		record.SourceTime = input.SourceTime
		record.DeliveryTag = input.DeliveryTag
		// The sampled current preference only re-arms waiting delivery; it never
		// changes the immutable tag or inbox eligibility.
		record.DeliveryFilter = deliveryPreferences[index].Tag
		record.DeliverySuppressed = deliveryPreferences[index].Suppressed
		record.FreshCheck = input.freshCheck
		record.SkipRetirementFence = input.skipRetirementFence
		if emailGateOpen && policy.EmailMode != "disabled" && t.RenderEmail != nil {
			due := now
			if policy.EmailMode == "delayed" {
				due = due.Add(policy.EmailDelay)
			}
			record.EmailDueAt = &due
		}
		records = append(records, record)
		recordIndices = append(recordIndices, index)
	}
	if len(records) == 0 {
		return results, nil
	}
	// The callback belongs to the batch transaction. A later write/hook error must
	// discard every earlier mutation and post-commit hook in this emission batch.
	afterRecord := func(tx context.Context, stored []persistence.NotificationRecordResult) error {
		if len(stored) != len(records) {
			return recordCardinalityError(len(records), len(stored))
		}
		for _, record := range stored {
			if record.Entry == nil && (!record.Suppressed || record.Changed) {
				return appLogger.InvariantError("notification_record_result_has_entry", errors.New("notification record result is missing its entry"), zap.String(logkeys.Stage, "record_many"))
			}
		}
		for _, record := range stored {
			for i := range record.Retired {
				h.publish(tx, &record.Retired[i])
			}
			if record.Changed {
				h.publish(tx, record.Entry)
			}
		}
		return nil
	}
	stored, err := h.store.RecordMany(ctx, records, afterRecord)
	if err != nil {
		return nil, err
	}
	if len(stored) != len(records) {
		return nil, recordCardinalityError(len(records), len(stored))
	}
	for index, record := range stored {
		results[recordIndices[index]].Entry = record.Entry
		if record.Entry == nil {
			results[recordIndices[index]].Allowed = false
		}
		results[recordIndices[index]].Changed = record.Changed
	}
	return results, nil
}

func recordCardinalityError(expected, actual int) error {
	return appLogger.InvariantError("notification_record_result_cardinality", errors.New("notification store returned mismatched batch results"), zap.String(logkeys.Stage, "record_many"), zap.Int(logkeys.Expected, expected), zap.Int(logkeys.Actual, actual))
}
