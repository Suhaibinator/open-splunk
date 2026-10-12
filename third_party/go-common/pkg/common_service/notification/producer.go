package notification

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/config"
)

var ErrUnknownProducer = errors.New("unknown notification producer")

// EventInput describes a source event without enumerating its audience.
// EmissionKey optionally separates successive async emissions of one source:
// deduplication and conflict checks use it, while occurrences and ResolveSource
// stay keyed by SourceKey, so one recipient still materializes once per source.
type EventInput struct {
	Producer                string
	OrganizationID          int64
	Type                    uint32
	Payload                 []byte
	FallbackText, SourceKey string
	EmissionKey             string
	SourceTime              time.Time
}

// ProducerEvent supplies durable source metadata. Async callbacks have no request actor.
type ProducerEvent struct {
	EventInput
	Async          bool
	SourceStamp    models.NotificationTransactionStamp
	SourceSnapshot models.NotificationVisibilitySnapshot
}

type RecipientCandidate struct {
	RecipientID uint64
	Metadata    []byte
}

type RecipientPage struct {
	Candidates []RecipientCandidate
	Cursor     []byte
	Done       bool
}

// Producer callbacks run inside a store transaction and must honor cancellation,
// tolerate retries and overlapping expired leases, and perform no external side effects.
// Snapshot uses stable keyset pagination. Prepare may remove candidates or rewrite
// payloads, but cannot widen the audience or alter the event's identity.
// Validate returns ErrInvalid for malformed input; operational failures retain
// their own error identity and are never classified as invalid input implicitly.
type Producer struct {
	Name          string
	Types         []uint32
	Validate      func(context.Context, EventInput) error
	Snapshot      func(context.Context, ProducerEvent, []byte) (RecipientPage, error)
	Prepare       func(context.Context, ProducerEvent, []RecipientCandidate) ([]EmitInput, error)
	prepareNative func(context.Context, ProducerEvent, []RecipientCandidate) ([]notificationnative.PreparedInput[EmitInput], error)
	internal      bool // Set by registration; public callers cannot name internal producers.
}

type producerRegistry struct {
	mu      sync.RWMutex
	frozen  bool
	entries map[string]Producer
}

func (r *producerRegistry) register(p Producer, internal bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	if p.Name == "" || len(p.Name) > config.MaxNotificationProducerBytes || p.Validate == nil || p.Snapshot == nil || (p.Prepare == nil && p.prepareNative == nil) || (!internal && len(p.Types) == 0) {
		return fmt.Errorf("%w: producer requires name, types, validation, snapshot and preparation", ErrInvalid)
	}
	if !internal && reservedProducerName(p.Name) {
		return fmt.Errorf("%w: reserved producer name", ErrInvalid)
	}
	p.Types = slices.Clone(p.Types)
	slices.Sort(p.Types)
	p.Types = slices.Compact(p.Types)
	if slices.Contains(p.Types, 0) {
		return ErrInvalid
	}
	if r.entries == nil {
		r.entries = map[string]Producer{}
	}
	if _, ok := r.entries[p.Name]; ok {
		return fmt.Errorf("%w: duplicate producer", ErrInvalid)
	}
	p.internal = internal
	r.entries[p.Name] = p
	return nil
}
func (r *producerRegistry) get(name string) (Producer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.entries[name]
	return p, ok
}
func (r *producerRegistry) freeze()                  { r.mu.Lock(); defer r.mu.Unlock(); r.frozen = true }
func (h *Handler) RegisterProducer(p Producer) error { return h.producers.register(p, false) }

// RegisterNativeProducer installs common_service's trusted database-only
// materialization checks. Importing applications use RegisterProducer instead.
func (h *Handler) RegisterNativeProducer(cap notificationnative.Capability, p Producer, prepare func(context.Context, ProducerEvent, []RecipientCandidate) ([]notificationnative.PreparedInput[EmitInput], error)) error {
	if !cap.Valid() || prepare == nil || p.Prepare != nil {
		return ErrInvalid
	}
	p.prepareNative = prepare
	return h.producers.register(p, true)
}

func reservedProducerName(name string) bool {
	return name == "application" || name == "organization" || name == "native_chat"
}

// validateExplicitProducers checks that every named producer is registered,
// public, and may emit the input's type. The default "application" attribution
// has no registration.
func (h *Handler) validateExplicitProducers(inputs []EmitInput, resolved map[string]Producer) error {
	for _, input := range inputs {
		if input.Producer == "" {
			continue
		}
		if len(input.Producer) > config.MaxNotificationProducerBytes || reservedProducerName(input.Producer) {
			return fmt.Errorf("%w: reserved or invalid explicit producer name", ErrInvalid)
		}
		producer, ok := resolved[input.Producer]
		if !ok {
			producer, ok = h.producers.get(input.Producer)
			if !ok {
				return ErrUnknownProducer
			}
			resolved[input.Producer] = producer
		}
		if producer.internal {
			return fmt.Errorf("%w: reserved or invalid explicit producer name", ErrInvalid)
		}
		if !slices.Contains(producer.Types, input.Type) {
			return fmt.Errorf("%w: producer cannot emit this notification type", ErrInvalid)
		}
	}
	return nil
}

// validateEvent admits internal (reserved) producers only through the native
// entry points. A public EmitEvent naming one is invalid input: it would run
// common_service's enumeration inline in the caller's transaction and bypass the
// organization path's async-only and enablement gates.
func (h *Handler) validateEvent(ctx context.Context, in EventInput, internal bool, limits emissionLimits) (Producer, error) {
	if !internal && reservedProducerName(in.Producer) {
		return Producer{}, fmt.Errorf("%w: reserved producer name", ErrInvalid)
	}
	p, ok := h.producers.get(in.Producer)
	if !ok {
		return Producer{}, ErrUnknownProducer
	}
	if p.internal && !internal {
		return Producer{}, fmt.Errorf("%w: reserved producer name", ErrInvalid)
	}
	if in.OrganizationID < 0 || in.SourceKey == "" || len(in.SourceKey) > config.MaxNotificationSourceKeyBytes || len(in.EmissionKey) > config.MaxNotificationEmissionKeyBytes || len(in.Payload) > limits.payload || len(in.FallbackText) > limits.fallbackText {
		return Producer{}, ErrInvalid
	}
	if len(p.Types) > 0 && !slices.Contains(p.Types, in.Type) {
		return Producer{}, fmt.Errorf("%w: producer cannot emit this notification type", ErrInvalid)
	}
	if _, ok := h.registry.get(in.Type); !ok {
		return Producer{}, ErrUnknownType
	}
	validationInput := in
	validationInput.Payload = bytes.Clone(in.Payload)
	if err := p.Validate(ctx, validationInput); err != nil {
		return Producer{}, safeCallbackError("producer_validate", err)
	}
	return p, nil
}

func (h *Handler) snapshotProducer(ctx context.Context, event ProducerEvent, cursor []byte, limits emissionLimits) (RecipientPage, error) {
	p, ok := h.producers.get(event.Producer)
	if !ok {
		return RecipientPage{}, ErrUnknownProducer
	}
	return snapshotResolvedProducer(ctx, p, event, cursor, limits)
}

func snapshotResolvedProducer(ctx context.Context, p Producer, event ProducerEvent, cursor []byte, limits emissionLimits) (RecipientPage, error) {
	if err := ctx.Err(); err != nil {
		return RecipientPage{}, err
	}
	callbackEvent := event
	callbackEvent.Payload = bytes.Clone(event.Payload)
	page, err := p.Snapshot(ctx, callbackEvent, bytes.Clone(cursor))
	if err != nil {
		return RecipientPage{}, safeCallbackError("producer_snapshot", err)
	}
	if err := ctx.Err(); err != nil {
		return RecipientPage{}, err
	}
	if len(page.Candidates) > maxEmitCandidates || len(page.Cursor) > config.MaxNotificationProducerCursorBytes || (!page.Done && (len(page.Candidates) == 0 || len(page.Cursor) == 0 || bytes.Equal(cursor, page.Cursor))) {
		return RecipientPage{}, fmt.Errorf("%w: invalid producer cursor progress", ErrInvalid)
	}
	total := 0
	seen := make(map[uint64][]byte, len(page.Candidates))
	unique := make([]RecipientCandidate, 0, len(page.Candidates))
	for _, candidate := range page.Candidates {
		total += len(candidate.Metadata)
		if candidate.RecipientID == 0 || len(candidate.Metadata) > limits.metadata || total > limits.total {
			return RecipientPage{}, fmt.Errorf("%w: invalid producer candidate metadata", ErrInvalid)
		}
		if prior, exists := seen[candidate.RecipientID]; exists {
			if !bytes.Equal(prior, candidate.Metadata) {
				return RecipientPage{}, fmt.Errorf("%w: conflicting producer candidate metadata", ErrInvalid)
			}
			continue
		}
		seen[candidate.RecipientID] = candidate.Metadata
		unique = append(unique, candidate)
	}
	page.Candidates = unique
	return page, nil
}

func (h *Handler) prepareProducer(ctx context.Context, event ProducerEvent, candidates []RecipientCandidate, limits emissionLimits) ([]materializationInput, error) {
	p, ok := h.producers.get(event.Producer)
	if !ok {
		return nil, ErrUnknownProducer
	}
	return prepareResolvedProducer(ctx, p, event, candidates, limits)
}

func prepareResolvedProducer(ctx context.Context, p Producer, event ProducerEvent, candidates []RecipientCandidate, limits emissionLimits) ([]materializationInput, error) {
	if len(candidates) > maxEmitCandidates {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Build the identity allow-list before calling application code. Its mutable
	// argument copies cannot widen that list or corrupt the durable envelope.
	allowed := make(map[uint64]bool, len(candidates))
	callbackCandidates := make([]RecipientCandidate, len(candidates))
	for i, candidate := range candidates {
		allowed[candidate.RecipientID] = true
		callbackCandidates[i] = RecipientCandidate{RecipientID: candidate.RecipientID, Metadata: bytes.Clone(candidate.Metadata)}
	}
	callbackEvent := event
	callbackEvent.Payload = bytes.Clone(event.Payload)
	var inputs []materializationInput
	var err error
	if p.prepareNative != nil {
		var native []notificationnative.PreparedInput[EmitInput]
		native, err = p.prepareNative(ctx, callbackEvent, callbackCandidates)
		inputs = make([]materializationInput, len(native))
		for i, input := range native {
			inputs[i] = materializationInput{EmitInput: input.Input, freshCheck: input.FreshCheck, skipRetirementFence: input.SkipRetirementFence}
		}
	} else {
		var public []EmitInput
		public, err = p.Prepare(ctx, callbackEvent, callbackCandidates)
		inputs = materializationInputs(public)
	}
	if err != nil {
		return nil, safeCallbackError("producer_prepare", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(inputs) > len(candidates) {
		return nil, fmt.Errorf("%w: producer preparation widened audience", ErrInvalid)
	}
	// Callback-owned templates may be shared safely across concurrent workers.
	// Never annotate or pass their mutable payload storage to type callbacks.
	seen := make(map[uint64]bool, len(inputs))
	for i := range inputs {
		in := &inputs[i]
		if !allowed[in.RecipientID] || seen[in.RecipientID] || in.Type != event.Type || in.SourceKey != event.SourceKey || in.OrganizationID != event.OrganizationID {
			return nil, fmt.Errorf("%w: producer preparation changed identity or audience", ErrInvalid)
		}
		if len(in.Payload) > limits.payload {
			return nil, ErrInvalid
		}
		seen[in.RecipientID] = true
		in.Payload = bytes.Clone(in.Payload)
		in.Producer = event.Producer
		in.sourceStamp = event.SourceStamp
		in.SourceTime = event.SourceTime
	}
	return inputs, nil
}
