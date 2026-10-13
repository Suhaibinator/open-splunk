package notification

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sort"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
)

const (
	FanoutInline = "inline"
	FanoutAsync  = "async"
)

var (
	ErrEmissionConflict        = persistence.ErrNotificationEmissionConflict
	ErrEmissionOperationClosed = errors.New("notification emission operation has ended")
)

type emissionIdentity struct {
	producer     string
	organization int64
	typ          uint32
	source       string
	emissionKey  string
}
type operationEmission struct {
	row        *models.NotificationEmission
	event      bool
	created    bool
	envelope   string
	recipients map[uint64]string
}

type inlineEmissionManifest struct {
	event      bool
	envelope   string
	recipients map[uint64]string
}

// EmissionOperation binds sequential emission calls to one source transaction and
// one enablement/default decision, with modes resolved once per type. It is valid
// only during its callback and is not safe for concurrent use. A failed batch,
// event, callback, or fingerprint check rolls back the operation and invalidates
// all provisional receipts.
type EmissionOperation struct {
	handler           *Handler
	id                string
	defaultMode       string
	forceAsync        bool
	modes             map[uint32]string
	asyncAttempted    bool
	inlineMaxPages    int
	limits            emissionLimits
	active            bool
	failure           error
	ctx               context.Context
	emissions         map[emissionIdentity]*operationEmission
	inlineManifests   map[emissionIdentity]*inlineEmissionManifest
	explicitProducers map[string]Producer
}

// WithEmissionOperation joins the source transaction and finalizes every emission
// fingerprint before returning. A failure rolls back all writes and commit hooks.
func (h *Handler) WithEmissionOperation(ctx context.Context, fn func(context.Context, *EmissionOperation) error) error {
	return h.withEmissionOperation(ctx, false, fn)
}
func (h *Handler) withEmissionOperation(ctx context.Context, forceAsync bool, fn func(context.Context, *EmissionOperation) error) error {
	if h == nil || h.store == nil {
		return ErrDisabled
	}
	cfg := h.deps.Config()
	if !cfg.Enabled {
		return ErrDisabled
	}
	mode := FanoutInline
	if cfg.FanoutAsync {
		mode = FanoutAsync
	}
	if fn == nil {
		return ErrInvalid
	}
	var op *EmissionOperation
	err := h.store.WithTransaction(ctx, func(tx context.Context) error {
		op = &EmissionOperation{
			handler: h, id: uuid.New().String(), defaultMode: mode, forceAsync: forceAsync,
			modes: map[uint32]string{}, emissions: map[emissionIdentity]*operationEmission{},
			inlineMaxPages: cfg.InlineSnapshotMaxPages,
			limits:         emissionLimitsFor(cfg), active: true, ctx: tx,
		}
		defer func() { op.active = false }()
		if err := fn(tx, op); err != nil {
			return err
		}
		if op.failure != nil {
			return op.failure
		}
		store := h.store
		ids := make([]emissionIdentity, 0, len(op.emissions))
		for key := range op.emissions {
			ids = append(ids, key)
		}
		sort.Slice(ids, func(i, j int) bool { return op.emissions[ids[i]].row.ID < op.emissions[ids[j]].row.ID })
		for _, key := range ids {
			emission := op.emissions[key]
			fingerprint := emission.envelope
			if !emission.event {
				fingerprint = manifestFingerprint(emission.recipients)
			}
			if _, err := store.FinalizeNotificationEmission(tx, emission.row.ID, op.id, fingerprint); err != nil {
				return err
			}
		}
		if len(ids) > 0 {
			h.nudgeFanoutAfterCommit(tx)
		}
		return nil
	})
	if op != nil && op.asyncAttempted {
		if err != nil {
			h.observeFanoutEnqueue("error")
		} else {
			for _, emission := range op.emissions {
				outcome := "duplicate"
				if emission.created {
					outcome = "created"
				}
				h.observeFanoutEnqueue(outcome)
			}
		}
	}
	return err
}

// modeFor resolves lazily so application configuration is read at emission time.
// Retaining the effective mode keeps every chunk of a type on the same path.
func (op *EmissionOperation) modeFor(ctx context.Context, typeID uint32) (string, error) {
	if mode, ok := op.modes[typeID]; ok {
		return mode, nil
	}
	typ, ok := op.handler.registry.get(typeID)
	if !ok {
		return "", ErrUnknownType
	}
	mode := ""
	if op.forceAsync {
		mode = FanoutAsync
	} else if typ.FanoutMode != nil {
		var err error
		mode, err = typ.FanoutMode(ctx)
		if err != nil {
			return "", safeCallbackError("fanout_mode", err)
		}
	}
	if mode == "" {
		mode = op.defaultMode
	}
	if mode != FanoutInline && mode != FanoutAsync {
		return "", fmt.Errorf("%w: fanout mode for type %d must be inline, async, or empty", ErrInvalid, typeID)
	}
	op.modes[typeID] = mode
	op.asyncAttempted = op.asyncAttempted || mode == FanoutAsync
	return mode, nil
}

func (h *Handler) observeFanoutEnqueue(outcome string) {
	if m, ok := h.fanoutMetrics(); ok {
		m.RecordNotificationFanoutEnqueue(outcome)
	}
}

func (op *EmissionOperation) check(ctx context.Context) error {
	if op == nil || !op.active {
		return ErrEmissionOperationClosed
	}
	if op.failure != nil {
		return op.failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	original, originalOK := scontext.GetTransaction(op.ctx)
	current, currentOK := scontext.GetTransaction(ctx)
	if originalOK != currentOK || (originalOK && ((original == nil) != (current == nil) || (original != nil && current != nil && original.GetDB() != current.GetDB()))) {
		return fmt.Errorf("%w: emission operation used outside its transaction", ErrInvalid)
	}
	return nil
}

// Emit emits one recipient/source pair. Recipient denial is a successful inline
// receipt with Allowed false and no Entry; async acceptance precedes authorization.
func (h *Handler) Emit(ctx context.Context, in EmitInput) (EmitReceipt, error) {
	var receipt EmitReceipt
	err := h.WithEmissionOperation(ctx, func(tx context.Context, op *EmissionOperation) error {
		var err error
		receipt, err = op.Emit(tx, in)
		return err
	})
	if err != nil {
		return EmitReceipt{}, err
	}
	return receipt, nil
}

// EmitMany returns positional receipts for at most 1,000 candidates and 16 MiB
// of input payloads. Group larger source audiences in WithEmissionOperation.
func (h *Handler) EmitMany(ctx context.Context, inputs []EmitInput) ([]EmitReceipt, error) {
	var receipts []EmitReceipt
	err := h.WithEmissionOperation(ctx, func(tx context.Context, op *EmissionOperation) error {
		var err error
		receipts, err = op.EmitMany(tx, inputs)
		return err
	})
	if err != nil {
		return nil, err
	}
	return receipts, nil
}
func (op *EmissionOperation) Emit(ctx context.Context, in EmitInput) (EmitReceipt, error) {
	receipts, err := op.EmitMany(ctx, []EmitInput{in})
	if err != nil {
		return EmitReceipt{}, err
	}
	return receipts[0], nil
}
func (op *EmissionOperation) EmitMany(ctx context.Context, inputs []EmitInput) (receipts []EmitReceipt, err error) {
	defer func() {
		if op != nil && op.failure == nil && err != nil {
			op.failure = err
		}
	}()
	if err := op.check(ctx); err != nil {
		return nil, err
	}
	if op.explicitProducers == nil {
		op.explicitProducers = make(map[string]Producer)
	}
	if err := op.handler.validateExplicitProducers(inputs, op.explicitProducers); err != nil {
		return nil, err
	}
	// Check the complete batch before splitting so modes cannot multiply its budget.
	if err := admitEmission(len(inputs), func(i int) EmitInput { return inputs[i] }, op.limits, false); err != nil {
		return nil, err
	}
	groups := [2]struct {
		mode    string
		inputs  []EmitInput
		indices []int
	}{{mode: FanoutInline}, {mode: FanoutAsync}}
	for i, in := range inputs {
		mode, err := op.modeFor(ctx, in.Type)
		if err != nil {
			return nil, err
		}
		group := &groups[0]
		if mode == FanoutAsync {
			group = &groups[1]
		}
		group.inputs = append(group.inputs, in)
		group.indices = append(group.indices, i)
	}
	receipts = make([]EmitReceipt, len(inputs))
	for _, group := range groups {
		if len(group.inputs) == 0 {
			continue
		}
		var results []EmitReceipt
		if group.mode == FanoutInline {
			if err := op.recordInlineManifest(group.inputs); err != nil {
				return nil, err
			}
			results, err = op.handler.materializeMany(ctx, materializationInputs(group.inputs), false, op.limits)
		} else {
			results, err = op.enqueueMany(ctx, group.inputs)
		}
		if err != nil {
			return nil, err
		}
		for i, result := range results {
			receipts[group.indices[i]] = result
		}
	}
	return receipts, nil
}

func (op *EmissionOperation) enqueueMany(ctx context.Context, inputs []EmitInput) ([]EmitReceipt, error) {
	if err := op.handler.validateAsyncInputs(ctx, inputs, op.limits); err != nil {
		return nil, err
	}
	receipts := make([]EmitReceipt, len(inputs))
	appends := make(map[uint64][]models.NotificationEmissionRecipient)
	order := make([]int, len(inputs))
	for i := range inputs {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := inputs[order[i]], inputs[order[j]]
		pa, pb := a.Producer, b.Producer
		if pa == "" {
			pa = "application"
		}
		if pb == "" {
			pb = "application"
		}
		if pa != pb {
			return pa < pb
		}
		if a.OrganizationID != b.OrganizationID {
			return a.OrganizationID < b.OrganizationID
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return a.SourceKey < b.SourceKey
	})
	for _, i := range order {
		in := inputs[i]
		producer := in.Producer
		if producer == "" {
			producer = "application"
		}
		key := emissionIdentity{producer, in.OrganizationID, in.Type, in.SourceKey, ""}
		emission := op.emissions[key]
		if emission == nil {
			store := op.handler.store
			sourceTime := in.SourceTime
			if sourceTime.IsZero() {
				sourceTime = op.handler.now()
			}
			row, created, err := store.EnqueueNotificationEmission(ctx, models.NotificationEmission{Producer: producer, OrganizationID: in.OrganizationID, Type: in.Type, SourceKey: in.SourceKey, OperationID: op.id, Payload: bytes.Clone(in.Payload), FallbackText: in.FallbackText, SnapshotReady: true, SourceTime: sourceTime, TraceID: scontext.GetTraceID(ctx)})
			if err != nil {
				return nil, err
			}
			if row == nil || row.ID == 0 {
				return nil, fmt.Errorf("%w: emission store returned no durable identity", ErrInvalid)
			}
			emission = &operationEmission{row: row, created: created, recipients: map[uint64]string{}}
			op.emissions[key] = emission
		}
		if emission.event {
			return nil, fmt.Errorf("%w: event and explicit emission share an operation identity", ErrInvalid)
		}
		content := contentFingerprint(in.Payload, in.FallbackText, in.DeliveryTag)
		added, err := admitRecipientContent(emission.recipients, in.RecipientID, content)
		if err != nil {
			return nil, err
		}
		if added {
			recipient := models.NotificationEmissionRecipient{EmissionID: emission.row.ID, UserID: in.RecipientID, DeliveryTag: in.DeliveryTag}
			if !bytes.Equal(in.Payload, emission.row.Payload) || in.FallbackText != emission.row.FallbackText {
				recipient.Override = true
				recipient.Payload = bytes.Clone(in.Payload)
				recipient.FallbackText = in.FallbackText
			}
			appends[emission.row.ID] = append(appends[emission.row.ID], recipient)
		}
		receipts[i] = EmitReceipt{Mode: FanoutAsync, EmissionID: emission.row.ID}
	}
	ids := make([]uint64, 0, len(appends))
	for id := range appends {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	store := op.handler.store
	for _, id := range ids {
		if err := store.AppendNotificationEmissionRecipients(ctx, id, op.id, appends[id]); err != nil {
			return nil, err
		}
	}
	return receipts, nil
}

// Inline operations retain only a transient manifest. It catches contradictory
// chunks in this operation without claiming cross-operation fingerprint checks.
func (op *EmissionOperation) recordInlineManifest(inputs []EmitInput) error {
	if err := admitEmission(len(inputs), func(i int) EmitInput { return inputs[i] }, op.limits, true); err != nil {
		return err
	}
	if op.inlineManifests == nil {
		op.inlineManifests = make(map[emissionIdentity]*inlineEmissionManifest)
	}
	for _, input := range inputs {
		producer := input.Producer
		if producer == "" {
			producer = "application"
		}
		key := emissionIdentity{producer, input.OrganizationID, input.Type, input.SourceKey, ""}
		manifest := op.inlineManifests[key]
		if manifest == nil {
			manifest = &inlineEmissionManifest{recipients: make(map[uint64]string)}
			op.inlineManifests[key] = manifest
		}
		if manifest.event {
			return fmt.Errorf("%w: event and explicit emission share an operation identity", ErrInvalid)
		}
		content := contentFingerprint(input.Payload, input.FallbackText, input.DeliveryTag)
		if _, err := admitRecipientContent(manifest.recipients, input.RecipientID, content); err != nil {
			return err
		}
	}
	return nil
}

func admitRecipientContent(recipients map[uint64]string, userID uint64, content string) (bool, error) {
	if previous, exists := recipients[userID]; exists {
		if previous != content {
			return false, fmt.Errorf("%w: contradictory recipient content in one emission operation", ErrInvalid)
		}
		return false, nil
	}
	recipients[userID] = content
	return true, nil
}

func (h *Handler) validateAsyncInputs(ctx context.Context, inputs []EmitInput, limits emissionLimits) error {
	inputAt := func(i int) EmitInput { return inputs[i] }
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := admitEmission(len(inputs), inputAt, limits, true); err != nil {
		return err
	}
	_, _, _, err := h.validateEmissionPayloads(ctx, len(inputs), inputAt)
	return err
}

// EmitEvent enqueues a producer envelope in async mode, or runs its paged
// audience and preparation callbacks within the source transaction in inline mode.
// Reserved producers (organization, native_chat) are rejected with ErrInvalid.
func (h *Handler) EmitEvent(ctx context.Context, in EventInput) (EmitReceipt, error) {
	return h.emitEvent(ctx, in, false, false)
}
func (h *Handler) emitEvent(ctx context.Context, in EventInput, forceAsync, internal bool) (EmitReceipt, error) {
	var receipt EmitReceipt
	err := h.withEmissionOperation(ctx, forceAsync, func(tx context.Context, op *EmissionOperation) error {
		var err error
		receipt, err = op.emitEvent(tx, in, internal)
		return err
	})
	if err != nil {
		return EmitReceipt{}, err
	}
	return receipt, nil
}

// EmitEvent is the public event entry point; it cannot name an internal producer.
func (op *EmissionOperation) EmitEvent(ctx context.Context, in EventInput) (EmitReceipt, error) {
	return op.emitEvent(ctx, in, false)
}

// EmitNativeEvent emits through a producer installed with RegisterNativeProducer.
// The capability is constructible only inside common_service, so importing
// applications cannot reach reserved producers through this path either.
func (op *EmissionOperation) EmitNativeEvent(ctx context.Context, cap notificationnative.Capability, in EventInput) (receipt EmitReceipt, err error) {
	if !cap.Valid() {
		err = fmt.Errorf("%w: native emission requires the native capability", ErrInvalid)
		if op != nil && op.failure == nil {
			op.failure = err
		}
		return EmitReceipt{}, err
	}
	return op.emitEvent(ctx, in, true)
}

func (op *EmissionOperation) emitEvent(ctx context.Context, in EventInput, internal bool) (receipt EmitReceipt, err error) {
	defer func() {
		if op != nil && op.failure == nil && err != nil {
			op.failure = err
		}
	}()
	if err := op.check(ctx); err != nil {
		return EmitReceipt{}, err
	}
	producer, err := op.handler.validateEvent(ctx, in, internal, op.limits)
	if err != nil {
		return EmitReceipt{}, err
	}
	if in.SourceTime.IsZero() {
		in.SourceTime = op.handler.now()
	}
	mode, err := op.modeFor(ctx, in.Type)
	if err != nil {
		return EmitReceipt{}, err
	}
	if mode == FanoutInline {
		key := emissionIdentity{in.Producer, in.OrganizationID, in.Type, in.SourceKey, in.EmissionKey}
		fingerprint := eventFingerprint(in)
		if op.inlineManifests == nil {
			op.inlineManifests = make(map[emissionIdentity]*inlineEmissionManifest)
		}
		if previous := op.inlineManifests[key]; previous != nil && (!previous.event || previous.envelope != fingerprint) {
			return EmitReceipt{}, fmt.Errorf("%w: contradictory event content in one emission operation", ErrInvalid)
		}
		op.inlineManifests[key] = &inlineEmissionManifest{event: true, envelope: fingerprint}

		event := ProducerEvent{EventInput: in}
		var cursor []byte
		receipt := EmitReceipt{Mode: FanoutInline}
		seen := map[uint64]bool{}
		seenCursors := map[[sha256.Size]byte]bool{}
		for range op.inlineMaxPages {
			page, err := snapshotResolvedProducer(ctx, producer, event, cursor, op.limits)
			if err != nil {
				return EmitReceipt{}, err
			}
			unique := make([]RecipientCandidate, 0, len(page.Candidates))
			for _, candidate := range page.Candidates {
				if !seen[candidate.RecipientID] {
					seen[candidate.RecipientID] = true
					unique = append(unique, candidate)
				}
			}
			if !page.Done {
				cursorHash := sha256.Sum256(page.Cursor)
				if len(unique) == 0 || seenCursors[cursorHash] {
					return EmitReceipt{}, fmt.Errorf("%w: producer enumeration made no progress", ErrInvalid)
				}
				seenCursors[cursorHash] = true
			}
			inputs, err := prepareResolvedProducer(ctx, producer, event, unique, op.limits)
			if err != nil {
				return EmitReceipt{}, err
			}
			results, err := op.handler.materializeMany(ctx, inputs, false, op.limits)
			if err != nil {
				return EmitReceipt{}, err
			}
			for _, result := range results {
				receipt.Allowed = receipt.Allowed || result.Allowed
				receipt.Changed = receipt.Changed || result.Changed
			}
			if page.Done {
				return receipt, nil
			}
			cursor = page.Cursor
		}
		return EmitReceipt{}, fmt.Errorf("%w: inline producer enumeration exceeds page limit", ErrInvalid)
	}
	fingerprint := eventFingerprint(in)
	key := emissionIdentity{in.Producer, in.OrganizationID, in.Type, in.SourceKey, in.EmissionKey}
	emission := op.emissions[key]
	if emission != nil {
		if !emission.event || emission.envelope != fingerprint {
			return EmitReceipt{}, fmt.Errorf("%w: contradictory event content in one emission operation", ErrInvalid)
		}
	} else {
		store := op.handler.store
		row, created, err := store.EnqueueNotificationEmission(ctx, models.NotificationEmission{Producer: in.Producer, OrganizationID: in.OrganizationID, Type: in.Type, SourceKey: in.SourceKey, EmissionKey: in.EmissionKey, OperationID: op.id, Payload: bytes.Clone(in.Payload), FallbackText: in.FallbackText, Event: true, SourceTime: in.SourceTime, TraceID: scontext.GetTraceID(ctx)})
		if err != nil {
			return EmitReceipt{}, err
		}
		if row == nil || row.ID == 0 {
			return EmitReceipt{}, ErrInvalid
		}
		emission = &operationEmission{row: row, created: created, event: true, envelope: fingerprint}
		op.emissions[key] = emission
	}
	return EmitReceipt{Mode: FanoutAsync, EmissionID: emission.row.ID}, nil
}

func hashField(h hash.Hash, value []byte) {
	_ = binary.Write(h, binary.BigEndian, uint64(len(value)))
	_, _ = h.Write(value)
}

// The delivery tag is always a length-prefixed field, so an untagged input can
// never collide with a tagged one and a changed tag is a changed occurrence.
func contentFingerprint(payload []byte, fallback, deliveryTag string) string {
	h := sha256.New()
	hashField(h, payload)
	hashField(h, []byte(fallback))
	hashField(h, []byte(deliveryTag))
	return hex.EncodeToString(h.Sum(nil))
}
func manifestFingerprint(recipients map[uint64]string) string {
	ids := make([]uint64, 0, len(recipients))
	for id := range recipients {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	h := sha256.New()
	hashField(h, []byte("explicit:1"))
	for _, id := range ids {
		_ = binary.Write(h, binary.BigEndian, id)
		hashField(h, []byte(recipients[id]))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func eventFingerprint(in EventInput) string {
	h := sha256.New()
	hashField(h, []byte("event:1"))
	// Events carry no delivery tag; producers assign tags per recipient.
	hashField(h, []byte(contentFingerprint(in.Payload, in.FallbackText, "")))
	return hex.EncodeToString(h.Sum(nil))
}
func (h *Handler) nudgeFanoutAfterCommit(ctx context.Context) {
	run := func(context.Context) error {
		select {
		case h.fanoutWake <- struct{}{}:
		default:
		}
		return nil
	}
	// A hook-less ambient transaction cannot safely trigger a local wakeup.
	// A slow recovery scan observes its commit; rollback never produces a nudge.
	if persistence.RegisterNotificationPostCommitOrSuppress(ctx, run) {
		return
	}
	_ = run(ctx)
}
