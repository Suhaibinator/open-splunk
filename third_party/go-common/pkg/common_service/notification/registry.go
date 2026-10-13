// Package notification implements a persistent inbox with independently controlled interruptions.
package notification

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
)

var (
	ErrDisabled       = errors.New("notifications disabled")
	ErrNotFound       = errors.New("notification not found")
	ErrInvalid        = errors.New("invalid notification")
	ErrUnknownType    = errors.New("unknown notification type")
	ErrRegistryFrozen = errors.New("notification registry frozen")
	// ErrEmailDisabled is returned by PrepareEmail or its send when the transport's
	// configuration gate is closed. The delivery stays dormant without
	// spending an attempt.
	ErrEmailDisabled = errors.New("notification email disabled")
)

type Email struct {
	Subject, Body string
	HTML          bool
}

// DeliveryPreference is one current resource-level interruption decision. It
// never grants access or changes inbox visibility. Tag is at most 64 bytes.
type DeliveryPreference struct {
	Suppressed bool
	Tag        string
}

// Type defines server-owned semantics. Source authorization must consult the
// current source authority through Authorize or EvaluateSources; a type needs at
// least one. The payload stays opaque to the inbox; callbacks own its format.
type Type struct {
	ID            uint32
	Name          string
	DefaultPolicy config.NotificationPolicy
	// FanoutMode resolves inline or async at first emission of this type in an
	// operation. Nil or an empty result inherits notification_fanout_async.
	// The decision is retained across chunks; errors abort the source operation.
	// Callbacks must support concurrent operations and honor cancellation.
	FanoutMode func(context.Context) (string, error)
	Validate   func(context.Context, []byte) error
	// Authorize is the scalar source check. The engine consults it, and Resolved,
	// only when EvaluateSources is nil.
	Authorize func(context.Context, uint64, []byte) (bool, error)
	// EvaluateSources evaluates a bounded group of this type at one current-source
	// observation boundary, including resolution when requested. Results must
	// retain positional order and have exactly one decision per candidate. No
	// decisions are cached.
	EvaluateSources func(context.Context, []SourceCandidate, SourceOptions) ([]SourceDecision, error)
	GroupKey        func(context.Context, []byte) (string, error)
	// ReusableGroup keeps one row per recipient/group across acknowledgments and
	// resolution. GroupKey is mandatory and cannot be disabled by interruption policy.
	ReusableGroup bool
	// ActivityScope returns a notificationkey-compatible scope, or empty for no
	// visibility suppression. GroupKey and source-event keys remain opaque.
	ActivityScope func(context.Context, []byte) (string, error)
	// DeliveryPreference samples resource suppression and occurrence filtering
	// together. A source evaluator can return the same snapshot without another
	// resource read. Alert claims sample this before source authorization to skip
	// ineligible occurrences. Empty Tag permits every occurrence.
	DeliveryPreference func(context.Context, uint64, []byte) (DeliveryPreference, error)
	// Resolved is the scalar resolution check paired with Authorize; an
	// evaluator reports resolution in its decisions instead.
	Resolved    func(context.Context, []byte) (bool, error)
	RenderEmail func(context.Context, *models.Notification) (Email, error)
	// EmailContentUsesPayloadOnly allows a reusable group's same-episode email
	// to survive new revisions with an identical payload. Opt in only when
	// RenderEmail uses the payload and stable recipient/type identity;
	// it must ignore changing fields such as fallback text and occurrence count.
	// Otherwise every revision change requires a fresh render.
	EmailContentUsesPayloadOnly bool
}

type registry struct {
	mu     sync.RWMutex
	frozen bool
	types  map[uint32]Type
}

func (r *registry) register(t Type, native bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	if t.ID == 0 || (!native && t.ID < 1000) || (native && t.ID >= 1000) || t.Name == "" || t.Validate == nil || (t.Authorize == nil && t.EvaluateSources == nil) {
		return fmt.Errorf("%w: type requires valid ID, name, validation and a source authorizer or evaluator", ErrInvalid)
	}
	if t.Resolved != nil && t.Authorize == nil {
		return fmt.Errorf("%w: scalar resolution requires scalar authorization", ErrInvalid)
	}
	if err := t.DefaultPolicy.Validate(); err != nil {
		return err
	}
	if t.ReusableGroup && t.GroupKey == nil {
		return fmt.Errorf("%w: reusable type requires a group key", ErrInvalid)
	}
	if r.types == nil {
		r.types = map[uint32]Type{}
	}
	if _, exists := r.types[t.ID]; exists {
		return fmt.Errorf("%w: duplicate type %d", ErrInvalid, t.ID)
	}
	for _, existing := range r.types {
		if existing.Name == t.Name {
			return fmt.Errorf("%w: duplicate type name", ErrInvalid)
		}
	}
	r.types[t.ID] = t
	return nil
}
func (r *registry) get(id uint32) (Type, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.types[id]
	return t, ok
}
func (r *registry) ids() []uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint32, 0, len(r.types))
	for id := range r.types {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
func (r *registry) freeze() { r.mu.Lock(); defer r.mu.Unlock(); r.frozen = true }
