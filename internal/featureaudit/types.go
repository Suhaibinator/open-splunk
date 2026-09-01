// Package featureaudit persists the bounded, identity-free operational events
// emitted by search sharing, retention, scheduled reports, and alerts.
package featureaudit

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

const (
	// MaximumEventsPerTenant is the rolling retention bound for the operational
	// journal. Appending beyond it atomically prunes the oldest retained event.
	MaximumEventsPerTenant = 100_000
	// MaximumListPageSize bounds an internal diagnostic traversal.
	MaximumListPageSize = 200

	defaultObservationTimeout = 5 * time.Second
	maximumObservationTimeout = 30 * time.Second
)

var (
	// ErrCorrupt means persisted journal state violates an invariant preserved
	// by package writes and migration triggers.
	ErrCorrupt = errors.New("featureaudit: persisted state is corrupt")
)

// Clock supplies occurrence times. Tests use a deterministic implementation;
// production defaults to the system clock.
type Clock interface {
	Now() time.Time
}

// FailureCategory is a fixed-cardinality, payload-free append failure label.
// It is safe to place in operational logs and metrics.
type FailureCategory string

const (
	FailureInvalid   FailureCategory = "invalid"
	FailureCapacity  FailureCategory = "capacity"
	FailureTimeout   FailureCategory = "timeout"
	FailureIntegrity FailureCategory = "integrity"
	FailureStorage   FailureCategory = "storage"
	FailureInternal  FailureCategory = "internal"
)

// Failure contains no database values, query text, URLs, secrets, or object
// identities. Runtime observers may safely log it.
type Failure struct {
	Category FailureCategory
}

// Options configures one tenant-scoped journal observer.
type Options struct {
	TenantID           string
	Clock              Clock
	ObservationTimeout time.Duration
	OnFailure          func(Failure)
}

// Record is one immutable, identity-free feature operation.
type Record struct {
	Sequence   uint64
	OccurredAt time.Time
	Event      featureops.Event
}

// Health is a fixed-cardinality view of best-effort observer failures.
type Health struct {
	FailedEvents uint64
	LastFailure  FailureCategory
}

func validateEvent(event featureops.Event) error {
	if !event.Valid() {
		return fmt.Errorf("%w: feature operation is invalid", control.ErrInvalidArgument)
	}
	if event.Items > math.MaxInt64 || event.Bytes > math.MaxInt64 {
		return fmt.Errorf("%w: feature operation aggregate is too large", control.ErrInvalidArgument)
	}
	return nil
}
