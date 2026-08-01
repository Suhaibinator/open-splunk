// Package ingestquota owns the dependency-neutral ingestion rate policy and
// virtual-schedule arithmetic shared by control-plane authorization and the
// durable SQLite visibility boundary.
package ingestquota

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	HardMaxEventsPerSecond            uint64 = 1_000_000
	HardMaxUncompressedBytesPerSecond uint64 = 1 << 40
	HardMaxAdmissionEvents            uint64 = 1_000
	HardMaxAdmissionUncompressedBytes uint64 = 8 << 20
	MaximumScopeIdentityBytes                = 255
	MaximumRetryAfter                        = time.Hour
)

// ScopeKind distinguishes a bearer-token budget from a logical-index budget.
type ScopeKind string

const (
	ScopeKindToken ScopeKind = "token"
	ScopeKindIndex ScopeKind = "index"
)

// Limits contains independently optional rates. Zero leaves one dimension
// unlimited rather than blocking ingestion.
type Limits struct {
	MaxEventsPerSecond            uint64
	MaxUncompressedBytesPerSecond uint64
}

func (limits Limits) Validate() error {
	switch {
	case limits.MaxEventsPerSecond > HardMaxEventsPerSecond:
		return fmt.Errorf(
			"max events per second cannot exceed %d",
			HardMaxEventsPerSecond,
		)
	case limits.MaxUncompressedBytesPerSecond >
		HardMaxUncompressedBytesPerSecond:
		return fmt.Errorf(
			"max uncompressed bytes per second cannot exceed %d",
			HardMaxUncompressedBytesPerSecond,
		)
	default:
		return nil
	}
}

// ScopeKey is derived exclusively from trusted authorization state.
type ScopeKey struct {
	Kind     ScopeKind
	TenantID string
	Identity string
}

func (key ScopeKey) Validate() error {
	if key.Kind != ScopeKindToken && key.Kind != ScopeKindIndex {
		return errors.New("quota scope kind is invalid")
	}
	if !validIdentity(key.TenantID) {
		return errors.New("quota tenant identity is invalid")
	}
	if !validIdentity(key.Identity) {
		return errors.New("quota resource identity is invalid")
	}
	return nil
}

func validIdentity(value string) bool {
	return len(value) >= 1 &&
		len(value) <= MaximumScopeIdentityBytes &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		strings.IndexByte(value, 0) < 0
}

// State is the durable virtual schedule for one scope. A nil State on Charge
// means no bucket row exists yet.
type State struct {
	Limits                     Limits
	NextEventAdmissionUnixNano int64
	NextByteAdmissionUnixNano  int64
	UpdatedAtUnixMicro         int64
}

// Charge supplies one scope's current policy, cost, and optional durable
// schedule. Events and UncompressedBytes cover only admitted source events.
type Charge struct {
	Scope             ScopeKey
	Limits            Limits
	Events            uint64
	UncompressedBytes uint64
	State             *State
}

// Admission is one all-or-nothing token plus index quota decision.
type Admission struct {
	Charges []Charge
}

// StateUpdate is persisted only when the complete Admission succeeds.
type StateUpdate struct {
	Scope ScopeKey
	State State
}

// Decision reports either an atomic set of durable updates or the scope which
// determines the longest retry delay.
type Decision struct {
	Allowed       bool
	RetryAfter    time.Duration
	BlockingScope ScopeKey
	Updates       []StateUpdate
}

// ExceededError is the typed data-plane result returned when a durable quota
// transaction cannot admit a fresh batch.
type ExceededError struct {
	Scope      ScopeKey
	RetryAfter time.Duration
}

func (err *ExceededError) Error() string {
	if err == nil {
		return "ingestion quota exceeded"
	}
	return fmt.Sprintf(
		"ingestion %s quota exceeded for %q; retry after %s",
		err.Scope.Kind,
		err.Scope.Identity,
		err.RetryAfter,
	)
}

func (*ExceededError) Temporary() bool { return true }

// Evaluate applies a one-complete-batch virtual schedule. It never mutates the
// supplied admission or states. Every update must be committed atomically by
// the caller; a denied decision contains no updates.
func Evaluate(now time.Time, admission Admission) (Decision, error) {
	now = now.Round(0).UTC()
	nowUnixNano, err := supportedUnixNano(now)
	if err != nil {
		return Decision{}, err
	}
	if len(admission.Charges) == 0 {
		return Decision{}, errors.New("quota admission requires at least one charge")
	}

	seen := make(map[ScopeKey]struct{}, len(admission.Charges))
	normalized := make([]Charge, len(admission.Charges))
	for index, charge := range admission.Charges {
		if err := validateCharge(charge); err != nil {
			return Decision{}, fmt.Errorf("quota charge %d: %w", index, err)
		}
		if _, duplicate := seen[charge.Scope]; duplicate {
			return Decision{}, errors.New("quota admission contains a duplicate scope")
		}
		seen[charge.Scope] = struct{}{}
		normalized[index] = normalizedCharge(charge, nowUnixNano)
	}

	var blocked bool
	var blockingScope ScopeKey
	var retryNanoseconds int64
	for _, charge := range normalized {
		wait := blockingNanoseconds(charge, nowUnixNano)
		if wait <= 0 {
			continue
		}
		if !blocked || wait > retryNanoseconds ||
			wait == retryNanoseconds && scopePrecedes(charge.Scope, blockingScope) {
			blocked = true
			retryNanoseconds = wait
			blockingScope = charge.Scope
		}
	}
	if blocked {
		retryAfter := time.Duration(retryNanoseconds)
		if retryAfter > MaximumRetryAfter {
			retryAfter = MaximumRetryAfter
		}
		return Decision{
			RetryAfter:    retryAfter,
			BlockingScope: blockingScope,
		}, nil
	}

	updates := make([]StateUpdate, 0, len(normalized))
	for index, charge := range normalized {
		if charge.Limits == (Limits{}) {
			prior := admission.Charges[index].State
			if prior != nil && prior.Limits != (Limits{}) {
				updates = append(updates, StateUpdate{
					Scope: charge.Scope,
					State: State{
						UpdatedAtUnixMicro: now.UnixMicro(),
					},
				})
			}
			continue
		}
		state := State{
			Limits:             charge.Limits,
			UpdatedAtUnixMicro: now.UnixMicro(),
		}
		if charge.Limits.MaxEventsPerSecond != 0 {
			advance, durationErr := scheduleDuration(
				charge.Events,
				charge.Limits.MaxEventsPerSecond,
			)
			if durationErr != nil || advance > math.MaxInt64-nowUnixNano {
				return Decision{}, errors.New("quota event schedule overflows persistent time")
			}
			state.NextEventAdmissionUnixNano = nowUnixNano + advance
		}
		if charge.Limits.MaxUncompressedBytesPerSecond != 0 {
			advance, durationErr := scheduleDuration(
				charge.UncompressedBytes,
				charge.Limits.MaxUncompressedBytesPerSecond,
			)
			if durationErr != nil || advance > math.MaxInt64-nowUnixNano {
				return Decision{}, errors.New("quota byte schedule overflows persistent time")
			}
			state.NextByteAdmissionUnixNano = nowUnixNano + advance
		}
		updates = append(updates, StateUpdate{Scope: charge.Scope, State: state})
	}
	return Decision{Allowed: true, Updates: updates}, nil
}

func validateCharge(charge Charge) error {
	if err := charge.Scope.Validate(); err != nil {
		return err
	}
	if err := charge.Limits.Validate(); err != nil {
		return err
	}
	if charge.Events == 0 || charge.Events > HardMaxAdmissionEvents {
		return fmt.Errorf(
			"event charge must be between 1 and %d",
			HardMaxAdmissionEvents,
		)
	}
	if charge.UncompressedBytes == 0 ||
		charge.UncompressedBytes > HardMaxAdmissionUncompressedBytes {
		return fmt.Errorf(
			"byte charge must be between 1 and %d",
			HardMaxAdmissionUncompressedBytes,
		)
	}
	if charge.State == nil {
		return nil
	}
	if err := charge.State.Limits.Validate(); err != nil {
		return fmt.Errorf("persisted quota limits: %w", err)
	}
	if charge.State.UpdatedAtUnixMicro <= 0 {
		return errors.New("persisted quota update time is invalid")
	}
	if !validDimensionState(
		charge.State.Limits.MaxEventsPerSecond,
		charge.State.NextEventAdmissionUnixNano,
	) || !validDimensionState(
		charge.State.Limits.MaxUncompressedBytesPerSecond,
		charge.State.NextByteAdmissionUnixNano,
	) {
		return errors.New("persisted quota schedule is invalid")
	}
	return nil
}

func validDimensionState(rate uint64, next int64) bool {
	if rate == 0 {
		return next == 0
	}
	return next > 0
}

func normalizedCharge(charge Charge, nowUnixNano int64) Charge {
	if charge.State == nil {
		charge.State = &State{
			Limits: charge.Limits,
			NextEventAdmissionUnixNano: enabledNow(
				charge.Limits.MaxEventsPerSecond,
				nowUnixNano,
			),
			NextByteAdmissionUnixNano: enabledNow(
				charge.Limits.MaxUncompressedBytesPerSecond,
				nowUnixNano,
			),
		}
		return charge
	}
	state := *charge.State
	if state.Limits.MaxEventsPerSecond != charge.Limits.MaxEventsPerSecond {
		state.NextEventAdmissionUnixNano = enabledNow(
			charge.Limits.MaxEventsPerSecond,
			nowUnixNano,
		)
	}
	if state.Limits.MaxUncompressedBytesPerSecond !=
		charge.Limits.MaxUncompressedBytesPerSecond {
		state.NextByteAdmissionUnixNano = enabledNow(
			charge.Limits.MaxUncompressedBytesPerSecond,
			nowUnixNano,
		)
	}
	state.Limits = charge.Limits
	charge.State = &state
	return charge
}

func enabledNow(rate uint64, nowUnixNano int64) int64 {
	if rate == 0 {
		return 0
	}
	return nowUnixNano
}

func blockingNanoseconds(charge Charge, nowUnixNano int64) int64 {
	var wait int64
	if charge.Limits.MaxEventsPerSecond != 0 &&
		charge.State.NextEventAdmissionUnixNano > nowUnixNano {
		wait = charge.State.NextEventAdmissionUnixNano - nowUnixNano
	}
	if charge.Limits.MaxUncompressedBytesPerSecond != 0 &&
		charge.State.NextByteAdmissionUnixNano > nowUnixNano &&
		charge.State.NextByteAdmissionUnixNano-nowUnixNano > wait {
		wait = charge.State.NextByteAdmissionUnixNano - nowUnixNano
	}
	return wait
}

func scopePrecedes(left, right ScopeKey) bool {
	if left.Kind != right.Kind {
		return left.Kind == ScopeKindToken
	}
	if left.Identity != right.Identity {
		return left.Identity < right.Identity
	}
	return left.TenantID < right.TenantID
}

func scheduleDuration(cost, rate uint64) (int64, error) {
	if cost == 0 || rate == 0 {
		return 0, errors.New("quota schedule cost and rate must be positive")
	}
	seconds := cost / rate
	if seconds > math.MaxInt64/uint64(time.Second) {
		return 0, errors.New("quota schedule duration overflows")
	}
	nanoseconds := seconds * uint64(time.Second)
	remainder := cost % rate
	if remainder != 0 {
		high, low := bits.Mul64(remainder, uint64(time.Second))
		fraction, leftover := bits.Div64(high, low, rate)
		if leftover != 0 {
			fraction++
		}
		if math.MaxUint64-nanoseconds < fraction {
			return 0, errors.New("quota schedule duration overflows")
		}
		nanoseconds += fraction
	}
	if nanoseconds == 0 || nanoseconds > math.MaxInt64 {
		return 0, errors.New("quota schedule duration is outside persistent bounds")
	}
	return int64(nanoseconds), nil
}

func supportedUnixNano(value time.Time) (int64, error) {
	// Durable state records both a nanosecond schedule and a positive
	// microsecond update timestamp. Values in the first microsecond after the
	// Unix epoch would otherwise validate here and then produce an
	// unpersistable UpdatedAtUnixMicro of zero.
	minimum := time.Unix(0, int64(time.Microsecond)).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	if value.IsZero() || value.Before(minimum) || value.After(maximum) {
		return 0, errors.New("quota evaluation time is outside persistent bounds")
	}
	return value.UnixNano(), nil
}
