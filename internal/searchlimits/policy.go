// Package searchlimits defines the node-wide, administrator-configurable
// limits shared by search admission and ClickHouse execution.
package searchlimits

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type contextKey struct{}

// WithPolicy binds one admitted policy snapshot to an execution context.
func WithPolicy(ctx context.Context, policy Policy) context.Context {
	return context.WithValue(ctx, contextKey{}, policy)
}

// FromContext returns the policy captured by search admission.
func FromContext(ctx context.Context) (Policy, bool) {
	if ctx == nil {
		return Policy{}, false
	}
	policy, ok := ctx.Value(contextKey{}).(Policy)
	return policy, ok && Validate(policy) == nil
}

// Policy is one immutable snapshot captured by a newly admitted search.
type Policy struct {
	MaxRuntime          time.Duration
	MaxMemoryBytes      uint64
	MaxRowsToRead       uint64
	MaxBytesToRead      uint64
	MaxGroupedRows      uint64
	MaxThreads          uint64
	MaxResultRows       uint64
	MaxResultBytes      uint64
	MaxTotalResultBytes uint64
	MaxConcurrent       uint32
	ResultRetention     time.Duration
}

// Range describes the inclusive supported policy envelope.
type Range struct {
	Minimum Policy
	Maximum Policy
}

var (
	defaultPolicy = Policy{
		MaxRuntime:          2 * time.Minute,
		MaxMemoryBytes:      1 << 30,
		MaxRowsToRead:       250_000_000,
		MaxBytesToRead:      64 << 30,
		MaxGroupedRows:      10_001,
		MaxThreads:          4,
		MaxResultRows:       10_000,
		MaxResultBytes:      64 << 20,
		MaxTotalResultBytes: 512 << 20,
		MaxConcurrent:       4,
		ResultRetention:     15 * time.Minute,
	}
	supportedRange = Range{
		Minimum: Policy{
			MaxRuntime:          10 * time.Second,
			MaxMemoryBytes:      64 << 20,
			MaxRowsToRead:       1,
			MaxBytesToRead:      1 << 20,
			MaxGroupedRows:      1,
			MaxThreads:          1,
			MaxResultRows:       1,
			MaxResultBytes:      1 << 20,
			MaxTotalResultBytes: 1 << 20,
			MaxConcurrent:       1,
			ResultRetention:     time.Minute,
		},
		Maximum: Policy{
			MaxRuntime:          24 * time.Hour,
			MaxMemoryBytes:      64 << 30,
			MaxRowsToRead:       10_000_000_000,
			MaxBytesToRead:      16 << 40,
			MaxGroupedRows:      10_000_000,
			MaxThreads:          64,
			MaxResultRows:       10_000_000,
			MaxResultBytes:      4 << 30,
			MaxTotalResultBytes: 64 << 30,
			MaxConcurrent:       256,
			ResultRetention:     30 * 24 * time.Hour,
		},
	}
)

func Default() Policy { return defaultPolicy }

func SupportedRange() Range { return supportedRange }

// Validate rejects policies outside the advertised envelope or whose
// per-result reservation cannot fit inside the manager-wide reservation.
func Validate(policy Policy) error {
	minimum, maximum := supportedRange.Minimum, supportedRange.Maximum
	checks := []struct {
		name       string
		value, min uint64
		max        uint64
	}{
		{"maximum memory bytes", policy.MaxMemoryBytes, minimum.MaxMemoryBytes, maximum.MaxMemoryBytes},
		{"maximum rows to read", policy.MaxRowsToRead, minimum.MaxRowsToRead, maximum.MaxRowsToRead},
		{"maximum bytes to read", policy.MaxBytesToRead, minimum.MaxBytesToRead, maximum.MaxBytesToRead},
		{"maximum grouped rows", policy.MaxGroupedRows, minimum.MaxGroupedRows, maximum.MaxGroupedRows},
		{"maximum threads", policy.MaxThreads, minimum.MaxThreads, maximum.MaxThreads},
		{"maximum result rows", policy.MaxResultRows, minimum.MaxResultRows, maximum.MaxResultRows},
		{"maximum result bytes", policy.MaxResultBytes, minimum.MaxResultBytes, maximum.MaxResultBytes},
		{"maximum total result bytes", policy.MaxTotalResultBytes, minimum.MaxTotalResultBytes, maximum.MaxTotalResultBytes},
		{"maximum concurrent searches", uint64(policy.MaxConcurrent), uint64(minimum.MaxConcurrent), uint64(maximum.MaxConcurrent)},
	}
	for _, check := range checks {
		if check.value < check.min || check.value > check.max {
			return fmt.Errorf("search limits: %s must be between %d and %d", check.name, check.min, check.max)
		}
	}
	if policy.MaxRuntime < minimum.MaxRuntime || policy.MaxRuntime > maximum.MaxRuntime {
		return fmt.Errorf("search limits: maximum runtime must be between %s and %s", minimum.MaxRuntime, maximum.MaxRuntime)
	}
	if policy.ResultRetention < minimum.ResultRetention || policy.ResultRetention > maximum.ResultRetention {
		return fmt.Errorf("search limits: result retention must be between %s and %s", minimum.ResultRetention, maximum.ResultRetention)
	}
	if policy.MaxResultBytes > policy.MaxTotalResultBytes {
		return errors.New("search limits: maximum result bytes cannot exceed total result bytes")
	}
	return nil
}

// Source publishes immutable policies atomically. Store validates before
// publication so Snapshot always returns a valid value.
type Source struct{ current atomic.Pointer[Policy] }

func NewSource(initial Policy) (*Source, error) {
	if err := Validate(initial); err != nil {
		return nil, err
	}
	source := &Source{}
	if err := source.Store(initial); err != nil {
		return nil, err
	}
	return source, nil
}

func (source *Source) Snapshot() Policy {
	if source == nil || source.current.Load() == nil {
		return Default()
	}
	return *source.current.Load()
}

func (source *Source) Store(policy Policy) error {
	if source == nil {
		return errors.New("search limits: policy source is required")
	}
	if err := Validate(policy); err != nil {
		return err
	}
	copy := policy
	source.current.Store(&copy)
	return nil
}
