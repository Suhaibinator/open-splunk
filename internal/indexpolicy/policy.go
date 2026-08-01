// Package indexpolicy owns the dependency-neutral ingestion policy shared by
// the SQLite control plane, authorization snapshots, native ingestion, and
// ClickHouse retention metadata.
package indexpolicy

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

const (
	// DefaultRetention is the deployment default used when an index keeps the
	// zero-valued inheritance sentinel.
	DefaultRetention = 30 * 24 * time.Hour

	// HardMax* values are the storage and resource-safety ceilings for the
	// optional per-index limits. A zero value inherits the server-wide limit.
	HardMaxEventBytes   uint64 = 1 << 20
	HardMaxFieldCount   uint32 = eventfields.MaximumStoredFieldsPerEvent
	HardMaxNestingDepth uint32 = eventfields.MaximumDynamicPathSegments
	HardMaxEventAge            = 365 * 24 * time.Hour
	HardMaxFutureSkew          = 5 * time.Minute

	MaximumDefaultSourcetypeBytes = 255
)

// Limits contains optional per-event validation limits for one index. A zero
// field inherits the server-wide limit; a nonzero value can only tighten it.
type Limits struct {
	MaxEventBytes     uint64
	MaxFieldCount     uint32
	MaxNestingDepth   uint32
	MaximumFutureSkew time.Duration
	MaximumEventAge   time.Duration
}

// Validate enforces the backend-wide policy ceilings while preserving zero as
// the inheritance sentinel.
func (limits Limits) Validate() error {
	switch {
	case limits.MaxEventBytes > HardMaxEventBytes:
		return fmt.Errorf("max event bytes cannot exceed %d", HardMaxEventBytes)
	case limits.MaxFieldCount > HardMaxFieldCount:
		return fmt.Errorf("max field count cannot exceed %d", HardMaxFieldCount)
	case limits.MaxNestingDepth > HardMaxNestingDepth:
		return fmt.Errorf("max nesting depth cannot exceed %d", HardMaxNestingDepth)
	case limits.MaximumFutureSkew < 0:
		return errors.New("maximum future skew cannot be negative")
	case limits.MaximumFutureSkew > HardMaxFutureSkew:
		return fmt.Errorf("maximum future skew cannot exceed %s", HardMaxFutureSkew)
	case limits.MaximumEventAge < 0:
		return errors.New("maximum event age cannot be negative")
	case limits.MaximumEventAge > HardMaxEventAge:
		return fmt.Errorf("maximum event age cannot exceed %s", HardMaxEventAge)
	default:
		return nil
	}
}

// Policy is one immutable, versioned index-policy snapshot admitted at a
// collector authorization boundary.
type Policy struct {
	Name                string
	Version             uint64
	RetentionPeriod     time.Duration
	DefaultSourcetype   string
	Limits              Limits
	IngestionRateLimits ingestquota.Limits
}

// ResolveRetentionAt validates the complete policy at reference and returns
// its positive effective retention. defaultRetention supplies the inherited
// value when RetentionPeriod is zero.
func (policy Policy) ResolveRetentionAt(
	reference time.Time,
	defaultRetention time.Duration,
) (time.Duration, error) {
	if err := policy.ValidateStoredAt(reference); err != nil {
		return 0, err
	}
	retention := policy.RetentionPeriod
	if retention == 0 {
		retention = defaultRetention
	}
	if err := ValidateRetentionAt(retention, reference, false); err != nil {
		return 0, err
	}
	return retention, nil
}

// ValidateStoredAt validates a persisted policy whose zero retention remains
// an unresolved deployment-default sentinel.
func (policy Policy) ValidateStoredAt(reference time.Time) error {
	if !ValidName(policy.Name) {
		return errors.New("index name is not canonical")
	}
	if policy.Version == 0 || policy.Version > math.MaxInt64 {
		return errors.New("index version is outside the supported range")
	}
	if !ValidDefaultSourcetype(policy.DefaultSourcetype) {
		return errors.New("default sourcetype is invalid")
	}
	if err := policy.Limits.Validate(); err != nil {
		return err
	}
	if err := policy.IngestionRateLimits.Validate(); err != nil {
		return err
	}
	return ValidateRetentionAt(policy.RetentionPeriod, reference, true)
}

// ValidateRetentionAt verifies the DateTime64(3)-compatible retention
// contract at the server-owned index-time reference. allowZero preserves the
// persisted inheritance sentinel; resolved values must be positive.
func ValidateRetentionAt(value time.Duration, reference time.Time, allowZero bool) error {
	if value == 0 && allowZero {
		return nil
	}
	if value <= 0 {
		return errors.New("retention must be positive")
	}
	if value%time.Millisecond != 0 {
		return errors.New("retention must use whole milliseconds")
	}
	reference = reference.UTC()
	if !searchtimebounds.Supports(reference, reference) {
		return errors.New("retention reference is outside the supported timestamp range")
	}
	indexTime := reference.Truncate(time.Millisecond)
	expiresAt := indexTime.Add(value)
	if !expiresAt.After(indexTime) || expiresAt.After(searchtimebounds.MaximumTime()) {
		return errors.New("retention expiration is outside the supported timestamp range")
	}
	return nil
}

// ValidName reports whether value is already a canonical logical index name.
func ValidName(value string) bool {
	return indexname.ValidCanonical(value)
}

// ValidDefaultSourcetype reports whether value is a canonical optional
// sourcetype suitable for every control and ingestion boundary.
func ValidDefaultSourcetype(value string) bool {
	return len(value) <= MaximumDefaultSourcetypeBytes &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		strings.IndexByte(value, 0) < 0 &&
		!strings.ContainsFunc(value, unicode.IsControl)
}
