package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

const DefaultRedactionReplacement = "[REDACTED]"

// Limits are hard ingestion limits advertised during collector negotiation and
// independently enforced against untrusted wire data.
type Limits struct {
	MaxBatchEvents    uint32
	MaxBatchBytes     uint64
	MaxEventBytes     uint64
	MaxFields         uint32
	MaxNestingDepth   uint32
	MaxFieldNameBytes uint32
	MaxIDBytes        uint32
	MaxEventAge       time.Duration
	MaxFutureSkew     time.Duration
}

// DefaultLimits returns conservative single-node limits. Callers may copy and
// adjust the result before constructing a Validator or Service.
func DefaultLimits() Limits {
	return Limits{
		MaxBatchEvents:    1_000,
		MaxBatchBytes:     8 << 20,
		MaxEventBytes:     1 << 20,
		MaxFields:         1_024,
		MaxNestingDepth:   16,
		MaxFieldNameBytes: 256,
		MaxIDBytes:        128,
		MaxEventAge:       365 * 24 * time.Hour,
		MaxFutureSkew:     5 * time.Minute,
	}
}

func (l Limits) validate() error {
	switch {
	case l.MaxBatchEvents == 0:
		return errors.New("max batch events must be positive")
	case l.MaxBatchBytes == 0:
		return errors.New("max batch bytes must be positive")
	case l.MaxEventBytes == 0:
		return errors.New("max event bytes must be positive")
	case l.MaxEventBytes > l.MaxBatchBytes:
		return errors.New("max event bytes cannot exceed max batch bytes")
	case l.MaxFields == 0:
		return errors.New("max fields must be positive")
	case l.MaxNestingDepth == 0:
		return errors.New("max nesting depth must be positive")
	case l.MaxFieldNameBytes == 0:
		return errors.New("max field name bytes must be positive")
	case l.MaxIDBytes == 0:
		return errors.New("max ID bytes must be positive")
	case l.MaxEventAge <= 0:
		return errors.New("max event age must be positive")
	case l.MaxFutureSkew < 0:
		return errors.New("max future skew cannot be negative")
	default:
		return nil
	}
}

// RedactionPolicy adds deployment-specific sensitive field names. Mandatory
// built-in names always remain enabled.
type RedactionPolicy struct {
	AdditionalSensitiveFields []string
	Replacement               string
}

// EventContext contains only server-derived metadata. None of these values are
// accepted from a dynamic collector field.
type EventContext struct {
	ReceivedAt         time.Time
	TimestampReference time.Time
	TenantID           string
	CollectorID        string
	BatchID            string
}

// StoredEvent is the normalized event passed across the storage trust
// boundary. Event is an independent clone of the collector message.
type StoredEvent struct {
	Event       *opensplunkv1.LogEvent
	TenantID    string
	CollectorID string
	BatchID     string
	IndexTime   time.Time
}

// EventError is a permanent, event-scoped validation failure.
type EventError struct {
	Code       opensplunkv1.EventRejectionCode
	Message    string
	Violations []*opensplunkv1.FieldViolation
}

func (e *EventError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return e.Code.String()
}

// Authorization is the result of authenticating one ingestion bearer token.
// CollectorID is optional; when set, the token is bound to that collector.
type Authorization struct {
	SubjectID         string
	TenantID          string
	CollectorID       string
	AuthorizedIndexes []string
}

// Authorizer authenticates a bearer token and resolves its immutable ingestion
// scope for the lifetime of a stream.
type Authorizer interface {
	Authorize(context.Context, string) (Authorization, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(context.Context, string) (Authorization, error)

func (f AuthorizerFunc) Authorize(ctx context.Context, token string) (Authorization, error) {
	return f(ctx, token)
}

// StoreBatch contains only events which passed validation, authorization, and
// mandatory redaction. BatchID and event IDs remain stable across retries.
type StoreBatch struct {
	TenantID      string
	CollectorID   string
	BatchID       string
	BatchSequence uint64
	ReceivedAt    time.Time
	Events        []*StoredEvent
}

// StoreResult is the idempotency contract of EventStore. Accepted plus
// Duplicate must exactly equal the number of supplied events. A successful
// return means the promised durability point has been reached.
type StoreResult struct {
	Accepted            uint32
	Duplicate           uint32
	AcknowledgedThrough *uint64
	CommittedAt         time.Time
}

// EventStore durably stores a normalized batch. Implementations must treat
// stable event IDs idempotently and report duplicates in StoreResult.
type EventStore interface {
	Store(context.Context, StoreBatch) (StoreResult, error)
}

// EventStoreFunc adapts a function to EventStore.
type EventStoreFunc func(context.Context, StoreBatch) (StoreResult, error)

func (f EventStoreFunc) Store(ctx context.Context, batch StoreBatch) (StoreResult, error) {
	return f(ctx, batch)
}

// TransientStoreError marks a failure for which the collector must retain and
// resend the exact same batch.
type TransientStoreError struct {
	Err        error
	Reason     opensplunkv1.RetryBatchReason
	RetryAfter time.Duration
}

func (e *TransientStoreError) Error() string {
	if e == nil || e.Err == nil {
		return "transient store error"
	}
	return fmt.Sprintf("transient store error: %v", e.Err)
}

func (e *TransientStoreError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *TransientStoreError) Temporary() bool { return true }
