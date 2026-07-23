package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

const (
	DefaultRedactionReplacement = "[REDACTED]"

	// HardMax* bounds are protocol and resource-safety ceilings. Deployment
	// configuration may tighten them, but must never advertise or accept values
	// which exceed the durable ingestion format's assumptions.
	HardMaxBatchEvents       uint32 = 1_000
	HardMaxBatchBytes        uint64 = 8 << 20
	HardMaxEventBytes        uint64 = 1 << 20
	HardMaxFields            uint32 = eventfields.MaximumStoredFieldsPerEvent
	HardMaxNestingDepth      uint32 = 16
	HardMaxFieldNameBytes    uint32 = 256
	HardMaxIDBytes           uint32 = 128
	HardMaxInFlightBatches   uint32 = 64
	HardMaxStreamsPerSubject uint32 = 16
	// HardMaxDurable*Bytes mirror the bounded server-owned replay formats. A
	// source batch can grow during mandatory redaction and rejection reporting,
	// so normalized representations are checked independently.
	HardMaxDurableOutboxBytes   uint64 = 16 << 20
	HardMaxDurableMetadataBytes uint64 = 1 << 20
	// HardMaxCollectResponseBytes bounds any server-to-collector protobuf
	// before gRPC compression. Oversized diagnostic lists are summarized so a
	// valid permanent rejection can always reach the collector.
	HardMaxCollectResponseBytes uint64 = 2 << 20
	HardMaxEventAge                    = 365 * 24 * time.Hour
	HardMaxFutureSkew                  = 5 * time.Minute
)

// ErrUnauthorized is returned by Authorizer only for invalid, expired,
// revoked, or otherwise forbidden collector credentials. Operational backend
// failures must retain their distinct error so the gRPC boundary can return a
// retryable Unavailable status instead of falsely reporting token revocation.
var ErrUnauthorized = errors.New("ingest: collector credential is unauthorized")

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
		MaxBatchEvents:    HardMaxBatchEvents,
		MaxBatchBytes:     HardMaxBatchBytes,
		MaxEventBytes:     HardMaxEventBytes,
		MaxFields:         HardMaxFields,
		MaxNestingDepth:   HardMaxNestingDepth,
		MaxFieldNameBytes: HardMaxFieldNameBytes,
		MaxIDBytes:        HardMaxIDBytes,
		MaxEventAge:       HardMaxEventAge,
		MaxFutureSkew:     HardMaxFutureSkew,
	}
}

func (l Limits) validate() error {
	switch {
	case l.MaxBatchEvents == 0:
		return errors.New("max batch events must be positive")
	case l.MaxBatchEvents > HardMaxBatchEvents:
		return fmt.Errorf("max batch events cannot exceed hard limit %d", HardMaxBatchEvents)
	case l.MaxBatchBytes == 0:
		return errors.New("max batch bytes must be positive")
	case l.MaxBatchBytes > HardMaxBatchBytes:
		return fmt.Errorf("max batch bytes cannot exceed hard limit %d", HardMaxBatchBytes)
	case l.MaxEventBytes == 0:
		return errors.New("max event bytes must be positive")
	case l.MaxEventBytes > HardMaxEventBytes:
		return fmt.Errorf("max event bytes cannot exceed hard limit %d", HardMaxEventBytes)
	case l.MaxEventBytes > l.MaxBatchBytes:
		return errors.New("max event bytes cannot exceed max batch bytes")
	case l.MaxFields == 0:
		return errors.New("max fields must be positive")
	case l.MaxFields > HardMaxFields:
		return fmt.Errorf("max fields cannot exceed hard limit %d", HardMaxFields)
	case l.MaxNestingDepth == 0:
		return errors.New("max nesting depth must be positive")
	case l.MaxNestingDepth > HardMaxNestingDepth:
		return fmt.Errorf("max nesting depth cannot exceed hard limit %d", HardMaxNestingDepth)
	case l.MaxFieldNameBytes == 0:
		return errors.New("max field name bytes must be positive")
	case l.MaxFieldNameBytes > HardMaxFieldNameBytes:
		return fmt.Errorf("max field name bytes cannot exceed hard limit %d", HardMaxFieldNameBytes)
	case l.MaxIDBytes == 0:
		return errors.New("max ID bytes must be positive")
	case l.MaxIDBytes > HardMaxIDBytes:
		return fmt.Errorf("max ID bytes cannot exceed hard limit %d", HardMaxIDBytes)
	case l.MaxEventAge <= 0:
		return errors.New("max event age must be positive")
	case l.MaxEventAge > HardMaxEventAge:
		return fmt.Errorf("max event age cannot exceed hard limit %s", HardMaxEventAge)
	case l.MaxFutureSkew < 0:
		return errors.New("max future skew cannot be negative")
	case l.MaxFutureSkew > HardMaxFutureSkew:
		return fmt.Errorf("max future skew cannot exceed hard limit %s", HardMaxFutureSkew)
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
	TenantID           string
	CollectorID        string
	BatchID            string
	BatchSequence      uint64
	OriginalEventCount uint32
	// SourceBatchSHA256 is the deterministic hash of the collector's original
	// EventBatch before authorization, validation, redaction, or server-derived
	// timestamps. It remains stable across streams and policy refreshes.
	SourceBatchSHA256 [sha256.Size]byte
	ReceivedAt        time.Time
	Events            []*StoredEvent
	// RejectedEvents is the original, terminal disposition for source events
	// omitted from Events. Durable stores retain it so a lost acknowledgment is
	// reproduced exactly even if authorization or validation policy changes.
	RejectedEvents []*opensplunkv1.EventRejection
}

// StoreBatchIdentity identifies an exact collector wire batch without
// re-running mutable validation or authorization policy.
type StoreBatchIdentity struct {
	TenantID          string
	CollectorID       string
	BatchID           string
	BatchSequence     uint64
	SourceBatchSHA256 [sha256.Size]byte
}

// StoredBatchState reports whether an exact source batch has a durable
// reservation and whether its ClickHouse insert is fully committed.
type StoredBatchState uint8

const (
	StoredBatchNotFound StoredBatchState = iota
	StoredBatchPending
	StoredBatchCommitted
)

// StoreResult is the idempotency contract of EventStore. Accepted plus
// Duplicate must exactly equal StoreBatch.Events; together with
// RejectedEvents they must account for OriginalEventCount. A successful return
// means the promised durability point has been reached.
type StoreResult struct {
	Accepted            uint32
	Duplicate           uint32
	AcknowledgedThrough *uint64
	CommittedAt         time.Time
	OriginalEventCount  uint32
	RejectedEvents      []*opensplunkv1.EventRejection
}

// EventStore durably stores a normalized batch. Implementations must treat its
// stable source-batch identity idempotently and report duplicates in
// StoreResult.
type EventStore interface {
	Store(context.Context, StoreBatch) (StoreResult, error)
}

// RecoverableEventStore exposes durable batch lookup and server-owned replay.
// The ingestion service uses it before applying mutable policy, which makes a
// retried acknowledgment identical to the original decision. Implementations
// must replay only their persisted normalized outbox, never caller-supplied
// events.
type RecoverableEventStore interface {
	EventStore
	LookupBatch(context.Context, StoreBatchIdentity) (StoredBatchState, StoreResult, error)
	ResumeBatch(context.Context, StoreBatchIdentity) (StoreResult, error)
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

// DurableIdentityConflictError reports that a durable batch ID or collector
// sequence was previously bound to different immutable source bytes. It is a
// terminal collector protocol conflict, not a retryable storage failure.
type DurableIdentityConflictError struct {
	Err error
}

func (e *DurableIdentityConflictError) Error() string {
	if e == nil || e.Err == nil {
		return "durable batch identity conflict"
	}
	return fmt.Sprintf("durable batch identity conflict: %v", e.Err)
}

func (e *DurableIdentityConflictError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
