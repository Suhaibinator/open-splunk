// Package visibility provides durable commit sequencing for immutable search
// snapshots. It is intentionally independent of event and index timestamps.
package visibility

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

var (
	// ErrInvalidArgument identifies an invalid dependency, context, batch key,
	// or sequence supplied to the sequencer.
	ErrInvalidArgument = errors.New("visibility sequencer: invalid argument")
	// ErrNotFound means no reservation has the supplied sequence.
	ErrNotFound = errors.New("visibility sequencer: reservation not found")
	// ErrReservationGone means an existing-only acquisition found no active
	// disposition. The caller must retry discovery instead of allocating a new
	// sequence from potentially incomplete replay data.
	ErrReservationGone = errors.New("visibility sequencer: active reservation is gone")
	// ErrExhausted means the SQLite signed-integer sequence space is exhausted.
	ErrExhausted = errors.New("visibility sequencer: sequence space exhausted")
	// ErrConflict means a stable batch key was reused for different normalized
	// event content. Its existing reservation is left untouched.
	ErrConflict = errors.New("visibility sequencer: batch content conflicts with its reservation")
	// ErrPendingCapacity is a transient admission-control failure. The caller
	// may retry after pending reservations have reached a terminal state.
	ErrPendingCapacity = errors.New("visibility sequencer: pending outbox capacity reached")
	// ErrAmbiguousBarrier is a transient freeze while an earlier Send outcome
	// must be resolved before a newer block can safely consume deduplication
	// window capacity.
	ErrAmbiguousBarrier = errors.New("visibility sequencer: ambiguous send requires reconciliation")
	// ErrAttemptInProgress means a live Store call already owns the reservation.
	ErrAttemptInProgress = errors.New("visibility sequencer: batch attempt is already in progress")
	// ErrAttemptLease means the caller does not own the reservation attempt it
	// tried to commit or release.
	ErrAttemptLease = errors.New("visibility sequencer: attempt does not own reservation")
	// ErrClosed means the sequencer owner has begun shutdown and no longer
	// admits visibility operations.
	ErrClosed = errors.New("visibility sequencer: closed")
	// ErrOwnerExists means another live SQLiteSequencer already owns attempt
	// fencing for the supplied control database file. Callers must share that
	// owner.
	ErrOwnerExists = errors.New("visibility sequencer: database file already has a live owner")
)

const (
	// MaxMetadataBytes bounds compact server-derived replay metadata.
	MaxMetadataBytes = 1 << 20
	// MaxOutboxBytes bounds the durable replay payload for one reservation.
	MaxOutboxBytes = 16 << 20
	// MaxPendingReservations bounds unresolved visibility reservations.
	MaxPendingReservations = 64
	// MaxPendingOutboxBytes bounds all unresolved replay payloads together.
	MaxPendingOutboxBytes = 256 << 20
	// MaxPruneLimit bounds work performed by one terminal-ledger prune call.
	MaxPruneLimit = 10_000
)

// ReserveRequest carries the deterministic event identity and server-derived
// metadata needed to reproduce one ClickHouse block after a restart.
type ReserveRequest struct {
	BatchKey    string
	SequenceKey string
	AttemptID   string
	// ExistingOnly makes Reserve an atomic acquire-or-replay operation. It may
	// return an existing pending, committed, or rejected disposition, but never
	// allocates a new sequence. IndexTime, Metadata, and Outbox are ignored in
	// this mode because the durable reservation is their source of truth.
	ExistingOnly  bool
	IndexTime     time.Time
	PayloadSHA256 [32]byte
	Metadata      []byte
	Outbox        []byte
	// QuotaAdmission is present only for fresh normalized ingestion. It is
	// ignored by existing-only and active durable replay paths. Nil preserves
	// the legacy non-quota reservation contract.
	QuotaAdmission   *ingestquota.Admission
	QuotaEvaluatedAt time.Time
}

// RejectRequest carries the stable event identity and compact server-derived
// response metadata needed to durably replay one terminal whole-batch
// rejection. Rejections never create a ClickHouse outbox or acquire an attempt
// lease.
type RejectRequest struct {
	BatchKey      string
	SequenceKey   string
	IndexTime     time.Time
	PayloadSHA256 [32]byte
	Metadata      []byte
	RejectedAt    time.Time
}

// Reservation is the durable sequence assigned to one stable batch key.
// AlreadyCommitted allows an idempotent retry to avoid another ClickHouse
// insert and report the batch as duplicate.
type Reservation struct {
	BatchKey         string
	SequenceKey      string
	Sequence         uint64
	AlreadyCommitted bool
	// Rejected is true only for a durable terminal whole-batch rejection.
	Rejected bool
	// NewlyRejected is true only when this Reject call inserted the terminal
	// rejection. It is call-scoped rather than persisted, so Lookup, Reserve,
	// and an exact Reject replay always return false.
	NewlyRejected bool
	// PreviouslyReserved is true only when this call reacquired a still-pending
	// reservation after its previous attempt lease was released or recovered.
	PreviouslyReserved bool
	// MayHaveReachedStorage is true once an owning Store durably marked the
	// reservation immediately before calling ClickHouse Send. Such a sequence
	// must never be abandoned because a late insert may still complete.
	MayHaveReachedStorage bool
	IndexTime             time.Time
	PayloadSHA256         [32]byte
	Metadata              []byte
	Outbox                []byte
	CommittedAt           time.Time
	RejectedAt            time.Time
}

// PendingUsage reports durable reservations that have not reached a terminal
// state, including reservations currently owned by a live attempt.
type PendingUsage struct {
	Reservations uint32
	OutboxBytes  uint64
}

// TerminalRetention independently bounds successful ClickHouse commits and
// whole-batch rejections. RejectedMetadataBytes optionally applies a second
// ceiling to the metadata retained by the newest rejected outcomes; zero leaves
// that ceiling disabled. Rejected outcomes must not displace successful commits
// from the storage deduplication horizon.
type TerminalRetention struct {
	Committed             uint64
	Rejected              uint64
	RejectedMetadataBytes uint64
}

// Sequencer establishes one persistent total order across all Store instances
// sharing a single-node control database.
type Sequencer interface {
	Lookup(context.Context, string, string, [32]byte) (Reservation, bool, error)
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Reject(context.Context, RejectRequest) (Reservation, error)
	AcquirePending(context.Context, string) (Reservation, bool, error)
	PendingUsage(context.Context) (PendingUsage, error)
	MarkSending(context.Context, uint64, string) error
	Commit(context.Context, uint64, string, time.Time) error
	Release(context.Context, uint64, string) error
	Abandon(context.Context, uint64, string) error
	Cutoff(context.Context) (uint64, error)
	PruneTerminal(context.Context, TerminalRetention, uint32) (uint32, error)
}
