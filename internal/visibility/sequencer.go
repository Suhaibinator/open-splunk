// Package visibility provides durable commit sequencing for immutable search
// snapshots. It is intentionally independent of event and index timestamps.
package visibility

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidArgument identifies an invalid dependency, context, batch key,
	// or sequence supplied to the sequencer.
	ErrInvalidArgument = errors.New("visibility sequencer: invalid argument")
	// ErrNotFound means no reservation has the supplied sequence.
	ErrNotFound = errors.New("visibility sequencer: reservation not found")
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
	BatchKey      string
	SequenceKey   string
	AttemptID     string
	IndexTime     time.Time
	PayloadSHA256 [32]byte
	Metadata      []byte
	Outbox        []byte
}

// Reservation is the durable sequence assigned to one stable batch key.
// AlreadyCommitted allows an idempotent retry to avoid another ClickHouse
// insert and report the batch as duplicate.
type Reservation struct {
	BatchKey         string
	SequenceKey      string
	Sequence         uint64
	AlreadyCommitted bool
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
}

// Sequencer establishes one persistent total order across all Store instances
// sharing a single-node control database.
type Sequencer interface {
	Lookup(context.Context, string, string, [32]byte) (Reservation, bool, error)
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	AcquirePending(context.Context, string) (Reservation, bool, error)
	MarkSending(context.Context, uint64, string) error
	Commit(context.Context, uint64, string, time.Time) error
	Release(context.Context, uint64, string) error
	Abandon(context.Context, uint64, string) error
	Cutoff(context.Context) (uint64, error)
	PruneTerminal(context.Context, uint64, uint32) (uint32, error)
}
