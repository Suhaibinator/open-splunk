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
	// ErrPendingBarrier means another batch has an unresolved ClickHouse insert.
	// The initial single-node sequencer permits only one unresolved sequence so
	// an ambiguous insert can never fall behind the captured visibility cutoff.
	ErrPendingBarrier = errors.New("visibility sequencer: another batch is unresolved")
	// ErrAttemptInProgress means a live Store call already owns the reservation.
	ErrAttemptInProgress = errors.New("visibility sequencer: batch attempt is already in progress")
	// ErrAttemptLease means the caller does not own the reservation attempt it
	// tried to commit or release.
	ErrAttemptLease = errors.New("visibility sequencer: attempt does not own reservation")
)

// MaxMetadataBytes is the durable upper bound for opaque reservation metadata.
// It accommodates 256 maximum-length logical index names plus framing.
const MaxMetadataBytes = 128 << 10

// ReserveRequest carries the deterministic event identity and server-derived
// metadata needed to reproduce one ClickHouse block after a restart.
type ReserveRequest struct {
	BatchKey      string
	AttemptID     string
	IndexTime     time.Time
	PayloadSHA256 [32]byte
	Metadata      []byte
}

// Reservation is the durable sequence assigned to one stable batch key.
// AlreadyCommitted allows an idempotent retry to avoid another ClickHouse
// insert and report the batch as duplicate.
type Reservation struct {
	Sequence         uint64
	AlreadyCommitted bool
	// PreviouslyReserved is true only when this call reacquired a still-pending
	// reservation after its previous attempt lease was released or recovered.
	PreviouslyReserved bool
	IndexTime          time.Time
	Metadata           []byte
}

// Sequencer establishes one persistent total order across all Store instances
// sharing a single-node control database.
type Sequencer interface {
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Commit(context.Context, uint64, string) error
	Release(context.Context, uint64, string) error
	Cutoff(context.Context) (uint64, error)
}
