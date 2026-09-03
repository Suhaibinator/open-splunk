// Package visibility provides durable commit sequencing for immutable search
// snapshots. It is intentionally independent of event and index timestamps.
package visibility

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// ErrHECAcknowledgmentCapacity means durable channel or acknowledgment
	// retention is full. Pending acknowledgments are never evicted for capacity.
	ErrHECAcknowledgmentCapacity = errors.New("visibility sequencer: HEC acknowledgment capacity reached")
	// ErrHECRequestCapacity means the retained per-token request ledger is at
	// its durable bound. It is distinct from channel/ACK capacity on the wire.
	ErrHECRequestCapacity = errors.New("visibility sequencer: HEC request capacity reached")
	// ErrHECAdmissionStale means the token purpose, version, state, expiration,
	// profile, or acknowledgment mode changed before durable staging.
	ErrHECAdmissionStale = errors.New("visibility sequencer: HEC admission snapshot is stale")
)

const (
	// MaxMetadataBytes bounds compact server-derived replay metadata.
	MaxMetadataBytes = 1 << 20
	// MaxOutboxBytes bounds the durable replay payload for one reservation.
	MaxOutboxBytes = 16 << 20
	// MaxPendingReservations bounds unresolved visibility reservations.
	MaxPendingReservations = 20_000
	// MaxPendingOutboxBytes bounds all unresolved replay payloads together.
	MaxPendingOutboxBytes = 256 << 20
	// MaxPendingMetadataBytes bounds all unresolved compact response metadata.
	MaxPendingMetadataBytes = 256 << 20
	// MaxReservationRows bounds one admitted logical batch.
	MaxReservationRows = uint32(ingestquota.HardMaxAdmissionEvents)
	// MaxReservationDecodedBytes bounds one admitted logical batch.
	MaxReservationDecodedBytes = ingestquota.HardMaxAdmissionUncompressedBytes
	// MaxWriteGroupRows is the hard physical insert row bound.
	MaxWriteGroupRows = 50_000
	// MaxWriteGroupDecodedBytes is the hard physical insert decoded-byte bound.
	MaxWriteGroupDecodedBytes = 64 << 20
	// MaxWriteGroupMembers is the hard logical membership bound.
	MaxWriteGroupMembers = 10_000
	// MaxPruneLimit bounds work performed by one terminal-ledger prune call.
	MaxPruneLimit = 10_000
	// MaxHECChannelsPerToken bounds retained channel identities for one token.
	MaxHECChannelsPerToken = 256
	// MaxHECAcknowledgmentsPerToken bounds retained acknowledgment rows for one
	// token. Pending rows are never evicted to admit another request.
	MaxHECAcknowledgmentsPerToken = 100_000
	// MaxHECRequestsPerToken independently bounds retained HEC request rows,
	// including requests from tokens which do not enable acknowledgment.
	MaxHECRequestsPerToken = 100_000
	// MaxHECAcknowledgmentsPerQuery bounds public status lookup work.
	MaxHECAcknowledgmentsPerQuery = 1_000
	// HECTerminalRetention is the fixed period for indexed and failed HEC
	// acknowledgment authority. Operational telemetry classifies rows older
	// than this period as expired until bounded cleanup deletes them.
	HECTerminalRetention = 24 * time.Hour
)

// HECAdmissionRequest binds one staged HEC request to its freshly revalidated
// token snapshot and optional channel-scoped acknowledgment. TokenID is the
// stable token record ID, never a secret, prefix, digest, or display name.
type HECAdmissionRequest struct {
	TenantID              string
	TokenID               string
	TokenVersion          uint64
	AuthorizedIndexes     []HECIndexAuthority
	RequestID             string
	Acknowledgment        bool
	AcknowledgmentChannel string
	CreatedAt             time.Time
}

// HECIndexAuthority identifies one selected, already-normalized index policy
// generation which must still be active and ingestion-enabled at commit.
type HECIndexAuthority struct {
	Name    string
	Version uint64
}

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
	// StoredRowCount and DecodedEventBytes are server-derived accounting for
	// the accepted rows represented by Outbox. They are sealed at admission so
	// group formation never needs to decode payload blobs while holding a write
	// transaction.
	StoredRowCount    uint32
	DecodedEventBytes uint64
	// QuotaAdmission is present only for fresh normalized ingestion. It is
	// ignored by existing-only and active durable replay paths. Nil preserves
	// the legacy non-quota reservation contract.
	QuotaAdmission   *ingestquota.Admission
	QuotaEvaluatedAt time.Time
	// HECAdmission, when present on a fresh reservation, is revalidated and
	// allocated in the same SQLite transaction as quota and outbox persistence.
	HECAdmission *HECAdmissionRequest
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
	OutboxSHA256          [32]byte
	StoredRowCount        uint32
	DecodedEventBytes     uint64
	CreatedAt             time.Time
	CommittedAt           time.Time
	RejectedAt            time.Time
	// HECAcknowledgmentID is positive only when this Reserve call allocated the
	// channel-scoped durable acknowledgment in the same transaction.
	HECAcknowledgmentID uint64
	// HECRequestSequence is the positive per-token sequence allocated in the
	// same transaction as this fresh HEC reservation.
	HECRequestSequence uint64
}

// PendingUsage reports durable reservations that have not reached a terminal
// state, including reservations currently owned by a live attempt.
type PendingUsage struct {
	Reservations          uint32
	UngroupedReservations uint32
	ReadyGroups           uint32
	AmbiguousGroups       uint32
	LiveGroupLeases       uint32
	OutboxBytes           uint64
	MetadataBytes         uint64
	OldestPendingAt       time.Time
}

// ValidateBackupDrain proves an offline control-plane snapshot contains no
// unresolved physical insertion authority. Backup callers must already hold
// the stopped-server lock, so no admission can race this read.
func ValidateBackupDrain(ctx context.Context, database interface{ SQLDB() *sql.DB }) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if database == nil || database.SQLDB() == nil {
		return fmt.Errorf("%w: backup database is required", ErrInvalidArgument)
	}
	usage, err := readPendingUsage(ctx, database.SQLDB())
	if err != nil {
		return err
	}
	if usage.Reservations != 0 || usage.UngroupedReservations != 0 ||
		usage.ReadyGroups != 0 || usage.AmbiguousGroups != 0 ||
		usage.LiveGroupLeases != 0 || usage.OutboxBytes != 0 ||
		usage.MetadataBytes != 0 {
		return fmt.Errorf(
			"backup requires a fully drained ingestion ledger: reservations=%d ungrouped=%d ready_groups=%d ambiguous_groups=%d live_group_leases=%d outbox_bytes=%d metadata_bytes=%d",
			usage.Reservations,
			usage.UngroupedReservations,
			usage.ReadyGroups,
			usage.AmbiguousGroups,
			usage.LiveGroupLeases,
			usage.OutboxBytes,
			usage.MetadataBytes,
		)
	}
	return nil
}

// WriteGroupState is the durable physical-insert phase.
type WriteGroupState string

const (
	WriteGroupReady     WriteGroupState = "ready"
	WriteGroupAmbiguous WriteGroupState = "ambiguous"
	WriteGroupCommitted WriteGroupState = "committed"
)

// WriteGroupFillReason is the bounded cause that sealed a new group. Recovery
// is reported for an already-durable group acquired by a later attempt.
type WriteGroupFillReason string

const (
	WriteGroupFillRowTarget    WriteGroupFillReason = "row_target"
	WriteGroupFillByteTarget   WriteGroupFillReason = "byte_target"
	WriteGroupFillHardBoundary WriteGroupFillReason = "hard_boundary"
	WriteGroupFillLinger       WriteGroupFillReason = "linger"
	WriteGroupFillDrain        WriteGroupFillReason = "drain"
	WriteGroupFillRecovery     WriteGroupFillReason = "recovery"
)

// WriteGroupLimits bounds one group and controls when sparse work is sealed.
// Hard limits are validated against the package-wide schema maxima.
type WriteGroupLimits struct {
	TargetRows          uint32
	HardMaxRows         uint32
	TargetDecodedBytes  uint64
	HardMaxDecodedBytes uint64
	MaxMembers          uint32
	MaxLinger           time.Duration
	ForceSeal           bool
}

// WriteGroupMember preserves one logical reservation's place in a physical
// insertion. Reservation owns the authoritative replay metadata and outbox.
type WriteGroupMember struct {
	Ordinal      uint32
	Reservation  Reservation
	RowCount     uint32
	DecodedBytes uint64
	OutboxLength uint64
	OutboxSHA256 [32]byte
}

// WriteGroup is one immutable ordered physical ClickHouse insertion unit.
type WriteGroup struct {
	ID               string
	State            WriteGroupState
	AttemptID        string
	Members          []WriteGroupMember
	RowCount         uint32
	DecodedBytes     uint64
	MembershipSHA256 [32]byte
	FirstSequence    uint64
	LastSequence     uint64
	CreatedAt        time.Time
	SendingAt        time.Time
	CommittedAt      time.Time
	FillReason       WriteGroupFillReason
	NewlyFormed      bool
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

// WriteGroupSequencer adds durable physical-insert grouping while retaining
// the logical reservation API during the staged migration of Store callers.
type WriteGroupSequencer interface {
	Sequencer
	AcquireUngroupedAmbiguous(context.Context, string) (Reservation, bool, error)
	FormOrAcquireWriteGroup(context.Context, string, WriteGroupLimits, time.Time) (WriteGroup, bool, time.Time, error)
	MarkWriteGroupSending(context.Context, string, string) error
	CommitWriteGroup(context.Context, string, string, time.Time) error
	ReleaseWriteGroup(context.Context, string, string) error
}
