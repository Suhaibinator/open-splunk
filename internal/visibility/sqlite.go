package visibility

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	reservationReserved  = "reserved"
	reservationCommitted = "committed"
	maxBatchKeyBytes     = 512
	maxAttemptIDBytes    = 128
)

// SQLiteSequencer persists sequence allocation and the highest committed
// boundary in the single-node control database. One sequencer must be shared
// by every Store in a server process.
type SQLiteSequencer struct {
	db *sql.DB
}

var _ Sequencer = (*SQLiteSequencer)(nil)

// NewSQLite constructs the process's sequencer over an already-open, migrated
// control DB. It releases attempt leases left by a previous process crash.
// The single-node server must call NewSQLite exactly once during startup; it is
// not a distributed lease and does not permit two server processes to ingest
// against the same control database concurrently.
func NewSQLite(ctx context.Context, db *control.DB) (*SQLiteSequencer, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if db == nil || db.SQLDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", ErrInvalidArgument)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET attempt_id = ''
		WHERE state = 'reserved' AND attempt_id <> ''`); err != nil {
		return nil, fmt.Errorf("recover visibility attempt leases: %w", err)
	}
	return &SQLiteSequencer{db: db.SQLDB()}, nil
}

// Reserve atomically acquires an attempt lease for an existing pending batch
// or allocates the next sequence. A different batch cannot be allocated while
// any sequence is unresolved. That barrier keeps ClickHouse's deduplication
// token at the head of the retry window and prevents a late ambiguous insert
// from becoming visible to a snapshot captured in the meantime.
func (sequencer *SQLiteSequencer) Reserve(ctx context.Context, request ReserveRequest) (Reservation, error) {
	if err := validateReserveRequest(ctx, request); err != nil {
		return Reservation{}, err
	}
	indexTime := request.IndexTime.Round(0).UTC()
	indexTimeMillis := indexTime.UnixMilli()
	if !time.UnixMilli(indexTimeMillis).UTC().Equal(indexTime.Truncate(time.Millisecond)) {
		return Reservation{}, fmt.Errorf("%w: index time is outside the persistent timestamp range", ErrInvalidArgument)
	}

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Reservation{}, fmt.Errorf("begin visibility reservation: %w", err)
	}
	defer rollback(tx)

	var (
		sequence              int64
		state                 string
		attemptID             string
		storedIndexTimeMillis int64
		storedDigest          []byte
		storedMetadata        []byte
	)
	err = tx.QueryRowContext(ctx, `
		SELECT sequence, state, attempt_id, index_time_unix_milli, payload_sha256, metadata
		FROM ingest_visibility_reservations
		WHERE batch_key = ?`, request.BatchKey).Scan(
		&sequence, &state, &attemptID, &storedIndexTimeMillis, &storedDigest, &storedMetadata,
	)
	if err == nil {
		if !slices.Equal(storedDigest, request.PayloadSHA256[:]) {
			return Reservation{}, ErrConflict
		}
		reservation := Reservation{
			Sequence:  uint64(sequence),
			IndexTime: time.UnixMilli(storedIndexTimeMillis).UTC(),
			Metadata:  slices.Clone(storedMetadata),
		}
		switch state {
		case reservationCommitted:
			reservation.AlreadyCommitted = true
			if err := tx.Commit(); err != nil {
				return Reservation{}, fmt.Errorf("commit visibility reservation lookup: %w", err)
			}
			return reservation, nil
		case reservationReserved:
			if attemptID != "" {
				return Reservation{}, ErrAttemptInProgress
			}
			if err := assertHeadReservation(ctx, tx, sequence); err != nil {
				return Reservation{}, err
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE ingest_visibility_reservations
				SET attempt_id = ?
				WHERE sequence = ? AND state = 'reserved' AND attempt_id = ''`,
				request.AttemptID, sequence)
			if err != nil {
				return Reservation{}, fmt.Errorf("acquire visibility attempt lease: %w", err)
			}
			if err := requireOneRow(result, "acquire visibility attempt lease"); err != nil {
				return Reservation{}, err
			}
			reservation.PreviouslyReserved = true
			if err := tx.Commit(); err != nil {
				return Reservation{}, fmt.Errorf("commit visibility attempt lease: %w", err)
			}
			return reservation, nil
		default:
			return Reservation{}, fmt.Errorf("visibility reservation has invalid state %q", state)
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Reservation{}, fmt.Errorf("read visibility reservation: %w", err)
	}

	sequence, err = allocateHeadSequence(ctx, tx)
	if err != nil {
		return Reservation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, attempt_id, index_time_unix_milli, payload_sha256, metadata)
		VALUES (?, ?, 'reserved', ?, ?, ?, ?)`,
		sequence, request.BatchKey, request.AttemptID, indexTimeMillis, request.PayloadSHA256[:], request.Metadata); err != nil {
		return Reservation{}, fmt.Errorf("persist visibility reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, fmt.Errorf("commit visibility reservation: %w", err)
	}
	return Reservation{
		Sequence:  uint64(sequence),
		IndexTime: time.UnixMilli(indexTimeMillis).UTC(),
		Metadata:  slices.Clone(request.Metadata),
	}, nil
}

func validateReserveRequest(ctx context.Context, request ReserveRequest) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if request.BatchKey == "" || len(request.BatchKey) > maxBatchKeyBytes || !utf8.ValidString(request.BatchKey) {
		return fmt.Errorf("%w: batch key must contain 1 to %d valid UTF-8 bytes", ErrInvalidArgument, maxBatchKeyBytes)
	}
	if request.AttemptID == "" || len(request.AttemptID) > maxAttemptIDBytes || !utf8.ValidString(request.AttemptID) {
		return fmt.Errorf("%w: attempt ID must contain 1 to %d valid UTF-8 bytes", ErrInvalidArgument, maxAttemptIDBytes)
	}
	if request.IndexTime.IsZero() || len(request.Metadata) > MaxMetadataBytes {
		return fmt.Errorf("%w: index time and bounded metadata are required", ErrInvalidArgument)
	}
	return nil
}

func allocateHeadSequence(ctx context.Context, tx *sql.Tx) (int64, error) {
	var lastAssigned, committedThrough int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_assigned, committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned, &committedThrough); err != nil {
		return 0, fmt.Errorf("read visibility sequence state: %w", err)
	}
	if lastAssigned != committedThrough {
		return 0, ErrPendingBarrier
	}
	if lastAssigned == math.MaxInt64 {
		return 0, ErrExhausted
	}
	next := lastAssigned + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = ?
		WHERE singleton = 1 AND last_assigned = ? AND committed_through = ?`, next, lastAssigned, committedThrough)
	if err != nil {
		return 0, fmt.Errorf("advance last visibility sequence: %w", err)
	}
	if err := requireOneRow(result, "advance last visibility sequence"); err != nil {
		return 0, err
	}
	return next, nil
}

func assertHeadReservation(ctx context.Context, tx *sql.Tx, sequence int64) error {
	var lastAssigned, committedThrough int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_assigned, committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned, &committedThrough); err != nil {
		return fmt.Errorf("read visibility sequence state: %w", err)
	}
	if sequence != committedThrough+1 || lastAssigned != sequence {
		return fmt.Errorf("visibility reservation %d is not the only sequence above cutoff %d", sequence, committedThrough)
	}
	return nil
}

// Commit marks the leased reservation visible. With exactly one outstanding
// sequence, advancing the cutoff is constant-time and cannot accumulate a
// large transactional scan.
func (sequencer *SQLiteSequencer) Commit(ctx context.Context, sequence uint64, attemptID string) error {
	return sequencer.finish(ctx, sequence, attemptID, true)
}

// Release relinquishes an attempt after any pre-Send or ambiguous Send error.
// It deliberately leaves the durable reservation unresolved; only a retry of
// the same batch may acquire it and make further visibility progress.
func (sequencer *SQLiteSequencer) Release(ctx context.Context, sequence uint64, attemptID string) error {
	return sequencer.finish(ctx, sequence, attemptID, false)
}

func (sequencer *SQLiteSequencer) finish(ctx context.Context, sequence uint64, attemptID string, commit bool) error {
	if err := validateAttempt(ctx, sequence, attemptID); err != nil {
		return err
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin visibility finalization: %w", err)
	}
	defer rollback(tx)

	var state, owner string
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, int64(sequence)).Scan(&state, &owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read visibility reservation state: %w", err)
	}
	if commit && state == reservationCommitted {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit visibility idempotency lookup: %w", err)
		}
		return nil
	}
	if state != reservationReserved || owner != attemptID {
		return ErrAttemptLease
	}

	if !commit {
		result, err := tx.ExecContext(ctx, `
			UPDATE ingest_visibility_reservations
			SET attempt_id = ''
			WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`, int64(sequence), attemptID)
		if err != nil {
			return fmt.Errorf("release visibility attempt lease: %w", err)
		}
		if err := requireOneRow(result, "release visibility attempt lease"); err != nil {
			return err
		}
	} else {
		if err := assertHeadReservation(ctx, tx, int64(sequence)); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE ingest_visibility_reservations
			SET state = 'committed', attempt_id = ''
			WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`, int64(sequence), attemptID)
		if err != nil {
			return fmt.Errorf("commit visibility reservation: %w", err)
		}
		if err := requireOneRow(result, "commit visibility reservation"); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `
			UPDATE ingest_visibility_state
			SET committed_through = ?
			WHERE singleton = 1 AND committed_through = ? AND last_assigned = ?`,
			int64(sequence), int64(sequence)-1, int64(sequence))
		if err != nil {
			return fmt.Errorf("advance visibility cutoff: %w", err)
		}
		if err := requireOneRow(result, "advance visibility cutoff"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit visibility finalization: %w", err)
	}
	return nil
}

func validateAttempt(ctx context.Context, sequence uint64, attemptID string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if sequence == 0 || sequence > math.MaxInt64 || attemptID == "" || len(attemptID) > maxAttemptIDBytes || !utf8.ValidString(attemptID) {
		return fmt.Errorf("%w: sequence or attempt ID is invalid", ErrInvalidArgument)
	}
	return nil
}

// Cutoff returns the highest committed visibility boundary in O(1) SQLite
// work. An unresolved insert holds the boundary until that exact batch retries.
func (sequencer *SQLiteSequencer) Cutoff(ctx context.Context) (uint64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	var cutoff int64
	if err := sequencer.db.QueryRowContext(ctx, `
		SELECT committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&cutoff); err != nil {
		return 0, fmt.Errorf("read visibility cutoff: %w", err)
	}
	return uint64(cutoff), nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func requireOneRow(result sql.Result, operation string) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: read affected rows: %w", operation, err)
	}
	if changed != 1 {
		return fmt.Errorf("%s: state changed concurrently", operation)
	}
	return nil
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
