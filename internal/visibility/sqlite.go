package visibility

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"modernc.org/sqlite"
)

const (
	reservationReserved  = "reserved"
	reservationCommitted = "committed"
	reservationAbandoned = "abandoned"
	phaseUnsent          = "unsent"
	phaseAmbiguous       = "ambiguous"
	phaseFinal           = "final"
	maxBatchKeyBytes     = 512
	maxSequenceKeyBytes  = 512
	maxAttemptIDBytes    = 128
	// Cleanup is detached from any one Shutdown caller so a short deadline
	// cannot strand process ownership. Its own hard bound prevents a drained
	// owner from retaining the borrowed database behind a stuck pool.
	ownerCleanupTimeout = 10 * time.Second
)

type processLeases struct {
	mu              sync.Mutex
	active          map[string]uint64
	possiblyDurable bool
}

var sqliteOwners = struct {
	sync.Mutex
	bySequencer map[*SQLiteSequencer]*control.DB
}{
	bySequencer: make(map[*SQLiteSequencer]*control.DB),
}

func (leases *processLeases) activate(id string) bool {
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if _, exists := leases.active[id]; exists {
		return false
	}
	leases.active[id] = 0
	// A transaction that follows may persist its attempt ID even if Commit
	// reports an outcome-ambiguous error. Shutdown must therefore perform
	// durable cleanup after any admitted acquisition attempt, not only after a
	// confirmed commit reaches bind.
	leases.possiblyDurable = true
	return true
}

func (leases *processLeases) bind(id string, sequence uint64) {
	leases.mu.Lock()
	if _, exists := leases.active[id]; exists {
		leases.active[id] = sequence
		leases.possiblyDurable = true
	}
	leases.mu.Unlock()
}

func (leases *processLeases) deactivate(id string) {
	leases.mu.Lock()
	delete(leases.active, id)
	leases.mu.Unlock()
}

func (leases *processLeases) contains(id string) bool {
	leases.mu.Lock()
	_, exists := leases.active[id]
	leases.mu.Unlock()
	return exists
}

func (leases *processLeases) owns(id string, sequence uint64) bool {
	leases.mu.Lock()
	ownedSequence, exists := leases.active[id]
	leases.mu.Unlock()
	return exists && ownedSequence == sequence
}

func (leases *processLeases) mayHaveDurableLease() bool {
	leases.mu.Lock()
	defer leases.mu.Unlock()
	return leases.possiblyDurable
}

func (leases *processLeases) clear() {
	leases.mu.Lock()
	clear(leases.active)
	leases.possiblyDurable = false
	leases.mu.Unlock()
}

// SQLiteSequencer persists sequence allocation, replay payloads, and the
// highest contiguous terminal boundary in the single-node control database.
// It is also the explicit process-local owner of every attempt lease over that
// physical SQLite file. All callers sharing that file must share one
// sequencer.
type SQLiteSequencer struct {
	db     *sql.DB
	leases *processLeases

	lifecycleMu sync.Mutex
	closed      bool
	operations  sync.WaitGroup

	shutdownOnce sync.Once
	terminalDone chan struct{}
	terminalErr  error
}

var _ Sequencer = (*SQLiteSequencer)(nil)

// NewSQLite constructs the exclusive process-local sequencer owner over an
// already-open, migrated control DB. Callers using any handle to the same
// physical file must share the returned sequencer rather than construct
// concurrent owners. Production additionally holds a process-wide server lock
// to fence separate processes. Close the owner before replacing it; the
// underlying control database remains caller-owned.
func NewSQLite(ctx context.Context, db *control.DB) (*SQLiteSequencer, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if db == nil || db.SQLDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", ErrInvalidArgument)
	}
	sequencer := &SQLiteSequencer{
		db:           db.SQLDB(),
		leases:       &processLeases{active: make(map[string]uint64)},
		terminalDone: make(chan struct{}),
	}
	if !registerSQLiteOwner(db, sequencer) {
		sequencer.db = nil
		sequencer.leases = nil
		return nil, ErrOwnerExists
	}
	if err := sequencer.clearStaleLeases(ctx); err != nil {
		unregisterSQLiteOwner(sequencer)
		sequencer.db = nil
		sequencer.leases = nil
		return nil, err
	}
	return sequencer, nil
}

func registerSQLiteOwner(db *control.DB, sequencer *SQLiteSequencer) bool {
	sqliteOwners.Lock()
	defer sqliteOwners.Unlock()
	for _, ownerDB := range sqliteOwners.bySequencer {
		if db.SameSQLiteFile(ownerDB) {
			return false
		}
	}
	sqliteOwners.bySequencer[sequencer] = db
	return true
}

func unregisterSQLiteOwner(sequencer *SQLiteSequencer) {
	if sequencer == nil {
		return
	}
	sqliteOwners.Lock()
	delete(sqliteOwners.bySequencer, sequencer)
	sqliteOwners.Unlock()
}

// clearStaleLeases makes durable reservations available for replay exactly
// once when an exclusive owner opens. A concurrent owner for the same physical
// database would violate NewSQLite's ownership contract and could steal a live
// attempt.
func (sequencer *SQLiteSequencer) clearStaleLeases(ctx context.Context) error {
	return releaseDurableAttemptLeases(
		ctx,
		sequencer.db,
		"visibility lease recovery",
	)
}

func releaseDurableAttemptLeases(
	ctx context.Context,
	db *sql.DB,
	operation string,
) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin %s: %w", operation, err)
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET attempt_id = ''
		WHERE state = 'reserved' AND attempt_id <> ''`); err != nil {
		return fmt.Errorf("release durable leases during %s: %w", operation, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", operation, err)
	}
	return nil
}

func (sequencer *SQLiteSequencer) beginOperation() error {
	if sequencer == nil {
		return ErrClosed
	}
	sequencer.lifecycleMu.Lock()
	defer sequencer.lifecycleMu.Unlock()
	if sequencer.closed || sequencer.db == nil || sequencer.leases == nil {
		return ErrClosed
	}
	sequencer.operations.Add(1)
	return nil
}

func (sequencer *SQLiteSequencer) endOperation() {
	sequencer.operations.Done()
}

// Shutdown rejects new operations and waits for the single owner finalizer.
// Each caller is independently bounded by ctx; a caller timeout does not stop
// the finalizer from draining admitted operations, attempting bounded durable
// lease cleanup, and unregistering ownership. The caller-owned control
// database is never closed here.
func (sequencer *SQLiteSequencer) Shutdown(ctx context.Context) error {
	if sequencer == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is nil", ErrInvalidArgument)
	}

	sequencer.lifecycleMu.Lock()
	initialized := sequencer.db != nil && sequencer.leases != nil &&
		sequencer.terminalDone != nil
	sequencer.lifecycleMu.Unlock()
	if !initialized {
		select {
		case <-sequencer.terminalDone:
			return sequencer.terminalErr
		default:
			return nil
		}
	}

	sequencer.shutdownOnce.Do(func() {
		sequencer.lifecycleMu.Lock()
		sequencer.closed = true
		sequencer.lifecycleMu.Unlock()
		go sequencer.finalizeShutdown()
	})
	select {
	case <-sequencer.terminalDone:
		return sequencer.terminalErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sequencer *SQLiteSequencer) finalizeShutdown() {
	sequencer.operations.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), ownerCleanupTimeout)
	cleanupErr := sequencer.clearActiveLeases(ctx)
	cancel()
	sequencer.finishShutdown(cleanupErr)
}

// Close waits without a caller deadline; finalizer cleanup retains its own
// hard bound. Server shutdown should use Shutdown with its existing timeout.
func (sequencer *SQLiteSequencer) Close() error {
	return sequencer.Shutdown(context.Background())
}

func (sequencer *SQLiteSequencer) clearActiveLeases(ctx context.Context) error {
	if !sequencer.leases.mayHaveDurableLease() {
		return nil
	}
	if err := releaseDurableAttemptLeases(
		ctx,
		sequencer.db,
		"visibility owner shutdown",
	); err != nil {
		return err
	}
	sequencer.leases.clear()
	return nil
}

func (sequencer *SQLiteSequencer) finishShutdown(shutdownErr error) {
	unregisterSQLiteOwner(sequencer)
	sequencer.lifecycleMu.Lock()
	sequencer.db = nil
	sequencer.leases = nil
	sequencer.terminalErr = shutdownErr
	close(sequencer.terminalDone)
	sequencer.lifecycleMu.Unlock()
}

// Lookup reads an active reservation without acquiring its attempt lease. Both
// independently stable identities must resolve to the same immutable digest.
func (sequencer *SQLiteSequencer) Lookup(
	ctx context.Context,
	batchKey string,
	sequenceKey string,
	payloadSHA256 [32]byte,
) (Reservation, bool, error) {
	if err := sequencer.beginOperation(); err != nil {
		return Reservation{}, false, err
	}
	defer sequencer.endOperation()
	if err := validateLookup(ctx, batchKey, sequenceKey); err != nil {
		return Reservation{}, false, err
	}
	// Identity and active-attempt reads must share one SQLite snapshot. A prune
	// may otherwise remove the old identity between reads and let a new batch
	// reuse batchKey, causing this lookup to return the new batch's reservation.
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		return Reservation{}, false, fmt.Errorf("begin visibility lookup: %w", err)
	}
	defer rollback(tx)
	legacy, err := legacyBatchTombstoned(ctx, tx, batchKey)
	if err != nil {
		return Reservation{}, false, err
	}
	if legacy {
		return Reservation{}, true, ErrConflict
	}
	_, matched, err := resolveIdentity(ctx, tx, batchKey, sequenceKey, payloadSHA256)
	if err != nil {
		return Reservation{}, matched, err
	}
	if !matched {
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, false, fmt.Errorf("commit empty visibility lookup: %w", commitErr)
		}
		return Reservation{}, false, nil
	}
	reservation, err := queryActiveReservationByBatch(ctx, tx, batchKey)
	if errors.Is(err, sql.ErrNoRows) {
		// All attempts for this still-known identity were safely abandoned.
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, false, fmt.Errorf("commit abandoned visibility lookup: %w", commitErr)
		}
		return Reservation{}, false, nil
	}
	if err != nil {
		return Reservation{}, false, fmt.Errorf("lookup visibility reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, false, fmt.Errorf("commit visibility lookup: %w", err)
	}
	return reservation, true, nil
}

// Reserve atomically acquires an existing batch attempt or allocates a new
// sequence and durable outbox entry. Allocation does not wait for earlier
// reservations; Cutoff remains the contiguous terminal boundary.
func (sequencer *SQLiteSequencer) Reserve(ctx context.Context, request ReserveRequest) (reservation Reservation, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return Reservation{}, err
	}
	defer sequencer.endOperation()
	if err := validateReserveRequest(ctx, request); err != nil {
		return Reservation{}, err
	}
	if !sequencer.leases.activate(request.AttemptID) {
		return Reservation{}, ErrAttemptInProgress
	}
	retainLease := false
	defer func() {
		if !retainLease {
			sequencer.leases.deactivate(request.AttemptID)
		}
	}()

	indexTime := request.IndexTime.Round(0).UTC()
	indexTimeMillis := indexTime.UnixMilli()
	if !time.UnixMilli(indexTimeMillis).UTC().Equal(indexTime.Truncate(time.Millisecond)) {
		return Reservation{}, fmt.Errorf("%w: index time is outside the persistent timestamp range", ErrInvalidArgument)
	}
	metadata := request.Metadata
	if metadata == nil {
		// database/sql binds a nil []byte as SQL NULL. The schema deliberately
		// distinguishes an empty opaque payload from a missing one.
		metadata = []byte{}
	}

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Reservation{}, fmt.Errorf("begin visibility reservation: %w", err)
	}
	defer rollback(tx)
	legacy, err := legacyBatchTombstoned(ctx, tx, request.BatchKey)
	if err != nil {
		return Reservation{}, err
	}
	if legacy {
		return Reservation{}, ErrConflict
	}

	_, identityExists, err := resolveIdentity(
		ctx,
		tx,
		request.BatchKey,
		request.SequenceKey,
		request.PayloadSHA256,
	)
	if err != nil {
		return Reservation{}, err
	}

	var sequence int64
	var state, owner string
	err = tx.QueryRowContext(ctx, `
		SELECT sequence, state, attempt_id
		FROM ingest_visibility_reservations
		WHERE batch_key = ? AND state IN ('reserved', 'committed')`, request.BatchKey).Scan(
		&sequence,
		&state,
		&owner,
	)
	if err == nil {
		if !identityExists {
			return Reservation{}, ErrConflict
		}
		if state == reservationCommitted {
			reservation, err = queryReservationBySequence(ctx, tx, sequence)
			if err != nil {
				return Reservation{}, fmt.Errorf("read committed visibility reservation: %w", err)
			}
			if commitErr := tx.Commit(); commitErr != nil {
				return Reservation{}, fmt.Errorf("commit visibility reservation lookup: %w", commitErr)
			}
			return reservation, nil
		}
		if state != reservationReserved {
			return Reservation{}, fmt.Errorf("visibility reservation has invalid state %q", state)
		}
		if owner != "" && owner != request.AttemptID && sequencer.leases.contains(owner) {
			return Reservation{}, ErrAttemptInProgress
		}
		result, leaseErr := tx.ExecContext(ctx, `
			UPDATE ingest_visibility_reservations
			SET attempt_id = ?
			WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`,
			request.AttemptID, sequence, owner)
		if leaseErr != nil {
			return Reservation{}, fmt.Errorf("acquire visibility attempt lease: %w", leaseErr)
		}
		if rowErr := requireOneRow(result, "acquire visibility attempt lease"); rowErr != nil {
			return Reservation{}, rowErr
		}
		reservation, err = queryReservationBySequence(ctx, tx, sequence)
		if err != nil {
			return Reservation{}, fmt.Errorf("read reacquired visibility reservation: %w", err)
		}
		reservation.PreviouslyReserved = true
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, fmt.Errorf("commit visibility attempt lease: %w", commitErr)
		}
		sequencer.leases.bind(request.AttemptID, reservation.Sequence)
		retainLease = true
		return reservation, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Reservation{}, fmt.Errorf("read visibility reservation: %w", err)
	}
	barrier, err := sequencer.orphanedAmbiguousExists(ctx, tx, 0, 0)
	if err != nil {
		return Reservation{}, err
	}
	if barrier {
		return Reservation{}, ErrAmbiguousBarrier
	}
	if capacityErr := ensurePendingCapacity(ctx, tx, len(request.Outbox)); capacityErr != nil {
		return Reservation{}, capacityErr
	}
	sequence, err = allocateSequence(ctx, tx)
	if err != nil {
		return Reservation{}, err
	}
	storedSequence, decodeErr := decodePositiveSequence(sequence)
	if decodeErr != nil {
		return Reservation{}, fmt.Errorf("decode allocated visibility sequence: %w", decodeErr)
	}
	createdAt := time.Now().UTC().UnixMicro()
	if !identityExists {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ingest_batch_identities
				(batch_key, sequence_key, payload_sha256, first_visibility_seq, created_at_unix_micro)
			VALUES (?, ?, ?, ?, ?)`,
			request.BatchKey,
			request.SequenceKey,
			request.PayloadSHA256[:],
			sequence,
			createdAt,
		); err != nil {
			if sqliteConstraint(err) {
				return Reservation{}, ErrConflict
			}
			return Reservation{}, fmt.Errorf("persist ingest batch identity: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, phase, attempt_id, index_time_unix_milli,
			 metadata, outbox, created_at_unix_micro, committed_at_unix_micro)
		VALUES (?, ?, 'reserved', 'unsent', ?, ?, ?, ?, ?, NULL)`,
		sequence, request.BatchKey, request.AttemptID, indexTimeMillis,
		metadata, request.Outbox, createdAt); err != nil {
		if sqliteConstraint(err) {
			return Reservation{}, ErrConflict
		}
		return Reservation{}, fmt.Errorf("persist visibility reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, fmt.Errorf("commit visibility reservation: %w", err)
	}
	sequencer.leases.bind(request.AttemptID, storedSequence)
	retainLease = true
	return Reservation{
		BatchKey:      request.BatchKey,
		SequenceKey:   request.SequenceKey,
		Sequence:      storedSequence,
		IndexTime:     time.UnixMilli(indexTimeMillis).UTC(),
		PayloadSHA256: request.PayloadSHA256,
		Metadata:      slices.Clone(metadata),
		Outbox:        slices.Clone(request.Outbox),
	}, nil
}

// AcquirePending leases the oldest replayable reservation that has no live
// in-process owner. It is used by startup/background reconciliation.
func (sequencer *SQLiteSequencer) AcquirePending(ctx context.Context, attemptID string) (reservation Reservation, found bool, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return Reservation{}, false, err
	}
	defer sequencer.endOperation()
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return Reservation{}, false, err
	}
	if !sequencer.leases.activate(attemptID) {
		return Reservation{}, false, ErrAttemptInProgress
	}
	retainLease := false
	defer func() {
		if !retainLease {
			sequencer.leases.deactivate(attemptID)
		}
	}()

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Reservation{}, false, fmt.Errorf("begin pending visibility acquisition: %w", err)
	}
	defer rollback(tx)
	rows, err := tx.QueryContext(ctx, `
		SELECT sequence, attempt_id
		FROM ingest_visibility_reservations
		WHERE state = 'reserved'
		ORDER BY CASE phase WHEN 'ambiguous' THEN 0 ELSE 1 END, sequence`)
	if err != nil {
		return Reservation{}, false, fmt.Errorf("read pending visibility reservations: %w", err)
	}
	var sequence int64
	var priorOwner string
	for rows.Next() {
		var candidate int64
		var owner string
		if scanErr := rows.Scan(&candidate, &owner); scanErr != nil {
			_ = rows.Close()
			return Reservation{}, false, fmt.Errorf("scan pending visibility reservation: %w", scanErr)
		}
		if owner == "" || owner == attemptID || !sequencer.leases.contains(owner) {
			sequence, priorOwner = candidate, owner
			break
		}
	}
	if closeErr := rows.Close(); closeErr != nil {
		return Reservation{}, false, fmt.Errorf("close pending visibility rows: %w", closeErr)
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		return Reservation{}, false, fmt.Errorf("iterate pending visibility rows: %w", iterationErr)
	}
	if sequence == 0 {
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, false, fmt.Errorf("commit empty pending visibility acquisition: %w", commitErr)
		}
		return Reservation{}, false, nil
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET attempt_id = ?
		WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`, attemptID, sequence, priorOwner)
	if err != nil {
		return Reservation{}, false, fmt.Errorf("acquire pending visibility lease: %w", err)
	}
	if rowErr := requireOneRow(result, "acquire pending visibility lease"); rowErr != nil {
		return Reservation{}, false, rowErr
	}
	reservation, err = queryReservationBySequence(ctx, tx, sequence)
	if err != nil {
		return Reservation{}, false, fmt.Errorf("read acquired pending visibility reservation: %w", err)
	}
	reservation.PreviouslyReserved = true
	if err := tx.Commit(); err != nil {
		return Reservation{}, false, fmt.Errorf("commit pending visibility acquisition: %w", err)
	}
	sequencer.leases.bind(attemptID, reservation.Sequence)
	retainLease = true
	return reservation, true, nil
}

type scanner interface{ Scan(...any) error }

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type batchIdentity struct {
	BatchKey           string
	SequenceKey        string
	PayloadSHA256      [32]byte
	FirstVisibilitySeq uint64
	CreatedAt          time.Time
}

func legacyBatchTombstoned(ctx context.Context, q queryer, batchKey string) (bool, error) {
	var exists int
	if err := q.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM ingest_visibility_legacy_tombstones
			WHERE batch_key = ?
		)`, batchKey).Scan(&exists); err != nil {
		return false, fmt.Errorf("read legacy ingest batch tombstone: %w", err)
	}
	return exists != 0, nil
}

func resolveIdentity(
	ctx context.Context,
	q queryer,
	batchKey string,
	sequenceKey string,
	payloadSHA256 [32]byte,
) (batchIdentity, bool, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT batch_key, sequence_key, payload_sha256,
		       first_visibility_seq, created_at_unix_micro
		FROM ingest_batch_identities
		WHERE batch_key = ? OR sequence_key = ?
		ORDER BY batch_key`, batchKey, sequenceKey)
	if err != nil {
		return batchIdentity{}, false, fmt.Errorf("resolve ingest batch identity: %w", err)
	}
	defer rows.Close()

	var matches []batchIdentity
	for rows.Next() {
		var identity batchIdentity
		var digest []byte
		var firstVisibilitySeq, createdAtMicros int64
		if scanErr := rows.Scan(
			&identity.BatchKey,
			&identity.SequenceKey,
			&digest,
			&firstVisibilitySeq,
			&createdAtMicros,
		); scanErr != nil {
			return batchIdentity{}, false, fmt.Errorf("scan ingest batch identity: %w", scanErr)
		}
		copy(identity.PayloadSHA256[:], digest)
		decodedSequence, decodeErr := decodePositiveSequence(firstVisibilitySeq)
		if decodeErr != nil {
			return batchIdentity{}, false, fmt.Errorf("decode first visibility sequence: %w", decodeErr)
		}
		identity.FirstVisibilitySeq = decodedSequence
		identity.CreatedAt = time.UnixMicro(createdAtMicros).UTC()
		matches = append(matches, identity)
	}
	if err := rows.Err(); err != nil {
		return batchIdentity{}, false, fmt.Errorf("iterate ingest batch identities: %w", err)
	}
	if len(matches) == 0 {
		return batchIdentity{}, false, nil
	}
	if len(matches) != 1 ||
		matches[0].BatchKey != batchKey ||
		matches[0].SequenceKey != sequenceKey ||
		matches[0].PayloadSHA256 != payloadSHA256 {
		return batchIdentity{}, true, ErrConflict
	}
	return matches[0], true, nil
}

func scanReservation(row scanner) (Reservation, error) {
	var reservation Reservation
	var sequence, indexTimeMillis int64
	var state, phase string
	var digest, metadata, outbox []byte
	var committedAt sql.NullInt64
	if err := row.Scan(
		&sequence,
		&reservation.BatchKey,
		&reservation.SequenceKey,
		&state,
		&phase,
		&indexTimeMillis,
		&digest,
		&metadata,
		&outbox,
		&committedAt,
	); err != nil {
		return Reservation{}, err
	}
	decodedSequence, err := decodePositiveSequence(sequence)
	if err != nil {
		return Reservation{}, fmt.Errorf("decode visibility reservation sequence: %w", err)
	}
	reservation.Sequence = decodedSequence
	reservation.AlreadyCommitted = state == reservationCommitted
	reservation.MayHaveReachedStorage = phase == phaseAmbiguous || state == reservationCommitted
	reservation.IndexTime = time.UnixMilli(indexTimeMillis).UTC()
	copy(reservation.PayloadSHA256[:], digest)
	reservation.Metadata = slices.Clone(metadata)
	reservation.Outbox = slices.Clone(outbox)
	if committedAt.Valid {
		reservation.CommittedAt = time.UnixMicro(committedAt.Int64).UTC()
	}
	return reservation, nil
}

func queryReservationBySequence(ctx context.Context, q queryer, sequence int64) (Reservation, error) {
	return scanReservation(q.QueryRowContext(ctx, `
		SELECT r.sequence, i.batch_key, i.sequence_key, r.state, r.phase,
		       r.index_time_unix_milli, i.payload_sha256, r.metadata, r.outbox,
		       r.committed_at_unix_micro
		FROM ingest_visibility_reservations AS r
		JOIN ingest_batch_identities AS i ON i.batch_key = r.batch_key
		WHERE r.sequence = ?`, sequence))
}

func queryActiveReservationByBatch(ctx context.Context, q queryer, batchKey string) (Reservation, error) {
	return scanReservation(q.QueryRowContext(ctx, `
		SELECT r.sequence, i.batch_key, i.sequence_key, r.state, r.phase,
		       r.index_time_unix_milli, i.payload_sha256, r.metadata, r.outbox,
		       r.committed_at_unix_micro
		FROM ingest_visibility_reservations AS r
		JOIN ingest_batch_identities AS i ON i.batch_key = r.batch_key
		WHERE r.batch_key = ? AND r.state IN ('reserved', 'committed')`, batchKey))
}

func validateLookup(ctx context.Context, batchKey, sequenceKey string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if batchKey == "" || len(batchKey) > maxBatchKeyBytes || !utf8.ValidString(batchKey) {
		return fmt.Errorf("%w: batch key must contain 1 to %d valid UTF-8 bytes", ErrInvalidArgument, maxBatchKeyBytes)
	}
	if sequenceKey == "" || len(sequenceKey) > maxSequenceKeyBytes || !utf8.ValidString(sequenceKey) {
		return fmt.Errorf("%w: sequence key must contain 1 to %d valid UTF-8 bytes", ErrInvalidArgument, maxSequenceKeyBytes)
	}
	return nil
}

func validateReserveRequest(ctx context.Context, request ReserveRequest) error {
	if err := validateLookup(ctx, request.BatchKey, request.SequenceKey); err != nil {
		return err
	}
	if err := validateAttemptID(ctx, request.AttemptID); err != nil {
		return err
	}
	if request.IndexTime.IsZero() {
		return fmt.Errorf("%w: index time is required", ErrInvalidArgument)
	}
	if len(request.Metadata) > MaxMetadataBytes {
		return fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidArgument, MaxMetadataBytes)
	}
	if len(request.Outbox) == 0 || len(request.Outbox) > MaxOutboxBytes {
		return fmt.Errorf("%w: outbox must contain 1 to %d bytes", ErrInvalidArgument, MaxOutboxBytes)
	}
	return nil
}

func validateAttemptID(ctx context.Context, attemptID string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if attemptID == "" || len(attemptID) > maxAttemptIDBytes || !utf8.ValidString(attemptID) {
		return fmt.Errorf("%w: attempt ID must contain 1 to %d valid UTF-8 bytes", ErrInvalidArgument, maxAttemptIDBytes)
	}
	return nil
}

func ensurePendingCapacity(ctx context.Context, tx *sql.Tx, additionalBytes int) error {
	var count, totalBytes int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(sum(length(outbox)), 0)
		FROM ingest_visibility_reservations
		WHERE state = 'reserved'`).Scan(&count, &totalBytes); err != nil {
		return fmt.Errorf("read pending visibility capacity: %w", err)
	}
	if pendingCapacityExceeded(count, totalBytes, int64(additionalBytes)) {
		return ErrPendingCapacity
	}
	return nil
}

func pendingCapacityExceeded(count, totalBytes, additionalBytes int64) bool {
	return count >= MaxPendingReservations ||
		totalBytes > MaxPendingOutboxBytes-additionalBytes
}

func allocateSequence(ctx context.Context, tx *sql.Tx) (int64, error) {
	var lastAssigned int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_assigned
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned); err != nil {
		return 0, fmt.Errorf("read visibility sequence state: %w", err)
	}
	if lastAssigned == math.MaxInt64 {
		return 0, ErrExhausted
	}
	next := lastAssigned + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = ?
		WHERE singleton = 1 AND last_assigned = ?`, next, lastAssigned)
	if err != nil {
		return 0, fmt.Errorf("advance last visibility sequence: %w", err)
	}
	if err := requireOneRow(result, "advance last visibility sequence"); err != nil {
		return 0, err
	}
	return next, nil
}

func (sequencer *SQLiteSequencer) orphanedAmbiguousExists(
	ctx context.Context,
	tx *sql.Tx,
	excludeSequence uint64,
	beforeSequence uint64,
) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT sequence, attempt_id
		FROM ingest_visibility_reservations
		WHERE state = 'reserved' AND phase = 'ambiguous'
		ORDER BY sequence`)
	if err != nil {
		return false, fmt.Errorf("read ambiguous visibility barrier: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence int64
		var owner string
		if err := rows.Scan(&sequence, &owner); err != nil {
			return false, fmt.Errorf("scan ambiguous visibility barrier: %w", err)
		}
		decodedSequence, err := decodePositiveSequence(sequence)
		if err != nil {
			return false, fmt.Errorf("decode ambiguous visibility sequence: %w", err)
		}
		if decodedSequence == excludeSequence ||
			(beforeSequence != 0 && decodedSequence >= beforeSequence) {
			continue
		}
		if owner == "" || !sequencer.leases.owns(owner, decodedSequence) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate ambiguous visibility barriers: %w", err)
	}
	return false, nil
}

func ambiguousExists(
	ctx context.Context,
	tx *sql.Tx,
	excludeSequence int64,
	beforeSequence int64,
) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM ingest_visibility_reservations
			WHERE state = 'reserved' AND phase = 'ambiguous'
			  AND sequence <> ?
			  AND (? = 0 OR sequence < ?)
		)`, excludeSequence, beforeSequence, beforeSequence).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("read visibility sending barrier: %w", err)
	}
	return exists != 0, nil
}

// MarkSending durably changes an owned unsent reservation to ambiguous before
// the caller invokes ClickHouse Send.
func (sequencer *SQLiteSequencer) MarkSending(ctx context.Context, sequence uint64, attemptID string) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	storedSequence, validationErr := validateAttempt(ctx, sequence, attemptID)
	if validationErr != nil {
		return validationErr
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin visibility sending transition: %w", err)
	}
	defer rollback(tx)
	var state, phase, owner string
	if queryErr := tx.QueryRowContext(ctx, `
		SELECT state, phase, attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, storedSequence).Scan(&state, &phase, &owner); queryErr != nil {
		if errors.Is(queryErr, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read visibility reservation before sending: %w", queryErr)
	}
	if state != reservationReserved || owner != attemptID {
		return ErrAttemptLease
	}
	var beforeSequence int64
	if phase == phaseAmbiguous {
		// Multiple sends can become orphaned in one crash. Replay them oldest
		// first so each exact ambiguous reservation can make progress without
		// allowing an unsent or later replay to jump the barrier.
		beforeSequence = storedSequence
	}
	barrier, err := ambiguousExists(ctx, tx, storedSequence, beforeSequence)
	if err != nil {
		return err
	}
	if barrier {
		return ErrAmbiguousBarrier
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET phase = 'ambiguous'
		WHERE sequence = ? AND state = 'reserved' AND phase IN ('unsent', 'ambiguous') AND attempt_id = ?`,
		storedSequence, attemptID)
	if err != nil {
		return fmt.Errorf("mark visibility reservation sending: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark visibility reservation sending: read affected rows: %w", err)
	}
	if changed != 1 {
		return ErrAttemptLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit visibility sending transition: %w", err)
	}
	return nil
}

func (sequencer *SQLiteSequencer) Commit(ctx context.Context, sequence uint64, attemptID string, committedAt time.Time) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if committedAt.IsZero() {
		return fmt.Errorf("%w: committed time is required", ErrInvalidArgument)
	}
	committedAt = committedAt.Round(0).UTC()
	committedAtMicros := committedAt.UnixMicro()
	if !time.UnixMicro(committedAtMicros).UTC().Equal(committedAt.Truncate(time.Microsecond)) {
		return fmt.Errorf("%w: committed time is outside the persistent timestamp range", ErrInvalidArgument)
	}
	return sequencer.finish(ctx, sequence, attemptID, reservationCommitted, committedAtMicros)
}

func (sequencer *SQLiteSequencer) Release(ctx context.Context, sequence uint64, attemptID string) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	return sequencer.finish(ctx, sequence, attemptID, reservationReserved, 0)
}

// Abandon records that Send provably never began. The tombstone may finish out
// of order and becomes visible only when every earlier sequence is terminal.
func (sequencer *SQLiteSequencer) Abandon(ctx context.Context, sequence uint64, attemptID string) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	return sequencer.finish(ctx, sequence, attemptID, reservationAbandoned, 0)
}

func (sequencer *SQLiteSequencer) finish(ctx context.Context, sequence uint64, attemptID, target string, committedAtMicros int64) error {
	if attemptID != "" && sequencer.leases.owns(attemptID, sequence) {
		defer sequencer.leases.deactivate(attemptID)
	}
	storedSequence, validationErr := validateAttempt(ctx, sequence, attemptID)
	if validationErr != nil {
		return validationErr
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin visibility finalization: %w", err)
	}
	defer rollback(tx)

	var state, phase, owner string
	if queryErr := tx.QueryRowContext(ctx, `
		SELECT state, phase, attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, storedSequence).Scan(&state, &phase, &owner); queryErr != nil {
		if errors.Is(queryErr, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read visibility reservation state: %w", queryErr)
	}
	if (target == reservationCommitted && state == reservationCommitted) ||
		(target == reservationAbandoned && state == reservationAbandoned) {
		return tx.Commit()
	}
	if state != reservationReserved || owner != attemptID {
		return ErrAttemptLease
	}
	if target == reservationAbandoned && phase != phaseUnsent {
		return ErrAttemptLease
	}
	if target == reservationCommitted && phase != phaseAmbiguous {
		return ErrAttemptLease
	}

	if target == reservationReserved {
		result, releaseErr := tx.ExecContext(ctx, `
			UPDATE ingest_visibility_reservations
			SET attempt_id = ''
			WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`, storedSequence, attemptID)
		if releaseErr != nil {
			return fmt.Errorf("release visibility attempt lease: %w", releaseErr)
		}
		if rowErr := requireOneRow(result, "release visibility attempt lease"); rowErr != nil {
			return rowErr
		}
	} else {
		var result sql.Result
		if target == reservationCommitted {
			result, err = tx.ExecContext(ctx, `
				UPDATE ingest_visibility_reservations
				SET state = 'committed', phase = 'final', attempt_id = '', outbox = X'',
				    committed_at_unix_micro = ?
				WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`,
				committedAtMicros, storedSequence, attemptID)
		} else {
			result, err = tx.ExecContext(ctx, `
				UPDATE ingest_visibility_reservations
				SET state = 'abandoned', phase = 'final', attempt_id = '', outbox = X'',
				    committed_at_unix_micro = NULL
				WHERE sequence = ? AND state = 'reserved' AND attempt_id = ?`,
				storedSequence, attemptID)
		}
		if err != nil {
			return fmt.Errorf("finalize visibility reservation: %w", err)
		}
		if err := requireOneRow(result, "finalize visibility reservation"); err != nil {
			return err
		}
		if err := advanceCutoff(ctx, tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit visibility finalization: %w", err)
	}
	return nil
}

func advanceCutoff(ctx context.Context, tx *sql.Tx) error {
	var lastAssigned, committedThrough int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_assigned, committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned, &committedThrough); err != nil {
		return fmt.Errorf("read visibility cutoff state: %w", err)
	}
	var firstPending sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT min(sequence)
		FROM ingest_visibility_reservations
		WHERE state = 'reserved' AND sequence > ?`, committedThrough).Scan(&firstPending); err != nil {
		return fmt.Errorf("read first pending visibility sequence: %w", err)
	}
	next := lastAssigned
	if firstPending.Valid {
		next = firstPending.Int64 - 1
	}
	if next < committedThrough {
		return fmt.Errorf("visibility cutoff regressed from %d to %d", committedThrough, next)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET committed_through = ?
		WHERE singleton = 1 AND committed_through = ?`, next, committedThrough)
	if err != nil {
		return fmt.Errorf("advance visibility cutoff: %w", err)
	}
	return requireOneRow(result, "advance visibility cutoff")
}

func validateAttempt(ctx context.Context, sequence uint64, attemptID string) (int64, error) {
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return 0, err
	}
	if sequence == 0 || sequence > math.MaxInt64 {
		return 0, fmt.Errorf("%w: sequence is invalid", ErrInvalidArgument)
	}
	return int64(sequence), nil
}

func (sequencer *SQLiteSequencer) Cutoff(ctx context.Context) (uint64, error) {
	if err := sequencer.beginOperation(); err != nil {
		return 0, err
	}
	defer sequencer.endOperation()
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
	decodedCutoff, err := decodeNonNegativeSequence(cutoff)
	if err != nil {
		return 0, fmt.Errorf("decode visibility cutoff: %w", err)
	}
	return decodedCutoff, nil
}

// PruneTerminal advances the explicit idempotency horizon by deleting at most
// limit terminal rows. Committed rows retain the newest retainSequences
// committed blocks; abandoned rows use the last-assigned allocation horizon so
// they cannot grow behind an old pending gap but never age a committed
// identity. An identity is deleted in the same transaction only after its last
// reservation is gone.
func (sequencer *SQLiteSequencer) PruneTerminal(
	ctx context.Context,
	retainSequences uint64,
	limit uint32,
) (uint32, error) {
	if err := sequencer.beginOperation(); err != nil {
		return 0, err
	}
	defer sequencer.endOperation()
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if limit == 0 || limit > MaxPruneLimit {
		return 0, fmt.Errorf("%w: prune limit must be between 1 and %d", ErrInvalidArgument, MaxPruneLimit)
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, fmt.Errorf("begin terminal visibility prune: %w", err)
	}
	defer rollback(tx)

	var lastAssigned, cutoff int64
	if queryErr := tx.QueryRowContext(ctx, `
		SELECT last_assigned, committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned, &cutoff); queryErr != nil {
		return 0, fmt.Errorf("read visibility cutoff for prune: %w", queryErr)
	}
	if lastAssigned < 0 || cutoff < 0 || cutoff > lastAssigned {
		return 0, errors.New("invalid visibility cutoff state in control-plane database")
	}
	abandonedThreshold := lastAssigned
	abandonedEligible := true
	committedThreshold := cutoff
	committedEligible := retainSequences == 0
	if retainSequences > math.MaxInt64 {
		abandonedEligible = false
		committedEligible = false
	} else if retainSequences > 0 {
		retained := int64(retainSequences)
		if retained >= lastAssigned {
			abandonedEligible = false
		} else {
			abandonedThreshold = lastAssigned - retained
		}
		// OFFSET retainSequences selects the (N+1)th newest committed
		// block. Deleting it and older commits leaves exactly N newer ones.
		if retained < cutoff {
			horizonErr := tx.QueryRowContext(ctx, `
				SELECT sequence
				FROM ingest_visibility_reservations
				WHERE state = 'committed' AND sequence <= ?
				ORDER BY sequence DESC
				LIMIT 1 OFFSET ?`, cutoff, retained).Scan(&committedThreshold)
			switch {
			case horizonErr == nil:
				committedEligible = true
			case errors.Is(horizonErr, sql.ErrNoRows):
				committedEligible = false
			default:
				return 0, fmt.Errorf("select terminal visibility prune horizon: %w", horizonErr)
			}
		}
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM ingest_visibility_reservations
		WHERE sequence IN (
			SELECT sequence
			FROM ingest_visibility_reservations
			WHERE (state = 'abandoned' AND ? AND sequence <= ?)
			   OR (state = 'committed' AND ? AND sequence <= ?)
			ORDER BY sequence
			LIMIT ?
		)`, abandonedEligible, abandonedThreshold, committedEligible, committedThreshold, limit)
	if err != nil {
		return 0, fmt.Errorf("delete terminal visibility reservations: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read pruned visibility reservation count: %w", err)
	}
	if deleted < 0 || deleted > math.MaxUint32 || deleted > int64(limit) {
		return 0, fmt.Errorf("invalid pruned visibility reservation count %d", deleted)
	}
	if deleted > 0 {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM ingest_batch_identities
			WHERE batch_key IN (
				SELECT i.batch_key
				FROM ingest_batch_identities AS i
				WHERE NOT EXISTS (
					SELECT 1
					FROM ingest_visibility_reservations AS r
					WHERE r.batch_key = i.batch_key
				)
				ORDER BY i.first_visibility_seq
				LIMIT ?
			)`, deleted); err != nil {
			return 0, fmt.Errorf("delete orphan ingest batch identities: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit terminal visibility prune: %w", err)
	}
	return uint32(deleted), nil
}

func decodePositiveSequence(value int64) (uint64, error) {
	if value < 1 {
		return 0, fmt.Errorf("invalid stored visibility sequence %d", value)
	}
	return uint64(value), nil
}

func decodeNonNegativeSequence(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("invalid stored visibility sequence %d", value)
	}
	return uint64(value), nil
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

func sqliteConstraint(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == 19
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }
