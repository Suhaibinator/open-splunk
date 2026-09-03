package visibility

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"modernc.org/sqlite"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

const (
	reservationReserved  = "reserved"
	reservationCommitted = "committed"
	reservationRejected  = "rejected"
	reservationAbandoned = "abandoned"
	phaseUnsent          = "unsent"
	phaseAmbiguous       = "ambiguous"
	phaseFinal           = "final"
	maxBatchKeyBytes     = 512
	maxSequenceKeyBytes  = 512
	maxAttemptIDBytes    = 128
	// One token plus one index per admitted event is the largest valid quota
	// shape. Keeping the bound explicit makes the dynamically constructed bulk
	// read and upsert statements independent of caller-controlled slice sizes.
	maximumQuotaAdmissionScopes = int(ingestquota.HardMaxAdmissionEvents) + 1
	// Cleanup is detached from any one Shutdown caller so a short deadline
	// cannot strand process ownership. Its own hard bound prevents a drained
	// owner from retaining the borrowed database behind a stuck pool.
	ownerCleanupTimeout = 10 * time.Second
)

type processLeases struct {
	mu              sync.Mutex
	active          map[string]leaseTarget
	possiblyDurable bool
}

type leaseTarget struct {
	sequence uint64
	groupID  string
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
	leases.active[id] = leaseTarget{}
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
		leases.active[id] = leaseTarget{sequence: sequence}
		leases.possiblyDurable = true
	}
	leases.mu.Unlock()
}

func (leases *processLeases) bindGroup(id, groupID string) {
	leases.mu.Lock()
	if _, exists := leases.active[id]; exists {
		leases.active[id] = leaseTarget{groupID: groupID}
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
	target, exists := leases.active[id]
	leases.mu.Unlock()
	return exists && target.sequence == sequence && target.groupID == ""
}

func (leases *processLeases) ownsGroup(id, groupID string) bool {
	leases.mu.Lock()
	target, exists := leases.active[id]
	leases.mu.Unlock()
	return exists && target.groupID == groupID && target.sequence == 0
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
	db                   *sql.DB
	leases               *processLeases
	now                  func() time.Time
	hecAcknowledgmentIDs hecAcknowledgmentIDSource

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
	hecAcknowledgmentIDs, err := newKeyedHECAcknowledgmentIDSource()
	if err != nil {
		return nil, err
	}
	sequencer := &SQLiteSequencer{
		db:                   db.SQLDB(),
		leases:               &processLeases{active: make(map[string]leaseTarget)},
		now:                  time.Now,
		hecAcknowledgmentIDs: hecAcknowledgmentIDs,
		terminalDone:         make(chan struct{}),
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
	if _, err := tx.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET attempt_id = ''
		WHERE state IN ('ready', 'ambiguous') AND attempt_id <> ''`); err != nil {
		return fmt.Errorf("release durable write group leases during %s: %w", operation, err)
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

// Lookup reads an active disposition without acquiring its attempt lease. Both
// independently stable identities must resolve to the same immutable digest.
// Terminal dispositions include their durable replay payload. Pending
// dispositions return only their stable identity and sequence; Reserve or
// AcquirePending hydrates the replay payload after it wins the attempt lease.
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
	identity, matched, err := resolveIdentity(ctx, tx, batchKey, sequenceKey, payloadSHA256)
	if err != nil {
		return Reservation{}, matched, err
	}
	if !matched {
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, false, fmt.Errorf("commit empty visibility lookup: %w", commitErr)
		}
		return Reservation{}, false, nil
	}
	activeSequence, activeState, activeErr := queryActiveDisposition(ctx, tx, batchKey)
	if errors.Is(activeErr, sql.ErrNoRows) {
		// All attempts for this still-known identity were safely abandoned.
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, false, fmt.Errorf("commit abandoned visibility lookup: %w", commitErr)
		}
		return Reservation{}, false, nil
	}
	if activeErr != nil {
		return Reservation{}, false, fmt.Errorf("lookup visibility reservation: %w", activeErr)
	}
	var reservation Reservation
	switch activeState {
	case reservationCommitted, reservationRejected:
		reservation, err = queryReservationBySequence(ctx, tx, activeSequence)
		if err != nil {
			return Reservation{}, false, fmt.Errorf("read terminal visibility disposition: %w", err)
		}
	case reservationReserved:
		sequence, decodeErr := decodePositiveSequence(activeSequence)
		if decodeErr != nil {
			return Reservation{}, false, fmt.Errorf("decode pending visibility sequence: %w", decodeErr)
		}
		reservation = Reservation{
			BatchKey:      identity.BatchKey,
			SequenceKey:   identity.SequenceKey,
			Sequence:      sequence,
			PayloadSHA256: identity.PayloadSHA256,
		}
	default:
		return Reservation{}, false, fmt.Errorf(
			"active visibility disposition has invalid state %q",
			activeState,
		)
	}
	if err := hydrateReservationHECIdentifiers(ctx, tx, &reservation); err != nil {
		return Reservation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, false, fmt.Errorf("commit visibility lookup: %w", err)
	}
	return reservation, true, nil
}

func hydrateReservationHECIdentifiers(ctx context.Context, q queryer, reservation *Reservation) error {
	var requestSequence int64
	var acknowledgmentID sql.NullInt64
	err := q.QueryRowContext(ctx, `
		SELECT request.request_sequence, acknowledgment.acknowledgment_id
		FROM hec_requests AS request
		LEFT JOIN hec_acknowledgments AS acknowledgment
		  ON acknowledgment.tenant_id = request.tenant_id
		 AND acknowledgment.ingestion_token_id = request.ingestion_token_id
		 AND acknowledgment.request_sequence = request.request_sequence
		WHERE request.visibility_sequence = ?`, reservation.Sequence).Scan(
		&requestSequence,
		&acknowledgmentID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read HEC identifiers for visibility reservation: %w", err)
	}
	if requestSequence <= 0 || acknowledgmentID.Valid &&
		(acknowledgmentID.Int64 <= 0 ||
			acknowledgmentID.Int64 > safecast.MustConv[int64](maximumHECAcknowledgmentID)) {
		return errors.New("visibility reservation has invalid HEC identifiers")
	}
	reservation.HECRequestSequence = safecast.MustConv[uint64](requestSequence)
	if acknowledgmentID.Valid {
		reservation.HECAcknowledgmentID = safecast.MustConv[uint64](acknowledgmentID.Int64)
	}
	return nil
}

// Reserve atomically acquires an existing batch attempt or allocates a new
// sequence and durable outbox entry. ExistingOnly requests never allocate.
// Allocation does not wait for earlier reservations; Cutoff remains the
// contiguous terminal boundary.
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

	var indexTimeMillis int64
	var metadata []byte
	if !request.ExistingOnly {
		indexTime := request.IndexTime.Round(0).UTC()
		indexTimeMillis = indexTime.UnixMilli()
		if !time.UnixMilli(indexTimeMillis).UTC().Equal(indexTime.Truncate(time.Millisecond)) {
			return Reservation{}, fmt.Errorf("%w: index time is outside the persistent timestamp range", ErrInvalidArgument)
		}
		metadata = request.Metadata
		if metadata == nil {
			// database/sql binds a nil []byte as SQL NULL. The schema deliberately
			// distinguishes an empty opaque payload from a missing one.
			metadata = []byte{}
		}
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
		WHERE batch_key = ? AND state IN ('reserved', 'committed', 'rejected')`, request.BatchKey).Scan(
		&sequence,
		&state,
		&owner,
	)
	if err == nil {
		if !identityExists {
			return Reservation{}, ErrConflict
		}
		if state == reservationCommitted || state == reservationRejected {
			reservation, err = queryReservationBySequence(ctx, tx, sequence)
			if err != nil {
				return Reservation{}, fmt.Errorf("read terminal visibility reservation: %w", err)
			}
			if commitErr := tx.Commit(); commitErr != nil {
				return Reservation{}, fmt.Errorf("commit visibility reservation lookup: %w", commitErr)
			}
			return reservation, nil
		}
		if state != reservationReserved {
			return Reservation{}, fmt.Errorf("visibility reservation has invalid state %q", state)
		}
		var grouped int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM ingest_write_group_members
				WHERE visibility_sequence = ?
			)`, sequence).Scan(&grouped); err != nil {
			return Reservation{}, fmt.Errorf("read visibility reservation group ownership: %w", err)
		}
		if grouped != 0 {
			return Reservation{}, ErrAttemptInProgress
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
	if request.ExistingOnly {
		return Reservation{}, ErrReservationGone
	}
	barrier, err := sequencer.orphanedAmbiguousExists(ctx, tx, 0, 0)
	if err != nil {
		return Reservation{}, err
	}
	if barrier {
		return Reservation{}, ErrAmbiguousBarrier
	}
	checkedAt := time.Now().UTC()
	if sequencer.now != nil {
		checkedAt = sequencer.now().Round(0).UTC()
	}
	if err := preflightHECAdmission(ctx, tx, request.HECAdmission, checkedAt); err != nil {
		return Reservation{}, err
	}
	if request.HECAdmission != nil {
		// Quota scheduling is an admission-time decision, not an event-time
		// decision. A client that starts before expiry and trickles a body must
		// neither extend token authority nor backdate its durable quota charge.
		request.QuotaEvaluatedAt = checkedAt
	}
	if capacityErr := ensurePendingCapacity(ctx, tx, len(request.Outbox), len(metadata)); capacityErr != nil {
		return Reservation{}, capacityErr
	}
	quotaPlan, err := planQuotaReservation(ctx, tx, request)
	if err != nil {
		return Reservation{}, err
	}
	sequence, err = allocateSequence(ctx, tx)
	if err != nil {
		return Reservation{}, err
	}
	storedSequence, decodeErr := decodePositiveSequence(sequence)
	if decodeErr != nil {
		return Reservation{}, fmt.Errorf("decode allocated visibility sequence: %w", decodeErr)
	}
	createdAt := checkedAt.UnixMicro()
	outboxSHA256 := sha256.Sum256(request.Outbox)
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
			 metadata, outbox, outbox_sha256, stored_row_count, decoded_event_bytes,
			 created_at_unix_micro, committed_at_unix_micro)
		VALUES (?, ?, 'reserved', 'unsent', ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		sequence, request.BatchKey, request.AttemptID, indexTimeMillis,
		metadata, request.Outbox, outboxSHA256[:], request.StoredRowCount,
		request.DecodedEventBytes, createdAt); err != nil {
		if sqliteConstraint(err) {
			return Reservation{}, ErrConflict
		}
		return Reservation{}, fmt.Errorf("persist visibility reservation: %w", err)
	}
	if quotaPlan != nil {
		if err := persistQuotaReservation(ctx, tx, request.BatchKey, *quotaPlan); err != nil {
			return Reservation{}, err
		}
	}
	hecRequestSequence, hecAcknowledgmentID, err := persistHECAdmission(
		ctx,
		tx,
		request.HECAdmission,
		sequence,
		sequencer.hecAcknowledgmentIDs,
	)
	if err != nil {
		return Reservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, fmt.Errorf("commit visibility reservation: %w", err)
	}
	sequencer.leases.bind(request.AttemptID, storedSequence)
	retainLease = true
	return Reservation{
		BatchKey:            request.BatchKey,
		SequenceKey:         request.SequenceKey,
		Sequence:            storedSequence,
		IndexTime:           time.UnixMilli(indexTimeMillis).UTC(),
		PayloadSHA256:       request.PayloadSHA256,
		Metadata:            slices.Clone(metadata),
		Outbox:              slices.Clone(request.Outbox),
		OutboxSHA256:        outboxSHA256,
		StoredRowCount:      request.StoredRowCount,
		DecodedEventBytes:   request.DecodedEventBytes,
		CreatedAt:           time.UnixMicro(createdAt).UTC(),
		HECRequestSequence:  hecRequestSequence,
		HECAcknowledgmentID: hecAcknowledgmentID,
	}, nil
}

// Reject atomically records a terminal whole-batch rejection or returns the
// existing active disposition for the same immutable identity. A rejection
// consumes a visibility sequence so Cutoff can represent one total order, but
// it never consumes pending capacity, creates an outbox, or acquires an
// attempt lease. An existing pending disposition returns only its stable
// identity and sequence; replay payloads remain private to its owning Store.
func (sequencer *SQLiteSequencer) Reject(
	ctx context.Context,
	request RejectRequest,
) (Reservation, error) {
	if err := sequencer.beginOperation(); err != nil {
		return Reservation{}, err
	}
	defer sequencer.endOperation()
	if err := validateRejectRequest(ctx, request); err != nil {
		return Reservation{}, err
	}

	indexTime := request.IndexTime.Round(0).UTC()
	indexTimeMillis := indexTime.UnixMilli()
	if !time.UnixMilli(indexTimeMillis).UTC().Equal(indexTime.Truncate(time.Millisecond)) {
		return Reservation{}, fmt.Errorf(
			"%w: index time is outside the persistent timestamp range",
			ErrInvalidArgument,
		)
	}
	rejectedAt := request.RejectedAt.Round(0).UTC()
	rejectedAtMicros := rejectedAt.UnixMicro()
	if !time.UnixMicro(rejectedAtMicros).UTC().Equal(rejectedAt.Truncate(time.Microsecond)) {
		return Reservation{}, fmt.Errorf(
			"%w: rejected time is outside the persistent timestamp range",
			ErrInvalidArgument,
		)
	}
	metadata := request.Metadata
	if metadata == nil {
		metadata = []byte{}
	}

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Reservation{}, fmt.Errorf("begin visibility rejection: %w", err)
	}
	defer rollback(tx)
	legacy, err := legacyBatchTombstoned(ctx, tx, request.BatchKey)
	if err != nil {
		return Reservation{}, err
	}
	if legacy {
		return Reservation{}, ErrConflict
	}

	identity, identityExists, err := resolveIdentity(
		ctx,
		tx,
		request.BatchKey,
		request.SequenceKey,
		request.PayloadSHA256,
	)
	if err != nil {
		return Reservation{}, err
	}

	activeSequence, activeState, activeErr := queryActiveDisposition(
		ctx,
		tx,
		request.BatchKey,
	)
	if activeErr == nil {
		if !identityExists {
			return Reservation{}, ErrConflict
		}
		var reservation Reservation
		switch activeState {
		case reservationCommitted, reservationRejected:
			reservation, err = queryReservationBySequence(ctx, tx, activeSequence)
			if err != nil {
				return Reservation{}, fmt.Errorf("read terminal visibility disposition: %w", err)
			}
		case reservationReserved:
			sequence, decodeErr := decodePositiveSequence(activeSequence)
			if decodeErr != nil {
				return Reservation{}, fmt.Errorf("decode pending visibility sequence: %w", decodeErr)
			}
			// Reject callers only need the winning pending identity. Avoid loading
			// or exposing its potentially 16 MiB replay outbox and metadata.
			reservation = Reservation{
				BatchKey:      identity.BatchKey,
				SequenceKey:   identity.SequenceKey,
				Sequence:      sequence,
				PayloadSHA256: identity.PayloadSHA256,
			}
		default:
			return Reservation{}, fmt.Errorf(
				"active visibility disposition has invalid state %q",
				activeState,
			)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return Reservation{}, fmt.Errorf("commit visibility rejection lookup: %w", commitErr)
		}
		return reservation, nil
	}
	if !errors.Is(activeErr, sql.ErrNoRows) {
		return Reservation{}, fmt.Errorf("read active visibility disposition: %w", activeErr)
	}

	sequence, err := allocateSequence(ctx, tx)
	if err != nil {
		return Reservation{}, err
	}
	storedSequence, err := decodePositiveSequence(sequence)
	if err != nil {
		return Reservation{}, fmt.Errorf("decode allocated rejection sequence: %w", err)
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
			return Reservation{}, fmt.Errorf("persist rejected ingest batch identity: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, phase, attempt_id, index_time_unix_milli,
			 metadata, outbox, outbox_sha256, stored_row_count, decoded_event_bytes,
			 created_at_unix_micro, committed_at_unix_micro)
		VALUES (?, ?, 'rejected', 'final', '', ?, ?, X'', X'', 0, 0, ?, ?)`,
		sequence,
		request.BatchKey,
		indexTimeMillis,
		metadata,
		createdAt,
		rejectedAtMicros,
	); err != nil {
		if sqliteConstraint(err) {
			return Reservation{}, ErrConflict
		}
		return Reservation{}, fmt.Errorf("persist terminal visibility rejection: %w", err)
	}
	if err := advanceCutoff(ctx, tx); err != nil {
		return Reservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, fmt.Errorf("commit visibility rejection: %w", err)
	}
	return Reservation{
		BatchKey:      request.BatchKey,
		SequenceKey:   request.SequenceKey,
		Sequence:      storedSequence,
		Rejected:      true,
		NewlyRejected: true,
		IndexTime:     time.UnixMilli(indexTimeMillis).UTC(),
		PayloadSHA256: request.PayloadSHA256,
		Metadata:      slices.Clone(metadata),
		Outbox:        []byte{},
		RejectedAt:    time.UnixMicro(rejectedAtMicros).UTC(),
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
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = ingest_visibility_reservations.sequence
		  )
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

// PendingUsage reports every durable non-terminal reservation, even when a
// live in-process attempt lease makes the reservation ineligible for
// AcquirePending.
func (sequencer *SQLiteSequencer) PendingUsage(ctx context.Context) (PendingUsage, error) {
	if err := sequencer.beginOperation(); err != nil {
		return PendingUsage{}, err
	}
	defer sequencer.endOperation()
	if err := validateContext(ctx); err != nil {
		return PendingUsage{}, err
	}
	return readPendingUsage(ctx, sequencer.db)
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
	var sequence, indexTimeMillis, storedRowCount, decodedEventBytes, createdAtMicros int64
	var state, phase string
	var digest, metadata, outbox, outboxDigest []byte
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
		&outboxDigest,
		&storedRowCount,
		&decodedEventBytes,
		&createdAtMicros,
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
	reservation.Rejected = state == reservationRejected
	reservation.MayHaveReachedStorage = phase == phaseAmbiguous || state == reservationCommitted
	reservation.IndexTime = time.UnixMilli(indexTimeMillis).UTC()
	copy(reservation.PayloadSHA256[:], digest)
	copy(reservation.OutboxSHA256[:], outboxDigest)
	if storedRowCount < 0 || storedRowCount > math.MaxUint32 || decodedEventBytes < 0 {
		return Reservation{}, errors.New("visibility reservation has invalid persisted accounting")
	}
	reservation.StoredRowCount = uint32(storedRowCount)
	reservation.DecodedEventBytes = uint64(decodedEventBytes)
	reservation.CreatedAt = time.UnixMicro(createdAtMicros).UTC()
	// database/sql Scan into *[]byte already returns caller-owned copies. Keep
	// those directly so hydrating a maximum-size replay outbox does not copy it
	// a second time in process memory.
	reservation.Metadata = metadata
	reservation.Outbox = outbox
	if committedAt.Valid {
		terminalAt := time.UnixMicro(committedAt.Int64).UTC()
		switch state {
		case reservationCommitted:
			reservation.CommittedAt = terminalAt
		case reservationRejected:
			reservation.RejectedAt = terminalAt
		}
	}
	return reservation, nil
}

func queryReservationBySequence(ctx context.Context, q queryer, sequence int64) (Reservation, error) {
	return scanReservation(q.QueryRowContext(ctx, `
		SELECT r.sequence, i.batch_key, i.sequence_key, r.state, r.phase,
		       r.index_time_unix_milli, i.payload_sha256, r.metadata, r.outbox,
		       r.outbox_sha256, r.stored_row_count, r.decoded_event_bytes,
		       r.created_at_unix_micro,
		       r.committed_at_unix_micro
		FROM ingest_visibility_reservations AS r
		JOIN ingest_batch_identities AS i ON i.batch_key = r.batch_key
		WHERE r.sequence = ?`, sequence))
}

func queryActiveDisposition(
	ctx context.Context,
	q queryer,
	batchKey string,
) (int64, string, error) {
	var sequence int64
	var state string
	err := q.QueryRowContext(ctx, `
		SELECT sequence, state
		FROM ingest_visibility_reservations
		WHERE batch_key = ? AND state IN ('reserved', 'committed', 'rejected')`,
		batchKey,
	).Scan(&sequence, &state)
	return sequence, state, err
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
	if request.ExistingOnly {
		return nil
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
	if request.StoredRowCount == 0 || request.StoredRowCount > MaxWriteGroupRows {
		return fmt.Errorf("%w: stored row count must be between 1 and %d", ErrInvalidArgument, MaxWriteGroupRows)
	}
	if request.DecodedEventBytes == 0 || request.DecodedEventBytes > MaxWriteGroupDecodedBytes {
		return fmt.Errorf("%w: decoded event bytes must be between 1 and %d", ErrInvalidArgument, MaxWriteGroupDecodedBytes)
	}
	if err := validateHECAdmissionRequest(request.HECAdmission); err != nil {
		return err
	}
	return nil
}

func validateHECAdmissionRequest(request *HECAdmissionRequest) error {
	if request == nil {
		return nil
	}
	if request.TenantID == "" || len(request.TenantID) > 255 || !utf8.ValidString(request.TenantID) ||
		strings.TrimSpace(request.TenantID) != request.TenantID || strings.IndexByte(request.TenantID, 0) >= 0 {
		return fmt.Errorf("%w: HEC acknowledgment tenant ID is invalid", ErrInvalidArgument)
	}
	if request.TokenID == "" || len(request.TokenID) > 128 || !utf8.ValidString(request.TokenID) ||
		strings.TrimSpace(request.TokenID) != request.TokenID || strings.IndexByte(request.TokenID, 0) >= 0 {
		return fmt.Errorf("%w: HEC acknowledgment token ID is invalid", ErrInvalidArgument)
	}
	if request.TokenVersion == 0 {
		return fmt.Errorf("%w: HEC token version is invalid", ErrInvalidArgument)
	}
	if len(request.AuthorizedIndexes) == 0 || len(request.AuthorizedIndexes) > MaxHECAcknowledgmentsPerQuery {
		return fmt.Errorf("%w: HEC selected index authority is invalid", ErrInvalidArgument)
	}
	previous := ""
	for _, selected := range request.AuthorizedIndexes {
		if !indexname.ValidCanonical(selected.Name) || selected.Version == 0 ||
			selected.Version > math.MaxInt64 || previous >= selected.Name && previous != "" {
			return fmt.Errorf("%w: HEC selected index authority is invalid", ErrInvalidArgument)
		}
		previous = selected.Name
	}
	if request.Acknowledgment &&
		(request.AcknowledgmentChannel == "" || len(request.AcknowledgmentChannel) > 128 ||
			!utf8.ValidString(request.AcknowledgmentChannel) ||
			strings.TrimSpace(request.AcknowledgmentChannel) != request.AcknowledgmentChannel ||
			strings.IndexByte(request.AcknowledgmentChannel, 0) >= 0) {
		return fmt.Errorf("%w: HEC acknowledgment channel is invalid", ErrInvalidArgument)
	}
	if !request.Acknowledgment && request.AcknowledgmentChannel != "" {
		return fmt.Errorf("%w: HEC acknowledgment channel is not enabled", ErrInvalidArgument)
	}
	if request.RequestID == "" || len(request.RequestID) > 128 || !utf8.ValidString(request.RequestID) ||
		strings.TrimSpace(request.RequestID) != request.RequestID || strings.IndexByte(request.RequestID, 0) >= 0 {
		return fmt.Errorf("%w: HEC acknowledgment request ID is invalid", ErrInvalidArgument)
	}
	if request.CreatedAt.IsZero() || request.CreatedAt.UnixMicro() < 0 {
		return fmt.Errorf("%w: HEC acknowledgment creation time is invalid", ErrInvalidArgument)
	}
	return nil
}

func validateRejectRequest(ctx context.Context, request RejectRequest) error {
	if err := validateLookup(ctx, request.BatchKey, request.SequenceKey); err != nil {
		return err
	}
	if request.IndexTime.IsZero() {
		return fmt.Errorf("%w: index time is required", ErrInvalidArgument)
	}
	if request.RejectedAt.IsZero() {
		return fmt.Errorf("%w: rejected time is required", ErrInvalidArgument)
	}
	if len(request.Metadata) > MaxMetadataBytes {
		return fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidArgument, MaxMetadataBytes)
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

type quotaReservationPlan struct {
	updates               []ingestquota.StateUpdate
	admittedAtUnixMicro   int64
	eventCount            uint64
	uncompressedByteCount uint64
}

// planQuotaReservation runs only after Reserve has proved there is no active
// durable disposition for the exact batch. The returned updates remain
// speculative until the caller persists them in the same transaction as the
// identity, reservation, and admission marker.
func planQuotaReservation(
	ctx context.Context,
	tx *sql.Tx,
	request ReserveRequest,
) (*quotaReservationPlan, error) {
	if request.QuotaAdmission == nil {
		return nil, nil
	}
	var alreadyAdmitted int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM ingest_quota_admissions
			WHERE batch_key = ?
		)`, request.BatchKey).Scan(&alreadyAdmitted); err != nil {
		return nil, fmt.Errorf("read durable ingestion quota admission: %w", err)
	}
	if alreadyAdmitted != 0 {
		return nil, nil
	}

	admission, eventCount, byteCount, err := hydrateQuotaAdmission(
		ctx,
		tx,
		request.QuotaEvaluatedAt,
		*request.QuotaAdmission,
	)
	if err != nil {
		return nil, err
	}
	decision, err := ingestquota.Evaluate(request.QuotaEvaluatedAt, admission)
	if err != nil {
		return nil, fmt.Errorf("evaluate durable ingestion quota: %w", err)
	}
	if !decision.Allowed {
		return nil, &ingestquota.ExceededError{
			Scope:      decision.BlockingScope,
			RetryAfter: decision.RetryAfter,
		}
	}
	admittedAt := request.QuotaEvaluatedAt.Round(0).UTC().UnixMicro()
	if admittedAt <= 0 {
		return nil, fmt.Errorf("%w: quota evaluation time is outside persistent bounds", ErrInvalidArgument)
	}
	return &quotaReservationPlan{
		updates:               decision.Updates,
		admittedAtUnixMicro:   admittedAt,
		eventCount:            eventCount,
		uncompressedByteCount: byteCount,
	}, nil
}

func hydrateQuotaAdmission(
	ctx context.Context,
	tx *sql.Tx,
	evaluatedAt time.Time,
	source ingestquota.Admission,
) (ingestquota.Admission, uint64, uint64, error) {
	if len(source.Charges) > maximumQuotaAdmissionScopes {
		return ingestquota.Admission{}, 0, 0, fmt.Errorf(
			"%w: quota admission contains too many scopes",
			ErrInvalidArgument,
		)
	}
	// Evaluate once without database state to validate the caller-owned policy,
	// costs, time, and duplicate-scope shape before using any scope as a SQL key.
	for _, charge := range source.Charges {
		if charge.State != nil {
			return ingestquota.Admission{}, 0, 0, fmt.Errorf(
				"%w: quota admission must not supply durable state",
				ErrInvalidArgument,
			)
		}
	}
	if _, err := ingestquota.Evaluate(evaluatedAt, source); err != nil {
		return ingestquota.Admission{}, 0, 0, fmt.Errorf(
			"%w: invalid quota admission: %w",
			ErrInvalidArgument,
			err,
		)
	}

	admission := ingestquota.Admission{
		Charges: append([]ingestquota.Charge(nil), source.Charges...),
	}
	var tokenCharge *ingestquota.Charge
	var tenantID string
	var indexCount int
	var indexEvents, indexBytes uint64
	for index := range admission.Charges {
		charge := &admission.Charges[index]
		if tenantID == "" {
			tenantID = charge.Scope.TenantID
		} else if charge.Scope.TenantID != tenantID {
			return ingestquota.Admission{}, 0, 0, fmt.Errorf(
				"%w: quota admission spans multiple tenants",
				ErrInvalidArgument,
			)
		}
		switch charge.Scope.Kind {
		case ingestquota.ScopeKindToken:
			if tokenCharge != nil {
				return ingestquota.Admission{}, 0, 0, fmt.Errorf(
					"%w: quota admission requires exactly one token scope",
					ErrInvalidArgument,
				)
			}
			tokenCharge = charge
		case ingestquota.ScopeKindIndex:
			indexCount++
			if math.MaxUint64-indexEvents < charge.Events ||
				math.MaxUint64-indexBytes < charge.UncompressedBytes {
				return ingestquota.Admission{}, 0, 0, fmt.Errorf(
					"%w: quota index charge totals overflow",
					ErrInvalidArgument,
				)
			}
			indexEvents += charge.Events
			indexBytes += charge.UncompressedBytes
		}
	}
	if tokenCharge == nil || indexCount == 0 {
		return ingestquota.Admission{}, 0, 0, fmt.Errorf(
			"%w: quota admission requires one token and at least one index scope",
			ErrInvalidArgument,
		)
	}
	if tokenCharge.Events != indexEvents ||
		tokenCharge.UncompressedBytes != indexBytes {
		return ingestquota.Admission{}, 0, 0, fmt.Errorf(
			"%w: token quota charge does not equal the index charge total",
			ErrInvalidArgument,
		)
	}
	states, err := readQuotaStates(ctx, tx, admission.Charges)
	if err != nil {
		return ingestquota.Admission{}, 0, 0, err
	}
	for index := range admission.Charges {
		admission.Charges[index].State = states[admission.Charges[index].Scope]
	}
	return admission, tokenCharge.Events, tokenCharge.UncompressedBytes, nil
}

func readQuotaStates(
	ctx context.Context,
	tx *sql.Tx,
	charges []ingestquota.Charge,
) (map[ingestquota.ScopeKey]*ingestquota.State, error) {
	if len(charges) == 0 || len(charges) > maximumQuotaAdmissionScopes {
		return nil, errors.New("read ingestion quota buckets: scope count is outside bounds")
	}

	requested := make([]struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}, len(charges))
	for index, charge := range charges {
		requested[index].Kind = string(charge.Scope.Kind)
		requested[index].ID = charge.Scope.Identity
	}
	encodedRequested, err := json.Marshal(requested)
	if err != nil {
		return nil, fmt.Errorf("encode ingestion quota bucket request: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
		WITH requested(scope_kind, scope_id) AS (
			SELECT json_extract(value, '$.kind'), json_extract(value, '$.id')
			FROM json_each(?)
		)
		SELECT bucket.scope_kind,
		       bucket.scope_id,
		       bucket.max_ingest_events_per_second,
		       bucket.max_ingest_uncompressed_bytes_per_second,
		       bucket.next_event_admission_unix_nano,
		       bucket.next_byte_admission_unix_nano,
		       bucket.updated_at_unix_micro
		FROM requested
		JOIN ingest_quota_buckets AS bucket
		  ON bucket.tenant_id = ?
		 AND bucket.scope_kind = requested.scope_kind
		 AND bucket.scope_id = requested.scope_id`,
		encodedRequested,
		charges[0].Scope.TenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("read ingestion quota buckets: %w", err)
	}
	defer rows.Close()

	states := make(map[ingestquota.ScopeKey]*ingestquota.State, len(charges))
	for rows.Next() {
		var kind, identity string
		var eventRate, byteRate int64
		var nextEvent, nextByte, updatedAt int64
		if err := rows.Scan(
			&kind,
			&identity,
			&eventRate,
			&byteRate,
			&nextEvent,
			&nextByte,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("read ingestion quota bucket: %w", err)
		}
		if eventRate < 0 || byteRate < 0 {
			return nil, errors.New("ingestion quota bucket contains a negative rate")
		}
		key := ingestquota.ScopeKey{
			Kind:     ingestquota.ScopeKind(kind),
			TenantID: charges[0].Scope.TenantID,
			Identity: identity,
		}
		states[key] = &ingestquota.State{
			Limits: ingestquota.Limits{
				MaxEventsPerSecond:            safecast.MustConv[uint64](eventRate),
				MaxUncompressedBytesPerSecond: safecast.MustConv[uint64](byteRate),
			},
			NextEventAdmissionUnixNano: nextEvent,
			NextByteAdmissionUnixNano:  nextByte,
			UpdatedAtUnixMicro:         updatedAt,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read ingestion quota buckets: %w", err)
	}
	return states, nil
}

func persistQuotaReservation(
	ctx context.Context,
	tx *sql.Tx,
	batchKey string,
	plan quotaReservationPlan,
) error {
	if err := persistQuotaUpdates(ctx, tx, plan.updates); err != nil {
		return err
	}

	eventCount := safecast.MustConv[int64](plan.eventCount)

	uncompressedByteCount := safecast.MustConv[int64](plan.uncompressedByteCount)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_quota_admissions (
			batch_key, admitted_at_unix_micro, event_count, uncompressed_bytes
		) VALUES (?, ?, ?, ?)`,
		batchKey,
		plan.admittedAtUnixMicro,
		eventCount,
		uncompressedByteCount,
	); err != nil {
		return fmt.Errorf("persist ingestion quota admission: %w", err)
	}
	return nil
}

func persistQuotaUpdates(
	ctx context.Context,
	tx *sql.Tx,
	updates []ingestquota.StateUpdate,
) error {
	if len(updates) == 0 {
		return nil
	}
	if len(updates) > maximumQuotaAdmissionScopes {
		return errors.New("persist ingestion quota buckets: update count is outside bounds")
	}

	statement, err := tx.PrepareContext(ctx, `
		INSERT INTO ingest_quota_buckets (
			tenant_id, scope_kind, scope_id,
			max_ingest_events_per_second,
			max_ingest_uncompressed_bytes_per_second,
			next_event_admission_unix_nano,
			next_byte_admission_unix_nano,
			updated_at_unix_micro,
			token_owner_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, scope_kind, scope_id) DO UPDATE SET
			max_ingest_events_per_second = excluded.max_ingest_events_per_second,
			max_ingest_uncompressed_bytes_per_second = excluded.max_ingest_uncompressed_bytes_per_second,
			next_event_admission_unix_nano = excluded.next_event_admission_unix_nano,
			next_byte_admission_unix_nano = excluded.next_byte_admission_unix_nano,
			updated_at_unix_micro = excluded.updated_at_unix_micro,
			token_owner_id = excluded.token_owner_id`)
	if err != nil {
		return fmt.Errorf("prepare ingestion quota bucket persistence: %w", err)
	}
	defer statement.Close()

	for _, update := range updates {
		var tokenOwnerID any
		if update.Scope.Kind == ingestquota.ScopeKindToken {
			tokenOwnerID = update.Scope.Identity
		}

		maxEventsPerSecond := safecast.MustConv[int64](update.State.Limits.MaxEventsPerSecond)
		maxBytesPerSecond := safecast.MustConv[int64](update.State.Limits.MaxUncompressedBytesPerSecond)
		if _, err := statement.ExecContext(
			ctx,
			update.Scope.TenantID,
			string(update.Scope.Kind),
			update.Scope.Identity,
			maxEventsPerSecond,
			maxBytesPerSecond,
			update.State.NextEventAdmissionUnixNano,
			update.State.NextByteAdmissionUnixNano,
			update.State.UpdatedAtUnixMicro,
			tokenOwnerID,
		); err != nil {
			return fmt.Errorf("persist ingestion quota bucket: %w", err)
		}
	}
	return nil
}

func ensurePendingCapacity(ctx context.Context, tx *sql.Tx, additionalOutboxBytes, additionalMetadataBytes int) error {
	usage, err := readPendingUsage(ctx, tx)
	if err != nil {
		return fmt.Errorf("read pending visibility capacity: %w", err)
	}

	totalBytes := safecast.MustConv[int64](usage.OutboxBytes)
	totalMetadataBytes := safecast.MustConv[int64](usage.MetadataBytes)
	if pendingCapacityExceeded(
		int64(usage.Reservations),
		totalBytes,
		int64(additionalOutboxBytes),
		totalMetadataBytes,
		int64(additionalMetadataBytes),
	) {
		return ErrPendingCapacity
	}
	return nil
}

func pendingCapacityExceeded(count, totalBytes, additionalBytes, totalMetadataBytes, additionalMetadataBytes int64) bool {
	return count >= MaxPendingReservations ||
		totalBytes > MaxPendingOutboxBytes-additionalBytes ||
		totalMetadataBytes > MaxPendingMetadataBytes-additionalMetadataBytes
}

func readPendingUsage(ctx context.Context, q queryer) (PendingUsage, error) {
	var reservations, ungrouped, outboxBytes, metadataBytes, validOutboxes int64
	var oldestPending sql.NullInt64
	if err := q.QueryRowContext(ctx, `
		SELECT
			count(*),
			COALESCE(sum(length(outbox)), 0),
			COALESCE(sum(length(metadata)), 0),
			COALESCE(sum(
				CASE WHEN length(outbox) BETWEEN 1 AND ? THEN 1 ELSE 0 END
			), 0),
			min(created_at_unix_micro),
			COALESCE(sum(CASE WHEN NOT EXISTS (
				SELECT 1 FROM ingest_write_group_members AS member
				WHERE member.visibility_sequence = ingest_visibility_reservations.sequence
			) THEN 1 ELSE 0 END), 0)
		FROM ingest_visibility_reservations
		WHERE state = 'reserved'`, MaxOutboxBytes).Scan(
		&reservations,
		&outboxBytes,
		&metadataBytes,
		&validOutboxes,
		&oldestPending,
		&ungrouped,
	); err != nil {
		return PendingUsage{}, fmt.Errorf("read pending visibility usage: %w", err)
	}
	usage, err := decodePendingUsage(reservations, outboxBytes, validOutboxes)
	if err != nil {
		return PendingUsage{}, err
	}
	if metadataBytes < 0 || metadataBytes > MaxPendingMetadataBytes ||
		ungrouped < 0 || ungrouped > reservations {
		return PendingUsage{}, errors.New("invalid pending visibility metadata or grouping usage")
	}
	usage.MetadataBytes = uint64(metadataBytes)
	usage.UngroupedReservations = uint32(ungrouped)
	if oldestPending.Valid {
		usage.OldestPendingAt = time.UnixMicro(oldestPending.Int64).UTC()
	}
	var readyGroups, ambiguousGroups, liveLeases int64
	if err := q.QueryRowContext(ctx, `
		SELECT
			COALESCE(sum(CASE WHEN state = 'ready' THEN 1 ELSE 0 END), 0),
			COALESCE(sum(CASE WHEN state = 'ambiguous' THEN 1 ELSE 0 END), 0),
			COALESCE(sum(CASE WHEN state IN ('ready', 'ambiguous') AND attempt_id <> '' THEN 1 ELSE 0 END), 0)
		FROM ingest_write_groups`).Scan(&readyGroups, &ambiguousGroups, &liveLeases); err != nil {
		return PendingUsage{}, fmt.Errorf("read pending write group usage: %w", err)
	}
	if readyGroups < 0 || ambiguousGroups < 0 || liveLeases < 0 ||
		readyGroups > math.MaxUint32 || ambiguousGroups > math.MaxUint32 || liveLeases > math.MaxUint32 {
		return PendingUsage{}, errors.New("invalid pending write group usage")
	}
	usage.ReadyGroups = uint32(readyGroups)
	usage.AmbiguousGroups = uint32(ambiguousGroups)
	usage.LiveGroupLeases = uint32(liveLeases)
	return usage, nil
}

func decodePendingUsage(
	reservations int64,
	outboxBytes int64,
	validOutboxes int64,
) (PendingUsage, error) {
	switch {
	case reservations < 0:
		return PendingUsage{}, fmt.Errorf("invalid pending visibility reservation count %d", reservations)
	case outboxBytes < 0:
		return PendingUsage{}, fmt.Errorf("invalid pending visibility outbox byte count %d", outboxBytes)
	case validOutboxes < 0:
		return PendingUsage{}, fmt.Errorf("invalid valid pending visibility outbox count %d", validOutboxes)
	case reservations > MaxPendingReservations:
		return PendingUsage{}, fmt.Errorf(
			"pending visibility reservation count %d exceeds limit %d",
			reservations,
			MaxPendingReservations,
		)
	case outboxBytes > MaxPendingOutboxBytes:
		return PendingUsage{}, fmt.Errorf(
			"pending visibility outbox byte count %d exceeds limit %d",
			outboxBytes,
			MaxPendingOutboxBytes,
		)
	case (reservations == 0) != (outboxBytes == 0):
		return PendingUsage{}, fmt.Errorf(
			"inconsistent pending visibility usage: %d reservations contain %d outbox bytes",
			reservations,
			outboxBytes,
		)
	case validOutboxes != reservations:
		return PendingUsage{}, fmt.Errorf(
			"inconsistent pending visibility outboxes: %d of %d are valid",
			validOutboxes,
			reservations,
		)
	case outboxBytes < reservations:
		return PendingUsage{}, fmt.Errorf(
			"inconsistent pending visibility usage: %d reservations contain only %d outbox bytes",
			reservations,
			outboxBytes,
		)
	case outboxBytes > reservations*MaxOutboxBytes:
		return PendingUsage{}, fmt.Errorf(
			"inconsistent pending visibility usage: %d reservations contain %d outbox bytes",
			reservations,
			outboxBytes,
		)
	default:
		return PendingUsage{
			Reservations: uint32(reservations),
			OutboxBytes:  uint64(outboxBytes),
		}, nil
	}
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
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = ingest_visibility_reservations.sequence
		  )
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
// limit terminal rows. Accepted ClickHouse commits and whole-batch rejections
// have independent horizons, so a rejection flood cannot displace successful
// blocks from storage's deduplication horizon. The rejected horizon keeps the
// newest prefix satisfying both its row-count and optional metadata-byte
// ceilings. Rejections may prune through the last assigned sequence because
// they have no ClickHouse side effect; commits remain bounded by the contiguous
// cutoff. Abandoned rows keep using the committed allocation horizon so they
// cannot grow behind an old pending gap. An identity is deleted in the same
// transaction only after its last reservation is gone.
func (sequencer *SQLiteSequencer) PruneTerminal(
	ctx context.Context,
	retention TerminalRetention,
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
	if retention.Committed > math.MaxInt64 {
		abandonedEligible = false
	} else if retention.Committed > 0 {
		retained := int64(retention.Committed)
		if retained >= lastAssigned {
			abandonedEligible = false
		} else {
			abandonedThreshold = lastAssigned - retained
		}
	}
	committedThreshold, committedEligible, err := terminalPruneHorizon(
		ctx,
		tx,
		reservationCommitted,
		cutoff,
		retention.Committed,
	)
	if err != nil {
		return 0, err
	}
	rejectedThreshold, rejectedEligible, err := rejectedPruneHorizon(
		ctx,
		tx,
		lastAssigned,
		retention.Rejected,
		retention.RejectedMetadataBytes,
	)
	if err != nil {
		return 0, err
	}
	// Terminal membership is diagnostic only. Remove the retained physical
	// group authority before pruning any referenced logical reservation, in the
	// same transaction, so the member foreign key can never force a partial
	// logical prune.
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM ingest_write_groups
		WHERE state = 'committed' AND write_group_id IN (
			SELECT member.write_group_id
			FROM ingest_write_group_members AS member
			JOIN ingest_visibility_reservations AS reservation
			  ON reservation.sequence = member.visibility_sequence
			WHERE (reservation.state = 'abandoned' AND ? AND reservation.sequence <= ?)
			   OR (reservation.state = 'committed' AND ? AND reservation.sequence <= ?)
			   OR (reservation.state = 'rejected' AND ? AND reservation.sequence <= ?)
			GROUP BY member.write_group_id
			ORDER BY min(reservation.sequence)
			LIMIT ?
		)`,
		abandonedEligible,
		abandonedThreshold,
		committedEligible,
		committedThreshold,
		rejectedEligible,
		rejectedThreshold,
		limit,
	); err != nil {
		return 0, fmt.Errorf("delete terminal write groups before visibility prune: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM ingest_visibility_reservations
		WHERE sequence IN (
			SELECT sequence
			FROM ingest_visibility_reservations
			WHERE (state = 'abandoned' AND ? AND sequence <= ?)
			   OR (state = 'committed' AND ? AND sequence <= ?)
			   OR (state = 'rejected' AND ? AND sequence <= ?)
			ORDER BY sequence
			LIMIT ?
		)`,
		abandonedEligible,
		abandonedThreshold,
		committedEligible,
		committedThreshold,
		rejectedEligible,
		rejectedThreshold,
		limit,
	)
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

func terminalPruneHorizon(
	ctx context.Context,
	tx *sql.Tx,
	state string,
	throughSequence int64,
	retained uint64,
) (int64, bool, error) {
	if retained == 0 {
		return throughSequence, true, nil
	}
	if throughSequence <= 0 || retained > math.MaxInt64 || retained >= uint64(throughSequence) {
		return throughSequence, false, nil
	}
	var threshold int64
	err := tx.QueryRowContext(ctx, `
		SELECT sequence
		FROM ingest_visibility_reservations
		WHERE state = ? AND sequence <= ?
		ORDER BY sequence DESC
		LIMIT 1 OFFSET ?`, state, throughSequence, int64(retained)).Scan(&threshold)
	switch {
	case err == nil:
		return threshold, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return throughSequence, false, nil
	default:
		return 0, false, fmt.Errorf(
			"select %s visibility prune horizon: %w",
			state,
			err,
		)
	}
}

func rejectedPruneHorizon(
	ctx context.Context,
	tx *sql.Tx,
	throughSequence int64,
	retained uint64,
	retainedMetadataBytes uint64,
) (int64, bool, error) {
	if retainedMetadataBytes == 0 || retainedMetadataBytes > math.MaxInt64 {
		return terminalPruneHorizon(
			ctx,
			tx,
			reservationRejected,
			throughSequence,
			retained,
		)
	}
	if retained == 0 {
		return throughSequence, true, nil
	}
	if throughSequence <= 0 {
		return throughSequence, false, nil
	}

	countBounded := retained <= math.MaxInt64
	retainedRows := int64(0)
	candidateLimit := throughSequence
	if countBounded {
		retainedRows = int64(retained)
		if retainedRows < throughSequence {
			// The first row beyond the count ceiling is sufficient to choose
			// the deletion horizon, even when the byte ceiling does not bind.
			candidateLimit = retainedRows + 1
		}
	}

	var threshold int64
	err := tx.QueryRowContext(ctx, `
		WITH candidates AS MATERIALIZED (
			SELECT sequence, length(metadata) AS metadata_bytes
			FROM ingest_visibility_reservations
				INDEXED BY ingest_visibility_reservations_state_sequence_idx
			WHERE state = 'rejected' AND sequence <= ?
			ORDER BY sequence DESC
			LIMIT ?
		), prefixes AS (
			SELECT sequence,
			       ROW_NUMBER() OVER (ORDER BY sequence DESC) AS retained_rows,
			       SUM(metadata_bytes) OVER (
			           ORDER BY sequence DESC
			           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			       ) AS retained_metadata_bytes
			FROM candidates
		)
		SELECT sequence
		FROM prefixes
		WHERE (? AND retained_rows > ?)
		   OR retained_metadata_bytes > ?
		ORDER BY sequence DESC
		LIMIT 1`,
		throughSequence,
		candidateLimit,
		countBounded,
		retainedRows,
		int64(retainedMetadataBytes),
	).Scan(&threshold)
	switch {
	case err == nil:
		return threshold, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return throughSequence, false, nil
	default:
		return 0, false, fmt.Errorf("select rejected visibility prune horizon: %w", err)
	}
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
