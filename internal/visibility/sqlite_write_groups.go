package visibility

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"fortio.org/safecast"
)

const writeGroupMembershipFormat = "open-splunk/write-group-membership/v1\x00"

var _ WriteGroupSequencer = (*SQLiteSequencer)(nil)

// AcquireUngroupedAmbiguous leases the oldest pre-group reservation whose
// ClickHouse send may already have occurred. This compatibility recovery path
// always runs before grouped work, preserving the original per-batch
// deduplication token across an upgrade or restored snapshot.
func (sequencer *SQLiteSequencer) AcquireUngroupedAmbiguous(
	ctx context.Context,
	attemptID string,
) (reservation Reservation, found bool, resultErr error) {
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
		return Reservation{}, false, fmt.Errorf("begin ungrouped ambiguous acquisition: %w", err)
	}
	defer rollback(tx)
	var sequence int64
	var priorOwner string
	err = tx.QueryRowContext(ctx, `
		SELECT reservation.sequence, reservation.attempt_id
		FROM ingest_visibility_reservations AS reservation
		WHERE reservation.state = 'reserved' AND reservation.phase = 'ambiguous'
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = reservation.sequence
		  )
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_groups AS earlier_group
		      WHERE earlier_group.state = 'ambiguous'
		        AND earlier_group.first_sequence < reservation.sequence
		  )
		ORDER BY reservation.sequence
		LIMIT 1`).Scan(&sequence, &priorOwner)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return Reservation{}, false, fmt.Errorf("commit empty ungrouped ambiguous acquisition: %w", err)
		}
		return Reservation{}, false, nil
	}
	if err != nil {
		return Reservation{}, false, fmt.Errorf("read ungrouped ambiguous reservation: %w", err)
	}
	if priorOwner != "" && sequencer.leases.contains(priorOwner) {
		return Reservation{}, false, ErrAttemptInProgress
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET attempt_id = ?
		WHERE sequence = ? AND state = 'reserved' AND phase = 'ambiguous'
		  AND attempt_id = ?`, attemptID, sequence, priorOwner)
	if err != nil {
		return Reservation{}, false, fmt.Errorf("acquire ungrouped ambiguous reservation: %w", err)
	}
	if err := requireOneRow(result, "acquire ungrouped ambiguous reservation"); err != nil {
		return Reservation{}, false, err
	}
	reservation, err = queryReservationBySequence(ctx, tx, sequence)
	if err != nil {
		return Reservation{}, false, fmt.Errorf("hydrate ungrouped ambiguous reservation: %w", err)
	}
	reservation.PreviouslyReserved = true
	if err := tx.Commit(); err != nil {
		return Reservation{}, false, fmt.Errorf("commit ungrouped ambiguous acquisition: %w", err)
	}
	sequencer.leases.bind(attemptID, reservation.Sequence)
	retainLease = true
	return reservation, true, nil
}

// ComputeWriteGroupMembershipSHA256 seals the ordered logical identities and
// replay payloads represented by a physical write group.
func ComputeWriteGroupMembershipSHA256(members []WriteGroupMember) ([32]byte, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte(writeGroupMembershipFormat))
	for index, member := range members {
		if member.Ordinal != uint32(index) || member.Reservation.Sequence == 0 ||
			member.Reservation.Sequence > math.MaxInt64 || member.Reservation.BatchKey == "" ||
			len(member.Reservation.BatchKey) > maxBatchKeyBytes || member.RowCount == 0 ||
			member.DecodedBytes == 0 {
			return [32]byte{}, fmt.Errorf("%w: invalid write group member %d", ErrInvalidArgument, index)
		}
		writeDigestUint64(hash, member.Reservation.Sequence)
		writeDigestBytes(hash, []byte(member.Reservation.BatchKey))
		_, _ = hash.Write(member.Reservation.PayloadSHA256[:])
		writeDigestUint64(hash, member.OutboxLength)
		_, _ = hash.Write(member.OutboxSHA256[:])
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestUint64(writer digestWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeDigestBytes(writer digestWriter, value []byte) {
	writeDigestUint64(writer, uint64(len(value)))
	_, _ = writer.Write(value)
}

// FormOrAcquireWriteGroup leases the oldest recoverable group, or atomically
// seals eligible ungrouped reservations. A nonzero deadline is returned only
// when sparse work is waiting for its durable linger threshold.
func (sequencer *SQLiteSequencer) FormOrAcquireWriteGroup(
	ctx context.Context,
	attemptID string,
	limits WriteGroupLimits,
	now time.Time,
) (group WriteGroup, found bool, nextLingerDeadline time.Time, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return WriteGroup{}, false, time.Time{}, err
	}
	defer sequencer.endOperation()
	if err := validateWriteGroupRequest(ctx, attemptID, limits, now); err != nil {
		return WriteGroup{}, false, time.Time{}, err
	}
	if !sequencer.leases.activate(attemptID) {
		return WriteGroup{}, false, time.Time{}, ErrAttemptInProgress
	}
	retainLease := false
	defer func() {
		if !retainLease {
			sequencer.leases.deactivate(attemptID)
		}
	}()

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return WriteGroup{}, false, time.Time{}, fmt.Errorf("begin write group acquisition: %w", err)
	}
	defer rollback(tx)

	groupID, existing, blocked, err := sequencer.acquireExistingWriteGroup(ctx, tx, attemptID)
	if err != nil {
		return WriteGroup{}, false, time.Time{}, err
	}
	if blocked {
		if err := tx.Commit(); err != nil {
			return WriteGroup{}, false, time.Time{}, fmt.Errorf("commit blocked write group acquisition: %w", err)
		}
		return WriteGroup{}, false, time.Time{}, nil
	}
	fillReason := WriteGroupFillRecovery
	newlyFormed := false
	if !existing {
		members, seal, deadline, selectErr := selectWriteGroupMembers(
			ctx,
			tx,
			sequencer.leases,
			limits,
			now,
		)
		if selectErr != nil {
			return WriteGroup{}, false, time.Time{}, selectErr
		}
		if !seal {
			if err := tx.Commit(); err != nil {
				return WriteGroup{}, false, time.Time{}, fmt.Errorf("commit deferred write group formation: %w", err)
			}
			return WriteGroup{}, false, deadline, nil
		}
		fillReason = classifyWriteGroupFill(members, limits, now)
		newlyFormed = true
		groupID, err = newWriteGroupID()
		if err != nil {
			return WriteGroup{}, false, time.Time{}, err
		}
		if err := persistWriteGroup(ctx, tx, groupID, attemptID, members, now); err != nil {
			return WriteGroup{}, false, time.Time{}, err
		}
	}

	group, err = queryWriteGroup(ctx, tx, groupID)
	if err != nil {
		return WriteGroup{}, false, time.Time{}, err
	}
	group.FillReason = fillReason
	group.NewlyFormed = newlyFormed
	if err := tx.Commit(); err != nil {
		return WriteGroup{}, false, time.Time{}, fmt.Errorf("commit write group acquisition: %w", err)
	}
	sequencer.leases.bindGroup(attemptID, group.ID)
	retainLease = true
	return group, true, time.Time{}, nil
}

func classifyWriteGroupFill(
	members []WriteGroupMember,
	limits WriteGroupLimits,
	now time.Time,
) WriteGroupFillReason {
	var rows uint64
	var decodedBytes uint64
	for _, member := range members {
		rows += uint64(member.RowCount)
		decodedBytes += member.DecodedBytes
	}
	if rows >= uint64(limits.TargetRows) {
		return WriteGroupFillRowTarget
	}
	if decodedBytes >= limits.TargetDecodedBytes {
		return WriteGroupFillByteTarget
	}
	if limits.ForceSeal {
		return WriteGroupFillDrain
	}
	if len(members) != 0 && !now.Before(members[0].Reservation.CreatedAt.Add(limits.MaxLinger)) {
		return WriteGroupFillLinger
	}
	return WriteGroupFillHardBoundary
}

func validateWriteGroupRequest(ctx context.Context, attemptID string, limits WriteGroupLimits, now time.Time) error {
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return err
	}
	if now.IsZero() {
		return fmt.Errorf("%w: write group clock is required", ErrInvalidArgument)
	}
	if limits.TargetRows == 0 || limits.HardMaxRows < limits.TargetRows ||
		limits.HardMaxRows > MaxWriteGroupRows || limits.TargetDecodedBytes == 0 ||
		limits.HardMaxDecodedBytes < limits.TargetDecodedBytes ||
		limits.HardMaxDecodedBytes > MaxWriteGroupDecodedBytes || limits.MaxMembers == 0 ||
		limits.MaxMembers > MaxWriteGroupMembers || limits.MaxLinger <= 0 {
		return fmt.Errorf("%w: invalid write group limits", ErrInvalidArgument)
	}
	return nil
}

func (sequencer *SQLiteSequencer) acquireExistingWriteGroup(
	ctx context.Context,
	tx *sql.Tx,
	attemptID string,
) (groupID string, found bool, blocked bool, resultErr error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT write_group_id, state, attempt_id
		FROM ingest_write_groups
		WHERE state IN ('ambiguous', 'ready')
		ORDER BY CASE state WHEN 'ambiguous' THEN 0 ELSE 1 END, first_sequence`)
	if err != nil {
		return "", false, false, fmt.Errorf("read recoverable write groups: %w", err)
	}
	var candidateID, candidateState, candidateOwner string
	if rows.Next() {
		var scannedID, state, owner string
		if err := rows.Scan(&scannedID, &state, &owner); err != nil {
			_ = rows.Close()
			return "", false, false, fmt.Errorf("scan recoverable write group: %w", err)
		}
		if owner != "" && owner != attemptID && sequencer.leases.contains(owner) {
			// A live ambiguous owner is the global send barrier. The oldest ready
			// owner likewise represents the sole physical sender.
			_ = rows.Close()
			return "", false, true, nil
		}
		candidateID, candidateState, candidateOwner = scannedID, state, owner
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return "", false, false, fmt.Errorf("iterate recoverable write groups: %w", err)
	}
	if err := rows.Close(); err != nil {
		return "", false, false, fmt.Errorf("close recoverable write groups: %w", err)
	}
	if candidateID == "" {
		return "", false, false, nil
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET attempt_id = ?
		WHERE write_group_id = ? AND state = ? AND attempt_id = ?`,
		attemptID, candidateID, candidateState, candidateOwner)
	if err != nil {
		return "", false, false, fmt.Errorf("acquire write group lease: %w", err)
	}
	if err := requireOneRow(result, "acquire write group lease"); err != nil {
		return "", false, false, err
	}
	return candidateID, true, false, nil
}

func selectWriteGroupMembers(
	ctx context.Context,
	tx *sql.Tx,
	leases *processLeases,
	limits WriteGroupLimits,
	now time.Time,
) ([]WriteGroupMember, bool, time.Time, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT r.sequence, r.batch_key, i.payload_sha256, r.stored_row_count,
		       r.decoded_event_bytes, r.outbox_sha256, length(r.outbox),
		       r.created_at_unix_micro, r.attempt_id
		FROM ingest_visibility_reservations AS r
		JOIN ingest_batch_identities AS i ON i.batch_key = r.batch_key
		WHERE r.state = 'reserved' AND r.phase = 'unsent'
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = r.sequence
		  )
		ORDER BY r.sequence
		LIMIT ?`, MaxPendingReservations)
	if err != nil {
		return nil, false, time.Time{}, fmt.Errorf("select write group candidates: %w", err)
	}
	defer rows.Close()

	members := make([]WriteGroupMember, 0, limits.MaxMembers)
	type inactiveOwner struct {
		sequence int64
		owner    string
	}
	inactiveOwners := make([]inactiveOwner, 0)
	var totalRows uint64
	var totalBytes uint64
	var oldest time.Time
	hardBoundary := false
	for rows.Next() {
		var sequence, rowCount, decodedBytes, outboxLength, createdAtMicros int64
		var batchKey, priorOwner string
		var payloadDigest, outboxDigest []byte
		if err := rows.Scan(
			&sequence,
			&batchKey,
			&payloadDigest,
			&rowCount,
			&decodedBytes,
			&outboxDigest,
			&outboxLength,
			&createdAtMicros,
			&priorOwner,
		); err != nil {
			return nil, false, time.Time{}, fmt.Errorf("scan write group candidate: %w", err)
		}
		if len(members) == int(limits.MaxMembers) {
			hardBoundary = true
			break
		}
		if priorOwner != "" && leases.contains(priorOwner) {
			continue
		}
		if sequence <= 0 || rowCount <= 0 || decodedBytes <= 0 || outboxLength <= 0 ||
			len(payloadDigest) != sha256.Size || len(outboxDigest) != sha256.Size ||
			rowCount > int64(limits.HardMaxRows) ||
			decodedBytes > safecast.MustConv[int64](limits.HardMaxDecodedBytes) {
			return nil, false, time.Time{}, errors.New("invalid persisted write group candidate")
		}
		candidateRows := safecast.MustConv[uint64](rowCount)
		candidateBytes := safecast.MustConv[uint64](decodedBytes)
		if totalRows+candidateRows > uint64(limits.HardMaxRows) ||
			totalBytes+candidateBytes > limits.HardMaxDecodedBytes {
			if len(members) == 0 {
				return nil, false, time.Time{}, errors.New("single reservation exceeds write group hard limits")
			}
			hardBoundary = true
			break
		}
		member := WriteGroupMember{
			Ordinal: safecast.MustConv[uint32](len(members)),
			Reservation: Reservation{
				BatchKey:          batchKey,
				Sequence:          safecast.MustConv[uint64](sequence),
				StoredRowCount:    safecast.MustConv[uint32](rowCount),
				DecodedEventBytes: safecast.MustConv[uint64](decodedBytes),
				CreatedAt:         time.UnixMicro(createdAtMicros).UTC(),
			},
			RowCount:     safecast.MustConv[uint32](rowCount),
			DecodedBytes: safecast.MustConv[uint64](decodedBytes),
			OutboxLength: safecast.MustConv[uint64](outboxLength),
		}
		copy(member.Reservation.PayloadSHA256[:], payloadDigest)
		copy(member.OutboxSHA256[:], outboxDigest)
		members = append(members, member)
		if priorOwner != "" {
			inactiveOwners = append(inactiveOwners, inactiveOwner{sequence: sequence, owner: priorOwner})
		}
		totalRows += candidateRows
		totalBytes += candidateBytes
		if oldest.IsZero() {
			oldest = member.Reservation.CreatedAt
		}
		if totalRows >= uint64(limits.TargetRows) || totalBytes >= limits.TargetDecodedBytes {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, time.Time{}, fmt.Errorf("iterate write group candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, false, time.Time{}, fmt.Errorf("close write group candidates: %w", err)
	}
	for _, inactive := range inactiveOwners {
		result, updateErr := tx.ExecContext(ctx, `
			UPDATE ingest_visibility_reservations
			SET attempt_id = ''
			WHERE sequence = ? AND state = 'reserved' AND phase = 'unsent'
			  AND attempt_id = ?`, inactive.sequence, inactive.owner)
		if updateErr != nil {
			return nil, false, time.Time{}, fmt.Errorf("reclaim inactive write group member lease: %w", updateErr)
		}
		if updateErr = requireOneRow(result, "reclaim inactive write group member lease"); updateErr != nil {
			return nil, false, time.Time{}, updateErr
		}
	}
	if len(members) == 0 {
		return nil, false, time.Time{}, nil
	}
	deadline := oldest.Add(limits.MaxLinger)
	seal := limits.ForceSeal || hardBoundary || totalRows >= uint64(limits.TargetRows) ||
		totalBytes >= limits.TargetDecodedBytes || !now.Before(deadline)
	if !seal {
		return nil, false, deadline, nil
	}
	return members, true, time.Time{}, nil
}

func newWriteGroupID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate write group ID: %w", err)
	}
	return "wg_" + hex.EncodeToString(entropy[:]), nil
}

func persistWriteGroup(
	ctx context.Context,
	tx *sql.Tx,
	groupID string,
	attemptID string,
	members []WriteGroupMember,
	createdAt time.Time,
) error {
	if len(members) == 0 || len(members) > MaxWriteGroupMembers {
		return fmt.Errorf("%w: invalid write group member count", ErrInvalidArgument)
	}
	digest, err := ComputeWriteGroupMembershipSHA256(members)
	if err != nil {
		return err
	}
	var rowCount uint64
	var decodedBytes uint64
	for _, member := range members {
		rowCount += uint64(member.RowCount)
		decodedBytes += member.DecodedBytes
	}
	if rowCount == 0 || rowCount > MaxWriteGroupRows || decodedBytes == 0 ||
		decodedBytes > MaxWriteGroupDecodedBytes {
		return errors.New("write group totals exceed schema limits")
	}
	createdAtMicros := createdAt.UTC().UnixMicro()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_write_groups
			(write_group_id, state, attempt_id, member_count, row_count, decoded_bytes,
			 membership_sha256, first_sequence, last_sequence, created_at_unix_micro,
			 sending_at_unix_micro, committed_at_unix_micro)
		VALUES (?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
		groupID,
		attemptID,
		len(members),
		rowCount,
		decodedBytes,
		digest[:],
		members[0].Reservation.Sequence,
		members[len(members)-1].Reservation.Sequence,
		createdAtMicros,
	); err != nil {
		return fmt.Errorf("persist write group: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
		INSERT INTO ingest_write_group_members
			(write_group_id, ordinal, visibility_sequence, row_count, decoded_bytes,
			 outbox_sha256)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare write group membership: %w", err)
	}
	defer statement.Close()
	for _, member := range members {
		if _, err := statement.ExecContext(
			ctx,
			groupID,
			member.Ordinal,
			member.Reservation.Sequence,
			member.RowCount,
			member.DecodedBytes,
			member.OutboxSHA256[:],
		); err != nil {
			return fmt.Errorf("persist write group member %d: %w", member.Ordinal, err)
		}
	}
	return nil
}

func queryWriteGroup(ctx context.Context, q queryer, groupID string) (WriteGroup, error) {
	var group WriteGroup
	var state string
	var memberCount, rowCount, decodedBytes, firstSequence, lastSequence int64
	var membershipDigest []byte
	var createdAtMicros int64
	var sendingAtMicros, committedAtMicros sql.NullInt64
	err := q.QueryRowContext(ctx, `
		SELECT write_group_id, state, attempt_id, member_count, row_count,
		       decoded_bytes, membership_sha256, first_sequence, last_sequence,
		       created_at_unix_micro, sending_at_unix_micro, committed_at_unix_micro
		FROM ingest_write_groups
		WHERE write_group_id = ?`, groupID).Scan(
		&group.ID,
		&state,
		&group.AttemptID,
		&memberCount,
		&rowCount,
		&decodedBytes,
		&membershipDigest,
		&firstSequence,
		&lastSequence,
		&createdAtMicros,
		&sendingAtMicros,
		&committedAtMicros,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return WriteGroup{}, ErrNotFound
	}
	if err != nil {
		return WriteGroup{}, fmt.Errorf("read write group: %w", err)
	}
	if memberCount <= 0 || memberCount > MaxWriteGroupMembers || rowCount <= 0 ||
		rowCount > MaxWriteGroupRows || decodedBytes <= 0 ||
		decodedBytes > MaxWriteGroupDecodedBytes || firstSequence <= 0 ||
		lastSequence < firstSequence || len(membershipDigest) != sha256.Size {
		return WriteGroup{}, errors.New("invalid persisted write group header")
	}
	group.State = WriteGroupState(state)
	group.RowCount = safecast.MustConv[uint32](rowCount)
	group.DecodedBytes = safecast.MustConv[uint64](decodedBytes)
	group.FirstSequence = safecast.MustConv[uint64](firstSequence)
	group.LastSequence = safecast.MustConv[uint64](lastSequence)
	group.CreatedAt = time.UnixMicro(createdAtMicros).UTC()
	copy(group.MembershipSHA256[:], membershipDigest)
	if sendingAtMicros.Valid {
		group.SendingAt = time.UnixMicro(sendingAtMicros.Int64).UTC()
	}
	if committedAtMicros.Valid {
		group.CommittedAt = time.UnixMicro(committedAtMicros.Int64).UTC()
	}

	return hydrateWriteGroupMembers(ctx, q, group, memberCount)
}

func hydrateWriteGroupMembers(ctx context.Context, q queryer, group WriteGroup, memberCount int64) (WriteGroup, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT member.ordinal, member.row_count, member.decoded_bytes,
		       member.outbox_sha256, r.sequence, i.batch_key, i.sequence_key,
		       r.state, r.phase, r.index_time_unix_milli, i.payload_sha256,
		       r.metadata, r.outbox, r.outbox_sha256, r.stored_row_count,
		       r.decoded_event_bytes, r.created_at_unix_micro,
		       r.committed_at_unix_micro
		FROM ingest_write_group_members AS member
		JOIN ingest_visibility_reservations AS r
		  ON r.sequence = member.visibility_sequence
		JOIN ingest_batch_identities AS i ON i.batch_key = r.batch_key
		WHERE member.write_group_id = ?
		ORDER BY member.ordinal`, group.ID)
	if err != nil {
		return WriteGroup{}, fmt.Errorf("read write group members: %w", err)
	}
	defer rows.Close()
	var totalRows uint64
	var totalBytes uint64
	for rows.Next() {
		var member WriteGroupMember
		var ordinal, memberRows, memberBytes int64
		var memberDigest []byte
		var sequence, indexTimeMillis, storedRows, storedBytes, createdAtMicros int64
		var state, phase string
		var payloadDigest, metadata, outbox, storedOutboxDigest []byte
		var committedAt sql.NullInt64
		if err := rows.Scan(
			&ordinal,
			&memberRows,
			&memberBytes,
			&memberDigest,
			&sequence,
			&member.Reservation.BatchKey,
			&member.Reservation.SequenceKey,
			&state,
			&phase,
			&indexTimeMillis,
			&payloadDigest,
			&metadata,
			&outbox,
			&storedOutboxDigest,
			&storedRows,
			&storedBytes,
			&createdAtMicros,
			&committedAt,
		); err != nil {
			return WriteGroup{}, fmt.Errorf("scan write group member: %w", err)
		}
		if ordinal != int64(len(group.Members)) || memberRows <= 0 || memberBytes <= 0 ||
			sequence <= 0 || len(memberDigest) != sha256.Size ||
			len(payloadDigest) != sha256.Size || len(storedOutboxDigest) != sha256.Size ||
			storedRows != memberRows || storedBytes != memberBytes {
			return WriteGroup{}, errors.New("invalid persisted write group membership")
		}
		member.Ordinal = safecast.MustConv[uint32](ordinal)
		member.RowCount = safecast.MustConv[uint32](memberRows)
		member.DecodedBytes = safecast.MustConv[uint64](memberBytes)
		member.OutboxLength = safecast.MustConv[uint64](len(outbox))
		copy(member.OutboxSHA256[:], memberDigest)
		member.Reservation.Sequence = safecast.MustConv[uint64](sequence)
		member.Reservation.AlreadyCommitted = state == reservationCommitted
		member.Reservation.MayHaveReachedStorage = phase == phaseAmbiguous || state == reservationCommitted
		member.Reservation.IndexTime = time.UnixMilli(indexTimeMillis).UTC()
		copy(member.Reservation.PayloadSHA256[:], payloadDigest)
		member.Reservation.Metadata = metadata
		member.Reservation.Outbox = outbox
		copy(member.Reservation.OutboxSHA256[:], storedOutboxDigest)
		member.Reservation.StoredRowCount = safecast.MustConv[uint32](storedRows)
		member.Reservation.DecodedEventBytes = safecast.MustConv[uint64](storedBytes)
		member.Reservation.CreatedAt = time.UnixMicro(createdAtMicros).UTC()
		if committedAt.Valid {
			member.Reservation.CommittedAt = time.UnixMicro(committedAt.Int64).UTC()
		}
		actualOutboxDigest := sha256.Sum256(outbox)
		if member.OutboxSHA256 != member.Reservation.OutboxSHA256 ||
			actualOutboxDigest != member.OutboxSHA256 ||
			(state != reservationReserved && state != reservationCommitted) {
			return WriteGroup{}, errors.New("write group member does not match its reservation")
		}
		group.Members = append(group.Members, member)
		totalRows += uint64(member.RowCount)
		totalBytes += member.DecodedBytes
	}
	if err := rows.Err(); err != nil {
		return WriteGroup{}, fmt.Errorf("iterate write group members: %w", err)
	}
	if int64(len(group.Members)) != memberCount || len(group.Members) == 0 ||
		group.Members[0].Reservation.Sequence != group.FirstSequence ||
		group.Members[len(group.Members)-1].Reservation.Sequence != group.LastSequence ||
		totalRows != uint64(group.RowCount) || totalBytes != group.DecodedBytes {
		return WriteGroup{}, errors.New("write group totals do not match its membership")
	}
	digest, err := ComputeWriteGroupMembershipSHA256(group.Members)
	if err != nil {
		return WriteGroup{}, err
	}
	if digest != group.MembershipSHA256 {
		return WriteGroup{}, errors.New("write group membership digest mismatch")
	}
	return group, nil
}

// MarkWriteGroupSending atomically marks a sealed group and every logical
// member ambiguous immediately before the physical Send.
func (sequencer *SQLiteSequencer) MarkWriteGroupSending(ctx context.Context, groupID, attemptID string) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if err := validateWriteGroupAttempt(ctx, groupID, attemptID); err != nil {
		return err
	}
	if !sequencer.leases.ownsGroup(attemptID, groupID) {
		return ErrAttemptLease
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin write group sending transition: %w", err)
	}
	defer rollback(tx)
	var state, owner string
	var firstSequence, createdAtMicros int64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id, first_sequence, created_at_unix_micro
		FROM ingest_write_groups WHERE write_group_id = ?`, groupID).Scan(
		&state,
		&owner,
		&firstSequence,
		&createdAtMicros,
	); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("read write group before sending: %w", err)
	}
	if owner != attemptID || state != string(WriteGroupReady) && state != string(WriteGroupAmbiguous) {
		return ErrAttemptLease
	}
	var barrier int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ingest_write_groups
			WHERE state = 'ambiguous' AND write_group_id <> ?
			  AND (? = 'ready' OR first_sequence < ?)
		)`, groupID, state, firstSequence).Scan(&barrier); err != nil {
		return fmt.Errorf("read write group ambiguous barrier: %w", err)
	}
	if barrier != 0 {
		return ErrAmbiguousBarrier
	}
	expectedPhase := phaseUnsent
	if state == string(WriteGroupAmbiguous) {
		expectedPhase = phaseAmbiguous
	}
	statement, err := tx.PrepareContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET phase = 'ambiguous'
		WHERE sequence = ? AND state = 'reserved' AND phase = ? AND attempt_id = ''`)
	if err != nil {
		return fmt.Errorf("prepare write group member sending transition: %w", err)
	}
	defer statement.Close()
	rows, err := tx.QueryContext(ctx, `
		SELECT visibility_sequence FROM ingest_write_group_members
		WHERE write_group_id = ? ORDER BY ordinal`, groupID)
	if err != nil {
		return fmt.Errorf("read write group member sequences: %w", err)
	}
	var sequences []int64
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan write group member sequence: %w", err)
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close write group member sequences: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate write group member sequences: %w", err)
	}
	for _, sequence := range sequences {
		result, err := statement.ExecContext(ctx, sequence, expectedPhase)
		if err != nil {
			return fmt.Errorf("mark write group member ambiguous: %w", err)
		}
		if err := requireOneRow(result, "mark write group member ambiguous"); err != nil {
			return err
		}
	}
	if state == string(WriteGroupReady) {
		sendingAt := time.Now().UTC()
		if sequencer.now != nil {
			sendingAt = sequencer.now().UTC()
		}
		createdAt := time.UnixMicro(createdAtMicros).UTC()
		if sendingAt.Before(createdAt) {
			sendingAt = createdAt
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE ingest_write_groups
			SET state = 'ambiguous', sending_at_unix_micro = ?
			WHERE write_group_id = ? AND state = 'ready' AND attempt_id = ?`,
			sendingAt.UnixMicro(), groupID, attemptID)
		if err != nil {
			return fmt.Errorf("mark write group ambiguous: %w", err)
		}
		if err := requireOneRow(result, "mark write group ambiguous"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit write group sending transition: %w", err)
	}
	return nil
}

// CommitWriteGroup atomically commits every logical member, fires the existing
// HEC terminal triggers, clears all outboxes, and advances visibility once.
func (sequencer *SQLiteSequencer) CommitWriteGroup(
	ctx context.Context,
	groupID string,
	attemptID string,
	committedAt time.Time,
) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if err := validateWriteGroupAttempt(ctx, groupID, attemptID); err != nil {
		return err
	}
	if committedAt.IsZero() {
		return fmt.Errorf("%w: committed time is required", ErrInvalidArgument)
	}
	committedAt = committedAt.Round(0).UTC()
	committedAtMicros := committedAt.UnixMicro()
	if !sequencer.leases.ownsGroup(attemptID, groupID) {
		return ErrAttemptLease
	}
	defer sequencer.leases.deactivate(attemptID)
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin write group commit: %w", err)
	}
	defer rollback(tx)
	var state, owner string
	var memberCount int64
	var sendingAtMicros sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id, member_count, sending_at_unix_micro
		FROM ingest_write_groups WHERE write_group_id = ?`, groupID).Scan(
		&state,
		&owner,
		&memberCount,
		&sendingAtMicros,
	); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("read write group before commit: %w", err)
	}
	if state != string(WriteGroupAmbiguous) || owner != attemptID || !sendingAtMicros.Valid {
		return ErrAttemptLease
	}
	if committedAtMicros < sendingAtMicros.Int64 {
		committedAtMicros = sendingAtMicros.Int64
	}
	statement, err := tx.PrepareContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET state = 'committed', phase = 'final', outbox = X'', attempt_id = '',
		    committed_at_unix_micro = ?
		WHERE sequence = ? AND state = 'reserved' AND phase = 'ambiguous'
		  AND attempt_id = ''`)
	if err != nil {
		return fmt.Errorf("prepare write group member commit: %w", err)
	}
	defer statement.Close()
	rows, err := tx.QueryContext(ctx, `
		SELECT visibility_sequence FROM ingest_write_group_members
		WHERE write_group_id = ? ORDER BY ordinal`, groupID)
	if err != nil {
		return fmt.Errorf("read committing write group members: %w", err)
	}
	var sequences []int64
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan committing write group member: %w", err)
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close committing write group members: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate committing write group members: %w", err)
	}
	if len(sequences) == 0 || int64(len(sequences)) != memberCount {
		return errors.New("write group member count does not match its header")
	}
	for _, sequence := range sequences {
		result, err := statement.ExecContext(ctx, committedAtMicros, sequence)
		if err != nil {
			return fmt.Errorf("commit write group member: %w", err)
		}
		if err := requireOneRow(result, "commit write group member"); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET state = 'committed', attempt_id = '', committed_at_unix_micro = ?
		WHERE write_group_id = ? AND state = 'ambiguous' AND attempt_id = ?`,
		committedAtMicros, groupID, attemptID)
	if err != nil {
		return fmt.Errorf("commit write group: %w", err)
	}
	if err := requireOneRow(result, "commit write group"); err != nil {
		return err
	}
	if err := advanceCutoff(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit write group transaction: %w", err)
	}
	return nil
}

// ReleaseWriteGroup removes only the live-process group lease. Sealed
// membership and ready/ambiguous state remain authoritative for recovery.
func (sequencer *SQLiteSequencer) ReleaseWriteGroup(ctx context.Context, groupID, attemptID string) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if err := validateWriteGroupAttempt(ctx, groupID, attemptID); err != nil {
		return err
	}
	if !sequencer.leases.ownsGroup(attemptID, groupID) {
		return ErrAttemptLease
	}
	defer sequencer.leases.deactivate(attemptID)
	result, err := sequencer.db.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET attempt_id = ''
		WHERE write_group_id = ? AND state IN ('ready', 'ambiguous') AND attempt_id = ?`,
		groupID, attemptID)
	if err != nil {
		return fmt.Errorf("release write group lease: %w", err)
	}
	return requireOneRow(result, "release write group lease")
}

func validateWriteGroupAttempt(ctx context.Context, groupID, attemptID string) error {
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return err
	}
	if groupID == "" || len(groupID) > 64 {
		return fmt.Errorf("%w: write group ID is invalid", ErrInvalidArgument)
	}
	return nil
}
